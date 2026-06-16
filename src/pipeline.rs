//! [`Pipeline`] — `a | b | c` without a shell.
//!
//! Each stage's stdout feeds the next stage's stdin — **no shell string**, so no
//! quoting or injection surface, and no `sh -c`. The connection is a small
//! in-process relay (a `tokio::io::copy` task per boundary), not a kernel pipe
//! spliced fd-to-fd; this has two consequences: a producer whose consumer exits
//! early stops on a [broken pipe](crate::Error) when the relay's next write fails
//! (rather than instantly via SIGPIPE), and the relay's own I/O is plumbing — a
//! closed sibling reads as EOF / writes as a broken pipe, neither reported as a
//! stage's stdin failure. Every stage spawns into one shared kill-on-drop
//! [`ProcessGroup`](crate::ProcessGroup), so the whole chain dies as a unit, and
//! the outcome is **pipefail**: the first stage without a clean exit decides the
//! reported code/diagnostics.

use std::time::Duration;

use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::{Outcome, ProcessResult};
use crate::running::Finished;
use tokio::task::AbortHandle;

/// Aborts in-flight stage drain tasks when the chain exits early (whole-chain
/// timeout or stage error) instead of detaching them to linger until the killed
/// children close their pipes. On the success path the tasks have already
/// completed, so the abort is a no-op.
struct AbortTasksOnDrop(Vec<AbortHandle>);

impl Drop for AbortTasksOnDrop {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

/// A chain of [`Command`]s connected stdout→stdin — built with
/// [`Command::pipe`], extended with [`pipe`](Self::pipe), driven with the same
/// verb vocabulary as a single [`Command`]:
/// [`output_string`](Self::output_string) / [`output_bytes`](Self::output_bytes)
/// for capture, [`run`](Self::run) / [`run_unit`](Self::run_unit) /
/// [`checked`](Self::checked) for success-checked runs,
/// [`exit_code`](Self::exit_code) / [`probe`](Self::probe) for the code, and
/// [`parse`](Self::parse) / [`try_parse`](Self::try_parse) for typed output —
/// each operating on the **pipefail** outcome. Bound the whole chain with
/// [`timeout`](Self::timeout) / [`cancel_on`](Self::cancel_on).
///
/// The streaming [`first_line`](Command::first_line) probe is intentionally
/// **not** on `Pipeline`: a chain consumes its last stage in full to fold the
/// pipefail outcome. To *capture* a finished chain's first matching line, add a
/// `| head -n1` (or `findstr`/`grep -m1`) stage and capture. A streaming
/// readiness probe over a chain that must **keep running** (wait for a banner
/// line, then leave the chain alive) is not supported — use a single
/// [`Command`] with `first_line`, since `| head` would tear the chain down.
///
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::Command;
///
/// let out = Command::new("git").args(["log", "--format=%an"])
///     .pipe(Command::new("sort"))
///     .pipe(Command::new("uniq").arg("-c"))
///     .output_string()
///     .await?;
/// println!("{}", out.stdout());
/// # Ok(())
/// # }
/// ```
///
/// Semantics:
///
/// - **One group, one fate** — all stages run inside a private
///   kill-on-drop group; cancelling the future (or a
///   [`timeout`](Self::timeout) elapsing) tears the whole chain down.
/// - **Pipefail** — `stdout` is always the *last* stage's output; `code`,
///   `stderr`, and the reported program come from the **first** stage that
///   didn't exit cleanly (non-zero, signal-killed, or timed out), or from the
///   last stage when every stage succeeded. Stages marked
///   [`unchecked_in_pipe`](Command::unchecked_in_pipe) are exempt: their unclean exits are
///   skipped during attribution (checked failures always trump unchecked
///   ones; a chain whose only failures are unchecked reports success) — the
///   `producer | head -1` escape hatch.
/// - **Stdin/stdout at the ends** — the *first* stage's configured
///   [`stdin`](Command::stdin) source is honored; inner stages' stdin is the
///   pipe (any configured source or `keep_stdin_open` on them is overridden).
///   Inner stages' stdout goes to the next stage; their stderr is captured
///   per-stage for pipefail diagnostics.
/// - Per-stage [`Command::timeout`]s still apply to their own stage; a staged
///   timeout surfaces as that stage's failure. [`timeout`](Self::timeout)
///   bounds the whole chain.
/// - A per-stage [`Command::retry`] is **not** applied: stages run through the
///   streaming `start` path (which never retries), so a `retry(..)` set on a
///   stage has no effect inside a pipeline. Retry a whole chain by wrapping the
///   `Pipeline` call yourself.
/// - A `Pipeline` can be re-run (stages are re-cloned per run), but a one-shot
///   [`Stdin`](crate::Stdin) source on the *first* stage
///   (`from_reader`/`from_lines`) is consumed by the first run; re-running then
///   **fails loud** (an [`Error::Io`](crate::Error::Io) at launch) rather
///   than silently feeding empty stdin — the same semantics as re-running a
///   [`Command`].
#[must_use = "a Pipeline does nothing until it is run"]
#[derive(Clone)]
pub struct Pipeline {
    stages: Vec<Command>,
    timeout: Option<Duration>,
    /// Applied to every stage at launch ([`cancel_on`](Self::cancel_on)) so the
    /// whole chain shares one cancel state and a cancellation errors the run.
    cancel_token: Option<tokio_util::sync::CancellationToken>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.stages.len())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// What one finished stage reported — input to the pipefail fold. The **last**
/// stage is folded in here too, so every stage is evaluated by the same
/// `is_clean` rule and a failure carries the stage's *own* deadline.
struct StageOutcome {
    program: String,
    outcome: Outcome,
    stderr: String,
    /// The stage opted out of pipefail attribution ([`Command::unchecked_in_pipe`]).
    unchecked: bool,
    /// Exit codes the stage treats as success ([`Command::ok_codes`]).
    ok_codes: Vec<i32>,
    /// The stage's *own* configured [`Command::timeout`] — carried so a stage
    /// that timed out reports its real deadline, not the chain's (or `0ns`).
    timeout: Option<Duration>,
}

impl Pipeline {
    pub(crate) fn new(first: Command, second: Command) -> Self {
        Pipeline {
            stages: vec![first, second],
            timeout: None,
            cancel_token: None,
        }
    }

    /// Append another stage: the current last stage's stdout becomes `next`'s
    /// stdin.
    pub fn pipe(mut self, next: Command) -> Self {
        self.stages.push(next);
        self
    }

    /// Kill the **whole chain** if it exceeds `timeout` (the group is torn
    /// down; the result reports `timed_out`). Unlike a single
    /// [`Command::timeout`] capture, no partial stdout is reported for a
    /// timed-out chain.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Cancel the **whole chain** when `token` fires: the shared kill-on-drop
    /// group tears every stage down and the run resolves to
    /// [`Error::Cancelled`](crate::Error::Cancelled).
    ///
    /// The token **gap-fills** — at launch it is applied to every stage that does
    /// not already carry its own [`Command::cancel_on`], leaving an explicit
    /// per-stage token intact (which still cancels the chain, since cancelling one
    /// stage errors the run and the group tears the rest down). This matches
    /// [`CliClient::default_cancel_on`](crate::CliClient::default_cancel_on)
    /// rather than silently overriding a per-stage choice. To have a stage
    /// cancelled by **both** its own token and the chain token, pass a child of
    /// this token as the stage's token (`token.child_token()`).
    ///
    /// Like [`Command::cancel_on`], a cancelled run is terminal: it is not
    /// retried, and the chain cannot be re-run through a token that stays
    /// cancelled.
    pub fn cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Run the chain to completion and capture the outcome (stdout as text). A
    /// failing stage is **not** an `Err` here — it is reported in the result
    /// (pipefail attribution, see the type docs); `Err` means a stage could not
    /// be started or driven at all.
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        self.capture(|last| async move { last.output_string().await })
            .await
    }

    /// Run the chain to completion and capture the last stage's stdout as **raw
    /// bytes** (the binary-pipe analogue of [`output_string`](Self::output_string)
    /// — e.g. `curl … | gunzip`). Pipefail attribution is identical; only the
    /// last stage's stdout is captured raw. Stderr (every stage, including the
    /// last) stays decoded text — it is diagnostics, never the binary payload.
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        self.capture(|last| async move { last.output_bytes().await })
            .await
    }

    /// The shared driver behind [`output_string`](Self::output_string) /
    /// [`output_bytes`](Self::output_bytes): start and chain every stage, drain
    /// them concurrently, and fold the pipefail outcome. `capture_last` decides
    /// how the **last** stage's stdout is captured (text vs raw bytes); inner
    /// stages are always line-drained for their stderr/exit.
    async fn capture<T, C, F>(&self, capture_last: C) -> Result<ProcessResult<T>>
    where
        T: Default + Send + 'static,
        C: FnOnce(crate::running::RunningProcess) -> F,
        F: std::future::Future<Output = Result<ProcessResult<T>>> + Send + 'static,
    {
        let group = ProcessGroup::new()?;

        // Start every stage, chaining stage N's stdout into stage N+1's stdin via
        // the stdin copy task `launch` spawns per stage.
        let mut running = Vec::with_capacity(self.stages.len());
        let mut upstream = None;
        for (index, stage) in self.stages.iter().enumerate() {
            let mut command = stage.clone();
            // The pipeline cancel token gap-fills: applied only where a stage has
            // no token of its own, never overriding an explicit per-stage choice.
            if let Some(token) = &self.cancel_token
                && command.cancel_token().is_none()
            {
                command = command.cancel_on(token.clone());
            }
            if let Some(reader) = upstream.take() {
                command.set_pipe_stdin(reader);
            }
            let mut process = group.start(&command).await?;
            if index + 1 < self.stages.len() {
                upstream = process.take_stdout_pipe();
            }
            // Carry the unchecked flag with the handle: the last stage is popped
            // off below, so positional lookups would be fragile.
            running.push((process, stage.is_unchecked()));
        }

        // Drain every stage concurrently: a stderr-chatty inner stage must not
        // block on a full pipe while we wait on its neighbours.
        let (last, last_unchecked) = running.pop().expect("a pipeline has at least two stages");
        // The last stage is evaluated by the same pipefail rule as inner stages,
        // so capture its `ok_codes` and own deadline for the fold below.
        let last_stage = self
            .stages
            .last()
            .expect("a pipeline has at least two stages");
        let last_ok_codes = last_stage.ok_codes_vec();
        let last_timeout = last_stage.configured_timeout();
        let mut inner_tasks = Vec::with_capacity(running.len());
        for ((process, unchecked), stage) in running.into_iter().zip(self.stages.iter()) {
            let program = process.program_name().to_owned();
            let ok_codes = stage.ok_codes_vec();
            let timeout = stage.configured_timeout();
            inner_tasks.push(tokio::spawn(async move {
                let Finished { outcome, stderr } = process.finish().await?;
                Ok::<_, crate::Error>(StageOutcome {
                    program,
                    outcome,
                    stderr,
                    unchecked,
                    ok_codes,
                    timeout,
                })
            }));
        }
        let last_task = tokio::spawn(capture_last(last));

        // Abort still-running drain tasks on an early exit rather than leaving
        // them parked until the group-killed pipes close.
        let _abort_guard = AbortTasksOnDrop(
            inner_tasks
                .iter()
                .map(|t| t.abort_handle())
                .chain(std::iter::once(last_task.abort_handle()))
                .collect(),
        );

        let collect = async {
            let mut outcomes = Vec::with_capacity(inner_tasks.len() + 1);
            for task in inner_tasks {
                outcomes.push(task.await.map_err(join_error)??);
            }
            let last_result = last_task.await.map_err(join_error)??;
            Ok::<_, crate::Error>((outcomes, last_result))
        };

        let (mut stages, last_result) = match self.timeout {
            None => collect.await?,
            Some(limit) => match tokio::time::timeout(limit, collect).await {
                Ok(collected) => collected?,
                Err(_elapsed) => {
                    // Whole-chain deadline: kill the chain (best-effort; the
                    // group's Drop backstops) and report the timeout in the
                    // result like `Command::timeout` does. `_abort_guard` reaps
                    // the drain tasks as this returns.
                    let _ = group.terminate_all();
                    return Ok(ProcessResult::new(
                        self.pipeline_name(),
                        // A timed-out chain reports no partial stdout.
                        T::default(),
                        String::new(),
                        Outcome::TimedOut,
                        Some(limit),
                    ));
                }
            },
        };

        // Fold the last stage into the same `StageOutcome` evaluation; its stdout
        // is the chain's stdout.
        let last_outcome = StageOutcome {
            program: last_result.program().to_owned(),
            outcome: last_result.outcome(),
            stderr: last_result.stderr().to_owned(),
            unchecked: last_unchecked,
            ok_codes: last_ok_codes,
            timeout: last_timeout,
        };
        // The chain's stdout *is* the last stage's, so its truncation state must
        // survive the fold — `pipefail` rebuilds via `ProcessResult::new` (which
        // defaults `truncated=false`), so re-stamp it below. This is what makes
        // the `parse`/`try_parse` truncation guard real.
        let last_truncated = last_result.truncated();
        let (last_total_lines, last_total_bytes) =
            (last_result.total_lines(), last_result.total_bytes());
        let last_stdout = last_result.into_stdout();
        stages.push(last_outcome);

        let mut result = pipefail(stages, last_stdout);
        if last_truncated {
            result = result
                .with_truncated(true)
                .with_overflow_totals(last_total_lines, last_total_bytes);
        }
        Ok(result)
    }

    /// Run the chain, require **every** stage to exit cleanly, and return the
    /// last stage's trimmed stdout. A failure surfaces as the first failing
    /// stage's [`Error::Exit`](crate::Error::Exit) (pipefail attribution;
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are exempt, so a chain whose
    /// only failures are unchecked returns `Ok`).
    /// [`Error::Timeout`](crate::Error::Timeout) is produced by the whole-chain
    /// [`timeout`](Self::timeout) or by **any** stage's own
    /// [`Command::timeout`] — the attributed stage's *own* deadline is reported,
    /// not the chain's.
    pub async fn run(&self) -> Result<String> {
        // `run` presents stdout as if complete, so it must fail loud on a
        // truncated last-stage capture rather than return a clipped tail.
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        Ok(out.into_stdout().trim_end().to_owned())
    }

    /// Run the chain, require **every** stage to exit cleanly (pipefail), and
    /// return the full captured [`ProcessResult`] (untrimmed stdout) — the
    /// building block when you need the whole result after success-checking,
    /// rather than trimmed stdout ([`run`](Self::run)). Mirrors
    /// [`Command::checked`](crate::Command::checked).
    pub async fn checked(&self) -> Result<ProcessResult<String>> {
        self.output_string().await?.ensure_success()
    }

    /// Run the chain for its side effect: require a clean pipefail outcome and
    /// discard the output. Mirrors [`Command::run_unit`](crate::Command::run_unit).
    pub async fn run_unit(&self) -> Result<()> {
        self.output_string().await?.ensure_success().map(drop)
    }

    /// Run the chain and return the pipefail-attributed exit code. A chain that
    /// produced no code surfaces as an error — a whole-chain or stage timeout as
    /// [`Error::Timeout`](crate::Error::Timeout), a signal-kill as
    /// [`Error::Signalled`](crate::Error::Signalled) — mirroring
    /// [`Command::exit_code`](crate::Command::exit_code).
    pub async fn exit_code(&self) -> Result<i32> {
        self.output_string().await?.require_code()
    }

    /// Read the chain's pipefail-attributed exit code as a boolean: `0` →
    /// `Ok(true)`, `1` → `Ok(false)`, anything else → `Err` (other code as
    /// [`Error::Exit`](crate::Error::Exit), a timeout as
    /// [`Error::Timeout`](crate::Error::Timeout), a signal-kill as
    /// [`Error::Signalled`](crate::Error::Signalled)). For a chain whose final
    /// answer is a yes/no exit — `producer | grep -q pattern`. Mirrors
    /// [`Command::probe`](crate::Command::probe) and keeps its strict 0/1
    /// contract regardless of any stage's `ok_codes`.
    ///
    /// The code is the **pipefail** code, so an *inner* stage exiting `1` reads as
    /// `false` even when the predicate is the last stage. If only the final
    /// stage's verdict should decide the boolean, mark the earlier stages
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) so they never speak for
    /// the chain.
    pub async fn probe(&self) -> Result<bool> {
        let result = self.output_string().await?;
        match result.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            // Reset to the default `{0}` so an accepted non-{0,1} code still
            // errors (probe's contract is strict), reusing ensure_success to
            // build the faithful error.
            _ => Err(result
                .with_ok_codes(vec![0])
                .ensure_success()
                .expect_err("a non-{0,1} exit code is never success")),
        }
    }

    /// Run the chain (requiring a clean pipefail outcome) and feed the last
    /// stage's stdout to an **infallible** `parse` closure. Fails loud on a
    /// bounded-buffer truncation of the last stage so the parser never sees a
    /// clipped tail. Mirrors [`Command::parse`](crate::Command::parse) — except
    /// the closure runs *inline* on the awaiting task (not across a `tokio::spawn`
    /// boundary), so it needs no `Send` bound, accepting strictly more closures
    /// than `Command::parse`.
    pub async fn parse<T, F>(&self, parse: F) -> Result<T>
    where
        F: FnOnce(&str) -> T,
    {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        Ok(parse(out.stdout()))
    }

    /// Run the chain (requiring a clean pipefail outcome) and feed the last
    /// stage's stdout to a *fallible* `parse` closure (the JSON-deserialization
    /// shape; a failure becomes [`Error::Parse`](crate::Error::Parse) or whatever
    /// the closure returns). Fails loud on truncation. Mirrors
    /// [`Command::try_parse`](crate::Command::try_parse).
    pub async fn try_parse<T, F>(&self, parse: F) -> Result<T>
    where
        F: FnOnce(&str) -> Result<T>,
    {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        parse(out.stdout())
    }

    /// Fail loud if the last stage's capture was truncated by its bounded buffer
    /// — the parsing verbs must not hand a clipped tail to the closure. The
    /// ceiling is the **last** stage's [`Command::output_buffer`] policy, since its
    /// stdout is the chain's output.
    fn reject_if_last_truncated(&self, out: &ProcessResult<String>) -> Result<()> {
        let policy = self
            .stages
            .last()
            .expect("a pipeline has at least two stages")
            .output_buffer_policy();
        out.reject_if_truncated(policy.max_lines, policy.max_bytes)
    }

    /// `"a | b | c"` — the chain's display name for timeout attribution.
    fn pipeline_name(&self) -> String {
        self.stages
            .iter()
            .map(|stage| stage.program_name())
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

/// Whether an outcome is SIGPIPE (Unix signal 13) — the usual symptom of an
/// upstream stage whose downstream consumer exited, not the actual culprit.
fn is_sigpipe(outcome: &Outcome) -> bool {
    #[cfg(unix)]
    return matches!(outcome, Outcome::Signalled(Some(13)));
    #[cfg(not(unix))]
    let _ = outcome;
    #[cfg(not(unix))]
    false
}

/// Fold **all** stages (the last included) into one pipefail result: the chain's
/// stdout is the last stage's `last_stdout`, with code/stderr/program attributed
/// to the first **checked** unclean stage (or the last stage when none failed).
///
/// One uniform rule for every stage:
/// - A stage is *clean* iff it `Exited(code)` with `code` in its own `ok_codes`
///   (honored for the **last** stage too — not reset to `[0]`).
/// - An `unchecked` **inner** stage is exempt from attribution **regardless of
///   how it ended** (non-zero exit, signal — including SIGPIPE — or its own
///   per-stage timeout): it never speaks for the chain and never shields another
///   stage. An `unchecked` **last** stage is the one carve-out: since its stdout
///   *is* the chain's output, only its non-zero *exit* is forgiven (the real
///   code is preserved, the chain succeeds) — a last-stage timeout or signal
///   still surfaces (the chain output is then broken).
/// - A checked failure always trumps an unchecked one (any position). Among
///   checked failures, a non-SIGPIPE culprit is preferred over a SIGPIPE victim,
///   else the leftmost.
/// - The attributed failure carries its **own** deadline, so a stage that timed
///   out reports its real `timeout`, never the chain's or `0ns`.
fn pipefail<T>(stages: Vec<StageOutcome>, last_stdout: T) -> ProcessResult<T> {
    let is_clean = |stage: &StageOutcome| match stage.outcome {
        Outcome::Exited(code) => stage.ok_codes.contains(&code),
        _ => false, // signal kill or timeout → unclean regardless
    };

    let checked_failures: Vec<_> = stages
        .iter()
        .filter(|s| !s.unchecked && !is_clean(s))
        .collect();

    if let Some(stage) = checked_failures
        .iter()
        .find(|s| !is_sigpipe(&s.outcome)) // prefer a non-SIGPIPE culprit over a SIGPIPE victim
        .or_else(|| checked_failures.first()) // else the leftmost failure
        .copied()
    {
        // A checked stage failed — its diagnostics and its own deadline win; the
        // chain's stdout is still the last stage's.
        return ProcessResult::new(
            stage.program.clone(),
            last_stdout,
            stage.stderr.clone(),
            stage.outcome,
            stage.timeout,
        );
    }

    // No checked failure: the last stage speaks for the chain. Honor its
    // `ok_codes`; an `unchecked` last stage that exited non-clean is forgiven —
    // preserve its real code so `is_success()` is true (not a fabricated `0`). A
    // signal/timeout outcome stays non-success regardless.
    let last = stages.last().expect("a pipeline has at least two stages");
    let ok_codes = match last.outcome {
        Outcome::Exited(code) if last.unchecked && !last.ok_codes.contains(&code) => vec![code],
        _ => last.ok_codes.clone(),
    };
    ProcessResult::new(
        last.program.clone(),
        last_stdout,
        last.stderr.clone(),
        last.outcome,
        last.timeout,
    )
    .with_ok_codes(ok_codes)
}

/// `a | b` — sugar for [`Command::pipe`]: the same shell-free, one-group,
/// pipefail pipeline. Parenthesize the chain before a terminal verb, since
/// method calls bind tighter than `|`:
///
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::Command;
///
/// let out = (Command::new("git").args(["log", "--format=%an"])
///     | Command::new("sort")
///     | Command::new("uniq").arg("-c"))
///     .output_string()
///     .await?;
/// println!("{}", out.stdout());
/// # Ok(())
/// # }
/// ```
impl std::ops::BitOr<Command> for Command {
    type Output = Pipeline;

    fn bitor(self, rhs: Command) -> Pipeline {
        self.pipe(rhs)
    }
}

/// `pipeline | c` — sugar for [`Pipeline::pipe`], so `a | b | c` chains
/// left-associatively into one pipeline.
impl std::ops::BitOr<Command> for Pipeline {
    type Output = Pipeline;

    fn bitor(self, rhs: Command) -> Pipeline {
        self.pipe(rhs)
    }
}

fn join_error(err: tokio::task::JoinError) -> crate::Error {
    crate::Error::Io(std::io::Error::other(format!(
        "pipeline stage task failed: {err}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(program: &str, outcome: Outcome) -> StageOutcome {
        StageOutcome {
            program: program.into(),
            outcome,
            stderr: String::new(),
            unchecked: false,
            ok_codes: vec![0],
            timeout: None,
        }
    }

    fn clean(program: &str) -> StageOutcome {
        stage(program, Outcome::Exited(0))
    }

    fn unclean(program: &str, outcome: Outcome, stderr: &str) -> StageOutcome {
        StageOutcome {
            stderr: stderr.into(),
            ..stage(program, outcome)
        }
    }

    /// An unclean stage that opted out of attribution.
    fn unchecked_fail(program: &str, outcome: Outcome) -> StageOutcome {
        StageOutcome {
            unchecked: true,
            ..unclean(program, outcome, "forgiven")
        }
    }

    /// The last stage as a `StageOutcome` (program `"last"`, stderr `"last-err"`)
    /// — folded into the same evaluation as the inner stages.
    fn last(outcome: Outcome, unchecked: bool) -> StageOutcome {
        StageOutcome {
            program: "last".into(),
            outcome,
            stderr: "last-err".into(),
            unchecked,
            ok_codes: vec![0],
            timeout: None,
        }
    }

    /// Run `pipefail` over the inner stages + the last stage + the chain stdout.
    fn pf(mut inner: Vec<StageOutcome>, last: StageOutcome, stdout: &str) -> ProcessResult<String> {
        inner.push(last);
        pipefail(inner, stdout.to_owned())
    }

    /// The expected result when the last stage speaks unchanged.
    fn expect_last(outcome: Outcome, stdout: &str) -> ProcessResult<String> {
        ProcessResult::new(
            "last".into(),
            stdout.into(),
            "last-err".into(),
            outcome,
            None,
        )
    }

    #[test]
    fn all_clean_inner_stages_let_the_last_stage_speak() {
        // Success and failure of the last stage alike pass through untouched.
        let ok = pf(
            vec![clean("a"), clean("b")],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert_eq!(ok, expect_last(Outcome::Exited(0), "final"));

        let failing_last = pf(vec![clean("a")], last(Outcome::Exited(3), false), "partial");
        assert_eq!(failing_last, expect_last(Outcome::Exited(3), "partial"));
    }

    #[test]
    fn failing_inner_stage_wins_but_stdout_stays_the_chains() {
        let result = pf(
            vec![clean("a"), unclean("b", Outcome::Exited(2), "b broke")],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert_eq!(result.program(), "b", "diagnostics from the failing stage");
        assert_eq!(result.code(), Some(2));
        assert_eq!(result.stderr(), "b broke");
        assert_eq!(
            result.stdout(),
            "final",
            "stdout is what the chain produced — the last stage's"
        );
        assert!(!result.timed_out());
        // The same attribution must survive the public error surface too.
        match result.ensure_success() {
            Err(crate::Error::Exit {
                program,
                code,
                stdout,
                stderr,
            }) => {
                assert_eq!(program, "b", "diagnostics from the failing stage");
                assert_eq!(code, 2);
                assert_eq!(stdout, "final");
                assert_eq!(stderr, "b broke");
            }
            other => panic!("expected Error::Exit, got {other:?}"),
        }
    }

    #[test]
    fn first_of_several_failures_is_attributed() {
        let result = pf(
            vec![
                unclean("a", Outcome::Exited(1), "first"),
                unclean("b", Outcome::Exited(2), "second"),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a", "pipefail blames the FIRST failure");
        assert_eq!(result.code(), Some(1));
        assert_eq!(result.stderr(), "first");
        match result.ensure_success() {
            Err(crate::Error::Exit { program, .. }) => {
                assert_eq!(program, "a", "...and so does the error surface");
            }
            other => panic!("expected Error::Exit, got {other:?}"),
        }
    }

    #[test]
    fn all_unchecked_failures_report_success() {
        // The head-pattern: the producer's SIGPIPE death (Signalled(None)) is
        // forgiven, the chain succeeds with the consumer's output.
        let result = pf(
            vec![unchecked_fail("producer", Outcome::Signalled(None))],
            last(Outcome::Exited(0), false),
            "first line",
        );
        assert!(result.is_success(), "got {result:?}");
        assert_eq!(result.stdout(), "first line");
        assert_eq!(result.program(), "last", "the clean last stage speaks");
    }

    #[test]
    fn checked_failure_trumps_unchecked_regardless_of_order() {
        // unchecked-then-checked: the later checked failure wins.
        let result = pf(
            vec![
                unchecked_fail("a", Outcome::Exited(141)),
                unclean("b", Outcome::Exited(2), "real"),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "b", "unchecked never shields a failure");
        assert_eq!(result.code(), Some(2));

        // checked-then-unchecked: the first (checked) failure wins, as today.
        let result = pf(
            vec![
                unclean("a", Outcome::Exited(1), "real"),
                unchecked_fail("b", Outcome::Exited(2)),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), Some(1));
    }

    #[test]
    fn attribution_skips_unchecked_to_the_first_checked_failure() {
        let result = pf(
            vec![
                clean("a"),
                unchecked_fail("b", Outcome::Exited(1)),
                unclean("c", Outcome::Exited(3), "c broke"),
                unclean("d", Outcome::Exited(4), "d broke"),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "c", "first CHECKED failure is blamed");
        assert_eq!(result.code(), Some(3));
        assert_eq!(result.stderr(), "c broke");
    }

    #[test]
    fn unchecked_last_stage_failure_is_forgiven() {
        // An unchecked last stage that exited non-zero reports success,
        // preserving the real exit code (not fabricating 0).
        let result = pf(
            vec![clean("a")],
            last(Outcome::Exited(141), true),
            "partial",
        );
        assert!(result.is_success(), "got {result:?}");
        assert_eq!(result.code(), Some(141), "real exit code preserved");
        assert_eq!(result.stdout(), "partial", "output is preserved");
        assert_eq!(result.stderr(), "last-err", "stderr kept for the curious");
        assert!(result.ensure_success().is_ok());
    }

    #[test]
    fn last_stage_ok_codes_are_honoured() {
        // The LAST stage's `ok_codes` are honored just like an inner stage's — a
        // last stage with ok_codes([0,1]) exiting 1 is a clean, successful chain.
        let mut last_grep = last(Outcome::Exited(1), false);
        last_grep.program = "grep".into();
        last_grep.ok_codes = vec![0, 1];
        let result = pf(vec![clean("a")], last_grep, "matched");
        assert!(
            result.is_success(),
            "exit 1 in the last stage's ok_codes: {result:?}"
        );
        assert_eq!(result.code(), Some(1), "real code preserved");
        assert_eq!(result.program(), "grep");
    }

    #[test]
    fn inner_stage_ok_codes_are_honoured_in_pipefail_cleanliness() {
        // An inner stage with ok_codes([0,1]) that exits 1 must not trigger
        // pipefail attribution — exit 1 is clean per its ok_codes.
        let mut with_ok = stage("grep", Outcome::Exited(1));
        with_ok.ok_codes = vec![0, 1];
        let result = pf(vec![with_ok], last(Outcome::Exited(0), false), "out");
        assert!(
            result.is_success(),
            "exit 1 in ok_codes should be clean: {result:?}"
        );
        assert_eq!(result.program(), "last", "clean inner → last stage speaks");
    }

    #[test]
    fn timed_out_stage_reports_its_own_deadline_not_the_chains() {
        // An inner stage that hit its own `Command::timeout` reports that stage's
        // OWN deadline, not the chain's (or `0ns`).
        let mut timed = unclean("slow", Outcome::TimedOut, "");
        timed.timeout = Some(Duration::from_millis(500));
        let result = pf(vec![timed], last(Outcome::Exited(0), false), "out");
        assert_eq!(result.program(), "slow");
        assert!(result.timed_out());
        match result.ensure_success() {
            Err(crate::Error::Timeout {
                program, timeout, ..
            }) => {
                assert_eq!(program, "slow");
                assert_eq!(
                    timeout,
                    Duration::from_millis(500),
                    "the stage's own deadline, not the chain's 0ns"
                );
            }
            other => panic!("expected Error::Timeout, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn sigpipe_victim_not_blamed_when_downstream_non_sigpipe_failure_exists() {
        // The SIGPIPE-killed upstream stage is the victim, not the culprit; the
        // downstream non-SIGPIPE failure should be attributed instead.
        let sigpipe_victim = unclean("producer", Outcome::Signalled(Some(13)), "pipe broken");
        let real_failure = unclean("consumer", Outcome::Exited(2), "consumer broke");
        let result = pf(
            vec![sigpipe_victim, real_failure],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(
            result.program(),
            "consumer",
            "downstream non-SIGPIPE culprit, not upstream SIGPIPE victim"
        );
        assert_eq!(result.code(), Some(2));
    }

    #[test]
    fn checked_last_stage_failure_still_speaks_verbatim() {
        // Regression guard: a checked last stage's unclean exit speaks as-is.
        let result = pf(vec![clean("a")], last(Outcome::Exited(3), false), "partial");
        assert_eq!(result, expect_last(Outcome::Exited(3), "partial"));
    }

    #[test]
    fn unchecked_never_forgives_a_timeout() {
        // An unchecked LAST stage that timed out still reports the timeout —
        // a deadline violation is not an exit status.
        let result = pf(vec![clean("a")], last(Outcome::TimedOut, true), "");
        assert!(result.timed_out());
        assert!(!result.is_success());
    }

    #[test]
    fn unchecked_never_forgives_a_signal_kill() {
        // An unchecked LAST stage killed by a signal is not forgiven — a signal
        // kill is not a voluntary exit with a code, just like a timeout.
        let result = pf(
            vec![clean("a")],
            last(Outcome::Signalled(Some(9)), true),
            "",
        );
        assert!(matches!(result.outcome(), Outcome::Signalled(Some(9))));
        assert!(!result.is_success());
    }

    #[test]
    fn bitor_chains_like_pipe() {
        let chain = Command::new("a") | Command::new("b") | Command::new("c");
        assert_eq!(chain.stages.len(), 3, "a | b | c is one three-stage chain");
        assert_eq!(chain.pipeline_name(), "a | b | c");
        assert!(chain.timeout.is_none());
    }

    #[test]
    fn signal_killed_inner_stage_counts_as_unclean() {
        // A signal-kill (or per-stage timeout kill) reports Signalled — that is
        // not a clean exit, so the stage must win the attribution.
        let result = pf(
            vec![unclean("a", Outcome::Signalled(None), "killed")],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), None);
        assert_eq!(result.stderr(), "killed");
        assert!(!result.timed_out(), "a stage kill is not a chain timeout");
        // Signalled outcome surfaces as Error::Signalled naming the attributed stage.
        match result.ensure_success() {
            Err(crate::Error::Signalled {
                program, signal, ..
            }) => {
                assert_eq!(program, "a");
                assert_eq!(signal, None);
            }
            other => panic!("expected Error::Signalled, got {other:?}"),
        }
    }
}
