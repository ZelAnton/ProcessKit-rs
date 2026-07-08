//! [`Pipeline`] — `a | b | c` without a shell.
//!
//! Each stage's stdout feeds the next stage's stdin — **no shell string**, so no
//! quoting or injection surface, and no `sh -c`. The connection is a small
//! in-process relay (a `tokio::io::copy` task per boundary), not a kernel pipe
//! spliced fd-to-fd; this has two consequences: a producer whose consumer exits
//! early stops on a [broken pipe](crate::Error) when the relay's next write fails
//! (rather than instantly via SIGPIPE), and the relay's own I/O is plumbing — a
//! closed sibling reads as EOF / writes as a broken pipe, neither reported as a
//! stage's stdin failure. Each stage spawns into its **own** kill-on-drop
//! [`ProcessGroup`](crate::ProcessGroup) sub-group, so a per-stage
//! [`Command::timeout`] tears down that stage's *whole* subtree (grandchildren of
//! a forking `sh -c …` included), while a chain-wide
//! [`Pipeline::timeout`]/teardown fans the kill across every sub-group so the
//! whole chain still dies as a unit. The outcome is **pipefail**: the first stage
//! without a clean exit decides the reported code/diagnostics.

use std::sync::Arc;
use std::time::Duration;

use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::{Outcome, ProcessResult};
use crate::running::Finished;
use tokio::task::AbortHandle;

/// Abort in-flight drain tasks on early exit so they don't linger until killed children close their pipes.
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
/// [`first_line`](Command::first_line) is intentionally **not** on `Pipeline`:
/// a chain consumes its last stage in full to fold the pipefail outcome. For a
/// streaming readiness probe that leaves the chain alive, use a single
/// [`Command`] with `first_line` instead.
///
/// Semantics:
///
/// - **Per-stage subtree, one chain fate** — each stage runs in its own
///   kill-on-drop group, so a per-stage [`Command::timeout`] tears down that
///   stage's whole subtree (a forking `sh -c …`'s grandchildren included).
///   Cancelling the future or a chain-wide [`timeout`](Self::timeout) elapsing
///   still tears the *whole* chain down — the kill fans across every stage's
///   sub-group.
/// - **Pipefail** — `stdout` is always the *last* stage's output; `code`,
///   `stderr`, and the reported program come from the **first** stage that
///   didn't exit cleanly, or from the last stage when every stage succeeded.
///   [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are exempt:
///   checked failures always trump unchecked ones; a chain whose only failures
///   are unchecked reports success.
/// - **Stdin/stdout at the ends** — the *first* stage's configured
///   [`stdin`](Command::stdin) is honored; inner stages' stdin is the pipe
///   (any configured source is overridden). Inner stages' stderr is captured
///   per-stage for pipefail diagnostics.
/// - A per-stage [`Command::retry`] is **not** applied inside a pipeline;
///   wrap the `Pipeline` call to retry the whole chain.
/// - A one-shot [`Stdin`](crate::Stdin) source on the *first* stage is
///   consumed by the first run; re-running **fails loud** rather than silently
///   feeding empty stdin.
#[must_use = "a Pipeline does nothing until it is run"]
#[derive(Clone)]
pub struct Pipeline {
    stages: Vec<Command>,
    timeout: Option<Duration>,
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

/// One finished stage's data — input to the pipefail fold.
struct StageOutcome {
    program: String,
    outcome: Outcome,
    stderr: String,
    /// Stage opted out of pipefail attribution.
    unchecked: bool,
    /// Exit codes the stage treats as success.
    ok_codes: Vec<i32>,
    /// Stage's own configured timeout — carried so a timed-out stage reports its real deadline.
    timeout: Option<Duration>,
    /// Stage was ended by the chain's proactive teardown (the group was killed
    /// after a *sibling* failed), not by a failure of its own. Treated like a
    /// SIGPIPE victim in attribution: de-prioritized so the real culprit — the
    /// stage that triggered teardown — is the one blamed.
    torn_down: bool,
    /// Whether a bounded, non-fail-loud `OutputBufferPolicy` silently dropped
    /// this stage's stderr. Stamped onto the folded result's `truncated` when
    /// this stage is the one pipefail blames, so an inner stage's clipped
    /// diagnostics are visible even when it isn't the last stage.
    stderr_truncated: bool,
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

    /// Kill the **whole chain** if it exceeds `timeout` (every stage's sub-group
    /// is torn down; the result reports `timed_out`). Unlike a single
    /// [`Command::timeout`] capture, no partial stdout is reported for a
    /// timed-out chain.
    ///
    /// This is the chain-wide backstop. A **per-stage** [`Command::timeout`] now
    /// also bounds a forking stage on its own: each stage runs in its own
    /// kill-on-drop group, so a per-stage deadline tears down that stage's whole
    /// subtree — a grandchild it forked (`sh -c …`) can no longer keep the stdout
    /// pipe open past the kill and stall the downstream stage. Reach for this
    /// whole-chain timeout when you want a single ceiling on the *entire* run
    /// regardless of which stage is slow; use a per-stage timeout to bound an
    /// individual stage. Either bounds a single-process stage.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Cancel the **whole chain** when `token` fires: the token reaches every
    /// stage (see gap-fill below), so each stage's run is cancelled and kills its
    /// own subtree, and the run resolves to
    /// [`Error::Cancelled`](crate::Error::Cancelled). This is **proactive** —
    /// firing the token cancels the stages directly, it does not wait for them to
    /// notice a closed pipe.
    ///
    /// A plain **stage failure** (a stage exits non-zero, is signal-killed, or hits
    /// its own timeout, *without* the chain being cancelled) is **also** proactive:
    /// the first such failure tears the whole group down at once, so a quiet,
    /// still-running sibling — classically an upstream producer that never writes,
    /// and so never dies of a broken pipe — cannot hold the run open. The failure
    /// still keeps its **pipefail** attribution: the stage that triggered teardown
    /// is blamed, while the siblings the teardown killed are treated as victims
    /// (like a downstream `SIGPIPE` death), never stealing the blame. The one
    /// death that does *not* trigger teardown is an
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) stage's — its unclean exit
    /// is forgiven, so it leaves the rest of the chain running.
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
    ///
    /// # Errors
    ///
    /// A failing stage (and a timeout) is *captured* in the returned
    /// [`ProcessResult`] (pipefail attribution), not raised. `Err` means a stage
    /// could not be *started or driven*: a launch failure
    /// ([`Error::NotFound`](crate::Error::NotFound) /
    /// [`Error::Spawn`](crate::Error::Spawn) /
    /// [`Error::Unsupported`](crate::Error::Unsupported)),
    /// [`Error::Cancelled`](crate::Error::Cancelled),
    /// [`Error::OutputTooLarge`](crate::Error::OutputTooLarge) (a fail-loud
    /// overflow of the last stage), [`Error::Stdin`](crate::Error::Stdin), or
    /// [`Error::Io`](crate::Error::Io).
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        self.capture(|last| async move { last.output_string().await })
            .await
    }

    /// Run the chain to completion and capture the last stage's stdout as **raw
    /// bytes** (the binary-pipe analogue of [`output_string`](Self::output_string)
    /// — e.g. `curl … | gunzip`). Pipefail attribution is identical; only the
    /// last stage's stdout is captured raw. Stderr (every stage, including the
    /// last) stays decoded text — it is diagnostics, never the binary payload.
    ///
    /// # Errors
    ///
    /// The same surface as [`output_string`](Self::output_string) — a failing
    /// stage (and a timeout) is captured, not raised — with the last stage's
    /// stdout captured as raw bytes.
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        self.capture(|last| async move { last.output_bytes().await })
            .await
    }

    /// Start and chain every stage, drain concurrently, and fold the pipefail
    /// outcome. `capture_last` decides how the last stage's stdout is captured.
    async fn capture<T, C, F>(&self, capture_last: C) -> Result<ProcessResult<T>>
    where
        T: Default + Send + 'static,
        C: FnOnce(crate::running::RunningProcess) -> F,
        F: std::future::Future<Output = Result<ProcessResult<T>>> + Send + 'static,
    {
        // Wall-clock start of the whole chain — from here to the moment the final
        // `ProcessResult` is assembled (success, pipefail failure, or chain-wide
        // timeout), so `duration()` reflects the run, not just the last stage.
        let started = std::time::Instant::now();

        // Each stage gets its own kill-on-drop sub-group, retained here as a
        // strong handle. A per-stage `Command::timeout`/`cancel_on` then tears
        // down that stage's *whole* subtree (grandchildren of a forking `sh -c …`
        // included) rather than only its direct child, and the chain-wide backstop
        // below fans a kill across every sub-group. Holding the strong handles for
        // the whole capture keeps the old lifetime — a stage's tree lives until the
        // chain ends, then kill-on-drop reaps any straggler.
        let mut stage_groups: Vec<Arc<ProcessGroup>> = Vec::with_capacity(self.stages.len());

        let mut running = Vec::with_capacity(self.stages.len());
        let mut upstream = None;
        for (index, stage) in self.stages.iter().enumerate() {
            let mut command = stage.clone();
            // Gap-fill: apply the pipeline cancel token only where a stage has no token of its own.
            if let Some(token) = &self.cancel_token
                && command.cancel_token().is_none()
            {
                command = command.cancel_on(token.clone());
            }
            if let Some(reader) = upstream.take() {
                command.set_pipe_stdin(reader);
            }
            // Spawn into a fresh per-stage group, then hand it to the stage handle
            // (`attach_group` also upgrades the cancel watchdog to a group+pid kill)
            // and keep a strong clone for the chain-wide teardown.
            let group = ProcessGroup::new()?;
            let mut process = group.start(&command).await?;
            process.attach_group(group);
            if let Some(handle) = process.own_group_handle() {
                stage_groups.push(handle);
            }
            if index + 1 < self.stages.len() {
                upstream = process.take_stdout_pipe();
            }
            // Bundle the unchecked flag with the handle; the last stage is popped below.
            running.push((process, stage.is_unchecked()));
        }

        // Proactive teardown: the first stage to finish with a *checked failure*
        // fires this token; a concurrent killer then tears every stage's sub-group
        // down so a quiet, still-running sibling (classically an upstream producer
        // that never writes, so never dies of a broken pipe) cannot hold the chain
        // open after the failure. Stages ended by that kill are flagged `torn_down` and
        // de-prioritized in the pipefail fold, so the real culprit is still blamed.
        // This is distinct from the user's `cancel_token`: a cancelled stage errors
        // out (`Err(Cancelled)`) before producing a `StageOutcome`, so cancellation
        // never fires this teardown and the cancel path is unchanged.
        let teardown = tokio_util::sync::CancellationToken::new();

        // Drain concurrently: a stderr-chatty inner stage must not block on a full pipe.
        let (last, last_unchecked) = running.pop().expect("a pipeline has at least two stages");
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
            let teardown = teardown.clone();
            inner_tasks.push(tokio::spawn(async move {
                let Finished {
                    outcome,
                    stderr,
                    stderr_truncated,
                } = process.finish().await?;
                // Snapshot *before* firing: a teardown already in flight when this
                // stage ended marks it a victim; the first genuine failure sees no
                // teardown yet, so it fires it and stays the (non-torn) culprit.
                let torn_down = teardown.is_cancelled();
                if !torn_down && is_checked_failure(outcome, &ok_codes, unchecked) {
                    teardown.cancel();
                }
                Ok::<_, crate::Error>(StageOutcome {
                    program,
                    outcome,
                    stderr,
                    unchecked,
                    ok_codes,
                    timeout,
                    torn_down,
                    stderr_truncated,
                })
            }));
        }
        // Call `capture_last` here (not inside the spawned future): it yields a
        // `Send` future `F`, whereas the closure `C` itself is not `Send` and must
        // not be captured across the `tokio::spawn` boundary.
        let last_future = capture_last(last);
        let last_task = tokio::spawn({
            let teardown = teardown.clone();
            let last_ok_codes = last_ok_codes.clone();
            async move {
                let result = last_future.await?;
                // The last stage triggers teardown too (a failing last stage should
                // not wait on a quiet upstream either); torn if a sibling already did.
                let torn_down = teardown.is_cancelled();
                if !torn_down
                    && is_checked_failure(result.outcome(), &last_ok_codes, last_unchecked)
                {
                    teardown.cancel();
                }
                Ok::<_, crate::Error>((result, torn_down))
            }
        });

        let _abort_guard = AbortTasksOnDrop(
            inner_tasks
                .iter()
                .map(|t| t.abort_handle())
                .chain(std::iter::once(last_task.abort_handle()))
                .collect(),
        );

        let collect = async {
            // Ordered gather (leftmost-first) — the success path is byte-for-byte
            // the old behavior. On failure the killer arm below wakes and tears
            // every stage's sub-group down, unblocking whichever ordered await was
            // stalled on a quiet sibling; the gather then completes and wins the `select!`.
            let gather = async {
                let mut outcomes = Vec::with_capacity(inner_tasks.len() + 1);
                for task in inner_tasks {
                    outcomes.push(task.await.map_err(join_error)??);
                }
                let (last_result, last_torn_down) = last_task.await.map_err(join_error)??;
                Ok::<_, crate::Error>((outcomes, last_result, last_torn_down))
            };
            tokio::select! {
                collected = gather => collected,
                // Fires once on the first checked failure, then never resolves —
                // it only exists for its kill side effect, letting `gather` finish.
                () = async {
                    teardown.cancelled().await;
                    kill_all_stage_groups(&stage_groups);
                    std::future::pending::<()>().await
                } => unreachable!("the teardown killer pends forever after firing"),
            }
        };

        let (mut stages, last_result, last_torn_down) = match self.timeout {
            None => collect.await?,
            Some(limit) => match tokio::time::timeout(limit, collect).await {
                Ok(collected) => collected?,
                Err(_elapsed) => {
                    // Kill every stage's subtree; `_abort_guard` reaps the drain tasks as this returns.
                    kill_all_stage_groups(&stage_groups);
                    return Ok(ProcessResult::new(
                        self.pipeline_name(),
                        T::default(),
                        String::new(),
                        Outcome::TimedOut,
                        Some(limit),
                    )
                    .with_duration(started.elapsed()));
                }
            },
        };

        // `pipefail` rebuilds via `ProcessResult::new` (which defaults `truncated=false`);
        // re-stamp truncation so the `parse`/`try_parse` guard fires correctly. The
        // last stage's own `truncated()` covers both its stdout and stderr drops —
        // reused below as this stage's `stderr_truncated` too, since either one
        // clips the chain's actual captured content regardless of attribution.
        let last_truncated = last_result.truncated();
        let (last_total_lines, last_total_bytes) =
            (last_result.total_lines(), last_result.total_bytes());
        let last_outcome = StageOutcome {
            program: last_result.program().to_owned(),
            outcome: last_result.outcome(),
            stderr: last_result.stderr().to_owned(),
            unchecked: last_unchecked,
            ok_codes: last_ok_codes,
            timeout: last_timeout,
            torn_down: last_torn_down,
            stderr_truncated: last_truncated,
        };
        let last_stdout = last_result.into_stdout();
        stages.push(last_outcome);

        let mut result = pipefail(stages, last_stdout).with_duration(started.elapsed());
        // The attributed stage's own stderr truncation is already folded in by
        // `pipefail` (see below); this additionally ORs in the last stage's own
        // capture truncation, since the chain's actual `stdout` is always the last
        // stage's — a clipped last-stage capture is real regardless of which stage
        // pipefail blames for the failure.
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
    ///
    /// # Errors
    ///
    /// The first failing stage's [`Error::Exit`](crate::Error::Exit) (pipefail
    /// attribution; [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are
    /// exempt), [`Error::Signalled`](crate::Error::Signalled),
    /// [`Error::Timeout`](crate::Error::Timeout) (the whole-chain
    /// [`timeout`](Self::timeout) or any stage's own — the attributed stage's
    /// deadline is reported), [`Error::Cancelled`](crate::Error::Cancelled),
    /// [`Error::OutputTooLarge`](crate::Error::OutputTooLarge) (a fail-loud
    /// truncation of the last stage), plus any launch failure or
    /// [`Error::Stdin`](crate::Error::Stdin).
    pub async fn run(&self) -> Result<String> {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        Ok(out.into_stdout().trim_end().to_owned())
    }

    /// Run the chain, require **every** stage to exit cleanly (pipefail), and
    /// return the full captured [`ProcessResult`] (untrimmed stdout) — the
    /// building block when you need the whole result after success-checking,
    /// rather than trimmed stdout ([`run`](Self::run)). Mirrors
    /// [`Command::checked`](crate::Command::checked).
    ///
    /// # Errors
    ///
    /// The same pipefail surface as [`run`](Self::run) —
    /// [`Error::Exit`](crate::Error::Exit) /
    /// [`Error::Signalled`](crate::Error::Signalled) /
    /// [`Error::Timeout`](crate::Error::Timeout) /
    /// [`Error::Cancelled`](crate::Error::Cancelled), plus launch failures and
    /// [`Error::Stdin`](crate::Error::Stdin) — but, as the lenient building block,
    /// it does not fail loud on a bounded-buffer truncation.
    pub async fn checked(&self) -> Result<ProcessResult<String>> {
        self.output_string().await?.ensure_success()
    }

    /// Run the chain for its side effect: require a clean pipefail outcome and
    /// discard the output. Mirrors [`Command::run_unit`](crate::Command::run_unit).
    ///
    /// # Errors
    ///
    /// The same surface as [`checked`](Self::checked); only the captured output
    /// is discarded.
    pub async fn run_unit(&self) -> Result<()> {
        self.output_string().await?.ensure_success().map(drop)
    }

    /// Run the chain and return the pipefail-attributed exit code. A chain that
    /// produced no code surfaces as an error — a whole-chain or stage timeout as
    /// [`Error::Timeout`](crate::Error::Timeout), a signal-kill as
    /// [`Error::Signalled`](crate::Error::Signalled) — mirroring
    /// [`Command::exit_code`](crate::Command::exit_code).
    ///
    /// # Errors
    ///
    /// A chain that produced no code errors as
    /// [`Error::Timeout`](crate::Error::Timeout) (whole-chain or stage),
    /// [`Error::Signalled`](crate::Error::Signalled), or
    /// [`Error::Cancelled`](crate::Error::Cancelled), atop any launch failure. A
    /// non-zero pipefail code is returned, not raised.
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
    ///
    /// # Errors
    ///
    /// A pipefail code other than `0`/`1` becomes
    /// [`Error::Exit`](crate::Error::Exit); a chain with no code errors as
    /// [`Error::Timeout`](crate::Error::Timeout),
    /// [`Error::Signalled`](crate::Error::Signalled), or
    /// [`Error::Cancelled`](crate::Error::Cancelled), atop any launch failure.
    pub async fn probe(&self) -> Result<bool> {
        let result = self.output_string().await?;
        match result.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            // Reset ok_codes to {0}: probe's contract is strict 0/1 regardless of
            // any stage's ok_codes; ensure_success builds the faithful error.
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
    ///
    /// # Errors
    ///
    /// The pipefail surface of [`run`](Self::run) (launch failures,
    /// [`Error::Exit`](crate::Error::Exit) /
    /// [`Error::Signalled`](crate::Error::Signalled) /
    /// [`Error::Timeout`](crate::Error::Timeout) /
    /// [`Error::Cancelled`](crate::Error::Cancelled) /
    /// [`Error::Stdin`](crate::Error::Stdin)), plus
    /// [`Error::OutputTooLarge`](crate::Error::OutputTooLarge) when a fail-loud
    /// buffer truncated the last stage's stdout. The `parse` closure is
    /// infallible, so it adds no error.
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
    ///
    /// # Errors
    ///
    /// Everything [`parse`](Self::parse) can return, plus whatever the fallible
    /// `parse` closure yields on malformed output — typically
    /// [`Error::Parse`](crate::Error::Parse).
    pub async fn try_parse<T, F>(&self, parse: F) -> Result<T>
    where
        F: FnOnce(&str) -> Result<T>,
    {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        parse(out.stdout())
    }

    fn reject_if_last_truncated(&self, out: &ProcessResult<String>) -> Result<()> {
        let policy = self
            .stages
            .last()
            .expect("a pipeline has at least two stages")
            .output_buffer_policy();
        out.reject_if_truncated(policy.max_lines, policy.max_bytes)
    }

    fn pipeline_name(&self) -> String {
        self.stages
            .iter()
            .map(|stage| stage.program_name())
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

/// Fan a hard kill across every stage's sub-group — the chain-wide backstop when
/// a [`Pipeline::timeout`] elapses or a stage's failure triggers proactive
/// teardown, now that each stage owns its own kill-on-drop group rather than
/// sharing one. Best-effort: a per-group error is swallowed, exactly as the old
/// single-group `kill_all()` was.
fn kill_all_stage_groups(groups: &[Arc<ProcessGroup>]) {
    for group in groups {
        let _ = group.kill_all();
    }
}

/// True for SIGPIPE (Unix signal 13) — the usual victim symptom, not the culprit.
fn is_sigpipe(outcome: &Outcome) -> bool {
    #[cfg(unix)]
    return matches!(outcome, Outcome::Signalled(Some(13)));
    #[cfg(not(unix))]
    let _ = outcome;
    #[cfg(not(unix))]
    false
}

/// Did this outcome count as a *clean* exit given the stage's accepted codes?
/// Exhaustive (no wildcard) so a future [`Outcome`] variant forces a decision
/// here rather than being silently treated as clean (H2).
fn is_clean_exit(outcome: Outcome, ok_codes: &[i32]) -> bool {
    match outcome {
        Outcome::Exited(code) => ok_codes.contains(&code),
        Outcome::Signalled(_) | Outcome::TimedOut => false, // kill/timeout → unclean
    }
}

/// A stage whose unclean, non-forgiven exit makes it a genuine pipefail culprit —
/// the trigger for proactive teardown. An [`unchecked_in_pipe`](Command::unchecked_in_pipe)
/// stage never qualifies (its unclean exit is forgiven, so it must not tear the
/// chain down).
fn is_checked_failure(outcome: Outcome, ok_codes: &[i32], unchecked: bool) -> bool {
    !unchecked && !is_clean_exit(outcome, ok_codes)
}

/// Fold all stages (last included) into one pipefail result.
///
/// Key invariants:
/// - An `unchecked` inner stage is fully exempt from attribution regardless of
///   how it ended. An `unchecked` last stage is the one carve-out: only its
///   non-zero *exit* is forgiven; a last-stage timeout or signal still surfaces.
/// - Among checked failures, a genuine culprit is preferred over a *victim* — a
///   downstream SIGPIPE death, or a stage the chain's proactive teardown killed
///   (`torn_down`) after a sibling failed; otherwise the leftmost wins.
fn pipefail<T>(stages: Vec<StageOutcome>, last_stdout: T) -> ProcessResult<T> {
    let checked_failures: Vec<_> = stages
        .iter()
        .filter(|s| !s.unchecked && !is_clean_exit(s.outcome, &s.ok_codes))
        .collect();

    if let Some(stage) = checked_failures
        .iter()
        // Prefer a genuine culprit over a mere victim (a SIGPIPE death or a stage
        // killed by our own teardown) — a teardown victim never steals the blame
        // from the failure that triggered the teardown.
        .find(|s| !is_sigpipe(&s.outcome) && !s.torn_down)
        .or_else(|| checked_failures.first()) // else leftmost
        .copied()
    {
        // Carry the stage's own `ok_codes` so the rebuilt result classifies
        // success exactly as the stage did. Without this the default `[0]` would
        // make a rejected-zero failure (a stage with `ok_codes` excluding `0`
        // that exited `0`) — which `is_clean` deemed a *failure* above — report
        // `is_success() == true`, so the whole chain would return `Ok`.
        //
        // Also stamp the attributed stage's own stderr truncation: a bounded
        // `OutputBufferPolicy` may have silently dropped this stage's stderr even
        // when it isn't the last stage, and pipefail's diagnostics come from
        // *this* stage — so its truncation must be visible on the folded result,
        // not just the last stage's.
        return ProcessResult::new(
            stage.program.clone(),
            last_stdout,
            stage.stderr.clone(),
            stage.outcome,
            stage.timeout,
        )
        .with_ok_codes(stage.ok_codes.clone())
        .with_truncated(stage.stderr_truncated);
    }

    // No checked failure: the last stage speaks. For an unchecked last stage that
    // exited non-clean, widen ok_codes to include the real code so is_success() is
    // true without fabricating 0. Signal/timeout outcomes stay non-success regardless.
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
    .with_truncated(last.stderr_truncated)
}

/// `a | b` — sugar for [`Command::pipe`]. Parenthesize the chain before a
/// terminal verb, since method calls bind tighter than `|`.
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
            torn_down: false,
            stderr_truncated: false,
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

    fn unchecked_fail(program: &str, outcome: Outcome) -> StageOutcome {
        StageOutcome {
            unchecked: true,
            ..unclean(program, outcome, "forgiven")
        }
    }

    fn last(outcome: Outcome, unchecked: bool) -> StageOutcome {
        StageOutcome {
            program: "last".into(),
            outcome,
            stderr: "last-err".into(),
            unchecked,
            ok_codes: vec![0],
            timeout: None,
            torn_down: false,
            stderr_truncated: false,
        }
    }

    fn pf(mut inner: Vec<StageOutcome>, last: StageOutcome, stdout: &str) -> ProcessResult<String> {
        inner.push(last);
        pipefail(inner, stdout.to_owned())
    }

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
        match result.ensure_success() {
            Err(crate::Error::Exit {
                program,
                code,
                stdout,
                stderr,
                ..
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
    fn rejected_zero_stage_stays_a_failure_after_attribution() {
        // A stage whose `ok_codes` exclude 0 that nonetheless exits 0 is a
        // *failure* (`is_clean` says so) and wins attribution. The rebuilt result
        // must carry that stage's own `ok_codes`, or the `ProcessResult::new`
        // default `[0]` would make `is_success()` report true for a chain the
        // fold deemed failed — so `run`/`checked`/`probe` would return `Ok`.
        let culprit = StageOutcome {
            ok_codes: vec![1],
            ..unclean("check", Outcome::Exited(0), "rejected zero")
        };
        let result = pf(vec![culprit], last(Outcome::Exited(0), false), "final");
        assert_eq!(result.program(), "check", "the rejected-zero stage wins");
        assert_eq!(result.code(), Some(0));
        assert_eq!(result.stderr(), "rejected zero");
        assert!(
            !result.is_success(),
            "a rejected-zero failure must not report success just because it exited 0"
        );
        assert!(
            result.ensure_success().is_err(),
            "the chain must surface the failure, not swallow it as Ok"
        );
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
    fn attributed_inner_stage_truncation_survives_the_fold_not_just_the_last_stage() {
        // T-042: an INNER (non-last) stage's silently-dropped stderr must show up
        // as `truncated()` on the folded result when pipefail blames *that*
        // stage — before this, only the last stage's `Finished` carried a
        // truncation signal, so a bounded-buffer-clipped diagnostic on the real
        // culprit was reported as complete (`truncated() == false`).
        let culprit = StageOutcome {
            stderr_truncated: true,
            ..unclean("b", Outcome::Exited(2), "b broke (clipped)")
        };
        let result = pf(
            vec![clean("a"), culprit],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert_eq!(result.program(), "b", "the inner failing stage is blamed");
        assert!(
            result.truncated(),
            "the attributed inner stage's dropped stderr must be visible: {result:?}"
        );

        // Contrast: the same inner failure WITHOUT truncation must not falsely
        // report truncated (a plain sanity check the flag isn't stuck on).
        let clean_culprit = unclean("b", Outcome::Exited(2), "b broke");
        let untruncated = pf(
            vec![clean("a"), clean_culprit],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert!(
            !untruncated.truncated(),
            "an inner failure with no dropped stderr must not report truncated: {untruncated:?}"
        );
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
    fn torn_down_victim_does_not_steal_blame_from_the_real_failure() {
        // Proactive teardown: a downstream stage fails, the chain kills the group,
        // and a quiet upstream (and the last stage) come back signal-killed. Those
        // teardown kills are victims — the genuine downstream failure must still be
        // blamed, even though its Signalled(9) siblings are leftward / would-be
        // culprits by position.
        let torn_upstream = StageOutcome {
            torn_down: true,
            ..unclean(
                "upstream",
                Outcome::Signalled(Some(9)),
                "killed by teardown",
            )
        };
        let culprit = unclean("downstream", Outcome::Exited(3), "the real failure");
        let torn_last = StageOutcome {
            torn_down: true,
            ..last(Outcome::Signalled(Some(9)), false)
        };
        let result = pf(vec![torn_upstream, culprit], torn_last, "");
        assert_eq!(
            result.program(),
            "downstream",
            "the failure that triggered teardown wins, not a torn-down victim"
        );
        assert_eq!(result.code(), Some(3));
        match result.ensure_success() {
            Err(crate::Error::Exit { program, code, .. }) => {
                assert_eq!(program, "downstream");
                assert_eq!(code, 3);
            }
            other => panic!("expected Error::Exit, got {other:?}"),
        }
    }

    #[test]
    fn all_torn_down_failures_fall_back_to_the_leftmost() {
        // Degenerate: every checked failure is a teardown victim (no un-torn
        // culprit survives). Attribution falls back to the leftmost, exactly as it
        // does when every failure is a SIGPIPE victim.
        let first = StageOutcome {
            torn_down: true,
            ..unclean("a", Outcome::Signalled(Some(9)), "first killed")
        };
        let second = StageOutcome {
            torn_down: true,
            ..unclean("b", Outcome::Signalled(Some(9)), "second killed")
        };
        let result = pf(vec![first, second], last(Outcome::Exited(0), false), "");
        assert_eq!(
            result.program(),
            "a",
            "leftmost victim when all are torn down"
        );
        assert!(!result.is_success());
    }

    #[test]
    fn checked_last_stage_failure_still_speaks_verbatim() {
        // Regression guard: a checked unclean last stage must not be discarded.
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
        let result = pf(
            vec![unclean("a", Outcome::Signalled(None), "killed")],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), None);
        assert_eq!(result.stderr(), "killed");
        assert!(!result.timed_out(), "a stage kill is not a chain timeout");
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
