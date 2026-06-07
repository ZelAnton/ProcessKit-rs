//! [`Pipeline`] — `a | b | c` without a shell.
//!
//! Each stage's stdout feeds the next stage's stdin through a native pipe — no
//! shell string, so no quoting or injection surface. Every stage spawns into
//! one shared kill-on-drop [`ProcessGroup`](crate::ProcessGroup), so the whole
//! chain dies as a unit, and the outcome is **pipefail**: the first stage
//! without a clean exit decides the reported code/diagnostics.

use std::time::Duration;

use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::ProcessResult;

/// A chain of [`Command`]s connected stdout→stdin — built with
/// [`Command::pipe`], extended with [`pipe`](Self::pipe), driven with
/// [`output_string`](Self::output_string) / [`run`](Self::run).
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
///   [`unchecked`](Command::unchecked) are exempt: their unclean exits are
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
/// - A `Pipeline` can be re-run (stages are re-cloned per run), but a one-shot
///   [`Stdin`](crate::Stdin) source on the *first* stage
///   (`from_reader`/`from_lines`) is consumed by the first run and feeds
///   empty stdin afterwards — the same semantics as re-running a
///   [`Command`].
#[must_use = "a Pipeline does nothing until it is run"]
#[derive(Clone)]
pub struct Pipeline {
    stages: Vec<Command>,
    timeout: Option<Duration>,
}

// Manual: `Command` has a manual Debug; keep the surface small.
impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.stages.len())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// What one finished inner stage reported — input to the pipefail fold. (An
/// inner stage that hit its own [`Command::timeout`] shows up here as its
/// unclean exit code, not as a separate timed-out flag.)
struct StageOutcome {
    program: String,
    code: Option<i32>,
    stderr: String,
    /// The stage opted out of pipefail attribution ([`Command::unchecked`]).
    unchecked: bool,
}

impl Pipeline {
    pub(crate) fn new(first: Command, second: Command) -> Self {
        Pipeline {
            stages: vec![first, second],
            timeout: None,
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

    /// Run the chain to completion and capture the outcome. A failing stage is
    /// **not** an `Err` here — it is reported in the result (pipefail
    /// attribution, see the type docs); `Err` means a stage could not be
    /// started or driven at all.
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        let group = ProcessGroup::new()?;

        // Start every stage, chaining stage N's stdout into stage N+1's stdin.
        // The relay is the stdin copy task `launch` spawns per stage, so data
        // flows without this future's involvement.
        let mut running = Vec::with_capacity(self.stages.len());
        let mut upstream = None;
        for (index, stage) in self.stages.iter().enumerate() {
            let mut command = stage.clone();
            if let Some(reader) = upstream.take() {
                command.set_pipe_stdin(reader);
            }
            let mut process = group.start(&command).await?;
            if index + 1 < self.stages.len() {
                upstream = process.take_stdout_pipe();
            }
            // Carry the stage's unchecked flag with its handle: the last stage
            // is popped off below, so positional lookups would be fragile.
            running.push((process, stage.is_unchecked()));
        }

        // Drain every stage concurrently: a stderr-chatty inner stage must not
        // block on a full pipe while we wait on its neighbours.
        let (last, last_unchecked) = running.pop().expect("a pipeline has at least two stages");
        let mut inner_tasks = Vec::with_capacity(running.len());
        for (process, unchecked) in running {
            let program = process.program_name().to_owned();
            inner_tasks.push(tokio::spawn(async move {
                let (code, stderr) = process.finish_streamed().await?;
                Ok::<_, crate::Error>(StageOutcome {
                    program,
                    code,
                    stderr,
                    unchecked,
                })
            }));
        }
        let last_task = tokio::spawn(async move { last.output_string().await });

        let collect = async {
            let mut outcomes = Vec::with_capacity(inner_tasks.len() + 1);
            for task in inner_tasks {
                outcomes.push(task.await.map_err(join_error)??);
            }
            let last_result = last_task.await.map_err(join_error)??;
            Ok::<_, crate::Error>((outcomes, last_result))
        };

        let (outcomes, last_result) = match self.timeout {
            None => collect.await?,
            Some(limit) => match tokio::time::timeout(limit, collect).await {
                Ok(collected) => collected?,
                Err(_elapsed) => {
                    // Deadline: kill the whole chain. The stages exit promptly,
                    // so the moved-out drain tasks finish on their own; report
                    // the timeout in the result like `Command::timeout` does.
                    let _ = group.terminate_all();
                    return Ok(ProcessResult::new(
                        self.pipeline_name(),
                        String::new(),
                        String::new(),
                        None,
                        true,
                        Some(limit),
                    ));
                }
            },
        };

        Ok(pipefail(
            outcomes,
            last_result,
            last_unchecked,
            self.timeout,
        ))
    }

    /// Run the chain, require **every** stage to exit cleanly, and return the
    /// last stage's trimmed stdout. A failure surfaces as the first failing
    /// stage's [`Error::Exit`](crate::Error::Exit) (pipefail attribution;
    /// [`unchecked`](Command::unchecked) stages are exempt, so a chain whose
    /// only failures are unchecked returns `Ok`).
    /// [`Error::Timeout`](crate::Error::Timeout) is produced by the
    /// whole-chain [`timeout`](Self::timeout) or the *last* stage's own
    /// deadline; an **inner** stage's [`Command::timeout`] kills just that
    /// stage and surfaces as its signal-kill IO error.
    pub async fn run(&self) -> Result<String> {
        Ok(self
            .output_string()
            .await?
            .ensure_success()?
            .into_stdout()
            .trim_end()
            .to_owned())
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

/// Fold the per-stage outcomes into one pipefail result: the last stage's
/// stdout, with code/stderr/program attributed to the first **checked**
/// unclean stage (or the last stage when no checked stage failed).
///
/// `unchecked` rules (duct's precedence, kept on OUR first-failure
/// attribution where duct ties go right): a checked failure always trumps an
/// unchecked one, regardless of position; an unchecked stage's unclean exit
/// neither speaks for the chain nor shields anyone else's failure. When the
/// only failures are unchecked, the chain reports success. An unchecked
/// *last* stage's unclean exit is likewise forgiven — but a timeout never is
/// (a deadline violation is not an exit status).
fn pipefail(
    outcomes: Vec<StageOutcome>,
    last: ProcessResult<String>,
    last_unchecked: bool,
    pipeline_timeout: Option<Duration>,
) -> ProcessResult<String> {
    // "Unclean" is any `code != Some(0)`: non-zero, and the `None` of a
    // signal kill (SIGPIPE included) or per-stage-timeout kill — no
    // platform-specific cases needed.
    if let Some(stage) = outcomes
        .into_iter()
        .find(|stage| stage.code != Some(0) && !stage.unchecked)
    {
        // A checked inner stage failed first — its diagnostics win; the last
        // stage's stdout is still what the chain produced.
        return ProcessResult::new(
            stage.program,
            last.into_stdout(),
            stage.stderr,
            stage.code,
            false,
            pipeline_timeout,
        );
    }
    if last_unchecked && !last.timed_out() && last.code() != Some(0) {
        // The last stage failed but opted out: report the chain as a success
        // carrying its output and (for the curious) its stderr.
        let program = last.program().to_owned();
        let stderr = last.stderr().to_owned();
        return ProcessResult::new(
            program,
            last.into_stdout(),
            stderr,
            Some(0),
            false,
            pipeline_timeout,
        );
    }
    // No checked inner failure: the last stage speaks for the chain,
    // succeeding or not.
    last
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

    fn clean(program: &str) -> StageOutcome {
        StageOutcome {
            program: program.into(),
            code: Some(0),
            stderr: String::new(),
            unchecked: false,
        }
    }

    fn unclean(program: &str, code: Option<i32>, stderr: &str) -> StageOutcome {
        StageOutcome {
            program: program.into(),
            code,
            stderr: stderr.into(),
            unchecked: false,
        }
    }

    /// An unclean stage that opted out of attribution.
    fn unchecked_fail(program: &str, code: Option<i32>) -> StageOutcome {
        StageOutcome {
            unchecked: true,
            ..unclean(program, code, "forgiven")
        }
    }

    fn last_stage(code: Option<i32>, stdout: &str) -> ProcessResult<String> {
        ProcessResult::new(
            "last".into(),
            stdout.into(),
            "last-err".into(),
            code,
            false,
            None,
        )
    }

    #[test]
    fn all_clean_inner_stages_let_the_last_stage_speak() {
        // Success and failure of the last stage alike pass through untouched.
        let ok = pipefail(
            vec![clean("a"), clean("b")],
            last_stage(Some(0), "final"),
            false,
            None,
        );
        assert_eq!(ok, last_stage(Some(0), "final"));

        let failing_last = pipefail(
            vec![clean("a")],
            last_stage(Some(3), "partial"),
            false,
            None,
        );
        assert_eq!(failing_last, last_stage(Some(3), "partial"));
    }

    #[test]
    fn failing_inner_stage_wins_but_stdout_stays_the_chains() {
        let result = pipefail(
            vec![clean("a"), unclean("b", Some(2), "b broke")],
            last_stage(Some(0), "final"),
            false,
            None,
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
        let result = pipefail(
            vec![
                unclean("a", Some(1), "first"),
                unclean("b", Some(2), "second"),
            ],
            last_stage(Some(0), "out"),
            false,
            None,
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
        // The head-pattern: the producer's SIGPIPE death (code None) is
        // forgiven, the chain succeeds with the consumer's output.
        let result = pipefail(
            vec![unchecked_fail("producer", None)],
            last_stage(Some(0), "first line"),
            false,
            None,
        );
        assert!(result.is_success(), "got {result:?}");
        assert_eq!(result.stdout(), "first line");
        assert_eq!(result.program(), "last", "the clean last stage speaks");
    }

    #[test]
    fn checked_failure_trumps_unchecked_regardless_of_order() {
        // unchecked-then-checked: the later checked failure wins.
        let result = pipefail(
            vec![
                unchecked_fail("a", Some(141)),
                unclean("b", Some(2), "real"),
            ],
            last_stage(Some(0), "out"),
            false,
            None,
        );
        assert_eq!(result.program(), "b", "unchecked never shields a failure");
        assert_eq!(result.code(), Some(2));

        // checked-then-unchecked: the first (checked) failure wins, as today.
        let result = pipefail(
            vec![unclean("a", Some(1), "real"), unchecked_fail("b", Some(2))],
            last_stage(Some(0), "out"),
            false,
            None,
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), Some(1));
    }

    #[test]
    fn attribution_skips_unchecked_to_the_first_checked_failure() {
        let result = pipefail(
            vec![
                clean("a"),
                unchecked_fail("b", Some(1)),
                unclean("c", Some(3), "c broke"),
                unclean("d", Some(4), "d broke"),
            ],
            last_stage(Some(0), "out"),
            false,
            None,
        );
        assert_eq!(result.program(), "c", "first CHECKED failure is blamed");
        assert_eq!(result.code(), Some(3));
        assert_eq!(result.stderr(), "c broke");
    }

    #[test]
    fn unchecked_last_stage_failure_is_forgiven() {
        let result = pipefail(
            vec![clean("a")],
            last_stage(Some(141), "partial"),
            true,
            None,
        );
        assert!(result.is_success(), "got {result:?}");
        assert_eq!(result.code(), Some(0));
        assert_eq!(result.stdout(), "partial", "output is preserved");
        assert_eq!(result.stderr(), "last-err", "stderr kept for the curious");
        assert!(result.ensure_success().is_ok());
    }

    #[test]
    fn checked_last_stage_failure_still_speaks_verbatim() {
        // Regression guard: without the marker nothing changes.
        let result = pipefail(
            vec![clean("a")],
            last_stage(Some(3), "partial"),
            false,
            None,
        );
        assert_eq!(result, last_stage(Some(3), "partial"));
    }

    #[test]
    fn unchecked_never_forgives_a_timeout() {
        // An unchecked LAST stage that timed out still reports the timeout —
        // a deadline violation is not an exit status.
        let timed_out_last = ProcessResult::new(
            "last".into(),
            String::new(),
            String::new(),
            None,
            true,
            None,
        );
        let result = pipefail(vec![clean("a")], timed_out_last, true, None);
        assert!(result.timed_out());
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
        // A signal-kill (or per-stage timeout kill) reports no code — that is
        // not a clean exit, so the stage must win the attribution.
        let result = pipefail(
            vec![unclean("a", None, "killed")],
            last_stage(Some(0), "out"),
            false,
            None,
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), None);
        assert_eq!(result.stderr(), "killed");
        assert!(!result.timed_out(), "a stage kill is not a chain timeout");
        // No code + no timeout surfaces as the signal-kill IO error, naming
        // the attributed (failing) stage.
        match result.ensure_success() {
            Err(crate::Error::Io(e)) => {
                assert!(e.to_string().contains("`a`"), "got: {e}");
            }
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }
}
