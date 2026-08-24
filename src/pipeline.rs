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
//! [`ProcessGroup`] sub-group, so a per-stage
//! [`Command::timeout`] tears down that stage's *whole* subtree (grandchildren of
//! a forking `sh -c …` included), while a chain-wide
//! [`Pipeline::timeout`]/teardown fans the kill across every sub-group so the
//! whole chain still dies as a unit. The outcome is **pipefail**: the first stage
//! without a clean exit decides the reported code/diagnostics.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::{Outcome, ProcessResult};
use crate::running::{
    ExitObservation, Finished, LineCapture, ProcessEvents, RawCapture, RunningProcess, StdoutLines,
};
use crate::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

// Once a stage closes its output with a checked failure, downstream stages need
// one bounded scheduling window to consume the final pipe bytes and EOF. Without
// it, the proactive whole-chain kill can race a filter that only flushes at EOF
// and discard the very diagnostic `merge_stderr_in_pipe` was asked to preserve.
const TEARDOWN_DRAIN_GRACE: Duration = Duration::from_millis(500);

// The cadence bounds of `spawn_last_stage_watcher`'s exit probe: it starts at the
// first and backs off by doubling to the second, so a chain whose last stage fails
// right after `start` is torn down quickly, while a healthy long-lived session
// settles at one non-blocking `try_wait` every `LAST_STAGE_PROBE_MAX`.
const LAST_STAGE_PROBE_MIN: Duration = Duration::from_millis(25);
const LAST_STAGE_PROBE_MAX: Duration = Duration::from_millis(500);

#[cfg(all(test, feature = "process-control"))]
thread_local! {
    /// Deterministic race seam: run one synchronous callback after the proactive
    /// drain grace but before its fallback group kill. Keeping it thread-local
    /// matches the existing fault injector and isolates parallel test runtimes.
    static BEFORE_PIPELINE_FALLBACK_KILL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, feature = "process-control"))]
struct PipelineFallbackKillHookGuard;

#[cfg(all(test, feature = "process-control"))]
impl Drop for PipelineFallbackKillHookGuard {
    fn drop(&mut self) {
        BEFORE_PIPELINE_FALLBACK_KILL.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(all(test, feature = "process-control"))]
fn before_pipeline_fallback_kill(hook: impl FnOnce() + 'static) -> PipelineFallbackKillHookGuard {
    BEFORE_PIPELINE_FALLBACK_KILL.with(|slot| {
        assert!(
            slot.borrow_mut().replace(Box::new(hook)).is_none(),
            "pipeline fallback-kill hook already armed on this thread"
        );
    });
    PipelineFallbackKillHookGuard
}

#[cfg(all(test, feature = "process-control"))]
fn run_before_pipeline_fallback_kill() {
    BEFORE_PIPELINE_FALLBACK_KILL.with(|slot| {
        if let Some(hook) = slot.borrow_mut().take() {
            hook();
        }
    });
}

#[cfg(not(all(test, feature = "process-control")))]
fn run_before_pipeline_fallback_kill() {}

#[cfg(test)]
#[derive(Clone, Copy)]
struct PipelineTerminalConfirmationTestConfig {
    budget: Duration,
    suppress: bool,
}

#[cfg(test)]
thread_local! {
    /// Deterministic terminal-confirmation seam. The ignored real-process tests
    /// run on a current-thread runtime, so spawned stage tasks inherit this
    /// thread-local override without leaking it into parallel test runtimes.
    static PIPELINE_TERMINAL_CONFIRMATION_TEST_CONFIG:
        std::cell::Cell<Option<PipelineTerminalConfirmationTestConfig>> = const {
            std::cell::Cell::new(None)
        };
}

#[cfg(test)]
struct PipelineTerminalConfirmationTestGuard;

#[cfg(test)]
impl Drop for PipelineTerminalConfirmationTestGuard {
    fn drop(&mut self) {
        PIPELINE_TERMINAL_CONFIRMATION_TEST_CONFIG.with(|slot| slot.set(None));
    }
}

#[cfg(test)]
fn suppress_pipeline_terminal_confirmation(
    budget: Duration,
) -> PipelineTerminalConfirmationTestGuard {
    PIPELINE_TERMINAL_CONFIRMATION_TEST_CONFIG.with(|slot| {
        assert!(
            slot.replace(Some(PipelineTerminalConfirmationTestConfig {
                budget,
                suppress: true,
            }))
            .is_none(),
            "pipeline terminal-confirmation seam already armed on this thread"
        );
    });
    PipelineTerminalConfirmationTestGuard
}

fn pipeline_terminal_confirmation_budget() -> Duration {
    #[cfg(test)]
    if let Some(config) = PIPELINE_TERMINAL_CONFIRMATION_TEST_CONFIG.with(std::cell::Cell::get) {
        return config.budget;
    }
    crate::running::PUMP_TEARDOWN
}

fn suppress_terminal_confirmation_for_test() -> bool {
    #[cfg(test)]
    {
        PIPELINE_TERMINAL_CONFIRMATION_TEST_CONFIG
            .with(std::cell::Cell::get)
            .is_some_and(|config| config.suppress)
    }
    #[cfg(not(test))]
    false
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
///   (any configured source is overridden). Stdout is the mirror image: the
///   *last* stage's is the chain's output and is honored as configured when that
///   destination is supported, while an inner stage's belongs to the pipe — a
///   [`stdout`](Command::stdout) mode or [`stdout_file`](Command::stdout_file)
///   redirect set on a non-final stage goes inert for that run (the file is
///   neither created nor truncated, and no stdout observer fires — as none
///   does on any non-final stage); see [`pipe`](Self::pipe) for the full
///   policy. Inner stages' stderr is captured per-stage for pipefail
///   diagnostics unless that stage opts into
///   [`merge_stderr_in_pipe`](Command::merge_stderr_in_pipe), which sends it
///   through the downstream pipe and gives up the separate capture.
/// - **PTY only at the end** — `Command::use_pty` is supported on the final
///   stage when both logical output destinations remain `Piped`; its merged
///   terminal stream is then the pipeline's captured or streamed stdout. Every
///   PTY stage's destination is checked before the first process starts. A PTY
///   on any earlier stage retains its more specific rejection with
///   [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported) before the
///   first process starts: a PTY master is a terminal session, not a stdout pipe
///   that can be handed to the next stage.
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
    /// Hermetic launch seam for this module's public-path tests. Production
    /// pipelines always spawn through a fresh per-stage [`ProcessGroup`].
    #[cfg(test)]
    test_runner: Option<Arc<dyn crate::runner::ProcessRunner>>,
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

/// One stage task's outcome, tagged with enough to fold it back into position
/// after the stages complete in **unordered** (true completion) order — see
/// the comment on `capture`'s task-driving `JoinSet` for why unordered
/// completion, rather than left-to-right positional awaiting, is what keeps
/// the chain live.
enum Joined<T> {
    /// An inner (non-last) stage, tagged with its original index so the fold
    /// can rebuild `stages` in left-to-right order once every task is in.
    Inner(usize, StageOutcome),
    /// The last stage: its captured output (`capture_last`'s result) plus
    /// whether the chain's teardown had already fired when it finished.
    Last(ProcessResult<T>, bool),
}

struct Captured<T> {
    stdout: T,
    stderr: String,
    truncated: bool,
    total_lines: usize,
    total_bytes: usize,
}

/// The two capture shapes `capture` is generic over — `String` (decoded lines)
/// and `Vec<u8>` (raw stdout bytes) — behind one seam: how to prepare the last
/// stage's sinks before it moves into a task, and how to snapshot whatever they
/// hold when a chain-wide timeout drops that task. No `Default` bound: the
/// timeout path salvages a real snapshot (`T::snapshot`), never an empty
/// placeholder.
trait PipelineCapture: Send + Clone + 'static {
    type Tracker: Clone;

    fn prepare(process: &mut RunningProcess) -> Result<Self::Tracker>;
    fn retained_snapshot(tracker: &Self::Tracker) -> Captured<Self>;
    fn snapshot(tracker: &Self::Tracker) -> Captured<Self>;
    fn to_diagnostic_text(captured: &Self) -> String;
    fn exact_bytes(captured: &Self) -> Option<Vec<u8>>;
}

impl PipelineCapture for String {
    type Tracker = LineCapture;

    fn prepare(process: &mut RunningProcess) -> Result<Self::Tracker> {
        process.prepare_line_capture()
    }

    fn retained_snapshot(tracker: &Self::Tracker) -> Captured<Self> {
        let (stdout, stderr, truncated, total_lines, total_bytes) = tracker.retained_snapshot();
        Captured {
            stdout,
            stderr,
            truncated,
            total_lines,
            total_bytes,
        }
    }

    fn snapshot(tracker: &Self::Tracker) -> Captured<Self> {
        let (stdout, stderr, truncated, total_lines, total_bytes) = tracker.snapshot();
        Captured {
            stdout,
            stderr,
            truncated,
            total_lines,
            total_bytes,
        }
    }

    fn to_diagnostic_text(captured: &Self) -> String {
        captured.clone()
    }

    fn exact_bytes(_captured: &Self) -> Option<Vec<u8>> {
        None
    }
}

impl PipelineCapture for Vec<u8> {
    type Tracker = RawCapture;

    fn prepare(process: &mut RunningProcess) -> Result<Self::Tracker> {
        process.prepare_raw_capture()
    }

    fn retained_snapshot(tracker: &Self::Tracker) -> Captured<Self> {
        let (stdout, stderr, truncated, total_lines, total_bytes) = tracker.retained_snapshot();
        Captured {
            stdout,
            stderr,
            truncated,
            total_lines,
            total_bytes,
        }
    }

    fn snapshot(tracker: &Self::Tracker) -> Captured<Self> {
        let (stdout, stderr, truncated, total_lines, total_bytes) = tracker.snapshot();
        Captured {
            stdout,
            stderr,
            truncated,
            total_lines,
            total_bytes,
        }
    }

    fn to_diagnostic_text(captured: &Self) -> String {
        String::from_utf8_lossy(captured).into_owned()
    }

    fn exact_bytes(captured: &Self) -> Option<Vec<u8>> {
        Some(captured.clone())
    }
}

/// The wait-boundary observer [`capture`](Pipeline::capture) hands to its `capture_last`
/// closure, to be armed on the capture verb it picks
/// (`output_string_observing_exit`/`output_bytes_observing_exit`): it latches the
/// last stage's [`ExitDisposition`] when its wait settles, before output finishes
/// draining, and separately says whether terminal reap was actually proven.
/// Boxed because `capture_last` is generic over the capture shape and the observer
/// has to be a *nameable* parameter of it; `Send` because the future it ends up
/// inside is spawned onto the chain's `JoinSet`.
type LastExitObserver = Box<dyn FnOnce(ExitObservation) + Send>;

fn captured_result<T>(result: ProcessResult<T>) -> Captured<T> {
    let stderr = result.stderr().to_owned();
    let truncated = result.truncated();
    let total_lines = result.total_lines();
    let total_bytes = result.total_bytes();
    Captured {
        stdout: result.into_stdout(),
        stderr,
        truncated,
        total_lines,
        total_bytes,
    }
}

/// A launched chain, before the last stage is split off — the shared product of
/// [`Pipeline::launch`], consumed by the buffering [`capture`](Pipeline::capture)
/// path and the streaming [`start`](Pipeline::start) path alike. Holding it keeps
/// every stage's kill-on-drop sub-group alive; dropping it (e.g. after a mid-chain
/// spawn failure returned by `launch` via `?`) tears every already-started stage
/// down, so a partially-launched chain never leaks.
struct LaunchedChain {
    /// A strong handle to every stage's kill-on-drop sub-group, so the chain-wide
    /// teardown can fan a hard kill across all of them.
    stage_groups: Vec<Arc<ProcessGroup>>,
    /// Every stage's live handle paired with its `unchecked_in_pipe` flag, in
    /// left-to-right order (the last stage is the final element).
    running: Vec<(RunningProcess, bool)>,
    /// Wall-clock start of the whole chain, captured before the first spawn.
    started: std::time::Instant,
}

/// The last stage split off a launched chain by [`Pipeline::detach_last`]: the
/// live streaming handle plus the pipefail metadata [`PipelineSession::finish`]
/// folds it back into position with.
struct DetachedLast {
    handle: RunningProcess,
    program: String,
    ok_codes: Vec<i32>,
    timeout: Option<Duration>,
    unchecked: bool,
}

impl Pipeline {
    pub(crate) fn new(first: Command, second: Command) -> Self {
        Pipeline {
            stages: vec![first, second],
            timeout: None,
            cancel_token: None,
            #[cfg(test)]
            test_runner: None,
        }
    }

    /// Replace real process launch only for in-crate tests that must drive the
    /// complete capture/session orchestration without a subprocess.
    #[cfg(test)]
    fn with_test_runner(mut self, runner: Arc<dyn crate::runner::ProcessRunner>) -> Self {
        self.test_runner = Some(runner);
        self
    }

    /// Append another stage: the current last stage's stdout becomes `next`'s
    /// stdin.
    ///
    /// That link is unconditional, and it is the chain — not the stage — that owns a
    /// **non-final** stage's stdout. A stage configured with
    /// [`stdout(StdioMode::Null)`](Command::stdout),
    /// [`stdout(StdioMode::Inherit)`](Command::stdout), or a
    /// [`stdout_file`](Command::stdout_file) redirect still feeds the next stage once
    /// it stops being the last one: the pipe wins, that configuration goes inert for
    /// the run (a redirect file is neither created nor truncated), and the stage's
    /// stdout observers — `on_stdout_line`, `stdout_tee`, `stdout_raw_tee` — do not
    /// fire, as they do not on any non-final stage. Stderr keeps its own
    /// configuration, and the **final** stage's stdout, being the chain's output, is
    /// honoured as configured when that destination is supported. In particular, a
    /// final `use_pty` stage requires piped logical stdout and stderr; the whole
    /// chain is validated before any stage starts.
    ///
    /// The one non-final stdout configuration that is rejected instead of overridden
    /// is `use_pty` (the `pty` feature): a PTY master's merged terminal stream cannot
    /// feed a later stage at all, so the chain fails before spawn with
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported).
    pub fn pipe(mut self, next: Command) -> Self {
        self.stages.push(next);
        self
    }

    /// Kill the **whole chain** if it exceeds `timeout` (every stage's sub-group
    /// is torn down; the result reports `timed_out`). The result keeps the
    /// best-effort stdout and stderr already captured by the last stage before
    /// the deadline, using the same buffer policy as a normal capture.
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
    /// stage, so each stage's run is cancelled and kills its
    /// own subtree, and the run resolves to
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) once terminal
    /// teardown is confirmed, or [`ErrorReason::Teardown`](crate::ErrorReason::Teardown)
    /// with the first OS teardown error when it is not. This is **proactive** —
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
    /// (like a downstream `SIGPIPE` death), never stealing the blame. A stage is a
    /// victim by *when it died*, not by when the last of its output arrived — one
    /// that had already exited on its own before the teardown fired stays the
    /// culprit even if a grandchild it forked holds its stderr pipe open long
    /// afterwards. The one
    /// death that does *not* trigger teardown is an
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) stage's — its unclean exit
    /// is forgiven, so it leaves the rest of the chain running.
    ///
    /// At launch the token **gap-fills** a stage with no token of its own. A stage
    /// that already carries an explicit [`Command::cancel_on`] keeps that token
    /// and observes the chain token as an additional trigger — firing either one
    /// cancels the stage, while neither token is cancelled as a side effect of the
    /// other. The chain therefore never overrides a per-stage choice, while its
    /// own token remains unconditionally chain-wide.
    ///
    /// Like [`Command::cancel_on`], a cancelled run is terminal: it is not
    /// retried, and the chain cannot be re-run through a token that stays
    /// cancelled. Finalization collects every stage's bounded terminal
    /// disposition: a sibling [`ErrorReason::Teardown`](crate::ErrorReason::Teardown)
    /// outranks ordinary `Cancelled`, stdin, or pump errors, so the chain never
    /// claims confirmed cancellation while another stage may still be live.
    /// If the fallback group kill itself fails, its [`TeardownCause`](crate::TeardownCause)
    /// is the first condition that fired proactive teardown: stage-local or chain
    /// cancellation remains `Cancellation`, while an earlier stage failure remains
    /// `PipelineFailure` even if another token fires during the drain grace.
    pub fn cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Launch every stage of the chain — the shared start-up core reused by the
    /// buffering [`capture`](Self::capture) verbs and the streaming
    /// [`start`](Self::start) session, so a change to how a chain comes up (per-stage
    /// sub-groups, the stdout→stdin relay, chain cancellation) lands on both.
    ///
    /// Each stage spawns into its **own** kill-on-drop sub-group, retained as a
    /// strong handle so a per-stage [`Command::timeout`]/[`cancel_on`](Command::cancel_on)
    /// tears down that stage's *whole* subtree (grandchildren of a forking `sh -c …`
    /// included) and a chain-wide teardown can fan a kill across every one. The
    /// pipeline [`cancel_on`](Self::cancel_on) token gap-fills a stage without a
    /// token and becomes an additional trigger for a stage with one of its own.
    ///
    /// # A non-final stage's stdout belongs to the pipe
    ///
    /// The relay this builds is what makes each stage's stdout the next stage's
    /// stdin, so on a **non-final** stage the internal downstream pipe **wins over
    /// that stage's own stdout destination**. A non-final
    /// [`stdout(StdioMode::Null)`](Command::stdout),
    /// [`stdout(StdioMode::Inherit)`](Command::stdout), or
    /// [`stdout_file`](Command::stdout_file)/[`stdout_file_append`](Command::stdout_file_append)
    /// is therefore **inert for that run**: the stage's bytes go downstream and
    /// nowhere else — not to `/dev/null`, not to the parent's terminal, not to the
    /// file. A configured stdout file is **not created and not truncated**, since the
    /// override drops the redirect instead of opening it (the same care the
    /// [`merge_stderr_in_pipe`](Command::merge_stderr_in_pipe) override takes with the
    /// file it displaces). The stage's stdout observers — `on_stdout_line`,
    /// `stdout_tee`, `stdout_raw_tee` — do not fire either, matching both the
    /// configuration being overridden (which documents them as inert) and every other
    /// non-final stage, whose stdout goes to the next stage rather than through a pump
    /// that could run them. **Stderr is untouched** by all of this: a non-final stage's
    /// stderr keeps its own configuration and stays available for pipefail
    /// attribution, unless the stage opted into `merge_stderr_in_pipe`.
    ///
    /// The alternative — rejecting such a stage before spawn — was not taken: the
    /// chain's documented contract is unconditional, the mirror-image
    /// [`set_pipe_stdin`](Command::set_pipe_stdin) already overrides a *downstream*
    /// stage's configured stdin the same way, and honouring the pipe keeps a `Command`
    /// that carries a redirect for its standalone use reusable as a stage. What it is
    /// **not** is silent data loss: before this, such a stage's output vanished (or
    /// landed in a file) while the next stage started on an immediate EOF and the
    /// chain reported success. The **final** stage's stdout is the chain's output and
    /// is honoured as configured when that destination is supported. A final
    /// `use_pty` stage specifically requires piped logical stdout and stderr.
    ///
    /// [`use_pty`](Command::use_pty) on a non-final stage stays a pre-spawn
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported) rather than an
    /// override: a PTY master carries a merged terminal stream that cannot be handed
    /// to a later stage at all, so there is no pipe to prefer.
    ///
    /// On a mid-chain spawn failure the `?` early-return drops the partially-built
    /// vectors, so kill-on-drop reaps every already-started stage — a partial chain
    /// never leaks (the "partially poured" teardown invariant).
    async fn launch(&self) -> Result<LaunchedChain> {
        if let Some(index) = self.stages[..self.stages.len() - 1]
            .iter()
            .position(Command::wants_pty)
        {
            return Err(crate::ErrorReason::Unsupported {
                operation: format!("pipeline use_pty on non-final stage {}", index + 1),
            }
            .into());
        }
        // Validate the complete chain before creating the first group. Keep this
        // after the topology check above so a non-final PTY retains the more useful
        // pipeline-specific diagnosis even when its own destination is invalid.
        #[cfg(feature = "pty")]
        for stage in &self.stages {
            stage.ensure_pty_stdio_compatible()?;
        }
        // Wall-clock start of the whole chain, before the first spawn.
        let started = std::time::Instant::now();

        let mut stage_groups: Vec<Arc<ProcessGroup>> = Vec::with_capacity(self.stages.len());
        let mut running = Vec::with_capacity(self.stages.len());
        let mut upstream = None;
        for (index, stage) in self.stages.iter().enumerate() {
            let mut command = stage.clone();
            let has_stage_cancel = command.cancel_token().is_some();
            let non_final = index + 1 < self.stages.len();
            if non_final && stage.wants_stderr_merged_in_pipe() {
                command.activate_stderr_merge_in_pipe();
            }
            // A non-final stage owes its stdout to the next stage's stdin, so the
            // internal pipe wins over a configured `stdout(Null)`/`stdout(Inherit)`/
            // `stdout_file(..)` — see this method's rustdoc for the policy and what
            // it means for the overridden destination. Without this the stage would
            // have no pipe to hand over, `take_stdout_pipe` below would come back
            // empty, and the next stage would start on a silent EOF (or, worse, on
            // whatever stdin it was configured with) while the producer's bytes went
            // to the terminal, to `/dev/null`, or to a file. Stages whose stdout is
            // already a pipe (the default) are left exactly as configured.
            if non_final && !stage.stdout_is_piped() {
                command.pipe_stdout_for_downstream();
            }
            // Preserve the command-level gap-fill for stages without their own
            // token, including the runner's pre-spawn cancellation check. A stage
            // with an explicit token gets the chain token as a second live-handle
            // trigger below, after its private group is attached.
            if let Some(token) = &self.cancel_token
                && !has_stage_cancel
            {
                command = command.cancel_on(token.clone());
            }
            if let Some(reader) = upstream.take() {
                command.set_pipe_stdin(reader);
            }
            // A stage-local token stays configured on the command so it remains
            // an independent live trigger. That means the runner cannot see the
            // chain token on this path, so enforce the chain's terminal contract
            // here before *every* sequential spawn (including the first one).
            if self
                .cancel_token
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            {
                return Err(crate::ErrorReason::Cancelled {
                    program: command.program().to_string_lossy().into_owned(),
                }
                .into());
            }
            #[cfg(test)]
            if let Some(runner) = &self.test_runner {
                let mut process = runner.start(&command).await?;
                if has_stage_cancel && let Some(token) = &self.cancel_token {
                    process.add_cancel_trigger(token.clone());
                }
                if non_final {
                    upstream = process.take_stdout_pipe();
                }
                running.push((process, stage.is_unchecked()));
                continue;
            }
            // Spawn into a fresh per-stage group, then hand it to the stage handle
            // (`attach_group` also upgrades the cancel watchdog to a group+pid kill)
            // and keep a strong clone for the chain-wide teardown.
            let group = ProcessGroup::new()?;
            let mut process = group.start(&command).await?;
            process.attach_group(group);
            if has_stage_cancel && let Some(token) = &self.cancel_token {
                process.add_cancel_trigger(token.clone());
            }
            if let Some(handle) = process.own_group_handle() {
                stage_groups.push(handle);
            }
            if non_final {
                upstream = process.take_stdout_pipe();
            }
            // Bundle the unchecked flag with the handle; the last stage is split off by the caller.
            running.push((process, stage.is_unchecked()));
        }

        Ok(LaunchedChain {
            stage_groups,
            running,
            started,
        })
    }

    /// Split the last stage off a launched chain's handles as the streaming
    /// surface, paired with the pipefail metadata [`finish`](PipelineSession::finish)
    /// folds it back with. Kept private so its infallible `pop`/`last` (a
    /// `Pipeline` starts at and never drops below two stages, so `running` is never
    /// empty and `self.stages` is never empty here) stays out of the public
    /// [`start`](Self::start) — matching how the buffering verbs keep their
    /// same-invariant `expect`s inside the private `capture`.
    fn detach_last(&self, running: &mut Vec<(RunningProcess, bool)>) -> DetachedLast {
        let (handle, unchecked) = running.pop().expect("a pipeline has at least two stages");
        let stage = self
            .stages
            .last()
            .expect("a pipeline has at least two stages");
        DetachedLast {
            program: handle.program_name().to_owned(),
            ok_codes: stage.ok_codes_vec(),
            timeout: stage.configured_timeout(),
            unchecked,
            handle,
        }
    }

    /// Start the chain as a **live streaming session** — the multi-stage analogue
    /// of [`Command::start`](crate::Command::start), returning a
    /// [`PipelineSession`] you drive yourself instead of buffering the whole run.
    /// It brings the streaming surface (`journalctl -f | grep …`, `tail -F | jq`,
    /// any long-lived chain you read *from* rather than wait *out*) to pipelines.
    ///
    /// The session gives:
    ///
    /// - the **last** stage's stdout as it arrives —
    ///   [`stdout_lines`](PipelineSession::stdout_lines) /
    ///   [`events`](PipelineSession::events), with the same
    ///   consume-once contract as [`RunningProcess`] (a
    ///   second take is a loud `Err`, never a silently-empty stream);
    /// - a readiness wait on that stream —
    ///   [`wait_for_line`](PipelineSession::wait_for_line);
    /// - a [`finish`](PipelineSession::finish) that folds the same **pipefail**
    ///   outcome as the buffering verbs: the culprit stage's outcome and *its own*
    ///   stderr, not just the last stage's;
    /// - whole-chain teardown — [`start_kill`](PipelineSession::start_kill) and
    ///   kill-on-drop — plus the chain-wide [`timeout`](Self::timeout) /
    ///   [`cancel_on`](Self::cancel_on) still bounding the live session.
    ///
    /// The chain comes up through the same launch core as the buffering verbs, so the
    /// stage-to-stage relay behaves identically here: a non-final stage's own stdout
    /// destination is overridden by the pipe it owes the next stage (see
    /// [`Pipeline::pipe`](Self::pipe)).
    ///
    /// # Errors
    ///
    /// A launch failure of any stage
    /// ([`ErrorReason::NotFound`](crate::ErrorReason::NotFound) /
    /// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) /
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported)) or
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) at start-up — every already-started
    /// stage is torn down before the error returns. A failing *stage* is not raised
    /// here; it surfaces at [`finish`](PipelineSession::finish).
    pub async fn start(&self) -> Result<PipelineSession> {
        let LaunchedChain {
            stage_groups,
            mut running,
            // A live session reports no duration (its `Finished` carries none — the
            // caller times the stream itself), so the launch anchor is unused here.
            started: _,
        } = self.launch().await?;

        // Split the last stage off as the live streaming surface; the caller drives
        // it. Carry its pipefail metadata so `finish` can fold it into position.
        // The infallible pop lives in `detach_last` (a `Pipeline` starts at and
        // never drops below two stages), so this public method carries no panic path.
        let DetachedLast {
            handle: last,
            program: last_program,
            ok_codes: last_ok_codes,
            timeout: last_timeout,
            unchecked: last_unchecked,
        } = self.detach_last(&mut running);

        let inner_count = running.len();

        // Proactive teardown token, exactly as in `capture`: an inner stage's first
        // checked failure (or a raw error) fires it, and the standing killer below
        // reacts by tearing the whole live chain down — so a quiet upstream can't
        // hold a failed streaming chain open.
        let teardown = tokio_util::sync::CancellationToken::new();
        let teardown_cause: PipelineTeardownCause = Arc::default();
        let terminal = PipelineTerminalState::new(inner_count + 1);

        // Background-drain every inner stage while the caller streams the last. Each
        // task reuses `finish_inner_stage` (the shared classify-and-teardown body)
        // and *additionally* fires teardown on a raw `Err`, standing in for the
        // central `drain_unordered` backstop the buffering path gets for free —
        // here no central drain runs while the caller streams, so each task must
        // self-report to keep the chain live behind a ready error.
        let mut inner_tasks: tokio::task::JoinSet<Result<(usize, StageOutcome)>> =
            tokio::task::JoinSet::new();
        for (index, ((process, unchecked), stage)) in
            running.into_iter().zip(self.stages.iter()).enumerate()
        {
            let program = process.program_name().to_owned();
            let ok_codes = stage.ok_codes_vec();
            let timeout = stage.configured_timeout();
            let teardown = teardown.clone();
            let teardown_cause = teardown_cause.clone();
            let terminal = terminal.clone();
            inner_tasks.spawn(async move {
                let result = finish_inner_stage(
                    process,
                    index,
                    program,
                    ok_codes,
                    timeout,
                    unchecked,
                    teardown.clone(),
                    teardown_cause.clone(),
                    terminal,
                )
                .await;
                if let Err(error) = &result {
                    trigger_pipeline_teardown(
                        &teardown,
                        &teardown_cause,
                        pipeline_error_teardown_cause(error),
                    );
                }
                result
            });
        }

        // Standing teardown killer: fans a hard kill across every stage's sub-group
        // the instant `teardown` fires, so a failing inner stage tears the *whole*
        // live chain down (last stage included) even while the caller is mid-stream.
        // `Weak` handles so it never pins a group past a session drop; aborted on
        // `finish`/drop.
        let teardown_failure: PipelineTeardownFailure = Arc::default();
        let killer = spawn_group_killer(
            teardown.clone(),
            &stage_groups,
            teardown_failure.clone(),
            teardown_cause.clone(),
            terminal.clone(),
        );

        // The last stage is the caller's to stream, so it gets no `inner_tasks`-style
        // drain that could fire `teardown` from its failure. Without an observer its
        // failure would only be classified at `finish` — leaving the upstream stages
        // (classically a quiet producer that never writes, so never dies of a broken
        // pipe against the closed stdin of the dead last stage) running for as long
        // as the caller holds the session unfinished. The standing watcher below
        // closes exactly that gap, sharing the handle with the caller rather than
        // taking it over.
        let last: SharedLast = Arc::new(std::sync::Mutex::new(Some(last)));
        let last_disposition = ExitDisposition::unobserved();
        let last_watch = spawn_last_stage_watcher(
            &last,
            last_ok_codes.clone(),
            last_unchecked,
            teardown.clone(),
            teardown_cause.clone(),
            last_disposition.clone(),
            terminal.clone(),
            inner_count,
        );

        // Chain-wide `Pipeline::timeout` on the live session: a background watchdog
        // reusing the shared deadline arbiter (no second implementation — K-034/K-007).
        // At the deadline it claims a fresh chain arbiter and hard-kills every
        // sub-group; `finish` reads the arbiter to report `Outcome::TimedOut`, exactly
        // as the buffering path's `tokio::time::timeout` branch does. Anchored to a
        // `tokio::time::Instant` (the deadline clock), captured now — after launch,
        // matching where the buffering `tokio::time::timeout(limit, collect)` begins.
        let chain_state = Arc::new(AtomicU8::new(crate::running::TS_PENDING));
        let deadline_task = self.timeout.map(|limit| {
            let state = chain_state.clone();
            let groups: Vec<Weak<ProcessGroup>> = stage_groups.iter().map(Arc::downgrade).collect();
            let teardown_failure = teardown_failure.clone();
            let teardown_cause = teardown_cause.clone();
            let terminal = terminal.clone();
            let anchor = tokio::time::Instant::now();
            tokio::spawn(async move {
                if crate::running::deadline::wait_deadline_and_claim(anchor, limit, &state).await {
                    let _ = teardown_cause.set(crate::TeardownCause::Timeout);
                    let cause = latched_pipeline_teardown_cause(&teardown_cause);
                    match kill_weak_stage_groups(&groups) {
                        Err(source) => {
                            record_pipeline_teardown_failure(&teardown_failure, cause, source);
                        }
                        Ok(()) => {
                            confirm_or_record_pipeline_terminal_failure(
                                &terminal,
                                &teardown_failure,
                                cause,
                            )
                            .await;
                        }
                    }
                }
            })
        });

        Ok(PipelineSession {
            last,
            last_program,
            last_ok_codes,
            last_unchecked,
            last_timeout,
            last_disposition,
            last_watch: Some(last_watch),
            inner_tasks: Some(inner_tasks),
            inner_count,
            stage_groups,
            teardown,
            teardown_cause,
            terminal,
            timeout: self.timeout,
            chain_state,
            deadline_task,
            killer: Some(killer),
            teardown_failure,
        })
    }

    /// Run the chain to completion and capture the outcome (stdout as text). A
    /// failing stage is **not** an `Err` here — it is reported in the result
    /// (pipefail attribution, see the type docs); `Err` means a stage could not
    /// be started or driven at all.
    /// A chain-wide timeout is likewise captured in the result and retains the
    /// best-effort stdout and stderr already read before teardown.
    ///
    /// # Errors
    ///
    /// A failing stage (and a timeout) is *captured* in the returned
    /// [`ProcessResult`] (pipefail attribution), not raised. `Err` means a stage
    /// could not be *started or driven*: a launch failure
    /// ([`ErrorReason::NotFound`](crate::ErrorReason::NotFound) /
    /// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) /
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported)),
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled),
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) when chain-wide or
    /// per-stage terminal teardown cannot be confirmed,
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud
    /// overflow of the last stage), [`ErrorReason::Stdin`](crate::ErrorReason::Stdin), or
    /// [`ErrorReason::Io`](crate::ErrorReason::Io).
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        self.capture(
            |last, at_exit| async move { last.output_string_observing_exit(at_exit).await },
        )
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
        self.capture(|last, at_exit| async move { last.output_bytes_observing_exit(at_exit).await })
            .await
    }

    /// Start and chain every stage, drain concurrently, and fold the pipefail
    /// outcome. `capture_last` decides how the last stage's stdout is captured; it
    /// is handed the [`LastExitObserver`] it must arm on that capture, so the last
    /// stage's disposition is latched at its exit like every other stage's.
    ///
    /// Only the **last** stage's stdout is captured here. Every earlier stage's goes
    /// to the next stage through [`launch`](Self::launch)'s relay — including a stage
    /// that configured `stdout(Null)`/`stdout(Inherit)`/`stdout_file(..)`, whose
    /// destination the relay overrides — so this path and the streaming
    /// [`start`](Self::start) see the same bytes arrive at the same place.
    async fn capture<T, C, F>(&self, capture_last: C) -> Result<ProcessResult<T>>
    where
        T: PipelineCapture,
        C: FnOnce(crate::running::RunningProcess, LastExitObserver) -> F,
        F: std::future::Future<Output = Result<ProcessResult<T>>> + Send + 'static,
    {
        // Launch the whole chain (shared with `start`'s streaming path): every
        // stage in its own kill-on-drop sub-group, stdout→stdin chained, strong
        // sub-group handles retained for the chain-wide teardown fan. `started` is
        // the wall-clock anchor from before the first spawn, so `duration()`
        // reflects the run, not just the last stage.
        let LaunchedChain {
            stage_groups,
            mut running,
            started,
        } = self.launch().await?;

        // Proactive teardown: the first stage to finish with a *checked failure*
        // fires this token; a concurrent killer then tears every stage's sub-group
        // down so a quiet, still-running sibling (classically an upstream producer
        // that never writes, so never dies of a broken pipe) cannot hold the chain
        // open after the failure. Stages ended by that kill are flagged `torn_down` and
        // de-prioritized in the pipefail fold, so the real culprit is still blamed.
        // This is distinct from the user's `cancel_token`: a cancelled stage errors
        // out (`Err(Cancelled)`) before producing a `StageOutcome`; the central raw
        // error path fires this token with `Cancellation` as its latched cause.
        let teardown = tokio_util::sync::CancellationToken::new();
        let teardown_cause: PipelineTeardownCause = Arc::default();
        let teardown_failure: PipelineTeardownFailure = Arc::default();

        // Drain concurrently: a stderr-chatty inner stage must not block on a full pipe.
        let (mut last, last_unchecked) = running.pop().expect("a pipeline has at least two stages");
        let last_stage = self
            .stages
            .last()
            .expect("a pipeline has at least two stages");
        let last_ok_codes = last_stage.ok_codes_vec();
        let last_timeout = last_stage.configured_timeout();
        // Prepare the last stage's capture before moving it into a task. The
        // tracker remains in this future's frame, so a chain-wide timeout can
        // salvage its retained prefix after the task is dropped.
        let capture = T::prepare(&mut last)?;
        let completed: Arc<std::sync::Mutex<Option<ProcessResult<T>>>> =
            Arc::new(std::sync::Mutex::new(None));
        // Drive every stage's task through one `JoinSet`, drained by
        // `drain_unordered` below: a stage's raw `Err` (`Cancelled` / `Teardown` /
        // `Stdin` / `Io` / `OutputTooLarge`) or a task panic never reaches the
        // `is_checked_failure` check, so it can't fire `teardown` itself the
        // way a checked failure does — `drain_unordered` fires it centrally
        // for *every* bad completion, in true completion order rather than
        // stage position, so a later stage's ready error can't sit behind an
        // earlier, still-quiet stage forever.
        let inner_count = running.len();
        let terminal = PipelineTerminalState::new(inner_count + 1);
        let mut tasks: tokio::task::JoinSet<Result<Joined<T>>> = tokio::task::JoinSet::new();
        for (index, ((process, unchecked), stage)) in
            running.into_iter().zip(self.stages.iter()).enumerate()
        {
            let program = process.program_name().to_owned();
            let ok_codes = stage.ok_codes_vec();
            let timeout = stage.configured_timeout();
            let teardown = teardown.clone();
            let teardown_cause = teardown_cause.clone();
            let terminal = terminal.clone();
            tasks.spawn(async move {
                // `finish_inner_stage` is the shared classify-and-teardown body,
                // reused by `start`'s streaming inner drains — so both paths blame
                // a stage and fire proactive teardown identically.
                let (index, outcome) = finish_inner_stage(
                    process,
                    index,
                    program,
                    ok_codes,
                    timeout,
                    unchecked,
                    teardown,
                    teardown_cause,
                    terminal,
                )
                .await?;
                Ok(Joined::Inner(index, outcome))
            });
        }
        // The last stage of a buffering capture is driven by its own drain, exactly
        // like an inner stage — so it gets the same latch, armed at its *exit*
        // through the capture's `at_exit` seam. Nothing else observes it here (this
        // path has no standing watcher), so the latch has one writer; it is here to
        // read the teardown at the right *instant*. Without it a last stage whose
        // stderr a forked grandchild holds open would be read as `torn_down` after
        // its drain — demoting the stage that died first to the victim of a teardown
        // that only fired afterwards. See [`ExitDisposition`].
        let last_disposition = ExitDisposition::unobserved();
        // Call `capture_last` here (not inside the spawned future): it yields a
        // `Send` future `F`, whereas the closure `C` itself is not `Send` and must
        // not be captured across the `tokio::spawn` boundary.
        let last_future = capture_last(last, {
            let disposition = last_disposition.clone();
            let teardown = teardown.clone();
            let terminal = terminal.clone();
            Box::new(move |observation| {
                observe_pipeline_stage_exit(
                    observation,
                    &terminal,
                    inner_count,
                    &disposition,
                    &teardown,
                );
            })
        });
        {
            let teardown = teardown.clone();
            let teardown_cause = teardown_cause.clone();
            let last_ok_codes = last_ok_codes.clone();
            let completed = completed.clone();
            let terminal = terminal.clone();
            tasks.spawn(async move {
                let result = match last_future.await {
                    Ok(result) => result,
                    Err(error) => {
                        if !terminal.stage_is_confirmed(inner_count) {
                            terminal.fail(pipeline_terminal_error(&error));
                        }
                        return Err(error);
                    }
                };
                *completed.lock().expect("pipeline capture result poisoned") = Some(result.clone());
                // The last stage triggers teardown too (a failing last stage should
                // not wait on a quiet upstream either); torn if a sibling's teardown
                // was already in flight when it *died* — the verdict the seam above
                // latched, not a post-drain read of the token.
                let torn_down = last_disposition.latch(teardown.is_cancelled());
                if !torn_down
                    && is_checked_failure(result.outcome(), &last_ok_codes, last_unchecked)
                {
                    trigger_pipeline_teardown(
                        &teardown,
                        &teardown_cause,
                        crate::TeardownCause::PipelineFailure,
                    );
                }
                Ok(Joined::Last(result, torn_down))
            });
        }

        let collect_failure = teardown_failure.clone();
        let collect_cause = teardown_cause.clone();
        let collect = async {
            // On a bad completion `drain_unordered` fires `teardown` itself; the
            // killer arm below wakes and tears every stage's sub-group down,
            // unblocking whichever task was stalled on a quiet sibling, so
            // `gather` (however long the drain takes to notice) still finishes
            // and wins the `select!` rather than hanging next to a pending kill.
            let gather = async {
                let drained = drain_unordered(tasks, &teardown, &teardown_cause).await;
                // A stage task can only complete after its process exit callback.
                // Keep that ownership relationship explicit during teardown so a
                // successful kill dispatch is never mistaken for a confirmed reap.
                if teardown.is_cancelled()
                    && !terminal.is_confirmed()
                    && let Err(failure) = terminal.wait_confirmed().await
                {
                    let cause = failure
                        .cause
                        .unwrap_or_else(|| latched_pipeline_teardown_cause(&collect_cause));
                    record_pipeline_failure(
                        &collect_failure,
                        cause,
                        "pipeline stage terminal confirmation",
                        failure.into_io(),
                    );
                    std::future::pending::<()>().await;
                }
                let joined = drained.map_err(|failure| failure.error)?;
                let mut inner_outcomes: Vec<Option<StageOutcome>> =
                    (0..inner_count).map(|_| None).collect();
                let mut last_slot: Option<(ProcessResult<T>, bool)> = None;
                for item in joined {
                    match item {
                        Joined::Inner(index, outcome) => inner_outcomes[index] = Some(outcome),
                        Joined::Last(result, torn_down) => last_slot = Some((result, torn_down)),
                    }
                }
                let outcomes: Vec<StageOutcome> = inner_outcomes
                    .into_iter()
                    .map(|outcome| {
                        outcome.expect("every inner stage slot is filled when every task succeeded")
                    })
                    .collect();
                let (last_result, last_torn_down) =
                    last_slot.expect("last slot is filled when every task succeeded");
                Ok::<_, crate::Error>((outcomes, last_result, last_torn_down))
            };
            tokio::select! {
                collected = gather => collected,
                // Give downstream filters one bounded window to consume the
                // culprit's final bytes and EOF before killing any stragglers.
                // `gather` still wins immediately when the chain drains naturally.
                () = async {
                    teardown.cancelled().await;
                    tokio::time::sleep(TEARDOWN_DRAIN_GRACE).await;
                    run_before_pipeline_fallback_kill();
                    let cause = latched_pipeline_teardown_cause(&collect_cause);
                    match kill_all_stage_groups(&stage_groups) {
                        Err(source) => {
                            record_pipeline_teardown_failure(
                                &collect_failure,
                                cause,
                                source,
                            );
                        }
                        Ok(()) => {
                            confirm_or_record_pipeline_terminal_failure(
                                &terminal,
                                &collect_failure,
                                cause,
                            )
                            .await;
                        }
                    }
                    std::future::pending::<()>().await
                } => unreachable!("the teardown killer pends forever after firing"),
            }
        };

        // A failed proactive group kill can leave every stage live, so `collect`
        // cannot be the only completion signal: it would wait forever for the
        // processes the failed kill did not terminate. The first-failure slot also
        // notifies this race; dropping `collect` drops its JoinSet, and the retained
        // capture tracker below still yields the prefix read before teardown.
        let collect_or_teardown_failure = async {
            tokio::select! {
                biased;
                () = wait_pipeline_teardown_failure(&teardown_failure) => None,
                collected = collect => Some(collected),
            }
        };
        tokio::pin!(collect_or_teardown_failure);
        let collected = match self.timeout {
            None => collect_or_teardown_failure.await,
            Some(limit) => {
                match tokio::time::timeout(limit, &mut collect_or_teardown_failure).await {
                    Ok(collected) => collected,
                    Err(_elapsed) => {
                        // Keep `collect` and its JoinSet alive across teardown. The
                        // stage-owned handles, not the successful fan-out syscall,
                        // confirm that every child reached a terminal disposition.
                        let _ = teardown_cause.set(crate::TeardownCause::Timeout);
                        let cause = latched_pipeline_teardown_cause(&teardown_cause);
                        // A stage may already be returning an unconfirmed wait
                        // while its pumps still retain the diagnostic prefix. Peek
                        // that prefix before the timeout's fallback kill wakes the
                        // stage finisher and lets it drain the same sinks. This is
                        // non-destructive: ordinary confirmed timeouts still use
                        // the existing post-reap snapshot below.
                        let mut pre_teardown_captured = Some(
                            completed
                                .lock()
                                .expect("pipeline capture result poisoned")
                                .as_ref()
                                .cloned()
                                .map(captured_result)
                                .unwrap_or_else(|| T::retained_snapshot(&capture)),
                        );
                        let prior_terminal_failure = terminal.confirmation_failure();
                        let teardown_result = kill_all_stage_groups(&stage_groups);
                        let terminal_failure = if let Some(failure) = prior_terminal_failure {
                            Some(failure)
                        } else if teardown_result.is_ok() {
                            confirm_pipeline_terminal_dispositions(&terminal)
                                .await
                                .err()
                        } else {
                            None
                        };
                        if let Some(failure) = terminal_failure {
                            let failure_cause = failure.cause.unwrap_or(cause);
                            let source = failure.into_io();
                            let captured = pre_teardown_captured
                                .take()
                                .expect("pre-teardown capture is available once");
                            return Err(crate::Error::teardown(
                                self.pipeline_name(),
                                failure_cause,
                                "pipeline stage terminal confirmation",
                                source,
                                T::to_diagnostic_text(&captured.stdout),
                                captured.stderr,
                                T::exact_bytes(&captured.stdout),
                            ));
                        }
                        if let Err(source) = teardown_result {
                            let captured = pre_teardown_captured
                                .take()
                                .expect("pre-teardown capture is available once");
                            return Err(crate::Error::teardown(
                                self.pipeline_name(),
                                cause,
                                "pipeline process-group hard kill",
                                source,
                                T::to_diagnostic_text(&captured.stdout),
                                captured.stderr,
                                T::exact_bytes(&captured.stdout),
                            ));
                        }

                        // Reaping is confirmed; now let the existing bounded pump
                        // salvage finish. This remains bounded by `join_pumps` and
                        // retains any prefix emitted before a survivor held a pipe.
                        let _ = (&mut collect_or_teardown_failure).await;
                        let captured = completed
                            .lock()
                            .expect("pipeline capture result poisoned")
                            .take()
                            .map(captured_result)
                            .unwrap_or_else(|| T::snapshot(&capture));
                        let Captured {
                            stdout,
                            stderr,
                            truncated,
                            total_lines,
                            total_bytes,
                        } = captured;
                        if let Some(failure) = take_pipeline_teardown_failure(&teardown_failure) {
                            return Err(crate::Error::teardown(
                                self.pipeline_name(),
                                failure.cause,
                                failure.operation,
                                failure.source,
                                T::to_diagnostic_text(&stdout),
                                stderr,
                                T::exact_bytes(&stdout),
                            ));
                        }
                        return Ok(ProcessResult::new(
                            self.pipeline_name(),
                            stdout,
                            stderr,
                            Outcome::TimedOut,
                            Some(limit),
                        )
                        .with_duration(started.elapsed())
                        .with_truncated(truncated)
                        .with_overflow_totals(total_lines, total_bytes));
                    }
                }
            }
        };

        if let Some(failure) = take_pipeline_teardown_failure(&teardown_failure) {
            let captured = completed
                .lock()
                .expect("pipeline capture result poisoned")
                .take()
                .map(captured_result)
                .unwrap_or_else(|| T::snapshot(&capture));
            return Err(crate::Error::teardown(
                self.pipeline_name(),
                failure.cause,
                failure.operation,
                failure.source,
                T::to_diagnostic_text(&captured.stdout),
                captured.stderr,
                T::exact_bytes(&captured.stdout),
            ));
        }
        let (mut stages, last_result, last_torn_down) =
            collected.expect("the notified pipeline teardown failure was consumed above")?;

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
    /// stage's [`ErrorReason::Exit`](crate::ErrorReason::Exit) (pipefail attribution;
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are exempt, so a chain whose
    /// only failures are unchecked returns `Ok`).
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) is produced by the whole-chain
    /// [`timeout`](Self::timeout) or by **any** stage's own
    /// [`Command::timeout`] — the attributed stage's *own* deadline is reported,
    /// not the chain's.
    ///
    /// # Errors
    ///
    /// The first failing stage's [`ErrorReason::Exit`](crate::ErrorReason::Exit) (pipefail
    /// attribution; [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are
    /// exempt), [`ErrorReason::Signalled`](crate::ErrorReason::Signalled),
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) (the whole-chain
    /// [`timeout`](Self::timeout) or any stage's own — the attributed stage's
    /// deadline is reported), [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled),
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) (terminal teardown
    /// could not be confirmed),
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud
    /// truncation of the last stage), plus any launch failure or
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin).
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
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit) /
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) /
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) /
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) /
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown), plus launch failures and
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) — but, as the lenient building block,
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
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout), a signal-kill as
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) — mirroring
    /// [`Command::exit_code`](crate::Command::exit_code).
    ///
    /// # Errors
    ///
    /// A chain that produced no code errors as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) (whole-chain or stage),
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled), or
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled), or
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown), atop any launch failure. A
    /// non-zero pipefail code is returned, not raised.
    pub async fn exit_code(&self) -> Result<i32> {
        self.output_string().await?.require_code()
    }

    /// Read the chain's pipefail-attributed exit code as a boolean: `0` →
    /// `Ok(true)`, `1` → `Ok(false)`, anything else → `Err` (other code as
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit), a timeout as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout), a signal-kill as
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled)). For a chain whose final
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
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit); a chain with no code errors as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout),
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled), or
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled), or
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown), atop any launch failure.
    pub async fn probe(&self) -> Result<bool> {
        let result = self.output_string().await?;
        result.probe_bool()
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
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit) /
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) /
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) /
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) /
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) /
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin)), plus
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) when a fail-loud
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
    /// shape; a failure becomes [`ErrorReason::Parse`](crate::ErrorReason::Parse) or whatever
    /// the closure returns). Fails loud on truncation. Mirrors
    /// [`Command::try_parse`](crate::Command::try_parse).
    ///
    /// # Errors
    ///
    /// Everything [`parse`](Self::parse) can return, plus whatever the fallible
    /// `parse` closure yields on malformed output — typically
    /// [`ErrorReason::Parse`](crate::ErrorReason::Parse).
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

/// A **live streaming session** over a running [`Pipeline`] — the multi-stage
/// analogue of a [`RunningProcess`], returned by
/// [`Pipeline::start`]. It streams the **last** stage's stdout as it arrives while
/// every inner stage drains in the background, then folds the same **pipefail**
/// outcome as the buffering verbs at [`finish`](Self::finish).
///
/// Drive it like a `RunningProcess`:
///
/// - [`stdout_lines`](Self::stdout_lines) / [`events`](Self::events)
///   — the last stage's stdout, line by line or as interleaved events, with the
///   same **consume-once** contract (a second take is a loud `Err`).
/// - [`wait_for_line`](Self::wait_for_line) — wait for a readiness banner on that
///   stream without tearing the chain down.
/// - [`finish`](Self::finish) — fold the pipefail outcome (the culprit stage's
///   outcome and *its own* stderr, not just the last stage's) into a
///   [`Finished`].
/// - [`start_kill`](Self::start_kill) — stop the whole chain now.
///
/// **Teardown is whole-chain.** A stage's checked failure proactively tears every
/// stage's sub-group down after a short bounded drain grace (so downstream can
/// consume final pipe bytes, while a quiet upstream still cannot hold a failed
/// live chain open), the chain-wide [`Pipeline::timeout`] / [`Pipeline::cancel_on`] still
/// bound the session, and **dropping** the session hard-kills every stage's tree —
/// the crate's no-orphan invariant holds for a live chain exactly as it does for a
/// single [`RunningProcess`]. A partially-started chain (one
/// stage up, the next failing to spawn) is torn down before [`start`](Pipeline::start)
/// even returns its error.
///
/// That applies to the **last** stage's own checked failure too, and without
/// waiting for [`finish`](Self::finish): a standing watcher re-probes the last
/// stage for a terminal outcome on a bounded, backing-off cadence and fires the
/// same teardown, so an unfinished session is not a way for a failed chain's
/// upstream to keep running. The teardown is therefore prompt but not
/// instantaneous — it lands within one probe interval of the failure plus the
/// drain grace. The watcher observes the last stage's *process outcome* and the
/// cancellation disposition latched with that exit; other non-outcome failures
/// ([`ErrorReason::Stdin`](crate::ErrorReason::Stdin),
/// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge), …) still
/// surface at `finish`. A last stage that exits cleanly without cancellation
/// deliberately fires no teardown — a chain whose stages all succeed is not a
/// failed chain, and `finish` still waits for the rest of it.
#[must_use = "a PipelineSession streams a live chain; drop it and the whole chain is killed unread"]
pub struct PipelineSession {
    /// The last stage's live handle — the streaming surface the caller drives —
    /// shared with the standing last-stage watcher (see
    /// [`spawn_last_stage_watcher`]). `Option` so [`finish`](Self::finish) can move
    /// it out without a partial move (the session has a `Drop`) and so an `async`
    /// session call can lend it out ([`LastBorrow`]); `None` only while borrowed,
    /// or for good once `finish` consumed it. The watcher observes it through a
    /// [`Weak`], so dropping the session drops the handle — and fires its
    /// kill-on-drop — without waiting for that task to be reclaimed.
    last: SharedLast,
    /// The last stage's pipefail metadata, kept so `finish` can fold it into place.
    last_program: String,
    last_ok_codes: Vec<i32>,
    last_unchecked: bool,
    last_timeout: Option<Duration>,
    /// The last stage's culprit-vs-victim disposition, latched by whichever of the
    /// watcher and `finish` observes its exit first. Without it `finish` would read
    /// a teardown the *last stage's own* failure fired as a sibling's and demote the
    /// culprit to a victim — see [`ExitDisposition`] and `finish`.
    last_disposition: ExitDisposition,
    /// The standing last-stage exit watcher, aborted on `finish`/drop.
    last_watch: Option<JoinHandle<()>>,
    /// Background drains of every inner (non-last) stage — each an index-tagged
    /// [`StageOutcome`] once its stage exits, sorted back into left-to-right order
    /// by `finish`. `Option`, taken by `finish`.
    inner_tasks: Option<tokio::task::JoinSet<Result<(usize, StageOutcome)>>>,
    /// How many inner stages there are — the `Debug` summary's stage count.
    inner_count: usize,
    /// Strong handles to every stage's sub-group, for [`start_kill`](Self::start_kill)
    /// and the kill-on-drop backstop.
    stage_groups: Vec<Arc<ProcessGroup>>,
    /// Proactive teardown token, fired by an inner stage's checked/raw failure.
    teardown: tokio_util::sync::CancellationToken,
    /// Cause paired with the first proactive trigger, published before `teardown`.
    teardown_cause: PipelineTeardownCause,
    /// Per-stage reap observations retained across every fan-out kill.
    terminal: PipelineTerminalConfirmation,
    /// The chain-wide [`Pipeline::timeout`], if any (gates the arbiter read).
    timeout: Option<Duration>,
    /// The chain-wide deadline arbiter (reusing the shared `running::deadline` CAS
    /// protocol): the watchdog claims `TimedOut`, `finish` claims `Exited`.
    chain_state: Arc<AtomicU8>,
    /// The chain-wide timeout watchdog, aborted on `finish`/drop.
    deadline_task: Option<JoinHandle<()>>,
    /// The standing teardown killer, aborted on `finish`/drop.
    killer: Option<JoinHandle<()>>,
    /// First chain-wide group-kill failure from the deadline or proactive killer.
    teardown_failure: PipelineTeardownFailure,
}

impl std::fmt::Debug for PipelineSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineSession")
            .field("last", &self.last_program)
            .field("inner_stages", &self.inner_count)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl PipelineSession {
    /// Stream the **last** stage's standard output line by line, as the chain
    /// produces it — the multi-stage analogue of
    /// [`RunningProcess::stdout_lines`](crate::RunningProcess::stdout_lines). Call
    /// this **once**. Every inner stage's stdout feeds the next stage's stdin (and
    /// its stderr is drained in the background for pipefail diagnostics); only the
    /// last stage's stdout reaches you.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when the last stage's stdout was not piped,
    /// or a prior readiness/streaming call already started its one line pump —
    /// returned instead of a silently-empty stream.
    pub fn stdout_lines(&mut self) -> Result<StdoutLines> {
        self.with_last(RunningProcess::stdout_lines)
    }

    /// Stream the **last** stage's full lifecycle as one ordered sequence of
    /// [`ProcessEvent`](crate::ProcessEvent)s — the multi-stage analogue of
    /// [`RunningProcess::events`](crate::RunningProcess::events). Call this
    /// **once**. Note this surfaces only the *last* stage's stderr as events;
    /// inner stages' stderr still folds into the pipefail diagnostics at
    /// [`finish`](Self::finish). Like the single-command verb, the terminal
    /// [`Exited`](crate::ProcessEvent::Exited) is delivered by the reaping
    /// [`finish`](Self::finish), so drive this stream and `finish` together.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when the last stage's stdout was not piped,
    /// or a prior readiness/streaming call already started its line pump.
    pub fn events(&mut self) -> Result<ProcessEvents> {
        self.with_last(RunningProcess::events)
    }

    /// Wait until a line on the **last** stage's stdout matches `predicate`
    /// (returning that line), or fail with [`ErrorReason::NotReady`](crate::ErrorReason::NotReady)
    /// when `within` elapses — the multi-stage analogue of
    /// [`RunningProcess::wait_for_line`](crate::RunningProcess::wait_for_line). Like
    /// there, a failed probe does **not** kill the chain and does not arm the
    /// chain-wide timeout; continue with [`finish`](Self::finish) for the outcome.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::NotReady`](crate::ErrorReason::NotReady) when `within` elapses with no
    /// matching line (or the last stage's stdout closes first), and
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when that stdout was not piped or was already
    /// consumed — the same surface as the single-process probe.
    pub async fn wait_for_line(
        &mut self,
        predicate: impl Fn(&str) -> bool + Send,
        within: Duration,
    ) -> Result<String> {
        // Borrowed out of the shared slot for the call: holding the slot's guard
        // across this `.await` would make the returned future `!Send`.
        let mut last = self.borrow_last();
        last.get().wait_for_line(predicate, within).await
    }

    /// The OS process id of the **last** stage, or `None` once it has been reaped —
    /// the stage whose stdout you stream. The inner stages' pids are not surfaced;
    /// the chain is driven and torn down as a unit.
    ///
    /// The reap that clears it is not only [`finish`](Self::finish)'s: a readiness
    /// probe ([`wait_for_line`](Self::wait_for_line)) and the session's own
    /// last-stage watcher both reap an exited child where they find one, so a live
    /// session reports `None` here shortly after the last stage exits. Treat it as
    /// "the last stage's pid while it runs", not as a session-lifetime handle.
    pub fn pid(&self) -> Option<u32> {
        lock_last(&self.last).as_ref().and_then(RunningProcess::pid)
    }

    /// Stop the **whole chain** now: fan a hard kill across every stage's sub-group.
    /// Idempotent; every stage is attempted even when one group rejects its kill.
    /// [`RunningProcess::start_kill`](crate::RunningProcess::start_kill) at chain
    /// scope. After it, the last stage's stream ends and [`finish`](Self::finish)
    /// reports the killed outcome via the usual pipefail fold.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) when a stage group
    /// rejects the kill. The source error is retained and the session remains
    /// finishable; kill-on-drop still backstops every group.
    pub fn start_kill(&mut self) -> Result<()> {
        match kill_all_stage_groups(&self.stage_groups) {
            Ok(()) => Ok(()),
            Err(source) => {
                record_pipeline_teardown_failure(
                    &self.teardown_failure,
                    crate::TeardownCause::ExplicitKill,
                    PipelineTerminalError::from_io(&source, None).into_io(),
                );
                Err(crate::Error::teardown(
                    self.last_program.clone(),
                    crate::TeardownCause::ExplicitKill,
                    "pipeline process-group hard kill",
                    source,
                    String::new(),
                    String::new(),
                    None,
                ))
            }
        }
    }

    /// Finish the live chain and fold the **pipefail** outcome, the streaming
    /// analogue of [`Pipeline::output_string`](Pipeline::output_string) /
    /// [`RunningProcess::finish`](crate::RunningProcess::finish). The last stage's
    /// stdout was already streamed to you, so — like [`Finished`] — none is
    /// re-bundled here; what you get back is *how the chain ended*.
    ///
    /// The returned [`Finished`] carries the **pipefail-attributed** stage's
    /// [`outcome`](Finished::outcome) and *its own* [`stderr`](Finished::stderr):
    /// the culprit is chosen by the same rule as the buffering verbs — the leftmost
    /// checked failure, preferring a genuine failure over a SIGPIPE/teardown victim,
    /// or the last stage when every stage exited cleanly. A chain-wide
    /// [`Pipeline::timeout`] that elapsed reports [`Outcome::TimedOut`]
    /// regardless of how the individual stages were killed, exactly as the buffering
    /// path's whole-chain timeout does.
    ///
    /// # Errors
    ///
    /// A failing stage (and a chain timeout) is *captured* in the returned
    /// [`Finished`]'s [`outcome`](Finished::outcome), not raised. `Err` mirrors the
    /// buffering verbs' non-outcome failures:
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) (the chain-wide
    /// [`cancel_on`](Pipeline::cancel_on) token fired, or a stage carried one),
    /// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) (a chain-wide or
    /// per-stage terminal kill could not be confirmed; this outranks the initiating
    /// timeout/cancellation/pipeline failure),
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud buffer
    /// overflowed on a stage), [`ErrorReason::Stdin`](crate::ErrorReason::Stdin), or
    /// [`ErrorReason::Io`](crate::ErrorReason::Io).
    ///
    /// As for a single [`Command::cancel_on`](crate::Command::cancel_on), each stage's
    /// cancellation is decided by whoever first *observes* that stage's exit: a token
    /// fired after a stage had already ended does not turn that stage's real outcome
    /// into [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled). Both ends of the
    /// chain are observed while it is live — every inner stage by its background drain,
    /// the last stage by the session's own watcher — so a chain that ran to completion
    /// before the token fired reports how it actually ended, even when `finish` is
    /// called afterwards.
    pub async fn finish(mut self) -> Result<Finished> {
        // The consume-once `take`s live in `take_live_parts` (this method takes
        // `self` by value, so they are always `Some` here), keeping the panic path
        // out of the public `finish`.
        let (last, inner_tasks) = self.take_live_parts();
        let teardown = self.teardown.clone();
        let teardown_cause = self.teardown_cause.clone();
        let terminal = self.terminal.clone();
        let last_ok_codes = self.last_ok_codes.clone();
        let last_unchecked = self.last_unchecked;
        let last_index = self.inner_count;

        // Drive the last stage to its `Finished` and drain the inner stages
        // concurrently, with the standing killer + chain-deadline watchdog still
        // armed. A failing/erroring last stage fires `teardown` too (mirroring the
        // buffering path's failing-last-stage teardown), so a quiet upstream can't
        // wedge the finalize; the killer then tears the chain down.
        let last_fut = {
            let teardown = teardown.clone();
            let teardown_cause = teardown_cause.clone();
            let disposition = self.last_disposition.clone();
            let terminal = terminal.clone();
            async move {
                // Same shape as `finish_inner_stage`: latch `torn_down` at the
                // stage's *exit* — a teardown already in flight then makes it a
                // victim; a last stage that fails on its own stays the (non-torn)
                // culprit even though its own failure fires the teardown.
                //
                // The standing watcher may have observed that same exit first and
                // latched it already; the read below then returns *its* verdict.
                // That is the whole point of watching the stage — to reach the
                // identical attribution sooner, not a different one — and it is what
                // keeps the last stage from reading the teardown its own failure
                // fired as evidence that a sibling killed it.
                let result = last
                    .finish_observing_exit({
                        let disposition = disposition.clone();
                        let teardown = teardown.clone();
                        let terminal = terminal.clone();
                        move |observation| {
                            observe_pipeline_stage_exit(
                                observation,
                                &terminal,
                                last_index,
                                &disposition,
                                &teardown,
                            );
                        }
                    })
                    .await;
                if let Err(error) = &result
                    && !terminal.stage_is_confirmed(last_index)
                {
                    terminal.fail(pipeline_terminal_error(error));
                }
                let torn_down = disposition.latch(teardown.is_cancelled());
                match &result {
                    Ok(finished) => {
                        if !torn_down
                            && is_checked_failure(finished.outcome, &last_ok_codes, last_unchecked)
                        {
                            trigger_pipeline_teardown(
                                &teardown,
                                &teardown_cause,
                                crate::TeardownCause::PipelineFailure,
                            );
                        }
                    }
                    Err(error) if !torn_down => trigger_pipeline_teardown(
                        &teardown,
                        &teardown_cause,
                        pipeline_error_teardown_cause(error),
                    ),
                    Err(_) => {}
                }
                (result, torn_down)
            }
        };
        // A failed background or explicit group kill may leave a stage live. Keep
        // owning every wait for one bounded confirmation window so siblings can
        // still publish their diagnostics, then let dropping the pending futures
        // and session run the existing kill-on-drop backstops.
        let mut inner_tasks = inner_tasks;
        let mut inner_completed = Vec::with_capacity(inner_tasks.len());
        let mut inner_error = None;
        let mut last_result = None;
        let mut failure_observed = false;
        let mut failure_deadline = None;
        tokio::pin!(last_fut);
        let fully_settled = loop {
            if last_result.is_some() && inner_tasks.is_empty() {
                break true;
            }
            tokio::select! {
                biased;
                () = wait_pipeline_teardown_failure(&self.teardown_failure), if !failure_observed => {
                    failure_observed = true;
                    failure_deadline = Some(
                        tokio::time::Instant::now() + pipeline_terminal_confirmation_budget()
                    );
                }
                result = &mut last_fut, if last_result.is_none() => {
                    last_result = Some(result);
                }
                joined = inner_tasks.join_next(), if !inner_tasks.is_empty() => {
                    if let Some(joined) = joined {
                        collect_drain_completion(
                            joined,
                            &mut inner_completed,
                            &mut inner_error,
                            &teardown,
                            &teardown_cause,
                        );
                    }
                }
                () = wait_pipeline_failure_deadline(failure_deadline), if failure_observed => {
                    break false;
                }
            }
        };

        if fully_settled
            && (teardown.is_cancelled()
                || self.chain_state.load(Ordering::Acquire) == crate::running::TS_TIMED_OUT)
            && !terminal.is_confirmed()
            && !pipeline_teardown_failure_is_recorded(&self.teardown_failure)
        {
            let cause = latched_pipeline_teardown_cause(&teardown_cause);
            confirm_or_record_pipeline_terminal_failure(&terminal, &self.teardown_failure, cause)
                .await;
        }

        // Either every side settled or the retained teardown failure's bounded
        // diagnostic window elapsed — stop the background watchdogs.
        self.abort_background();

        let chain_timed_out = self.chain_timed_out();
        let inner_res = finish_partial_drain(inner_completed, inner_error);
        if let Some(failure) = take_pipeline_teardown_failure(&self.teardown_failure) {
            let stderr = partial_pipeline_finished(
                inner_res,
                last_result,
                &self.last_program,
                &self.last_ok_codes,
                self.last_unchecked,
                self.last_timeout,
            )
            .map(|finished| finished.stderr)
            .unwrap_or_default();
            return Err(crate::Error::teardown(
                self.last_program.clone(),
                failure.cause,
                failure.operation,
                failure.source,
                String::new(),
                stderr,
                None,
            ));
        }
        if !fully_settled {
            return Err(crate::Error::io(std::io::Error::other(
                "pipeline teardown notification had no retained failure",
            )));
        }
        let (last_res, last_torn_down) = last_result.ok_or_else(|| {
            crate::Error::io(std::io::Error::other(
                "a fully settled pipeline had no last-stage result",
            ))
        })?;

        // Chain-wide timeout wins over the stages' own (hard-killed) outcomes, just
        // like the buffering path's `tokio::time::timeout` branch. Claiming the
        // arbiter is race-free against the watchdog: whichever of `Exited`/`TimedOut`
        // CASes from `PENDING` first decides.
        if chain_timed_out {
            return Ok(timeout_finished(
                inner_res,
                last_res,
                &self.last_program,
                &self.last_ok_codes,
                self.last_unchecked,
                self.last_timeout,
                last_torn_down,
            ));
        }

        // Surface a raw `Err` from either side (Cancelled / Teardown /
        // OutputTooLarge / Stdin / Io). Both sides have already reached a terminal
        // disposition, so inspect them together: an inner-stage Teardown must not
        // be hidden by the last stage winning this lexical `?` with Cancelled.
        let (last_finished, mut inner) = match (last_res, inner_res) {
            (Ok(last), Ok(inner)) => (last, inner),
            (Err(last), Err(inner)) => {
                return Err(prefer_pipeline_terminal_error(last, inner.error));
            }
            (Err(error), Ok(_)) => return Err(error),
            (Ok(_), Err(failure)) => return Err(failure.error),
        };

        // Rebuild the inner stages in left-to-right order (a clean drain returns all
        // `inner_count` items, each tagged with its unique launch index), then append
        // the last. Sorting by index needs no `expect` for a missing slot.
        inner.sort_by_key(|(index, _)| *index);
        let mut stages: Vec<StageOutcome> = inner.into_iter().map(|(_, outcome)| outcome).collect();
        stages.push(StageOutcome {
            program: self.last_program.clone(),
            outcome: last_finished.outcome,
            stderr: last_finished.stderr,
            unchecked: self.last_unchecked,
            ok_codes: self.last_ok_codes.clone(),
            timeout: self.last_timeout,
            torn_down: last_torn_down,
            stderr_truncated: last_finished.stderr_truncated,
        });

        // Reuse the exact pipefail attribution. The last stage's real stdout was
        // already streamed to the caller, so fold with a unit payload and surface
        // only the attributed stage's outcome + stderr as a `Finished`.
        let folded = pipefail(stages, ());
        Ok(Finished {
            outcome: folded.outcome(),
            stderr: folded.stderr().to_owned(),
            stderr_truncated: folded.truncated(),
        })
    }

    /// Run `f` against the last stage's handle under the shared slot's lock — the
    /// synchronous counterpart of [`borrow_last`](Self::borrow_last), for the session
    /// methods that need `&mut RunningProcess` for the length of one non-`async`
    /// call. The lock is never held across an `.await` here or in the watcher, so
    /// the only thing it can ever wait on is one non-blocking exit probe.
    fn with_last<R>(&mut self, f: impl FnOnce(&mut RunningProcess) -> R) -> R {
        let mut slot = lock_last(&self.last);
        f(slot
            .as_mut()
            .expect("the last stage is live until finish consumes the session"))
    }

    /// Lend the last stage's handle out of the shared slot for the length of one
    /// `async` session call — see [`LastBorrow`]. `&mut self` is what makes a second
    /// concurrent borrow impossible, so the only observer that can find the slot
    /// empty is the watcher, which simply skips that probe round.
    fn borrow_last(&mut self) -> LastBorrow<'_> {
        let handle = lock_last(&self.last)
            .take()
            .expect("the last stage is live until finish consumes the session");
        LastBorrow {
            slot: &self.last,
            handle: Some(handle),
        }
    }

    /// Take the last stage handle and the inner-stage drains out of the session for
    /// [`finish`](Self::finish). Private so its consume-once `expect`s (both fields
    /// are always `Some` until `finish` — which owns `self` — runs) stay out of the
    /// public API's panic-doc surface.
    #[allow(clippy::type_complexity)]
    fn take_live_parts(
        &mut self,
    ) -> (
        RunningProcess,
        tokio::task::JoinSet<Result<(usize, StageOutcome)>>,
    ) {
        // `finish` takes the last stage's classification over from here, so stand the
        // watcher down before the handle leaves the shared slot: from this point on
        // exactly one observer decides whether the last stage fires `teardown`.
        if let Some(task) = self.last_watch.take() {
            task.abort();
        }
        let last = lock_last(&self.last)
            .take()
            .expect("finish consumes the session exactly once");
        let inner_tasks = self
            .inner_tasks
            .take()
            .expect("finish consumes the session exactly once");
        (last, inner_tasks)
    }

    /// Whether the chain-wide [`Pipeline::timeout`] elapsed: claim the arbiter for a
    /// natural finish (`Exited`); if a fired deadline already claimed `TimedOut`,
    /// report the timeout. Reuses the shared `running::deadline` claim protocol.
    fn chain_timed_out(&self) -> bool {
        self.timeout.is_some()
            && !crate::running::deadline::claim_exited(&self.chain_state)
            && self.chain_state.load(Ordering::Acquire) == crate::running::TS_TIMED_OUT
    }

    /// Abort the standing background tasks (deadline watchdog, teardown killer, and
    /// the last-stage exit watcher). Idempotent — `finish` calls it once the chain
    /// has settled (having already stood the watcher down in `take_live_parts`), and
    /// `Drop` calls it for the drop-without-finish path.
    fn abort_background(&mut self) {
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.killer.take() {
            task.abort();
        }
        if let Some(task) = self.last_watch.take() {
            task.abort();
        }
    }
}

impl Drop for PipelineSession {
    fn drop(&mut self) {
        // Abort the detached watchdogs so a session dropped unfinished leaves no
        // parked killer/deadline/watcher task behind. The chain itself is torn down
        // by kill-on-drop as the fields fall: the last stage's `RunningProcess::drop`
        // kills its own tree (the watcher holds only a `Weak` on the shared slot,
        // upgraded for one non-blocking probe at a time, so an aborted task parked at
        // its sleep cannot defer that drop), every inner stage's handle (moved into
        // the aborted `inner_tasks`) does the same as the `JoinSet` drops, and the
        // retained `stage_groups` are the kill-on-drop backstop for any straggler.
        self.abort_background();
    }
}

/// The last stage's live handle as [`Pipeline::start`] shares it: held by the
/// [`PipelineSession`], lent to the caller's own session calls, and observed by the
/// standing last-stage watcher through a [`Weak`] it upgrades only for the length
/// of one non-blocking probe — never across an `.await` — so the session's is the
/// only strong reference that outlives a scheduling point. `Option` because an
/// `async` session call moves the handle out for its duration ([`LastBorrow`]) and
/// `finish` moves it out for good.
type SharedLast = Arc<std::sync::Mutex<Option<RunningProcess>>>;

/// Lock the shared last-stage slot, recovering the guard from a poisoned mutex
/// rather than panicking. The only work done under this lock is a `take`/put-back
/// or one non-blocking exit probe, so poisoning cannot happen in practice;
/// recovering keeps a hypothetical panic in the detached watcher from turning
/// every session method into a panic (the same reasoning as `PidGate::lock`).
fn lock_last(slot: &std::sync::Mutex<Option<RunningProcess>>) -> LastGuard<'_> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

type LastGuard<'a> = std::sync::MutexGuard<'a, Option<RunningProcess>>;

/// A scoped move-out borrow of the last stage's handle, for an `async`
/// [`PipelineSession`] call that must hold `&mut RunningProcess` across an
/// `.await` ([`wait_for_line`](PipelineSession::wait_for_line)). Holding the
/// slot's `MutexGuard` across that await would make the returned future `!Send`
/// — a public regression — so the handle is moved *out* of the slot instead and
/// put back by `Drop`, including when the future itself is dropped mid-await, so
/// a cancelled readiness probe can never lose the chain's last stage.
struct LastBorrow<'a> {
    slot: &'a std::sync::Mutex<Option<RunningProcess>>,
    handle: Option<RunningProcess>,
}

impl LastBorrow<'_> {
    fn get(&mut self) -> &mut RunningProcess {
        self.handle
            .as_mut()
            .expect("a borrowed last stage is put back only by Drop")
    }
}

impl Drop for LastBorrow<'_> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            *lock_last(self.slot) = Some(handle);
        }
    }
}

/// Fan a hard kill across every stage's sub-group — the chain-wide backstop when
/// a [`Pipeline::timeout`] elapses or a stage's failure triggers proactive
/// teardown, now that each stage owns its own kill-on-drop group rather than
/// sharing one. Every group is still attempted; the first non-routine error is
/// returned so a caller never reports a confirmed chain teardown over a survivor.
///
/// Called synchronously from a spawned async task (both the timeout branch and
/// the cancellation killer above), so on a cgroup backend it is one of the four
/// async call sites of the accepted bounded blocking sweep (~100 ms) documented on
/// `Cgroup::kill` (`src/sys/linux.rs`) — see that comment for the pre-5.14/
/// restricted-cgroup tradeoff. That ~100 ms is the ceiling for every backend:
/// FreeBSD's reaper keeps its post-kill corpse drain in `Drop` alone (see
/// `DRAIN_BUDGET`, `src/sys/freebsd.rs`), so this path does not block there.
#[derive(Debug)]
struct PipelineKillFailure {
    cause: crate::TeardownCause,
    operation: &'static str,
    source: std::io::Error,
}

#[derive(Debug, Default)]
struct PipelineTeardownState {
    first: std::sync::Mutex<Option<PipelineKillFailure>>,
    changed: tokio::sync::Notify,
}

type PipelineTeardownFailure = Arc<PipelineTeardownState>;

/// Reap confirmation for every stage whose group participated in a pipeline
/// fan-out kill. Dispatching a kill only proves that the request reached the
/// backend; the stage-owned `RunningProcess` remains the authority for whether
/// its child actually reached a terminal disposition.
#[derive(Debug)]
struct PipelineTerminalState {
    observed: Box<[AtomicBool]>,
    remaining: AtomicUsize,
    failure: std::sync::Mutex<Option<PipelineTerminalError>>,
    changed: tokio::sync::Notify,
}

#[derive(Clone, Debug)]
struct PipelineTerminalError {
    cause: Option<crate::TeardownCause>,
    raw_os_error: Option<i32>,
    kind: std::io::ErrorKind,
    message: String,
}

impl PipelineTerminalError {
    fn from_io(source: &std::io::Error, cause: Option<crate::TeardownCause>) -> Self {
        Self {
            cause,
            raw_os_error: source.raw_os_error(),
            kind: source.kind(),
            message: source.to_string(),
        }
    }

    fn into_io(self) -> std::io::Error {
        self.raw_os_error.map_or_else(
            || std::io::Error::new(self.kind, self.message),
            std::io::Error::from_raw_os_error,
        )
    }
}

type PipelineTerminalConfirmation = Arc<PipelineTerminalState>;

impl PipelineTerminalState {
    fn new(stage_count: usize) -> PipelineTerminalConfirmation {
        Arc::new(Self {
            observed: (0..stage_count).map(|_| AtomicBool::new(false)).collect(),
            remaining: AtomicUsize::new(stage_count),
            failure: std::sync::Mutex::new(None),
            changed: tokio::sync::Notify::new(),
        })
    }

    fn confirm(&self, index: usize) {
        if suppress_terminal_confirmation_for_test() {
            return;
        }
        if self.observed[index]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && self.remaining.fetch_sub(1, Ordering::AcqRel) == 1
        {
            self.changed.notify_waiters();
        }
    }

    fn is_confirmed(&self) -> bool {
        self.remaining.load(Ordering::Acquire) == 0
    }

    fn stage_is_confirmed(&self, index: usize) -> bool {
        self.observed[index].load(Ordering::Acquire)
    }

    fn fail(&self, source: PipelineTerminalError) {
        let mut guard = self
            .failure
            .lock()
            .expect("pipeline terminal confirmation failure poisoned");
        if guard.is_none() {
            *guard = Some(source);
            drop(guard);
            self.changed.notify_waiters();
        }
    }

    fn confirmation_failure(&self) -> Option<PipelineTerminalError> {
        self.failure
            .lock()
            .expect("pipeline terminal confirmation failure poisoned")
            .clone()
    }

    async fn wait_confirmed(&self) -> std::result::Result<(), PipelineTerminalError> {
        loop {
            // Enable before checking the counter so a final confirmation between
            // the check and await cannot be lost (`notify_waiters` has no permit).
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(source) = self.confirmation_failure() {
                return Err(source);
            }
            if self.is_confirmed() {
                return Ok(());
            }
            changed.await;
        }
    }
}

fn pipeline_terminal_error(error: &crate::Error) -> PipelineTerminalError {
    match error.reason() {
        crate::ErrorReason::Teardown { cause, source, .. } => {
            PipelineTerminalError::from_io(source, Some(*cause))
        }
        crate::ErrorReason::Io(source) | crate::ErrorReason::Spawn { source, .. } => {
            PipelineTerminalError::from_io(source, None)
        }
        crate::ErrorReason::Cancelled { .. } => PipelineTerminalError::from_io(
            &std::io::Error::other(error.to_string()),
            Some(crate::TeardownCause::Cancellation),
        ),
        _ => PipelineTerminalError::from_io(&std::io::Error::other(error.to_string()), None),
    }
}

fn observe_pipeline_stage_exit(
    observation: ExitObservation,
    terminal: &PipelineTerminalConfirmation,
    index: usize,
    disposition: &ExitDisposition,
    teardown: &tokio_util::sync::CancellationToken,
) {
    // Causal attribution happens at every completed wait boundary, but terminal
    // confirmation is reserved for a proven reap. This ordering also ensures a
    // concurrent chain timeout observes the real stage failure before it can
    // classify successful fan-out dispatch as an ordinary timeout.
    disposition.latch(teardown.is_cancelled());
    match observation {
        ExitObservation::Reaped => terminal.confirm(index),
        ExitObservation::Unconfirmed { cause, source } => {
            terminal.fail(PipelineTerminalError::from_io(&source, cause));
        }
    }
}

/// The condition that first fired a pipeline's proactive teardown. It is
/// published before the cancellation token so every fallback observer sees the
/// initiating cause, never a later token's ambient state.
type PipelineTeardownCause = Arc<std::sync::OnceLock<crate::TeardownCause>>;

fn trigger_pipeline_teardown(
    teardown: &tokio_util::sync::CancellationToken,
    cause_slot: &PipelineTeardownCause,
    cause: crate::TeardownCause,
) {
    let _ = cause_slot.set(cause);
    teardown.cancel();
}

fn pipeline_error_teardown_cause(error: &crate::Error) -> crate::TeardownCause {
    match error.reason() {
        crate::ErrorReason::Cancelled { .. } => crate::TeardownCause::Cancellation,
        crate::ErrorReason::Teardown { cause, .. } => *cause,
        _ => crate::TeardownCause::PipelineFailure,
    }
}

fn latched_pipeline_teardown_cause(cause_slot: &PipelineTeardownCause) -> crate::TeardownCause {
    cause_slot
        .get()
        .copied()
        .unwrap_or(crate::TeardownCause::PipelineFailure)
}

fn record_pipeline_teardown_failure(
    slot: &PipelineTeardownFailure,
    cause: crate::TeardownCause,
    source: std::io::Error,
) {
    record_pipeline_failure(slot, cause, "pipeline process-group hard kill", source);
}

fn record_pipeline_failure(
    slot: &PipelineTeardownFailure,
    cause: crate::TeardownCause,
    operation: &'static str,
    source: std::io::Error,
) {
    let mut guard = slot
        .first
        .lock()
        .expect("pipeline teardown failure slot poisoned");
    if guard.is_none() {
        *guard = Some(PipelineKillFailure {
            cause,
            operation,
            source,
        });
        drop(guard);
        slot.changed.notify_waiters();
    }
}

async fn confirm_pipeline_terminal_dispositions(
    terminal: &PipelineTerminalConfirmation,
) -> std::result::Result<(), PipelineTerminalError> {
    match tokio::time::timeout(
        pipeline_terminal_confirmation_budget(),
        terminal.wait_confirmed(),
    )
    .await
    {
        Ok(confirmed) => confirmed,
        Err(_) => Err(PipelineTerminalError::from_io(
            &std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "pipeline stages did not reach terminal disposition after group kill",
            ),
            None,
        )),
    }
}

async fn confirm_or_record_pipeline_terminal_failure(
    terminal: &PipelineTerminalConfirmation,
    slot: &PipelineTeardownFailure,
    cause: crate::TeardownCause,
) {
    if let Err(failure) = confirm_pipeline_terminal_dispositions(terminal).await {
        record_pipeline_failure(
            slot,
            failure.cause.unwrap_or(cause),
            "pipeline stage terminal confirmation",
            failure.into_io(),
        );
    }
}

async fn wait_pipeline_teardown_failure(slot: &PipelineTeardownFailure) {
    loop {
        // Enable before checking the slot so a writer between the check and the
        // await cannot be lost (`notify_waiters` stores no broadcast permit).
        let changed = slot.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if slot
            .first
            .lock()
            .expect("pipeline teardown failure slot poisoned")
            .is_some()
        {
            return;
        }
        changed.await;
    }
}

async fn wait_pipeline_failure_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn pipeline_teardown_failure_is_recorded(slot: &PipelineTeardownFailure) -> bool {
    slot.first
        .lock()
        .expect("pipeline teardown failure slot poisoned")
        .is_some()
}

fn take_pipeline_teardown_failure(slot: &PipelineTeardownFailure) -> Option<PipelineKillFailure> {
    slot.first
        .lock()
        .expect("pipeline teardown failure slot poisoned")
        .take()
}

fn kill_all_stage_groups(groups: &[Arc<ProcessGroup>]) -> std::io::Result<()> {
    let mut first_error = None;
    for group in groups {
        if let Err(error) = crate::running::group_hard_kill(group)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// The [`Weak`] analogue of [`kill_all_stage_groups`], for the detached watchdog
/// tasks a live [`PipelineSession`] holds: `Weak` so a still-parked killer/deadline
/// task can never keep a stage's sub-group (and its kill-on-drop backstop) alive
/// past a session drop — a dropped-away group simply fails to upgrade and is
/// skipped. Best-effort per group, exactly like the strong-handle version.
fn kill_weak_stage_groups(groups: &[Weak<ProcessGroup>]) -> std::io::Result<()> {
    let mut first_error = None;
    for group in groups {
        if let Some(group) = group.upgrade()
            && let Err(error) = crate::running::group_hard_kill(&group)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Spawn the standing teardown killer for [`Pipeline::start`]'s live session: it
/// waits on `teardown`, gives downstream filters a bounded window to drain final
/// bytes and EOF, then fans a hard kill across every stage's sub-group (the last
/// stage included). Holds [`Weak`] handles so it never pins the groups; the
/// session aborts it on `finish`/drop. The initiating cause was latched before
/// `teardown` fired, so a token that changes later cannot relabel a failed kill.
fn spawn_group_killer(
    teardown: tokio_util::sync::CancellationToken,
    stage_groups: &[Arc<ProcessGroup>],
    teardown_failure: PipelineTeardownFailure,
    teardown_cause: PipelineTeardownCause,
    terminal: PipelineTerminalConfirmation,
) -> JoinHandle<()> {
    let groups: Vec<Weak<ProcessGroup>> = stage_groups.iter().map(Arc::downgrade).collect();
    tokio::spawn(async move {
        teardown.cancelled().await;
        tokio::time::sleep(TEARDOWN_DRAIN_GRACE).await;
        run_before_pipeline_fallback_kill();
        let cause = latched_pipeline_teardown_cause(&teardown_cause);
        match kill_weak_stage_groups(&groups) {
            Err(source) => {
                record_pipeline_teardown_failure(&teardown_failure, cause, source);
            }
            Ok(()) => {
                confirm_or_record_pipeline_terminal_failure(&terminal, &teardown_failure, cause)
                    .await;
            }
        }
    })
}

/// Which side of the chain's proactive teardown one stage was on **when its exit
/// was first observed**: no teardown in flight yet, so its own failure is a
/// genuine *culprit* (`torn_down == false`), or a sibling's teardown had already
/// fired, making it a *victim* the pipefail fold de-prioritizes
/// (`torn_down == true`).
///
/// Latched once — by whichever observer reaches the stage's exit first — and never
/// moved afterwards. That is the same first-observation-wins rule the runner
/// already applies to a run's *cancel* disposition (`cancel_at_exit`, snapshotted
/// at the reap), and two properties of the chain depend on it:
///
/// - **Which observer looked doesn't matter.** The last stage has two possible
///   observers — [`spawn_last_stage_watcher`]'s probe and
///   [`finish`](PipelineSession::finish)'s own drive to [`Finished`] — and
///   whichever gets there first has to reach the *same* verdict, or watching the
///   stage would silently change the attribution instead of merely reaching it
///   sooner.
/// - **Draining doesn't matter.** A stage is reaped when it exits, but its drain
///   ends only once the last writer of its pipes is gone — a grandchild it forked
///   (`sh -c '… &'`) that inherited the write end can hold stderr open long after.
///   Latching at the exit keeps such a stage the culprit it actually was, instead
///   of demoting it to the victim of a teardown that only fired *after* it had
///   already died (and letting a later, downstream failure inherit the blame, and
///   with it the reported exit code and stderr).
///
/// **Firing** the teardown is a separate question from latching it, and the two
/// kinds of driver answer it differently — deliberately, because they exist for
/// different reasons:
///
/// - A stage driven by **its own drain** — every inner stage
///   ([`finish_inner_stage`], on both paths), and the last stage once
///   [`finish`](PipelineSession::finish) or a buffering [`capture`](Pipeline::capture)
///   drives it — fires the teardown only once its own output is collected, so the
///   killer's drain grace cannot cut that culprit's diagnostics short. A wedged
///   drain therefore still delays the teardown such a stage owes; the crate's
///   answer to a stage whose forked grandchild wedges it remains a per-stage
///   [`Command::timeout`] (see [`Pipeline::timeout`]).
/// - The last stage's **watcher** ([`spawn_last_stage_watcher`], live sessions
///   only) fires on the exit it observes, with no drain to wait for — which is the
///   entire point of watching it. A failing last stage has to tear its upstream
///   down while the caller is still streaming; deferring that to the caller's
///   eventual `finish()` is the leak the watcher exists to close, so here a wedged
///   drain delays nothing.
///
/// The price of that second bullet, accepted knowingly rather than by default: for
/// the last stage of a live session the killer's [`TEARDOWN_DRAIN_GRACE`] starts at
/// the stage's exit, i.e. *before* its own stderr has finished draining, and the
/// killer's fan covers that stage's sub-group too — so a grandchild still writing
/// into its stderr pipe past the grace is killed mid-sentence. What can be lost is
/// only that survivor's *remaining* output: the bytes the stage itself already
/// emitted sit in the pipe and are still read out after the writer dies. Waiting
/// for the drain instead would reopen exactly the unbounded-upstream leak above.
#[derive(Clone, Debug)]
struct ExitDisposition(Arc<AtomicU8>);

/// Nobody has observed this stage's exit yet.
const DISPOSITION_UNOBSERVED: u8 = 0;
/// Observed exiting with no teardown in flight — a genuine culprit.
const DISPOSITION_CULPRIT: u8 = 1;
/// Observed exiting into a teardown a sibling had already fired — a victim.
const DISPOSITION_VICTIM: u8 = 2;

impl ExitDisposition {
    fn unobserved() -> Self {
        Self(Arc::new(AtomicU8::new(DISPOSITION_UNOBSERVED)))
    }

    /// Record `torn_down` as this stage's disposition and return the one that
    /// *counts*: `torn_down` when this call was the first observation of the
    /// stage's exit, the earlier observer's verdict when it was not.
    ///
    /// Idempotent, so a later observer calls it as a plain read-with-fallback —
    /// including the case where no earlier observer exists at all (nothing watches
    /// an inner stage but its own drain), which is why the fallback is the
    /// caller's freshly read `torn_down` rather than an arbitrary default.
    ///
    /// `AcqRel`/`Acquire`: a winning observer publishes the disposition *before*
    /// cancelling the teardown token, so anyone who sees the fired token also sees
    /// who fired it.
    fn latch(&self, torn_down: bool) -> bool {
        let observed = if torn_down {
            DISPOSITION_VICTIM
        } else {
            DISPOSITION_CULPRIT
        };
        match self.0.compare_exchange(
            DISPOSITION_UNOBSERVED,
            observed,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => torn_down,
            Err(first) => first == DISPOSITION_VICTIM,
        }
    }
}

/// Spawn the standing **last-stage watcher** for [`Pipeline::start`]'s live
/// session — the last stage's stand-in for the `inner_tasks` drains, which fire
/// proactive teardown from the *checked* failure of the stage each one drives.
///
/// The last stage cannot be driven that way: the caller streams it and
/// [`finish`](PipelineSession::finish) consumes it, so nothing may take it over.
/// This task instead **observes** it — a non-blocking
/// [`exit_outcome_now`](RunningProcess::exit_outcome_now) probe plus its latched
/// cancellation disposition under the shared slot's lock. The probe neither
/// consumes the handle nor touches its streams, so the
/// consume-once contract of
/// [`stdout_lines`](PipelineSession::stdout_lines)/[`events`](PipelineSession::events)
/// is untouched — and then applies `finish_inner_stage`'s rule to what it sees:
/// latch the stage's [`ExitDisposition`] against the teardown in flight (if any),
/// and fire `teardown` only for a checked failure or observed cancellation that no
/// sibling's teardown preceded. Both drives observe the same thing — the stage's
/// *exit* — so this
/// watcher reaches the attribution `finish` would have reached, sooner; it does not
/// reach a different one. The latch is also what stops `finish` from reading the
/// teardown *this* stage triggered as evidence that a sibling killed it.
///
/// The observation is a **poll**: a passive observer cannot await a child it does
/// not own. The cadence backs off from [`LAST_STAGE_PROBE_MIN`] to
/// [`LAST_STAGE_PROBE_MAX`], so a failed chain's upstream outlives the failure by at
/// most one probe interval plus the killer's [`TEARDOWN_DRAIN_GRACE`], and the task
/// then ends — a terminal outcome is the last thing there is to observe. A [`Weak`]
/// slot handle, exactly like [`spawn_group_killer`]'s groups, so a session dropped
/// mid-stream frees the last stage's handle (and fires its kill-on-drop) without
/// waiting for this task to be reclaimed; the session also aborts it on
/// `finish`/drop.
#[allow(clippy::too_many_arguments)]
fn spawn_last_stage_watcher(
    last: &SharedLast,
    ok_codes: Vec<i32>,
    unchecked: bool,
    teardown: tokio_util::sync::CancellationToken,
    teardown_cause: PipelineTeardownCause,
    disposition: ExitDisposition,
    terminal: PipelineTerminalConfirmation,
    terminal_index: usize,
) -> JoinHandle<()> {
    let slot = Arc::downgrade(last);
    tokio::spawn(async move {
        let mut delay = LAST_STAGE_PROBE_MIN;
        loop {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(LAST_STAGE_PROBE_MAX);
            // No strong reference is ever held across the sleep above: a dropped
            // session must be free to reap its chain the instant it falls.
            let Some(slot) = slot.upgrade() else {
                return;
            };
            // An empty slot means an `async` session call holds the handle right now
            // (`wait_for_line`); skip this round rather than making the caller wait.
            let observed = lock_last(&slot).as_mut().and_then(|process| {
                process
                    .exit_outcome_now()
                    .map(|outcome| (outcome, process.cancelled_at_exit()))
            });
            drop(slot);
            let Some((outcome, cancelled)) = observed else {
                continue;
            };
            terminal.confirm(terminal_index);
            // Same rule and same order as `finish_inner_stage`: latch the stage's
            // disposition against the exit just observed — a teardown already in
            // flight makes it a victim of a sibling's failure — and only then fire
            // the teardown its own checked failure calls for. Latching first is what
            // makes the fired token safe to read: anything that observes the teardown
            // also observes who fired it.
            if !disposition.latch(teardown.is_cancelled())
                && (cancelled || is_checked_failure(outcome, &ok_codes, unchecked))
            {
                trigger_pipeline_teardown(
                    &teardown,
                    &teardown_cause,
                    if cancelled {
                        crate::TeardownCause::Cancellation
                    } else {
                        crate::TeardownCause::PipelineFailure
                    },
                );
            }
            return;
        }
    })
}

/// Drive one already-launched **non-last** stage to its [`Finished`] and fold it
/// into a positioned [`StageOutcome`], firing `teardown` on its first *checked*
/// failure (the proactive-teardown trigger). The shared classify-and-teardown body
/// of both the buffering [`capture`](Pipeline::capture) path and the streaming
/// [`start`](Pipeline::start) session's inner drains, so both blame a stage — and
/// decide whether it is a teardown victim — by exactly the same rule.
///
/// The `torn_down` disposition is latched at the stage's **exit**, before it can
/// fire `teardown` itself: a teardown already in flight when the stage *died* marks
/// it a victim (de-prioritized in the pipefail fold), while the first genuine
/// failure sees no teardown yet and stays the (non-torn) culprit. Reading it at the
/// exit rather than after the stage's output drained is what makes a slow drain a
/// latency question instead of an attribution one — see [`ExitDisposition`].
#[allow(clippy::too_many_arguments)]
async fn finish_inner_stage(
    process: RunningProcess,
    index: usize,
    program: String,
    ok_codes: Vec<i32>,
    timeout: Option<Duration>,
    unchecked: bool,
    teardown: tokio_util::sync::CancellationToken,
    teardown_cause: PipelineTeardownCause,
    terminal: PipelineTerminalConfirmation,
) -> Result<(usize, StageOutcome)> {
    // Nothing else observes an inner stage, so this latch has exactly one writer —
    // it is here to read the teardown at the right *instant*, not to arbitrate
    // between observers the way the last stage's shared one does.
    let disposition = ExitDisposition::unobserved();
    let finished = process
        .finish_observing_exit({
            let disposition = disposition.clone();
            let teardown = teardown.clone();
            let terminal = terminal.clone();
            move |observation| {
                observe_pipeline_stage_exit(observation, &terminal, index, &disposition, &teardown);
            }
        })
        .await;
    let Finished {
        outcome,
        stderr,
        stderr_truncated,
    } = match finished {
        Ok(finished) => finished,
        Err(error) => {
            if !terminal.stage_is_confirmed(index) {
                terminal.fail(pipeline_terminal_error(&error));
            }
            return Err(error);
        }
    };
    // Fire only now: the stage's own stderr is fully collected, so the killer's
    // drain grace can't cut the culprit's diagnostics short.
    let torn_down = disposition.latch(teardown.is_cancelled());
    if !torn_down && is_checked_failure(outcome, &ok_codes, unchecked) {
        trigger_pipeline_teardown(
            &teardown,
            &teardown_cause,
            crate::TeardownCause::PipelineFailure,
        );
    }
    Ok((
        index,
        StageOutcome {
            program,
            outcome,
            stderr,
            unchecked,
            ok_codes,
            timeout,
            torn_down,
            stderr_truncated,
        },
    ))
}

/// Fold every complete snapshot available at a bounded pipeline stop. A raw stage
/// error has no `Finished` payload of its own, so it is ignored while completed
/// siblings still preserve the usual pipefail attribution and truncation state.
fn partial_pipeline_finished(
    inner_res: InnerDrain,
    last_res: Option<(Result<Finished>, bool)>,
    last_program: &str,
    last_ok_codes: &[i32],
    last_unchecked: bool,
    last_timeout: Option<Duration>,
) -> Option<Finished> {
    let mut inner = match inner_res {
        Ok(inner) => inner,
        Err(failure) => failure.completed,
    };
    inner.sort_by_key(|(index, _)| *index);
    let mut stages: Vec<StageOutcome> = inner.into_iter().map(|(_, outcome)| outcome).collect();

    if let Some((Ok(last_finished), last_torn_down)) = last_res {
        stages.push(StageOutcome {
            program: last_program.to_owned(),
            outcome: last_finished.outcome,
            stderr: last_finished.stderr,
            unchecked: last_unchecked,
            ok_codes: last_ok_codes.to_vec(),
            timeout: last_timeout,
            torn_down: last_torn_down,
            stderr_truncated: last_finished.stderr_truncated,
        });
    }

    if stages.is_empty() {
        return None;
    }

    let folded = pipefail(stages, ());
    Some(Finished {
        outcome: folded.outcome(),
        stderr: folded.stderr().to_owned(),
        stderr_truncated: folded.truncated(),
    })
}

fn timeout_finished(
    inner_res: InnerDrain,
    last_res: Result<Finished>,
    last_program: &str,
    last_ok_codes: &[i32],
    last_unchecked: bool,
    last_timeout: Option<Duration>,
    last_torn_down: bool,
) -> Finished {
    // The deadline wins only for the public outcome; diagnostics still follow the
    // natural pipefail fold over every stage that reached a complete snapshot.
    partial_pipeline_finished(
        inner_res,
        Some((last_res, last_torn_down)),
        last_program,
        last_ok_codes,
        last_unchecked,
        last_timeout,
    )
    .map(|finished| Finished {
        outcome: Outcome::TimedOut,
        ..finished
    })
    .unwrap_or(Finished {
        outcome: Outcome::TimedOut,
        stderr: String::new(),
        stderr_truncated: false,
    })
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
        Outcome::Signalled(_) | Outcome::TimedOut | Outcome::InactivityTimedOut => false,
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
    crate::Error::io(std::io::Error::other(format!(
        "pipeline stage task failed: {err}"
    )))
}

/// Keep the first observed raw stage error unless a later terminal teardown
/// failure proves the chain was not actually stopped. Among two teardown errors
/// the first remains authoritative, making source selection stable.
fn prefer_pipeline_terminal_error(first: crate::Error, later: crate::Error) -> crate::Error {
    if !first.is_teardown() && later.is_teardown() {
        later
    } else {
        first
    }
}

#[derive(Debug)]
struct PartialDrainError<Item> {
    completed: Vec<Item>,
    error: crate::Error,
}

type InnerStage = (usize, StageOutcome);
type InnerDrain = std::result::Result<Vec<InnerStage>, PartialDrainError<InnerStage>>;

/// Drain every task in `tasks` to completion in **true completion order**
/// (`JoinSet::join_next`, not a left-to-right positional await), firing
/// `teardown` the instant *any* task ends badly — a raw `Err` (a stage's own
/// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) /
/// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) /
/// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) / [`ErrorReason::Io`](crate::ErrorReason::Io) /
/// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge)) or a task panic
/// (surfaced here as a `JoinError`).
///
/// This is `capture`'s liveness fix, factored out so it's testable without
/// spawning a single real process: a positional `for task in tasks { task
/// .await?? }` gather stalls forever on an earlier, still-quiet task once a
/// *later* task is already sitting on a ready `Err` — that `Err` never got a
/// chance to fire `teardown` either, since it happened on a path (`?`) that
/// skips the checked-failure attribution logic entirely. Draining in
/// completion order instead means whichever task is ready first — quiet or
/// not, wherever it sits — is the one examined first, so a bad completion is
/// never stuck behind a pending one.
///
/// A task that resolves to a *checked* failure (an `Outcome` `pipefail`
/// treats as unclean) already fires `teardown` itself, from inside the task
/// — see the spawn bodies in `capture` — since that decision needs
/// domain knowledge (the stage's `ok_codes`/`unchecked` flag) this function
/// doesn't have; this function only has to backstop the paths those spawn
/// bodies *can't* self-report: a raw `Err` return (still inside the task,
/// but past `is_checked_failure`) and a panic (never returns at all).
///
/// After the first raw error the drain keeps collecting sibling terminal
/// dispositions. The proactive teardown/group-killer paths bound those siblings;
/// this extra collection is what lets a later
/// [`ErrorReason::Teardown`](crate::ErrorReason::Teardown) outrank an
/// ordinary `Cancelled`/stdin/pump error instead of aborting the task that carries
/// proof of a potentially-live process. The first teardown error in completion
/// order remains authoritative.
///
/// Every bad completion also publishes its proactive-teardown cause before firing
/// the token: raw cancellation preserves `Cancellation`, an existing stage
/// `Teardown` preserves its cause, and other errors or panics use `PipelineFailure`.
///
/// On success, every task's `Ok` payload is returned — in **completion** order,
/// not submission order; a payload that must be rebuilt in original stage order
/// (as `capture` does) needs to carry its own position, the way [`Joined::Inner`]'s
/// index does.
async fn drain_unordered<Item: 'static>(
    mut tasks: tokio::task::JoinSet<Result<Item>>,
    teardown: &tokio_util::sync::CancellationToken,
    teardown_cause: &PipelineTeardownCause,
) -> std::result::Result<Vec<Item>, PartialDrainError<Item>> {
    let mut collected = Vec::with_capacity(tasks.len());
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        collect_drain_completion(
            joined,
            &mut collected,
            &mut first_error,
            teardown,
            teardown_cause,
        );
    }
    finish_partial_drain(collected, first_error)
}

fn collect_drain_completion<Item>(
    joined: std::result::Result<Result<Item>, tokio::task::JoinError>,
    collected: &mut Vec<Item>,
    first_error: &mut Option<crate::Error>,
    teardown: &tokio_util::sync::CancellationToken,
    teardown_cause: &PipelineTeardownCause,
) {
    match joined {
        Ok(Ok(item)) => collected.push(item),
        Ok(Err(err)) => {
            trigger_pipeline_teardown(
                teardown,
                teardown_cause,
                pipeline_error_teardown_cause(&err),
            );
            *first_error = Some(match first_error.take() {
                Some(first) => prefer_pipeline_terminal_error(first, err),
                None => err,
            });
        }
        Err(join_err) => {
            trigger_pipeline_teardown(
                teardown,
                teardown_cause,
                crate::TeardownCause::PipelineFailure,
            );
            let error = join_error(join_err);
            *first_error = Some(match first_error.take() {
                Some(first) => prefer_pipeline_terminal_error(first, error),
                None => error,
            });
        }
    }
}

fn finish_partial_drain<Item>(
    collected: Vec<Item>,
    first_error: Option<crate::Error>,
) -> std::result::Result<Vec<Item>, PartialDrainError<Item>> {
    match first_error {
        Some(error) => Err(PartialDrainError {
            completed: collected,
            error,
        }),
        None => Ok(collected),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline_marker(tag: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "processkit_pipeline_teardown_{tag}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn marked_long_pipeline(marker: &std::path::Path) -> Pipeline {
        #[cfg(unix)]
        return Command::new("sh")
            .args(["-c", "sleep 60"])
            .pipe(Command::new("sh").args([
                "-c",
                &format!(
                    "printf 'pipeline-prefix\\n'; : > '{}'; sleep 60",
                    marker.display()
                ),
            ]));
        #[cfg(windows)]
        {
            let marker = marker.display().to_string().replace('\'', "''");
            let producer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 60",
            ]);
            let consumer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[Console]::Out.WriteLine('pipeline-prefix'); \
                     [IO.File]::WriteAllText('{marker}', 'ready'); \
                     Start-Sleep -Seconds 60"
                ),
            ]);
            producer.pipe(consumer)
        }
    }

    fn marked_pipeline_commands(
        marker: &std::path::Path,
        producer_pid: &std::path::Path,
        consumer_pid: &std::path::Path,
        fail_consumer: bool,
    ) -> (Command, Command) {
        #[cfg(unix)]
        {
            let producer = Command::new("sh").args([
                "-c",
                &format!(
                    "printf '%s' \"$$\" > '{}'; sleep 60",
                    producer_pid.display()
                ),
            ]);
            let consumer_tail = if fail_consumer { "exit 7" } else { "sleep 60" };
            let consumer = Command::new("sh").args([
                "-c",
                &format!(
                    "printf 'pipeline-prefix\\n'; printf 'pipeline-stderr-prefix\\n' >&2; \
                     printf '%s' \"$$\" > '{}'; : > '{}'; {consumer_tail}",
                    consumer_pid.display(),
                    marker.display()
                ),
            ]);
            (producer, consumer)
        }
        #[cfg(windows)]
        {
            let marker = marker.display().to_string().replace('\'', "''");
            let producer_pid = producer_pid.display().to_string().replace('\'', "''");
            let consumer_pid = consumer_pid.display().to_string().replace('\'', "''");
            let consumer_tail = if fail_consumer {
                "exit 7"
            } else {
                "Start-Sleep -Seconds 60"
            };
            let producer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[IO.File]::WriteAllText('{producer_pid}', [string]$PID); \
                     Start-Sleep -Seconds 60"
                ),
            ]);
            let consumer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[Console]::Out.WriteLine('pipeline-prefix'); \
                     [Console]::Error.WriteLine('pipeline-stderr-prefix'); \
                     [IO.File]::WriteAllText('{consumer_pid}', [string]$PID); \
                     [IO.File]::WriteAllText('{marker}', 'ready'); \
                     {consumer_tail}"
                ),
            ]);
            (producer, consumer)
        }
    }

    #[cfg(feature = "process-control")]
    fn partial_prefix_pipeline(
        marker: &std::path::Path,
        producer_pid: &std::path::Path,
        consumer_pid: &std::path::Path,
        stdout_prefix: &str,
        stderr_prefix: &str,
        token: crate::CancellationToken,
    ) -> Pipeline {
        #[cfg(unix)]
        {
            let producer = Command::new("sh").args([
                "-c",
                &format!(
                    "printf '%s' \"$$\" > '{}'; sleep 60",
                    producer_pid.display()
                ),
            ]);
            let consumer = Command::new("sh").args([
                "-c",
                &format!(
                    "printf '%s' '{stdout_prefix}'; printf '%s' '{stderr_prefix}' >&2; \
                     printf '%s' \"$$\" > '{}'; : > '{}'; sleep 60",
                    consumer_pid.display(),
                    marker.display()
                ),
            ]);
            producer.pipe(consumer.cancel_on(token))
        }
        #[cfg(windows)]
        {
            let marker = marker.display().to_string().replace('\'', "''");
            let producer_pid = producer_pid.display().to_string().replace('\'', "''");
            let consumer_pid = consumer_pid.display().to_string().replace('\'', "''");
            let producer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[IO.File]::WriteAllText('{producer_pid}', [string]$PID); \
                     Start-Sleep -Seconds 60"
                ),
            ]);
            let consumer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[Console]::Out.Write('{stdout_prefix}'); \
                     [Console]::Error.Write('{stderr_prefix}'); \
                     [IO.File]::WriteAllText('{consumer_pid}', [string]$PID); \
                     [IO.File]::WriteAllText('{marker}', 'ready'); \
                     Start-Sleep -Seconds 60"
                ),
            ]);
            producer.pipe(consumer.cancel_on(token))
        }
    }

    #[cfg(feature = "process-control")]
    fn cancellable_marked_pipeline(
        marker: &std::path::Path,
        producer_pid: &std::path::Path,
        consumer_pid: &std::path::Path,
    ) -> Pipeline {
        let (producer, consumer) =
            marked_pipeline_commands(marker, producer_pid, consumer_pid, false);
        producer.pipe(consumer)
    }

    #[cfg(feature = "process-control")]
    fn stage_local_cancellable_marked_pipeline(
        marker: &std::path::Path,
        producer_pid: &std::path::Path,
        consumer_pid: &std::path::Path,
        token: crate::CancellationToken,
    ) -> Pipeline {
        let (producer, consumer) =
            marked_pipeline_commands(marker, producer_pid, consumer_pid, false);
        producer.pipe(consumer.cancel_on(token))
    }

    fn failing_marked_pipeline(
        marker: &std::path::Path,
        producer_pid: &std::path::Path,
        consumer_pid: &std::path::Path,
    ) -> Pipeline {
        let (producer, consumer) =
            marked_pipeline_commands(marker, producer_pid, consumer_pid, true);
        producer.pipe(consumer)
    }

    #[cfg(feature = "process-control")]
    fn survivor_held_output_pipeline(
        marker: &std::path::Path,
        survivor_pid: &std::path::Path,
    ) -> Pipeline {
        #[cfg(unix)]
        return Command::new("sh")
            .args(["-c", "printf 'input\\n'"])
            .pipe(Command::new("sh").args([
                "-c",
                &format!(
                    "(trap '' HUP TERM; sleep 60) & survivor=$!; \
                     printf '%s' \"$survivor\" > '{}'; \
                     printf 'pipeline-prefix\\n'; printf 'pipeline-stderr-prefix\\n' >&2; \
                     : > '{}'",
                    survivor_pid.display(),
                    marker.display()
                ),
            ]));
        #[cfg(windows)]
        {
            let marker = marker.display().to_string().replace('\'', "''");
            let survivor_pid = survivor_pid.display().to_string().replace('\'', "''");
            let producer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::Out.WriteLine('input')",
            ]);
            let consumer = Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "$child = Start-Process -FilePath 'powershell' \
                       -ArgumentList @('-NoLogo','-NoProfile','-NonInteractive','-Command', \
                       'Start-Sleep -Seconds 60') -NoNewWindow -PassThru; \
                     [IO.File]::WriteAllText('{survivor_pid}', [string]$child.Id); \
                     [Console]::Out.WriteLine('pipeline-prefix'); \
                     [Console]::Error.WriteLine('pipeline-stderr-prefix'); \
                     [IO.File]::WriteAllText('{marker}', 'ready')"
                ),
            ]);
            producer.pipe(consumer)
        }
    }

    async fn wait_for_pipeline_marker(marker: &std::path::Path) {
        for _ in 0..500 {
            if marker.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "pipeline stage did not publish readiness marker: {}",
            marker.display()
        );
    }

    #[cfg(feature = "process-control")]
    async fn read_pipeline_pids(paths: &[&std::path::Path]) -> Vec<u32> {
        for _ in 0..500 {
            let parsed: Option<Vec<u32>> = paths
                .iter()
                .map(|path| {
                    std::fs::read_to_string(path)
                        .ok()?
                        .trim()
                        .parse::<u32>()
                        .ok()
                })
                .collect();
            if let Some(pids) = parsed {
                return pids;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("pipeline stages did not publish pid files: {paths:?}");
    }

    #[cfg(feature = "process-control")]
    async fn assert_pipeline_pids_gone(pids: &[u32]) {
        for _ in 0..500 {
            if pids.iter().all(|pid| {
                !crate::process_is_alive(*pid, None).expect("pipeline stage liveness probe")
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let alive: Vec<_> = pids
            .iter()
            .copied()
            .filter(|pid| crate::process_is_alive(*pid, None).unwrap_or(true))
            .collect();
        panic!("pipeline teardown left live stage processes: {alive:?}");
    }

    fn assert_pipeline_teardown(error: &crate::Error, expected: crate::TeardownCause) {
        match error.reason() {
            crate::ErrorReason::Teardown {
                cause,
                operation,
                source,
                ..
            } => {
                assert_eq!(*cause, expected);
                assert_eq!(*operation, "pipeline process-group hard kill");
                assert_eq!(source.raw_os_error(), Some(5));
            }
            other => panic!("expected pipeline Teardown, got {other:?}"),
        }
    }

    fn assert_pipeline_confirmation_teardown(error: &crate::Error, expected: crate::TeardownCause) {
        match error.reason() {
            crate::ErrorReason::Teardown {
                cause,
                operation,
                source,
                ..
            } => {
                assert_eq!(*cause, expected);
                assert_eq!(*operation, "pipeline stage terminal confirmation");
                assert_eq!(source.kind(), std::io::ErrorKind::TimedOut);
            }
            other => panic!("expected pipeline terminal-confirmation Teardown, got {other:?}"),
        }
    }

    #[cfg(feature = "process-control")]
    fn assert_pipeline_confirmation_source(
        error: &crate::Error,
        expected: crate::TeardownCause,
        raw_os_error: i32,
    ) {
        match error.reason() {
            crate::ErrorReason::Teardown {
                cause,
                operation,
                source,
                ..
            } => {
                assert_eq!(*cause, expected);
                assert_eq!(*operation, "pipeline stage terminal confirmation");
                assert_eq!(source.raw_os_error(), Some(raw_os_error));
            }
            other => panic!("expected pipeline terminal-confirmation Teardown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn terminal_confirmation_propagates_a_stage_reap_error() {
        let terminal = PipelineTerminalState::new(2);
        terminal.confirm(0);
        terminal.fail(PipelineTerminalError::from_io(
            &std::io::Error::from_raw_os_error(5),
            Some(crate::TeardownCause::Cancellation),
        ));

        let failure = confirm_pipeline_terminal_dispositions(&terminal)
            .await
            .expect_err("a stage reap error prevents terminal confirmation");
        assert_eq!(failure.cause, Some(crate::TeardownCause::Cancellation));
        assert_eq!(failure.into_io().raw_os_error(), Some(5));
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for deferred teardown production wiring"]
    async fn buffered_deferred_teardown_observer_outranks_chain_timeout() {
        let marker = pipeline_marker("buffered_deferred_teardown");
        let producer_pid = pipeline_marker("buffered_deferred_teardown_producer");
        let consumer_pid = pipeline_marker("buffered_deferred_teardown_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let pipeline = stage_local_cancellable_marked_pipeline(
            &marker,
            &producer_pid,
            &consumer_pid,
            token.clone(),
        )
        .timeout(Duration::from_secs(5));
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                1,
                5,
            )
            .arm();

        // Keep the failed stage's pumps inside their existing bounded salvage
        // while the earlier chain deadline fires. The production capture observer
        // must already have failed terminal confirmation with the cancellation
        // source, so that later timeout cannot downgrade it to `TimedOut`.
        tokio::time::pause();
        token.cancel();
        for _ in 0..1_000 {
            if faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "the stage-local hard teardown must reach its deferred failure"
        );
        assert!(
            !task.is_finished(),
            "the retained pipe pumps keep the stage future pending before the chain deadline"
        );
        // The readiness/prefix settle above consumes more than 100 ms of the
        // chain's five-second budget. Advancing just under the pump's own
        // five-second salvage bound therefore fires the older chain deadline
        // without letting the later pump join drain the retained prefix first.
        tokio::time::advance(Duration::from_millis(4_950)).await;
        for _ in 0..1_000 {
            if task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(task.is_finished(), "the chain deadline must stay bounded");
        tokio::time::resume();

        let error = task
            .await
            .expect("pipeline task")
            .expect_err("the unconfirmed stage teardown must fail closed");
        assert_pipeline_confirmation_source(&error, crate::TeardownCause::Cancellation, 5);
        match error.reason() {
            crate::ErrorReason::Teardown { stdout, .. } => assert!(
                stdout.contains("pipeline-prefix"),
                "the timeout race retains the available last-stage prefix: {stdout:?}"
            ),
            _ => unreachable!(),
        }
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for partial-tail production wiring"]
    async fn buffered_text_failed_confirmation_retains_unterminated_prefixes() {
        const STDOUT_PREFIX: &str = "text-partial-prefix";
        const STDERR_PREFIX: &str = "text-partial-stderr";
        let marker = pipeline_marker("buffered_text_partial");
        let producer_pid = pipeline_marker("buffered_text_partial_producer");
        let consumer_pid = pipeline_marker("buffered_text_partial_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let mut publications = crate::pump::observe_partial_tail_publications();
        let pipeline = partial_prefix_pipeline(
            &marker,
            &producer_pid,
            &consumer_pid,
            STDOUT_PREFIX,
            STDERR_PREFIX,
            token.clone(),
        )
        .timeout(Duration::from_secs(5));
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            publications.wait_for_all(&[STDOUT_PREFIX, STDERR_PREFIX]),
        )
        .await
        .expect("both text partial tails must reach the production pump seam");
        // Make the chain deadline observably older than the pump-salvage bound.
        // Fast Unix hosts can publish both tails in under 20 ms, so advancing
        // 4.95 s without this settle does not yet reach the five-second chain
        // timeout and tests scheduler speed instead of the teardown race.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                1,
                5,
            )
            .arm();

        tokio::time::pause();
        token.cancel();
        for _ in 0..1_000 {
            if faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        tokio::time::advance(Duration::from_millis(4_950)).await;
        for _ in 0..1_000 {
            if task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(task.is_finished(), "partial-tail teardown remains bounded");
        tokio::time::resume();

        let error = task
            .await
            .expect("pipeline task")
            .expect_err("unconfirmed text teardown must fail closed");
        assert_pipeline_confirmation_source(&error, crate::TeardownCause::Cancellation, 5);
        match error.reason() {
            crate::ErrorReason::Teardown { stdout, stderr, .. } => {
                assert_eq!(stdout, STDOUT_PREFIX);
                assert_eq!(stderr, STDERR_PREFIX);
            }
            _ => unreachable!(),
        }
        drop(faults);
        drop(publications);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real raw pipeline for partial-tail production wiring"]
    async fn buffered_raw_failed_confirmation_retains_bytes_and_unterminated_stderr() {
        const STDOUT_PREFIX: &[u8] = b"raw-partial-bytes";
        const STDERR_PREFIX: &str = "raw-partial-stderr";
        let marker = pipeline_marker("buffered_raw_partial");
        let producer_pid = pipeline_marker("buffered_raw_partial_producer");
        let consumer_pid = pipeline_marker("buffered_raw_partial_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let mut tail_publications = crate::pump::observe_partial_tail_publications();
        let mut raw_publications = crate::running::observe_raw_stdout_publications();
        let pipeline = partial_prefix_pipeline(
            &marker,
            &producer_pid,
            &consumer_pid,
            std::str::from_utf8(STDOUT_PREFIX).expect("ASCII test prefix"),
            STDERR_PREFIX,
            token.clone(),
        )
        .timeout(Duration::from_secs(5));
        let task = tokio::spawn(async move { pipeline.output_bytes().await });
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            tail_publications.wait_for_all(&[STDERR_PREFIX]),
        )
        .await
        .expect("raw stderr partial tail must reach the production pump seam");
        tokio::time::timeout(
            Duration::from_secs(2),
            raw_publications.wait_until_contains(STDOUT_PREFIX),
        )
        .await
        .expect("raw stdout bytes must reach the production capture buffer");
        // Keep the chain deadline strictly ahead of the pump-salvage deadline
        // on fast Unix hosts; see the text-capture counterpart above.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                1,
                5,
            )
            .arm();

        tokio::time::pause();
        token.cancel();
        for _ in 0..1_000 {
            if faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        tokio::time::advance(Duration::from_millis(4_950)).await;
        for _ in 0..1_000 {
            if task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            task.is_finished(),
            "raw partial-tail teardown remains bounded"
        );
        tokio::time::resume();

        let error = task
            .await
            .expect("pipeline task")
            .expect_err("unconfirmed raw teardown must fail closed");
        assert_pipeline_confirmation_source(&error, crate::TeardownCause::Cancellation, 5);
        match error.reason() {
            crate::ErrorReason::Teardown {
                stdout,
                stderr,
                stdout_bytes,
                ..
            } => {
                assert_eq!(stdout.as_bytes(), STDOUT_PREFIX);
                assert_eq!(stderr, STDERR_PREFIX);
                assert_eq!(stdout_bytes.as_deref(), Some(STDOUT_PREFIX));
            }
            _ => unreachable!(),
        }
        drop(faults);
        drop(tail_publications);
        drop(raw_publications);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real live pipeline for backend-wait production wiring"]
    async fn live_backend_wait_error_observer_outranks_chain_timeout() {
        let marker = pipeline_marker("live_backend_wait_error");
        let producer_pid = pipeline_marker("live_backend_wait_error_producer");
        let consumer_pid = pipeline_marker("live_backend_wait_error_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let wait_failure = crate::running::defer_next_backend_wait_error(6);
        let session = cancellable_marked_pipeline(&marker, &producer_pid, &consumer_pid)
            .timeout(Duration::from_secs(5))
            .start()
            .await
            .expect("start live pipeline");
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        wait_failure.wait_until_entered().await;
        let task = tokio::spawn(async move { session.finish().await });

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(
            !task.is_finished(),
            "the chain deadline must await the blocked stage's terminal disposition"
        );
        wait_failure.release();
        for _ in 0..1_000 {
            if task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            task.is_finished(),
            "the real backend-wait error must unblock terminal confirmation"
        );
        tokio::time::resume();

        let error = task
            .await
            .expect("pipeline finish task")
            .expect_err("an unconfirmed backend wait must fail closed");
        assert_pipeline_confirmation_source(&error, crate::TeardownCause::Timeout, 6);
        drop(wait_failure);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real pipeline for deterministic group-kill fault injection"]
    async fn pipeline_start_kill_failure_bounds_finish_and_preserves_diagnostics() {
        let marker = pipeline_marker("explicit_kill_failure");
        let producer_pid = pipeline_marker("explicit_kill_failure_producer");
        let consumer_pid = pipeline_marker("explicit_kill_failure_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let mut session = cancellable_marked_pipeline(&marker, &producer_pid, &consumer_pid)
            .start()
            .await
            .expect("start live pipeline");
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                1,
                5,
            )
            .arm();

        let start_error = session
            .start_kill()
            .expect_err("pipeline kill failure must not be swallowed");
        match start_error.reason() {
            crate::ErrorReason::Teardown {
                cause,
                operation,
                source,
                ..
            } => {
                assert_eq!(*cause, crate::TeardownCause::ExplicitKill);
                assert_eq!(*operation, "pipeline process-group hard kill");
                assert_eq!(source.raw_os_error(), Some(5));
            }
            other => panic!("expected pipeline Teardown, got {other:?}"),
        }
        assert_eq!(
            faults.matched(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            2,
            "every stage group is attempted even after the first error"
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "only the selected first group kill fails"
        );
        assert!(
            crate::process_is_alive(pids[0], None).expect("producer liveness probe"),
            "the injected group-kill failure must leave one stage live"
        );

        let started = tokio::time::Instant::now();
        let finish_error = tokio::time::timeout(Duration::from_secs(10), session.finish())
            .await
            .expect("the retained explicit-kill failure must bound finish")
            .expect_err("finish must repeat the retained teardown failure");
        assert!(started.elapsed() < Duration::from_secs(10));
        match finish_error.reason() {
            crate::ErrorReason::Teardown {
                cause,
                operation,
                source,
                stderr,
                ..
            } => {
                assert_eq!(*cause, crate::TeardownCause::ExplicitKill);
                assert_eq!(*operation, "pipeline process-group hard kill");
                assert_eq!(source.raw_os_error(), Some(5));
                assert_eq!(stderr, "pipeline-stderr-prefix");
            }
            other => panic!("expected retained pipeline Teardown, got {other:?}"),
        }
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "finish cleanup reaches the real backend after the one-shot fault"
        );
        assert_pipeline_pids_gone(&pids).await;
        drop(faults);
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for deterministic timeout fault injection"]
    async fn buffered_pipeline_timeout_keeps_prefix_and_surfaces_group_failure() {
        let marker = pipeline_marker("buffered_timeout");
        let _ = std::fs::remove_file(&marker);
        let pipeline = marked_long_pipeline(&marker).timeout(Duration::from_secs(5));
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let error = task
            .await
            .expect("pipeline task")
            .expect_err("an unconfirmed chain timeout must fail closed");
        assert_pipeline_teardown(&error, crate::TeardownCause::Timeout);
        match error.reason() {
            crate::ErrorReason::Teardown { stdout, .. } => assert!(
                stdout.contains("pipeline-prefix"),
                "the timeout error retains the buffered pipeline prefix: {stdout:?}"
            ),
            _ => unreachable!(),
        }
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            2,
            "the chain timeout attempts every stage group"
        );
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real streaming pipeline for deterministic timeout fault injection"]
    async fn pipeline_session_timeout_failure_finishes_without_waiting_for_live_stages() {
        let marker = pipeline_marker("session_timeout");
        let _ = std::fs::remove_file(&marker);
        let session = marked_long_pipeline(&marker)
            .timeout(Duration::from_secs(30))
            .start()
            .await
            .expect("start live pipeline");
        wait_for_pipeline_marker(&marker).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let source = kill_all_stage_groups(&session.stage_groups)
            .expect_err("the injected session group kill fails");
        record_pipeline_teardown_failure(
            &session.teardown_failure,
            crate::TeardownCause::Timeout,
            source,
        );
        session
            .chain_state
            .store(crate::running::TS_TIMED_OUT, Ordering::Release);
        let error = tokio::time::timeout(Duration::from_secs(5), session.finish())
            .await
            .expect("teardown failure must bound pipeline finish")
            .expect_err("an unconfirmed live-chain timeout must fail closed");
        assert_pipeline_teardown(&error, crate::TeardownCause::Timeout);
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            2,
            "the live-session timeout attempts every stage group"
        );
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for cancellation teardown fault injection"]
    async fn buffered_pipeline_cancellation_collects_a_sibling_teardown_failure() {
        let marker = pipeline_marker("buffered_cancel");
        let producer_pid = pipeline_marker("buffered_cancel_producer");
        let consumer_pid = pipeline_marker("buffered_cancel_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let pipeline = cancellable_marked_pipeline(&marker, &producer_pid, &consumer_pid)
            .cancel_on(token.clone());
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_from_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                2,
                5,
            )
            .arm();

        let started = tokio::time::Instant::now();
        token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("buffered sibling disposition collection must stay bounded")
            .expect("pipeline task")
            .expect_err("a sibling teardown failure must outrank cancellation");
        assert!(started.elapsed() < Duration::from_secs(10));
        match error.reason() {
            crate::ErrorReason::Teardown { cause, source, .. } => {
                assert_eq!(*cause, crate::TeardownCause::Cancellation);
                assert_eq!(source.raw_os_error(), Some(5));
            }
            other => panic!("expected sibling cancellation Teardown, got {other:?}"),
        }
        assert!(
            faults.matched(crate::sys::fault_injection::Site::ProcessGroupTeardown) >= 2,
            "one stage must confirm cancellation before its sibling teardown fails"
        );
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real live pipeline for cancellation teardown fault injection"]
    async fn live_pipeline_cancellation_collects_a_sibling_teardown_failure() {
        let marker = pipeline_marker("live_cancel");
        let producer_pid = pipeline_marker("live_cancel_producer");
        let consumer_pid = pipeline_marker("live_cancel_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let session = cancellable_marked_pipeline(&marker, &producer_pid, &consumer_pid)
            .cancel_on(token.clone())
            .start()
            .await
            .expect("start cancellable live pipeline");
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_from_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                2,
                5,
            )
            .arm();

        let started = tokio::time::Instant::now();
        token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(10), session.finish())
            .await
            .expect("live sibling disposition collection must stay bounded")
            .expect_err("a sibling teardown failure must outrank cancellation");
        assert!(started.elapsed() < Duration::from_secs(10));
        match error.reason() {
            crate::ErrorReason::Teardown { cause, source, .. } => {
                assert_eq!(*cause, crate::TeardownCause::Cancellation);
                assert_eq!(source.raw_os_error(), Some(5));
            }
            other => panic!("expected sibling cancellation Teardown, got {other:?}"),
        }
        assert!(
            faults.matched(crate::sys::fault_injection::Site::ProcessGroupTeardown) >= 2,
            "one stage must confirm cancellation before its sibling teardown fails"
        );
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for stage-local cancellation fault injection"]
    async fn buffered_stage_local_cancellation_latches_fallback_teardown_cause() {
        let marker = pipeline_marker("buffered_stage_cancel");
        let producer_pid = pipeline_marker("buffered_stage_cancel_producer");
        let consumer_pid = pipeline_marker("buffered_stage_cancel_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let pipeline = stage_local_cancellable_marked_pipeline(
            &marker,
            &producer_pid,
            &consumer_pid,
            token.clone(),
        );
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                2,
                5,
            )
            .arm();

        token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("buffered stage-local cancellation must stay bounded")
            .expect("pipeline task")
            .expect_err("the sibling fallback failure must fail closed");
        assert_pipeline_teardown(&error, crate::TeardownCause::Cancellation);
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "only the selected sibling fallback group kill is faulted"
        );
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real live pipeline for stage-local cancellation fault injection"]
    async fn live_stage_local_cancellation_latches_fallback_teardown_cause() {
        let marker = pipeline_marker("live_stage_cancel");
        let producer_pid = pipeline_marker("live_stage_cancel_producer");
        let consumer_pid = pipeline_marker("live_stage_cancel_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let token = crate::CancellationToken::new();
        let session = stage_local_cancellable_marked_pipeline(
            &marker,
            &producer_pid,
            &consumer_pid,
            token.clone(),
        )
        .start()
        .await
        .expect("start stage-local cancellable live pipeline");
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                2,
                5,
            )
            .arm();

        token.cancel();
        tokio::time::timeout(Duration::from_secs(2), session.teardown.cancelled())
            .await
            .expect("the live last-stage watcher must observe stage-local cancellation");
        assert_eq!(
            session.teardown_cause.get(),
            Some(&crate::TeardownCause::Cancellation),
            "the watcher publishes cancellation before the fallback kill"
        );
        let error = tokio::time::timeout(Duration::from_secs(10), session.finish())
            .await
            .expect("live stage-local cancellation must stay bounded")
            .expect_err("the sibling fallback failure must fail closed");
        assert_pipeline_teardown(&error, crate::TeardownCause::Cancellation);
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "only the selected sibling fallback group kill is faulted"
        );
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for a deterministic late-cancel race"]
    async fn buffered_pipeline_failure_cause_survives_late_chain_cancellation() {
        let marker = pipeline_marker("buffered_late_chain_cancel");
        let producer_pid = pipeline_marker("buffered_late_chain_cancel_producer");
        let consumer_pid = pipeline_marker("buffered_late_chain_cancel_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let chain_token = crate::CancellationToken::new();
        let late_token = chain_token.clone();
        let _hook = before_pipeline_fallback_kill(move || late_token.cancel());
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                1,
                5,
            )
            .arm();
        let pipeline = failing_marked_pipeline(&marker, &producer_pid, &consumer_pid)
            .cancel_on(chain_token.clone());
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;

        let error = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("buffered late-cancel race must stay bounded")
            .expect("pipeline task")
            .expect_err("the injected fallback failure must fail closed");
        assert!(
            chain_token.is_cancelled(),
            "the fallback hook fired the late token"
        );
        assert_pipeline_teardown(&error, crate::TeardownCause::PipelineFailure);
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "the fallback group kill retains the injected OS source"
        );
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real live pipeline for a deterministic late-cancel race"]
    async fn live_pipeline_failure_cause_survives_late_chain_cancellation() {
        let marker = pipeline_marker("live_late_chain_cancel");
        let producer_pid = pipeline_marker("live_late_chain_cancel_producer");
        let consumer_pid = pipeline_marker("live_late_chain_cancel_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let chain_token = crate::CancellationToken::new();
        let late_token = chain_token.clone();
        let _hook = before_pipeline_fallback_kill(move || late_token.cancel());
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_nth(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                1,
                5,
            )
            .arm();
        let session = failing_marked_pipeline(&marker, &producer_pid, &consumer_pid)
            .cancel_on(chain_token.clone())
            .start()
            .await
            .expect("start failing live pipeline");
        wait_for_pipeline_marker(&marker).await;
        let pids = read_pipeline_pids(&[&producer_pid, &consumer_pid]).await;

        tokio::time::timeout(Duration::from_secs(2), session.teardown.cancelled())
            .await
            .expect("the live last-stage watcher must observe the checked failure");
        assert_eq!(
            session.teardown_cause.get(),
            Some(&crate::TeardownCause::PipelineFailure),
            "the checked failure publishes its cause before the late token fires"
        );

        let error = tokio::time::timeout(Duration::from_secs(10), session.finish())
            .await
            .expect("live late-cancel race must stay bounded")
            .expect_err("the injected fallback failure must fail closed");
        assert!(
            chain_token.is_cancelled(),
            "the fallback hook fired the late token"
        );
        assert_pipeline_teardown(&error, crate::TeardownCause::PipelineFailure);
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1,
            "the fallback group kill retains the injected OS source"
        );
        drop(faults);
        assert_pipeline_pids_gone(&pids).await;
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for terminal-confirmation fault injection"]
    async fn buffered_pipeline_timeout_requires_bounded_terminal_confirmation() {
        let marker = pipeline_marker("buffered_confirmation_timeout");
        let _ = std::fs::remove_file(&marker);
        let _confirmation = suppress_pipeline_terminal_confirmation(Duration::from_millis(100));
        let pipeline = marked_long_pipeline(&marker).timeout(Duration::from_secs(10));
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;

        let started = tokio::time::Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .expect("buffered terminal confirmation must stay bounded")
            .expect("pipeline task")
            .expect_err("kill dispatch without reap confirmation must fail closed");
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_pipeline_confirmation_teardown(&error, crate::TeardownCause::Timeout);
        match error.reason() {
            crate::ErrorReason::Teardown { stdout, .. } => assert!(
                stdout.contains("pipeline-prefix"),
                "terminal-confirmation failure retains the buffered prefix: {stdout:?}"
            ),
            _ => unreachable!(),
        }
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real live pipeline for terminal-confirmation fault injection"]
    async fn live_pipeline_timeout_requires_bounded_terminal_confirmation() {
        let marker = pipeline_marker("live_confirmation_timeout");
        let _ = std::fs::remove_file(&marker);
        let _confirmation = suppress_pipeline_terminal_confirmation(Duration::from_millis(100));
        let session = marked_long_pipeline(&marker)
            .timeout(Duration::from_secs(10))
            .start()
            .await
            .expect("start live pipeline");
        wait_for_pipeline_marker(&marker).await;

        let started = tokio::time::Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(15), session.finish())
            .await
            .expect("live terminal confirmation must stay bounded")
            .expect_err("kill dispatch without reap confirmation must fail closed");
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_pipeline_confirmation_teardown(&error, crate::TeardownCause::Timeout);
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real buffered pipeline for the timely-reap timeout control"]
    async fn buffered_pipeline_timeout_stays_timed_out_when_every_stage_reaps() {
        let marker = pipeline_marker("buffered_confirmation_control");
        let _ = std::fs::remove_file(&marker);
        let pipeline = marked_long_pipeline(&marker).timeout(Duration::from_secs(10));
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;

        let result = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .expect("timely stage reaps preserve the ordinary timeout outcome")
            .expect("pipeline task")
            .expect("confirmed timeout is a captured outcome");
        assert!(result.timed_out());
        assert!(
            result.stdout().contains("pipeline-prefix"),
            "the ordinary timeout retains its buffered prefix: {:?}",
            result.stdout()
        );
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real failing pipeline for terminal-confirmation fault injection"]
    async fn proactive_pipeline_failure_requires_bounded_sibling_confirmation() {
        let marker = pipeline_marker("proactive_confirmation_failure");
        let producer_pid = pipeline_marker("proactive_confirmation_producer");
        let consumer_pid = pipeline_marker("proactive_confirmation_consumer");
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
        let _confirmation = suppress_pipeline_terminal_confirmation(Duration::from_millis(100));
        let pipeline = failing_marked_pipeline(&marker, &producer_pid, &consumer_pid);
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;

        let started = tokio::time::Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .expect("proactive sibling confirmation must stay bounded")
            .expect("pipeline task")
            .expect_err("an unreaped sibling must fail closed");
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_pipeline_confirmation_teardown(&error, crate::TeardownCause::PipelineFailure);
        match error.reason() {
            crate::ErrorReason::Teardown { stdout, .. } => assert!(
                stdout.contains("pipeline-prefix"),
                "proactive confirmation failure retains the last-stage prefix: {stdout:?}"
            ),
            _ => unreachable!(),
        }
        for path in [&marker, &producer_pid, &consumer_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a descendant that outlives the pipeline stage while holding output"]
    async fn buffered_pipeline_finalization_bounds_survivor_held_output_and_salvages_prefix() {
        let marker = pipeline_marker("survivor_output");
        let survivor_pid = pipeline_marker("survivor_output_pid");
        for path in [&marker, &survivor_pid] {
            let _ = std::fs::remove_file(path);
        }
        let pipeline = survivor_held_output_pipeline(&marker, &survivor_pid);
        let task = tokio::spawn(async move { pipeline.output_string().await });
        wait_for_pipeline_marker(&marker).await;
        let survivor = read_pipeline_pids(&[&survivor_pid]).await[0];

        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(9), task)
            .await
            .expect("pipeline output-pump finalization must remain bounded")
            .expect("pipeline task")
            .expect("the direct stages exited successfully");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(4),
            "the survivor must genuinely hold output through the pump grace: {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(9));
        assert!(
            result.stdout().contains("pipeline-prefix"),
            "the bounded abort keeps the last-stage prefix: {:?}",
            result.stdout()
        );
        assert!(
            result.stderr().contains("pipeline-stderr-prefix"),
            "the bounded abort keeps the stderr prefix: {:?}",
            result.stderr()
        );
        assert_pipeline_pids_gone(&[survivor]).await;
        for path in [&marker, &survivor_pid] {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Compile-time proof that sharing the last stage's handle with the standing
    /// watcher did not cost the session any auto trait: a [`PipelineSession`] is
    /// still `Send + Sync`, and its readiness probe's future is still `Send`.
    ///
    /// Both are load-bearing and neither is observable from this crate's own
    /// `#[tokio::test]`s (a current-thread runtime never requires `Send`), so they
    /// would otherwise regress silently in a downstream `tokio::spawn`. The shape
    /// that would break them is holding the shared slot's `MutexGuard` across the
    /// `.await` instead of moving the handle out for the call — see [`LastBorrow`].
    /// Never called: constructing a session needs a real chain, and the assertion
    /// is discharged by type-checking this body.
    #[allow(dead_code)]
    fn session_and_its_probe_future_stay_send(session: &mut PipelineSession) {
        fn assert_send<T: Send>(_: &T) {}
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(session);
        assert_send(&session.wait_for_line(|line| line.is_empty(), Duration::ZERO));
    }

    fn pending_pipeline_with_distinct_tokens(
        first_token: tokio_util::sync::CancellationToken,
        second_token: tokio_util::sync::CancellationToken,
        chain_token: tokio_util::sync::CancellationToken,
    ) -> Pipeline {
        let runner = Arc::new(
            crate::doubles::ScriptedRunner::new().fallback(crate::doubles::Reply::pending()),
        );
        pipeline_with_distinct_tokens(first_token, second_token, chain_token, runner)
    }

    fn pipeline_with_distinct_tokens(
        first_token: tokio_util::sync::CancellationToken,
        second_token: tokio_util::sync::CancellationToken,
        chain_token: tokio_util::sync::CancellationToken,
        runner: Arc<dyn crate::runner::ProcessRunner>,
    ) -> Pipeline {
        Command::new("producer")
            .cancel_on(first_token)
            .pipe(Command::new("consumer").cancel_on(second_token))
            .cancel_on(chain_token)
            .with_test_runner(runner)
    }

    #[tokio::test]
    async fn capture_uses_the_chain_token_when_every_stage_owns_a_distinct_token() {
        let first_token = tokio_util::sync::CancellationToken::new();
        let second_token = tokio_util::sync::CancellationToken::new();
        let chain_token = tokio_util::sync::CancellationToken::new();
        let pipeline = pending_pipeline_with_distinct_tokens(
            first_token.clone(),
            second_token.clone(),
            chain_token.clone(),
        );
        let capture = tokio::spawn(async move { pipeline.output_string().await });

        tokio::task::yield_now().await;
        chain_token.cancel();
        let err = tokio::time::timeout(Duration::from_secs(5), capture)
            .await
            .expect("chain cancellation must bound the hermetic capture")
            .expect("capture task")
            .expect_err("the chain token must cancel every explicitly-tokened stage");
        assert!(matches!(err.reason(), crate::ErrorReason::Cancelled { .. }));
        assert!(!first_token.is_cancelled() && !second_token.is_cancelled());
    }

    #[tokio::test]
    async fn session_uses_the_chain_token_when_every_stage_owns_a_distinct_token() {
        let first_token = tokio_util::sync::CancellationToken::new();
        let second_token = tokio_util::sync::CancellationToken::new();
        let chain_token = tokio_util::sync::CancellationToken::new();
        let pipeline = pending_pipeline_with_distinct_tokens(
            first_token.clone(),
            second_token.clone(),
            chain_token.clone(),
        );
        let session = pipeline.start().await.expect("start hermetic live session");

        chain_token.cancel();
        let err = tokio::time::timeout(Duration::from_secs(5), session.finish())
            .await
            .expect("chain cancellation must bound the hermetic live session")
            .expect_err("the chain token must cancel every explicitly-tokened stage");
        assert!(matches!(err.reason(), crate::ErrorReason::Cancelled { .. }));
        assert!(!first_token.is_cancelled() && !second_token.is_cancelled());
    }

    #[tokio::test]
    async fn capture_precancelled_chain_does_not_launch_an_explicitly_tokened_stage() {
        let first_token = tokio_util::sync::CancellationToken::new();
        let second_token = tokio_util::sync::CancellationToken::new();
        let chain_token = tokio_util::sync::CancellationToken::new();
        chain_token.cancel();
        let runner = Arc::new(crate::doubles::RecordingRunner::new(
            crate::doubles::ScriptedRunner::new().fallback(crate::doubles::Reply::not_found()),
        ));
        let pipeline = pipeline_with_distinct_tokens(
            first_token.clone(),
            second_token.clone(),
            chain_token,
            runner.clone(),
        );

        let err = pipeline
            .output_string()
            .await
            .expect_err("a pre-cancelled chain must short-circuit capture");
        assert!(
            matches!(
                err.reason(),
                crate::ErrorReason::Cancelled { program } if program == "producer"
            ),
            "the chain cancellation must win before the first scripted spawn: {err:?}"
        );
        assert!(
            runner.calls().is_empty(),
            "capture must not ask the runner to spawn any stage"
        );
        assert!(!first_token.is_cancelled() && !second_token.is_cancelled());
    }

    #[tokio::test]
    async fn start_precancelled_chain_does_not_launch_an_explicitly_tokened_stage() {
        let first_token = tokio_util::sync::CancellationToken::new();
        let second_token = tokio_util::sync::CancellationToken::new();
        let chain_token = tokio_util::sync::CancellationToken::new();
        chain_token.cancel();
        let runner = Arc::new(crate::doubles::RecordingRunner::new(
            crate::doubles::ScriptedRunner::new().fallback(crate::doubles::Reply::not_found()),
        ));
        let pipeline = pipeline_with_distinct_tokens(
            first_token.clone(),
            second_token.clone(),
            chain_token,
            runner.clone(),
        );

        let err = pipeline
            .start()
            .await
            .expect_err("a pre-cancelled chain must short-circuit session start");
        assert!(
            matches!(
                err.reason(),
                crate::ErrorReason::Cancelled { program } if program == "producer"
            ),
            "the chain cancellation must win before the first scripted spawn: {err:?}"
        );
        assert!(
            runner.calls().is_empty(),
            "start must not ask the runner to spawn any stage"
        );
        assert!(!first_token.is_cancelled() && !second_token.is_cancelled());
    }

    #[tokio::test]
    async fn cancellation_between_launches_prevents_the_next_explicitly_tokened_stage() {
        let first_token = tokio_util::sync::CancellationToken::new();
        let second_token = tokio_util::sync::CancellationToken::new();
        let chain_token = tokio_util::sync::CancellationToken::new();
        let cancel_during_first_launch = chain_token.clone();
        let scripted = crate::doubles::ScriptedRunner::new()
            .when(
                move |command| {
                    if command.program() == std::ffi::OsStr::new("producer") {
                        cancel_during_first_launch.cancel();
                        true
                    } else {
                        false
                    }
                },
                crate::doubles::Reply::pending(),
            )
            .on(["consumer"], crate::doubles::Reply::not_found());
        let runner = Arc::new(crate::doubles::RecordingRunner::new(scripted));
        let pipeline = pipeline_with_distinct_tokens(
            first_token.clone(),
            second_token.clone(),
            chain_token,
            runner.clone(),
        );

        let err = pipeline
            .output_string()
            .await
            .expect_err("cancelling after stage one must stop before stage two");
        assert!(
            matches!(
                err.reason(),
                crate::ErrorReason::Cancelled { program } if program == "consumer"
            ),
            "the chain cancellation must beat the second stage's NotFound: {err:?}"
        );
        let calls = runner.calls();
        assert_eq!(calls.len(), 1, "only the first stage may reach the runner");
        assert_eq!(calls[0].program, std::ffi::OsStr::new("producer"));
        assert!(!first_token.is_cancelled() && !second_token.is_cancelled());
    }

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

    #[cfg(feature = "pty")]
    #[tokio::test]
    async fn non_final_pty_stage_is_rejected_before_any_spawn() {
        let error = Command::new("never-spawn-first")
            .use_pty()
            .stdout(crate::StdioMode::Null)
            .pipe(Command::new("never-spawn-second"))
            .start()
            .await
            .expect_err("a PTY cannot provide the next stage's stdin pipe");
        assert!(
            matches!(
                error.reason(),
                crate::ErrorReason::Unsupported { operation }
                    if operation.contains("use_pty") && operation.contains("stage 1")
            ),
            "the wiring error must be typed and identify the non-final stage: {error:?}"
        );
    }

    #[cfg(feature = "pty")]
    #[tokio::test]
    async fn final_pty_destination_is_rejected_before_any_pipeline_spawn_or_file_effect() {
        let dir = tempfile::tempdir().expect("temp dir");
        let preserved = dir.path().join("preserved.log");
        let absent = dir.path().join("never-created.log");
        std::fs::write(&preserved, "sentinel").expect("seed preserved redirect");
        let runner = Arc::new(crate::doubles::RecordingRunner::new(
            crate::doubles::ScriptedRunner::new().fallback(crate::doubles::Reply::not_found()),
        ));
        let pipeline = Command::new("never-spawn-producer")
            .pipe(
                Command::new("never-spawn-final")
                    .use_pty()
                    .stdout_file(&preserved)
                    .stderr_file(&absent),
            )
            .with_test_runner(runner.clone());

        let error = pipeline
            .start()
            .await
            .expect_err("an incompatible final PTY destination must reject the whole chain");
        assert!(
            matches!(
                error.reason(),
                crate::ErrorReason::Unsupported { operation }
                    if operation == "use_pty with a non-piped stdout destination"
            ),
            "the command-level PTY destination diagnosis must be retained: {error:?}"
        );
        assert!(
            runner.calls().is_empty(),
            "preflight must not ask the runner to spawn an upstream stage"
        );
        assert_eq!(
            std::fs::read_to_string(&preserved).expect("read preserved redirect"),
            "sentinel",
            "preflight must not truncate the configured stdout redirect"
        );
        assert!(
            !absent.exists(),
            "preflight must not create the configured stderr redirect"
        );
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
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Exit {
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
            other => panic!("expected ErrorReason::Exit, got {other:?}"),
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
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Exit { program, .. }) => {
                assert_eq!(program, "a", "...and so does the error surface");
            }
            other => panic!("expected ErrorReason::Exit, got {other:?}"),
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
    fn chain_timeout_keeps_pipefail_stderr_and_truncation() {
        let mut culprit = stage("culprit", Outcome::Signalled(None));
        culprit.stderr = "retained diagnostic".into();
        culprit.stderr_truncated = true;
        let finished = timeout_finished(
            Ok(vec![(0, culprit)]),
            Ok(Finished {
                outcome: Outcome::Signalled(None),
                stderr: String::new(),
                stderr_truncated: false,
            }),
            "last",
            &[0],
            false,
            None,
            false,
        );

        assert_eq!(finished.outcome, Outcome::TimedOut);
        assert_eq!(finished.stderr, "retained diagnostic");
        assert!(finished.stderr_truncated);
    }

    #[test]
    fn chain_timeout_keeps_completed_stderr_when_another_stage_returns_raw_error() {
        let mut culprit = unclean("completed", Outcome::Exited(7), "retained diagnostic");
        culprit.stderr_truncated = true;
        let finished = timeout_finished(
            Err(PartialDrainError {
                completed: vec![(0, culprit)],
                error: crate::Error::io(std::io::Error::other("raw stage error")),
            }),
            Ok(Finished {
                outcome: Outcome::Signalled(None),
                stderr: String::new(),
                stderr_truncated: false,
            }),
            "last",
            &[0],
            false,
            None,
            false,
        );

        assert_eq!(finished.outcome, Outcome::TimedOut);
        assert_eq!(finished.stderr, "retained diagnostic");
        assert!(finished.stderr_truncated);
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
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Timeout {
                program, timeout, ..
            }) => {
                assert_eq!(program, "slow");
                assert_eq!(
                    timeout,
                    Duration::from_millis(500),
                    "the stage's own deadline, not the chain's 0ns"
                );
            }
            other => panic!("expected ErrorReason::Timeout, got {other:?}"),
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
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Exit { program, code, .. }) => {
                assert_eq!(program, "downstream");
                assert_eq!(code, 3);
            }
            other => panic!("expected ErrorReason::Exit, got {other:?}"),
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
    fn a_stage_disposition_is_the_first_observers_and_stays_it() {
        // The last stage has two possible observers of one exit — the standing
        // watcher's probe and `finish`'s own drive — and the second one always looks
        // later, once the teardown the *first* one fired is already in flight. If the
        // later read won, the stage would read its own teardown as a sibling's and
        // demote itself from culprit to victim.
        let culprit = ExitDisposition::unobserved();
        assert!(
            !culprit.latch(false),
            "no teardown in flight at the exit: a culprit"
        );
        assert!(
            !culprit.latch(true),
            "a later observer reads the latched verdict, it never overwrites it"
        );

        // And symmetrically: a stage that died into a sibling's teardown stays a
        // victim even if the token were somehow read as clear afterwards.
        let victim = ExitDisposition::unobserved();
        assert!(victim.latch(true), "a teardown was already in flight");
        assert!(victim.latch(false), "still a victim on a later read");
    }

    #[test]
    fn a_slow_draining_culprit_is_not_demoted_by_a_later_failure() {
        // The fold half of the same rule. An inner stage exits non-zero first, but a
        // grandchild it forked inherited its stderr pipe, so its drain only ends long
        // after the last stage has failed too. Latched at their exits, neither stage
        // saw a teardown in flight, so neither is a victim and the leftmost genuine
        // failure is blamed — the attribution an all-fast chain would have reached.
        // Reading `torn_down` after the drain instead would mark the upstream a
        // victim and hand the chain's exit code and stderr to the downstream failure.
        let slow_draining_culprit = unclean("upstream", Outcome::Exited(3), "the real failure");
        let later_failure = last(Outcome::Exited(5), false);
        let result = pf(vec![slow_draining_culprit], later_failure, "");
        assert_eq!(
            result.program(),
            "upstream",
            "the stage that failed first is blamed, however long its drain took"
        );
        assert_eq!(result.code(), Some(3));
        assert_eq!(result.stderr(), "the real failure");
    }

    #[cfg(unix)]
    #[test]
    fn a_slow_draining_last_stage_keeps_the_blame_from_a_sigpipe_producer() {
        // The fold half for the shape where the *last* stage's own latch decides,
        // which is the buffering `output_string`/`output_bytes` path's canonical
        // case: the last stage fails first while a grandchild holds its stderr open,
        // and the producer then dies of SIGPIPE against the pipe that failure
        // closed. Latched at its exit the last stage saw no teardown in flight, so
        // it stays a genuine culprit — and the fold prefers it over the SIGPIPE
        // victim, reporting the failure a user can act on.
        let result = pf(
            vec![unclean(
                "producer",
                Outcome::Signalled(Some(13)),
                "producer noise",
            )],
            last(Outcome::Exited(3), false),
            "",
        );
        assert_eq!(
            result.code(),
            Some(3),
            "the last stage failed on its own and is not a teardown victim"
        );
        assert_eq!(result.stderr(), "last-err", "with its own diagnostics");

        // The counterfactual, i.e. exactly what reading `torn_down` *after* the
        // drain used to produce here: the producer's SIGPIPE death fires the chain's
        // teardown while the last stage is still draining, so a post-drain read
        // demotes the stage that died first to a victim. No un-torn, non-SIGPIPE
        // candidate is then left and the fold falls back to the leftmost — handing
        // the user the producer's signal and stderr instead of the real failure.
        let demoted = StageOutcome {
            torn_down: true,
            ..last(Outcome::Exited(3), false)
        };
        let regressed = pf(
            vec![unclean(
                "producer",
                Outcome::Signalled(Some(13)),
                "producer noise",
            )],
            demoted,
            "",
        );
        assert!(
            matches!(regressed.outcome(), Outcome::Signalled(Some(13))),
            "the demotion is user-visible, not cosmetic: {:?}",
            regressed.outcome()
        );
        assert_eq!(regressed.stderr(), "producer noise");
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
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Signalled {
                program, signal, ..
            }) => {
                assert_eq!(program, "a");
                assert_eq!(signal, None);
            }
            other => panic!("expected ErrorReason::Signalled, got {other:?}"),
        }
    }

    // T-085: `drain_unordered` regression coverage. These are hermetic — no
    // real process is spawned — and exercise the exact liveness bug: a raw
    // `Err` (or a task panic) on one task must fire `teardown` and let the
    // whole drain finish, even while a *sibling* task is deliberately still
    // pending. The quiet sibling below is a "controlled future": it can only
    // resolve once `teardown` actually fires, so if `drain_unordered`
    // regressed to a positional (left-to-right) gather that stalls on a
    // still-pending earlier task, these tests hang instead of failing fast —
    // wrapped in `tokio::time::timeout` so a regression is reported as a
    // failure rather than a wedged test run.

    /// A task that can only ever resolve once `teardown` fires — the
    /// hermetic stand-in for a quiet upstream stage that never writes and so
    /// never dies on its own; only the chain's proactive teardown ends it.
    async fn quiet_until_teardown(
        teardown: tokio_util::sync::CancellationToken,
        item: &'static str,
    ) -> Result<&'static str> {
        teardown.cancelled().await;
        Ok(item)
    }

    #[tokio::test]
    async fn drain_unordered_wakes_a_quiet_task_when_a_later_task_returns_a_raw_error() {
        let teardown = tokio_util::sync::CancellationToken::new();
        let teardown_cause: PipelineTeardownCause = Arc::default();
        let mut tasks: tokio::task::JoinSet<Result<&'static str>> = tokio::task::JoinSet::new();
        // Spawned first (the "earlier, leftmost" stage in `capture`'s terms) —
        // a positional gather would await this one to completion before ever
        // looking at the task below, and it never completes on its own.
        tasks.spawn(quiet_until_teardown(teardown.clone(), "quiet-upstream"));
        // Spawned second (the "later" stage) but ready immediately — the raw
        // `Err` a checked-failure path never produces, so nothing but the
        // centralized `drain_unordered` firing `teardown` on its behalf can
        // wake the sibling above.
        tasks.spawn(async { Err(crate::Error::io(std::io::Error::other("downstream boom"))) });

        let drained = tokio::time::timeout(
            Duration::from_secs(5),
            drain_unordered(tasks, &teardown, &teardown_cause),
        )
        .await
        .expect(
            "drain_unordered must not hang on the still-pending quiet task \
             once the sibling's raw error has fired teardown",
        );

        match drained.map_err(|e| e.error.into_reason()) {
            Err(crate::ErrorReason::Io(err)) => {
                assert_eq!(err.to_string(), "downstream boom");
            }
            other => panic!("expected the downstream stage's own Io error, got {other:?}"),
        }
        assert!(
            teardown.is_cancelled(),
            "a raw Err must fire teardown so a quiet sibling is unblocked"
        );
        assert_eq!(
            teardown_cause.get(),
            Some(&crate::TeardownCause::PipelineFailure),
            "a raw non-cancellation error initiates pipeline-failure teardown"
        );
    }

    #[tokio::test]
    async fn drain_unordered_collects_a_sibling_teardown_after_the_first_raw_error() {
        let teardown = tokio_util::sync::CancellationToken::new();
        let teardown_cause: PipelineTeardownCause = Arc::default();
        let sibling_settled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut tasks: tokio::task::JoinSet<Result<&'static str>> = tokio::task::JoinSet::new();
        tasks.spawn(async {
            Err(crate::Error::io(std::io::Error::other(
                "first cancelled-like error",
            )))
        });
        tasks.spawn({
            let teardown = teardown.clone();
            let sibling_settled = sibling_settled.clone();
            async move {
                teardown.cancelled().await;
                sibling_settled.store(true, Ordering::Release);
                Err(crate::Error::teardown(
                    "sibling",
                    crate::TeardownCause::Cancellation,
                    "process-group hard kill",
                    std::io::Error::from_raw_os_error(5),
                    "prefix".into(),
                    String::new(),
                    None,
                ))
            }
        });

        let failure = tokio::time::timeout(
            Duration::from_secs(5),
            drain_unordered(tasks, &teardown, &teardown_cause),
        )
        .await
        .expect("terminal sibling collection must remain bounded")
        .expect_err("the retained sibling teardown must fail the drain");

        assert!(
            sibling_settled.load(Ordering::Acquire),
            "the first raw error must not abort the sibling disposition task"
        );
        match failure.error.reason() {
            crate::ErrorReason::Teardown { cause, source, .. } => {
                assert_eq!(*cause, crate::TeardownCause::Cancellation);
                assert_eq!(source.raw_os_error(), Some(5));
            }
            other => panic!("a sibling Teardown must outrank the first raw error: {other:?}"),
        }
        assert_eq!(
            teardown_cause.get(),
            Some(&crate::TeardownCause::PipelineFailure),
            "the later sibling Teardown cannot rewrite the first raw-error trigger"
        );
    }

    #[tokio::test]
    async fn drain_unordered_fires_teardown_on_a_task_panic_so_a_quiet_sibling_still_resolves() {
        let teardown = tokio_util::sync::CancellationToken::new();
        let teardown_cause: PipelineTeardownCause = Arc::default();
        // Synchronizes the two tasks so the panic only happens once the quiet
        // task has genuinely started waiting on `teardown` — proving this is
        // a real concurrent-blocking scenario, not a fluke of scheduling
        // order where the quiet task happened to run (and finish) first.
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

        let mut tasks: tokio::task::JoinSet<Result<&'static str>> = tokio::task::JoinSet::new();
        tasks.spawn({
            let teardown = teardown.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                quiet_until_teardown(teardown, "quiet-upstream").await
            }
        });
        tasks.spawn(async move {
            barrier.wait().await;
            panic!("downstream stage task panicked");
        });

        let drained = tokio::time::timeout(
            Duration::from_secs(5),
            drain_unordered(tasks, &teardown, &teardown_cause),
        )
        .await
        .expect(
            "drain_unordered must not hang on the still-pending quiet task \
             once the sibling's panic has fired teardown",
        );

        match drained.map_err(|e| e.error.into_reason()) {
            Err(crate::ErrorReason::Io(err)) => {
                assert!(
                    err.to_string().contains("pipeline stage task failed"),
                    "expected the wrapped JoinError, got {err}"
                );
            }
            other => panic!("expected a wrapped JoinError, got {other:?}"),
        }
        assert!(
            teardown.is_cancelled(),
            "a task panic must fire teardown so a quiet sibling is unblocked"
        );
        assert_eq!(
            teardown_cause.get(),
            Some(&crate::TeardownCause::PipelineFailure),
            "a task panic initiates pipeline-failure teardown"
        );
    }

    #[tokio::test]
    async fn drain_unordered_returns_every_item_when_the_whole_set_finishes_clean() {
        let teardown = tokio_util::sync::CancellationToken::new();
        let teardown_cause: PipelineTeardownCause = Arc::default();
        let mut tasks: tokio::task::JoinSet<Result<u32>> = tokio::task::JoinSet::new();
        for item in [1u32, 2, 3] {
            tasks.spawn(async move {
                tokio::task::yield_now().await;
                Ok(item)
            });
        }

        let mut drained = drain_unordered(tasks, &teardown, &teardown_cause)
            .await
            .expect("every task succeeded");
        drained.sort_unstable();
        assert_eq!(
            drained,
            vec![1, 2, 3],
            "every task's payload survives the drain, completion order aside"
        );
        assert!(
            !teardown.is_cancelled(),
            "an all-clean drain must never fire teardown"
        );
        assert!(
            teardown_cause.get().is_none(),
            "an all-clean drain must never publish a teardown cause"
        );
    }
}
