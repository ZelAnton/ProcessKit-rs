//! [`RunningProcess`] — a live handle to a spawned child.
//!
//! Split by concern: this file owns the handle's state and the consuming
//! capture paths (exit driving, kill/teardown, the post-exit checkpoint);
//! [`probes`] holds the non-consuming readiness probes; [`stream`] holds the
//! incremental stdout streaming surface.

pub(crate) mod deadline;
mod probes;
mod scripted;
mod stream;

#[cfg(feature = "json")]
pub use stream::JsonLines;
pub use stream::{Finished, OutputLine, ProcessEvent, ProcessEvents, StdoutLines};
// Re-exported so `crate::doubles`/`crate::cassette` keep addressing these at
// `crate::running::...` even though they now live in the `scripted` submodule.
pub(crate) use scripted::{
    ScriptedOutcome, ScriptedProc, ScriptedResultInfo, split_pump_frames, split_pump_lines,
};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

// The timeout arbiter (`timeout_state`) is built from the crate's
// `cfg(loom)`-swappable sync layer so its `PENDING → TIMED_OUT`/`EXITED` CAS
// protocol — funnelled through `deadline::claim_timed_out`/`claim_exited` — can be
// loom-modeled; `std::sync::atomic::AtomicU8` in ordinary builds. See `crate::sync`.
use crate::sync::atomic::AtomicU8;

use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdin};
use tokio::task::JoinHandle;

use crate::buffer::{OutputBufferPolicy, OverflowMode, clamp_dropoldest_tail, push_capped_bytes};
use crate::error::Result;
use crate::error::{Error, ErrorReason, TeardownCause};
use crate::group::ProcessGroup;
use crate::pump::{OutputActivity, SharedLines, StreamConfig, pump_lines_core};
use crate::result::{Outcome, ProcessResult};
use crate::stdin::ProcessStdin;
use crate::sys::pid_gate::PidGate;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
pub(crate) const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

#[cfg(all(test, feature = "process-control"))]
#[derive(Debug)]
struct DeferredBackendWaitFailure {
    entered: AtomicBool,
    entered_changed: tokio::sync::Notify,
    released: AtomicBool,
    release_changed: tokio::sync::Notify,
    raw_os_error: i32,
}

#[cfg(all(test, feature = "process-control"))]
impl DeferredBackendWaitFailure {
    async fn wait_until_entered(&self) {
        loop {
            let changed = self.entered_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.entered.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }

    async fn wait_until_released(&self) {
        loop {
            let changed = self.release_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_changed.notify_waiters();
    }
}

#[cfg(all(test, feature = "process-control"))]
thread_local! {
    static BACKEND_WAIT_FAILURE: std::cell::RefCell<Option<Arc<DeferredBackendWaitFailure>>> =
        const { std::cell::RefCell::new(None) };
}

/// Test-only production-wiring seam: the next `backend_wait` reaches its normal
/// ownership boundary, then blocks until released and returns the requested OS
/// error. This lets pipeline tests race a genuine wait failure against their
/// chain deadline without faking `PipelineTerminalState` calls.
#[cfg(all(test, feature = "process-control"))]
pub(crate) struct BackendWaitFailureGuard {
    failure: Arc<DeferredBackendWaitFailure>,
}

#[cfg(all(test, feature = "process-control"))]
impl BackendWaitFailureGuard {
    pub(crate) async fn wait_until_entered(&self) {
        self.failure.wait_until_entered().await;
    }

    pub(crate) fn release(&self) {
        self.failure.release();
    }
}

#[cfg(all(test, feature = "process-control"))]
impl Drop for BackendWaitFailureGuard {
    fn drop(&mut self) {
        self.failure.release();
        BACKEND_WAIT_FAILURE.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|failure| Arc::ptr_eq(failure, &self.failure))
            {
                slot.take();
            }
        });
    }
}

#[cfg(all(test, feature = "process-control"))]
pub(crate) fn defer_next_backend_wait_error(raw_os_error: i32) -> BackendWaitFailureGuard {
    let failure = Arc::new(DeferredBackendWaitFailure {
        entered: AtomicBool::new(false),
        entered_changed: tokio::sync::Notify::new(),
        released: AtomicBool::new(false),
        release_changed: tokio::sync::Notify::new(),
        raw_os_error,
    });
    BACKEND_WAIT_FAILURE.with(|slot| {
        assert!(
            slot.borrow_mut().replace(failure.clone()).is_none(),
            "backend wait failure seam already armed"
        );
    });
    BackendWaitFailureGuard { failure }
}

#[cfg(all(test, feature = "process-control"))]
fn take_backend_wait_failure() -> Option<Arc<DeferredBackendWaitFailure>> {
    BACKEND_WAIT_FAILURE.with(|slot| slot.borrow_mut().take())
}

#[cfg(all(test, feature = "process-control"))]
thread_local! {
    static RAW_STDOUT_TEST_TX:
        std::cell::RefCell<Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>> =
        const { std::cell::RefCell::new(None) };
}

/// Test-only observation of bytes accepted by the production raw stdout pump.
/// The guard lets pipeline regressions synchronize teardown with a real captured
/// prefix instead of relying on a scheduler delay after the child wrote it.
#[cfg(all(test, feature = "process-control"))]
pub(crate) struct RawStdoutPublicationGuard {
    receiver: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    observed: Vec<u8>,
}

#[cfg(all(test, feature = "process-control"))]
impl RawStdoutPublicationGuard {
    pub(crate) async fn wait_until_contains(&mut self, expected: &[u8]) {
        if expected.is_empty() {
            return;
        }
        while !self
            .observed
            .windows(expected.len())
            .any(|window| window == expected)
        {
            let chunk = self
                .receiver
                .recv()
                .await
                .expect("raw stdout publication sender remains installed");
            self.observed.extend_from_slice(&chunk);
        }
    }
}

#[cfg(all(test, feature = "process-control"))]
impl Drop for RawStdoutPublicationGuard {
    fn drop(&mut self) {
        RAW_STDOUT_TEST_TX.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(all(test, feature = "process-control"))]
pub(crate) fn observe_raw_stdout_publications() -> RawStdoutPublicationGuard {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    RAW_STDOUT_TEST_TX.with(|slot| {
        assert!(
            slot.borrow_mut().replace(sender).is_none(),
            "raw stdout publication observer already installed"
        );
    });
    RawStdoutPublicationGuard {
        receiver,
        observed: Vec::new(),
    }
}

#[cfg(all(test, feature = "process-control"))]
fn publish_raw_stdout_for_test(chunk: &[u8]) {
    RAW_STDOUT_TEST_TX.with(|slot| {
        if let Some(sender) = slot.borrow().as_ref() {
            let _ = sender.send(chunk.to_vec());
        }
    });
}

/// In-flight byte cap for the discard sink used by `wait`/`profile`. These verbs
/// retain no lines, but the pump still assembles each line in memory before
/// discarding it; without a byte cap a newline-free flood (e.g. `base64 -w0`)
/// or a single enormous terminated line would grow that in-flight buffer to
/// O(total) and can OOM. The cap bounds it to a fixed ceiling. It is set large
/// enough that no realistically-sized line is affected: the only observable
/// consequence is that a single line whose content exceeds this cap is not
/// delivered to a per-line handler or [`stdout_tee`](crate::Command::stdout_tee)
/// during a `wait`/`profile` (the same skip a user-set byte cap already applies
/// to over-cap lines) — an acceptable trade against an unbounded-memory crash.
const DISCARD_INFLIGHT_CAP: usize = 64 << 20;

// Timeout-arbitration states for `RunningProcess::timeout_state`. Whichever of
// the natural reap (claims `EXITED`) or a fired deadline (claims `TIMED_OUT`)
// first `compare_exchange`s from `PENDING` wins — a single CAS arbiter that
// keeps "timed out vs exited" race-free even when the child exits within a
// scheduler quantum of the deadline.
// `pub(crate)` so `Pipeline::start`'s chain-wide deadline arbiter (a live
// streaming session, `src/pipeline.rs`) can initialize its own arbiter word to
// the same `PENDING` start state — it reuses the shared `deadline` claim helpers
// rather than duplicating the CAS protocol (see K-034).
pub(crate) const TS_PENDING: u8 = 0;
const TS_EXITED: u8 = 1;
// `pub(crate)` so `first_line` (in `crate::runner`) can classify a timed-out
// streamed run: the deadline watchdog stores `TS_TIMED_OUT` *before* it kills, so
// reading it after the stream closes distinguishes a deadline kill from a natural
// end race-free.
pub(crate) const TS_TIMED_OUT: u8 = 2;
pub(crate) const TS_INACTIVITY_TIMED_OUT: u8 = 3;

/// Why a reap-via-wait ended — the race result, not a post-hoc token read.
enum ExitCause {
    /// Child exited on its own (or deadline fired). Cancellation did not win.
    Exited(Outcome),
    /// Cancel arm won and terminal teardown was confirmed. Becomes `Err(Cancelled)`.
    Cancelled,
    /// The initiating terminal condition won, but teardown was not confirmed.
    /// The original OS error is retained in `teardown_failure` until the pumps
    /// finish and the consuming surface can attach their captured prefix.
    TeardownFailed { intended: Outcome, cancelled: bool },
}

/// What the consuming finisher proved at its wait boundary. Pipeline teardown
/// needs the distinction: reaching `drive_to_exit` is useful for causal
/// attribution, but only the `Reaped` arm may discharge a stage's terminal
/// confirmation obligation.
pub(crate) enum ExitObservation {
    Reaped,
    Unconfirmed {
        cause: Option<TeardownCause>,
        source: std::io::Error,
    },
}

/// One unconfirmed terminal teardown, shared with detached watchdogs so a
/// consuming finisher cannot silently turn their OS failure into a timeout or
/// cancellation disposition.
#[derive(Debug)]
struct TeardownFailure {
    cause: TeardownCause,
    operation: &'static str,
    source: std::io::Error,
}

type SharedTeardownFailure = Arc<std::sync::Mutex<Option<TeardownFailure>>>;

fn record_teardown_failure(slot: &SharedTeardownFailure, failure: TeardownFailure) {
    let mut guard = slot.lock().expect("teardown failure slot poisoned");
    if guard.is_none() {
        *guard = Some(failure);
    }
}

fn clone_io_error(source: &std::io::Error) -> std::io::Error {
    source.raw_os_error().map_or_else(
        || std::io::Error::new(source.kind(), source.to_string()),
        std::io::Error::from_raw_os_error,
    )
}

fn child_start_kill(
    target: &'static str,
    kill: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(injected) = crate::sys::fault_injection::check(
        crate::sys::fault_injection::Site::DirectChildKill,
        target,
    ) {
        return Err(injected);
    }
    #[cfg(not(test))]
    let _ = target;
    match kill() {
        Ok(()) => Ok(()),
        // Tokio treats an already-reaped child as a no-op today. Preserve that
        // routine race explicitly if its implementation ever returns this kind.
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) fn group_hard_kill(group: &ProcessGroup) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(injected) = crate::sys::fault_injection::check(
        crate::sys::fault_injection::Site::ProcessGroupTeardown,
        "hard",
    ) {
        return Err(injected);
    }
    group.kill_all_io()
}

async fn group_graceful_kill(
    group: &ProcessGroup,
    grace: Duration,
    signal: i32,
) -> std::io::Result<()> {
    #[cfg(test)]
    if let Some(injected) = crate::sys::fault_injection::check(
        crate::sys::fault_injection::Site::ProcessGroupTeardown,
        "graceful",
    ) {
        return Err(injected);
    }
    group.graceful_terminate_io(grace, signal).await
}

async fn confirm_reap<T>(
    budget: Duration,
    wait: impl std::future::Future<Output = std::io::Result<T>>,
) -> std::io::Result<()> {
    match tokio::time::timeout(budget, wait).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "child did not reach a confirmed terminal state during teardown",
        )),
    }
}

/// Wait until either of the two independent cancellation sources fires.
///
/// This future deliberately observes the original tokens rather than a derived
/// token published by another task: a consuming waiter must see a token that was
/// already cancelled before it was polled, even if no spawned watchdog has run.
async fn wait_for_cancellation(
    configured: Option<tokio_util::sync::CancellationToken>,
    additional: Option<tokio_util::sync::CancellationToken>,
) {
    match (configured, additional) {
        (Some(configured), Some(additional)) => {
            tokio::select! {
                biased;
                () = configured.cancelled() => {}
                () = additional.cancelled() => {}
            }
        }
        (Some(configured), None) => configured.cancelled().await,
        (None, Some(additional)) => additional.cancelled().await,
        (None, None) => std::future::pending::<()>().await,
    }
}

/// Internal result of `finish_lines` — distinct from the public `Finished`.
struct FinishedLines {
    outcome: Outcome,
    stdout_lines: Vec<String>,
    stderr_lines: Vec<String>,
}

/// The line sinks prepared before a pipeline moves its last stage into a task.
/// Keeping these `Arc`s outside the task frame lets the chain-wide timeout
/// salvage the prefix that the pump had retained when the task is cancelled.
#[derive(Clone)]
pub(crate) struct LineCapture {
    stdout: Arc<SharedLines>,
    stderr: Arc<SharedLines>,
    stdout_config: StreamConfig,
    stderr_config: StreamConfig,
}

/// A non-consuming stderr-only checkpoint for a live process.
///
/// Unlike [`LineCapture`], preparing this checkpoint never depends on stdout
/// being piped or still available. Pipeline teardown uses it after normal stdout
/// streaming has already become unavailable, while the process finisher remains
/// the sole consumer of the shared stderr sink.
#[derive(Clone)]
pub(crate) struct StderrCapture {
    stderr: Arc<SharedLines>,
    stderr_config: StreamConfig,
}

impl StderrCapture {
    /// Retain the decoded backlog plus a live unterminated tail without draining
    /// either, so the ordinary finisher can still consume the same sink.
    pub(crate) fn retained_snapshot(&self) -> (String, bool, usize, usize) {
        let stderr = self
            .stderr
            .retained_snapshot(|tail| self.stderr_config.shape_capture_line(tail))
            .join("\n");
        (
            stderr,
            self.stderr.dropped() > 0,
            self.stderr.count(),
            self.stderr.seen_bytes(),
        )
    }
}

impl LineCapture {
    /// Non-destructive complete-line view used just before a pipeline fallback
    /// kill. If that kill wakes this capture's finisher, the chain error can still
    /// retain the prefix even though the finisher drains the shared sinks first.
    pub(crate) fn retained_snapshot(&self) -> (String, String, bool, usize, usize) {
        let stdout = self
            .stdout
            .retained_snapshot(|tail| self.stdout_config.shape_capture_line(tail))
            .join("\n");
        let stderr = self
            .stderr
            .retained_snapshot(|tail| self.stderr_config.shape_capture_line(tail))
            .join("\n");
        (
            stdout,
            stderr,
            self.stdout.dropped() > 0 || self.stderr.dropped() > 0,
            self.stdout.count().saturating_add(self.stderr.count()),
            self.stdout
                .seen_bytes()
                .saturating_add(self.stderr.seen_bytes()),
        )
    }

    /// The best-effort capture a chain-wide timeout salvages: each stream's
    /// still-pending partial tail folded into its backlog, and the backlog taken.
    ///
    /// The pumps may still be alive here — dropping the capture task only
    /// *requests* their abort — so each stream does both steps in **one**
    /// critical section ([`SharedLines::drain_with_partial_tail`]). Folding and
    /// draining separately let a live pump push the completed line whose prefix
    /// this tail is, in between, and the salvaged output then repeated that
    /// prefix. A push that lands after the drain is still lost (the documented
    /// best-effort degradation) — but never duplicated.
    pub(crate) fn snapshot(&self) -> (String, String, bool, usize, usize) {
        let stdout_lines = self
            .stdout
            .drain_with_partial_tail(|tail| self.stdout_config.shape_capture_line(tail));
        let stderr_lines = self
            .stderr
            .drain_with_partial_tail(|tail| self.stderr_config.shape_capture_line(tail));
        // Read the totals after the folds: a salvaged tail counts as a line, and
        // an over-cap one as a drop, exactly like a line the pump completed.
        let truncated = self.stdout.dropped() > 0 || self.stderr.dropped() > 0;
        let total_lines = self.stdout.count().saturating_add(self.stderr.count());
        let total_bytes = self
            .stdout
            .seen_bytes()
            .saturating_add(self.stderr.seen_bytes());
        (
            stdout_lines.join("\n"),
            stderr_lines.join("\n"),
            truncated,
            total_lines,
            total_bytes,
        )
    }
}

/// How [`RunningProcess::finish_lines`] treats the pumped lines.
#[derive(Clone, Copy)]
enum CaptureMode {
    /// Retain both streams' lines (`output_string`).
    Lines,
    /// Pump (so the child never blocks on a full pipe) but drop the lines,
    /// bounding the in-flight assembly with a fixed internal cap
    /// ([`DISCARD_INFLIGHT_CAP`]) — the caller's `output_buffer` is ignored
    /// (`wait`/`profile`).
    Discard,
    /// Like [`Discard`](Self::Discard) — pump, feed the configured tee/per-line
    /// handlers, retain nothing, classify the outcome exactly as `wait` — but
    /// bound the in-flight assembly by the caller's configured
    /// [`Command::output_buffer`](crate::Command::output_buffer) byte cap
    /// (falling back to [`DISCARD_INFLIGHT_CAP`] when it is unbounded) so held
    /// memory tracks the *configured* limit, not the child's output size
    /// (`drain`).
    DrainBounded,
}

/// The fields produced by a spawn, handed to [`RunningProcess::from_spawned`].
pub(crate) struct Spawned {
    pub program: String,
    pub child: Child,
    pub own_group: Option<ProcessGroup>,
    pub stdout: Option<OutputReader>,
    pub stderr: Option<OutputReader>,
    pub stdin: Option<ChildStdin>,
    pub stdin_task: Option<JoinHandle<std::io::Result<()>>>,
    pub timeout: Option<Duration>,
    pub inactivity_timeout: Option<Duration>,
    /// Grace window for a graceful timeout (`None` = hard kill at the deadline).
    pub timeout_grace: Option<Duration>,
    /// Raw signal for the graceful-timeout phase (default `SIGTERM`).
    pub timeout_signal: i32,
    pub pid: Option<u32>,
    /// Per-stream pump config (encoding/handler/tee/terminator) — one value per
    /// stream, carried straight onto the [`RunningProcess`]. See [`StreamConfig`].
    pub stdout_config: StreamConfig,
    pub stderr_config: StreamConfig,
    pub buffer: OutputBufferPolicy,
    /// Exit codes treated as success (default `[0]`), carried onto the result.
    pub ok_codes: Vec<i32>,
    /// Whether stdout is `Piped` (capturable) vs `Inherit`/`Null`.
    pub stdout_piped: bool,
    /// Whether stderr is `Piped` (observable) vs `Inherit`/`Null`.
    pub stderr_piped: bool,
    pub cancel_token: Option<tokio_util::sync::CancellationToken>,
    /// Grace window for a graceful cancellation (`None` = hard kill on the token).
    pub cancel_grace: Option<Duration>,
    /// Raw signal for the graceful-cancellation phase (default `SIGTERM`).
    pub cancel_signal: i32,
}

/// The fields produced by a PTY spawn, handed to [`RunningProcess::from_pty`].
/// The PTY analogue of [`Spawned`]: one **merged** output reader (stdout+stderr
/// collapsed onto the master) instead of a stdout/stderr pair, a single-fd
/// `writer` for stdin, and a platform [`PtyChild`](crate::sys::pty::PtyChild) in
/// place of a tokio `Child`.
#[cfg(feature = "pty")]
pub(crate) struct PtySpawned {
    pub program: String,
    pub child: crate::sys::pty::PtyChild,
    /// The merged stdout+stderr, read through the standard pump.
    pub reader: OutputReader,
    /// The master's input side (stdin), unless it was moved into `stdin_task`.
    pub writer: Option<crate::sys::pty::PtyWriter>,
    pub own_group: Option<ProcessGroup>,
    pub stdin_task: Option<JoinHandle<std::io::Result<()>>>,
    pub timeout: Option<Duration>,
    pub inactivity_timeout: Option<Duration>,
    pub timeout_grace: Option<Duration>,
    pub timeout_signal: i32,
    pub pid: Option<u32>,
    /// The pump config for the merged stream (the command's stdout config).
    pub stdout_config: StreamConfig,
    pub buffer: OutputBufferPolicy,
    pub ok_codes: Vec<i32>,
    pub stdout_piped: bool,
    pub cancel_token: Option<tokio_util::sync::CancellationToken>,
    pub cancel_grace: Option<Duration>,
    pub cancel_signal: i32,
}

/// A handle to a process spawned by a runner.
pub struct RunningProcess {
    // The Option fields below encode the handle's de-facto states (fresh /
    // streaming / consumed) implicitly. No runtime state enum on purpose:
    // consuming verbs take `self` by value (double consumption is a compile
    // error), and the two &mut entry points handle a repeat call without
    // panicking — `stdout_lines`/`events` return a loud `Err`, and
    // `take_stdin` returns `None`. A state enum would only add panic paths to
    // guard doors the borrow checker already locks.
    program: String,
    /// The I/O-bearing half: a real OS child, or a scripted double feeding the
    /// same pump machinery (see [`Backend`]).
    backend: Backend,
    timeout: Option<Duration>,
    inactivity_timeout: Option<Duration>,
    timeout_grace: Option<Duration>,
    timeout_signal: i32,
    pid: Option<u32>,
    // The child's OS start-time identity, captured once at spawn (while the child
    // is provably alive). The metrics sampler and the `cpu_time`/`peak_memory_bytes`
    // accessors pass it to `process_metrics` so a reading taken against a pid the OS
    // recycled for an unrelated process — after `Child::wait` freed the number but
    // before the sampler observes `reaped` — is rejected rather than folded in.
    // `None` where the platform can't report a start identity (macOS/BSD) or the
    // capture raced the child's exit; both degrade to the number-only behavior.
    #[cfg(feature = "stats")]
    proc_identity: Option<crate::sys::ProcIdentity>,
    // Per-stream pump config (encoding/handler/tee/terminator) threaded whole into
    // every pump this handle spawns — one value per stream.
    stdout_config: StreamConfig,
    stderr_config: StreamConfig,
    buffer: OutputBufferPolicy,
    ok_codes: Vec<i32>,
    stdout_sink: Option<Arc<SharedLines>>,
    stderr_sink: Option<Arc<SharedLines>>,
    // Shared raw-capture state prepared by a pipeline before its last stage is
    // moved into the capture task. The task takes this state at the start of
    // `output_bytes`; the pipeline keeps a clone for timeout salvage.
    raw_capture: Option<RawCapture>,
    // Joined before the overflow check so the last lines are visible.
    stdout_pump: Option<JoinHandle<()>>,
    stderr_pump: Option<JoinHandle<()>>,
    // Non-broken-pipe stdin failure stashed by `observe_stdin_task`; surfaced as
    // `ErrorReason::Stdin` by `checked_outcome` only when the run otherwise succeeded.
    stdin_error: Option<std::io::Error>,
    // First terminal teardown error, preserved until the bounded pump drain can
    // attach its already-read prefix to `ErrorReason::Teardown`.
    teardown_failure: SharedTeardownFailure,
    // Test-only seam for delayed stdin-writer completion on a hermetic scripted
    // handle. Real and PTY handles keep the task in their backend-specific slot.
    #[cfg(test)]
    test_stdin_task: Option<JoinHandle<std::io::Result<()>>>,
    // Bulk capture verbs fail loudly on non-piped stdout rather than returning empty.
    stdout_piped: bool,
    // Stderr readiness probes likewise fail loudly when there is no pipe to observe.
    stderr_piped: bool,
    // Streaming deadline watchdog; aborted on drop.
    deadline_task: Option<JoinHandle<()>>,
    // Resettable output-inactivity watchdog for streamed runs; bulk finishers
    // reclaim it and race their own child-owning arm.
    inactivity_task: Option<JoinHandle<()>>,
    // One activity clock shared by stdout/stderr (PTY uses the merged stdout).
    output_activity: Arc<OutputActivity>,
    // Shared (`Arc`) because the watchdog is detached. See `TS_*` constants.
    timeout_state: Arc<AtomicU8>,
    // The linearizable gate every raw direct-child `kill(pid)` is funneled
    // through. A `Child`-owning path (a consuming finisher's `drive_to_exit_inner`,
    // `kill_tree`, `backend_wait`, `teardown_on_timeout`, or an exit probe) reaps
    // the child — which frees the pid, letting the OS recycle it — and `retire`s
    // the gate; the detached deadline and cancel watchdogs, and the shared-group
    // graceful pid-killer, issue their raw kill *inside* the gate lock, so a kill
    // and the retire are one indivisible step and a kill can never land after the
    // reap. This replaces the old `handed_off: AtomicBool`, whose separate
    // load-then-`kill(pid)` was a check-then-act race: the load proved the child
    // un-reaped only at the instant of the load, never at the instant of the kill.
    // The owner-driven paths (`drive_to_exit_inner`/`kill_tree`) retire *before*
    // they free the pid; the passive backstop (`backend_wait`) and the detached
    // Drop reaper reap the child *inside* the gate lock (polling `Child::wait()`
    // through `reap_under_lock`), so their pid-free and retire are one indivisible
    // step too — no reap→retire residual on any path. A `Drop` that hands the child
    // to no detached reaper (own-group, or a shared group without a graceful
    // window) likewise retires the gate synchronously before the structural drop
    // frees the pid, so an aborted-but-mid-poll watchdog's raw kill can't outlive
    // that free either. `Arc` because the watchdogs (and the Drop reaper) are
    // detached.
    pid_gate: Arc<PidGate>,
    cancel_token: Option<tokio_util::sync::CancellationToken>,
    // A pipeline-wide source kept separate from the command's own token. Keeping
    // both originals lets every consuming path observe either source directly;
    // forwarding through a spawned bridge task would add a scheduler race where
    // a ready child could be classified as successful before the bridge ran.
    additional_cancel_token: Option<tokio_util::sync::CancellationToken>,
    // The cancellation teardown policy — the exact mirror of
    // `timeout_grace`/`timeout_signal` for the token path (`Command::cancel_grace`/
    // `cancel_signal`). `None` (the default) keeps a cancellation an immediate hard
    // kill, byte-identically to before the knobs existed; `Some(grace)` routes EVERY
    // cancellation path this handle has — the consuming finishers'
    // `drive_to_exit_inner`, the borrowed `wait_exit` (`wait_any`/`wait_all`), and
    // the detached `cancel_task` watchdog that bounds bulk verbs and live streams —
    // through the same soft-signal → grace → hard-kill ladder the deadline uses.
    cancel_grace: Option<Duration>,
    cancel_signal: i32,
    // Armed at spawn time so every consuming path kills the tree when the token
    // fires, not just `drive_to_exit`.
    cancel_task: Option<JoinHandle<()>>,
    // Cancel disposition snapshotted at first reap (first-observation wins);
    // `None` = not yet snapshotted.
    cancel_at_exit: Option<bool>,
    // Wall-clock anchor (real `std::time::Instant`), captured at spawn. Backs the
    // wall-clock reports ONLY — `elapsed()`, and the `duration()` every capture
    // verb derives — so those keep reflecting real elapsed time, never tokio's
    // virtual clock. Deadline arithmetic deliberately does NOT read this; it
    // reads `deadline_anchor` below.
    started: Instant,
    // Deadline anchor (`tokio::time::Instant`), captured at spawn alongside
    // `started`. Every handle-level deadline measures its remaining budget from
    // HERE — the stream/scripted watchdogs (`arm_stream_deadline` /
    // `arm_scripted_deadline`), `drive_to_exit_inner`, and `shutdown`'s "already
    // elapsed?" check — so the `limit - anchor.elapsed()` arithmetic shares the
    // clock those deadlines `tokio::time::sleep` on. Under a paused runtime this
    // makes virtual time a readiness probe already burned count against the
    // limit; anchoring deadlines on `started` (the real clock) would let a late
    // arm silently re-grant the full limit — the exact hermetic-vs-live drift
    // `sys::graceful` avoids by the same deliberate split.
    deadline_anchor: tokio::time::Instant,
    start_time: SystemTime,
    // Recorded truncation/overflow/duration a cassette `start`-replay carries, so
    // a consumed replay reports them instead of the values the re-pumped canned
    // output would derive. `None` for a real child or a plain scripted reply.
    scripted_result: Option<ScriptedResultInfo>,
    // A live `events()` lifecycle stream's terminal `ProcessEvent::Exited` is fed
    // from here: the single reap choke point (`on_reaped`) publishes the run's
    // `Outcome` on this channel, which the stream drains once its output pipes
    // close. `None` unless `events()` armed a stream; a pure addition to the reap
    // path — a `send` on a dropped receiver (the stream was dropped) is ignored.
    exit_event_tx: Option<tokio::sync::oneshot::Sender<Outcome>>,
    // Set by `events()`: stderr is delivered to the caller as `ProcessEvent::Stderr`
    // events, so `finish` must NOT also drain it into `Finished::stderr` (that
    // would race the live stream for the lines). `Finished::stderr` is empty by
    // design for an events run.
    merged_events_stream: bool,
}

/// A boxed output reader: real `ChildStdout`/`ChildStderr`, scripted bytes, or a
/// PTY master. All flow through the same pump machinery via `AsyncRead`. `+ Sync`
/// keeps [`RunningProcess`] `Sync` (as it was before the PTY backend stored one on
/// `PtyProc`); every concrete reader boxed here — `ChildStdout`/`ChildStderr`, the
/// scripted `DuplexStream`, and the per-platform PTY masters — is `Sync`.
pub(crate) type OutputReader = Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>;

/// The I/O-bearing half of a [`RunningProcess`]: a real OS child, a scripted
/// double that feeds canned bytes through the same pumps/sinks, or a PTY child
/// whose merged output flows through the same pumps over a single master.
/// Platform code only ever constructs `Real`/`Pty`.
enum Backend {
    // Boxed: the variants are large and the enum lives in every handle.
    Real(Box<RealProc>),
    Scripted(Box<ScriptedProc>),
    /// A child spawned under a pseudo-terminal ([`Command::use_pty`](crate::Command::use_pty)).
    /// Its stdout and stderr are **merged** onto the single master reader, so it
    /// exposes no separate stderr; stdin is the master's input side.
    #[cfg(feature = "pty")]
    Pty(Box<PtyProc>),
}

/// The PTY-child fields. Mirrors [`RealProc`] but over a single pseudo-terminal
/// master: one merged reader (stdout+stderr collapsed), one writer (stdin), and a
/// platform [`PtyChild`](crate::sys::pty::PtyChild) lifecycle handle in place of a
/// tokio `Child`. Containment (`own_group`) and the stdin-writer task are handled
/// exactly as for a real child.
#[cfg(feature = "pty")]
struct PtyProc {
    /// The owned PTY child. `Some` for the whole live-handle lifetime; taken to
    /// `None` only by [`RunningProcess::drop`] on the detached-reap path.
    child: Option<crate::sys::pty::PtyChild>,
    own_group: Option<Arc<ProcessGroup>>,
    /// The merged stdout+stderr, read through the standard pump. Taken by the
    /// first pump that consumes it.
    reader: Option<OutputReader>,
    /// The master's input side (the child's stdin). Taken by
    /// [`take_stdin`](RunningProcess::take_stdin), or moved into `stdin_task`.
    writer: Option<crate::sys::pty::PtyWriter>,
    stdin_task: Option<JoinHandle<std::io::Result<()>>>,
}

#[cfg(feature = "pty")]
impl PtyProc {
    /// The owned PTY child (present until [`RunningProcess::drop`] extracts it on
    /// the detached-reap path — never on any live-handle path).
    fn child_mut(&mut self) -> &mut crate::sys::pty::PtyChild {
        self.child
            .as_mut()
            .expect("pty child is present until Drop extracts it")
    }
}

/// The real-child fields — exactly the ones that touch the OS.
struct RealProc {
    /// The owned OS child. `Some` for the whole live-handle lifetime; taken to
    /// `None` only by [`RunningProcess::drop`], which hands it to a detached
    /// gated reaper so tokio's orphan reaper never frees (and lets the OS
    /// recycle) the pid without the [`PidGate`] being retired first.
    child: Option<Child>,
    // `Arc` so a streaming deadline timer can hold a `Weak` to kill the tree
    // without keeping the group alive (kill-on-close on drop stays prompt).
    own_group: Option<Arc<ProcessGroup>>,
    stdout_pipe: Option<OutputReader>,
    stderr_pipe: Option<OutputReader>,
    stdin_pipe: Option<ChildStdin>,
    stdin_task: Option<JoinHandle<std::io::Result<()>>>,
}

impl RealProc {
    /// The owned child. Panics only if called after [`RunningProcess::drop`]
    /// extracted it — which never happens on any live-handle path, since `Drop`
    /// is that handle's final act.
    fn child_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("child is present until Drop extracts it")
    }
}

impl Backend {
    fn own_group(&self) -> Option<&Arc<ProcessGroup>> {
        match self {
            Backend::Real(real) => real.own_group.as_ref(),
            Backend::Scripted(s) => s.own_group(),
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => pty.own_group.as_ref(),
        }
    }

    fn scripted_kill(&self) -> Option<scripted::ScriptedKill> {
        match self {
            Backend::Real(_) => None,
            Backend::Scripted(s) => Some(s.kill_handle()),
            #[cfg(feature = "pty")]
            Backend::Pty(_) => None,
        }
    }

    fn take_stdout_reader(&mut self) -> Option<OutputReader> {
        match self {
            Backend::Real(real) => real.stdout_pipe.take(),
            Backend::Scripted(s) => s.take_stdout_reader(),
            // The PTY master carries the merged stdout+stderr.
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => pty.reader.take(),
        }
    }

    fn take_stderr_reader(&mut self) -> Option<OutputReader> {
        match self {
            Backend::Real(real) => real.stderr_pipe.take(),
            Backend::Scripted(s) => s.take_stderr_reader(),
            // PTY merges stderr into the master, so there is no separate stderr —
            // the `on_stderr_line`/stderr split collapses (documented on `use_pty`).
            #[cfg(feature = "pty")]
            Backend::Pty(_) => None,
        }
    }
}

/// The honest refusal [`RunningProcess::resize_pty`](RunningProcess::resize_pty)
/// gives for a run that has no pseudo-terminal to resize (a three-pipe child or a
/// non-PTY scripted double) — a typed [`ErrorReason::Unsupported`] naming the
/// operation, matching the crate's precedent for a refused mode-specific request.
#[cfg(feature = "pty")]
fn pty_resize_not_a_pty(program: &str) -> Error {
    ErrorReason::Unsupported {
        operation: format!("resize_pty on `{program}` (not a use_pty run)"),
    }
    .into()
}

/// The honest refusal [`RunningProcess::resize_pty`](RunningProcess::resize_pty)
/// gives once the PTY child has exited (the pseudo-terminal is gone) — a typed
/// [`ErrorReason::Unsupported`] rather than a panic or a silently-dropped resize.
#[cfg(feature = "pty")]
fn pty_resize_gone(program: &str) -> Error {
    ErrorReason::Unsupported {
        operation: format!("resize_pty on `{program}` (the process has already exited)"),
    }
    .into()
}

impl RunningProcess {
    pub(crate) fn from_spawned(s: Spawned) -> Self {
        Self {
            program: s.program,
            backend: Backend::Real(Box::new(RealProc {
                child: Some(s.child),
                own_group: s.own_group.map(Arc::new),
                stdout_pipe: s.stdout,
                stderr_pipe: s.stderr,
                stdin_pipe: s.stdin,
                stdin_task: s.stdin_task,
            })),
            timeout: s.timeout,
            inactivity_timeout: s.inactivity_timeout,
            timeout_grace: s.timeout_grace,
            timeout_signal: s.timeout_signal,
            pid: s.pid,
            // Capture the identity anchor now, while the freshly-spawned child is
            // provably alive, so a later sample can prove the pid still names it.
            #[cfg(feature = "stats")]
            proc_identity: s.pid.and_then(crate::sys::process_identity),
            stdout_config: s.stdout_config,
            stderr_config: s.stderr_config,
            buffer: s.buffer,
            ok_codes: s.ok_codes,
            stdout_sink: None,
            stderr_sink: None,
            raw_capture: None,
            stdout_pump: None,
            stderr_pump: None,
            stdin_error: None,
            teardown_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            test_stdin_task: None,
            stdout_piped: s.stdout_piped,
            stderr_piped: s.stderr_piped,
            deadline_task: None,
            inactivity_task: None,
            output_activity: Arc::new(OutputActivity::new(tokio::time::Instant::now())),
            timeout_state: Arc::new(AtomicU8::new(TS_PENDING)),
            pid_gate: Arc::new(PidGate::new(s.pid)),
            cancel_token: s.cancel_token,
            additional_cancel_token: None,
            cancel_grace: s.cancel_grace,
            cancel_signal: s.cancel_signal,
            cancel_task: None,
            cancel_at_exit: None,
            started: Instant::now(),
            // Captured next to `started` so the two anchors agree at spawn; they
            // diverge only later, under a paused runtime, where `deadline_anchor`
            // tracks tokio's virtual clock and `started` the real one.
            deadline_anchor: tokio::time::Instant::now(),
            start_time: SystemTime::now(),
            scripted_result: None,
            exit_event_tx: None,
            merged_events_stream: false,
        }
    }

    /// Build a live handle for a PTY spawn. The merged master reader flows through
    /// the same pump as a real child's stdout; there is no separate stderr, so
    /// `stderr_config`/`stderr_sink` stay at their defaults and no stderr pump ever
    /// runs (the `on_stderr_line`/stderr split collapses, per
    /// [`Command::use_pty`](crate::Command::use_pty)).
    #[cfg(feature = "pty")]
    pub(crate) fn from_pty(s: PtySpawned) -> Self {
        Self {
            program: s.program,
            backend: Backend::Pty(Box::new(PtyProc {
                child: Some(s.child),
                own_group: s.own_group.map(Arc::new),
                reader: Some(s.reader),
                writer: s.writer,
                stdin_task: s.stdin_task,
            })),
            timeout: s.timeout,
            inactivity_timeout: s.inactivity_timeout,
            timeout_grace: s.timeout_grace,
            timeout_signal: s.timeout_signal,
            pid: s.pid,
            #[cfg(feature = "stats")]
            proc_identity: s.pid.and_then(crate::sys::process_identity),
            stdout_config: s.stdout_config,
            // No separate stderr stream on a PTY — this is never read.
            stderr_config: StreamConfig::new(),
            buffer: s.buffer,
            ok_codes: s.ok_codes,
            stdout_sink: None,
            stderr_sink: None,
            raw_capture: None,
            stdout_pump: None,
            stderr_pump: None,
            stdin_error: None,
            teardown_failure: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            test_stdin_task: None,
            stdout_piped: s.stdout_piped,
            // A PTY exposes one merged stream through stdout; separate stderr is
            // intentionally unavailable.
            stderr_piped: false,
            deadline_task: None,
            inactivity_task: None,
            output_activity: Arc::new(OutputActivity::new(tokio::time::Instant::now())),
            timeout_state: Arc::new(AtomicU8::new(TS_PENDING)),
            pid_gate: Arc::new(PidGate::new(s.pid)),
            cancel_token: s.cancel_token,
            additional_cancel_token: None,
            cancel_grace: s.cancel_grace,
            cancel_signal: s.cancel_signal,
            cancel_task: None,
            cancel_at_exit: None,
            started: Instant::now(),
            deadline_anchor: tokio::time::Instant::now(),
            start_time: SystemTime::now(),
            scripted_result: None,
            exit_event_tx: None,
            merged_events_stream: false,
        }
    }

    pub(crate) fn attach_group(&mut self, group: ProcessGroup) {
        match &mut self.backend {
            Backend::Real(real) => real.own_group = Some(Arc::new(group)),
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => pty.own_group = Some(Arc::new(group)),
            Backend::Scripted(_) => {}
        }
        // Re-arm the cancel watchdog now that the group is known: upgrade from
        // the pid-only task armed in `launch` to a full group+pid kill.
        self.arm_cancel_watchdog();
    }

    /// A strong handle to this process's own group, if it owns one — so a
    /// [`Pipeline`](crate::Pipeline) can retain each stage's sub-group and fan a
    /// chain-wide teardown across every one. Cloning the `Arc` keeps the group
    /// (and its kill-on-drop backstop) alive alongside this handle; the handle
    /// still owns its own strong reference, so per-stage timeout/cancel kills
    /// stay routed through it. `None` for a shared-group or scripted handle.
    pub(crate) fn own_group_handle(&self) -> Option<Arc<ProcessGroup>> {
        self.backend.own_group().cloned()
    }

    /// Arm (or re-arm) the cancel kill task. Aborts any existing task first so
    /// `attach_group` upgrades from pid-only to group+pid. No-op without a token.
    ///
    /// This is the watchdog that bounds every cancellation the consuming
    /// `drive_to_exit_inner` isn't already driving — the bulk verbs on a
    /// [`ProcessGroup`], a `Supervisor` incarnation, a live
    /// streamed run whose consumer is still reading. Its teardown mirrors the
    /// streaming *deadline* watchdog's (`stream::arm_stream_deadline`) branch for
    /// branch, reading the cancellation knobs instead of the deadline ones:
    ///
    /// - **No `cancel_grace` (the default):** unchanged — group `kill_all` (when a
    ///   group is still reachable) plus the gated raw `force_kill` backstop for the
    ///   direct child, i.e. an immediate hard kill.
    /// - **With `cancel_grace`:** the whole-tree case hands off to
    ///   `ProcessGroup::graceful_terminate` (the crate's single `sys::graceful::run`
    ///   escalation driver) instead of `kill_all`; the shared-group case — which owns
    ///   no group and reaches only its direct child — hands off to the same
    ///   **detached** `stream::spawn_graceful_kill_and_reap` the deadline watchdog
    ///   uses, so the final `SIGKILL` still lands if this (abortable) task is aborted
    ///   by `RunningProcess::Drop` mid-grace. Every raw op stays gated, so the
    ///   `PidGate` remains the stand-down and the recycled-pid backstop.
    pub(crate) fn arm_cancel_watchdog(&mut self) {
        let configured = self.cancel_token.clone();
        let additional = self.additional_cancel_token.clone();
        if configured.is_none() && additional.is_none() {
            return;
        }
        self.arm_cancel_watchdog_on(wait_for_cancellation(configured, additional));
    }

    fn is_cancelled(&self) -> bool {
        self.cancel_token
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            || self
                .additional_cancel_token
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    }

    /// Add another cancellation source without replacing the command's own token.
    ///
    /// A pipeline uses this once after attaching the stage's private group: its
    /// chain-wide token must cancel the handle even when the [`crate::Command`] already
    /// carried a distinct stage-local token. Both tokens remain independently
    /// observable by every reap/probe/finisher, while one watchdog owns the same
    /// idempotent teardown as the ordinary single-token path.
    pub(crate) fn add_cancel_trigger(&mut self, additional: tokio_util::sync::CancellationToken) {
        if self.cancel_token.is_none() {
            self.cancel_token = Some(additional);
            self.arm_cancel_watchdog();
            return;
        }
        debug_assert!(
            self.additional_cancel_token.is_none(),
            "a running process accepts only one additional cancellation source"
        );
        self.additional_cancel_token = Some(additional);
        self.arm_cancel_watchdog();
    }

    /// Install the one cancellation watchdog around an arbitrary trigger future.
    /// Keeping the teardown body here prevents the single-token and combined-token
    /// paths from drifting in grace, group ownership, or recycled-pid handling.
    fn arm_cancel_watchdog_on(
        &mut self,
        cancelled: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        if let Some(old) = self.cancel_task.take() {
            old.abort();
        }
        let group_weak = self.backend.own_group().map(Arc::downgrade);
        let gate = self.pid_gate.clone();
        let grace = self.cancel_grace;
        let signal = self.cancel_signal;
        let teardown_failure = self.teardown_failure.clone();
        self.cancel_task = Some(tokio::spawn(async move {
            cancelled.await;
            // Stand down if a `Child`-owning finisher has taken over teardown:
            // it kills the tree/child through the owned handles (`start_kill`,
            // a no-op once reaped), so a raw `kill(pid)` here could only signal
            // a pid the OS recycled. This early `is_retired` load is only an
            // optimization to skip even the group kill; the raw direct-child
            // kill below re-checks retirement *atomically with the kill* under
            // the gate lock, so — unlike the old bare `handed_off` load whose
            // load→kill gap let a reap slip in — it can never fire on a freed pid.
            if gate.is_retired() {
                return;
            }
            match group_weak {
                Some(group) => match grace {
                    // Whole tree, gracefully: signal → grace → hard kill, driven
                    // by the shared escalation driver. Like the deadline
                    // watchdog, this task cannot reap the child, so a child that
                    // exits on the signal is only observed as gone once whoever
                    // owns the `Child` reaps it.
                    Some(grace) => match group.upgrade() {
                        Some(group) => {
                            if let Err(source) = group_graceful_kill(&group, grace, signal).await {
                                record_teardown_failure(
                                    &teardown_failure,
                                    TeardownFailure {
                                        cause: TeardownCause::Cancellation,
                                        operation: "process-group graceful escalation",
                                        source,
                                    },
                                );
                            }
                        }
                        None => crate::sys::pid_gate::force_kill(&gate), // group gone
                    },
                    // The unchanged default: `kill_all` on a still-reachable
                    // group, then the gated raw kill of the direct child.
                    None => {
                        if let Err(source) = stream::kill_via_weak(&group, &gate) {
                            record_teardown_failure(
                                &teardown_failure,
                                TeardownFailure {
                                    cause: TeardownCause::Cancellation,
                                    operation: "process-group hard kill",
                                    source,
                                },
                            );
                        }
                    }
                },
                // Shared group: pid-only teardown (a forking child's
                // grandchildren are the documented shared-group teardown gap).
                None => match grace {
                    // Detached on purpose — see `spawn_graceful_kill_and_reap`:
                    // this watchdog is aborted by `RunningProcess::Drop`, and a
                    // child that catches the signal, closes stdout and keeps
                    // running must still be forced down when the grace elapses.
                    Some(grace) => stream::spawn_graceful_kill_and_reap(gate, grace, signal),
                    None => crate::sys::pid_gate::force_kill(&gate),
                },
            }
        }));
    }

    /// Take the raw stdout reader for `Pipeline` plumbing. Usually a child's
    /// stdout pipe; for a `merge_stderr_in_pipe` stage it is the reader paired
    /// with the shared stdout/stderr writer. `None` for a scripted backend.
    pub(crate) fn take_stdout_pipe(&mut self) -> Option<OutputReader> {
        match &mut self.backend {
            Backend::Real(real) => real.stdout_pipe.take(),
            Backend::Scripted(_) => None,
            // A PTY master and its merged terminal stream cannot feed a later
            // shell-free pipeline stage — so there is no stdout pipe to hand off.
            #[cfg(feature = "pty")]
            Backend::Pty(_) => None,
        }
    }

    /// The program this handle is running (for error/outcome attribution).
    pub(crate) fn program_name(&self) -> &str {
        &self.program
    }
}

// Manual impl: pipes, pump tasks, and line handlers are opaque.
impl std::fmt::Debug for RunningProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningProcess")
            .field("program", &self.program)
            .field("pid", &self.pid)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl RunningProcess {
    /// The OS process id, or `None` if the child has already been reaped.
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Wall-clock instant the process was started.
    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    /// Time elapsed since the process started (sampled now).
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// CPU time (user + kernel) consumed so far, if the platform can report it.
    #[cfg(feature = "stats")]
    pub fn cpu_time(&self) -> Option<Duration> {
        self.pid
            .and_then(|pid| crate::sys::process_metrics(pid, self.proc_identity).cpu_time)
    }

    /// Peak resident memory in bytes, if the platform can report it.
    #[cfg(feature = "stats")]
    pub fn peak_memory_bytes(&self) -> Option<u64> {
        self.pid
            .and_then(|pid| crate::sys::process_metrics(pid, self.proc_identity).peak_memory_bytes)
    }

    /// A clone of the timeout arbiter, so a consumer that has moved the handle
    /// into a search future (e.g. `first_line`) can still learn — race-free —
    /// whether the deadline watchdog fired. The watchdog stores `TS_TIMED_OUT`
    /// *before* it kills, so a `TS_TIMED_OUT` read after the stream has closed
    /// means the run was timed out, not a natural end.
    pub(crate) fn deadline_arbiter(&self) -> Arc<AtomicU8> {
        self.timeout_state.clone()
    }

    /// Share the resettable activity clock with a consuming helper that needs a
    /// teardown backstop after the stream watchdog fires.
    pub(crate) fn output_activity(&self) -> Arc<OutputActivity> {
        self.output_activity.clone()
    }

    /// Lines read from stdout so far (counts every line, even ones dropped by an
    /// [`OutputBufferPolicy`]). Live only once stdout is being pumped.
    pub fn stdout_line_count(&self) -> usize {
        self.stdout_sink.as_ref().map_or(0, |s| s.count())
    }

    /// Lines read from stderr so far (see [`stdout_line_count`](Self::stdout_line_count)).
    pub fn stderr_line_count(&self) -> usize {
        self.stderr_sink.as_ref().map_or(0, |s| s.count())
    }

    /// Raw bytes read from stdout's pipe so far, before decoding or line
    /// splitting. The counter is monotonic, includes bytes discarded by any
    /// [`OutputBufferPolicy`] (including oversized lines), and remains stable
    /// after the process and its pump complete. A stream that is not pumped —
    /// for example a file redirect or [`StdioMode::Null`](crate::StdioMode::Null)
    /// / [`StdioMode::Inherit`](crate::StdioMode::Inherit) — returns `0` rather
    /// than an unknown sentinel.
    pub fn stdout_bytes_seen(&self) -> usize {
        self.stdout_sink.as_ref().map_or(0, |s| s.seen_bytes())
    }

    /// Raw bytes read from stderr's pipe so far, before decoding or line
    /// splitting. The counter is monotonic, includes bytes discarded by any
    /// [`OutputBufferPolicy`] (including oversized lines), and remains stable
    /// after the process and its pump complete. A stream that is not pumped —
    /// for example a file redirect or [`StdioMode::Null`](crate::StdioMode::Null)
    /// / [`StdioMode::Inherit`](crate::StdioMode::Inherit) — returns `0` rather
    /// than an unknown sentinel.
    pub fn stderr_bytes_seen(&self) -> usize {
        self.stderr_sink.as_ref().map_or(0, |s| s.seen_bytes())
    }

    /// Take the interactive stdin writer, if the command was built with
    /// [`keep_stdin_open`](crate::Command::keep_stdin_open). Returns `None` after
    /// the first call (or when stdin was not kept open).
    pub fn take_stdin(&mut self) -> Option<ProcessStdin> {
        match &mut self.backend {
            Backend::Real(real) => real.stdin_pipe.take().map(ProcessStdin::new),
            // A scripted double models interactive stdin only for a
            // `Reply::dialog` (its feeder reads what the test writes and answers);
            // a plain reply carries no stdin writer, so this stays `None` — the
            // "stdin wasn't kept open" contract for a non-dialog double.
            Backend::Scripted(s) => s.take_stdin_writer().map(ProcessStdin::from_scripted),
            // The PTY master's input side is the child's stdin.
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => pty.writer.take().map(ProcessStdin::from_pty),
        }
    }

    /// Whether **dropping** this handle will tear down (hard-kill) the process
    /// tree.
    ///
    /// `true` — owns a **private** process group; drop hard-kills the whole tree.
    /// `false` — runs inside a **shared** [`ProcessGroup`]
    /// whose lifetime the group owns (drop does *not* kill the tree), or a
    /// scripted test double (no OS tree).
    pub fn kills_tree_on_drop(&self) -> bool {
        self.backend.own_group().is_some()
    }

    /// Resize the running pseudo-terminal to `cols` columns by `rows` rows.
    ///
    /// The live counterpart of [`Command::pty_size`](crate::Command::pty_size):
    /// propagate a host window resize (`SIGWINCH`-style) into a live PTY child so a
    /// TUI/pager re-renders for the new geometry. On **Unix** it issues
    /// `TIOCSWINSZ` on the master, which delivers `SIGWINCH` to the child's
    /// foreground process group; on **Windows** it calls `ResizePseudoConsole` (a
    /// console client learns of the change on its next console query — there is no
    /// `SIGWINCH`, and conhost may reflow asynchronously).
    ///
    /// Callable at any point while you still hold the handle — typically
    /// interleaved with driving an owned output stream
    /// ([`stdout_lines`](Self::stdout_lines)/[`events`](Self::events)) and writing
    /// the [`take_stdin`](Self::take_stdin) side of a live session.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorReason::Unsupported`],
    /// never panicking and never silently ignoring, when the resize cannot apply:
    ///
    /// - the run is **not** a PTY run (no [`use_pty`](crate::Command::use_pty)),
    ///   including a non-PTY scripted double — there is no terminal to resize; or
    /// - the child has **already exited** — the pseudo-terminal is gone.
    ///
    /// A genuine OS failure of the resize syscall surfaces as
    /// [`ErrorReason::Io`].
    /// A zero column or row is also an `Io` error with
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput). On Windows, where
    /// ConPTY represents each axis as a signed 16-bit `COORD`, values above
    /// `i16::MAX` are rejected the same way; Unix accepts every non-zero `u16`.
    /// Invalid geometry is never clamped and never reaches the live terminal.
    ///
    /// The PTY-variant scripted double ([`ScriptedRunner`](crate::testing::ScriptedRunner)
    /// with [`use_pty`](crate::Command::use_pty)) models this hermetically:
    /// `resize_pty` succeeds while the double is "running" and fails with the same
    /// `Unsupported` shape once it has "exited" or when the double is not a PTY —
    /// so a resize can be exercised in tests without a real pseudo-terminal.
    #[cfg(feature = "pty")]
    #[cfg_attr(docsrs, doc(cfg(feature = "pty")))]
    pub fn resize_pty(&mut self, cols: u16, rows: u16) -> Result<()> {
        // `program` is a distinct field from `backend`, so this immutable borrow
        // coexists with the `&mut self.backend` match below (disjoint-field NLL).
        let program = &self.program;
        // Clone the gate up front — exactly as `has_exited_now` does — so the PTY
        // arm's liveness reap can run under the gate lock without borrowing `self`
        // again while `self.backend` is mutably matched below (disjoint-field NLL).
        let gate = self.pid_gate.clone();
        match &mut self.backend {
            Backend::Pty(pty) => {
                // `child` is present on every live-handle path (only `Drop` extracts
                // it, and `Drop` is the handle's final act); treat an absent child
                // as "already torn down" rather than panicking.
                if pty.child.is_none() {
                    return Err(pty_resize_gone(program));
                }
                // A resize on an exited child is meaningless (and the pseudoconsole
                // may already be closed) — surface it honestly instead of poking a
                // dead terminal. But tokio's `try_wait` REAPS an exited child and
                // frees its pid, so it must run through `PidGate::reap_under_lock` —
                // exactly as the sibling `has_exited_now` does — fusing the pid-free
                // and the gate retire into one critical section. A bare `try_wait`
                // here would free the pid off-gate without retiring the gate: a
                // detached force-kill watchdog (which raw-`kill`s the pid until the
                // gate is retired) could then observe the gate still live and SIGKILL
                // an unrelated process the OS had recycled that freed pid for.
                let exited =
                    gate.reap_under_lock(|| matches!(pty.child_mut().try_wait(), Ok(Some(_))));
                if exited {
                    return Err(pty_resize_gone(program));
                }
                crate::sys::pty::validate_size(cols, rows).map_err(Error::io)?;
                pty.child_mut().resize(cols, rows).map_err(Error::io)
            }
            // A scripted double answers only when it models a PTY run; the model is
            // hermetic (records the size, no real tty). A non-PTY scripted handle
            // refuses exactly as a real non-PTY run does.
            Backend::Scripted(scripted) => {
                if !scripted.models_pty() {
                    return Err(pty_resize_not_a_pty(program));
                }
                if scripted.has_exited_now() {
                    return Err(pty_resize_gone(program));
                }
                crate::sys::pty::validate_size(cols, rows).map_err(Error::io)?;
                scripted.record_resize(cols, rows);
                Ok(())
            }
            // A real (three-pipe) child has no terminal to resize.
            Backend::Real(_) => Err(pty_resize_not_a_pty(program)),
        }
    }

    /// A test-only view of the resizes a scripted PTY double recorded (`None` for
    /// a non-scripted backend) — lets a hermetic unit test assert that
    /// [`resize_pty`](Self::resize_pty) delivered the requested geometry.
    #[cfg(all(test, feature = "pty"))]
    pub(crate) fn scripted_recorded_resizes(&self) -> Option<Vec<(u16, u16)>> {
        match &self.backend {
            Backend::Scripted(scripted) => Some(scripted.recorded_resizes().to_vec()),
            _ => None,
        }
    }

    /// A bulk capture verb on a stdout that wasn't piped (`Inherit`/`Null`) would
    /// return silently-empty output — surface it as a clear error instead.
    /// `stdout_piped` reflects the command's `stdout` mode for *both* real and
    /// scripted handles, so a scripted run with `stdout(Null)` errors here too.
    fn ensure_stdout_capturable(&self) -> Result<()> {
        if self.stdout_piped {
            return Ok(());
        }
        Err(crate::error::stdout_not_piped_error(&self.program))
    }

    /// Prepare the line pumps before a caller moves this handle into a task.
    ///
    /// Pipeline capture needs the sinks to outlive that task: a chain-wide
    /// timeout drops the task frame, but must still be able to return the lines
    /// the pumps retained before the deadline.
    pub(crate) fn prepare_line_capture(&mut self) -> Result<LineCapture> {
        self.ensure_stdout_capturable()?;
        let stdout_sink = self.stdout_sink.clone().unwrap_or_else(|| {
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone())
        });
        let stderr_sink = self.stderr_sink.clone().unwrap_or_else(|| {
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone())
        });
        self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        self.stdout_sink = Some(stdout_sink.clone());
        self.stderr_sink = Some(stderr_sink.clone());
        Ok(LineCapture {
            stdout: stdout_sink,
            stderr: stderr_sink,
            stdout_config: self.stdout_config.clone(),
            stderr_config: self.stderr_config.clone(),
        })
    }

    /// Prepare a non-consuming view of stderr independently of stdout.
    ///
    /// A live pipeline may honor `stdout(Null)`/`stdout(Inherit)`, or its caller
    /// may already own the one-shot stdout stream. Neither condition should stop
    /// a bounded teardown failure from retaining piped stderr. Reuse an existing
    /// sink when another streaming path already armed it; otherwise start exactly
    /// one stderr pump and leave the process finisher as the sole consumer.
    pub(crate) fn prepare_stderr_capture(&mut self) -> StderrCapture {
        let stderr_sink = self.stderr_sink.clone().unwrap_or_else(|| {
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone())
        });
        if self.stderr_sink.is_none() {
            self.stderr_pump = self.backend.take_stderr_reader().map(|pipe| {
                tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_config.clone(),
                    stderr_sink.clone(),
                ))
            });
            if self.stderr_pump.is_none() {
                stderr_sink.close_now();
            }
            self.stderr_sink = Some(stderr_sink.clone());
        }
        StderrCapture {
            stderr: stderr_sink,
            stderr_config: self.stderr_config.clone(),
        }
    }

    /// Prepare raw stdout plus line-oriented stderr capture before a caller
    /// moves this handle into a task. See [`Self::prepare_line_capture`] for
    /// why the returned state must be independently owned by the caller.
    pub(crate) fn prepare_raw_capture(&mut self) -> Result<RawCapture> {
        if let Some(capture) = &self.raw_capture {
            return Ok(capture.clone());
        }
        self.ensure_stdout_capturable()?;
        if self.stdout_sink.is_some() || self.stderr_sink.is_some() {
            return Err(Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "`{}`: output_bytes cannot follow a readiness or streaming call (stdout \
                     was already consumed as decoded lines) — use output_string to collect the \
                     unconsumed lines, or call output_bytes before line-oriented consumers",
                    self.program
                ),
            )));
        }

        let stderr_sink =
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
        self.stderr_pump = self.backend.take_stderr_reader().map(|pipe| {
            tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_config.clone(),
                stderr_sink.clone(),
            ))
        });
        self.stderr_sink = Some(stderr_sink.clone());

        let stdout_cap = self.buffer.max_bytes;
        let stdout_mode = self.buffer.overflow;
        let signals = RawStdoutSignals {
            seen: Arc::new(AtomicUsize::new(0)),
            overflowed: Arc::new(AtomicBool::new(false)),
            truncated: Arc::new(AtomicBool::new(false)),
            read_error: Arc::new(std::sync::Mutex::new(None)),
        };
        let stdout_pipe = self.backend.take_stdout_reader();
        let out_buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let output_activity = self.output_activity.clone();
        self.stdout_pump = stdout_pipe.map(|pipe| {
            tokio::spawn(pump_raw_bytes(
                pipe,
                out_buf.clone(),
                stdout_cap,
                stdout_mode,
                signals.clone(),
                output_activity,
            ))
        });

        let capture = RawCapture {
            out_buf,
            stderr_sink,
            signals,
            stdout_cap,
            stdout_mode,
            stderr_config: self.stderr_config.clone(),
        };
        self.raw_capture = Some(capture.clone());
        Ok(capture)
    }

    /// Fail loud if streaming is not possible: (a) stdout not piped, or
    /// (b) a prior readiness or streaming call already started its one line pump.
    fn ensure_stdout_streamable(&self) -> Result<()> {
        self.ensure_stdout_capturable()?; // (a) non-piped stdout
        if self.stdout_sink.is_some() {
            return Err(Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "`{}`: stdout was already consumed by an earlier readiness or streaming \
                     call — stdout has a single line pump (a second consumer would yield \
                     empty output)",
                    self.program
                ),
            )));
        }
        Ok(())
    }

    /// Fail loud if a stderr readiness probe cannot take its one-shot stream.
    fn ensure_stderr_streamable(&self) -> Result<()> {
        if !self.stderr_piped {
            return Err(Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("`{}`: stderr is not piped", self.program),
            )));
        }
        if self.stderr_sink.is_some() {
            return Err(Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "`{}`: stderr was already consumed by an earlier readiness or events call",
                    self.program
                ),
            )));
        }
        Ok(())
    }

    /// Drain both streams, wait for exit, and return the captured text output
    /// (line-normalized to `\n`).
    ///
    /// If you previously called [`stdout_lines`](Self::stdout_lines) and
    /// consumed some lines from the stream, those already-consumed lines are
    /// gone from the buffer; `output_string` returns only the unconsumed tail.
    /// To capture the full output, avoid mixing streaming and `output_string`.
    ///
    /// # Errors
    ///
    /// A **timeout** or **signal-kill** is *captured* in the returned
    /// [`ProcessResult`]'s [`outcome`](ProcessResult::outcome), not raised — this
    /// is a non-checking path; call
    /// [`ensure_success`](ProcessResult::ensure_success) to turn a non-zero,
    /// timed-out, or signalled outcome into an error. The `Err` cases are:
    ///
    /// - [`ErrorReason::Cancelled`] — the run was cancelled via
    ///   [`Command::cancel_on`](crate::Command::cancel_on). Unlike a timeout,
    ///   cancellation is *always* raised (and discards any captured output).
    /// - [`ErrorReason::Teardown`] — the timeout/cancellation required a terminal
    ///   kill, escalation, or reap that the OS did not confirm. This takes
    ///   precedence over the initiating disposition and retains the original IO
    ///   error plus the best-effort output prefix captured before teardown.
    /// - [`ErrorReason::OutputTooLarge`] — the
    ///   [`OutputBufferPolicy`] is fail-loud
    ///   ([`OverflowMode::Error`](crate::OverflowMode)) and the captured output
    ///   exceeded its line or byte ceiling.
    /// - [`ErrorReason::Stdin`] — a configured stdin source failed for a reason other
    ///   than a broken pipe, on an *otherwise-successful* run.
    /// - [`ErrorReason::Io`] — stdout is not piped, waiting on the child failed,
    ///   or a pipe pump ended with a read error. A prior line-oriented readiness
    ///   or streaming call is supported; only its unconsumed tail is returned.
    pub async fn output_string(self) -> Result<ProcessResult<String>> {
        self.output_string_observing_exit(|_| ()).await
    }

    /// [`output_string`](Self::output_string) with an observation seam at the
    /// child's **wait boundary**: `at_exit` runs once the wait either proves a
    /// terminal reap or retains an unconfirmed teardown/wait error — after the
    /// deadline/cancel arbiter settled it, but *before* the output pumps are
    /// joined. `output_string` is this with a no-op observer.
    ///
    /// The buffering counterpart of
    /// [`finish_observing_exit`](Self::finish_observing_exit), and it exists for the
    /// same one caller and the same reason: the last stage of a buffering
    /// [`Pipeline`](crate::Pipeline) capture decides culprit-vs-victim attribution
    /// by whether the chain's proactive teardown was already in flight *when the
    /// stage died*, and a stage whose stderr pipe outlives it (a forked grandchild
    /// inherited the write end) is reaped long before its drain can end. Reading
    /// that disposition after the drain would blame whoever drained first instead of
    /// whoever died first; see `pipeline::ExitDisposition`.
    ///
    /// The observer receives whether that wait boundary proved a terminal reap or
    /// retained an unconfirmed teardown/wait error. It is synchronous and must
    /// stay cheap: it runs on this future's own poll, between the wait and the pump
    /// join.
    pub(crate) async fn output_string_observing_exit(
        mut self,
        at_exit: impl FnOnce(ExitObservation),
    ) -> Result<ProcessResult<String>> {
        let finished = self
            .finish_lines(CaptureMode::Lines, /* expose_counts */ true, at_exit)
            .await?;
        // A cassette `start`-replay carries the recorded truncation/overflow/
        // duration, so a consumed replay agrees with the bulk `Entry::to_result`
        // path instead of re-deriving them from the (un-truncated, instantly-fed)
        // canned output. A real child or a plain scripted reply (`None`) derives
        // them from the run itself.
        let (truncated, total_lines, total_bytes, duration) = match self.scripted_result {
            Some(rec) => (
                rec.truncated,
                rec.total_lines,
                rec.total_bytes,
                rec.duration,
            ),
            None => {
                // `dropped()` = lines the buffer policy discarded, NOT lines a prior
                // stream consumed — so partial streaming under the unbounded policy
                // is never mis-reported as truncated.
                let truncated = self.stdout_sink.as_ref().is_some_and(|s| s.dropped() > 0)
                    || self.stderr_sink.as_ref().is_some_and(|s| s.dropped() > 0);
                let total_lines = self.stdout_sink.as_ref().map_or(0, |s| s.count())
                    + self.stderr_sink.as_ref().map_or(0, |s| s.count());
                let total_bytes = self.stdout_sink.as_ref().map_or(0, |s| s.seen_bytes())
                    + self.stderr_sink.as_ref().map_or(0, |s| s.seen_bytes());
                (truncated, total_lines, total_bytes, self.started.elapsed())
            }
        };
        let timeout = if finished.outcome.inactivity_timed_out() {
            self.inactivity_timeout
        } else {
            self.timeout
        };
        Ok(ProcessResult::new(
            self.program.clone(),
            finished.stdout_lines.join("\n"),
            finished.stderr_lines.join("\n"),
            finished.outcome,
            timeout,
        )
        .with_duration(duration)
        .with_truncated(truncated)
        .with_overflow_totals(total_lines, total_bytes)
        .with_ok_codes(self.ok_codes.clone()))
    }

    /// Drain both streams, wait for exit, and return the exact raw stdout bytes
    /// (stderr captured as text). On **timeout** the bytes read before the
    /// deadline are returned as a best-effort prefix (the outcome is
    /// [`Outcome::TimedOut`]); a **cancelled** run instead errors with
    /// [`ErrorReason::Cancelled`] and no bytes — cancellation via
    /// [`Command::cancel_on`](crate::Command::cancel_on) is always terminal.
    ///
    /// A byte ceiling on the [`OutputBufferPolicy`] bounds the raw stdout capture
    /// (its `max_lines` does not — raw bytes have no lines): with
    /// [`OverflowMode::Error`](crate::OverflowMode) a flood past the cap errors
    /// with [`ErrorReason::OutputTooLarge`], while the drop modes keep a bounded
    /// head/tail and set [`ProcessResult::truncated`]. With no byte cap the
    /// capture is unbounded — bound a flooding child with
    /// [`with_max_bytes`](crate::OutputBufferPolicy::with_max_bytes) or a
    /// [`timeout`](crate::Command::timeout).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorReason::Io(InvalidInput)`](std::io::ErrorKind::InvalidInput) if
    /// stdout is not piped, or if a prior readiness or streaming call already
    /// started the decoded-line pump (the raw bytes cannot be reconstructed). Returns
    /// [`ErrorReason::OutputTooLarge`] if the byte ceiling is set to
    /// [`OverflowMode::Error`](crate::OverflowMode) and the raw stdout exceeds it.
    /// (A cancelled run is [`ErrorReason::Cancelled`]; an unconfirmed terminal
    /// teardown is [`ErrorReason::Teardown`] with the exact stdout prefix attached;
    /// a non-zero exit, a confirmed timeout, or a signal-kill is *captured* in the
    /// returned [`ProcessResult`]'s [`outcome`](ProcessResult::outcome), not raised.)
    ///
    /// # Panics
    ///
    /// Panics if the internal raw-stdout capture buffer's mutex is poisoned —
    /// which happens only if a pump task previously panicked while holding it (a
    /// crate bug), never from any caller input.
    pub async fn output_bytes(self) -> Result<ProcessResult<Vec<u8>>> {
        self.output_bytes_observing_exit(|_| ()).await
    }

    /// [`output_bytes`](Self::output_bytes) with the same exit-observation seam as
    /// [`output_string_observing_exit`](Self::output_string_observing_exit) — see
    /// there for what the seam is for and what its observation means.
    /// `output_bytes` is this with a no-op observer.
    ///
    /// # Panics
    ///
    /// The same single panic as [`output_bytes`](Self::output_bytes) (a poisoned
    /// internal raw-stdout buffer, i.e. a crate bug).
    pub(crate) async fn output_bytes_observing_exit(
        mut self,
        at_exit: impl FnOnce(ExitObservation),
    ) -> Result<ProcessResult<Vec<u8>>> {
        let capture = if let Some(capture) = self.raw_capture.take() {
            capture
        } else {
            self.prepare_raw_capture()?
        };
        let RawCapture {
            out_buf,
            stderr_sink,
            signals,
            stdout_cap,
            stdout_mode,
            ..
        } = capture;

        // Same seam placement as `finish_lines`: this wait boundary is observable
        // here and nowhere later without first waiting on output, which a survivor
        // can stretch arbitrarily. The explicit disposition prevents an attempted
        // but unconfirmed reap from masquerading as terminal completion.
        let outcome = self.drive_to_exit().await;
        at_exit(self.exit_observation(&outcome));
        let outcome = outcome?;
        self.observe_stdin_task().await;
        // Bound the stdout drain: a surviving grandchild can hold stdout open past
        // the child's death; an unbounded read would park forever.
        if let Some(out_task) = self.stdout_pump.take() {
            let abort = out_task.abort_handle();
            if tokio::time::timeout(PUMP_TEARDOWN, out_task).await.is_err() {
                abort.abort();
            }
        }
        // The `out_buf` bytes are consistent even on the abort path: the mutex
        // orders every push, and a push never spans the task's only await
        // (`pipe.read`), so the lock here sees whole writes. The overflow/seen
        // atomics read below are likewise current when the task ran to EOF (the
        // JoinHandle await orders them); on the abort path (a grandchild held the
        // pipe past PUMP_TEARDOWN) they are best-effort, matching this verb's
        // documented best-effort-prefix-on-teardown contract.
        let mut stdout = std::mem::take(&mut *out_buf.lock().expect("stdout buffer poisoned"));
        clamp_dropoldest_tail(&mut stdout, stdout_cap, stdout_mode);
        join_pumps(self.stderr_pump.take().into_iter().collect()).await;
        // Re-observe stdin after the pumps drained: a writer that failed inside
        // the teardown window is only visible now (see `finalize_stdin_task`).
        self.finalize_stdin_task().await;
        let stderr_lines = stderr_sink.drain();
        if self
            .teardown_failure
            .lock()
            .expect("teardown failure slot poisoned")
            .is_some()
        {
            let stdout_text = String::from_utf8_lossy(&stdout).into_owned();
            let error = self
                .take_teardown_error(stdout_text, stderr_lines.join("\n"), Some(stdout))
                .expect("teardown failure was observed under the same slot lock");
            return Err(error);
        }
        let outcome = self.checked_outcome(outcome)?;

        // A raw-stdout fail-loud (Error mode) byte overflow surfaces first, like
        // the stderr line ceiling below. Raw stdout has no lines, so report only
        // the byte ceiling that actually fired (`max_lines: None`).
        if signals.overflowed.load(Ordering::Relaxed) {
            return Err(crate::ErrorReason::OutputTooLarge {
                program: self.program.clone(),
                max_lines: None,
                max_bytes: self.buffer.max_bytes,
                total_lines: 0,
                total_bytes: signals.seen.load(Ordering::Relaxed),
            }
            .into());
        }
        if stderr_sink.overflowed() {
            return Err(crate::ErrorReason::OutputTooLarge {
                program: self.program.clone(),
                max_lines: self.buffer.max_lines,
                max_bytes: self.buffer.max_bytes,
                total_lines: stderr_sink.count(),
                total_bytes: stderr_sink.seen_bytes(),
            }
            .into());
        }

        // An incomplete capture from a first OS read error on either stream
        // surfaces as `ErrorReason::Io` — a short raw-stdout prefix (or a truncated
        // stderr) is not a full success. Checked after the overflow ceilings (the
        // more specific signal if both fire) and after `checked_outcome`
        // (cancellation wins). A timeout closes the pipe with a *clean* EOF, not a
        // read error, so the documented best-effort-prefix-on-timeout contract is
        // unaffected; on the teardown-abort path the signal is best-effort.
        if let Some(source) = signals
            .read_error
            .lock()
            .expect("stdout read-error slot poisoned")
            .take()
        {
            return Err(Error::io(source));
        }
        if let Some(source) = stderr_sink.take_read_error() {
            return Err(Error::io(source));
        }

        let truncated = signals.truncated.load(Ordering::Relaxed) || stderr_sink.dropped() > 0;
        let duration = self.started.elapsed();
        let timeout = if outcome.inactivity_timed_out() {
            self.inactivity_timeout
        } else {
            self.timeout
        };
        Ok(ProcessResult::new(
            self.program.clone(),
            stdout,
            stderr_lines.join("\n"),
            outcome,
            timeout,
        )
        .with_duration(duration)
        .with_truncated(truncated)
        .with_overflow_totals(
            stderr_sink.count(),
            signals
                .seen
                .load(Ordering::Relaxed)
                .saturating_add(stderr_sink.seen_bytes()),
        )
        .with_ok_codes(self.ok_codes.clone()))
    }

    /// Wait for exit, returning how the run ended as an [`Outcome`] (output is
    /// drained and discarded so the child never blocks on a full pipe).
    ///
    /// Reports the raw outcome — timeout and signals are not raised as errors
    /// here. Exception: cancellation via `Command::cancel_on` always errors with
    /// `ErrorReason::Cancelled` once teardown is confirmed; an unconfirmed
    /// terminal timeout/cancellation teardown is `ErrorReason::Teardown`.
    ///
    /// # Errors
    ///
    /// A timeout or signal-kill is *captured* in the returned [`Outcome`], not
    /// raised. The `Err` cases are [`ErrorReason::Cancelled`] (the run was cancelled
    /// via [`Command::cancel_on`](crate::Command::cancel_on) — always raised),
    /// [`ErrorReason::Teardown`] (terminal timeout/cancellation teardown could not
    /// be confirmed and therefore outranks the ordinary outcome),
    /// [`ErrorReason::Stdin`] (a non-broken-pipe stdin-source failure on an
    /// otherwise-successful run), or [`ErrorReason::Io`] (waiting on the child failed).
    pub async fn wait(mut self) -> Result<Outcome> {
        Ok(self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, |_| {})
            .await?
            .outcome)
    }

    /// Wait for exit like [`wait`](Self::wait) — draining both pipes so the child
    /// never blocks on a full one and returning the same [`Outcome`] classification
    /// — but **respecting the configured
    /// [`Command::output_buffer`](crate::Command::output_buffer) byte cap** for the
    /// in-flight memory bound instead of `wait`'s fixed internal cap.
    ///
    /// Use this when the output is already going where you want it — a
    /// [`stdout_tee`](crate::Command::stdout_tee)/`stderr_tee` writing to a file, or
    /// an [`on_stdout_line`](crate::Command::on_stdout_line)/`on_stderr_line`
    /// handler — and you have no use for an in-memory capture. Those sinks still
    /// receive **every** decoded line (exactly as under `wait`); `drain` simply
    /// retains nothing itself, so a build log of hundreds of megabytes is streamed
    /// through to your tee without ever being held in memory. Contrast
    /// [`output_string`](Self::output_string), which would retain the whole
    /// capture only for you to throw it away.
    ///
    /// The one behavioral difference from [`wait`](Self::wait) is the in-flight
    /// bound: `wait` ignores `output_buffer` and pins a large fixed internal cap,
    /// whereas `drain` bounds the pump's line assembly by the configured
    /// [`max_bytes`](crate::OutputBufferPolicy::max_bytes) byte ceiling. Held
    /// memory therefore tracks the *configured* limit, not the child's output
    /// size. As with any byte cap
    /// ([`with_max_bytes`](crate::OutputBufferPolicy::with_max_bytes)), a single
    /// line whose length exceeds the cap is never assembled — so it is neither teed
    /// nor handed to the per-line handler, counted only via the truncation signal.
    /// An **unbounded** `output_buffer` (no byte cap) falls back to the same fixed
    /// internal cap `wait` uses, so a newline-free flood still cannot exhaust
    /// memory. `max_lines` is irrelevant here (it governs retention, and `drain`
    /// retains nothing).
    ///
    /// # Errors
    ///
    /// The same surface as [`wait`](Self::wait): a timeout or signal-kill is
    /// *captured* in the returned [`Outcome`], not raised. The `Err` cases are
    /// [`ErrorReason::Cancelled`] (the run was cancelled via
    /// [`Command::cancel_on`](crate::Command::cancel_on) — always raised),
    /// [`ErrorReason::Teardown`] (terminal timeout/cancellation teardown could not
    /// be confirmed and therefore outranks the ordinary outcome),
    /// [`ErrorReason::Stdin`] (a non-broken-pipe stdin-source failure on an
    /// otherwise-successful run), or [`ErrorReason::Io`] (waiting on the child, or a
    /// pipe read, failed). A [`fail_loud`](crate::OutputBufferPolicy::fail_loud)
    /// policy does **not** raise [`ErrorReason::OutputTooLarge`] here — like `wait`,
    /// `drain` captures nothing, so there is no backlog to overflow.
    pub async fn drain(mut self) -> Result<Outcome> {
        Ok(self
            .finish_lines(
                CaptureMode::DrainBounded,
                /* expose_counts */ false,
                |_| {},
            )
            .await?
            .outcome)
    }

    /// Gracefully stop the process tree: `SIGTERM`, wait up to `grace`, then
    /// `SIGKILL` any survivor. On Windows the kill is atomic and `grace` is not
    /// awaited.
    ///
    /// Only an **own-group** handle can be shut down here — a **shared-group**
    /// handle returns [`ErrorReason::Unsupported`] because
    /// shutting it down would tear down the caller's other children too.
    ///
    /// If the configured timeout deadline already elapsed when `shutdown` is
    /// called the run is classified as `Outcome::TimedOut`.
    ///
    /// # Errors
    ///
    /// - [`ErrorReason::Unsupported`] — this is a **shared-group** handle, which does
    ///   not own its group (tearing it down would kill the caller's other
    ///   children); use [`ProcessGroup::shutdown`](crate::ProcessGroup::shutdown)
    ///   or [`start_kill`](Self::start_kill) instead.
    /// - [`ErrorReason::Cancelled`] — the run was cancelled via
    ///   [`Command::cancel_on`](crate::Command::cancel_on).
    /// - [`ErrorReason::Teardown`] — terminal cancellation teardown could not be
    ///   confirmed; the initiating cancellation is not reported as complete.
    /// - [`ErrorReason::Stdin`] — a non-broken-pipe stdin-source failure on an
    ///   otherwise-successful run.
    /// - [`ErrorReason::Io`] — the graceful teardown or the exit wait failed. A
    ///   graceful-teardown failure is returned as soon as it is observed, without
    ///   waiting indefinitely for a child the failed escalation left alive; it
    ///   takes precedence over a concurrent exit-wait result.
    ///
    /// A timeout or signal-kill is *captured* in the returned [`Outcome`], not
    /// raised.
    pub async fn shutdown(mut self, grace: std::time::Duration) -> Result<Outcome> {
        let Some(group) = self.backend.own_group().cloned() else {
            return Err(ErrorReason::Unsupported {
                operation: "shutdown (a shared-group handle does not own its group — \
                            use ProcessGroup::shutdown, or start_kill for just this child)"
                    .into(),
            }
            .into());
        };
        // Disable the concurrent `wait()`'s deadline and inactivity arms to avoid
        // overlapping teardowns. A timeout that already elapsed still classifies
        // as `TimedOut` — claim the arbiter before nulling `self.timeout`. An
        // inactivity timeout that already won remains recorded in `timeout_state`.
        // Measured off `deadline_anchor` (tokio's clock), not `started`, so this
        // "already elapsed?" check agrees with `wait_deadline_and_claim` under a
        // paused runtime instead of reading the real clock the deadline never slept on.
        if let Some(limit) = self.timeout
            && self.deadline_anchor.elapsed() >= limit
        {
            let _ = deadline::claim_timed_out(&self.timeout_state);
        }
        self.timeout = None;
        self.inactivity_timeout = None;
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.inactivity_task.take() {
            task.abort();
        }
        // Reap concurrently: an unreaped zombie still answers `kill(pgid, 0)`
        // probes, so without a concurrent reap a SIGTERM-handling child would
        // look alive for the whole grace and eat a pointless SIGKILL. Do not
        // `join!` the two futures, though: a failed hard-kill can leave the child
        // alive, making the wait hide the teardown error forever. Returning that
        // primary error drops the still-owned wait future, so `RunningProcess::Drop`
        // keeps the group kill-on-drop and orphan-reap backstops intact.
        let teardown = group_graceful_kill(&group, grace, crate::sys::SIGTERM_RAW);
        let wait = self.wait();
        tokio::pin!(teardown, wait);
        tokio::select! {
            teardown_result = &mut teardown => {
                teardown_result.map_err(Error::io)?;
                wait.await
            }
            outcome = &mut wait => {
                // Preserve the historical error priority even when the wait
                // finishes first: teardown failure outranks a successful Outcome
                // or a secondary wait error.
                teardown.await.map_err(Error::io)?;
                outcome
            }
        }
    }

    /// Minimal non-consuming exit wait — the [`wait_any`](crate::wait_any) race
    /// participant. Spawns no pumps, applies no timeout. Cancel-safe and
    /// re-awaitable (tokio caches exit status). A cancelled run returns
    /// `Err(Cancelled)`; a non-broken-pipe stdin failure on an otherwise-
    /// successful run returns `Err(Stdin)`.
    pub(crate) async fn wait_exit(&mut self) -> Result<Outcome> {
        // Must NOT close an untaken `keep_stdin_open` pipe: `wait_any`/`wait_all`
        // borrow contenders and promise losers "remain fully usable".
        // Short-circuit when a prior reap already snapshotted `cancel_at_exit`
        // so a late cancel can't flip a cached natural exit to `Err(Cancelled)`.
        let cause = if self.cancel_at_exit.is_some() {
            ExitCause::Exited(self.backend_wait().await?)
        } else {
            // No deadline arm: a streamed run's deadline is owned by its watchdog.
            let configured = self.cancel_token.clone();
            let additional = self.additional_cancel_token.clone();
            let cancelled = wait_for_cancellation(configured, additional);
            tokio::select! {
                biased; // cancel arm first: a cancel that fires mid-wait wins
                () = cancelled => {
                    // Take teardown over from the detached cancel watchdog BEFORE
                    // driving it, exactly as `drive_to_exit_inner` does: this arm
                    // owns the `Child` and reaps through it, and the graceful branch
                    // frees the pid part-way (its own `retire` comes after the reap,
                    // which is safe only because its `drive_to_exit_inner` caller
                    // already retired). Retiring here first closes that window —
                    // the watchdog (and any detached pid-scoped grace killer it
                    // spawned on this same token) is linearized to either land
                    // entirely before this retire or be skipped, never onto the pid
                    // this teardown's reap is about to free. `retire` is idempotent,
                    // so the `kill_tree` default's own retire still stands; the only
                    // change on that default path is that it now happens a few
                    // statements earlier, which strictly widens the stand-down.
                    self.pid_gate.retire();
                    // The same teardown seam the consuming finishers use, so
                    // `wait_any`/`wait_all` honor `cancel_grace` too instead of
                    // silently hard-killing a run that opted into a graceful
                    // goodbye. Unset (the default) → the unchanged `kill_tree`.
                    match self.teardown_on_cancel().await {
                        Ok(()) => ExitCause::Cancelled,
                        Err(failure) => {
                            record_teardown_failure(&self.teardown_failure, failure);
                            ExitCause::TeardownFailed {
                                intended: Outcome::Signalled(None),
                                cancelled: true,
                            }
                        }
                    }
                }
                outcome = self.backend_wait() => ExitCause::Exited(outcome?),
            }
        };
        let outcome = match cause {
            ExitCause::TeardownFailed {
                intended,
                cancelled,
            } => {
                if self.cancel_at_exit.is_none() {
                    self.cancel_at_exit = Some(cancelled);
                }
                intended
            }
            reaped => self.on_reaped(reaped),
        };
        // Borrowed waits have no pump-drain window to give a still-running
        // source a chance to finish. Finalize the writer after the child reap,
        // bounded like the consuming paths, so a delayed source error cannot be
        // mistaken for a clean successful exit while a hung source cannot park
        // wait_any/wait_all forever.
        self.finalize_stdin_task().await;
        if let Some(error) = self.take_teardown_error(String::new(), String::new(), None) {
            return Err(error);
        }
        self.checked_outcome(outcome)
    }

    /// Run the process to completion while sampling CPU and memory every `every`,
    /// returning a [`RunProfile`](crate::stats::RunProfile). Behaves like
    /// [`wait`](Self::wait) — output is discarded, timeout applies. A zero
    /// `every` is clamped to 1 ms.
    ///
    /// # Errors
    ///
    /// The same surface as [`wait`](Self::wait): a timeout or signal-kill is
    /// *captured* in the returned [`RunProfile`](crate::stats::RunProfile)'s
    /// outcome, not raised. The `Err` cases are [`ErrorReason::Cancelled`] (cancelled
    /// via [`Command::cancel_on`](crate::Command::cancel_on)),
    /// [`ErrorReason::Teardown`] (the terminal timeout/cancellation teardown could
    /// not be confirmed), [`ErrorReason::Stdin`]
    /// (a non-broken-pipe stdin-source failure on an otherwise-successful run),
    /// or [`ErrorReason::Io`] (waiting on the child failed).
    #[cfg(feature = "stats")]
    pub async fn profile(mut self, every: Duration) -> Result<crate::stats::RunProfile> {
        use std::sync::{Arc, Mutex};

        // tokio panics on a zero interval period; clamp rather than panic a
        // detached sampling task on a legal-looking input.
        let every = every.max(Duration::from_millis(1));
        let started = self.started;
        let acc = Arc::new(Mutex::new(ProfileAcc::default()));
        // Set by `on_exit` once the child is reaped so the sampler stops early — an
        // optimization, not the load-bearing guard. The real protection is the
        // identity gate: each sample calls `process_metrics(pid, identity)`, which
        // returns the all-`None` default if the pid was recycled for an unrelated
        // process, so even a sample that slips past this flag (the reap lands a few
        // frames earlier in `backend_wait`, and the pump drain can run for
        // PUMP_TEARDOWN on a leaked pipe) can never fold a stranger's counters.
        let reaped = Arc::new(AtomicBool::new(false));
        let identity = self.proc_identity;
        let sampler = self.pid.map(|pid| {
            let acc = Arc::clone(&acc);
            let reaped = Arc::clone(&reaped);
            // The identity captured at spawn binds every reading to *this* child.
            tokio::spawn(run_profile_sampler(every, reaped, acc, move || {
                crate::sys::process_metrics(pid, identity)
            }))
        });

        // Abort the sampler if the `profile()` future is dropped before it returns
        // (e.g. `tokio::time::timeout(d, p.profile(e))`).`on_exit` below is the
        // primary path; this is the fallback.
        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _sampler_guard = sampler.as_ref().map(|h| AbortOnDrop(h.abort_handle()));

        // Stop the sampler as the reap is observed: set the flag (its next tick
        // breaks) and abort the task. `abort` is async and the pump drain can run for
        // PUMP_TEARDOWN on a leaked pipe, so a late tick can still fire — but the
        // identity gate (see the `reaped` comment above) makes any such reading harmless.
        let outcome = self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, |_| {
                reaped.store(true, Ordering::Release);
                if let Some(task) = &sampler {
                    task.abort();
                }
            })
            .await?
            .outcome;
        let duration = started.elapsed();
        let (cpu_time, peak_memory_bytes, samples) = match acc.lock() {
            Ok(acc) => (acc.cpu_time, acc.peak_memory_bytes, acc.samples),
            Err(_) => (None, None, 0),
        };
        Ok(crate::stats::RunProfile {
            outcome,
            duration,
            cpu_time,
            peak_memory_bytes,
            samples,
        })
    }

    /// Shared consuming core behind `output_string`, `wait`, and `profile`:
    /// spawn pumps, drive to exit, call `on_exit` between the await and `?`
    /// (fires even on error — `profile` uses it to abort the sampler before
    /// reap), join pumps, check cancellation, drain per `capture`.
    ///
    /// `expose_counts` stores the sinks on `self` for the live
    /// `stdout_line_count`/`stderr_line_count` accessors.
    ///
    /// `output_bytes` and `finish` deliberately do not route here — their
    /// teardown spines differ by nature.
    async fn finish_lines(
        &mut self,
        capture: CaptureMode,
        expose_counts: bool,
        on_exit: impl FnOnce(ExitObservation),
    ) -> Result<FinishedLines> {
        // The capturing path needs a piped stdout; fail loudly rather than return
        // empty. The discard paths (wait/profile/drain) hand nothing back, so a
        // non-piped stdout is fine for them — they are exempt.
        if matches!(capture, CaptureMode::Lines) {
            self.ensure_stdout_capturable()?;
        }
        // Reuse a sink already populated by a prior readiness or streaming call
        // so output_string after stdout_lines/events sees those lines rather
        // than returning empty. For the discard paths use a retain-nothing sink
        // (not the user's retention policy) so a chatty child never accumulates
        // O(total) heap in wait/profile/drain. The byte cap bounds the pump's
        // in-flight line assembly too — `bounded(0)` alone retains no lines but
        // would still let a newline-free flood grow the in-flight buffer without
        // limit. `wait`/`profile` pin that cap to the fixed `discard_sink_policy`;
        // `drain` honors the caller's configured `output_buffer` byte cap
        // (`drain_sink_policy`) so held memory tracks the *configured* limit.
        let sink_policy: OutputBufferPolicy = match capture {
            CaptureMode::Discard => discard_sink_policy(),
            CaptureMode::DrainBounded => drain_sink_policy(&self.buffer),
            CaptureMode::Lines => self.buffer,
        };
        let discard_in_flight_cap = sink_policy.max_bytes;
        let stdout_sink = self.stdout_sink.clone().unwrap_or_else(|| {
            SharedLines::new_with_activity(&sink_policy, self.output_activity.clone())
        });
        let stderr_sink = self.stderr_sink.clone().unwrap_or_else(|| {
            SharedLines::new_with_activity(&sink_policy, self.output_activity.clone())
        });
        // The discard verbs must never accumulate a user-policy backlog. A sink
        // adopted from a *dropped* stream is still in the caller's
        // `OutputBufferPolicy` (possibly unbounded); switch it to retain-nothing
        // *before* `drive_to_exit` so a chatty child can't grow O(total) heap
        // while we wait for it to exit. A freshly built sink already uses a
        // retain-nothing policy (`discard_sink_policy`/`drain_sink_policy`), so
        // this is a no-op there. Both discard paths (`wait`/`profile` and `drain`)
        // share this one `start_discarding` seam rather than forking a second
        // retain-nothing variant, keeping the `DropNewest` seal-on-first-drop
        // latch (K-054) single-sourced. The capture path (`output_string`) leaves
        // the sink untouched so it can still hand back the streamed tail.
        if matches!(capture, CaptureMode::Discard | CaptureMode::DrainBounded) {
            let cap = discard_in_flight_cap
                .expect("discard sink policies always carry an in-flight byte cap");
            stdout_sink.start_discarding(cap);
            stderr_sink.start_discarding(cap);
        }
        self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        if expose_counts {
            if self.stdout_sink.is_none() {
                self.stdout_sink = Some(stdout_sink.clone());
            }
            if self.stderr_sink.is_none() {
                self.stderr_sink = Some(stderr_sink.clone());
            }
        }

        let outcome = self.drive_to_exit().await;
        on_exit(self.exit_observation(&outcome));
        let outcome = outcome?;
        self.observe_stdin_task().await;
        let pumps: Vec<_> = [self.stdout_pump.take(), self.stderr_pump.take()]
            .into_iter()
            .flatten()
            .collect();
        join_pumps(pumps).await;
        // Re-observe stdin after the pumps drained: a writer that failed inside
        // the teardown window is only visible now (see `finalize_stdin_task`).
        self.finalize_stdin_task().await;

        let (stdout_lines, stderr_lines) = match capture {
            CaptureMode::Lines => (stdout_sink.drain(), stderr_sink.drain()),
            CaptureMode::Discard | CaptureMode::DrainBounded => (Vec::new(), Vec::new()),
        };
        if let Some(error) =
            self.take_teardown_error(stdout_lines.join("\n"), stderr_lines.join("\n"), None)
        {
            return Err(error);
        }
        let outcome = self.checked_outcome(outcome)?;

        if matches!(capture, CaptureMode::Lines) {
            for sink in [&stdout_sink, &stderr_sink] {
                if sink.overflowed() {
                    return Err(crate::ErrorReason::OutputTooLarge {
                        program: self.program.clone(),
                        max_lines: self.buffer.max_lines,
                        max_bytes: self.buffer.max_bytes,
                        total_lines: sink.count(),
                        total_bytes: sink.seen_bytes(),
                    }
                    .into());
                }
            }
        }

        // A first OS read error on either pipe means the capture is incomplete:
        // surface it as `ErrorReason::Io` for the capturing (`output_string`) and the
        // discard (`wait`/`profile`) paths alike, rather than reporting a
        // silently-short read as a full success. Checked after the fail-loud
        // overflow ceiling (the more specific signal if both fire) and after
        // `checked_outcome` (so cancellation/stdin priority is preserved); a
        // broken-pipe read was already folded into a clean EOF by the pump, so a
        // normal writer-closed stream never trips this.
        for sink in [&stdout_sink, &stderr_sink] {
            if let Some(source) = sink.take_read_error() {
                return Err(Error::io(source));
            }
        }

        Ok(FinishedLines {
            outcome,
            stdout_lines,
            stderr_lines,
        })
    }

    /// Spawn line pumps for still-untaken pipes into the given sinks.
    /// Handles stored on `self` so `Drop` aborts them on error propagation.
    fn spawn_line_pumps(&mut self, stdout_sink: &Arc<SharedLines>, stderr_sink: &Arc<SharedLines>) {
        if let Some(pipe) = self.backend.take_stdout_reader() {
            self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stdout_config.clone(),
                stdout_sink.clone(),
            )));
        }
        if let Some(pipe) = self.backend.take_stderr_reader() {
            self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_config.clone(),
                stderr_sink.clone(),
            )));
        }
    }

    /// Post-exit checkpoint every consuming path passes after pumps settle and
    /// after any retained terminal teardown failure has taken precedence:
    /// cancellation wins next (returns `Err(Cancelled)`), then a non-broken-pipe
    /// stdin failure surfaces as `Err(Stdin)` only on an otherwise-successful run.
    fn checked_outcome(&mut self, outcome: Outcome) -> Result<Outcome> {
        // Pre-pump snapshot: prevents a cancel firing during `join_pumps` from
        // discarding real output. `unwrap_or(false)` — `None` is not yet
        // snapshotted; treat conservatively as "not cancelled".
        if self.cancel_at_exit.unwrap_or(false) {
            return Err(ErrorReason::Cancelled {
                program: self.program.clone(),
            }
            .into());
        }
        let succeeded = matches!(outcome, Outcome::Exited(code) if self.ok_codes.contains(&code));
        if succeeded && let Some(source) = self.stdin_error.take() {
            return Err(ErrorReason::Stdin {
                program: self.program.clone(),
                source,
            }
            .into());
        }
        Ok(outcome)
    }

    /// Convert the first unconfirmed teardown into its public structured error
    /// only after the caller's existing bounded pump drain has salvaged output.
    fn take_teardown_error(
        &mut self,
        stdout: String,
        stderr: String,
        stdout_bytes: Option<Vec<u8>>,
    ) -> Option<Error> {
        let failure = self
            .teardown_failure
            .lock()
            .expect("teardown failure slot poisoned")
            .take()?;
        Some(Error::teardown(
            self.program.clone(),
            failure.cause,
            failure.operation,
            failure.source,
            stdout,
            stderr,
            stdout_bytes,
        ))
    }

    /// Non-blocking pre-pump peek at the stdin writer: stash a non-broken-pipe
    /// failure of a writer that has *already finished* in `self.stdin_error` for
    /// `checked_outcome`. A still-running writer is re-parked — it might yet fail
    /// inside the `join_pumps` window — and picked up by the final,
    /// post-pump [`finalize_stdin_task`](Self::finalize_stdin_task). Peeking
    /// (never blocking) here keeps the fast path cheap and never waits on a
    /// hung writer.
    async fn observe_stdin_task(&mut self) {
        let task = match &mut self.backend {
            Backend::Real(real) => real.stdin_task.take(),
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => pty.stdin_task.take(),
            Backend::Scripted(_) => None,
        };
        let Some(task) = task else {
            return;
        };
        if !task.is_finished() {
            // Not done yet — re-park for the post-pump `finalize_stdin_task`, so
            // a writer that fails during `join_pumps` is not silently lost.
            match &mut self.backend {
                Backend::Real(real) => real.stdin_task = Some(task),
                #[cfg(feature = "pty")]
                Backend::Pty(pty) => pty.stdin_task = Some(task),
                Backend::Scripted(_) => {}
            }
            return;
        }
        let observed = Self::classify_stdin_join(task.await);
        self.record_stdin_error(observed);
    }

    /// Final stdin-writer observation, run after `join_pumps` (or directly after
    /// a borrowed wait reaps the child) and before `checked_outcome`. The
    /// pre-pump [`observe_stdin_task`](Self::observe_stdin_task) only peeks
    /// non-blockingly, so a writer that failed with a non-broken-pipe error
    /// *inside* the `join_pumps` window (up to [`PUMP_TEARDOWN`]) — e.g. a
    /// `from_reader`/`from_file` source that erred while the pumps were still
    /// draining the child's output — was re-parked and would otherwise never
    /// reach `self.stdin_error`, letting an otherwise-successful run report a
    /// silent success (exactly the case `ErrorReason::Stdin` exists to diagnose).
    ///
    /// This waits for that writer, but only *bounded* by [`PUMP_TEARDOWN`]: a
    /// writer still blocked on a genuinely hung source is aborted and left
    /// unreported rather than stalling the caller forever — the same "never wait
    /// on a hung writer" contract the pre-fix single peek kept. In the common
    /// case the writer already finished (the pre-pump peek took it, or it wraps
    /// up during pump teardown), so the timeout resolves immediately.
    async fn finalize_stdin_task(&mut self) {
        // Keep the JoinHandle in its owning slot while waiting. If the borrowed
        // wait is cancelled after the child reap, dropping a timeout around an
        // owned JoinHandle would detach the writer and make the next wait unable
        // to observe its source error. Borrowing the handle leaves it available
        // for that next wait; only a completed or explicitly-aborted task is
        // removed below.
        let observed = {
            let Some(slot) = self.stdin_task_slot() else {
                return;
            };
            let Some(task) = slot.as_mut() else {
                return;
            };
            let abort = task.abort_handle();
            match tokio::time::timeout(PUMP_TEARDOWN, &mut *task).await {
                Ok(joined) => Self::classify_stdin_join(joined),
                // Still writing after the teardown grace — a hung source. Abort
                // it and remove the handle, never blocking the caller forever.
                Err(_elapsed) => {
                    abort.abort();
                    None
                }
            }
        };
        let _ = self.stdin_task_slot().and_then(Option::take);
        self.record_stdin_error(observed);
    }

    /// Return the owning slot for the background stdin writer, if this backend
    /// has one. The test-only slot takes precedence so scripted handles exercise
    /// the same cancellation boundary as real and PTY handles.
    fn stdin_task_slot(&mut self) -> Option<&mut Option<JoinHandle<std::io::Result<()>>>> {
        #[cfg(test)]
        if self.test_stdin_task.is_some() {
            return Some(&mut self.test_stdin_task);
        }
        match &mut self.backend {
            Backend::Real(real) => Some(&mut real.stdin_task),
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => Some(&mut pty.stdin_task),
            Backend::Scripted(_) => None,
        }
    }

    /// Classify a finished stdin-writer join into a recordable failure, if any.
    /// A routine EPIPE (the child closed stdin before consuming all input) is not
    /// a failure; a genuine read/write error or a task panic is. A cancelled
    /// `JoinError` is never seen here — the abort sites (`Drop` and
    /// `finalize_stdin_task`'s timeout) never await the handle afterward.
    fn classify_stdin_join(
        joined: std::result::Result<std::io::Result<()>, tokio::task::JoinError>,
    ) -> Option<std::io::Error> {
        match joined {
            Ok(Ok(())) => None,
            // Routine EPIPE (child exited before reading all stdin) — not a failure.
            Ok(Err(e)) if is_broken_pipe(&e) => None,
            Ok(Err(e)) => Some(e),
            // In practice a panic; the abort sites take the handle without awaiting it.
            Err(join_err) => Some(std::io::Error::other(if join_err.is_panic() {
                format!("stdin writer task panicked: {join_err}")
            } else {
                format!("stdin writer task did not complete: {join_err}")
            })),
        }
    }

    /// Record a classified stdin-writer failure for `checked_outcome`, tracing it
    /// once. A no-op when there was no failure.
    fn record_stdin_error(&mut self, observed: Option<std::io::Error>) {
        if let Some(e) = observed {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "processkit",
                program = %self.program,
                error = %e,
                "stdin writer failed"
            );
            self.stdin_error = Some(e);
        }
    }

    #[cfg(test)]
    fn set_test_stdin_task(&mut self, task: JoinHandle<std::io::Result<()>>) {
        self.test_stdin_task = Some(task);
    }

    /// Abort all watchdog tasks and clear the recorded pid after reap.
    /// Aborting before the pid is freed limits the recycled-pid window to a
    /// scheduler quantum (an already-executing kill cannot be recalled).
    fn abort_watchdogs(&mut self) {
        // Retire the gate too: `abort_watchdogs` only runs after a reap, so once
        // it does, no detached watchdog may raw-kill the (now freed) pid. This is
        // idempotent with the earlier retire the owner-driven reap paths already
        // did before freeing the pid; here it is the canonical post-reap backstop.
        self.pid_gate.retire();
        self.pid = None;
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.inactivity_task.take() {
            task.abort();
        }
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
    }

    /// Post-reap bookkeeping run in one fixed order: (1) snapshot the cancel
    /// disposition from `cause` (first-observation wins — not a post-hoc token
    /// read), (2) abort watchdogs, (3) classify a fired deadline as `TimedOut`.
    fn on_reaped(&mut self, cause: ExitCause) -> Outcome {
        if self.cancel_at_exit.is_none() {
            self.cancel_at_exit = Some(matches!(cause, ExitCause::Cancelled));
        }
        self.abort_watchdogs();
        let outcome = match cause {
            ExitCause::Exited(outcome) => outcome,
            // Moot — `checked_outcome` maps the cancel snapshot to `Err(Cancelled)`.
            ExitCause::Cancelled => Outcome::Signalled(None),
            ExitCause::TeardownFailed { .. } => {
                unreachable!("an unconfirmed teardown is not a reap")
            }
        };
        let outcome = self.classify_watchdog_timeout(outcome);
        // Feed a live `events()` lifecycle stream its terminal
        // `ProcessEvent::Exited`. This is the single reap choke point every
        // consuming finisher (`finish`/`wait`/`drain`/…) funnels through, so the
        // stream sees the exact `Outcome` the finisher reports. A no-op unless
        // `events()` armed a stream; `send` failing (the stream was dropped) is
        // ignored. Purely additive — no reap/gate/teardown invariant is touched.
        if let Some(tx) = self.exit_event_tx.take() {
            let _ = tx.send(outcome);
        }
        outcome
    }

    /// Wait for the child to exit, applying the timeout (killing the tree on
    /// elapse). Returns the [`Outcome`] of the run.
    async fn drive_to_exit(&mut self) -> Result<Outcome> {
        // Close an untaken `keep_stdin_open` writer so a stdin-reading child sees
        // EOF instead of blocking to its timeout. PTY writers translate close to
        // the platform terminal gesture (configured VEOF on Unix, Ctrl-Z+Enter on
        // ConPTY) because a master has no ordinary pipe-style half-close.
        match &mut self.backend {
            Backend::Real(real) => drop(real.stdin_pipe.take()),
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => drop(pty.writer.take()),
            Backend::Scripted(_) => {}
        }
        // Short-circuit when already reaped: re-running the select would fire the
        // cancel arm immediately for an already-cancelled token and overwrite the
        // snapshot. `backend_wait` returns the cached status; `on_reaped` preserves
        // the first-observation snapshot.
        let cause = if self.cancel_at_exit.is_some() {
            ExitCause::Exited(self.backend_wait().await?)
        } else {
            self.drive_to_exit_inner().await?
        };
        let outcome = match cause {
            ExitCause::TeardownFailed {
                intended,
                cancelled,
            } => {
                if self.cancel_at_exit.is_none() {
                    self.cancel_at_exit = Some(cancelled);
                }
                // No exit event or exit metric: the point of the deferred error is
                // that the child/tree has not been confirmed terminal.
                return Ok(intended);
            }
            reaped => self.on_reaped(reaped),
        };
        // One elapsed read off the existing `started` anchor, shared by both
        // observability seams — metrics add no third clock (K-007).
        #[cfg(any(feature = "tracing", feature = "metrics"))]
        let elapsed = self.started.elapsed();
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            program = %self.program,
            outcome = ?outcome,
            elapsed_ms = elapsed.as_millis() as u64,
            "process exited"
        );
        // `on_reaped` has already snapshotted the cancel disposition, so a run
        // torn down by cancellation (reported here as `Signalled(None)`) is tallied
        // as `cancelled`, not `signalled`.
        #[cfg(feature = "metrics")]
        crate::metrics::record_run(
            &self.program,
            &outcome,
            self.cancel_at_exit == Some(true),
            elapsed,
        );
        Ok(outcome)
    }

    /// Convert the just-completed wait boundary into the pipeline observer's
    /// proof signal without consuming the deferred public teardown error. A
    /// `TeardownFailed` drive deliberately returns its intended outcome so pumps
    /// can salvage diagnostics; the retained failure must therefore be checked
    /// before treating any `Ok` as a successful reap.
    fn exit_observation(&self, result: &Result<Outcome>) -> ExitObservation {
        if let Some(failure) = self
            .teardown_failure
            .lock()
            .expect("teardown failure slot poisoned")
            .as_ref()
        {
            return ExitObservation::Unconfirmed {
                cause: Some(failure.cause),
                source: clone_io_error(&failure.source),
            };
        }

        match result {
            Ok(_) => ExitObservation::Reaped,
            Err(error) => {
                let (cause, source) = match error.reason() {
                    ErrorReason::Teardown { cause, source, .. } => {
                        (Some(*cause), clone_io_error(source))
                    }
                    ErrorReason::Io(source) | ErrorReason::Spawn { source, .. } => {
                        (None, clone_io_error(source))
                    }
                    _ => (None, std::io::Error::other(error.to_string())),
                };
                ExitObservation::Unconfirmed { cause, source }
            }
        }
    }

    /// A fired deadline overrides whatever `backend_wait` observed — a child that
    /// exits cleanly within the grace still timed out. Cancellation is classified
    /// later in `checked_outcome` and always wins over `TimedOut`.
    fn classify_watchdog_timeout(&self, outcome: Outcome) -> Outcome {
        match self.timeout_state.load(Ordering::Acquire) {
            TS_TIMED_OUT => Outcome::TimedOut,
            TS_INACTIVITY_TIMED_OUT => Outcome::InactivityTimedOut,
            _ => outcome,
        }
    }

    /// Raw exit wait — no timeout/cancel. Real: maps exit status to `Outcome`
    /// (captures Unix signal number when available). Scripted: resolves at the
    /// canned `exit_at`, or immediately as `Signalled` if killed.
    async fn backend_wait(&mut self) -> Result<Outcome> {
        #[cfg(all(test, feature = "process-control"))]
        if let Some(failure) = take_backend_wait_failure() {
            failure.entered.store(true, Ordering::Release);
            failure.entered_changed.notify_waiters();
            failure.wait_until_released().await;
            return Err(Error::io(std::io::Error::from_raw_os_error(
                failure.raw_os_error,
            )));
        }

        let gate = self.pid_gate.clone();
        let outcome = match &mut self.backend {
            Backend::Real(real) => {
                // Reap the child *inside* the gate lock so the pid-freeing reap and
                // the retire are one indivisible step — closing the window a plain
                // `child.wait().await` then `retire()` leaves open, where a racing
                // detached watchdog (cancel/deadline) could raw-kill the freed (and
                // possibly OS-recycled) pid. tokio frees the pid via `try_wait`
                // *inside* `Child::wait()`'s poll (see tokio's `Reaper::poll`), so
                // running that poll under the gate lock via `reap_under_lock` makes
                // the pid-free and the retire atomic: a watchdog's gated kill takes
                // the same lock and so either lands entirely before this reap (pid
                // still valid) or is skipped (retired first). A dropped wait (a
                // `wait_any`/`wait_all` loser whose future is cancelled) simply stops
                // polling — no reap, gate untouched — so losers stay usable.
                let status = gated_reap(&gate, real.child_mut())
                    .await
                    .map_err(Error::io)?;
                outcome_of_exit_status(&status)
            }
            // The PTY child reaps through the same gate discipline as `Real` (the
            // Unix pty child IS a tokio `Child`; the Windows ConPTY child holds its
            // process handle open across the wait, so its pid is never freed
            // mid-wait). `PtyExitStatus` carries the code and (Unix) the signal.
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => {
                let status = pty.child_mut().reap(&gate).await.map_err(Error::io)?;
                outcome_of_pty_exit_status(&status)
            }
            // A scripted double owns no OS process, so its gate is pid-less: no reap
            // frees an OS pid and every gated kill is a no-op. The `.await` cannot
            // hold the lock, but there is no pid to recycle, so the retire below
            // suffices.
            Backend::Scripted(s) => s.wait_outcome().await,
        };
        // Real: the `gated_reap` above already retired atomically with the reap;
        // this is an idempotent backstop. Scripted: the retire that stands the
        // (pid-less) watchdogs down.
        self.pid_gate.retire();
        // Claim natural reap. If a deadline already won (`TS_TIMED_OUT`), this
        // CAS fails and the run stays `TimedOut`.
        let _ = deadline::claim_exited(&self.timeout_state);
        Ok(outcome)
    }

    /// Race the cancel token against the deadline-bounded wait. Unset knobs
    /// become never-resolving arms. `biased` with cancel first, so a simultaneous
    /// cancel+deadline is always resolved **as a cancellation** — the winner picks
    /// the teardown, and the run reports `Cancelled` (never `TimedOut`).
    ///
    /// What that tie means for the *manner* of the teardown follows the cancel
    /// path's own policy, and is deliberately documented here rather than left to
    /// drift (T-255):
    ///
    /// - **Without `cancel_grace` (the default, unchanged):** the cancel arm hard-
    ///   kills, so a simultaneous cancel+deadline still bypasses the graceful tier
    ///   entirely — exactly as before these knobs existed, even when `timeout_grace`
    ///   is configured.
    /// - **With `cancel_grace`:** the cancel arm runs *its own* soft-signal → grace
    ///   → hard-kill ladder, so the tie is now graceful too. This is a deliberate
    ///   change from "a tie always hard-kills": the caller explicitly asked
    ///   cancellation to be graceful, and making the goodbye hinge on whether the
    ///   deadline happened to land in the same poll would be a scheduling-dependent
    ///   surprise (and would hard-kill even a run that set *both* graces). The
    ///   outcome remains `ErrorReason::Cancelled` either way when teardown is
    ///   confirmed; an unconfirmed teardown becomes `ErrorReason::Teardown`.
    async fn drive_to_exit_inner(&mut self) -> Result<ExitCause> {
        // Reclaim teardown from the streaming deadline watchdog before reaping.
        // This future owns the `Child` and drives BOTH kills through it — the
        // deadline via `teardown_on_timeout` and cancel via `teardown_on_cancel`
        // (`kill_tree` by default, the shared graceful ladder with `cancel_grace`),
        // whose `start_kill` is a no-op once the child is reaped and so can never
        // signal a recycled pid. `retire` the gate FIRST (so a watchdog racing us stands
        // down its raw-pid kill), THEN abort the deadline watchdog so only our own
        // arm fires the graceful teardown. Retiring *before* the reap is what
        // fully closes the window: a racing watchdog's raw kill runs under the gate
        // lock, so it either lands before this retire (pid still valid) or is
        // skipped — it can never win a kill on a pid this reap is about to free.
        // During pure streaming (no finisher, so nothing retires) the gate stays
        // live and the watchdog remains the sole killer of a genuinely un-reaped
        // child, as it must to bound the timeout.
        self.pid_gate.retire();
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.inactivity_task.take() {
            task.abort();
        }
        if let Some(failure) = self
            .teardown_failure
            .lock()
            .expect("teardown failure slot poisoned")
            .as_ref()
        {
            let (intended, cancelled) = match failure.cause {
                TeardownCause::Timeout => (Outcome::TimedOut, false),
                TeardownCause::InactivityTimeout => (Outcome::InactivityTimedOut, false),
                TeardownCause::Cancellation => (Outcome::Signalled(None), true),
                TeardownCause::ExplicitKill => (Outcome::Signalled(None), false),
                TeardownCause::PipelineFailure => (Outcome::Signalled(None), false),
            };
            return Ok(ExitCause::TeardownFailed {
                intended,
                cancelled,
            });
        }
        // Own the knobs so the helper futures borrow nothing from `self` —
        // only `self.backend_wait()` does, keeping the select! borrows disjoint.
        let limit = self.timeout;
        let inactivity_limit = self.inactivity_timeout;
        let output_activity = self.output_activity.clone();
        let configured = self.cancel_token.clone();
        let additional = self.additional_cancel_token.clone();
        // The deadline anchor is on tokio's clock (see the field docs) so the
        // `limit - started.elapsed()` in `wait_deadline_and_claim` counts virtual
        // time already burned before this consuming call armed the deadline.
        let started = self.deadline_anchor;
        let cancelled = wait_for_cancellation(configured, additional);
        // Anchor to spawn time so a late consuming call can't re-grant the full
        // limit. The CAS runs as part of this raced future itself (rather than
        // after `select!` names a winner). With the streaming watchdog now
        // reclaimed above, this arm is the sole claimant of `TS_TIMED_OUT`, so the
        // two orderings are equivalent — and the shared arbiter core lives in one
        // place either way.
        let timeout_state = self.timeout_state.clone();
        let deadline = async move {
            match limit {
                Some(limit) => {
                    deadline::wait_deadline_and_claim(started, limit, &timeout_state).await
                }
                None => std::future::pending::<bool>().await,
            }
        };
        let inactivity_state = self.timeout_state.clone();
        let inactivity = async move {
            match inactivity_limit {
                Some(limit) => {
                    output_activity.wait_for_inactivity(limit).await;
                    deadline::claim_inactivity_timed_out(&inactivity_state)
                }
                None => std::future::pending::<bool>().await,
            }
        };
        tokio::select! {
            biased; // cancel arm checked first: always beats a simultaneous deadline
            () = cancelled => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: "processkit",
                    program = %self.program,
                    cancel_grace_ms = self.cancel_grace.map(|g| g.as_millis() as u64),
                    "cancellation fired; tearing the tree down"
                );
                // `cancel_grace` unset (the default) → the unchanged immediate hard
                // kill; set → the same graceful ladder the deadline arm drives.
                match self.teardown_on_cancel().await {
                    Ok(()) => Ok(ExitCause::Cancelled),
                    Err(failure) => {
                        record_teardown_failure(&self.teardown_failure, failure);
                        Ok(ExitCause::TeardownFailed {
                            intended: Outcome::Signalled(None),
                            cancelled: true,
                        })
                    }
                }
            }
            outcome = self.backend_wait() => outcome.map(ExitCause::Exited),
            _won = deadline => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    program = %self.program,
                    timeout_ms = limit.map(|l| l.as_millis() as u64).unwrap_or(0),
                    "timeout elapsed; killing the tree"
                );
                match self.teardown_on_timeout(TeardownCause::Timeout).await {
                    Ok(()) => Ok(ExitCause::Exited(Outcome::TimedOut)),
                    Err(failure) => {
                        record_teardown_failure(&self.teardown_failure, failure);
                        Ok(ExitCause::TeardownFailed {
                            intended: Outcome::TimedOut,
                            cancelled: false,
                        })
                    }
                }
            }
            _won = inactivity => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    program = %self.program,
                    inactivity_ms = inactivity_limit.map(|l| l.as_millis() as u64).unwrap_or(0),
                    "output inactivity elapsed; killing the tree"
                );
                match self.teardown_on_timeout(TeardownCause::InactivityTimeout).await {
                    Ok(()) => Ok(ExitCause::Exited(Outcome::InactivityTimedOut)),
                    Err(failure) => {
                        record_teardown_failure(&self.teardown_failure, failure);
                        Ok(ExitCause::TeardownFailed {
                            intended: Outcome::InactivityTimedOut,
                            cancelled: false,
                        })
                    }
                }
            }
        }
    }

    /// Hard-kill the child and its tree (for a private group), then reap.
    async fn kill_tree(
        &mut self,
        cause: TeardownCause,
    ) -> std::result::Result<(), TeardownFailure> {
        let gate = self.pid_gate.clone();
        match &mut self.backend {
            Backend::Real(real) => {
                let child_error = child_start_kill("real", || real.child_mut().start_kill()).err();
                // The child is being torn down through the owned `Child`; retire
                // the gate (before the reap below frees the pid) so the
                // cancel/deadline watchdogs stand down rather than racing that reap
                // with a raw `kill(pid)` that could land on a recycled pid.
                gate.retire();
                let group_error = if let Some(group) = &real.own_group {
                    // On Linux + legacy/restricted cgroup this can synchronously
                    // block this worker thread up to ~100ms — accepted, not
                    // routed through `spawn_blocking`; see the sweep loop in
                    // `Cgroup::kill` (src/sys/linux.rs) for the full rationale.
                    // ~100ms is the ceiling for every backend: FreeBSD's reaper
                    // keeps its post-kill corpse drain in `Drop` alone (see
                    // `DRAIN_BUDGET`, src/sys/freebsd.rs), so this call does not
                    // block there at all.
                    group_hard_kill(group).err()
                } else {
                    None
                };
                // Bound the reap: a D-state child can ignore SIGKILL until I/O
                // unblocks, and an unbounded wait hangs shared-group handles.
                let reap = confirm_reap(PUMP_TEARDOWN, real.child_mut().wait()).await;
                if let Some(source) = group_error {
                    return Err(TeardownFailure {
                        cause,
                        operation: "process-group hard kill",
                        source,
                    });
                }
                if let Err(source) = reap {
                    return Err(TeardownFailure {
                        cause,
                        operation: if child_error.is_some() {
                            "direct child hard kill"
                        } else {
                            "child terminal reap"
                        },
                        source: child_error.unwrap_or(source),
                    });
                }
            }
            // The PTY child tears down exactly like `Real`: kill through the owned
            // handle, retire the gate before the group kill, then bound the reap.
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => {
                let child_error = child_start_kill("pty", || pty.child_mut().start_kill()).err();
                gate.retire();
                let group_error = if let Some(group) = &pty.own_group {
                    group_hard_kill(group).err()
                } else {
                    None
                };
                let reap = confirm_reap(PUMP_TEARDOWN, pty.child_mut().wait()).await;
                if let Some(source) = group_error {
                    return Err(TeardownFailure {
                        cause,
                        operation: "process-group hard kill",
                        source,
                    });
                }
                if let Err(source) = reap {
                    return Err(TeardownFailure {
                        cause,
                        operation: if child_error.is_some() {
                            "direct child hard kill"
                        } else {
                            "child terminal reap"
                        },
                        source: child_error.unwrap_or(source),
                    });
                }
            }
            Backend::Scripted(s) => s.kill(),
        }
        Ok(())
    }

    /// Teardown when the deadline elapses. With `timeout_grace`: signal → wait up
    /// to grace → SIGKILL, so a signal-handling child ends the grace early. Without
    /// grace: hard `kill_tree`. Windows has no signal tier; graceful degrades to
    /// the atomic kill.
    async fn teardown_on_timeout(
        &mut self,
        cause: TeardownCause,
    ) -> std::result::Result<(), TeardownFailure> {
        match self.timeout_grace {
            Some(grace) => {
                self.graceful_teardown(grace, self.timeout_signal, cause)
                    .await
            }
            None => self.kill_tree(cause).await,
        }
    }

    /// Teardown when the **cancel token** fires — the exact mirror of
    /// [`teardown_on_timeout`](Self::teardown_on_timeout), reading the cancellation
    /// knobs instead of the deadline ones. With
    /// [`Command::cancel_grace`](crate::Command::cancel_grace) it drives the SAME
    /// soft-signal → grace → hard-kill ladder (one seam, not a second cancellation
    /// driver); without it — the default — it is the unchanged immediate
    /// `kill_tree`, so a run that never opts in behaves exactly as before.
    ///
    /// The *ordinary* outcome is unaffected: after confirmed teardown the caller
    /// still reports `ExitCause::Cancelled` (and so `ErrorReason::Cancelled`)
    /// whichever branch ran. An OS failure that leaves teardown unconfirmed is
    /// retained separately and surfaces as `ErrorReason::Teardown`.
    async fn teardown_on_cancel(&mut self) -> std::result::Result<(), TeardownFailure> {
        match self.cancel_grace {
            Some(grace) => {
                self.graceful_teardown(grace, self.cancel_signal, TeardownCause::Cancellation)
                    .await
            }
            None => self.kill_tree(TeardownCause::Cancellation).await,
        }
    }

    /// The shared graceful teardown both the deadline and the cancellation paths
    /// drive: send `signal` to the tree, give it up to `grace` to drain, then hard
    /// kill — reaping concurrently so a signal-handling child ends the grace early.
    /// Windows has no signal tier; the graceful branch degrades to the atomic kill.
    ///
    /// Whole-tree work is delegated to
    /// [`ProcessGroup::graceful_terminate`](crate::ProcessGroup::graceful_terminate)
    /// (and so to the crate's single `sys::graceful::run` escalation driver); a
    /// shared-group handle owns no group and so reaches only its own direct child.
    async fn graceful_teardown(
        &mut self,
        grace: Duration,
        signal: i32,
        cause: TeardownCause,
    ) -> std::result::Result<(), TeardownFailure> {
        let gate = self.pid_gate.clone();
        match &mut self.backend {
            Backend::Real(real) => match real.own_group.clone() {
                // Own group: tear the whole tree down pgid/cgroup-scoped (which
                // never touches the raw pid, so it is recycled-pid safe), reaping
                // concurrently so a signal-handling child that exits ends the grace
                // early instead of eating a pointless `SIGKILL`.
                Some(group) => {
                    let teardown = async move { group_graceful_kill(&group, grace, signal).await };
                    // Bound the reap: a D-state child can ignore the final SIGKILL.
                    let reap = async {
                        let r = confirm_reap(
                            grace.saturating_add(PUMP_TEARDOWN),
                            real.child_mut().wait(),
                        )
                        .await;
                        // The group teardown never raw-kills, so retiring here only
                        // keeps the gate consistent for any lingering external
                        // watchdog once the pid is freed.
                        gate.retire();
                        r
                    };
                    let (teardown, reap) = tokio::join!(teardown, reap);
                    if let Err(source) = teardown {
                        return Err(TeardownFailure {
                            cause,
                            operation: "process-group graceful escalation",
                            source,
                        });
                    }
                    if let Err(source) = reap {
                        return Err(TeardownFailure {
                            cause,
                            operation: "child terminal reap",
                            source,
                        });
                    }
                }
                // Shared group: we own no group, so we reach only the direct child.
                // Escalate the hard kill through the OWNED `Child` (`start_kill`)
                // instead of a raw `kill(pid)`, so the SIGKILL is reaped by the same
                // `Child` and can never outlive the reap to hit a recycled pid — the
                // recycled-pid hazard the pid-only path guards with the gate is
                // simply absent here. Only the graceful signal is sent by pid, and
                // only while the child is provably un-reaped: this teardown is the
                // sole reaper — the arm that called us won its `select!`, so
                // `backend_wait` never ran — and EVERY caller retired the gate before
                // reaching us (`drive_to_exit_inner` up front for the deadline,
                // inactivity and cancel arms; `wait_exit`'s cancel arm likewise), so
                // no detached watchdog can still be racing the reap below with a raw
                // pid kill. That ordering is load-bearing here, because the trailing
                // `gate.retire()` in this branch runs only AFTER the reap has already
                // freed the pid — so it is `debug_assert`ed below rather than left to
                // this comment. `Child::id()` is additionally `None` once the child
                // has been reaped, so the pid-scoped signal below degrades to a no-op
                // rather than a stray signal even if it were reached late.
                None => {
                    // The invariant the paragraph above rests on, made enforceable
                    // instead of merely documented: every caller must have retired
                    // the gate BEFORE reaching this branch, because the trailing
                    // `gate.retire()` here runs only after the reap has already freed
                    // the pid. A future third caller that forgets trips this in debug
                    // and under `cargo test` rather than shipping a silent SIGKILL on
                    // a recycled pid (the K-044 / T-093 class). Debug-only on purpose:
                    // this is an internal call-ordering contract, not user input, and
                    // a hard `assert!` would abort a *teardown* in release — turning a
                    // caller's ordering slip into a child left un-reaped, which is the
                    // worse failure. Every build that could introduce such a caller
                    // (`cargo test`, CI, the debug profile) carries the check.
                    debug_assert!(
                        gate.is_retired(),
                        "graceful_teardown's shared-group branch requires the caller \
                         to have retired the PidGate first — it retires only after \
                         its own reap has freed the pid, so an un-retired gate would \
                         leave a detached watchdog free to raw-kill a recycled pid"
                    );
                    #[cfg(unix)]
                    {
                        stream::signal_direct_child(real.child_mut().id(), signal);
                        // Wait up to `grace` for the child to exit on the signal; a
                        // child that catches it and stays up rides out the grace.
                        // Only a *clean* reap skips escalation — on a grace elapse
                        // (or a rare wait error) escalate through the owned `Child`,
                        // whose `start_kill` is a harmless no-op if it turns out the
                        // child was already reaped.
                        let reaped_cleanly =
                            confirm_reap(grace, real.child_mut().wait()).await.is_ok();
                        if !reaped_cleanly {
                            let child_error =
                                child_start_kill("real", || real.child_mut().start_kill()).err();
                            let reap = confirm_reap(PUMP_TEARDOWN, real.child_mut().wait()).await;
                            if let Err(source) = reap {
                                return Err(TeardownFailure {
                                    cause,
                                    operation: if child_error.is_some() {
                                        "direct child hard-kill escalation"
                                    } else {
                                        "child terminal reap"
                                    },
                                    source: child_error.unwrap_or(source),
                                });
                            }
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        // Windows has no graceful tier: hard-kill immediately
                        // through the owned Child and reap.
                        let _ = signal;
                        let child_error =
                            child_start_kill("real", || real.child_mut().start_kill()).err();
                        let reap = confirm_reap(PUMP_TEARDOWN, real.child_mut().wait()).await;
                        if let Err(source) = reap {
                            return Err(TeardownFailure {
                                cause,
                                operation: if child_error.is_some() {
                                    "direct child hard kill"
                                } else {
                                    "child terminal reap"
                                },
                                source: child_error.unwrap_or(source),
                            });
                        }
                    }
                    // Reaped (pid freed); retire so any lingering external watchdog
                    // stands down. `drive_to_exit_inner` already retired before
                    // calling us; this keeps the post-reap invariant explicit.
                    gate.retire();
                }
            },
            // The PTY child follows the same graceful tiers as `Real`: an own-group
            // handle drives the whole-tree signal→grace→kill through the group; a
            // shared-group handle reaches only its direct child (a real signal on
            // Unix, a hard kill on Windows, which has no signal tier).
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => match pty.own_group.clone() {
                Some(group) => {
                    let teardown = async move { group_graceful_kill(&group, grace, signal).await };
                    let reap = async {
                        let r = confirm_reap(
                            grace.saturating_add(PUMP_TEARDOWN),
                            pty.child_mut().wait(),
                        )
                        .await;
                        gate.retire();
                        r
                    };
                    let (teardown, reap) = tokio::join!(teardown, reap);
                    if let Err(source) = teardown {
                        return Err(TeardownFailure {
                            cause,
                            operation: "process-group graceful escalation",
                            source,
                        });
                    }
                    if let Err(source) = reap {
                        return Err(TeardownFailure {
                            cause,
                            operation: "PTY child terminal reap",
                            source,
                        });
                    }
                }
                None => {
                    // Same caller contract as the `Real` shared-group branch above,
                    // for the same reason (the retire below trails the reap).
                    debug_assert!(
                        gate.is_retired(),
                        "graceful_teardown's shared-group PTY branch requires the \
                         caller to have retired the PidGate first"
                    );
                    #[cfg(unix)]
                    {
                        stream::signal_direct_child(pty.child_mut().id(), signal);
                        let reaped_cleanly =
                            confirm_reap(grace, pty.child_mut().wait()).await.is_ok();
                        if !reaped_cleanly {
                            let child_error =
                                child_start_kill("pty", || pty.child_mut().start_kill()).err();
                            let reap = confirm_reap(PUMP_TEARDOWN, pty.child_mut().wait()).await;
                            if let Err(source) = reap {
                                return Err(TeardownFailure {
                                    cause,
                                    operation: if child_error.is_some() {
                                        "PTY child hard-kill escalation"
                                    } else {
                                        "PTY child terminal reap"
                                    },
                                    source: child_error.unwrap_or(source),
                                });
                            }
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = signal;
                        let child_error =
                            child_start_kill("pty", || pty.child_mut().start_kill()).err();
                        let reap = confirm_reap(PUMP_TEARDOWN, pty.child_mut().wait()).await;
                        if let Err(source) = reap {
                            return Err(TeardownFailure {
                                cause,
                                operation: if child_error.is_some() {
                                    "PTY child hard kill"
                                } else {
                                    "PTY child terminal reap"
                                },
                                source: child_error.unwrap_or(source),
                            });
                        }
                    }
                    gate.retire();
                }
            },
            Backend::Scripted(s) => s.kill(),
        }
        Ok(())
    }

    /// Whether the child has already exited, polled without blocking — the
    /// discard-the-outcome form of [`exit_outcome_now`](Self::exit_outcome_now),
    /// which is the single implementation of this probe.
    fn has_exited_now(&mut self) -> bool {
        self.exit_outcome_now().is_some()
    }

    /// The child's terminal [`Outcome`] once it has exited, polled **without
    /// blocking**: `Some` after the reap, `None` while the child still runs.
    ///
    /// This is the readiness probe `poll_until` uses (via
    /// [`has_exited_now`](Self::has_exited_now)) widened to also report *how* the
    /// child ended, so a caller that observes the exit passively — the pipeline's
    /// last-stage teardown watcher (`src/pipeline.rs`) — can classify the outcome
    /// without taking the consuming [`finish`](Self::finish) away from the handle's
    /// owner. Every observation-time side effect is exactly the one probe's, and
    /// none of them consume the handle: a following `finish`/`wait` still reports
    /// the same outcome off tokio's cached exit status.
    pub(crate) fn exit_outcome_now(&mut self) -> Option<Outcome> {
        let gate = self.pid_gate.clone();
        let mut observed: Option<Outcome> = None;
        // Reap-and-retire in one critical section: the non-blocking `try_wait`
        // that reaps (and frees) the pid runs under the gate lock and retires it
        // in the same step, so a watchdog's gated raw kill can never observe the
        // pid live after this reap freed it. Being synchronous, this fully closes
        // the window the async `backend_wait` backstop can only bound.
        let exited = gate.reap_under_lock(|| match &mut self.backend {
            Backend::Real(real) => match real.child_mut().try_wait() {
                Ok(Some(status)) => {
                    observed = Some(outcome_of_exit_status(&status));
                    true
                }
                Ok(None) | Err(_) => false,
            },
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => match pty.child_mut().try_wait() {
                Ok(Some(status)) => {
                    observed = Some(outcome_of_pty_exit_status(&status));
                    true
                }
                Ok(None) | Err(_) => false,
            },
            Backend::Scripted(s) => {
                observed = s.outcome_now();
                observed.is_some()
            }
        });
        if exited {
            // Claim the arbiter: a deadline watchdog racing on another thread could
            // win `PENDING -> TIMED_OUT` before `abort_watchdogs` stops it,
            // misclassifying a clean exit. Claiming `EXITED` closes that window.
            let _ = deadline::claim_exited(&self.timeout_state);
            self.abort_watchdogs();
            // Snapshot the cancel disposition at the moment this probe observes
            // the reap, first-observation-wins. This is *observation-time*
            // semantics, matching the bulk paths' biased `select!`
            // (`drive_to_exit_inner`/`wait_exit`): a token already cancelled when
            // the exit is observed resolves to `Cancelled` there, so latching
            // `is_cancelled()` here keeps the probe consistent with a no-probe
            // wait at the same timeline and honours the contract documented on
            // `Command::cancel_on` — a mid-run cancel during a probe surfaces as
            // that probe's `NotReady`, and the consuming finisher afterwards
            // still reports `Cancelled`. Freezing the disposition on this first
            // observation stops a *later* cancel (one that fires after the probe
            // already saw a natural exit) from flipping it, without dropping an
            // *earlier* cancel that was already active — and already killed the
            // tree via the cancel watchdog — by the time the probe noticed.
            if self.cancel_at_exit.is_none() {
                self.cancel_at_exit = Some(self.is_cancelled());
            }
            // Same override, in the same order relative to the claim above, as the
            // reap choke point `on_reaped` applies: a deadline that won the arbitration
            // makes this a `TimedOut` run even though the child's own status says it
            // exited cleanly within the grace. Reading it here is what lets a passive
            // observer classify a run exactly as the consuming finisher will.
            observed = observed.map(|outcome| self.classify_watchdog_timeout(outcome));
        }
        debug_assert_eq!(
            exited,
            observed.is_some(),
            "the reap probe and the observed outcome must agree"
        );
        observed
    }

    /// Whether the first exit observation latched an active cancellation source.
    /// The pipeline's passive last-stage watcher calls this only after
    /// [`exit_outcome_now`](Self::exit_outcome_now) returned `Some`, so it can fire
    /// chain teardown with the same disposition the consuming finisher will report.
    pub(crate) fn cancelled_at_exit(&self) -> bool {
        self.cancel_at_exit == Some(true)
    }

    /// Send a kill to the process without waiting for it to exit. The owning
    /// group still governs the rest of the tree.
    ///
    /// The [`Outcome`] afterwards is platform-dependent: `Signalled` on Unix,
    /// `Exited` with a platform code on Windows. A scripted handle reports
    /// `Signalled(None)`.
    ///
    /// **Idempotent:** killing an already-reaped child is a successful no-op.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`] if the OS rejects the kill for a reason other than the
    /// child having already been reaped (which is treated as a no-op success).
    pub fn start_kill(&mut self) -> Result<()> {
        match &mut self.backend {
            Backend::Real(real) => match real.child_mut().start_kill() {
                Ok(()) => {}
                // tokio/std currently return `Ok` for a reaped child; treat
                // `InvalidInput` as the same no-op in case that ever changes.
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {}
                Err(e) => return Err(Error::io(e)),
            },
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => match pty.child_mut().start_kill() {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {}
                Err(e) => return Err(Error::io(e)),
            },
            Backend::Scripted(s) => s.kill(),
        }
        Ok(())
    }
}

impl Drop for RunningProcess {
    fn drop(&mut self) {
        // Abort the *abortable* teardown watchdogs first. The deadline/cancel tasks
        // raw-kill by pid, so they must be stopped before we may free the pid below.
        // (The shared-group graceful kill-and-reap is a separate DETACHED task, not
        // one of these — closing the window it leaves is what the child hand-off
        // below is for.)
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.inactivity_task.take() {
            task.abort();
        }
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
        let graceful_cancel_fired = self.cancel_grace.is_some() && self.is_cancelled();
        // A surviving grandchild holding the pipe could keep a pump alive
        // indefinitely on a shared-group handle without this abort.
        if let Some(task) = self.stdout_pump.take() {
            task.abort();
        }
        if let Some(task) = self.stderr_pump.take() {
            task.abort();
        }
        #[cfg(test)]
        if let Some(task) = self.test_stdin_task.take() {
            task.abort();
        }
        match &mut self.backend {
            Backend::Real(real) => {
                if let Some(task) = real.stdin_task.take() {
                    task.abort();
                }
                // Window: a *shared-group* streamed run whose graceful-timeout
                // deadline (or, with `cancel_grace`, whose cancel token) fired leaves
                // a DETACHED pid-only kill-and-reap
                // (`stream::spawn_graceful_kill_and_reap`) running past this handle.
                // If we let the owned child drop here, tokio's orphan reaper would
                // reap it — freeing (and letting the OS recycle) the pid — WITHOUT
                // retiring the gate, so that detached task could then probe or
                // SIGKILL a stranger. Instead hand the child to a detached reaper
                // that reaps it *under the gate* (retiring atomically), so the pid is
                // freed only as the gate retires and the detached grace task stands
                // down before it can touch a recycled pid. The grace task still
                // delivers its escalation SIGKILL by pid while the pid is provably
                // un-reaped (this reaper owns the sole `Child`), so a survivor that
                // rode out the grace is not stranded.
                //
                // Scoped to the exact preconditions of that detached task — a shared
                // group (`own_group` is `None`) plus EITHER a deadline with a
                // `timeout_grace` window OR a cancel token that has ALREADY FIRED
                // with a `cancel_grace` window (the two arming sites: the streaming
                // deadline/inactivity watchdogs and the cancel watchdog). An
                // own-group handle tears its whole tree down on drop and arms no
                // detached pid-killer; a shared handle with neither graceful window
                // never spawns one either, so neither needs this hand-off — they
                // fall through to the `else` and retire the gate synchronously
                // instead (see below).
                //
                // The two halves are deliberately asymmetric:
                //
                //   * the deadline half is purely *static* — `timeout` /
                //     `inactivity_timeout` / `timeout_grace` are read from `self`,
                //     so there is no race with the watchdog that arms the detached
                //     task (a "deadline fired" flag would be set by that very
                //     watchdog, concurrently with this read);
                //   * the cancel half also reads the token's `is_cancelled()`, which
                //     is dynamic but **monotone** (`false` → `true`, never back), so
                //     it is race-free here all the same. `true` means a cancel
                //     watchdog may already have armed the detached killer, so hand
                //     the child off. `false` is read *before* the `else`'s
                //     synchronous `retire()`, which linearizes every watchdog that
                //     fires afterwards behind it: such a watchdog either stands down
                //     at its own `is_retired()` check or spawns a grace killer whose
                //     every raw op is suppressed under the now-retired gate — in
                //     neither case can it touch a recycled pid. Reading `true` may
                //     over-approximate (the watchdog can have been aborted at the top
                //     of this `drop()` before it ever ran), which is the safe
                //     direction: at worst a deterministic gated reap instead of the
                //     orphan reap, never a missing one.
                //
                // The cancel half must NOT use the deadline half's static form
                // ("a token is configured"). `cancel_grace` needs no deadline, so
                // that form is true for a handle whose token may never fire at all:
                // every dropped handle of the very shape the docs recommend (one
                // shared token for the whole app + `cancel_grace` on the bulk verbs)
                // would then park a detached reaper on `child.wait()` — and leave its
                // `PidGate` un-retired — for the child's entire, unbounded life,
                // without a single cancellation having happened. Gating on the fired
                // token keeps the hand-off where its reason to exist is: an
                // actually-armable detached killer.
                //
                // The hand-off ALSO needs two *dynamic* conditions, and the `else`
                // now covers every case where one of them fails (this is what closes
                // the T-093 no-runtime window): a live (un-retired) gate — a consuming
                // reap already retires it, leaving no detached killer to survive — and
                // a *current* tokio runtime to spawn the reaper on. `try_current()` is
                // checked BEFORE `real.child.take()` on purpose: when no runtime is
                // current the chain short-circuits WITHOUT taking the child, so the
                // child is still owned by `real` when the `else` retires the gate,
                // preserving the "retire before the pid is freed" ordering (the child's
                // pid is freed only as `real` drops at the end of this `drop()`, after
                // the retire — never before it). On the *deadline* half, when the
                // deadline had not actually fired, the handed-off reaper is merely a
                // harmless deterministic replacement for the orphan reap: that half is
                // over-approximate only for as long as the deadline itself, which is
                // configured and will fire. That bound is exactly what the cancel half
                // lacks — hence its fired-token gate above, without which "harmless"
                // would have meant "for the child's entire life".
                if real.own_group.is_none()
                    && (((self.timeout.is_some() || self.inactivity_timeout.is_some())
                        && self.timeout_grace.is_some())
                        || graceful_cancel_fired)
                    && !self.pid_gate.is_retired()
                    && let Ok(handle) = tokio::runtime::Handle::try_current()
                    && let Some(child) = real.child.take()
                {
                    let gate = self.pid_gate.clone();
                    handle.spawn(gated_reap_and_retire(gate, child));
                } else {
                    // Every OTHER Real drop reaches here: an own-group handle, a
                    // shared group without a graceful window, OR a shared-group+grace
                    // handle whose hand-off could not run (no current runtime, an
                    // already-retired gate, or an already-taken child). None of these
                    // leaves a detached grace kill-and-reap that a retire could strand:
                    //
                    //   * own-group / shared-without-grace never arm that detached task
                    //     at all — it needs `own_group.is_none()` plus one of the two
                    //     graceful shapes (`timeout`/`inactivity_timeout` +
                    //     `timeout_grace`, or an already-fired `cancel_token` +
                    //     `cancel_grace`), precisely the config we are NOT in on those
                    //     shapes. A `cancel_grace` handle whose token has NOT fired
                    //     lands here on purpose (see the monotone-read rationale
                    //     above): nothing detached exists yet, and this retire is what
                    //     stands down anything the token could still arm;
                    //   * a shared-group+grace handle dropped with NO runtime current
                    //     never armed it *from a live path here* either — the grace
                    //     kill-and-reap is spawned by the streaming deadline or cancel
                    //     watchdog, each of which itself needs a runtime, so with none
                    //     current the hand-off is simply unavailable and retiring is
                    //     the only way to close the window (a deadline/cancel watchdog
                    //     mid-poll on another worker/runtime could otherwise outlive an
                    //     un-retired gate onto a recycled pid — the T-093 gap this
                    //     branch closes);
                    //   * an already-retired gate means a consuming reap already ran.
                    //
                    // `PidGate::retire` is idempotent, so retiring an already-retired
                    // gate is a safe no-op (the same idempotence `abort_watchdogs`
                    // relies on). The child, when not handed off, is freed only as the
                    // owned `Child` drops at the end of this `drop()` — by tokio's
                    // orphan reaper for a shared-group handle (the caller-owned group
                    // still tears the child's tree down on ITS own drop, per
                    // shared-group semantics) or as the owned group tears the whole
                    // tree down (an own-group handle).
                    //
                    // Retiring the gate NOW closes the window the non-synchronous
                    // `abort()`s at the top of `drop()` leave open: a deadline/cancel
                    // watchdog still mid-poll on another worker thread can reach its
                    // gated raw `force_kill`/`kill_via_weak` (both routed through
                    // `PidGate::with_live_pid`) after the structural drop above frees
                    // — and lets the OS recycle — the pid. Retiring synchronously and
                    // before `drop()` returns (so before that structural free)
                    // linearizes any such raw kill to either land entirely before the
                    // retire (the child is still un-reaped — a legitimate kill) or be
                    // skipped once retired (a safe no-op), never a
                    // SIGKILL/TerminateProcess on a recycled pid. This is the same
                    // "retire before the pid is freed" discipline `kill_tree` and
                    // `teardown_on_timeout` already follow; `abort()`, which only
                    // schedules cancellation, is never relied on alone. The teardown
                    // scope is untouched: an own-group tree still dies with its group
                    // and a shared-group child is still left to the caller's group —
                    // the gate governs only the raw pid-kill, never the group kill.
                    self.pid_gate.retire();
                }
            }
            // The PTY child takes the conservative structural-drop path (no
            // detached grace hand-off): abort the stdin writer, then retire the
            // gate synchronously — before the `PtyProc` (and its `PtyChild`) drops
            // at the end of this `drop()` and frees the pid — so a deadline/cancel
            // watchdog still mid-poll on another thread can never raw-kill a
            // recycled pid (the same "retire before the pid is freed" discipline
            // the `Real` else-branch uses). An own-group PTY tree still dies with
            // its group; a shared-group PTY child is left to the caller's group.
            #[cfg(feature = "pty")]
            Backend::Pty(pty) => {
                if let Some(task) = pty.stdin_task.take() {
                    task.abort();
                }
                self.pid_gate.retire();
            }
            Backend::Scripted(s) => s.kill(),
        }
    }
}

/// Whether `e` is the routine pipe-closed error — `BrokenPipe`, plus the raw
/// Windows encodings (`ERROR_BROKEN_PIPE` = 109, `ERROR_NO_DATA` = 232) that
/// don't always map to the kind. Used on the stdin *write* side (a child that
/// closed stdin early is not a failure) and on the stdout/stderr *read* side (a
/// writer-closed read is the normal end of a stream, not an incomplete capture),
/// so it is shared with [`crate::pump`].
pub(crate) fn is_broken_pipe(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::BrokenPipe || matches!(e.raw_os_error(), Some(109 | 232))
}

/// The retain-nothing sink policy shared by the discard paths (`wait`/`profile`
/// and a bare `finish`): keep no lines, but bound the pump's in-flight line
/// assembly with [`DISCARD_INFLIGHT_CAP`] so a newline-free flood can't grow it
/// unboundedly (`bounded(0)` alone retains no lines but would still let the
/// in-flight buffer grow without limit).
fn discard_sink_policy() -> OutputBufferPolicy {
    OutputBufferPolicy::bounded(0).with_max_bytes(DISCARD_INFLIGHT_CAP)
}

/// The retain-nothing sink policy for [`RunningProcess::drain`]: like
/// [`discard_sink_policy`] it keeps no lines, but it bounds the pump's in-flight
/// assembly by the caller's *configured*
/// [`OutputBufferPolicy::max_bytes`](crate::OutputBufferPolicy::max_bytes)
/// instead of the fixed [`DISCARD_INFLIGHT_CAP`]. A configured byte cap is
/// honored verbatim (so held memory tracks the configured limit, not the child's
/// output size); an *unbounded* policy falls back to [`DISCARD_INFLIGHT_CAP`] so
/// a newline-free flood still can't grow the in-flight buffer unboundedly — the
/// same anti-OOM floor `wait` always applies. Only the **byte** ceiling is read:
/// `max_lines` governs retention, and `drain` retains nothing.
fn drain_sink_policy(buffer: &OutputBufferPolicy) -> OutputBufferPolicy {
    OutputBufferPolicy::bounded(0).with_max_bytes(buffer.max_bytes.unwrap_or(DISCARD_INFLIGHT_CAP))
}

/// The running accumulator behind [`RunningProcess::profile`]: the latest CPU
/// reading, the peak memory across samples, and how many ticks ran.
#[cfg(feature = "stats")]
#[derive(Default)]
struct ProfileAcc {
    cpu_time: Option<Duration>,
    peak_memory_bytes: Option<u64>,
    samples: usize,
}

#[cfg(feature = "stats")]
impl ProfileAcc {
    /// Fold one metrics reading into the accumulator. A reading whose fields are
    /// all `None` — the shape `process_metrics` returns for a pid whose identity no
    /// longer matches (recycled) or a gone process — still counts as a tick but
    /// contributes no CPU/memory, so a sample taken against a stranger can never
    /// corrupt the numbers.
    fn fold(&mut self, metrics: crate::sys::ProcMetrics) {
        self.samples += 1;
        if let Some(cpu) = metrics.cpu_time {
            self.cpu_time = Some(cpu);
        }
        if let Some(peak) = metrics.peak_memory_bytes {
            self.peak_memory_bytes =
                Some(self.peak_memory_bytes.map_or(peak, |prev| prev.max(peak)));
        }
    }
}

/// The [`RunningProcess::profile`] sampler loop, factored over its `source` of
/// metrics so a test can drive the PID-reuse window with a substitutable source
/// (no real OS process). Ticks every `every`, folding each reading into `acc`, and
/// stops as soon as `reaped` is set — checked BOTH before the read and before the
/// fold, so a reap landing mid-read short-circuits before a sample the recycled pid
/// could have produced is folded. The `source` is expected to be identity-gated
/// (production passes `move || process_metrics(pid, identity)`), so even a reading
/// that slips past the flag folds a stranger's data as the all-`None` default.
#[cfg(feature = "stats")]
async fn run_profile_sampler(
    every: Duration,
    reaped: Arc<AtomicBool>,
    acc: Arc<std::sync::Mutex<ProfileAcc>>,
    mut source: impl FnMut() -> crate::sys::ProcMetrics,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if reaped.load(Ordering::Acquire) {
            break;
        }
        let metrics = source();
        if reaped.load(Ordering::Acquire) {
            break;
        }
        if let Ok(mut acc) = acc.lock() {
            acc.fold(metrics);
        }
    }
}

/// Map a reaped OS child's [`ExitStatus`](std::process::ExitStatus) to this
/// crate's [`Outcome`] — the code when there is one, else the Unix signal number
/// (never available off Unix). Shared by the async reap (`backend_wait`) and the
/// synchronous exit probe (`exit_outcome_now`) so the two can never classify the
/// same status differently.
fn outcome_of_exit_status(status: &std::process::ExitStatus) -> Outcome {
    match status.code() {
        Some(code) => Outcome::Exited(code),
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                Outcome::Signalled(status.signal())
            }
            #[cfg(not(unix))]
            Outcome::Signalled(None)
        }
    }
}

/// The PTY analogue of [`outcome_of_exit_status`]: the platform PTY child reports
/// its status through its own type, carrying the same code and (Unix) signal.
#[cfg(feature = "pty")]
fn outcome_of_pty_exit_status(status: &crate::sys::pty::PtyExitStatus) -> Outcome {
    match status.code() {
        Some(code) => Outcome::Exited(code),
        #[cfg(unix)]
        None => Outcome::Signalled(status.signal()),
        #[cfg(not(unix))]
        None => Outcome::Signalled(None),
    }
}

/// Reap `child` to exit **inside the gate lock**: poll `Child::wait()` within
/// [`PidGate::reap_under_lock`](crate::sys::pid_gate::PidGate) so tokio's
/// pid-freeing `try_wait` — which runs *inside* that poll (see tokio's
/// `Reaper::poll`) — and the gate retire land in one indivisible critical
/// section. A detached watchdog's gated kill takes the same lock, so it either
/// completes entirely before this reap frees the pid or is skipped once retired —
/// never on the freed (possibly OS-recycled) pid, closing the reap→retire window
/// a plain `child.wait().await` then `retire()` leaves open. The `Child::wait()`
/// poll is a synchronous, non-blocking readiness poll plus a `waitpid(WNOHANG)`,
/// so holding the gate across it stays within the gate's bounded-work contract.
/// Dropping this future (a cancelled `wait_any`/`wait_all` contender) just stops
/// polling — no reap, gate untouched — so race losers stay usable.
async fn gated_reap(
    gate: &PidGate,
    child: &mut Child,
) -> std::io::Result<std::process::ExitStatus> {
    use std::future::Future;
    let mut wait = std::pin::pin!(child.wait());
    std::future::poll_fn(|cx| {
        let mut out = std::task::Poll::Pending;
        gate.reap_under_lock(|| match wait.as_mut().poll(cx) {
            std::task::Poll::Ready(res) => {
                out = std::task::Poll::Ready(res);
                true
            }
            std::task::Poll::Pending => false,
        });
        out
    })
    .await
}

/// Own `child` and [`gated_reap`] it, then retire the gate — the detached reaper
/// [`RunningProcess::drop`] hands a shared-group child to. Owning the child makes
/// this the sole reaper, so tokio's orphan reaper never frees the pid behind the
/// detached graceful kill-and-reap's back; the pid is freed exactly as the gated
/// reap retires. The trailing `retire` is an idempotent backstop for the rare
/// wait error where the reap did not land.
async fn gated_reap_and_retire(gate: Arc<PidGate>, mut child: Child) {
    let _ = gated_reap(&gate, &mut child).await;
    gate.retire();
}

/// The shared signals the raw stdout byte drain ([`pump_raw_bytes`]) writes and
/// [`RunningProcess::output_bytes`] reads after teardown — bytes seen, the two
/// byte-cap overflow flags, and the first OS read error. Bundled (all `Arc`) so
/// the detached drain task and the finisher share one set (and so the seam stays
/// within a sane argument count).
#[derive(Clone)]
struct RawStdoutSignals {
    /// Cumulative bytes read, including any dropped past a byte cap.
    seen: Arc<AtomicUsize>,
    /// Set when an [`OverflowMode::Error`] byte ceiling is breached.
    overflowed: Arc<AtomicBool>,
    /// Set when a drop-mode byte cap discarded bytes (the truncation signal).
    truncated: Arc<AtomicBool>,
    /// The first non-broken-pipe OS read error, surfaced as [`ErrorReason::Io`].
    read_error: Arc<std::sync::Mutex<Option<std::io::Error>>>,
}

/// Shared state for a raw stdout capture and line-oriented stderr capture.
/// The pipeline keeps a clone so a timeout can salvage the bytes that the raw
/// pump had appended before the consuming task was dropped.
#[derive(Clone)]
pub(crate) struct RawCapture {
    out_buf: Arc<std::sync::Mutex<Vec<u8>>>,
    stderr_sink: Arc<SharedLines>,
    stderr_config: StreamConfig,
    signals: RawStdoutSignals,
    stdout_cap: Option<usize>,
    stdout_mode: OverflowMode,
}

impl RawCapture {
    /// Non-destructive counterpart of [`Self::snapshot`] for a pipeline's
    /// pre-kill diagnostic checkpoint. Raw stdout is already mutex-framed; stderr
    /// intentionally includes complete retained lines only, matching
    /// [`LineCapture::retained_snapshot`].
    pub(crate) fn retained_snapshot(&self) -> (Vec<u8>, String, bool, usize, usize) {
        let mut stdout = self.out_buf.lock().expect("stdout buffer poisoned").clone();
        clamp_dropoldest_tail(&mut stdout, self.stdout_cap, self.stdout_mode);
        let stderr = self
            .stderr_sink
            .retained_snapshot(|tail| self.stderr_config.shape_capture_line(tail))
            .join("\n");
        (
            stdout,
            stderr,
            self.signals.truncated.load(Ordering::Relaxed) || self.stderr_sink.dropped() > 0,
            self.stderr_sink.count(),
            self.signals
                .seen
                .load(Ordering::Relaxed)
                .saturating_add(self.stderr_sink.seen_bytes()),
        )
    }

    /// The raw analogue of [`LineCapture::snapshot`]: the stdout bytes the raw
    /// pump had appended, plus the line-oriented stderr salvage. Stdout needs no
    /// tail handling (raw bytes have no line framing, and the mutex orders every
    /// whole chunk write); stderr folds its still-pending tail in and drains in
    /// the same **single** critical section, so a still-live stderr pump cannot
    /// slip the tail's own completed line in between and duplicate the prefix.
    pub(crate) fn snapshot(&self) -> (Vec<u8>, String, bool, usize, usize) {
        let mut stdout = self.out_buf.lock().expect("stdout buffer poisoned").clone();
        clamp_dropoldest_tail(&mut stdout, self.stdout_cap, self.stdout_mode);
        let stderr_lines = self
            .stderr_sink
            .drain_with_partial_tail(|tail| self.stderr_config.shape_capture_line(tail));
        // Read the totals after the fold, like `LineCapture::snapshot`.
        let truncated =
            self.signals.truncated.load(Ordering::Relaxed) || self.stderr_sink.dropped() > 0;
        let total_lines = self.stderr_sink.count();
        let total_bytes = self
            .signals
            .seen
            .load(Ordering::Relaxed)
            .saturating_add(self.stderr_sink.seen_bytes());
        (
            stdout,
            stderr_lines.join("\n"),
            truncated,
            total_lines,
            total_bytes,
        )
    }
}

/// Drain a child's **raw** stdout bytes into `out_buf`, honoring the byte
/// ceiling (`cap`/`mode`) and updating the shared `signals` (bytes seen, the two
/// overflow flags, and the first non-broken-pipe OS read error) so
/// [`RunningProcess::output_bytes`] can surface an incomplete capture as
/// [`ErrorReason::Io`] instead of a silently-short prefix. The raw (non-line) analogue
/// of [`pump_lines_core`](crate::pump)'s read loop, extracted as a seam so the
/// read-error / broken-pipe / clean-EOF classification is unit-testable without a
/// live child. A broken-pipe read (the writer closing) is the normal end of a
/// stream and ends the drain cleanly, recording no error.
async fn pump_raw_bytes<R>(
    mut reader: R,
    out_buf: Arc<std::sync::Mutex<Vec<u8>>>,
    cap: Option<usize>,
    mode: OverflowMode,
    signals: RawStdoutSignals,
    output_activity: Arc<OutputActivity>,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0u8; 8 * 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                output_activity.record();
                signals.seen.fetch_add(n, Ordering::Relaxed);
                let mut guard = out_buf.lock().expect("stdout buffer poisoned");
                push_capped_bytes(
                    &mut guard,
                    &chunk[..n],
                    cap,
                    mode,
                    &signals.overflowed,
                    &signals.truncated,
                );
                drop(guard);
                #[cfg(all(test, feature = "process-control"))]
                publish_raw_stdout_for_test(&chunk[..n]);
            }
            // Broken pipe = the writer end closed = the normal end of a child
            // stream (std already maps it to `Ok(0)`; this is a defensive net):
            // end cleanly, recording no error.
            Err(e) if is_broken_pipe(&e) => break,
            Err(e) => {
                // Keep the partial prefix already captured, but record the error so
                // the consuming finisher reports the incomplete capture.
                #[cfg(feature = "tracing")]
                tracing::warn!(target: "processkit", error = %e, "stdout read error; ending byte capture early");
                *signals
                    .read_error
                    .lock()
                    .expect("stdout read-error slot poisoned") = Some(e);
                break;
            }
        }
    }
}

/// Await the output pumps, bounded by [`PUMP_TEARDOWN`]; abort stragglers.
async fn join_pumps(tasks: Vec<JoinHandle<()>>) {
    if tasks.is_empty() {
        return;
    }
    let aborts: Vec<_> = tasks.iter().map(|t| t.abort_handle()).collect();
    let join = async {
        for task in tasks {
            // A panicking pump closes its sink via close-on-drop: partial output
            // is intact. Surface it for diagnostics, never as a run error.
            #[cfg(feature = "tracing")]
            if let Err(e) = task.await {
                tracing::warn!(target: "processkit", error = %e, "output pump task ended abnormally");
            }
            #[cfg(not(feature = "tracing"))]
            let _ = task.await;
        }
    };
    if tokio::time::timeout(PUMP_TEARDOWN, join).await.is_err() {
        // A pipe is still held open past the child's death (the surviving-
        // grandchild case PUMP_TEARDOWN exists for) — abort and keep what
        // arrived.
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: "processkit",
            timeout_ms = PUMP_TEARDOWN.as_millis() as u64,
            aborted = aborts.len(),
            "output pumps overran teardown grace; aborting stragglers"
        );
        for abort in aborts {
            abort.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use crate::doubles::{Reply, ScriptedRunner};
    use crate::runner::ProcessRunner;

    fn marker_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "processkit_teardown_{label}_{}_{nonce}.ready",
            std::process::id()
        ))
    }

    fn live_command(marker: &std::path::Path, use_pty: bool) -> Command {
        #[cfg(unix)]
        let command = Command::new("sh").args([
            "-c",
            &format!(
                "printf 'teardown-prefix\\n'; : > '{}'; sleep 60",
                marker.display()
            ),
        ]);
        #[cfg(windows)]
        let command = {
            let marker = marker.display().to_string().replace('\'', "''");
            Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[Console]::Out.WriteLine('teardown-prefix'); \
                     [IO.File]::WriteAllText('{marker}', 'ready'); Start-Sleep -Seconds 60"
                ),
            ])
        };
        #[cfg(feature = "pty")]
        if use_pty {
            return command.use_pty();
        }
        #[cfg(not(feature = "pty"))]
        let _ = use_pty;
        command
    }

    fn completed_command(marker: &std::path::Path) -> Command {
        #[cfg(unix)]
        return Command::new("sh").args([
            "-c",
            &format!("printf 'teardown-prefix\\n'; : > '{}'", marker.display()),
        ]);
        #[cfg(windows)]
        {
            let marker = marker.display().to_string().replace('\'', "''");
            Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "[Console]::Out.WriteLine('teardown-prefix'); \
                     [IO.File]::WriteAllText('{marker}', 'ready')"
                ),
            ])
        }
    }

    #[cfg(all(feature = "process-control", windows))]
    fn survivor_held_output_command(marker: &std::path::Path, use_pty: bool) -> Command {
        #[cfg(unix)]
        let command = Command::new("sh").args([
            "-c",
            &format!(
                "(trap '' HUP TERM; sleep 60) & survivor=$!; \
                 printf '%s' \"$survivor\" > '{}'; \
                 printf 'teardown-prefix\\n'; printf 'teardown-stderr-prefix\\n' >&2; \
                 sleep 0.25",
                marker.display()
            ),
        ]);
        #[cfg(windows)]
        let command = {
            let marker = marker.display().to_string().replace('\'', "''");
            Command::new("powershell").args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!(
                    "$child = Start-Process -FilePath 'powershell' \
                       -ArgumentList @('-NoLogo','-NoProfile','-NonInteractive','-Command', \
                       'Start-Sleep -Seconds 60') -NoNewWindow -PassThru; \
                     [IO.File]::WriteAllText('{marker}', [string]$child.Id); \
                     [Console]::Out.WriteLine('teardown-prefix'); \
                     [Console]::Error.WriteLine('teardown-stderr-prefix'); \
                     Start-Sleep -Milliseconds 250"
                ),
            ])
        };
        #[cfg(feature = "pty")]
        if use_pty {
            return command.use_pty();
        }
        #[cfg(not(feature = "pty"))]
        let _ = use_pty;
        command
    }

    async fn wait_for_marker(marker: &std::path::Path) {
        for _ in 0..500 {
            if marker.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "child did not publish readiness marker: {}",
            marker.display()
        );
    }

    #[cfg(all(feature = "process-control", windows))]
    async fn read_survivor_pid(marker: &std::path::Path) -> u32 {
        for _ in 0..500 {
            if let Ok(text) = std::fs::read_to_string(marker)
                && let Ok(pid) = text.trim().parse()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("survivor pid was not published: {}", marker.display());
    }

    #[cfg(all(feature = "process-control", windows))]
    async fn cleanup_group_members(group: &ProcessGroup) {
        group.kill_all().expect("cleanup survivor group");
        for _ in 0..500 {
            if group.members().is_ok_and(|members| members.is_empty()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("process-group members remained after explicit cleanup");
    }

    #[cfg(all(feature = "process-control", windows))]
    async fn assert_survivor_and_cleanup(group: &ProcessGroup, survivor: u32) {
        assert!(
            group.members().expect("members").contains(&survivor),
            "the descendant must outlive its direct parent while holding output"
        );
        assert!(
            crate::process_is_alive(survivor, None).expect("survivor liveness"),
            "the survivor must still be alive before explicit cleanup"
        );
        cleanup_group_members(group).await;
        for _ in 0..500 {
            if !crate::process_is_alive(survivor, None).expect("survivor cleanup liveness probe") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("survivor {survivor} remained after explicit group cleanup");
    }

    fn assert_teardown_error(error: &Error, expected: TeardownCause, expected_operation: &str) {
        match error.reason() {
            ErrorReason::Teardown {
                cause,
                operation,
                source,
                stdout,
                stderr,
                ..
            } => {
                assert_eq!(*cause, expected);
                assert_eq!(*operation, expected_operation);
                assert_eq!(source.raw_os_error(), Some(5));
                assert!(
                    stdout.contains("teardown-prefix"),
                    "the bounded drain must retain the prefix: {stdout:?}"
                );
                assert!(
                    stderr.is_empty() || stderr.contains("teardown-stderr-prefix"),
                    "the bounded drain must retain the stderr prefix when emitted: {stderr:?}"
                );
            }
            other => panic!("expected Teardown, got {other:?}"),
        }
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real owned-group child for graceful-shutdown fault injection"]
    async fn shutdown_returns_graceful_group_failure_without_waiting_for_live_child() {
        let marker = marker_path("shutdown_graceful_failure");
        let run = live_command(&marker, false)
            .start()
            .await
            .expect("start owned-group child");
        let pid = run.pid().expect("live child pid");
        wait_for_marker(&marker).await;
        assert!(
            crate::process_is_alive(pid, None).expect("child liveness before shutdown"),
            "the injected graceful failure must begin with a live child"
        );
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("graceful"),
                5,
            )
            .arm();

        let started = Instant::now();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            run.shutdown(Duration::from_secs(30)),
        )
        .await
        .expect("shutdown must return the graceful failure without awaiting the live child")
        .expect_err("the injected graceful group teardown must fail");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shutdown waited for the live child after teardown failed"
        );
        match error.reason() {
            ErrorReason::Io(source) => assert_eq!(source.raw_os_error(), Some(5)),
            other => panic!("the original graceful teardown error must win, got {other:?}"),
        }
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        drop(faults);

        for _ in 0..500 {
            if !crate::process_is_alive(pid, None).expect("child liveness after shutdown failure") {
                let _ = std::fs::remove_file(marker);
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("kill-on-drop backstop left child {pid} alive after shutdown failure");
    }

    #[cfg(feature = "process-control")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real shared-group child for deterministic kill fault injection"]
    async fn shared_real_child_kill_failure_is_not_reported_as_cancelled() {
        let marker = marker_path("real_child_kill");
        let group = ProcessGroup::new().expect("group");
        let mut run = group
            .start(&live_command(&marker, false))
            .await
            .expect("start shared child");
        wait_for_marker(&marker).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::DirectChildKill,
                Some("real"),
                5,
            )
            .arm();

        tokio::time::pause();
        let failure = run
            .teardown_on_cancel()
            .await
            .expect_err("the injected direct-child kill fails");
        record_teardown_failure(&run.teardown_failure, failure);
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::DirectChildKill),
            1
        );
        assert!(
            !group.members().expect("members").is_empty(),
            "the injected child kill really left a live group member"
        );
        let error = run
            .take_teardown_error("teardown-prefix\n".into(), String::new(), None)
            .expect("an unconfirmed child kill must fail closed");
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "direct child hard kill",
        );
        drop(faults);
        run.start_kill().expect("direct-child cleanup kill");
        run.backend_wait().await.expect("cleanup reap");
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real short-lived child for the already-exited kill race"]
    async fn already_exited_child_keeps_the_routine_cancelled_classification() {
        let marker = marker_path("already_exited");
        let group = ProcessGroup::new().expect("group");
        let token = crate::CancellationToken::new();
        let run = group
            .start(&completed_command(&marker).cancel_on(token.clone()))
            .await
            .expect("start short-lived child");
        wait_for_marker(&marker).await;
        // The marker is the child's final operation. Give the OS process a small
        // wall-clock window to exit without observing/reaping it through the API.
        std::thread::sleep(Duration::from_millis(100));
        tokio::time::pause();
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::DirectChildKill,
                Some("real"),
                5,
            )
            .arm();

        token.cancel();
        let error = run
            .output_string()
            .await
            .expect_err("cancellation remains an error on the unobserved-exit race");
        assert!(
            matches!(error.reason(), ErrorReason::Cancelled { .. }),
            "a confirmed reap makes the failed kill a benign already-exited race: {error:?}"
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::DirectChildKill),
            1
        );
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(all(feature = "process-control", windows))]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real owned-group child for deterministic kill fault injection"]
    async fn whole_group_hard_kill_failure_outranks_cancellation() {
        let marker = marker_path("group_hard_kill");
        let token = crate::CancellationToken::new();
        let run = survivor_held_output_command(&marker, false)
            .cancel_on(token.clone())
            .start()
            .await
            .expect("start owned-group child");
        wait_for_marker(&marker).await;
        let survivor = read_survivor_pid(&marker).await;
        let group = run.own_group_handle().expect("owned process group");
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let started = tokio::time::Instant::now();
        token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(9), run.output_string())
            .await
            .expect("hard-failure output finalization must remain bounded")
            .expect_err("an unconfirmed group kill must fail closed");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(4),
            "the survivor must hold the pumps through their bounded grace: {elapsed:?}"
        );
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "process-group hard kill",
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        assert_survivor_and_cleanup(&group, survivor).await;
        drop(faults);
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real owned-group child for deterministic timeout fault injection"]
    async fn whole_group_hard_kill_failure_outranks_timeout() {
        let marker = marker_path("group_timeout");
        let mut run = live_command(&marker, false)
            .start()
            .await
            .expect("start deadline-bound child");
        wait_for_marker(&marker).await;
        tokio::time::pause();
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let failure = run
            .teardown_on_timeout(TeardownCause::Timeout)
            .await
            .expect_err("the injected group kill fails");
        record_teardown_failure(&run.teardown_failure, failure);
        let error = run
            .output_string()
            .await
            .expect_err("an unconfirmed deadline teardown must fail closed");
        assert_teardown_error(&error, TeardownCause::Timeout, "process-group hard kill");
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real owned-group child for deterministic inactivity fault injection"]
    async fn whole_group_hard_kill_failure_outranks_inactivity_timeout() {
        let marker = marker_path("group_inactivity");
        let mut run = live_command(&marker, false)
            .start()
            .await
            .expect("start inactivity-bound child");
        wait_for_marker(&marker).await;
        tokio::time::pause();
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let failure = run
            .teardown_on_timeout(TeardownCause::InactivityTimeout)
            .await
            .expect_err("the injected group kill fails");
        record_teardown_failure(&run.teardown_failure, failure);
        let error = run
            .output_string()
            .await
            .expect_err("an unconfirmed inactivity teardown must fail closed");
        assert_teardown_error(
            &error,
            TeardownCause::InactivityTimeout,
            "process-group hard kill",
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(all(feature = "process-control", windows))]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real owned-group child for deterministic graceful fault injection"]
    async fn graceful_group_failure_outranks_cancellation() {
        let marker = marker_path("group_graceful");
        let token = crate::CancellationToken::new();
        let run = survivor_held_output_command(&marker, false)
            .cancel_on(token.clone())
            .cancel_grace(Duration::from_secs(1))
            .start()
            .await
            .expect("start graceful child");
        wait_for_marker(&marker).await;
        let survivor = read_survivor_pid(&marker).await;
        let group = run.own_group_handle().expect("owned process group");
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("graceful"),
                5,
            )
            .arm();

        let started = tokio::time::Instant::now();
        token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(9), run.output_string())
            .await
            .expect("graceful-failure output finalization must remain bounded")
            .expect_err("an unconfirmed graceful escalation must fail closed");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(4),
            "the survivor must hold the pumps through their bounded grace: {elapsed:?}"
        );
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "process-group graceful escalation",
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        assert_survivor_and_cleanup(&group, survivor).await;
        drop(faults);
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(all(feature = "pty", feature = "process-control", windows))]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns an owned-group PTY tree for deterministic hard-kill fault injection"]
    async fn owned_pty_group_hard_failure_bounds_survivor_output_and_salvages_prefix() {
        let marker = marker_path("pty_group_hard");
        let token = crate::CancellationToken::new();
        #[cfg(unix)]
        let command = survivor_held_output_command(&marker, true);
        // A ConPTY root's descendants are terminated/disconnected with that root;
        // use the long-lived root to cover PTY ownership on Windows, while the Unix
        // branch separately proves a surviving slave-fd holder below.
        #[cfg(windows)]
        let command = live_command(&marker, true);
        let run = command
            .cancel_on(token.clone())
            .start()
            .await
            .expect("start owned-group PTY child");
        wait_for_marker(&marker).await;
        #[cfg(unix)]
        let survivor = read_survivor_pid(&marker).await;
        let group = run.own_group_handle().expect("owned PTY process group");
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let started = tokio::time::Instant::now();
        token.cancel();
        let error = tokio::time::timeout(Duration::from_secs(9), run.output_string())
            .await
            .expect("PTY hard-failure finalization must remain bounded")
            .expect_err("an unconfirmed PTY group kill must fail closed");
        let elapsed = started.elapsed();
        // Unix descendants retain the slave fd and exercise the pump deadline.
        // ConPTY closes the root's terminal stream when that root exits even while
        // the separately tracked Job member survives, so Windows proves the PTY
        // disposition/prefix/cleanup axes without a false open-descriptor premise.
        #[cfg(unix)]
        assert!(
            elapsed >= Duration::from_secs(4),
            "the PTY survivor must hold output through the bounded grace: {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(9));
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "process-group hard kill",
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        #[cfg(unix)]
        assert_survivor_and_cleanup(&group, survivor).await;
        #[cfg(windows)]
        assert!(
            group
                .members()
                .expect("members after PTY teardown")
                .is_empty(),
            "the failed group primitive must not hide a live ConPTY root after direct-child kill"
        );
        drop(faults);
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(all(feature = "pty", feature = "process-control", windows))]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns an owned-group PTY tree for deterministic graceful fault injection"]
    async fn owned_pty_group_graceful_failure_bounds_survivor_output_and_salvages_prefix() {
        let marker = marker_path("pty_group_graceful");
        let token = crate::CancellationToken::new();
        #[cfg(unix)]
        let command = survivor_held_output_command(&marker, true);
        #[cfg(windows)]
        let command = live_command(&marker, true);
        let run = command
            .cancel_on(token.clone())
            .cancel_grace(Duration::from_secs(1))
            .start()
            .await
            .expect("start graceful owned-group PTY child");
        wait_for_marker(&marker).await;
        #[cfg(unix)]
        let survivor = read_survivor_pid(&marker).await;
        let group = run.own_group_handle().expect("owned PTY process group");
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("graceful"),
                5,
            )
            .arm();

        let started = tokio::time::Instant::now();
        token.cancel();
        let finish_bound = if cfg!(windows) {
            Duration::from_secs(14)
        } else {
            Duration::from_secs(9)
        };
        let error = tokio::time::timeout(finish_bound, run.output_string())
            .await
            .expect("PTY graceful-failure finalization must remain bounded")
            .expect_err("an unconfirmed PTY graceful escalation must fail closed");
        let elapsed = started.elapsed();
        #[cfg(unix)]
        assert!(
            elapsed >= Duration::from_secs(4),
            "the PTY survivor must hold output through the bounded grace: {elapsed:?}"
        );
        assert!(elapsed < finish_bound);
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "process-group graceful escalation",
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown),
            1
        );
        #[cfg(unix)]
        {
            assert_survivor_and_cleanup(&group, survivor).await;
            drop(faults);
        }
        #[cfg(windows)]
        {
            drop(faults);
            if !group
                .members()
                .expect("members after PTY teardown")
                .is_empty()
            {
                cleanup_group_members(&group).await;
            }
            assert!(
                group
                    .members()
                    .expect("members after PTY cleanup")
                    .is_empty(),
                "the PTY graceful-failure case must leave no survivor"
            );
        }
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(all(
        feature = "pty",
        feature = "process-control",
        any(windows, target_os = "linux")
    ))]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real shared-group PTY child for deterministic kill fault injection"]
    async fn shared_pty_child_kill_failure_is_not_reported_as_cancelled() {
        let marker = marker_path("pty_child_kill");
        let group = ProcessGroup::new().expect("group");
        let mut run = group
            .start(&live_command(&marker, true))
            .await
            .expect("start shared PTY child");
        wait_for_marker(&marker).await;
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::DirectChildKill,
                Some("pty"),
                5,
            )
            .arm();

        let failure = run
            .teardown_on_cancel()
            .await
            .expect_err("the injected PTY child kill fails");
        record_teardown_failure(&run.teardown_failure, failure);
        assert!(
            !group.members().expect("members").is_empty(),
            "the injected PTY child kill really left a live group member"
        );
        assert_eq!(
            faults.fired(crate::sys::fault_injection::Site::DirectChildKill),
            1
        );
        let error = run
            .take_teardown_error("teardown-prefix\n".into(), String::new(), None)
            .expect("an unconfirmed PTY kill must fail closed");
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "direct child hard kill",
        );
        drop(faults);
        run.start_kill().expect("PTY cleanup kill");
        run.backend_wait().await.expect("cleanup reap");
        let _ = std::fs::remove_file(marker);
    }

    #[tokio::test]
    async fn teardown_failure_outranks_cancel_stdin_and_pump_errors() {
        let mut run = scripted_handle(&[0]).await;
        let stdout = SharedLines::new(&OutputBufferPolicy::unbounded());
        stdout.push("teardown-prefix".into());
        stdout.set_read_error(std::io::Error::other("pump failed"));
        run.stdout_sink = Some(stdout);
        run.stdin_error = Some(std::io::Error::other("stdin failed"));
        record_teardown_failure(
            &run.teardown_failure,
            TeardownFailure {
                cause: TeardownCause::Cancellation,
                operation: "process-group hard kill",
                source: std::io::Error::from_raw_os_error(5),
            },
        );

        let error = run
            .output_string()
            .await
            .expect_err("terminal teardown failure has the highest priority");
        assert_teardown_error(
            &error,
            TeardownCause::Cancellation,
            "process-group hard kill",
        );
        assert_eq!(error.kind(), crate::ErrorKind::Teardown);
    }

    /// A scripted (hermetic) handle for `tool`, with the given `ok_codes`.
    async fn scripted_handle(ok_codes: &[i32]) -> RunningProcess {
        let cmd = Command::new("tool").ok_codes(ok_codes.iter().copied());
        ScriptedRunner::new()
            .fallback(Reply::ok(""))
            .start(&cmd)
            .await
            .expect("scripted start")
    }

    #[cfg(all(feature = "pty", windows))]
    #[tokio::test]
    async fn conpty_resize_boundaries_fail_without_recording_or_closing_the_session() {
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let mut run = runner
            .start(&Command::new("tui").use_pty())
            .await
            .expect("scripted PTY start");
        let max = i16::MAX as u16;

        for (cols, rows) in [
            (0, 1),
            (1, 0),
            (max + 1, 1),
            (1, max + 1),
            (u16::MAX, u16::MAX),
        ] {
            let error = run
                .resize_pty(cols, rows)
                .expect_err("invalid ConPTY resize must fail");
            assert!(
                matches!(error.reason(), ErrorReason::Io(source) if source.kind() == std::io::ErrorKind::InvalidInput),
                "expected Io(InvalidInput) for {cols}x{rows}, got {error:?}"
            );
        }
        assert_eq!(
            run.scripted_recorded_resizes(),
            Some(vec![]),
            "invalid geometry must never be delivered to the backend"
        );

        run.resize_pty(1, 1)
            .expect("the live PTY remains usable after refused resizes");
        run.resize_pty(max, max)
            .expect("the signed COORD boundary remains valid");
        assert_eq!(
            run.scripted_recorded_resizes(),
            Some(vec![(1, 1), (max, max)])
        );
    }

    #[cfg(all(feature = "pty", unix))]
    #[tokio::test]
    async fn unix_resize_rejects_zero_but_keeps_large_u16_values() {
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let mut run = runner
            .start(&Command::new("tui").use_pty())
            .await
            .expect("scripted PTY start");

        let error = run
            .resize_pty(0, 1)
            .expect_err("zero-sized Unix PTY geometry must fail");
        assert!(
            matches!(error.reason(), ErrorReason::Io(source) if source.kind() == std::io::ErrorKind::InvalidInput)
        );
        run.resize_pty(i16::MAX as u16 + 1, i16::MAX as u16 + 1)
            .expect("Unix must not inherit ConPTY's signed-coordinate limit");
        run.resize_pty(u16::MAX, u16::MAX)
            .expect("Unix winsize retains the full non-zero u16 range");
        assert_eq!(
            run.scripted_recorded_resizes(),
            Some(vec![
                (i16::MAX as u16 + 1, i16::MAX as u16 + 1),
                (u16::MAX, u16::MAX),
            ])
        );
    }

    /// Install the task slot a real stdin writer uses, without spawning a
    /// subprocess. The delayed completion makes the child-reap-before-source
    /// failure ordering deterministic for borrowed-wait regressions.
    fn delayed_stdin_task(run: &mut RunningProcess, delay: Duration, result: std::io::Result<()>) {
        run.set_test_stdin_task(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            result
        }));
    }

    /// The shape `prepare_line_capture` hands the pipeline — two fresh unbounded
    /// sinks plus their stream configs — without a live child, so the salvage
    /// snapshot can be driven through an exact pump interleaving.
    fn line_capture() -> LineCapture {
        let policy = OutputBufferPolicy::unbounded();
        LineCapture {
            stdout: SharedLines::new(&policy),
            stderr: SharedLines::new(&policy),
            stdout_config: StreamConfig::new(),
            stderr_config: StreamConfig::new(),
        }
    }

    /// The `prepare_raw_capture` analogue of [`line_capture`].
    fn raw_capture() -> RawCapture {
        RawCapture {
            out_buf: Arc::new(std::sync::Mutex::new(Vec::new())),
            stderr_sink: SharedLines::new(&OutputBufferPolicy::unbounded()),
            stderr_config: StreamConfig::new(),
            signals: RawStdoutSignals {
                seen: Arc::new(AtomicUsize::new(0)),
                overflowed: Arc::new(AtomicBool::new(false)),
                truncated: Arc::new(AtomicBool::new(false)),
                read_error: Arc::new(std::sync::Mutex::new(None)),
            },
            stdout_cap: None,
            stdout_mode: OverflowMode::DropOldest,
        }
    }

    #[tokio::test]
    async fn stderr_checkpoint_is_independent_of_stdout_and_non_consuming() {
        let command = Command::new("tool")
            .stdout(crate::StdioMode::Null)
            .output_buffer(OutputBufferPolicy::bounded(1));
        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok("").with_stderr("discarded\nretained-tail"))
            .start(&command)
            .await
            .expect("scripted start");
        let checkpoint = run.prepare_stderr_capture();

        let (stderr, truncated) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (stderr, truncated, _, _) = checkpoint.retained_snapshot();
                if stderr == "retained-tail" {
                    break (stderr, truncated);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stderr pump reaches the independent checkpoint");
        assert_eq!(stderr, "retained-tail");
        assert!(truncated, "the checkpoint retains stderr's buffer policy");

        let finished = run.finish().await.expect("stderr remains consumable");
        assert_eq!(finished.stderr, "retained-tail");
        assert!(
            finished.stderr_truncated,
            "the sole consumer sees the same truncation state"
        );
    }

    /// A chain-wide timeout snapshots the last stage's capture while its pumps
    /// may still be running — dropping the capture task only *requests* their
    /// abort, and a pump stops at its next await. `pump_lines_core` pushes a
    /// completed line and publishes the replacement tail under two separate
    /// locks, so the snapshot can land between them; there it used to take the
    /// stale tail — the prefix of the line it then drained — and repeat it in the
    /// salvaged output.
    #[test]
    fn timeout_salvage_does_not_repeat_a_line_the_pump_just_completed() {
        let capture = line_capture();
        // Each stream's previous read published an un-terminated prefix…
        capture.stdout.set_partial_tail("ab");
        capture.stderr.set_partial_tail("xy");
        // …and the next read completed it into a line. The line is pushed, the
        // replacement tail is not published yet — the deadline fires exactly here.
        capture.stdout.push("abcd".to_owned());
        capture.stderr.push("xyz".to_owned());

        let (stdout, stderr, truncated, total_lines, _total_bytes) = capture.snapshot();

        assert_eq!(
            stdout, "abcd",
            "the tail is the head of this line, not a second line"
        );
        assert_eq!(stderr, "xyz");
        assert!(!truncated, "the policy dropped nothing");
        assert_eq!(total_lines, 2, "one completed line per stream");
    }

    /// …while a tail the pump genuinely had *not* completed is still salvaged —
    /// the whole reason the timeout path snapshots instead of just draining.
    #[test]
    fn timeout_salvage_still_recovers_a_live_partial_tail() {
        let capture = line_capture();
        capture.stdout.push("first".to_owned());
        capture.stdout.set_partial_tail("prompt: ");
        capture.stderr.set_partial_tail("warn");

        let (stdout, stderr, _truncated, total_lines, _total_bytes) = capture.snapshot();

        assert_eq!(stdout, "first\nprompt: ");
        assert_eq!(stderr, "warn");
        assert_eq!(total_lines, 3, "two stdout lines plus the stderr tail");
    }

    /// The same race through the *raw* capture's line-oriented stderr (its
    /// `Vec<u8>` stdout has no line framing to duplicate).
    #[test]
    fn raw_timeout_salvage_does_not_repeat_a_stderr_line_the_pump_just_completed() {
        let capture = raw_capture();
        capture
            .out_buf
            .lock()
            .expect("stdout buffer")
            .extend_from_slice(b"raw bytes");
        capture.stderr_sink.set_partial_tail("bo");
        capture.stderr_sink.push("boom".to_owned());

        let (stdout, stderr, truncated, total_lines, _total_bytes) = capture.snapshot();

        assert_eq!(stdout, b"raw bytes".to_vec());
        assert_eq!(stderr, "boom", "not \"boom\\nbo\"");
        assert!(!truncated);
        assert_eq!(total_lines, 1);
    }

    /// …and the raw capture likewise still salvages a live stderr tail.
    #[test]
    fn raw_timeout_salvage_still_recovers_a_live_stderr_tail() {
        let capture = raw_capture();
        capture.stderr_sink.push("warning:".to_owned());
        capture.stderr_sink.set_partial_tail("no newline yet");

        let (_stdout, stderr, _truncated, total_lines, _total_bytes) = capture.snapshot();

        assert_eq!(stderr, "warning:\nno newline yet");
        assert_eq!(total_lines, 2);
    }

    /// A stashed non-broken-pipe stdin failure surfaces as `ErrorReason::Stdin` only on
    /// an otherwise-successful outcome; a non-zero exit or a signal is the "realer"
    /// failure and wins (outcome passed through).
    #[tokio::test]
    async fn stdin_error_surfaces_only_on_a_successful_outcome() {
        let mut run = scripted_handle(&[0]).await;
        run.stdin_error = Some(std::io::Error::other("boom"));
        match run
            .checked_outcome(Outcome::Exited(0))
            .map_err(|e| e.into_reason())
        {
            Err(ErrorReason::Stdin { program, source }) => {
                assert_eq!(program, "tool");
                assert_eq!(source.to_string(), "boom");
            }
            other => panic!("expected ErrorReason::Stdin, got {other:?}"),
        }

        // Non-zero exit wins: outcome returned for the caller's classifier.
        let mut run = scripted_handle(&[0]).await;
        run.stdin_error = Some(std::io::Error::other("boom"));
        assert!(matches!(
            run.checked_outcome(Outcome::Exited(7)),
            Ok(Outcome::Exited(7))
        ));

        // A signal wins too (not a success).
        let mut run = scripted_handle(&[0]).await;
        run.stdin_error = Some(std::io::Error::other("boom"));
        assert!(matches!(
            run.checked_outcome(Outcome::Signalled(Some(9))),
            Ok(Outcome::Signalled(Some(9)))
        ));
    }

    /// The success gate honors `ok_codes`: a code widened to "accepted" is a
    /// success, so the stdin failure surfaces there too.
    #[tokio::test]
    async fn stdin_error_respects_ok_codes_widened_success() {
        let mut run = scripted_handle(&[0, 3]).await;
        run.stdin_error = Some(std::io::Error::other("boom"));
        assert!(matches!(
            run.checked_outcome(Outcome::Exited(3))
                .map_err(|e| e.into_reason()),
            Err(ErrorReason::Stdin { .. })
        ));
    }

    #[tokio::test]
    async fn no_stdin_error_is_a_clean_passthrough() {
        let mut run = scripted_handle(&[0]).await;
        assert!(matches!(
            run.checked_outcome(Outcome::Exited(0)),
            Ok(Outcome::Exited(0))
        ));
    }

    /// `output_string` after a partial `stdout_lines` stream must NOT report
    /// truncation under the default unbounded policy — the consumed lines were
    /// popped by the stream, not discarded by the buffer.
    #[tokio::test]
    async fn output_string_after_partial_stream_is_not_truncated() {
        use tokio_stream::StreamExt;

        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");

        {
            let mut lines = run.stdout_lines().unwrap();
            assert_eq!(lines.next().await.as_deref(), Some("a"));
            assert_eq!(lines.next().await.as_deref(), Some("b"));
        }

        let result = run.output_string().await.expect("output_string");
        assert!(
            !result.truncated(),
            "consumed lines are not truncation under unbounded policy: {result:?}"
        );
        assert_eq!(
            result.stdout(),
            "c\nd",
            "output_string returns the unconsumed tail"
        );
    }

    #[tokio::test]
    async fn output_bytes_after_streaming_errors_instead_of_empty() {
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");

        drop(run.stdout_lines().unwrap());

        let err = run
            .output_bytes()
            .await
            .expect_err("output_bytes after streaming must error, not return empty");
        assert!(
            err.to_string().contains("readiness or streaming call"),
            "the diagnostic names every line-pump owner: {err}"
        );
        match err.into_reason() {
            ErrorReason::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn readiness_owned_stdout_is_attributed_to_readiness_not_a_fake_stream_call() {
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["ready", "tail"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");
        assert_eq!(
            run.wait_for_line(|line| line == "ready", Duration::from_secs(1))
                .await
                .expect("readiness line"),
            "ready"
        );

        let err = match run.stdout_lines() {
            Ok(_) => panic!("a second stdout consumer must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("readiness or streaming call"),
            "the error must not invent an earlier stdout_lines/events call: {err}"
        );
    }

    /// Other direction: a bounded buffer that genuinely discards lines during
    /// streaming must STILL report `truncated=true` — narrowing only the false
    /// positive (consumed-by-stream), never masking real truncation. Filling
    /// `bounded(2)` with four un-consumed lines drops two deterministically.
    #[tokio::test]
    async fn output_string_after_stream_still_reports_real_truncation() {
        let cmd = Command::new("tool").output_buffer(OutputBufferPolicy::bounded(2));
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&cmd)
            .await
            .expect("scripted start");

        drop(run.stdout_lines().unwrap());

        let result = run.output_string().await.expect("output_string");
        assert!(
            result.truncated(),
            "a bounded buffer that dropped lines during streaming must report truncation: {result:?}"
        );
    }

    /// A bare `finish()` (no prior `stdout_lines`) drains stdout through the
    /// internal discard sink, so a `fail_loud` policy must NOT raise
    /// `OutputTooLarge` for output the caller never asked to capture — the outcome
    /// is consistent with a successful `wait()` for the same process.
    #[tokio::test]
    async fn bare_finish_does_not_error_under_fail_loud_on_uncaptured_stdout() {
        let cmd = Command::new("tool").output_buffer(OutputBufferPolicy::fail_loud(1));

        let finished = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&cmd)
            .await
            .expect("scripted start")
            .finish()
            .await
            .expect("bare finish must not error under fail_loud");
        assert_eq!(finished.outcome, Outcome::Exited(0));

        // The same fail_loud process reaches the same success through wait().
        let outcome = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&cmd)
            .await
            .expect("scripted start")
            .wait()
            .await
            .expect("wait must not error under fail_loud either");
        assert_eq!(
            outcome, finished.outcome,
            "bare finish and wait agree on the uncaptured-stdout outcome"
        );
    }

    #[tokio::test]
    async fn output_string_errors_when_line_terminator_exceeds_raw_byte_cap() {
        let cmd = Command::new("tool").output_buffer(
            OutputBufferPolicy::unbounded()
                .with_overflow(OverflowMode::Error)
                .with_max_bytes(2),
        );
        let err = ScriptedRunner::new()
            .fallback(Reply::ok("ab\n"))
            .start(&cmd)
            .await
            .expect("scripted start")
            .output_string()
            .await
            .expect_err("the newline must exceed the raw-byte cap");

        match err.into_reason() {
            ErrorReason::OutputTooLarge { total_bytes, .. } => {
                assert_eq!(total_bytes, 3, "content plus the newline is reported")
            }
            other => panic!("expected OutputTooLarge, got {other:?}"),
        }
    }

    /// A bare `finish()` still drains BOTH pipes under the default (unbounded)
    /// policy: stdout is discarded (never returned — `Finished` carries no stdout),
    /// stderr is captured in the background and handed back.
    #[tokio::test]
    async fn bare_finish_discards_stdout_but_returns_stderr() {
        let finished = ScriptedRunner::new()
            .fallback(Reply::fail(0, "e1\ne2\n").with_stdout("o1\no2\n"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .finish()
            .await
            .expect("bare finish");
        assert_eq!(finished.outcome, Outcome::Exited(0));
        assert_eq!(finished.stderr, "e1\ne2");
    }

    /// T-042: a plain (non-pipeline) streaming `finish()` must surface stderr
    /// truncation through `Finished::stderr_truncated` when a bounded
    /// `OutputBufferPolicy` silently dropped stderr lines — previously invisible
    /// to any streaming consumer, since `Finished` carried no truncation signal
    /// at all.
    #[tokio::test]
    async fn bare_finish_reports_stderr_truncated_when_the_policy_drops_lines() {
        let cmd = Command::new("tool").output_buffer(OutputBufferPolicy::bounded(2));
        let finished = ScriptedRunner::new()
            .fallback(Reply::fail(1, "e1\ne2\ne3\ne4\n"))
            .start(&cmd)
            .await
            .expect("scripted start")
            .finish()
            .await
            .expect("bare finish");
        assert!(
            finished.stderr_truncated,
            "a bounded policy that dropped stderr lines must set stderr_truncated: {finished:?}"
        );

        // Contrast: the default unbounded policy retains everything — no truncation.
        let untouched = ScriptedRunner::new()
            .fallback(Reply::fail(1, "e1\ne2\ne3\ne4\n"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .finish()
            .await
            .expect("bare finish");
        assert!(
            !untouched.stderr_truncated,
            "an unbounded policy must not report stderr truncation: {untouched:?}"
        );
    }

    /// `stdout_lines()` → drop → `wait()`: the discard verb must complete cleanly
    /// after a dropped live stream (the adopted sink is switched to retain-nothing
    /// so it never reuses the stream's user-policy sink to grow O(total) heap).
    #[tokio::test]
    async fn wait_after_a_dropped_stream_completes() {
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");
        drop(run.stdout_lines().expect("stdout_lines"));
        assert_eq!(
            run.wait().await.expect("wait after a dropped stream"),
            Outcome::Exited(0)
        );
    }

    /// `drain_sink_policy` derives a retain-nothing sink whose in-flight byte cap
    /// tracks the *configured* `output_buffer` — the memory-bound contract of
    /// `drain`, pinned with exact values (K-017): a configured byte cap is used
    /// verbatim; an unbounded policy falls back to the fixed anti-OOM floor; and
    /// the sink is always pure retain-nothing (line cap 0, drop-oldest), never
    /// inheriting the caller's line cap or `Error` overflow mode.
    #[test]
    fn drain_sink_policy_tracks_the_configured_byte_cap() {
        // A configured byte cap is honored verbatim (memory tracks the *limit*).
        let p = drain_sink_policy(&OutputBufferPolicy::unbounded().with_max_bytes(4096));
        assert_eq!(
            p.max_bytes,
            Some(4096),
            "configured byte cap is used verbatim"
        );
        assert_eq!(p.max_lines, Some(0), "drain retains no lines");
        assert_eq!(
            p.overflow,
            OverflowMode::DropOldest,
            "the retain-nothing sink never carries the caller's overflow mode"
        );

        // Unbounded → the same fixed anti-OOM floor `wait` always applies, so a
        // newline-free flood still cannot grow the in-flight buffer unboundedly.
        let p = drain_sink_policy(&OutputBufferPolicy::unbounded());
        assert_eq!(p.max_bytes, Some(DISCARD_INFLIGHT_CAP));
        assert_eq!(p.max_lines, Some(0));

        // A fail-loud caller policy contributes only its byte cap: the sink must
        // NOT inherit `Error` (that would resurrect the overflow bookkeeping
        // `drain` deliberately skips, K-054) nor the caller's line cap.
        let p = drain_sink_policy(&OutputBufferPolicy::fail_loud(100).with_max_bytes(1 << 20));
        assert_eq!(p.max_bytes, Some(1 << 20));
        assert_eq!(p.max_lines, Some(0));
        assert_eq!(p.overflow, OverflowMode::DropOldest);
    }

    /// `drain` feeds every decoded line to the configured per-line handler while
    /// retaining nothing, even when the child prints far more than the configured
    /// byte cap — and it classifies the outcome exactly as `wait`. The child emits
    /// 500 short lines under a 64-byte cap: each line fits (so it is delivered),
    /// the running total dwarfs the cap, and `drain` holds no backlog. Proven by an
    /// exact delivered-line counter (K-017), never by timing.
    #[tokio::test]
    async fn drain_feeds_every_fitting_line_and_matches_wait_outcome() {
        let lines: Vec<String> = (0..500).map(|i| format!("line-{i:04}")).collect();
        // A byte cap far below the cumulative output, but above any single line,
        // so nothing is skipped for the memory bound.
        let cap = OutputBufferPolicy::unbounded().with_max_bytes(64);

        // drain: the handler must see all 500 lines; nothing is retained.
        let seen_drain = Arc::new(AtomicUsize::new(0));
        let sink = seen_drain.clone();
        let cmd = Command::new("chatty")
            .output_buffer(cap)
            .on_stdout_line(move |_| {
                sink.fetch_add(1, Ordering::Relaxed);
            });
        let drain_outcome = ScriptedRunner::new()
            .fallback(Reply::lines(lines.clone()))
            .start(&cmd)
            .await
            .expect("scripted start")
            .drain()
            .await
            .expect("drain");
        assert_eq!(
            seen_drain.load(Ordering::Relaxed),
            500,
            "drain must feed every fitting line to the per-line handler"
        );

        // wait on the identical input: same delivery, same outcome classification.
        let seen_wait = Arc::new(AtomicUsize::new(0));
        let sink = seen_wait.clone();
        let cmd = Command::new("chatty")
            .output_buffer(cap)
            .on_stdout_line(move |_| {
                sink.fetch_add(1, Ordering::Relaxed);
            });
        let wait_outcome = ScriptedRunner::new()
            .fallback(Reply::lines(lines))
            .start(&cmd)
            .await
            .expect("scripted start")
            .wait()
            .await
            .expect("wait");
        assert_eq!(
            seen_wait.load(Ordering::Relaxed),
            500,
            "wait feeds the same lines to the handler; drain skips nothing wait delivers"
        );
        assert_eq!(
            drain_outcome, wait_outcome,
            "drain classifies the outcome exactly as wait"
        );
        assert_eq!(drain_outcome, Outcome::Exited(0));
    }

    /// `drain` classifies a non-zero exit exactly as `wait` — the same
    /// non-checking contract (a failing code is captured in the `Outcome`, not
    /// raised as an error).
    #[tokio::test]
    async fn drain_matches_wait_on_a_failing_outcome() {
        let reply = || Reply::fail(2, "boom").with_stdout("o1\no2\n");
        let drained = ScriptedRunner::new()
            .fallback(reply())
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .drain()
            .await
            .expect("drain does not raise a non-zero exit");
        let waited = ScriptedRunner::new()
            .fallback(reply())
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .wait()
            .await
            .expect("wait does not raise a non-zero exit");
        assert_eq!(drained, waited, "drain and wait agree on a failing outcome");
        assert_eq!(drained, Outcome::Exited(2));
    }

    /// `drain` retains nothing, so a `fail_loud` policy must NOT raise
    /// `OutputTooLarge` for output the caller never asked to capture — exactly like
    /// `wait` (and unlike `output_string`, which WOULD overflow). Pins the discard
    /// contract: `drain` reuses the one `start_discarding` seam, so it does no
    /// retention/overflow bookkeeping (K-054), never forking a second variant.
    #[tokio::test]
    async fn drain_does_not_error_under_fail_loud() {
        let cmd = Command::new("tool").output_buffer(OutputBufferPolicy::fail_loud(1));
        let drained = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&cmd)
            .await
            .expect("scripted start")
            .drain()
            .await
            .expect("drain must not error under fail_loud");
        let waited = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&cmd)
            .await
            .expect("scripted start")
            .wait()
            .await
            .expect("wait must not error under fail_loud either");
        assert_eq!(
            drained, waited,
            "drain and wait agree: uncaptured output does not trip fail_loud"
        );
        assert_eq!(drained, Outcome::Exited(0));
    }

    /// `drain` honors the *configured* `output_buffer` byte cap for its in-flight
    /// bound — the one behavior distinguishing it from `wait` (whose cap is a fixed
    /// 64 MiB). A single line longer than the configured cap is never assembled, so
    /// it reaches neither the per-line handler nor a tee (counted only via the
    /// truncation signal), while every fitting line is delivered in full. This is
    /// what bounds held memory to the *configured* limit rather than the output
    /// size.
    #[tokio::test]
    async fn drain_honors_the_configured_byte_cap() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let sink = seen.clone();
        // 8-byte cap: "aaaa"/"bbbb" fit; the 40-'x' line is over-cap and skipped.
        let over_cap = "x".repeat(40);
        let cmd = Command::new("tool")
            .output_buffer(OutputBufferPolicy::unbounded().with_max_bytes(8))
            .on_stdout_line(move |line| sink.lock().unwrap().push(line.to_owned()));
        let outcome = ScriptedRunner::new()
            .fallback(Reply::lines(["aaaa", over_cap.as_str(), "bbbb"]))
            .start(&cmd)
            .await
            .expect("scripted start")
            .drain()
            .await
            .expect("drain");
        assert_eq!(outcome, Outcome::Exited(0));
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["aaaa".to_owned(), "bbbb".to_owned()],
            "the over-cap line is skipped by the configured byte cap; fitting lines are delivered"
        );
    }

    /// Same as above for `profile()` — the other discard verb.
    #[cfg(feature = "stats")]
    #[tokio::test]
    async fn profile_after_a_dropped_stream_completes() {
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");
        drop(run.stdout_lines().expect("stdout_lines"));
        let profile = run
            .profile(Duration::from_millis(1))
            .await
            .expect("profile after a dropped stream");
        assert_eq!(profile.outcome, Outcome::Exited(0));
    }

    #[tokio::test]
    async fn output_bytes_returns_exact_raw_stdout() {
        let result = ScriptedRunner::new()
            .fallback(Reply::ok("raw\u{0}bytes\nno trailing newline"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .output_bytes()
            .await
            .expect("output_bytes");
        assert_eq!(result.stdout(), b"raw\x00bytes\nno trailing newline");
        assert!(!result.truncated(), "no policy drop: {result:?}");
    }

    /// A `TS_TIMED_OUT` arbiter state overrides `backend_wait`'s clean exit 0.
    #[tokio::test]
    async fn timed_out_flag_classifies_a_clean_exit_as_timed_out() {
        let run = scripted_handle(&[0]).await; // Reply::ok -> Exited(0)
        run.timeout_state.store(TS_TIMED_OUT, Ordering::Release); // simulate the watchdog firing
        let outcome = run.wait().await.expect("wait");
        assert_eq!(
            outcome,
            Outcome::TimedOut,
            "a run whose deadline fired must report TimedOut, not the in-grace exit"
        );
    }

    /// Disarming the inactivity arm for an explicit shutdown must not erase an
    /// inactivity timeout that already won the shared terminal-state arbiter.
    #[tokio::test]
    async fn preclaimed_inactivity_survives_disarming_the_wait_arm() {
        let mut run = scripted_handle(&[0]).await; // Reply::ok -> Exited(0)
        run.inactivity_timeout = Some(Duration::from_secs(1));
        run.timeout_state
            .store(TS_INACTIVITY_TIMED_OUT, Ordering::Release);
        run.inactivity_timeout = None; // mirrors `shutdown` taking sole teardown ownership

        let outcome = run.wait().await.expect("wait");
        assert_eq!(outcome, Outcome::InactivityTimedOut);
    }

    /// Cancellation is checked after `classify_timed_out` and always wins.
    #[tokio::test]
    async fn cancellation_beats_the_timed_out_flag() {
        let token = crate::CancellationToken::new();
        let run = ScriptedRunner::new()
            .fallback(Reply::ok(""))
            .start(&Command::new("tool").cancel_on(token.clone()))
            .await
            .expect("scripted start");
        run.timeout_state.store(TS_TIMED_OUT, Ordering::Release);
        token.cancel();
        match run.wait().await.map_err(|e| e.into_reason()) {
            Err(ErrorReason::Cancelled { .. }) => {}
            other => panic!("expected Err(Cancelled), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_additional_cancel_trigger_cancels_without_firing_the_configured_token() {
        let configured = tokio_util::sync::CancellationToken::new();
        let additional = tokio_util::sync::CancellationToken::new();
        // A ready success is intentional: after cancellation, the biased waiter
        // must observe the additional source synchronously, before a separately
        // spawned watchdog gets any scheduler turn.
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut run = runner
            .start(&Command::new("tool").cancel_on(configured.clone()))
            .await
            .expect("scripted start");
        run.add_cancel_trigger(additional.clone());

        additional.cancel();
        let err = run
            .wait_exit()
            .await
            .expect_err("the additional trigger is a cancellation");
        assert!(matches!(err.reason(), ErrorReason::Cancelled { .. }));
        assert!(
            !configured.is_cancelled(),
            "an additional trigger must not cancel the command-owned token"
        );
    }

    #[tokio::test]
    async fn the_configured_token_still_cancels_after_an_additional_trigger_is_attached() {
        let configured = tokio_util::sync::CancellationToken::new();
        let additional = tokio_util::sync::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let mut run = runner
            .start(&Command::new("tool").cancel_on(configured.clone()))
            .await
            .expect("scripted start");
        run.add_cancel_trigger(additional.clone());

        configured.cancel();
        let err = tokio::time::timeout(Duration::from_secs(5), run.finish())
            .await
            .expect("the configured token must still bound the pending handle")
            .expect_err("the configured token remains a cancellation");
        assert!(matches!(err.reason(), ErrorReason::Cancelled { .. }));
        assert!(
            !additional.is_cancelled(),
            "the command-owned token must not cancel the additional trigger"
        );
    }

    /// Cancel disposition is the race result (`ExitCause`), not a post-hoc read.
    /// A token cancelled after a natural `wait_any` reap cannot flip the cached exit.
    #[tokio::test]
    async fn a_natural_wait_any_exit_is_not_flipped_by_a_late_cancel() {
        let token = crate::CancellationToken::new();
        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok("done\n"))
            .start(&Command::new("tool").cancel_on(token.clone()))
            .await
            .expect("scripted start");
        let (idx, outcome) = crate::wait_any(&mut [&mut run]).await.expect("wait_any");
        assert_eq!((idx, outcome), (0, Outcome::Exited(0)));
        token.cancel();
        let outcome = run
            .wait()
            .await
            .expect("a late cancel must not flip a natural exit");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    /// The probe path (`wait_for`/`wait_for_port` → `has_exited_now`) snapshots
    /// the cancel disposition at observation time, first-observation-wins, so a
    /// cancel that fires *after* the probe has already seen a natural reap cannot
    /// flip it — mirroring the no-probe `wait_any` path
    /// (`a_natural_wait_any_exit_is_not_flipped_by_a_late_cancel`).
    #[tokio::test]
    async fn a_probe_reap_is_not_flipped_by_a_later_cancel() {
        let token = crate::CancellationToken::new();
        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok("done\n"))
            .start(&Command::new("tool").cancel_on(token.clone()))
            .await
            .expect("scripted start");
        // `Reply::ok` gives the scripted child a zero lifetime — it has already
        // "exited" (exit_at == start). A never-passing check drives `poll_until`
        // straight into `has_exited_now`, which observes the reap while the token
        // is still live, snapshots the natural disposition, and bails NotReady.
        match run
            .wait_for(|| async { false }, Duration::from_secs(5))
            .await
            .map_err(|e| e.into_reason())
        {
            Err(ErrorReason::NotReady { .. }) => {}
            other => panic!("expected Err(NotReady), got {other:?}"),
        }
        // Cancel only now, after the probe already observed the exit: the frozen
        // observation-time snapshot wins over this late cancel.
        token.cancel();
        let outcome = run
            .wait()
            .await
            .expect("a cancel after the probe's reap observation must not flip it");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    /// A cancel already *active* when the probe observes the reap is not dropped:
    /// the observation-time snapshot latches `Cancelled`, so the consuming
    /// finisher reports `Err(Cancelled)`. This is the contract documented on
    /// `Command::cancel_on` (the probe surfaces `NotReady`; the finisher
    /// afterwards still reports `Cancelled`) and the disposition the bulk
    /// biased-`select!` paths give for the same "cancel before observation"
    /// timeline — a real-backend cancel watchdog would have killed the tree
    /// between the cancel and the probe's observation.
    #[tokio::test]
    async fn a_probe_reap_with_an_active_cancel_still_surfaces_cancelled() {
        let token = crate::CancellationToken::new();
        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok("done\n"))
            .start(&Command::new("tool").cancel_on(token.clone()))
            .await
            .expect("scripted start");
        // Cancel BEFORE the probe observes the (zero-lifetime) child's exit, so
        // the token is live at observation time and the snapshot must latch it.
        token.cancel();
        match run
            .wait_for(|| async { false }, Duration::from_secs(5))
            .await
            .map_err(|e| e.into_reason())
        {
            Err(ErrorReason::NotReady { .. }) => {}
            other => panic!("expected Err(NotReady), got {other:?}"),
        }
        // The cancel active at observation is preserved: the finisher reports it,
        // never a silent `Ok` for a run the cancel really tore down.
        match run.wait().await.map_err(|e| e.into_reason()) {
            Err(ErrorReason::Cancelled { .. }) => {}
            other => panic!("expected Err(Cancelled), got {other:?}"),
        }
    }

    /// The blind spot first-observation-wins leaves, pinned because both the
    /// `Command::cancel_on` rustdoc and the cancellation guide now promise exactly
    /// this: a held run that **nobody observed** has no earlier observation for a
    /// late token to lose to, so the consuming finisher *is* the first observation —
    /// and a token fired before it reports `Cancelled` even though the child had
    /// already exited on its own (the `biased` cancel arm in `drive_to_exit_inner`
    /// wins the race by design). Probing first is what buys the real outcome, which
    /// is `a_probe_reap_is_not_flipped_by_a_later_cancel` above; this is its
    /// counterpart, and the two together are the whole contract.
    #[tokio::test]
    async fn an_unobserved_exit_is_cancelled_by_a_token_fired_before_the_finisher() {
        let token = crate::CancellationToken::new();
        let run = ScriptedRunner::new()
            .fallback(Reply::ok("done\n"))
            .start(&Command::new("tool").cancel_on(token.clone()))
            .await
            .expect("scripted start");
        // `Reply::ok` gives the scripted child a zero lifetime: it has already
        // "exited" here — and nothing has looked at it. Fire the token, then look
        // for the very first time.
        token.cancel();
        match run.finish().await.map_err(|e| e.into_reason()) {
            Err(ErrorReason::Cancelled { .. }) => {}
            other => panic!("expected Err(Cancelled), got {other:?}"),
        }
    }

    /// The buffering verbs' exit seam (`output_string_observing_exit` /
    /// `output_bytes_observing_exit`) must actually fire, exactly once. A seam that
    /// silently never fired would not fail loudly anywhere: the pipeline's last
    /// stage would just fall back to reading the teardown token *after* its drain —
    /// the very attribution bug the latch exists to prevent — so nothing but this
    /// test stands between that regression and a green run.
    #[tokio::test]
    async fn the_buffering_verbs_fire_their_exit_seam_exactly_once() {
        let observed = Arc::new(AtomicUsize::new(0));
        let counter = observed.clone();
        let result = ScriptedRunner::new()
            .fallback(Reply::ok("done\n"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .output_string_observing_exit(move |_| {
                counter.fetch_add(1, Ordering::Release);
            })
            .await
            .expect("output_string_observing_exit");
        assert_eq!(result.outcome(), Outcome::Exited(0));
        assert_eq!(
            observed.load(Ordering::Acquire),
            1,
            "output_string's seam fires once for the one exit it observes"
        );

        let observed = Arc::new(AtomicUsize::new(0));
        let counter = observed.clone();
        let result = ScriptedRunner::new()
            .fallback(Reply::ok("raw"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start")
            .output_bytes_observing_exit(move |_| {
                counter.fetch_add(1, Ordering::Release);
            })
            .await
            .expect("output_bytes_observing_exit");
        assert_eq!(result.outcome(), Outcome::Exited(0));
        assert_eq!(
            observed.load(Ordering::Acquire),
            1,
            "and so does output_bytes', which drives its own teardown spine"
        );
    }

    /// T-078: every consuming reap retires the shared `PidGate`, so the detached
    /// cancellation / streaming-deadline watchdogs — which funnel every raw
    /// `kill(pid)` through it — stand down and can never signal the freed (and
    /// possibly OS-recycled) pid. The gate's own linearizability (a retired gate
    /// runs no kill, even under contention) is proven in `sys::pid_gate::tests`;
    /// these cases prove each reap *path* reaches that retired state. The gate
    /// `Arc` is cloned before the consuming call so it can be inspected after.
    #[tokio::test]
    async fn a_consuming_reap_retires_the_pid_gate() {
        let run = scripted_handle(&[0]).await;
        let gate = run.pid_gate.clone();
        assert!(!gate.is_retired(), "a fresh handle's gate is live");
        run.wait().await.expect("wait");
        assert!(
            gate.is_retired(),
            "the reap must retire the gate so the watchdogs stand down"
        );
    }

    /// The cancellation-watchdog path: cancelling and then finishing retires the
    /// gate (`drive_to_exit_inner` retires *before* it reaps, and `kill_tree`/
    /// `abort_watchdogs` keep it retired), so the cancel watchdog cannot race the
    /// reap with a raw kill.
    #[tokio::test]
    async fn a_cancelled_reap_retires_the_pid_gate() {
        let token = crate::CancellationToken::new();
        let run = ScriptedRunner::new()
            .fallback(Reply::ok(""))
            .start(&Command::new("tool").cancel_on(token.clone()))
            .await
            .expect("scripted start");
        let gate = run.pid_gate.clone();
        token.cancel();
        // Consumes the run; the outcome is `Err(Cancelled)`, but the gate must be
        // retired regardless so the cancel watchdog stands down.
        let _ = run.wait().await;
        assert!(
            gate.is_retired(),
            "the cancellation reap path must retire the gate"
        );
    }

    /// The probe path (`wait_for` → `has_exited_now`) retires the gate *atomically
    /// with* its synchronous `try_wait` reap via `PidGate::reap_under_lock`, fully
    /// closing the window rather than merely bounding it.
    #[tokio::test]
    async fn a_probe_reap_retires_the_pid_gate() {
        let mut run = scripted_handle(&[0]).await; // Reply::ok -> zero-lifetime child
        let gate = run.pid_gate.clone();
        // A never-passing check drives `poll_until` straight into `has_exited_now`,
        // which observes (and reaps) the already-exited scripted child.
        let _ = run
            .wait_for(|| async { false }, Duration::from_secs(5))
            .await;
        assert!(
            gate.is_retired(),
            "the probe's reap_under_lock must retire the gate atomically"
        );
    }

    /// The natural-exit backstop through `wait_any` (`wait_exit` → `backend_wait`)
    /// retires the gate: the reap now runs *inside* the gate lock (the `gated_reap`
    /// poll of `Child::wait()`), so a detached cancel/deadline watchdog racing the
    /// reap stands down — it can never raw-kill the freed (possibly recycled) pid.
    /// The gate's own linearizability is proven in `sys::pid_gate::tests`; this
    /// proves the `wait_any` reap path *reaches* the retired state.
    #[tokio::test]
    async fn a_wait_any_reap_retires_the_pid_gate() {
        let mut run = scripted_handle(&[0]).await; // Reply::ok -> zero-lifetime child
        let gate = run.pid_gate.clone();
        assert!(!gate.is_retired(), "a fresh handle's gate is live");
        let (idx, outcome) = crate::wait_any(&mut [&mut run]).await.expect("wait_any");
        assert_eq!((idx, outcome), (0, Outcome::Exited(0)));
        assert!(
            gate.is_retired(),
            "the wait_any (backend_wait) reap must retire the gate so the watchdogs \
             stand down"
        );
    }

    /// Same for the `wait_all` join path — the other non-consuming reap surface.
    #[tokio::test]
    async fn a_wait_all_reap_retires_the_pid_gate() {
        let mut run = scripted_handle(&[0]).await;
        let gate = run.pid_gate.clone();
        let outcomes = crate::wait_all(&mut [&mut run]).await.expect("wait_all");
        assert_eq!(outcomes, vec![Outcome::Exited(0)]);
        assert!(
            gate.is_retired(),
            "the wait_all reap must retire the gate too"
        );
    }

    #[tokio::test]
    async fn wait_any_observes_delayed_stdin_error_after_reap() {
        let mut run = scripted_handle(&[0]).await;
        delayed_stdin_task(
            &mut run,
            Duration::from_millis(10),
            Err(std::io::Error::other("delayed stdin failure")),
        );

        let err = crate::wait_any(&mut [&mut run])
            .await
            .expect_err("wait_any must classify a delayed source failure");
        assert!(
            matches!(err.reason(), ErrorReason::Stdin { .. }),
            "got: {err:?}"
        );
        assert!(
            run.stdin_error.is_none(),
            "checked_outcome must consume the observed source error exactly once"
        );
        assert_eq!(
            run.wait()
                .await
                .expect("a repeated wait still returns the cached exit"),
            Outcome::Exited(0)
        );
    }

    #[tokio::test]
    async fn wait_all_observes_delayed_stdin_error_after_reap() {
        let mut run = scripted_handle(&[0]).await;
        delayed_stdin_task(
            &mut run,
            Duration::from_millis(10),
            Err(std::io::Error::other("delayed stdin failure")),
        );

        let err = crate::wait_all(&mut [&mut run])
            .await
            .expect_err("wait_all must classify a delayed source failure");
        assert!(
            matches!(err.reason(), ErrorReason::Stdin { .. }),
            "got: {err:?}"
        );
        assert!(
            run.stdin_error.is_none(),
            "checked_outcome must consume the observed source error exactly once"
        );
    }

    #[tokio::test]
    async fn borrowed_wait_preserves_successful_stdin_completion() {
        let mut run = scripted_handle(&[0]).await;
        delayed_stdin_task(&mut run, Duration::from_millis(10), Ok(()));

        assert_eq!(
            crate::wait_all(&mut [&mut run])
                .await
                .expect("a successful source must not change the exit result"),
            vec![Outcome::Exited(0)]
        );
        assert!(run.stdin_error.is_none());
        assert_eq!(
            crate::wait_any(&mut [&mut run])
                .await
                .expect("a repeated borrowed wait remains usable"),
            (0, Outcome::Exited(0))
        );
    }

    /// Dropping the losing `wait_any` future after it has reaped its child must
    /// leave the stdin writer available for a later borrowed wait.
    #[tokio::test]
    async fn wait_any_loser_keeps_a_post_reap_stdin_error() {
        let mut loser = scripted_handle(&[0]).await;
        delayed_stdin_task(
            &mut loser,
            Duration::from_millis(100),
            Err(std::io::Error::other("loser stdin failure")),
        );
        let mut winner = scripted_handle(&[0]).await;

        let (index, outcome) = crate::wait_any(&mut [&mut loser, &mut winner])
            .await
            .expect("the winner exits cleanly");
        assert_eq!((index, outcome), (1, Outcome::Exited(0)));
        assert_eq!(
            loser.cancel_at_exit,
            Some(false),
            "the loser must have been reaped before wait_any cancelled its wait"
        );

        let err = crate::wait_any(&mut [&mut loser])
            .await
            .expect_err("the cancelled loser wait must retain its stdin error");
        assert!(
            matches!(err.reason(), ErrorReason::Stdin { .. }),
            "got: {err:?}"
        );
        assert!(
            loser.stdin_error.is_none(),
            "the source error is consumed exactly once"
        );
        assert_eq!(
            crate::wait_any(&mut [&mut loser])
                .await
                .expect("a repeated wait remains usable"),
            (0, Outcome::Exited(0))
        );
    }

    /// When `wait_all` short-circuits on another contender's error, a contender
    /// suspended in post-reap stdin finalization must remain re-awaitable too.
    #[tokio::test]
    async fn wait_all_loser_keeps_a_post_reap_stdin_error() {
        let mut loser = scripted_handle(&[0]).await;
        delayed_stdin_task(
            &mut loser,
            Duration::from_millis(100),
            Err(std::io::Error::other("wait_all loser stdin failure")),
        );
        let mut failing = scripted_handle(&[0]).await;
        let task = tokio::spawn(async {
            Err::<(), std::io::Error>(std::io::Error::other("join short-circuit"))
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        failing.set_test_stdin_task(task);

        let err = crate::wait_all(&mut [&mut loser, &mut failing])
            .await
            .expect_err("the finished stdin error must short-circuit wait_all");
        assert!(
            matches!(err.reason(), ErrorReason::Stdin { .. }),
            "got: {err:?}"
        );
        assert_eq!(
            loser.cancel_at_exit,
            Some(false),
            "the loser must have been reaped before wait_all short-circuited"
        );

        let err = crate::wait_any(&mut [&mut loser])
            .await
            .expect_err("the cancelled wait_all loser must retain its stdin error");
        assert!(
            matches!(err.reason(), ErrorReason::Stdin { .. }),
            "got: {err:?}"
        );
        assert!(
            loser.stdin_error.is_none(),
            "the source error is observed once"
        );
        assert_eq!(
            crate::wait_any(&mut [&mut loser])
                .await
                .expect("a repeated wait remains usable"),
            (0, Outcome::Exited(0))
        );
    }

    /// An external timeout can cancel the borrowed wait while its post-reap
    /// finalization is pending; the next wait must still classify the source.
    #[tokio::test]
    async fn externally_cancelled_wait_all_keeps_a_post_reap_stdin_error() {
        let mut run = scripted_handle(&[0]).await;
        delayed_stdin_task(
            &mut run,
            Duration::from_millis(100),
            Err(std::io::Error::other("externally cancelled stdin failure")),
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(10), crate::wait_all(&mut [&mut run]))
                .await
                .is_err(),
            "the outer timeout must cancel while stdin finalization is pending"
        );
        assert_eq!(
            run.cancel_at_exit,
            Some(false),
            "the external cancellation must happen after the child reap"
        );

        let err = crate::wait_any(&mut [&mut run])
            .await
            .expect_err("the re-await must classify the retained stdin error");
        assert!(
            matches!(err.reason(), ErrorReason::Stdin { .. }),
            "got: {err:?}"
        );
        assert!(
            run.stdin_error.is_none(),
            "the source error is observed once"
        );
        assert_eq!(
            crate::wait_all(&mut [&mut run])
                .await
                .expect("a repeated wait remains usable"),
            vec![Outcome::Exited(0)]
        );
    }

    /// Finalizing a borrowed wait still filters the routine broken-pipe result.
    #[tokio::test]
    async fn borrowed_wait_keeps_broken_pipe_as_a_clean_exit() {
        let mut run = scripted_handle(&[0]).await;
        delayed_stdin_task(
            &mut run,
            Duration::from_millis(10),
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        );

        assert_eq!(
            crate::wait_any(&mut [&mut run])
                .await
                .expect("a broken pipe is normal stdin closure"),
            (0, Outcome::Exited(0))
        );
        assert!(run.stdin_error.is_none());
    }

    #[tokio::test]
    async fn wait_any_keeps_cancellation_precedence_over_delayed_stdin_error() {
        let token = crate::CancellationToken::new();
        let cmd = Command::new("tool").cancel_on(token.clone());
        let mut run = ScriptedRunner::new()
            .fallback(Reply::pending())
            .start(&cmd)
            .await
            .expect("scripted start");
        delayed_stdin_task(
            &mut run,
            Duration::from_millis(10),
            Err(std::io::Error::other("delayed stdin failure")),
        );
        token.cancel();

        let err = crate::wait_any(&mut [&mut run])
            .await
            .expect_err("cancellation must remain the dominant classification");
        assert!(
            matches!(err.reason(), ErrorReason::Cancelled { .. }),
            "got: {err:?}"
        );
        assert!(
            run.test_stdin_task.is_none(),
            "the source task is finalized once even when cancellation wins"
        );
    }

    // --- T-092: Drop retires the gate on the branches with no detached reaper -----
    //
    // A scripted double is pid-less (`Backend::Scripted`, `PidGate::new(None)`) and
    // its Drop takes the `s.kill()` arm, so it can't exercise the `Backend::Real`
    // Drop branches. These two therefore spawn a real child — hence `#[ignore]`, run
    // in CI via `cargo test -- --include-ignored`, like the rest of the crate's
    // real-subprocess coverage. The assertions are still deterministic: `drop()`
    // retires the gate *synchronously*, so `is_retired()` right after the drop does
    // not depend on the child's own exit timing. The gate's linearizability once
    // retired (a retired gate runs no raw kill, even under thread contention) is
    // proven hermetically in `sys::pid_gate::tests`
    // (`a_retire_before_a_separate_pid_free_still_bars_a_racing_kill` models this very
    // retire-before-free ordering); these prove each Drop *branch reaches* that
    // retired state before the pid can be freed.

    /// A real child that runs a while with no output, per platform — held alive
    /// across the Drop so the gate's live→retired transition is what the assertion
    /// turns on, not the child exiting on its own.
    fn sleeper_cmd() -> Command {
        if cfg!(windows) {
            Command::new("cmd").args(["/c", "ping", "-n", "30", "127.0.0.1"])
        } else {
            Command::new("sleep").arg("30")
        }
    }

    /// Dropping a **shared-group** handle with a timeout but NO grace window
    /// (`own_group.is_none()`, `timeout_grace.is_none()`) — the structural-drop Drop
    /// branch that hands the child to no detached reaper — must retire the shared
    /// `PidGate` synchronously, so a deadline/cancel watchdog aborted mid-poll can
    /// never land its gated raw kill on the freed (and possibly OS-recycled) pid.
    #[tokio::test]
    #[ignore = "spawns a real subprocess (shared-group Drop-branch gate retirement)"]
    async fn dropping_a_shared_group_handle_without_grace_retires_the_gate() {
        let group = crate::group::ProcessGroup::new().expect("a shared process group");
        let cmd = sleeper_cmd().timeout(Duration::from_secs(30));
        // `launch` (unlike `JobRunner::start`) attaches no owned group, so this is a
        // shared-group handle: `own_group` is `None` and the caller's `group` owns the
        // tree teardown.
        let run = crate::runner::launch(&group, &cmd)
            .await
            .expect("launch into the shared group");
        assert!(
            !run.kills_tree_on_drop(),
            "a shared-group handle owns no tree — its group does"
        );
        assert_eq!(
            run.timeout_grace, None,
            "the branch under test has a timeout but no graceful window"
        );
        let gate = run.pid_gate.clone();
        assert!(
            !gate.is_retired(),
            "a fresh live handle's gate is not retired"
        );
        drop(run); // exercises Drop's shared-group-without-grace branch
        assert!(
            gate.is_retired(),
            "Drop must retire the gate so a mid-poll watchdog's raw kill is a \
             linearized no-op, never a SIGKILL on a recycled pid"
        );
        // The shared group still owns the child's teardown; drop it to tear the
        // (orphan-reaped) child down and keep the test process-clean.
        drop(group);
    }

    /// R-01/T-255: a shared-group handle that merely *configures* `cancel_grace`,
    /// with a token that has NOT fired, must take the same synchronous
    /// retire-and-structurally-drop `else` branch — NOT the detached child hand-off.
    ///
    /// The hand-off exists solely to keep tokio's orphan reaper from freeing the pid
    /// behind a detached `spawn_graceful_kill_and_reap`, and only a *fired* token can
    /// have armed one. Keyed on the static "a token is configured" shape instead, this
    /// handle — the shape the cancellation docs recommend (one shared app-wide token
    /// plus `cancel_grace`) — would park a detached reaper on `child.wait()` and leave
    /// its `PidGate` un-retired for the child's entire, unbounded life, with no
    /// cancellation having happened at all.
    ///
    /// Deterministic, no timing: `drop()` retires the gate *synchronously*, so the
    /// assertion turns on the branch taken, not on the child's exit (a 30-second
    /// sleeper outlives the whole test). Real subprocess — hence `#[ignore]` — because
    /// a scripted double is pid-less and takes the `Scripted` Drop arm.
    #[tokio::test]
    #[ignore = "spawns a real subprocess (shared-group + cancel_grace, un-fired token, Drop)"]
    async fn dropping_a_cancel_grace_handle_with_an_unfired_token_retires_the_gate() {
        let group = crate::group::ProcessGroup::new().expect("a shared process group");
        let token = tokio_util::sync::CancellationToken::new();
        // No timeout at all: `cancel_grace` needs none, which is exactly why the
        // static form has no upper bound to fall back on.
        let cmd = sleeper_cmd()
            .cancel_on(token.clone())
            .cancel_grace(Duration::from_secs(5));
        let run = crate::runner::launch(&group, &cmd)
            .await
            .expect("launch into the shared group");
        assert!(
            !run.kills_tree_on_drop(),
            "a shared-group handle owns no tree — its group does"
        );
        assert!(
            run.timeout.is_none() && run.inactivity_timeout.is_none(),
            "the shape under test is cancellation-only: no deadline bounds the hold"
        );
        let gate = run.pid_gate.clone();
        assert!(
            !token.is_cancelled(),
            "the token under test has NOT fired, so no detached killer can exist"
        );
        assert!(
            !gate.is_retired(),
            "a fresh live handle's gate is not retired"
        );
        drop(run); // exercises Drop with the cancel half's dynamic read = false
        assert!(
            gate.is_retired(),
            "an un-fired cancel token must not divert Drop into the detached \
             hand-off: the gate has to be retired synchronously, as it was before \
             cancel_grace existed"
        );
        drop(group);
    }

    /// The positive counterpart: once the token HAS fired, the same shared-group +
    /// `cancel_grace` handle still hands its child to the gated reaper, because a
    /// cancel watchdog may already have armed the detached `graceful_kill_pid` — the
    /// pid must then be freed only *under* the gate.
    ///
    /// Deterministic without timing games: `#[tokio::test]` is a current-thread
    /// runtime, so no spawned task can run between `token.cancel()` and the assertion
    /// (there is no `.await` between them) — the observed state is exactly the branch
    /// Drop took. Reading a fired token is deliberately conservative: it also covers
    /// the case where `Drop` aborted the watchdog before it ever armed anything.
    #[tokio::test]
    #[ignore = "spawns a real subprocess (shared-group + cancel_grace, fired token, Drop)"]
    async fn dropping_a_cancel_grace_handle_with_a_fired_token_hands_the_child_off() {
        let group = crate::group::ProcessGroup::new().expect("a shared process group");
        let token = tokio_util::sync::CancellationToken::new();
        let cmd = sleeper_cmd()
            .cancel_on(token.clone())
            .cancel_grace(Duration::from_secs(5));
        let run = crate::runner::launch(&group, &cmd)
            .await
            .expect("launch into the shared group");
        let gate = run.pid_gate.clone();
        token.cancel();
        drop(run); // exercises Drop with the cancel half's dynamic read = true
        assert!(
            !gate.is_retired(),
            "a fired cancel token keeps the hand-off: the gate must stay live until \
             the detached gated reaper retires it atomically with the reap, so a \
             detached grace killer can never outlive it onto a recycled pid"
        );
        // The shared group owns the teardown; dropping it kills the child, which the
        // handed-off reaper then reaps under the gate.
        drop(group);
    }

    /// The own-group counterpart: dropping a private-group handle
    /// (`own_group.is_some()`, so `kills_tree_on_drop()` is `true`) also retires the
    /// gate. The tree is still torn down by the owned group as it drops; the retire
    /// only stands the raw-pid watchdogs down so their kill can't outlive that
    /// teardown onto a recycled pid.
    #[tokio::test]
    #[ignore = "spawns a real subprocess (own-group Drop-branch gate retirement)"]
    async fn dropping_an_own_group_handle_retires_the_gate() {
        let cmd = sleeper_cmd().timeout(Duration::from_secs(30));
        let run = crate::runner::JobRunner::new()
            .start(&cmd)
            .await
            .expect("start a private-group run");
        assert!(
            run.kills_tree_on_drop(),
            "a private-group handle tears its whole tree down on drop"
        );
        let gate = run.pid_gate.clone();
        assert!(
            !gate.is_retired(),
            "a fresh live handle's gate is not retired"
        );
        drop(run); // exercises Drop's own-group branch (tree torn down + gate retired)
        assert!(
            gate.is_retired(),
            "the own-group Drop branch must retire the gate too"
        );
    }

    /// T-093: the **no-runtime** Drop of a shared-group + grace handle. Its static
    /// shape (`own_group.is_none() && timeout.is_some() && timeout_grace.is_some()`)
    /// is exactly the detached-handoff branch's, but that branch also needs a
    /// *current* tokio runtime to spawn its reaper on. Dropping such a handle with
    /// NO runtime current — `Handle::try_current()` is `Err`, so the hand-off cannot
    /// run — must STILL retire the `PidGate` synchronously (via the `else`), or a
    /// deadline/cancel watchdog aborted mid-poll could outlive an un-retired gate and
    /// land a raw kill on the freed (and possibly OS-recycled) pid.
    ///
    /// Deterministic, no timing: the handle is built *inside* a runtime (spawning a
    /// child needs one) but dropped only AFTER `block_on` returns, when this thread
    /// provably holds no runtime context — asserted directly via `try_current()` —
    /// so the drop takes the no-runtime path every run. A long-lived sleeper keeps
    /// the child alive across the drop, so the assertion turns on the gate's
    /// synchronous live→retired transition, never on the child exiting. Like the two
    /// T-092 Drop cases above this spawns a real subprocess, hence `#[ignore]` (run
    /// in CI via `--include-ignored`); a scripted double is pid-less and takes the
    /// `Scripted` Drop arm, so it cannot exercise the `Backend::Real` branch. The
    /// gate's linearizability once retired is proven hermetically in
    /// `sys::pid_gate::tests`; this proves the no-runtime Drop *reaches* the retired
    /// state before the pid can be freed.
    #[test] // NOT `#[tokio::test]`: the drop must happen with no current runtime.
    #[ignore = "spawns a real subprocess (no-runtime shared-group+grace Drop gate retirement)"]
    fn dropping_a_shared_group_grace_handle_with_no_runtime_retires_the_gate() {
        let rt = tokio::runtime::Runtime::new().expect("a test runtime");
        let group = crate::group::ProcessGroup::new().expect("a shared process group");
        // Build the handoff-SHAPE handle inside the runtime: a shared group (no owned
        // group, via `launch`) with BOTH a timeout and a grace window — the exact
        // static preconditions of the detached-handoff branch.
        let (run, gate) = rt.block_on(async {
            let cmd = sleeper_cmd()
                .timeout(Duration::from_secs(30))
                .timeout_grace(Duration::from_secs(5));
            let run = crate::runner::launch(&group, &cmd)
                .await
                .expect("launch into the shared group");
            let gate = run.pid_gate.clone();
            (run, gate)
        });
        // The handle matches the handoff branch's static preconditions...
        assert!(
            !run.kills_tree_on_drop(),
            "a shared-group handle owns no tree — its group does (own_group is None)"
        );
        assert!(
            run.timeout.is_some() && run.timeout_grace.is_some(),
            "the shape under test has both a timeout and a graceful window"
        );
        assert!(
            !gate.is_retired(),
            "a fresh live handle's gate is not retired"
        );
        // ...but the drop below happens OUTSIDE any runtime: `block_on` has returned,
        // so this thread holds no runtime context and the hand-off cannot be spawned.
        // Asserting this makes the no-runtime scenario deterministic, not incidental.
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the drop below must run with no current runtime — the scenario under test"
        );
        drop(run); // exercises Drop's shared-group+grace shape with NO runtime current
        assert!(
            gate.is_retired(),
            "Drop must retire the gate even with no runtime current, so a watchdog \
             aborted mid-poll can't land a raw kill on the freed/recycled pid"
        );
        // The shared group still owns the child's teardown; dropping it tears the
        // child down (job-close / SIGKILL, synchronous and runtime-free) so the test
        // leaves no live subprocess. `rt` is dropped last, after the child is gone.
        drop(group);
        drop(rt);
    }

    /// `wait_exit` applies `classify_timed_out` so the `stdout_lines` → `wait_any`
    /// composition is consistent with `finish`.
    #[tokio::test]
    async fn wait_any_classifies_a_timed_out_run() {
        let mut run = scripted_handle(&[0]).await; // Reply::ok -> Exited(0)
        run.timeout_state.store(TS_TIMED_OUT, Ordering::Release); // simulate the watchdog firing
        let (idx, outcome) = crate::wait_any(&mut [&mut run]).await.expect("wait_any");
        assert_eq!(idx, 0);
        assert_eq!(
            outcome,
            Outcome::TimedOut,
            "a timed-out run must report TimedOut through wait_any, not the raw exit"
        );
    }

    /// The timeout arbiter is race-free. Once the natural reap claims
    /// `EXITED`, a watchdog whose timer fires late cannot flip the run to
    /// `TimedOut` (its CAS from `PENDING` fails), so a child that exits on its own
    /// within a scheduler quantum of the deadline keeps its real outcome. (The
    /// reverse — the deadline claiming `TIMED_OUT` first — is covered by
    /// `timed_out_flag_classifies_a_clean_exit_as_timed_out`.)
    #[tokio::test]
    async fn natural_reap_claim_beats_a_late_timeout_cas() {
        let run = scripted_handle(&[0]).await;
        assert!(
            run.timeout_state
                .compare_exchange(TS_PENDING, TS_EXITED, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        );
        assert!(
            run.timeout_state
                .compare_exchange(
                    TS_PENDING,
                    TS_TIMED_OUT,
                    Ordering::AcqRel,
                    Ordering::Relaxed
                )
                .is_err()
        );
        assert_eq!(
            run.classify_watchdog_timeout(Outcome::Exited(0)),
            Outcome::Exited(0)
        );
    }

    #[tokio::test]
    async fn scripted_handle_does_not_kill_a_tree_on_drop() {
        let run = scripted_handle(&[0]).await;
        assert!(
            !run.kills_tree_on_drop(),
            "a scripted double has no OS tree to tear down"
        );
    }

    #[tokio::test]
    async fn capture_verbs_error_on_a_non_piped_stdout() {
        let runner = ScriptedRunner::new().fallback(Reply::ok("ignored"));

        let run = runner
            .start(&Command::new("tool").stdout(crate::StdioMode::Null))
            .await
            .unwrap();
        match run.output_string().await.map_err(|e| e.into_reason()) {
            Err(ErrorReason::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }

        // output_bytes on an Inherit stdout → also errors.
        let run = runner
            .start(&Command::new("tool").stdout(crate::StdioMode::Inherit))
            .await
            .unwrap();
        assert!(matches!(
            run.output_bytes().await.map_err(|e| e.into_reason()),
            Err(ErrorReason::Io(_))
        ));

        let run = ScriptedRunner::new()
            .fallback(Reply::ok("hi"))
            .start(&Command::new("tool"))
            .await
            .unwrap();
        assert_eq!(run.output_string().await.unwrap().stdout(), "hi");

        let run = runner
            .start(&Command::new("tool").stdout(crate::StdioMode::Null))
            .await
            .unwrap();
        assert!(
            run.wait().await.is_ok(),
            "discard verbs do not require a piped stdout"
        );
    }

    // --- T-087: raw `output_bytes` read-error seam --------------------------

    /// A reader that yields predefined byte chunks one `poll_read` at a time, then
    /// either EOFs or returns one IO error — the raw-bytes analogue of `pump.rs`'s
    /// `ChunkedReader`, exercising [`pump_raw_bytes`]'s read-error / clean-EOF /
    /// broken-pipe classification deterministically without a live child.
    struct RawChunkedReader {
        chunks: std::collections::VecDeque<Vec<u8>>,
        err_at_end: Option<std::io::Error>,
    }

    impl RawChunkedReader {
        fn new(
            chunks: impl IntoIterator<Item = Vec<u8>>,
            err_at_end: Option<std::io::Error>,
        ) -> Self {
            Self {
                chunks: chunks.into_iter().collect(),
                err_at_end,
            }
        }
    }

    impl tokio::io::AsyncRead for RawChunkedReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if let Some(chunk) = self.chunks.pop_front() {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..n]);
                if n < chunk.len() {
                    self.chunks.push_front(chunk[n..].to_vec());
                }
                std::task::Poll::Ready(Ok(()))
            } else if let Some(err) = self.err_at_end.take() {
                std::task::Poll::Ready(Err(err))
            } else {
                std::task::Poll::Ready(Ok(())) // 0 bytes filled == EOF
            }
        }
    }

    /// Drive [`pump_raw_bytes`] over `reader` under the default unbounded policy,
    /// returning `(captured_bytes, recorded_read_error)`.
    async fn drive_pump_raw_bytes(reader: RawChunkedReader) -> (Vec<u8>, Option<std::io::Error>) {
        let out_buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let signals = RawStdoutSignals {
            seen: Arc::new(AtomicUsize::new(0)),
            overflowed: Arc::new(AtomicBool::new(false)),
            truncated: Arc::new(AtomicBool::new(false)),
            read_error: Arc::new(std::sync::Mutex::new(None)),
        };
        pump_raw_bytes(
            reader,
            out_buf.clone(),
            None,
            OverflowMode::DropOldest,
            signals.clone(),
            Arc::new(OutputActivity::new(tokio::time::Instant::now())),
        )
        .await;
        let bytes = std::mem::take(&mut *out_buf.lock().unwrap());
        let err = signals.read_error.lock().unwrap().take();
        (bytes, err)
    }

    #[tokio::test]
    async fn pump_raw_bytes_records_a_mid_stream_error_and_keeps_the_prefix() {
        let (bytes, err) = drive_pump_raw_bytes(RawChunkedReader::new(
            [b"partial".to_vec()],
            Some(std::io::Error::other("boom")),
        ))
        .await;
        assert_eq!(
            bytes, b"partial",
            "the prefix read before the error is kept"
        );
        assert!(
            err.is_some(),
            "the raw stdout OS read error is recorded for output_bytes to surface as ErrorReason::Io"
        );
    }

    #[tokio::test]
    async fn pump_raw_bytes_clean_eof_records_no_error() {
        let (bytes, err) = drive_pump_raw_bytes(RawChunkedReader::new(
            [b"all".to_vec(), b"good".to_vec()],
            None,
        ))
        .await;
        assert_eq!(bytes, b"allgood");
        assert!(err.is_none(), "a clean EOF is a complete capture");
    }

    #[tokio::test]
    async fn pump_raw_bytes_treats_a_broken_pipe_read_as_clean_eof() {
        let (bytes, err) = drive_pump_raw_bytes(RawChunkedReader::new(
            [b"done".to_vec()],
            Some(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        ))
        .await;
        assert_eq!(bytes, b"done", "the prefix is kept");
        assert!(
            err.is_none(),
            "a broken-pipe read is the normal writer-closed end, not an incomplete capture"
        );
    }

    // --- T-087: consuming finishers surface a recorded read error -----------

    /// The capturing line finisher (`output_string`, via `finish_lines`) surfaces
    /// a recorded stdout read error as `ErrorReason::Io` rather than a silently-short
    /// `Ok(ProcessResult)`. The sink stands in for one a pump populated (the pump
    /// seam is covered in `pump.rs`); a clean-EOF sink carries no error, so a
    /// normal run is unaffected — the other tests here exercise that path.
    #[tokio::test]
    async fn output_string_surfaces_a_recorded_read_error_as_io() {
        let mut run = scripted_handle(&[0]).await; // Reply::ok("") -> empty, exit 0
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.set_read_error(std::io::Error::other("stdout read boom"));
        run.stdout_sink = Some(sink);
        match run.output_string().await.map_err(|e| e.into_reason()) {
            Err(ErrorReason::Io(e)) => assert_eq!(e.to_string(), "stdout read boom"),
            other => panic!("expected Err(Io) for an incomplete capture, got {other:?}"),
        }
    }

    /// The discard finisher (`wait`, also via `finish_lines`) likewise classifies
    /// an incomplete stderr capture as `ErrorReason::Io`, not a silent success.
    #[tokio::test]
    async fn wait_surfaces_a_recorded_read_error_as_io() {
        let mut run = scripted_handle(&[0]).await;
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.set_read_error(std::io::Error::other("stderr read boom"));
        run.stderr_sink = Some(sink);
        match run.wait().await.map_err(|e| e.into_reason()) {
            Err(ErrorReason::Io(e)) => assert_eq!(e.to_string(), "stderr read boom"),
            other => panic!("expected Err(Io) for an incomplete capture, got {other:?}"),
        }
    }
}

/// T-090: the `profile` sampler must fold only readings taken against the child's
/// own identity. The fold logic and the sampler loop are exercised with a
/// substitutable metrics `source`, reproducing PID reuse in the sampler window
/// deterministically — no real OS process, no reliance on a live child's timing.
#[cfg(all(test, feature = "stats"))]
mod profile_sampler_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{ProfileAcc, run_profile_sampler};
    use crate::sys::ProcMetrics;

    fn metrics(cpu_ms: u64, mem: u64) -> ProcMetrics {
        ProcMetrics {
            cpu_time: Some(Duration::from_millis(cpu_ms)),
            peak_memory_bytes: Some(mem),
        }
    }

    #[test]
    fn fold_ignores_all_none_readings() {
        // The shape `process_metrics` returns for a recycled or gone pid: it counts
        // as a tick but contributes no CPU/memory, so it can never overwrite a real
        // reading nor reset the running peak.
        let mut acc = ProfileAcc::default();
        acc.fold(metrics(100, 8192)); // a real reading
        acc.fold(ProcMetrics::default()); // a recycled-pid / gone reading
        acc.fold(metrics(200, 4096)); // a later real reading
        assert_eq!(acc.samples, 3, "every tick is counted, even empty ones");
        assert_eq!(
            acc.cpu_time,
            Some(Duration::from_millis(200)),
            "CPU tracks the latest real reading, not the empty one"
        );
        assert_eq!(
            acc.peak_memory_bytes,
            Some(8192),
            "peak is the max across real readings; an empty reading never lowers it"
        );
    }

    /// The sampler folds identity-matched readings and drops the stranger's default
    /// after the pid is "recycled". Under `start_paused` the runtime auto-advances
    /// the clock while the sampler awaits its interval, so this is deterministic:
    /// the fake `source` returns two real readings, then all-`None` defaults (what
    /// the identity gate yields once the pid is reused), and latches `reaped` after
    /// enough ticks so the loop terminates.
    #[tokio::test(start_paused = true)]
    async fn sampler_folds_only_identity_matched_readings() {
        let reaped = Arc::new(AtomicBool::new(false));
        let acc = Arc::new(std::sync::Mutex::new(ProfileAcc::default()));
        let calls = Arc::new(AtomicUsize::new(0));

        let reaped_src = Arc::clone(&reaped);
        let calls_src = Arc::clone(&calls);
        let source = move || {
            let n = calls_src.fetch_add(1, Ordering::Relaxed);
            // Latch reaped after several ticks so the loop breaks (the sampler's
            // post-read reaped check stops before folding this call).
            if n >= 4 {
                reaped_src.store(true, Ordering::Release);
            }
            match n {
                0 => metrics(100, 8192), // real: identity matches
                1 => metrics(200, 4096), // real: identity matches
                // pid recycled → identity mismatch → process_metrics default
                _ => ProcMetrics::default(),
            }
        };

        run_profile_sampler(
            Duration::from_millis(5),
            Arc::clone(&reaped),
            Arc::clone(&acc),
            source,
        )
        .await;

        let acc = acc.lock().expect("acc mutex");
        assert_eq!(
            acc.cpu_time,
            Some(Duration::from_millis(200)),
            "the last identity-matched CPU reading is kept; the stranger's default is ignored"
        );
        assert_eq!(
            acc.peak_memory_bytes,
            Some(8192),
            "peak reflects only identity-matched readings — the recycled pid never enters it"
        );
        assert!(
            acc.samples >= 2,
            "at least the two real readings were sampled"
        );
    }
}
