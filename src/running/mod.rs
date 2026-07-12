//! [`RunningProcess`] — a live handle to a spawned child.
//!
//! Split by concern: this file owns the handle's state and the consuming
//! capture paths (exit driving, kill/teardown, the post-exit checkpoint);
//! [`probes`] holds the non-consuming readiness probes; [`stream`] holds the
//! incremental stdout streaming surface.

mod deadline;
mod probes;
mod scripted;
mod stream;

pub use stream::{Finished, OutputEvent, OutputEvents, OutputLine, StdoutLines};
// Re-exported so `crate::doubles`/`crate::cassette` keep addressing these at
// `crate::running::...` even though they now live in the `scripted` submodule.
pub(crate) use scripted::{ScriptedProc, ScriptedResultInfo, split_pump_lines};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;

use crate::buffer::{OutputBufferPolicy, OverflowMode, clamp_dropoldest_tail, push_capped_bytes};
use crate::error::Error;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::pump::{SharedLines, StreamConfig, pump_lines_core};
use crate::result::{Outcome, ProcessResult};
use crate::stdin::ProcessStdin;
use crate::sys::pid_gate::PidGate;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

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
const TS_PENDING: u8 = 0;
const TS_EXITED: u8 = 1;
// `pub(crate)` so `first_line` (in `crate::runner`) can classify a timed-out
// streamed run: the deadline watchdog stores `TS_TIMED_OUT` *before* it kills, so
// reading it after the stream closes distinguishes a deadline kill from a natural
// end race-free.
pub(crate) const TS_TIMED_OUT: u8 = 2;

/// Why a reap-via-wait ended — the race result, not a post-hoc token read.
enum ExitCause {
    /// Child exited on its own (or deadline fired). Cancellation did not win.
    Exited(Outcome),
    /// Cancel arm won: token fired, tree killed. Becomes `Err(Cancelled)`.
    Cancelled,
}

/// Internal result of `finish_lines` — distinct from the public `Finished`.
struct FinishedLines {
    outcome: Outcome,
    stdout_lines: Vec<String>,
    stderr_lines: Vec<String>,
}

/// How [`RunningProcess::finish_lines`] treats the pumped lines.
#[derive(Clone, Copy)]
enum CaptureMode {
    /// Retain both streams' lines (`output_string`).
    Lines,
    /// Pump (so the child never blocks on a full pipe) but drop the lines.
    Discard,
}

/// The fields produced by a spawn, handed to [`RunningProcess::from_spawned`].
pub(crate) struct Spawned {
    pub program: String,
    pub child: Child,
    pub own_group: Option<ProcessGroup>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
    pub stdin: Option<ChildStdin>,
    pub stdin_task: Option<JoinHandle<std::io::Result<()>>>,
    pub timeout: Option<Duration>,
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
    pub cancel_token: Option<tokio_util::sync::CancellationToken>,
}

/// A handle to a process spawned by a runner.
pub struct RunningProcess {
    // The Option fields below encode the handle's de-facto states (fresh /
    // streaming / consumed) implicitly. No runtime state enum on purpose:
    // consuming verbs take `self` by value (double consumption is a compile
    // error), and the two &mut entry points handle a repeat call without
    // panicking — `stdout_lines`/`output_events` return a loud `Err`, and
    // `take_stdin` returns `None`. A state enum would only add panic paths to
    // guard doors the borrow checker already locks.
    program: String,
    /// The I/O-bearing half: a real OS child, or a scripted double feeding the
    /// same pump machinery (see [`Backend`]).
    backend: Backend,
    timeout: Option<Duration>,
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
    // Joined before the overflow check so the last lines are visible.
    stdout_pump: Option<JoinHandle<()>>,
    stderr_pump: Option<JoinHandle<()>>,
    // Non-broken-pipe stdin failure stashed by `observe_stdin_task`; surfaced as
    // `Error::Stdin` by `checked_outcome` only when the run otherwise succeeded.
    stdin_error: Option<std::io::Error>,
    // Bulk capture verbs fail loudly on non-piped stdout rather than returning empty.
    stdout_piped: bool,
    // Streaming deadline watchdog; aborted on drop.
    deadline_task: Option<JoinHandle<()>>,
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
}

/// A boxed output reader: real `ChildStdout`/`ChildStderr` or scripted bytes.
/// Both flow through the same pump machinery via `AsyncRead`.
type OutputReader = Box<dyn tokio::io::AsyncRead + Send + Unpin>;

/// The I/O-bearing half of a [`RunningProcess`]: a real OS child or a scripted
/// double that feeds canned bytes through the same pumps/sinks. Platform code
/// only ever constructs `Real`.
enum Backend {
    // Boxed: both variants are large and the enum lives in every handle.
    Real(Box<RealProc>),
    Scripted(Box<ScriptedProc>),
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
    stdout_pipe: Option<ChildStdout>,
    stderr_pipe: Option<ChildStderr>,
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
        }
    }

    fn scripted_kill(&self) -> Option<scripted::ScriptedKill> {
        match self {
            Backend::Real(_) => None,
            Backend::Scripted(s) => Some(s.kill_handle()),
        }
    }

    fn take_stdout_reader(&mut self) -> Option<OutputReader> {
        match self {
            Backend::Real(real) => real.stdout_pipe.take().map(|p| Box::new(p) as OutputReader),
            Backend::Scripted(s) => s.take_stdout_reader(),
        }
    }

    fn take_stderr_reader(&mut self) -> Option<OutputReader> {
        match self {
            Backend::Real(real) => real.stderr_pipe.take().map(|p| Box::new(p) as OutputReader),
            Backend::Scripted(s) => s.take_stderr_reader(),
        }
    }
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
            stdout_pump: None,
            stderr_pump: None,
            stdin_error: None,
            stdout_piped: s.stdout_piped,
            deadline_task: None,
            timeout_state: Arc::new(AtomicU8::new(TS_PENDING)),
            pid_gate: Arc::new(PidGate::new(s.pid)),
            cancel_token: s.cancel_token,
            cancel_task: None,
            cancel_at_exit: None,
            started: Instant::now(),
            // Captured next to `started` so the two anchors agree at spawn; they
            // diverge only later, under a paused runtime, where `deadline_anchor`
            // tracks tokio's virtual clock and `started` the real one.
            deadline_anchor: tokio::time::Instant::now(),
            start_time: SystemTime::now(),
            scripted_result: None,
        }
    }

    pub(crate) fn attach_group(&mut self, group: ProcessGroup) {
        if let Backend::Real(real) = &mut self.backend {
            real.own_group = Some(Arc::new(group));
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
    pub(crate) fn arm_cancel_watchdog(&mut self) {
        {
            if let Some(old) = self.cancel_task.take() {
                old.abort();
            }
            let Some(token) = self.cancel_token.clone() else {
                return;
            };
            let group_weak = self.backend.own_group().map(Arc::downgrade);
            let gate = self.pid_gate.clone();
            self.cancel_task = Some(tokio::spawn(async move {
                token.cancelled().await;
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
                if let Some(g) = group_weak.and_then(|w| w.upgrade()) {
                    // On Linux + legacy/restricted cgroup this can synchronously
                    // block this worker thread up to ~100ms — accepted, not
                    // routed through `spawn_blocking`; see the sweep loop in
                    // `Cgroup::kill` (src/sys/linux.rs) for the full rationale.
                    let _ = g.kill_all();
                }
                crate::sys::pid_gate::force_kill(&gate);
            }));
        }
    }

    /// Take the raw stdout pipe for `Pipeline` plumbing. `None` for a scripted
    /// backend (scripted doubles don't compose into real pipelines).
    pub(crate) fn take_stdout_pipe(&mut self) -> Option<ChildStdout> {
        match &mut self.backend {
            Backend::Real(real) => real.stdout_pipe.take(),
            Backend::Scripted(_) => None,
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

    /// Lines read from stdout so far (counts every line, even ones dropped by an
    /// [`OutputBufferPolicy`]). Live only once stdout is being pumped.
    pub fn stdout_line_count(&self) -> usize {
        self.stdout_sink.as_ref().map_or(0, |s| s.count())
    }

    /// Lines read from stderr so far (see [`stdout_line_count`](Self::stdout_line_count)).
    pub fn stderr_line_count(&self) -> usize {
        self.stderr_sink.as_ref().map_or(0, |s| s.count())
    }

    /// Take the interactive stdin writer, if the command was built with
    /// [`keep_stdin_open`](crate::Command::keep_stdin_open). Returns `None` after
    /// the first call (or when stdin was not kept open).
    pub fn take_stdin(&mut self) -> Option<ProcessStdin> {
        match &mut self.backend {
            Backend::Real(real) => real.stdin_pipe.take().map(ProcessStdin::new),
            // Scripted doubles don't model interactive stdin yet; `None` matches
            // the "stdin wasn't kept open" contract.
            Backend::Scripted(_) => None,
        }
    }

    /// Whether **dropping** this handle will tear down (hard-kill) the process
    /// tree.
    ///
    /// `true` — owns a **private** process group; drop hard-kills the whole tree.
    /// `false` — runs inside a **shared** [`ProcessGroup`](crate::ProcessGroup)
    /// whose lifetime the group owns (drop does *not* kill the tree), or a
    /// scripted test double (no OS tree).
    pub fn kills_tree_on_drop(&self) -> bool {
        self.backend.own_group().is_some()
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

    /// Fail loud if streaming is not possible: (a) stdout not piped, or
    /// (b) a prior streaming verb already consumed stdout on this handle.
    fn ensure_stdout_streamable(&self) -> Result<()> {
        self.ensure_stdout_capturable()?; // (a) non-piped stdout
        if self.stdout_sink.is_some() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "`{}`: stdout was already consumed by an earlier stdout_lines/output_events \
                     call — stream it once (a second call would yield an empty stream)",
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
    /// - [`Error::Cancelled`] — the run was cancelled via
    ///   [`Command::cancel_on`](crate::Command::cancel_on). Unlike a timeout,
    ///   cancellation is *always* raised (and discards any captured output).
    /// - [`Error::OutputTooLarge`] — the
    ///   [`OutputBufferPolicy`](crate::OutputBufferPolicy) is fail-loud
    ///   ([`OverflowMode::Error`](crate::OverflowMode)) and the captured output
    ///   exceeded its line or byte ceiling.
    /// - [`Error::Stdin`] — a configured stdin source failed for a reason other
    ///   than a broken pipe, on an *otherwise-successful* run.
    /// - [`Error::Io`] — stdout is not piped, a prior streaming call already
    ///   consumed it as decoded lines, or waiting on the child failed.
    pub async fn output_string(mut self) -> Result<ProcessResult<String>> {
        let finished = self
            .finish_lines(CaptureMode::Lines, /* expose_counts */ true, || {})
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
        Ok(ProcessResult::new(
            self.program.clone(),
            finished.stdout_lines.join("\n"),
            finished.stderr_lines.join("\n"),
            finished.outcome,
            self.timeout,
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
    /// [`Error::Cancelled`] and no bytes — cancellation via
    /// [`Command::cancel_on`](crate::Command::cancel_on) is always terminal.
    ///
    /// A byte ceiling on the [`OutputBufferPolicy`] bounds the raw stdout capture
    /// (its `max_lines` does not — raw bytes have no lines): with
    /// [`OverflowMode::Error`](crate::OverflowMode) a flood past the cap errors
    /// with [`Error::OutputTooLarge`], while the drop modes keep a bounded
    /// head/tail and set [`ProcessResult::truncated`]. With no byte cap the
    /// capture is unbounded — bound a flooding child with
    /// [`with_max_bytes`](crate::OutputBufferPolicy::with_max_bytes) or a
    /// [`timeout`](crate::Command::timeout).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io(InvalidInput)`](std::io::ErrorKind::InvalidInput) if
    /// stdout is not piped, or if a prior streaming call already consumed stdout
    /// as decoded lines (the raw bytes cannot be reconstructed). Returns
    /// [`Error::OutputTooLarge`] if the byte ceiling is set to
    /// [`OverflowMode::Error`](crate::OverflowMode) and the raw stdout exceeds it.
    /// (A cancelled run is [`Error::Cancelled`]; a non-zero exit, a timeout, or a
    /// signal-kill is *captured* in the returned [`ProcessResult`]'s
    /// [`outcome`](ProcessResult::outcome), not raised.)
    ///
    /// # Panics
    ///
    /// Panics if the internal raw-stdout capture buffer's mutex is poisoned —
    /// which happens only if a pump task previously panicked while holding it (a
    /// crate bug), never from any caller input.
    pub async fn output_bytes(mut self) -> Result<ProcessResult<Vec<u8>>> {
        self.ensure_stdout_capturable()?;
        if self.stdout_sink.is_some() || self.stderr_sink.is_some() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "`{}`: output_bytes cannot follow a streaming call (stdout was already \
                     consumed as lines) — use output_string to collect the streamed lines, or \
                     call output_bytes without streaming first",
                    self.program
                ),
            )));
        }
        let stderr_sink = SharedLines::new(&self.buffer);
        self.stderr_pump = self.backend.take_stderr_reader().map(|pipe| {
            tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_config.clone(),
                stderr_sink.clone(),
            ))
        });
        self.stderr_sink = Some(stderr_sink.clone());

        // Read stdout raw, concurrently, so it never blocks the child. Bytes
        // accumulate in a shared buffer (not the task's return value) so the
        // bounded teardown below can salvage a partial read. Stored on `self` (not
        // a frame-local) so a `drive_to_exit` error aborts it via `Drop` instead
        // of leaving it to grow `out_buf` unboundedly on a shared-group handle.
        //
        // Honor the byte ceiling (`max_bytes`) on the raw stdout capture so a
        // caller that set `with_max_bytes(..)` / `fail_loud(..).with_max_bytes(..)`
        // is bounded rather than OOM'd by a flooding child; `max_lines` does not
        // apply to a non-line stream. `None` cap keeps the exact old (unbounded)
        // behavior. The signals are shared (not the task's return value) so the
        // bounded teardown below can read them even if it has to abort the task.
        let stdout_cap = self.buffer.max_bytes;
        let stdout_mode = self.buffer.overflow;
        // Shared signals the raw drain writes and the bounded teardown reads even if
        // it has to abort the task — including the first non-broken-pipe OS read
        // error, so an incomplete byte capture surfaces as `Error::Io` below rather
        // than a silently-truncated `Ok(ProcessResult)` prefix.
        let signals = RawStdoutSignals {
            seen: Arc::new(AtomicUsize::new(0)),
            overflowed: Arc::new(AtomicBool::new(false)),
            truncated: Arc::new(AtomicBool::new(false)),
            read_error: Arc::new(std::sync::Mutex::new(None)),
        };
        let stdout_pipe = self.backend.take_stdout_reader();
        let out_buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        self.stdout_pump = stdout_pipe.map(|pipe| {
            tokio::spawn(pump_raw_bytes(
                pipe,
                out_buf.clone(),
                stdout_cap,
                stdout_mode,
                signals.clone(),
            ))
        });

        let outcome = self.drive_to_exit().await?;
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
        let outcome = self.checked_outcome(outcome)?;

        // A raw-stdout fail-loud (Error mode) byte overflow surfaces first, like
        // the stderr line ceiling below. Raw stdout has no lines, so report only
        // the byte ceiling that actually fired (`max_lines: None`).
        if signals.overflowed.load(Ordering::Relaxed) {
            return Err(crate::Error::OutputTooLarge {
                program: self.program.clone(),
                max_lines: None,
                max_bytes: self.buffer.max_bytes,
                total_lines: 0,
                total_bytes: signals.seen.load(Ordering::Relaxed),
            });
        }
        if stderr_sink.overflowed() {
            return Err(crate::Error::OutputTooLarge {
                program: self.program.clone(),
                max_lines: self.buffer.max_lines,
                max_bytes: self.buffer.max_bytes,
                total_lines: stderr_sink.count(),
                total_bytes: stderr_sink.seen_bytes(),
            });
        }

        // An incomplete capture from a first OS read error on either stream
        // surfaces as `Error::Io` — a short raw-stdout prefix (or a truncated
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
            return Err(Error::Io(source));
        }
        if let Some(source) = stderr_sink.take_read_error() {
            return Err(Error::Io(source));
        }

        let stderr_lines = stderr_sink.drain();
        let truncated = signals.truncated.load(Ordering::Relaxed) || stderr_sink.dropped() > 0;
        let duration = self.started.elapsed();
        Ok(ProcessResult::new(
            self.program.clone(),
            stdout,
            stderr_lines.join("\n"),
            outcome,
            self.timeout,
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
    /// `Error::Cancelled`.
    ///
    /// # Errors
    ///
    /// A timeout or signal-kill is *captured* in the returned [`Outcome`], not
    /// raised. The `Err` cases are [`Error::Cancelled`] (the run was cancelled
    /// via [`Command::cancel_on`](crate::Command::cancel_on) — always raised),
    /// [`Error::Stdin`] (a non-broken-pipe stdin-source failure on an
    /// otherwise-successful run), or [`Error::Io`] (waiting on the child failed).
    pub async fn wait(mut self) -> Result<Outcome> {
        Ok(self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, || {})
            .await?
            .outcome)
    }

    /// Gracefully stop the process tree: `SIGTERM`, wait up to `grace`, then
    /// `SIGKILL` any survivor. On Windows the kill is atomic and `grace` is not
    /// awaited.
    ///
    /// Only an **own-group** handle can be shut down here — a **shared-group**
    /// handle returns [`Error::Unsupported`](crate::Error::Unsupported) because
    /// shutting it down would tear down the caller's other children too.
    ///
    /// If the configured timeout deadline already elapsed when `shutdown` is
    /// called the run is classified as `Outcome::TimedOut`.
    ///
    /// # Errors
    ///
    /// - [`Error::Unsupported`] — this is a **shared-group** handle, which does
    ///   not own its group (tearing it down would kill the caller's other
    ///   children); use [`ProcessGroup::shutdown`](crate::ProcessGroup::shutdown)
    ///   or [`start_kill`](Self::start_kill) instead.
    /// - [`Error::Cancelled`] — the run was cancelled via
    ///   [`Command::cancel_on`](crate::Command::cancel_on).
    /// - [`Error::Stdin`] — a non-broken-pipe stdin-source failure on an
    ///   otherwise-successful run.
    /// - [`Error::Io`] — the graceful teardown or the exit wait failed.
    ///
    /// A timeout or signal-kill is *captured* in the returned [`Outcome`], not
    /// raised.
    pub async fn shutdown(mut self, grace: std::time::Duration) -> Result<Outcome> {
        let Some(group) = self.backend.own_group().cloned() else {
            return Err(Error::Unsupported {
                operation: "shutdown (a shared-group handle does not own its group — \
                            use ProcessGroup::shutdown, or start_kill for just this child)"
                    .into(),
            });
        };
        // Disable the concurrent `wait()`'s deadline arm to avoid two overlapping
        // graceful teardowns. A timeout that already elapsed still classifies
        // as `TimedOut` — claim the arbiter before nulling `self.timeout`.
        // Measured off `deadline_anchor` (tokio's clock), not `started`, so this
        // "already elapsed?" check agrees with `wait_deadline_and_claim` under a
        // paused runtime instead of reading the real clock the deadline never slept on.
        if let Some(limit) = self.timeout
            && self.deadline_anchor.elapsed() >= limit
        {
            let _ = self.timeout_state.compare_exchange(
                TS_PENDING,
                TS_TIMED_OUT,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }
        self.timeout = None;
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        // Reap concurrently: an unreaped zombie still answers `kill(pgid, 0)`
        // probes, so without a concurrent reap a SIGTERM-handling child would
        // look alive for the whole grace and eat a pointless SIGKILL.
        let (term_result, outcome) = tokio::join!(
            group.graceful_terminate(grace, crate::sys::SIGTERM_RAW),
            self.wait(),
        );
        term_result?;
        outcome
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
            let token = self.cancel_token.clone();
            let cancelled = async {
                match &token {
                    Some(token) => token.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                biased; // cancel arm first: a cancel that fires mid-wait wins
                () = cancelled => {
                    self.kill_tree().await;
                    ExitCause::Cancelled
                }
                outcome = self.backend_wait() => ExitCause::Exited(outcome?),
            }
        };
        let outcome = self.on_reaped(cause);
        self.observe_stdin_task().await;
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
    /// outcome, not raised. The `Err` cases are [`Error::Cancelled`] (cancelled
    /// via [`Command::cancel_on`](crate::Command::cancel_on)), [`Error::Stdin`]
    /// (a non-broken-pipe stdin-source failure on an otherwise-successful run),
    /// or [`Error::Io`] (waiting on the child failed).
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
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, || {
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
        on_exit: impl FnOnce(),
    ) -> Result<FinishedLines> {
        // The capturing path needs a piped stdout; fail loudly rather than return
        // empty. The discard path (wait/profile) reads nothing, so it is exempt.
        if matches!(capture, CaptureMode::Lines) {
            self.ensure_stdout_capturable()?;
        }
        // Reuse a sink already populated by a prior streaming call so that
        // output_string after stdout_lines/output_events sees those lines rather
        // than returning empty. For the discard path use a retain-nothing sink
        // (not the user's policy) so a chatty child never accumulates O(total)
        // heap in wait/profile. The byte cap bounds the pump's in-flight line
        // assembly too — `bounded(0)` alone retains no lines but would still let
        // a newline-free flood grow the in-flight buffer without limit.
        let discard_policy = discard_sink_policy();
        let sink_policy: &OutputBufferPolicy = match capture {
            CaptureMode::Discard => &discard_policy,
            CaptureMode::Lines => &self.buffer,
        };
        let stdout_sink = self
            .stdout_sink
            .clone()
            .unwrap_or_else(|| SharedLines::new(sink_policy));
        let stderr_sink = self
            .stderr_sink
            .clone()
            .unwrap_or_else(|| SharedLines::new(sink_policy));
        // The discard verbs must never accumulate a user-policy backlog. A sink
        // adopted from a *dropped* stream is still in the caller's
        // `OutputBufferPolicy` (possibly unbounded); switch it to retain-nothing
        // *before* `drive_to_exit` so a chatty child can't grow O(total) heap
        // while we wait for it to exit. A freshly built sink already uses
        // `discard_sink_policy`, so this is a no-op there. The capture path
        // (`output_string`) leaves the sink untouched so it can still hand back
        // the streamed tail.
        if matches!(capture, CaptureMode::Discard) {
            stdout_sink.start_discarding();
            stderr_sink.start_discarding();
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
        on_exit();
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
        let outcome = self.checked_outcome(outcome)?;

        if matches!(capture, CaptureMode::Lines) {
            for sink in [&stdout_sink, &stderr_sink] {
                if sink.overflowed() {
                    return Err(crate::Error::OutputTooLarge {
                        program: self.program.clone(),
                        max_lines: self.buffer.max_lines,
                        max_bytes: self.buffer.max_bytes,
                        total_lines: sink.count(),
                        total_bytes: sink.seen_bytes(),
                    });
                }
            }
        }

        // A first OS read error on either pipe means the capture is incomplete:
        // surface it as `Error::Io` for the capturing (`output_string`) and the
        // discard (`wait`/`profile`) paths alike, rather than reporting a
        // silently-short read as a full success. Checked after the fail-loud
        // overflow ceiling (the more specific signal if both fire) and after
        // `checked_outcome` (so cancellation/stdin priority is preserved); a
        // broken-pipe read was already folded into a clean EOF by the pump, so a
        // normal writer-closed stream never trips this.
        for sink in [&stdout_sink, &stderr_sink] {
            if let Some(source) = sink.take_read_error() {
                return Err(Error::Io(source));
            }
        }

        let (stdout_lines, stderr_lines) = match capture {
            CaptureMode::Lines => (stdout_sink.drain(), stderr_sink.drain()),
            CaptureMode::Discard => (Vec::new(), Vec::new()),
        };
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

    /// Post-exit checkpoint every consuming path passes after pumps settle:
    /// cancellation always wins (returns `Err(Cancelled)`), then a non-broken-
    /// pipe stdin failure surfaces as `Err(Stdin)` only on an otherwise-
    /// successful run.
    fn checked_outcome(&mut self, outcome: Outcome) -> Result<Outcome> {
        // Pre-pump snapshot: prevents a cancel firing during `join_pumps` from
        // discarding real output. `unwrap_or(false)` — `None` is not yet
        // snapshotted; treat conservatively as "not cancelled".
        if self.cancel_at_exit.unwrap_or(false) {
            return Err(Error::Cancelled {
                program: self.program.clone(),
            });
        }
        let succeeded = matches!(outcome, Outcome::Exited(code) if self.ok_codes.contains(&code));
        if succeeded && let Some(source) = self.stdin_error.take() {
            return Err(Error::Stdin {
                program: self.program.clone(),
                source,
            });
        }
        Ok(outcome)
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
            Backend::Scripted(_) => None,
        };
        let Some(task) = task else {
            return;
        };
        if !task.is_finished() {
            // Not done yet — re-park for the post-pump `finalize_stdin_task`, so
            // a writer that fails during `join_pumps` is not silently lost.
            if let Backend::Real(real) = &mut self.backend {
                real.stdin_task = Some(task);
            }
            return;
        }
        let observed = Self::classify_stdin_join(task.await);
        self.record_stdin_error(observed);
    }

    /// Final stdin-writer observation, run after `join_pumps` and before
    /// `checked_outcome` in every pump-draining consuming path. The pre-pump
    /// [`observe_stdin_task`](Self::observe_stdin_task) only peeks
    /// non-blockingly, so a writer that failed with a non-broken-pipe error
    /// *inside* the `join_pumps` window (up to [`PUMP_TEARDOWN`]) — e.g. a
    /// `from_reader`/`from_file` source that erred while the pumps were still
    /// draining the child's output — was re-parked and would otherwise never
    /// reach `self.stdin_error`, letting an otherwise-successful run report a
    /// silent success (exactly the case `Error::Stdin` exists to diagnose).
    ///
    /// This waits for that writer, but only *bounded* by [`PUMP_TEARDOWN`]: a
    /// writer still blocked on a genuinely hung source is aborted and left
    /// unreported rather than stalling the caller forever — the same "never wait
    /// on a hung writer" contract the pre-fix single peek kept. In the common
    /// case the writer already finished (the pre-pump peek took it, or it wraps
    /// up during pump teardown), so the timeout resolves immediately.
    async fn finalize_stdin_task(&mut self) {
        let task = match &mut self.backend {
            Backend::Real(real) => real.stdin_task.take(),
            Backend::Scripted(_) => None,
        };
        let Some(task) = task else {
            return;
        };
        let abort = task.abort_handle();
        let observed = match tokio::time::timeout(PUMP_TEARDOWN, task).await {
            Ok(joined) => Self::classify_stdin_join(joined),
            // Still writing after the teardown grace — a hung source. Abort it
            // (a dropped `timeout` future only detaches the handle) and move on
            // unreported, never blocking the caller unboundedly.
            Err(_elapsed) => {
                abort.abort();
                None
            }
        };
        self.record_stdin_error(observed);
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
        };
        self.classify_timed_out(outcome)
    }

    /// Wait for the child to exit, applying the timeout (killing the tree on
    /// elapse). Returns the [`Outcome`] of the run.
    async fn drive_to_exit(&mut self) -> Result<Outcome> {
        // Close an untaken `keep_stdin_open` pipe so a stdin-reading child sees
        // EOF instead of blocking to its timeout.
        if let Backend::Real(real) = &mut self.backend {
            drop(real.stdin_pipe.take());
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
        let outcome = self.on_reaped(cause);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            program = %self.program,
            outcome = ?outcome,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "process exited"
        );
        Ok(outcome)
    }

    /// A fired deadline overrides whatever `backend_wait` observed — a child that
    /// exits cleanly within the grace still timed out. Cancellation is classified
    /// later in `checked_outcome` and always wins over `TimedOut`.
    fn classify_timed_out(&self, outcome: Outcome) -> Outcome {
        if self.timeout_state.load(Ordering::Acquire) == TS_TIMED_OUT {
            Outcome::TimedOut
        } else {
            outcome
        }
    }

    /// Raw exit wait — no timeout/cancel. Real: maps exit status to `Outcome`
    /// (captures Unix signal number when available). Scripted: resolves at the
    /// canned `exit_at`, or immediately as `Signalled` if killed.
    async fn backend_wait(&mut self) -> Result<Outcome> {
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
                    .map_err(Error::Io)?;
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
        let _ = self.timeout_state.compare_exchange(
            TS_PENDING,
            TS_EXITED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        Ok(outcome)
    }

    /// Race the cancel token against the deadline-bounded wait. Unset knobs
    /// become never-resolving arms. `biased` with cancel first so a simultaneous
    /// cancel+deadline always hard-kills rather than routing through the graceful
    /// teardown tier.
    async fn drive_to_exit_inner(&mut self) -> Result<ExitCause> {
        // Reclaim teardown from the streaming deadline watchdog before reaping.
        // This future owns the `Child` and drives BOTH kills through it — the
        // deadline via `teardown_on_timeout` and cancel via `kill_tree`, whose
        // `start_kill` is a no-op once the child is reaped and so can never signal
        // a recycled pid. `retire` the gate FIRST (so a watchdog racing us stands
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
        // Own the knobs so the helper futures borrow nothing from `self` —
        // only `self.backend_wait()` does, keeping the select! borrows disjoint.
        let limit = self.timeout;
        let token = self.cancel_token.clone();
        // The deadline anchor is on tokio's clock (see the field docs) so the
        // `limit - started.elapsed()` in `wait_deadline_and_claim` counts virtual
        // time already burned before this consuming call armed the deadline.
        let started = self.deadline_anchor;
        let cancelled = async {
            match &token {
                Some(token) => token.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
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
        tokio::select! {
            biased; // cancel arm checked first: always beats a simultaneous deadline
            () = cancelled => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: "processkit",
                    program = %self.program,
                    "cancellation fired; killing the tree"
                );
                self.kill_tree().await;
                Ok(ExitCause::Cancelled)
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
                self.teardown_on_timeout().await;
                Ok(ExitCause::Exited(Outcome::TimedOut))
            }
        }
    }

    /// Hard-kill the child and its tree (for a private group), then reap.
    async fn kill_tree(&mut self) {
        let gate = self.pid_gate.clone();
        match &mut self.backend {
            Backend::Real(real) => {
                let _ = real.child_mut().start_kill();
                // The child is being torn down through the owned `Child`; retire
                // the gate (before the reap below frees the pid) so the
                // cancel/deadline watchdogs stand down rather than racing that reap
                // with a raw `kill(pid)` that could land on a recycled pid.
                gate.retire();
                if let Some(group) = &real.own_group {
                    // On Linux + legacy/restricted cgroup this can synchronously
                    // block this worker thread up to ~100ms — accepted, not
                    // routed through `spawn_blocking`; see the sweep loop in
                    // `Cgroup::kill` (src/sys/linux.rs) for the full rationale.
                    let _ = group.kill_all();
                }
                // Bound the reap: a D-state child can ignore SIGKILL until I/O
                // unblocks, and an unbounded wait hangs shared-group handles.
                let _ = tokio::time::timeout(PUMP_TEARDOWN, real.child_mut().wait()).await;
            }
            Backend::Scripted(s) => s.kill(),
        }
    }

    /// Teardown when the deadline elapses. With `timeout_grace`: signal → wait up
    /// to grace → SIGKILL, so a signal-handling child ends the grace early. Without
    /// grace: hard `kill_tree`. Windows has no signal tier; graceful degrades to
    /// the atomic kill.
    async fn teardown_on_timeout(&mut self) {
        let Some(grace) = self.timeout_grace else {
            self.kill_tree().await;
            return;
        };
        let signal = self.timeout_signal;
        let gate = self.pid_gate.clone();
        match &mut self.backend {
            Backend::Real(real) => match real.own_group.clone() {
                // Own group: tear the whole tree down pgid/cgroup-scoped (which
                // never touches the raw pid, so it is recycled-pid safe), reaping
                // concurrently so a signal-handling child that exits ends the grace
                // early instead of eating a pointless `SIGKILL`.
                Some(group) => {
                    let teardown = async move {
                        let _ = group.graceful_terminate(grace, signal).await;
                    };
                    // Bound the reap: a D-state child can ignore the final SIGKILL.
                    let reap = async {
                        let r = tokio::time::timeout(
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
                    let _ = tokio::join!(teardown, reap);
                }
                // Shared group: we own no group, so we reach only the direct child.
                // Escalate the hard kill through the OWNED `Child` (`start_kill`)
                // instead of a raw `kill(pid)`, so the SIGKILL is reaped by the same
                // `Child` and can never outlive the reap to hit a recycled pid — the
                // recycled-pid hazard the pid-only path guards with the gate is
                // simply absent here. Only the graceful signal is sent by pid, and
                // only while the child is provably un-reaped: this teardown is the
                // sole reaper (the deadline arm won `drive_to_exit_inner`'s
                // `select!`, so `backend_wait` never ran and no watchdog is racing —
                // `drive_to_exit_inner` retired the gate to stand them down).
                None => {
                    #[cfg(unix)]
                    {
                        stream::signal_direct_child(real.child_mut().id(), signal);
                        // Wait up to `grace` for the child to exit on the signal; a
                        // child that catches it and stays up rides out the grace.
                        // Only a *clean* reap skips escalation — on a grace elapse
                        // (or a rare wait error) escalate through the owned `Child`,
                        // whose `start_kill` is a harmless no-op if it turns out the
                        // child was already reaped.
                        let reaped_cleanly = matches!(
                            tokio::time::timeout(grace, real.child_mut().wait()).await,
                            Ok(Ok(_))
                        );
                        if !reaped_cleanly {
                            let _ = real.child_mut().start_kill();
                            let _ =
                                tokio::time::timeout(PUMP_TEARDOWN, real.child_mut().wait()).await;
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        // Windows has no graceful tier: hard-kill immediately
                        // through the owned Child and reap.
                        let _ = signal;
                        let _ = real.child_mut().start_kill();
                        let _ = tokio::time::timeout(PUMP_TEARDOWN, real.child_mut().wait()).await;
                    }
                    // Reaped (pid freed); retire so any lingering external watchdog
                    // stands down. `drive_to_exit_inner` already retired before
                    // calling us; this keeps the post-reap invariant explicit.
                    gate.retire();
                }
            },
            Backend::Scripted(s) => s.kill(),
        }
    }

    /// Whether the child has already exited, polled without blocking.
    fn has_exited_now(&mut self) -> bool {
        let gate = self.pid_gate.clone();
        // Reap-and-retire in one critical section: the non-blocking `try_wait`
        // that reaps (and frees) the pid runs under the gate lock and retires it
        // in the same step, so a watchdog's gated raw kill can never observe the
        // pid live after this reap freed it. Being synchronous, this fully closes
        // the window the async `backend_wait` backstop can only bound.
        let exited = gate.reap_under_lock(|| match &mut self.backend {
            Backend::Real(real) => matches!(real.child_mut().try_wait(), Ok(Some(_))),
            Backend::Scripted(s) => s.has_exited_now(),
        });
        if exited {
            // Claim the arbiter: a deadline watchdog racing on another thread could
            // win `PENDING -> TIMED_OUT` before `abort_watchdogs` stops it,
            // misclassifying a clean exit. Claiming `EXITED` closes that window.
            let _ = self.timeout_state.compare_exchange(
                TS_PENDING,
                TS_EXITED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
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
                self.cancel_at_exit =
                    Some(self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled()));
            }
        }
        exited
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
    /// [`Error::Io`] if the OS rejects the kill for a reason other than the
    /// child having already been reaped (which is treated as a no-op success).
    pub fn start_kill(&mut self) -> Result<()> {
        match &mut self.backend {
            Backend::Real(real) => match real.child_mut().start_kill() {
                Ok(()) => {}
                // tokio/std currently return `Ok` for a reaped child; treat
                // `InvalidInput` as the same no-op in case that ever changes.
                Err(e) if e.kind() == std::io::ErrorKind::InvalidInput => {}
                Err(e) => return Err(Error::Io(e)),
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
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
        // A surviving grandchild holding the pipe could keep a pump alive
        // indefinitely on a shared-group handle without this abort.
        if let Some(task) = self.stdout_pump.take() {
            task.abort();
        }
        if let Some(task) = self.stderr_pump.take() {
            task.abort();
        }
        match &mut self.backend {
            Backend::Real(real) => {
                if let Some(task) = real.stdin_task.take() {
                    task.abort();
                }
                // Window: a *shared-group* streamed run whose graceful-timeout
                // deadline fired leaves a DETACHED pid-only kill-and-reap
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
                // Scoped to the exact *static* preconditions of that detached task —
                // a shared group (`own_group` is `None`) with both a timeout and a
                // grace window — which are all read from `self` here, so there is no
                // race with the watchdog that arms it (unlike a "deadline fired"
                // flag the watchdog would set concurrently). An own-group handle tears
                // its whole tree down on drop and arms no detached pid-killer; a
                // shared handle without a graceful timeout never spawns one either, so
                // neither needs this hand-off — they fall through to the `else` and
                // retire the gate synchronously instead (see below).
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
                // the retire — never before it). When the deadline had not actually
                // fired the handed-off reaper is merely a harmless deterministic
                // replacement for the orphan reap.
                if real.own_group.is_none()
                    && self.timeout.is_some()
                    && self.timeout_grace.is_some()
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
                    //     at all — it needs `own_group.is_none() && timeout.is_some()
                    //     && timeout_grace.is_some()`, precisely the config we are NOT
                    //     in on those shapes;
                    //   * a shared-group+grace handle dropped with NO runtime current
                    //     never armed it *from a live path here* either — the grace
                    //     kill-and-reap is spawned by the streaming deadline watchdog,
                    //     which itself needs a runtime, so with none current the
                    //     hand-off is simply unavailable and retiring is the only way
                    //     to close the window (a deadline/cancel watchdog mid-poll on
                    //     another worker/runtime could otherwise outlive an un-retired
                    //     gate onto a recycled pid — the T-093 gap this branch closes);
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
    /// The first non-broken-pipe OS read error, surfaced as [`Error::Io`].
    read_error: Arc<std::sync::Mutex<Option<std::io::Error>>>,
}

/// Drain a child's **raw** stdout bytes into `out_buf`, honoring the byte
/// ceiling (`cap`/`mode`) and updating the shared `signals` (bytes seen, the two
/// overflow flags, and the first non-broken-pipe OS read error) so
/// [`RunningProcess::output_bytes`] can surface an incomplete capture as
/// [`Error::Io`] instead of a silently-short prefix. The raw (non-line) analogue
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
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0u8; 8 * 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
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

    /// A scripted (hermetic) handle for `tool`, with the given `ok_codes`.
    async fn scripted_handle(ok_codes: &[i32]) -> RunningProcess {
        let cmd = Command::new("tool").ok_codes(ok_codes.iter().copied());
        ScriptedRunner::new()
            .fallback(Reply::ok(""))
            .start(&cmd)
            .await
            .expect("scripted start")
    }

    /// A stashed non-broken-pipe stdin failure surfaces as `Error::Stdin` only on
    /// an otherwise-successful outcome; a non-zero exit or a signal is the "realer"
    /// failure and wins (outcome passed through).
    #[tokio::test]
    async fn stdin_error_surfaces_only_on_a_successful_outcome() {
        let mut run = scripted_handle(&[0]).await;
        run.stdin_error = Some(std::io::Error::other("boom"));
        match run.checked_outcome(Outcome::Exited(0)) {
            Err(Error::Stdin { program, source }) => {
                assert_eq!(program, "tool");
                assert_eq!(source.to_string(), "boom");
            }
            other => panic!("expected Error::Stdin, got {other:?}"),
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
            run.checked_outcome(Outcome::Exited(3)),
            Err(Error::Stdin { .. })
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
        match err {
            Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
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
        match run.wait().await {
            Err(Error::Cancelled { .. }) => {}
            other => panic!("expected Err(Cancelled), got {other:?}"),
        }
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
        {
            Err(Error::NotReady { .. }) => {}
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
        {
            Err(Error::NotReady { .. }) => {}
            other => panic!("expected Err(NotReady), got {other:?}"),
        }
        // The cancel active at observation is preserved: the finisher reports it,
        // never a silent `Ok` for a run the cancel really tore down.
        match run.wait().await {
            Err(Error::Cancelled { .. }) => {}
            other => panic!("expected Err(Cancelled), got {other:?}"),
        }
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
            run.classify_timed_out(Outcome::Exited(0)),
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
        match run.output_string().await {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }

        // output_bytes on an Inherit stdout → also errors.
        let run = runner
            .start(&Command::new("tool").stdout(crate::StdioMode::Inherit))
            .await
            .unwrap();
        assert!(matches!(run.output_bytes().await, Err(Error::Io(_))));

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
            "the raw stdout OS read error is recorded for output_bytes to surface as Error::Io"
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
    /// a recorded stdout read error as `Error::Io` rather than a silently-short
    /// `Ok(ProcessResult)`. The sink stands in for one a pump populated (the pump
    /// seam is covered in `pump.rs`); a clean-EOF sink carries no error, so a
    /// normal run is unaffected — the other tests here exercise that path.
    #[tokio::test]
    async fn output_string_surfaces_a_recorded_read_error_as_io() {
        let mut run = scripted_handle(&[0]).await; // Reply::ok("") -> empty, exit 0
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.set_read_error(std::io::Error::other("stdout read boom"));
        run.stdout_sink = Some(sink);
        match run.output_string().await {
            Err(Error::Io(e)) => assert_eq!(e.to_string(), "stdout read boom"),
            other => panic!("expected Err(Io) for an incomplete capture, got {other:?}"),
        }
    }

    /// The discard finisher (`wait`, also via `finish_lines`) likewise classifies
    /// an incomplete stderr capture as `Error::Io`, not a silent success.
    #[tokio::test]
    async fn wait_surfaces_a_recorded_read_error_as_io() {
        let mut run = scripted_handle(&[0]).await;
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        sink.set_read_error(std::io::Error::other("stderr read boom"));
        run.stderr_sink = Some(sink);
        match run.wait().await {
            Err(Error::Io(e)) => assert_eq!(e.to_string(), "stderr read boom"),
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
