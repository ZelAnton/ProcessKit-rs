//! [`RunningProcess`] — a live handle to a spawned child.
//!
//! Split by concern: this file owns the handle's state and the consuming
//! capture paths (exit driving, kill/teardown, the post-exit checkpoint);
//! [`probes`] holds the non-consuming readiness probes; [`stream`] holds the
//! incremental stdout streaming surface.

mod probes;
mod stream;

pub use stream::{Finished, OutputEvent, OutputEvents, StdoutLines};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant, SystemTime};

use encoding_rs::Encoding;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::Notify;
use tokio::task::{AbortHandle, JoinHandle};

use crate::buffer::OutputBufferPolicy;
use crate::error::Error;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::pump::{LineHandler, SharedLines, pump_lines_core};
use crate::result::{Outcome, ProcessResult};
use crate::stdin::ProcessStdin;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

// Timeout-arbitration states for `RunningProcess::timeout_state` (B1).
// `PENDING` until the run resolves; whichever of the natural reap (claims
// `EXITED` in `backend_wait`) or a fired deadline (the streaming watchdog / the
// bulk deadline arm claim `TIMED_OUT`) first `compare_exchange`s from `PENDING`
// wins. This single CAS arbiter makes "timed out vs exited" race-free even when
// the child exits within a scheduler quantum of the deadline.
const TS_PENDING: u8 = 0;
const TS_EXITED: u8 = 1;
const TS_TIMED_OUT: u8 = 2;

/// What [`RunningProcess::finish_lines`] hands back to its thin public verbs.
/// (Internal — distinct from the public [`Finished`](crate::Finished) returned
/// by the streaming `finish()`.)
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
    /// Pump — so the child can never block on a full pipe — but drop the
    /// lines (`wait`, `profile`).
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
    pub stdout_encoding: &'static Encoding,
    pub stderr_encoding: &'static Encoding,
    pub stdout_handler: Option<LineHandler>,
    pub stderr_handler: Option<LineHandler>,
    pub stdout_tee: Option<crate::pump::TeeSink>,
    pub stderr_tee: Option<crate::pump::TeeSink>,
    pub buffer: OutputBufferPolicy,
    /// Exit codes treated as success (default `[0]`), carried onto the result.
    pub ok_codes: Vec<i32>,
    /// D5: whether stdout is `Piped` (capturable) vs `Inherit`/`Null`.
    pub stdout_piped: bool,
    pub cancel_token: Option<tokio_util::sync::CancellationToken>,
}

/// A handle to a process spawned by a runner.
///
/// While this handle is alive the process keeps running; dropping it (for a
/// private-group run) tears the process tree down. Capture the outcome with
/// [`output_string`](Self::output_string) / [`output_bytes`](Self::output_bytes)
/// / [`wait`](Self::wait), or stream stdout incrementally with
/// [`stdout_lines`](Self::stdout_lines). When the command set
/// [`keep_stdin_open`](crate::Command::keep_stdin_open), drive stdin via
/// [`take_stdin`](Self::take_stdin).
pub struct RunningProcess {
    // (Debug: manual impl below — pipes/tasks/handlers are opaque.)
    //
    // The Option fields below encode the handle's de-facto states (fresh /
    // streaming / consumed) implicitly. No runtime state enum on purpose:
    // every consuming verb takes `self` BY VALUE (double consumption is a
    // compile error), and the two &mut entry points handle a repeat call
    // explicitly without panicking — `stdout_lines`/`output_events` return a
    // loud `Err` on a second call (D2, tested by
    // `second_stdout_lines_errors_and_first_overflow_is_preserved`), and
    // `take_stdin` returns `None`. A state enum would add panic paths to
    // guard doors the borrow checker already locks.
    program: String,
    /// The I/O-bearing half: a real OS child, or a scripted double feeding the
    /// same pump machinery (see [`Backend`]).
    backend: Backend,
    timeout: Option<Duration>,
    timeout_grace: Option<Duration>,
    timeout_signal: i32,
    pid: Option<u32>,
    stdout_encoding: &'static Encoding,
    stderr_encoding: &'static Encoding,
    stdout_handler: Option<LineHandler>,
    stderr_handler: Option<LineHandler>,
    stdout_tee: Option<crate::pump::TeeSink>,
    stderr_tee: Option<crate::pump::TeeSink>,
    buffer: OutputBufferPolicy,
    ok_codes: Vec<i32>,
    stdout_sink: Option<Arc<SharedLines>>,
    stderr_sink: Option<Arc<SharedLines>>,
    // The background stdout-pump task started by `output_events`, joined by
    // `finish` before the overflow check (ensures the pump has written
    // its last lines before `overflowed()` is queried).
    stdout_pump: Option<JoinHandle<()>>,
    // The background stderr-drain task started by `stdout_lines`/`output_events`,
    // awaited by `finish` so no trailing line is missed.
    stderr_pump: Option<JoinHandle<()>>,
    // B3: a non-broken-pipe stdin-writer failure stashed by `observe_stdin_task`,
    // surfaced as `Error::Stdin` by `checked_outcome` when the run otherwise
    // succeeded (a non-zero exit / signal / timeout wins). `None` = no failure
    // (or the routine broken pipe, which never stashes).
    stdin_error: Option<std::io::Error>,
    // D5: whether stdout was captured into a pipe (vs `Inherit`/`Null`). The bulk
    // capture verbs fail loudly instead of returning silently-empty output when
    // stdout wasn't piped.
    stdout_piped: bool,
    // A timer started by `stdout_lines` when a timeout is set: kills the tree at
    // the deadline so a streamed run can't hang forever. Aborted on drop.
    deadline_task: Option<JoinHandle<()>>,
    // B1: timeout arbitration (`TS_PENDING`/`TS_EXITED`/`TS_TIMED_OUT`).
    // Whichever of the natural reap (`backend_wait` claims `EXITED`) or a fired
    // deadline (the streaming `deadline_task` watchdog / the bulk
    // `drive_to_exit_inner` deadline arm claim `TIMED_OUT`) first
    // `compare_exchange`s from `PENDING` wins; `classify_timed_out` reports
    // `Outcome::TimedOut` iff the deadline won. The single CAS arbiter makes the
    // streamed-timeout-vs-natural-exit boundary race-free (a child that exits on
    // its own within a scheduler quantum of the deadline reports its real exit,
    // not a spurious TimedOut, even though the detached watchdog's timer fired).
    // Shared (`Arc`) because the watchdog is a detached task.
    timeout_state: Arc<AtomicU8>,
    cancel_token: Option<tokio_util::sync::CancellationToken>,
    // Armed by `arm_cancel_watchdog` at spawn time (via `launch`/`attach_group`)
    // so that *every* consuming path — including `wait_any`, probes, and pure
    // streaming — kills the tree when the token fires, not just `drive_to_exit`.
    cancel_task: Option<JoinHandle<()>>,
    // Snapshotted at the first reap (by `wait_exit`, `has_exited_now`, or
    // `drive_to_exit`) from the live token so no later cancel can reclassify a
    // natural exit. `None` = not yet snapshotted; `Some(v)` = snapshotted.
    // `drive_to_exit` short-circuits (skipping the cancel/deadline select) when
    // already `Some`, preserving the snapshot taken at the true reap point.
    cancel_at_exit: Option<bool>,
    started: Instant,
    start_time: SystemTime,
}

/// A boxed output reader the pumps consume — a real `ChildStdout`/`ChildStderr`
/// or a scripted in-memory stream; `pump_lines` is generic over `AsyncRead`,
/// so both flow through the *same* machinery.
type OutputReader = Box<dyn tokio::io::AsyncRead + Send + Unpin>;

/// The I/O-bearing half of a [`RunningProcess`]: a real OS child, or a
/// scripted double ([`ScriptedRunner::start`](crate::testing::ScriptedRunner)) that
/// feeds canned bytes through the same pumps/sinks — which is what makes
/// streaming, probes, and `finish` hermetically testable. Platform
/// code only ever constructs `Real`.
enum Backend {
    // Boxed: both variants are large, and the enum lives in every handle.
    Real(Box<RealProc>),
    Scripted(Box<ScriptedProc>),
}

/// The real-child fields — exactly the ones that touch the OS.
struct RealProc {
    child: Child,
    // `Arc` so a streaming deadline timer can hold a `Weak` to kill the tree
    // without keeping the group alive (kill-on-close on drop stays prompt).
    own_group: Option<Arc<ProcessGroup>>,
    stdout_pipe: Option<ChildStdout>,
    stderr_pipe: Option<ChildStderr>,
    stdin_pipe: Option<ChildStdin>,
    stdin_task: Option<JoinHandle<std::io::Result<()>>>,
}

/// Shared kill state for a scripted child, clonable out of the [`ScriptedProc`]
/// so a *detached* watchdog (the streaming `deadline_task`) can end the run
/// without holding `&mut` the backend. `fire` is the scripted analogue of
/// killing a real tree: it hangs up the feeders (each abort drops a writer,
/// EOF-ing the matching reader so pumps and streams end), flags the child dead,
/// and wakes a parked `backend_wait` via `signal`.
#[derive(Clone)]
struct ScriptedKill {
    /// Set once the scripted child is killed (cancel/deadline/start_kill/drop).
    killed: Arc<AtomicBool>,
    /// Notifies a `backend_wait` that is parked on a not-yet-exited (or
    /// never-exiting `pending`) script. `notify_one` stores a permit, so a kill
    /// that fires before the wait parks is not missed.
    signal: Arc<Notify>,
    /// Abort handles for the writer tasks feeding the duplex streams. Aborting a
    /// writer drops its end, EOF-ing the reader — exactly as a real tree's death
    /// closes its pipes. `abort` is idempotent, so repeat fires are harmless.
    feeders: Arc<Vec<AbortHandle>>,
}

impl ScriptedKill {
    /// Kill the scripted child: flag dead, hang up the feeders, wake any parked
    /// `backend_wait`. Idempotent and callable from a detached task.
    fn fire(&self) {
        self.killed.store(true, Ordering::Release);
        for feeder in self.feeders.iter() {
            feeder.abort();
        }
        self.signal.notify_one();
    }
}

/// A scripted "child": canned output readers (fed by detached writer tasks so
/// per-line delays work under a paused clock) plus a canned exit.
pub(crate) struct ScriptedProc {
    /// Canned stdout/stderr, taken once like real pipes.
    stdout: Option<tokio::io::DuplexStream>,
    stderr: Option<tokio::io::DuplexStream>,
    /// Shared kill state (feeder abort handles + dead flag + wakeup), held so a
    /// detached deadline watchdog can tear the run down. The feeder writer tasks
    /// run detached; only these abort handles reach them.
    kill: ScriptedKill,
    /// Canned exit: code + timed-out flag + optional signal number.
    code: Option<i32>,
    timed_out: bool,
    signal: Option<i32>,
    /// When the scripted child "exits": `Some(at)` resolves at that instant
    /// (now = immediately), `None` never exits on its own (`Reply::pending` —
    /// cancel/timeout still end it).
    exit_at: Option<tokio::time::Instant>,
}

impl ScriptedProc {
    /// Assemble a scripted child. Each output's text is fed through a duplex
    /// pipe by a detached writer task — with `line_delay`, the writer sleeps
    /// before each line (virtual-time friendly under a paused clock). The
    /// "process" exits after `lifetime` (`None` = never on its own).
    pub(crate) fn new(
        stdout_text: String,
        stderr_text: String,
        code: Option<i32>,
        timed_out: bool,
        signal: Option<i32>,
        lifetime: Option<Duration>,
        line_delay: Option<Duration>,
    ) -> Self {
        let mut feeders = Vec::new();
        let mut feed = |text: String| {
            let (mut tx, rx) = tokio::io::duplex(64 * 1024);
            if text.is_empty() {
                // Dropping the writer immediately EOFs the reader.
                return rx;
            }
            // The writer runs detached; its `AbortHandle` (kept in `feeders`) is
            // the only way to hang it up early — a dropped `JoinHandle` would
            // leave the task running to completion.
            let task = tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                match line_delay {
                    None => {
                        let _ = tx.write_all(text.as_bytes()).await;
                    }
                    Some(delay) => {
                        for line in text.split_inclusive('\n') {
                            tokio::time::sleep(delay).await;
                            if tx.write_all(line.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                // tx drops here → EOF.
            });
            feeders.push(task.abort_handle());
            rx
        };
        let stdout = feed(stdout_text);
        let stderr = feed(stderr_text);
        Self {
            stdout: Some(stdout),
            stderr: Some(stderr),
            kill: ScriptedKill {
                killed: Arc::new(AtomicBool::new(false)),
                signal: Arc::new(Notify::new()),
                feeders: Arc::new(feeders),
            },
            code,
            timed_out,
            signal,
            exit_at: lifetime.map(|d| tokio::time::Instant::now() + d),
        }
    }

    /// The scripted kill: mark dead and hang up the feeders (aborting a
    /// writer drops its end, EOF-ing the matching reader — pumps and streams
    /// end exactly as when a real tree dies and its pipes close).
    fn kill(&self) {
        self.kill.fire();
    }
}

impl Backend {
    /// The owning group, when this is a real child with a private group.
    fn own_group(&self) -> Option<&Arc<ProcessGroup>> {
        match self {
            Backend::Real(real) => real.own_group.as_ref(),
            Backend::Scripted(_) => None,
        }
    }

    /// A clone of the scripted kill state, for arming a detached streaming
    /// deadline watchdog (the scripted analogue of `own_group`'s `Weak` for the
    /// real path). `None` for a real child.
    fn scripted_kill(&self) -> Option<ScriptedKill> {
        match self {
            Backend::Real(_) => None,
            Backend::Scripted(s) => Some(s.kill.clone()),
        }
    }

    /// Take the stdout reader for pumping (boxed: real pipe or scripted bytes).
    fn take_stdout_reader(&mut self) -> Option<OutputReader> {
        match self {
            Backend::Real(real) => real.stdout_pipe.take().map(|p| Box::new(p) as OutputReader),
            Backend::Scripted(s) => s.stdout.take().map(|p| Box::new(p) as OutputReader),
        }
    }

    /// Take the stderr reader for pumping.
    fn take_stderr_reader(&mut self) -> Option<OutputReader> {
        match self {
            Backend::Real(real) => real.stderr_pipe.take().map(|p| Box::new(p) as OutputReader),
            Backend::Scripted(s) => s.stderr.take().map(|p| Box::new(p) as OutputReader),
        }
    }
}

impl RunningProcess {
    pub(crate) fn from_spawned(s: Spawned) -> Self {
        Self {
            program: s.program,
            backend: Backend::Real(Box::new(RealProc {
                child: s.child,
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
            stdout_encoding: s.stdout_encoding,
            stderr_encoding: s.stderr_encoding,
            stdout_handler: s.stdout_handler,
            stderr_handler: s.stderr_handler,
            stdout_tee: s.stdout_tee,
            stderr_tee: s.stderr_tee,
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
            cancel_token: s.cancel_token,
            cancel_task: None,
            cancel_at_exit: None,
            started: Instant::now(),
            start_time: SystemTime::now(),
        }
    }

    /// Build a scripted handle for `command` (the seam doubles' `start`): the
    /// command's encodings/handlers/buffer/timeout/token apply exactly as on a
    /// real run, so a hermetic streamed run exercises the same pump machinery.
    /// `pid()` is `None` — a scripted child has no OS identity.
    pub(crate) fn from_scripted(command: &crate::command::Command, scripted: ScriptedProc) -> Self {
        Self {
            program: command.program_name(),
            backend: Backend::Scripted(Box::new(scripted)),
            timeout: command.configured_timeout(),
            timeout_grace: command.configured_timeout_grace(),
            timeout_signal: command.timeout_signal_raw(),
            pid: None,
            stdout_encoding: command.out_encoding(),
            stderr_encoding: command.err_encoding(),
            stdout_handler: command.stdout_handler(),
            stderr_handler: command.stderr_handler(),
            stdout_tee: command.stdout_tee_sink(),
            stderr_tee: command.stderr_tee_sink(),
            buffer: command.output_buffer_policy(),
            ok_codes: command.ok_codes_vec(),
            stdout_sink: None,
            stderr_sink: None,
            stdout_pump: None,
            stderr_pump: None,
            stdin_error: None,
            stdout_piped: command.stdout_is_piped(),
            deadline_task: None,
            timeout_state: Arc::new(AtomicU8::new(TS_PENDING)),
            cancel_token: command.cancel_token(),
            cancel_task: None,
            cancel_at_exit: None,
            started: Instant::now(),
            start_time: SystemTime::now(),
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

    /// Arm (or re-arm) the spawn-time cancel kill task. Called from `launch`
    /// (pid-only, for shared-group runs) and `attach_group` (group+pid, for
    /// own-group runs). If a task is already armed it is aborted and replaced —
    /// `attach_group` upgrades the initial pid-only version to the group-aware
    /// one. No-op when no cancel token is configured.
    ///
    /// Storing the handle in `self.cancel_task` means `Drop` / `abort_watchdogs`
    /// will abort it on the normal paths, limiting the recycled-pid window to a
    /// brief scheduler quantum.
    pub(crate) fn arm_cancel_watchdog(&mut self) {
        {
            if let Some(old) = self.cancel_task.take() {
                old.abort();
            }
            let Some(token) = self.cancel_token.clone() else {
                return;
            };
            let group_weak = self.backend.own_group().map(Arc::downgrade);
            let pid = self.pid;
            self.cancel_task = Some(tokio::spawn(async move {
                token.cancelled().await;
                // Full tree kill when we own the group; direct child kill as
                // backstop for shared-group runs or after the group is gone.
                if let Some(g) = group_weak.and_then(|w| w.upgrade()) {
                    let _ = g.terminate_all();
                }
                stream::kill_direct_child(pid);
            }));
        }
    }

    /// F2: bound a *scripted* streamed run by its [`timeout`](crate::Command::timeout).
    /// A scripted handle has no process group, so the real watchdog (which kills
    /// the tree) never arms for it; this detached task instead hangs up the
    /// feeders at the deadline — their EOF ends the pump and the stream, exactly
    /// as a real tree's closing pipes do. Claims the timeout via the arbiter
    /// (`PENDING` → `TIMED_OUT`) so the finisher classifies `TimedOut`; if the
    /// script already exited the CAS fails and the kill is skipped. Armed once
    /// (a second streaming call won't duplicate it) and only when a timeout is
    /// set; no-op for a real backend. The handle lands in `self.deadline_task`,
    /// which also disables the bulk deadline arm in `drive_to_exit_inner` (so the
    /// teardown happens here, once) and is aborted by `Drop`/`abort_watchdogs`.
    fn arm_scripted_deadline(&mut self) {
        if self.deadline_task.is_some() {
            return;
        }
        let (Some(limit), Some(kill)) = (self.timeout, self.backend.scripted_kill()) else {
            return;
        };
        // Anchor to spawn time so a late stream call can't re-grant the full
        // limit (B7 fix); `started` is std::time::Instant (Copy).
        let started = self.started;
        let timeout_state = self.timeout_state.clone();
        self.deadline_task = Some(tokio::spawn(async move {
            let remaining = limit
                .checked_sub(started.elapsed())
                .unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining).await;
            if timeout_state
                .compare_exchange(
                    TS_PENDING,
                    TS_TIMED_OUT,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                return; // the script already exited on its own — no kill
            }
            kill.fire();
        }));
    }

    /// Take the raw stdout pipe — the [`Pipeline`](crate::Pipeline) plumbing
    /// that feeds it into the next stage's stdin. Afterwards this handle can
    /// still report exit + stderr via [`finish`](Self::finish)
    /// (which tolerates a taken stdout), like after `stdout_lines`.
    /// `None` for a scripted backend — scripted doubles don't compose into a
    /// real pipeline (pipelines are a real-process concern).
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

// Manual: pipes, pump tasks, and line handlers are opaque.
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
            .and_then(|pid| crate::sys::process_metrics(pid).cpu_time)
    }

    /// Peak resident memory in bytes, if the platform can report it.
    #[cfg(feature = "stats")]
    pub fn peak_memory_bytes(&self) -> Option<u64> {
        self.pid
            .and_then(|pid| crate::sys::process_metrics(pid).peak_memory_bytes)
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
    ///
    /// # Example
    ///
    /// Drive a process interactively — write requests on stdin, read answers
    /// from stdout:
    ///
    /// `ProcessStdin`'s writer methods return [`std::io::Result`] (idiomatic for
    /// a writer); mix them with the crate's `Result` via `Box<dyn Error>` here,
    /// or `.map_err(processkit::Error::Io)?` in a `processkit::Result` function.
    ///
    /// ```no_run
    /// use processkit::{Command, StreamExt};
    ///
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// // `bc` evaluates each stdin line and prints the result on stdout.
    /// let mut run = Command::new("bc").keep_stdin_open().start().await?;
    ///
    /// let mut stdin = run.take_stdin().expect("stdin was kept open");
    /// stdin.write_line("2 + 2").await?;
    /// stdin.write_line("6 * 7").await?;
    /// stdin.finish().await?; // send EOF so bc finishes
    ///
    /// let mut answers = run.stdout_lines().unwrap();
    /// while let Some(line) = answers.next().await {
    ///     println!("bc says: {line}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn take_stdin(&mut self) -> Option<ProcessStdin> {
        match &mut self.backend {
            Backend::Real(real) => real.stdin_pipe.take().map(ProcessStdin::new),
            // Scripted doubles don't model interactive stdin (yet): the
            // writer would need a scripted reader on the other end. `None`
            // matches the "stdin wasn't kept open" contract.
            Backend::Scripted(_) => None,
        }
    }

    /// Whether **dropping** this handle will tear down (hard-kill) the process
    /// tree (D10).
    ///
    /// `true` — the handle owns a **private** process group, so dropping it
    /// without a graceful [`wait`](Self::wait) / shutdown hard-kills the whole
    /// tree via kill-on-close (the crate's leak-safety guarantee). Dropping it on
    /// an error path is therefore sufficient cleanup.
    ///
    /// `false` — the handle runs inside a **shared**
    /// [`ProcessGroup`](crate::ProcessGroup) (started via
    /// [`ProcessGroup::start`](crate::ProcessGroup::start)) whose lifetime the
    /// group owns: dropping this handle does *not* kill the tree — the group's
    /// owner does, on its own drop or [`shutdown`](crate::ProcessGroup::shutdown).
    /// (Also `false` for a scripted test double, which has no OS tree.)
    ///
    /// A function handed a `RunningProcess` can use this to reason about whether
    /// letting it drop is enough to contain the child, or whether the shared
    /// group must be torn down separately.
    pub fn kills_tree_on_drop(&self) -> bool {
        self.backend.own_group().is_some()
    }

    /// D5: a bulk capture verb on a stdout that wasn't piped (`Inherit`/`Null`)
    /// would return silently-empty output — surface it as a clear error instead.
    /// `stdout_piped` reflects the command's `stdout` mode for *both* real and
    /// scripted handles, so a scripted run with `stdout(Null)` errors here too
    /// (the mode is honored uniformly, not special-cased per backend).
    fn ensure_stdout_capturable(&self) -> Result<()> {
        if self.stdout_piped {
            return Ok(());
        }
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "`{}`: stdout is not piped (Command::stdout was set to Inherit/Null), so the \
                 capture verbs have nothing to read — use StdioMode::Piped to capture it",
                self.program
            ),
        )))
    }

    /// D2: the streaming verbs ([`stdout_lines`](Self::stdout_lines) /
    /// [`output_events`](Self::output_events)) are fallible — they fail loud
    /// instead of handing back a silently-empty stream when (a) stdout was not
    /// piped (nothing to read), or (b) a streaming verb already consumed stdout
    /// on this handle (it streams **once**; a second call would be empty). This
    /// mirrors the bulk verbs' D5 loudness, closing the crate's least-predictable
    /// corner (and making a second `wait_for_line` a clear error rather than a
    /// stream that is forever `NotReady`).
    fn ensure_stdout_streamable(&self) -> Result<()> {
        self.ensure_stdout_capturable()?; // (a) non-piped stdout
        if self.stdout_sink.is_some() {
            // (b) a prior stdout_lines / output_events already took stdout.
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
    pub async fn output_string(mut self) -> Result<ProcessResult<String>> {
        let finished = self
            .finish_lines(CaptureMode::Lines, /* expose_counts */ true, || {})
            .await?;
        // B4: truncation = lines the buffer *policy* discarded, reported by each
        // sink's `dropped()`. The old `count() > retained` test conflated policy
        // drops with lines a prior `stdout_lines` stream *consumed* via `try_pop`
        // — so `output_string` after partial streaming under the default
        // unbounded policy falsely reported `truncated()` even though nothing was
        // dropped. `dropped()` counts only policy discards, staying `0` here.
        let truncated = self.stdout_sink.as_ref().is_some_and(|s| s.dropped() > 0)
            || self.stderr_sink.as_ref().is_some_and(|s| s.dropped() > 0);
        // B12: carry the total lines/bytes seen (retained + dropped) so a
        // checking verb can report a faithful `OutputTooLarge` on truncation.
        let total_lines = self.stdout_sink.as_ref().map_or(0, |s| s.count())
            + self.stderr_sink.as_ref().map_or(0, |s| s.count());
        let total_bytes = self.stdout_sink.as_ref().map_or(0, |s| s.seen_bytes())
            + self.stderr_sink.as_ref().map_or(0, |s| s.seen_bytes());
        let duration = self.started.elapsed();
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

    /// Drain both streams, wait for exit, and return the raw stdout bytes
    /// (exact; stderr is captured as text).
    ///
    /// Deliberately NOT routed through `finish_lines`: stdout is a raw byte
    /// reader (no line pump), with its own bounded drain-then-abort teardown.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] with [`InvalidInput`](std::io::ErrorKind::InvalidInput)
    /// if stdout is not piped (`Command::stdout` set to `Inherit`/`Null` — there
    /// is nothing to read), or if a streaming call ([`stdout_lines`](Self::stdout_lines)
    /// / [`output_events`](Self::output_events)) already consumed stdout as decoded
    /// lines: the raw bytes are gone and cannot be faithfully reconstructed, so
    /// `output_bytes` fails loudly rather than returning empty (B3). Collect the
    /// streamed lines with [`output_string`](Self::output_string), or call
    /// `output_bytes` without streaming first.
    pub async fn output_bytes(mut self) -> Result<ProcessResult<Vec<u8>>> {
        self.ensure_stdout_capturable()?; // D5
        // B3: `output_bytes` returns the EXACT raw stdout bytes, so it must own
        // the raw stdout reader. A prior streaming call (`stdout_lines` /
        // `output_events`) already took that reader and pumped it as decoded,
        // line-normalized text — the raw bytes are gone and cannot be faithfully
        // reconstructed. Fail loudly rather than silently returning empty output
        // (and clobbering the streamed sinks), mirroring D5's "never silently
        // empty" stance. `stdout_lines`/`output_events` set both sinks, so either
        // being present means a streaming call already consumed this run's output.
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
        // L3 parity (B3): store the stderr pump on `self.stderr_pump` so `Drop`
        // aborts it if `drive_to_exit` errors and the `?` below propagates before
        // we join — an orphaned pump on a shared-group handle would otherwise
        // buffer unboundedly for the child's remaining lifetime.
        self.stderr_pump = self.backend.take_stderr_reader().map(|pipe| {
            tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                self.stderr_tee.clone(),
                stderr_sink.clone(),
            ))
        });
        self.stderr_sink = Some(stderr_sink.clone());

        // Read stdout raw, concurrently, so it never blocks the child. The
        // bytes accumulate in a shared buffer (not the task's return value) so
        // the bounded teardown below can salvage a partial read. B3: the task is
        // stored on `self.stdout_pump` (not a frame-local) for the same reason as
        // the stderr pump — a `drive_to_exit` error must abort it via `Drop`, not
        // detach it to grow `out_buf` unboundedly on a shared-group handle.
        let mut stdout_pipe = self.backend.take_stdout_reader();
        let out_buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        self.stdout_pump = Some({
            let out_buf = out_buf.clone();
            tokio::spawn(async move {
                if let Some(pipe) = &mut stdout_pipe {
                    let mut chunk = [0u8; 8 * 1024];
                    loop {
                        match pipe.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => out_buf
                                .lock()
                                .expect("stdout buffer poisoned")
                                .extend_from_slice(&chunk[..n]),
                        }
                    }
                }
            })
        });

        let outcome = self.drive_to_exit().await?;
        self.observe_stdin_task().await;
        // Take the raw-stdout task back off `self` (so `Drop` won't double-abort
        // it) and bound its drain by the same teardown grace as the line pumps:
        // on a shared-group handle a surviving descendant can hold stdout open
        // past the child's death, and an unbounded `read_to_end` here would park
        // this call forever (`output_string`/`wait` are bounded via `join_pumps`
        // — `output_bytes` must be too).
        if let Some(out_task) = self.stdout_pump.take() {
            let abort = out_task.abort_handle();
            if tokio::time::timeout(PUMP_TEARDOWN, out_task).await.is_err() {
                // The reader is still parked on a held-open pipe: abort it (like
                // `join_pumps` aborts stragglers) and keep whatever arrived —
                // parity with the line pumps' partial capture.
                abort.abort();
            }
        }
        let stdout = std::mem::take(&mut *out_buf.lock().expect("stdout buffer poisoned"));
        join_pumps(self.stderr_pump.take().into_iter().collect()).await;
        let outcome = self.checked_outcome(outcome)?;

        // Fail-loud ceiling check for the line-pumped stderr.
        if stderr_sink.overflowed() {
            return Err(crate::Error::OutputTooLarge {
                program: self.program.clone(),
                line_limit: self.buffer.max_lines,
                byte_limit: self.buffer.max_bytes,
                total_lines: stderr_sink.count(),
                total_bytes: stderr_sink.seen_bytes(),
            });
        }

        // stdout is raw bytes (not line-buffered), so only the line-pumped stderr
        // can be truncated by the buffer policy here (B4: policy drops, not pops).
        let stderr_lines = stderr_sink.drain();
        let truncated = stderr_sink.dropped() > 0;
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
        .with_ok_codes(self.ok_codes.clone()))
    }

    /// Wait for exit, returning how the run ended as an [`Outcome`] (output is
    /// drained and discarded so the child never blocks on a full pipe).
    ///
    /// This low-level handle method reports the **raw** outcome: a run killed by
    /// its timeout returns [`Outcome::TimedOut`](crate::Outcome::TimedOut); a
    /// signal-terminated run returns [`Outcome::Signalled`](crate::Outcome::Signalled)
    /// with the signal number when the platform reports one. Neither is raised as
    /// an error here — use the one-shot helpers
    /// ([`Command::exit_code`](crate::Command::exit_code) /
    /// [`ProcessRunnerExt::exit_code`](crate::ProcessRunnerExt::exit_code)) for
    /// the timeout-as-error behavior.
    /// One exception: a run cancelled via its token (`Command::cancel_on`)
    /// errors with `Error::Cancelled` here too — cancellation is always an
    /// error, on every consuming path.
    pub async fn wait(mut self) -> Result<Outcome> {
        Ok(self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, || {})
            .await?
            .outcome)
    }

    /// Gracefully stop the process tree and report how it ended (D4): send
    /// `SIGTERM`, wait up to `grace` for the tree to exit on its own, then
    /// `SIGKILL` any survivor. On Windows there is no signal tier, so the kill is
    /// atomic and `grace` is not awaited. The returned [`Outcome`] reflects how
    /// the child actually ended — `Exited(0)` if it handled `SIGTERM` and shut
    /// down cleanly within the grace, or [`Signalled`](crate::Outcome::Signalled)
    /// / a platform kill code otherwise.
    ///
    /// This is the "started a dev server, exercised it, now stop it cleanly"
    /// verb — the graceful counterpart to dropping the handle (an immediate hard
    /// kill) or [`start_kill`](Self::start_kill) (kill now, `wait` for the code
    /// yourself).
    ///
    /// Only an **own-group** handle (from
    /// [`Command::start`](crate::Command::start) / the
    /// [`JobRunner`](crate::JobRunner)) can be gracefully shut down here — it owns
    /// its process group. A **shared-group** handle (from
    /// [`ProcessGroup::start`](crate::ProcessGroup::start)) does not own its
    /// group; shutting it down would tear down the caller's *other* children too,
    /// so this returns [`Error::Unsupported`](crate::Error::Unsupported) — stop
    /// the whole group via
    /// [`ProcessGroup::shutdown`](crate::ProcessGroup::shutdown) instead, or kill
    /// just this child with [`start_kill`](Self::start_kill).
    ///
    /// A configured [`Command::timeout`](crate::Command::timeout) still applies
    /// while shutting down: if its deadline has already elapsed (or is shorter
    /// than `grace`), this reports [`Outcome::TimedOut`](crate::Outcome::TimedOut)
    /// rather than the graceful exit — the run's own deadline is a hard ceiling
    /// on the grace.
    pub async fn shutdown(self, grace: std::time::Duration) -> Result<Outcome> {
        let Some(group) = self.backend.own_group().cloned() else {
            return Err(Error::Unsupported {
                operation: "shutdown (a shared-group handle does not own its group — \
                            use ProcessGroup::shutdown, or start_kill for just this child)"
                    .into(),
            });
        };
        // SIGTERM → wait `grace` → SIGKILL survivors (atomic kill on Windows).
        // Reap the child *concurrently* with the graceful teardown: on the
        // process-group backend (macOS/BSD, Linux without cgroup) the grace loop
        // polls liveness via `kill(pgid, 0)`, and an unreaped zombie still
        // answers that probe — so without a concurrent reap a child that exits
        // immediately on `SIGTERM` would be seen as alive for the *whole* grace
        // and eat a pointless `SIGKILL`. Reaping alongside lets the loop end as
        // soon as the child is gone (the same pattern as `teardown_on_timeout`).
        let (term_result, outcome) = tokio::join!(
            group.graceful_terminate(grace, crate::sys::SIGTERM_RAW),
            self.wait(),
        );
        term_result?;
        outcome
    }

    /// Minimal non-consuming exit wait — the [`wait_any`](crate::wait_any) race
    /// participant. Unlike [`wait`](Self::wait) it spawns no pumps and applies
    /// no [`timeout`](crate::Command::timeout). Cancel-safe and re-awaitable:
    /// tokio caches the exit status, so a raced-and-cancelled process can be
    /// waited again (or consumed normally) afterwards.
    ///
    /// Aborts watchdog tasks after reap to prevent late-firing deadline/cancel
    /// tasks from sending signals to a recycled pid (B1/B2 fix).
    ///
    /// Surfaces the same errors as the bulk verbs' post-exit checkpoint: a
    /// cancelled run is [`Error::Cancelled`], and a stdin source that failed for
    /// a non-broken-pipe reason on an otherwise-successful run is
    /// [`Error::Stdin`] (E8). On a repeat call after the first reap the original
    /// cancel snapshot is preserved (B2) and the one-shot stdin error has already
    /// been consumed, so the cached [`Outcome`] is returned unchanged.
    pub(crate) async fn wait_exit(&mut self) -> Result<Outcome> {
        // B15: `wait_exit` must NOT close an untaken `keep_stdin_open` pipe.
        // `wait_any`/`wait_all` only *borrow* each contender and promise the
        // losers "remain fully usable" — closing their stdin here would give a
        // loser a premature EOF and leave `take_stdin()` returning `None`
        // afterward. Like the documented "no output pumping" non-feature, a
        // `keep_stdin_open` child blocked on stdin is the caller's
        // responsibility: take its writer (or don't keep stdin open) before
        // racing it, otherwise it never reaches EOF and never exits. The
        // consuming `drive_to_exit` (`wait()`) *does* close the pipe — it
        // consumes the handle, so there is no loser to keep usable.
        // B2: preserve a cancel snapshot taken by an earlier reap observation
        // (a prior `wait_exit` / `has_exited_now` / `drive_to_exit`). Re-running
        // the reap bookkeeping would re-query the live token, and a token
        // cancelled *after* this child's natural exit would then misclassify the
        // Tokio-cached exit as `Err(Cancelled)` on a second `wait_any`/`wait_all`
        // (the documented "race them, keep watching the rest" pattern — and for
        // `wait_all` that spurious error discards every other contender's outcome
        // too). Mirrors `drive_to_exit`'s `cancel_at_exit.is_some()` guard.
        if self.cancel_at_exit.is_some() {
            let outcome = self.backend_wait().await?;
            // E8: observe the stdin writer here too. If the first observation
            // was a readiness probe (`has_exited_now`, which snapshots
            // `cancel_at_exit` but does not observe stdin), this repeat path is
            // where a genuine `Error::Stdin` surfaces. Idempotent: once the task
            // was taken (a prior `wait_exit`/consuming verb), this is a no-op.
            self.observe_stdin_task().await;
            // B1: classify a streamed timeout (the watchdog claimed `TimedOut`) as
            // TimedOut here too — `wait_any`/`wait_all` must match `drive_to_exit`
            // (classify before checked_outcome so cancellation still wins).
            let outcome = self.classify_timed_out(outcome);
            return self.checked_outcome(outcome);
        }
        // F1: honor cancellation DURING the wait so `wait_any`/`wait_all` don't
        // hang on a never-exiting handle (e.g. a scripted `Reply::pending`, whose
        // `backend_wait` parks forever) when the token fires. Mirrors
        // `drive_to_exit_inner`'s cancel arm — kill the tree and resolve as
        // `Signalled(None)`, which the `cancel_at_exit` snapshot below turns into
        // `Err(Cancelled)`. No deadline arm: this path applies no timeout by
        // contract (a streamed run's deadline is owned by its watchdog).
        let outcome = {
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
                    Ok(Outcome::Signalled(None))
                }
                outcome = self.backend_wait() => outcome,
            }?
        };
        // First reap: abort watchdogs and clear pid before returning. This
        // mirrors the `abort_watchdogs` call in `drive_to_exit` and prevents a
        // streaming deadline/cancel task from waking up minutes later and
        // killing an unrelated process that recycled this pid.
        self.abort_watchdogs();
        // Snapshot the cancel state at the true reap point (before any pump
        // teardown or caller code runs). A consuming verb called on the winner
        // after `wait_any` must see this snapshot — not re-query the live
        // token — so a cancel that fires after natural exit doesn't convert
        // success to `Err(Cancelled)` (Issue 1 / L14 companion fix).
        {
            self.cancel_at_exit =
                Some(self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled()));
        }
        // E8: observe a finished stdin writer that failed for a non-broken-pipe
        // reason, so a genuine `Error::Stdin` surfaces on the `wait_any`/
        // `wait_all` path too (parity with `finish_lines`' B3 contract;
        // previously this path never observed the writer, silently losing it).
        self.observe_stdin_task().await;
        // B1: a streamed run whose deadline fired (the watchdog set `timed_out`)
        // must report `Outcome::TimedOut` through `wait_any`/`wait_all` too —
        // matching `drive_to_exit`. Classify before `checked_outcome` so
        // cancellation still takes precedence.
        let outcome = self.classify_timed_out(outcome);
        self.checked_outcome(outcome)
    }

    /// Run the process to completion while sampling its CPU and memory every
    /// `every`, returning a [`RunProfile`](crate::stats::RunProfile) summary
    /// (exit code, wall duration, last CPU reading, peak RSS, sample count).
    ///
    /// Behaves exactly like [`wait`](Self::wait) — output is pumped (and
    /// dropped), the configured [`timeout`](crate::Command::timeout) applies —
    /// with a sampling task alongside. Samples come from the started child
    /// *process* (the [`cpu_time`](Self::cpu_time) /
    /// [`peak_memory_bytes`](Self::peak_memory_bytes) source); for a series
    /// covering a whole tree, sample the group via
    /// [`ProcessGroup::sample_stats`](crate::ProcessGroup::sample_stats)
    /// instead. The first sample lands immediately, so even a short run
    /// usually reports; a child that exits faster still profiles `None`s. A
    /// zero `every` is clamped to 1 ms.
    #[cfg(feature = "stats")]
    pub async fn profile(mut self, every: Duration) -> Result<crate::stats::RunProfile> {
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct Acc {
            cpu_time: Option<Duration>,
            peak_memory_bytes: Option<u64>,
            samples: usize,
        }

        // tokio panics on a zero interval period; clamp rather than panic a
        // detached sampling task on a legal-looking input.
        let every = every.max(Duration::from_millis(1));
        let started = self.started;
        let acc = Arc::new(Mutex::new(Acc::default()));
        // Sampling needs only the pid (process_metrics is a free query), so the
        // task never borrows `self` and the consuming wait below stays intact.
        let sampler = self.pid.map(|pid| {
            let acc = Arc::clone(&acc);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(every);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    let metrics = crate::sys::process_metrics(pid);
                    if let Ok(mut acc) = acc.lock() {
                        acc.samples += 1;
                        // Cumulative CPU only grows while the process lives;
                        // keep the latest reading. Peak RSS keeps the maximum.
                        if let Some(cpu) = metrics.cpu_time {
                            acc.cpu_time = Some(cpu);
                        }
                        if let Some(peak) = metrics.peak_memory_bytes {
                            acc.peak_memory_bytes =
                                Some(acc.peak_memory_bytes.map_or(peak, |prev| prev.max(peak)));
                        }
                    }
                }
            })
        });

        // Guard against future-drop (e.g. `tokio::time::timeout(d, p.profile(e))`):
        // dropping the `profile()` future before it returns would leave the
        // sampler ticking forever against a pid that may be recycled after reap.
        // `AbortOnDrop` ensures the task is aborted whether we exit via `on_exit`,
        // via `?`, or via a future-drop. The `on_exit` abort below is still the
        // primary path; this is the fallback for the drop case.
        struct AbortOnDrop(tokio::task::AbortHandle);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _sampler_guard = sampler.as_ref().map(|h| AbortOnDrop(h.abort_handle()));

        // The `on_exit` hook stops the sampler the moment the child is reaped:
        // its pid is free for reuse from that point (Linux), and the pump
        // drain can idle out PUMP_TEARDOWN on a leaked pipe — long enough for
        // a recycled pid to masquerade as the child and corrupt the readings.
        let outcome = self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, || {
                if let Some(task) = &sampler {
                    task.abort();
                }
            })
            .await?
            .outcome;
        let exit_code = match outcome {
            Outcome::Exited(c) => Some(c),
            _ => None,
        };
        let duration = started.elapsed();
        let (cpu_time, peak_memory_bytes, samples) = match acc.lock() {
            Ok(acc) => (acc.cpu_time, acc.peak_memory_bytes, acc.samples),
            Err(_) => (None, None, 0),
        };
        Ok(crate::stats::RunProfile {
            exit_code,
            duration,
            cpu_time,
            peak_memory_bytes,
            samples,
        })
    }

    /// The shared line-pumped consuming core behind [`output_string`](Self::output_string),
    /// [`wait`](Self::wait), and [`profile`](Self::profile): spawn both line
    /// pumps, drive to exit, run `on_exit` in the slot **between the exit
    /// await and the `?`** (so it fires even when the drive errored — this is
    /// where `profile` aborts its pid sampler before a recycled pid could be
    /// read), join the pumps (bounded by `PUMP_TEARDOWN`), pass the
    /// cancellation gate, and drain per `capture`.
    ///
    /// `expose_counts` stores the sinks on `self` so the live
    /// `stdout_line_count`/`stderr_line_count` accessors read — only
    /// `output_string` does (today's behavior, preserved).
    ///
    /// `output_bytes` (raw stdout reader, its own bounded teardown) and
    /// `finish` (already-streaming state, late stderr pump)
    /// deliberately do NOT route through this core — their spines differ by
    /// nature, not by copy-paste.
    async fn finish_lines(
        &mut self,
        capture: CaptureMode,
        expose_counts: bool,
        on_exit: impl FnOnce(),
    ) -> Result<FinishedLines> {
        // D5: the capturing path needs a piped stdout; fail loudly rather than
        // returning empty. The discard path (wait/profile) reads nothing, so it
        // is exempt — a non-piped stream is fine there.
        if matches!(capture, CaptureMode::Lines) {
            self.ensure_stdout_capturable()?;
        }
        // B10: reuse a sink already populated by a prior streaming call so that
        // output_string called after stdout_lines/output_events sees the lines
        // the streaming pump wrote rather than silently returning empty output.
        // B9: for the discard path, create a retain-nothing sink (bounded(0),
        // DropOldest) rather than the user's policy, so a chatty long-running
        // child never accumulates O(total) heap in wait/profile.
        let discard_policy = OutputBufferPolicy::bounded(0);
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
        // Spawn pumps for any still-untaken pipes.  L3: handles are stored on
        // self (not a frame-local Vec), so Drop aborts them if drive_to_exit
        // errors and the future propagates before join_pumps can run — orphaned
        // pumps on a shared-group handle would otherwise buffer unboundedly.
        self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        if expose_counts {
            // Keep the first sink when one was set by a prior streaming call
            // (B10: same Arc, same overflow state, same count).
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
        // Take the pump handles stored by spawn_line_pumps (or a prior streaming
        // call) and join them so their final writes are visible before the
        // overflow check and drain.
        let pumps: Vec<_> = [self.stdout_pump.take(), self.stderr_pump.take()]
            .into_iter()
            .flatten()
            .collect();
        join_pumps(pumps).await;
        let outcome = self.checked_outcome(outcome)?;

        // Fail-loud ceiling: only meaningful for capturing verbs.  The discard
        // path (wait/profile) uses a retain-nothing sink that never sets
        // overflowed, so this check is a structural no-op there.  Gate it on
        // CaptureMode::Lines for clarity.
        if matches!(capture, CaptureMode::Lines) {
            for sink in [&stdout_sink, &stderr_sink] {
                if sink.overflowed() {
                    return Err(crate::Error::OutputTooLarge {
                        program: self.program.clone(),
                        line_limit: self.buffer.max_lines,
                        byte_limit: self.buffer.max_bytes,
                        total_lines: sink.count(),
                        total_bytes: sink.seen_bytes(),
                    });
                }
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

    /// Spawn line pumps for any still-untaken pipes into the given sinks.
    /// Stores the task handles on `self` (`stdout_pump` / `stderr_pump`)
    /// rather than returning them, so [`Drop`] aborts them if a consuming
    /// verb propagates an error before `join_pumps` runs (L3 fix).
    fn spawn_line_pumps(&mut self, stdout_sink: &Arc<SharedLines>, stderr_sink: &Arc<SharedLines>) {
        if let Some(pipe) = self.backend.take_stdout_reader() {
            self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stdout_encoding,
                self.stdout_handler.clone(),
                self.stdout_tee.clone(),
                stdout_sink.clone(),
            )));
        }
        if let Some(pipe) = self.backend.take_stderr_reader() {
            self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                self.stderr_tee.clone(),
                stderr_sink.clone(),
            )));
        }
    }

    /// The single post-exit checkpoint **every consuming path passes
    /// through** after its pumps settle: folds in the cancellation gate — a
    /// cancelled run is *always* an error, and the check runs before any
    /// outcome classification, so cancellation beats a simultaneous timeout.
    /// Centralizing it here makes the documented invariant structural instead
    /// of per-consumer copy-paste discipline.
    fn checked_outcome(&mut self, outcome: Outcome) -> Result<Outcome> {
        // Use the pre-pump snapshot rather than a live token read: prevents
        // a cancel that fires during `join_pumps` from discarding real output.
        // `unwrap_or(false)`: None means not yet snapshotted — only reachable
        // if a future code path calls checked_outcome without drive_to_exit,
        // which is not possible today; treat as "not cancelled" conservatively.
        if self.cancel_at_exit.unwrap_or(false) {
            return Err(Error::Cancelled {
                program: self.program.clone(),
            });
        }
        // B3 (Decision 2): a stashed non-broken-pipe stdin-writer failure
        // surfaces as `Error::Stdin` — but **only when the run otherwise
        // succeeded**. A non-zero exit, signal, or timeout is the "realer"
        // failure and wins: it stays in `outcome` for the caller's helpers
        // (`ensure_success`/`require_code`) to classify, and the stdin error is
        // dropped. Cancellation already returned above. (Surfacing here, before
        // the per-verb fail-loud overflow check, means a run that both
        // overflowed *and* failed stdin reports `Stdin` — both are valid
        // "otherwise-succeeded" errors and the case is pathological.)
        let succeeded = matches!(outcome, Outcome::Exited(code) if self.ok_codes.contains(&code));
        if succeeded && let Some(source) = self.stdin_error.take() {
            return Err(Error::Stdin {
                program: self.program.clone(),
                source,
            });
        }
        Ok(outcome)
    }

    /// Observe a stdin writer that failed for a reason other than the normal
    /// broken pipe (the child exiting before reading all of stdin is routine
    /// and tested), stashing it in `self.stdin_error` for `checked_outcome` to
    /// surface as [`Error::Stdin`] (B3). Only a writer that already **finished**
    /// is observed — a task still parked (e.g. on a slow `from_reader` source)
    /// is left for `Drop`'s abort, so teardown timing is unchanged.
    async fn observe_stdin_task(&mut self) {
        // Take the task out (dropping the `&mut self.backend` borrow before we
        // touch `self.program`/`self.stdin_error`).
        let task = match &mut self.backend {
            Backend::Real(real) => real.stdin_task.take(),
            Backend::Scripted(_) => None,
        };
        let Some(task) = task else {
            return;
        };
        if !task.is_finished() {
            // Not done — put it back for `Drop` to abort; only a finished
            // writer is observed, so teardown timing is unchanged.
            if let Backend::Real(real) = &mut self.backend {
                real.stdin_task = Some(task);
            }
            return;
        }
        // The task is finished, so this await is immediate.
        let observed = match task.await {
            Ok(Ok(())) => None,
            // A routine EPIPE (the child closed stdin / exited early) is expected,
            // not a failure — don't stash it.
            Ok(Err(e)) if is_broken_pipe(&e) => None,
            Ok(Err(e)) => Some(e),
            // L1: the writer task did not complete normally — surface it rather
            // than swallowing it (a panicking stdin source must not read as a
            // clean success). It is awaited only once `is_finished()`, and the
            // only abort site (`Drop`) takes the handle first, so in practice this
            // is a panic; word it precisely either way.
            Err(join_err) => Some(std::io::Error::other(if join_err.is_panic() {
                format!("stdin writer task panicked: {join_err}")
            } else {
                format!("stdin writer task did not complete: {join_err}")
            })),
        };
        if let Some(e) = observed {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "processkit",
                program = %self.program,
                error = %e,
                "stdin writer failed"
            );
            // B3: stash so `checked_outcome` surfaces it as `Error::Stdin` when
            // the run otherwise succeeded.
            self.stdin_error = Some(e);
        }
    }

    /// Abort all watchdog tasks and clear the recorded pid once the child has
    /// been reaped. Aborting before the pid is freed limits the window in which
    /// a watchdog could SIGKILL an innocent process that recycled the pid
    /// (though an already-executing kill in a task cannot be recalled — the
    /// window is a scheduler quantum, mirroring the acknowledged tradeoff in
    /// `graceful_kill_pid`). Clearing `self.pid` also makes `pid()`/`cpu_time()`/
    /// `peak_memory_bytes()` report correctly after reap. (`Drop` is still the
    /// backstop for handles that are dropped without consuming.)
    fn abort_watchdogs(&mut self) {
        self.pid = None;
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
    }

    /// Wait for the child to exit, applying the timeout (killing the tree on
    /// elapse). Returns the [`Outcome`] of the run.
    async fn drive_to_exit(&mut self) -> Result<Outcome> {
        // A `keep_stdin_open` pipe nobody took can never be taken once a
        // consuming verb is driving (the verbs own `self`): close it NOW so a
        // stdin-reading child sees EOF instead of blocking to its timeout. A
        // writer the caller did take via `take_stdin()` is unaffected —
        // the pipe moved out of `self` then.
        if let Backend::Real(real) = &mut self.backend {
            drop(real.stdin_pipe.take());
        }
        // Short-circuit when the child was already reaped by `wait_exit` or
        // a probe (`has_exited_now`): those paths snapshot `cancel_at_exit` at
        // the true reap point. Re-running the cancel/deadline select here would
        // fire the cancel arm immediately (token already cancelled), overwriting
        // the correct snapshot and converting a natural exit to `Err(Cancelled)`.
        // `backend_wait` returns the Tokio-cached exit status instantly for an
        // already-reaped child — safe and cheap.
        if self.cancel_at_exit.is_some() {
            let outcome = self.backend_wait().await?;
            return Ok(self.classify_timed_out(outcome));
        }
        let outcome = self.drive_to_exit_inner().await?;
        // The child is reaped (or being reaped) — the watchdogs' job is done.
        self.abort_watchdogs();
        // Snapshot cancel state NOW (before the ≤5 s pump teardown in the
        // caller): a token that fires during `join_pumps` must not convert a
        // real success into `Err(Cancelled)` (L14 fix). If the token already
        // fired during the run, the select! cancel arm already ran kill_tree,
        // so this snapshot will be `true` and the error is correct.
        //
        // Narrow known race (Issue 7, documented): on the `multi_thread`
        // runtime, another thread could cancel the token in the synchronous
        // window between `abort_watchdogs` returning and the `is_cancelled()`
        // read below. Fully closing it requires the cancel arm of
        // `drive_to_exit_inner` to carry an "exit was due to cancel" flag
        // through the return type, which Phase B (result-shape reshape) enables.
        {
            self.cancel_at_exit =
                Some(self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled()));
        }
        let outcome = self.classify_timed_out(outcome);
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

    /// B1: a run whose deadline fired (set by the streaming `deadline_task`
    /// watchdog or the bulk deadline arm) is `Outcome::TimedOut` regardless of
    /// what `backend_wait` observed — a child that catches the signal and exits
    /// cleanly within the grace still timed out. Deterministic and consistent
    /// across the bulk and streamed paths. Cancellation still wins: it is
    /// classified later in `checked_outcome` (which runs after this).
    fn classify_timed_out(&self, outcome: Outcome) -> Outcome {
        // Acquire pairs with the arbiter's AcqRel `compare_exchange`s. The run is
        // TimedOut iff a deadline won the CAS race against the natural reap (B1).
        if self.timeout_state.load(Ordering::Acquire) == TS_TIMED_OUT {
            Outcome::TimedOut
        } else {
            outcome
        }
    }

    /// The raw exit wait — no timeout/cancel applied. Real: the child's
    /// `wait()`, mapping the exit status to an [`Outcome`] (capturing the Unix
    /// signal number when the platform reports one). Scripted: resolve at the
    /// canned `exit_at` (never, for a pending script); a killed script
    /// resolves immediately as `Signalled`, like a killed child.
    async fn backend_wait(&mut self) -> Result<Outcome> {
        let outcome = match &mut self.backend {
            Backend::Real(real) => {
                let status = real.child.wait().await.map_err(Error::Io)?;
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
            Backend::Scripted(s) => {
                // F5: a kill AFTER the scripted child already exited naturally
                // must still report the cached natural outcome — a real child's
                // exit status survives a post-exit kill. Only an un-exited (or
                // never-exiting `pending`) script that is killed is `Signalled`.
                let already_exited =
                    matches!(s.exit_at, Some(at) if at <= tokio::time::Instant::now());
                let classify = |s: &ScriptedProc| match (s.code, s.timed_out) {
                    (_, true) => Outcome::TimedOut,
                    (Some(code), false) => Outcome::Exited(code),
                    (None, false) => Outcome::Signalled(s.signal),
                };
                if s.kill.killed.load(Ordering::Acquire) && !already_exited {
                    // Killed before its natural exit (cancel/deadline), or a
                    // never-exiting `pending` script that was killed.
                    Outcome::Signalled(None)
                } else if already_exited {
                    // Already past its exit instant: the cached natural outcome,
                    // even if a kill landed afterwards (F5). No kill race here —
                    // a stored `signal` permit must not preempt the real outcome.
                    classify(s)
                } else {
                    match s.exit_at {
                        // F2: race the natural exit against a kill so a streaming
                        // `deadline_task` (which disables the bulk deadline arm in
                        // `drive_to_exit_inner`) can still end this wait — a
                        // not-yet-drained stream finished right after arming would
                        // otherwise park here until the full scripted lifetime.
                        Some(at) => {
                            tokio::select! {
                                biased;
                                () = s.kill.signal.notified() => Outcome::Signalled(None),
                                () = tokio::time::sleep_until(at) => classify(s),
                            }
                        }
                        // Never exits on its own: park until a kill (cancel or a
                        // streaming deadline watchdog) wakes us.
                        None => {
                            s.kill.signal.notified().await;
                            Outcome::Signalled(None)
                        }
                    }
                }
            }
        };
        // B1: claim the natural reap (PENDING -> EXITED). If a streaming deadline
        // watchdog whose timer fired in the same instant already won the race
        // (PENDING -> TIMED_OUT), this CAS fails and the run stays TimedOut — so a
        // child that exits on its own near the deadline is never misclassified.
        let _ = self.timeout_state.compare_exchange(
            TS_PENDING,
            TS_EXITED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        Ok(outcome)
    }

    /// Race the cancellation token against the (deadline-bounded) wait. Unset
    /// knobs become never-resolving arms, so one `select!` covers the whole
    /// timeout × token matrix. The cancel arm does NOT set the outcome to
    /// `TimedOut` — callers classify cancellation via `cancel_at_exit` afterwards.
    ///
    /// `biased` with cancel first ensures cancel always beats a simultaneously-
    /// ready deadline (L4: prevents routing through the graceful teardown tier
    /// when both fire on the same poll, which would delay the promised
    /// immediate hard kill by up to `timeout_grace`).
    async fn drive_to_exit_inner(&mut self) -> Result<Outcome> {
        // Own the knobs so the helper futures borrow nothing from `self` —
        // only `self.backend_wait()` does, keeping the select! borrows disjoint.
        let limit = self.timeout;
        let token = self.cancel_token.clone();
        let started = self.started;
        // E10: when a streaming `deadline_task` watchdog already owns the
        // deadline, disable this select's deadline arm — otherwise the graceful
        // signal is delivered twice (watchdog + here). The watchdog kills at the
        // deadline and sets `timed_out`, which `drive_to_exit` reads to classify.
        let watchdog_owns_deadline = self.deadline_task.is_some();
        let cancelled = async {
            match &token {
                Some(token) => token.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        // Anchor deadline to spawn time: consuming verbs called long after
        // spawn must not re-grant the full limit (B7 fix).
        let deadline = async move {
            match limit {
                Some(limit) if !watchdog_owns_deadline => {
                    let remaining = limit
                        .checked_sub(started.elapsed())
                        .unwrap_or(Duration::ZERO);
                    tokio::time::sleep(remaining).await
                }
                _ => std::future::pending::<()>().await,
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
                // Outcome is Signalled(None): the tree was killed by us (SIGKILL).
                // The caller snapshots `cancel_at_exit` from `is_cancelled()` after
                // this returns; because the token IS cancelled (it fired the arm),
                // the snapshot is always `Some(true)` and `checked_outcome` converts
                // this to `Err(Cancelled)` before the caller ever sees the outcome.
                Ok(Outcome::Signalled(None))
            }
            outcome = self.backend_wait() => outcome,
            () = deadline => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    program = %self.program,
                    timeout_ms = limit.map(|l| l.as_millis() as u64).unwrap_or(0),
                    "timeout elapsed; killing the tree"
                );
                let _ = self.timeout_state.compare_exchange(
                    TS_PENDING,
                    TS_TIMED_OUT,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                self.teardown_on_timeout().await;
                Ok(Outcome::TimedOut)
            }
        }
    }

    /// Hard-kill the child and (for a private group) its tree, then reap —
    /// the shared teardown of the timeout and cancellation arms.
    async fn kill_tree(&mut self) {
        match &mut self.backend {
            Backend::Real(real) => {
                // Best-effort: the child may already be exiting or reaped.
                let _ = real.child.start_kill();
                if let Some(group) = &real.own_group {
                    // Best-effort whole-tree kill; the group's Drop backstops it.
                    let _ = group.terminate_all();
                }
                // Reap after the kill; a wait error here cannot change the
                // outcome the caller is about to report.
                let _ = real.child.wait().await;
            }
            Backend::Scripted(s) => s.kill(),
        }
    }

    /// Teardown when the deadline elapses. With a grace window (`timeout_grace`),
    /// gracefully tear the run down — signal, wait up to the grace, then
    /// `SIGKILL` — reusing the same tier as `ProcessGroup::shutdown`, reaping
    /// concurrently so a child that exits on the signal ends the grace early
    /// instead of looking alive as an unreaped zombie. Without a grace, the hard
    /// `kill_tree`. (Windows has no signal tier: graceful degrades to the atomic
    /// kill.) Cancellation never routes here — it always hard-kills.
    async fn teardown_on_timeout(&mut self) {
        let Some(grace) = self.timeout_grace else {
            self.kill_tree().await;
            return;
        };
        let signal = self.timeout_signal;
        match &mut self.backend {
            Backend::Real(real) => {
                let pid = real.child.id();
                let own = real.own_group.clone();
                let teardown = async {
                    match &own {
                        // Own private group: gracefully tear the whole tree down.
                        Some(group) => {
                            let _ = group.graceful_terminate(grace, signal).await;
                        }
                        // Shared group: gracefully terminate only our direct child.
                        None => {
                            crate::running::stream::graceful_kill_pid(pid, grace, signal).await;
                        }
                    }
                };
                // Reap concurrently so the liveness probe sees a signal-handling
                // child leave, ending the grace early (see ProcessGroup::shutdown).
                let _ = tokio::join!(teardown, real.child.wait());
            }
            Backend::Scripted(s) => s.kill(),
        }
    }

    /// Whether the child has already exited, polled without blocking — the
    /// readiness probes' early-exit check. Aborts watchdogs on true so a
    /// probe-then-idle-handle doesn't leave a stale-pid deadline task running.
    fn has_exited_now(&mut self) -> bool {
        let exited = match &mut self.backend {
            Backend::Real(real) => matches!(real.child.try_wait(), Ok(Some(_))),
            Backend::Scripted(s) => {
                s.kill.killed.load(Ordering::Acquire)
                    || s.exit_at
                        .is_some_and(|at| tokio::time::Instant::now() >= at)
            }
        };
        if exited {
            self.abort_watchdogs();
            // Same snapshot logic as `wait_exit`: a consuming verb called after
            // a probe that observed the exit must not be misclassified as Cancelled
            // by a token that fires in the interim. B2: only on the FIRST reap
            // observation — a repeat probe after a late cancel must not overwrite
            // an existing snapshot and reclassify a natural exit as Cancelled.
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
    /// The [`Outcome`] reported afterwards (by [`wait`](Self::wait) /
    /// [`wait_any`](crate::wait_any)) for a killed child is platform-dependent
    /// — `Outcome::Signalled` on a Unix signal kill, `Outcome::Exited` with a
    /// platform code on Windows `TerminateProcess` (D18: Windows has no signal
    /// abstraction, see [`Outcome::Signalled`](crate::Outcome::Signalled)); a
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner) handle reports
    /// `Outcome::Signalled(None)` (matching Unix).
    ///
    /// **Idempotent (D20):** killing a child that has already exited (and been
    /// reaped — e.g. by a prior [`wait_for_line`](Self::wait_for_line) probe or a
    /// [`wait_any`](crate::wait_any) observation) is a successful no-op, like
    /// `kill` on a Unix zombie — not an error.
    pub fn start_kill(&mut self) -> Result<()> {
        match &mut self.backend {
            Backend::Real(real) => match real.child.start_kill() {
                Ok(()) => {}
                // Defensive (D20): current tokio/std already return `Ok` for a
                // reaped/exited child (the handle is fused to "done"), so this
                // arm is normally unreachable. Should any tokio/std version
                // instead surface `InvalidInput` ("can't kill an exited
                // process"), treat it as the benign no-op it is rather than
                // leaking it as a spurious error. A real failure on a *live*
                // child surfaces as the OS error (e.g. permission denied), never
                // `InvalidInput`, so this can't mask one.
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
        match &mut self.backend {
            Backend::Real(real) => {
                // Abort a still-running stdin writer; a finished one is unaffected.
                if let Some(task) = real.stdin_task.take() {
                    task.abort();
                }
            }
            // Hang up the scripted feeders so no detached writer outlives the
            // handle.
            Backend::Scripted(s) => s.kill(),
        }
        // Abort the streaming deadline timer (it holds only a `Weak` to the group,
        // so this never blocks the group's kill-on-close).
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        // Likewise the streaming cancellation listener.
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
        // Abort streaming output pumps. For a private-group handle the tree kill
        // (above) closes the pipes promptly, so these tasks are already near EOF;
        // abort is a cheap backstop. For a shared-group handle the group is NOT
        // torn down on drop, so a surviving grandchild holding the pipe could keep
        // a pump alive indefinitely without this abort.
        if let Some(task) = self.stdout_pump.take() {
            task.abort();
        }
        if let Some(task) = self.stderr_pump.take() {
            task.abort();
        }
    }
}

/// Whether `e` is the routine pipe-closed write error — `BrokenPipe`, plus the
/// raw Windows encodings (`ERROR_BROKEN_PIPE` = 109, `ERROR_NO_DATA` = 232)
/// that don't always map to the kind.
fn is_broken_pipe(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::BrokenPipe || matches!(e.raw_os_error(), Some(109 | 232))
}

/// Await the output pumps, bounded by [`PUMP_TEARDOWN`]; abort stragglers.
async fn join_pumps(tasks: Vec<JoinHandle<()>>) {
    if tasks.is_empty() {
        return;
    }
    let aborts: Vec<_> = tasks.iter().map(|t| t.abort_handle()).collect();
    let join = async {
        for task in tasks {
            // A pump that panicked (e.g. a panicking user line-handler) has
            // already closed its sink via its close-on-drop guard, so partial
            // output is intact — the documented contract. Surface the panic
            // for diagnostics, never as a run error.
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

    /// B3 (Decision 2): a stashed non-broken-pipe stdin failure surfaces as
    /// `Error::Stdin` only on an otherwise-successful outcome; a non-zero exit
    /// or a signal is the "realer" failure and wins (outcome passed through).
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

    /// With no stashed stdin error, `checked_outcome` is a clean passthrough.
    #[tokio::test]
    async fn no_stdin_error_is_a_clean_passthrough() {
        let mut run = scripted_handle(&[0]).await;
        assert!(matches!(
            run.checked_outcome(Outcome::Exited(0)),
            Ok(Outcome::Exited(0))
        ));
    }

    /// B4: `output_string` after a partial `stdout_lines` stream must NOT report
    /// truncation under the default unbounded policy — the consumed lines were
    /// popped by the stream, not discarded by the buffer. (The old `count() >
    /// retained` test conflated the two and falsely flagged truncation.)
    #[tokio::test]
    async fn output_string_after_partial_stream_is_not_truncated() {
        use tokio_stream::StreamExt;

        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");

        // Consume the first two lines via the stream.
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

    /// B3: `output_bytes` after a streaming call must fail loudly. The raw stdout
    /// bytes were already taken and pumped as decoded lines, so they cannot be
    /// reconstructed; the old code silently returned empty output (and clobbered
    /// the streamed stderr sink).
    #[tokio::test]
    async fn output_bytes_after_streaming_errors_instead_of_empty() {
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b"]))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");

        // A streaming call consumes stdout as lines (sets both sinks).
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

    /// B4 (other direction): a bounded buffer that genuinely discards lines
    /// during streaming must STILL report `truncated=true` — the fix must narrow
    /// only the false positive (consumed-by-stream), never mask real truncation.
    /// Filling `bounded(2)` with four un-consumed lines drops two deterministically.
    #[tokio::test]
    async fn output_string_after_stream_still_reports_real_truncation() {
        let cmd = Command::new("tool").output_buffer(OutputBufferPolicy::bounded(2));
        let mut run = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b", "c", "d"]))
            .start(&cmd)
            .await
            .expect("scripted start");

        // Set up the stream but consume nothing, so the pump fills the bounded
        // buffer and the policy drops the two oldest lines.
        drop(run.stdout_lines().unwrap());

        let result = run.output_string().await.expect("output_string");
        assert!(
            result.truncated(),
            "a bounded buffer that dropped lines during streaming must report truncation: {result:?}"
        );
    }

    /// B3 happy path (now hermetic): `output_bytes` with no prior streaming
    /// returns the EXACT raw stdout bytes — a NUL byte and a missing trailing
    /// newline prove it is not line-processed.
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

    /// B1 (core): once the `timed_out` flag is set (as the streaming deadline
    /// watchdog does when it fires), a consuming verb classifies the run as
    /// `Outcome::TimedOut` even though `backend_wait` observed a clean exit 0 —
    /// the same deterministic contract the bulk `output_string` path already had.
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

    /// Cancellation still wins over a timed-out run: `checked_outcome` classifies
    /// cancellation after `classify_timed_out`, so a run that both timed out and
    /// was cancelled surfaces as `Err(Cancelled)` (cancellation is terminal).
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

    /// B1 (wait path): a streamed run whose deadline fired must report
    /// `Outcome::TimedOut` through `wait_any`/`wait_all` too — `wait_exit` applies
    /// `classify_timed_out` just like `drive_to_exit`, so the streamed-then-raced
    /// composition (`stdout_lines` → `wait_any`) is consistent with `finish`.
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

    /// B1: the timeout arbiter is race-free. Once the natural reap claims
    /// `EXITED`, a watchdog whose timer fires late cannot flip the run to
    /// `TimedOut` (its CAS from `PENDING` fails), so a child that exits on its own
    /// within a scheduler quantum of the deadline keeps its real outcome. (The
    /// reverse — the deadline claiming `TIMED_OUT` first — is covered by
    /// `timed_out_flag_classifies_a_clean_exit_as_timed_out`.)
    #[tokio::test]
    async fn natural_reap_claim_beats_a_late_timeout_cas() {
        let run = scripted_handle(&[0]).await;
        // The finisher reaps first and claims EXITED.
        assert!(
            run.timeout_state
                .compare_exchange(TS_PENDING, TS_EXITED, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        );
        // A late watchdog tries to claim the timeout — it must lose.
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
        // classify therefore preserves the real exit, not TimedOut.
        assert_eq!(
            run.classify_timed_out(Outcome::Exited(0)),
            Outcome::Exited(0)
        );
    }

    /// D10: a scripted double owns no private group, so it never kills a tree on
    /// drop (the real own-group vs shared-group distinction is covered by the
    /// integration suite, which needs real subprocesses).
    #[tokio::test]
    async fn scripted_handle_does_not_kill_a_tree_on_drop() {
        let run = scripted_handle(&[0]).await;
        assert!(
            !run.kills_tree_on_drop(),
            "a scripted double has no OS tree to tear down"
        );
    }

    /// D5: a bulk capture verb on a non-piped stdout (`Inherit`/`Null`) must fail
    /// loudly instead of returning silently-empty output; the discard verbs
    /// (`wait`) are exempt.
    #[tokio::test]
    async fn capture_verbs_error_on_a_non_piped_stdout() {
        let runner = ScriptedRunner::new().fallback(Reply::ok("ignored"));

        // output_string on a Null stdout → Io(InvalidInput).
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

        // The default piped stdout still captures.
        let run = ScriptedRunner::new()
            .fallback(Reply::ok("hi"))
            .start(&Command::new("tool"))
            .await
            .unwrap();
        assert_eq!(run.output_string().await.unwrap().stdout(), "hi");

        // wait() discards output, so a non-piped stdout is fine there.
        let run = runner
            .start(&Command::new("tool").stdout(crate::StdioMode::Null))
            .await
            .unwrap();
        assert!(
            run.wait().await.is_ok(),
            "discard verbs do not require a piped stdout"
        );
    }
}
