//! [`RunningProcess`] — a live handle to a spawned child.
//!
//! Split by concern: this file owns the handle's state and the consuming
//! capture paths (exit driving, kill/teardown, the post-exit checkpoint);
//! [`probes`] holds the non-consuming readiness probes; [`stream`] holds the
//! incremental stdout streaming surface.

mod probes;
mod stream;

pub use stream::{OutputEvent, OutputEvents, StdoutLines, StreamedFinish};

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use encoding_rs::Encoding;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;

use crate::buffer::OutputBufferPolicy;
#[cfg(feature = "cancellation")]
use crate::error::Error;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::pump::{LineHandler, SharedLines, pump_lines};
use crate::result::{Outcome, ProcessResult};
use crate::stdin::ProcessStdin;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

/// What [`RunningProcess::finish_lines`] hands back to its thin public verbs.
struct Finished {
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
    pub buffer: OutputBufferPolicy,
    /// Exit codes treated as success (default `[0]`), carried onto the result.
    pub ok_codes: Vec<i32>,
    #[cfg(feature = "cancellation")]
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
/// [`standard_input`](Self::standard_input).
pub struct RunningProcess {
    // (Debug: manual impl below — pipes/tasks/handlers are opaque.)
    //
    // The Option fields below encode the handle's de-facto states (fresh /
    // streaming / consumed) implicitly. No runtime state enum on purpose:
    // every consuming verb takes `self` BY VALUE (double consumption is a
    // compile error), and the two &mut entry points (`stdout_lines`,
    // `standard_input`) have explicit, tested, non-panicking handling for
    // repeat calls (`second_stdout_lines_call_ends_immediately`,
    // `finish_streamed_without_streaming_first…`). A state enum would add
    // panic paths to guard doors the borrow checker already locks.
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
    buffer: OutputBufferPolicy,
    ok_codes: Vec<i32>,
    stdout_sink: Option<Arc<SharedLines>>,
    stderr_sink: Option<Arc<SharedLines>>,
    // The background stdout-pump task started by `output_events`, joined by
    // `finish_events` before the overflow check (ensures the pump has written
    // its last lines before `overflowed()` is queried).
    stdout_pump: Option<JoinHandle<()>>,
    // The background stderr-drain task started by `stdout_lines`/`output_events`,
    // awaited by `finish_streamed`/`finish_events` so no trailing line is missed.
    stderr_pump: Option<JoinHandle<()>>,
    // A timer started by `stdout_lines` when a timeout is set: kills the tree at
    // the deadline so a streamed run can't hang forever. Aborted on drop.
    deadline_task: Option<JoinHandle<()>>,
    #[cfg(feature = "cancellation")]
    cancel_token: Option<tokio_util::sync::CancellationToken>,
    // Armed by `arm_cancel_watchdog` at spawn time (via `launch`/`attach_group`)
    // so that *every* consuming path — including `wait_any`, probes, and pure
    // streaming — kills the tree when the token fires, not just `drive_to_exit`.
    #[cfg(feature = "cancellation")]
    cancel_task: Option<JoinHandle<()>>,
    // Snapshotted at the first reap (by `wait_exit`, `has_exited_now`, or
    // `drive_to_exit`) from the live token so no later cancel can reclassify a
    // natural exit. `None` = not yet snapshotted; `Some(v)` = snapshotted.
    // `drive_to_exit` short-circuits (skipping the cancel/deadline select) when
    // already `Some`, preserving the snapshot taken at the true reap point.
    #[cfg(feature = "cancellation")]
    cancel_at_exit: Option<bool>,
    started: Instant,
    start_time: SystemTime,
}

/// A boxed output reader the pumps consume — a real `ChildStdout`/`ChildStderr`
/// or a scripted in-memory stream; `pump_lines` is generic over `AsyncRead`,
/// so both flow through the *same* machinery.
type OutputReader = Box<dyn tokio::io::AsyncRead + Send + Unpin>;

/// The I/O-bearing half of a [`RunningProcess`]: a real OS child, or a
/// scripted double ([`ScriptedRunner::start`](crate::ScriptedRunner)) that
/// feeds canned bytes through the same pumps/sinks — which is what makes
/// streaming, probes, and `finish_streamed` hermetically testable. Platform
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

/// A scripted "child": canned output readers (fed by detached writer tasks so
/// per-line delays work under a paused clock) plus a canned exit.
pub(crate) struct ScriptedProc {
    /// Canned stdout/stderr, taken once like real pipes.
    stdout: Option<tokio::io::DuplexStream>,
    stderr: Option<tokio::io::DuplexStream>,
    /// The writer tasks feeding the duplex streams; aborted on kill/drop
    /// (dropping the writer EOFs the reader, ending pumps and streams).
    feeders: Vec<JoinHandle<()>>,
    /// Canned exit: code + timed-out flag + optional signal number.
    code: Option<i32>,
    timed_out: bool,
    signal: Option<i32>,
    /// When the scripted child "exits": `Some(at)` resolves at that instant
    /// (now = immediately), `None` never exits on its own (`Reply::pending` —
    /// cancel/timeout still end it).
    exit_at: Option<tokio::time::Instant>,
    /// Set by `kill_tree`/`start_kill`: the scripted child is dead now.
    killed: bool,
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
            feeders.push(tokio::spawn(async move {
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
            }));
            rx
        };
        let stdout = feed(stdout_text);
        let stderr = feed(stderr_text);
        Self {
            stdout: Some(stdout),
            stderr: Some(stderr),
            feeders,
            code,
            timed_out,
            signal,
            exit_at: lifetime.map(|d| tokio::time::Instant::now() + d),
            killed: false,
        }
    }

    /// The scripted kill: mark dead and hang up the feeders (aborting a
    /// writer drops its end, EOF-ing the matching reader — pumps and streams
    /// end exactly as when a real tree dies and its pipes close).
    fn kill(&mut self) {
        self.killed = true;
        for task in self.feeders.drain(..) {
            task.abort();
        }
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
            buffer: s.buffer,
            ok_codes: s.ok_codes,
            stdout_sink: None,
            stderr_sink: None,
            stdout_pump: None,
            stderr_pump: None,
            deadline_task: None,
            #[cfg(feature = "cancellation")]
            cancel_token: s.cancel_token,
            #[cfg(feature = "cancellation")]
            cancel_task: None,
            #[cfg(feature = "cancellation")]
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
            buffer: command.output_buffer_policy(),
            ok_codes: command.ok_codes_vec(),
            stdout_sink: None,
            stderr_sink: None,
            stdout_pump: None,
            stderr_pump: None,
            deadline_task: None,
            #[cfg(feature = "cancellation")]
            cancel_token: command.cancel_token(),
            #[cfg(feature = "cancellation")]
            cancel_task: None,
            #[cfg(feature = "cancellation")]
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
        #[cfg(feature = "cancellation")]
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

    /// Take the raw stdout pipe — the [`Pipeline`](crate::Pipeline) plumbing
    /// that feeds it into the next stage's stdin. Afterwards this handle can
    /// still report exit + stderr via [`finish_streamed`](Self::finish_streamed)
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
    /// ```no_run
    /// use processkit::{Command, StreamExt};
    ///
    /// # async fn demo() -> processkit::Result<()> {
    /// // `bc` evaluates each stdin line and prints the result on stdout.
    /// let mut run = Command::new("bc").keep_stdin_open().start().await?;
    ///
    /// let mut stdin = run.standard_input().expect("stdin was kept open");
    /// stdin.write_line("2 + 2").await?;
    /// stdin.write_line("6 * 7").await?;
    /// stdin.finish().await?; // send EOF so bc finishes
    ///
    /// let mut answers = run.stdout_lines();
    /// while let Some(line) = answers.next().await {
    ///     println!("bc says: {line}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn standard_input(&mut self) -> Option<ProcessStdin> {
        match &mut self.backend {
            Backend::Real(real) => real.stdin_pipe.take().map(ProcessStdin::new),
            // Scripted doubles don't model interactive stdin (yet): the
            // writer would need a scripted reader on the other end. `None`
            // matches the "stdin wasn't kept open" contract.
            Backend::Scripted(_) => None,
        }
    }

    /// Drain both streams, wait for exit, and return the captured text output
    /// (line-normalized to `\n`).
    pub async fn output_string(mut self) -> Result<ProcessResult<String>> {
        let finished = self
            .finish_lines(CaptureMode::Lines, /* expose_counts */ true, || {})
            .await?;
        // `count > retained` on either sink means a bounded buffer dropped lines
        // (the counters tally every line; the sinks were exposed by finish_lines).
        let truncated = self
            .stdout_sink
            .as_ref()
            .is_some_and(|s| s.count() > finished.stdout_lines.len())
            || self
                .stderr_sink
                .as_ref()
                .is_some_and(|s| s.count() > finished.stderr_lines.len());
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
        .with_ok_codes(self.ok_codes.clone()))
    }

    /// Drain both streams, wait for exit, and return the raw stdout bytes
    /// (exact; stderr is captured as text).
    ///
    /// Deliberately NOT routed through `finish_lines`: stdout is a raw byte
    /// reader (no line pump), with its own bounded drain-then-abort teardown.
    pub async fn output_bytes(mut self) -> Result<ProcessResult<Vec<u8>>> {
        let stderr_sink = SharedLines::new(&self.buffer);
        let err_pump = self.backend.take_stderr_reader().map(|pipe| {
            tokio::spawn(pump_lines(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                stderr_sink.clone(),
            ))
        });
        self.stderr_sink = Some(stderr_sink.clone());

        // Read stdout raw, concurrently, so it never blocks the child. The
        // bytes accumulate in a shared buffer (not the task's return value) so
        // the bounded teardown below can salvage a partial read.
        let mut stdout_pipe = self.backend.take_stdout_reader();
        let out_buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let out_task = {
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
        };

        let outcome = self.drive_to_exit().await?;
        self.observe_stdin_task().await;
        // Bound the drain by the same teardown grace as the line pumps: on a
        // shared-group handle a surviving descendant can hold stdout open past
        // the child's death, and an unbounded `read_to_end` here would park
        // this call forever (`output_string`/`wait` are bounded via
        // `join_pumps` — `output_bytes` must be too).
        let abort = out_task.abort_handle();
        if tokio::time::timeout(PUMP_TEARDOWN, out_task).await.is_err() {
            // The reader is still parked on a held-open pipe: abort it (like
            // `join_pumps` aborts stragglers) and keep whatever arrived —
            // parity with the line pumps' partial capture.
            abort.abort();
        }
        let stdout = std::mem::take(&mut *out_buf.lock().expect("stdout buffer poisoned"));
        join_pumps(err_pump.into_iter().collect()).await;
        let outcome = self.checked_outcome(outcome)?;

        // Fail-loud ceiling check for the line-pumped stderr.
        if stderr_sink.overflowed() {
            return Err(crate::Error::OutputTooLarge {
                program: self.program.clone(),
                limit: self.buffer.max_lines.unwrap_or(0),
                total_lines: stderr_sink.count(),
            });
        }

        // stdout is raw bytes (not line-buffered), so only the line-pumped stderr
        // can be truncated by the buffer policy here.
        let stderr_lines = stderr_sink.drain();
        let truncated = stderr_sink.count() > stderr_lines.len();
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

    /// Minimal non-consuming exit wait — the [`wait_any`](crate::wait_any) race
    /// participant. Unlike [`wait`](Self::wait) it spawns no pumps and applies
    /// no [`timeout`](crate::Command::timeout). Cancel-safe and re-awaitable:
    /// tokio caches the exit status, so a raced-and-cancelled process can be
    /// waited again (or consumed normally) afterwards.
    ///
    /// Aborts watchdog tasks after reap to prevent late-firing deadline/cancel
    /// tasks from sending signals to a recycled pid (B1/B2 fix).
    pub(crate) async fn wait_exit(&mut self) -> Result<Outcome> {
        let outcome = self.backend_wait().await?;
        // Reap happened: abort watchdogs and clear pid before returning.
        // This mirrors the `abort_watchdogs` call in `drive_to_exit` and
        // prevents a streaming deadline/cancel task from waking up minutes
        // later and killing an unrelated process that recycled this pid.
        self.abort_watchdogs();
        // Snapshot the cancel state at the true reap point (before any pump
        // teardown or caller code runs). A consuming verb called on the winner
        // after `wait_any` must see this snapshot — not re-query the live
        // token — so a cancel that fires after natural exit doesn't convert
        // success to `Err(Cancelled)` (Issue 1 / L14 companion fix).
        #[cfg(feature = "cancellation")]
        {
            self.cancel_at_exit =
                Some(self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled()));
        }
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
    /// `finish_streamed` (already-streaming state, late stderr pump)
    /// deliberately do NOT route through this core — their spines differ by
    /// nature, not by copy-paste.
    async fn finish_lines(
        &mut self,
        capture: CaptureMode,
        expose_counts: bool,
        on_exit: impl FnOnce(),
    ) -> Result<Finished> {
        let stdout_sink = SharedLines::new(&self.buffer);
        let stderr_sink = SharedLines::new(&self.buffer);
        let pumps = self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        if expose_counts {
            self.stdout_sink = Some(stdout_sink.clone());
            self.stderr_sink = Some(stderr_sink.clone());
        }

        let outcome = self.drive_to_exit().await;
        on_exit();
        let outcome = outcome?;
        self.observe_stdin_task().await;
        join_pumps(pumps).await;
        let outcome = self.checked_outcome(outcome)?;

        // Fail-loud ceiling: OverflowMode::Error hit during pumping.
        for sink in [&stdout_sink, &stderr_sink] {
            if sink.overflowed() {
                return Err(crate::Error::OutputTooLarge {
                    program: self.program.clone(),
                    limit: self.buffer.max_lines.unwrap_or(0),
                    total_lines: sink.count(),
                });
            }
        }

        let (stdout_lines, stderr_lines) = match capture {
            CaptureMode::Lines => (stdout_sink.drain(), stderr_sink.drain()),
            CaptureMode::Discard => (Vec::new(), Vec::new()),
        };
        Ok(Finished {
            outcome,
            stdout_lines,
            stderr_lines,
        })
    }

    /// Spawn line pumps for both streams into the given sinks; returns their
    /// task handles.
    fn spawn_line_pumps(
        &mut self,
        stdout_sink: &Arc<SharedLines>,
        stderr_sink: &Arc<SharedLines>,
    ) -> Vec<JoinHandle<()>> {
        let mut tasks = Vec::new();
        if let Some(pipe) = self.backend.take_stdout_reader() {
            tasks.push(tokio::spawn(pump_lines(
                pipe,
                self.stdout_encoding,
                self.stdout_handler.clone(),
                stdout_sink.clone(),
            )));
        }
        if let Some(pipe) = self.backend.take_stderr_reader() {
            tasks.push(tokio::spawn(pump_lines(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                stderr_sink.clone(),
            )));
        }
        tasks
    }

    /// The single post-exit checkpoint **every consuming path passes
    /// through** after its pumps settle: folds in the cancellation gate — a
    /// cancelled run is *always* an error, and the check runs before any
    /// outcome classification, so cancellation beats a simultaneous timeout.
    /// Centralizing it here makes the documented invariant structural instead
    /// of per-consumer copy-paste discipline.
    fn checked_outcome(&self, outcome: Outcome) -> Result<Outcome> {
        // Use the pre-pump snapshot rather than a live token read: prevents
        // a cancel that fires during `join_pumps` from discarding real output.
        // `unwrap_or(false)`: None means not yet snapshotted — only reachable
        // if a future code path calls checked_outcome without drive_to_exit,
        // which is not possible today; treat as "not cancelled" conservatively.
        #[cfg(feature = "cancellation")]
        if self.cancel_at_exit.unwrap_or(false) {
            return Err(Error::Cancelled {
                program: self.program.clone(),
            });
        }
        Ok(outcome)
    }

    /// Surface a stdin writer that failed for a reason other than the normal
    /// broken pipe (the child exiting before reading all of stdin is routine
    /// and tested). Only a writer that already **finished** is observed — a
    /// task still parked (e.g. on a slow `from_reader` source) is left for
    /// `Drop`'s abort, so teardown timing is unchanged. Diagnostics only:
    /// never alters the run's result.
    async fn observe_stdin_task(&mut self) {
        let Backend::Real(real) = &mut self.backend else {
            return;
        };
        let Some(task) = real.stdin_task.take() else {
            return;
        };
        if !task.is_finished() {
            real.stdin_task = Some(task);
            return;
        }
        // The task is finished, so this await is immediate.
        match task.await {
            Ok(Err(e)) if !is_broken_pipe(&e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: "processkit",
                    program = %self.program,
                    error = %e,
                    "stdin writer failed"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = e;
            }
            // Clean completion, the routine broken pipe, or an abort.
            _ => {}
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
        #[cfg(feature = "cancellation")]
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
        // writer the caller did take via `standard_input()` is unaffected —
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
        #[cfg(feature = "cancellation")]
        if self.cancel_at_exit.is_some() {
            return self.backend_wait().await;
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
        #[cfg(feature = "cancellation")]
        {
            self.cancel_at_exit =
                Some(self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled()));
        }
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

    /// The raw exit wait — no timeout/cancel applied. Real: the child's
    /// `wait()`, mapping the exit status to an [`Outcome`] (capturing the Unix
    /// signal number when the platform reports one). Scripted: resolve at the
    /// canned `exit_at` (never, for a pending script); a killed script
    /// resolves immediately as `Signalled`, like a killed child.
    async fn backend_wait(&mut self) -> Result<Outcome> {
        match &mut self.backend {
            Backend::Real(real) => {
                let status = real.child.wait().await?;
                let outcome = match status.code() {
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
                };
                Ok(outcome)
            }
            Backend::Scripted(s) => {
                if s.killed {
                    return Ok(Outcome::Signalled(None));
                }
                match s.exit_at {
                    Some(at) => {
                        tokio::time::sleep_until(at).await;
                        Ok(match (s.code, s.timed_out) {
                            (_, true) => Outcome::TimedOut,
                            (Some(code), false) => Outcome::Exited(code),
                            (None, false) => Outcome::Signalled(s.signal),
                        })
                    }
                    None => std::future::pending().await,
                }
            }
        }
    }

    /// Without the `cancellation` feature: the plain timeout/no-timeout shape.
    #[cfg(not(feature = "cancellation"))]
    async fn drive_to_exit_inner(&mut self) -> Result<Outcome> {
        match self.timeout {
            Some(limit) => {
                // Anchor the deadline to spawn time (`self.started`): consuming
                // verbs called long after spawn must not re-grant the full limit.
                let remaining = limit
                    .checked_sub(self.started.elapsed())
                    .unwrap_or(Duration::ZERO);
                let waited = {
                    let wait = self.backend_wait();
                    tokio::pin!(wait);
                    tokio::time::timeout(remaining, &mut wait).await
                };
                match waited {
                    Ok(outcome) => outcome,
                    Err(_elapsed) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: "processkit",
                            program = %self.program,
                            timeout_ms = limit.as_millis() as u64,
                            "timeout elapsed; killing the tree"
                        );
                        self.teardown_on_timeout().await;
                        Ok(Outcome::TimedOut)
                    }
                }
            }
            None => self.backend_wait().await,
        }
    }

    /// With the feature: race the cancellation token against the
    /// (deadline-bounded) wait. Unset knobs become never-resolving arms, so one
    /// `select!` covers the whole timeout × token matrix. The cancel arm does
    /// NOT set the outcome to `TimedOut` — callers classify cancellation via
    /// `cancel_at_exit` afterwards.
    ///
    /// `biased` with cancel first ensures cancel always beats a simultaneously-
    /// ready deadline (L4: prevents routing through the graceful teardown tier
    /// when both fire on the same poll, which would delay the promised
    /// immediate hard kill by up to `timeout_grace`).
    #[cfg(feature = "cancellation")]
    async fn drive_to_exit_inner(&mut self) -> Result<Outcome> {
        // Own the knobs so the helper futures borrow nothing from `self` —
        // only `self.backend_wait()` does, keeping the select! borrows disjoint.
        let limit = self.timeout;
        let token = self.cancel_token.clone();
        let started = self.started;
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
                Some(limit) => {
                    let remaining = limit
                        .checked_sub(started.elapsed())
                        .unwrap_or(Duration::ZERO);
                    tokio::time::sleep(remaining).await
                }
                None => std::future::pending::<()>().await,
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
                s.killed
                    || s.exit_at
                        .is_some_and(|at| tokio::time::Instant::now() >= at)
            }
        };
        if exited {
            self.abort_watchdogs();
            // Same snapshot logic as `wait_exit`: a consuming verb called after
            // a probe that observed the exit must not be misclassified as Cancelled
            // by a token that fires in the interim.
            #[cfg(feature = "cancellation")]
            {
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
    /// platform code on Windows `TerminateProcess`; a
    /// [`ScriptedRunner`](crate::ScriptedRunner) handle reports
    /// `Outcome::Signalled(None)` (matching Unix).
    pub fn start_kill(&mut self) -> Result<()> {
        match &mut self.backend {
            Backend::Real(real) => {
                real.child.start_kill()?;
            }
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
        #[cfg(feature = "cancellation")]
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
