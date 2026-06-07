//! [`RunningProcess`] — a live handle to a spawned child.
//!
//! Split by concern: this file owns the handle's state and the consuming
//! capture paths (exit driving, kill/teardown, the post-exit checkpoint);
//! [`probes`] holds the non-consuming readiness probes; [`stream`] holds the
//! incremental stdout streaming surface.

mod probes;
mod stream;

pub use stream::StdoutLines;

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
use crate::result::ProcessResult;
use crate::stdin::ProcessStdin;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

/// What [`RunningProcess::finish_lines`] hands back to its thin public verbs.
struct Finished {
    code: Option<i32>,
    timed_out: bool,
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
    pub pid: Option<u32>,
    pub stdout_encoding: &'static Encoding,
    pub stderr_encoding: &'static Encoding,
    pub stdout_handler: Option<LineHandler>,
    pub stderr_handler: Option<LineHandler>,
    pub buffer: OutputBufferPolicy,
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
    child: Child,
    // `Arc` so a streaming deadline timer can hold a `Weak` to kill the tree
    // without keeping the group alive (kill-on-close on drop stays prompt).
    own_group: Option<Arc<ProcessGroup>>,
    stdout_pipe: Option<ChildStdout>,
    stderr_pipe: Option<ChildStderr>,
    stdin_pipe: Option<ChildStdin>,
    stdin_task: Option<JoinHandle<std::io::Result<()>>>,
    timeout: Option<Duration>,
    pid: Option<u32>,
    stdout_encoding: &'static Encoding,
    stderr_encoding: &'static Encoding,
    stdout_handler: Option<LineHandler>,
    stderr_handler: Option<LineHandler>,
    buffer: OutputBufferPolicy,
    stdout_sink: Option<Arc<SharedLines>>,
    stderr_sink: Option<Arc<SharedLines>>,
    // The background stderr-drain task started by `stdout_lines`, awaited by
    // `finish_streamed` so no trailing stderr line is missed.
    stderr_pump: Option<JoinHandle<()>>,
    // A timer started by `stdout_lines` when a timeout is set: kills the tree at
    // the deadline so a streamed run can't hang forever. Aborted on drop.
    deadline_task: Option<JoinHandle<()>>,
    #[cfg(feature = "cancellation")]
    cancel_token: Option<tokio_util::sync::CancellationToken>,
    // Armed by `stdout_lines` when a token is set: kills the tree on cancel so
    // a pure-streaming consumer's stream ends. Mirrors `deadline_task` (Weak
    // to the group; aborted on drop).
    #[cfg(feature = "cancellation")]
    cancel_task: Option<JoinHandle<()>>,
    started: Instant,
    start_time: SystemTime,
}

impl RunningProcess {
    pub(crate) fn from_spawned(s: Spawned) -> Self {
        Self {
            program: s.program,
            child: s.child,
            own_group: s.own_group.map(Arc::new),
            stdout_pipe: s.stdout,
            stderr_pipe: s.stderr,
            stdin_pipe: s.stdin,
            stdin_task: s.stdin_task,
            timeout: s.timeout,
            pid: s.pid,
            stdout_encoding: s.stdout_encoding,
            stderr_encoding: s.stderr_encoding,
            stdout_handler: s.stdout_handler,
            stderr_handler: s.stderr_handler,
            buffer: s.buffer,
            stdout_sink: None,
            stderr_sink: None,
            stderr_pump: None,
            deadline_task: None,
            #[cfg(feature = "cancellation")]
            cancel_token: s.cancel_token,
            #[cfg(feature = "cancellation")]
            cancel_task: None,
            started: Instant::now(),
            start_time: SystemTime::now(),
        }
    }

    pub(crate) fn attach_group(&mut self, group: ProcessGroup) {
        self.own_group = Some(Arc::new(group));
    }

    /// Take the raw stdout pipe — the [`Pipeline`](crate::Pipeline) plumbing
    /// that feeds it into the next stage's stdin. Afterwards this handle can
    /// still report exit + stderr via [`finish_streamed`](Self::finish_streamed)
    /// (which tolerates a taken stdout), like after `stdout_lines`.
    pub(crate) fn take_stdout_pipe(&mut self) -> Option<ChildStdout> {
        self.stdout_pipe.take()
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
        self.stdin_pipe.take().map(ProcessStdin::new)
    }

    /// Drain both streams, wait for exit, and return the captured text output
    /// (line-normalized to `\n`).
    pub async fn output_string(mut self) -> Result<ProcessResult<String>> {
        let finished = self
            .finish_lines(CaptureMode::Lines, /* expose_counts */ true, || {})
            .await?;
        Ok(ProcessResult::new(
            self.program.clone(),
            finished.stdout_lines.join("\n"),
            finished.stderr_lines.join("\n"),
            finished.code,
            finished.timed_out,
            self.timeout,
        ))
    }

    /// Drain both streams, wait for exit, and return the raw stdout bytes
    /// (exact; stderr is captured as text).
    ///
    /// Deliberately NOT routed through `finish_lines`: stdout is a raw byte
    /// reader (no line pump), with its own bounded drain-then-abort teardown.
    pub async fn output_bytes(mut self) -> Result<ProcessResult<Vec<u8>>> {
        let stderr_sink = SharedLines::new(&self.buffer);
        let err_pump = self.stderr_pipe.take().map(|pipe| {
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
        let mut stdout_pipe = self.stdout_pipe.take();
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

        let (code, timed_out) = self.drive_to_exit().await?;
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
        let (code, timed_out) = self.checked_outcome((code, timed_out))?;

        Ok(ProcessResult::new(
            self.program.clone(),
            stdout,
            stderr_sink.drain().join("\n"),
            code,
            timed_out,
            self.timeout,
        ))
    }

    /// Wait for exit, returning just the exit code (output is drained and
    /// discarded so the child never blocks on a full pipe).
    ///
    /// This low-level handle method reports the **raw** outcome: a run killed by
    /// its timeout (or by a signal) returns `None` (it is not raised as an
    /// error). For the timeout-aware behavior use the one-shot helpers
    /// ([`Command::exit_code`](crate::Command::exit_code) /
    /// [`ProcessRunnerExt::exit_code`](crate::ProcessRunnerExt::exit_code)), which
    /// surface a deadline as [`Error::Timeout`](crate::Error::Timeout).
    /// One exception: a run cancelled via its token (`Command::cancel_on`)
    /// errors with `Error::Cancelled` here too — cancellation is always an
    /// error, on every consuming path.
    pub async fn wait(mut self) -> Result<Option<i32>> {
        Ok(self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, || {})
            .await?
            .code)
    }

    /// Minimal non-consuming exit wait — the [`wait_any`](crate::wait_any) race
    /// participant. Unlike [`wait`](Self::wait) it spawns no pumps and applies
    /// no [`timeout`](crate::Command::timeout). Cancel-safe and re-awaitable:
    /// tokio caches the exit status, so a raced-and-cancelled process can be
    /// waited again (or consumed normally) afterwards.
    pub(crate) async fn wait_exit(&mut self) -> Result<Option<i32>> {
        Ok(self.child.wait().await?.code())
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

        // The `on_exit` hook stops the sampler the moment the child is reaped:
        // its pid is free for reuse from that point (Linux), and the pump
        // drain can idle out PUMP_TEARDOWN on a leaked pipe — long enough for
        // a recycled pid to masquerade as the child and corrupt the readings.
        let exit_code = self
            .finish_lines(CaptureMode::Discard, /* expose_counts */ false, || {
                if let Some(task) = &sampler {
                    task.abort();
                }
            })
            .await?
            .code;
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
        join_pumps(pumps).await;
        let (code, timed_out) = self.checked_outcome(outcome)?;

        let (stdout_lines, stderr_lines) = match capture {
            CaptureMode::Lines => (stdout_sink.drain(), stderr_sink.drain()),
            CaptureMode::Discard => (Vec::new(), Vec::new()),
        };
        Ok(Finished {
            code,
            timed_out,
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
        if let Some(pipe) = self.stdout_pipe.take() {
            tasks.push(tokio::spawn(pump_lines(
                pipe,
                self.stdout_encoding,
                self.stdout_handler.clone(),
                stdout_sink.clone(),
            )));
        }
        if let Some(pipe) = self.stderr_pipe.take() {
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
    /// `timed_out` classification, so cancellation beats a simultaneous
    /// timeout. Centralizing it here makes the documented invariant
    /// structural instead of per-consumer copy-paste discipline.
    fn checked_outcome(&self, outcome: (Option<i32>, bool)) -> Result<(Option<i32>, bool)> {
        #[cfg(feature = "cancellation")]
        if let Some(err) = self.cancelled_error() {
            return Err(err);
        }
        Ok(outcome)
    }

    /// Stop the streaming watchdog tasks (deadline/cancel) once the child's
    /// fate is settled — mirroring the `profile` sampler's early abort, so a
    /// late firing can't `kill_direct_child` a pid the consuming method has
    /// already reaped. (`Drop` remains the backstop for non-consuming exits.)
    fn abort_watchdogs(&mut self) {
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        #[cfg(feature = "cancellation")]
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
    }

    /// Wait for the child to exit, applying the timeout (killing the tree and
    /// flagging `timed_out` on elapse). The code is `None` for a run that
    /// produced none — a timeout, or a signal termination on Unix.
    async fn drive_to_exit(&mut self) -> Result<(Option<i32>, bool)> {
        let outcome = self.drive_to_exit_inner().await?;
        // The child is reaped (or being reaped) — the watchdogs' job is done.
        self.abort_watchdogs();
        #[cfg(feature = "tracing")]
        {
            let (code, timed_out) = outcome;
            tracing::debug!(
                target: "processkit",
                program = %self.program,
                code = ?code,
                timed_out,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "process exited"
            );
        }
        Ok(outcome)
    }

    /// Without the `cancellation` feature: the plain timeout/no-timeout shape.
    #[cfg(not(feature = "cancellation"))]
    async fn drive_to_exit_inner(&mut self) -> Result<(Option<i32>, bool)> {
        match self.timeout {
            Some(limit) => match tokio::time::timeout(limit, self.child.wait()).await {
                Ok(status) => Ok((status?.code(), false)),
                Err(_elapsed) => {
                    self.kill_tree().await;
                    Ok((None, true))
                }
            },
            None => Ok((self.child.wait().await?.code(), false)),
        }
    }

    /// With the feature: race the cancellation token against the
    /// (deadline-bounded) wait. Unset knobs become never-resolving arms, so one
    /// `select!` covers the whole timeout × token matrix. The cancel arm does
    /// NOT set `timed_out` — callers classify it via
    /// [`cancelled_error`](Self::cancelled_error) afterwards.
    #[cfg(feature = "cancellation")]
    async fn drive_to_exit_inner(&mut self) -> Result<(Option<i32>, bool)> {
        // Own the knobs so the helper futures borrow nothing from `self` —
        // only `self.child.wait()` does, keeping the select! borrows disjoint.
        let limit = self.timeout;
        let token = self.cancel_token.clone();
        let cancelled = async {
            match &token {
                Some(token) => token.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let deadline = async {
            match limit {
                Some(limit) => tokio::time::sleep(limit).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            status = self.child.wait() => Ok((status?.code(), false)),
            () = cancelled => {
                self.kill_tree().await;
                Ok((None, false))
            }
            () = deadline => {
                self.kill_tree().await;
                Ok((None, true))
            }
        }
    }

    /// Hard-kill the child and (for a private group) its tree, then reap —
    /// the shared teardown of the timeout and cancellation arms.
    async fn kill_tree(&mut self) {
        let _ = self.child.start_kill();
        if let Some(group) = &self.own_group {
            let _ = group.terminate_all();
        }
        let _ = self.child.wait().await;
    }

    /// After [`drive_to_exit`](Self::drive_to_exit): the typed cancellation
    /// error when the run's token fired — checked by every consuming path
    /// BEFORE any timeout classification (an explicit cancel wins).
    #[cfg(feature = "cancellation")]
    fn cancelled_error(&self) -> Option<Error> {
        match &self.cancel_token {
            Some(token) if token.is_cancelled() => Some(Error::Cancelled {
                program: self.program.clone(),
            }),
            _ => None,
        }
    }

    /// Send a kill to the process without waiting for it to exit. The owning
    /// group still governs the rest of the tree.
    pub fn start_kill(&mut self) -> Result<()> {
        self.child.start_kill()?;
        Ok(())
    }
}

impl Drop for RunningProcess {
    fn drop(&mut self) {
        // Abort a still-running stdin writer; a finished one is unaffected.
        if let Some(task) = self.stdin_task.take() {
            task.abort();
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
            let _ = task.await;
        }
    };
    if tokio::time::timeout(PUMP_TEARDOWN, join).await.is_err() {
        for abort in aborts {
            abort.abort();
        }
    }
}
