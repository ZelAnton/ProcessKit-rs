//! [`RunningProcess`] — a live handle to a spawned child.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use encoding_rs::Encoding;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;
use tokio_stream::Stream;

use crate::buffer::OutputBufferPolicy;
use crate::error::{Error, Result};
use crate::group::ProcessGroup;
use crate::pump::{LineHandler, Popped, SharedLines, pump_lines};
use crate::result::ProcessResult;
use crate::stdin::ProcessStdin;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

/// How often [`RunningProcess::wait_for`] / [`wait_for_port`]
/// (RunningProcess::wait_for_port) re-check readiness — responsive without
/// busy-spinning; matches the 50 ms liveness-poll cadence used elsewhere.
const READINESS_POLL: Duration = Duration::from_millis(50);

/// Cap on a single `wait_for_port` connect attempt (clamped to the remaining
/// budget), so one stalled connect can't overrun the probe deadline.
const CONNECT_ATTEMPT_CAP: Duration = Duration::from_secs(1);

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

    // (continued below)
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

    /// Stream the child's standard output line by line. Call this **once**.
    ///
    /// Standard error is drained in the background (so the child can't block on a
    /// full stderr pipe) and discarded — use [`output_string`](Self::output_string)
    /// when you need both. Keep this `RunningProcess` in scope while consuming;
    /// dropping it tears the process down.
    ///
    /// The command's [`timeout`](crate::Command::timeout), if set, **bounds the
    /// stream**: at the deadline the process tree is killed, so the pipes close
    /// and this stream ends — a streamed run can't hang past its timeout. A
    /// following [`finish_streamed`](Self::finish_streamed) then reports the kill
    /// (no clean exit: `code` is `None` on a Unix signal-kill, a platform code on
    /// a Windows Job kill). With no timeout the stream is unbounded as before.
    /// (Bounding applies to a run that owns its group — the
    /// [`Command::start`](crate::Command::start) / [`JobRunner`](crate::JobRunner)
    /// path. A handle from [`ProcessGroup::start`](crate::ProcessGroup::start)
    /// shares its group, so the caller bounds the stream.)
    ///
    /// # Example
    ///
    /// Stream stdout line by line as it is produced, then collect the exit code
    /// and stderr:
    ///
    /// ```no_run
    /// use processkit::{Command, StreamExt};
    ///
    /// # async fn demo() -> processkit::Result<()> {
    /// let mut run = Command::new("git").args(["log", "--oneline", "-n", "20"]).start().await?;
    ///
    /// let mut lines = run.stdout_lines();
    /// while let Some(line) = lines.next().await {
    ///     println!("commit: {line}");
    /// }
    ///
    /// let (code, stderr) = run.finish_streamed().await?;
    /// # let _ = (code, stderr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn stdout_lines(&mut self) -> StdoutLines {
        // Background-drain stderr (counter + handler still apply). The handle is
        // kept so `finish_streamed` can await the last line before draining. Only
        // set up once: a second `stdout_lines` call must not overwrite the first
        // call's sink/pump, or `finish_streamed` would return empty stderr.
        if self.stderr_sink.is_none() {
            let stderr_sink = SharedLines::new(&self.buffer);
            if let Some(pipe) = self.stderr_pipe.take() {
                self.stderr_pump = Some(tokio::spawn(pump_lines(
                    pipe,
                    self.stderr_encoding,
                    self.stderr_handler.clone(),
                    stderr_sink.clone(),
                )));
            }
            self.stderr_sink = Some(stderr_sink);
        }

        let stdout_sink = SharedLines::new(&self.buffer);
        match self.stdout_pipe.take() {
            Some(pipe) => {
                tokio::spawn(pump_lines(
                    pipe,
                    self.stdout_encoding,
                    self.stdout_handler.clone(),
                    stdout_sink.clone(),
                ));
            }
            // Called more than once: hand back an immediately-finished stream.
            None => stdout_sink.close_now(),
        }
        self.stdout_sink = Some(stdout_sink.clone());

        // Bound the stream by the command's timeout: kill the tree at the deadline
        // so the pipes close and this stream ends. A `Weak` to the group means the
        // timer never delays kill-on-close when the handle is dropped early. Armed
        // once (a second `stdout_lines` call won't spawn a duplicate timer).
        if self.deadline_task.is_none()
            && let (Some(limit), Some(group)) = (self.timeout, self.own_group.as_ref())
        {
            let group = Arc::downgrade(group);
            self.deadline_task = Some(tokio::spawn(async move {
                tokio::time::sleep(limit).await;
                if let Some(group) = group.upgrade() {
                    let _ = group.terminate_all();
                }
            }));
        }

        StdoutLines {
            sink: stdout_sink,
            wait: None,
        }
    }

    /// Drain both streams, wait for exit, and return the captured text output
    /// (line-normalized to `\n`).
    pub async fn output_string(mut self) -> Result<ProcessResult<String>> {
        let stdout_sink = SharedLines::new(&self.buffer);
        let stderr_sink = SharedLines::new(&self.buffer);
        let pumps = self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        self.stdout_sink = Some(stdout_sink.clone());
        self.stderr_sink = Some(stderr_sink.clone());

        let (code, timed_out) = self.drive_to_exit().await?;
        join_pumps(pumps).await;

        Ok(ProcessResult::new(
            self.program.clone(),
            stdout_sink.drain().join("\n"),
            stderr_sink.drain().join("\n"),
            code,
            timed_out,
            self.timeout,
        ))
    }

    /// Drain both streams, wait for exit, and return the raw stdout bytes
    /// (exact; stderr is captured as text).
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

        // Read stdout raw, concurrently, so it never blocks the child.
        let mut stdout_pipe = self.stdout_pipe.take();
        let out_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(pipe) = &mut stdout_pipe {
                let _ = pipe.read_to_end(&mut buf).await;
            }
            buf
        });

        let (code, timed_out) = self.drive_to_exit().await?;
        let stdout = out_task.await.unwrap_or_default();
        join_pumps(err_pump.into_iter().collect()).await;

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
    pub async fn wait(mut self) -> Result<Option<i32>> {
        let stdout_sink = SharedLines::new(&self.buffer);
        let stderr_sink = SharedLines::new(&self.buffer);
        let pumps = self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        let (code, _timed_out) = self.drive_to_exit().await?;
        join_pumps(pumps).await;
        Ok(code)
    }

    /// Minimal non-consuming exit wait — the [`wait_any`](crate::wait_any) race
    /// participant. Unlike [`wait`](Self::wait) it spawns no pumps and applies
    /// no [`timeout`](crate::Command::timeout). Cancel-safe and re-awaitable:
    /// tokio caches the exit status, so a raced-and-cancelled process can be
    /// waited again (or consumed normally) afterwards.
    pub(crate) async fn wait_exit(&mut self) -> Result<Option<i32>> {
        Ok(self.child.wait().await?.code())
    }

    /// Wait until a stdout line matches `predicate` (returning that line), or
    /// fail with [`Error::NotReady`] when `within` elapses — or immediately
    /// when stdout closes before a match (e.g. the child exited and no
    /// descendant kept the pipe open), since no further line can arrive. A
    /// child that exits while a descendant still holds its stdout keeps the
    /// stream open, so that case waits out the deadline — the pipe, not the
    /// process, is what this probe watches.
    ///
    /// The readiness idiom: start a server, wait for its "listening on …"
    /// banner, then use it — no arbitrary sleeps.
    ///
    /// # Caveats
    ///
    /// - **Consumes stdout** up to and including the matching line (this is
    ///   the one-shot [`stdout_lines`](Self::stdout_lines) stream underneath —
    ///   if it was already called, the probe sees a closed stream and reports
    ///   `NotReady` immediately). Continue with
    ///   [`finish_streamed`](Self::finish_streamed) for the exit code and
    ///   stderr; the other probes don't touch stdout.
    /// - A failed probe does **not** kill the child — unlike
    ///   [`Command::timeout`](crate::Command::timeout), whose deadline (if
    ///   configured) is armed by this call exactly as `stdout_lines` always
    ///   arms it, and still tears the tree down independently of the probe.
    pub async fn wait_for_line(
        &mut self,
        predicate: impl Fn(&str) -> bool,
        within: Duration,
    ) -> Result<String> {
        use tokio_stream::StreamExt;

        // Bound the borrow: `StdoutLines` owns its sink (no borrow of self), so
        // `self.program` stays readable after the search.
        let mut lines = self.stdout_lines();
        let search = async {
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None // stdout closed before any match — readiness can't happen.
        };
        match tokio::time::timeout(within, search).await {
            Ok(Some(line)) => Ok(line),
            Ok(None) | Err(_) => Err(self.not_ready(within)),
        }
    }

    /// Wait until `check` (re-invoked every ~50 ms, first attempt immediate)
    /// returns `true`, or fail with [`Error::NotReady`] when `within` elapses —
    /// or immediately when the child exits first (a dead process never becomes
    /// ready).
    ///
    /// The check is any async predicate — an HTTP health endpoint, a file
    /// appearing, a database accepting connections. Doesn't touch the child's
    /// pipes, so it composes with any later consumption
    /// ([`wait`](Self::wait), [`output_string`](Self::output_string), …). A
    /// failed probe does not kill the child. The deadline bounds the polling
    /// loop, not an in-flight check: a slow `check` future can overrun
    /// `within` by its own duration.
    pub async fn wait_for<F, Fut>(&mut self, check: F, within: Duration) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        self.poll_until(check, within).await
    }

    /// Wait until a TCP connection to `addr` is accepted, or fail with
    /// [`Error::NotReady`] when `within` elapses — or immediately when the
    /// child exits first.
    ///
    /// One connect attempt per ~50 ms tick (each attempt itself bounded so a
    /// stalled connect can't overrun the deadline); the probe connection is
    /// dropped as soon as it succeeds. Doesn't touch the child's pipes; a
    /// failed probe does not kill the child.
    pub async fn wait_for_port(&mut self, addr: SocketAddr, within: Duration) -> Result<()> {
        let deadline = Instant::now() + within;
        self.poll_until(
            move || {
                let remaining = deadline.saturating_duration_since(Instant::now());
                async move {
                    // Clamp the attempt to the remaining budget; floor at 1ms so
                    // the final tick still makes a (brief) attempt.
                    let cap = CONNECT_ATTEMPT_CAP
                        .min(remaining)
                        .max(Duration::from_millis(1));
                    matches!(
                        tokio::time::timeout(cap, TcpStream::connect(addr)).await,
                        Ok(Ok(_))
                    )
                }
            },
            within,
        )
        .await
    }

    /// Re-run `check` on the readiness cadence until it passes, the child
    /// exits, or the deadline elapses.
    async fn poll_until<F, Fut>(&mut self, mut check: F, within: Duration) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let deadline = Instant::now() + within;
        loop {
            if check().await {
                return Ok(());
            }
            // An exited child can never become ready — fail fast rather than
            // burning the rest of the deadline. (`Err` from try_wait means
            // "couldn't tell"; keep polling, the deadline still bounds us.)
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return Err(self.not_ready(within));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(self.not_ready(within));
            }
            tokio::time::sleep(READINESS_POLL.min(remaining)).await;
        }
    }

    fn not_ready(&self, within: Duration) -> Error {
        Error::NotReady {
            program: self.program.clone(),
            timeout: within,
        }
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

        // Inline `wait`'s steps so the sampler stops the moment the child is
        // reaped: its pid is free for reuse from that point (Linux), and the
        // pump drain below can idle out PUMP_TEARDOWN on a leaked pipe — long
        // enough for a recycled pid to masquerade as the child and corrupt the
        // readings.
        let stdout_sink = SharedLines::new(&self.buffer);
        let stderr_sink = SharedLines::new(&self.buffer);
        let pumps = self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        let outcome = self.drive_to_exit().await;
        if let Some(task) = &sampler {
            task.abort();
        }
        let (exit_code, _timed_out) = outcome?;
        join_pumps(pumps).await;
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

    /// Wait for the child to exit, applying the timeout (killing the tree and
    /// flagging `timed_out` on elapse). The code is `None` for a run that
    /// produced none — a timeout, or a signal termination on Unix.
    async fn drive_to_exit(&mut self) -> Result<(Option<i32>, bool)> {
        let outcome = match self.timeout {
            Some(limit) => match tokio::time::timeout(limit, self.child.wait()).await {
                Ok(status) => (status?.code(), false),
                Err(_elapsed) => {
                    let _ = self.child.start_kill();
                    if let Some(group) = &self.own_group {
                        let _ = group.terminate_all();
                    }
                    let _ = self.child.wait().await;
                    (None, true)
                }
            },
            None => (self.child.wait().await?.code(), false),
        };
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

    /// Send a kill to the process without waiting for it to exit. The owning
    /// group still governs the rest of the tree.
    pub fn start_kill(&mut self) -> Result<()> {
        self.child.start_kill()?;
        Ok(())
    }

    /// Finish a streamed run: wait for exit and return the exit code plus the
    /// stderr collected in the background by [`stdout_lines`](Self::stdout_lines).
    ///
    /// Designed to pair with `stdout_lines` (consume the stdout stream first),
    /// but safe to call on its own — any pipe the stream didn't take is drained
    /// here so the child can never block on a full pipe.
    pub async fn finish_streamed(mut self) -> Result<(Option<i32>, String)> {
        // Drain a stdout pipe a prior `stdout_lines` didn't take (and discard
        // it) so the child can't block writing to it while we wait for exit.
        if let Some(mut pipe) = self.stdout_pipe.take() {
            tokio::spawn(async move {
                let mut sink = Vec::new();
                let _ = pipe.read_to_end(&mut sink).await;
            });
        }
        // Likewise start a stderr pump if streaming never did (so its output is
        // still captured and the pipe never fills).
        if self.stderr_pump.is_none()
            && let Some(pipe) = self.stderr_pipe.take()
        {
            let sink = SharedLines::new(&self.buffer);
            self.stderr_pump = Some(tokio::spawn(pump_lines(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                sink.clone(),
            )));
            self.stderr_sink = Some(sink);
        }

        let (code, _timed_out) = self.drive_to_exit().await?;
        // The child has exited, so its stderr pipe is closed — await the pump so
        // the final buffered line is captured before we drain.
        if let Some(pump) = self.stderr_pump.take() {
            let _ = pump.await;
        }
        let stderr = self
            .stderr_sink
            .as_ref()
            .map(|sink| sink.drain().join("\n"))
            .unwrap_or_default();
        Ok((code, stderr))
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

/// A `Stream` of the child's standard-output lines (see
/// [`RunningProcess::stdout_lines`]).
pub struct StdoutLines {
    sink: Arc<SharedLines>,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

// Manual: the sink and the pending-wait future are opaque.
impl std::fmt::Debug for StdoutLines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdoutLines").finish_non_exhaustive()
    }
}

impl Stream for StdoutLines {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<String>> {
        let this = self.get_mut();
        loop {
            match this.sink.try_pop() {
                Popped::Line(line) => {
                    this.wait = None;
                    return Poll::Ready(Some(line));
                }
                Popped::Closed => return Poll::Ready(None),
                Popped::Empty => {
                    if this.wait.is_none() {
                        this.wait = Some(Box::pin(this.sink.clone().changed()));
                    }
                    // `notify_one` stores a permit, so a push between the `try_pop`
                    // above and registering here is not missed.
                    match this.wait.as_mut().expect("just set").as_mut().poll(cx) {
                        Poll::Ready(()) => {
                            this.wait = None;
                            continue;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}
