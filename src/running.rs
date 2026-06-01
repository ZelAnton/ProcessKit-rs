//! [`RunningProcess`] — a live handle to a spawned child.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use encoding_rs::Encoding;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;
use tokio_stream::Stream;

use crate::buffer::OutputBufferPolicy;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::pump::{LineHandler, Popped, SharedLines, pump_lines};
use crate::result::ProcessResult;
use crate::stdin::ProcessStdin;

/// How long teardown waits for output pumps to finish before aborting them, so a
/// surviving grandchild holding a pipe can't hang the run.
const PUMP_TEARDOWN: Duration = Duration::from_secs(5);

/// Exit code reported for a run that was killed by its timeout.
const TIMEOUT_EXIT_CODE: i32 = -1;

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
    program: String,
    child: Child,
    own_group: Option<ProcessGroup>,
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
    started: Instant,
    start_time: SystemTime,
}

impl RunningProcess {
    pub(crate) fn from_spawned(s: Spawned) -> Self {
        Self {
            program: s.program,
            child: s.child,
            own_group: s.own_group,
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
            started: Instant::now(),
            start_time: SystemTime::now(),
        }
    }

    pub(crate) fn attach_group(&mut self, group: ProcessGroup) {
        self.own_group = Some(group);
    }

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
    pub fn cpu_time(&self) -> Option<Duration> {
        self.pid
            .and_then(|pid| crate::sys::process_metrics(pid).cpu_time)
    }

    /// Peak resident memory in bytes, if the platform can report it.
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
    /// The command's [`timeout`](crate::Command::timeout) is **not** auto-enforced
    /// while you pull lines from this stream (only the run-to-completion helpers —
    /// `output_string`/`wait`/`finish_streamed` and `Command::first_line` — apply
    /// it). To bound a manual stream, wrap your consumption in
    /// [`tokio::time::timeout`] and drop this handle (which kills the tree) on
    /// elapse.
    pub fn stdout_lines(&mut self) -> StdoutLines {
        // Background-drain stderr (counter + handler still apply). The handle is
        // kept so `finish_streamed` can await the last line before draining.
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
    /// its timeout returns `-1` (it is not raised as an error). For the
    /// timeout-aware behavior use the one-shot helpers
    /// ([`Command::exit_code`](crate::Command::exit_code) /
    /// [`ProcessRunnerExt::exit_code`](crate::ProcessRunnerExt::exit_code)), which
    /// surface a deadline as [`Error::Timeout`](crate::Error::Timeout).
    pub async fn wait(mut self) -> Result<i32> {
        let stdout_sink = SharedLines::new(&self.buffer);
        let stderr_sink = SharedLines::new(&self.buffer);
        let pumps = self.spawn_line_pumps(&stdout_sink, &stderr_sink);
        let (code, _timed_out) = self.drive_to_exit().await?;
        join_pumps(pumps).await;
        Ok(code)
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
    /// flagging `timed_out` on elapse).
    async fn drive_to_exit(&mut self) -> Result<(i32, bool)> {
        let outcome = match self.timeout {
            Some(limit) => match tokio::time::timeout(limit, self.child.wait()).await {
                Ok(status) => (exit_code(status?), false),
                Err(_elapsed) => {
                    let _ = self.child.start_kill();
                    if let Some(group) = &self.own_group {
                        let _ = group.terminate_all();
                    }
                    let _ = self.child.wait().await;
                    (TIMEOUT_EXIT_CODE, true)
                }
            },
            None => (exit_code(self.child.wait().await?), false),
        };
        #[cfg(feature = "tracing")]
        {
            let (code, timed_out) = outcome;
            tracing::debug!(
                target: "processkit",
                program = %self.program,
                code,
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
    pub async fn finish_streamed(mut self) -> Result<(i32, String)> {
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

/// The numeric exit code, or `-1` when the process was terminated by a signal
/// (which carries no exit code on Unix).
fn exit_code(status: std::process::ExitStatus) -> i32 {
    status.code().unwrap_or(-1)
}

/// A `Stream` of the child's standard-output lines (see
/// [`RunningProcess::stdout_lines`]).
pub struct StdoutLines {
    sink: Arc<SharedLines>,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
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
