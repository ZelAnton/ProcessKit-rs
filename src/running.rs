//! [`RunningProcess`] — a live handle to a spawned child.

use std::time::{Duration, Instant, SystemTime};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::LinesStream;

use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::ProcessResult;

/// A `Stream` of the child's standard-output lines (see
/// [`RunningProcess::stdout_lines`]).
pub type StdoutLines = LinesStream<BufReader<ChildStdout>>;

/// A handle to a process spawned by a runner.
///
/// While this handle is alive the process keeps running; dropping it (for a
/// private-group run) tears the process tree down. Capture the outcome with
/// [`wait`](Self::wait) / [`output_string`](Self::output_string) /
/// [`output_bytes`](Self::output_bytes), or stream stdout incrementally with
/// [`stdout_lines`](Self::stdout_lines).
pub struct RunningProcess {
    program: String,
    child: Child,
    // The private group this run owns, if any. Kept alive so its kill-on-drop
    // governs the tree; `None` for runs into a caller-owned (shared) group.
    own_group: Option<ProcessGroup>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    // Background task writing buffered/file stdin, then closing the pipe (EOF).
    stdin_task: Option<JoinHandle<std::io::Result<()>>>,
    timeout: Option<Duration>,
    started: Instant,
    start_time: SystemTime,
    pid: Option<u32>,
}

impl RunningProcess {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        program: String,
        child: Child,
        own_group: Option<ProcessGroup>,
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
        stdin_task: Option<JoinHandle<std::io::Result<()>>>,
        timeout: Option<Duration>,
        pid: Option<u32>,
    ) -> Self {
        Self {
            program,
            child,
            own_group,
            stdout,
            stderr,
            stdin_task,
            timeout,
            started: Instant::now(),
            start_time: SystemTime::now(),
            pid,
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

    /// Stream the child's standard output line by line.
    ///
    /// Call this **once**. Standard error is drained in the background so the
    /// child can never block on a full stderr pipe; that drained stderr is
    /// discarded — use [`output_string`](Self::output_string) when you need both
    /// streams. The handle (and the process) stay alive until this
    /// `RunningProcess` is dropped, so keep it in scope while consuming.
    pub fn stdout_lines(&mut self) -> StdoutLines {
        if let Some(mut err) = self.stderr.take() {
            tokio::spawn(async move {
                let mut sink = Vec::new();
                let _ = err.read_to_end(&mut sink).await;
            });
        }
        let stdout = self
            .stdout
            .take()
            .expect("stdout is piped; call stdout_lines only once");
        LinesStream::new(BufReader::new(stdout).lines())
    }

    /// Drain both streams, wait for exit, and return the captured text output.
    pub async fn output_string(self) -> Result<ProcessResult<String>> {
        let program = self.program.clone();
        let (out, err, code, timed_out) = self.capture().await?;
        Ok(ProcessResult::new(
            program,
            String::from_utf8_lossy(&out).into_owned(),
            err,
            code,
            timed_out,
        ))
    }

    /// Drain both streams, wait for exit, and return the raw stdout bytes.
    pub async fn output_bytes(self) -> Result<ProcessResult<Vec<u8>>> {
        let program = self.program.clone();
        let (out, err, code, timed_out) = self.capture().await?;
        Ok(ProcessResult::new(program, out, err, code, timed_out))
    }

    /// Wait for exit, returning just the exit code (output is drained and
    /// discarded so the child never blocks on a full pipe).
    pub async fn wait(self) -> Result<i32> {
        let (_out, _err, code, _timed_out) = self.capture().await?;
        Ok(code)
    }

    /// Drive the process to completion: concurrently read both pipes and wait
    /// for exit, honoring the timeout. Returns `(stdout, stderr, code, timed_out)`.
    async fn capture(mut self) -> Result<(Vec<u8>, String, i32, bool)> {
        let mut out_pipe = self.stdout.take();
        let mut err_pipe = self.stderr.take();

        let read_out = async {
            let mut buf = Vec::new();
            if let Some(p) = &mut out_pipe {
                p.read_to_end(&mut buf).await?;
            }
            Ok::<_, std::io::Error>(buf)
        };
        let read_err = async {
            let mut buf = Vec::new();
            if let Some(p) = &mut err_pipe {
                p.read_to_end(&mut buf).await?;
            }
            Ok::<_, std::io::Error>(buf)
        };
        let wait = self.child.wait();
        let drive = async {
            let (o, e, status) = tokio::try_join!(read_out, read_err, wait)?;
            Ok::<_, std::io::Error>((o, e, status))
        };

        let result = match self.timeout {
            Some(limit) => match tokio::time::timeout(limit, drive).await {
                Ok(driven) => {
                    let (o, e, status) = driven?;
                    (
                        o,
                        String::from_utf8_lossy(&e).into_owned(),
                        exit_code(status),
                        false,
                    )
                }
                Err(_elapsed) => {
                    // The deadline fired: `drive` is dropped here, releasing its
                    // borrows. Hard-kill the tree (private group if we own one,
                    // else just this child) and reap so we don't leave a zombie.
                    let _ = self.child.start_kill();
                    if let Some(group) = &self.own_group {
                        let _ = group.terminate_all();
                    }
                    let _ = self.child.wait().await;
                    (Vec::new(), String::new(), TIMEOUT_EXIT_CODE, true)
                }
            },
            None => {
                let (o, e, status) = drive.await?;
                (
                    o,
                    String::from_utf8_lossy(&e).into_owned(),
                    exit_code(status),
                    false,
                )
            }
        };

        // The stdin writer (if any) is detached; a still-running write just hits
        // a closed pipe and stops. Nothing to await here.
        Ok(result)
    }
}

impl Drop for RunningProcess {
    fn drop(&mut self) {
        // If a stdin writer is still running (e.g. the handle is dropped early),
        // abort it; otherwise it would linger writing to a soon-to-be-closed
        // pipe. A task that already finished is unaffected.
        if let Some(task) = self.stdin_task.take() {
            task.abort();
        }
    }
}

/// Exit code reported for a run that was killed by its timeout.
const TIMEOUT_EXIT_CODE: i32 = -1;

/// The numeric exit code, or `-1` when the process was terminated by a signal
/// (which carries no exit code on Unix).
fn exit_code(status: std::process::ExitStatus) -> i32 {
    status.code().unwrap_or(-1)
}

/// Elapsed wall-clock time since the process was started.
impl RunningProcess {
    /// Time elapsed since the process started (sampled now).
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
}
