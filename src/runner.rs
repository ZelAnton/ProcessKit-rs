//! The [`ProcessRunner`] seam and its real implementations.
//!
//! The mock seam is at the *captured-output* level: [`ProcessRunner::output`]
//! returns a finished [`ProcessResult`], so a test double can return canned
//! output without a real OS process (a live [`RunningProcess`] can't be
//! fabricated). Live-handle / streaming runs use the concrete
//! [`start`](JobRunner::start) methods instead.

use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::ProcessResult;
use crate::running::{RunningProcess, Spawned};

/// Runs a [`Command`] to completion and returns its captured text output.
///
/// This one-method seam is the mock point (mirroring the .NET `IProcessRunner`):
/// production code takes `&dyn ProcessRunner`; tests pass a
/// [`ScriptedRunner`](crate::ScriptedRunner) /
/// [`RecordingRunner`](crate::RecordingRunner) (or, behind the `mock` feature,
/// a generated `MockRunner`) instead of spawning real processes.
#[cfg_attr(feature = "mock", mockall::automock)]
#[async_trait::async_trait]
pub trait ProcessRunner: Send + Sync {
    /// Run `command` to completion, capturing stdout/stderr and the exit code.
    /// A non-zero exit is reported in the result, not raised.
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>>;
}

/// A shared reference to a runner is itself a runner, so a borrowed
/// [`RecordingRunner`](crate::RecordingRunner) (or any `&R`) can be injected
/// where a `ProcessRunner` is expected.
#[async_trait::async_trait]
impl<R: ProcessRunner + ?Sized> ProcessRunner for &R {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        (**self).output(command).await
    }
}

/// Convenience methods available on every [`ProcessRunner`] (including
/// `&dyn ProcessRunner`), layered over [`output`](ProcessRunner::output) —
/// the analogue of the .NET `ProcessRunnerExtensions`.
#[async_trait::async_trait]
pub trait ProcessRunnerExt: ProcessRunner {
    /// Run, require a zero exit, and return trimmed stdout.
    async fn run(&self, command: &Command) -> Result<String> {
        let result = self.output(command).await?.ensure_success()?;
        Ok(result.into_stdout().trim_end().to_owned())
    }

    /// Run and return just the exit code. A run killed by its timeout has no
    /// meaningful code, so it surfaces as [`Error::Timeout`](crate::Error::Timeout)
    /// rather than the synthetic `-1` — mirroring [`ensure_success`](crate::ProcessResult::ensure_success).
    async fn exit_code(&self, command: &Command) -> Result<i32> {
        let result = self.output(command).await?;
        match result.timeout_error() {
            Some(err) => Err(err),
            None => Ok(result.exit_code()),
        }
    }

    /// Run, require a zero exit, and return the full captured result (untrimmed
    /// stdout). The building block for the `parse`/`try_parse` helpers — use it
    /// when you need the whole `ProcessResult` after success-checking, rather
    /// than just trimmed stdout (`run`) or the raw result (`output`).
    async fn checked(&self, command: &Command) -> Result<ProcessResult<String>> {
        self.output(command).await?.ensure_success()
    }
}

#[async_trait::async_trait]
impl<T: ProcessRunner + ?Sized> ProcessRunnerExt for T {}

/// The default runner: every run gets a fresh, private [`ProcessGroup`] owned by
/// the run, so its tree is torn down when the run finishes (or its handle drops).
#[derive(Debug, Default, Clone)]
pub struct JobRunner;

impl JobRunner {
    /// Create a `JobRunner`.
    pub fn new() -> Self {
        Self
    }

    /// Start `command` and return a live handle, backed by a fresh private
    /// group the handle owns. Use this for streaming or incremental stdin.
    pub async fn start(&self, command: &Command) -> Result<RunningProcess> {
        let group = ProcessGroup::new()?;
        let mut process = launch(&group, command).await?;
        process.attach_group(group);
        Ok(process)
    }
}

#[async_trait::async_trait]
impl ProcessRunner for JobRunner {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        self.start(command).await?.output_string().await
    }
}

impl ProcessGroup {
    /// Start `command` as a member of this (shared) group and return a live
    /// handle. The handle does **not** own the group, so dropping it leaves the
    /// group and any sibling processes intact — the caller controls teardown.
    pub async fn start(&self, command: &Command) -> Result<RunningProcess> {
        launch(self, command).await
    }
}

#[async_trait::async_trait]
impl ProcessRunner for ProcessGroup {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        self.start(command).await?.output_string().await
    }
}

/// Build the OS command, spawn it into `group`, wire stdin, and wrap everything
/// in a [`RunningProcess`] (with no owned group).
pub(crate) async fn launch(group: &ProcessGroup, command: &Command) -> Result<RunningProcess> {
    let mut tokio_cmd = command.build_tokio();
    let mut child = group.spawn(&mut tokio_cmd)?;
    let pid = child.id();

    let (stdin_pipe, stdin_task) = if command.keeps_stdin_open() {
        // Interactive: hand the pipe to the caller via `standard_input`.
        (child.stdin.take(), None)
    } else {
        match command.stdin_source() {
            // Write buffered/file/stream stdin on a background task so a large
            // payload can't deadlock against the child's stdout; dropping the
            // sink sends EOF.
            Some(source) if !source.is_empty() => {
                let task = child.stdin.take().map(|mut sink| {
                    let source = source.clone();
                    tokio::spawn(async move {
                        let result = source.write_to(&mut sink).await;
                        drop(sink);
                        result
                    })
                });
                (None, task)
            }
            _ => (None, None),
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    Ok(RunningProcess::from_spawned(Spawned {
        program: command.program_name(),
        child,
        own_group: None,
        stdout,
        stderr,
        stdin: stdin_pipe,
        stdin_task,
        timeout: command.configured_timeout(),
        pid,
        stdout_encoding: command.out_encoding(),
        stderr_encoding: command.err_encoding(),
        stdout_handler: command.stdout_handler(),
        stderr_handler: command.stderr_handler(),
        buffer: command.output_buffer_policy(),
    }))
}
