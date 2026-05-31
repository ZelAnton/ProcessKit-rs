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
use crate::running::RunningProcess;

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

    /// Run and return just the exit code.
    async fn exit_code(&self, command: &Command) -> Result<i32> {
        Ok(self.output(command).await?.exit_code())
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

/// Build the OS command, spawn it into `group`, kick off the background stdin
/// writer, and wrap everything in a [`RunningProcess`] (with no owned group).
pub(crate) async fn launch(group: &ProcessGroup, command: &Command) -> Result<RunningProcess> {
    let mut tokio_cmd = command.build_tokio();
    let mut child = group.spawn(&mut tokio_cmd)?;
    let pid = child.id();

    // Write buffered/file stdin on a background task so a large payload can't
    // deadlock against the child's stdout; dropping the sink sends EOF.
    let stdin_task = match command.stdin_source() {
        Some(source) if !source.is_empty() => child.stdin.take().map(|mut sink| {
            let source = source.clone();
            tokio::spawn(async move {
                let result = source.write_to(&mut sink).await;
                drop(sink);
                result
            })
        }),
        _ => None,
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    Ok(RunningProcess::new(
        command.program_name(),
        child,
        None,
        stdout,
        stderr,
        stdin_task,
        command.timeout_value(),
        pid,
    ))
}
