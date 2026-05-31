//! [`Command`] — a builder describing a process to run.

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use crate::error::Result;
use crate::result::ProcessResult;
use crate::runner::JobRunner;
use crate::running::RunningProcess;
use crate::stdin::Stdin;

/// A description of a child process to launch: program, arguments, working
/// directory, environment, stdin source, and an optional timeout.
///
/// This collapses the .NET `ProcessStartInfo` + `ProcessRunOptions` pair into a
/// single Rust builder. Build it, then either drive it to completion with a
/// helper ([`output_string`](Self::output_string), [`run`](Self::run), …) or
/// start it via a [`ProcessRunner`](crate::ProcessRunner) for streaming/shared
/// groups.
#[derive(Debug, Clone)]
pub struct Command {
    program: OsString,
    args: Vec<OsString>,
    cwd: Option<OsString>,
    envs: Vec<(OsString, Option<OsString>)>,
    env_clear: bool,
    stdin: Option<Stdin>,
    timeout: Option<Duration>,
}

impl Command {
    /// Start a command for `program` (resolved on `PATH`).
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            args: Vec::new(),
            cwd: None,
            envs: Vec::new(),
            env_clear: false,
            stdin: None,
            timeout: None,
        }
    }

    /// Append a single argument.
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_os_string());
        self
    }

    /// Append several arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|a| a.as_ref().to_os_string()));
        self
    }

    /// Set the working directory.
    pub fn current_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.cwd = Some(dir.as_ref().as_os_str().to_os_string());
        self
    }

    /// Set (or, with a `None` value, remove) an environment variable.
    pub fn env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.envs.push((
            key.as_ref().to_os_string(),
            Some(value.as_ref().to_os_string()),
        ));
        self
    }

    /// Remove an environment variable inherited from the parent.
    pub fn env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        self.envs.push((key.as_ref().to_os_string(), None));
        self
    }

    /// Clear all inherited environment variables before applying any set here.
    pub fn env_clear(mut self) -> Self {
        self.env_clear = true;
        self
    }

    /// Provide standard input for the child (see [`Stdin`]).
    pub fn stdin(mut self, stdin: Stdin) -> Self {
        self.stdin = Some(stdin);
        self
    }

    /// Kill the run if it exceeds `timeout`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    // --- Accessors used by the runner layer --------------------------------

    pub(crate) fn program_name(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    pub(crate) fn timeout_value(&self) -> Option<Duration> {
        self.timeout
    }

    pub(crate) fn stdin_source(&self) -> Option<&Stdin> {
        self.stdin.as_ref()
    }

    pub(crate) fn program_os(&self) -> &OsStr {
        &self.program
    }

    pub(crate) fn args_slice(&self) -> &[OsString] {
        &self.args
    }

    pub(crate) fn cwd_os(&self) -> Option<&OsStr> {
        self.cwd.as_deref()
    }

    pub(crate) fn envs_slice(&self) -> &[(OsString, Option<OsString>)] {
        &self.envs
    }

    /// Build the `tokio` command with stdio wired for capture. Containment
    /// (cgroup/job/process-group) is added by the group's `spawn`.
    pub(crate) fn build_tokio(&self) -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new(&self.program);
        cmd.args(&self.args);
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }
        if self.env_clear {
            cmd.env_clear();
        }
        for (key, value) in &self.envs {
            match value {
                Some(val) => {
                    cmd.env(key, val);
                }
                None => {
                    cmd.env_remove(key);
                }
            }
        }
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        match &self.stdin {
            Some(src) => {
                cmd.stdin(src.stdio());
            }
            // No source given: close stdin so the child reads EOF at start.
            None => {
                cmd.stdin(Stdio::null());
            }
        }
        cmd
    }

    // --- Live handle (private one-shot group) ------------------------------

    /// Start the command and return a live [`RunningProcess`] backed by a fresh
    /// private group. Use this for streaming stdout
    /// ([`RunningProcess::stdout_lines`]) or inspecting the process while it
    /// runs; keep the handle in scope, as dropping it tears the tree down.
    pub async fn start(&self) -> Result<RunningProcess> {
        JobRunner::new().start(self).await
    }

    // --- High-level run helpers (private one-shot group) -------------------

    /// Run to completion and capture stdout as text, stderr, and the exit code.
    /// A non-zero exit is reported, not raised — call
    /// [`ProcessResult::ensure_success`] to turn it into an error.
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        JobRunner::new().start(self).await?.output_string().await
    }

    /// Run to completion and capture stdout as raw bytes (plus stderr/exit code).
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        JobRunner::new().start(self).await?.output_bytes().await
    }

    /// Run to completion and return just the exit code (output is discarded).
    pub async fn exit_code(&self) -> Result<i32> {
        JobRunner::new().start(self).await?.wait().await
    }

    /// Run to completion, requiring a zero exit, and return trimmed stdout.
    pub async fn run(&self) -> Result<String> {
        let result = self.output_string().await?.ensure_success()?;
        Ok(result.into_stdout().trim_end().to_owned())
    }

    /// Return the first stdout line matching `predicate` (or the first line when
    /// the predicate is trivial), then tear the process down.
    pub async fn first_line<F>(&self, predicate: F) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool,
    {
        use tokio_stream::StreamExt;

        let mut process = JobRunner::new().start(self).await?;
        let mut lines = process.stdout_lines();
        while let Some(line) = lines.next().await {
            let line = line?;
            if predicate(&line) {
                return Ok(Some(line));
            }
        }
        Ok(None)
    }
}
