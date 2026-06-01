//! [`Command`] — a builder describing a process to run.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use encoding_rs::{Encoding, UTF_8};

use crate::buffer::OutputBufferPolicy;
use crate::error::{Error, Result};
use crate::pump::LineHandler;
use crate::result::ProcessResult;
use crate::runner::{JobRunner, ProcessRunnerExt};
use crate::running::RunningProcess;
use crate::stdin::Stdin;

/// A description of a child process to launch: program, arguments, working
/// directory, environment, stdin source, and an optional timeout.
///
/// A single builder for everything a run needs. Build it, then either drive it
/// to completion with a
/// helper ([`output_string`](Self::output_string), [`run`](Self::run), …) or
/// start it via a [`ProcessRunner`](crate::ProcessRunner) for streaming/shared
/// groups.
#[derive(Clone)]
pub struct Command {
    program: OsString,
    args: Vec<OsString>,
    cwd: Option<OsString>,
    envs: Vec<(OsString, Option<OsString>)>,
    env_clear: bool,
    stdin: Option<Stdin>,
    keep_stdin_open: bool,
    timeout: Option<Duration>,
    stdout_handler: Option<LineHandler>,
    stderr_handler: Option<LineHandler>,
    output_buffer: OutputBufferPolicy,
    stdout_encoding: &'static Encoding,
    stderr_encoding: &'static Encoding,
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
            keep_stdin_open: false,
            timeout: None,
            stdout_handler: None,
            stderr_handler: None,
            output_buffer: OutputBufferPolicy::unbounded(),
            stdout_encoding: UTF_8,
            stderr_encoding: UTF_8,
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

    /// Leave stdin open after start so the child can be driven interactively via
    /// [`RunningProcess::standard_input`](crate::RunningProcess::standard_input).
    /// Has no effect on the bulk run helpers (they always close stdin).
    pub fn keep_stdin_open(mut self) -> Self {
        self.keep_stdin_open = true;
        self
    }

    /// Invoke `handler` for each decoded stdout line as it is read (in addition
    /// to capture/streaming). Runs on the pump task; keep it cheap and panic-free.
    pub fn on_stdout_line<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.stdout_handler = Some(Arc::new(handler));
        self
    }

    /// Invoke `handler` for each decoded stderr line as it is read.
    pub fn on_stderr_line<F>(mut self, handler: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.stderr_handler = Some(Arc::new(handler));
        self
    }

    /// Cap the in-memory backlog of captured output lines (see
    /// [`OutputBufferPolicy`]). The pump still drains the pipe; only retention is
    /// bounded.
    pub fn output_buffer(mut self, policy: OutputBufferPolicy) -> Self {
        self.output_buffer = policy;
        self
    }

    /// Decode stdout with `encoding` instead of UTF-8 (e.g.
    /// `encoding_rs::SHIFT_JIS`).
    pub fn stdout_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stdout_encoding = encoding;
        self
    }

    /// Decode stderr with `encoding` instead of UTF-8.
    pub fn stderr_encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stderr_encoding = encoding;
        self
    }

    /// Decode both stdout and stderr with `encoding`.
    pub fn encoding(mut self, encoding: &'static Encoding) -> Self {
        self.stdout_encoding = encoding;
        self.stderr_encoding = encoding;
        self
    }

    // --- Accessors used by the runner layer --------------------------------

    pub(crate) fn keeps_stdin_open(&self) -> bool {
        self.keep_stdin_open
    }

    pub(crate) fn stdout_handler(&self) -> Option<LineHandler> {
        self.stdout_handler.clone()
    }

    pub(crate) fn stderr_handler(&self) -> Option<LineHandler> {
        self.stderr_handler.clone()
    }

    pub(crate) fn output_buffer_policy(&self) -> OutputBufferPolicy {
        self.output_buffer
    }

    pub(crate) fn out_encoding(&self) -> &'static Encoding {
        self.stdout_encoding
    }

    pub(crate) fn err_encoding(&self) -> &'static Encoding {
        self.stderr_encoding
    }

    pub(crate) fn program_name(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    // ----- Public accessors -----------------------------------------------
    // Exposed so external `ScriptedRunner::when(|cmd| …)` predicates and other
    // inspection can read what a command will run. Named to avoid clashing with
    // the same-named builder methods (`program` has no builder; `arguments` vs
    // `args`, `working_dir` vs `current_dir`, etc.).

    /// The program to launch.
    pub fn program(&self) -> &OsStr {
        &self.program
    }

    /// The arguments, in order.
    pub fn arguments(&self) -> &[OsString] {
        &self.args
    }

    /// The working-directory override, if one was set.
    pub fn working_dir(&self) -> Option<&Path> {
        self.cwd.as_deref().map(Path::new)
    }

    /// The environment overrides, in order (a `None` value removes the variable).
    pub fn env_overrides(&self) -> &[(OsString, Option<OsString>)] {
        &self.envs
    }

    /// The configured stdin source, if any.
    pub fn stdin_source(&self) -> Option<&Stdin> {
        self.stdin.as_ref()
    }

    /// The configured timeout, if any.
    pub fn configured_timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Build a `tokio::process::Command` with this command's program, args,
    /// working dir, and environment — stdio wired for capture. Use it to feed
    /// the low-level [`ProcessGroup::spawn`](crate::ProcessGroup::spawn) escape
    /// hatch directly (which returns a raw [`tokio::process::Child`]).
    pub fn to_tokio_command(&self) -> tokio::process::Command {
        self.build_tokio()
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
        if self.keep_stdin_open {
            // Interactive: keep a pipe open for the caller to write to.
            cmd.stdin(Stdio::piped());
        } else {
            match &self.stdin {
                Some(src) => {
                    cmd.stdin(src.stdio());
                }
                // No source given: close stdin so the child reads EOF at start.
                None => {
                    cmd.stdin(Stdio::null());
                }
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

    /// Run to completion and return just the exit code (output is discarded). A
    /// run killed by its timeout has no meaningful code, so it surfaces as
    /// [`Error::Timeout`](crate::Error::Timeout) — consistent with
    /// [`ProcessRunnerExt::exit_code`](crate::ProcessRunnerExt::exit_code) and
    /// [`CliClient::code`](crate::CliClient::code).
    pub async fn exit_code(&self) -> Result<i32> {
        JobRunner::new().exit_code(self).await
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

        let process = JobRunner::new().start(self).await?;
        let search = async move {
            let mut process = process;
            let mut lines = process.stdout_lines();
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None
        };
        match self.timeout {
            // Bound the search by the configured deadline. On elapse, `search`
            // is dropped — tearing the process tree down — and the timeout is
            // surfaced as `Error::Timeout` (consistent with `run`/`exit_code`),
            // never an indefinite hang on a process that stalls without exiting.
            Some(limit) => match tokio::time::timeout(limit, search).await {
                Ok(found) => Ok(found),
                Err(_elapsed) => Err(Error::Timeout {
                    program: self.program_name(),
                    timeout: limit,
                }),
            },
            None => Ok(search.await),
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Command")
            .field("program", &self.program)
            .field("args", &self.args)
            .field("cwd", &self.cwd)
            .field("envs", &self.envs)
            .field("env_clear", &self.env_clear)
            .field("stdin", &self.stdin)
            .field("keep_stdin_open", &self.keep_stdin_open)
            .field("timeout", &self.timeout)
            .field("has_stdout_handler", &self.stdout_handler.is_some())
            .field("has_stderr_handler", &self.stderr_handler.is_some())
            .field("output_buffer", &self.output_buffer)
            .field("stdout_encoding", &self.stdout_encoding.name())
            .field("stderr_encoding", &self.stderr_encoding.name())
            .finish()
    }
}
