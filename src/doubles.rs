//! Test doubles for the [`ProcessRunner`] seam — no real subprocess required.
//!
//! - [`ScriptedRunner`] returns canned output for commands matched by argument
//!   prefix or a predicate.
//! - [`RecordingRunner`] wraps another runner and records every [`Invocation`]
//!   so tests can assert exactly what was run.
//!
//! Behind the `mock` feature, [`mockall`] additionally generates a `MockRunner`
//! (re-exported from the crate root) for expectation-style mocking.

use std::ffi::{OsStr, OsString};
use std::sync::Mutex;

use crate::command::Command;
use crate::error::Result;
use crate::result::ProcessResult;
use crate::runner::ProcessRunner;

/// A canned reply: stdout/stderr text plus an exit code (or a timed-out run).
#[derive(Debug, Clone)]
pub struct Reply {
    stdout: String,
    stderr: String,
    code: i32,
    timed_out: bool,
}

impl Reply {
    /// A successful reply (exit code 0) producing `stdout`.
    pub fn ok(stdout: impl Into<String>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            code: 0,
            timed_out: false,
        }
    }

    /// A failing reply with exit `code` and `stderr` text.
    pub fn fail(code: i32, stderr: impl Into<String>) -> Self {
        Self {
            stdout: String::new(),
            stderr: stderr.into(),
            code,
            timed_out: false,
        }
    }

    /// A timed-out reply — drives the timeout path so a test can assert that a
    /// command which exceeds its deadline surfaces as [`Error::Timeout`](crate::Error::Timeout).
    pub fn timeout() -> Self {
        Self {
            stdout: String::new(),
            stderr: String::new(),
            // Unused — a timed-out result carries no code; `timed_out` is what
            // the helpers key on.
            code: 0,
            timed_out: true,
        }
    }

    /// Attach stdout to a reply — e.g. the `CONFLICT …` text `git merge` writes
    /// to stdout on a failing reply, so a test can exercise
    /// [`Error::Exit`](crate::Error::Exit)'s stdout field /
    /// [`ProcessResult::diagnostic`](crate::ProcessResult::diagnostic).
    pub fn with_stdout(mut self, stdout: impl Into<String>) -> Self {
        self.stdout = stdout.into();
        self
    }

    fn into_result(
        self,
        program: String,
        timeout: Option<std::time::Duration>,
    ) -> ProcessResult<String> {
        // A timed-out run carries no code (`None`); otherwise the canned code.
        let code = (!self.timed_out).then_some(self.code);
        // Carry the command's configured timeout so a timed-out reply surfaces as
        // `Error::Timeout` with the *real* deadline (matching the live runner),
        // not a zero duration.
        ProcessResult::new(
            program,
            self.stdout,
            self.stderr,
            code,
            self.timed_out,
            timeout,
        )
    }
}

type Predicate = Box<dyn Fn(&Command) -> bool + Send + Sync>;

enum Rule {
    /// Match when the command's arguments start with this prefix.
    Prefix(Vec<OsString>),
    /// Match when the predicate accepts the command.
    Predicate(Predicate),
}

impl Rule {
    fn matches(&self, command: &Command) -> bool {
        match self {
            Rule::Prefix(prefix) => command.arguments().starts_with(prefix),
            Rule::Predicate(pred) => pred(command),
        }
    }
}

/// A [`ProcessRunner`] that returns canned [`Reply`]s for matched commands.
///
/// Rules are tried in registration order; the first match wins. With no match,
/// the [`fallback`](Self::fallback) reply is used, or an error is returned.
#[derive(Default)]
pub struct ScriptedRunner {
    rules: Vec<(Rule, Reply)>,
    fallback: Option<Reply>,
}

// Manual: `Rule` holds an opaque predicate closure.
impl std::fmt::Debug for ScriptedRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedRunner")
            .field("rules", &self.rules.len())
            .field("has_fallback", &self.fallback.is_some())
            .finish_non_exhaustive()
    }
}

impl ScriptedRunner {
    /// An empty runner (every command misses until rules are added).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reply with `reply` when the command's arguments start with `prefix`.
    pub fn on<I, S>(mut self, prefix: I, reply: Reply) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let prefix = prefix
            .into_iter()
            .map(|s| s.as_ref().to_os_string())
            .collect();
        self.rules.push((Rule::Prefix(prefix), reply));
        self
    }

    /// Reply with `reply` when `predicate` accepts the command.
    pub fn when<F>(mut self, predicate: F, reply: Reply) -> Self
    where
        F: Fn(&Command) -> bool + Send + Sync + 'static,
    {
        self.rules
            .push((Rule::Predicate(Box::new(predicate)), reply));
        self
    }

    /// Reply with `reply` for any command no rule matched.
    pub fn fallback(mut self, reply: Reply) -> Self {
        self.fallback = Some(reply);
        self
    }
}

#[async_trait::async_trait]
impl ProcessRunner for ScriptedRunner {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        let program = command.program().to_string_lossy().into_owned();
        let timeout = command.configured_timeout();
        for (rule, reply) in &self.rules {
            if rule.matches(command) {
                return Ok(reply.clone().into_result(program, timeout));
            }
        }
        match &self.fallback {
            Some(reply) => Ok(reply.clone().into_result(program, timeout)),
            None => Err(crate::error::Error::Spawn {
                program,
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "ScriptedRunner: no rule matched and no fallback set",
                ),
            }),
        }
    }
}

/// A captured record of one command a runner was asked to run.
///
/// Captures the *routing* knobs — program, args, cwd, env overrides, whether
/// stdin was supplied — not the I/O-shaping ones (`timeout`, encodings, buffer
/// policy, line handlers, `keep_stdin_open`, retry). Tests that need to assert
/// those inspect the built [`Command`] itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    /// The program name.
    pub program: OsString,
    /// The arguments, in order.
    pub args: Vec<OsString>,
    /// The working directory, if one was set.
    pub cwd: Option<OsString>,
    /// Environment overrides (`None` value = removal), in order.
    pub envs: Vec<(OsString, Option<OsString>)>,
    /// Whether a (non-empty) stdin source was provided.
    pub has_stdin: bool,
}

impl Invocation {
    fn from_command(command: &Command) -> Self {
        Self {
            program: command.program().to_os_string(),
            args: command.arguments().to_vec(),
            cwd: command.working_dir().map(|p| p.as_os_str().to_os_string()),
            envs: command.env_overrides().to_vec(),
            has_stdin: command
                .stdin_source()
                .is_some_and(|stdin| !stdin.is_empty()),
        }
    }

    /// Whether `flag` appears among the arguments.
    pub fn has_flag(&self, flag: impl AsRef<OsStr>) -> bool {
        let flag = flag.as_ref();
        self.args.iter().any(|a| a == flag)
    }

    /// The arguments as lossy UTF-8 strings, for ergonomic assertions
    /// (e.g. `assert_eq!(call.args_str(), ["pr", "create"])`).
    pub fn args_str(&self) -> Vec<String> {
        self.args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }
}

/// Wraps another [`ProcessRunner`], recording every [`Invocation`] before
/// delegating, so tests can assert exactly what was run.
pub struct RecordingRunner<R: ProcessRunner = ScriptedRunner> {
    inner: R,
    calls: Mutex<Vec<Invocation>>,
}

// Manual: the inner runner type parameter carries no `Debug` bound.
impl<R: ProcessRunner> std::fmt::Debug for RecordingRunner<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let calls = self.calls.lock().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("RecordingRunner")
            .field("calls", &calls)
            .finish_non_exhaustive()
    }
}

impl RecordingRunner<ScriptedRunner> {
    /// A recorder whose inner runner replies with `reply` to everything.
    pub fn replying(reply: Reply) -> Self {
        Self::new(ScriptedRunner::new().fallback(reply))
    }
}

impl<R: ProcessRunner> RecordingRunner<R> {
    /// Wrap `inner`, recording all calls.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// A snapshot of every recorded invocation, in order.
    pub fn calls(&self) -> Vec<Invocation> {
        self.calls.lock().expect("recorder lock poisoned").clone()
    }

    /// The single recorded invocation; panics unless exactly one was made.
    pub fn only_call(&self) -> Invocation {
        let calls = self.calls();
        assert_eq!(
            calls.len(),
            1,
            "expected exactly one call, got {}",
            calls.len()
        );
        calls.into_iter().next().expect("length checked above")
    }
}

#[async_trait::async_trait]
impl<R: ProcessRunner> ProcessRunner for RecordingRunner<R> {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        self.calls
            .lock()
            .expect("recorder lock poisoned")
            .push(Invocation::from_command(command));
        self.inner.output(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::ProcessRunnerExt;

    #[tokio::test]
    async fn prefix_rule_matches_and_replies() {
        let runner = ScriptedRunner::new().on(["status"], Reply::ok("clean"));
        let out = runner
            .output(&Command::new("git").arg("status"))
            .await
            .unwrap();
        assert_eq!(out.stdout(), "clean");
        assert!(out.is_success());
    }

    #[tokio::test]
    async fn predicate_rule_and_fallback() {
        let runner = ScriptedRunner::new()
            .when(
                |c| c.arguments().iter().any(|a| a == "--version"),
                Reply::ok("v1"),
            )
            .fallback(Reply::fail(1, "unknown"));

        assert_eq!(
            runner
                .output(&Command::new("tool").arg("--version"))
                .await
                .unwrap()
                .stdout(),
            "v1"
        );
        let miss = runner.output(&Command::new("tool").arg("x")).await.unwrap();
        assert_eq!(miss.code(), Some(1));
        assert!(!miss.is_success());
    }

    #[tokio::test]
    async fn timeout_reply_surfaces_as_timeout_error() {
        use crate::error::Error;
        let runner = ScriptedRunner::new().fallback(Reply::timeout());
        // capture/output exposes the flag without erroring …
        let out = runner.output(&Command::new("git")).await.unwrap();
        assert!(out.timed_out());
        // … but the success-checking helpers raise a distinct Timeout.
        assert!(matches!(
            runner.run(&Command::new("git")).await.unwrap_err(),
            Error::Timeout { .. }
        ));
        assert!(matches!(
            runner.exit_code(&Command::new("git")).await.unwrap_err(),
            Error::Timeout { .. }
        ));
        // The reply carries the command's *real* configured deadline, matching the
        // live runner — not a zero duration.
        let cmd = Command::new("git").timeout(std::time::Duration::from_secs(7));
        match runner.run(&cmd).await.unwrap_err() {
            Error::Timeout { timeout, .. } => {
                assert_eq!(timeout, std::time::Duration::from_secs(7))
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn probe_reads_exit_code_as_bool() {
        use crate::error::Error;
        let runner = ScriptedRunner::new()
            .on(["yes"], Reply::ok(""))
            .on(["no"], Reply::fail(1, ""))
            .on(["boom"], Reply::fail(2, "bad"))
            .fallback(Reply::timeout());
        // 0 -> true, 1 -> false.
        assert!(runner.probe(&Command::new("t").arg("yes")).await.unwrap());
        assert!(!runner.probe(&Command::new("t").arg("no")).await.unwrap());
        // Any other code -> Exit error; no code (timeout) -> Timeout error.
        assert!(matches!(
            runner
                .probe(&Command::new("t").arg("boom"))
                .await
                .unwrap_err(),
            Error::Exit { code: 2, .. }
        ));
        assert!(matches!(
            runner
                .probe(&Command::new("t").arg("other"))
                .await
                .unwrap_err(),
            Error::Timeout { .. }
        ));
    }

    #[tokio::test]
    async fn run_ext_trims_and_checks_success() {
        let runner = ScriptedRunner::new().fallback(Reply::ok("  hello \n"));
        let trimmed = runner.run(&Command::new("echo")).await.unwrap();
        assert_eq!(trimmed, "  hello");
    }

    #[tokio::test]
    async fn recording_captures_args_cwd_and_absence() {
        let recorder = RecordingRunner::replying(Reply::ok("ok"));
        recorder
            .output(
                &Command::new("gh")
                    .current_dir("/repo")
                    .args(["pr", "create", "--title", "T"]),
            )
            .await
            .unwrap();

        let call = recorder.only_call();
        assert_eq!(call.program, OsString::from("gh"));
        assert_eq!(call.cwd, Some(OsString::from("/repo")));
        assert!(call.has_flag("--title"));
        assert!(!call.has_flag("--base"), "no --base flag was passed");
    }
}
