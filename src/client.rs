//! [`CliClient`] — a small, reusable core for building typed wrappers around an
//! external CLI tool (`git`, `jj`, `gh`, …).
//!
//! It owns the program name, a [`ProcessRunner`], and an optional default
//! timeout; hands back preconfigured [`Command`]s; and provides the terminal
//! run/parse helpers a wrapper otherwise repeats. A wrapper then reduces to a
//! typed facade over its parsers, with no process plumbing — and is mockable by
//! construction, since the runner is injectable (pass a
//! [`ScriptedRunner`](crate::ScriptedRunner) in tests).
//!
//! The [`cli_client!`](crate::cli_client) macro scaffolds the wrapper struct and
//! its constructors.

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::time::Duration;

use crate::command::Command;
use crate::error::Result;
use crate::result::ProcessResult;
use crate::runner::{JobRunner, ProcessRunner, ProcessRunnerExt};

/// Owns a CLI tool's program name, [`ProcessRunner`], and default timeout, and
/// builds + runs [`Command`]s against them.
///
/// Generic over the runner so tests inject a fake; [`new`](Self::new) uses the
/// real job-backed [`JobRunner`].
pub struct CliClient<R: ProcessRunner = JobRunner> {
    program: OsString,
    runner: R,
    timeout: Option<Duration>,
}

impl CliClient<JobRunner> {
    /// A client driving `program` through the real job-backed runner.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            runner: JobRunner,
            timeout: None,
        }
    }
}

impl<R: ProcessRunner> CliClient<R> {
    /// A client driving `program` through `runner` — pass a fake in tests.
    pub fn with_runner(program: impl AsRef<OsStr>, runner: R) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            runner,
            timeout: None,
        }
    }

    /// Apply a default timeout to every command this client builds.
    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The injected runner — for direct [`ProcessRunner`]/[`ProcessRunnerExt`] use.
    pub fn runner(&self) -> &R {
        &self.runner
    }

    /// The default timeout, if one was set.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// A [`Command`] for `program <args>` in the current directory, default
    /// timeout pre-applied. Chain more builders (`.arg`, `.stdin`, …) for
    /// dynamic-argument commands.
    pub fn command<I, S>(&self, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.apply_timeout(Command::new(&self.program).args(args))
    }

    /// A [`Command`] for `program <args>` run in `dir`, default timeout pre-applied.
    pub fn command_in<I, S>(&self, dir: &Path, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.apply_timeout(Command::new(&self.program).current_dir(dir).args(args))
    }

    fn apply_timeout(&self, command: Command) -> Command {
        match self.timeout {
            Some(timeout) => command.timeout(timeout),
            None => command,
        }
    }

    /// Run `command`, returning trimmed stdout on success (errors on a non-zero exit).
    pub async fn text(&self, command: Command) -> Result<String> {
        Ok(self
            .runner
            .checked(&command)
            .await?
            .into_stdout()
            .trim()
            .to_owned())
    }

    /// Run `command`, capturing the result without erroring on a non-zero exit.
    pub async fn capture(&self, command: Command) -> Result<ProcessResult<String>> {
        self.runner.output(&command).await
    }

    /// Run `command` for its side effect, discarding stdout (errors on a non-zero exit).
    pub async fn unit(&self, command: Command) -> Result<()> {
        self.runner.checked(&command).await.map(drop)
    }

    /// Run `command` and return its exit code (e.g. `git diff --quiet`,
    /// `gh auth status`) — never errors on a non-zero exit.
    pub async fn code(&self, command: Command) -> Result<i32> {
        self.runner.exit_code(&command).await
    }

    /// Run `command` (errors on a non-zero exit) and feed its stdout to an
    /// infallible `parse` — the shape of git/jj struct-returning commands.
    pub async fn parse<T>(&self, command: Command, parse: impl FnOnce(&str) -> T) -> Result<T> {
        let out = self.runner.checked(&command).await?;
        Ok(parse(out.stdout()))
    }

    /// Run `command` (errors on a non-zero exit) and feed its stdout to a
    /// *fallible* `parse` — the shape of JSON deserialization, where a parse
    /// failure becomes [`Error::Parse`](crate::Error::Parse).
    pub async fn try_parse<T>(
        &self,
        command: Command,
        parse: impl FnOnce(&str) -> Result<T>,
    ) -> Result<T> {
        let out = self.runner.checked(&command).await?;
        parse(out.stdout())
    }
}

/// Scaffold a typed CLI-wrapper struct around a [`CliClient`].
///
/// Expands `cli_client!(pub struct Git => "git");` into a
/// `struct Git<R: ProcessRunner = JobRunner> { core: CliClient<R> }` with
/// `new()` (real runner), a `Default` impl, `with_runner(runner)`, and
/// `default_timeout(d)`. Implement the tool's typed methods on it, delegating to
/// `self.core` — see the crate docs for an example.
#[macro_export]
macro_rules! cli_client {
    ($(#[$meta:meta])* $vis:vis struct $name:ident => $binary:expr) => {
        $(#[$meta])*
        $vis struct $name<R: $crate::ProcessRunner = $crate::JobRunner> {
            core: $crate::CliClient<R>,
        }

        impl $name<$crate::JobRunner> {
            /// Create a client driving the real job-backed runner.
            pub fn new() -> Self {
                Self { core: $crate::CliClient::new($binary) }
            }
        }

        impl ::core::default::Default for $name<$crate::JobRunner> {
            fn default() -> Self {
                Self::new()
            }
        }

        impl<R: $crate::ProcessRunner> $name<R> {
            /// Create a client driving `runner` — inject a fake in tests.
            pub fn with_runner(runner: R) -> Self {
                Self { core: $crate::CliClient::with_runner($binary, runner) }
            }

            /// Apply a default timeout to every command this client builds.
            pub fn default_timeout(mut self, timeout: ::core::time::Duration) -> Self {
                self.core = self.core.default_timeout(timeout);
                self
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::{Error, RecordingRunner, Reply, ScriptedRunner};

    // A `vcs-git`-shaped wrapper, expressed on ProcessKit-rs with zero process
    // plumbing — the proof that the convenience layer fits a real consumer.
    crate::cli_client!(struct Demo => "git");

    impl<R: ProcessRunner> Demo<R> {
        async fn head(&self, dir: &Path) -> Result<String> {
            self.core
                .text(self.core.command_in(dir, ["rev-parse", "HEAD"]))
                .await
        }
        async fn is_clean(&self, dir: &Path) -> Result<bool> {
            Ok(self
                .core
                .code(self.core.command_in(dir, ["diff", "--quiet"]))
                .await?
                == 0)
        }
        async fn branches(&self, dir: &Path) -> Result<Vec<String>> {
            self.core
                .parse(self.core.command_in(dir, ["branch"]), |s| {
                    s.lines().map(|l| l.trim().to_owned()).collect()
                })
                .await
        }
    }

    #[tokio::test]
    async fn text_trims_and_drives_the_scripted_runner() {
        let demo =
            Demo::with_runner(ScriptedRunner::new().on(["rev-parse"], Reply::ok("  abc123\n")));
        assert_eq!(demo.head(Path::new(".")).await.unwrap(), "abc123");
    }

    #[tokio::test]
    async fn code_maps_exit_status() {
        let demo = Demo::with_runner(ScriptedRunner::new().on(["diff"], Reply::fail(1, "")));
        assert!(!demo.is_clean(Path::new(".")).await.unwrap());
    }

    #[tokio::test]
    async fn parse_builds_a_typed_value() {
        let demo =
            Demo::with_runner(ScriptedRunner::new().on(["branch"], Reply::ok("main\nfeature\n")));
        assert_eq!(
            demo.branches(Path::new(".")).await.unwrap(),
            vec!["main", "feature"]
        );
    }

    #[tokio::test]
    async fn try_parse_maps_failure_to_parse_error() {
        let client = CliClient::with_runner(
            "gh",
            ScriptedRunner::new().fallback(Reply::ok("not a number")),
        );
        let err = client
            .try_parse::<u32>(client.command(["x"]), |s| {
                s.trim().parse::<u32>().map_err(|e| Error::Parse {
                    program: "gh".into(),
                    message: e.to_string(),
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Parse { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn when_predicate_reads_public_command_accessors() {
        // Proves `Command`'s accessors are public enough for an external
        // `ScriptedRunner::when` predicate to inspect the command.
        let runner = ScriptedRunner::new()
            .when(
                |c| c.working_dir() == Some(Path::new("/repo")),
                Reply::ok("in-repo"),
            )
            .fallback(Reply::ok("elsewhere"));
        let client = CliClient::with_runner("git", runner);
        assert_eq!(
            client
                .text(client.command_in(Path::new("/repo"), ["status"]))
                .await
                .unwrap(),
            "in-repo"
        );
        assert_eq!(
            client.text(client.command(["status"])).await.unwrap(),
            "elsewhere"
        );
    }

    #[tokio::test]
    async fn recording_runner_captures_args_cwd_and_absence() {
        let rec = RecordingRunner::replying(Reply::ok("https://gh/pr/2\n"));
        let client = CliClient::with_runner("gh", &rec);
        let _ = client
            .text(client.command_in(Path::new("/repo"), ["pr", "create", "--title", "T"]))
            .await
            .unwrap();

        let call = rec.only_call();
        assert_eq!(call.cwd.as_deref(), Some(std::ffi::OsStr::new("/repo")));
        assert_eq!(call.args_str(), ["pr", "create", "--title", "T"]);
        assert!(!call.has_flag("--base"), "no --base flag was passed");
    }

    #[tokio::test]
    async fn code_errors_on_timeout() {
        // A timed-out run has no meaningful exit code: `code` must raise
        // Error::Timeout, not return the synthetic -1 (so a consumer like
        // `gh auth status` can't misread a timeout as "exited non-zero").
        let client = CliClient::with_runner("gh", ScriptedRunner::new().fallback(Reply::timeout()));
        assert!(matches!(
            client
                .code(client.command(["auth", "status"]))
                .await
                .unwrap_err(),
            Error::Timeout { .. }
        ));
    }

    #[tokio::test]
    async fn default_timeout_is_applied() {
        let client = CliClient::new("git").default_timeout(Duration::from_secs(7));
        assert_eq!(
            client.command(["status"]).configured_timeout(),
            Some(Duration::from_secs(7))
        );
    }

    #[test]
    fn macro_generates_all_constructors() {
        // Exercises every method the `cli_client!` macro emits (these are public
        // API for real consumers; touch them here so our own build sees no
        // dead code). Construction only — no subprocess is spawned.
        let _real = Demo::new();
        let _default = Demo::default();
        let _fake =
            Demo::with_runner(ScriptedRunner::new()).default_timeout(Duration::from_secs(1));
    }
}
