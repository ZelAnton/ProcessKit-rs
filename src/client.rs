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
    /// Environment overrides applied to every built command (`None` = remove),
    /// in registration order — e.g. `GIT_TERMINAL_PROMPT=0` set once instead of
    /// on every probe.
    envs: Vec<(OsString, Option<OsString>)>,
    /// A cancellation token applied to every built command (see
    /// [`default_cancel_on`](Self::default_cancel_on)).
    #[cfg(feature = "cancellation")]
    cancel: Option<tokio_util::sync::CancellationToken>,
}

// Manual: the runner type parameter carries no `Debug` bound, and (B4) the env
// *values* are never rendered — only the sorted variable names (the crate-wide
// secret-safety rule; see `lib.rs`).
impl<R: ProcessRunner> std::fmt::Debug for CliClient<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("CliClient");
        d.field("program", &self.program)
            .field("timeout", &self.timeout)
            .field("env_names", &crate::command::redacted_env_names(&self.envs));
        #[cfg(feature = "cancellation")]
        d.field("has_default_cancel", &self.cancel.is_some());
        d.finish_non_exhaustive()
    }
}

impl CliClient<JobRunner> {
    /// A client driving `program` through the real job-backed runner.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            runner: JobRunner,
            timeout: None,
            envs: Vec::new(),
            #[cfg(feature = "cancellation")]
            cancel: None,
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
            envs: Vec::new(),
            #[cfg(feature = "cancellation")]
            cancel: None,
        }
    }

    /// Apply a default timeout to every command this client builds.
    #[must_use]
    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set an environment variable on every command this client builds — e.g.
    /// `GIT_TERMINAL_PROMPT=0` so a probe can never block on a credential
    /// prompt. Per-command [`Command::env`] still works and is layered after.
    #[must_use]
    pub fn default_env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.envs.push((
            key.as_ref().to_os_string(),
            Some(value.as_ref().to_os_string()),
        ));
        self
    }

    /// Remove an inherited environment variable on every command this client
    /// builds.
    #[must_use]
    pub fn default_env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        self.envs.push((key.as_ref().to_os_string(), None));
        self
    }

    /// Cancel every command this client builds when `token` fires: each built
    /// command gets [`cancel_on(token.clone())`](Command::cancel_on), so
    /// cancelling the token kills every in-flight run of **this client** (the
    /// whole tree, surfacing [`Error::Cancelled`](crate::Error::Cancelled) on
    /// the awaiting call — same semantics as the per-command builder).
    ///
    /// **Precedence:** a per-command [`Command::cancel_on`] chained on a built
    /// command *replaces* the default (an explicit setting beats a default,
    /// like a per-command [`timeout`](Command::timeout) after
    /// [`default_timeout`](Self::default_timeout)). When both sources should
    /// fire, wire it explicitly — derive a child of the default
    /// (`let c = default.child_token()`), hand the command `cancel_on(c.clone())`,
    /// and have the second source call `c.cancel()` — or simply build a
    /// dedicated client per scope.
    ///
    /// Scope cancellation by client, not by call: clients are cheap — build
    /// one per cancellable scope and hand each its own token.
    #[cfg(feature = "cancellation")]
    #[must_use]
    pub fn default_cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = Some(token);
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

    /// A [`Command`] for `program <args>` in the current directory, defaults
    /// (timeout, env) pre-applied. Chain more builders (`.arg`, `.stdin`, …) for
    /// dynamic-argument commands.
    pub fn command<I, S>(&self, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.apply_defaults(Command::new(&self.program).args(args))
    }

    /// A [`Command`] for `program <args>` run in `dir`, defaults (timeout, env)
    /// pre-applied.
    pub fn command_in<I, S>(&self, dir: &Path, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.apply_defaults(Command::new(&self.program).current_dir(dir).args(args))
    }

    /// Apply this client's defaults (timeout, env overrides, cancellation
    /// token) to a freshly built command.
    fn apply_defaults(&self, command: Command) -> Command {
        let mut command = match self.timeout {
            Some(timeout) => command.timeout(timeout),
            None => command,
        };
        for (key, value) in &self.envs {
            command = match value {
                Some(value) => command.env(key, value),
                None => command.env_remove(key),
            };
        }
        // Applied at build time, so a later per-command `cancel_on` chained on
        // the returned command replaces it — the documented override precedence.
        #[cfg(feature = "cancellation")]
        if let Some(token) = &self.cancel {
            command = command.cancel_on(token.clone());
        }
        command
    }

    /// Run `command`, returning stdout (trailing whitespace trimmed) on success
    /// (errors on a non-zero exit) — the same verb, with the same semantics, as
    /// [`Command::run`](crate::Command::run) and
    /// [`ProcessRunnerExt::run`](crate::ProcessRunnerExt::run). Trims with
    /// `trim_end`: the trailing newline is noise, but leading whitespace can be
    /// significant.
    pub async fn run(&self, command: Command) -> Result<String> {
        Ok(self
            .runner
            .checked(&command)
            .await?
            .into_stdout()
            .trim_end()
            .to_owned())
    }

    /// Run `command`, capturing the full result without erroring on a non-zero
    /// exit — the same verb as [`ProcessRunner::output`].
    pub async fn output(&self, command: Command) -> Result<ProcessResult<String>> {
        self.runner.output(&command).await
    }

    /// Run `command` for its side effect, discarding stdout (errors on a
    /// non-zero exit) — the same verb as
    /// [`ProcessRunnerExt::run_unit`](crate::ProcessRunnerExt::run_unit).
    pub async fn run_unit(&self, command: Command) -> Result<()> {
        self.runner.run_unit(&command).await
    }

    /// Run `command` and return its exit code (e.g. `git diff --quiet`,
    /// `gh auth status`) — never errors on a non-zero exit. The same verb as
    /// [`Command::exit_code`](crate::Command::exit_code).
    pub async fn exit_code(&self, command: Command) -> Result<i32> {
        self.runner.exit_code(&command).await
    }

    /// Run a predicate `command` and read its exit code as a boolean: exit `0` →
    /// `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err`. Collapses the
    /// `match code { 0 => …, 1 => …, _ => Err }` idiom for commands whose exit
    /// code is the answer (`git diff --quiet`, `git show-ref --verify --quiet`,
    /// `grep -q`, …); other codes / timeout / signal-kill all error.
    pub async fn probe(&self, command: Command) -> Result<bool> {
        self.runner.probe(&command).await
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
///
/// D7: this macro is **committed public API**. Because it is `#[macro_export]`,
/// it lives at the crate root and is a stable part of the surface — kept (rather
/// than removed in favor of the hand-rolled wrapper) as the supported scaffold
/// for typed CLI wrappers. The hand-rolled equivalent (a struct wrapping
/// [`CliClient`]) remains valid and interchangeable.
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

            /// Set an environment variable on every command this client builds
            /// (e.g. `GIT_TERMINAL_PROMPT=0`).
            pub fn default_env(
                mut self,
                key: impl ::core::convert::AsRef<::std::ffi::OsStr>,
                value: impl ::core::convert::AsRef<::std::ffi::OsStr>,
            ) -> Self {
                self.core = self.core.default_env(key, value);
                self
            }

            /// Remove an inherited environment variable on every command this
            /// client builds.
            pub fn default_env_remove(
                mut self,
                key: impl ::core::convert::AsRef<::std::ffi::OsStr>,
            ) -> Self {
                self.core = self.core.default_env_remove(key);
                self
            }
        }

        $crate::__cli_client_cancellation!($name);
    };
}

/// Internal helper for [`cli_client!`]: emits the `default_cancel_on` builder
/// when **processkit** was built with the `cancellation` feature. A plain
/// `#[cfg]` inside the macro body would be evaluated against the *downstream*
/// crate's features (which may not even declare `cancellation`), so the gate
/// must live here, on which definition processkit exports.
#[cfg(feature = "cancellation")]
#[doc(hidden)]
#[macro_export]
macro_rules! __cli_client_cancellation {
    ($name:ident) => {
        impl<R: $crate::ProcessRunner> $name<R> {
            /// Cancel every command this client builds when `token` fires (a
            /// per-command `cancel_on` replaces the default — see
            /// `CliClient::default_cancel_on`).
            pub fn default_cancel_on(mut self, token: $crate::CancellationToken) -> Self {
                self.core = self.core.default_cancel_on(token);
                self
            }
        }
    };
}

/// Counterpart of the gated definition above: without the `cancellation`
/// feature the helper expands to nothing.
#[cfg(not(feature = "cancellation"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __cli_client_cancellation {
    ($name:ident) => {};
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::{Error, RecordingRunner, Reply, ScriptedRunner};

    #[test]
    fn debug_redacts_default_env_values_keeping_names() {
        // B4: CliClient Debug must surface default-env *names*, never values.
        let client = CliClient::new("git")
            .default_env("API_TOKEN", "topsecret-value")
            .default_env_remove("GIT_PAGER");
        let dbg = format!("{client:?}");
        assert!(
            !dbg.contains("topsecret-value"),
            "env value must not appear in Debug: {dbg}"
        );
        assert!(
            dbg.contains("API_TOKEN") && dbg.contains("GIT_PAGER"),
            "env names should appear: {dbg}"
        );
    }

    // A `vcs-git`-shaped wrapper, expressed on ProcessKit-rs with zero process
    // plumbing — the proof that the convenience layer fits a real consumer.
    crate::cli_client!(struct Demo => "git");

    impl<R: ProcessRunner> Demo<R> {
        async fn head(&self, dir: &Path) -> Result<String> {
            self.core
                .run(self.core.command_in(dir, ["rev-parse", "HEAD"]))
                .await
        }
        async fn is_clean(&self, dir: &Path) -> Result<bool> {
            Ok(self
                .core
                .exit_code(self.core.command_in(dir, ["diff", "--quiet"]))
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
    async fn run_trims_trailing_whitespace_only() {
        // `run` trims with `trim_end`: the trailing newline is dropped, but
        // leading whitespace is significant and preserved.
        let demo = Demo::with_runner(
            ScriptedRunner::new().on(["git", "rev-parse"], Reply::ok("  abc123 \n")),
        );
        assert_eq!(demo.head(Path::new(".")).await.unwrap(), "  abc123");
    }

    #[tokio::test]
    async fn exit_code_maps_exit_status() {
        let demo = Demo::with_runner(ScriptedRunner::new().on(["git", "diff"], Reply::fail(1, "")));
        assert!(!demo.is_clean(Path::new(".")).await.unwrap());
    }

    #[tokio::test]
    async fn parse_builds_a_typed_value() {
        let demo = Demo::with_runner(
            ScriptedRunner::new().on(["git", "branch"], Reply::ok("main\nfeature\n")),
        );
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
                .run(client.command_in(Path::new("/repo"), ["status"]))
                .await
                .unwrap(),
            "in-repo"
        );
        assert_eq!(
            client.run(client.command(["status"])).await.unwrap(),
            "elsewhere"
        );
    }

    #[tokio::test]
    async fn recording_runner_captures_args_cwd_and_absence() {
        let rec = RecordingRunner::replying(Reply::ok("https://gh/pr/2\n"));
        let client = CliClient::with_runner("gh", &rec);
        let _ = client
            .run(client.command_in(Path::new("/repo"), ["pr", "create", "--title", "T"]))
            .await
            .unwrap();

        let call = rec.only_call();
        assert_eq!(call.cwd.as_deref(), Some(std::path::Path::new("/repo")));
        assert_eq!(call.args_str(), ["pr", "create", "--title", "T"]);
        assert!(!call.has_flag("--base"), "no --base flag was passed");
    }

    #[tokio::test]
    async fn exit_code_errors_on_timeout() {
        // A timed-out run has no meaningful exit code: `code` must raise
        // Error::Timeout, not return the synthetic -1 (so a consumer like
        // `gh auth status` can't misread a timeout as "exited non-zero").
        let client = CliClient::with_runner("gh", ScriptedRunner::new().fallback(Reply::timeout()));
        assert!(matches!(
            client
                .exit_code(client.command(["auth", "status"]))
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

    #[tokio::test]
    async fn probe_maps_exit_code_to_bool() {
        let client = CliClient::with_runner(
            "git",
            ScriptedRunner::new()
                .on(["git", "diff"], Reply::fail(1, ""))
                .fallback(Reply::ok("")),
        );
        // `git diff --quiet` exits 1 (dirty) -> false; anything else (0) -> true.
        assert!(
            !client
                .probe(client.command(["diff", "--quiet"]))
                .await
                .unwrap()
        );
        assert!(client.probe(client.command(["status"])).await.unwrap());
    }

    #[tokio::test]
    async fn default_env_is_applied_to_every_command() {
        use std::ffi::OsString;
        let client = CliClient::new("git").default_env("GIT_TERMINAL_PROMPT", "0");
        // Set once on the client, present on each built command without a per-call
        // `.env`.
        for cmd in [
            client.command(["status"]),
            client.command_in(Path::new("."), ["fetch"]),
        ] {
            assert!(
                cmd.env_overrides()
                    .iter()
                    .any(|(k, v)| k == "GIT_TERMINAL_PROMPT"
                        && v.as_deref() == Some(OsString::from("0").as_os_str())),
                "default env missing on built command",
            );
        }
    }

    #[tokio::test]
    async fn default_env_reaches_the_invocation() {
        let rec = RecordingRunner::replying(Reply::ok("ok\n"));
        let client = CliClient::with_runner("git", &rec).default_env("GIT_TERMINAL_PROMPT", "0");
        let _ = client.run(client.command(["status"])).await.unwrap();
        let call = rec.only_call();
        assert!(
            call.envs
                .iter()
                .any(|(k, v)| k == "GIT_TERMINAL_PROMPT" && v.is_some()),
            "env override did not reach the runner: {:?}",
            call.envs
        );
    }

    #[cfg(feature = "cancellation")]
    #[tokio::test]
    async fn default_cancel_on_is_applied_to_every_command() {
        let token = crate::CancellationToken::new();
        let client = CliClient::new("git").default_cancel_on(token);
        for cmd in [
            client.command(["status"]),
            client.command_in(Path::new("."), ["fetch"]),
        ] {
            assert!(
                cmd.cancel_token().is_some(),
                "default token missing on built command"
            );
        }
        assert!(format!("{client:?}").contains("has_default_cancel: true"));
    }

    #[cfg(feature = "cancellation")]
    #[tokio::test(start_paused = true)]
    async fn per_command_cancel_on_overrides_the_default() {
        use crate::CancellationToken;
        // Pins the documented precedence: an explicit `cancel_on` on a built
        // command REPLACES the client default — the default token firing must
        // not resolve the call, the explicit one must.
        let default_token = CancellationToken::new();
        let explicit = CancellationToken::new();
        let client = CliClient::with_runner("gh", ScriptedRunner::new().fallback(Reply::pending()))
            .default_cancel_on(default_token.clone());
        let cmd = client.command(["run", "watch"]).cancel_on(explicit.clone());

        let call = client.output(cmd);
        tokio::pin!(call);
        default_token.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), &mut call)
                .await
                .is_err(),
            "the replaced default token must not cancel the call"
        );
        explicit.cancel();
        // Guarded await: a compound regression (no token at all) would park
        // forever — fail cleanly instead of hanging the suite.
        let err = tokio::time::timeout(Duration::from_secs(3600), call)
            .await
            .expect("the explicit token must resolve the call")
            .expect_err("explicit token cancels");
        assert!(matches!(err, Error::Cancelled { .. }), "got {err:?}");
    }

    #[cfg(feature = "cancellation")]
    #[tokio::test(start_paused = true)]
    async fn acceptance_pending_reply_with_client_default_cancel() {
        use crate::CancellationToken;
        // The vcs-toolkit spec's R2 acceptance, verbatim: a pending reply +
        // `default_cancel_on` — the call parks until the token fires, then
        // yields Cancelled naming the program, and the invocation is recorded.
        let token = CancellationToken::new();
        let rec = RecordingRunner::new(
            ScriptedRunner::new().on(["gh", "run", "watch"], Reply::pending()),
        );
        let client = CliClient::with_runner("gh", &rec).default_cancel_on(token.clone());

        let call = client.output(client.command(["run", "watch", "123"]));
        tokio::pin!(call);
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), &mut call)
                .await
                .is_err(),
            "must not resolve before the token fires"
        );
        token.cancel();
        // Guarded await: see per_command_cancel_on_overrides_the_default.
        match tokio::time::timeout(Duration::from_secs(3600), call)
            .await
            .expect("the cancelled token must resolve the call")
        {
            Err(Error::Cancelled { program }) => assert_eq!(program, "gh"),
            other => panic!("expected Error::Cancelled, got {other:?}"),
        }
        assert_eq!(rec.only_call().args_str(), ["run", "watch", "123"]);
    }

    #[cfg(feature = "cancellation")]
    #[test]
    fn macro_emits_default_cancel_on() {
        let _client = Demo::with_runner(ScriptedRunner::new())
            .default_cancel_on(crate::CancellationToken::new());
    }

    #[test]
    fn macro_generates_all_constructors() {
        // Exercises every method the `cli_client!` macro emits (these are public
        // API for real consumers; touch them here so our own build sees no
        // dead code). Construction only — no subprocess is spawned.
        let _real = Demo::new();
        let _default = Demo::default();
        let _fake = Demo::with_runner(ScriptedRunner::new())
            .default_timeout(Duration::from_secs(1))
            .default_env("GIT_TERMINAL_PROMPT", "0")
            .default_env_remove("GIT_PAGER");
    }
}
