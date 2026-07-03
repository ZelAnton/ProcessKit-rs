//! [`CliClient`] — a small, reusable core for building typed wrappers around an
//! external CLI tool (`git`, `jj`, `gh`, …).
//!
//! It owns the program name, a [`ProcessRunner`], and an optional default
//! timeout; hands back preconfigured [`Command`]s; and provides the terminal
//! run/parse helpers a wrapper otherwise repeats. A wrapper then reduces to a
//! typed facade over its parsers, with no process plumbing — and is mockable by
//! construction, since the runner is injectable (pass a
//! [`ScriptedRunner`](crate::testing::ScriptedRunner) in tests).
//!
//! The [`cli_client!`](crate::cli_client) macro scaffolds the wrapper struct and
//! its constructors.

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::command::Command;
use crate::error::{Error, Result};
use crate::result::ProcessResult;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::runner::{JobRunner, ProcessRunner, ProcessRunnerExt};

mod sealed {
    use std::ffi::OsStr;
    pub trait Sealed {}
    impl Sealed for crate::Command {}
    impl<S: AsRef<OsStr>, const N: usize> Sealed for [S; N] {}
    impl<S: AsRef<OsStr>> Sealed for Vec<S> {}
    impl<S: AsRef<OsStr>> Sealed for &[S] {}
}

/// What a [`CliClient`] verb accepts: either an **argument list** — built
/// into a [`Command`] for the client's program with its defaults (timeout, env,
/// cancellation) applied — or a ready-made `Command`, run as-is.
///
/// This lets one verb serve both the common `git.run(["status"])` and the
/// customized `git.run(git.command(["push"]).timeout(d))`, removing the
/// double-mention of the older `git.run(git.command([…]))` form. Implemented for
/// argument containers (`[S; N]`, `Vec<S>`, `&[S]` where `S: AsRef<OsStr>`) and
/// for [`Command`]; **sealed** (not implementable downstream).
///
/// Either form receives the client's defaults (timeout / env /
/// [`default_cancel_on`](CliClient::default_cancel_on)): an argument list builds a
/// fresh command with them, and a ready-made [`Command`] has them **filled into
/// the gaps it left** — its own explicit settings win, but a client-wide cancel
/// token / timeout / env still reaches a per-call-customized command rather than
/// being silently dropped.
///
/// **Program note:** a ready-made [`Command`] keeps *its own* `program` — the
/// client fills only defaults, it does **not** substitute its program. So
/// `git_client.run(Command::new("rsync"))` runs `rsync`, but with git's
/// env/timeout/cancel defaults grafted on (e.g. `GIT_TERMINAL_PROMPT=0` on an
/// rsync process). Pass a [`Command`] to a client's verb only when you want *that
/// client's* defaults on it; otherwise build from an argument list (which uses
/// the client's own program) or run the command through its own client.
pub trait IntoCommand<R: ProcessRunner>: sealed::Sealed {
    /// Build the [`Command`] to run for `client` — used by the verbs.
    #[doc(hidden)]
    fn into_command(self, client: &CliClient<R>) -> Command;
}

impl<R: ProcessRunner> IntoCommand<R> for Command {
    fn into_command(self, client: &CliClient<R>) -> Command {
        // Fill defaults into the caller-supplied command's gaps; its explicit
        // settings win. Idempotent if `command()` already applied them.
        client.apply_defaults(self)
    }
}

impl<R: ProcessRunner, S: AsRef<OsStr>, const N: usize> IntoCommand<R> for [S; N] {
    fn into_command(self, client: &CliClient<R>) -> Command {
        client.command(self)
    }
}

impl<R: ProcessRunner, S: AsRef<OsStr>> IntoCommand<R> for Vec<S> {
    fn into_command(self, client: &CliClient<R>) -> Command {
        client.command(self)
    }
}

impl<R: ProcessRunner, S: AsRef<OsStr>> IntoCommand<R> for &[S] {
    fn into_command(self, client: &CliClient<R>) -> Command {
        client.command(self)
    }
}

/// Owns a CLI tool's program name, [`ProcessRunner`], and default timeout, and
/// builds + runs [`Command`]s against them.
///
/// Generic over the runner so tests inject a fake; [`new`](Self::new) uses the
/// real job-backed [`JobRunner`].
///
/// `Clone` when the runner is `Clone` — the default [`JobRunner`] is, as are
/// [`Command`] and [`Pipeline`](crate::Pipeline), so the whole CLI-wrapper family
/// clones uniformly (e.g. to hand an owned, `'static` value to a spawned task or
/// an async-runtime bridge). A clone copies the program, timeout, and env
/// defaults and **shares the same default cancellation token**
/// ([`default_cancel_on`](Self::default_cancel_on)): cancelling via one clone
/// cancels every command built from any of them, as a shared token should.
#[derive(Clone)]
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
    cancel: Option<tokio_util::sync::CancellationToken>,
    /// A retry policy + classifier applied to every built command (see
    /// [`default_retry`](Self::default_retry)).
    retry: Option<RetryConfig>,
    /// Per-build env resolvers — `(key, resolver)`, evaluated once when each
    /// command is built and baked into it (see
    /// [`default_env_fn`](Self::default_env_fn)).
    env_fns: Vec<(OsString, EnvResolver)>,
}

/// A closure that computes an environment value when a command is built — backs
/// [`CliClient::default_env_fn`]. `Arc`-shared so a `CliClient` stays `Clone`.
type EnvResolver = Arc<dyn Fn() -> OsString + Send + Sync>;

// Manual Debug: no `Debug` bound on R; env values omitted per secret-safety rule.
impl<R: ProcessRunner> std::fmt::Debug for CliClient<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("CliClient");
        d.field("program", &self.program)
            .field("timeout", &self.timeout)
            .field("env_names", &crate::command::redacted_env_names(&self.envs));
        d.field("has_default_cancel", &self.cancel.is_some());
        d.field("has_default_retry", &self.retry.is_some());
        // Dynamic-env *keys* only (names aren't secret; the resolved values are).
        if !self.env_fns.is_empty() {
            let keys: Vec<&OsString> = self.env_fns.iter().map(|(key, _)| key).collect();
            d.field("dynamic_env_keys", &keys);
        }
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
            cancel: None,
            retry: None,
            env_fns: Vec::new(),
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
            cancel: None,
            retry: None,
            env_fns: Vec::new(),
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
    ///
    /// **Duplicate keys: last registration wins** among the *static* env ops
    /// (`default_env`/[`default_env_remove`](Self::default_env_remove)), matching
    /// [`Command::env`]'s later-wins: `default_env("K","a").default_env("K","b")`
    /// yields `K=b`, and a later `default_env_remove("K")` supersedes an earlier
    /// set. This is *within* the static channel only — a static `default_env`
    /// always beats a [`default_env_fn`](Self::default_env_fn) for the same key
    /// regardless of registration order (the resolver is a fallback, never run when
    /// a static value is present).
    #[must_use]
    pub fn default_env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        let key = key.as_ref().to_os_string();
        self.envs
            .retain(|(k, _)| !crate::command::env_key_eq(k, &key));
        self.envs.push((key, Some(value.as_ref().to_os_string())));
        self
    }

    /// Remove an inherited environment variable on every command this client
    /// builds. Last registration wins for a repeated key (see
    /// [`default_env`](Self::default_env)).
    #[must_use]
    pub fn default_env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        let key = key.as_ref().to_os_string();
        self.envs
            .retain(|(k, _)| !crate::command::env_key_eq(k, &key));
        self.envs.push((key, None));
        self
    }

    /// Set an environment variable on every command this client builds to a value
    /// **computed once per built command** by `resolver` — the dynamic companion to
    /// [`default_env`](Self::default_env), for a value that must be refreshed for
    /// each new operation rather than frozen at client construction (a rotating
    /// token, a per-invocation request id, the current trace span). `resolver` runs
    /// when each command is built — i.e. once per `command()` / verb call — and the
    /// value is gap-filled like the other defaults: a per-command
    /// [`env`](Command::env) / [`env_remove`](Command::env_remove) for the same key
    /// wins, and an explicit static [`default_env`](Self::default_env) /
    /// [`default_env_remove`](Self::default_env_remove) for the same key takes
    /// precedence over the resolver (in which case the resolver is not run at all).
    /// Lets a typed wrapper
    /// drop the boilerplate of re-implementing every verb just to inject the value
    /// first.
    ///
    /// **Scope of "fresh":** the value is resolved when the command is built and
    /// then **baked into that command** — it is *not* re-resolved per process
    /// spawn. A built command that is retried (via [`default_retry`] /
    /// [`Command::retry`]) or run more than once reuses the value captured at build
    /// time on every attempt. So this refreshes per *operation*, not per *attempt*:
    /// suitable for a credential whose lifetime comfortably exceeds the retry
    /// window, not for a strictly single-use token that must differ between two
    /// attempts of the same command.
    ///
    /// `resolver` runs synchronously on the thread building the command (typically
    /// an async runtime worker), once per build — keep it cheap and non-blocking;
    /// cache or fall back inside it rather than blocking on I/O. It is infallible
    /// (returns a value, not a `Result`): do any fallible resolution inside and have
    /// it fall back. A panic propagates out of the build like any other; it does not
    /// corrupt the client. The resolved value lands in the command's env exactly
    /// like a static [`default_env`](Self::default_env) — and is never logged (env
    /// values are redacted from `Debug`/tracing). See [`Command::env`]'s **Secrets**
    /// note for typing/zeroizing a secret value at the call site.
    ///
    /// [`default_retry`]: Self::default_retry
    /// [`Command::env`]: crate::Command::env
    #[must_use]
    pub fn default_env_fn<V, F>(mut self, key: impl AsRef<OsStr>, resolver: F) -> Self
    where
        V: Into<OsString>,
        F: Fn() -> V + Send + Sync + 'static,
    {
        let key = key.as_ref().to_os_string();
        // Last registration wins for a repeated key, like `default_env`.
        self.env_fns
            .retain(|(k, _)| !crate::command::env_key_eq(k, &key));
        self.env_fns
            .push((key, Arc::new(move || resolver().into())));
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
    #[must_use]
    pub fn default_cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Retry **every** verb of this client whose failure surfaces as an [`Error`]
    /// the classifier accepts, on a shared [`RetryPolicy`] (exponential backoff +
    /// cap + jitter) — the client-wide analogue of the per-call
    /// [`Command::retry`](crate::Command::retry), filled into each built command
    /// the same way [`default_timeout`](Self::default_timeout) is. A per-command
    /// [`Command::retry`] / [`retry_with`](crate::Command::retry_with) **wins**
    /// (gap-fill, not override); this takes the same [`RetryPolicy`] as
    /// [`Command::retry_with`](crate::Command::retry_with), applied to every verb.
    ///
    /// Honored by the success-checking verbs — [`run`](Self::run) /
    /// [`run_unit`](Self::run_unit) / [`checked`](Self::checked) /
    /// [`exit_code`](Self::exit_code) / [`probe`](Self::probe) /
    /// [`parse`](Self::parse) / [`try_parse`](Self::try_parse) — the ones that
    /// surface failure as an [`Error`] the classifier can inspect
    /// (read it via [`is_transient`](crate::Error::is_transient) /
    /// [`is_timeout`](crate::Error::is_timeout) / [`combined`](crate::Error::combined)).
    /// The non-erroring `output_string`/`output_bytes` paths don't retry.
    ///
    /// **Each attempt re-executes the whole command** — a fresh process. Gate
    /// retries on a classifier that matches *pre-effect* failures; see
    /// [`Command::retry`]'s caveats on replayed side effects and one-shot stdin.
    #[must_use]
    pub fn default_retry(
        mut self,
        policy: RetryPolicy,
        retry_if: impl Fn(&Error) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retry = Some(RetryConfig::new(policy, retry_if));
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

    /// Fill the client's defaults into `command`, but only where the command has
    /// not set them itself — so a fresh [`command()`](Self::command) (no settings)
    /// gets every default, while a caller-supplied [`Command`] passed straight to
    /// a verb keeps its own explicit timeout/cancel/env and only fills the gaps
    /// (so a client-wide cancel token / timeout / env is not silently dropped
    /// when you customize a single call). Idempotent — running it twice (a verb
    /// applies it to a command that `command()` already defaulted) is a no-op
    /// the second time.
    fn apply_defaults(&self, mut command: Command) -> Command {
        if command.accepts_default_timeout()
            && let Some(timeout) = self.timeout
        {
            command = command.timeout(timeout);
        }
        if command.cancel_token().is_none()
            && let Some(token) = &self.cancel
        {
            command = command.cancel_on(token.clone());
        }
        command.fill_default_envs(&self.envs);
        // Dynamic env defaults, applied after the static ones: resolve and set each
        // only when the key is still absent — so a per-command `env` or an explicit
        // `default_env` wins, AND a resolver (which may do real work — read a vault)
        // never runs when the key is already set at the moment defaults are applied.
        for (key, resolver) in &self.env_fns {
            if !command.has_env_override(key) {
                command = command.env(key, resolver());
            }
        }
        command.fill_default_retry(&self.retry);
        command
    }

    /// Run, returning stdout (trailing whitespace trimmed) on success (errors on
    /// a non-zero exit) — the same verb, with the same semantics, as
    /// [`Command::run`](crate::Command::run) and
    /// [`ProcessRunnerExt::run`](crate::ProcessRunnerExt::run). Trims with
    /// `trim_end`: the trailing newline is noise, but leading whitespace can be
    /// significant.
    ///
    /// Accepts an argument list (`git.run(["status"])`) or a customized
    /// [`Command`] (`git.run(git.command(["push"]).timeout(d))`) — see
    /// [`IntoCommand`].
    pub async fn run(&self, call: impl IntoCommand<R>) -> Result<String> {
        let command = call.into_command(self);
        let result = self.runner.checked(&command).await?;
        let policy = command.output_buffer_policy();
        result.reject_if_truncated(policy.max_lines, policy.max_bytes)?;
        Ok(result.into_stdout().trim_end().to_owned())
    }

    /// Run, requiring an accepted exit, and return the full
    /// [`ProcessResult`] (untrimmed) — the [`CliClient`] analogue of
    /// [`ProcessRunnerExt::checked`](crate::ProcessRunnerExt::checked); the
    /// building block when you need the whole result after success-checking.
    pub async fn checked(&self, call: impl IntoCommand<R>) -> Result<ProcessResult<String>> {
        self.runner.checked(&call.into_command(self)).await
    }

    /// Run, capturing the full result without erroring on a non-zero exit — the
    /// same verb as [`ProcessRunner::output_string`].
    pub async fn output_string(&self, call: impl IntoCommand<R>) -> Result<ProcessResult<String>> {
        self.runner.output_string(&call.into_command(self)).await
    }

    /// Run, capturing stdout as **raw bytes** (stderr as text), without erroring
    /// on a non-zero exit — the same verb as [`ProcessRunner::output_bytes`].
    /// For binary tools whose stdout is not UTF-8.
    pub async fn output_bytes(&self, call: impl IntoCommand<R>) -> Result<ProcessResult<Vec<u8>>> {
        self.runner.output_bytes(&call.into_command(self)).await
    }

    /// Run for the side effect, discarding stdout (errors on a non-zero exit) —
    /// the same verb as
    /// [`ProcessRunnerExt::run_unit`](crate::ProcessRunnerExt::run_unit).
    pub async fn run_unit(&self, call: impl IntoCommand<R>) -> Result<()> {
        self.runner.run_unit(&call.into_command(self)).await
    }

    /// Run and return the exit code (e.g. `git diff --quiet`, `gh auth status`)
    /// — never errors on a non-zero exit. The same verb as
    /// [`Command::exit_code`](crate::Command::exit_code).
    pub async fn exit_code(&self, call: impl IntoCommand<R>) -> Result<i32> {
        self.runner.exit_code(&call.into_command(self)).await
    }

    /// Run a predicate and read its exit code as a boolean: exit `0` →
    /// `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err`. Collapses the
    /// `match code { 0 => …, 1 => …, _ => Err }` idiom for commands whose exit
    /// code is the answer (`git diff --quiet`, `git show-ref --verify --quiet`,
    /// `grep -q`, …); other codes / timeout / signal-kill all error.
    pub async fn probe(&self, call: impl IntoCommand<R>) -> Result<bool> {
        self.runner.probe(&call.into_command(self)).await
    }

    /// Stream stdout and return the first line matching `predicate` (`None` if
    /// the stream ends first) — the [`CliClient`] analogue of
    /// [`ProcessRunnerExt::first_line`](crate::ProcessRunnerExt::first_line),
    /// bounded by the command's [`timeout`](crate::Command::timeout).
    pub async fn first_line<F>(
        &self,
        call: impl IntoCommand<R>,
        predicate: F,
    ) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool + Send,
    {
        self.runner
            .first_line(&call.into_command(self), predicate)
            .await
    }

    /// Run (errors on a non-zero exit) and feed stdout to an infallible
    /// `parse` — the shape of git/jj struct-returning commands. Fails loud on a
    /// bounded-buffer truncation. Delegates to
    /// [`ProcessRunnerExt::parse`](crate::ProcessRunnerExt::parse).
    pub async fn parse<T, F>(&self, call: impl IntoCommand<R>, parse: F) -> Result<T>
    where
        T: Send,
        F: FnOnce(&str) -> T + Send,
    {
        self.runner.parse(&call.into_command(self), parse).await
    }

    /// Run (errors on a non-zero exit) and feed stdout to a *fallible* `parse` —
    /// the shape of JSON deserialization, where a parse failure becomes
    /// [`Error::Parse`](crate::Error::Parse). Fails loud on a bounded-buffer
    /// truncation. Delegates to
    /// [`ProcessRunnerExt::try_parse`](crate::ProcessRunnerExt::try_parse).
    pub async fn try_parse<T, F>(&self, call: impl IntoCommand<R>, parse: F) -> Result<T>
    where
        T: Send,
        F: FnOnce(&str) -> Result<T> + Send,
    {
        self.runner.try_parse(&call.into_command(self), parse).await
    }
}

/// Scaffold a typed CLI-wrapper struct around a [`CliClient`].
///
/// Expands `cli_client!(pub struct Git => "git");` into a
/// `struct Git<R: ProcessRunner = JobRunner> { core: CliClient<R> }` with
/// `new()` (real runner), a `Default` impl, `with_runner(runner)`, and
/// `default_timeout(d)`. Implement the tool's typed methods on it, delegating to
/// `self.core` — see the *Wrapping a CLI tool* section of the crate's
/// `docs/testing.md` guide for a worked example.
///
/// This macro is **committed public API**. Because it is `#[macro_export]`,
/// it lives at the crate root and is a stable part of the surface — the
/// supported scaffold for typed CLI wrappers. The hand-rolled equivalent (a
/// struct wrapping [`CliClient`]) remains valid and interchangeable.
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

            /// Set an env variable on every command to a value computed per built
            /// command (see `CliClient::default_env_fn`).
            pub fn default_env_fn<V, F>(
                mut self,
                key: impl ::core::convert::AsRef<::std::ffi::OsStr>,
                resolver: F,
            ) -> Self
            where
                V: ::core::convert::Into<::std::ffi::OsString>,
                F: ::core::ops::Fn() -> V
                    + ::core::marker::Send
                    + ::core::marker::Sync
                    + 'static,
            {
                self.core = self.core.default_env_fn(key, resolver);
                self
            }
        }

        impl<R: $crate::ProcessRunner> $name<R> {
            /// Cancel every command this client builds when `token` fires (a
            /// per-command `cancel_on` replaces the default — see
            /// `CliClient::default_cancel_on`).
            pub fn default_cancel_on(mut self, token: $crate::CancellationToken) -> Self {
                self.core = self.core.default_cancel_on(token);
                self
            }

            /// Retry every verb on a shared `RetryPolicy` + classifier
            /// (see `CliClient::default_retry`).
            pub fn default_retry(
                mut self,
                policy: $crate::RetryPolicy,
                retry_if: impl Fn(&$crate::Error) -> bool
                    + ::core::marker::Send
                    + ::core::marker::Sync
                    + 'static,
            ) -> Self {
                self.core = self.core.default_retry(policy, retry_if);
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
    use crate::Error;
    use crate::testing::{RecordingRunner, Reply, ScriptedRunner};

    #[test]
    fn debug_redacts_default_env_values_keeping_names() {
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

    #[test]
    fn client_is_clone_with_the_default_runner() {
        // `Command`/`Pipeline` are `Clone`; so is the default-runner `CliClient`,
        // so the whole CLI-wrapper family clones uniformly (e.g. to own a `'static`
        // value for a spawned task or an async-runtime bridge).
        fn assert_clone<T: Clone>() {}
        assert_clone::<CliClient>();

        let client = CliClient::new("git")
            .default_timeout(Duration::from_secs(3))
            .default_env("GIT_TERMINAL_PROMPT", "0")
            .default_cancel_on(tokio_util::sync::CancellationToken::new());
        let clone = client.clone();
        // Every field (program, timeout, env defaults, and the presence of a
        // shared cancel token) survives the clone verbatim.
        assert_eq!(format!("{client:?}"), format!("{clone:?}"));
    }

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
            .try_parse::<u32, _>(client.command(["x"]), |s| {
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
    async fn verbs_accept_args_directly_or_a_customized_command() {
        use std::time::Duration;
        let runner = ScriptedRunner::new().on(["git", "status"], Reply::ok("clean"));
        let client = CliClient::with_runner("git", runner);

        // Argument list — the program comes from the client, defaults applied.
        assert_eq!(client.run(["status"]).await.unwrap(), "clean");
        assert_eq!(client.run(vec!["status"]).await.unwrap(), "clean");
        // A customized Command runs through the same verb (pass-through).
        let custom = client.command(["status"]).timeout(Duration::from_secs(3));
        assert_eq!(custom.configured_timeout(), Some(Duration::from_secs(3)));
        assert_eq!(client.run(custom).await.unwrap(), "clean");
        let args = ["status"];
        assert_eq!(client.run(&args[..]).await.unwrap(), "clean");
        let result = client.checked(["status"]).await.unwrap();
        assert_eq!(result.stdout(), "clean");
    }

    #[tokio::test]
    async fn first_line_verb_streams_and_matches() {
        let runner =
            ScriptedRunner::new().on(["git", "log"], Reply::lines(["one", "two", "three"]));
        let client = CliClient::with_runner("git", runner);
        let found = client
            .first_line(["log"], |line| line.starts_with('t'))
            .await
            .unwrap();
        assert_eq!(found.as_deref(), Some("two"));
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

    #[test]
    fn duplicate_default_env_is_last_registration_wins() {
        use std::ffi::OsString;
        // G3: matches Command::env's later-wins (was first-wins).
        let client = CliClient::new("tool")
            .default_env("K", "a")
            .default_env("K", "b");
        let cmd = client.command(["x"]);
        let vals: Vec<_> = cmd
            .env_overrides()
            .iter()
            .filter(|(k, _)| k == "K")
            .collect();
        assert_eq!(
            vals.len(),
            1,
            "duplicate default_env collapses to one entry"
        );
        assert_eq!(
            vals[0].1.as_deref(),
            Some(OsString::from("b").as_os_str()),
            "last registration wins"
        );
        // A later remove of the same key supersedes an earlier set.
        let removed = CliClient::new("tool")
            .default_env("K", "a")
            .default_env_remove("K")
            .command(["x"]);
        let k: Vec<_> = removed
            .env_overrides()
            .iter()
            .filter(|(k, _)| k == "K")
            .collect();
        assert_eq!(k.len(), 1);
        assert_eq!(k[0].1, None, "the later remove wins");

        // Cross-channel: last-wins is *within* a channel. A static `default_env`
        // beats a `default_env_fn` for the same key EVEN when the fn is registered
        // later — the resolver is a fallback, so static-beats-dynamic is orthogonal
        // to registration order.
        let static_wins = CliClient::new("tool")
            .default_env("K", "static")
            .default_env_fn("K", || "dynamic")
            .command(["x"]);
        assert!(
            static_wins.env_overrides().iter().any(
                |(k, v)| k == "K" && v.as_deref() == Some(OsString::from("static").as_os_str())
            ),
            "a static default_env beats a later-registered default_env_fn"
        );
    }

    #[test]
    fn no_timeout_command_ignores_a_client_default_timeout() {
        // G4: an explicitly-unbounded command opts out of the client gap-fill.
        let client = CliClient::new("tail").default_timeout(Duration::from_secs(9));
        let bounded = Command::new("tail").into_command(&client);
        assert_eq!(
            bounded.configured_timeout(),
            Some(Duration::from_secs(9)),
            "the default fills an unset command"
        );
        let unbounded = Command::new("tail").no_timeout().into_command(&client);
        assert_eq!(
            unbounded.configured_timeout(),
            None,
            "no_timeout opts out of the client default_timeout"
        );
    }

    #[test]
    fn env_isolation_ignores_client_env_defaults() {
        // G2: a client default_env must not pierce env_clear/inherit_env isolation.
        let client = CliClient::new("tool").default_env("LANG", "C");
        let isolated = Command::new("tool").env_clear().into_command(&client);
        assert!(
            !isolated.env_overrides().iter().any(|(k, _)| k == "LANG"),
            "env_clear isolates from a client default_env"
        );
        let plain = Command::new("tool").into_command(&client);
        assert!(
            plain.env_overrides().iter().any(|(k, _)| k == "LANG"),
            "a non-isolated command still gets the client default"
        );
    }

    #[tokio::test]
    async fn a_prebuilt_command_passed_to_a_verb_still_gets_client_defaults() {
        let token = crate::CancellationToken::new();
        let client = CliClient::new("git")
            .default_timeout(Duration::from_secs(9))
            .default_env("GIT_TERMINAL_PROMPT", "0")
            .default_cancel_on(token);

        // Built WITHOUT the client (no defaults applied yet).
        let raw = Command::new("git").args(["push"]);
        let filled = raw.into_command(&client);
        assert_eq!(
            filled.configured_timeout(),
            Some(Duration::from_secs(9)),
            "the client default timeout fills the gap"
        );
        assert!(
            filled.cancel_token().is_some(),
            "the client cancel token reaches it"
        );
        assert!(
            filled
                .env_overrides()
                .iter()
                .any(|(k, _)| k == "GIT_TERMINAL_PROMPT"),
            "the client default env reaches it"
        );

        let explicit = Command::new("git")
            .args(["push"])
            .timeout(Duration::from_secs(2))
            .env("GIT_TERMINAL_PROMPT", "1");
        let filled = explicit.into_command(&client);
        assert_eq!(
            filled.configured_timeout(),
            Some(Duration::from_secs(2)),
            "an explicit per-command timeout wins"
        );
        let prompt: Vec<_> = filled
            .env_overrides()
            .iter()
            .filter(|(k, _)| k == "GIT_TERMINAL_PROMPT")
            .collect();
        assert_eq!(prompt.len(), 1, "no duplicate env op for the same key");
        assert_eq!(
            prompt[0].1.as_deref(),
            Some(std::ffi::OsStr::new("1")),
            "the per-command env value wins over the client default"
        );
    }

    #[tokio::test]
    async fn prebuilt_command_env_wins_over_a_case_differing_client_default() {
        let client = CliClient::new("git").default_env("Path", "from-client");
        let cmd = Command::new("git").env("PATH", "from-command");
        let filled = cmd.into_command(&client);
        let path_ops: Vec<_> = filled
            .env_overrides()
            .iter()
            .filter(|(k, _)| k.to_str().is_some_and(|k| k.eq_ignore_ascii_case("PATH")))
            .collect();
        #[cfg(windows)]
        {
            assert_eq!(
                path_ops.len(),
                1,
                "the case-differing client default for the same var is skipped"
            );
            assert_eq!(
                path_ops[0].1.as_deref(),
                Some(std::ffi::OsStr::new("from-command")),
                "the explicit per-command value wins"
            );
        }
        #[cfg(not(windows))]
        {
            assert_eq!(
                path_ops.len(),
                2,
                "on Unix PATH and Path are distinct variables — both kept"
            );
        }
    }

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

    #[tokio::test(start_paused = true)]
    async fn per_command_cancel_on_overrides_the_default() {
        use crate::CancellationToken;
        let default_token = CancellationToken::new();
        let explicit = CancellationToken::new();
        let client = CliClient::with_runner("gh", ScriptedRunner::new().fallback(Reply::pending()))
            .default_cancel_on(default_token.clone());
        let cmd = client.command(["run", "watch"]).cancel_on(explicit.clone());

        let call = client.output_string(cmd);
        tokio::pin!(call);
        default_token.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), &mut call)
                .await
                .is_err(),
            "the replaced default token must not cancel the call"
        );
        explicit.cancel();
        let err = tokio::time::timeout(Duration::from_secs(3600), call)
            .await
            .expect("the explicit token must resolve the call")
            .expect_err("explicit token cancels");
        assert!(matches!(err, Error::Cancelled { .. }), "got {err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn acceptance_pending_reply_with_client_default_cancel() {
        use crate::CancellationToken;
        let token = CancellationToken::new();
        let rec = RecordingRunner::new(
            ScriptedRunner::new().on(["gh", "run", "watch"], Reply::pending()),
        );
        let client = CliClient::with_runner("gh", &rec).default_cancel_on(token.clone());

        let call = client.output_string(client.command(["run", "watch", "123"]));
        tokio::pin!(call);
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), &mut call)
                .await
                .is_err(),
            "must not resolve before the token fires"
        );
        token.cancel();
        match tokio::time::timeout(Duration::from_secs(3600), call)
            .await
            .expect("the cancelled token must resolve the call")
        {
            Err(Error::Cancelled { program }) => assert_eq!(program, "gh"),
            other => panic!("expected Error::Cancelled, got {other:?}"),
        }
        assert_eq!(rec.only_call().args_str(), ["run", "watch", "123"]);
    }

    #[tokio::test(start_paused = true)]
    async fn clone_shares_the_default_cancel_token() {
        // The load-bearing half of `CliClient: Clone`'s doc: a clone shares the
        // SAME default cancel token, not a copy. A command built from the *clone*
        // must therefore respond to the token the *original* was built with —
        // parking until it fires, then resolving as `Cancelled`.
        use crate::CancellationToken;
        let token = CancellationToken::new();
        let rec = RecordingRunner::new(
            ScriptedRunner::new().on(["gh", "run", "watch"], Reply::pending()),
        );
        let original = CliClient::with_runner("gh", &rec).default_cancel_on(token.clone());
        let clone = original.clone();

        let call = clone.output_string(clone.command(["run", "watch", "123"]));
        tokio::pin!(call);
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), &mut call)
                .await
                .is_err(),
            "the clone's command must park on the shared token, not resolve early"
        );
        token.cancel();
        match tokio::time::timeout(Duration::from_secs(3600), call)
            .await
            .expect("cancelling the shared token must resolve the clone's call")
        {
            Err(Error::Cancelled { program }) => assert_eq!(program, "gh"),
            other => panic!("expected Error::Cancelled, got {other:?}"),
        }
    }

    #[test]
    fn macro_emits_default_cancel_on() {
        let _client = Demo::with_runner(ScriptedRunner::new())
            .default_cancel_on(crate::CancellationToken::new());
    }

    #[test]
    fn macro_emits_default_env_fn() {
        let _client = Demo::with_runner(ScriptedRunner::new()).default_env_fn("TOKEN", || "x");
    }

    #[test]
    fn default_env_fn_resolves_per_build_and_respects_precedence() {
        use crate::testing::Invocation;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);
        let client = CliClient::new("git").default_env_fn("TOKEN", move || {
            format!("t{}", c.fetch_add(1, Ordering::SeqCst))
        });

        // Each built command resolves a FRESH value.
        let inv1 = Invocation::from_command(&client.command(["status"]));
        let inv2 = Invocation::from_command(&client.command(["status"]));
        assert!(inv1.env_is("TOKEN", "t0"));
        assert!(inv2.env_is("TOKEN", "t1"));

        // A pre-built command that sets TOKEN keeps its own value, and the resolver
        // is NOT invoked for it (the counter stays at the 2 resolves above).
        let filled = client.apply_defaults(Command::new("git").env("TOKEN", "explicit"));
        assert!(Invocation::from_command(&filled).env_is("TOKEN", "explicit"));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "the resolver must be skipped when the key is already set"
        );
    }

    #[test]
    fn default_env_fn_precedence_against_static_and_self() {
        use crate::testing::Invocation;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        // (a) A static `default_env` for the same key wins, and the resolver is
        //     skipped entirely (the doc's stated precedence).
        let calls = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&calls);
        let client = CliClient::new("git")
            .default_env("TOKEN", "static")
            .default_env_fn("TOKEN", move || {
                c.fetch_add(1, Ordering::SeqCst);
                "dynamic"
            });
        let inv = Invocation::from_command(&client.command(["status"]));
        assert!(inv.env_is("TOKEN", "static"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "a static default_env must shadow the resolver, which then never runs"
        );

        // (b) A per-command `env_remove` for the key suppresses the resolver too —
        //     an explicit unset beats the dynamic default, like the static one.
        let calls = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&calls);
        let client = CliClient::new("git").default_env_fn("TOKEN", move || {
            c.fetch_add(1, Ordering::SeqCst);
            "dynamic"
        });
        let removed = client.apply_defaults(Command::new("git").env_remove("TOKEN"));
        assert!(matches!(
            Invocation::from_command(&removed).env("TOKEN"),
            Some(None)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        // (c) Two resolvers for the same key: the LAST registered wins (G3 —
        //     consistent with `default_env`'s later-wins), and the superseded
        //     first resolver never runs (it is dropped at registration time).
        let first_ran = Arc::new(AtomicU32::new(0));
        let f = Arc::clone(&first_ran);
        let client = CliClient::new("git")
            .default_env_fn("TOKEN", move || {
                f.fetch_add(1, Ordering::SeqCst);
                "first"
            })
            .default_env_fn("TOKEN", || "second");
        let inv = Invocation::from_command(&client.command(["status"]));
        assert!(
            inv.env_is("TOKEN", "second"),
            "the last-registered resolver wins"
        );
        assert_eq!(
            first_ran.load(Ordering::SeqCst),
            0,
            "the superseded resolver never runs"
        );
    }

    #[test]
    fn default_env_fn_value_is_baked_in_and_stable_across_clone_and_reuse() {
        use crate::testing::Invocation;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);
        let client = CliClient::new("git").default_env_fn("TOKEN", move || {
            format!("t{}", c.fetch_add(1, Ordering::SeqCst))
        });

        // A built command captures its value; re-running it (here: re-deriving the
        // Invocation) does not re-resolve — the retry loop reuses this same value.
        let built = client.command(["status"]);
        assert!(Invocation::from_command(&built).env_is("TOKEN", "t0"));
        assert!(Invocation::from_command(&built).env_is("TOKEN", "t0"));

        // A clone shares the resolver's state (Arc), so it continues the sequence
        // rather than restarting it — documents the shared-state semantics.
        let clone = client.clone();
        assert!(Invocation::from_command(&clone.command(["status"])).env_is("TOKEN", "t1"));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn default_retry_retries_client_verbs() {
        use crate::RetryPolicy;
        // on_sequence: a retryable failure then success — the client-wide retry
        // re-runs the verb. Exercises the macro-generated `default_retry`, the
        // apply_defaults gap-fill, and the retry loop end-to-end.
        let demo = Demo::with_runner(ScriptedRunner::new().on_sequence(
            ["git", "rev-parse"],
            [
                Reply::fail(128, "could not read from remote"),
                Reply::ok("abc123\n"),
            ],
        ))
        .default_retry(RetryPolicy::new().initial_backoff(Duration::ZERO), |_e| {
            true
        });
        // `head` runs `git rev-parse HEAD`: attempt 1 fails, the retry succeeds.
        assert_eq!(demo.head(Path::new(".")).await.unwrap(), "abc123");
    }

    #[tokio::test]
    async fn default_retry_classifier_can_decline() {
        use crate::RetryPolicy;
        // A classifier that rejects the error → no retry; the first failure surfaces.
        let demo = Demo::with_runner(ScriptedRunner::new().on_sequence(
            ["git", "rev-parse"],
            [Reply::fail(128, "fatal"), Reply::ok("abc\n")],
        ))
        .default_retry(RetryPolicy::new().initial_backoff(Duration::ZERO), |_e| {
            false
        });
        assert!(demo.head(Path::new(".")).await.is_err());
    }

    #[test]
    fn macro_generates_all_constructors() {
        let _real = Demo::new();
        let _default = Demo::default();
        let _fake = Demo::with_runner(ScriptedRunner::new())
            .default_timeout(Duration::from_secs(1))
            .default_env("GIT_TERMINAL_PROMPT", "0")
            .default_env_remove("GIT_PAGER");
    }
}
