//! The [`ProcessRunner`] seam and its real implementations.
//!
//! The seam covers both shapes of a run: [`ProcessRunner::output`] (a finished
//! [`ProcessResult`]) and [`ProcessRunner::start`] (a live [`RunningProcess`]
//! for streaming/probes). A [`ScriptedRunner`](crate::ScriptedRunner) fakes
//! both — its `start` hands back a scripted handle that feeds canned lines
//! through the same pump machinery a real child uses.

use crate::command::{Command, RetryPolicy, find_in_path, is_bare_name};
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::ProcessResult;
use crate::running::{RunningProcess, Spawned};

/// Runs a [`Command`] — to a captured result ([`output`](Self::output)) or a
/// live handle ([`start`](Self::start)).
///
/// This two-method seam is the mock point: production code takes
/// `&dyn ProcessRunner`; tests pass a
/// [`ScriptedRunner`](crate::ScriptedRunner) /
/// [`RecordingRunner`](crate::RecordingRunner) (or, behind the `mock` feature,
/// a generated `MockRunner`) instead of spawning real processes.
#[cfg_attr(feature = "mock", mockall::automock)]
#[async_trait::async_trait]
pub trait ProcessRunner: Send + Sync {
    /// Run `command` to completion, capturing stdout/stderr and the exit code.
    /// A non-zero exit is reported in the result, not raised.
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>>;

    /// Start `command` and return a live [`RunningProcess`] for streaming,
    /// readiness probes, or incremental consumption.
    ///
    /// Defaulted to [`Error::Unsupported`](crate::Error::Unsupported) so an
    /// `output`-only runner (a hand-rolled double, a cassette runner) keeps
    /// compiling; the real runners ([`JobRunner`], `&ProcessGroup`) and
    /// [`ScriptedRunner`](crate::ScriptedRunner) override it.
    ///
    /// D4: this is deliberately a **runtime** capability (a default that errors)
    /// rather than a compile-time split (e.g. a separate `ProcessStarter:
    /// ProcessRunner` supertrait). The trade-off is intentional: an output-only
    /// runner stays a one-method `impl`, at the cost that calling a streaming
    /// verb on one surfaces `Unsupported` at run time instead of failing to
    /// compile. Check [`RunningProcess`] support out-of-band if you need the
    /// guarantee statically.
    async fn start(&self, command: &Command) -> Result<RunningProcess> {
        let _ = command;
        Err(crate::Error::Unsupported {
            operation: "start".into(),
        })
    }
}

/// A shared reference to a runner is itself a runner, so a borrowed
/// [`RecordingRunner`](crate::RecordingRunner) (or any `&R`) can be injected
/// where a `ProcessRunner` is expected.
#[async_trait::async_trait]
impl<R: ProcessRunner + ?Sized> ProcessRunner for &R {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        (**self).output(command).await
    }

    async fn start(&self, command: &Command) -> Result<RunningProcess> {
        (**self).start(command).await
    }
}

/// Convenience methods available on every [`ProcessRunner`] (including
/// `&dyn ProcessRunner`), layered over [`output`](ProcessRunner::output).
#[async_trait::async_trait]
pub trait ProcessRunnerExt: ProcessRunner {
    /// Run, require an **accepted** exit, and return trimmed stdout. Accepted is
    /// `0` by default, widened by [`Command::ok_codes`](crate::Command::ok_codes);
    /// any other code is [`Error::Exit`](crate::Error::Exit).
    async fn run(&self, command: &Command) -> Result<String> {
        Ok(self
            .checked(command)
            .await?
            .into_stdout()
            .trim_end()
            .to_owned())
    }

    /// Run for the side effect: require an **accepted** exit (`0`, or any code in
    /// [`Command::ok_codes`](crate::Command::ok_codes)), discard the output.
    async fn run_unit(&self, command: &Command) -> Result<()> {
        self.checked(command).await.map(drop)
    }

    /// Run and return just the exit code. A run that produced no code surfaces as
    /// an error — a timeout as [`Error::Timeout`](crate::Error::Timeout), a
    /// signal-kill as [`Error::Signalled`](crate::Error::Signalled) — rather than a
    /// synthetic sentinel, mirroring
    /// [`ensure_success`](crate::ProcessResult::ensure_success).
    async fn exit_code(&self, command: &Command) -> Result<i32> {
        retrying(command.retry_policy(), || async {
            self.output(command).await?.require_code()
        })
        .await
    }

    /// Run a predicate command and read its exit code as a boolean: exit `0` →
    /// `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err` (other code as
    /// [`Error::Exit`](crate::Error::Exit), timeout as
    /// [`Error::Timeout`](crate::Error::Timeout), signal-kill as
    /// [`Error::Signalled`](crate::Error::Signalled)). For
    /// commands whose exit code *is* the answer — `git diff --quiet`, `grep -q`, …
    async fn probe(&self, command: &Command) -> Result<bool> {
        retrying(command.retry_policy(), || async {
            let result = self.output(command).await?;
            match result.code() {
                Some(0) => Ok(true),
                Some(1) => Ok(false),
                // Any other code (or no code: timeout / signal) is not a yes/no
                // answer — reuse ensure_success to build the faithful error.
                // Reset `ok_codes` to the default {0} first: `probe` keeps its
                // strict 0/1 contract regardless of a command's `ok_codes`, and
                // an *accepted* non-{0,1} code would otherwise make
                // `ensure_success` return `Ok` and panic the `expect_err`.
                _ => Err(result
                    .with_ok_codes(vec![0])
                    .ensure_success()
                    .expect_err("a non-{0,1} exit code is never success")),
            }
        })
        .await
    }

    /// Run, require a zero exit, and return the full captured result (untrimmed
    /// stdout). The building block for the `parse`/`try_parse` helpers — use it
    /// when you need the whole `ProcessResult` after success-checking, rather
    /// than just trimmed stdout (`run`) or the raw result (`output`).
    async fn checked(&self, command: &Command) -> Result<ProcessResult<String>> {
        retrying(command.retry_policy(), || async {
            self.output(command).await?.ensure_success()
        })
        .await
    }

    /// Stream `command`'s stdout and return the first line matching `predicate`
    /// (`None` if the stream ends first), bounded by the command's
    /// [`timeout`](crate::Command::timeout) (a `Some` deadline surfaces as
    /// [`Error::Timeout`](crate::Error::Timeout) and tears the tree down).
    ///
    /// D6: routes through [`start`](ProcessRunner::start) — the streaming seam —
    /// so it is exercisable with **any** runner (a
    /// [`ScriptedRunner`](crate::ScriptedRunner) in tests), unlike the
    /// real-runner-only [`Command::first_line`](crate::Command::first_line),
    /// which now delegates here.
    async fn first_line<F>(&self, command: &Command, predicate: F) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool + Send,
    {
        use tokio_stream::StreamExt;
        let process = self.start(command).await?;
        let program = command.program_name();
        let timeout = command.configured_timeout();
        let search = async move {
            let mut process = process;
            // Close an untaken `keep_stdin_open` pipe (taking it here drops it →
            // EOF) so a stdin-reading filter isn't left blocking — `first_line`
            // gives no way to write to it. A no-op for the usual case.
            let _ = process.standard_input();
            let mut lines = process.stdout_lines();
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None
        };
        match timeout {
            Some(limit) => match tokio::time::timeout(limit, search).await {
                Ok(found) => Ok(found),
                Err(_elapsed) => Err(crate::Error::Timeout {
                    program,
                    timeout: limit,
                }),
            },
            None => Ok(search.await),
        }
    }
}

/// Run `attempt` once, or — when the command carries a [`RetryPolicy`] — up to
/// `max_attempts` times, retrying while the error is classified retryable and
/// sleeping `backoff` between tries. The building block under the success-checking
/// `ProcessRunnerExt` helpers; the non-erroring `output` path never retries.
async fn retrying<T, Fut, F>(policy: Option<RetryPolicy>, mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
{
    let mut tries = 0u32;
    loop {
        tries += 1;
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                // A cancelled run is terminal regardless of the classifier: the
                // token stays cancelled forever, so every retry would just hit
                // the pre-spawn short-circuit again (mirrors the Supervisor).
                #[cfg(feature = "cancellation")]
                if matches!(err, crate::Error::Cancelled { .. }) {
                    return Err(err);
                }
                match &policy {
                    Some(p) if tries < p.max_attempts && (p.classifier)(&err) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            target: "processkit",
                            attempt = tries,
                            max_attempts = p.max_attempts,
                            backoff_ms = p.backoff.as_millis() as u64,
                            error = %err,
                            "retrying after a retryable failure"
                        );
                        tokio::time::sleep(p.backoff).await;
                    }
                    _ => return Err(err),
                }
            }
        }
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
        JobRunner::start(self, command).await?.output_string().await
    }

    async fn start(&self, command: &Command) -> Result<RunningProcess> {
        JobRunner::start(self, command).await
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
        ProcessGroup::start(self, command)
            .await?
            .output_string()
            .await
    }

    async fn start(&self, command: &Command) -> Result<RunningProcess> {
        ProcessGroup::start(self, command).await
    }
}

/// Build the OS command, spawn it into `group`, wire stdin, and wrap everything
/// in a [`RunningProcess`] (with no owned group).
pub(crate) async fn launch(group: &ProcessGroup, command: &Command) -> Result<RunningProcess> {
    // A requested privilege drop or session detach must never be silently
    // skipped: on targets without the POSIX primitives, fail before spawning.
    #[cfg(not(unix))]
    {
        if command.requested_uid().is_some() {
            return Err(crate::Error::Unsupported {
                operation: "uid".into(),
            });
        }
        if command.requested_gid().is_some() {
            return Err(crate::Error::Unsupported {
                operation: "gid".into(),
            });
        }
        if command.requested_groups() {
            return Err(crate::Error::Unsupported {
                operation: "groups".into(),
            });
        }
        if command.wants_setsid() {
            return Err(crate::Error::Unsupported {
                operation: "setsid".into(),
            });
        }
    }

    // A token already cancelled before launch: short-circuit without spawning —
    // cheaper and cleaner than spawn-then-kill. (A cancel landing between this
    // check and the first wait poll is caught by drive_to_exit's cancel branch.)
    #[cfg(feature = "cancellation")]
    if let Some(token) = command.cancel_token()
        && token.is_cancelled()
    {
        return Err(crate::Error::Cancelled {
            program: command.program_name(),
        });
    }

    // A working directory that doesn't exist (or isn't a directory) makes the OS
    // spawn fail with a bare ENOENT — indistinguishable from "program not found".
    // Check it up front so the error names the real cause. Best-effort: a TOCTOU
    // race where it vanishes after this check just falls back to the OS error.
    if let Some(cwd) = command.working_dir()
        && !cwd.is_dir()
    {
        // Distinguish "missing" from "exists but isn't a directory" so the
        // message is accurate and `is_not_found()` stays honest (a file at the
        // path is found, just not usable as a cwd).
        let (kind, what) = if cwd.exists() {
            (std::io::ErrorKind::NotADirectory, "is not a directory")
        } else {
            (std::io::ErrorKind::NotFound, "does not exist")
        };
        return Err(crate::Error::Spawn {
            program: command.program_name(),
            source: std::io::Error::new(
                kind,
                format!("working directory {what}: {}", cwd.display()),
            ),
        });
    }

    let mut tokio_cmd = command.build_tokio();
    let opts = crate::sys::SpawnOptions {
        setsid: command.wants_setsid(),
        creation_flags: command.extra_creation_flags(),
        kill_on_parent_death: command.wants_kill_on_parent_death(),
    };
    // Enrich the OS's opaque "not found" for a *bare* program name into a rich
    // Error::NotFound naming the searched directories — the bare ENOENT is
    // otherwise indistinguishable from a missing cwd or other filesystem error.
    // (Absolute/relative paths skip this: the caller already located the file,
    // and `find_in_path` only knows PATH.)
    //
    // Done *after* the spawn attempt rather than as a pre-check so the OS stays
    // the source of truth — a program the OS can launch by a path we don't model
    // is never falsely reported missing. This matters on Windows, where std also
    // searches the *application directory* (the running exe's dir), not just
    // PATH: a helper shipped beside the binary spawns fine, and a PATH-only
    // pre-check would have rejected it. The cwd was already validated above, so a
    // NotFound here is genuinely the program, not the directory.
    //
    // Skip the enrichment when the command customizes PATH (explicit PATH
    // override/removal, env_clear, inherit_env): `find_in_path` reads the
    // *process* PATH, so its "searched" list would name the wrong directories.
    // The plain Error::Spawn (NotFound kind) still reports is_not_found() == true.
    let mut child = match group.spawn_with_options(&mut tokio_cmd, &opts) {
        Ok(child) => child,
        Err(crate::Error::Spawn { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound
                && is_bare_name(command.program())
                && !command.customizes_path() =>
        {
            let (found, searched) = find_in_path(command.program());
            if found.is_some() {
                // B8: program is on PATH but the OS can't exec it directly
                // (e.g. a .cmd/.bat on Windows needs cmd.exe). Return the
                // original spawn error — NotFound is misleading here.
                return Err(crate::Error::Spawn {
                    program: command.program_name(),
                    source,
                });
            }
            return Err(crate::Error::NotFound {
                program: command.program_name(),
                searched,
            });
        }
        Err(other) => return Err(other),
    };
    let pid = child.id();
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        program = %command.program_name(),
        pid = ?pid,
        mechanism = ?group.mechanism(),
        "child spawned"
    );

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

    let mut process = RunningProcess::from_spawned(Spawned {
        program: command.program_name(),
        child,
        own_group: None,
        stdout,
        stderr,
        stdin: stdin_pipe,
        stdin_task,
        timeout: command.configured_timeout(),
        timeout_grace: command.configured_timeout_grace(),
        timeout_signal: command.timeout_signal_raw(),
        pid,
        stdout_encoding: command.out_encoding(),
        stderr_encoding: command.err_encoding(),
        stdout_handler: command.stdout_handler(),
        stderr_handler: command.stderr_handler(),
        buffer: command.output_buffer_policy(),
        ok_codes: command.ok_codes_vec(),
        stdout_piped: command.stdout_is_piped(),
        #[cfg(feature = "cancellation")]
        cancel_token: command.cancel_token(),
    });
    // Arm the spawn-time cancel watchdog with a pid-only kill. For own-group
    // runs, `attach_group` (called immediately after `launch` in `JobRunner`)
    // will re-arm with the full group+pid kill. For shared-group runs, this
    // pid-only kill is the permanent watchdog — ensuring the cancel token kills
    // the child even when no consuming verb has been called (B2 fix).
    process.arm_cancel_watchdog();
    Ok(process)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::result::Outcome;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    /// A fake runner that reports a non-zero exit for its first `fail_times`
    /// calls, then a success — and counts total calls. No real process.
    struct Flaky {
        calls: AtomicU32,
        fail_times: u32,
    }

    #[async_trait::async_trait]
    impl ProcessRunner for Flaky {
        async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let code = if n < self.fail_times { 1 } else { 0 };
            Ok(ProcessResult::new(
                command.program().to_string_lossy().into_owned(),
                "out".to_owned(),
                "transient".to_owned(),
                Outcome::Exited(code),
                None,
            ))
        }
    }

    fn flaky(fail_times: u32) -> Flaky {
        Flaky {
            calls: AtomicU32::new(0),
            fail_times,
        }
    }

    #[tokio::test]
    async fn retry_retries_until_success() {
        let runner = flaky(2);
        let cmd = Command::new("x").retry(5, Duration::from_millis(0), |e| {
            matches!(e, Error::Exit { .. })
        });
        assert_eq!(runner.run(&cmd).await.unwrap(), "out");
        assert_eq!(runner.calls.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn retry_stops_when_classifier_rejects() {
        let runner = flaky(5);
        let cmd = Command::new("x").retry(5, Duration::from_millis(0), |_| false);
        assert!(runner.run(&cmd).await.is_err());
        assert_eq!(runner.calls.load(Ordering::SeqCst), 1); // no retry
    }

    #[tokio::test]
    async fn retry_caps_at_max_attempts() {
        let runner = flaky(10);
        let cmd = Command::new("x").retry(3, Duration::from_millis(0), |_| true);
        assert!(runner.run(&cmd).await.is_err());
        assert_eq!(runner.calls.load(Ordering::SeqCst), 3); // capped
    }

    #[tokio::test]
    async fn no_policy_runs_once() {
        let runner = flaky(10);
        assert!(runner.run(&Command::new("x")).await.is_err());
        assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn probe_with_ok_codes_does_not_panic_on_a_non_binary_exit() {
        // Regression: `probe` reuses `ensure_success` to build its error for a
        // non-{0,1} exit. With `ok_codes` widening success, an accepted code like
        // 2 would make `ensure_success` return `Ok` and panic the `expect_err` —
        // probe must keep its strict 0/1 contract regardless of `ok_codes`.
        use crate::{Reply, ScriptedRunner};
        let runner = ScriptedRunner::new().on(["x"], Reply::fail(2, "boom"));
        let cmd = Command::new("tool").args(["x"]).ok_codes([0, 1, 2]);
        assert!(matches!(
            runner.probe(&cmd).await,
            Err(Error::Exit { code: 2, .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_sleeps_the_backoff_between_attempts() {
        // Two failures before success → exactly two backoff sleeps. The paused
        // clock advances only through tokio sleeps, so elapsed virtual time
        // proves the backoff is actually awaited (not silently skipped).
        let runner = flaky(2);
        let cmd = Command::new("x").retry(5, Duration::from_millis(100), |e| {
            matches!(e, Error::Exit { .. })
        });
        let start = tokio::time::Instant::now();
        assert_eq!(runner.run(&cmd).await.unwrap(), "out");
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(200),
            "two retries must sleep two backoffs, waited {waited:?}"
        );
        assert!(
            waited < Duration::from_millis(400),
            "no extra sleeps expected, waited {waited:?}"
        );
    }

    /// A runner whose every attempt fails with `Cancelled` — the token never
    /// un-cancels, so this is exactly what real retries would see.
    #[cfg(feature = "cancellation")]
    struct AlwaysCancelled(AtomicU32);

    #[cfg(feature = "cancellation")]
    #[async_trait::async_trait]
    impl ProcessRunner for AlwaysCancelled {
        async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Error::Cancelled {
                program: command.program().to_string_lossy().into_owned(),
            })
        }
    }

    #[cfg(feature = "cancellation")]
    #[tokio::test]
    async fn cancelled_is_terminal_even_when_the_classifier_accepts() {
        let runner = AlwaysCancelled(AtomicU32::new(0));
        let cmd = Command::new("x").retry(5, Duration::from_millis(0), |_| true);
        let err = runner.run(&cmd).await.expect_err("cancelled run errors");
        assert!(
            matches!(err, Error::Cancelled { .. }),
            "expected Cancelled, got {err:?}"
        );
        assert_eq!(
            runner.0.load(Ordering::SeqCst),
            1,
            "a cancelled run must not be retried"
        );
    }
}
