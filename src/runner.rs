//! The [`ProcessRunner`] seam and its real implementations.
//!
//! The seam covers both shapes of a run: [`ProcessRunner::output_string`] (a finished
//! [`ProcessResult`]) and [`ProcessRunner::start`] (a live [`RunningProcess`]
//! for streaming/probes). A [`ScriptedRunner`](crate::testing::ScriptedRunner) fakes
//! both — its `start` hands back a scripted handle that feeds canned lines
//! through the same pump machinery a real child uses.

use crate::command::{Command, find_in_path, is_bare_name};
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::ProcessResult;
use crate::running::{RunningProcess, Spawned};

/// Runs a [`Command`] — to a captured result ([`output_string`](Self::output_string) /
/// [`output_bytes`](Self::output_bytes)) or a live handle ([`start`](Self::start)).
///
/// This seam is the mock point — only [`output_string`](Self::output_string) is required
/// (`output_bytes`/`start` are defaulted): production code takes
/// `&dyn ProcessRunner`; tests pass a
/// [`ScriptedRunner`](crate::testing::ScriptedRunner) /
/// [`RecordingRunner`](crate::testing::RecordingRunner) (or, behind the `mock` feature,
/// a generated `MockRunner`) instead of spawning real processes.
///
/// The defaulting note above applies to **hand-written** runners. The
/// `mock`-feature `MockRunner` is different: `mockall::automock` replaces *every*
/// method — including the defaulted `output_bytes`/`start` — with an expectation,
/// so a `MockRunner` does **not** inherit the `Unsupported` default. Set the
/// expectations you exercise (`expect_output_string()`, and `expect_start()` /
/// `expect_output_bytes()` if a verb routes through them) or an unset call panics.
/// `ScriptedRunner` is the recommended double — it provides the defaults and the
/// streaming seam out of the box. (The `mock` feature / `MockRunner` are
/// semver-exempt — see the crate-level docs.)
#[cfg_attr(feature = "mock", mockall::automock)]
#[async_trait::async_trait]
pub trait ProcessRunner: Send + Sync {
    /// Run `command` to completion, capturing stdout/stderr and the exit code.
    /// A non-zero exit is reported in the result, not raised.
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>>;

    /// Run `command` to completion, capturing stdout as **raw bytes** (`output_string`
    /// captures it as lossy-UTF-8 text); stderr is still text. For binary tools
    /// — `git cat-file`, `tar -c`, an image transcoder — whose stdout is not
    /// UTF-8.
    ///
    /// Part of the seam (not just `Command`), so byte-producing tools are
    /// testable through a [`ScriptedRunner`](crate::testing::ScriptedRunner) /
    /// `&ProcessGroup` / [`JobRunner`] like text ones. Defaulted in terms of
    /// [`start`](Self::start) — so a runner that overrides `start` gets byte
    /// capture for free, and an `output_string`-only runner (one that does **not**
    /// override `start`) surfaces [`Error::Unsupported`](crate::Error::Unsupported),
    /// matching `start`. A text fixture (a `record`-feature cassette stores
    /// lossy-UTF-8) cannot reproduce exact bytes; capture bytes from a real or
    /// scripted runner.
    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
        self.start(command).await?.output_bytes().await
    }

    /// Start `command` and return a live [`RunningProcess`] for streaming,
    /// readiness probes, or incremental consumption.
    ///
    /// Defaulted to [`Error::Unsupported`](crate::Error::Unsupported) so an
    /// `output_string`-only runner (a hand-rolled double, a cassette runner) keeps
    /// compiling; the real runners ([`JobRunner`], `&ProcessGroup`) and
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner) override it.
    ///
    /// This is deliberately a **runtime** capability (a default that errors)
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
/// [`RecordingRunner`](crate::testing::RecordingRunner) (or any `&R`) can be injected
/// where a `ProcessRunner` is expected.
#[async_trait::async_trait]
impl<R: ProcessRunner + ?Sized> ProcessRunner for &R {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        (**self).output_string(command).await
    }

    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
        // Forward (don't fall through to the default) so a runner that overrides
        // `output_bytes` is honored through a `&R`.
        (**self).output_bytes(command).await
    }

    async fn start(&self, command: &Command) -> Result<RunningProcess> {
        (**self).start(command).await
    }
}

/// Convenience methods available on every [`ProcessRunner`] (including
/// `&dyn ProcessRunner`), layered over [`output_string`](ProcessRunner::output_string).
#[async_trait::async_trait]
pub trait ProcessRunnerExt: ProcessRunner {
    /// Run, require an **accepted** exit, and return trimmed stdout. Accepted is
    /// `0` by default, widened by [`Command::ok_codes`](crate::Command::ok_codes);
    /// any other code is [`Error::Exit`](crate::Error::Exit).
    async fn run(&self, command: &Command) -> Result<String> {
        let result = self.checked(command).await?;
        // `run` presents stdout as if complete, so fail loud on a bounded-buffer
        // truncation rather than hand back a silently clipped tail.
        let policy = command.output_buffer_policy();
        result.reject_if_truncated(policy.max_lines, policy.max_bytes)?;
        Ok(result.into_stdout().trim_end().to_owned())
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
        retrying(command, || async {
            self.output_string(command).await?.require_code()
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
        retrying(command, || async {
            let result = self.output_string(command).await?;
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

    /// Run, require an **accepted** exit (`0` by default, widened by
    /// [`Command::ok_codes`](crate::Command::ok_codes)), and return the full
    /// captured result (untrimmed stdout). The building block for the
    /// `parse`/`try_parse` helpers — use it when you need the whole
    /// `ProcessResult` after success-checking, rather than just trimmed stdout
    /// (`run`) or the raw result (`output_string`).
    ///
    /// Unlike [`run`](Self::run) (and the
    /// [`CliClient::parse`](crate::CliClient::parse)/[`try_parse`](crate::CliClient::try_parse)
    /// verbs built over it), `checked` does **not** fail loud on a bounded-buffer
    /// truncation: it
    /// hands back the (possibly truncated) `ProcessResult` so the caller can decide
    /// — inspect [`truncated()`](crate::ProcessResult::truncated) before relying on
    /// the stdout. This is deliberate: `checked` is the lenient building block;
    /// the trimming / parsing verbs add the loud-on-truncation guard because they
    /// present stdout as if complete.
    async fn checked(&self, command: &Command) -> Result<ProcessResult<String>> {
        retrying(command, || async {
            self.output_string(command).await?.ensure_success()
        })
        .await
    }

    /// Run (requiring an **accepted** exit) and feed the captured stdout to an
    /// **infallible** `parse` closure — the shape of struct-returning CLI
    /// commands (git/jj `--format` output). Built on [`checked`](Self::checked),
    /// but unlike it, fails loud on a bounded-buffer truncation so the
    /// parser never silently sees a clipped tail; returns the parsed value.
    ///
    /// Because it is generic over the parser `F`, `parse` — like
    /// [`first_line`](Self::first_line) — is **not object-safe** and so is
    /// unavailable on a `&dyn ProcessRunner`: call it on a concrete runner
    /// ([`JobRunner`], `&ProcessGroup`, a
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner)), or via the
    /// [`Command::parse`](crate::Command::parse) /
    /// [`CliClient::parse`](crate::CliClient::parse) wrappers.
    async fn parse<T, F>(&self, command: &Command, parse: F) -> Result<T>
    where
        T: Send,
        F: FnOnce(&str) -> T + Send,
    {
        let out = self.checked(command).await?;
        // A parser must not silently see a truncated tail.
        let policy = command.output_buffer_policy();
        out.reject_if_truncated(policy.max_lines, policy.max_bytes)?;
        Ok(parse(out.stdout()))
    }

    /// Run (requiring an **accepted** exit) and feed the captured stdout to a
    /// *fallible* `parse` closure — the shape of JSON deserialization, where a
    /// parse failure becomes [`Error::Parse`](crate::Error::Parse) (or whatever
    /// error the closure returns). Like [`parse`](Self::parse) it is built on
    /// [`checked`](Self::checked), fails loud on truncation, and — being
    /// generic over `F` — is unavailable on a `&dyn ProcessRunner`; use a
    /// concrete runner or the [`Command::try_parse`](crate::Command::try_parse) /
    /// [`CliClient::try_parse`](crate::CliClient::try_parse) wrappers.
    async fn try_parse<T, F>(&self, command: &Command, parse: F) -> Result<T>
    where
        T: Send,
        F: FnOnce(&str) -> Result<T> + Send,
    {
        let out = self.checked(command).await?;
        // A parser must not silently see a truncated tail.
        let policy = command.output_buffer_policy();
        out.reject_if_truncated(policy.max_lines, policy.max_bytes)?;
        parse(out.stdout())
    }

    /// Stream `command`'s stdout and return the first line matching `predicate`
    /// (`None` if the stream ends first), bounded by the command's
    /// [`timeout`](crate::Command::timeout) (a `Some` deadline surfaces as
    /// [`Error::Timeout`](crate::Error::Timeout) and tears the tree down).
    ///
    /// Routes through [`start`](ProcessRunner::start) — the streaming seam —
    /// so it is exercisable with **any** runner (a
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner) in tests), unlike the
    /// real-runner-only [`Command::first_line`](crate::Command::first_line),
    /// which now delegates here.
    ///
    /// Because it is generic over the predicate `F`, `first_line` is **not
    /// object-safe** and so is unavailable on a `&dyn ProcessRunner`: call it
    /// on a concrete runner ([`JobRunner`], `&ProcessGroup`, a
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner)), or via the
    /// [`Command::first_line`] / [`CliClient::first_line`](crate::CliClient::first_line)
    /// wrappers. All other [`ProcessRunnerExt`] verbs work through `&dyn`.
    async fn first_line<F>(&self, command: &Command, predicate: F) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool + Send,
    {
        use tokio_stream::StreamExt;
        let mut process = self.start(command).await?;
        let program = command.program_name();
        let timeout = command.configured_timeout();
        let cancel = command.cancel_token();
        // Drop any open stdin pipe so a stdin-reading child isn't left blocking.
        let _ = process.take_stdin();
        let mut lines = process.stdout_lines()?;
        let search = async move {
            let _process = process; // keep alive; drop on timeout tears the tree down
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None
        };
        let found = match timeout {
            Some(limit) => match tokio::time::timeout(limit, search).await {
                Ok(found) => found,
                Err(_elapsed) => {
                    return Err(crate::Error::Timeout {
                        program,
                        timeout: limit,
                        stdout: String::new(), // streaming probe buffers nothing
                        stderr: String::new(),
                    });
                }
            },
            None => search.await,
        };
        // A cancelled run's stream just ends; surface the cancellation so a
        // readiness probe doesn't misread `None` as "predicate never matched".
        if found.is_none() && cancel.is_some_and(|t| t.is_cancelled()) {
            return Err(crate::Error::Cancelled { program });
        }
        Ok(found)
    }
}

/// Run `attempt` once, or up to the policy's `max_attempts` when the command
/// carries a retry config, sleeping the policy's per-retry delay (capped
/// exponential backoff, optionally jittered) between retries while the error is
/// classified retryable.
async fn retrying<T, Fut, F>(command: &Command, mut attempt: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
{
    let config = command.retry_config();
    // A one-shot streaming stdin is consumed by the first attempt; run once.
    let one_shot_stdin = !command.keeps_stdin_open()
        && command
            .stdin_source()
            .is_some_and(crate::Stdin::is_one_shot);
    let mut tries = 0u32;
    loop {
        tries += 1;
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                // Cancelled is terminal — token stays cancelled, every retry
                // would hit the pre-spawn short-circuit again.
                if matches!(err, crate::Error::Cancelled { .. }) {
                    return Err(err);
                }
                if one_shot_stdin {
                    return Err(err);
                }
                match &config {
                    Some(c) if tries < c.policy.max_attempts() && (c.classifier)(&err) => {
                        // `tries` is the attempts-so-far count (1-based); the delay
                        // before the next attempt uses the 0-based retry index.
                        let delay = c.policy.delay_for(tries - 1);
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            target: "processkit",
                            attempt = tries,
                            max_attempts = c.policy.max_attempts(),
                            backoff_ms = delay.as_millis() as u64,
                            error = %err,
                            "retrying after a retryable failure"
                        );
                        tokio::time::sleep(delay).await;
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
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
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
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
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

    // Already cancelled: short-circuit before spawning.
    if let Some(token) = command.cancel_token()
        && token.is_cancelled()
    {
        return Err(crate::Error::Cancelled {
            program: command.program_name(),
        });
    }

    // A missing/non-directory cwd produces a bare ENOENT, indistinguishable from
    // "program not found"; check up front so the error names the real cause.
    if let Some(cwd) = command.working_dir()
        && !cwd.is_dir()
    {
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

    // Take stdin atomically so a concurrent second run of a one-shot source sees
    // it consumed and fails loud. Taken before the spawn so a failed spawn never
    // leaves a child to feed.
    let taken_stdin = if command.keeps_stdin_open() {
        None
    } else {
        match command.stdin_source() {
            Some(source) => match source.take_for_run().await {
                Ok(taken) => Some(taken),
                Err(crate::stdin::OneShotConsumed) => {
                    return Err(crate::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "`{}`: its one-shot streaming stdin (from_reader/from_lines) was \
                             already consumed by a previous run — such a source feeds a single \
                             run and cannot be retried or re-run; use Stdin::from_bytes/from_string \
                             (re-runnable), or rebuild the command with a fresh source",
                            command.program_name()
                        ),
                    )));
                }
            },
            None => None,
        }
    };

    let mut tokio_cmd = command.build_tokio();
    let opts = crate::sys::SpawnOptions {
        setsid: command.wants_setsid(),
        creation_flags: command.extra_creation_flags(),
        kill_on_parent_death: command.wants_kill_on_parent_death(),
    };
    // Translate the OS's opaque NotFound into `Error::NotFound` after the spawn
    // attempt, so the OS stays the source of truth. The cwd was validated above,
    // so NotFound here is genuinely the program. A bare name reports searched dirs;
    // a path-form program or customized PATH gets `searched: None`.
    let mut child = match group.spawn_with_options(&mut tokio_cmd, &opts) {
        Ok(child) => child,
        Err(crate::Error::Spawn { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            if is_bare_name(command.program()) && !command.customizes_path() {
                let (found, searched) = find_in_path(command.program());
                if found.is_some() {
                    // On PATH but not directly executable (e.g. .cmd/.bat on Windows).
                    return Err(crate::Error::Spawn {
                        program: command.program_name(),
                        source,
                    });
                }
                return Err(crate::Error::NotFound {
                    program: command.program_name(),
                    searched: Some(searched),
                });
            }
            return Err(crate::Error::NotFound {
                program: command.program_name(),
                searched: None,
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
        (child.stdin.take(), None)
    } else {
        match taken_stdin {
            // Background write so a large payload can't deadlock against the child's
            // stdout; dropping the sink sends EOF.
            Some(payload) if !payload.is_empty() => {
                let task = child.stdin.take().map(|mut sink| {
                    tokio::spawn(async move {
                        let result = payload.write_to(&mut sink).await;
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
        stdout_tee: command.stdout_tee_sink(),
        stderr_tee: command.stderr_tee_sink(),
        buffer: command.output_buffer_policy(),
        ok_codes: command.ok_codes_vec(),
        stdout_piped: command.stdout_is_piped(),
        cancel_token: command.cancel_token(),
    });
    // Pid-only watchdog; own-group runs re-arm with full group+pid via `attach_group`.
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
        async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
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
    async fn retry_with_rich_policy_goes_through_the_retry_loop() {
        use crate::RetryPolicy;
        let runner = flaky(10);
        // A per-command rich `RetryPolicy`: max_retries(2) → 3 total attempts.
        let cmd = Command::new("x").retry_with(
            RetryPolicy::new()
                .max_retries(2)
                .initial_backoff(Duration::ZERO),
            |_| true,
        );
        assert!(runner.run(&cmd).await.is_err());
        assert_eq!(runner.calls.load(Ordering::SeqCst), 3); // 1 attempt + 2 retries
    }

    #[tokio::test]
    async fn no_policy_runs_once() {
        let runner = flaky(10);
        assert!(runner.run(&Command::new("x")).await.is_err());
        assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn one_shot_stdin_command_is_not_retried() {
        let runner = flaky(10);
        let cmd = Command::new("x")
            .stdin(crate::Stdin::from_reader(&b"once"[..]))
            .retry(5, Duration::from_millis(0), |_| true);
        assert!(runner.run(&cmd).await.is_err());
        assert_eq!(
            runner.calls.load(Ordering::SeqCst),
            1,
            "a one-shot stdin command is attempted once, not retried"
        );

        let runner = flaky(10);
        let cmd = Command::new("x")
            .stdin(crate::Stdin::from_bytes(b"again".to_vec()))
            .retry(3, Duration::from_millis(0), |_| true);
        assert!(runner.run(&cmd).await.is_err());
        assert_eq!(
            runner.calls.load(Ordering::SeqCst),
            3,
            "a re-runnable stdin source retries up to the cap"
        );
    }

    #[tokio::test]
    async fn probe_with_ok_codes_does_not_panic_on_a_non_binary_exit() {
        use crate::testing::{Reply, ScriptedRunner};
        let runner = ScriptedRunner::new().on(["tool", "x"], Reply::fail(2, "boom"));
        let cmd = Command::new("tool").args(["x"]).ok_codes([0, 1, 2]);
        assert!(matches!(
            runner.probe(&cmd).await,
            Err(Error::Exit { code: 2, .. })
        ));
    }

    #[tokio::test]
    async fn parse_feeds_checked_stdout_to_the_parser() {
        use crate::testing::{Reply, ScriptedRunner};
        let runner = ScriptedRunner::new().on(["wc", "-l"], Reply::ok("  42\n"));
        let cmd = Command::new("wc").arg("-l");
        let n: u32 = runner
            .parse(&cmd, |s| s.trim().parse().unwrap_or(0))
            .await
            .expect("parse");
        assert_eq!(n, 42);
    }

    #[tokio::test]
    async fn try_parse_surfaces_a_parser_error_and_a_nonzero_exit() {
        use crate::testing::{Reply, ScriptedRunner};
        let ok_runner = ScriptedRunner::new().on(["tool"], Reply::ok("nope"));
        let err = ok_runner
            .try_parse::<u32, _>(&Command::new("tool"), |s| {
                s.trim().parse::<u32>().map_err(|e| Error::Parse {
                    program: "tool".into(),
                    message: e.to_string(),
                })
            })
            .await
            .expect_err("a parser failure is an error");
        assert!(matches!(err, Error::Parse { .. }), "got {err:?}");

        let fail_runner = ScriptedRunner::new().on(["tool"], Reply::fail(3, "boom"));
        let err = fail_runner
            .try_parse::<u32, _>(&Command::new("tool"), |_| {
                panic!("parser must not run on a failed exit")
            })
            .await
            .expect_err("a non-zero exit is an error");
        assert!(matches!(err, Error::Exit { code: 3, .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn parse_fails_loud_on_a_truncated_capture() {
        struct TruncatedRunner;
        #[async_trait::async_trait]
        impl ProcessRunner for TruncatedRunner {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    "clipped".to_owned(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                )
                .with_truncated(true)
                .with_overflow_totals(100, 9999))
            }
        }
        let err = TruncatedRunner
            .parse(&Command::new("tool"), |_| {
                panic!("parser must not run on a truncated capture")
            })
            .await
            .expect_err("a truncated capture must fail loud, not parse a clipped tail");
        assert!(matches!(err, Error::OutputTooLarge { .. }), "got {err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_sleeps_the_backoff_between_attempts() {
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

    struct AlwaysCancelled(AtomicU32);

    #[async_trait::async_trait]
    impl ProcessRunner for AlwaysCancelled {
        async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Error::Cancelled {
                program: command.program().to_string_lossy().into_owned(),
            })
        }
    }

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
