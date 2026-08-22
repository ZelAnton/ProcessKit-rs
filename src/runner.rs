//! The [`ProcessRunner`] seam and its real implementations.
//!
//! The seam covers both shapes of a run: [`ProcessRunner::output_string`] (a finished
//! [`ProcessResult`]) and [`ProcessRunner::start`] (a live [`RunningProcess`]
//! for streaming/probes). A [`ScriptedRunner`](crate::testing::ScriptedRunner) fakes
//! both — its `start` hands back a scripted handle that feeds canned lines
//! through the same pump machinery a real child uses.

use crate::command::{Command, ProgramResolution, is_bare_name, resolve_program};
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::ProcessResult;
use crate::running::{OutputReader, RunningProcess, Spawned};

/// Fixed teardown headroom past a streamed run's own deadline/grace before
/// [`first_line`](ProcessRunnerExt::first_line)'s drain backstop gives up. It sits
/// well beyond any legitimate kill, so a backstop built on it only ever trips in
/// the shared-group forking gap — a grandchild that inherited stdout and holds the
/// pipe open past the watchdog's pid-only kill, so the stream never closes and the
/// drain would otherwise hang. It never preempts a slow-but-legitimate
/// single-process teardown. Shared by both drain backstops (cancel and deadline)
/// so they can't drift.
const TEARDOWN_BACKSTOP_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);

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
    /// override `start`) surfaces [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported),
    /// matching `start`. A text fixture (a `record`-feature cassette stores
    /// lossy-UTF-8) cannot reproduce exact bytes; capture bytes from a real or
    /// scripted runner.
    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
        self.start(command).await?.output_bytes().await
    }

    /// Start `command` and return a live [`RunningProcess`] for streaming,
    /// readiness probes, or incremental consumption.
    ///
    /// Defaulted to [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported) so an
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
        Err(crate::ErrorReason::Unsupported {
            operation: "start".into(),
        }
        .into())
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

/// A boxed runner is a runner. Generic over `R: ?Sized` so it covers both a
/// type-erased `Box<dyn ProcessRunner>` — a runner chosen at **runtime** (the real
/// [`JobRunner`] vs a `record`-feature cassette, picked from config) and stored in
/// `CliClient`/`Supervisor` state — and a boxed concrete `Box<JobRunner>`.
/// Forwards every method (including `output_bytes`/`start`), so a boxed runner
/// that overrides them is honored. (`dyn ProcessRunner` is `Send + Sync` via the
/// trait's supertraits, so the box is too — store it as `Box<dyn ProcessRunner>`,
/// no `+ Send + Sync` marker needed.)
#[async_trait::async_trait]
impl<R: ProcessRunner + ?Sized> ProcessRunner for Box<R> {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        (**self).output_string(command).await
    }

    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
        (**self).output_bytes(command).await
    }

    async fn start(&self, command: &Command) -> Result<RunningProcess> {
        (**self).start(command).await
    }
}

/// A shared runner is a runner — the `Arc` twin of the `Box` impl above, for when
/// one runner must be **shared** (cloned into several
/// [`Supervisor`](crate::Supervisor)s or spawned tasks) rather than owned once.
/// Generic over `R: ?Sized`, so both `Arc<dyn ProcessRunner>` (runtime-selected)
/// and `Arc<JobRunner>` (a shared concrete runner) qualify.
#[async_trait::async_trait]
impl<R: ProcessRunner + ?Sized> ProcessRunner for std::sync::Arc<R> {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        (**self).output_string(command).await
    }

    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
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
    /// any other code is [`ErrorReason::Exit`](crate::ErrorReason::Exit).
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
    /// an error — a timeout as [`ErrorReason::Timeout`](crate::ErrorReason::Timeout), a
    /// signal-kill as [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) — rather than a
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
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit), timeout as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout), signal-kill as
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled)). For
    /// commands whose exit code *is* the answer — `git diff --quiet`, `grep -q`, …
    async fn probe(&self, command: &Command) -> Result<bool> {
        retrying(command, || async {
            let result = self.output_string(command).await?;
            result.probe_bool()
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
    /// [`first_line`](Self::first_line) — makes the ext trait **not object-safe**,
    /// so it cannot be dispatched through a `dyn ProcessRunnerExt` object; it *is*
    /// callable on a `&dyn ProcessRunner` (via the blanket ext impl). Reach for it
    /// on a concrete runner ([`JobRunner`], `&ProcessGroup`, a
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
    /// parse failure becomes [`ErrorReason::Parse`](crate::ErrorReason::Parse) (or whatever
    /// error the closure returns). Like [`parse`](Self::parse) it is built on
    /// [`checked`](Self::checked), fails loud on truncation, and — being generic
    /// over `F` — cannot be dispatched through a `dyn ProcessRunnerExt` **object**
    /// (the trait isn't object-safe), though it *is* callable on a
    /// `&dyn ProcessRunner`. The [`Command::try_parse`](crate::Command::try_parse) /
    /// [`CliClient::try_parse`](crate::CliClient::try_parse) wrappers are the
    /// ergonomic path.
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

    /// Run `command`, require an accepted exit, and deserialize its complete
    /// stdout as JSON.
    ///
    /// This is the typed counterpart to [`try_parse`](Self::try_parse): it uses
    /// the same retry and success-checking contract and rejects a truncated
    /// capture before deserialization. A malformed document becomes
    /// [`ErrorReason::Parse`](crate::ErrorReason::Parse) whose message identifies
    /// the program and decoded-output line/column plus zero-based byte offset
    /// while retaining at most a 160-byte, control-escaped raw fragment.
    ///
    /// # Errors
    ///
    /// Everything [`try_parse`](Self::try_parse) can return, with
    /// [`ErrorReason::Parse`](crate::ErrorReason::Parse) added for malformed JSON
    /// or a value that does not match `T`. Available with the `json` feature.
    #[cfg(feature = "json")]
    async fn output_json<T>(&self, command: &Command) -> Result<T>
    where
        T: serde::de::DeserializeOwned + Send,
    {
        let program = command.program_name();
        self.try_parse(command, move |stdout| crate::json::decode(&program, stdout))
            .await
    }

    /// Stream `command`'s stdout and return the first line matching `predicate`
    /// (`None` if the stream ends first), bounded by the command's
    /// [`timeout`](crate::Command::timeout): a `Some` deadline surfaces as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) and tears the process down. On an
    /// **own-group** runner ([`JobRunner`], the default) that teardown covers the
    /// whole tree; on a **shared** [`ProcessGroup`] it reaches
    /// the run's direct child by pid — a forking child's grandchildren (and, on the
    /// Linux cgroup mechanism, a direct child that catches the graceful signal and
    /// closes stdout but keeps running) may outlive the probe until the group is
    /// dropped. Bound such a run with a whole-chain owner instead.
    ///
    /// Routes through [`start`](ProcessRunner::start) — the streaming seam —
    /// so it is exercisable with **any** runner (a
    /// [`ScriptedRunner`](crate::testing::ScriptedRunner) in tests), unlike the
    /// real-runner-only [`Command::first_line`](crate::Command::first_line),
    /// which now delegates here.
    ///
    /// Because it is generic over the predicate `F`, `first_line` makes the ext
    /// trait **not object-safe**, so it cannot be dispatched through a `dyn
    /// ProcessRunnerExt` object; it *is* callable on a `&dyn ProcessRunner` (via
    /// the blanket ext impl), like every other [`ProcessRunnerExt`] verb. The
    /// [`Command::first_line`] / [`CliClient::first_line`](crate::CliClient::first_line)
    /// wrappers are the ergonomic path.
    async fn first_line<F>(&self, command: &Command, predicate: F) -> Result<Option<String>>
    where
        F: Fn(&str) -> bool + Send,
    {
        use tokio_stream::StreamExt;
        let mut process = self.start(command).await?;
        let program = command.program_name();
        let timeout = command.configured_timeout();
        let inactivity_timeout = command.configured_inactivity_timeout();
        let grace = command
            .configured_timeout_grace()
            .unwrap_or(std::time::Duration::ZERO);
        // The teardown a *cancellation* can now legitimately take: with
        // `Command::cancel_grace` the cancel watchdog drives a soft-signal → grace →
        // hard-kill ladder instead of killing at once, so the cancel drain below must
        // outlast the LONGER of the two graces (either teardown may be the one in
        // flight when the token fires). `ZERO` without the knobs keeps today's bound
        // byte-identical.
        let teardown_grace = grace.max(
            command
                .configured_cancel_grace()
                .unwrap_or(std::time::Duration::ZERO),
        );
        let cancel = command.cancel_token();
        // A race-free record of whether the deadline watchdog fired: it stores
        // `TS_TIMED_OUT` *before* it kills, so reading it once the stream has
        // closed distinguishes a deadline kill from a natural end.
        let arbiter = process.deadline_arbiter();
        let output_activity = process.output_activity();
        // Drop any open stdin pipe so a stdin-reading child isn't left blocking.
        let _ = process.take_stdin();
        // `stdout_lines` arms the deadline watchdog, which enforces the timeout
        // and tears the tree down — including on a shared-group handle, where it
        // reaches the direct child by pid. `first_line` therefore runs no
        // `tokio::time::timeout` of its own: dropping the search on a raised
        // deadline would abort that watchdog before it fired and strand the child.
        let mut lines = process.stdout_lines()?;
        let search = async move {
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None
        };
        // Race the search against cancellation. A match or a natural/deadline
        // end-of-stream commits via the biased `&mut search` arm, so a token that
        // fires an instant after a natural end can't reclassify `Ok(None)` as
        // `Cancelled`. On a firing token we DRAIN the search to its end before
        // reporting `Cancelled`, rather than dropping it early: keeping the line
        // stream alive lets the cancel watchdog close the pipes and retain any
        // terminal teardown failure for `process.finish()` below.
        let raced = async move {
            tokio::pin!(search);
            match cancel {
                Some(token) => tokio::select! {
                    biased;
                    found = &mut search => Ok(found),
                    () = token.cancelled() => {
                        // The cancel watchdog fired its kill the instant the token
                        // did; drain the search to EOF before reporting `Cancelled`
                        // rather than dropping it early. But BOUND the drain: on a
                        // shared-group handle the watchdog's pid-only kill can't
                        // close a stdout that the direct child's grandchild
                        // inherited and holds open, so the pipe never closes and
                        // the drain would hang forever. The teardown grace + a fixed
                        // margin sits past any legitimate teardown, so this backstop only
                        // trips in that forking gap — where the kill has already
                        // been attempted, making it safe to stop draining. It
                        // applies in BOTH timeout branches: the `None` branch has
                        // no outer whole-race backstop to lean on, so this is its
                        // only bound. Either way the honest reason is cancellation,
                        // so it stays `Cancelled`, never a false `Timeout`.
                        //
                        // `teardown_grace` (not the deadline's `grace`) is what this
                        // must clear: a `cancel_grace` run's watchdog only kills after
                        // its own grace window, and bounding the drain shorter would
                        // stop draining the line stream while the (own-group,
                        // inline) graceful teardown is still mid-grace.
                        let drain_backstop =
                            teardown_grace.saturating_add(TEARDOWN_BACKSTOP_MARGIN);
                        let _ = tokio::time::timeout(drain_backstop, &mut search).await;
                        Err(())
                    }
                },
                None => Ok(search.await),
            }
        };
        // A firing cancel is already bounded inside `raced`. Backstop either
        // watchdog's post-kill drain: a shared-group grandchild can inherit stdout
        // and keep it open after the direct child dies. The inactivity backstop
        // follows the same resettable activity clock as the real watchdog, so
        // healthy periodic output never gets bounded by time-since-spawn.
        // Both post-kill drain backstops are measured with `teardown_grace` — the
        // longer of the deadline and cancellation graces — so they still sit past
        // ANY legitimate teardown once `cancel_grace` can stretch one. Without that
        // knob it equals the deadline `grace`, leaving these bounds unchanged; with
        // it, a narrower bound would let this `Timeout` fallback preempt a
        // still-legitimate cancellation drain and report the wrong disposition.
        let absolute_backstop = async {
            match timeout {
                Some(limit) => {
                    tokio::time::sleep(
                        limit
                            .saturating_add(teardown_grace)
                            .saturating_add(TEARDOWN_BACKSTOP_MARGIN),
                    )
                    .await
                }
                None => std::future::pending::<()>().await,
            }
        };
        let inactivity_backstop = async {
            match inactivity_timeout {
                Some(limit) => {
                    output_activity.wait_for_inactivity(limit).await;
                    tokio::time::sleep(teardown_grace.saturating_add(TEARDOWN_BACKSTOP_MARGIN))
                        .await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        enum Completion {
            Search(std::result::Result<Option<String>, ()>),
            AbsoluteTimeout,
            InactivityTimeout,
        }
        let completion = tokio::select! {
            biased;
            result = raced => Completion::Search(result),
            () = absolute_backstop => Completion::AbsoluteTimeout,
            () = inactivity_backstop => Completion::InactivityTimeout,
        };

        match completion {
            Completion::Search(Err(())) => {
                // Cancellation outranks stdin/pump failures, but an unconfirmed
                // terminal teardown outranks cancellation. `finish` reclaims the
                // watchdog and performs the bounded drain before exposing that
                // distinction; keeping `process` outside `search` is what makes
                // this confirmation possible.
                match process.finish().await {
                    Err(error) if error.is_teardown() => return Err(error),
                    Err(error) if error.is_cancelled() => return Err(error),
                    Ok(_) | Err(_) => {
                        return Err(crate::ErrorReason::Cancelled { program }.into());
                    }
                }
            }
            timeout_completion @ (Completion::AbsoluteTimeout | Completion::InactivityTimeout) => {
                let inactivity = matches!(timeout_completion, Completion::InactivityTimeout);
                // Timeout/inactivity outranks stdin/pump failures, but not a
                // potentially-live child/tree. A persistent kill/escalation/reap
                // failure is retained by `finish` as Teardown; otherwise preserve
                // first_line's established Timeout classification.
                if let Err(error) = process.finish().await
                    && error.is_teardown()
                {
                    return Err(error);
                }
                return Err(crate::ErrorReason::Timeout {
                    program,
                    timeout: if inactivity {
                        inactivity_timeout.unwrap_or_default()
                    } else {
                        timeout.unwrap_or_default()
                    },
                    inactivity,
                    stdout: String::new(),
                    stderr: String::new(),
                    stdout_bytes: None,
                }
                .into());
            }
            Completion::Search(Ok(found)) => {
                // Continue with the arbiter classification below.
                if found.is_some() {
                    return Ok(found);
                }
            }
        }
        // Distinguish a deadline kill (arbiter `TS_TIMED_OUT`, set before the kill,
        // so the teardown was already attempted when the stream closed) from a
        // natural end. `finish` below decides whether that attempt was confirmed.
        // In the narrow tie where a cancel token also fired in the same poll that
        // saw the deadline-closed stream, the biased search-first arm already
        // committed `Ok(None)` here, so this surfaces as `Timeout` rather than
        // `Cancelled` — the arbiter is a committed record of the deadline, whereas
        // re-reading the token would reintroduce the natural-end-vs-late-token race
        // the drain fixed. Both still error; a retry re-hits the cancel short-circuit.
        let (timeout, inactivity) = match arbiter.load(std::sync::atomic::Ordering::Acquire) {
            crate::running::TS_TIMED_OUT => (timeout.unwrap_or_default(), false),
            crate::running::TS_INACTIVITY_TIMED_OUT => {
                (inactivity_timeout.unwrap_or_default(), true)
            }
            _ => return Ok(None),
        };
        if let Err(error) = process.finish().await
            && error.is_teardown()
        {
            return Err(error);
        }
        Err(crate::ErrorReason::Timeout {
            program,
            timeout,
            inactivity,
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        }
        .into())
    }
}

/// Whether `err` is a launch failure **guaranteed to have occurred before any
/// child process was spawned** — the program was never located
/// ([`ErrorReason::NotFound`](crate::ErrorReason::NotFound)), the spawn attempt itself
/// failed ([`ErrorReason::Spawn`](crate::ErrorReason::Spawn) — including a transient
/// `ETXTBSY` that [`Error::is_transient`](crate::Error::is_transient) accepts),
/// or a required platform primitive was refused up front
/// ([`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported)). In each of these no live
/// child ever existed.
///
/// This is what lets [`retrying`] safely re-run a command carrying a **one-shot**
/// stdin source: [`launch`] reserves that payload transactionally and commits it
/// only once a child exists (see [`take_stdin_for_run`] and
/// [`StdinReservation`](crate::stdin::StdinReservation)), so a pre-child failure
/// rolls the reservation back and leaves the payload intact for the retried
/// attempt. Every other error may have reached a live child that already
/// consumed the source, so it is **not** treated as pre-child — including the
/// ambiguous [`ErrorReason::Io`](crate::ErrorReason::Io), which arises both before a child
/// (a process group that could not be created, a source already consumed by an
/// *earlier* run) and after one (driving or tearing down a live child).
///
/// A conservative `matches!`, not an exhaustive match: the safe default for the
/// retry gate is "not pre-child" (do not retry), so a future error variant is
/// correctly refused a one-shot retry until it is deliberately added here.
fn is_pre_child_launch_failure(err: &crate::Error) -> bool {
    matches!(
        err.reason(),
        crate::ErrorReason::NotFound { .. }
            | crate::ErrorReason::Spawn { .. }
            | crate::ErrorReason::Unsupported { .. }
    )
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
    // A one-shot streaming stdin feeds a single run, so a retry may re-feed it
    // only when the failed attempt is guaranteed not to have consumed it — see
    // the gate on `is_pre_child_launch_failure` below.
    let one_shot_stdin = command
        .effective_stdin_source()
        .is_some_and(crate::Stdin::is_one_shot);
    let mut tries = 0u32;
    loop {
        tries += 1;
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                // Cancelled is terminal because the token stays cancelled.
                // Teardown is terminal because the failed attempt may still be
                // live; launching another copy would compound the unconfirmed
                // process tree even when a broad caller classifier says retry.
                if err.is_cancelled() || err.is_teardown() {
                    return Err(err);
                }
                // A one-shot streaming stdin (from_reader/from_lines) feeds a
                // single run. Only a failure guaranteed to precede a live child
                // (NotFound/Spawn/Unsupported) rolled the transactional stdin
                // reservation back (see `launch`), leaving the payload intact for
                // a safe retry. Any other error may have spawned a child that
                // consumed the source (Exit/Timeout/Signalled/Stdin/
                // OutputTooLarge, or the ambiguous Io), so retrying could only
                // replay empty stdin or spuriously re-classify the re-consume —
                // return the first error as-is.
                if one_shot_stdin && !is_pre_child_launch_failure(&err) {
                    return Err(err);
                }
                match &config {
                    Some(c) if tries < c.policy.max_attempts() && (c.classifier)(&err) => {
                        // `tries` is the attempts-so-far count (1-based); the delay
                        // before the next attempt uses the 0-based retry index.
                        let delay = c.policy.delay_for(tries - 1);
                        #[cfg(feature = "metrics")]
                        crate::metrics::record_retry(&command.program_name());
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            target: "processkit",
                            attempt = tries,
                            max_attempts = c.policy.max_attempts(),
                            backoff_ms = delay.as_millis() as u64,
                            error = %err,
                            "retrying after a retryable failure"
                        );
                        // Race the backoff against the command's cancel token: a
                        // cancellation mid-backoff resolves promptly with
                        // `Cancelled` instead of waiting out a (possibly 30 s)
                        // delay, honoring `cancel_on`'s "bound the total with
                        // cancellation" advice. For the built-in runners this only
                        // changes *when*, not *what* — the next attempt would hit
                        // `launch`'s pre-spawn short-circuit and return `Cancelled`
                        // anyway. (A custom runner that ignores the token would
                        // otherwise have retried to exhaustion; surfacing the
                        // cancellation here is the more faithful behavior.)
                        match command.cancel_token() {
                            Some(token) => {
                                tokio::select! {
                                    biased;
                                    () = token.cancelled() => {
                                        return Err(crate::ErrorReason::Cancelled {
                                            program: command.program_name(),
                                        }
                                        .into());
                                    }
                                    () = tokio::time::sleep(delay) => {}
                                }
                            }
                            None => tokio::time::sleep(delay).await,
                        }
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
    ///
    /// # Errors
    ///
    /// The full launch surface: [`ErrorReason::NotFound`](crate::ErrorReason::NotFound) or
    /// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) (the program could not be located or
    /// started), [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported) (a requested
    /// platform primitive — user/group switch, `setsid`, umask, or Linux I/O
    /// priority — unavailable on this platform, or — with the `pty` feature —
    /// `use_pty` is combined with a stdout destination other than its merged
    /// `Piped` stream or with a separate stderr destination),
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) (the command's
    /// token was already cancelled), or [`ErrorReason::Io`](crate::ErrorReason::Io) (the
    /// private [`ProcessGroup`] could not be created, or a one-shot streaming
    /// stdin source was already consumed by a previous run).
    ///
    /// With the `pty` feature, a zero PTY axis on all platforms, or an axis above
    /// `i16::MAX` on Windows, returns `ErrorReason::Io(InvalidInput)` before child
    /// spawn. `InvalidInput` is not unique to PTY geometry.
    #[cfg_attr(
        feature = "limits",
        doc = "A resource cap on the new group that cannot be enforced is [`ErrorReason::ResourceLimit`](crate::ErrorReason::ResourceLimit)."
    )]
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
    ///
    /// # Errors
    ///
    /// The launch surface: [`ErrorReason::NotFound`](crate::ErrorReason::NotFound) /
    /// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) (locate/start failure),
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported) (a requested POSIX or
    /// Linux-only primitive unavailable on this platform, or — with the `pty`
    /// feature — `use_pty` is combined with a stdout destination other than its
    /// merged `Piped` stream or with a separate stderr destination),
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) (a pre-cancelled token), or
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) (e.g. a one-shot stdin source already
    /// consumed). Unlike [`JobRunner::start`], no new group is created here — the
    /// child joins this existing group.
    ///
    /// With the `pty` feature, a zero PTY axis on all platforms, or an axis above
    /// `i16::MAX` on Windows, returns `ErrorReason::Io(InvalidInput)` before child
    /// spawn. `InvalidInput` is not unique to PTY geometry.
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

/// Reserve `command`'s stdin source for one run, exactly as the live launch path
/// does: atomically taking a one-shot source
/// ([`Stdin::from_reader`](crate::Stdin::from_reader)/
/// [`Stdin::from_lines`](crate::Stdin::from_lines)) out of its shared cell so a
/// concurrent or later re-run observes it taken and fails loud, instead of
/// silently feeding the next run empty stdin. Returns `Ok(None)` when the command
/// keeps stdin open ([`Command::keep_stdin_open`](crate::Command::keep_stdin_open))
/// or has no stdin source configured at all — neither case reserves anything.
///
/// The reservation is *transactional*: the returned
/// [`StdinReservation`](crate::stdin::StdinReservation) must be
/// [`commit`](crate::stdin::StdinReservation::commit)ted once a child exists, or
/// dropped uncommitted to roll the payload back (so a failed launch does not eat
/// a one-shot source). See [`launch`] and
/// [`ScriptedRunner`](crate::testing::ScriptedRunner).
///
/// Shared by [`launch`] (which commits and drives the payload into the child's
/// pipe) and `ScriptedRunner` (which needs the same reserve-then-commit-or-roll-back
/// consumption side effect — a canned spawn error must leave a one-shot source
/// intact, while a scripted successful start must consume it exactly once, just
/// like live), so the two call sites can never drift on the semantics or the
/// error's wording.
///
/// It also enforces [`Command::inherit_stdin`](crate::Command::inherit_stdin)'s
/// mutual exclusion with a mediated stdin (see below), so every runner rejects an
/// incompatible stdin setup identically.
pub(crate) fn take_stdin_for_run(
    command: &Command,
) -> Result<Option<crate::stdin::StdinReservation>> {
    // PTY stdio compatibility is part of the same shared pre-child launch
    // boundary as stdin reservation. Live, scripted, dry-run, and cassette
    // record paths all route through here, so none can silently accept a
    // destination the real single-master transport cannot honor.
    #[cfg(feature = "pty")]
    command.ensure_pty_stdio_compatible()?;

    if command.inherits_stdin() {
        // `inherit_stdin` hands the child the parent's own stdin fd, so the crate
        // neither drives nor closes stdin. That is a contradiction with either way
        // the crate *would* mediate stdin — an interactive `keep_stdin_open` pipe,
        // or a configured `stdin(Stdin::…)` source (including an explicit
        // `Stdin::empty()`). Reject the conflict here, at the shared launch
        // boundary every runner routes through (live launch, the scripted/fake
        // doubles, and cassette record via `JobRunner`), as a typed
        // `ErrorReason::Io(InvalidInput)` — the same failure mode as the one-shot-consumed
        // guard below — rather than silently letting one setting win.
        if command.keeps_stdin_open() {
            return Err(inherit_stdin_conflict(command, "keep_stdin_open()"));
        }
        if command.stdin_source().is_some() {
            return Err(inherit_stdin_conflict(
                command,
                "a stdin source set via stdin(Stdin::…)",
            ));
        }
        // Nothing to feed: the child reads the parent's stdin directly (wired as
        // `Stdio::inherit()` in `Command::build_tokio`).
        return Ok(None);
    }
    if command.keeps_stdin_open() {
        return Ok(None);
    }
    match command.stdin_source() {
        Some(source) => match source.take_for_run() {
            Ok(reservation) => Ok(Some(reservation)),
            Err(crate::stdin::OneShotConsumed) => Err(crate::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "`{}`: its one-shot streaming stdin (from_reader/from_lines) was \
                     already consumed by a previous run — such a source feeds a single \
                     run and cannot be retried or re-run; use Stdin::from_bytes/from_string \
                     (re-runnable), or rebuild the command with a fresh source",
                    command.program_name()
                ),
            ))),
        },
        None => Ok(None),
    }
}

/// The typed error raised when [`Command::inherit_stdin`](crate::Command::inherit_stdin)
/// is combined with another stdin knob that would drive/close stdin (`other`
/// names it). An `ErrorReason::Io(InvalidInput)` — mirroring the crate's other
/// stdin-misconfiguration refusal (a consumed one-shot source) — so a caller
/// gets one uniform "bad stdin setup" failure mode to match on.
fn inherit_stdin_conflict(command: &Command, other: &str) -> crate::Error {
    crate::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "`{}`: inherit_stdin() cannot be combined with {other} — a child either \
             inherits the parent's stdin or has its stdin mediated by the crate, not \
             both; drop one of the two",
            command.program_name()
        ),
    ))
}

/// Build the OS command, spawn it into `group`, wire stdin, and wrap everything
/// in a [`RunningProcess`] (with no owned group).
pub(crate) async fn launch(group: &ProcessGroup, command: &Command) -> Result<RunningProcess> {
    // A requested argv[0] override, privilege drop, session detach, umask,
    // rlimit, or I/O priority must never be silently skipped: on targets without
    // the relevant primitive, fail before spawning. (`priority` is deliberately
    // absent here — it is implemented on both platform families and never gated
    // as Unsupported.)
    #[cfg(not(unix))]
    {
        if command.requested_arg0() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "arg0 (Unix-only)".into(),
            }
            .into());
        }
        if command.requested_uid().is_some() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "uid".into(),
            }
            .into());
        }
        if command.requested_gid().is_some() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "gid".into(),
            }
            .into());
        }
        if command.requested_groups() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "groups".into(),
            }
            .into());
        }
        if command.wants_setsid() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "setsid".into(),
            }
            .into());
        }
        if command.requested_umask().is_some() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "umask".into(),
            }
            .into());
        }
        if command.requested_rlimits() {
            return Err(crate::ErrorReason::Unsupported {
                operation: "rlimit (Unix-only)".into(),
            }
            .into());
        }
    }
    #[cfg(not(target_os = "linux"))]
    if command.requested_io_priority().is_some() {
        return Err(crate::ErrorReason::Unsupported {
            operation: "io_priority (Linux-only)".into(),
        }
        .into());
    }

    // Already cancelled: short-circuit before spawning.
    if let Some(token) = command.cancel_token()
        && token.is_cancelled()
    {
        return Err(crate::ErrorReason::Cancelled {
            program: command.program_name(),
        }
        .into());
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
        return Err(crate::ErrorReason::Spawn {
            program: command.program_name(),
            source: std::io::Error::new(
                kind,
                format!("working directory {what}: {}", cwd.display()),
            ),
        }
        .into());
    }

    // Reserve stdin before the spawn: a concurrent second run of a one-shot
    // source sees it taken and fails loud, and a spawn failure below rolls the
    // reservation back (via its Drop) rather than eating the payload — so the
    // same command can be launched again. The reservation is committed only once
    // a child exists (see below), after which the source stays consumed even if
    // the stdin write then fails.
    let stdin_reservation = take_stdin_for_run(command)?;

    let mut tokio_cmd = command.build_tokio()?;
    let stderr_is_merged = command.stderr_is_merged_in_pipe();
    let merged_stdout: Option<OutputReader> = if stderr_is_merged {
        #[cfg(any(unix, windows))]
        {
            let (reader, writer) = std::io::pipe().map_err(crate::Error::io)?;
            let stderr_writer = writer.try_clone().map_err(crate::Error::io)?;
            tokio_cmd.stdout(writer);
            tokio_cmd.stderr(stderr_writer);
            Some(crate::sys::merge_pipe::reader(reader).map_err(crate::Error::io)?)
        }
        #[cfg(not(any(unix, windows)))]
        {
            return Err(crate::ErrorReason::Unsupported {
                operation: "merge_stderr_in_pipe (Unix/Windows only)".into(),
            }
            .into());
        }
    } else {
        None
    };
    let opts = crate::sys::SpawnOptions {
        setsid: command.wants_setsid(),
        creation_flags: command.extra_creation_flags(),
        cpu_affinity: {
            #[cfg(windows)]
            {
                command
                    .configured_cpu_affinity()
                    .map(crate::cpu_affinity::windows_mask)
                    .transpose()
                    .map_err(crate::Error::io)?
            }
            #[cfg(not(windows))]
            {
                None
            }
        },
        kill_on_parent_death: command.wants_kill_on_parent_death(),
        windows_new_process_group: command.wants_windows_graceful_ctrl_break(),
        use_pty: command.wants_pty(),
        pty_size: command.configured_pty_size(),
    };
    // PTY mode diverges here: the child is spawned over a single pseudo-terminal
    // master (openpty / ConPTY) instead of three pipes, so its stdin wiring and
    // handle construction differ. Everything up to this point (stdin reservation,
    // cwd/cancel preflight, `SpawnOptions`) is shared. `opts` is `Copy` and the
    // moved values are only used on the (unreachable-when-`use_pty`) pipe path
    // below, so this conditional move type-checks.
    #[cfg(feature = "pty")]
    if command.wants_pty() {
        return launch_pty(group, command, tokio_cmd, opts, stdin_reservation).await;
    }
    // Translate the OS's opaque NotFound into `ErrorReason::NotFound` after the spawn
    // attempt, so the OS stays the source of truth. The cwd was validated above,
    // so NotFound here is genuinely the program. A bare name reports searched dirs;
    // a path-form program gets `searched: None`.
    let mut child = match group.spawn_with_options(&mut tokio_cmd, &opts) {
        Ok(child) => child,
        Err(err) => match err.into_reason() {
            crate::ErrorReason::Spawn { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                if is_bare_name(command.program()) {
                    // Reuse the *same* spawn-free resolution the preflight helper
                    // uses (`command::resolve_program`) to enrich the diagnostic —
                    // one decision, so the two can never disagree. `prefer_local`
                    // is parent-side (plain filesystem probes, independent of the
                    // child env), so it is always searched and always safe to name.
                    // The shared path source selects either the process `PATH` or
                    // the command's effective child `PATH`, matching preflight and
                    // the launch rewrite exactly.
                    return match resolve_program(
                        command.program(),
                        command.prefer_local_dirs(),
                        command.resolution_path_source(),
                    ) {
                        // Located parent-side (a `prefer_local` match) or on `PATH`,
                        // yet the OS still refused with NotFound — the program
                        // exists but isn't *directly* executable (e.g. a .cmd/.bat
                        // on Windows), which is a `Spawn` condition, not "missing".
                        ProgramResolution::Found(_) => Err(crate::ErrorReason::Spawn {
                            program: command.program_name(),
                            source,
                        }
                        .into()),
                        ProgramResolution::NotFound { searched } => {
                            Err(crate::ErrorReason::NotFound {
                                program: command.program_name(),
                                searched,
                            }
                            .into())
                        }
                    };
                }
                return Err(crate::ErrorReason::NotFound {
                    program: command.program_name(),
                    searched: None,
                }
                .into());
            }
            other => return Err(other.into()),
        },
    };
    // A child now exists: commit the reservation so a one-shot source is consumed
    // for good. Every failure path above returned before this point, dropping the
    // reservation uncommitted and rolling its payload back. The commit precedes
    // the stdin write below, so the source stays consumed even if that write ends
    // in BrokenPipe (the child closed its read end) or any other error.
    let taken_stdin = stdin_reservation.map(crate::stdin::StdinReservation::commit);
    let pid = child.id();
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        program = %command.program_name(),
        pid = ?pid,
        mechanism = ?group.mechanism(),
        "child spawned"
    );
    #[cfg(feature = "metrics")]
    crate::metrics::record_spawn(&command.program_name(), group.mechanism());

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

    let stdout = merged_stdout.or_else(|| {
        child
            .stdout
            .take()
            .map(|pipe| Box::new(pipe) as OutputReader)
    });
    let stderr = if stderr_is_merged {
        None
    } else {
        child
            .stderr
            .take()
            .map(|pipe| Box::new(pipe) as OutputReader)
    };

    let mut process = RunningProcess::from_spawned(Spawned {
        program: command.program_name(),
        child,
        own_group: None,
        stdout,
        stderr,
        stdin: stdin_pipe,
        stdin_task,
        timeout: command.configured_timeout(),
        inactivity_timeout: command.configured_inactivity_timeout(),
        timeout_grace: command.configured_timeout_grace(),
        timeout_signal: command.timeout_signal_raw(),
        pid,
        stdout_config: command.stdout_config(),
        stderr_config: command.stderr_config(),
        buffer: command.output_buffer_policy(),
        ok_codes: command.ok_codes_vec(),
        stdout_piped: stderr_is_merged || command.stdout_is_piped(),
        stderr_piped: !stderr_is_merged && command.stderr_is_piped(),
        cancel_token: command.cancel_token(),
        cancel_grace: command.configured_cancel_grace(),
        cancel_signal: command.cancel_signal_raw(),
    });
    // Pid-only watchdog; own-group runs re-arm with full group+pid via `attach_group`.
    process.arm_cancel_watchdog();
    Ok(process)
}

/// Translate a raw spawn [`Error`](crate::Error) into the crate's launch error, mapping the
/// OS's opaque `NotFound` into [`ErrorReason::NotFound`](crate::ErrorReason::NotFound)
/// (enriched with the searched dirs for a bare name, or reclassified as
/// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) when the program *is* locatable
/// but not directly executable) — the same enrichment the pipe launch path does
/// inline. Shared with the PTY launch path so the two never diverge on how a
/// missing program is reported.
#[cfg(feature = "pty")]
fn map_spawn_error(command: &Command, err: crate::Error) -> crate::Error {
    match err.into_reason() {
        crate::ErrorReason::Spawn { source, .. }
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            if is_bare_name(command.program()) {
                match resolve_program(
                    command.program(),
                    command.prefer_local_dirs(),
                    command.resolution_path_source(),
                ) {
                    ProgramResolution::Found(_) => crate::ErrorReason::Spawn {
                        program: command.program_name(),
                        source,
                    }
                    .into(),
                    ProgramResolution::NotFound { searched } => crate::ErrorReason::NotFound {
                        program: command.program_name(),
                        searched,
                    }
                    .into(),
                }
            } else {
                crate::ErrorReason::NotFound {
                    program: command.program_name(),
                    searched: None,
                }
                .into()
            }
        }
        other => other.into(),
    }
}

/// The [`launch`] counterpart for [`Command::use_pty`](crate::Command::use_pty):
/// spawn the child over a single pseudo-terminal master (openpty / ConPTY),
/// contained in the same group, and wrap it in a merged-stream
/// [`RunningProcess`]. Shares `launch`'s stdin-reservation contract — the
/// reservation is committed once the child exists, and the same one-shot stdin
/// payload is driven into the master's input side on a background task.
#[cfg(feature = "pty")]
async fn launch_pty(
    group: &ProcessGroup,
    command: &Command,
    mut tokio_cmd: tokio::process::Command,
    opts: crate::sys::SpawnOptions,
    stdin_reservation: Option<crate::stdin::StdinReservation>,
) -> Result<RunningProcess> {
    // The Windows raw-`CreateProcessW` path needs the fully-resolved env (it
    // bypasses `std`'s env handling); ignored on Unix.
    let env = command.resolved_pty_env();
    let pty = group
        .spawn_pty_with_options(&mut tokio_cmd, &opts, env)
        .map_err(|e| map_spawn_error(command, e))?;
    // A child now exists: commit the reservation so a one-shot source is consumed
    // for good (see `launch`).
    let taken_stdin = stdin_reservation.map(crate::stdin::StdinReservation::commit);
    let pid = pty.pid;
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        program = %command.program_name(),
        pid = ?pid,
        mechanism = ?group.mechanism(),
        "pty child spawned"
    );
    #[cfg(feature = "metrics")]
    crate::metrics::record_spawn(&command.program_name(), group.mechanism());

    let crate::sys::pty::PtySpawn {
        child,
        reader,
        writer,
        pid: _,
    } = pty;

    // Stdin wiring over the single master input side. `keep_stdin_open` keeps the
    // writer for `take_stdin`; otherwise a background task drives any configured
    // source and then asks the platform writer to deliver its terminal EOF
    // gesture. A PTY master has no true half-close, so merely dropping one dup is
    // insufficient on Unix while the reader/resize dups remain alive.
    let (writer_for_stdin, stdin_task) = if command.keeps_stdin_open() {
        (Some(writer), None)
    } else {
        let mut sink = writer;
        let task = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;

            if let Some(payload) = taken_stdin
                && !payload.is_empty()
            {
                payload.write_to(&mut sink).await?;
            }
            sink.shutdown().await
        });
        (None, Some(task))
    };

    let mut process = RunningProcess::from_pty(crate::running::PtySpawned {
        program: command.program_name(),
        child,
        reader,
        writer: writer_for_stdin,
        own_group: None,
        stdin_task,
        timeout: command.configured_timeout(),
        inactivity_timeout: command.configured_inactivity_timeout(),
        timeout_grace: command.configured_timeout_grace(),
        timeout_signal: command.timeout_signal_raw(),
        pid,
        stdout_config: command.stdout_config(),
        buffer: command.output_buffer_policy(),
        ok_codes: command.ok_codes_vec(),
        stdout_piped: command.stdout_is_piped(),
        cancel_token: command.cancel_token(),
        cancel_grace: command.configured_cancel_grace(),
        cancel_signal: command.cancel_signal_raw(),
    });
    process.arm_cancel_watchdog();
    Ok(process)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, ErrorReason};
    use crate::result::Outcome;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[cfg(feature = "pty")]
    #[test]
    fn pty_spawn_error_names_the_effective_child_path() {
        let dir = tempfile::tempdir().expect("temp PATH dir");
        let child_path = std::env::join_paths([dir.path()]).expect("single PATH entry");
        let command = Command::new("processkit-missing-pty-path").env("PATH", &child_path);
        let raw = ErrorReason::Spawn {
            program: command.program_name(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "synthetic PTY spawn miss"),
        }
        .into();

        match map_spawn_error(&command, raw).into_reason() {
            ErrorReason::NotFound { searched, .. } => assert_eq!(
                searched,
                Some(child_path.to_string_lossy().into_owned()),
                "PTY enrichment must use the effective child PATH"
            ),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_unix_backslash_name_reports_path_search() {
        let command = Command::new(r"processkit-definitely-missing\backslash-T113");
        let err = JobRunner::new()
            .start(&command)
            .await
            .expect_err("the deliberately absent program must not launch");

        match err.into_reason() {
            ErrorReason::NotFound {
                searched: Some(_), ..
            } => {}
            other => panic!("a Unix backslash name must be enriched as bare: {other:?}"),
        }
    }

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

    /// A plain `inherit_stdin()` (no conflicting knob) reserves nothing to feed —
    /// the child reads the parent's stdin directly, so there is no payload and no
    /// error.
    #[test]
    fn inherit_stdin_alone_reserves_no_payload() {
        let command = Command::new("child").inherit_stdin();
        let reservation = take_stdin_for_run(&command).expect("plain inherit is valid");
        assert!(
            reservation.is_none(),
            "inherit_stdin feeds no payload — nothing to reserve"
        );
    }

    /// `inherit_stdin()` + `keep_stdin_open()` is a contradiction (share the
    /// parent's stdin AND be handed an interactive pipe) and is rejected at the
    /// launch boundary with a typed `ErrorReason::Io(InvalidInput)`, not a silent
    /// last-write-wins.
    #[test]
    fn inherit_stdin_conflicts_with_keep_stdin_open() {
        // `Ok` carries a `StdinReservation`, which is intentionally not `Debug`
        // (it guards a live payload), so assert on the error via `match`, not
        // `expect_err`.
        let command = Command::new("child").inherit_stdin().keep_stdin_open();
        match take_stdin_for_run(&command).map_err(|e| e.into_reason()) {
            Err(ErrorReason::Io(io)) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
            Err(other) => panic!("expected ErrorReason::Io(InvalidInput), got {other:?}"),
            Ok(_) => panic!("inherit_stdin + keep_stdin_open must be rejected"),
        }
        // Order-independent: the conflict is rejected regardless of builder order.
        let reversed = Command::new("child").keep_stdin_open().inherit_stdin();
        assert!(
            take_stdin_for_run(&reversed).is_err(),
            "the conflict holds whichever knob was set last"
        );
    }

    /// `inherit_stdin()` + a configured `stdin(Stdin::…)` source (a re-runnable
    /// payload, a one-shot stream, or even an explicit `Stdin::empty()`) is
    /// rejected the same way — you cannot both feed the child a source and let it
    /// read the parent's stdin.
    #[test]
    fn inherit_stdin_conflicts_with_a_configured_source() {
        for source in [
            crate::Stdin::from_string("payload"),
            crate::Stdin::empty(),
            crate::Stdin::from_reader(&b"stream"[..]),
        ] {
            let command = Command::new("child").stdin(source).inherit_stdin();
            match take_stdin_for_run(&command).map_err(|e| e.into_reason()) {
                Err(ErrorReason::Io(io)) => assert_eq!(io.kind(), std::io::ErrorKind::InvalidInput),
                Err(other) => panic!("expected ErrorReason::Io(InvalidInput), got {other:?}"),
                Ok(_) => panic!("inherit_stdin + a stdin source must be rejected"),
            }
        }
    }

    #[tokio::test]
    async fn boxed_and_arc_runners_are_runners() {
        // G1: a runner chosen at runtime (real vs cassette, from config) can be
        // stored type-erased and still injected wherever a `ProcessRunner` is
        // expected — including the `ProcessRunnerExt` verbs.
        let boxed: Box<dyn ProcessRunner> = Box::new(flaky(0));
        assert_eq!(
            boxed.run(&Command::new("x")).await.expect("boxed runs"),
            "out"
        );
        // `start` forwards to the inner runner (Flaky doesn't override it →
        // Unsupported), proving the box doesn't shadow with its own default.
        assert!(
            boxed.start(&Command::new("x")).await.is_err(),
            "start forwards to the inner runner's Unsupported default"
        );

        let shared: std::sync::Arc<dyn ProcessRunner> = std::sync::Arc::new(flaky(0));
        let shared2 = std::sync::Arc::clone(&shared);
        assert_eq!(
            shared.run(&Command::new("x")).await.expect("arc runs"),
            "out"
        );
        assert_eq!(
            shared2
                .run(&Command::new("x"))
                .await
                .expect("arc clone runs"),
            "out"
        );

        // The impls are generic over `R: ?Sized`, so a *concrete* boxed/shared
        // runner (not just the type-erased `dyn` form) is a runner too.
        let boxed_concrete: Box<Flaky> = Box::new(flaky(0));
        assert_eq!(
            boxed_concrete
                .run(&Command::new("x"))
                .await
                .expect("box<concrete>"),
            "out"
        );
        let arc_concrete: std::sync::Arc<Flaky> = std::sync::Arc::new(flaky(0));
        assert_eq!(
            arc_concrete
                .run(&Command::new("x"))
                .await
                .expect("arc<concrete>"),
            "out"
        );
    }

    #[tokio::test]
    async fn retry_retries_until_success() {
        let runner = flaky(2);
        let cmd = Command::new("x").retry(5, Duration::from_millis(0), |e| {
            matches!(e.reason(), ErrorReason::Exit { .. })
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
    async fn one_shot_stdin_is_not_retried_on_a_post_child_error() {
        // `flaky` reports a non-zero exit — a *post-child* `ErrorReason::Exit`: a child
        // ran, so a one-shot source would have been consumed. The gate refuses to
        // retry it (contrast the pre-child launch failure covered below), while a
        // re-runnable source still retries to the cap.
        let runner = flaky(10);
        let cmd = Command::new("x")
            .stdin(crate::Stdin::from_reader(&b"once"[..]))
            .retry(5, Duration::from_millis(0), |_| true);
        assert!(runner.run(&cmd).await.is_err());
        assert_eq!(
            runner.calls.load(Ordering::SeqCst),
            1,
            "a one-shot stdin command is not retried after a post-child error"
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
    async fn one_shot_stdin_is_retried_after_a_pre_child_launch_failure() {
        // A launch failure guaranteed to precede a live child (a transient
        // `Spawn`/ETXTBSY, or `NotFound`) rolls the one-shot stdin reservation
        // back, so the command IS retried and the eventual successful attempt
        // feeds the untouched payload — the canonical transient-spawn retry that
        // the old blanket refusal wrongly denied.

        /// Mimics the live launch's transactional stdin handling: reserves the
        /// command's stdin exactly as `launch` does, fails *before a child
        /// exists* for its first `fail_times` calls (dropping the reservation
        /// uncommitted, so a one-shot payload rolls back intact), then commits
        /// the reservation and echoes the payload it read.
        struct PreChildThenEcho {
            calls: AtomicU32,
            fail_times: u32,
            make_err: fn(String) -> Error,
        }

        #[async_trait::async_trait]
        impl ProcessRunner for PreChildThenEcho {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                let reservation = take_stdin_for_run(command)?;
                let program = command.program().to_string_lossy().into_owned();
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_times {
                    // Fail before a child exists: dropping the reservation
                    // uncommitted rolls a one-shot payload back into its cell.
                    drop(reservation);
                    return Err((self.make_err)(program));
                }
                // A child would now exist: commit and drive the payload into a
                // sink to prove the retried attempt saw the real, untouched bytes.
                let mut sink = Vec::new();
                if let Some(reservation) = reservation {
                    reservation
                        .commit()
                        .write_to(&mut sink)
                        .await
                        .expect("write the reserved one-shot payload");
                }
                Ok(ProcessResult::new(
                    program,
                    String::from_utf8(sink).expect("utf8 stdin"),
                    String::new(),
                    Outcome::Exited(0),
                    None,
                ))
            }
        }

        let transient_spawn: fn(String) -> Error = |p| {
            Error::spawn(
                p,
                std::io::Error::from(std::io::ErrorKind::ExecutableFileBusy),
            )
        };
        let not_found: fn(String) -> Error = |p| Error::not_found(p, None);

        for make_err in [transient_spawn, not_found] {
            let runner = PreChildThenEcho {
                calls: AtomicU32::new(0),
                fail_times: 2,
                make_err,
            };
            let cmd = Command::new("x")
                .stdin(crate::Stdin::from_reader(&b"hello"[..]))
                // Accept both a transient spawn error and a not-found, so it is
                // the pre-child gate — not the classifier — that this exercises.
                .retry(5, Duration::from_millis(0), |e| {
                    e.is_transient() || e.is_not_found()
                });
            let out = runner
                .run(&cmd)
                .await
                .expect("a pre-child launch failure is retried");
            assert_eq!(
                out, "hello",
                "the retried attempt fed the untouched one-shot payload"
            );
            assert_eq!(
                runner.calls.load(Ordering::SeqCst),
                3,
                "two pre-child failures then a success"
            );
        }
    }

    #[tokio::test]
    async fn one_shot_stdin_post_child_error_is_returned_as_is_not_reclassified() {
        // A post-child failure (here a `Timeout`) means a child ran and
        // committed — consumed — the one-shot source. The gate returns that first
        // error unchanged; it never retries into the spent source and so never
        // reclassifies it as an `Io` "already consumed" failure.

        /// Commits the reservation on every call (a live child existed), then
        /// reports a `Timeout` — a post-child error over a consumed one-shot
        /// source. A retry would re-reserve and hit the "already consumed" `Io`;
        /// the test asserts that never happens.
        struct SpawnThenTimeout {
            calls: AtomicU32,
        }

        #[async_trait::async_trait]
        impl ProcessRunner for SpawnThenTimeout {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                let reservation = take_stdin_for_run(command)?;
                self.calls.fetch_add(1, Ordering::SeqCst);
                // A child existed and consumed the one-shot source (commit),
                // exactly as `launch` does on a successful spawn.
                let _consumed = reservation.map(crate::stdin::StdinReservation::commit);
                Err(Error::timeout(
                    command.program().to_string_lossy().into_owned(),
                    Duration::from_secs(1),
                    "",
                    "",
                ))
            }
        }

        let runner = SpawnThenTimeout {
            calls: AtomicU32::new(0),
        };
        let cmd = Command::new("x")
            .stdin(crate::Stdin::from_reader(&b"once"[..]))
            .retry(5, Duration::from_millis(0), |_| true);
        let err = runner
            .run(&cmd)
            .await
            .expect_err("a post-child timeout on a one-shot command errors");
        assert!(
            matches!(err.reason(), ErrorReason::Timeout { .. }),
            "the first post-child error is returned as-is, got {err:?}"
        );
        assert_eq!(
            runner.calls.load(Ordering::SeqCst),
            1,
            "a post-child error on a one-shot stdin command is not retried"
        );
    }

    #[tokio::test]
    async fn probe_with_ok_codes_does_not_panic_on_a_non_binary_exit() {
        use crate::testing::{Reply, ScriptedRunner};
        let runner = ScriptedRunner::new().on(["tool", "x"], Reply::fail(2, "boom"));
        let cmd = Command::new("tool").args(["x"]).ok_codes([0, 1, 2]);
        assert!(matches!(
            runner.probe(&cmd).await.map_err(|e| e.into_reason()),
            Err(ErrorReason::Exit { code: 2, .. })
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
                s.trim().parse::<u32>().map_err(|e| {
                    crate::Error::from(ErrorReason::Parse {
                        program: "tool".into(),
                        message: e.to_string(),
                    })
                })
            })
            .await
            .expect_err("a parser failure is an error");
        assert!(
            matches!(err.reason(), ErrorReason::Parse { .. }),
            "got {err:?}"
        );

        let fail_runner = ScriptedRunner::new().on(["tool"], Reply::fail(3, "boom"));
        let err = fail_runner
            .try_parse::<u32, _>(&Command::new("tool"), |_| {
                panic!("parser must not run on a failed exit")
            })
            .await
            .expect_err("a non-zero exit is an error");
        assert!(
            matches!(err.reason(), ErrorReason::Exit { code: 3, .. }),
            "got {err:?}"
        );
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn output_json_deserializes_checked_stdout_and_bounds_failures() {
        use crate::testing::{Reply, ScriptedRunner};

        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct Payload {
            value: u32,
        }

        let runner = ScriptedRunner::new()
            .on(["tool", "ok"], Reply::ok("{\"value\":42}"))
            .on(
                ["tool", "bad"],
                Reply::ok(format!(
                    "{{\"padding\":\"{}\",\"value\":nope}}",
                    "secret".repeat(100)
                )),
            );
        let payload: Payload = runner
            .output_json(&Command::new("tool").arg("ok"))
            .await
            .expect("valid JSON");
        assert_eq!(payload, Payload { value: 42 });

        let error = runner
            .output_json::<Payload>(&Command::new("tool").arg("bad"))
            .await
            .expect_err("invalid JSON");
        let ErrorReason::Parse { program, message } = error.reason() else {
            panic!("expected Parse, got {error:?}");
        };
        assert_eq!(program, "tool");
        assert!(message.contains("line 1"));
        assert!(message.contains("byte offset"));
        assert!(message.contains("fragment `…"));
        assert!(
            !message.contains(&"secret".repeat(50)),
            "the public field must not retain the complete child output"
        );
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn output_json_rejects_a_truncated_capture_before_decoding() {
        struct TruncatedJsonRunner;

        #[async_trait::async_trait]
        impl ProcessRunner for TruncatedJsonRunner {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    "{\"value\":42}".to_owned(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                )
                .with_truncated(true)
                .with_overflow_totals(2, 4096))
            }
        }

        let error = TruncatedJsonRunner
            .output_json::<serde_json::Value>(&Command::new("tool"))
            .await
            .expect_err("a JSON parser must not see an incomplete document");
        assert!(
            matches!(error.reason(), ErrorReason::OutputTooLarge { .. }),
            "got {error:?}"
        );
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
        assert!(
            matches!(err.reason(), ErrorReason::OutputTooLarge { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_backoff_is_cancellable() {
        // E1: a cancel token firing during the retry backoff resolves promptly
        // with Cancelled instead of waiting out the (here 60s) delay.
        let runner = flaky(5); // always fails within the retry budget
        let token = crate::CancellationToken::new();
        let cmd = Command::new("x")
            .retry(5, Duration::from_secs(60), |_| true)
            .cancel_on(token.clone());
        let canceller = tokio::spawn({
            let token = token.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                token.cancel();
            }
        });
        let start = tokio::time::Instant::now();
        let err = runner
            .run(&cmd)
            .await
            .expect_err("a cancelled backoff errors");
        assert!(
            matches!(err.reason(), ErrorReason::Cancelled { .. }),
            "got {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the backoff must be cancellable, took {:?}",
            start.elapsed()
        );
        canceller.await.expect("canceller");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_sleeps_the_backoff_between_attempts() {
        let runner = flaky(2);
        let cmd = Command::new("x").retry(5, Duration::from_millis(100), |e| {
            matches!(e.reason(), ErrorReason::Exit { .. })
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
            Err(ErrorReason::Cancelled {
                program: command.program().to_string_lossy().into_owned(),
            }
            .into())
        }
    }

    #[tokio::test]
    async fn cancelled_is_terminal_even_when_the_classifier_accepts() {
        let runner = AlwaysCancelled(AtomicU32::new(0));
        let cmd = Command::new("x").retry(5, Duration::from_millis(0), |_| true);
        let err = runner.run(&cmd).await.expect_err("cancelled run errors");
        assert!(
            matches!(err.reason(), ErrorReason::Cancelled { .. }),
            "expected Cancelled, got {err:?}"
        );
        assert_eq!(
            runner.0.load(Ordering::SeqCst),
            1,
            "a cancelled run must not be retried"
        );
    }

    struct AlwaysTeardown(AtomicU32);

    #[async_trait::async_trait]
    impl ProcessRunner for AlwaysTeardown {
        async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(crate::Error::teardown(
                command.program().to_string_lossy(),
                crate::TeardownCause::Timeout,
                "process-group hard kill",
                std::io::Error::other("kill refused"),
                String::new(),
                String::new(),
                None,
            ))
        }
    }

    #[tokio::test]
    async fn unconfirmed_teardown_is_terminal_even_when_the_classifier_accepts() {
        let runner = AlwaysTeardown(AtomicU32::new(0));
        let cmd = Command::new("x").retry(5, Duration::ZERO, |_| true);
        let error = runner
            .run(&cmd)
            .await
            .expect_err("unconfirmed teardown is never retried");
        assert!(error.is_teardown(), "expected Teardown, got {error:?}");
        assert_eq!(
            runner.0.load(Ordering::SeqCst),
            1,
            "a potentially-live failed attempt must not be duplicated"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns a real child for the streamed deadline teardown-failure path"]
    async fn first_line_surfaces_an_unconfirmed_deadline_teardown() {
        #[cfg(unix)]
        let command = Command::new("sh")
            .args(["-c", "printf 'prefix\\n'; sleep 60"])
            .timeout(Duration::from_millis(100));
        #[cfg(windows)]
        let command = Command::new("powershell")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::Out.WriteLine('prefix'); Start-Sleep -Seconds 60",
            ])
            .timeout(Duration::from_millis(100));
        let faults = crate::sys::fault_injection::Faults::new()
            .fail_every(
                crate::sys::fault_injection::Site::ProcessGroupTeardown,
                Some("hard"),
                5,
            )
            .arm();

        let error = JobRunner::new()
            .first_line(&command, |_| false)
            .await
            .expect_err("a potentially-live child must not be reported as Timeout");
        match error.reason() {
            ErrorReason::Teardown {
                cause,
                operation,
                source,
                ..
            } => {
                assert_eq!(*cause, crate::TeardownCause::Timeout);
                assert_eq!(*operation, "process-group hard kill");
                assert_eq!(source.raw_os_error(), Some(5));
            }
            other => panic!("expected Teardown, got {other:?}"),
        }
        assert!(faults.fired(crate::sys::fault_injection::Site::ProcessGroupTeardown) >= 1);
    }

    // The natural-end-of-stream-vs-late-token *race* the E7 change eliminates is
    // a real-time window with no post-await gap in the new code, so it can't be
    // reproduced deterministically here; these two lock in the invariant that
    // matters day-to-day — merely *wiring* a (never-fired) cancel token must not
    // perturb a normal `first_line` result. The end-to-end cancel path (a token
    // that actually fires) is covered by the cancellation integration suite.
    #[tokio::test]
    async fn first_line_no_match_is_none_with_a_cancel_token_wired() {
        use crate::testing::{Reply, ScriptedRunner};
        let runner = ScriptedRunner::new().on(["tool"], Reply::lines(["alpha", "beta"]));
        let cmd = Command::new("tool").cancel_on(crate::CancellationToken::new());
        let found = runner
            .first_line(&cmd, |l| l.contains("zzz"))
            .await
            .expect("a no-match run is Ok(None), not an error");
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn first_line_returns_the_match_with_a_cancel_token_wired() {
        use crate::testing::{Reply, ScriptedRunner};
        let runner =
            ScriptedRunner::new().on(["tool"], Reply::lines(["alpha", "ready: yes", "beta"]));
        let cmd = Command::new("tool").cancel_on(crate::CancellationToken::new());
        let found = runner
            .first_line(&cmd, |l| l.contains("ready"))
            .await
            .expect("first_line");
        assert_eq!(found.as_deref(), Some("ready: yes"));
    }

    #[tokio::test]
    async fn first_line_with_an_unfired_timeout_still_returns_none_on_a_natural_end() {
        use crate::testing::{Reply, ScriptedRunner};
        // A timeout is *set* but the fast scripted stream ends with no match well
        // before it, so the deadline arbiter stays `PENDING` and first_line reports
        // `Ok(None)`, not `Timeout`. Locks in that reading the arbiter to classify a
        // deadline kill doesn't misclassify a natural end when a deadline was
        // configured but never fired.
        let runner = ScriptedRunner::new().on(["tool"], Reply::lines(["alpha", "beta"]));
        let cmd = Command::new("tool").timeout(Duration::from_secs(30));
        let found = runner
            .first_line(&cmd, |l| l.contains("zzz"))
            .await
            .expect("a natural no-match end with an unfired timeout is Ok(None)");
        assert_eq!(found, None);
    }

    // ---- T-084: live one-shot stdin transactionality (real spawn path) ----
    //
    // These exercise the real `JobRunner` launch path end to end, so — like the
    // rest of the crate's real-subprocess coverage — they are `#[ignore]`d and run
    // explicitly (`cargo test -- --ignored`). The always-on hermetic coverage lives
    // in `stdin.rs` (the reservation state machine) and `doubles.rs` (the scripted
    // seam sharing the same `take_stdin_for_run` reserve/commit/rollback).

    /// A program that echoes its stdin to stdout: `cat` on Unix, `cmd /c sort` on
    /// Windows (`sort` reads stdin). A single-line payload passes through unchanged.
    fn stdin_echo(source: crate::Stdin) -> Command {
        if cfg!(windows) {
            Command::new("cmd").args(["/c", "sort"]).stdin(source)
        } else {
            Command::new("cat").stdin(source)
        }
    }

    /// A program that exits immediately without reading its stdin.
    fn exits_zero(source: crate::Stdin) -> Command {
        if cfg!(windows) {
            Command::new("cmd").args(["/c", "exit", "0"]).stdin(source)
        } else {
            Command::new("sh").args(["-c", "exit 0"]).stdin(source)
        }
    }

    #[tokio::test]
    #[ignore = "exercises the real spawn path (creates a process group and a child)"]
    async fn one_shot_stdin_is_returned_after_a_spawn_error_and_reused_live() {
        // A launch that fails before a child exists returns the payload, so the
        // same one-shot source feeds a later successful run — which consumes it.
        let source = crate::Stdin::from_reader(&b"hello stdin\n"[..]);
        let runner = JobRunner::new();

        // A missing program: NotFound *after* the reservation but *before* a child.
        let missing =
            Command::new("processkit-definitely-missing-T084-stdin").stdin(source.clone());
        let err = runner
            .output_string(&missing)
            .await
            .expect_err("a missing program must error");
        assert!(
            matches!(
                err.reason(),
                ErrorReason::NotFound { .. } | ErrorReason::Spawn { .. }
            ),
            "expected a pre-child launch failure, got {err:?}"
        );

        // The rolled-back payload now feeds a real child.
        let result = runner
            .output_string(&stdin_echo(source.clone()))
            .await
            .expect("the preserved one-shot stdin feeds the echo program");
        assert!(result.is_success(), "result: {result:?}");
        assert!(
            result.stdout().contains("hello stdin"),
            "the child should have received the preserved stdin: {result:?}"
        );

        // Consumed exactly once: a re-run of the now-spent source fails loud.
        let err = runner
            .output_string(&stdin_echo(source))
            .await
            .expect_err("the one-shot source is consumed after the successful run");
        assert!(
            matches!(err.reason(), ErrorReason::Io(_)),
            "expected Io, got {err:?}"
        );
    }

    #[tokio::test]
    #[ignore = "exercises the real spawn path with two concurrent children"]
    async fn two_concurrent_live_launches_let_only_one_feed_a_child() {
        // Two concurrent launches of one cloned one-shot source never both spawn a
        // child: exactly one reserves the payload; the other fails loud.
        let source = crate::Stdin::from_reader(&b"data\n"[..]);
        let runner = JobRunner::new();

        let first = stdin_echo(source.clone());
        let second = stdin_echo(source.clone());
        let (r1, r2) = tokio::join!(runner.output_string(&first), runner.output_string(&second));

        let successes = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
        assert_eq!(
            successes, 1,
            "exactly one concurrent launch feeds a child; r1={r1:?} r2={r2:?}"
        );
        // The loser fails loud on the taken source, not silently with empty stdin.
        let loser = if r1.is_err() { r1 } else { r2 };
        assert!(
            matches!(loser.unwrap_err().reason(), ErrorReason::Io(_)),
            "the losing concurrent launch must fail loud"
        );
    }

    #[tokio::test]
    #[ignore = "exercises the real spawn path (creates a process group)"]
    async fn one_shot_stdin_survives_cancellation_before_spawn_live() {
        // A launch cancelled before it spawns must not eat the one-shot source —
        // the cancel short-circuits ahead of the reservation.
        let source = crate::Stdin::from_reader(&b"data\n"[..]);
        let runner = JobRunner::new();

        let token = crate::CancellationToken::new();
        token.cancel();
        let cancelled = stdin_echo(source.clone()).cancel_on(token);
        let err = runner
            .output_string(&cancelled)
            .await
            .expect_err("a pre-cancelled launch errors");
        assert!(
            matches!(err.reason(), ErrorReason::Cancelled { .. }),
            "expected Cancelled, got {err:?}"
        );

        // The source was never reserved, so it still feeds a run.
        let result = runner
            .output_string(&stdin_echo(source))
            .await
            .expect("cancel-before-spawn left the one-shot source intact");
        assert!(result.is_success(), "result: {result:?}");
    }

    #[tokio::test]
    #[ignore = "exercises the real spawn path; the child ignores a large stdin then exits"]
    async fn one_shot_stdin_stays_consumed_after_a_stdin_writer_error_live() {
        // Once a child exists the source is consumed for good, even if the stdin
        // write then fails (the child exited without reading — a broken pipe).
        use tokio::io::AsyncReadExt;
        // A megabyte of stdin fed to a child that exits immediately forces the
        // writer to hit BrokenPipe partway through.
        let source = crate::Stdin::from_reader(tokio::io::repeat(b'x').take(1 << 20));
        let runner = JobRunner::new();

        let result = runner
            .output_string(&exits_zero(source.clone()))
            .await
            .expect("a broken-pipe stdin writer must not fail an otherwise-successful run");
        assert!(result.is_success(), "result: {result:?}");

        // The successful spawn consumed the source despite the write failing.
        let err = runner
            .output_string(&exits_zero(source))
            .await
            .expect_err("the one-shot source stays consumed after a successful spawn");
        assert!(
            matches!(err.reason(), ErrorReason::Io(_)),
            "expected Io, got {err:?}"
        );
    }
}
