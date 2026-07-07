// `doc_cfg` (nightly-only) auto-derives the "Available on crate feature" badges
// from `#[cfg]` gates; gated behind `docsrs` so stable/CI `cargo doc` ignores it.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

//! `processkit` — async child-process management for Rust + [tokio]: whole-tree
//! kill-on-drop (no orphaned subprocesses), run-and-capture, streaming,
//! shell-free pipelines, timeouts & cancellation, and supervision.
//!
//! [tokio]: https://tokio.rs/
//!
//! Two layers:
//!
//! - **[`ProcessGroup`]** — a kill-on-drop container for a process *tree*. Every
//!   child spawned into the group, and everything those children spawn, dies
//!   with the group, so an exiting or panicking owner doesn't leak subprocesses.
//!   Containment is a Windows [Job Object], a Linux [cgroup v2] (with a POSIX
//!   process-group fallback), or a POSIX process group on macOS/BSD —
//!   observable via [`Mechanism`]. Two caveats the [`ProcessGroup`] /
//!   [`Mechanism`] docs spell out: the guarantee rides on `Drop` running (a
//!   `panic = "abort"` process, or a `SIGKILL`/power-loss of the *owner*, skips
//!   it — the OS-owned Job Object / cgroup still reaps on handle close, the POSIX
//!   process-group fallback does not), and on the process-group mechanism a child
//!   that calls `setsid` escapes containment. The whole tree can be
//!   signalled (`ProcessGroup::signal`, see `Signal`), paused/resumed
//!   (`ProcessGroup::suspend` / `ProcessGroup::resume`), and inspected
//!   (`ProcessGroup::members`); [`wait_any`] races several running processes
//!   and reports the first to exit.
//! - **runner** — async run-and-capture built on the group. Describe a run with
//!   [`Command`], then drive it to completion ([`Command::output_string`],
//!   [`Command::run`], …) or [`start`](Command::start) it for streaming and
//!   interactive I/O. The [`ProcessRunner`] trait runs commands to completion
//!   and is the mock seam (see [`ScriptedRunner`](testing::ScriptedRunner)). A
//!   [`Supervisor`] keeps a command *alive* — restarting it per policy with
//!   backoff — where [`Command::retry`] merely replays one run to success.
//!   Readiness probes ([`RunningProcess::wait_for_line`] /
//!   [`wait_for_port`](RunningProcess::wait_for_port) /
//!   [`wait_for`](RunningProcess::wait_for)) wait until a started child is
//!   actually *ready* instead of sleeping. A [`Pipeline`]
//!   ([`Command::pipe`]) chains commands stdout→stdin without a shell — one
//!   shared group, pipefail outcome. [`Command::cancel_on`] ties a run to a
//!   [`CancellationToken`]: cancelling it kills the tree and every consuming
//!   path resolves to [`Error::Cancelled`]. Spawn-time sandboxing knobs:
//!   [`Command::inherit_env`] (env allow-list), [`Command::uid`] /
//!   [`Command::gid`] (Unix privilege drop), [`Command::setsid`],
//!   [`Command::create_no_window`], [`Command::priority`] (CPU-scheduling
//!   priority, both platforms), [`Command::umask`] (Unix file-creation mask).
//!
//! Async throughout (tokio). Errors are the structured [`Error`]; a non-zero
//! exit is reported in [`ProcessResult`], not raised, until you call
//! [`ProcessResult::ensure_success`].
//!
//! **Stability.** Since **1.0**, `processkit` follows [Semantic Versioning]: the
//! public API is stable, and any breaking change lands only in a new *major*
//! version, so `1.x` upgrades are backward-compatible. (The lone exception is the
//! `mock` feature's `mockall`-generated `expect_*` surface — see below.)
//!
//! [Semantic Versioning]: https://semver.org/spec/v2.0.0.html
//!
//! Beyond this page, the repository ships a narrative [guide set] — a
//! task-oriented [cookbook] ("I want to …" → snippet), a deep guide per
//! capability, and every per-platform caveat collected in one place.
//!
//! [guide set]: https://github.com/ZelAnton/ProcessKit-rs/tree/main/docs#readme
//! [cookbook]: https://github.com/ZelAnton/ProcessKit-rs/blob/main/docs/cookbook.md
//!
//! **Run vocabulary** — one verb, one meaning, at every layer ([`Command`],
//! [`ProcessRunner`]/[`ProcessRunnerExt`], [`CliClient`]):
//!
//! - **`run`** — require an **accepted** exit (`0` by default, widened by
//!   [`Command::ok_codes`]) and return stdout as a `String`, trailing whitespace
//!   trimmed (`trim_end`: the final newline is noise, but leading whitespace can
//!   be significant). **`run_unit`** — the same, discarding the output.
//! - **`output_string`** / **`output_bytes`** — return the full
//!   [`ProcessResult`] (stdout as text / raw bytes); a non-zero exit is *not* an
//!   error here. (`output_string`, not a bare `output`, since
//!   `std::process::Command::output` yields *bytes* — the explicit name avoids
//!   that footgun and is spelled the same on every layer.)
//! - **`exit_code`** — the exit code, with a missing code surfaced as an
//!   error. (On a [`ProcessResult`], [`code`](ProcessResult::code) is the
//!   plain `Option<i32>` accessor — `None` for a timeout/signal kill, never a
//!   `-1` sentinel.)
//! - **`probe`** — run a predicate and read its exit code as a `bool`: `0` →
//!   `true`, `1` → `false`, anything else is an error (`git diff --quiet`, …).
//!
//! # Features
//!
//! Every flag is *additive* and gates visibility only — the kill-on-drop tree
//! guarantee is unconditional in every configuration.
//!
//! - **`stats`** — resource measurement: `ProcessGroupStats`,
//!   `ProcessGroup::stats` (plus the `sample_stats` time-series sampler), the
//!   per-process `RunningProcess::cpu_time`/`peak_memory_bytes` diagnostics,
//!   and the `RunningProcess::profile` run summary. **Opt-in** for its
//!   specialized purpose (on Windows it calls the system `ProcessStatus`/PSAPI
//!   API — a link to an OS library, *not* an added crate dependency); enable with
//!   `features = ["stats"]`, or `limits`, which implies it. (The features that do
//!   pull an extra crate are `mock` → `mockall`, `tracing` → `tracing`, and
//!   `record` → `serde`/`serde_json`.)
//! - **`process-control`** *(default)* — tree control beyond contain+kill:
//!   `Signal` and `ProcessGroup::{signal, suspend, resume, members, adopt}`.
//! - **`limits`** — whole-tree resource caps: `ResourceLimits`, the
//!   `max_memory`/`max_processes`/`cpu_quota` builders on
//!   [`ProcessGroupOptions`], and `Error::ResourceLimit`. Implies `stats`.
//! - **`mock`** — the `mockall`-generated `testing::MockRunner` for
//!   consumers' tests. Its
//!   `expect_*` surface is generated by `mockall` and is **exempt from this
//!   crate's semver guarantees** — it tracks the `mockall` version (an
//!   implementation detail) rather than a frozen API. The first-class doubles
//!   ([`ScriptedRunner`](testing::ScriptedRunner) /
//!   [`RecordingRunner`](testing::RecordingRunner)) are the stable, recommended
//!   seam; reach for `mock` only if you specifically want expectation-style
//!   mocking.
//! - **`tracing`** — `tracing` events on the `processkit` target: spawn and
//!   exit (program/pid/mechanism), timeout and cancellation firing, group
//!   terminate/shutdown, retry attempts, supervisor restarts and storm
//!   pauses, and teardown anomalies (stdin-writer failures, pump overruns).
//!   Never logs argv or environment values.
//! - **`record`** — record/replay cassettes over the [`ProcessRunner`] seam:
//!   `RecordReplayRunner` records real `Invocation → ProcessResult` pairs to a
//!   JSON fixture once, then replays them hermetically — no subprocess in CI.
//!   Pulls in `serde` + `serde_json`.
//!
//! # Other languages
//!
//! Not on Rust? [`processkit-py`](https://pypi.org/project/processkit-py/) is a
//! Python wrapper (PyO3 bindings) over this crate's core, with an asyncio-facing
//! API. This crate remains the single source of truth for the containment/runner
//! logic underneath.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

mod batch;
mod buffer;
#[cfg(feature = "record")]
mod cassette;
mod client;
mod command;
// Test-only: compiles docs/*.md + README.md's fenced Rust blocks as doctests
// (see the module's own doc comment). Only under `--all-features` — the
// guides collectively use every optional feature, and the default /
// `--no-default-features` CI legs don't need a second copy of this check.
#[cfg(all(
    feature = "process-control",
    feature = "stats",
    feature = "limits",
    feature = "mock",
    feature = "tracing",
    feature = "record"
))]
mod doc_examples;
mod doubles;
mod error;
mod group;
#[cfg(feature = "limits")]
mod limits;
mod mechanism;
mod pipeline;
mod priority;
mod pump;
mod result;
mod retry;
mod runner;
mod running;
#[cfg(feature = "process-control")]
mod signal;
#[cfg(feature = "stats")]
mod stats;
mod stdin;
mod supervisor;
mod sys;

/// Clamp ceiling for `Instant + Duration` deadline math: a timeout, grace,
/// or `within` longer than this is treated as "effectively forever", so a
/// `Duration::MAX`-ish input can't overflow `Instant + Duration` and panic.
/// ~10 years — far beyond any real process deadline, with ample margin below
/// `Instant`'s representable range on every platform.
pub(crate) const MAX_DEADLINE: std::time::Duration =
    std::time::Duration::from_secs(10 * 365 * 24 * 60 * 60);

pub use batch::{output_all, output_all_bytes};
pub use buffer::{LineTerminator, OutputBufferPolicy, OverflowMode, StdioMode};
pub use client::{CliClient, IntoCommand};
pub use command::Command;
pub use error::{Error, Result};
pub use group::{ProcessGroup, ProcessGroupOptions};
#[cfg(feature = "limits")]
pub use limits::{LimitKind, LimitReason, ResourceLimits};
pub use mechanism::Mechanism;
pub use pipeline::Pipeline;
pub use priority::Priority;
// Fuzzing-only entry point for `fuzz/fuzz_targets/decode_pump_lines.rs` (see
// `src/pump.rs`). `cfg(fuzzing)` is set automatically by `cargo fuzz build`
// for the whole dependency graph, never in an ordinary build — so this
// never shows up in `cargo public-api`'s (`--all-features`, no `--cfg
// fuzzing`) surface, and thus never touches `public-api.txt`.
#[cfg(fuzzing)]
pub use pump::fuzz_decode_pump_lines;
pub use result::{Outcome, ProcessResult};
pub use retry::RetryPolicy;
pub use runner::{JobRunner, ProcessRunner, ProcessRunnerExt};
pub use running::{Finished, OutputEvent, OutputEvents, OutputLine, RunningProcess, StdoutLines};
#[cfg(feature = "process-control")]
pub use signal::Signal;
#[cfg(feature = "stats")]
pub use stats::{ProcessGroupStats, RunProfile, StatsSampler};
pub use stdin::{ProcessStdin, Stdin};
pub use supervisor::{GiveUpAttempt, RestartPolicy, StopReason, SupervisionOutcome, Supervisor};

use std::ffi::OsStr;

/// Run `program` with `args` inside a private job and return trimmed stdout, or
/// an [`Error`] on a non-zero exit / spawn failure / timeout. A thin shim over
/// [`Command`]; use the builder for a working directory, env, stdin, timeout, or
/// the full verb vocabulary.
///
/// # Errors
///
/// The same surface as [`Command::run`]: a launch failure ([`Error::NotFound`] /
/// [`Error::Spawn`] / [`Error::Unsupported`] / [`Error::Io`]), a non-accepted
/// exit ([`Error::Exit`]), [`Error::Signalled`], [`Error::Timeout`], or
/// [`Error::OutputTooLarge`] on a fail-loud truncation.
pub async fn run<I, S>(program: impl AsRef<OsStr>, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program).args(args).run().await
}

/// Run `program` with `args` inside a private job and capture the result
/// without erroring on a non-zero exit — for commands whose exit code is meaningful.
///
/// # Errors
///
/// The same surface as [`Command::output_string`]: a non-zero exit, a timeout,
/// and a signal-kill are *captured* in the returned [`ProcessResult`], not
/// raised; beyond a launch failure, only [`Error::Cancelled`],
/// [`Error::OutputTooLarge`], [`Error::Stdin`], and [`Error::Io`] surface.
pub async fn output_string<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
) -> Result<ProcessResult<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program).args(args).output_string().await
}

/// Wait for whichever of several running processes exits **first**, returning
/// its index in `processes` and its [`Outcome`] (matching
/// [`RunningProcess::wait`]).
///
/// The processes are only *borrowed*: the race is cancel-safe, so the losers —
/// and the winner, whose exit status tokio caches — remain fully usable
/// afterwards ([`wait`](RunningProcess::wait), another `wait_any`, …).
///
/// Two deliberate non-features:
///
/// - **No per-process [`timeout`](Command::timeout)** — the configured deadline
///   is armed by the consuming wait paths, not here. Bound the whole race with
///   [`tokio::time::timeout`] when a deadline is wanted.
/// - **No output pumping** — a contender that fills its stdout/stderr pipe
///   blocks and never exits. Drain chatty children first (e.g. via
///   [`stdout_lines`](RunningProcess::stdout_lines)) or race low-output ones.
///   Note the interplay: a [`tokio::time::timeout`] bounding the race fires,
///   but leaves such pipe-blocked contenders alive and still wedged — kill or
///   drain them afterwards; the timeout alone is not the mitigation.
/// - **No stdin management** — symmetrically, a contender started with
///   [`keep_stdin_open`](Command::keep_stdin_open) and blocked reading stdin
///   never reaches EOF, so it never exits. The race does **not** close its
///   stdin for it (that would break the "losers remain usable" guarantee):
///   take its writer via [`take_stdin`](RunningProcess::take_stdin)
///   (or don't keep stdin open) before racing it.
///
/// An empty `processes` slice is an error ([`Error::Io`] with
/// [`InvalidInput`](std::io::ErrorKind::InvalidInput)) rather than a future
/// that never resolves.
///
/// The first finisher's result carries the same errors as a bulk verb:
/// `Error::Cancelled` for a cancelled run, or [`Error::Stdin`] when its stdin
/// source failed (non-broken-pipe) on an otherwise-successful exit. A non-zero
/// exit or signal is *not* an error here — it is returned as its [`Outcome`].
///
/// # Errors
///
/// [`Error::Io`] with [`InvalidInput`](std::io::ErrorKind::InvalidInput) when
/// `processes` is empty. Otherwise the first finisher's error surfaces:
/// [`Error::Cancelled`] (a cancelled run), [`Error::Stdin`] (a non-broken-pipe
/// stdin-source failure on an otherwise-successful exit), or [`Error::Io`] (a
/// failed reap). A non-zero exit or signal is returned as an [`Outcome`], not an
/// error.
pub async fn wait_any(processes: &mut [&mut RunningProcess]) -> Result<(usize, Outcome)> {
    use std::future::Future;

    if processes.is_empty() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wait_any requires at least one process",
        )));
    }
    let mut waits: Vec<_> = processes
        .iter_mut()
        .map(|process| Box::pin(process.wait_exit()))
        .collect();
    // Hand-rolled race (avoids a `futures` dependency): first `Ready` wins, the
    // rest are dropped cancel-safe so the caller can still wait on them.
    std::future::poll_fn(move |cx| {
        for (idx, wait) in waits.iter_mut().enumerate() {
            if let std::task::Poll::Ready(result) = wait.as_mut().poll(cx) {
                return std::task::Poll::Ready(result.map(|outcome| (idx, outcome)));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// Wait for **all** of several running processes to exit, returning their
/// [`Outcome`]s in the same order as `processes`. The processes are only
/// *borrowed* and stay usable afterwards (the exit status tokio caches remains
/// re-readable).
///
/// Same two non-features as [`wait_any`]: **no per-process
/// [`timeout`](Command::timeout)** (bound the whole batch with
/// [`tokio::time::timeout`]) and **no output pumping** (a contender that fills
/// its stdout/stderr pipe blocks forever — drain chatty children first). Unlike
/// `wait_any`, an empty slice resolves immediately to an empty `Vec`: collecting
/// zero outcomes is well-defined, where racing none is not.
///
/// If a contender fails to reap (an OS I/O error), that `Err` is returned and
/// the remaining processes stay waitable (cancel-safe). A contender's
/// `Error::Cancelled` (cancelled run) or [`Error::Stdin`] (a non-broken-pipe
/// stdin-source failure on its otherwise-successful exit) likewise short-circuits
/// the join — like the bulk verbs, these surface as an `Err`, not an `Outcome`.
///
/// # Errors
///
/// A contender's [`Error::Io`] (a failed reap), [`Error::Cancelled`] (a
/// cancelled run), or [`Error::Stdin`] (a non-broken-pipe stdin-source failure
/// on its otherwise-successful exit) short-circuits the join; the remaining
/// processes stay waitable (cancel-safe). A non-zero exit or signal is returned
/// as an [`Outcome`], not an error. An empty slice resolves to an empty `Vec`.
///
/// # Panics
///
/// Does not panic on any caller input: the final collection step carries an
/// internal consistency assertion (every outcome slot is filled once all
/// contenders have exited, an invariant the join loop maintains). It is
/// documented only because that assertion is a hard `expect`.
pub async fn wait_all(processes: &mut [&mut RunningProcess]) -> Result<Vec<Outcome>> {
    use std::future::Future;
    use std::task::Poll;

    // A slot goes `None` once resolved so finishers aren't re-polled.
    let mut waits: Vec<_> = processes
        .iter_mut()
        .map(|process| Some(Box::pin(process.wait_exit())))
        .collect();
    // `None` outcome slot = not yet resolved; all are `Some` when `remaining ==
    // 0`, so the final `expect` cannot fire.
    let mut outcomes: Vec<Option<Outcome>> = vec![None; waits.len()];
    let mut remaining = waits.len();

    // Hand-rolled join (avoids a `futures` dependency): store each outcome at its
    // input-order index, resolve once all have exited. Cancel-safe like wait_any.
    std::future::poll_fn(move |cx| {
        for (idx, slot) in waits.iter_mut().enumerate() {
            if let Some(wait) = slot.as_mut()
                && let Poll::Ready(result) = wait.as_mut().poll(cx)
            {
                match result {
                    Ok(outcome) => {
                        outcomes[idx] = Some(outcome);
                        *slot = None;
                        remaining -= 1;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
        }
        if remaining == 0 {
            Poll::Ready(Ok(std::mem::take(&mut outcomes)
                .into_iter()
                .map(|o| o.expect("all slots filled when remaining == 0"))
                .collect()))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Test doubles for the [`ProcessRunner`] seam: a
/// [`ScriptedRunner`](testing::ScriptedRunner) that serves canned replies, a
/// [`RecordingRunner`](testing::RecordingRunner) that asserts on invocations,
/// the [`Invocation`](testing::Invocation) it captures, a
/// [`DryRunRunner`](testing::DryRunRunner) that renders and echoes commands
/// without spawning them, and (behind features) record/replay cassettes and a
/// `mockall` mock.
pub mod testing {
    pub use crate::doubles::{DryRunRunner, Invocation, RecordingRunner, Reply, ScriptedRunner};

    /// Record/replay cassette runner (enabled by the `record` feature).
    #[cfg(feature = "record")]
    pub use crate::cassette::RecordReplayRunner;

    /// The `mockall`-generated mock of [`ProcessRunner`](crate::ProcessRunner)
    /// (enabled by the `mock` feature), re-exported under a friendlier name.
    ///
    /// **Semver-exempt:** the `expect_*` builder surface is generated by
    /// `mockall` and its exact shape (including the opaque expectation types) is
    /// an implementation detail that follows the `mockall` dependency, **not**
    /// part of this crate's frozen public API. For a stable double, prefer
    /// [`ScriptedRunner`] (canned replies) or [`RecordingRunner`] (input
    /// assertions).
    #[cfg(feature = "mock")]
    pub use crate::runner::MockProcessRunner as MockRunner;
}

/// Re-exports of small vocabulary types from the crate's `0.x` dependencies,
/// kept out of the crate root so `use processkit::*` doesn't pull them in (and
/// so a future `0.x` major bump of either dependency stays contained to this
/// module rather than the whole crate surface).
///
/// ```
/// use processkit::prelude::StreamExt;
/// ```
pub mod prelude {
    /// Re-exported from [`encoding_rs`] so a caller can name the type passed to
    /// [`Command::stdout_encoding`](crate::Command::stdout_encoding) /
    /// [`stderr_encoding`](crate::Command::stderr_encoding) without a direct
    /// dependency on `encoding_rs`.
    pub use encoding_rs::Encoding;

    /// Re-exported from [`tokio_stream`] so callers can `.next()` the stdout/event
    /// streams (e.g. [`StdoutLines`](crate::StdoutLines) /
    /// [`OutputEvents`](crate::OutputEvents)) without a direct `tokio-stream`
    /// dependency. Collides with `futures::StreamExt` under a glob import — import
    /// by path (`processkit::prelude::StreamExt`) if both traits are in scope.
    pub use tokio_stream::StreamExt;
}

/// Re-exported so callers can `use processkit::CancellationToken;` without a
/// direct `tokio-util` dependency. See [`Command::cancel_on`].
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::Outcome;

    /// The deadline-clamp ceiling must be small enough that
    /// `Instant + MAX_DEADLINE` cannot overflow, and a `Duration::MAX` input must
    /// clamp down to it — so `Instant::now() + within.min(MAX_DEADLINE)` is
    /// panic-free for any timeout/grace, however absurd.
    #[test]
    fn max_deadline_clamp_prevents_instant_overflow() {
        use std::time::{Duration, Instant};
        let _ = Instant::now() + super::MAX_DEADLINE; // must not panic
        assert_eq!(Duration::MAX.min(super::MAX_DEADLINE), super::MAX_DEADLINE);
    }

    // Regression: a bulk verb on the winner after a late cancel must not
    // reclassify its natural exit as Err(Cancelled) (wait_exit must snapshot
    // cancel_at_exit rather than re-evaluate the now-cancelled token).
    #[tokio::test]
    async fn wait_any_winner_natural_exit_preserved_after_late_cancel() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("test-prog").cancel_on(token.clone()))
            .await
            .expect("start scripted process");

        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0), "process exited naturally");

        token.cancel(); // after the natural exit
        let result = process.wait().await.expect("wait after wait_any");
        assert_eq!(
            result,
            Outcome::Exited(0),
            "natural exit must not be converted to Err(Cancelled)"
        );
    }

    // Regression: the same snapshot hazard via a *second* wait_any (the
    // "race them, keep watching the rest" pattern) rather than a bulk verb.
    #[tokio::test]
    async fn wait_any_winner_preserved_after_late_cancel_and_second_wait_any() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("test-prog").cancel_on(token.clone()))
            .await
            .expect("start scripted process");

        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("first wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0));

        token.cancel();

        let (idx2, outcome2) = super::wait_any(&mut [&mut process])
            .await
            .expect("second wait_any must not error after a late cancel");
        assert_eq!(idx2, 0);
        assert_eq!(
            outcome2,
            Outcome::Exited(0),
            "repeat wait_any must preserve the natural exit, not reclassify as Cancelled"
        );
    }

    // Regression for wait_all: a late cancel then a re-join must not error the
    // whole batch (it short-circuits on first Err, discarding every outcome).
    #[tokio::test]
    async fn wait_all_winners_preserved_after_late_cancel_and_re_wait() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut a = runner
            .start(&crate::Command::new("a").cancel_on(token.clone()))
            .await
            .expect("start a");
        let mut b = runner
            .start(&crate::Command::new("b").cancel_on(token.clone()))
            .await
            .expect("start b");

        let outcomes = super::wait_all(&mut [&mut a, &mut b])
            .await
            .expect("first wait_all");
        assert_eq!(outcomes, vec![Outcome::Exited(0), Outcome::Exited(0)]);

        token.cancel();

        let outcomes2 = super::wait_all(&mut [&mut a, &mut b])
            .await
            .expect("re-join after a late cancel must not error");
        assert_eq!(
            outcomes2,
            vec![Outcome::Exited(0), Outcome::Exited(0)],
            "repeat wait_all must preserve natural exits, not reclassify as Cancelled"
        );
    }

    #[tokio::test]
    async fn wait_returns_outcome() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let process = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let outcome = process.wait().await.expect("wait");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    #[tokio::test]
    async fn wait_any_returns_outcome() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0));
    }

    #[tokio::test]
    async fn wait_all_returns_outcomes() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut a = runner
            .start(&crate::Command::new("a"))
            .await
            .expect("start a");
        let mut b = runner
            .start(&crate::Command::new("b"))
            .await
            .expect("start b");
        let outcomes = super::wait_all(&mut [&mut a, &mut b])
            .await
            .expect("wait_all");
        assert_eq!(outcomes, vec![Outcome::Exited(0), Outcome::Exited(0)]);
    }

    #[tokio::test]
    async fn wait_all_collects_a_mix_of_outcomes_in_order() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        // Distinct terminal states must each surface as their own Outcome, in order.
        let runner = ScriptedRunner::new()
            .on(["p", "clean"], Reply::ok(""))
            .on(["p", "fail"], Reply::fail(3, "boom"))
            .on(["p", "killed"], Reply::signalled(Some(9)));
        let mut a = runner
            .start(&crate::Command::new("p").arg("clean"))
            .await
            .expect("start a");
        let mut b = runner
            .start(&crate::Command::new("p").arg("fail"))
            .await
            .expect("start b");
        let mut c = runner
            .start(&crate::Command::new("p").arg("killed"))
            .await
            .expect("start c");
        let outcomes = super::wait_all(&mut [&mut a, &mut b, &mut c])
            .await
            .expect("wait_all");
        assert_eq!(
            outcomes,
            vec![
                Outcome::Exited(0),
                Outcome::Exited(3),
                Outcome::Signalled(Some(9)),
            ]
        );
    }

    // Regression: a run cancelled before exit must surface as Err(Cancelled)
    // from wait_any, not Ok(Signalled(None)).
    #[tokio::test]
    async fn wait_any_cancelled_run_surfaces_as_err_cancelled() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog").cancel_on(token.clone()))
            .await
            .expect("start");

        token.cancel(); // before wait_any
        let err = super::wait_any(&mut [&mut process])
            .await
            .expect_err("cancelled run must error");
        assert!(
            matches!(err, crate::Error::Cancelled { .. }),
            "expected Error::Cancelled, got {err:?}"
        );
    }

    // Symmetry: a genuine cancellation must stay sticky across a re-wait, not
    // just a clean exit — the guard must not make cancellation non-sticky.
    #[tokio::test]
    async fn wait_any_genuine_cancel_stays_cancelled_on_re_wait() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog").cancel_on(token.clone()))
            .await
            .expect("start");

        token.cancel(); // genuine cancel before the race

        let err = super::wait_any(&mut [&mut process])
            .await
            .expect_err("first wait_any: cancelled run errors");
        assert!(matches!(err, crate::Error::Cancelled { .. }), "got {err:?}");

        let err2 = super::wait_any(&mut [&mut process])
            .await
            .expect_err("re-wait must stay cancelled, not flip to Ok");
        assert!(
            matches!(err2, crate::Error::Cancelled { .. }),
            "repeat wait_any must preserve the cancellation, got {err2:?}"
        );
    }

    #[tokio::test]
    async fn wait_any_on_an_empty_slice_errors_instead_of_pending() {
        let err = super::wait_any(&mut [])
            .await
            .expect_err("an empty race must error, not pend forever");
        match err {
            crate::Error::Io(source) => {
                assert_eq!(source.kind(), std::io::ErrorKind::InvalidInput);
            }
            other => panic!("expected Error::Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_all_on_an_empty_slice_is_an_empty_vec() {
        // Unlike `wait_any`, joining zero processes is well-defined: it
        // resolves immediately to an empty `Vec`, not an error or a hang.
        let outcomes = super::wait_all(&mut [])
            .await
            .expect("an empty join resolves cleanly");
        assert!(outcomes.is_empty());
    }

    // ── output-capture integrity ─────────────────────────────────────────────

    // A bare finish (no prior stdout_lines) drains untaken stdout through the
    // internal discard sink — the caller never asked to capture it — so it does
    // NOT enforce fail_loud on that output, matching wait() (see below).
    #[tokio::test]
    async fn bare_finish_does_not_enforce_fail_loud_on_untaken_stdout() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let run = runner
            .start(&crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2)))
            .await
            .expect("start");
        let finished = run
            .finish()
            .await
            .expect("a bare finish discards untaken stdout, so fail_loud does not fire");
        assert_eq!(finished.outcome, Outcome::Exited(0));
    }

    // wait discards output, so it must never fire fail_loud (retain-nothing sink).
    #[tokio::test]
    async fn wait_does_not_error_on_fail_loud() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let run = runner
            .start(&crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2)))
            .await
            .expect("start");
        let outcome = run
            .wait()
            .await
            .expect("wait must succeed despite fail_loud");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    // output_string after stdout_lines must see the lines the streaming pump
    // wrote, not silently return empty output.
    #[tokio::test]
    async fn output_string_after_stdout_lines_captures_buffered_output() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["x", "y", "z"]));
        let mut run = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let _ = run.stdout_lines().unwrap(); // take the pipe, start the streaming pump
        let result = run.output_string().await.expect("output_string");
        assert!(
            !result.stdout().is_empty(),
            "output_string after stdout_lines must not return empty; got {:?}",
            result.stdout()
        );
    }

    // A second stdout_lines call is a loud error (stdout streams once), not a
    // silent empty stream — and the first pump's overflow is still seen by finish.
    #[tokio::test]
    async fn second_stdout_lines_errors_and_first_overflow_is_preserved() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::prelude::StreamExt;
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let cmd = crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2));
        let mut run = runner.start(&cmd).await.expect("start");
        let mut first = run.stdout_lines().expect("first stdout_lines");
        while first.next().await.is_some() {}
        let err = run
            .stdout_lines()
            .expect_err("a second stdout_lines must be a loud error");
        assert!(matches!(err, crate::Error::Io(_)), "got {err:?}");
        let err = run
            .finish()
            .await
            .expect_err("overflow from first pump must still be visible");
        assert!(
            matches!(err, crate::Error::OutputTooLarge { .. }),
            "expected OutputTooLarge, got {err:?}"
        );
    }

    // A second output_events call is likewise a loud error.
    #[tokio::test]
    async fn second_output_events_is_a_loud_error() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::prelude::StreamExt;
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::fail(1, "stderr-only"));
        let mut run = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let mut first = run.output_events().expect("first output_events");
        while first.next().await.is_some() {}
        let err = run
            .output_events()
            .expect_err("a second output_events must be a loud error");
        assert!(matches!(err, crate::Error::Io(_)), "got {err:?}");
        let _ = run.finish().await;
    }
}
