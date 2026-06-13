// On docs.rs (which builds with `--cfg docsrs`, see `[package.metadata.docs.rs]`)
// derive the "Available on crate feature `X`" badges from the existing `#[cfg]`
// gates. `doc_cfg` (which absorbed `doc_auto_cfg` in 1.92) is nightly-only, so
// it is gated behind `docsrs` — stable/CI `cargo doc` ignores it.
#![cfg_attr(docsrs, feature(doc_cfg))]
// Enforce that every public item carries docs — the crate's public surface is
// fully documented today, and this keeps it that way. Lib-scoped (examples and
// tests are exempt); CI's `-D warnings` promotes it to a hard error.
#![warn(missing_docs)]

//! `processkit` — child-process management for Rust.
//!
//! Two layers:
//!
//! - **[`ProcessGroup`]** — a kill-on-drop container for a process *tree*. Every
//!   child spawned into the group, and everything those children spawn, dies
//!   with the group, so an exiting or panicking owner never leaks subprocesses.
//!   Containment is a Windows [Job Object], a Linux [cgroup v2] (with a POSIX
//!   process-group fallback), or a POSIX process group on macOS/BSD —
//!   observable via [`Mechanism`]. The whole tree can be
//!   signalled (`ProcessGroup::signal`, see `Signal`), paused/resumed
//!   (`ProcessGroup::suspend` / `ProcessGroup::resume`), and inspected
//!   (`ProcessGroup::members`); [`wait_any`] races several running processes
//!   and reports the first to exit.
//! - **runner** — async run-and-capture built on the group. Describe a run with
//!   [`Command`], then drive it to completion ([`Command::output_string`],
//!   [`Command::run`], …) or [`start`](Command::start) it for streaming and
//!   interactive I/O. The [`ProcessRunner`] trait runs commands to completion
//!   and is the mock seam (see [`ScriptedRunner`]). A
//!   [`Supervisor`] keeps a command *alive* — restarting it per policy with
//!   backoff — where [`Command::retry`] merely replays one run to success.
//!   Readiness probes ([`RunningProcess::wait_for_line`] /
//!   [`wait_for_port`](RunningProcess::wait_for_port) /
//!   [`wait_for`](RunningProcess::wait_for)) wait until a started child is
//!   actually *ready* instead of sleeping. A [`Pipeline`]
//!   ([`Command::pipe`]) chains commands stdout→stdin without a shell — one
//!   shared group, pipefail outcome. Spawn-time sandboxing knobs:
//!   [`Command::inherit_env`] (env allow-list), [`Command::uid`] /
//!   [`Command::gid`] (Unix privilege drop), [`Command::setsid`],
//!   [`Command::create_no_window`].
//!
//! Async throughout (tokio). Errors are the structured [`Error`]; a non-zero
//! exit is reported in [`ProcessResult`], not raised, until you call
//! [`ProcessResult::ensure_success`].
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
//! - **`run`** — require a zero exit and return stdout as a `String`, trailing
//!   whitespace trimmed (`trim_end`: the final newline is noise, but leading
//!   whitespace can be significant). **`run_unit`** — the same, discarding the
//!   output.
//! - **`output`** — return the full [`ProcessResult`]; a non-zero exit is
//!   *not* an error here. (`Command` splits the verb by payload:
//!   `output_string` / `output_bytes`.)
//! - **`exit_code`** — the exit code, with a missing code surfaced as an
//!   error. (On a [`ProcessResult`], [`code`](ProcessResult::code) is the
//!   plain `Option<i32>` accessor — `None` for a timeout/signal kill, never a
//!   `-1` sentinel.)
//! - **`probe`** — run a predicate and read its exit code as a `bool`: `0` →
//!   `true`, `1` → `false`, anything else is an error (`git diff --quiet`, …).
//!
//! ```no_run
//! # async fn demo() -> processkit::Result<()> {
//! use processkit::Command;
//!
//! // Capture output; a non-zero exit does not error on its own.
//! let result = Command::new("git").args(["rev-parse", "HEAD"]).output_string().await?;
//! println!("HEAD is {}", result.stdout().trim());
//!
//! // Or require success and get trimmed stdout directly.
//! let version = Command::new("cargo").arg("--version").run().await?;
//! # let _ = version;
//! # Ok(())
//! # }
//! ```
//!
//! # Recipes
//!
//! ```no_run
//! # use std::time::Duration;
//! # async fn demo() -> processkit::Result<()> {
//! use processkit::{Command, Error};
//!
//! // Exit code *is* the answer (0 = yes, 1 = no; anything else errors):
//! let clean = Command::new("git").args(["diff", "--quiet"]).probe().await?;
//!
//! // Retry a transient failure (replays the command; classifier inspects the error):
//! let fetched = Command::new("git")
//!     .args(["fetch", "--quiet"])
//!     .timeout(Duration::from_secs(10))
//!     .retry(3, Duration::from_millis(200), |e| {
//!         matches!(e, Error::Timeout { .. })
//!             || e.diagnostic().is_some_and(|m| m.contains("Could not resolve host"))
//!     })
//!     .run()
//!     .await;
//!
//! // A friendly failure message — stderr, falling back to stdout (git writes
//! // `CONFLICT …` / `nothing to commit` there):
//! if let Err(e) = Command::new("git").args(["merge", "topic"]).run().await {
//!     eprintln!("merge failed: {}", e.diagnostic().unwrap_or("(no output)"));
//! }
//!
//! // Set an env var once for every command (typed CLI wrapper):
//! use processkit::CliClient;
//! let git = CliClient::new("git").default_env("GIT_TERMINAL_PROMPT", "0");
//! let _ = git.run(git.command(["status", "--porcelain"])).await?;
//! # let _ = (clean, fetched);
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! Every flag is *additive* and gates visibility only — the kill-on-drop tree
//! guarantee is unconditional in every configuration.
//!
//! - **`stats`** *(default)* — resource measurement: `ProcessGroupStats`,
//!   `ProcessGroup::stats` (plus the `sample_stats` time-series sampler), the
//!   per-process `RunningProcess::cpu_time`/`peak_memory_bytes` diagnostics,
//!   and the `RunningProcess::profile` run summary. Disable
//!   (`default-features = false`) to compile the accounting code out.
//! - **`process-control`** *(default)* — tree control beyond contain+kill:
//!   `Signal` and `ProcessGroup::{signal, suspend, resume, members, adopt}`.
//! - **`limits`** — whole-tree resource caps: `ResourceLimits`, the
//!   `memory_max`/`max_processes`/`cpu_quota` builders on
//!   [`ProcessGroupOptions`], and `Error::ResourceLimit`. Implies `stats`.
//! - **`mock`** — the `mockall`-generated `MockRunner` for consumers' tests.
//! - **`tracing`** — `tracing` events on the `processkit` target: spawn and
//!   exit (program/pid/mechanism), timeout and cancellation firing, group
//!   terminate/shutdown, retry attempts, supervisor restarts and storm
//!   pauses, and teardown anomalies (stdin-writer failures, pump overruns).
//!   Never logs argv or environment values.
//! - **`cancellation`** — first-class run cancellation:
//!   `Command::cancel_on` ties a run to a `CancellationToken`; cancelling it
//!   kills the tree and every consuming path resolves to `Error::Cancelled`.
//!   Re-exports `CancellationToken` (from `tokio-util`).
//! - **`record`** — record/replay cassettes over the [`ProcessRunner`] seam:
//!   `RecordReplayRunner` records real `Invocation → ProcessResult` pairs to a
//!   JSON fixture once, then replays them hermetically — no subprocess in CI.
//!   Pulls in `serde` + `serde_json`.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

mod batch;
mod buffer;
#[cfg(feature = "record")]
mod cassette;
mod client;
mod command;
mod doubles;
mod error;
mod group;
#[cfg(feature = "limits")]
mod limits;
mod mechanism;
mod pipeline;
mod pump;
mod result;
mod runner;
mod running;
#[cfg(feature = "process-control")]
mod signal;
#[cfg(feature = "stats")]
mod stats;
mod stdin;
mod supervisor;
mod sys;

/// Clamp ceiling for `Instant + Duration` deadline math (Э15): a timeout, grace,
/// or `within` longer than this is treated as "effectively forever", so a
/// `Duration::MAX`-ish input can't overflow `Instant + Duration` and panic.
/// ~10 years — far beyond any real process deadline, with ample margin below
/// `Instant`'s representable range on every platform.
pub(crate) const MAX_DEADLINE: std::time::Duration =
    std::time::Duration::from_secs(10 * 365 * 24 * 60 * 60);

pub use batch::output_all;
pub use buffer::{OutputBufferPolicy, OverflowMode, StdioMode};
#[cfg(feature = "record")]
pub use cassette::RecordReplayRunner;
pub use client::CliClient;
pub use command::Command;
pub use doubles::{Invocation, RecordingRunner, Reply, ScriptedRunner};
pub use encoding_rs::Encoding;
pub use error::{Error, Result};
pub use group::{ProcessGroup, ProcessGroupOptions};
#[cfg(feature = "limits")]
pub use limits::ResourceLimits;
pub use mechanism::Mechanism;
pub use pipeline::Pipeline;
pub use result::{Outcome, ProcessResult};
pub use runner::{JobRunner, ProcessRunner, ProcessRunnerExt};
pub use running::{OutputEvent, OutputEvents, RunningProcess, StdoutLines, StreamedFinish};
#[cfg(feature = "process-control")]
pub use signal::Signal;
#[cfg(feature = "stats")]
pub use stats::{ProcessGroupStats, RunProfile, StatsSampler};
pub use stdin::{ProcessStdin, Stdin};
pub use supervisor::{RestartPolicy, StopReason, SupervisionOutcome, Supervisor};
// Re-exported so callers can `use processkit::StreamExt;` to consume
// [`RunningProcess::stdout_lines`]'s [`StdoutLines`] stream (`.next().await`,
// combinators) without depending on `tokio-stream` directly.
pub use tokio_stream::StreamExt;
// `cli_client!` is exported at the crate root via `#[macro_export]`.

use std::ffi::OsStr;

/// Run `program` with `args` inside a private job and return trimmed stdout, or
/// an [`Error`] on a non-zero exit / spawn failure / timeout. A thin shim over
/// [`Command`]; use the builder for a working directory, env, stdin, or timeout.
pub async fn run<I, S>(program: impl AsRef<OsStr>, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program).args(args).run().await
}

/// Run `program` with `args` inside a private job and capture the result
/// without erroring on a non-zero exit — for commands whose exit code is meaningful.
pub async fn output<I, S>(program: impl AsRef<OsStr>, args: I) -> Result<ProcessResult<String>>
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
/// afterwards ([`wait`](RunningProcess::wait), another `wait_any`, …). This is
/// the natural primitive for supervising several long-lived children: race
/// them, handle the one that finished, keep watching the rest.
///
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::{Command, ProcessGroup, wait_any};
///
/// let group = ProcessGroup::new()?;
/// let mut a = group.start(&Command::new("server-a")).await?;
/// let mut b = group.start(&Command::new("server-b")).await?;
/// let (idx, outcome) = wait_any(&mut [&mut a, &mut b]).await?;
/// println!("contender #{idx} exited first with {outcome:?}");
/// # Ok(())
/// # }
/// ```
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
///
/// An empty `processes` slice is an error ([`Error::Io`] with
/// [`InvalidInput`](std::io::ErrorKind::InvalidInput)) rather than a future
/// that never resolves.
///
/// The first finisher's result carries the same errors as a bulk verb:
/// `Error::Cancelled` for a cancelled run, or [`Error::Stdin`] when its stdin
/// source failed (non-broken-pipe) on an otherwise-successful exit. A non-zero
/// exit or signal is *not* an error here — it is returned as its [`Outcome`].
pub async fn wait_any(processes: &mut [&mut RunningProcess]) -> Result<(usize, Outcome)> {
    use std::future::Future;

    if processes.is_empty() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wait_any requires at least one process",
        )));
    }
    // One future per contender; `iter_mut` hands out disjoint `&mut` borrows.
    let mut waits: Vec<_> = processes
        .iter_mut()
        .map(|process| Box::pin(process.wait_exit()))
        .collect();
    // Hand-rolled race (no `futures` dependency): poll every contender; the
    // first `Ready` wins, the rest are dropped — cancel-safe, so they stay
    // waitable by the caller.
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
/// [`Outcome`]s in the same order as `processes`.
///
/// The companion to [`wait_any`]: where `wait_any` races and returns the first
/// finisher, `wait_all` drives every contender to completion concurrently and
/// collects them. The processes are only *borrowed* and stay usable afterwards
/// (the exit status tokio caches remains re-readable). This is the natural
/// primitive for fanning a fixed set of children out and joining on the lot.
///
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::{Command, ProcessGroup, wait_all};
///
/// let group = ProcessGroup::new()?;
/// let mut a = group.start(&Command::new("worker-a")).await?;
/// let mut b = group.start(&Command::new("worker-b")).await?;
/// let outcomes = wait_all(&mut [&mut a, &mut b]).await?;
/// assert_eq!(outcomes.len(), 2); // one entry per process, in input order
/// # Ok(())
/// # }
/// ```
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
pub async fn wait_all(processes: &mut [&mut RunningProcess]) -> Result<Vec<Outcome>> {
    use std::future::Future;
    use std::task::Poll;

    // One future per contender; `iter_mut` hands out disjoint `&mut` borrows.
    // A slot goes `None` once it has resolved, so finishers aren't re-polled.
    let mut waits: Vec<_> = processes
        .iter_mut()
        .map(|process| Some(Box::pin(process.wait_exit())))
        .collect();
    // `None` is the "not yet resolved" sentinel; replaced by `Some(Outcome)` on
    // completion. All slots are `Some` when `remaining == 0`, so the final
    // `unwrap` below is always safe.
    let mut outcomes: Vec<Option<Outcome>> = vec![None; waits.len()];
    let mut remaining = waits.len();

    // Hand-rolled join (no `futures` dependency): poll every unfinished
    // contender each wake, store its outcome at the input-order index, and
    // resolve once all have exited. Cancel-safe, mirroring `wait_any`.
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

/// The `mockall`-generated mock of [`ProcessRunner`] (enabled by the `mock`
/// feature), re-exported under a friendlier name.
#[cfg(feature = "mock")]
pub use runner::MockProcessRunner as MockRunner;

/// Re-exported (under the `cancellation` feature) so callers can
/// `use processkit::CancellationToken;` without a direct `tokio-util`
/// dependency. See [`Command::cancel_on`].
#[cfg(feature = "cancellation")]
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::Outcome;

    /// Э15: the deadline-clamp ceiling must be small enough that
    /// `Instant + MAX_DEADLINE` cannot overflow, and a `Duration::MAX` input must
    /// clamp down to it — so `Instant::now() + within.min(MAX_DEADLINE)` is
    /// panic-free for any timeout/grace, however absurd.
    #[test]
    fn max_deadline_clamp_prevents_instant_overflow() {
        use std::time::{Duration, Instant};
        let _ = Instant::now() + super::MAX_DEADLINE; // must not panic
        assert_eq!(Duration::MAX.min(super::MAX_DEADLINE), super::MAX_DEADLINE);
    }

    // Regression: wait_exit (used by wait_any/wait_all) did not snapshot
    // cancel_at_exit, so a .wait()/.output_string()/etc. on the winner after
    // wait_any returned — with the token now cancelled — would re-run
    // drive_to_exit_inner whose biased cancel arm fires (token already cancelled),
    // converting a natural exit to Err(Cancelled).
    #[cfg(feature = "cancellation")]
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

        // Race the single process — scripted Reply::ok exits immediately (code 0).
        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0), "process exited naturally");

        // Cancel the token AFTER the natural exit.
        token.cancel();

        // A bulk verb on the winner must return the natural exit, not Err(Cancelled).
        let result = process.wait().await.expect("wait after wait_any");
        assert_eq!(
            result,
            Outcome::Exited(0),
            "natural exit must not be converted to Err(Cancelled)"
        );
    }

    // Б2 regression: the same snapshot hazard via a *second* wait_any (not a
    // bulk verb). The existing test above covers wait_exit -> wait() (the guarded
    // drive_to_exit path); this covers wait_exit -> wait_exit, the documented
    // "race them, keep watching the rest" pattern, where wait_exit re-snapshotted
    // cancel_at_exit unconditionally and flipped a natural exit to Err(Cancelled).
    #[cfg(feature = "cancellation")]
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

    // Б2 regression for wait_all: a late cancel followed by a re-join must not
    // make the whole batch error out (wait_all short-circuits on the first Err,
    // so a spurious Cancelled would discard every other contender's outcome too).
    #[cfg(feature = "cancellation")]
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
        // Three distinct terminal states must each surface as their own Outcome,
        // in input order — not collapse to a single shape.
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

    // Regression: wait_exit now calls checked_outcome, so a run whose
    // cancel token was fired before exit snapshots cancel_at_exit=Some(true)
    // and wait_any correctly raises Err(Cancelled) instead of Ok(Signalled(None)).
    #[cfg(feature = "cancellation")]
    #[tokio::test]
    async fn wait_any_cancelled_run_surfaces_as_err_cancelled() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        // Reply::ok exits immediately so backend_wait() returns right away and
        // the cancel_at_exit snapshot captures the already-cancelled token.
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog").cancel_on(token.clone()))
            .await
            .expect("start");

        // Cancel before wait_any so the snapshot sees is_cancelled()=true.
        token.cancel();

        let err = super::wait_any(&mut [&mut process])
            .await
            .expect_err("cancelled run must error");
        assert!(
            matches!(err, crate::Error::Cancelled { .. }),
            "expected Error::Cancelled, got {err:?}"
        );
    }

    // Б2 symmetry: the snapshot-preserving guard must keep a genuine
    // cancellation *sticky* across a re-wait, not just preserve a clean exit.
    // A second wait_any after a genuinely cancelled run must STILL be
    // Err(Cancelled) (the guard preserves Some(true) exactly as it preserves
    // Some(false)) — the fix must not make cancellation non-sticky on re-wait.
    #[cfg(feature = "cancellation")]
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

        token.cancel(); // genuine cancel before the race -> snapshot Some(true)

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

    // ── Phase C: output-capture integrity ────────────────────────────────────

    // B5: finish_streamed without a prior stdout_lines call must route the
    // untaken stdout through the policy-aware pump, not read_to_end into an
    // unbounded Vec.  A fail_loud ceiling must be enforced.
    #[tokio::test]
    async fn finish_streamed_on_untaken_stdout_respects_fail_loud() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let run = runner
            .start(&crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2)))
            .await
            .expect("start");
        let err = run
            .finish_streamed()
            .await
            .expect_err("fail_loud(2) with 3 lines must error");
        assert!(
            matches!(err, crate::Error::OutputTooLarge { .. }),
            "expected OutputTooLarge, got {err:?}"
        );
    }

    // B5 companion: finish_events without a prior output_events call must also
    // route untaken stdout through the policy-aware pump.
    #[tokio::test]
    async fn finish_events_on_untaken_stdout_respects_fail_loud() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let run = runner
            .start(&crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2)))
            .await
            .expect("start");
        let err = run
            .finish_events()
            .await
            .expect_err("fail_loud(2) with 3 lines must error");
        assert!(
            matches!(err, crate::Error::OutputTooLarge { .. }),
            "expected OutputTooLarge, got {err:?}"
        );
    }

    // B9: wait must not accumulate lines or fire fail_loud — the discard path
    // uses a retain-nothing sink that never trips the overflow ceiling.
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
        // wait discards output — fail_loud must not fire.
        let outcome = run
            .wait()
            .await
            .expect("wait must succeed despite fail_loud");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    // B10: output_string called after stdout_lines must see the lines the
    // streaming pump wrote rather than silently returning empty output.
    #[tokio::test]
    async fn output_string_after_stdout_lines_captures_buffered_output() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["x", "y", "z"]));
        let mut run = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let _ = run.stdout_lines(); // take the pipe, start the streaming pump
        // output_string must join the streaming pump and drain its sink.
        let result = run.output_string().await.expect("output_string");
        assert!(
            !result.stdout().is_empty(),
            "output_string after stdout_lines must not return empty; got {:?}",
            result.stdout()
        );
    }

    // L1: a second stdout_lines call must not overwrite self.stdout_sink with
    // the new call's immediately-closed empty sink — the first pump's overflow
    // flag and line count must still be visible to finish_streamed.
    #[tokio::test]
    async fn repeat_stdout_lines_preserves_overflow_flag() {
        use crate::StreamExt;
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let cmd = crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2));
        let mut run = runner.start(&cmd).await.expect("start");
        // Drain the first stream to completion.
        let mut first = run.stdout_lines();
        while first.next().await.is_some() {}
        // Second call — pipe already gone; returns immediately-closed stream.
        let _ = run.stdout_lines();
        // finish_streamed must observe the first pump's overflow, not the
        // second call's fresh empty closed sink.
        let err = run
            .finish_streamed()
            .await
            .expect_err("overflow from first pump must still be visible");
        assert!(
            matches!(err, crate::Error::OutputTooLarge { .. }),
            "expected OutputTooLarge, got {err:?}"
        );
    }

    // L2: a second output_events call must get its own immediately-closed
    // stderr sink, not share the first call's SharedLines where notify_one
    // on close could leave one consumer parked forever.
    #[tokio::test]
    async fn second_output_events_yields_no_events() {
        use crate::StreamExt;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::fail(1, "stderr-only"));
        let mut run = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        // First call: drain both streams.
        let mut first = run.output_events();
        while first.next().await.is_some() {}
        // Second call: both sinks are immediately closed.
        let mut second = run.output_events();
        let mut events: usize = 0;
        while second.next().await.is_some() {
            events += 1;
        }
        assert_eq!(events, 0, "second output_events must yield no events");
        let _ = run.finish_events().await;
    }
}
