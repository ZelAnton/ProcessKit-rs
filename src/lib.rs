//! `processkit` — child-process management for Rust.
//!
//! Two layers:
//!
//! - **[`ProcessGroup`]** — a kill-on-drop container for a process *tree*. Every
//!   child spawned into the group, and everything those children spawn, dies
//!   with the group, so an exiting or panicking owner never leaks subprocesses.
//!   Containment is a Windows [Job Object], a Linux [cgroup v2] (with a POSIX
//!   process-group fallback), a POSIX process group on macOS/BSD, or nothing on
//!   other targets — observable via [`Mechanism`]. The whole tree can be
//!   signalled ([`ProcessGroup::signal`], see [`Signal`]), paused/resumed
//!   ([`ProcessGroup::suspend`] / [`ProcessGroup::resume`]), and inspected
//!   ([`ProcessGroup::members`]); [`wait_any`] races several running processes
//!   and reports the first to exit.
//! - **runner** — async run-and-capture built on the group. Describe a run with
//!   [`Command`], then drive it to completion ([`Command::output_string`],
//!   [`Command::run`], …) or start it via a [`ProcessRunner`] for streaming or a
//!   shared group. The trait is the mock seam (see [`ScriptedRunner`]). A
//!   [`Supervisor`] keeps a command *alive* — restarting it per policy with
//!   backoff — where [`Command::retry`] merely replays one run to success.
//!   Readiness probes ([`RunningProcess::wait_for_line`] /
//!   [`wait_for_port`](RunningProcess::wait_for_port) /
//!   [`wait_for`](RunningProcess::wait_for)) wait until a started child is
//!   actually *ready* instead of sleeping.
//!
//! Async throughout (tokio). Errors are the structured [`Error`]; a non-zero
//! exit is reported in [`ProcessResult`], not raised, until you call
//! [`ProcessResult::ensure_success`].
//!
//! **Run vocabulary** — the same verb means the same thing at every layer
//! ([`Command`], [`ProcessRunner`]/[`ProcessRunnerExt`], [`CliClient`]):
//!
//! - **`run` / `text`** — require a zero exit and return stdout as a `String`,
//!   trailing whitespace trimmed (`trim_end`: the final newline is noise, but
//!   leading whitespace can be significant).
//! - **`output` / `capture` / `output_string` / `output_bytes`** — return the
//!   full [`ProcessResult`]; a non-zero exit is *not* an error here.
//! - **`exit_code` / `code`** — the exit code. On a [`ProcessResult`],
//!   [`code`](ProcessResult::code) is `Option<i32>` (`None` for a run killed by
//!   its timeout or a signal — there is no `-1` sentinel); the `exit_code`
//!   helpers instead surface a missing code as an error.
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
//! let _ = git.text(git.command(["status", "--porcelain"])).await?;
//! # let _ = (clean, fetched);
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - **`stats`** *(default)* — resource measurement: `ProcessGroupStats`,
//!   `ProcessGroup::stats` (plus the `sample_stats` time-series sampler), the
//!   per-process `RunningProcess::cpu_time`/`peak_memory_bytes` diagnostics,
//!   and the `RunningProcess::profile` run summary. Disable
//!   (`default-features = false`) to compile the accounting code out.
//! - **`limits`** — whole-tree resource caps: `ResourceLimits`, the
//!   `memory_max`/`max_processes`/`cpu_quota` builders on
//!   [`ProcessGroupOptions`], and `Error::ResourceLimit`. Implies `stats`.
//! - **`mock`** — the `mockall`-generated `MockRunner` for consumers' tests.
//! - **`tracing`** — a `tracing` event per command run.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

mod buffer;
mod client;
mod command;
mod doubles;
mod error;
mod group;
#[cfg(feature = "limits")]
mod limits;
mod mechanism;
mod pump;
mod result;
mod runner;
mod running;
mod signal;
#[cfg(feature = "stats")]
mod stats;
mod stdin;
mod supervisor;
mod sys;

pub use buffer::{OutputBufferPolicy, OverflowMode};
pub use client::CliClient;
pub use command::Command;
pub use doubles::{Invocation, RecordingRunner, Reply, ScriptedRunner};
pub use encoding_rs::Encoding;
pub use error::{Error, Result};
pub use group::{ProcessGroup, ProcessGroupOptions};
#[cfg(feature = "limits")]
pub use limits::ResourceLimits;
pub use mechanism::Mechanism;
pub use result::ProcessResult;
pub use runner::{JobRunner, ProcessRunner, ProcessRunnerExt};
pub use running::{RunningProcess, StdoutLines};
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
/// its index in `processes` and its exit code (`None` for a signal-killed run,
/// matching [`RunningProcess::wait`]).
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
/// let (idx, code) = wait_any(&mut [&mut a, &mut b]).await?;
/// println!("contender #{idx} exited first with {code:?}");
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
pub async fn wait_any(processes: &mut [&mut RunningProcess]) -> Result<(usize, Option<i32>)> {
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
                return std::task::Poll::Ready(result.map(|code| (idx, code)));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// The `mockall`-generated mock of [`ProcessRunner`] (enabled by the `mock`
/// feature), re-exported under a friendlier name.
#[cfg(feature = "mock")]
pub use runner::MockProcessRunner as MockRunner;

#[cfg(test)]
mod tests {
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
}
