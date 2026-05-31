//! `processkit` — child-process management for Rust.
//!
//! A port of the .NET ProcessKit library, in two layers:
//!
//! - **[`ProcessGroup`]** — a kill-on-drop container for a process *tree*. Every
//!   child spawned into the group, and everything those children spawn, dies
//!   with the group, so an exiting or panicking owner never leaks subprocesses.
//!   Containment is a Windows [Job Object], a Linux [cgroup v2] (with a POSIX
//!   process-group fallback), or nothing on other targets — observable via
//!   [`Mechanism`].
//! - **runner** — async run-and-capture built on the group. Describe a run with
//!   [`Command`], then drive it to completion ([`Command::output_string`],
//!   [`Command::run`], …) or start it via a [`ProcessRunner`] for streaming or a
//!   shared group. The trait is the mock seam (see [`ScriptedRunner`]).
//!
//! Async throughout (tokio). Errors are the structured [`Error`]; a non-zero
//! exit is reported in [`ProcessResult`], not raised, until you call
//! [`ProcessResult::ensure_success`].
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
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

mod buffer;
mod client;
mod command;
mod doubles;
mod error;
mod group;
mod mechanism;
mod pump;
mod result;
mod runner;
mod running;
mod stats;
mod stdin;
mod sys;

pub use buffer::{OutputBufferPolicy, OverflowMode};
pub use client::CliClient;
pub use command::Command;
pub use doubles::{Invocation, RecordingRunner, Reply, ScriptedRunner};
pub use encoding_rs::Encoding;
pub use error::{Error, Result};
pub use group::{ProcessGroup, ProcessGroupOptions};
pub use mechanism::Mechanism;
pub use result::ProcessResult;
pub use runner::{JobRunner, ProcessRunner, ProcessRunnerExt};
pub use running::{RunningProcess, StdoutLines};
pub use stats::ProcessGroupStats;
pub use stdin::{ProcessStdin, Stdin};
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

/// The `mockall`-generated mock of [`ProcessRunner`] (enabled by the `mock`
/// feature), re-exported under a friendlier name.
#[cfg(feature = "mock")]
pub use runner::MockProcessRunner as MockRunner;
