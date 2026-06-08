//! Real-subprocess integration tests for `processkit`.
//!
//! These spawn actual child processes (and create OS jobs / cgroups), so they
//! are `#[ignore]`d to keep `cargo test` hermetic on CI. Run them locally with:
//!
//! ```text
//! cargo test --all-features -- --ignored
//! ```
//!
//! (`--all-features` because the `limits` tests are compiled out by default.)
//!
//! The no-orphan kernel guarantee can only be proven against a real process
//! tree, which is exactly what these cover.
//!
//! One binary, one topic per module — the `mod` declarations carry the same
//! feature/platform gates the tests had as a flat file.

mod common;

mod batch;
#[cfg(feature = "cancellation")]
mod cancellation;
mod capture;
mod env_privileges;
mod groups;
#[cfg(feature = "limits")]
mod limits;
#[cfg(target_os = "linux")]
mod parent_death;
mod pipeline;
#[cfg(feature = "process-control")]
mod process_control;
mod races;
mod readiness;
#[cfg(unix)]
mod shutdown;
#[cfg(feature = "stats")]
mod stats;
mod streaming;
mod supervision;
