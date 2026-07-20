//! Standalone [loom] model-check harness for the parent crate's three
//! historically race-prone PID-lifecycle lock-free protocols (archive:
//! T-064/066/078/079/082/092/093):
//!
//! * the [`PidGate`](pid_gate::PidGate) mutex that linearizes a watchdog's raw
//!   `kill(pid)` against the reap that frees (and lets the OS recycle) the pid;
//! * the deadline arbiter's `PENDING → TIMED_OUT` / `PENDING → EXITED` CAS
//!   ([`deadline::claim_timed_out`] / [`deadline::claim_exited`]);
//! * the [`SkipDropKill`](skip_drop_kill::SkipDropKill) generation latch guarding
//!   the spawn/shutdown Drop-kill re-arm race.
//!
//! # Why a separate crate
//!
//! loom must swap in its atomics/mutex under `--cfg loom`, which — via `RUSTFLAGS`
//! — applies to the *whole* dependency graph. The parent `processkit` crate uses
//! `tokio::process`, and tokio compiles its `process`/`net`/`fs` modules out under
//! `cfg(loom)` (it can't model real I/O), so `processkit` itself cannot build under
//! `--cfg loom`. This harness sidesteps that by depending only on `loom` and
//! pulling the three *pure* cores in from the parent `src/` via `#[path]` — the
//! same source the real crate compiles, no copy. The cores' tokio/libc/windows-sys
//! parts are `#[cfg(not(loom))]`-gated, so nothing here needs them.
//!
//! Run with:
//!
//! ```text
//! cd loom && RUSTFLAGS="--cfg loom" cargo test --release
//! ```
//!
//! Each core's `#[cfg(all(test, loom))]` suite (compiled only here) drives
//! `loom::model` to exhaustively permute thread interleavings and memory orderings
//! and assert the protocols' invariants: no gated kill after a freed pid, no double
//! claim of the arbiter, no missed timeout, and no silently-stripped Drop-kill
//! backstop. The parent crate keeps the deterministic real-thread equivalents
//! (`#[cfg(all(test, not(loom)))]`), which run in the ordinary test suite.
//!
//! [loom]: https://docs.rs/loom

// The `cfg(loom)`-swappable sync layer, shared verbatim with the parent crate.
// Under this harness's `cargo test --cfg loom` (cfg(test) + cfg(loom)) it resolves
// to loom's atomics/mutex; the cores below reference it as `crate::sync`.
#[path = "../../src/sync.rs"]
pub(crate) mod sync;

// The timeout-arbiter states the deadline core references as `super::TS_*`. These
// mirror the (private) constants in `src/running/mod.rs`; they are trivially stable
// (a single CAS arbiter — PENDING/EXITED/TIMED_OUT). Kept in sync by their meaning,
// not shared, since `running/mod.rs` also drags in tokio.
const TS_PENDING: u8 = 0;
const TS_EXITED: u8 = 1;
const TS_TIMED_OUT: u8 = 2;

// The three pure lock-free cores, pulled straight from the parent crate's source
// (not copies). Their `#[cfg(all(test, loom))]` suites are what this harness runs.
#[path = "../../src/sys/pid_gate.rs"]
mod pid_gate;

#[path = "../../src/running/deadline.rs"]
mod deadline;

#[path = "../../src/sys/skip_drop_kill.rs"]
mod skip_drop_kill;
