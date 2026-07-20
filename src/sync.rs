//! The `cfg(loom)`-swappable synchronization layer for the crate's lock-free
//! lifecycle protocols.
//!
//! The three historically race-prone protocols around a child's PID lifecycle —
//! the [`PidGate`](crate::sys::pid_gate::PidGate) mutex, the deadline arbiter's
//! `PENDING → TIMED_OUT` / `PENDING → EXITED` CAS
//! ([`running::deadline`](crate::running::deadline)), and the
//! [`SkipDropKill`](crate::sys::SkipDropKill) generation latch — build their sync
//! primitives from *this* module rather than reaching for `std::sync` directly.
//!
//! In every ordinary build of this crate these are plain re-exports of
//! `std::sync`, so there is **zero** change to runtime behavior, code generation,
//! or the dependency graph — `cfg(loom)` is never set here, and `loom` is not a
//! dependency of this crate at all.
//!
//! # How the loom models reach these cores
//!
//! `--cfg loom` cannot be applied to this crate as a whole: it depends on
//! `tokio::process`, which tokio compiles out under `cfg(loom)` (loom cannot model
//! real I/O). So the model checking lives in a **standalone harness** — the `loom/`
//! sub-package (its own manifest and lockfile, like `fuzz/`), which depends only on
//! `loom`, never on this crate. That harness `#[path]`-includes the three pure core
//! source files *and this module*, compiling them with `RUSTFLAGS=--cfg loom`; the
//! `all(loom, test)` arm below then resolves to loom's atomics/mutex, and each
//! core's `#[cfg(all(test, loom))]` suite exhaustively permutes the interleavings.
//! The tokio-touching parts of those files are `#[cfg(not(loom))]`-gated so the
//! harness never pulls them (or tokio) in. See `loom/README.md`.

#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic;
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic;

#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Mutex, MutexGuard};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Mutex, MutexGuard};
