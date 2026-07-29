# `processkit-loom` — loom model-check harness

A standalone helper crate that runs [loom](https://docs.rs/loom) over the parent
crate's three historically race-prone PID-lifecycle lock-free protocols:

- **`PidGate`** (`src/sys/pid_gate.rs`) — linearizes a teardown watchdog's raw
  `kill(pid)` against the reap that frees (and lets the OS recycle) the pid.
- **the watchdog arbiter** (`src/running/deadline.rs`) — the single-word CAS
  shared by the absolute deadline, output-inactivity watchdog, and natural reap.
- **`SkipDropKill`** (`src/sys/skip_drop_kill.rs`) — the generation-packed
  "don't kill on Drop" latch guarding the spawn/shutdown re-arm race (T-079).

## Why a separate crate

loom swaps in its atomics/mutex under `--cfg loom`, which (via `RUSTFLAGS`) applies
to the entire dependency graph. The parent `processkit` crate depends on
`tokio::process`, and tokio compiles its `process`/`net`/`fs` modules **out** under
`cfg(loom)` (it cannot model real I/O) — so `processkit` itself cannot build under
`--cfg loom`.

This crate therefore depends **only** on `loom` (never on `processkit`) and pulls
the three *pure* cores in from the parent `src/` via `#[path]` — the exact same
source files the real crate compiles, not copies. The cores' tokio/libc/windows-sys
parts are `#[cfg(not(loom))]`-gated, so nothing here needs them; the swappable
`src/sync.rs` layer is shared verbatim.

## Running

```sh
cd loom
RUSTFLAGS="--cfg loom" cargo test --release
```

The `loom` CI job (`.github/workflows/ci.yml`) runs exactly this on one Linux leg —
the modeled protocols are platform-agnostic, so a single interleaving search covers
every target. The parent crate keeps deterministic real-thread equivalents of every
model (`#[cfg(all(test, not(loom)))]`), which run in the ordinary `cargo test`.
