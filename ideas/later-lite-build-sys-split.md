# A "lite" build / lower-requirement subset — what's worth doing

> **Status:** decision record. Assessed 2026-06-09 after an owner question: would a
> **lite version of the package** — trimmed to the most basic functionality, with
> lower requirements for the user — make sense? No concrete blocker drove it; it's
> a "what's in scope" sweep, like the siblings. Sibling decision records:
> [`runtime-agnostic-vs-tokio.md`](later-runtime-agnostic.md) (the other facet of
> "lower the tokio floor"), [`architecture-audit-2026-06.md`](../decisions/architecture-audit-2026-06.md),
> and [`permissions-privileges-pty-network.md`](../decisions/permissions-privileges-pty-network.md).

## TL;DR verdicts

| Aspect | Verdict | Rationale |
|---|---|---|
| **Feature-trimmed lite** | **Already shipped** | `--no-default-features` drops `stats` + `process-control` and their FFI. Features are additive *visibility* gates — kill-on-drop is unconditional in every config. The "turn off what you don't need" lite already exists. |
| **Does trimming lower the floor?** | **No** | `tokio` (8 features), `encoding_rs`, `tokio-stream`, `async-trait`, `thiserror` stay mandatory. "Lower requirements" really means **dropping tokio** — a different class of change than a feature toggle. |
| **A truly low-requirement lite = a sync, no-tokio core** | **Feasible** (unlike the agnostic idea) | Containment is sync at heart: `ProcessGroup::spawn`/`Job::spawn`/Drop have no `.await`. A sync core reaps with blocking `std::process::Child::wait` — **no reactor**, so it sidesteps the async-reaping problem that sank runtime-agnosticism, and `std` preserves the Windows containment seam. |
| **Ship it as a forked "lite" crate** | **Reject** | A duplicate-and-trim copy of `sys/` would drift. The correct shape is layering. |
| **Ship it as a layered `processkit-sys` core** | **Defer** | This is the seam the Cargo.toml comments already mark. Real new code + parallel spawn path + API/docs/CI for a crate with **no concrete consumer**. |
| **Make `encoding_rs` optional** | **Defer** (folds into the sys split) | On inspection it's not the clean toggle it first looked: the *default* UTF-8 path also runs through `encoding_rs`, and `&'static Encoding` is threaded as a stored field through ~7 files — a cfg-split with conditional struct-field types, for a saving that's marginal next to the mandatory tokio floor, with no consumer asking. |

---

## 1. Feature-trimmed lite already exists

The crate's features are **additive visibility gates**, not behavioural switches —
the kill-on-drop-the-whole-tree guarantee is unconditional in every configuration
(Cargo.toml lines 90-96). A consumer who wants less already has it:

- `--no-default-features` drops `stats` (`ProcessGroup::stats`, per-process
  `cpu_time`/`peak_memory_bytes`, and on Windows the ProcessStatus FFI) and
  `process-control` (`Signal` + `signal`/`suspend`/`resume`/`members`/`adopt`).
- The remaining optional surfaces (`limits`, `mock`, `tracing`, `cancellation`,
  `record`) are off unless asked for.

So "lite" in the sense of *compile out what you don't use* is shipped today.

## 2. …but trimming features does not lower the floor

`--no-default-features` leaves the **mandatory base** untouched: `tokio` with
`process, time, io-util, rt, macros, sync, fs, net` (Cargo.toml:56), plus
`encoding_rs`, `tokio-stream`, `async-trait`, `thiserror`. The dominant
"requirement for the user" is **an async runtime (tokio)**. Lowering *that* is not
a feature toggle — it's an architectural subset.

## 3. The split seams are already marked

This isn't a new idea to the design — the Cargo.toml comments pre-place the
boundaries a lite crate would cut along:

- `limits` "travel[s] together toward the possible **`processkit-resource`
  split**" (Cargo.toml:105).
- `process-control` "matches the layer a future **`processkit-sys` split** would
  carve out" (Cargo.toml:109).
- The broader visibility split that was tried and rolled back is recorded in
  [`architecture-audit-2026-06.md`](../decisions/architecture-audit-2026-06.md) (Cargo.toml:92-95).

A lite/core crate would carve along these existing gates, not invent new ones.

## 4. Why a sync core is feasible where runtime-agnostic was not

The decisive contrast with [`runtime-agnostic-vs-tokio.md`](later-runtime-agnostic.md):
that idea died because async `child.wait()` needs a runtime-specific reactor
(SIGCHLD / handle). **A sync core never awaits a child**, so the reactor problem
does not arise:

- `ProcessGroup::spawn` (`src/group.rs:173`) and the platform `Job::spawn`
  (`src/sys/windows.rs:155`, `src/sys/pgroup.rs:168`) are **already sync** — no
  `.await`. Drop's hard-kill is sync. Only `shutdown` / `graceful_shutdown`
  (`src/group.rs:347`, `src/sys/pgroup.rs:276`) and the runner/pump/streaming layer
  are async.
- The containment mechanism is **runtime-independent FFI already** — `windows-sys`
  Job Object, `libc` pgroup/cgroup. The lone tokio touch in the core is that spawn
  goes through `tokio::process::Command` rather than `std::process::Command`.
- A sync core spawns via `std::process::Command`, reaps via blocking
  `std::process::Child::{wait, try_wait}`, and uses `std::thread::sleep` for the
  graceful SIGTERM→wait→SIGKILL tier. Crucially, `std` exposes the Windows
  `CREATE_SUSPENDED` creation flag and `Child::as_raw_handle()` — the exact seam
  the race-free `spawn-suspended → AssignProcessToJobObject → resume` dance needs
  (`src/sys/windows.rs:160-202`). So the headline guarantee **survives** on the
  sync backend, which could not be guaranteed for the async-process backend in the
  sibling record.

So the crate's actual value — contain a process tree, kill it on drop — is sync at
heart and needs no tokio.

## 5. Why defer (and reject the fork)

- **Reject a forked "lite" crate.** Duplicating and trimming `sys/` produces two
  copies that drift. Wrong structure.
- **The right shape is layering:** a sync, no-tokio **`processkit-sys`** core
  (contain + hard-kill + sync graceful tier + the `process-control` verbs) that the
  full `processkit` *depends on* and wraps with the async runner/pump/streaming.
  One source of containment truth; the async crate adds I/O on top.
- **Defer building it.** It's a real second spawn path, a new public API surface,
  separate docs, and a wider CI matrix — for a crate with **no concrete consumer**.
  The repo's discipline (see the PTY defer in the sibling record) is to wait for a
  real ask. A sync-core split has a *more tractable* design than runtime-agnostic
  but the *same* missing-consumer gate.

## 6. The `encoding_rs`-optional lever — assessed and deferred

The other heavy mandatory dep is `encoding_rs` (Cargo.toml:63-66 — non-optional).
The tempting move: gate it behind a default-on `encoding` feature with a
`std`-only UTF-8-lossy fallback when off. Assessed 2026-06-09; **deferred** — it's
smaller than the sys split but neither as clean nor as worthwhile as it first looks:

- **The default UTF-8 path also runs through `encoding_rs`.** `src/pump.rs:181`
  decodes *every* line via `encoding.decode(&buf)` — UTF-8 included; there is no
  `std::str::from_utf8_lossy` fallback today. Turning the feature off must replace
  the **default** decode path, not merely drop the override.
- **`&'static Encoding` is a stored field threaded through the core**, so gating it
  is a cfg-split across ~7 files with conditional struct-field types (cfg-noise in
  the pump hot path, not a clean toggle): `Command` (fields + the three public
  builders `stdout_encoding`/`stderr_encoding`/`encoding` at `src/command.rs:490-506`,
  accessors, `Debug`, `new`), `Spawned`/`RunningProcess` (`src/running/mod.rs`),
  the `pump_lines` signature (`src/pump.rs`), `src/running/stream.rs`,
  `src/runner.rs`, and the `pub use encoding_rs::Encoding;` re-export
  (`src/lib.rs:182`).
- **The saving is marginal.** `encoding_rs` is one crate next to the mandatory
  `tokio` (8 features) floor; shaving it while tokio stays barely lowers
  "requirements for the user," and a new default-on feature is one more knob carried
  into 1.0 — for a footprint nobody has reported as a problem.

**Conclusion:** don't do it as a standalone change now. It belongs **inside the
`processkit-sys` sync core** (§4-5): a no-tokio sync core that also drops
`encoding_rs` (UTF-8-lossy decode) is coherent and motivated; gating `encoding_rs`
alone on the async crate is not worth the cfg-noise.

## Revisit when

A concrete consumer needs **synchronous** or **no-tokio** process-tree containment
(an embedded/CLI tool that won't pull an async runtime), at which point extract
`processkit-sys` along the already-marked seam (§3) and layer `processkit` on it —
not before, and not as a fork (§5). The `encoding_rs`-optional lever (§6) rides
along with that split (or with a real, reported footprint goal) — not as a
standalone change before then.
