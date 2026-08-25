# ProcessKit roadmap

> **What this is.** The committed near-term work — the **"do now"** bucket of the
> development sweeps. Open ideas not yet committed live in [`ideas/`](ideas/)
> (tagged `next-` = reconsider first, `later-` = further out); settled
> "won't do / won't change" decisions live in [`decisions/`](decisions/). See
> [`ideas/README.md`](ideas/README.md) for the taxonomy.
>
> **Stability since 1.0.** The crate follows
> [Semantic Versioning](https://semver.org/): the public API is stable within a
> major line, and breaking changes land only in a new major version. The current
> implementation and API are the Tokio-based v3 line. The next breaking window is
> v4; its proposed architecture and delivery gates are documented in the
> [runtime-neutral v4 roadmap](docs/runtime-neutral-v4-roadmap.md). Historical
> entries below retain the release language used when they were written.
>
> Items are roughly ordered by leverage, not strict sequence. Cost is a gut
> estimate (trivial / moderate / major).

## Next major — runtime-neutral v4

The recommended v4 direction is a **runtime-neutral `processkit` core with
replaceable, additive backends**. Tokio and async-io are the initial official
backends; applications may use either or both, and the core plus async-io path
must remain strictly Tokio-free. Runtime choice is explicit on a non-generic
`ProcessKit` owner, while platform containment remains one shared implementation.

This is a future architecture, not functionality available in v3. Goals,
non-goals, capability contracts, migration examples, feasibility spikes, risks,
effort ranges, conformance gates, and release criteria live in the
**[ProcessKit v4 runtime-neutral roadmap](docs/runtime-neutral-v4-roadmap.md)**.

## Shipped

The original ten-item sweep (2026-06-09) is **done**: items 1–9 shipped across
`0.9.0`/`0.9.1` (README polish, community-health files, doc-coverage lint + manifest
metadata, `examples/`, the pre-1.0 API review with `#[non_exhaustive]`, `ProcessResult`
enrichment, `ok_codes`, graceful run-level timeout, spawn-error quality + the full
`Error::Exit` streams fix). See [`CHANGELOG.md`](CHANGELOG.md) `[0.9.0]`/`[0.9.1]`.
Item 10 (test gaps) was the only one left open and is folded into **item 1** below.

## Now — post-0.9.1 sweep (2026-06-10)

The 2026-06-10 sweep (same eight dimensions) found the idea space already well-mapped
by the prior sweep — most candidates were already filed in [`ideas/`](ideas/) or
settled in [`decisions/`](decisions/). What it *did* surface: the 0.9.1 graceful-timeout
feature shipped with uneven test coverage, two `next-` ideas became ripe now that their
dependencies shipped and the roadmap drained, and a few cheap doc/CI wins. Those are the
items below. The sweep deliberately stops at six rather than padding to ten — the
remaining backlog stays consumer-gated in `ideas/`, which is the honest "nothing more
worth committing yet" state.

### 1. Close the highest-risk test gaps + 0.9.1 graceful-timeout coverage ✅ SHIPPED (2026-06-10)
*Dimension: testing · Cost: moderate*

The 0.9.1 graceful timeout (`timeout_grace`/`timeout_signal`) shipped with the **bulk**
path covered (`tests/integration/shutdown.rs`) but two branches uncovered, plus the
pre-existing item-10 gaps:
- **Streaming-path graceful teardown** (`src/running/stream.rs:104-124`) — the
  `stdout_lines` deadline watchdog arms a *distinct* graceful branch that does **not**
  hold the `Child` (no concurrent reap) and has a `group.upgrade()` → direct-kill
  fallback. **Zero** coverage today. Highest-value, lowest-cost addition — mirror the
  unix shell-trap shutdown tests but drive `stdout_lines()` + `finish_streamed()`.
- **Windows graceful-degrade-to-atomic-kill** — the documented contract that
  `timeout_grace`/`timeout_signal` silently degrade to the Job kill on Windows (no
  phantom grace wait) is unasserted. One `#[cfg(windows)]` `#[ignore]` test.
- **`ok_codes` end-to-end through the real verbs** — solid at the `ensure_success`
  unit layer, but no real-subprocess test that `ok_codes([0,1])` plumbs through
  `run`/`output_string`/**`output_bytes`** (the bytes verb carries `ok_codes` and has
  no test at any layer). Cheap integration smoke.
- **Original item 10** — cancellation fired *between spawn and first wait-poll*
  (`runner.rs:256-266` short-circuits pre-spawn; the spawn→`select!` window is
  untested) and the pump's no-trailing-newline last line (`pump.rs` `read_until` tail).
  The multibyte-split-across-chunks case is largely reassembled by `BufReader` already —
  prove it with the property test in [`ideas/later-advanced-testing.md`](ideas/later-advanced-testing.md), not a hand-rolled unit case.

**Shipped:** streaming graceful-teardown test (unix `#[ignore]`); shared-group graceful-timeout
test; Windows graceful-degrade test (`#[cfg(windows)]` `#[ignore]`); `ok_codes` e2e through
`output_bytes`; cancel-race (spawn→first-poll window) with hermetic `ScriptedRunner`; pump
no-trailing-newline unit test. Verified fmt/clippy/doc/test/cross-compile.

### 2. Output-handling design pass — Stdio modes, tee, ordered event stream ✅ SHIPPED (2026-06-10)
*Dimension: user-scenario · Cost: major (one deliberate design)*

**Shipped:** `StdioMode { Piped, Inherit, Null }` + `Command::stdout(mode)` /
`Command::stderr(mode)` builders; `Command::stdout_tee<W>()` / `stderr_tee<W>()` tee;
`OutputEvent { Stdout(OutputLine), Stderr(OutputLine) }` + `OutputEvents`
merged stream + `RunningProcess::output_events()` / `finish()`; `OverflowMode::Error`
+ `OutputBufferPolicy::fail_loud(n)` + `Error::OutputTooLarge`.
Verified fmt/clippy/doc/test/cross-compile.
*(Revised by the 0.9.2 inspection sweep: the tee now takes a `tokio::io::AsyncWrite`
sink — async, backpressuring, error-surfacing, independent of `on_*_line`; the buffer
gained a `with_max_bytes` ceiling; and `Error::OutputTooLarge` became
`{ program, line_limit, byte_limit, total_lines, total_bytes }`. See the CHANGELOG.)*
*(Renamed and widened in the pending 3.0 major: `OutputEvent`/`OutputEvents`/
`output_events()` became `ProcessEvent`/`ProcessEvents`/`events()`, and the merged
stream gained `Started`/`Exited` lifecycle events — see [`[Unreleased]`](CHANGELOG.md)
and [Upgrading](docs/upgrading.md).)*

### 3. Program resolution & error-quality completion — `which`/PATH + cwd-relative clarity ✅ SHIPPED (2026-06-10)
*Dimension: user-scenario / ergonomics · Cost: moderate*

Promote the lead candidate of [`ideas/next-launch-ergonomics.md`](ideas/next-launch-ergonomics.md)
(A): resolve a program to an absolute path before spawn and report *which* PATH was
searched on failure — `` `foo` not found on PATH ``. 0.9.1 shipped the three
pieces this completes: `is_not_found()`, `command_line()` quoting, and the `current_dir`
pre-check. Cross-platform PATH/PATHEXT resolution is the real cost. Same design pass:
document/guard the **cwd-relative-program gotcha** — `Command::new("./tool")` with a
`current_dir` set resolves `./tool` against the *parent's* cwd, not the child's (a classic
silent surprise). Bulk env loaders (C — `IntoIterator<(K,V)>`, trivial) can ride along.

**Shipped:** `Error::NotFound { program, searched }` (new variant, `is_not_found()` updated),
enriched from the OS's not-found error in `launch()` for bare program names (the OS stays the
source of truth, so a program it resolves by another route — e.g. the application directory on
Windows — is never falsely rejected); `Command::envs([…])` bulk builder; `current_dir` doc
with the relative-program warning. Cross-platform `searched`-PATH formatting (Unix execute-bit
check + Windows PATHEXT expansion). Verified fmt/clippy/doc/test/cross-compile.

### 4. CI quality hardening — the pre-1.0 wins ✅ SHIPPED (2026-06-10)
*Dimension: stabilization / conventions · Cost: trivial each*

The "now" subset of [`ideas/next-ci-and-quality-hardening.md`](ideas/next-ci-and-quality-hardening.md),
plus one new tool:
- **`cargo-hack --feature-powerset --depth 2`** — the standout: CI tests three fixed
  feature configs, but the optional features have combinations (`limits` w/o `stats`,
  `record`+`tracing`) that can break compilation undetected. Cheapest real correctness gain.
- **`-Z minimal-versions`** check and **`cargo llvm-cov` coverage + badge** (run
  `--include-ignored` — much of the suite is `#[ignore]`d real-subprocess tests).
- **`cargo-public-api` snapshot** committed and diffed in CI — turns "did this PR move the
  public surface?" into a reviewable diff *while we still break freely*. The pre-1.0
  complement to `cargo-semver-checks` (which stays deferred to ~1.0, post-freeze).

**Shipped:** `hack` job (`--feature-powerset --depth 2`); `public-api` CI diff job +
`public-api.txt` baseline snapshot. (`-Z minimal-versions` and `llvm-cov` deferred — both
require nightly/extra toolchain install in CI and no concrete coverage goal yet.)
Verified fmt/clippy/doc/test.

### 5. Executable doctests through the hermetic seam ✅ SHIPPED (2026-06-10)
*Dimension: docs / testing · Cost: trivial–moderate*

All 13 fenced examples in `src/` are `no_run` — they catch signature breaks but never run.
The crate already ships the tool to fix a subset hermetically: convert 2–4 of the
most-read examples (e.g. `output_string`/`probe`/pipefail attribution) to **execute**
through `ScriptedRunner`, so they run in `cargo test` on every OS with no subprocess.
The crate's own `docs/testing.md` sells that seam — the API docs should eat the dog food.

**Shipped (partial — scope trimmed on review):** a new **executing** doctest on the
`ScriptedRunner` type (`src/doubles.rs`) driving a command through scripted replies — the
test seam eating its own dog food, hermetic on every OS — plus a runnable `Command::envs`
example (`src/command.rs`). The ergonomic-intro examples (`src/lib.rs`, `stdout_lines` /
`output_events` in `src/running/stream.rs`, `src/pipeline.rs`) were **deliberately left
`no_run`**: they teach the real `Command::new(...).verb()` / `.start()` surface, and routing
them through a mock runner to make them execute would misrepresent the very API they
demonstrate. Net: 2 of 16 doctests now execute (the two that *should*); the rest stay
compile-checked. Verified all doctests pass on all platforms.

### 6. Resolve the README ↔ crate-doc duplication ✅ SHIPPED (2026-06-10)
*Dimension: docs / stabilization · Cost: trivial (decide) / moderate (if adopting)*

`lib.rs`'s `//!` crate doc substantially duplicates the README intro, verb table, and
feature list — they *will* drift. The standard fix is `#![doc = include_str!("../README.md")]`,
but the README's `docs.rs`-absolute links, relative `docs/*.md` links, and raw cover image
make a naive include lossy from docs.rs. So this is a **decision**, not a slam-dunk: either
restructure the README links to be include-safe and adopt it, or trim the crate doc to a
concise intro that *defers* to the README/guides instead of duplicating them — and record
the choice. Right now it's drift-by-default with no rationale on file.

**Shipped:** Option B chosen — `src/lib.rs` crate-doc trimmed to a concise intro pointing at
the README and docs guides; duplicated verb table / feature list removed. Decision rationale
recorded in `decisions/readme-crate-doc-sourcing.md`. Verified fmt/clippy/doc/test.

## Now — 2.x additive sweep (2026-07-08)

Two `next-` ideas from [`ideas/next-launch-ergonomics.md`](ideas/next-launch-ergonomics.md) came
ripe and shipped directly, ahead of a dedicated sweep write-up.

### 7. Project-local executable preference ✅ SHIPPED (2026-07-08)
*Dimension: user-scenario / ergonomics · Cost: trivial*

Moved from [`ideas/next-launch-ergonomics.md`](ideas/next-launch-ergonomics.md) §B.
**Shipped:** `Command::prefer_local(self, impl Into<PathBuf>) -> Self`, which prepends a
project-local bin directory to `PATH` during command resolution. Additive in the `2.x`
release line. Verified fmt/clippy/doc/test/cross-compile.

### 8. Send stdin control characters ✅ SHIPPED (2026-07-08)
*Dimension: user-scenario / ergonomics · Cost: trivial*

Moved from [`ideas/next-launch-ergonomics.md`](ideas/next-launch-ergonomics.md) §E.
**Shipped:** `ProcessStdin::send_control(&mut self, char) -> Result<()>`, enabling callers
to send Ctrl+C or Ctrl+D to the stdin of a live subprocess. Additive in the `2.x` release
line. Verified fmt/clippy/doc/test/cross-compile.

---

## Notes on what did *not* make the cut (and why)

- **The rest of the filed `next-` ideas stay deferred, by discipline.** Process scheduling
  knobs (`nice`/`ionice`/`umask`, [`ideas/next-scheduling-knobs.md`](ideas/next-scheduling-knobs.md))
  remain consumer-gated. `prefer_local` and `send_control` are now shipped and have been
  moved to the Shipped section above.
- **`later-*` ideas:** several landed in the pending 3.0 major —
  [`ideas/later-pty-support.md`](ideas/later-pty-support.md) (the `pty` feature /
  `Command::use_pty()`), [`ideas/later-detached-handoff.md`](ideas/later-detached-handoff.md)
  (`Command::spawn_detached()` → `DetachedChild`), and
  [`ideas/later-buffer-policy-seam.md`](ideas/later-buffer-policy-seam.md) (the
  `CapturePolicy` redaction-at-capture seam) shipped. The storable-hook half of
  [`ideas/later-extensibility-hooks.md`](ideas/later-extensibility-hooks.md) (the
  `before_spawn` raw-`Command` mutator) was assessed and **declined**
  ([`decisions/before-spawn-hook-2026-07.md`](decisions/before-spawn-hook-2026-07.md)),
  leaving the now-documented `Command::to_tokio_command()` as the honest escape hatch.
  Runtime-neutral execution has since been promoted to the explicit
  [v4 roadmap](docs/runtime-neutral-v4-roadmap.md). The remaining items (lite/sys
  split, observability/docs-site, cassette-cwd portability, retry jitter, and
  internal-simplification notes) stay gated on a concrete consumer or further-out —
  see those records.
- **`cargo-semver-checks` + clippy `pedantic`** stay in `ideas/next-ci-*` — semver-checks
  pays off only *after* the 1.0 freeze; full pedantic is a noisy triage. The targeted
  `# Errors`/`# Panics` doc convention (the high-value slice of pedantic for this
  error-rich crate) is noted there for cherry-picking.
- **New won't-do verdicts** from this sweep (a `clear_env()` verb redundant with
  `inherit_env([])`, `arg_if` builder sugar, env-var-*name* redaction, finer stdin
  flush knobs subsumed by PTY) → [`decisions/wont-do-2026-06.md`](decisions/wont-do-2026-06.md).
- **Built-in shell mode, IPC/message-passing, detached-as-default, miri, object-mode
  streams** → [`decisions/wont-do-2026-06.md`](decisions/wont-do-2026-06.md) (unchanged).
- **Reattach-to-container-by-identity** (the `processkit-cli` wishlist's last open
  strand — a stable container id readable from a live group, reattached from a later
  process to inspect or kill the leaked tree after abrupt owner death) →
  [`decisions/container-reattach-identity.md`](decisions/container-reattach-identity.md).
  Declined as out of crate scope: the safe-identity property it requires is
  unachievable on either backend (Linux `cgroupfs` gives no contractual inode/birth-time
  and no place for a marker; a Windows named/duplicated Job Object handle regresses the
  free `KILL_ON_JOB_CLOSE` kill-on-owner-death and its name is reuse-unprotected), and
  the residual honest need is already served by the T-151 `ParentDeathCleanup` report
  plus the documented "keep an external owner (subreaper / `systemd` scope)" remedy.
  Carries a bounded revisit condition for a read-only, inspection-only identifier getter.
- **The `processkit-sys` sync-core split** (a synchronous, no-tokio containment crate —
  `src/sys/**` extracted so a consumer without an async runtime can contain and kill a
  process tree, with `processkit` layered on top as the async wrapper;
  [`ideas/later-lite-build-sys-split.md`](ideas/later-lite-build-sys-split.md)) →
  assessed for the 3.0 major and **deferred** in
  [`decisions/sync-core-processkit-sys-split.md`](decisions/sync-core-processkit-sys-split.md).
  The "3.0 is the only window" framing does not hold: a *layered* extraction
  (`processkit` keeps its public types and wraps the core, or re-exports the leaf sync
  types) is additive and non-breaking, so it needs **no major** — it can land in any
  future minor the moment a concrete consumer appears. Absent that consumer (still
  none), the standing "don't ship a second SemVer-committed public surface
  speculatively" discipline ([`decisions/runtime-tokio-only.md`](decisions/runtime-tokio-only.md))
  governs. `encoding_rs` stays mandatory on the async crate; the make-it-optional lever
  rides along with the deferred split, not as a standalone change.
