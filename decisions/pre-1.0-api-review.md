# Decision record: the pre-1.0 public API review

> **Status:** decision record / closed. Captured 2026-06-09 as ROADMAP item 5 —
> the pre-1.0 review that locks the public API shape before 1.0 freezes it. The
> mechanical outcome (the `#[non_exhaustive]` additions + the `ProcessGroupOptions`
> builders) shipped in the companion change; this note records the verdicts and the
> **recommended shapes for ROADMAP items 6–9** so those later stages are
> execution-only. Sibling: [`architecture-audit-2026-06.md`](architecture-audit-2026-06.md)
> (the standing rejected/confirmed-sound list — its API rejections still hold).

## TL;DR verdicts

| Aspect | Verdict | Rationale |
|---|---|---|
| **`#[non_exhaustive]` coverage** | **Add to 5 types** | `RestartPolicy`, `OverflowMode`, `OutputBufferPolicy`, `ResourceLimits`, `ProcessGroupOptions` lacked it; the rest of the public surface already had it. Uniformly future-proofs growable config/policy types. |
| **`ProcessGroupOptions` builders** | **Add two** | `shutdown_timeout` / `escalate_to_kill` had public fields but no builder; once the struct is non_exhaustive a builder is the construction path. |
| **Dependency-leak re-exports** | **Keep, documented** | `encoding_rs::Encoding`, `tokio_stream::StreamExt`, `tokio_util::sync::CancellationToken` are the currency consumers need to *use* the API; wrapping adds surface for no gain. |
| **Verb surface** | **Confirmed sound** | `Command` / `ProcessRunner(Ext)` / `CliClient` are consistent; the layering asymmetry is intentional. No renames. |
| **Items 6–9 API shape** | **Pre-settled below** | Recorded so the later stages land mechanically. |

---

## 1. `#[non_exhaustive]` — added to five types

Added to `RestartPolicy` (`src/supervisor.rs`), `OverflowMode` +
`OutputBufferPolicy` (`src/buffer.rs`), `ResourceLimits` (`src/limits.rs`), and
`ProcessGroupOptions` (`src/group.rs`). The rest of the public surface already
carried it (`Error`, `Outcome`, `Mechanism`, `Signal`, `StopReason`,
`SupervisionOutcome`, `ProcessGroupStats`, `RunProfile`).

**Why:** these are growable config/policy types — a future limit, buffer mode,
restart policy, or group knob should be a non-breaking addition, not a churn.
Uniform coverage means downstream `match` arms and struct literals don't break when
the crate grows.

**Breaking note:** external crates can no longer build the structs via struct
literal — they use the constructors/builders (`ProcessGroupOptions::default()`,
`OutputBufferPolicy::bounded(..)`/`unbounded()`, `ResourceLimits::default()` then
field-set). Same-crate construction is unaffected. Two in-repo consumers were
migrated to the builder form (`tests/integration/shutdown.rs`,
`docs/process-groups.md`). Pre-1.0, so the break is acceptable; flagged
`**Breaking**` in the changelog so the release tooling forces at least a minor bump.

## 2. `ProcessGroupOptions` builders

Added `shutdown_timeout(Duration)` and `escalate_to_kill(bool)` (`#[must_use]`,
matching the `memory_max`/`max_processes`/`cpu_quota` style). The public fields stay
(readable/mutable); the builders are the ergonomic construction path now that the
struct is non_exhaustive.

## 3. Dependency-leak re-exports — keep

`pub use encoding_rs::Encoding`, `pub use tokio_stream::StreamExt`, and (gated)
`pub use tokio_util::sync::CancellationToken` leak dependency types into the public
API. **Decision: keep all three**, documented as deliberate. They are exactly what a
consumer needs to *use* the surface — set a non-UTF-8 encoding
(`Command::encoding`), consume the `StdoutLines` stream (`.next().await`), and build
a cancellation token — without taking a direct dependency. Wrapping each in a
newtype/enum adds API surface and friction for stable, ubiquitous types, with no real
isolation gain (the dependency is already transitive and load-bearing). The
`Encoding` leak is the one with a latent alternative — it ties into the deferred
`encoding_rs`-optional idea (see
[`../ideas/later-lite-build-sys-split.md`](../ideas/later-lite-build-sys-split.md));
if that crate split ever happens, revisit then, not now.

## 4. Verb surface — confirmed sound

The consuming verbs across the three layers are consistent and intentionally
layered: `ProcessRunner` is the minimal 2-method seam; `ProcessRunnerExt` adds the
convenience verbs (`run`/`exit_code`/`probe`); `Command` and `CliClient` each expose
the same vocabulary over their own construction style. The `output_string` /
`output_bytes` split (not a bare `output`) and the absence of generic `on_command`
hooks are **already-settled** decisions (`architecture-audit-2026-06.md`). No renames
or consolidation — re-confirmed, not re-litigated.

## 5. Recommended shapes for items 6–9 (pre-settled)

So the later Phase-B stages are mechanical, not re-designed:

- **6 — `ProcessResult` enrichment.** Add `duration() -> Duration` and
  `truncated() -> bool` (new fields; the `ProcessResult::new()` constructor expands
  across **~13 call sites**). The **rendered command is *not* a `ProcessResult`
  field** — it lives on `Command` (item 9), so a result never carries argv (keeps the
  "never carry argv" posture; `tracing` stays argv-free).
- **7 — accepted exit codes.** `Command::ok_codes(impl IntoIterator<Item = i32>)`,
  stored on `Command` and threaded into `ProcessResult` so `is_success()` /
  `ensure_success()` honor it; default `{0}`. (Today the zero-only decision is
  hardcoded in `ProcessResult::is_success`/`ensure_success`.)
- **8 — run-level graceful timeout.** `Command::graceful_timeout(Duration)` +
  `timeout_signal(Signal)`; the run-level timeout signals-then-escalates by reusing
  the existing `graceful_shutdown` tier (`ProcessGroup::shutdown` →
  `sys/*/graceful_shutdown`) instead of the current hard `kill_tree`, respecting the
  own-group-vs-shared distinction. Windows stays a hard kill (documented; no signal
  tier).
- **9 — spawn-error quality.** A hand-rolled per-platform quoting helper (**no new
  dependency** — not `shlex`), surfaced as `Command::command_line() -> String`;
  `Error::Spawn` embeds the rendered command in its `Display`; `current_dir` gets an
  existence pre-check in `build_tokio` for a clear error instead of an opaque spawn
  failure.

These are **recommendations** — confirm at implementation; nothing here is binding if
a better shape surfaces while building.

## What was NOT changed

No verb renames, no re-export wrapping, no field-privatization of the config structs
(public fields kept for read/mutate). The review's scope was deliberately
"freeze-proof + decide," not "redesign."
