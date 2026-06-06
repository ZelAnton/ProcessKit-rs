# Idea: three-layer split — extracting resource measurement & limits

> **Status:** idea / not committed. Captured 2026-06-05 for possible future work.
> Nothing here is scheduled; this is a design note so we can pick it up later
> without re-deriving the analysis.
>
> **2026-06-06 decision record appended at the bottom** — a full
> "light base vs containment" packaging analysis was run; the outcome (monolith
> + visibility features, no crate split yet) is recorded there with the facts
> that drove it. Read that section before reopening this topic.

## Motivation

`processkit` now both *reads* a group's resource usage (`ProcessGroupStats`) and
*bounds* it (`ResourceLimits`, shipped in 0.6.x). The question this note answers:
should that "assess & manage resources" functionality become its own crate (a
workspace member in this repo), and if so, how — without an awkward seam?

The honest finding: resource stats/limits are today **properties of the containment
object**, not a separable layer. They are applied inside `Job::new()` and read back
from the same `Job`. A naïve "move the files into a new crate" would force the core
crate to make its platform `Job`/`sys` seam public (a large, unstable, FFI-heavy API
surface) or duplicate the FFI. So the split only pays off with the *right* layering —
described below — and only once there's a reason to do it.

## The deciding question

What is the resource functionality actually attached to?

1. **"Measure/limit a tree we spawned and contain."** — what exists today. Inseparable
   from `ProcessGroup`. Splitting here invents a seam where there is none.
2. **"Measure/limit *any* process / cgroup / job by handle — without spawning it."**
   (`measure(pid)`, attach a cap to an already-running process, sample over time.)
   This is a genuinely separate, independently-useful library with potential
   consumers beyond `processkit`. **This** is what justifies a crate.

Today the code is (1). Turning it into (2) is not a file move — it's designing a new,
broader API that operates on foreign processes, with real platform limits (see
[Hard truths](#hard-truths-per-platform)).

## Target architecture (three layers)

Share a **foundation**, not a "core". The common layer is the platform primitives,
not the high-level process-group type.

```
processkit-sys         # platform primitives + shared data types (FFI lives here)
   ├── processkit            # spawn + kill-on-drop containment + run-and-capture
   └── processkit-resource   # generic measure/limit for any pid / cgroup / job
```

- **`processkit-sys`** (low-level, the only crate with `windows-sys` / `libc`):
  - Platform backends: today's `sys/{windows,linux,unix,pgroup,other}.rs`.
  - The `Job` container abstraction: create (with `ResourceLimits`), `spawn`,
    `adopt`, `kill_all`, `graceful_shutdown`, `stats`, `mechanism`.
  - Free-standing primitives the resource crate needs: `process_metrics(pid)` and
    (new) limit-application to an *existing* container/process.
  - **Shared data types live here** so both upper crates use the same ones:
    `Mechanism`, `ResourceLimits`, the usage struct (today `ProcessGroupStats` —
    likely rename to a neutral `ResourceUsage` / `Metrics`).
  - Documented but explicitly low-level ("you probably want `processkit`").

- **`processkit`** (unchanged externally): `group.rs`, `command.rs`, `running.rs`,
  `runner.rs`, `client.rs`, `pump.rs`, `stdin.rs`, `buffer.rs`, `error.rs`,
  `result.rs`, `doubles.rs`. Depends on `processkit-sys` for `Job`, **re-exports**
  `Mechanism` / `ResourceLimits` / the usage type so existing consumers see no
  change.

- **`processkit-resource`** (new capability): operate on processes we did *not*
  spawn — read metrics for an arbitrary pid, attach/adjust caps on an existing
  cgroup / Windows job (via assign) where the platform allows, sample usage over
  time. Builds on `processkit-sys`; does **not** depend on `processkit`.

### File → crate mapping (mechanical part)

| Current | Lands in |
|---|---|
| `src/sys/*.rs` | `processkit-sys` |
| `src/mechanism.rs` | `processkit-sys` (shared type) |
| `src/limits.rs` (`ResourceLimits`) | `processkit-sys` (shared type) |
| `src/stats.rs` (`ProcessGroupStats`) | `processkit-sys` (rename → `ResourceUsage`?) |
| `src/group.rs`, `command.rs`, `running.rs`, `runner.rs`, `client.rs`, `pump.rs`, `stdin.rs`, `buffer.rs`, `error.rs`, `result.rs`, `doubles.rs` | `processkit` |
| *(new)* generic measure/limit-any API | `processkit-resource` |

## Hard truths (per platform)

The resource crate's "operate on a process we didn't spawn" promise is **not
uniformly available** — this must be documented, not papered over:

- **Linux cgroup v2:** can move an existing pid into a cgroup and cap it — works,
  subject to the same delegation / "no internal processes" constraint already
  documented for limits today.
- **Windows Job Objects:** can `AssignProcessToJobObject` a *running* process to a
  fresh job and then cap it — works, but a process can only belong to one job
  hierarchy; assigning an already-jobbed process may fail on older Windows.
- **POSIX (macOS/BSD, Linux pgroup fallback):** `setrlimit` only applies to *self*
  pre-exec — you **cannot** retroactively cap an arbitrary foreign process. Reading
  metrics for a foreign pid is also `/proc`-only (absent on macOS/BSD). So the
  general-purpose crate is honestly Linux+Windows-first, degrading elsewhere.

This asymmetry is itself an argument: the foreign-process story is where a dedicated
crate earns its keep (it's non-trivial and worth isolating), but it can't pretend to
be fully cross-platform.

## Cost / risk of splitting

- **3× release & maintenance overhead:** versioning, the release workflow (currently
  single-crate), MSRV tracking, CI matrix, docs.rs, and inter-crate version pinning.
- **Stability commitment on `sys`:** what is freely-churning `pub(crate)` FFI today
  becomes a published, semver-bound surface.
- **Churn right after 0.6.1:** a restructure now buys nothing without a consumer.

## Cheaper interim step (do this first if the goal is just modularity)

If the real motive is *optionality* (don't pull FFI/`windows-sys`/`libc` paths for
users who don't need stats/limits) rather than *reuse outside processkit*, solve it
in-crate with Cargo features — no split, ~90% of the benefit:

```toml
[features]
default = ["stats"]
stats  = []          # ProcessGroupStats + process_metrics
limits = ["stats"]   # ResourceLimits
```

Keeping the `sys` module boundary clean (as it already is) means a later extraction
stays mechanical.

## Migration plan (phased, low-risk) — when triggered

1. **Tighten boundaries now (cheap):** keep `sys` self-contained; make sure the upper
   layers only touch it via the `Job` wrapper + free functions; treat
   `Mechanism`/`ResourceLimits`/usage as "foundation" types conceptually.
2. **(Optional) features dry-run:** introduce `stats`/`limits` features to validate
   the optionality story without a workspace.
3. **Extract `processkit-sys` first (mechanical):** create the workspace, move `sys/*`
   + shared types; `processkit` depends on it and **re-exports** — zero consumer break,
   one coordinated release.
4. **Build `processkit-resource` (the real new work):** design the foreign-process
   API on top of `processkit-sys`, with the platform caveats above made explicit.

## Triggers — do it when, not before

- A concrete need to **measure/limit processes we didn't spawn** (the only thing
  `processkit` structurally can't already do).
- `sys` grows large enough that the internal coupling genuinely hurts.
- A wish to ship resource tooling on an **independent cadence / dependency set**.

Until one of these is true: keep it in `processkit` (optionally behind features). The
extraction is then a small, well-understood follow-up rather than a speculative
restructure.

## Open questions to settle before committing

- Rename `ProcessGroupStats` → a process-neutral `ResourceUsage`/`Metrics` in the
  foundation? (It's currently named for the group.)
- Does `processkit-resource` expose a time-series **sampler** (ties into roadmap item
  4) — and if so, does the sampler live there or stay in `processkit`?
- Workspace release orchestration: extend the existing single-crate release workflow,
  or publish `-sys` manually on its own slower cadence?
- MSRV: can the foundation hold a lower MSRV than the runner layer, or keep them in
  lockstep (current 1.88)?

---

## 2026-06-06 decision record — "light base" packaging analysis

A second split axis was evaluated: a LIGHTWEIGHT base crate (spawn + stdio +
capture/streaming + timeouts — "what most users need") with containment/
resources layered on top. Two design agents + fact-checking produced these
findings; the decisions below were taken explicitly.

### Facts established

1. **tokio itself depends on `libc` (unix) and `windows-sys` (Windows, via
   mio and tokio's own bindings).** An "FFI-free base" therefore removes ZERO
   crates from a consumer's dependency tree — only our own ~1.7k lines of sys
   code stop compiling. The dependency-weight argument for a light base is an
   illusion. (Bonus found: our windows-sys 0.59 duplicated tokio's 0.61 in the
   lockfile — fixed by bumping to 0.61.)
2. **A base without our sys layer can only kill the DIRECT child** on
   timeout/drop/cancel (`kill_on_drop`); grandchildren survive. That silently
   downgrades the crate's headline no-orphan guarantee exactly in the cases it
   exists for (`cargo build`'s rustc children, wrapper scripts, `cmd /c`).
   It also creates a feature-unification hazard: kill semantics would depend
   on whether ANYONE in the dependency graph enabled the tree feature.
3. **The "one name + optional sibling re-export" packaging (processkit light,
   `tree` feature pulls processkit-tree which depends back on processkit) does
   not compile** — Cargo forbids package-graph cycles even via optional deps.
   Workable packagings are: a 3-crate facade (expensive), a sys-foundation
   below (this note's original plan), or a monolith with features.
4. The genuinely real pain ("too much in one crate") is **visible API
   surface**, which feature gates solve in-crate without any of the above.

### Decisions (owner-confirmed)

- **Base-A (direct-child light mode) — REJECTED.** The tree guarantee is the
  crate's identity; its price (two semantics, doubled test matrix forever,
  unification hazard) buys an illusory dependency win (see fact 1).
- **Crate split — DEFERRED, unchanged triggers.** The sys-foundation split
  (this note's plan) remains the right mechanical move WHEN a trigger fires
  (foreign-process measurement / independent FFI cadence). The new fact 1
  *weakens* the "light core" motivation further: tokio already carries the FFI.
- **ADOPTED: monolith + ONE visibility feature** (shipped in the same change
  as this record): `process-control` (default-ON) gates `Signal` and
  `ProcessGroup::{signal, suspend, resume, members, adopt}` — kept because it
  matches the exact layer a future `processkit-sys` split would carve out.
  The flag is additive and hides API only; the kill-on-drop tree guarantee is
  unconditional in every configuration.
- **TRIED AND ROLLED BACK in the same change: zero-dep visibility gates for
  `pipeline` / `supervisor` / `client` / `doubles`.** They were implemented,
  then reverted on honest re-assessment: each was `= []` (removed no
  dependency, saved sub-second compile time) while costing ~30 scattered
  `cfg` attributes, a downgrade of a dozen intra-doc links to plain code (a
  daily docs-UX regression), and a permanently larger test matrix. The same
  "illusory win, real cost" logic that killed Base-A applies. Don't re-add
  zero-dep visibility gates unless the gated code grows a real dependency or
  the sys-split materializes.

### If this topic is reopened

Start from fact 1 (tokio→FFI) and fact 3 (the cycle): any future split must
be the sys-foundation shape (downward optional dep), and a "light" config
must keep the tree guarantee or fail loudly — never silently degrade it.
