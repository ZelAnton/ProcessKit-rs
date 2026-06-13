# next: CI & quality hardening

> **Status:** open idea (next). From the 2026-06-09 conventions audit. The existing
> CI is already strong — fmt, clippy `-D warnings` × 3 feature configs × 3 OSes,
> a test matrix that runs `--include-ignored` across feature configs, an MSRV job
> with cross-target checks, `cargo deny`, nightly-docsrs doc builds, and a separate
> nightly stress tier. These are the **additive** gates it lacks.

## Candidates

### A. `cargo-semver-checks`
*Cost: trivial to wire · Value: post-1.0*

Detect accidental SemVer-breaking API changes between releases. **Deliberately
`next-`, not now:** pre-1.0 we break the API freely, so it has little value until the
1.0 line is cut (the pre-1.0 API review already shipped in 0.9.1; semver-checks is the
*post*-freeze enforcement tool). Wire it in
informational mode around 1.0, enforcing after.

### B. `cargo-hack` feature-powerset — ✅ SHIPPED (2026-06-10)
*Cost: trivial · Value: now*

CI tests three *fixed* feature configs (`--no-default-features`, default,
`--all-features`). With 6 features, combinations like `limits` without `stats`, or
`record` alone, can break compilation undetected. `cargo hack
--feature-powerset --depth 2` catches feature-gate mistakes the fixed configs miss.
**Added as the `hack` CI job** — all 34 ≤2-feature combinations check clean today.

### C. `-Z minimal-versions` check — BLOCKED (found real under-constraints)
*Cost: trivial to wire · Value: moderate · Status: blocked on a dep lower-bound audit*

Verify the crate builds with the *lowest* dependency versions its `Cargo.toml`
constraints allow. Tried 2026-06-10 and it **found genuine under-constraints** — so
wiring it now would red CI until the bounds are fixed:
- `-Z minimal-versions` fails compiling `async-trait` at its minimal: a *transitive*
  proc-macro2/quote/syn-too-old issue in a crate we don't control (the long-standing
  full-minimal ecosystem problem).
- `-Z direct-minimal-versions` (only our direct deps; the robust variant) fails at
  resolution on **`tokio-stream`**: our `"0.1"` predates the `io-util` feature we use,
  so the direct minimum `0.1.0` can't satisfy it.

The fix is a deliberate lower-bound audit (raise `tokio-stream` to the version that
introduced `io-util`, then iterate until `direct-minimal-versions` resolves) — its own
small task, against the AGENTS "loose major pin" policy. **Do that first**, then wire
`-Z direct-minimal-versions` (not full minimal-versions) as the CI job.

### D. Code coverage + badge
*Borrow: ubiquitous · Cost: trivial · Value: moderate*

`cargo llvm-cov` with a Codecov/Coveralls upload + README badge. Caveat: much of the
suite is `#[ignore]`d real-subprocess tests, so coverage must run `--include-ignored`
to be meaningful, and platform-specific `sys/` code only shows on its own OS.

### E. clippy pedantic/nursery + `clippy.toml`
*Cost: moderate (triage) · Value: moderate*

Opt into `clippy::pedantic` (selectively — pedantic is noisy) and a `clippy.toml` for
complexity thresholds. One-time triage cost; ongoing quality signal.

**Cherry-pick first: `# Errors` / `# Panics` doc sections.** The highest-value slice of
pedantic for *this* crate is `clippy::missing_errors_doc` + `missing_panics_doc` — it has
a rich typed `Error` with documented invariants (Timeout *captured* vs Cancelled *raised*,
NotReady ≠ Timeout) that live in the guides but not in the per-item rustdoc a docs.rs
reader hits first (Rust API Guidelines C-FAILURE). Worth enabling these two lints and
adding the blocks even if full pedantic isn't adopted.

### F. `cargo-public-api` surface snapshot (the pre-1.0 complement to A)
*Borrow: de-facto pre-1.0 surface-tracking tool · Cost: trivial · Value: now*

Commit a `cargo-public-api` snapshot (`public-api.txt`) and diff it in CI, so every PR
shows the public-surface delta as a reviewable artifact **while we still break freely**.
It's a *review aid*, not a gate (a changed snapshot is expected pre-1.0) — value is
visibility, directly serving the "get the shape right before 1.0" mandate. Distinct from
(A) `cargo-semver-checks`, which *enforces* and pays off only post-freeze: F is for now,
A is for ~1.0. The two are complementary, not redundant.

> **Sequencing (2026-06-10):** best added **after** the in-flight pre-1.0 API work
> settles (the output-handling redesign + `which`/PATH resolution), so the committed
> baseline doesn't churn every PR while the surface is actively changing. Wire it once
> those land.

## Assessment

**(B) and (F) are the standouts** — both cheap, immediate, and serve the pre-1.0 window:
(B) catches a real feature-gate gap, (F) makes API drift visible. (C) and (D) are easy
wins. (A) is genuinely valuable but timed to 1.0. (E) is the most effort for the softest
benefit — but its `# Errors`/`# Panics` slice is worth cherry-picking early. Suggested
order when picked up: B → F → C → D → A (at 1.0) → E.

**Revisit:** (B)(C)(D)(F) anytime — promoted to ROADMAP item 4; (A) when approaching the
1.0 API freeze.
