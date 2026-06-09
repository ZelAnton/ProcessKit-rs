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

### B. `cargo-hack` feature-powerset
*Cost: trivial · Value: now*

CI tests three *fixed* feature configs (`--no-default-features`, default,
`--all-features`). With 7 features, combinations like `limits` without `stats`, or
`cancellation` + `record` alone, can break compilation undetected. `cargo hack
--feature-powerset --depth 2` (capped depth to bound runtime) catches feature-gate
mistakes the fixed configs miss. The cheapest real correctness gain here.

### C. `-Z minimal-versions` check
*Cost: trivial · Value: moderate*

Verify the crate actually builds with the *lowest* dependency versions its
`Cargo.toml` constraints allow — catches under-constrained deps (a `"1"` that really
needs `1.5`). Runs on nightly.

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

## Assessment

**(B) and (F) are the standouts** — both cheap, immediate, and serve the pre-1.0 window:
(B) catches a real feature-gate gap, (F) makes API drift visible. (C) and (D) are easy
wins. (A) is genuinely valuable but timed to 1.0. (E) is the most effort for the softest
benefit — but its `# Errors`/`# Panics` slice is worth cherry-picking early. Suggested
order when picked up: B → F → C → D → A (at 1.0) → E.

**Revisit:** (B)(C)(D)(F) anytime — promoted to ROADMAP item 4; (A) when approaching the
1.0 API freeze.
