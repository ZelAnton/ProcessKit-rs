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
1.0 line is cut (it pairs with roadmap item 5, the API review). Wire it in
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

## Assessment

**(B) is the standout** — cheap, immediate, catches a real gap. (C) and (D) are easy
wins. (A) is genuinely valuable but timed to 1.0. (E) is the most effort for the
softest benefit. Suggested order when picked up: B → C → D → A (at 1.0) → E.

**Revisit:** (B)(C)(D) anytime; (A) when approaching the 1.0 API freeze (roadmap item 5).
