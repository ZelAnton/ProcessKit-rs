# next: executable doctests through the hermetic seam

> **Status:** open idea (next). From the 2026-06-10 docs sweep. Promoted to the
> ROADMAP as item 5 — this file is the detail/rationale behind it.

## The gap

All 13 fenced code examples in `src/` are `no_run`: they compile (so a doctest catches a
**signature/type** break) but never execute, so they cannot catch a **runtime** regression.
For a process-management crate most examples *can't* run in a doctest sandbox — they'd
spawn real children, touch the network, or need a real OS mechanism. That's the honest
reason `no_run` is there.

But the crate ships exactly the tool to make a subset runnable hermetically: the
`ProcessRunner` seam + `ScriptedRunner` (`src/doubles.rs`), which drives canned replies
through the *real* pump/capture machinery with **no subprocess**. `docs/testing.md` sells
this to consumers ("no subprocess in your tests") — yet the API docs don't eat their own
dog food.

## Shape

Convert 2–4 of the most-read examples to **execute** (` ```rust ` not ` ```rust,no_run `)
by routing them through `ScriptedRunner`:

- `output_string` / `run` against a canned reply (assert the captured text).
- `probe` returning the 0/1 result from a scripted exit code.
- `Pipeline` pipefail attribution (a scripted non-zero middle stage) — high-value because
  the attribution logic is subtle and worth a *running* example.

These then run in `cargo test` on every OS, hermetically, and double as living proof the
seam works as documented.

## Assessment

Cheap, in-identity (uses the crate's own testing story), and strictly additive. Genuinely
new — not in [`later-advanced-testing.md`](later-advanced-testing.md) (proptest/fuzz/loom)
nor the ROADMAP item-1 integration tests. Lower urgency than the graceful-timeout coverage
gap, hence a clean doc-quality win rather than a correctness fix.

**Revisit:** alongside any docs pass, or when touching `ScriptedRunner`.
