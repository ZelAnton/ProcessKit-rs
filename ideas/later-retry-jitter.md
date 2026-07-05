# Retry backoff — optional jitter

> **Status:** open idea (later, low priority). Raised 2026-06-09 by the
> **vcs-toolkit-rs** team
> ([thread `T-20260609-vcs-processkit-feedback`](../../.hq/comms/threads/T-20260609-vcs-processkit-feedback/)).
> Small, additive, no concrete pain yet — backlog it.

## TL;DR

`retrying()` (in `src/runner.rs`) uses a **fixed** backoff between attempts: every
retry of a given run waits the same interval, with no randomization. When many
callers retry the same flaky dependency at once (a fleet of CI jobs hitting a rate-
limited registry, say), fixed backoff lets their retries stay phase-aligned — a
thundering herd that re-collides on each wave instead of spreading out.

The standard fix is **jitter**: perturb each wait by a random fraction so retries
decorrelate.

**Prior art — the crate already jitters, just not on this path.** `Supervisor`'s
restart backoff has a `jitter(bool)` knob (`src/supervisor.rs`, **default on**) backed
by a dependency-free `jitter_factor()` PRNG that multiplies each delay by a uniform
`[0.5, 1.5)` — built on `RandomState`'s fresh per-instance keys, no `rand` crate. So
this idea is **not** greenfield: it's extending the existing `jitter_factor()` to the
`retrying()` path, which today sleeps `RetryPolicy.backoff` verbatim with no
randomization.

## Shape (when built)

- A `RetryPolicy`-level `jitter` knob (`src/command.rs` `RetryPolicy` has only
  `max_attempts` / `backoff` / `classifier` today). Reuse the supervisor's
  `jitter_factor()` (factor multiply, the band the crate already uses) rather than
  inventing a second jitter scheme.
- **Mind the default-direction mismatch:** the supervisor jitters **on** by default
  (a restart storm is the common case there); `retrying()` should default jitter
  **off/zero** so the virtual-time backoff tests stay deterministic and current
  callers see no behavior change. Two subsystems, opposite defaults — deliberately,
  because the contention case differs. Document that when shipped.
- Randomness source: the existing `jitter_factor()` (no new dependency). If it moves
  out of `supervisor.rs` to be shared, keep it crate-private.

## Why low priority

Fixed backoff is correct for single-consumer retries; jitter only matters under
concurrent contention against a shared dependency, which no in-tree consumer has hit.
Additive and self-contained — it doesn't block anything and carries no design risk,
so it sits in the backlog until a herd actually forms.

## Revisit when

A consumer reports retry storms / correlated backoff against a shared rate-limited
service, or the `next-scheduling-knobs.md` work touches `RetryPolicy` anyway and can
fold this in cheaply.
