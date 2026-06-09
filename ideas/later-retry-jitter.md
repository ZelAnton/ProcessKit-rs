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
decorrelate. The ask is to make it an opt-in knob, **defaulting to zero** (today's
deterministic behavior, which keeps the backoff-timing tests on the paused clock
reproducible).

## Shape (when built)

- A `RetryPolicy`-level `jitter` knob (e.g. a fraction `0.0..=1.0` of the computed
  delay, or full/equal jitter à la the AWS "Exponential Backoff and Jitter" note).
- **Default zero** — no behavior change for current callers; the
  virtual-time supervisor tests that assert exact backoff intervals stay green.
- Randomness source: a small, dependency-free PRNG seeded per-run, or gate the RNG
  so the core stays lean. Avoid pulling `rand` into the default build for one knob.

## Why low priority

Fixed backoff is correct for single-consumer retries; jitter only matters under
concurrent contention against a shared dependency, which no in-tree consumer has hit.
Additive and self-contained — it doesn't block anything and carries no design risk,
so it sits in the backlog until a herd actually forms.

## Revisit when

A consumer reports retry storms / correlated backoff against a shared rate-limited
service, or the `next-scheduling-knobs.md` work touches `RetryPolicy` anyway and can
fold this in cheaply.
