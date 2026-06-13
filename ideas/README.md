# `ideas/` — open proposals not yet committed

This directory holds **open** development ideas: things worth doing eventually but
not committed to the near-term [`ROADMAP.md`](../ROADMAP.md). Each file is a small
decision record (status header → TL;DR → critical assessment → revisit condition).

## The four buckets

A development sweep classifies every candidate into one of four homes:

| Bucket | Meaning | Lives in |
|---|---|---|
| **Today** | Committed; will do | [`../ROADMAP.md`](../ROADMAP.md) |
| **Next** | Open; reconsider **first** when the roadmap drains | `ideas/next-*.md` |
| **Later** | Open; further out, lower urgency | `ideas/later-*.md` |
| **Won't do** | Settled against (or won't change) | [`../decisions/`](../decisions/) |

"Next / Later" are hyperbole for ordering, not calendar dates — **next-** items
are simply the first re-examined once committed work is done.

## Filename marker

The horizon is encoded in the **filename prefix**:

- `next-<topic>.md` — reconsider first (high value, just below the cut).
- `later-<topic>.md` — further out, or gated on a concrete consumer.

When an idea graduates to committed work, move its substance into `ROADMAP.md` and
either delete the file or leave a one-line pointer. When an idea is rejected
outright, move it to [`../decisions/`](../decisions/).

## Current contents

**Next:**
- `next-output-handling.md` — Stdio inherit/null modes, output tee, merged
  stdout+stderr ordering, unified event stream.
- `next-launch-ergonomics.md` — `which`/PATH resolution, bulk env, cwd
  conveniences, `send_control`.
- `next-scheduling-knobs.md` — nice/priority, ionice, umask, Windows priority class.
- `next-ci-and-quality-hardening.md` — cargo-hack feature-powerset, minimal-versions,
  coverage, cargo-public-api snapshot, cargo-semver-checks (at 1.0), clippy pedantic.
- `next-doctests-hermetic.md` — make the most-read `no_run` doctests *execute* through
  `ScriptedRunner` (ROADMAP item 5).

**Later:**
- `later-runtime-agnostic.md` — decouple from tokio (gated on a non-tokio consumer).
- `later-lite-build-sys-split.md` — a sync, no-tokio `processkit-sys` core.
- `later-pty-support.md` — pseudo-terminal for prompt-driven tools (design sketch in
  `decisions/permissions-privileges-pty-network.md`).
- `later-extensibility-hooks.md` — `before_spawn` raw-Command mutator, dry-run mode.
- `later-advanced-testing.md` — proptest/fuzz/loom, cross-platform leak checks, the
  full risk-zone inventory.
- `later-observability-and-docs-site.md` — `metrics` feature, mdBook docs site.
- `later-detached-handoff.md` — one deliberate "outlive the parent" escape hatch.
- `later-cassette-cwd-portability.md` — make the `record` cassette match key
  portable across machines (gated on a cross-machine replay consumer; from
  vcs-toolkit-rs feedback).
- `later-retry-jitter.md` — optional, default-zero jitter on `retrying()` backoff
  (from vcs-toolkit-rs feedback).
- `later-buffer-policy-seam.md` — a consumer-pluggable `BufferPolicy` seam
  (redaction-at-capture; gated on a concrete consumer).
- `later-internal-simplifications.md` — minor internal dedup + doc-only clarifications
  (the public surface was found 1.0-ready; these are the residuals).
