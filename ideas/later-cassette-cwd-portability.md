# Cassette match key — make the `cwd` component portable

> **Status:** open idea (later). Raised 2026-06-09 by the **vcs-toolkit-rs** team
> while adopting the `record` feature
> ([thread `T-20260609-vcs-processkit-feedback`](../../.hq/comms/threads/T-20260609-vcs-processkit-feedback/)).
> Gated on a concrete cross-machine replay need — defer until a consumer hits it.

## TL;DR

The `RecordReplayRunner` match key is `(program, args, cwd, has_stdin)`, with `cwd`
stored as the **exact absolute path string**. A cassette recorded in
`/home/ci/work/repo` will not replay in `/Users/dev/checkout/repo` or
`C:\actions\repo` — the same logical run misses on the literal path. That makes a
cassette machine-bound: it can't be recorded on a developer's box and replayed in
CI, or shared across a team, which is much of the point of a hermetic fixture.

| Option | Verdict |
|---|---|
| **(a) Drop `cwd` from the match key** | Simplest; matches on `(program, args, has_stdin)`. Loses the ability to distinguish two runs that differ *only* by cwd — rare, and a recorded cassette is already scoped to one logical scenario. **Leading candidate.** |
| **(b) Normalize `cwd` to a path relative to a recording root** | Portable *and* preserves cwd distinctions, but needs a "root" concept the runner doesn't have today, and relative-path canonicalization is its own footgun. More machinery than the problem has earned. |
| **(c) Leave as-is, document the constraint** | Zero code; cassettes stay single-machine. Acceptable only until a real cross-machine consumer appears. |

## Why defer (for now)

No in-tree consumer records on one machine and replays on another yet; vcs-toolkit-rs
raised it as a forward-looking concern, not a current blocker. The house discipline
is to wait for a concrete need before adding a portability layer (cf.
[`later-runtime-agnostic.md`](later-runtime-agnostic.md), PTY in
[`later-pty-support.md`](later-pty-support.md)). When it lands, **(a)** is the
default unless a consumer demonstrates a real two-runs-differ-only-by-cwd case, in
which case **(b)** with an explicit `record_root` is the fallback.

## Revisit when

A consumer needs a cassette recorded on one machine (a dev box) to replay on another
(CI, a teammate). Confirm whether any scenario legitimately distinguishes runs by cwd
alone before choosing (a) vs (b); reply on the thread when decided.
