# later: a consumer-pluggable buffer policy (redaction-at-capture)

> **Status:** open idea (later). From the 2026-06-10 extensibility sweep. Gated on a
> concrete consumer — most plausibly a redaction-at-capture need.
>
> **Note (2026-06-29):** this idea is about redacting/transforming captured **output**
> (a `BufferPolicy` seam on the pump). The separate `Secret`-on-`env` **input** strand
> that `next-vcs-toolkit-feedback.md` loosely filed here was decided independently —
> deferred, see [`../decisions/secret-type-deferral-2026-06.md`](../decisions/secret-type-deferral-2026-06.md).
> That decision does **not** close this output-redaction idea, which remains open.

## The gap

`OutputBufferPolicy` / `OverflowMode` are public and configurable, but a consumer can only
pick from the crate's **built-in** modes (unbounded / `DropOldest` / `DropNewest`, and the
proposed `Error` ceiling — see [`next-output-handling.md`](next-output-handling.md) E).
A consumer who wants capture-*influencing* behavior — "redact lines matching a secret
pattern as they're captured", "fold to a custom ring with my own eviction" — has no seam.
The `on_stdout_line`/`on_stderr_line` push handlers see each line but run **alongside**
capture, not in front of it, so they can observe but not *shape* what gets retained.

## Why this is allowed (vs the rejected generic hooks)

The architecture audit ([`../decisions/architecture-audit-2026-06.md`](../decisions/architecture-audit-2026-06.md))
rejected generic `on_command`/`default_map` stored-closure hooks — but it *accepts* typed,
narrow seams (that's exactly what `ProcessRunner` is). A `trait BufferPolicy` on the one
well-defined extension point (the in-memory backlog the pump writes to) is the same shape:
a single, named, introspectable boundary — not an opaque per-command closure grab-bag. So
this doesn't reopen the settled decision; it's a different, narrower seam.

## Assessment

Real but **speculative** — no consumer has asked. The existing `on_*_line` handlers +
encoding overrides cover most line-*transform* needs (the audit cited this when rejecting
execa object-mode). The genuinely-uncovered case is *capture-influencing* policy, and the
most plausible trigger is **redaction-at-capture**, which dovetails with the crate's
secret-hygiene posture (cassette stores env *names* only; argv/env values never logged).
Moderate cost: the pump owns buffer writes, so a trait boundary there is real surface.

## Revisit when

A consumer needs to redact/transform output *as it is captured* (not just observe it), or
wants a custom eviction policy the built-in modes don't express. Until then the built-in
`OverflowMode` set + `on_*_line` handlers suffice.
