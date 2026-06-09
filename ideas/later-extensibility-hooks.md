# later: extensibility hooks — before_spawn mutator, dry-run

> **Status:** open idea (later). From the 2026-06-09 sweep. Extensibility seams that
> need a **deliberate decision**, because the architecture audit already drew a line
> nearby — generic stored closures were rejected once
> ([`../decisions/architecture-audit-2026-06.md`](../decisions/architecture-audit-2026-06.md):
> the `on_command`/`default_map` rejection). These are different in kind, but the
> precedent means they warrant their own verdict rather than a casual add.

## Candidates

### A. `before_spawn` — mutate the raw `tokio::process::Command` per launch
*Borrow: duct `before_spawn` · Cost: moderate*

An escape hatch to set platform knobs ProcessKit doesn't model (a niche creation
flag, a `pre_exec` of one's own) **without losing containment + the pump**.
`to_tokio_command()` exists but bypasses `ProcessGroup::spawn` entirely — this would
be a hook *inside* the high-level launch path. **The tension:** the audit rejected
generic per-command closures for *defaults* (introspection/Debug/cassette
transparency). A raw-`Command` mutator is narrower (an explicit per-spawn escape, not
a stored default-policy closure) — but it still adds an opaque closure to a builder
that prides itself on being inspectable. Needs a yes/no with that trade-off stated.

### B. Dry-run / echo mode — resolve and render, don't spawn
*Borrow: execa dry-run, zx/xshell/cmd_lib echo · Cost: trivial–moderate*

Print the rendered command (reusing the roadmap's shell-quoting helper) and return a
synthetic result **without spawning** — for `--dry-run` flags in tools built on
ProcessKit. Largely composes the existing `ProcessRunner` seam (a `DryRunRunner`
beside `ScriptedRunner`), so cost is low; the open question is whether it's a runner,
a `Command` flag, or both. Lower urgency because the mock seam already lets callers
fake this today.

## Assessment

(B) is benign and mostly compositional — likely a small `DryRunRunner` plus the
quoting helper once that lands. (A) is the one that needs a real decision and may end
up **rejected** (moved to `decisions/`) on the same inspectability grounds as
`on_command`, or accepted as a deliberately-narrow escape hatch. Either way, don't
add it casually.

**Revisit:** (B) opportunistically after the quoting helper (roadmap item 9); (A)
when a consumer hits a platform knob the crate genuinely can't model — then decide.
