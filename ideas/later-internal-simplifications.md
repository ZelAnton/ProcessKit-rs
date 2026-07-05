# later: minor internal simplifications & doc-only clarifications

> **Status:** open idea (later, low priority). From the 2026-06-10 simplification +
> stabilization sweep. The headline finding was that the **public surface is clean and
> 1.0-ready** — no functionality-preserving simplification was large enough to justify
> churning it before the freeze. These are the only residual, internal, non-urgent items
> worth recording so the next audit doesn't re-derive them.

## Candidates

### A. Dedup the `from_spawned` / `from_scripted` field initializers
*Cost: moderate · Internal only*

`RunningProcess::from_spawned` (`src/running/mod.rs:269`) and `from_scripted` (`:307`)
share ~18 identical struct-literal field inits (`stdout_sink: None`, `stderr_pump: None`,
`deadline_task: None`, `started: Instant::now()`, …), differing only in `backend`, `pid`,
and `own_group`. The single honest internal-dedup candidate. **But** the two pull their
run-knobs from different sources (a `Spawned` struct vs a `&Command`), so a shared core
would still need both adapters — the win is modest and the current form is dead-obvious to
read. **Defer unless a third constructor appears** (then the shared helper earns itself).

### B. Doc-only clarifications (no code change)
*Cost: trivial · Doc only*

- `RunningProcess::start_kill` (`src/running/mod.rs:1024`) is the **only** non-consuming
  teardown verb (`&mut self`); every other consuming verb takes `self`. Correct (kill then
  `wait`, mirroring `tokio::process::Child`), but worth one line on the type so 1.0 freezes
  the asymmetry deliberately.
- `JobRunner::start` exists as both an inherent method (`runner.rs:185`) and the trait
  method (`runner.rs:199`); the inherent one is the no-trait-import path for
  `Command`/`CliClient`. Harmless and load-bearing — a one-line comment noting *why* both
  exist would pre-empt a future "why is this duplicated?" audit.

## Explicitly *not* doing (confirmed sound, recorded so they aren't re-litigated)

The sweep checked and **rejected churning** these — they look like inconsistencies but are
deliberate:
- `Command::to_tokio_command` naming / its `tokio::process::Command` return-type leak — an
  intrinsic, blessed escape hatch (same posture as the `Encoding`/`CancellationToken` leaks
  in [`../decisions/pre-1.0-api-review.md`](../decisions/pre-1.0-api-review.md) §3).
- `JobRunner` unit-struct literal *and* `::new()` — both are conventional ZST idioms.
- `ProcessResult::new`'s 6-arg positional `pub(crate)` constructor — internal, tested, and
  the cassette-equality invariant (excludes `duration`/`truncated`) is subtle; a builder
  would risk it for cosmetics.
- `quote_arg` unix/non-unix cfg-split (`command.rs`) — honest platform duplication, same
  category as the confirmed-sound `drive_to_exit_inner` split.

## Revisit when

Touching `running/mod.rs` constructors anyway (fold in A), or during the final pre-1.0 doc
pass (B). None block 1.0.
