# Runtime: tokio-only by design (no sync variant, no runtime abstraction)

**Decided 2026-06-14.** Recurring proposal: make async optional behind a feature
so callers who want a "simple" path don't pull an async runtime, and/or abstract
the runtime so `smol` / `async-std` users can use the crate without tokio.

**Verdict: not now, and not as a feature flag.** The benefits do not outweigh the
cost for *this* crate. Revisit only on a concrete consumer request (see reopening
below), not speculatively.

## Why

This crate is not a thin async wrapper — tokio *is* the engine, and the reliability
core (kill-on-drop tree containment, graceful-shutdown tiers, cancel/deadline
arbitration) is inherently concurrent. At the time of writing tokio is woven
through all of `src/`: ~46 `tokio::spawn` sites (the detached kill-on-drop
watchdogs and cancel/deadline tasks), ~64 `tokio::time` uses (including
`tokio::time::pause`, the virtual clock the hermetic tests stand on), ~21
`tokio::process` uses, plus the `select!` arbiter that closing Issue-7 turned on.

The public API is *already* tokio-typed in several places (a deliberate
re-export-leak decision — see [`pre-1.0-api-review.md`](pre-1.0-api-review.md)):
`tokio_util::CancellationToken` (`cancel_on` / `default_cancel_on`),
`tokio::io::AsyncWrite` (`stdout_tee` / `stderr_tee`), `tokio::io::AsyncRead`
(`Stdin::from_reader`), and `tokio::process::{Child, Command}`
(`ProcessGroup::adopt` / `spawn`). So an async-as-a-feature flag would not gate one
API — it would require a *second, differently-typed public API* (a different
cancellation type, different tee bounds, different adopt/spawn signatures).

### Runtime abstraction (smol / async-std) — rejected

A near-total rewrite of the engine, not a flag. It would have to abstract, behind
per-runtime trait impls: detached task spawning (the heart of the kill-on-drop
guarantee, fundamentally runtime-coupled), the child-process type (smol's
`async-process` has a different API *and different Drop semantics* — and Drop
semantics are load-bearing here), timers (no cross-runtime `pause` for the tests),
and the `select!` arbiter. The payoff (smol users) is small in a tokio-dominated
ecosystem, and the cost lands precisely on the reliability core — the crate's main
value — which would then need validating across N runtimes.

### Sync / "simple" variant — rejected

The simplicity is illusory: even "run it and grab the output" needs concurrent
draining of stdout+stderr (the classic pipe-fill deadlock), i.e. at least two
threads plus a deadline-watcher thread. Two ways to do it, both bad:

- **A real thread-based sync core** is a *second implementation of the most
  safety-critical part of the crate* (the concurrency arbitration). It doubles the
  test surface and guarantees drift — a bug fixed in one path lingers in the other.
- **A `block_on` facade over the async core** is cheap but *keeps tokio as a
  dependency*, so it does not serve "no tokio" callers — only "no async syntax"
  ones — a thin benefit with real footguns (panic on `block_on` from an async
  context, nested runtimes). And it still hits the typed-API problem above.

"No users yet" argues *against* building this, not for it: a runtime abstraction
is YAGNI at its most expensive when the cost is huge and concentrated in the core.

## What we do instead

- Keep the crate tokio-only and say so.
- "Simple" as *ergonomics* (not sync) is already served by one-liners
  (`Command::checked`, `run_unit`); more sugar there is cheap and doesn't touch the
  core.
- Don't widen the tokio leak in the public API without need — where we return/accept
  our own types, keep them. That preserves the option of a future facade cleanly.

## Reopening condition

A concrete consumer that (a) genuinely cannot adopt tokio and (b) needs this crate's
differentiators (tree containment / graceful tiers / cancellation), not just
`std::process` / `duct`. At that point move the substance into `ideas/` and design
the runtime seam against that real shape — most likely starting from a `block_on`
facade (smallest blast radius), not a full multi-runtime abstraction.
