# `decisions/` — settled decisions (won't do / won't change)

This directory holds **closed** decision records: proposals decided *against*, and
designs deliberately confirmed as-is. It is distinct from:

- [`../ROADMAP.md`](../ROADMAP.md) — committed near-term work.
- [`../ideas/`](../ideas/) — **open** proposals to reconsider (`next-` / `later-`).

The split exists so a rejected idea isn't re-derived from scratch, and so the open
idea backlog isn't cluttered with things already settled. A record here is not
immutable — a genuinely **new argument** (a concrete consumer, a changed
constraint) can reopen one by moving its substance back into `ideas/`.

## Contents

- **`architecture-audit-2026-06.md`** — the 2026-06 fresh-eyes architecture audit:
  what was rejected (bounded-buffer default, a formal `Job` trait, a `RunningProcess`
  state enum, generic `on_command` hooks, …) and what was confirmed sound (don't
  re-litigate without new arguments). Supersedes two retired records (full texts in
  git history).
- **`permissions-privileges-pty-network.md`** — the launch-permission / locks /
  run-as-user / SSH-tty / network sweep. Mostly shipped or declined; carries the
  **PTY design sketch** (deferred — the open follow-up lives at
  [`../ideas/later-pty-support.md`](../ideas/later-pty-support.md)) and the declined
  Windows run-as-user.
- **`wont-do-2026-06.md`** — the "won't do" verdicts from the 2026-06 development
  sweeps. 2026-06-09: built-in shell mode, IPC, detached-as-default, miri, object-mode
  streams. 2026-06-10: a `clear_env()` verb (redundant with `inherit_env([])`), `arg_if`
  builder sugar, env-var-*name* redaction, finer stdin flush knobs (PTY-subsumed).
- **`pre-1.0-api-review.md`** — the pre-1.0 public-API sweep: the `#[non_exhaustive]`
  additions (shipped 0.9.1) and the deliberate re-export-leak decisions
  (`CancellationToken`/`encoding_rs::Encoding` kept).
- **`readme-crate-doc-sourcing.md`** — why the README and the `lib.rs` crate doc stay
  separate (not `#![doc = include_str!]`): `include_str!` is lossy on docs.rs (relative
  links, cover image) and the two serve different audiences. Drift managed by review.
