# later: one deliberate "outlive the parent" escape hatch

> **Status:** open idea (later — borderline). From the 2026-06-09 sweep. Kept in the
> open backlog rather than rejected outright, but it sits **against the grain** of the
> crate's identity, so it's deliberately low-priority and tightly scoped. The
> "detached *as a default/headline* feature" framing is already rejected in
> [`../decisions/wont-do-2026-06.md`](../decisions/wont-do-2026-06.md) — this is only
> the narrow, opt-in version.

## The idea

ProcessKit is **kill-on-drop by design**: the whole value proposition is that nothing
escapes. But there are legitimate handoff scenarios — daemonizing, spawning a
long-lived helper that should survive the launcher, a deliberate `nohup`-style
detach. Today the only way is to *not hold the handle*, which is implicit and
undocumented. Borrow: execa `detached`, plumbum `nohup`, `setsid`, systemd-run
`--scope`.

## Why this is dangerous to get wrong

- It **inverts the headline guarantee.** If `detached` is easy or default, the crate
  stops being trustworthy as a containment tool — the exact opposite of why it exists.
- On Windows, the Job Object kills the tree on handle close; a true detach needs
  `JOB_OBJECT_LIMIT_BREAKAWAY_OK` / breakaway or *not* assigning to the job at all —
  a real divergence from the spawn path, not a flag flip.

## If it's ever built

- **One** explicit, loudly-named, documented verb (e.g. `spawn_detached` /
  `release`) — never a `Command` default, never reachable by accident.
- Crystal-clear docs that this child is **no longer contained** and the caller owns
  its lifetime.
- Consider making it return something that *can't* be confused with a normal
  contained handle.

## Assessment

Low priority and treated with suspicion. It's in `ideas/` (not `decisions/`) only
because the *narrow opt-in* form is a legitimate occasional need — but the bar to
implement is "a real consumer with a real daemon/handoff use case," and even then it
ships as a single deliberate escape hatch with the guarantee-inversion spelled out.

**Revisit when:** a concrete consumer needs a child to intentionally outlive the
launcher — and not before.
