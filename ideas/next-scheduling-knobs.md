# next: process scheduling & resource knobs

> **Status:** open idea (next). From the 2026-06-09 cross-language sweep. These sit
> naturally beside the existing privilege (`uid`/`gid`/`groups`/`setsid`) and
> `limits` features, reusing the **same seams**: the Unix `pre_exec` hook and the
> Windows `creation_flags` extra. That existing infrastructure is why they're cheap.

## Candidates

### A. Process priority — `nice` (Unix) / priority class (Windows)
*Borrow: Unix `nice`, systemd `Nice=`, Windows priority classes · Cost: moderate*

Launch background/batch children at lower CPU priority so they don't starve the
foreground. Unix: `setpriority` in `pre_exec`. Windows: a priority-class creation
flag via the existing `creation_flags` seam. Natural companion to `limits`.

### B. I/O priority — `ionice` (Linux)
*Borrow: `ionice`, systemd `IOSchedulingClass=` · Cost: moderate*

`ioprio_set` in `pre_exec` (Linux-only; gate elsewhere as `Unsupported`, matching
the `uid`/`gid` pattern). Lower value than (A) but cheap once (A)'s plumbing exists.

### C. `umask` for the child (Unix)
*Borrow: mixlib-shellout `umask` · Cost: trivial*

Control the permissions of files the child creates — another `pre_exec` knob
alongside `setsid`/`groups`. Trivial.

## Assessment

Good scope fit (resource governance is already a crate theme via `limits`) and cheap
because the platform seams exist. **But no concrete consumer** has asked, so they're
`next-`, not committed. Likely a single small change once one is wanted — implement
(A)+(C) together (both `pre_exec`/flag knobs), add (B) if Linux I/O-priority is
specifically needed. Apply the same non-Unix `Error::Unsupported` gate the existing
privilege builders use, so a knob is never silently ignored.

**Revisit:** when a consumer needs to run background children politely, or alongside
the next `limits` expansion.
