# Platform support

[‹ docs index](README.md)

`processkit` treats platform support as first-class: every capability is
either fully implemented, *honestly partial* (documented and typed), or
refused with `Error::Unsupported` — never silently skipped. This page
collects all the matrices and fine print in one place.

- [Containment mechanisms](#containment-mechanisms)
- [Capability matrices](#capability-matrices)
- [Caveats](#caveats)

## Containment mechanisms

`ProcessGroup::mechanism()` reports which one you actually got:

| `Mechanism` | Platform | How containment works |
|---|---|---|
| `JobObject` | Windows | A Job Object with kill-on-close; children are created suspended, assigned to the job, then resumed — so even a grandchild forked in the first instant is contained |
| `CgroupV2` | Linux (with delegation) | A private cgroup; children join in `pre_exec`, before `exec`, so descendants can never escape; teardown is `cgroup.kill` |
| `ProcessGroup` | macOS, BSDs, Linux fallback | POSIX process groups (`setpgid`); teardown is `killpg`; tracked per started/adopted child |
| `None` | anything else | No containment facility — children are *unmanaged*, not absent |

On Linux the cgroup backend requires controller **delegation** (root, a
container, or a systemd unit with `Delegate=yes`); without it the crate
quietly falls back to `ProcessGroup` — unless you requested
[resource limits](#capability-matrices), which fail fast instead
(`Error::ResourceLimit`), because an unapplied cap is no protection.

## Capability matrices

**Teardown & containment**

| Capability | Windows JobObject | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| Kill-on-drop, whole tree | ✅ | ✅ | ✅ groups-based | ✅ groups-based |
| Graceful `shutdown` (TERM → grace → KILL) | 🟡 atomic kill only | ✅ | ✅ | ✅ |
| `adopt` an external child | ✅ (future forks contained) | ✅ (future forks contained) | 🟡 exec'd child tracked individually | 🟡 same |

**Signals & freezing**

| Capability | Windows | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| Arbitrary signal (`Hup`, `Usr1`, `Other(n)`, …) | ❌ `Kill` only | ✅ | ✅ | ✅ |
| `suspend` / `resume` | 🟡 per-thread counts | ✅ `cgroup.freeze` | ✅ `SIGSTOP`/`CONT` | ✅ `SIGSTOP`/`CONT` |

**Inspection & accounting** (`stats` feature)

| Capability | Windows | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| `members()` | ✅ whole tree | ✅ whole tree | 🟡 leaders only | 🟡 leaders only |
| Group CPU / peak memory | ✅ | ✅ | ❌ count only | ❌ count only |
| Per-run `cpu_time` / `peak_memory_bytes` / `profile` | ✅ | ✅ | ✅ (`/proc`) | ❌ `None` |

**Resource limits** (`limits` feature)

| Capability | Windows | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| `memory_max` (whole tree) | ✅ | ✅ | ❌ | ❌ |
| `max_processes` | ✅ | ✅ | ❌ | ❌ |
| `cpu_quota` | 🟡 approximate | ✅ | ❌ | ❌ |

**Spawn-time controls**

| Capability | Windows | Unix (all) |
|---|---|---|
| `inherit_env` allow-list | ✅ | ✅ |
| `uid` / `gid` drop | ❌ `Unsupported` | ✅ |
| `setsid` | ❌ `Unsupported` | ✅ |
| `create_no_window` | ✅ | no-op |
| `kill_on_parent_death` | ✅ always on (kernel) | Linux: direct child; macOS/BSD: no-op |

Everything not listed — capture, streaming, interactive stdin, encodings,
buffer policies, timeouts, retry, pipelines, supervision, readiness probes,
the test doubles, cassettes, cancellation — is **platform-agnostic** and
behaves identically everywhere.

## Caveats

The honest fine print, mostly consequences of OS semantics:

**Linux cgroup delegation.** Creating the per-group cgroup needs write access
to the cgroup v2 hierarchy. Dev boxes typically lack it → the pgroup fallback.
CI inside containers usually has it. Check `mechanism()` when behavior must
not silently degrade.

**`uid()`/`gid()` × the cgroup mechanism.** The OS applies the uid drop
*before* `pre_exec` hooks, and the cgroup join runs in `pre_exec` — as the
already-dropped user, who can't write the root-owned `cgroup.procs`. The spawn
fails with a permission error (never an uncontained child). Privilege drop
composes cleanly with the process-group mechanism.

**`setsid()` × process groups.** A new session implies a new process group;
the crate coordinates the two (the containment tracking follows the new
session's group), so `setsid` keeps the kill-on-drop guarantee instead of
breaking out of it.

**`kill_on_parent_death()` is thread-scoped on Linux.** `PR_SET_PDEATHSIG`
fires when the spawning *thread* dies, not only the process. On a
multi-threaded tokio runtime a retired worker thread could kill the child
early; spawn from a current-thread runtime for the strongest guarantee. It
covers the **direct child only** — with the parent SIGKILLed, nothing tears
the cgroup/pgroup down, so grandchildren survive. The
parent-died-before-arming race is closed by re-checking `getppid()` in the
child against the spawner's pid captured before the fork — which stays
correct when the spawner itself is PID 1 (a container entrypoint).

**Windows: the suspended-spawn handshake.** Children are created
`CREATE_SUSPENDED`, assigned to the job, then resumed — closing the classic
race where a fast child forks before it's in the job. A consequence: a raw
`ProcessGroup::spawn` caller passing its own creation flags gets them OR'd
with `CREATE_SUSPENDED` (the `Command`-driven paths handle this for you, incl.
`create_no_window`).

**Windows: nested suspends.** `SuspendThread` keeps per-thread *counts* — two
`suspend()` calls need two `resume()`s. The POSIX backends are level-triggered
(idempotent). Suspension is also best-effort against a tree that is spawning
threads mid-walk.

**Spawning into a suspended cgroup group.** The freeze is group *state*: a
child spawned or adopted while suspended joins frozen — the forked child
joins the cgroup *before* `exec`, so it can freeze before completing the
spawn handshake and **`start()` may never return until resume**. Resume
before starting new work; details in
[Process groups](process-groups.md#suspending-and-resuming).

**Frozen trees and graceful shutdown.** Hard kills penetrate a frozen tree
(SIGKILL / `cgroup.kill` / job terminate), but a graceful `shutdown` leads
with a `SIGTERM` the frozen processes can't handle — it waits out the full
grace. Resume first.

**pgroup backends: leaders, zombies, pid reuse.** `members()` lists tracked
group leaders only; an exited-but-unreaped child (zombie) still probes as
alive (keep `wait()`ing handles if you need prompt liveness, e.g. for
`shutdown`'s early return); and pid-based signalling is inherently
best-effort against pid reuse — the crate prunes dead entries on every probe
to keep the window minimal.

**No-containment targets.** `Mechanism::None` means signals, suspension, and
membership are `Unsupported`/empty, and nothing outlives-protection exists —
the runner layer still works (spawn, capture, timeout the direct child).

---

Next: [Process groups](process-groups.md) ·
[Running commands](commands.md) ·
[docs index](README.md)
