# Platform support

[‹ docs index](README.md)

`processkit` supports **Unix and Windows only** — it requires `tokio::process`
and OS job / process-group primitives that have no equivalent on bare targets
like wasm. Building for such a target fails at compile time (a `compile_error!`
guard, or earlier in tokio's own dependencies). Within the supported set, it
treats platform support as first-class: every capability is either fully
implemented, *honestly partial* (documented and typed), or refused with
`Error::Unsupported` — never silently skipped. This page collects all the
matrices and fine print in one place.

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

On Linux the cgroup backend requires controller **delegation**, and resource
limits specifically need this process to run at the **real cgroup-v2 root**. The
crate creates the limit cgroup under this process's own cgroup and enables the
controllers in that cgroup's `subtree_control`, which cgroup v2's "no internal
processes" rule allows only for the real hierarchy root (the one exempt cgroup). A
cgroup *namespace* root does **not** qualify — it only virtualizes the view — so an
ordinary (private-cgroupns) container fails `EBUSY` just like a systemd
session/scope/service. The crate does not migrate your process into a sub-cgroup
to work around it, so in practice limits apply only at a minimal non-systemd init
sitting at the real root. Without a usable cgroup it quietly falls back to `ProcessGroup` —
unless you requested [resource limits](#capability-matrices), which fail fast
instead (`Error::ResourceLimit`), because an unapplied cap is no protection. The
error's `reason` distinguishes the two ways this happens: `LimitReason::Unsupported`
when no cgroup v2 is mounted at all (or on macOS/BSD, which has no whole-tree
container of any kind), `LimitReason::Unenforceable` when cgroup v2 exists but this
process isn't at the real hierarchy root (the delegation case above) or the OS
otherwise rejected the request.

## Capability matrices

**Teardown & containment**

| Capability | Windows JobObject | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| Kill-on-drop, whole tree | ✅ | ✅ | ✅ groups-based | ✅ groups-based |
| Graceful `shutdown` (TERM → grace → KILL) | 🟡 atomic kill only | ✅ | ✅ | ✅ |
| `adopt` an external child | ✅ (future forks contained) | ✅ (future forks contained) | 🟡 exec'd child tracked individually | 🟡 same |

Windows has no signal tier, so a graceful `shutdown` collapses to the atomic Job
kill — but it still honors `escalate_to_kill`: `false` **spares** the survivors
(closes the Job handle without `KILL_ON_JOB_CLOSE`) rather than killing them, so
the Windows column is "atomic kill *when it kills*", not an unconditional kill.

**Signals & freezing**

| Capability | Windows | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| Arbitrary signal (`Hup`, `Usr1`, `Other(n)`, …) | ❌ `Kill` only | ✅ | ✅ | ✅ |
| `suspend` / `resume` | 🟡 per-thread counts | ✅ `cgroup.freeze` | ✅ `SIGSTOP`/`CONT` | ✅ `SIGSTOP`/`CONT` |

On the cgroup mechanism, a non-`Kill` `signal` (and the `SIGSTOP`/`SIGCONT`
fallback used for `suspend`/`resume` on pre-5.2 kernels without `cgroup.freeze`)
surfaces a real per-member delivery failure (e.g. `EPERM`) as an `Err` rather
than swallowing it — consistent with the "never silently skipped" philosophy; an
`ESRCH` race (the member already exited) is still success.

**Inspection & accounting**

| Capability | Windows | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| `members()` | ✅ whole tree | ✅ whole tree | 🟡 leaders only | 🟡 leaders only |
| Group CPU / peak memory | ✅ | ✅ | ❌ count only | ❌ count only |
| Per-run `cpu_time` / `peak_memory_bytes` / `profile` | ✅ | ✅ | ✅ (`/proc`) | ❌ `None` |

`members()` is gated on the **`process-control`** feature; the CPU / memory /
`profile` rows are gated on the **`stats`** feature.

**Resource limits** (`limits` feature)

| Capability | Windows | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| `max_memory` (whole tree) | ✅ | ✅ | ❌ | ❌ |
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
| `priority` | ✅ (priority class) | ✅ (`nice`/`setpriority`) |
| `umask` | ❌ `Unsupported` | ✅ |

Everything not listed — capture, streaming, interactive stdin, encodings,
buffer policies, timeouts, retry, pipelines, supervision, readiness probes,
the test doubles, cassettes, cancellation — is **platform-agnostic** and
behaves identically everywhere.

## Caveats

The honest fine print, mostly consequences of OS semantics:

**Windows: termination is an exit code, never `Signalled` (D18).** Windows has
no signal abstraction, so a killed process reports
[`Outcome::Exited`](https://docs.rs/processkit/latest/processkit/enum.Outcome.html),
not `Outcome::Signalled`. `TerminateProcess` / `TerminateJobObject(_, 1)` is
`Exited(1)` — indistinguishable from a voluntary `exit(1)` — and `Ctrl-C`
surfaces as `Exited(-1073741510)` (`STATUS_CONTROL_C_EXIT` as a signed `i32`).
The crate reports the platform truth rather than fabricating a `Signalled` from
an NTSTATUS code (that mapping would be a lossy guess). When you need to *know*
the run was killed, use a `ProcessGroup` deadline or a cancellation token (which
surface as `TimedOut` / `Error::Cancelled` on every platform). `Outcome::Signalled`
is therefore Unix-only.

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

---

Next: [Process groups](process-groups.md) ·
[Running commands](commands.md) ·
[docs index](README.md)
