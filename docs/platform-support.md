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

- [CI coverage](#ci-coverage)
- [Containment mechanisms](#containment-mechanisms)
- [Capability matrices](#capability-matrices)
- [Caveats](#caveats)

## CI coverage

`.github/workflows/ci.yml`'s `test` job runs the full real-subprocess suite
(`--include-ignored`, so kill-on-drop is actually exercised) on glibc x86_64
(`ubuntu-latest`), glibc aarch64 (`ubuntu-24.04-arm`), Windows and macOS. The
aarch64 leg is the only place the native Linux syscall/layout code
(`sys/{linux,pgroup,unix,pid_gate}.rs`) actually runs on Linux/glibc-aarch64 —
elsewhere aarch64 is only `cargo check`-compiled against `aarch64-apple-darwin`
(Darwin, in the `msrv` job), never executed. A separate `test-musl` job runs the
full suite a further time inside a real `rust:alpine` container — musl libc and a
busybox userland, not merely a cross-compiled `x86_64-unknown-linux-musl`
binary executed by glibc userland tools — because musl/Alpine is the de-facto
standard for the container images this crate actually runs in, and its libc,
signal, and userland-utility details genuinely differ from glibc's. Alpine's
busybox already covers every external utility the suite spawns (`sh`, `cat`,
`sleep`, `yes`, `head`, `grep`, `sort`, `id`, `printf`, `env`, `seq`) except
one: its `ps` applet has no `-p PID` filter, so the job installs `procps`
(`procps-ng`) to get one that does. `gcc`/`musl-dev` (needed to link) already
ship in the base image.

Two container-runtime quirks — unrelated to musl/Alpine itself, but specific
to running *any* test suite inside a plain container — need working around,
via the job's (and the `just test-musl` recipe's) `--init` and
`--cap-add=SYS_NICE` options:

- **No subreaper.** A plain container's PID 1 is just the job's own entry
  process, not a real init — so a killed process's orphaned grandchildren
  become zombies that are never reaped and still probe alive via
  `kill(pid, 0)`. That silently breaks every test that asserts a forked
  grandchild is actually gone after teardown. `--init` runs
  [tini](https://github.com/krallin/tini) as PID 1, a real subreaper, which
  is what a properly configured container host provides. Any container's own
  PID 1 has the identical gap for its own orphaned grandchildren — see
  [Running in containers → PID 1](containers.md#pid-1-signals-zombies-and-whats-contained).
- **`CAP_SYS_NICE` dropped by default.** Docker excludes it from the default
  capability set even for a root container user, so *raising* scheduling
  priority fails `EPERM` regardless of uid (lowering it never needs the
  capability). One test exercises `Priority::High` (a negative `nice`) together
  with a privilege drop; `--cap-add=SYS_NICE` restores just that one narrow,
  low-blast-radius capability rather than skipping the test.

Run the same job locally with `just test-musl` (requires Docker); see the
recipe in `justfile` for details.

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
| Graceful `shutdown` (TERM → grace → KILL) | 🟡 atomic kill (opt-in `CTRL_BREAK` soft tier) | ✅ | ✅ | ✅ |
| `adopt` an external child | ✅ (future forks contained) | ✅ (future forks contained) | 🟡 exec'd child tracked individually | 🟡 same |

By default Windows has no signal tier, so a graceful `shutdown` collapses to the
atomic Job kill — but it still honors `escalate_to_kill`: `false` **spares** the
survivors (closes the Job handle without `KILL_ON_JOB_CLOSE`) rather than killing
them, so the Windows column is "atomic kill *when it kills*", not an
unconditional kill.

**Opt-in soft tier (Windows).**
[`Command::windows_graceful_ctrl_break()`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.windows_graceful_ctrl_break)
gives Windows a real soft-shutdown trigger: the direct child is spawned in its
own console process group (`CREATE_NEW_PROCESS_GROUP`), and at graceful teardown
it is sent `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` before the grace
window — driven through the *same* signal → wait → escalate loop the unix
backends use. A console child that handles `CTRL_BREAK` (many CLIs, Node,
Python, Go services do) can flush and exit within the grace; any survivor is then
`TerminateJobObject`'d, the same hard fallback as before, so containment is never
weakened. **Boundaries:** it works only for children that share this process's
console — a child spawned `create_no_window` / `DETACHED_PROCESS` (or a GUI /
service parent with no console) never receives the event and simply rides the
grace to the fallback kill; the event is `CTRL_BREAK` (not `CTRL_C`, which a new
process group disables); and only the direct child is addressed (an `adopt`ed
child is not). Off Windows the builder is a no-op — the graceful ladder already
sends a real signal.

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
| `kill_on_parent_death_scope()` (abrupt-death reach) | `WholeTree` | Linux: `DirectChildOnly`; macOS/BSD: `Unsupported` |
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
not silently degrade. For the container-specific version of this — what a
plain `docker run` actually gets, and why resource limits stay unenforceable
even under `--privileged` — see
[Running in containers → mechanism](containers.md#which-containment-mechanism-you-get)
and
[Running in containers → resource limits](containers.md#container-resource-limits-vs-the-crates-limits).

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

**The reach of `kill_on_parent_death()` on *abrupt* owner death is reported
honestly, not overpromised.** There is no portable Unix primitive that kills a
whole process tree when its creator dies — only Windows Job Objects give it for
free. So `Command::kill_on_parent_death_scope()` returns a `ParentDeathCleanup`
capability report — `WholeTree` on Windows, `DirectChildOnly` on Linux,
`Unsupported` on macOS/BSD — letting a wrapper (e.g. a CLI) state the *actual*
scope instead of guaranteeing a whole-tree cleanup the kernel cannot deliver.
This describes only the abrupt-death path (a SIGKILL of the owner, where `Drop`
never runs); ordinary graceful teardown still kills the whole tree everywhere.

**Windows: the suspended-spawn handshake.** Children are created
`CREATE_SUSPENDED`, assigned to the job, then resumed — closing the classic
race where a fast child forks before it's in the job. A consequence: on the raw
`ProcessGroup::spawn` escape hatch, any creation flags the caller set are
**overwritten** — the child is forced to `CREATE_SUSPENDED` alone, because Win32
exposes no way to read the flags back and OR the suspend bit in. The
`Command`-driven paths don't have this limitation: their extras (incl.
`create_no_window`) travel alongside the OS command and are OR'd in.

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
grace. Resume first. For the orchestrator's own `SIGTERM` to your
container's PID 1 (a related but distinct signal from the one `shutdown`
sends to the tree it manages), see
[Running in containers → graceful shutdown](containers.md#graceful-shutdown-on-the-orchestrators-sigterm).

**pgroup backends: leaders, zombies, pid reuse.** `members()` lists tracked
group leaders only; an exited-but-unreaped child (zombie) still probes as
alive (keep `wait()`ing handles if you need prompt liveness, e.g. for
`shutdown`'s early return); and pid-based signalling is inherently
best-effort against pid reuse — the crate prunes dead entries on every probe
to keep the window minimal.

Launching a program you don't trust? [Running untrusted children](untrusted-children.md)
assembles the containment/limits/privilege-drop caveats above into a
threat-aware checklist.

---

Next: [Process groups](process-groups.md) ·
[Running in containers](containers.md) ·
[Running untrusted children](untrusted-children.md) ·
[docs index](README.md)
