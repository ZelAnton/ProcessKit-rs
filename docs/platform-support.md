# Platform support

[‹ docs index](README.md)

`processkit` supports **Unix and Windows only** — it requires `tokio::process`
and OS job / process-group primitives that have no equivalent on bare targets
like wasm. Building for such a target fails at compile time (a `compile_error!`
guard, or earlier in tokio's own dependencies). Within the supported set, it
treats platform support as first-class: every capability is either fully
implemented, *honestly partial* (documented and typed), or refused with
`ErrorReason::Unsupported` — never silently skipped. This page collects all the
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

To learn which mechanism you *would* get **without creating a group** — for a
spawn-free preflight / host-check that must have no side effects — call
`host_containment()`,
which returns a `HostContainment` (`mechanism()`, plus `soft_stop_scope()`,
`parent_death_cleanup()`, and `crate_version()`) from read-only checks that create
no container and spawn no process (on Linux: no cgroup directory). The predicted
mechanism matches a real `ProcessGroup::new` on the same host; the Linux cgroup
answer is best-effort (it probes whether a cgroup *could* be created rather than
creating one), so a live `ProcessGroup::mechanism()` remains the final word in the
rare window where a writable-looking cgroup then rejects creation. See [Running in
containers → Which containment mechanism you get](containers.md#which-containment-mechanism-you-get).

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
instead (`ErrorReason::ResourceLimit`), because an unapplied cap is no protection. The
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
| Graceful `shutdown` (TERM → grace → KILL) | 🟡 auto `WM_CLOSE` soft tier for windowed children; opt-in `CTRL_BREAK` for console children; else atomic kill | ✅ | ✅ | ✅ |
| `adopt` an external child | ✅ (future forks contained) | ✅ (future forks contained) | 🟡 exec'd child tracked individually | 🟡 same |

Windows has no POSIX signal tier, so for a **windowless** child with no opt-in a
graceful `shutdown` collapses to the atomic Job kill — but it still honors
`escalate_to_kill`: `false` **spares** the survivors (closes the Job handle
without `KILL_ON_JOB_CLOSE`) rather than killing them, so the Windows column is
"atomic kill *when it kills*", not an unconditional kill.

**Automatic soft tier for windowed children (Windows).** Before the atomic kill,
a graceful `shutdown` posts `WM_CLOSE` to every top-level window owned by a live
member and then drives the *same* signal → wait → escalate loop the unix backends
use. A windowed child (Electron app, desktop tool, windowed service) that handles
`WM_CLOSE` can flush and exit within the grace; any survivor is then
`TerminateJobObject`'d, the same hard fallback as before. This is automatic — no
opt-in — and `WM_CLOSE` is *posted*, never *sent*, so a hung window can never
block teardown. A windowless tree with no console opt-in still hard-kills promptly
at the deadline (no grace wait is introduced for it), so its timings are unchanged.

**Opt-in soft tier for console children (Windows).**
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
| Arbitrary signal (`Hup`, `Usr1`, `Other(n)`, …) | 🟡 `Kill`, plus `Int`/`Term` as a best-effort soft close (`CTRL_BREAK` + `WM_CLOSE`); others unsupported | ✅ | ✅ | ✅ |
| `soft_stop_scope()` (soft `Int`/`Term` reach) | 🟡 `OptInMembers` with a console/windowed member, else `Unsupported` | `WholeTree` | `WholeTree` | `WholeTree` |
| `suspend` / `resume` | 🟡 per-thread counts | ✅ `cgroup.freeze` | ✅ `SIGSTOP`/`CONT` | ✅ `SIGSTOP`/`CONT` |

On **both** Unix mechanisms, a `signal` broadcast surfaces a real send failure as
an `Err` rather than swallowing it — an `EINVAL` (an out-of-range `Other(n)`) and
an `EPERM` against a **live, non-zombie** member (a uid-changed child, or a
seccomp/container restriction) — consistent with the "never silently skipped"
philosophy. The process-group backend (macOS/BSD, Linux fallback) matches the
cgroup verdict by checking the target's run state after an `EPERM`, so a harmless
zombie-only `EPERM` — and every `EPERM` on the bare BSDs (no state reader) —
stays swallowed; an `ESRCH` race (the member already exited) is still success,
and `Signal::Other(0)` returns `Ok` having delivered nothing (the POSIX existence
probe). On the cgroup mechanism the `SIGSTOP`/`SIGCONT` fallback used for
`suspend`/`resume` on pre-5.2 kernels (no `cgroup.freeze`) surfaces failures the
same way.

`soft_stop_scope()` answers, *before* you attempt a soft `Int`/`Term`, which
members it would reach — a side-effect-free `SoftStopScope` capability report read
from the group's live membership, so a caller cancelling on its own schedule can
decide up front instead of firing a `signal` and parsing an `ErrorReason::Unsupported`
back. The Unix backends always reach the whole tree (`WholeTree`, never
`Unsupported`); Windows reports `OptInMembers` when a live console-CTRL leader
(`windows_graceful_ctrl_break`) or a windowed member exists, and `Unsupported`
otherwise — exactly the split where `signal(Int/Term)` returns `Ok` versus
`ErrorReason::Unsupported`. It is the group-axis sibling of
`kill_on_parent_death_scope()` below, but read at runtime rather than fixed per
platform. Gated on the **`process-control`** feature, like `signal`.

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

**Readiness probes**

| Capability | Windows | Unix |
|---|---|---|
| `wait_for_line`, `wait_for_port`, `wait_for` | ✅ | ✅ |
| `wait_for_socket` (AF_UNIX) | ❌ `Unsupported` | ✅ |

`wait_for_socket` attempts a real Unix domain socket connection, so an orphaned
socket file does not count as ready. On Windows and any target without AF_UNIX,
it returns `ErrorReason::Unsupported` immediately.

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
buffer policies, timeouts, retry, pipelines, supervision, the non-socket
readiness probes, the test doubles, cassettes, cancellation — is
**platform-agnostic** and behaves identically everywhere.

## PTY mode (`use_pty`, the `pty` feature)

[`Command::use_pty`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.use_pty)
launches the child under a real pseudo-terminal instead of three pipes, so an
`isatty()`-gated tool works. Per platform:

| | Unix | Windows |
|---|---|---|
| Mechanism | `openpty` — the pty slave becomes the child's stdio, spawned through the **same** cgroup/process-group containment path as any other run | `CreatePseudoConsole` (ConPTY) — the child is created suspended, `AssignProcessToJobObject`'d to the **same** Job Object, then resumed |
| stdout/stderr | **merged** onto the single master (the `on_stderr_line`/`stderr_tee` split collapses; `ProcessResult::stderr` is empty) | **merged**, same |
| Echo control | terminal **echo disabled** (termios) so a written secret is not echoed back into the merged output | ConPTY has no portable per-write echo control — echo behavior is host-managed (not disabled) |
| Window size | `winsize` passed to `openpty`, default 80×24; set with [`Command::pty_size(cols, rows)`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.pty_size) | `COORD` passed to `CreatePseudoConsole`, default 80×24; same builder |
| Live resize | [`RunningProcess::resize_pty(cols, rows)`](https://docs.rs/processkit/latest/processkit/struct.RunningProcess.html#method.resize_pty) → `TIOCSWINSZ` on the master, which delivers **`SIGWINCH`** to the child's foreground process group | `resize_pty` → `ResizePseudoConsole`; **no `SIGWINCH`** — a console client learns of the new geometry on its next console query, and conhost may reflow **asynchronously** (delivery is best-effort, not synchronously observable) |
| Containment | unchanged — cgroup/pgroup kill-on-drop reaps the whole tree | unchanged — Job Object kill-on-close reaps the whole tree |

Off by default and additive: with the `pty` feature off (or on but `use_pty`
unset) the three-pipe behavior is byte-identical. It is a **minimal
single-master-fd mode**, not a terminal emulator. The Windows master I/O runs
over the ConPTY pipes bridged by dedicated blocking threads (acceptable for the
low-volume interactive use case). Whether a Windows ConPTY child's *standard
handles* bind to the pseudoconsole (rather than a launcher-inherited console) can
depend on the host's console state; the containment and spawn are validated on CI.

**Window size and live resize.** The pseudo-terminal opens at 80×24 unless
[`Command::pty_size(cols, rows)`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.pty_size)
requests otherwise (a documented **no-op** on a non-`use_pty` command — the
three-pipe launch has no terminal to size). Change a *running* session's size —
e.g. propagating a host window resize — with
[`RunningProcess::resize_pty(cols, rows)`](https://docs.rs/processkit/latest/processkit/struct.RunningProcess.html#method.resize_pty).
It returns [`ErrorReason::Unsupported`](https://docs.rs/processkit/latest/processkit/enum.ErrorReason.html)
(never a panic or a silent no-op) on a non-PTY run or once the child has exited.
The platform delivery differs: Unix `TIOCSWINSZ` raises `SIGWINCH` on the child
synchronously, whereas Windows `ResizePseudoConsole` has no signal — conhost
reflows and the client observes the new geometry on its next console query,
possibly a little later.

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
surface as `TimedOut` / `ErrorReason::Cancelled` on every platform). `Outcome::Signalled`
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
