# Process groups

[‹ docs index](README.md)

A `ProcessGroup` ties the lifetime of a whole child-process **tree** to a Rust
value: every process spawned into the group — and everything *those* processes
spawn — is killed when the group is dropped. An exiting, panicking, or
`?`-returning owner never leaks subprocesses; the kernel object enforcing this
(Job Object / cgroup / POSIX process group) catches even grandchildren you
never knew about. (Killing grandchildren is the problem `duct.py`'s gotchas
list files under "currently unsolved" for pipe-based designs — kernel
containment is the solution, and the reason this crate exists.)

- [Creating a group](#creating-a-group)
- [Putting processes in](#putting-processes-in)
- [Tearing down: drop, terminate, shutdown](#tearing-down-drop-terminate-shutdown)
- [Signalling the whole tree](#signalling-the-whole-tree)
- [Suspending and resuming](#suspending-and-resuming)
- [Listing members](#listing-members)
- [Resource limits](#resource-limits)
- [Stats and sampling](#stats-and-sampling)

## Creating a group

```rust,no_run
use processkit::{ProcessGroup, ProcessGroupOptions};
use std::time::Duration;

fn main() -> processkit::Result<()> {
    // Defaults: 2s graceful-shutdown grace, escalate to SIGKILL.
    let group = ProcessGroup::new()?;

    // Tuned:
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .shutdown_timeout(Duration::from_secs(10))
            .escalate_to_kill(true),
    )?;

    // Which kernel mechanism is actually containing the tree?
    println!("{:?}", group.mechanism()); // JobObject | CgroupV2 | ProcessGroup
    Ok(())
}
```

`mechanism()` reports what you actually got: `CgroupV2` quietly falls back to
`ProcessGroup` on Linux hosts without cgroup delegation (see
[Platform support](platform-support.md)).

You rarely create a group explicitly for one-shot runs: every
`Command::run()`-style call makes a private group automatically. Reach for an
explicit group when several children should share one fate, or when you need
the group verbs below.

## Putting processes in

Three doors, in order of preference:

```rust,no_run
use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let group = ProcessGroup::new()?;

    // 1. start(): the full Command experience (capture, streaming, timeouts) in a
    //    SHARED group. The handle does not own the group — dropping the handle
    //    kills that child, dropping the group kills everyone.
    let server = group.start(&Command::new("dev-server")).await?;

    // 2. spawn(): the raw escape hatch for a tokio::process::Command you already
    //    have. You get the bare Child back; pipes and reaping are your problem.
    //    spawn() takes the command BY VALUE (reuse would stack pre-exec hooks).
    let raw = tokio::process::Command::new("background-helper");
    let child = group.spawn(raw)?;

    // 3. adopt(): contain a child that was spawned OUTSIDE the group.
    let external = tokio::process::Command::new("legacy-launcher").spawn()?;
    group.adopt(&external)?;
    let _ = (server, child);
    Ok(())
}
```

`adopt` moves only the named process: descendants it *already* has keep their
old containment (future forks are captured — on Windows/cgroup). A few sharp
edges worth knowing:

- A child that already exited **but has not been reaped** (no `wait()` yet — a
  zombie whose pid/handle is still valid) is a successful **no-op**: there is
  nothing left to contain, so `adopt` returns `Ok` on the containment backends.
- A child that already exited **and was reaped** (`wait()`ed) has no pid left —
  `adopt` returns an error rather than silently tracking nothing.
- On the POSIX process-group mechanism, a child that has already `exec`'d
  can't be re-grouped (POSIX forbids it), so it is tracked *individually*: the
  child itself is signalled/killed with the group, but its future forks are
  not. The caller keeps the `Child` handle and is responsible for reaping.

## Tearing down: drop, terminate, shutdown

| Verb | What happens | When |
|---|---|---|
| `drop(group)` | Immediate **hard kill** of the whole tree (kill-on-close) | The safety net — always on |
| `group.kill_all()` | The same hard kill, group stays usable (cgroup-`kill` / Job Object / process-group backends). On a **pre-5.14 Linux kernel** lacking `cgroup.kill`, the per-pid `SIGKILL` fallback returns `Err` if the tree doesn't drain (a fork bomb still out-spawning, or `D`-state zombies) | Explicit teardown mid-flight; idempotent |
| `group.shutdown().await` | Unix: `SIGTERM` → wait `shutdown_timeout` → `SIGKILL` survivors (if `escalate_to_kill`); Windows: atomic job kill when `escalate_to_kill`, else the survivors are **spared** (handle closed without kill-on-close) — unless a child opted into `windows_graceful_ctrl_break` (see below), which gives Windows a real `CTRL_BREAK` → wait → kill tier. Consumes the group (`shutdown_ref(&self)` is the same teardown, borrowing — for a group held behind an `Arc`/supervisor) | Graceful service stop |

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .shutdown_timeout(Duration::from_secs(5))
            .escalate_to_kill(true),
    )?;
    let _service = group.start(&Command::new("my-service")).await?;

    // SIGTERM, give it 5s to flush and exit, SIGKILL stragglers:
    group.shutdown().await?;
    Ok(())
}
```

A child that handles `SIGTERM` ends the grace **early** — `shutdown` returns
as soon as the tree is empty, not after the full timeout. One subtlety: the
liveness probe sees an exited-but-unreaped child (a zombie) as alive on the
process-group backends, so keep `wait()`ing your handles concurrently if you
want the early return. `Drop` can't `await`, which is why the graceful tier
lives in this async method — dropping without calling it performs only the
hard kill.

### Windows: the graceful soft tier (`WM_CLOSE`, opt-in `CTRL_BREAK`)

A Windows `shutdown` has no POSIX `SIGTERM`, but it still tries to *trigger* a
clean exit before the atomic Job Object kill. For a **windowed** child (Electron
app, desktop tool, windowed service) this is automatic: `WM_CLOSE` is *posted*
(never *sent*, so a hung window can't block us) to every top-level window a live
member owns, then the *same* signal → wait → escalate ladder runs — the child
gets the `shutdown_timeout` to flush and exit, else `TerminateJobObject`. A
**console** child has no window, so opt in per child with
[`Command::windows_graceful_ctrl_break()`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.windows_graceful_ctrl_break):
the direct child is spawned in its own console process group
(`CREATE_NEW_PROCESS_GROUP`), and `shutdown` then sends it
`GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)`, waits the `shutdown_timeout`,
and `TerminateJobObject`s any survivor — the very same signal → wait → escalate
ladder as Unix, so a console child that handles `CTRL_BREAK` shuts down softly.

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(5)),
    )?;
    // CTRL_BREAK is sent on shutdown; a console child gets 5s to exit, else kill.
    let _service = group
        .start(&Command::new("my-service").windows_graceful_ctrl_break())
        .await?;
    group.shutdown().await?;
    Ok(())
}
```

The `CTRL_BREAK` opt-in is **console-only**: a child spawned
[`create_no_window`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.create_no_window)
or `DETACHED_PROCESS` does not share this process's console, so it never receives
the event and rides the grace to the `TerminateJobObject` fallback. Only the
*direct* child is addressed by `CTRL_BREAK` — an
[`adopt`](https://docs.rs/processkit/latest/processkit/struct.ProcessGroup.html#method.adopt)ed
child is not — and the event is `CTRL_BREAK`, not `CTRL_C` (a new process group
disables `CTRL_C`). The automatic `WM_CLOSE` path is the complement: it reaches
any live member that owns a top-level window (including forked descendants and
adopted children), needs no console and no opt-in, but only a member that actually
has a window. A member with neither a window nor the console opt-in is hard-killed
promptly at the deadline. Off Windows the builder is a no-op.

## Signalling the whole tree

> `signal`/`suspend`/`resume`/`members`/`adopt` — this section and the two
> below — require the default-on **`process-control`** feature. The teardown
> verbs above are core and always present.

```rust,no_run
use processkit::{Command, ProcessGroup, Signal};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _server = group.start(&Command::new("my-server")).await?;

    group.signal(Signal::Hup)?;        // "reload your configuration"
    group.signal(Signal::Usr1)?;       // whatever the tool defines
    group.signal(Signal::Other(34))?;  // raw signal number escape hatch
    Ok(())
}
```

| Platform | Deliverable signals |
|---|---|
| Linux (cgroup or pgroup), macOS/BSD | Any — `Term`, `Kill`, `Int`, `Hup`, `Quit`, `Usr1`, `Usr2`, `Other(n)` |
| Windows | `Kill` (Job Object terminate); `Int`/`Term` as a best-effort soft close (`CTRL_BREAK` to console leaders + `WM_CLOSE` to windowed members) — `Error::Unsupported` only when neither exists; every other signal → `Error::Unsupported` |

`Signal::Kill` always takes the same *atomic* whole-tree kill path as
`kill_all` (`cgroup.kill` / `killpg` / job terminate), so it cannot miss
a process forked mid-broadcast. Other signals are a per-member broadcast —
best-effort against a tree that is forking at that exact moment. On Windows,
`Signal::Int`/`Signal::Term` do not wait or escalate (they only *trigger* a soft
close — contrast the graceful `shutdown`, which then waits the grace and
escalates). An empty group accepts any deliverable signal trivially — except
Windows `Int`/`Term`, which report `Unsupported` on an empty group (no member,
hence no console or windowed target to soft-close). On the **cgroup** mechanism a
real per-member delivery failure (e.g. `EPERM` from a member that changed uid, or
a seccomp/container restriction) is surfaced as an `Err` rather than swallowed —
an `ESRCH` race (the member already exited) is still success; the pgroup
(macOS/BSD, Linux-without-cgroup) backend remains purely best-effort.

## Suspending and resuming

Freeze a tree (to snapshot it, to starve a runaway while you investigate, to
pause background work), then thaw it:

```rust,no_run
use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _cruncher = group.start(&Command::new("cpu-hog")).await?;

    group.suspend()?;   // the whole tree stops consuming CPU
    // … inspect, snapshot, wait for the user …
    group.resume()?;
    Ok(())
}
```

Per-platform machinery — and its visible differences:

| Platform | Mechanism | Notes |
|---|---|---|
| Linux cgroup | one `cgroup.freeze` write | Atomic over the subtree; freeze is **group state** |
| Linux pgroup, macOS/BSD | `SIGSTOP` / `SIGCONT` broadcast | Idempotent (level-triggered) |
| Windows | per-thread `SuspendThread` walk | **Counted**: N suspends need N resumes; best-effort against mid-walk thread churn |

Two caveats that bite in practice:

- **Spawning into a suspended group diverges.** Under the cgroup mechanism a
  child spawned or adopted while the group is frozen **starts frozen** — and
  `start()` *may never return* until `resume` (the forked child joins the
  cgroup before `exec`, so it can freeze before completing the spawn
  handshake). Windows and the pgroup backends freeze only members present at
  the call. Rule of thumb: resume before starting new work.
- A suspended tree can still be **hard-killed** (drop / `kill_all` /
  `Signal::Kill` all act on frozen processes), but a graceful `shutdown`
  starts with a `SIGTERM` the frozen tree can't act on — it would wait out the
  whole grace. Resume first for a clean shutdown.

## Listing members

```rust,no_run
use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _a = group.start(&Command::new("worker-a")).await?;
    let _b = group.start(&Command::new("worker-b")).await?;

    let pids: Vec<u32> = group.members()?;
    println!("live members: {pids:?}");
    Ok(())
}
```

What "members" means depends on the mechanism: Windows and Linux-cgroup list
the **whole tree** (every descendant pid); the POSIX process-group backends
list the tracked group *leaders* (one pid per started/adopted child) — their
descendants are contained but not enumerated. An exited child still counts
until it is reaped. The snapshot is point-in-time: a tree that is forking
races it.

To *wait* on members rather than list them, race the handles with
[`wait_any`](streaming.md#racing-children-with-wait_any).

### Enriched snapshot: `members_info`

When bare pids aren't enough — a diagnostic `members_snapshot` event, a
process-tree view — `members_info` returns the same member set as `members`,
but each pid comes wrapped in a `MemberInfo` carrying best-effort **parent
pid**, **image name**, and **start time**:

```rust,no_run
use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _a = group.start(&Command::new("worker-a")).await?;

    for m in group.members_info()? {
        println!(
            "pid={} ppid={:?} exe={:?} start={:?}",
            m.pid(),
            m.ppid(),
            m.exe_name(),
            m.start_time(),
        );
    }
    Ok(())
}
```

The fields are read where the platform can report them and are `None`
otherwise — never a fabricated value. Windows and Linux (both the cgroup and
`/proc` fallback paths) and macOS fill all four; on the bare BSDs only the pid
is reported and the rest are `None`. `start_time` is an **opaque** identity
anchor (its unit and epoch differ per platform), not a wall-clock timestamp —
its use is pairing with the pid to tell a recycled number apart from the
original process, not display. The raw command line is **deliberately never**
included on any platform: it routinely carries secrets, and redaction is the
consumer's policy to own.

Same point-in-time contract as `members`, with one addition: if a member exits
between its pid being enumerated and its metadata being read, that pid is
**skipped** rather than reported with fabricated fields — a single vanished
member never fails the whole call.

## Resource limits

Requires the **`limits`** feature. Caps are a property of the group, set at
creation (and adjustable later — see [Updating a live
group](#updating-a-live-group)) and enforced by the same kernel object that
contains the tree:

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .max_memory(512 * 1024 * 1024) // bytes, whole tree
            .max_processes(64)             // fork-bomb ceiling
            .cpu_quota(0.5),               // half of one core
    )?;
    let _sandboxed = group.start(&Command::new("untrusted-tool")).await?;
    Ok(())
}
```

| Capability | Windows Job Object | Linux cgroup v2 | pgroup / macOS / BSD |
|---|---|---|---|
| Memory cap | ✅ whole-tree | ✅ whole-tree (`memory.max`) | ❌ |
| Process-count cap | ✅ | ✅ (`pids.max`) | ❌ |
| CPU quota | 🟡 approximate (rate vs. total CPU) | ✅ (`cpu.max`) | ❌ |

`cpu_quota` is a fraction of a **single** core (`2.0` = two cores). Limits
need a real container; when a requested cap can't be enforced — no Job
Object/cgroup, or a Linux cgroup whose controllers can't be enabled —
`with_options` returns `Error::ResourceLimit { kind, reason, detail }` instead
of handing back a silently-unbounded group: `kind` names the limit
(`max_memory`/`max_processes`/`cpu_quota`), `reason` says whether the value was
simply invalid, the platform has no whole-tree mechanism at all
(`Unsupported`), or a mechanism exists but rejected this request
(`Unenforceable`) — branch on these instead of parsing `detail`. On Linux this
needs the process to run at the
**real cgroup-v2 root**: the crate enables the controllers in this process's own
cgroup, which cgroup v2's "no internal processes" rule allows only for the real
hierarchy root — *not* a cgroup-namespace root (so an ordinary container fails
too), *not* under systemd — and the crate doesn't migrate your process. See the
limits prerequisites in
[Platform support](platform-support.md#containment-mechanisms). The `uid()`-drop
interaction lives under its [Caveats](platform-support.md#caveats).

### Updating a live group

`ProcessGroup::update_limits(ResourceLimits)` re-applies a fresh set of caps to
an **already-running** group — without recreating the container or restarting
its children — for adaptive resource management (tighten a slumping batch's
memory, widen a long-lived worker pool's CPU quota):

```rust,no_run
use processkit::{ProcessGroup, ResourceLimits};

# fn main() -> processkit::Result<()> {
let mut group = ProcessGroup::new()?;

// Later, adapt the caps on the already-running group:
let mut limits = ResourceLimits::default();
limits.max_memory = Some(256 * 1024 * 1024); // tighten to 256 MiB
limits.cpu_quota = Some(2.0);                 // widen CPU to two cores
group.update_limits(limits)?; // max_processes left None → that cap is lifted
# Ok(())
# }
```

The new value is a **full replacement**, not a merge: an axis left `None` is
lifted back to unbounded — it does *not* keep its previous cap — so always
describe the complete desired state (start from `ResourceLimits::default()` and
set the axes you want capped). On Windows the live Job Object's caps are
reissued; on Linux cgroup v2 the `memory.max` / `pids.max` / `cpu.max` files are
rewritten (a removed axis written back to `max`). It routes through the same
live container the tree-control verbs use, so the same platform matrix and
`Error::ResourceLimit { kind, reason, detail }` classification apply — a
process-group mechanism (macOS/BSD, the Linux fallback) refuses any requested
cap with `Unsupported` rather than silently dropping it, while lifting *all*
caps there is a trivial success.

## Stats and sampling

Requires the opt-in **`stats`** feature (`features = ["stats"]`, or `limits`).

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, ProcessGroup};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _worker = group.start(&Command::new("worker")).await?;

    // Point-in-time:
    let snap = group.stats()?;
    println!(
        "procs={} cpu={:?} peak_rss={:?}",
        snap.active_process_count, snap.total_cpu_time, snap.peak_memory_bytes,
    );

    // …or a series: first sample immediate, then every 250ms; missed ticks are
    // skipped; the stream ends when the group can no longer report.
    let mut samples = group.sample_stats(Duration::from_millis(250));
    while let Some(s) = samples.next().await {
        println!("rss now: {:?}", s.peak_memory_bytes);
    }
    Ok(())
}
```

CPU time and peak memory are available where the kernel accounts for the
whole tree (Windows, Linux cgroup); the process-group backends report the
member **count** only — the `Option` fields stay `None`. The sampler borrows
the group, so it can neither outlive it nor keep it (and the kill-on-drop
guarantee) alive. For a *single run's* end-to-end summary, see
[`profile`](streaming.md#per-run-telemetry).

---

Next: [Streaming & interactive I/O](streaming.md) ·
[Platform support](platform-support.md) ·
[Supervision](supervision.md)
