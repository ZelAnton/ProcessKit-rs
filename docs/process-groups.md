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
| `group.terminate_all()` | The same hard kill, group stays usable (cgroup-`kill` / Job Object / process-group backends). On a **pre-5.14 Linux kernel** lacking `cgroup.kill`, the per-pid `SIGKILL` fallback returns `Err` if the tree doesn't drain (a fork bomb still out-spawning, or `D`-state zombies) | Explicit teardown mid-flight; idempotent |
| `group.shutdown().await` | Unix: `SIGTERM` → wait `shutdown_timeout` → `SIGKILL` survivors (if `escalate_to_kill`); Windows: atomic job kill. Consumes the group | Graceful service stop |

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};
use std::time::Duration;

let group = ProcessGroup::with_options(
    ProcessGroupOptions::default()
        .shutdown_timeout(Duration::from_secs(5))
        .escalate_to_kill(true),
)?;
let _service = group.start(&Command::new("my-service")).await?;

// SIGTERM, give it 5s to flush and exit, SIGKILL stragglers:
group.shutdown().await?;
```

A child that handles `SIGTERM` ends the grace **early** — `shutdown` returns
as soon as the tree is empty, not after the full timeout. One subtlety: the
liveness probe sees an exited-but-unreaped child (a zombie) as alive on the
process-group backends, so keep `wait()`ing your handles concurrently if you
want the early return. `Drop` can't `await`, which is why the graceful tier
lives in this async method — dropping without calling it performs only the
hard kill.

## Signalling the whole tree

> `signal`/`suspend`/`resume`/`members`/`adopt` — this section and the two
> below — require the default-on **`process-control`** feature. The teardown
> verbs above are core and always present.

```rust,no_run
use processkit::{Command, ProcessGroup, Signal};

let group = ProcessGroup::new()?;
let _server = group.start(&Command::new("my-server")).await?;

group.signal(Signal::Hup)?;        // "reload your configuration"
group.signal(Signal::Usr1)?;       // whatever the tool defines
group.signal(Signal::Other(34))?;  // raw signal number escape hatch
```

| Platform | Deliverable signals |
|---|---|
| Linux (cgroup or pgroup), macOS/BSD | Any — `Term`, `Kill`, `Int`, `Hup`, `Quit`, `Usr1`, `Usr2`, `Other(n)` |
| Windows | `Kill` only (maps to the Job Object terminate); anything else → `Error::Unsupported` |

`Signal::Kill` always takes the same *atomic* whole-tree kill path as
`terminate_all` (`cgroup.kill` / `killpg` / job terminate), so it cannot miss
a process forked mid-broadcast. Other signals are a per-member broadcast —
best-effort against a tree that is forking at that exact moment. An empty
group accepts any deliverable signal trivially. On the **cgroup** mechanism a
real per-member delivery failure (e.g. `EPERM` from a member that changed uid, or
a seccomp/container restriction) is surfaced as an `Err` rather than swallowed —
an `ESRCH` race (the member already exited) is still success; the pgroup
(macOS/BSD, Linux-without-cgroup) backend remains purely best-effort.

## Suspending and resuming

Freeze a tree (to snapshot it, to starve a runaway while you investigate, to
pause background work), then thaw it:

```rust,no_run
use processkit::{Command, ProcessGroup};

let group = ProcessGroup::new()?;
let _cruncher = group.start(&Command::new("cpu-hog")).await?;

group.suspend()?;   // the whole tree stops consuming CPU
// … inspect, snapshot, wait for the user …
group.resume()?;
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
- A suspended tree can still be **hard-killed** (drop / `terminate_all` /
  `Signal::Kill` all act on frozen processes), but a graceful `shutdown`
  starts with a `SIGTERM` the frozen tree can't act on — it would wait out the
  whole grace. Resume first for a clean shutdown.

## Listing members

```rust,no_run
use processkit::{Command, ProcessGroup};

let group = ProcessGroup::new()?;
let _a = group.start(&Command::new("worker-a")).await?;
let _b = group.start(&Command::new("worker-b")).await?;

let pids: Vec<u32> = group.members()?;
println!("live members: {pids:?}");
```

What "members" means depends on the mechanism: Windows and Linux-cgroup list
the **whole tree** (every descendant pid); the POSIX process-group backends
list the tracked group *leaders* (one pid per started/adopted child) — their
descendants are contained but not enumerated. An exited child still counts
until it is reaped. The snapshot is point-in-time: a tree that is forking
races it.

To *wait* on members rather than list them, race the handles with
[`wait_any`](streaming.md#racing-children-with-wait_any).

## Resource limits

Requires the **`limits`** feature. Caps are a property of the group, set once
at creation and enforced by the same kernel object that contains the tree:

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};

let group = ProcessGroup::with_options(
    ProcessGroupOptions::default()
        .memory_max(512 * 1024 * 1024) // bytes, whole tree
        .max_processes(64)             // fork-bomb ceiling
        .cpu_quota(0.5),               // half of one core
)?;
let _sandboxed = group.start(&Command::new("untrusted-tool")).await?;
```

| Capability | Windows Job Object | Linux cgroup v2 | pgroup / macOS / BSD |
|---|---|---|---|
| Memory cap | ✅ whole-tree | ✅ whole-tree (`memory.max`) | ❌ |
| Process-count cap | ✅ | ✅ (`pids.max`) | ❌ |
| CPU quota | 🟡 approximate (rate vs. total CPU) | ✅ (`cpu.max`) | ❌ |

`cpu_quota` is a fraction of a **single** core (`2.0` = two cores). Limits
need a real container; when a requested cap can't be enforced — no Job
Object/cgroup, or a Linux cgroup whose controllers can't be enabled —
`with_options` returns `Error::ResourceLimit` instead of handing back a
silently-unbounded group. On Linux this needs the process to run at the
**real cgroup-v2 root**: the crate enables the controllers in this process's own
cgroup, which cgroup v2's "no internal processes" rule allows only for the real
hierarchy root — *not* a cgroup-namespace root (so an ordinary container fails
too), *not* under systemd — and the crate doesn't migrate your process. See the
limits prerequisites in
[Platform support](platform-support.md#containment-mechanisms). The `uid()`-drop
interaction lives under its [Caveats](platform-support.md#caveats).

## Stats and sampling

Requires the opt-in **`stats`** feature (`features = ["stats"]`, or `limits`).

```rust,no_run
use processkit::{Command, ProcessGroup, StreamExt};
use std::time::Duration;

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
