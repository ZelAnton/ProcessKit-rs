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
- [Asking whether a soft stop is available](#asking-whether-a-soft-stop-is-available)
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
| `group.stop(grace, escalate).await` | The **observable** graceful stop (needs `process-control`): the *same* teardown as `shutdown_ref`, with an explicit `grace`/`escalate`, returning a `ShutdownReport` — the attempted soft signal (and whether it landed), member counts before/after, whether the tree drained within the grace or was hard-killed, and the actual elapsed. Borrows the group (usable afterwards) | You own the end-of-run race and want the observed facts — or a "kill and wait" via `stop(Duration::ZERO, true)` |

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

### Observing the teardown: `stop` and `ShutdownReport`

> Requires the default-on **`process-control`** feature (the report carries a
> `Signal`).

`shutdown`/`shutdown_ref` are fire-and-forget — they report only success or an
error. When you own your *own* end-of-run race (a timeout ⨯ Ctrl-C ⨯
control-socket race, not a `Command::timeout`) you usually want to stop the instant
the tree is empty rather than always spend the whole grace, and to report the tier
the kernel *observed* rather than what you *tried*. `ProcessGroup::stop(grace,
escalate)` is that verb: the same `SIGTERM` / `CTRL_BREAK` / `WM_CLOSE` → wait →
escalate ladder, taking `grace`/`escalate` explicitly and returning a
[`ShutdownReport`](https://github.com/ZelAnton/ProcessKit-rs/blob/main/src/shutdown_report.rs).

```rust,no_run
use processkit::{Command, ProcessGroup};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _service = group.start(&Command::new("my-service")).await?;

    // SIGTERM, up to 5s to drain, then SIGKILL survivors — and report what happened.
    let report = group.stop(Duration::from_secs(5), true).await?;
    if report.drained_within_grace() {
        println!("clean exit in {:?}", report.elapsed());
    } else if report.escalated() {
        let survivors = report.members_after().unwrap_or(0);
        eprintln!("hard-killed {survivors} survivor(s) after the grace");
    }
    if let Some(sig) = report.attempted_signal() {
        println!("attempted soft signal: {sig:?}");
    }
    Ok(())
}
```

The report is honest per platform. `members_before`/`members_after` count the same
member set as [`members`](#listing-members) — the whole
tree on the Job Object / cgroup mechanisms, the tracked group *leaders* on the
process-group fallback (where an unreaped zombie still counts, so reap your handles
for a true `members_after`). The `soft_signal()` verdict is three-way —
`SoftSignal::Sent(sig)`, `SoftSignal::Failed(sig)`, or `SoftSignal::Unsupported`;
the last arises **only** on a windowless Windows Job Object with no console-CTRL
leader (every Unix mechanism always has a real `SIGTERM` tier). `stop(Duration::ZERO,
true)` is the "**kill and wait**" path: it hard-kills at once (a zero grace waits
not at all) and reports what was still live — where bare `kill_all` returns as soon
as the kill is *issued*. `shutdown`/`shutdown_ref` are unchanged; `stop` is purely
additive.

`ShutdownReport` is the teardown facts as a typed value *after* the teardown
returns. For the same transitions **live** — stamped the instant each one happens —
enable the `tracing` feature: the teardown driver narrates `soft_signal →
grace_started → drained | escalated | spared` (each in a stable `phase` field) on
the `processkit` target, for every graceful path (`stop`, `shutdown`, a run-level
`timeout_grace`, a supervisor's graceful stop). The two are one seam read two ways
(both derive from the same driver outcome); neither can influence the teardown, and
neither carries argv/env.

## Deliberately detaching a child (`spawn_detached`)

**This inverts the crate's headline guarantee — on purpose.** Everything above is
about *keeping* a tree contained so nothing escapes. `Command::spawn_detached` is
the crate's one deliberate escape hatch for the opposite need: a child that
**must outlive its launcher** — daemonizing, a `nohup`-style long-lived helper, a
handoff to a process you want to keep running after this one exits.

```rust,no_run
use processkit::Command;

# fn main() -> processkit::Result<()> {
// Launch a helper that survives this process. Its stdout goes to a file — never
// a pipe, which would deadlock the child once nothing is left to drain it.
let child = Command::new("my-daemon")
    .arg("--serve")
    .stdout_file("/var/log/my-daemon.log")
    .spawn_detached()?;
println!("detached daemon pid = {}", child.pid());
// Dropping `child` does NOT kill the daemon — the crate is done with it.
# Ok(())
# }
```

For a safe runnable demonstration whose detached child exits by itself, see
[`examples/detached.rs`](https://github.com/ZelAnton/ProcessKit-rs/blob/main/examples/detached.rs).

What it does, and what it deliberately does **not**:

- **Detach at birth.** Unix — a **new session** (`setsid`), no controlling
  terminal. Windows — the child is **not assigned** to this crate's Job Object.
  It is *not* made to break away from a Job Object / cgroup the host already put
  *your* process in (a CI runner, a `systemd` scope, this crate's own supervisor):
  that would be hostile to whoever set up the host containment. So a detached
  child escapes **this crate's** per-run containment, not a broader host one it
  inherits.
- **A separate, non-interchangeable type.** You get a `DetachedChild` carrying
  only the `pid` — no `kill`, no `wait`, no timeout, no capture, no teardown verbs
  — because it is no longer contained. Dropping it does nothing to the child.
  (Left as a bare code span, not a `docs.rs` link: this type ships in the next
  release, so a `docs.rs` URL would 404 until then.)
- **stdio is null, or a file — never a pipe.** With no owner left to drain it, a
  pipe would deadlock the child the moment its buffer fills. stdout/stderr are
  null by default; the only alternative is a file redirect (`stdout_file` /
  `stderr_file`). stdin is always null.
- **Incompatible knobs are refused loudly.** A `Command` carrying a timeout,
  capture wiring (`on_stdout_line`/tees/`capture_policy`), an interactive stdin
  (`keep_stdin_open`/`inherit_stdin`/a `stdin` source), `retry`, `cancel_on`,
  `kill_on_parent_death` (its exact opposite), `windows_graceful_ctrl_break`, or
  Linux `io_priority` is
  rejected with a typed `ErrorReason::Unsupported` naming it — never silently
  ignored. Program/args/env/working-directory and the privilege-drop knobs
  (`uid`/`gid`/`groups`/`umask`/`priority`) **are** honored.

Reach for `spawn_detached` **only** when you truly want a child to outlive its
launcher. For everything else, `start`/`run`/`output_*` keep the child contained.

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
| Windows | `Kill` (Job Object terminate); `Int`/`Term` as a best-effort soft close (`CTRL_BREAK` to console leaders + `WM_CLOSE` to windowed members) — `ErrorReason::Unsupported` only when neither exists; every other signal → `ErrorReason::Unsupported` |

`Signal::Kill` always takes the same *atomic* whole-tree kill path as
`kill_all` (`cgroup.kill` / `killpg` / job terminate), so it cannot miss
a process forked mid-broadcast. Other signals are a per-member broadcast —
best-effort against a tree that is forking at that exact moment. On Windows,
`Signal::Int`/`Signal::Term` do not wait or escalate (they only *trigger* a soft
close — contrast the graceful `shutdown`, which then waits the grace and
escalates). An empty group accepts any deliverable signal trivially — except
Windows `Int`/`Term`, which report `Unsupported` on an empty group (no member,
hence no console or windowed target to soft-close). On **both** Unix mechanisms a
real send failure is surfaced as an `Err` rather than swallowed — an `EINVAL` (an
out-of-range `Other(n)`) always, and an `EPERM` against a **live, non-zombie**
member (a `sudo`/setuid child that rejects the signal, or a seccomp/container
restriction). The **process-group** mechanism (macOS/BSD, Linux-without-cgroup)
reaches the same verdict as the cgroup one by checking the target's run state
after an `EPERM`, so a harmless zombie-only `EPERM` — and, on the bare BSDs where
no state reader exists, every `EPERM` — stays swallowed. An `ESRCH` race (the
member already exited) is still success. `Signal::Other(0)` is the POSIX
existence probe: it returns `Ok` having **delivered nothing** (a live target was
reached, not signalled).

## Asking whether a soft stop is available

Before you fire a soft stop (`signal(Signal::Term)` / `Signal::Int`), you can ask
the group whether one will actually reach anything — `soft_stop_scope()` returns a
`SoftStopScope` **capability report**, so a caller cancelling a run on *its own*
schedule (a UI Cancel, a control-socket command, a timeout it owns) can decide up
front whether to attempt a graceful stop and can tell its user the real reach,
instead of firing a `signal`, catching `ErrorReason::Unsupported`, and reverse-engineering
the scope. It is the group-axis sibling of
`Command::kill_on_parent_death_scope() -> ParentDeathCleanup`, but read from the
group's **live membership** (not fixed per platform) and **side-effect-free** — it
delivers no signal, posts no `WM_CLOSE`, spawns nothing, and does not mutate the
group.

```rust,no_run
use processkit::{Command, ProcessGroup, Signal, SoftStopScope};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _server = group
        .start(&Command::new("my-server").windows_graceful_ctrl_break())
        .await?;

    // Decide BEFORE attempting — no ErrorReason::Unsupported to parse back.
    match group.soft_stop_scope() {
        SoftStopScope::WholeTree | SoftStopScope::OptInMembers => {
            group.signal(Signal::Term)?; // a soft stop will reach a member
        }
        SoftStopScope::Unsupported => {
            group.kill_all()?; // no soft tier here — go straight to the hard kill
        }
        other => eprintln!("unknown soft-stop scope: {}", other.name()),
    }
    Ok(())
}
```

| Mechanism | `soft_stop_scope()` | Why |
|---|---|---|
| Linux cgroup v2, macOS/BSD, Linux pgroup fallback | `WholeTree` | `signal(Int/Term)` reaches every member of the tree (the cgroup, or every tracked process group via `killpg`); never `Unsupported` |
| Windows, with a live console-CTRL leader (`windows_graceful_ctrl_break`) or a windowed member | `OptInMembers` | a soft close reaches only members it can *trigger* — a curated subset, not the whole tree |
| Windows, with neither | `Unsupported` | a Job Object has no POSIX signal and there is nothing to soft-close, so `signal(Int/Term)` would return `ErrorReason::Unsupported` |

Consistent with `signal` by construction: it reads the very same live-membership
primitives `signal(Int/Term)` acts on, so what it reports matches what a real soft
stop would then reach. It describes the *soft* tier only — the unconditional hard
kill (`Signal::Kill`, `kill_all`, dropping the group) always tears the whole tree
down regardless. `SoftStopScope` is `#[non_exhaustive]` and carries a stable
`name()` / `from_name()` machine identifier (`whole_tree` / `opt_in_members` /
`none`), like the other reporting enums.

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

### Identifying a process by pid (outside a group)

Sometimes the pid you care about is **not** a member of any group you own — a pid
saved to disk between runs, a launch registry checking whether the owner of a
crash-surviving entry is still alive, an e2e probe watching a process from outside
its container. For that, the crate publishes the *same* identity query as a
free-standing function (needs `process-control`):

```rust,no_run
# fn main() -> processkit::Result<()> {
let pid = 4321;

// Look up an arbitrary pid — the standalone twin of `members_info`, returning the
// same best-effort `MemberInfo` (parent pid, image name, start time).
match processkit::process_info(pid)? {
    Some(info) => println!(
        "pid={} ppid={:?} exe={:?} start={:?}",
        info.pid(), info.ppid(), info.exe_name(), info.start_time(),
    ),
    None => println!("pid {pid} is not running"),
}
# Ok(())
# }
```

`process_info` returns **three** distinct outcomes, and the distinction is the
point:

- `Ok(Some(info))` — the process exists; fields are best-effort `Option` exactly as
  in `members_info`.
- `Ok(None)` — the pid names **no** process: an honest negative, the "it's gone"
  answer a liveness check wants.
- `Err` — the process may well exist, but you couldn't inspect it (no permission —
  a Windows protected/`System` process, a Linux `hidepid` mount, a macOS restricted
  process — or an OS read error). **Never read this as "dead."** That is the whole
  reason it is an error rather than `Ok(None)`.

It reads no argv/environment, on any platform — the same "never argv/env" stance
`MemberInfo` documents.

#### Reuse-safe liveness: `process_is_alive`

Because the OS reuses pid *numbers*, "is pid N still alive?" is the wrong question
for a saved pid: a stranger may have recycled the number after your process exited.
Pair the pid with the **start-time token** and ask instead "is the *same* process
still running?":

```rust,no_run
# fn main() -> processkit::Result<()> {
// Earlier: record identity.
let pid = 4321;
let saved_start = processkit::process_info(pid)?.and_then(|i| i.start_time());

// Later (perhaps after a restart): is that same process still alive?
if processkit::process_is_alive(pid, saved_start)? {
    println!("the original process {pid} is still alive");
} else {
    println!("process {pid} is gone (exited, or its number was recycled)");
}
# Ok(())
# }
```

`process_is_alive` is `Ok(true)` only when the process exists **and** its current
start time matches the saved one; a **different** start time on the same number
(the number was recycled) reads as `Ok(false)`, and so does a nonexistent pid. A
permission `Err` propagates just like `process_info` — again, never "dead". The
start time is an opaque identity anchor (unit/epoch differ per platform), used only
for this pairing, never displayed. Where the platform reports **no** start time
(the bare BSDs, where `start_time()` is `None`), the check degrades to bare-pid
liveness — exactly the number-only check you'd otherwise write by hand, no weaker,
and never a false "dead".

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
`with_options` returns `ErrorReason::ResourceLimit { kind, reason, detail }` instead
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
`ErrorReason::ResourceLimit { kind, reason, detail }` classification apply — a
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

### An owning, `'static` sampler

Because `sample_stats` borrows the group, its `StatsSampler` is tied to that
borrow — it can't be moved into a [`tokio::spawn`]ed task or handed across an FFI
boundary, both of which need a `'static` value. When the group already lives
behind a shared [`Arc`] (a long-lived service, a supervisor, an FFI wrapper),
reach instead for `OwnedStatsSampler`, the owning twin — the sampling analogue of
how [`shutdown_ref`](#tearing-down-drop-terminate-shutdown) is the non-consuming
twin of `shutdown`:

```rust,no_run
use std::sync::Arc;
use std::time::Duration;

use processkit::prelude::StreamExt;
use processkit::{Command, OwnedStatsSampler, ProcessGroup};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = Arc::new(ProcessGroup::new()?);
    let _worker = group.start(&Command::new("worker")).await?;

    // `Send + 'static`: build it from `&Arc<…>` (the caller keeps the `Arc`) and
    // move it into a task. Same cadence as `sample_stats` — first sample
    // immediate, then one per interval, missed ticks skipped.
    let mut samples = OwnedStatsSampler::new(&group, Duration::from_millis(250));
    tokio::spawn(async move {
        while let Some(s) = samples.next().await {
            println!("rss now: {:?}", s.peak_memory_bytes);
        }
        // The series ended: either the container can no longer report, or every
        // `Arc` to the group was dropped and the tree is gone.
    });
    Ok(())
}
```

It holds the group only **weakly**, so — exactly like the borrowing
`StatsSampler` — it never keeps the group or its kill-on-drop guarantee alive; a
sampler left running in a detached task can't pin a tree that should have been
torn down. That makes its behaviour when the group goes away well-defined: the
stream ends — yields `None`, and stays ended (it is fused) — on the first tick
that can't produce a snapshot, whether because the container was torn down (a
failed `stats()`, same as the borrowing sampler) **or** because every strong
`Arc` to the group was released while the sampler ran (the weak handle no longer
upgrades). It never silently repeats the last snapshot and never leaves the task
awaiting a tick that will never come.

---

Next: [Streaming & interactive I/O](streaming.md) ·
[Platform support](platform-support.md) ·
[Supervision](supervision.md)
