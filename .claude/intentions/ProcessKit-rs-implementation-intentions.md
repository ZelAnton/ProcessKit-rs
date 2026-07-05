# ProcessKit-rs — Implementation Intentions

A design note for ten candidate features for the `processkit` crate.

> **Status update (2026-06-06):** all ten items below have shipped (the 0.7.0
> cycle). This file remains as the historical design record; nothing here is
> outstanding.

**What `processkit` is today.** A two-layer child-process toolkit:

- **`ProcessGroup`** — a kill-on-drop container for a child-process tree, backed by
  Windows Job Objects, Linux cgroup v2 (with a POSIX process-group fallback), and a
  POSIX process group on macOS/BSD.
- **`Command` / `RunningProcess`** — async (tokio) run-and-capture built on the
  group layer, with a mockable `ProcessRunner` seam, streaming, interactive stdin,
  buffer policies, encoding overrides, per-run stats, `timeout`, and `retry`.

**Status of this document.** These are *intentions*, not commitments — a menu of
where the crate could grow next. Every "Where it plugs in" line names a real symbol
in the current source so each item starts from a known extension point, not a blank
page. Platform support is treated as first-class: each feature carries a matrix that
is honest about where a capability is partial or simply unavailable.

**Shared anchor.** Group-level features (1–4) all extend the same seam — the `Job`
wrapper in `src/sys/mod.rs:51-108`. The pattern is: add a method to that wrapper,
then implement it in each backend `imp::Job` (`src/sys/windows.rs`,
`src/sys/linux.rs`, `src/sys/unix.rs`, `src/sys/pgroup.rs`). Runner/command/testing
features (5–10) compose over `Command`, `RunningProcess`, and the `ProcessRunner`
trait without touching the platform layer.

Legend for the platform matrices: ✅ full · 🟡 partial · ❌ unsupported (documented).

---

## 1. Resource limits on a group (memory / pids / CPU)

**Intent.** The group layer already *reads* CPU and memory (`ProcessGroup::stats`);
the symmetric missing half is *bounding* them. A caller running untrusted or
runaway-prone children should be able to cap a tree's memory, process count, and CPU
share so a misbehaving descendant cannot exhaust the host. The limit is a property of
the group, set once at construction, enforced by the kernel object that already
contains the tree.

**Public API sketch.**

```rust
let group = ProcessGroup::with_options(
    ProcessGroupOptions::default()
        .memory_max(512 * 1024 * 1024) // bytes, whole-tree
        .max_processes(64)
        .cpu_quota(0.5),               // ≈ half a core
)?;
```

**Where it plugs in.** `ProcessGroupOptions` (`src/group.rs:18-34`) gains the limit
fields; each backend applies them when the `Job` is created.

- **Windows** — extend the `SetInformationJobObject` call already in `Job::new`
  (`src/sys/windows.rs`) with `JOB_OBJECT_LIMIT_JOB_MEMORY` (→ `JobMemoryLimit`) and
  `JOB_OBJECT_LIMIT_ACTIVE_PROCESS` (→ `ActiveProcessLimit`) on the existing
  `JOBOBJECT_EXTENDED_LIMIT_INFORMATION`. No new struct — only more flags on one
  you already populate.
- **Linux (cgroup)** — at `Cgroup::create()` (`src/sys/linux.rs`, ~line 237) enable
  `+memory +pids +cpu` in the parent `cgroup.subtree_control`, then write
  `memory.max`, `pids.max`, `cpu.max` in the new cgroup dir.
- **POSIX (Linux fallback / macOS / BSD)** — `setrlimit(RLIMIT_AS, …)` in the
  `pre_exec` hook for a *per-process* memory cap (not whole-tree); pids/cpu have no
  portable per-group equivalent.

New `Error::LimitExceeded` surfaced from exit inspection where the kernel reports an
OOM/limit kill (cgroup `memory.events`, Windows job notifications).

**Platform matrix.**

| Capability | Win JobObject | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| Memory cap | ✅ whole-tree | ✅ whole-tree | 🟡 per-process (`setrlimit`) | 🟡 per-process |
| Process count cap | ✅ | ✅ | ❌ | ❌ |
| CPU quota | ❌ (accounting only) | ✅ (`cpu.max`) | ❌ | ❌ |

**Testing.** Spawn a child that allocates past the cap / forks past the count and
assert the kernel kills it and `stats()`/the typed error reflect the limit. Gate the
cgroup-controller assertions on `mechanism() == CgroupV2` so CI on a controller-less
runner skips cleanly.

**Effort / risk.** **L** — most FFI surface; main hazard is cgroup delegation /
controller availability differing across hosts (already a code path the crate
tolerates via the pgroup fallback).

---

## 2. Signals and suspend/resume across the whole tree

**Intent.** Today the only teardown verbs are `terminate_all` (hard kill) and
`shutdown` (graceful SIGTERM→SIGKILL). Callers often want more: send an arbitrary
signal to the whole tree (e.g. `SIGHUP` to reload), or pause/resume a tree (freeze a
workload, snapshot it, let it run). The POSIX backend already sends signals to its
groups; this generalizes that to any signal plus a freeze/thaw pair.

**Public API sketch.**

```rust
group.signal(Signal::Hup)?;   // broadcast to every member
group.suspend()?;             // freeze the whole tree
group.resume()?;
```

**Where it plugs in.** New `signal`/`suspend`/`resume` methods on the `Job` wrapper
(`src/sys/mod.rs`).

- **POSIX** (`src/sys/pgroup.rs`) — generalize the existing `signal_groups()` /
  `killpg` path to take any signal; suspend/resume are `SIGSTOP`/`SIGCONT`.
- **Linux (cgroup)** — prefer `cgroup.freeze` (write `1`/`0`) over per-pid stops; it
  is atomic over the subtree and races nothing. Fall back to the pgroup path.
- **Windows** — no POSIX signals: `signal` accepts only `Kill` (maps to the existing
  job terminate); other signals return an "unsupported on this platform" error.
  suspend/resume iterate the tree's threads with `SuspendThread`/`ResumeThread`,
  mirroring the existing `resume_process_threads()` snapshot logic.

**Platform matrix.**

| Capability | Win JobObject | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| Arbitrary signal | 🟡 Kill only | ✅ | ✅ | ✅ |
| Suspend / resume | 🟡 per-thread | ✅ (`cgroup.freeze`) | ✅ (`SIGSTOP`/`CONT`) | ✅ |

**Testing.** Spawn a child that prints on `SIGHUP`; assert it reacts. For
suspend/resume, sample CPU time across a freeze window and assert it does not
advance, then resume and assert progress. Windows signal-unsupported is asserted as a
typed error.

**Effort / risk.** **M–L** — the Windows thread-walking suspend is the fiddly part
(must enumerate every thread of every process in the job and avoid suspend/resume
imbalance).

---

## 3. Tree inspection (enumerate member PIDs, `wait_any`)

**Intent.** The group knows its members; callers can't currently ask who they are.
Exposing the live PID set enables diagnostics, targeted action, and dashboards. A
companion `wait_any` lets a caller block on "whichever of these children exits
first" — the natural primitive for supervising several long-lived processes.

**Public API sketch.**

```rust
let pids: Vec<u32> = group.members();
// Return the index + outcome of whichever finishes first.
let (idx, result) = wait_any(&[&server_a, &server_b, &worker]).await?;
```

**Where it plugs in.**

- **Linux (cgroup)** — `Cgroup::members()` already reads `cgroup.procs`; surface it.
- **POSIX** — enumerate from the tracked pgids in `src/sys/pgroup.rs` (probe
  liveness with `kill(-pgid, 0)`, already the idiom there).
- **Windows** — a toolhelp process snapshot filtered by `IsProcessInJob` against the
  job handle.
- **`wait_any`** is pure runner-layer: a `FuturesUnordered` / `tokio::select!` over
  each `RunningProcess`'s wait future — no platform code at all.

**Platform matrix.**

| Capability | Win JobObject | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| List member PIDs | ✅ (`IsProcessInJob`) | ✅ (`cgroup.procs`) | 🟡 leaders only | 🟡 leaders only |
| `wait_any` | ✅ | ✅ | ✅ | ✅ |

> On the pgroup backends the crate tracks group leaders, so `members()` lists leader
> PIDs rather than every descendant — noted in the doc comment.

**Testing.** Spawn N children, assert `members().len() == N`, kill one, assert it
drops out. `wait_any` over two scripted children (one exits fast) asserts the right
index returns first. Both fit the existing integration-style liveness probes.

**Effort / risk.** **S** — mostly surfacing data already computed; `wait_any` is a
small composition.

---

## 4. Stats sampler over time (time-series of CPU / RSS)

**Intent.** `stats()` is a point-in-time snapshot. Benchmarking and diagnostics want
a *series*: peak RSS, average CPU, a curve over the run. A sampler polls the existing
snapshot on an interval and yields a stream (or accumulates peak/avg), so callers get
profiling for free without new syscalls.

**Public API sketch.**

```rust
let mut samples = run.sample_stats(Duration::from_millis(250)); // impl Stream
while let Some(s) = samples.next().await {
    println!("rss={:?} cpu={:?}", s.peak_memory_bytes, s.total_cpu_time);
}
// …or an accumulator:
let summary = run.profile().await?; // { peak_rss, avg_cpu, samples }
```

**Where it plugs in.** Pure composition over `ProcessGroup::stats`
(`src/group.rs:128`) and `sys::process_metrics` (`src/stats.rs`). A background tokio
task ticks `tokio::time::interval`, calls `stats()`, and pushes onto a channel
adapted into a `Stream` (the crate already adapts readers into streams via
`tokio-stream`). No backend changes.

**Platform matrix.**

| Capability | Win JobObject | Linux cgroup | Linux pgroup | macOS/BSD |
|---|---|---|---|---|
| CPU + memory series | ✅ | ✅ | ❌ counts only | ❌ counts only |
| Active-count series | ✅ | ✅ | ✅ | ✅ |

(The matrix simply inherits whatever `stats()` reports per platform.)

**Testing.** Run a child that grows its heap in steps; assert the peak-RSS sample is
monotonic and ends near the expected size (with slack). On pgroup/macOS assert the
CPU/mem fields stay `None` and only the count series populates.

**Effort / risk.** **S** — no new platform code; main care is task lifecycle (the
sampler must stop and not outlive the process / keep the group alive — use a `Weak`
to the group exactly as the existing deadline task does).

---

## 5. Supervisor / restart with policy and backoff

**Intent.** `retry` answers "run this once, replaying on failure." A supervisor
answers the different question "keep this alive": restart a child whenever it exits
(unless it exited cleanly per a predicate), with bounded restarts and exponential
backoff + jitter — a minimal `runit`/`systemd`-style keeper on top of the existing
group machinery.

**Public API sketch.**

```rust
Supervisor::new(Command::new("my-server").args(["--port", "8080"]))
    .restart(RestartPolicy::OnCrash)          // Always | OnCrash | Never
    .max_restarts(5)
    .backoff(Duration::from_millis(200), 2.0) // base, multiplier (+ jitter)
    .stop_when(|res| res.code() == Some(0))   // clean exit ends supervision
    .run()
    .await?;
```

**Where it plugs in.** A new `supervisor` module composing `ProcessGroup` + the
restart loop generalized from `retrying()` (`src/runner.rs:102`) — same
sleep/backoff/classifier shape, but the loop condition is "process exited" rather
than "operation returned `Err`," and the policy decides restart vs stop. Reuses
`RunningProcess` health accessors (`pid`/`cpu_time`/`elapsed`) for logging/metrics.

**Platform matrix.** Platform-agnostic — sits entirely above the group layer, so it
works identically wherever `ProcessGroup` works.

**Testing.** Fully hermetic with `ScriptedRunner`: queue a sequence of replies
(`fail, fail, ok`) and assert the restart count, the backoff timing (via a paused
tokio clock), and that a clean exit ends supervision without a further restart.

**Effort / risk.** **M** — logic-heavy but no FFI; the subtle parts are jitter and
not counting a clean stop as a crash.

---

## 6. Readiness / health probes

**Intent.** The README's own "start a server, then use it" example has no way to wait
until the server is *ready* — callers resort to sleeps. First-class readiness probes
("wait until this line appears" / "wait until this port accepts" / "wait until this
async check passes") remove the race and the arbitrary sleep.

**Public API sketch.**

```rust
let mut run = Command::new("my-server").start().await?;
run.wait_for_line(|l| l.contains("listening on"), Duration::from_secs(10)).await?;
run.wait_for_port("127.0.0.1:8080".parse()?, Duration::from_secs(10)).await?;
run.wait_for(|| async { reqwest::get("…/health").await.is_ok() },
             Duration::from_secs(10)).await?;
```

**Where it plugs in.** `wait_for_line` wraps the existing `stdout_lines()` stream
(`src/running.rs:221`) with a `.find()` bounded by the deadline-timer machinery
already in that file. `wait_for_port` / `wait_for` are generic helpers using
`tokio::net::TcpStream::connect` / a user future under `tokio::time::timeout`. New
`Error::NotReady { program, timeout }` (the `Error` enum is `#[non_exhaustive]`, so
this is non-breaking — `src/error.rs`).

**Platform matrix.** Platform-agnostic — built on streaming + tokio timers, identical
everywhere.

**Testing.** A scripted child that prints a banner after a delay → assert
`wait_for_line` returns after the banner and `NotReady` when the predicate never
matches within the deadline. Port probe against an ephemeral listener.

**Effort / risk.** **S** — pure composition over existing streaming + timers.

---

## 7. Pipelines without a shell

**Intent.** Running `a | b | c` today means either a shell string (quoting and
injection hazards) or manual plumbing of pipes. A typed pipeline builder wires each
child's stdout into the next child's stdin natively, spawns the whole chain into one
shared `ProcessGroup` so it dies as a unit, and reports a pipefail-style outcome — no
shell, so no quoting or injection surface at all.

**Public API sketch.**

```rust
let out = Command::new("git").args(["log", "--format=%an"])
    .pipe(Command::new("sort"))
    .pipe(Command::new("uniq").arg("-c"))
    .output_string()
    .await?;
```

**Where it plugs in.** A new `Pipeline` type. Each stage's stdout feeds the next via
`Stdin::from_reader` (`src/stdin.rs`), which already accepts any async reader. All
stages spawn into one shared `ProcessGroup` (the group already implements
`ProcessRunner`, `src/runner.rs:153`), so dropping the pipeline reaps every stage.
Exit policy: the first non-zero stage wins (pipefail), documented.

**Platform matrix.** Platform-agnostic — uses tokio pipes + the existing group layer,
identical everywhere.

**Testing.** A three-stage pipeline with `ScriptedRunner` stand-ins for each stage
asserts data flows end-to-end and that a non-zero middle stage surfaces as the
pipeline's error. A real integration test (`echo | sort | uniq`) confirms the plumbing.

**Effort / risk.** **M** — the data-flow plumbing and the exit/error-aggregation
policy (which stage's failure to report, how to drain on early exit) need care;
no FFI.

---

## 8. Environment and privileges builder

**Intent.** `Command` already does `env`, `env_remove`, `env_clear`, and `cwd`.
Missing are the spawn-time controls that matter for sandboxing and service launch:
drop privileges (`uid`/`gid`), detach into a new session (`setsid`), inherit only a
whitelisted subset of the parent env, and — on Windows — suppress the console window
for a GUI app spawning a CLI.

**Public API sketch.**

```rust
Command::new("worker")
    .inherit_env(&["PATH", "HOME", "LANG"]) // whitelist on top of env_clear
    .uid(1000).gid(1000)                    // Unix: drop privileges
    .setsid()                               // Unix: new session
    .create_no_window()                     // Windows: no console window
    .run().await?;
```

**Where it plugs in.** Extends `Command` (`src/command.rs`). `inherit_env` builds on
the existing `envs: Vec<(OsString, Option<OsString>)>` + `env_clear` applied in
`build_tokio()`. `uid`/`gid`/`setsid` are set through the Unix `pre_exec` hook
(the same hook the Linux cgroup backend already uses). `create_no_window` adds the
`CREATE_NO_WINDOW` creation flag on Windows.

**Platform matrix.**

| Capability | Windows | Unix (all) |
|---|---|---|
| `inherit_env` whitelist | ✅ | ✅ |
| `uid` / `gid` drop | ❌ | ✅ |
| `setsid` | ❌ | ✅ |
| `create_no_window` | ✅ | ❌ |

**Testing.** `inherit_env` is hermetic via `RecordingRunner` (assert the built
command's env overrides). `uid`/`gid` need a privileged integration test (skipped
unless running as root); assert the child reports the dropped identity. Windows
`create_no_window` is asserted via the creation-flag on the built command.

**Effort / risk.** **M** — small per-item, but the privilege bits are
security-sensitive and must be ordered correctly in `pre_exec` (set gid before uid)
and well-documented.

---

## 9. Cancellation-token integration

**Intent.** The README currently advises bounding a manual stream by wrapping the
loop in `tokio::time::timeout` and dropping the handle. A first-class
`CancellationToken` makes cancellation explicit and composable: hand a token to a
command (or take one from a running process), and cancelling it tears down the whole
tree — the standard structured-concurrency shape.

**Public API sketch.**

```rust
let token = CancellationToken::new();
let run = Command::new("long-job").cancel_on(token.child_token()).start().await?;
// elsewhere:
token.cancel(); // → terminate_all() on the tree, run resolves to Error::Cancelled
```

**Where it plugs in.** Integrates `tokio_util::sync::CancellationToken` into the same
deadline-task machinery in `src/running.rs` (the task already holds a `Weak` to the
group and calls `terminate_all()` on the deadline — cancellation is a second trigger
on the same path). New `Error::Cancelled` (non-breaking; `Error` is
`#[non_exhaustive]`). Adds an optional `tokio-util` dependency, gated behind a feature
flag (e.g. `cancellation`) consistent with the existing optional `tracing`/`mock`
features.

**Platform matrix.** Platform-agnostic — reuses the existing teardown path, identical
everywhere.

**Testing.** Start a sleeping child, cancel the token, assert the run resolves to
`Error::Cancelled` promptly and that a liveness probe shows the tree gone. Hermetic
where possible; one integration test for the real kill.

**Effort / risk.** **S** — small, reuses existing teardown; only new surface is the
optional dependency + feature gate.

---

## 10. Record/replay (golden) runner

**Intent.** Record real subprocess outputs to a fixture once, then replay them
deterministically in tests — a "golden"/cassette mode over the `ProcessRunner` seam.
The natural next step after `ScriptedRunner` (hand-written canned replies) and
`RecordingRunner` (captures what was *run*): instead of authoring replies by hand or
only asserting inputs, capture real `Invocation → ProcessResult` pairs against the
live tool once and serve them offline forever after — fast, hermetic, no real
subprocess in CI.

**Public API sketch.**

```rust
// Record once against the real tool (e.g. an opt-in `--record` test run):
let runner = RecordReplayRunner::record("fixtures/git.json", JobRunner::default());
let client = Git::with_runner(runner);
client.head(repo).await?;            // real git runs; the call is captured
// runner is flushed to fixtures/git.json on drop / explicit save().

// Replay everywhere else — no subprocess, byte-identical results:
let runner = RecordReplayRunner::replay("fixtures/git.json")?;
let client = Git::with_runner(runner);
assert_eq!(client.head(repo).await?, "abc123…");
```

**Where it plugs in.** Builds directly on the existing doubles (`src/doubles.rs`):

- `RecordingRunner` (`src/doubles.rs:226-274`) already captures the **input** —
  `Invocation` (program / args / cwd / envs / has_stdin) — and delegates to a real
  inner runner. The two gaps Item 10 closes: it does **not** capture the **output**
  (`ProcessResult` stdout/stderr/code), and there is **no** serialize-to-disk /
  load-from-disk path.
- New `RecordReplayRunner` (a `ProcessRunner`): in *record* mode it wraps a real
  `JobRunner`, captures each `Invocation → ProcessResult` pair, and writes a JSON
  cassette; in *replay* mode it loads the cassette and serves matching invocations,
  reusing `ScriptedRunner`'s registration-order / first-match idea
  (`src/doubles.rs:157-178`) but keyed on the captured `Invocation` rather than a
  hand-written prefix.
- Serialization is an optional `serde` / `serde_json` dependency behind a feature
  flag (e.g. `record`), consistent with the existing optional `mock` / `tracing`
  gating in `Cargo.toml`.

**Platform matrix.** Platform-agnostic — sits entirely on the `ProcessRunner` seam,
identical everywhere. (A cassette recorded on one OS may embed OS-specific output;
that's a property of the captured data, not of the runner.)

**Testing.** Record against a `ScriptedRunner` (so the "real" runner is itself
hermetic — no subprocess needed), round-trip the cassette through disk, and assert
replay returns byte-identical `ProcessResult`s. Assert that an invocation absent from
the cassette errors in strict replay mode (and, if a passthrough mode is offered,
that it falls through to the inner runner instead).

**Effort / risk.** **S–M** — no platform code; the care points are the match key
(which `Invocation` fields are significant — likely program + args + cwd, with env
optional) and a stable, human-diffable cassette format.

---

## Suggested sequencing

- **Land first (low risk, pure composition):** 3 (tree inspection), 4 (stats
  sampler), 6 (readiness probes), 9 (cancellation), 10 (record/replay). No new FFI;
  each is a small, well-contained win that builds directly on existing
  streaming/stats/teardown or the test-double seam.
- **Medium:** 5 (supervisor) and 8 (env/privileges) — logic- or security-heavy but
  still mostly above the platform layer.
- **Highest platform risk (do deliberately):** 1 (resource limits), 2
  (signals/suspend), 7 (pipelines) — these touch the most backend-specific FFI and
  the widest platform-behaviour spread, so they warrant the most per-platform testing.

As features are chosen for implementation, add their bullets to `CHANGELOG.md`'s
`[Unreleased] → Added` section so they promote cleanly on the next release.
