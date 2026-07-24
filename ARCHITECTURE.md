# Architecture

This document is for **contributors to `processkit` itself** — it explains how
the crate is put together internally: the layers, the seams, how data flows
through a run, and the invariants that must never regress. If you're
*consuming* the crate, start with the [`docs/` guide set](docs/README.md)
instead; this page assumes you've read `src/lib.rs`'s crate doc and are about
to change code under `src/`.

It is a map, not a tutorial: every claim below is a name you can `grep` for and
check against the current source. Where a design choice was deliberately
argued out (and might look surprising without the history), the relevant
section links to its [`decisions/`](decisions/) record.

## The layers

A single run passes through five layers, top to bottom:

```text
Command            (command.rs)              — describe a run
  │
runner / client    (runner.rs, client.rs)     — dispatch the seam
  │
running            (running/{mod,probes,stream}.rs) — the live handle
  │
pump / buffer      (pump.rs, buffer.rs)        — drain + decode + cap
  │
sys backends       (sys/{mod,windows,linux,unix,pgroup,graceful}.rs) — OS containment
```

- **`Command`** (`command.rs`) is a plain-data builder: program, args, cwd,
  env, stdin source, timeout, buffer policy, encodings, line handlers/tees,
  retry, and the Unix/Windows spawn knobs (`uid`/`gid`/`groups`/`setsid`/
  `priority`/`umask`/`kill_on_parent_death`). It holds no OS resources and does
  nothing by itself (`#[must_use]` says so on the type) — every consuming verb
  (`output_string`, `run`, `start`, …) hands the built command to a runner.
  `Timeout` is its own three-case enum (`Unset` / `Unbounded` / `After(Duration)`)
  rather than an `Option<Duration>` + `bool` pair, so "explicitly unbounded" is a
  variant, not an invariant two fields have to agree on.

- **runner / client** is the seam layer. [`ProcessRunner`](src/runner.rs) is the
  trait every run goes through — `output_string` (required), `output_bytes` and
  `start` (defaulted to `Error::Unsupported`, so a capture-only double stays a
  one-method `impl`). [`JobRunner`](src/runner.rs) is the real, production
  implementation: every call gets a **fresh, private** `ProcessGroup` that the
  returned handle owns. `ProcessGroup` itself also implements `ProcessRunner`
  (`&ProcessGroup` as a *shared*-group runner — used by `Pipeline` stages and
  `Supervisor::with_runner`). `ProcessRunnerExt` layers the whole run
  vocabulary (`run`/`run_unit`/`checked`/`exit_code`/`probe`/`parse`/
  `try_parse`/`first_line`, plus retry) over the one required method, so any
  `ProcessRunner` — real or fake — gets the full verb set for free.
  [`CliClient`](src/client.rs) is one further layer up: it owns a program name,
  a `ProcessRunner` (generic, injectable), and client-wide defaults (timeout,
  env, cancellation), and hands out preconfigured `Command`s — the reusable
  core the `cli_client!` macro scaffolds a typed CLI wrapper around.

- **running** (`running/mod.rs` + `probes.rs` + `stream.rs`) is
  [`RunningProcess`](src/running/mod.rs) — the live handle to a spawned child,
  split by concern across the three files: `mod.rs` owns the handle's state and
  the *consuming* capture paths (drive-to-exit, kill/teardown, the post-exit
  checkpoint); `probes.rs` holds the **non-consuming** readiness probes
  (`wait_for_line`/`wait_for`/`wait_for_port`); `stream.rs` holds the
  incremental streaming surface (`StdoutLines`/`ProcessEvents`) and the
  watchdog tasks that bound a *streamed* run. `RunningProcess` wraps one of two
  backends (`Backend::Real` — a real `tokio::process::Child` — or
  `Backend::Scripted` — a canned reply from the test doubles), so the capture
  logic above this split is backend-agnostic.

- **pump / buffer** (`pump.rs`, `buffer.rs`) is the shared I/O engine: a
  background task per stream reads raw bytes, feeds them through one
  persistent `encoding_rs::Decoder` (so multi-byte/stateful encodings and BOMs
  are handled correctly across read boundaries), splits the decoded text into
  lines per the configured [`LineTerminator`](src/buffer.rs), and writes each
  line into a [`SharedLines`](src/pump.rs) buffer capped by
  [`OutputBufferPolicy`](src/buffer.rs) (`DropOldest`/`DropNewest`/`Error`,
  bounded by `max_lines`/`max_bytes`). The same pumped lines feed an optional
  per-line handler and an optional async tee sink — both isolated so a panic or
  a write error disables just that sink instead of poisoning the run. This is
  the **one** definition of "a line" in the crate: streaming verbs, handlers,
  tees, and `output_string`'s join all read the same split.

- **sys backends** (`sys/mod.rs` + one platform file) is the containment
  primitive: `sys::Job` wraps a Windows Job Object, a Linux cgroup v2 (falling
  back to a POSIX process group when no writable cgroup exists), or a POSIX
  process group on macOS/BSD. Exactly one platform module compiles in per
  target (selected by `#[cfg_attr(..., path = ...)]` in `sys/mod.rs`); every
  `imp::Job` exposes the same inherent methods (`spawn`, `kill_all`, `signal`,
  `suspend`/`resume`, `members`, `graceful_shutdown`, `stats`, `mechanism`) so
  the layers above never branch on platform. `sys/pgroup.rs` is the process-group
  backend shared by the Linux fallback and the Unix backend; `sys/graceful.rs`
  is the shared signal → grace → kill escalation driver for both. `group.rs`'s
  [`ProcessGroup`](src/group.rs) is the public type wrapping `Job` plus
  `ProcessGroupOptions` — the "process groups" half of the crate's two-layer
  public model (`src/lib.rs`'s crate doc), independent of whether a run ever
  goes through `Command` at all.

Why cut here rather than, say, one `execution` module: each layer owns exactly
one concern and one direction of dependency (`Command` never spawns anything;
`running` never knows about buffer policy internals beyond the `SharedLines`
handle it holds; `sys` never knows a line exists). That keeps the platform
duplication honest and contained — `sys/{windows,linux,unix,pgroup}.rs` are
allowed to diverge in *how* they contain a tree, but every layer above sees the
same `Job` shape (confirmed sound, not to be re-litigated without a new
argument — see
[`decisions/architecture-audit-2026-06.md`](decisions/architecture-audit-2026-06.md)).

## Seams

The single mock point for the whole crate is `ProcessRunner` (`runner.rs`).
Everything that needs to run a command — `Command`'s own helpers, `CliClient`,
`Pipeline`, `Supervisor` — takes `&dyn ProcessRunner` (or is generic over
`R: ProcessRunner`) rather than calling `tokio::process` directly, so
production code and test code share one code path up to that point.

Real implementors: `JobRunner` (fresh private group per run) and
`ProcessGroup`/`&ProcessGroup` (shared group — the caller controls the group's
lifetime, so dropping the returned `RunningProcess` does **not** tear the tree
down).

Test doubles (`doubles.rs`, re-exported as `processkit::testing`):

- [`ScriptedRunner`](src/doubles.rs) — canned `Reply`s matched by
  program+argv prefix or predicate. Its `start()` hands back a scripted
  `RunningProcess` whose canned lines flow through the **same pump machinery**
  a real child uses (`pump_lines_core`), so `stdout_lines`/`wait_for_line`/
  `finish` behave identically to a live run — this is deliberate: the fake and
  the real backend diverge only in *where the bytes come from*
  (`Backend::Scripted` vs `Backend::Real`), never in how they're consumed.
- [`RecordingRunner`](src/doubles.rs) — wraps another runner and records every
  `Invocation`, for tests that assert on *what* was run rather than faking the
  result.
- `MockRunner` (the `mock` feature, `mockall::automock`-generated) —
  expectation-style mocking. Semver-exempt (tracks the `mockall` dependency,
  not the crate's own guarantees); `ScriptedRunner`/`RecordingRunner` are the
  recommended, stable doubles.
- [`RecordReplayRunner`](src/cassette.rs) (the `record` feature) — record a
  real run once to a JSON cassette (`Invocation → ProcessResult`, matched on
  program+args+stdin digest, deliberately **not** `cwd` — see the type's own
  doc for the portability argument), then replay it hermetically. Does not
  cover `start()`/streaming runs (inherits the `Unsupported` default) — a
  streamed recording needs line timing and stream shape the current schema
  doesn't carry; deferred until a real consumer asks (see
  [`decisions/architecture-audit-2026-06.md`](decisions/architecture-audit-2026-06.md)).

A deliberately-rejected alternative: a formal `Job` trait over the platform
backends in `sys/`. The compiler only checks the trait against whichever
target it's compiling for, so a trait would add no cross-platform compile-time
safety over today's duck-typed `imp::Job` contract — CI's cross-target clippy
(Linux ×2, macOS, Windows) is what actually catches divergence; a trait would
only centralize documentation at the cost of trait-object noise in a seam
that's deliberately concrete (again,
[`decisions/architecture-audit-2026-06.md`](decisions/architecture-audit-2026-06.md)).

## Data flow

**Launch** (`runner::launch`, shared by `JobRunner::start` and
`ProcessGroup::start`): validate the spawn-time knobs the target can't honor
(fail loud rather than silently drop a requested `uid`/`setsid`/`umask` on an
unsupported platform), check an already-cancelled token up front, validate
`cwd` (so a bad working directory reports as itself, not a confusing bare
`ENOENT` that looks like "program not found"), atomically take the command's
stdin source (`take_stdin_for_run` — a one-shot source observed as consumed by
a concurrent or later re-run fails loud instead of silently feeding EOF),
build the `tokio::process::Command`, and spawn it into the group
(`group.spawn_with_options`). A `NotFound` spawn error is enriched by
searching `PATH` when the program was a bare name, so the error says *found on
PATH but not executable* vs *not found at all*. The result is wrapped into a
`RunningProcess` via `Spawned` (every per-run knob the handle needs: encodings,
handlers, tees, buffer policy, ok-codes, the cancel token, …), and the cancel
watchdog is armed.

**Consuming a live handle**: capture verbs (`output_string`/`output_bytes`/
`wait`/`finish_lines`) drive both streams through `pump_lines_core` into a
`SharedLines` per stream, then `drive_to_exit` races the child's natural exit
against the cancel token and the deadline (see "Cancel-vs-timeout arbitration"
below). Streaming verbs (`stdout_lines`/`events`) instead hand the
caller a `Stream` reading directly off `SharedLines`, and arm a *separate*
watchdog if the run has a timeout (so a streamed run's deadline still fires
even though nothing is presently awaiting `drive_to_exit`). A second call to a
one-shot streaming verb is a loud `Error::Io`, not a silently empty stream —
stdout streams once.

**Stdin**: a `keep_stdin_open` command hands the caller a `ProcessStdin`
writer directly; otherwise a one-shot payload (from `Stdin::from_reader`/
`from_lines`) is written on a **background** task (`tokio::spawn`), so a large
payload can't deadlock against the child's own stdout backpressure — dropping
the sink sends EOF.

**Watchdogs.** Two independent tasks can end a run early, both racing a single
CAS arbiter (`timeout_state`, `running/mod.rs`):

- The **cancel watchdog** (`arm_cancel_watchdog`) waits on the command's
  `CancellationToken` and, if it fires while the arbiter is still `PENDING`,
  kills the tree (group, if owned) and the direct child (by pid, always — a
  belt-and-suspenders kill for a shared-group handle). Re-armed by
  `attach_group` once the owning group is known, upgrading from a pid-only
  kill to a full group+pid kill.
- The **deadline** is armed inline inside `drive_to_exit_inner`'s `select!`
  for a bulk/waited run, or via `arm_scripted_deadline`/a `stream.rs` watchdog
  for a streamed run (so the deadline still fires while the caller is only
  reading lines, not awaiting exit).

## Key invariants

### Kill-on-drop

The tree-level guarantee lives at the **group**, not the handle:
`ProcessGroup`'s `Drop` hard-kills every process still inside it (via the
platform `Job`'s own `Drop`) — that's what makes an exiting or panicking owner
never leak a subprocess. `RunningProcess` participates in this in two
different ways depending on how it was obtained:

- **Private one-shot** (`JobRunner::start`, and every one-shot helper built on
  it): the handle owns its group as `Arc<ProcessGroup>` (`Backend::Real::own_group`).
  When the handle is the group's last owner and drops, the `Arc` drops the
  group, which hard-kills the tree — this is why "keep the handle in scope" is
  the documented contract on `Command::start`.
- **Shared group** (`ProcessGroup::start` called directly, `Supervisor::with_runner`):
  the handle does **not** own the group, so dropping it leaves the group (and
  any sibling processes) intact; the *group's* lifetime is what governs, and
  the caller controls that explicitly.
- **Pipeline stages** spawn through the same shared-group `ProcessGroup::start`
  seam, but into a **fresh, per-stage** group that `Pipeline::capture` then
  attaches back onto the handle (`RunningProcess::attach_group`) — so each
  stage's handle *does* own its own sub-group, closer to the private one-shot
  case. This is what lets a per-stage `Command::timeout` (or the chain-wide
  teardown) reach the stage's whole subtree, grandchildren of a forking
  `sh -c …` included, instead of only its direct child by pid.

Separately, `RunningProcess::Drop` **always** aborts its own background tasks
regardless of group ownership — the stdin writer, the deadline/cancel
watchdogs, and the stdout/stderr pump tasks. This matters even on a
shared-group handle where dropping doesn't kill anything: a surviving
grandchild holding a pipe open could otherwise keep a pump task alive
indefinitely.

The `Drop` guarantee covers every exit that runs destructors (a return, an
unwinding panic). An **abrupt** owner death (`SIGKILL`, `abort()`) skips `Drop`
entirely — on Windows the kernel still reaps the tree because the Job Object
handle closes with the process; on Linux/macOS/BSD that hardening is the
opt-in `Command::kill_on_parent_death` (Linux-only via `PR_SET_PDEATHSIG`, no
equivalent on macOS/BSD). This asymmetry is inherent to the mechanisms, not a
gap to "fix" — see `sys/mod.rs`'s module doc and `group.rs`'s doc comment on
[`ProcessGroup`](src/group.rs).

### Teardown paths

Three distinct tiers, at two different scopes:

- **`kill_tree`** (`running/mod.rs`) — immediate hard kill: `start_kill` on the
  direct child plus `group.kill_all()` if the handle owns its group, then a
  *bounded* reap (`PUMP_TEARDOWN`, 5s) so a `D`-state child that ignores
  `SIGKILL` can't hang teardown forever.
- **`teardown_on_timeout`** (`running/mod.rs`) — used when a deadline (not a
  cancel) fires *with* `Command::timeout_grace` set: signal (default
  `SIGTERM`) → wait up to the grace window (concurrently reaping, so a
  signal-handling child ends the grace early) → hard kill. Without a grace
  window this degrades to `kill_tree`. Windows has no soft-signal tier at all,
  so its graceful path is always the atomic job kill regardless of grace.
- **`ProcessGroup::shutdown`/`graceful_shutdown`** (`group.rs`/`sys/mod.rs`) —
  the *group*-level graceful tier: `SIGTERM` → wait `shutdown_timeout` →
  `SIGKILL` survivors if `escalate_to_kill`. This is a **method**, not `Drop`,
  because `Drop` can't `await`; `escalate = false` is honored via a
  cross-backend `SkipDropKill` latch (`sys/mod.rs`) that the group's `Drop`
  reads to skip the hard kill and spare the survivors the caller chose to
  leave running. The latch re-arms (`clear()`) on the next spawn into a reused
  group, so a stale "don't kill" decision can't silently spare a *different*,
  later child.

### Cancel-vs-timeout arbitration

`timeout_state` (`running/mod.rs`, a single `AtomicU8`: `PENDING` →
`EXITED`/`TIMED_OUT`) is the one CAS arbiter every code path that might
classify a run's ending goes through — a natural reap, a fired deadline, and a
liveness poll (`has_exited_now`) all attempt the same `compare_exchange` from
`PENDING`, so whichever wins first is the classification, race-free even when
the child exits within a scheduler quantum of the deadline. `drive_to_exit_inner`'s
`select!` is additionally `biased` with the cancel arm listed first, so a
cancel and a deadline firing in the same poll always resolve to cancellation,
never a race between the two outcomes. Cancellation itself is classified
**after** this arbiter, downstream in the consuming verbs — a fired deadline
becomes `Outcome::TimedOut` even if the child happened to exit cleanly inside
the grace window (`classify_timed_out`), while a genuine cancel always
surfaces as `Err(Error::Cancelled)`, never bundled into an `Outcome`. A
repeated wait/probe after the run has already been classified must reread the
same snapshot rather than re-evaluate a since-cancelled token — this is what
`cancel_at_exit`'s first-observation-only snapshot in `has_exited_now`/
`drive_to_exit` guards against (see the regression tests in `src/lib.rs` —
`wait_any`/`wait_all` "preserved after late cancel").

## Why the modules are cut this way

The layer boundaries above track *who needs to know what*, not file size:

- `Command` is pure data so it can be `Clone`d cheaply, sent across an
  `async` boundary, and reused (a `Pipeline` stage, a retried run) without
  re-parsing a spawn description.
- The `ProcessRunner` seam sits exactly at the boundary between "describe a
  run" and "actually touch the OS", so everything downstream of it (retry,
  `CliClient`, `Supervisor`, `Pipeline`) is testable against a double without
  caring whether the real backend is `JobRunner` or a shared `ProcessGroup`.
- `running` is split three ways (state/capture vs probes vs streaming) because
  those are genuinely different *consumption disciplines* over one handle: the
  probes are non-consuming and never affect the run's outcome, the streaming
  surface is the one-shot-stdout path, and the state/capture file is where the
  handle's invariants (the arbiter, `Drop`) actually live. Keeping them in one
  file would bury the non-consuming/one-shot distinction in file-scroll rather
  than module structure.
- `pump`/`buffer` are separate from `running` because the pump has no idea
  it's draining a *child's* pipe — it operates on any `AsyncRead` — and is
  exercised directly by its own unit tests and a fuzz target
  (`fuzz/fuzz_targets/decode_pump_lines.rs`) without spinning up a process at
  all.
- `sys` is isolated behind one inherent-method shape per platform specifically
  so the divergence between Windows Job Objects, Linux cgroups, and POSIX
  process groups stays *honest* — each backend does what its OS actually
  offers, rather than a lowest-common-denominator abstraction papering over
  real capability differences (Windows' atomic-only kill, no graceful tier at
  all; the POSIX pgroup's recycled-pid window, a kernel limitation, not a bug
  to fix in userspace — see
  [`decisions/architecture-audit-2026-06.md`](decisions/architecture-audit-2026-06.md)).

Two crate-wide standing decisions shape several of the choices above and are
worth reading before touching the affected code:

- [`decisions/runtime-tokio-only.md`](decisions/runtime-tokio-only.md) — why
  there is no sync variant and no runtime abstraction; the reliability core
  (the watchdogs, the pump tasks, the arbiter) is inherently concurrent and
  tokio is woven through every layer above `sys`.
- [`decisions/interface-freeze-2026-06.md`](decisions/interface-freeze-2026-06.md) —
  the pre-1.0 signature/type shapes kept as-is (e.g. no `From<&str> for Command`,
  `usize` batch concurrency), so a change that looks like a small ergonomics
  win may already have been argued against.

See [`decisions/README.md`](decisions/README.md) for the full index, and
[`ideas/README.md`](ideas/README.md) for open proposals that would touch this
architecture if taken up.
