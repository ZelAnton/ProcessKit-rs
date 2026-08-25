# ProcessKit v4: runtime-neutral core roadmap

> **Status:** proposed architecture for the next breaking release.
>
> **Recommendation:** a runtime-neutral `processkit` core with replaceable,
> additive runtime backends. The first official backends are Tokio and an
> async-io/async-process path.
>
> **Current availability:** none of this architecture is implemented in v3.
> This document is a delivery plan and design boundary, not a compatibility
> promise for the current API.

## Decision summary

The recommended v4 architecture makes the async runtime an explicit dependency
of a `ProcessKit` instance. The public `ProcessKit`, `Command`, `RunningProcess`,
`ProcessGroup`, result, event, and client types do not carry a runtime generic.
Internally, `ProcessKit` stores type-erased capability objects supplied by the
selected backend.

The initial package shape is:

```text
processkit                 runtime-neutral public core and orchestration
├── command/result/client  specifications and value types
├── backend_api            object-safe capability contracts
├── platform               one shared containment implementation per OS
└── orchestration          pumps, deadlines, cancellation, pipelines, supervision

processkit-tokio           Tokio child/I/O/task/clock/readiness adapters
processkit-async-io        async-process/async-io child/I/O/clock adapters,
                           with an injected Send task spawner
```

Applications may depend on either adapter or both. Runtime selection is not a
pair of mutually exclusive Cargo features: Cargo feature unification makes that
model unsafe for libraries that meet in a larger dependency graph. Core behavior
features remain additive.

The OS containment implementation remains single-owned by `processkit`.
Adapters provide process and runtime mechanics, but do not copy the Windows Job
Object, Linux cgroup v2, FreeBSD process-reaper, or POSIX process-group logic.

### What is decided

- The core dependency path is strictly Tokio-free, not merely usable from a
  non-Tokio application while Tokio still runs internally.
- Runtime selection is explicit at the `ProcessKit` boundary and type-erased
  inside public live handles.
- The core has no implicit default runtime backend; adapter crates own their
  convenience constructors and extension traits.
- Tokio and non-Tokio adapters are additive and may coexist in one process.
- Core stream and async-I/O contracts use `futures_core::Stream` and
  `futures_io::{AsyncRead, AsyncWrite}` (or a semantically equivalent owned
  compatibility layer if a feasibility spike proves a concrete blocker).
- Process lifecycle, task spawning, time, and TCP readiness are separate
  capabilities rather than methods on one monolithic runtime trait.
- Platform containment and its rollback rules remain common to every backend.
- The Tokio adapter owns all public Tokio interop.

### What is still provisional

- Final crate names and whether the backend-author API is a module in
  `processkit` or a small, separately versioned support crate.
- Exact trait signatures, boxed-future aliases, and whether a few capabilities
  use enums instead of trait objects after measurement.
- Whether the public owner is named `ProcessKit`, `Engine`, or `ProcessEngine`.
  This roadmap uses `ProcessKit` as the working and recommended name.
- The exact `async-process` integration on Windows, including raw-handle access
  and conversion of a prepared `std::process::Command`.
- Performance budgets. They must be set from v3 baselines before the first alpha.

These questions must be closed by ADRs before the broad migration begins; they
do not weaken the architectural decision above.

## Why v4 needs this boundary

There are two different requests hidden behind “non-Tokio runtime”:

1. A caller wants to run ProcessKit from smol or async-std but accepts Tokio in
   the dependency graph and as an internal executor.
2. A caller requires a Tokio-free dependency graph and no Tokio reactor,
   scheduler, clock, or I/O types at runtime.

A compatibility bridge can satisfy the first case. It cannot satisfy the
second. v4 targets the stricter case; the easier case then works through either
official backend.

The committed v3 implementation has useful seams, but is not runtime-neutral:

- [`Command`](../src/command.rs) builds `tokio::process::Command`, exposes
  `to_tokio_command`, and accepts Tokio async writers for tee sinks.
- [`ProcessGroup`](../src/group.rs) spawns and adopts Tokio child handles.
- [`RunningProcess`](../src/running/mod.rs) owns Tokio child, stdin, and task
  handles.
- [`Stdin`](../src/stdin.rs) accepts Tokio async readers and writers.
- The crate prelude and cancellation surface in [`lib.rs`](../src/lib.rs)
  re-export Tokio stream extensions and `tokio_util::CancellationToken`.
- Pumps, deadlines, readiness probes, pipelines, and supervision call Tokio
  I/O, time, synchronization, and task APIs directly.

Those are observations about v3, not a requirement to preserve its internal
shape. The reusable foundations are the command specification, result and event
value types, the object-safe runner/test seam, and the common platform
containment layer.

## Goals

- Provide a documented Tokio-free dependency path for capture, streaming,
  stdin, cancellation, readiness, pipelines, supervision, PTY, and containment.
- Preserve v3's observable result, error, buffering, decoding, line-normalization,
  tracing privacy, and teardown behavior unless a migration note explicitly
  records a deliberate v4 change.
- Preserve kill-on-drop and whole-tree containment to the same
  mechanism-specific scope v3 documents for each platform.
- Let Tokio and async-io backends coexist in one binary and even be selected for
  different `ProcessKit` instances.
- Keep public command and live-handle types non-generic over the runtime.
- Keep scripted, recording, mock, and record/replay testing possible without a
  real child or an OS containment object.
- Make backend conformance independently testable with one contract suite.
- Leave room for additional backends without committing v4.0 to every async I/O
  model in the Rust ecosystem.

## Non-goals

- Implementing the refactor in this roadmap task. v3 source, manifests, public
  API, dependency graph, and runtime behavior remain unchanged.
- Supporting every executor in v4.0. The first non-Tokio target is the
  readiness-based async-io ecosystem.
- Designing an in-house cross-platform process reactor or child reaper before
  the official backend spikes show it is necessary.
- Making runtime selection a global singleton or ambient “current runtime”.
- Maintaining two independent copies of pipeline, supervision, buffering,
  containment, or error logic.
- Promising `!Send` futures or completion-based I/O in v4.0. Monoio, Compio, and
  other local/completion-based models need a later `LocalProcessKit`-style
  design or a distinct adapter boundary.
- Extracting a new general-purpose synchronous containment product as a side
  effect. Shared platform code is an implementation boundary for v4, not an
  automatically stabilized `processkit-sys` API.

## Target architecture

```text
user code / CliClient / typed wrappers
                  │
                  ▼
        ProcessKit + Command specification
                  │
        runtime-neutral orchestration
      ┌───────────┼───────────┬────────────┐
      ▼           ▼           ▼            ▼
 ProcessBackend TaskSpawner  Clock       NetProbe
      │           │           │            │
      └───────────┴─────┬─────┴────────────┘
                        ▼
       TokioBackend or AsyncIoBackend
                        │
                        ▼
       shared platform launch transaction
      ┌──────────┬──────────┬──────────────┐
      ▼          ▼          ▼              ▼
 Windows Job  Linux cgroup FreeBSD reaper POSIX pgroup
```

### Core ownership

The `processkit` core owns:

- immutable command specifications and validation;
- result, event, error, stats, limits, and report types;
- bounded buffers, decoding, capture policies, and line accounting;
- cancellation arbitration and teardown-cause precedence;
- pumps, pipeline and supervisor state machines, retry, and readiness policy;
- `ProcessGroup` ownership and every platform containment implementation;
- the backend-author contracts and the shared conformance suite;
- the high-level `ProcessRunner` mock seam.

The core must compile and expose its public API without a Tokio dependency.

### Adapter ownership

Each official adapter owns only runtime mechanics:

- constructing and spawning the runtime's child-process wrapper;
- exposing the child's stdin/stdout/stderr through runtime-neutral erased I/O;
- waiting, polling, killing, and retrieving the platform process identity or raw
  process handle needed by the common launch transaction;
- spawning, joining, and aborting background futures;
- monotonic sleeps and deadline wakeups;
- TCP connect attempts for readiness probes;
- adapter-specific escape hatches and compatibility traits.

An adapter does not reimplement buffering, error classification, retries,
pipeline rollback, supervision policy, or platform tree containment.

### The shared launch transaction

Process creation is not “spawn first, contain later whenever convenient”. The
core backend API exposes an opaque launch transaction. An adapter supplies the
runtime-specific raw-spawn operation and child wrapper; the core owns the
ordering, guard, rollback, and publication of the resulting child.

On Windows every backend must preserve this invariant:

```text
CREATE_SUSPENDED -> arm rollback guard -> assign Job Object
                 -> apply contained-child settings -> resume -> publish child
```

No `RunningProcess` may become visible before assignment and resume succeed.
Every failure after spawn but before containment publication must reap the
still-suspended child. The current sequence is implemented in
[`src/sys/windows.rs`](../src/sys/windows.rs); the v4 extraction must move it,
not duplicate or weaken it.

Linux cgroup v2, the Linux process-group fallback, the FreeBSD reaper, and the
other POSIX process-group targets likewise keep their existing ordering,
rollback, and mechanism-specific containment scope. A backend is conformant
only when it drives those shared transactions.

## Public API direction

The intended ergonomic shape is explicit backend construction with a
non-generic owner:

```rust,ignore
use processkit::ProcessKit;
use processkit_async_io::AsyncIoBackend;

let kit = ProcessKit::new(AsyncIoBackend::with_spawner(my_spawner));
let result = kit
    .command("git")
    .args(["status", "--short"])
    .output_string()
    .await?;
```

Tokio applications choose the other adapter without changing the command or
result vocabulary:

```rust,ignore
use processkit::ProcessKit;
use processkit_tokio::TokioBackend;

let kit = ProcessKit::new(TokioBackend::new());
let mut child = kit.command("server").start().await?;
```

`ProcessKit::new` may be generic over its constructor argument, but it erases
that value to `Arc<dyn RuntimeBackend>` immediately. `ProcessKit`,
`Command`, `RunningProcess`, `ProcessGroup`, `Pipeline`, and `CliClient` do not
become `Type<R>` throughout the public API.

`Command` remains a reusable specification/builder. Execution verbs are bound
to the `ProcessKit` that created or is asked to execute the command. A small
Tokio extension trait may retain familiar v3 shortcuts, but those shortcuts
live in `processkit-tokio`, not the core.

### Runtime-neutral vocabulary

- Public readers and writers use `futures_io::AsyncRead` and
  `futures_io::AsyncWrite`.
- Public output and lifecycle streams implement `futures_core::Stream`.
- Convenience `.next()` comes from `futures_util::StreamExt`, not a core
  re-export of `tokio_stream::StreamExt`.
- Core defines `processkit::CancellationToken`. It is clone-shared,
  cancellation is monotonic and idempotent, and a waiter registered concurrently
  with `cancel()` cannot miss the transition.
- The core token exposes no Tokio types. Adapter-specific conversions, where
  useful, live in extension traits in the adapter crate.
- Runtime-specific raw command, child, I/O, and abort-handle conversions are
  opt-in adapter APIs. They are never required for ordinary core use.

## Capability contracts

The following pseudocode defines responsibilities, not final source signatures:

```rust,ignore
pub trait RuntimeBackend: Send + Sync {
    fn processes(&self) -> &dyn ProcessBackend;
    fn tasks(&self) -> &dyn TaskSpawner;
    fn clock(&self) -> &dyn Clock;
    fn network(&self) -> Option<&dyn NetProbe>;
}

pub trait ProcessBackend: Send + Sync {
    fn spawn<'a>(&'a self, request: SpawnRequest<'a>)
        -> BoxFuture<'a, Result<Box<dyn ChildHandle>>>;
}

pub trait TaskSpawner: Send + Sync {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> Box<dyn TaskHandle>;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> MonotonicInstant;
    fn sleep_until(&self, deadline: MonotonicInstant) -> BoxFuture<'static, ()>;
}

pub trait NetProbe: Send + Sync {
    fn connect<'a>(&'a self, target: SocketAddr)
        -> BoxFuture<'a, io::Result<Box<dyn ProbeConnection>>>;
}
```

The contracts are deliberately narrow:

### `ProcessBackend` and `ChildHandle`

- `spawn` consumes a validated `SpawnRequest` and participates in the core-owned
  containment transaction.
- A child exposes an immutable process identity plus `try_wait`, `wait`, and hard
  kill operations with normalized error semantics.
- stdin is an erased `AsyncWrite`; stdout/stderr are erased `AsyncRead` values.
  Taking a pipe transfers it exactly once and reports a second take.
- Dropping the child wrapper cannot disarm the owning `ProcessGroup`. A live
  child's last owner either retains synchronous kill-on-drop ownership or hands
  wait/reap responsibility to a joined teardown path.
- Kill acceptance is not reap confirmation. Lifecycle completion requires the
  backend's wait/reap proof.
- Raw pid/handle access is internal to the launch transaction unless an adapter
  deliberately exposes a documented escape hatch.

### `TaskSpawner` and `TaskHandle`

- v4.0 tasks are `Send + 'static`; support for local `!Send` tasks is deferred.
- `abort()` requests cancellation but is not evidence that the future stopped.
- `join()` reports completion, panic, or abort and is the only proof that a task
  released its child/I/O ownership.
- Core teardown code aborts and then joins where resource lifetime matters. It
  must not assume dropping a task handle cancels the task.
- Backends document whether a task can start inline before `spawn` returns; core
  state machines must be correct either way.

### `Clock`

- Deadlines use one backend-neutral monotonic instant domain.
- `sleep_until` defines immediate completion for an elapsed deadline and never
  converts through wall-clock time.
- Timeout, inactivity, grace, retry, probe, and supervisor scheduling all use
  this capability.
- A deterministic manual clock is part of the conformance harness. Virtual-time
  behavior is tested as a contract rather than inferred from Tokio's paused clock.

### `NetProbe`

- TCP readiness is optional because process capture does not require networking.
- Absence produces the existing structured `Unsupported` class before a probe
  task is started.
- Connect cancellation closes the in-flight connection attempt and joins any
  helper task.
- HTTP or custom probes remain policies layered above this primitive rather than
  additions to the process backend.

### Cancellation

Cancellation belongs to core orchestration, not to `TaskSpawner` or a runtime's
token type. The required semantics are:

- clone-shared, monotonic, and idempotent state;
- no missed wakeup between checking state and registering a waiter;
- deterministic precedence between natural exit, timeout, cancellation, and
  teardown failure;
- one teardown owner, with every other observer sharing its terminal result;
- graceful cancellation followed by the same mechanism-specific hard-kill and
  reap confirmation used by timeout and explicit shutdown.

The feasibility phase chooses the concrete notification primitive only after it
passes loom/model tests and both runtime backends.

## Official backends

### Tokio backend: behavioral baseline

`processkit-tokio` is the first migration target. It wraps
`tokio::process`, Tokio task spawning and time, Tokio TCP, and Tokio I/O through
compatibility adapters that implement the core's `futures_io` contracts.

It owns:

- conversions from and to `tokio::process::Command` where the conversion can
  preserve ProcessKit validation and containment;
- Tokio `AsyncRead`/`AsyncWrite` compatibility helpers;
- Tokio cancellation-token conversion or forwarding helpers;
- Tokio-native task and clock implementations.

The adapter is the behavioral oracle during extraction: its conformance results
must match the v3 baseline before the non-Tokio backend is allowed to define new
semantics by accident.

### Async-io backend: first Tokio-free path

`processkit-async-io` should first attempt an `async-process` child implementation
with async-io time and readiness. Task spawning is injected so the same backend
can be hosted by smol, async-std, or another ordinary executor that polls `Send`
futures.

This is a candidate implementation, not a pre-spike guarantee. The spike must
prove:

- prepared-command conversion retains every environment, cwd, stdio, Unix
  pre-exec, and Windows creation option ProcessKit needs;
- child pid/raw-handle access is sufficient for the common containment launch
  transaction;
- dropping or cancelling wait futures does not lose reap ownership;
- exact pipe EOF and backpressure behavior matches the Tokio backend;
- the dependency graph and public API remain Tokio-free;
- smol and async-std hosts pass the same behavioral suite.

If `async-process` fails one of those gates, the next option is a small
ProcessKit-owned adapter around async-io plus the platform wait primitives. That
fallback is a separately estimated decision; it is not silently included in the
v4.0 schedule.

### Deferred runtime families

Monoio, Compio, and similar runtimes use local and/or completion-based ownership
models that do not map honestly onto v4.0's `Send` readiness-I/O contracts. They
remain a follow-up investigation. A likely direction is a `LocalProcessKit` with
different task and buffer ownership, not weakening the main contracts with
optional `Send` bounds or backend-specific special cases.

## Alternatives considered

| Alternative | Decision | Reason |
|---|---|---|
| Keep Tokio internally and add a compatibility bridge | Not the v4 solution | Helps non-Tokio callers, but cannot produce a Tokio-free dependency or reactor path. |
| Mutually exclusive `tokio` / `async-io` core features | Rejected | Cargo unifies features, so two downstream users can accidentally enable an invalid combination. It also prevents two backends in one process. |
| Make every public type generic over `R: Runtime` | Rejected | Infects results, clients, pipelines, trait objects, mocks, and downstream wrapper APIs with a runtime parameter. |
| Copy the full crate once per runtime | Rejected | Duplicates containment and orchestration, making behavior and safety fixes drift. |
| One giant `Runtime` trait | Rejected | Forces unrelated capabilities onto simple backends and makes clocks, tests, and readiness hard to substitute independently. |
| Write a custom cross-platform process reactor first | Deferred | Highest control and highest cost; justified only if official primitives fail the feasibility gates. |
| Core plus type-erased capability backends | Recommended | Keeps the public model stable, permits coexistence, and centralizes safety-critical platform behavior. |

## v3 to v4 migration map

Names below are directional examples; the migration guide will use the final
ratified API.

| Scenario | v3 | Recommended v4 direction |
|---|---|---|
| Construct a command | `Command::new("git")` | `kit.command("git")`, yielding a reusable runtime-neutral command specification. |
| Capture and wait | `Command::new("git").output_string().await` | `kit.command("git").output_string().await`; execution is bound to the explicit kit. |
| Start a live child | `Command::new("server").start().await` | `kit.command("server").start().await`; `RunningProcess` remains non-generic and stores an erased child. |
| Inject a runner into `CliClient` | `CliClient<R: ProcessRunner>` | Keep the high-level mock seam; real clients receive/clone a `ProcessKit`, while scripted/recording runners remain child-free. |
| Stream output | Import `processkit::prelude::StreamExt` backed by `tokio_stream` | Stream implements `futures_core::Stream`; import `futures_util::StreamExt` or an adapter-specific convenience prelude. |
| Tee output | Pass `tokio::io::AsyncWrite` | Pass `futures_io::AsyncWrite`; Tokio sinks use the Tokio adapter's compat helper. |
| Streaming stdin | `Stdin::from_reader(tokio::io::AsyncRead)` | `Stdin::from_reader(futures_io::AsyncRead)`; adapter helpers convert native readers. |
| Interactive stdin | `ProcessStdin` wraps Tokio child stdin/PTY writer | `ProcessStdin` wraps an erased core `AsyncWrite` and keeps the same flush/shutdown contract. |
| Cancellation | `tokio_util::CancellationToken` | `processkit::CancellationToken`; optional adapter conversions stay outside core. |
| Group spawn | `ProcessGroup::spawn(tokio::process::Command)` | `kit.group().spawn(CommandSpec)` or a backend-neutral prepared command. |
| Adopt a child | Borrow a Tokio child or adopt a pid | Core accepts a `ChildHandle`/stable process identity; Tokio-child adoption is an extension trait and pid adoption preserves current identity checks. |
| Raw Tokio command | `Command::to_tokio_command()` | Move to `processkit_tokio::CommandExt`; document which containment guarantees the raw escape hatch cannot carry. |
| Select two runtimes | Not supported | Construct two kits with different backend objects; no feature conflict or global runtime state. |

## Delivery plan

Estimates are engineering ranges for planning, not release dates. Re-estimate
after the feasibility gate. The current assumption is roughly 16–28 engineer
weeks if existing child wrappers satisfy the platform contracts; a custom
process reactor adds a separately approved 6–12+ weeks.

### Phase 0 — requirements and ADRs (0.5–1 week)

- Inventory the v3 public API and Tokio dependency leaks.
- Ratify crate names, owner type name, supported hosts, MSRV, and compatibility
  policy.
- Define the v3 observable-behavior baseline and initial performance budgets.

**Exit criteria:** ADRs distinguish decisions, assumptions, and open questions;
every v3 Tokio public path has an owner in the proposed v4 layout; no broad
implementation starts while a boundary decision remains open.

### Phase 1 — vertical feasibility spikes (1.5–3 weeks)

Build disposable end-to-end slices for one capture command and one streaming
command under Tokio and smol/async-io. Include timeout, cancellation, explicit
kill, handle drop, and survivor-held pipe EOF.

The spikes must exercise:

- Windows `CREATE_SUSPENDED -> assign Job Object -> resume`, including each
  rollback edge;
- Linux cgroup v2 and process-group fallback;
- FreeBSD reaper and POSIX process-group compile/contract fixtures;
- type-erased child/I/O/task/clock objects without adding a runtime generic to
  `RunningProcess` or `ProcessGroup`;
- a dependency-tree proof that the async-io slice contains no Tokio crate;
- a public-API proof that core exposes no `tokio::*` type.

**Exit criteria:** both live slices pass the same contract assertions; raw child
access is sufficient on Windows and Unix; no uncontained-child publication or
lost-reaper path is found; the architecture is either validated or revised by a
recorded ADR before production migration.

### Phase 2 — extract runtime-neutral values and API (1.5–2.5 weeks)

- Separate immutable command specification from execution.
- Move public stream/I/O bounds to `futures_core`/`futures_io`.
- Introduce the core cancellation token and runtime-neutral monotonic instant.
- Preserve result, event, error, buffer, encoding, and report shapes where useful.
- Keep the existing high-level mock/recording seam independent of raw backends.

**Exit criteria:** core value types compile without Tokio; public-API inspection
finds no Tokio path; command specifications can be consumed by a fake backend;
existing scripted and recording tests cover the new boundary.

### Phase 3 — capability contracts and platform transaction (1.5–3 weeks)

- Stabilize `ProcessBackend`, `ChildHandle`, `TaskSpawner`, `TaskHandle`, `Clock`,
  and `NetProbe` contracts.
- Move launch ordering and rollback behind the common containment transaction.
- Add manual-clock, fake-task, fake-child, and fault-injection conformance tools.
- Specify auto-traits and cancellation/reap precedence in tests.

**Exit criteria:** contracts are object-safe at MSRV; fake capabilities can drive
capture, stream, timeout, cancel, and drop tests; platform launch rollback tests
do not depend on a specific runtime.

### Phase 4 — Tokio backend baseline (2–3 weeks)

- Adapt current Tokio process, task, clock, I/O, network, PTY, and merged-pipe
  machinery to the capability contracts.
- Move `to_tokio_command`, native Tokio I/O compatibility, and token helpers to
  the adapter layer.
- Run the v3 behavior corpus against the adapter.

**Exit criteria:** supported v3 scenarios pass on the Tokio adapter with agreed
error/result/drop semantics; platform ignored tests pass where runners exist;
core still has a Tokio-free build and public surface.

### Phase 5 — async-io backend (2.5–4 weeks)

- Implement the proven async-process/async-io path.
- Provide smol, async-std, and custom `Send` spawner construction examples.
- Close child wait/reap, cancellation, EOF, raw-handle, and readiness gaps found
  by the spike.

**Exit criteria:** the common contract suite passes under smol and async-std;
the packaged dependency graph is Tokio-free; real containment and kill-on-drop
tests pass on supported hosts; unsupported capabilities fail before spawn.

### Phase 6 — move common orchestrators (3–5 weeks)

Migrate pumps, stdin feeders, deadlines/inactivity, retries, readiness,
pipelines, supervision, PTY, merged pipes, and statistics one subsystem at a
time. Each subsystem must pass on both backends before the next becomes the
default core implementation.

**Exit criteria:** every public execution path uses core orchestration; no
adapter contains a copied policy state machine; partial captures and teardown
errors match the conformance contract on both backends.

### Phase 7 — conformance, CI, and performance (2–3 weeks)

- Turn spike assertions into a backend contract suite.
- Add backend/OS/feature/MSRV matrices and dependency/public-API proofs.
- Benchmark spawn, capture, line pumping, cancellation, and teardown against v3.
- Exercise both backends in one binary to detect global-state assumptions.

**Exit criteria:** the matrix below is green, performance stays inside the
ratified budgets or has an accepted exception, and no backend-specific test is
being used to excuse a core contract failure.

### Phase 8 — migration and pre-release (1.5–3 weeks)

- Publish the complete v3-to-v4 guide and examples for Tokio, smol, and async-std.
- Publish adapter-author documentation and state which contracts are stable.
- Run downstream wrapper migrations before the release candidate.
- Freeze the v4 public API only after alpha and beta feedback.

**Exit criteria:** representative downstream clients migrate without private
hooks; every breaking change has a worked example; release artifacts prove the
same dependency and public-API claims as CI.

## Conformance and CI matrix

The matrix is a release gate, not a best-effort dashboard.

| Axis | Required v4 coverage |
|---|---|
| Backend | Tokio; async-io with smol host; async-io with async-std host; both official backends in one binary. |
| Windows | Job Object launch, suspended rollback edges, nested-job behavior, ordinary pipes, merged pipes, ConPTY, kill-on-drop, graceful-to-hard escalation. |
| Linux | cgroup v2 and forced process-group fallback; ordinary pipes, PTY, merged pipes, adoption, limits/stats where enabled. |
| FreeBSD | Process reaper plus its process-group bookkeeping; adoption, signals, graceful and hard teardown. |
| macOS/other supported Unix | POSIX process-group behavior and its documented scope; PTY and pipe coverage where supported. |
| Contract suite | Capture text/bytes, streaming/events, stdin, tee, backpressure, EOF, timeout, inactivity, cancellation, retry, probes, pipelines, supervision, drop, partial diagnostics, reap confirmation. |
| Features | Default, no-default, each additive feature, supported feature powerset, and both adapter dependencies together. |
| Toolchain | Manifest MSRV plus stable fmt/clippy/test/doc; adapter crates use the same MSRV unless an ADR records otherwise. |
| Dependency proof | `cargo tree` (including all target-specific normal dependencies) demonstrates no Tokio packages in the core + async-io consumer fixture. |
| Public API proof | `cargo-public-api`/rustdoc inspection finds no `tokio::*` path in `processkit`; Tokio paths appear only in the Tokio adapter. |
| Documentation | Markdown renders with the CI-pinned mdBook when included in the book; local relative links and the CI link checker pass. |

The contract suite should be parameterized by backend construction rather than
copied into adapter-specific test files. Platform fixtures remain platform
specific, but their assertions use the same lifecycle vocabulary.

## Risk register

| Risk | Failure mode | Mitigation / proof |
|---|---|---|
| Platform spawn race | A child runs or leaks before containment. | Core-owned launch transaction; injected failures at every post-spawn step; Windows suspended/assign/resume proof on each backend. |
| Abort/join/drop mismatch | A pump or watchdog retains a child or pipe after the public handle disappears. | Specify abort as a request and join as proof; survivor-held descriptor and dropped-future tests. |
| Wait/reap cancellation | Dropping a wait future loses the only reaper. | Backend child contract retains monotonic reap ownership; cancel-at-every-await fault tests. |
| Clock mismatch | Timeout order differs by runtime or paused time. | One monotonic domain, manual clock contract suite, precedence tests at equal deadlines. |
| Backpressure and EOF | One backend deadlocks, drops a tail, or waits forever on a descendant-held descriptor. | Bounded-buffer invariants, slow-sink tests, no-trailing-newline tests, survivor-held pipe fixtures. |
| Pipelines/supervision | One stage's cancellation or failure leaks siblings or loses partial diagnostics. | Shared orchestrators, chain-wide pre-spawn gates, terminal-state and partial-capture matrix on both backends. |
| `Send`/`Sync` drift | Type erasure silently weakens public auto-traits. | Compile-time assertions and public-API baselines for every live type; defer `!Send` rather than making bounds conditional. |
| PTY/merged-pipe differences | Runtime-specific handles require copied platform code or blocking workers that cannot be cancelled. | Dedicated spikes and cancellation/EOF fixtures before declaring feature parity. |
| Raw interop | An escape hatch bypasses validation or containment. | Keep raw types in adapters, name lost guarantees, and prefer conversions into a validated command specification. |
| Double reactors | A backend starts unnecessary global reactors or adds latency. | Tokio-free process fixture, both-backends fixture, task/thread counts, spawn/pump benchmarks. |
| Trait-object overhead | Per-line virtual calls regress high-volume capture. | Erase at coarse lifecycle/I/O boundaries, not per decoded line; benchmark before considering enum specialization. |
| Dependency drift | A transitive dependency reintroduces Tokio into the non-Tokio path. | Lockfile review plus automated inverse dependency checks on packaged fixtures. |

## v4.0 readiness criteria

v4.0 is ready only when all of the following are true:

- A packaged `processkit` + async-io example has no Tokio package in its target
  dependency graph on every supported target.
- The core public API contains no Tokio type, trait, module path, or re-export.
- Tokio and async-io backends pass one shared behavioral suite, including real
  process and kill-on-drop tests on supported hosts.
- Every backend preserves the current platform mechanism's containment scope and
  the Windows suspended/assign/resume invariant.
- Capture, streaming, stdin, cancellation, timeout, pipeline, supervisor, PTY,
  merged-pipe, readiness, stats, limits, mock, and record/replay support either
  reach documented parity or have an explicit, pre-spawn `Unsupported` entry in
  the migration guide. Silent degradation is not acceptable.
- `ProcessKit` and its live public types are non-generic over the runtime and
  retain their ratified `Send`/`Sync` properties.
- Both official backends can coexist and run in one binary without global
  backend selection or mutually exclusive features.
- Performance meets the budgets ratified in Phase 0, including spawn overhead,
  output throughput, allocation count, and teardown latency.
- The migration guide, adapter guide, API baseline, MSRV checks, Markdown build,
  and local/external link checks are green for the exact release commit.

## Release sequence and v3 support

1. **v4 alpha:** after the Tokio baseline and async-io vertical path both pass
   containment, dependency, and public-API gates. Feature parity may still be
   incomplete and is listed explicitly.
2. **v4 beta:** after common orchestrators and the CI matrix pass on both
   backends. No known safety or teardown semantic gap remains.
3. **v4 release candidate:** after downstream migrations, performance gates,
   platform ignored tests, and documentation are complete. Only release-blocking
   fixes may change public API.
4. **v4.0:** after an RC soak with no unresolved containment, reap, cancellation,
   or dependency-proof failures.

The v3 line continues to receive critical correctness, security, and containment
fixes during alpha/beta and through a defined stabilization window after v4.0.
Feature development moves to v4 once beta begins. The exact window is ratified
before the first alpha so users are not asked to infer support from commit
activity.

## Decisions required before broad implementation

- Ratify public and crate names and the backend-author API's SemVer boundary.
- Confirm whether v4.0 promises all current optional features on both official
  backends or allows explicit, temporary adapter-specific gaps.
- Confirm the minimum supported async-process/async-io versions and the executor
  injection API after the vertical spike.
- Define the stable process identity/raw-handle contract needed by adoption and
  platform containment.
- Define public `Send`/`Sync` expectations for `ProcessKit`, `RunningProcess`,
  `ProcessStdin`, streams, task handles, and callbacks.
- Choose the core cancellation notification primitive from conformance and loom
  evidence.
- Set measurable performance budgets from v3 baselines.
- Decide whether `ProcessGroup` is created by a `ProcessKit`, is transferable
  between compatible kits, or permanently retains its creator backend.
- Decide which Tokio escape hatches preserve containment and which must be named
  as raw/uncontained operations.
- Decide the post-v4 direction for local/completion-based runtimes separately;
  it is not a hidden v4.0 commitment.

Until these decisions and Phase 1 exit criteria are satisfied, implementation
should remain in disposable spikes. That gate is what keeps an attractive
runtime abstraction from weakening ProcessKit's defining process-lifecycle
guarantees.
