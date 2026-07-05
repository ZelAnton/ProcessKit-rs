# next: Python-binding (PyO3 wrapper) feedback

> **Status:** triaged + largely actioned (2026-06-28). From the `processkit-py`
> PyO3 wrapper build, which binds `processkit = "=1.0.1"`. The dev tree is the same
> `1.0.1` the wrapper pins, so every citation below is current. These are concrete
> API frictions the binding had to work around. None are bugs; the crate is
> excellent. They are ergonomics for the "someone builds a typed FFI over this"
> consumer, toward a thinner, more reliable, more extensible, more testable binding.

## Resolution (2026-06-28) — maintainer triage

> **Shipped 2026-06-28 as `v1.1.0`** (crates.io). Per-item response sent to the
> `processkit-py` agent via the `.hq` thread
> `comms/threads/T-20260629-processkit-py-binding-feedback-response/` (msg `00`).
> A/B/D/E/F/I/J landed in 1.1.0; C/G/H deferred to 2.0; K rejected.

Maintainer call: ship the additive items + the two method-aliasable renames now as
**1.1.0** (curated in `CHANGELOG.md` `[Unreleased]`); batch the field-level breaking
renames into **2.0**. Two of the feedback's SemVer labels were corrected: **A** is a
bound-tightening (not "additive" — but real-world breakage ≈ 0, so shipped in the
minor), and **C** is breaking (restructuring an existing variant's fields is breaking
*despite* `#[non_exhaustive]`, which only governs adding variants).

**Landed in 1.1.0 (this tree):**
- **A** — `wait_for_line` / `wait_for` callbacks are now `+ Send` (probe futures are
  `Send`; a compile-time assertion in `probes.rs` locks it in). `wait_for_port` was
  already `Send`. The binding can now bind the crate's probes instead of
  re-implementing them.
- **B** — `ProcessGroup::shutdown_ref(&self)` added; `shutdown(self)` delegates. The
  wrapper can drop its `Arc::try_unwrap` + hard-kill fallback.
- **D** — `RunProfile::outcome` (+ `signal()` / `timed_out()`). `profile()` is now a
  superset of `wait()`.
- **E** — cassette `start` now records (capture-whole) + replays through a scripted
  handle. Documented caveat: interactive (stdin-mid-stream) streaming can't be
  cassette-recorded — script those with `ScriptedRunner`.
- **F** — `kill_all` added; `terminate_all` `#[deprecated]` (removed in 2.0).
- **I** — `avg_cpu_cores` added; `avg_cpu` `#[deprecated]` (removed in 2.0).
- **J** — `CliClient: Clone` (when the runner is `Clone`).

**Deferred to 2.0 (breaking, no clean alias path):**
- **C** — structured `ResourceLimit { kind, reason, detail }`. Cost is *moderate*, not
  "small": an honest `reason` (Invalid vs Unenforceable vs Unsupported) needs backend
  signal on the `Job::new` path, not just the `validate_limits` path.
- **G** — `OutputTooLarge` `line_limit`/`byte_limit` → `max_lines`/`max_bytes`.
- **H** — `ResourceLimits.memory_max` → `max_memory` (field + builder together, to
  avoid a transient field/builder mismatch).
- Plus removing the **F**/**I** deprecated aliases.

**Rejected:**
- **K** — typing `ProcessStdin`'s `io::Result`. Raw `io::Result` is the right type for
  a low-level interactive byte sink; wrapping it in the run-level `Error::Stdin` (which
  carries `program`) is a category error, forces new state onto `ProcessStdin`, and is
  breaking — for ~zero benefit (the wrapper's `OSError` mapping is already clean).

When 1.1.0 publishes, re-sync the `processkit-py` wrapper to drop the A/B re-implementations
and the F/I renames, and to bind the cassette streaming path (E).

## Why a binding is useful feedback

A binding is a stress test of an API's *shape*: every place the wrapper has to
**rename, re-wrap, or re-implement** marks a spot where the surface fought the
consumer. The items are ordered by how much wrapper code (and risk) they remove.

## High-impact — remove whole workarounds

### A. Readiness probes a non-Rust async runtime can actually drive
*Affects: the wrapper RE-IMPLEMENTS all three probes in pure Python · Additive · Cost: moderate*

`RunningProcess::wait_for_line` / `wait_for` / `wait_for_port`
(`src/running/probes.rs:51,90,106`) all take **`&mut self`**, and their callbacks
carry **no `Send` bound**. So the returned futures borrow the handle and are **not
`Send + 'static`** — they cannot cross a `pyo3-async-runtimes` (tokio <-> asyncio)
bridge, which needs `'static` (and in practice `Send`) futures. The wrapper
therefore **re-implements all readiness logic in Python** (`processkit-py
src/processkit/_aio.py`: `wait_for` / `wait_for_line` / `wait_for_port`, built over
the public `stdout_lines()` stream plus a raw TCP connect) — duplicating semantics
the crate already owns and risking drift from the crate's `NotReady` behaviour.

What would let the wrapper bind the crate's probes instead of re-implementing:
- a probe whose future is `Send + 'static` — e.g. driven on an owned handle / `Arc`,
  or kept `&mut self` but with `+ Send` futures and `Fn(..) -> .. + Send` callbacks
  (cf. `Command::first_line`, whose predicate already IS `Fn(&str) -> bool + Send`,
  `src/command.rs:1125`); **or**
- expose the readiness loop as a free function over the public streaming API, so a
  binding can call it on the handle it owns.

This is the single largest "delete wrapper code + kill a drift risk" item.

### B. A graceful group teardown that does not consume `self`
*Affects: the wrapper's `Arc::try_unwrap` + hard-kill fallback dance · Additive · Cost: small*

`ProcessGroup::shutdown(self)` (`src/group.rs:393`) consumes `self` by value. A
Python `ProcessGroup` is a long-lived object the wrapper holds in an `Arc` shared
with any in-flight `astart` future, so it cannot move the group out to call
`shutdown`. Its workaround (`processkit-py src/group.rs::shutdown_group`):
`Arc::try_unwrap(group)` → `shutdown().await` when sole owner, **else fall back to
the hard `terminate_all()`**. Net effect: a group torn down while an `astart`
future still races silently **downgrades from graceful to hard kill** — a
correctness wart forced purely by the by-value signature.

The crate already has `pub(crate) graceful_terminate(&self, grace, signal)`
(`src/group.rs:418`). **Making it public (or adding `shutdown_ref(&self, ...)`)**
would let the wrapper always tear down gracefully on `__exit__` with no
`try_unwrap` and no hard-kill fallback.

### C. A structured `ResourceLimit` error (which limit, and why)
*Affects: the wrapper had to DROP `.message`; can expose nothing branchable · Additive (`Error` is `#[non_exhaustive]`) · Cost: small*

`Error::ResourceLimit { message: String }` (`src/error.rs:229`) carries only a
free-form string — unlike every sibling (`Exit{code}`, `Signalled{signal}`,
`Timeout{timeout}`, `OutputTooLarge{...ints}`, `Unsupported{operation}`). The
wrapper gives every other exception structured attributes, but for `ResourceLimit`
it had to **remove `.message`** (it merely duplicated `str(exc)`) and now exposes
nothing. A caller cannot tell "invalid `cpu_quota` value" from "platform can't
enforce" from "kernel rejected it" without parsing English.

A structured shape — e.g.
`ResourceLimit { kind: LimitKind /* Memory|Processes|Cpu */, reason: LimitReason /* Invalid|Unenforceable|Unsupported */, detail: String }`
— would let the binding surface `exc.kind` / `exc.reason` like the rest of the
hierarchy. Additive because the enum is `#[non_exhaustive]`.

### D. A `RunProfile` that can answer "how did it end"
*Affects: wrapper `profile()` is telemetry-only · Additive (`#[non_exhaustive]`) · Cost: small*

`RunProfile` (`src/stats.rs:134`) has `exit_code/duration/cpu_time/peak_memory_bytes/samples`
but **no `signal` and no `timed_out`**; `exit_code` is `None` for *both* a timeout
and a signal kill, with no way to tell them apart. So the wrapper's `profile()`
returns resource telemetry but not the run's actual outcome — and since it consumes
the handle, the caller cannot also `wait()`. Adding `signal: Option<i32>` +
`timed_out: bool` (or embedding the `Outcome`) makes `profile()` a superset of
`wait()`, and the binding can expose one rich result.

### E. A record/replay double that also covers streaming (`start`)
*Affects: wrapper record/replay can't replay streaming runs · Additive · Cost: moderate*

`RecordReplayRunner` (cassette) implements only `output_string`; `start` falls
through to `Error::Unsupported` (`src/cassette.rs:301`). `ScriptedRunner` already
proves a double can hand back a real streaming `RunningProcess`
(`src/doubles.rs:564`). Teaching the cassette double to replay recorded output
through a scripted handle would let the wrapper's `RecordReplayRunner` cover the
streaming path too — closing a testability gap for binding users who stream.

## Consistency — each lets the wrapper delete a rename; two are crate-internal

### F. Name the hard kill `kill_all`, not `terminate_all`
*Breaking (2.0 / deprecated alias) · Cost: trivial*

`ProcessGroup::terminate_all` (`src/group.rs:256`) is documented as "**Immediately
hard-kill** every process" and its impl calls `self.job.kill_all()` (`:263`).
"terminate" reads as POSIX `SIGTERM` (graceful), so the name fights the behaviour.
The wrapper renames it to **`kill_all`** for honesty; renaming it in the crate
would (a) match the behaviour, (b) match the internal `Job::kill_all` it already
delegates to, and (c) let the wrapper drop the rename.

### G. Align the `OutputTooLarge` error fields with the buffer policy
*Breaking (or add aliases) · Cost: trivial · crate-internal inconsistency*

`OutputBufferPolicy` configures caps as `max_lines` / `max_bytes`
(`src/buffer.rs:120,127`) but `Error::OutputTooLarge` reports them as `line_limit`
/ `byte_limit` (`src/error.rs:161`) — the same concept spelled two ways depending
on whether you set it or catch it. The wrapper renames the error fields back to
`max_lines`/`max_bytes` so the kwarg you pass and the field you read match.
Renaming the variant fields to `max_lines`/`max_bytes` fixes the inconsistency and
removes the wrapper rename.

### H. One word order for the resource-limit knobs
*Breaking (or add aliases) · Cost: trivial · crate-internal inconsistency*

`ResourceLimits` mixes orders: **`memory_max`** but **`max_processes`**
(`src/limits.rs:48,64`; same on the builders `src/group.rs:79,86`). The wrapper
normalises to `max_memory` / `max_processes`. Picking one order — `max_memory` to
match the existing `max_processes` — removes the inconsistency and the rename.

### I. `avg_cpu` -> `avg_cpu_cores`
*Breaking (or alias) · Cost: trivial*

`RunProfile::avg_cpu()` (`src/stats.rs:153`) returns CPU **cores** (0.5 = half a
core). The wrapper exposes it as `avg_cpu_cores` so the unit is self-documenting.

### J. `#[derive(Clone)]` on `CliClient`
*Additive · Cost: trivial*

`Command` and `Pipeline` are `Clone` (`src/command.rs:28`, `src/pipeline.rs:71`),
which lets the wrapper clone an owned value to obtain a `'static` future for the
async bridge. `CliClient` is **not** `Clone` (`src/client.rs:85`), so the wrapper
must `Arc`-wrap it instead. Deriving `Clone` (it is program + defaults + a runner)
makes the binding uniform.

## Low priority

### K. Type the interactive `ProcessStdin` I/O errors
*Additive · Cost: small*

`ProcessStdin::write`/`write_line`/`flush`/`finish` return raw `std::io::Result`
(`src/stdin.rs:282`), while the background stdin-feed path is already a typed
`Error::Stdin` (`src/error.rs:319`). The wrapper maps the raw `io::Error` to Python
`OSError` (a clean mapping, so this is genuinely low priority); typing it would
only make the surface uniform.

## Please KEEP — already binding-ideal, do not "fix" these

- **Consuming verbs are `async fn(self) -> Result<Owned>`** (`wait` / `finish` /
  `output_string` / `output_bytes` / `profile` / `shutdown`, `src/running/mod.rs`):
  the future owns `self`, so it is trivially `'static` and bridges cleanly. This is
  exactly what makes most of `RunningProcess` easy to bind — please keep it.
  (Contrast the `&mut self` probes in item A.)
- **`Command` / `Pipeline` are `Clone`** — the binding clones to own a `'static`
  future. Just extend it to `CliClient` (item J).
- **The `ProcessRunner` seam + full-fidelity `ScriptedRunner`** (`src/runner.rs`,
  `src/doubles.rs`): the wrapper re-exports the trait as a `typing.Protocol` and the
  doubles as a `processkit.testing` submodule. Keep the trait and the doubles in
  lockstep with the real runner's verb set (item E is the one gap).
- **`#[non_exhaustive]` on `Error` / `Outcome` / `StopReason` / `RunProfile` /
  `OutputBufferPolicy`**: the wrapper carries forward-compat fallbacks (`_ =>
  "unknown"` for `StopReason`, `_ => {}` for error fields), so new variants/fields
  never break it. Keep this — just keep the existing variant *names* stable.
- **The uniform run-vocabulary across `Command` / `Pipeline` / `CliClient` /
  `Runner`** (`output_string` / `run` / `exit_code` / `probe` / `start`): it maps
  1:1 onto a uniform Python surface. Keep it identical across layers.

## Assessment

A and B remove whole workarounds (a re-implemented Python module; a
correctness-downgrading fallback) and are the highest value. C/D/E enrich what the
binding can expose (errors, profiles, replayed streams) and are all additive under
`#[non_exhaustive]`. F–I are naming: two (G, H) are genuine crate-internal
inconsistencies worth fixing regardless of any binding; the rest let the wrapper
shed a thin translation layer. The by-value-async consuming verbs and the
`ProcessRunner` / `ScriptedRunner` seam are exactly right for a binding — the asks
are about the edges, not the core.

## Revisit condition

Pick up the additive items (A–E, J, K) opportunistically when next touching the
relevant module; batch the breaking renames (F–I) into the next major (or land them
now as deprecated aliases). When any land, re-sync with the `processkit-py` wrapper
so it can drop the corresponding workaround and thin its translation layer.
