# v2 — deferred breaking changes (implement in 2.0)

> **Status:** aggregator (local-only). Breaking changes that are worth doing but
> can't land in a 1.x minor (the public API is stable within 1.x; breaking lands
> only in a new major). Curated as additive work proceeds — when an additive
> change would be *cleaner* as a breaking one, the breaking variant is recorded
> here and the additive (alias/accessor) form ships now.
>
> When 2.0 is cut, batch all of these into one breaking release with a single
> migration note, and announce via the `.hq` rollout protocol.

## From the python-binding feedback (`next-python-binding-feedback.md`)

- **C — structured `ResourceLimit` — IMPLEMENTED.** Replaced
  `Error::ResourceLimit { message: String }` with `ResourceLimit { kind: LimitKind
  /* Memory|Processes|Cpu */, reason: LimitReason /* Invalid|Unenforceable|Unsupported */,
  detail: String }`, plus `Error::limit_kind()`/`limit_reason()` accessors. `reason` is
  a real backend signal (`io::ErrorKind::Unsupported` from the platform backend maps to
  `LimitReason::Unsupported`; every other `Job::new` failure to `Unenforceable`;
  `validate_limits` rejections to `Invalid`), not a parse of `validate_limits` text alone.
  (`src/error.rs`, `src/group.rs`, `src/limits.rs`.)
- **G — `OutputTooLarge` field names — IMPLEMENTED.** Renamed `line_limit`/`byte_limit`
  → `max_lines`/`max_bytes` to match `OutputBufferPolicy` — the variant, its manual
  `Debug`, and every constructor (`src/running/mod.rs`, `src/running/stream.rs`,
  `ProcessResult::reject_if_truncated`) use the new names. (`src/error.rs`.)
- **H — one word order for the resource-limit knobs — IMPLEMENTED.** Renamed
  `ResourceLimits::memory_max` → `max_memory` (to match `max_processes`), field **and**
  builder (`ProcessGroupOptions::max_memory`) together, including the Linux/Windows
  backends and `validate_limits`. (`src/limits.rs`, `src/group.rs`, `src/sys/linux.rs`,
  `src/sys/windows.rs`.)

## Deprecated-alias removals (added in 1.1.0, removed in 2.0) — IMPLEMENTED

- `ProcessGroup::terminate_all` → removed (use `kill_all`). (`src/group.rs`.)
- `RunProfile::avg_cpu` → removed (use `avg_cpu_cores`). (`src/stats.rs`.)
- `RunProfile::exit_code` *field* removed (was redundant with `outcome.code()` /
  the `code()` method — kept the method, dropped the duplicate field).
  (`src/stats.rs`, `src/running/mod.rs`.)

<!-- Append items discovered while doing the 1.x additive ergonomics work below. -->

## From the vcs-toolkit additive sweep (`next-vcs-toolkit-feedback.md`)

- **`#[non_exhaustive]` on the data-bearing `Error` variants — IMPLEMENTED.** The
  `Error` *enum* was already `#[non_exhaustive]`, but its variants were not — so
  adding a field to `Exit` / `Timeout` / `Signalled` / `Spawn` / `NotFound` /
  `Parse` / `OutputTooLarge` / `Stdin` / `ResourceLimit` was breaking. All nine are
  now individually `#[non_exhaustive]` (`src/error.rs`), so a future field addition
  to any of them is a non-event. The existing `Error` accessor family
  (`program`/`code`/`signal`/`stdout`/`stderr`/`combined`/`is_*`) and the
  `#[doc(hidden)]` `Error::{exit,timeout,signalled}` constructors already let
  consumers read/build these variants without a struct match, so no further
  migration was needed on top of the attribute.

## From the deep-audit-2026-07 sweep (`next-deep-audit-2026-07.md`)

- **Make `output_bytes` honor the `OutputBufferPolicy` byte cap (`max_bytes`) on
  its raw stdout capture.** Today raw stdout is *documented* as exempt (only the
  line-pumped stderr is capped; "bound a flooding child with a `timeout`"), so a
  caller who set `with_max_bytes(..)` and calls `output_bytes` still gets the
  complete bytes. Honoring the cap (Error mode → `OutputTooLarge`; drop modes →
  bounded head/tail + `truncated`) is more consistent with the line verbs and
  removes an OOM footgun, but it is a **breaking behavior change**: it truncates /
  errors where callers previously received full bytes, exactly the class the
  crate flagged as "Breaking (behavior)" for the 0.10.0 `run`/`parse` truncation
  change. Deferred out of 1.x for that reason. When done, keep `max_lines`
  inapplicable (raw bytes have no lines) and default (no byte cap) unchanged.
  Implemented-then-reverted once in the audit sweep — the reverted diff
  (`push_capped_bytes`/`clamp_dropoldest_tail` + the `output_bytes` finalize) is a
  ready starting point. (`src/running/mod.rs`, `src/buffer.rs`.)

## Deferred non-breaking enhancements (deep-audit-2026-07, shared-group teardown)

*Not breaking — internal robustness. Recorded so they aren't re-discovered.*

- **Robust shared-group `first_line`/streaming timeout teardown on the Linux
  cgroup mechanism.** A `first_line` (or streaming `finish`-less) timeout on a
  **shared** group reaches the direct child by pid via the deadline watchdog.
  With a **grace**, the watchdog does `graceful_kill_pid` (signal → poll →
  SIGKILL); if the child catches the signal, closes stdout, but keeps running,
  the search ends on stream-close and `RunningProcess::Drop` aborts the watchdog
  mid-grace, so the final SIGKILL is skipped. pgroup backstops this via
  `kill_on_drop`; Windows hard-kills (no grace); own-group `ProcessGroup::Drop`
  hard-kills — only the cgroup shared path is exposed (the child outlives the
  probe until the group is dropped). A clean fix needs a shared-group
  graceful-kill-**and-reap** primitive whose SIGKILL isn't abortable and doesn't
  race `kill_on_drop`'s reap (the pgroup recycle hazard blocks a naive
  await-the-watchdog). Documented on `first_line`/CHANGELOG for now.
- **Surface a genuine `SIGKILL` `EPERM` (uid-changed tree) from the process-group
  teardown (C2).** `kill_all`/`hard_kill` on the pgroup mechanism swallow a
  delivery `EPERM`, so a `sudo`/setuid child that rejects `SIGKILL` can outlive
  `kill_all` reported as success. A first attempt to surface it (return
  `io::Result` from `signal_all`) was reverted: on macOS/BSD `killpg` returns
  `EPERM` for a group whose only member is an unreaped **zombie** too, so
  surfacing it falsely failed a normal shutdown of a group with unreaped children
  (broke `batch::kill_on_drop_provenance…` on macOS CI). A real fix must
  distinguish a genuinely-alive uid-changed process from a zombie — e.g. reap
  before probing, or check the process state (`proc_pidinfo`/`/proc`) after an
  `EPERM` — before it can surface without false positives. Documented as a
  limitation on `kill_all` for now. (`src/sys/pgroup.rs`, `src/group.rs`.)
- **B3 — `finish()` without a live stream should use the discard sink.** A bare
  `finish()` (no prior `stdout_lines`) pumps stdout into a sink built from the
  *user's* `OutputBufferPolicy` (default unbounded) and retains every line nobody
  reads; under `fail_loud` it can error `OutputTooLarge` for output the caller
  never asked to capture — where `wait()` succeeds. Also: after `stdout_lines()` →
  drop stream → `wait()`/`profile()`, the existing user-policy sink is reused,
  defeating the discard optimization. Fix: `finish()`-without-a-live-stream uses
  the internal discard sink (it returns no stdout); reset the sink when the stream
  is dropped. Deferred: it reshapes the streaming sink lifecycle and wasn't
  sequenced into the audit's execution order. (`src/running/stream.rs`, `mod.rs`.)
- **B5 — per-line extraction write amplification.** `pending.drain(..=nl).collect::<String>()`
  memmoves the entire remaining tail per line and builds the `String`
  char-by-char, so a short-line flood is ~2000× write-amplified. Fix: index-based
  subslice copies / two-pass split per chunk. Pure perf (not correctness),
  deferred. (`src/pump.rs`.)
- **F2 — proactive pipeline teardown on first stage failure.** A stage *failure*
  (non-token) tears the other stages down only passively (pipe EOF), and `collect`
  awaits stages in input order, so a quiet upstream stage can hold the run open
  after a downstream failure. The 1.x doc now states this honestly; the code fix
  (kill/drop the group on first stage error) is deferred — it changes the pipeline
  execution/await model and wants review coverage. Token *cancellation* is already
  proactive (each stage carries the token). (`src/pipeline.rs`.)
- **Per-stage sub-groups for pipelines (A3).** A per-stage `Command::timeout` on
  a pipeline stage reaches only that stage's direct child (shared group); a
  forking stage's grandchildren can hold stdout and stall downstream. The
  whole-chain `Pipeline::timeout` (`group.kill_all()`) is the documented backstop.
  Giving each stage its own kill-on-drop sub-group would make per-stage timeouts
  tear the stage subtree down on their own. (`src/pipeline.rs`, `src/running/`.)

## Deferred test-double fidelity (deep-audit-2026-07, doubles)

*Behavior changes to `ScriptedRunner`/cassette doubles — churny (many tests
depend on current double output), so deferred to a focused pass, not shipped
mid-audit. Each is a fake-vs-real fidelity gap: a test that passes on the fake
can fail on the real runner.*

- **D3 — `Reply::into_result` returns canned stdout verbatim** while the real
  bulk path joins decoded lines (strips the trailing `\n`, normalizes CRLF→LF):
  `Reply::ok("done\n")` yields `"done\n"` on the fake, `"done"` on a real run —
  and the same reply differs by verb on one double. Normalizing canned text
  through the line-join would fix it but changes output for every existing
  `Reply` test. (`src/doubles.rs`.)
- **D4 — start-based doubles double-decode canned text**: the scripted feeder
  writes the canned `String`'s UTF-8 bytes, and the pump re-decodes them with the
  command's `stdout_encoding`, so `.stdout_encoding(UTF_16LE)` + a scripted/cassette
  `start` yields garbage while `output_string` is correct. Feeder should encode
  with the command's encoding (or scripted pumps force UTF-8). (`src/doubles.rs`,
  `src/running/`.) **Related (from Stage 4):** the cassette `start`-replay handle
  (`scripted_running_from_parts` → `ScriptedProc`) re-pumps the canned output, so
  it re-derives `truncated`/`duration` instead of carrying the recorded `Entry`
  fields that `output_string` replay now applies (`Entry::to_result`). The
  fail-loud checking verbs (`run`/`parse`) route through `output_string` and are
  covered; only a caller manually consuming a `start` handle loses the signal.
  Thread the recorded flags through the scripted handle to close it. (`src/doubles.rs`,
  `src/cassette.rs`.)
- **D7 — `ScriptedRunner` never consumes `stdin_source()`**, so an app-level
  re-run of a one-shot-stdin command passes the fake but fails live with
  `OneShotConsumed`. The fake should call `take_for_run` too. (`src/doubles.rs`.)
- **D8 — doubles can't express spawn-side failures**: a rule miss yields
  `Spawn{NotFound}` with `is_not_found()==false`, and `Reply` can't script
  `Error::NotFound`/bad-cwd; a "tool not installed → fallback" branch is
  untestable. Add `Reply::not_found()` / spawn-error constructors. (`src/doubles.rs`.)
- **D9/D10/D11/D13 — smaller**: replay `output_string` skips the non-piped-stdout
  guard the real path enforces; env excluded from the match key can serve a
  differently-`LC_ALL`'d recording (documented, consider opt-in env keying);
  record-mode errors record nothing so replay is `CassetteMiss` (variant differs
  from record-time); version gate runs after full deserialize; `Rule::Prefix`
  exact-`OsStr` match vs Windows case/extension. Mostly docs + small guards.

## From deep-audit-2026-07 Stage 6 (API additions — additive shipped, breaking deferred)

- **G8 — flat crate-root re-exports → `prelude` module — IMPLEMENTED.** Moved
  `encoding_rs::Encoding` and `tokio_stream::StreamExt` off the crate root into
  a new `processkit::prelude` module — `use processkit::*` no longer pulls
  either in, and a future `0.x` major bump of either dependency is contained to
  `prelude`. `CancellationToken` (from `tokio-util`) stays at the root: out of
  this item's scope (a stable 0.7 dependency, not part of the G8 complaint).
  (`src/lib.rs`.)
- **G4 — timeout as a tri-state `enum` + a `timeout_opt(Option<Duration>)` verb.**
  1.x ships `no_timeout()` alongside `timeout(Duration)`, with the "explicitly
  unbounded" state modeled as a `bool` next to `Option<Duration>` (the two setters
  maintain the invariant, so the nonsensical `Some + no_timeout` pair is
  unreachable). Cleaner in v2: model it as `enum Timeout { Unset, Unbounded,
  After(Duration) }` so the invariant is type-level, and add a single
  `timeout_opt(Option<Duration>)` that folds set/unset into one composable verb
  (the current API forces `match cfg { Some(d) => c.timeout(d), None => c.no_timeout() }`
  at config-driven call sites). Breaking (field-type change). (`src/command.rs`.)
- **G4 — `Command::retry_never()` (companion to the shipped `no_timeout()`).** Not
  added in 1.x: a per-command opt-out of a client `default_retry` already exists via
  `retry(1, ZERO, |_| false)` (max_attempts 1 = one run, and any explicit `retry`
  suppresses the gap-fill). A dedicated `retry_never()` would be tidier/symmetric with
  `no_timeout()`; consider it (additive, could ship in a later 1.x minor too). No
  breaking aspect — recorded here only to track the symmetry decision.

## From deep-audit-2026-07 Stage 7 (error/result design)

- **H4 — carry the raw bytes in the failure error, so exact bytes survive the
  consuming path — IMPLEMENTED.** The checking verbs (`run`/`ensure_success`/…)
  *consumed* the `ProcessResult` to build `Error::Exit`/`Timeout`/`Signalled`,
  which store stdout as a **lossy UTF-8 `String`** — so after
  `output_bytes().await?.ensure_success()?` failed, the exact bytes existed
  nowhere. Fixed with a lightweight payload rather than the originally-sketched
  `Option<ProcessResult<Vec<u8>>>`: all three variants gain
  `stdout_bytes: Option<Vec<u8>>` (`Some` on the bytes path via
  `StdoutText::into_raw`, `None` on the text path), readable through the new
  `Error::stdout_bytes()` accessor. (`src/error.rs`, `src/result.rs`.)
- **D11 — record the error *type* in a cassette so a record-mode failure replays as
  the same `Error`, not a `CassetteMiss`.** Today a record-mode call that returns
  `Err` records **nothing** (only `Ok` results become `Entry`s), so replaying that
  invocation misses the cassette and surfaces `Error::CassetteMiss` — a different
  variant than the record-time error. v2: extend the cassette schema with an
  optional recorded-error discriminant (`Spawn`/`NotFound`/`Timeout`/… + payload) so
  replay reproduces the original error. Schema change — additive with a
  `CASSETTE_VERSION` bump, but the *behavior* (replay now errors where it used to
  miss) is worth a major. (`src/cassette.rs`.)

## From deep-audit-2026-07 (streaming line terminators)

- **B4 — a `\r`-aware line-terminator knob.** The line pumps split on `\n` (via
  `str::lines`-style splitting), so a tool that emits **carriage-return progress**
  (`\rProgress: 50%\rProgress: 100%`) delivers one giant "line", and the bounded
  diagnostic tail shows the *oldest* frame. A `Command` knob to also treat `\r` as a
  line terminator (or a "progress mode" that keeps only the last `\r`-frame) would
  render such output sanely. Deferred: it interacts with the decode/pump path and the
  tee/handler contracts, and the right default is unclear — likely additive (a new
  builder) but recorded here so the design is considered as a set. (`src/pump.rs`,
  `src/command.rs`.)

## Considered and rejected (deep-audit-2026-07, teardown)

- **C6 — Windows graceful-shutdown "drain window" — REJECTED (kept doc-only).** The
  audit proposed polling for a natural exit up to the grace `timeout` before the
  atomic `TerminateJobObject`, to give a self-exiting tree time to flush. Implemented,
  but CI's `graceful_timeout_degrades_to_a_prompt_kill_on_windows` failed: Windows has
  no soft signal to *trigger* graceful exit, so for the common case (a child that
  ignores the absent signal) the poll only delays the inevitable kill by the whole
  grace — a data-losing 30 s stall, not a drain. The existing test encodes the
  deliberate "prompt hard kill, don't wait a phantom grace" decision. Reverted to
  doc-only: `graceful_shutdown`'s `timeout` is Unix-only; Windows is a prompt kill at
  the deadline. Reopen only with a design that *triggers* Windows shutdown (a console
  CTRL event / named-pipe stop) so the wait is meaningful. (`src/sys/windows.rs`.)

## Low-priority / opportunistic

- **Widen `ProcessResult::output_contains_any(&[&str])` — IMPLEMENTED.** Now
  `impl IntoIterator<Item = impl AsRef<str>>`, matching the crate's other multi-value
  inputs (`args`/`envs`/`ok_codes`) — a bare array, `Vec<String>`, or slice all work
  directly. Source-compatible for literal call sites (`&["a"]` still coerces); the one
  wrinkle is an empty-literal call site, which now needs a type annotation (e.g.
  `[] as [&str; 0]`) since the generic form can't infer the element type from `&[]`
  alone. (`src/result.rs`.)
