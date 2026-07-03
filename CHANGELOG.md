# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Add entries to `[Unreleased]` as you work — manual bullets always win over the
git-cliff auto-fill (config: `cliff.toml`). On release, promote `[Unreleased]`
to a dated version section.

## [Unreleased]

### Added
- `impl<R: ProcessRunner + ?Sized> ProcessRunner for Box<R>` and `Arc<R>` — a
  runner chosen at **runtime** (the real `JobRunner` vs a `record`-feature
  cassette, picked from config) can now be stored type-erased as
  `Box<dyn ProcessRunner>` in `CliClient`/`Supervisor` state and shared across
  tasks as `Arc<dyn ProcessRunner>`, not just borrowed as `&R`. Generic over
  `?Sized`, so a boxed/shared *concrete* runner (`Arc<JobRunner>`) qualifies too.
  Both forward every method (`output_string`/`output_bytes`/`start`).
- `Command::no_timeout()` — mark a command **explicitly unbounded** so a client
  `default_timeout` gap-fill leaves it alone (a `tail -f`, a watch loop). Distinct
  from simply leaving the timeout unset (which the client *does* fill); the last
  of `timeout`/`no_timeout` wins.
- `Error` accessors so consumers stop matching the `#[non_exhaustive]` `Error`
  enum's variants by hand: `program()`, `stdout()`, `stderr()` -> `Option<&str>`;
  `combined()` -> `Option<String>` (streams for the `Exit`/`Timeout`/`Signalled`
  variants, `None` elsewhere); `code()` / `signal()` -> `Option<i32>`; and the
  `is_timeout()` / `is_cancelled()` / `is_signalled()` predicates. `code` /
  `signal` / `program` reuse the crate-wide `ProcessResult` / `Outcome` /
  `RunProfile` vocabulary, so a wrapper reads every failure off `Error` through
  accessors — making a future *variant* addition a non-event for the whole
  re-exporting dependent tree (and, once the data-bearing variants become
  `#[non_exhaustive]` in 2.0, field additions too).
- `ProcessResult::output_contains_any(&[&str]) -> bool` — case-insensitive (ASCII)
  search across both captured streams, for the lenient "a specific non-zero exit
  is benign when a known stderr/stdout marker is present" idiom.
- `#[doc(hidden)]` constructors `Error::{exit, timeout, signalled}` for custom
  `ProcessRunner` doubles and error-classifier tests, so they stop spelling out
  struct literals that a future field addition (a 2.0 change) would break.
- Client-wide retry: `CliClient::default_retry(policy, retry_if)` retries **every**
  verb on a shared `RetryPolicy` (exponential backoff + per-delay cap + AWS-style
  full jitter) — the client-wide analogue of the per-call `Command::retry`. A
  per-command `Command::retry` still wins (gap-fill, not override). The new public
  `RetryPolicy` (`#[non_exhaustive]`, builder + `Default`: 3 retries / 100 ms / ×2
  growth / 30 s cap / jitter on) is also usable per-command via
  `Command::retry_with(policy, retry_if)`, and is the schedule behind the released
  `Command::retry`, whose fixed-backoff form is unchanged. Jitter uses a small
  per-thread PRNG seeded from system entropy — no new dependency.
- `testing::Invocation` env assertions — the env analogue of `has_flag`:
  `env(name) -> Option<Option<&OsStr>>` (full fidelity: untouched / set / removed,
  last override wins), `env_is(name, value) -> bool`, and `has_env(name) -> bool`
  — so tests stop hand-rolling `envs.iter().any(..)` closures to assert injected
  variables.
- `CliClient::default_env_fn(key, resolver)` — a client-wide env default whose
  value is **recomputed once each time a command is built** (per `command()` /
  verb call), for a value that must refresh per operation rather than freeze at
  client construction (a fresh request id, the current trace span, a
  periodically-rotated token) where the static `default_env` would pin a stale
  value. The resolver is
  gap-filled: it runs only when the command doesn't already set that key at the
  moment the client's defaults are applied (so a per-command `Command::env` and a
  static `default_env` for the same key win, and the resolver is then skipped, not
  merely overwritten). The value is baked into the built command — it is not
  re-resolved per process spawn, so retries and re-runs of one command reuse it.
  `Arc`-shared so `CliClient` stays `Clone`.

### Changed
- `CliClient::default_env` / `default_env_remove` / `default_env_fn` now resolve a
  **duplicate key last-registration-wins**, matching `Command::env`'s later-wins
  (previously first-registered won, opposite of the builder intuition). A later
  registration for a key supersedes — and drops — the earlier one (a superseded
  `default_env_fn` resolver never runs).

### Fixed
- A client-wide `default_env`/`default_env_fn` no longer **pierces a command's env
  isolation**: `env_clear()` now opts the command out of the gap-fill for *every*
  key (a clean slate the client must not pierce), and `inherit_env([...])` blocks a
  client default only for its **allow-listed** keys (a client default must not
  override a value the command chose to inherit from the parent). A client default
  for a key an `inherit_env` command did not list still fills — so a client-wide
  safety default (e.g. `GIT_TERMINAL_PROMPT=0`) keeps reaching such commands.
- `Command::ok_codes([])` (an empty set) is now truly **ignored** — a no-op that
  keeps the previously configured codes — rather than resetting the accepted set
  to `[0]`, matching its doc.
- Windows `command_line()` display now doubles a backslash that **precedes an
  embedded quote** (`a\"b` → `"a\\\"b"`), so the rendered argument round-trips
  through `CommandLineToArgvW` instead of re-parsing as `a\b`.
- `Command`'s `Debug` no longer claims to be exhaustive (`finish()` →
  `finish_non_exhaustive()`) and now includes the previously-omitted
  `timeout_grace`/`timeout_signal`/`ok_codes`/tee-presence and the
  security-relevant `groups` (a privilege-drop's supplementary group set).
- Pipeline pipefail attribution now preserves the failing stage's `ok_codes`: a
  stage whose `ok_codes` exclude `0` that nonetheless exited `0` (a rejected-zero
  failure) was rebuilt with the default accepted set `[0]`, so the whole chain
  reported `is_success()` / `Ok` even though the fold had deemed that stage
  failed. `run`/`checked`/`run_unit` on such a chain now correctly surface the
  failure (matching how the same rejected-zero exit behaves on a single
  `Command`). `probe` and `exit_code` are unchanged by design — both read the raw
  `0` exit (`probe` is strictly 0/1, `exit_code` returns the code as data).
- `wait` / `profile` no longer let a newline-free flood (e.g. `base64 -w0`) or a
  single enormous terminated line grow the pump's in-flight line-assembly buffer
  without bound (a potential OOM): their internal retain-nothing discard sink now
  carries a 64 MiB in-flight byte cap. The cap is far above any realistic line,
  so the only observable effect is that a single line exceeding it is not
  delivered to a per-line handler / `stdout_tee` during those discard verbs — the
  same skip a user-set byte cap already applies to over-cap lines.
- `ProcessRunnerExt::first_line` no longer misreports a run that ends naturally
  with no match as `Error::Cancelled` when its `cancel_on` token fires an instant
  after the stream closes; the search is now raced against the token (a firing
  token wins only while the search is still pending), so a natural end still
  reports `Ok(None)`. On genuine cancellation the search is drained to its
  watchdog-closed end before returning, so a `first_line` run on a **shared**
  process group (`ProcessGroup::first_line`) reliably tears its child down on
  cancel instead of racing the watchdog against handle teardown.
- `Command::timeout` is now enforced while **streaming** on a shared-group handle
  (`ProcessGroup::start(&cmd) → stdout_lines`). Previously the deadline watchdog
  armed only for own-group handles, so a quiet, never-exiting child left the
  stream pending forever and `finish` never reported `TimedOut`; the watchdog now
  also arms on a shared-group handle, reaching the direct child by pid.
- `first_line` on a shared-group handle now honors the command timeout — it
  surfaces `Error::Timeout` **and** tears the direct child down, instead of
  returning `Timeout` while stranding the process (its own deadline wrapper could
  abort the watchdog before it fired). It now relies on the deadline watchdog for
  teardown and classifies the timeout from the shared arbiter (set before the
  kill); a backstop bounds the wait for the forking-child gap. On a shared group
  the teardown reaches the direct child by pid — a forking child's grandchildren
  (and, on the Linux cgroup mechanism, a direct child that catches the graceful
  signal and closes stdout but keeps running) may outlive the probe until the
  group is dropped, same as the other shared-group teardown edges.
- Reusing a `ProcessGroup` after a graceful `shutdown`/`shutdown_ref` with
  `escalate_to_kill = false` no longer orphans the fresh children: that shutdown
  latches "skip the kill on Drop" to spare the survivors it left running, and the
  latch was never cleared, so a child spawned into the still-usable group
  afterwards was silently spared by `Drop` too. Spawning now **re-arms** the
  kill-on-drop backstop for the whole group (a group left untouched still keeps
  its spared survivors).
- Cassette replay (`record` feature) is more faithful on the **capturing** verbs
  (`output_string` and the checking verbs `run`/`parse` that route through it):
  (1) a recording made under a bounded `OutputBufferPolicy` now replays as
  **truncated**, so `run`/`parse` still fail loud on the clipped tail instead of
  silently passing it; (2) a pre-cancelled `cancel_on` token now **short-circuits**
  replay with `Error::Cancelled` (both `output_string` and `start`), mirroring the
  real runner's pre-spawn check, instead of handing back a recorded `Ok`; (3) the
  recorded wall-clock **duration** survives replay, so `duration()` is the
  recording's, not a synthetic `0`; (4) the overflow line/byte totals behind an
  `OutputTooLarge` are carried across replay. (The `start`/streaming replay
  re-pumps the canned output through the command's pumps, so its `finish` result
  re-derives truncation/duration rather than reading the recorded flags — a known
  doubles-layer gap.) Old cassettes load the new fields as defaults and can be
  re-recorded.
- Writing a cassette is now **crash-safe**: it writes a sibling temp file and
  atomically renames it over the target, so an interrupted write can no longer
  truncate or destroy an existing good cassette (the symlink-refusal guard is
  preserved).
- Retry and `Supervisor` backoffs are now **cancellable**: a `cancel_on` token
  firing during a backoff (or a supervisor storm pause) resolves promptly with
  `Error::Cancelled` instead of waiting out the full — possibly 30 s+ — delay.
- `Supervisor` backoff escalation now **resets after a healthy run** — one that
  stayed up at least as long as `max_backoff`. A long-lived service that crashes
  occasionally no longer restarts at the `max_backoff` ceiling forever; a tight
  loop whose incarnations are each shorter than the ceiling keeps climbing (the
  floor is on uptime, not exit kind, so an instant `exit 0` loop under `Always`
  escalates and self-throttles rather than spinning at the base delay).
- `RetryPolicy` now folds a **non-finite `multiplier`** (`±∞`) to `1.0`, matching
  its documented contract and `Supervisor::backoff` — previously `+∞` exploded the
  backoff to the cap on the second retry while `Supervisor` treated it as constant.

### Documentation
- `Command::stdout_tee`/`stderr_tee`: documented that the sink is shared through
  `Clone` (an `Arc<Mutex<…>>`), so concurrent `Pipeline` stages **interleave** and
  sequential `retry`/`Supervisor` re-runs **append** (a retried command's sink
  accumulates the failed attempt's output before the successful one's).
- `IntoCommand`: documented the graft trap — passing a ready-made `Command` for a
  *different* program to a client verb runs that program with the **client's**
  env/timeout defaults grafted on (the program is not substituted).
- Crate-root `Encoding`/`StreamExt` re-exports: documented the `0.x`-dependency
  semver coupling and glob-import collision risk (a `prelude` move is tracked for
  v2).
- `Command::retry`: documented that `max_attempts` of `0` and `1` both mean a
  single run (a command always runs at least once).
- `Supervisor`: documented that a permanently-failing command restarts forever
  under the default unlimited `OnCrash` policy (bound it with `max_restarts` /
  `stop_when`); that a single jittered backoff can reach `1.5 ×` `max_backoff`;
  and the healthy-run backoff reset. Corrected `Command::retry`'s one-shot-stdin
  note: such a command is **not retried** (the first error is returned as-is),
  rather than "failing loud on the second attempt".
- `Command::env` documents how to pass secrets: env values are redacted from
  `Debug`/tracing/cassettes (argv is not — prefer env or `stdin`), processkit ships
  no `Secret` type (bring your own `secrecy`/`zeroize` and pass the exposed value),
  and `default_env_fn` covers a per-operation rotating value. `Command::retry` ↔
  `Command::retry_with`/`RetryPolicy` now cross-reference their attempt-vs-retry
  counting, and `RetryPolicy` ↔ `Supervisor::backoff` cross-reference their shared
  backoff schedule.

## [1.1.0] - 2026-06-28

### Added
- `ProcessGroup::shutdown_ref(&self)` — graceful `SIGTERM` → grace → `SIGKILL`
  teardown that **borrows** the group instead of consuming it, for a group held
  behind a shared handle (an `Arc`, a long-lived supervisor, an FFI binding) that
  can't be moved out by value. `shutdown(self)` now delegates to it.
- `ProcessGroup::kill_all` — the honest name for the immediate hard kill
  (mirrors the underlying `Job::kill_all`); replaces the misleadingly-named
  `terminate_all`, which read as a graceful `SIGTERM`.
- `RunProfile::outcome` — the full `Outcome` of a profiled run, plus `code()`,
  `signal()`, and `timed_out()` accessors (mirroring `ProcessResult`/`Outcome`),
  so a profile distinguishes a clean exit from a signal kill from a timeout (all
  three leave `exit_code` `None`). `profile()` is now a superset of `wait()`: one
  call yields both telemetry and the outcome.
- `RunProfile::avg_cpu_cores` — unit-explicit rename of `avg_cpu` (the value is
  in CPU cores).
- `RecordReplayRunner` now covers the streaming `start` verb in both record and
  replay: a recorded run replays through a scripted `RunningProcess` (its output
  flowing through the command's real pumps), where `start` previously returned
  `Error::Unsupported`. Recording a `start` captures the run whole, so an
  interactive streaming run fed stdin mid-stream still can't be cassette-recorded.
  The runner's `output_bytes` verb is now explicitly rejected with
  `Error::Unsupported` in both modes (a lossy-UTF-8 text fixture can't reproduce
  exact bytes) rather than silently re-encoding through the new `start` path.
- `CliClient` now implements `Clone` (when its runner is `Clone`, as the default
  `JobRunner` is), so the whole CLI-wrapper family — `Command`, `Pipeline`,
  `CliClient` — clones uniformly. A clone shares the same default cancellation
  token.

### Changed
- The readiness probes `RunningProcess::wait_for_line` / `wait_for` now require
  `Send` callbacks (matching `Command::first_line`), so the returned futures are
  `Send` — they can cross a thread boundary on a multi-threaded runtime or be
  bridged onto another async runtime, not only `.await`ed in place. This tightens
  a bound on two pre-existing methods: a caller passing a non-`Send` readiness
  closure (rare) would need to adjust. Real-world impact is negligible, so it
  ships in the minor rather than waiting for 2.0.

### Deprecated
- `ProcessGroup::terminate_all` — renamed to `kill_all`; the forwarding alias is
  removed in 2.0.
- `RunProfile::avg_cpu` — renamed to `avg_cpu_cores`; the forwarding alias is
  removed in 2.0.

### Fixed
-

## [1.0.1] - 2026-06-16

### Added
-

### Changed
- fixes in README.md

### Fixed
-

## [1.0.0] - 2026-06-16

**First stable release.** From 1.0.0 onward `processkit` follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html): the public API is
**stable**, and any breaking change lands only in a new **major** version. Within
the `1.x` line, upgrades are backward-compatible. (The `mock` feature's
`mockall`-generated `expect_*` surface stays semver-exempt — it tracks the
`mockall` version; prefer `ScriptedRunner` / `RecordingRunner` for a stable
double.)

The entries below are the final shape-fixes and additions made before the freeze,
relative to `0.11.1`.

### Added

- `SupervisionOutcome` now derives `Clone`, `PartialEq`, and `Eq` — consistent
  with the other result types (`ProcessResult`, `Finished`, `Outcome`,
  `RunProfile`), so a supervision report can be stored, compared, and logged.
- `Outcome` gained accessor methods — `code() -> Option<i32>`,
  `signal() -> Option<i32>`, `timed_out() -> bool` — so code holding a bare
  `Outcome` (e.g. from `RunningProcess::wait` or `Finished::outcome`) needn't
  `match` a `#[non_exhaustive]` enum with a wildcard. (No `is_success`: success is
  `ok_codes`-aware and lives on `ProcessResult::is_success`.) `ProcessResult`
  gained `signal() -> Option<i32>` for the same vocabulary parity (it already had
  `code`/`timed_out`). (R5-3)

### Changed

- **Breaking:** `OutputLine` (the per-line payload of
  `RunningProcess::output_events`) no longer exposes its `text` as a public field —
  read it via `OutputLine::text() -> &str` or `into_text() -> String`. This
  accessor-fronting (matching `ProcessResult`/`Stdin`) frees the line
  representation to evolve post-1.0 without a break. Migration: `line.text` →
  `line.text()`.
- **Breaking:** `Error::ResourceLimit(String)` is now the struct
  variant `Error::ResourceLimit { message: String }` (parity with the crate's
  other rich error variants; room to add structured detail later without a break).
  Only relevant with the `limits` feature. Migration: match
  `Error::ResourceLimit(m)` → `Error::ResourceLimit { message: m }`.
- **Breaking:** the text-capture verb is now spelled **`output_string`
  everywhere** — `ProcessRunner::output_string` (the trait seam, was `output`),
  `CliClient::output_string` (was `output`), and the free fn
  `processkit::output_string` (was `processkit::output`). `Command`, `Pipeline`,
  and `RunningProcess` already used `output_string`, so the surface is now uniform
  (`output_string` / `output_bytes` on every layer). Two reasons: cross-type
  consistency (the same operation no longer has two names), and disambiguation
  from `std::process::Command::output`, which returns **bytes** — a bare `output`
  returning text was a footgun. Migration: rename `.output(` calls to
  `.output_string(`, `processkit::output(` to `processkit::output_string(`, and —
  for custom `ProcessRunner` impls — the required method and any `mockall`
  `expect_output` to `expect_output_string` (M-1).

### Fixed

- `Pipeline::run` now fails loud (`Error::OutputTooLarge`) when the last stage's
  capture was truncated by a bounded `output_buffer`, instead of silently
  returning the clipped tail as if complete — matching `ProcessRunnerExt::run`,
  `CliClient::run`, and the pipeline's own `parse`/`try_parse` (R5-2).
- A readiness probe (`wait_for` / `wait_for_port`) that reaps a cleanly-exited
  child now claims the timeout arbiter, like every other reap path. This closes a
  multi-threaded race where a streaming deadline watchdog firing on another thread
  at the same instant could misclassify the clean exit as `TimedOut` (R5-1).
- Linux per-process sampling (`stats()` / `RunningProcess::cpu_time` /
  `peak_memory_bytes`) now uses saturating arithmetic throughout: the CPU
  user+system tick counts combine with `saturating_add` and the nanosecond cast
  is clamped, and the VmHWM kB→bytes conversion uses `saturating_mul` — so an
  implausibly large tick or memory figure clamps instead of debug-panicking or
  silently wrapping (parity with the `stats()` fold and the Windows combine) (A1).

### Security

- Error `Display` now sanitizes the Unicode line/paragraph separators U+2028 and
  U+2029 (replaced with `U+FFFD`), alongside the existing control- and
  bidi-control neutralization. `char::is_control()` does not cover these two, so a
  hostile child's stderr/`Parse` text carrying them could previously inject a line
  break into a one-line `{err}` log/terminal render (N4-1).

## [0.11.1] - 2026-06-15

### Added

- `output_all_bytes` — the raw-bytes companion to `output_all`: the same
  bounded-concurrency fan-out, but each command's stdout is captured as
  `Vec<u8>` (for batching binary-producing commands). Same ordering, partial-
  failure, and teardown semantics (S-7). The `concurrency` argument stays a
  plain `usize` clamped to ≥ 1 (not `NonZeroUsize`) — the documented clamp keeps
  the common call ergonomic.
- `Pipeline` gained verb parity with a single `Command`: `output_bytes` (binary
  capture), `run_unit`, `exit_code`, `checked`, `probe`, and `parse` / `try_parse`
  — each operating on the pipefail outcome — plus a chain-level `cancel_on(token)`
  that tears the whole chain down to `Error::Cancelled`. `cancel_on` **gap-fills**
  (it leaves an explicit per-stage `Command::cancel_on` intact), matching
  `CliClient::default_cancel_on` rather than `Command::cancel_on`'s last-write-wins
  override. `Pipeline::parse` / `try_parse` deliberately carry **no** `Send` bound
  on the closure (unlike the `Command`/`ProcessRunnerExt`/`CliClient` versions),
  since the pipeline runs the parser inline rather than across a task boundary — so
  they accept strictly more closures. The streaming `first_line` is intentionally
  omitted (a chain consumes its last stage in full; add a `| head -n1` stage
  instead) (S-1).
- `parse` / `try_parse` are now first-class verbs on `ProcessRunnerExt` (so every
  runner — `JobRunner`, `&ProcessGroup`, a `ScriptedRunner` — has them) and on
  `Command` (`cmd.parse(|s| …)` / `cmd.try_parse(|s| …)`), not just `CliClient`.
  Each runs success-checked, fails loud on a bounded-buffer truncation (so a
  parser never sees a clipped tail), and feeds stdout to the closure. Like
  `first_line`, they are generic over the closure and therefore unavailable on a
  `&dyn ProcessRunner` (call them on a concrete runner or via the wrappers) (S-2).
- `ProcessResult` and `Finished` are now `#[must_use]`: dropping one unread (its
  exit status / outcome is the only signal of how the run ended) triggers an
  `unused_must_use` warning. Inspect `is_success()`/`code()`/`ensure_success()`
  (or `Finished::outcome`), or bind to `let _` to discard on purpose (L-4).
- `RunProfile` now derives `Eq` (it already derived `PartialEq`; all fields are
  integer/`Duration`), so profiles can be compared exactly and used as keys (L-5).

### Changed

- Internal: the per-backend "spare survivors on drop" flag is now a shared
  `SkipDropKill` latch (one place owns the load-bearing memory ordering), and the
  two display sanitize-and-cap loops in `error.rs` are factored into one helper
  with a single `DIAG_CAP` constant. No behavior change.
- **Breaking (minor):** `CliClient::parse` / `try_parse` gained an explicit
  closure type parameter and `T: Send` / `F: Send` bounds (they now delegate to
  the new `ProcessRunnerExt` verbs). Turbofish call sites that named only the
  output type (`try_parse::<u32>(…)`) become `try_parse::<u32, _>(…)`; the common
  inferred-generics form is unaffected. The `Send` bounds (required because the
  verbs return a boxed `Send` future) also narrow what compiles: a parser that
  returns a non-`Send` value, or a closure that captures a non-`Send` value, is
  no longer accepted — extract the parse into a `Send`-returning step first.
- **Breaking (pre-1.0 hardening):** `Finished` (returned by `RunningProcess::finish`)
  is now `#[non_exhaustive]` — read its `outcome` / `stderr` fields or destructure
  with a trailing `..`, so a future field (e.g. a duration) can be added without
  another break. Brings it in line with every other crate-produced result struct.
- `RunningProcess::wait_for_line` no longer arms the `Command::timeout`
  watchdog: a readiness probe is now bounded only by its own `within` and can
  never kill the process tree or
  flip the run's outcome to `TimedOut` — matching `wait_for` / `wait_for_port`.
  The command timeout is still enforced, by the consuming verb (`finish`) after
  the probe. (Behavior fix; the probe's signature is unchanged.)
- The supervisor now classifies a "crash" with `ProcessResult::is_success` (which
  honors `Command::ok_codes`) instead of raw `code() == Some(0)`. A supervised
  command with custom accepted codes (e.g. `ok_codes([0, 2])` exiting `2`) is no
  longer treated as a crash — `RestartPolicy::OnCrash` agrees with the rest of
  the crate and stops feeding the failure-storm score on accepted exits.

### Fixed

- The output line pump's handling of an over-cap line is now independent of how
  the OS chunked the read: (a) its byte accounting no longer counts a CRLF
  terminator's `\r` as content when it lands at the end of a chunk, and (b) a line
  whose content is exactly `max_bytes` and ends in CRLF is retained whether the
  CRLF arrives whole or split across a read (previously it was dropped when split).
  An over-cap *unterminated* final line is dropped — never delivered to a line
  handler or tee — upholding the "an over-cap line is never retained or delivered"
  contract. This stabilizes the `OverflowMode::Error` byte ceiling and the
  seen-byte total.
- Record/replay (`record` feature): `RecordReplayRunner::replay` now rejects, as
  `Error::Io(InvalidData)` at load, a cassette entry with a contradictory outcome
  (more than one of an exit code, a timeout, or a signal set) and a cassette file
  over 64 MiB — a malformed or stray fixture fails loud instead of replaying a
  silently-wrong outcome or buffering an unbounded file.
- The supervisor's crash classification, the `output_bytes` truncation totals, the
  pipeline's whole-chain-timeout task teardown, and the cgroup/Job-Object
  graceful-shutdown survivor flag were tightened (no behavior change for the common
  path); the `profile` sampler no longer folds a metrics reading taken once the
  child's pid may have been recycled.
- Windows: a child is reaped if `spawn` unwinds in the window between process
  creation (`CREATE_SUSPENDED`) and Job-Object assignment, so a panic there can
  no longer leak a suspended, uncontained process. The reaper guard is now armed
  *before* the fallible `id()`/`raw_handle()` reads as well, closing the matching
  early-return leak window (N-1).
- Linux: `ProcessGroupStats` now sums per-process CPU time and memory with
  `saturating_add`, so an implausibly large aggregate clamps instead of panicking
  on overflow (N-2, parity with the Windows fold).
- Record/replay (`record` feature): a command whose stdin is a one-shot streaming
  source (`Stdin::from_reader`/`from_lines`) is now rejected with
  `Error::Unsupported` in both record and replay modes, instead of silently keying
  all such invocations alike (their bytes can't be captured into the match key).
  Use a replayable source (`from_bytes`/`from_string`/`from_file`) to record a
  stdin-bearing invocation (L-1).

### Security

- `Error`'s one-line `Display` now neutralizes Unicode **bidirectional-formatting
  controls** (the "Trojan Source" class, CVE-2021-42574) in addition to the ASCII
  / C1 control characters it already replaced, and the `Parse` variant's `Display`
  is sanitized too (it previously only truncated). A hostile child's output can no
  longer inject terminal-reordering or escape sequences into an operator's log or
  terminal through an error's `{}`.

## [0.11.0] - 2026-06-14

### Changed

- **Breaking:** the `stats` feature is now **opt-in** — it is no longer in the
  default feature set (`default = ["process-control"]`). The metrics surface it
  gates — `ProcessGroup::stats` / `ProcessGroupStats`, `RunningProcess::cpu_time` /
  `peak_memory_bytes`, and `RunProfile` / `RunningProcess::profile` — is hidden by
  default; enable it with `features = ["stats"]` (or `limits`, which still implies
  it). The motivation: `stats` is the only feature carrying an extra dependency (on
  Windows, the `ProcessStatus` FFI used solely for the peak-memory readout) and is a
  specialized add-on the crate's core (spawn / contain / capture / stream / pipeline)
  never needs — so it shouldn't be in every default build. The kill-on-drop tree
  guarantee and all non-metrics behavior are unchanged. Migration: add `stats` (or
  `limits`) to your `processkit` features if you use any of the metrics APIs.
- **Breaking:** `OutputEvent` (yielded by `RunningProcess::output_events`) now
  carries an `OutputLine` per stream instead of a bare `String`:
  `OutputEvent::Stdout(OutputLine)` / `Stderr(OutputLine)`, where `OutputLine` is a
  `#[non_exhaustive]` struct with a `text` field. This reserves room to attach
  per-line metadata (e.g. a timestamp or a monotonic line index) in a future
  release without another breaking change. A new `OutputEvent::text() -> Option<&str>`
  reads the line text regardless of stream. Migration: replace `OutputEvent::Stdout(s)`
  with `OutputEvent::Stdout(line)` and use `line.text` (or `event.text()`).

### Fixed

- Closed the documented cancel-precedence race ("Issue 7"): a run reaped on its own
  is no longer at risk of being misreported as `Err(Cancelled)` by a token that
  fires in the window between the reap and the disposition check. The reap paths now
  carry *which wait arm won* (`backend_wait` vs the cancel arm) out of the `select!`
  and record the cancel disposition from that, instead of a post-hoc
  `is_cancelled()` read that another thread could flip. Internal refactor (the reap
  bookkeeping — cancel snapshot, watchdog abort, timeout classification — is now a
  single `on_reaped` step shared by every wait path); no public API change.

## [0.10.2] - 2026-06-14

### Added

- `Command::checked` and `Command::run_unit` — the success-checking verbs that the
  `ProcessRunnerExt` and `CliClient` families already carry, now also on `Command`
  itself (`cmd.checked()` returns the whole success-checked `ProcessResult`;
  `cmd.run_unit()` requires an accepted exit and discards the output). Closes a
  verb-family inconsistency: `Command` already had `run`/`probe`/`exit_code`/
  `first_line` but not these two.

### Security

- `record`-feature cassette writes no longer follow a symlink at the cassette path
  on Unix (`O_NOFOLLOW`): a planted `cassette.json` symlink can no longer redirect
  the secret-bearing write (and its `0600`) onto a victim file the link targets —
  the write fails loud (`ELOOP`) instead. (On Windows the cassette still inherits
  the directory ACL; restrict the containing directory, or use a per-user temp
  dir, for sensitive fixtures — now documented on the writer.)
- The one-line `Error` `Display` (the `: <last diagnostic line>` tail on
  `Exit`/`Timeout`/`Signalled`) now replaces control characters with `U+FFFD`, so a
  hostile child's stderr cannot inject terminal escape sequences (ANSI, `BEL`,
  `NUL`, cursor moves) into an operator's log or terminal through a `{err}` format.
  Printable text and the 200-byte cap are unchanged.

### Changed

- A ready-made [`Command`] passed straight to a [`CliClient`] verb (e.g.
  `git.run(some_command)`, the per-call-customization form) now receives the
  client's defaults — `default_timeout`, `default_env`, `default_cancel_on` —
  **filled into the gaps it left**, instead of being run with none of them (M7).
  The command's own explicit settings still win; only unset ones are filled, and
  the fill is idempotent (a verb running `client.command(..)`, already defaulted,
  is unaffected). This closes a silent footgun where a client-wide cancellation
  token (wired for shutdown) would not reach a per-call-customized command. An
  argument-list call is unchanged.
- `ProcessStdin` (the interactive `keep_stdin_open` writer) documents the
  full-duplex deadlock hazard: feeding a large stdin while nothing drains the
  child's stdout can wedge both sides — drain stdout concurrently. (No behavior
  change; the non-interactive `Stdin` sources are already safe.)

### Fixed

- A retained-byte cap ([`OutputBufferPolicy::with_max_bytes`]) now bounds the pump's
  **in-flight** line-assembly buffer, not just the retained backlog. Previously a
  newline-free flood (`base64 -w0`, a multi-gigabyte single "line") accumulated in full
  in the decode buffer before the cap was ever consulted — defeating the very memory
  bound the byte cap exists to provide. A line whose own length exceeds the cap is now
  dropped as it arrives (it can never be retained whole): under the drop modes it sets
  the truncation signal, under [`OverflowMode::Error`] it trips the fail-loud ceiling.
  Consequence: an over-cap line, never assembled, is also not delivered to a per-line
  handler or `stdout_tee` (set no byte cap if a tee must see arbitrarily long lines).
- A child stream interrupted by a **read error** mid-multibyte-character no longer
  fabricates a phantom replacement-character (`U+FFFD`) line. The decoder's end-of-stream
  flush — which turns a dangling incomplete sequence into `U+FFFD` — is now performed only
  on a *clean* EOF; a read error means the stream was truncated, so the incomplete trailing
  bytes are dropped rather than invented into output.
- Linux cgroup join (`write_self_pid`) now treats a **short write** to
  `cgroup.procs` as an error instead of a success, so a child can't end up only
  partially joined to its cgroup (silent containment degradation). The check is
  allocation-free, preserving the async-signal-safety of the fork→exec hook.
- `ProcessGroup::signal` — and the `process-control` `suspend`/`resume` verbs on
  their per-member fallback path (older kernels without `cgroup.freeze`) — on the
  Linux **cgroup** backend now surface a
  real per-member delivery failure instead of always reporting success: a non-`ESRCH`
  `kill(2)` error (notably `EPERM` — a member that changed uid, or a seccomp/container
  restriction) is returned rather than silently swallowed. `ESRCH` (the member already
  exited) is still treated as success, and `signal(Signal::Kill)` still takes the
  atomic whole-tree `cgroup.kill` path. The graceful-shutdown SIGTERM tier is unchanged
  (it is best-effort and already ignores the per-member result before escalating to
  SIGKILL).
- `RunningProcess::first_line` now surfaces `Error::Cancelled` when its command's
  cancellation token has fired, instead of returning `Ok(None)`. A cancelled run's
  stdout stream simply ends, which was indistinguishable from "the predicate never
  matched" — a readiness probe with a shutdown token could misread cancellation as
  "the line never appeared / startup failed".
- The background cancellation watchdog no longer signals a process whose pid may
  have already been reaped (and recycled by an unrelated process): it now checks the
  same run-state arbiter the deadline watchdog uses and skips the kill once the run
  has been reaped, closing the residual window between the reap and the watchdog's
  abort.
- `RunningProcess::shutdown(grace)` no longer tears the tree down twice when the
  command also has a `Command::timeout`: its own graceful SIGTERM→grace→SIGKILL is
  the single teardown (the run's timeout teardown is suppressed). An already-elapsed
  deadline still classifies the outcome as `Outcome::TimedOut`; the `grace` governs
  the teardown timing.
- Hardened the "no watchdog task outlives the reap" invariant: the watchdog abort
  now runs on every reap path (including the short-circuit repeat-reap branches),
  not only the first observer, so a future code path cannot leave a deadline/cancel
  task live past the child's exit.
- Concurrent runs of the same cloned **one-shot** stdin source
  ([`Stdin::from_reader`]/[`from_lines`]) can no longer race so that one silently
  feeds the child empty stdin. The payload is now taken **atomically** at launch
  (a single step, under the source's async lock), so a second concurrent run
  observes it consumed and fails loud, closing the check-then-take TOCTOU.
- A command with a one-shot streaming stdin source is **no longer retried**: such
  a source feeds a single run and cannot be replayed, so a retryable failure no
  longer spins the retry loop re-hitting the consumed-stdin launch error
  `max_attempts` times with backoff between — it runs exactly once regardless of
  the retry policy. (Re-runnable sources — `from_bytes`/`from_string`/`from_file`/
  `from_iter_lines` — still retry normally.)
- A **panicking** stdin-writer task is now surfaced as `Error::Stdin` on an
  otherwise-successful run instead of being silently swallowed into a clean
  success (the writer task's `JoinError` was previously dropped).

## [0.10.1] - 2026-06-14

### Changed

- **Packaging/metadata only — no code or API changes.** Sharpened the crates.io
  discovery surface so the crate is easier to find: rewrote `description` to lead
  with the kill-on-drop no-orphan guarantee; replaced the mis-applied
  `command-line-utilities` category (that slug is for CLI *binaries*, not a
  library) with `asynchronous`, `os`, `os::unix-apis`, `os::windows-apis`, and
  `concurrency`; refreshed `keywords` to high-volume search terms
  (`process`, `subprocess`, `tokio`, `async`, `process-group`); and excluded the
  3.5 MB `cover.png` banner from the published archive (it renders from its
  absolute URL), shrinking the package from ~4.7 MiB to ~1.3 MiB. Also tightened
  the README and crate-doc intros. Cut as `0.10.1` so crates.io picks the new
  metadata up — the live `0.10.0` shipped the old/mis-categorized values.

## [0.10.0] - 2026-06-14

### Added

- `OutputBufferPolicy::with_max_bytes(n)` (and a `max_bytes` field) — a retained-byte
  ceiling, independent of `max_lines`, so one enormous newline-free line can no longer
  evade the line cap and exhaust memory. Composes with `bounded`/`fail_loud`/`unbounded`;
  under `OverflowMode::Error` it is a fail-loud byte ceiling.
- `ScriptedRunner::on_sequence(prefix, replies)` — serve an ordered sequence of replies
  (each once in turn, then the last repeats forever), matching the cassette replay model
  so a fail-then-succeed retry scenario is scriptable declaratively.
- `Error::CassetteMiss { program }` — a cassette replay with no matching recording (a
  stale or incomplete cassette), kept distinct from a missing-program error so
  `is_not_found()` is `false` and a wrapper can't mistake it for an absent optional tool.
- `RunningProcess::shutdown(grace)` (D4) — gracefully stop a started handle's process tree:
  `SIGTERM`, wait up to `grace`, then `SIGKILL` survivors (atomic on Windows), returning the
  resulting `Outcome`. The "started a dev server, exercised it, now stop it cleanly" verb — the
  graceful counterpart to dropping the handle (hard kill) or `start_kill`. Own-group handles
  (`Command::start`/`JobRunner`) only; a shared-group handle (`ProcessGroup::start`) returns
  `Error::Unsupported` (use `ProcessGroup::shutdown`).
- `CliClient` verbs now take an **argument list directly** (D7): `git.run(["status"])`
  instead of the double-mention `git.run(git.command(["status"]))`. A new sealed
  `IntoCommand` trait lets every verb (`run`/`output`/`output_bytes`/`run_unit`/`exit_code`/
  `probe`/`parse`/`try_parse`) accept either an argument list (built for the client's program
  with its defaults) **or** a ready-made `Command` (for per-call customization) — so existing
  `git.run(git.command(…))` call sites keep compiling. Two missing verbs were added to
  `CliClient`: `checked` and `first_line`.
- `ProcessRunner::output_bytes` (with a default impl) and `CliClient::output_bytes` (D5) —
  raw-byte stdout capture is now part of the runner **seam**, not just `Command`, so a
  byte-producing tool (`git cat-file`, `tar -c`, an image transcoder) is testable through a
  `ScriptedRunner` / `&ProcessGroup` / `JobRunner` exactly like a text one. The default
  routes through `start`, so a runner that overrides `start` gets it for free; an
  `output`-only runner surfaces `Error::Unsupported`, matching `start`. (Adding a defaulted
  trait method is source-compatible — existing `impl ProcessRunner` blocks keep compiling.)

### Changed

- **Breaking:** `Error::OutputTooLarge` fields changed from `{ program, limit, total_lines }`
  to `{ program, line_limit: Option<usize>, byte_limit: Option<usize>, total_lines,
  total_bytes }` — the ceiling can now be a line cap, a byte cap, or both, so the error
  reports each configured cap and both totals. The `Display` message changed to match.
- **Breaking:** `Command::stdout_tee<W>` / `stderr_tee<W>` now take a
  `tokio::io::AsyncWrite` sink (was `std::io::Write`). The write is awaited on the capture
  pump, so a slow sink applies backpressure rather than blocking the runtime, and a write
  error disables the tee with a `tracing` warn instead of being silently swallowed. The
  tee now runs **independently** of `on_stdout_line` (it no longer replaces the handler).
- The fail-loud `OverflowMode::Error` ceiling now fires on the **cumulative** output the
  pump has seen (total lines / bytes), not the current backlog — so a streaming consumer
  draining lines as they arrive can no longer evade it.
- `ProcessGroup::terminate_all` / `shutdown` / `signal` now return `Err` when the pre-5.14
  Linux cgroup per-pid `SIGKILL` fallback cannot drain the tree (a fork bomb still
  out-spawning, or `D`-state zombies) — previously a false success. The atomic backends
  (cgroup `kill`, Windows Job Objects, the POSIX process-group fallback) never report this.
- `ProcessGroup::adopt` of a child that has exited but is **not yet reaped** is now a
  successful no-op (`Ok`) on the containment backends, instead of surfacing the backend's
  raw assign/write error. (An already-*reaped* child still errors — no pid/handle left.)
- **Breaking:** `ScriptedRunner::on(prefix, …)` now matches the **program name** as well
  as the arguments — the first prefix element is the program, so `.on(["git", "status"])`
  answers for `git status` but not `rm status` (aligning with the program-aware cassette
  key). Existing argument-only rules must prepend the program name.
- **Breaking:** a cassette replay that finds no matching recording now returns
  `Error::CassetteMiss` instead of `Error::Spawn` with a not-found source.
- **Breaking:** "program not found" now has a **single representation** (D11). Every
  launch failure where the program can't be located — a bare name absent from `PATH`, a
  path that doesn't resolve, a customized `PATH` — surfaces as `Error::NotFound`, and
  `Error::NotFound::searched` changed from `String` to `Option<String>` (`Some(dirs)` when a
  bare name was searched against `PATH`, `None` when no `PATH` search applied). As a result
  **`is_not_found()` is now true *only* for `Error::NotFound`**: a missing or invalid working
  directory (a `Spawn` carrying a `NotFound` io kind), a program that is installed but
  not directly executable (a Windows `.cmd`/`.bat`, surfaced as `Spawn`), and a missing
  cassette *file* (an `Io` not-found) are no longer reported as "not found", so the
  "command not installed?" hint can't misfire. `Error::NotFound`'s `Display` now says
  "not found on PATH" only when a `PATH` search happened (`searched` is `Some`); a path-form
  or customized-`PATH` program reads simply "not found".
- **Breaking:** run cancellation is now a **core feature, not opt-in** — the `cancellation`
  Cargo feature is removed. `Command::cancel_on`, `CliClient::default_cancel_on`,
  `Error::Cancelled`, `Reply::pending`, and the re-exported `CancellationToken` are always
  available, and `tokio-util` is now an unconditional dependency. Remove `"cancellation"`
  from your `features` list (a build that named it will fail with "unknown feature"); no code
  change is otherwise needed. Cancellation is core semantics, not an option — the feature gate
  bought little (`tokio-util`'s `sync` module is tiny and usually already in the graph) at the
  cost of the crate's largest `#[cfg]` surface.
- **Breaking:** `processkit` now supports **only Unix and Windows targets**. A bare target
  (e.g. `wasm32-unknown-unknown`) no longer compiles a containment-less fallback that
  couldn't honor kill-on-close or a graceful timeout — it now fails at compile time, via a
  `compile_error!` guard (or, since the crate needs `tokio::process`, earlier in tokio's own
  dependencies, which don't support such targets). The `Mechanism::None` variant (only ever
  produced on those targets) is removed; `Mechanism` stays `#[non_exhaustive]`, so a future
  fallback can re-add it.
- **Breaking:** `Error::Timeout` and `Error::Signalled` now carry `stdout` and `stderr`
  fields (D12) — whatever the run captured before the deadline or signal killed it. A
  hung-then-killed tool's partial stderr is frequently the explanation, and it was
  previously unreachable from `run()` / `checked()`: `diagnostic()` returned `None`. Now
  `Error::diagnostic()` covers both variants, and their one-line `Display` appends the same
  bounded last-line tail as `Error::Exit` (`` `db-migrate` timed out after 30s: waiting for
  lock held by pid 4123 ``). `Error::Cancelled` deliberately carries no streams —
  cancellation is a caller-initiated immediate stop; any output captured before the kill
  is intentionally discarded.
- **Breaking:** the blanket `impl From<std::io::Error> for Error` is removed (D13). An
  arbitrary `io::Error` no longer converts into `Error::Io` implicitly through `?`, so a
  caller's unrelated IO error can't silently fall into the crate's taxonomy (where
  `is_transient` / `is_permission_denied` would classify it). `Error::Io` is now produced
  only at the crate's own deliberate IO sites (driving a child, controlling a group,
  cassette files). Code that relied on `?`-converting an `io::Error` into `processkit::Error`
  should map explicitly (`.map_err(processkit::Error::Io)`) or use `Box<dyn Error>` /
  `anyhow`. `ProcessStdin`'s writer methods already returned `std::io::Result` and are
  unchanged.
- **Breaking (behavior):** the checking verbs that hand back stdout — `run`, `parse`,
  `try_parse` (on `Command`, `ProcessRunnerExt`, and `CliClient`) — now **fail loud** with
  `Error::OutputTooLarge` when a bounded `OutputBufferPolicy` silently dropped captured
  lines (B12), instead of returning a truncated tail as if complete (a parser would have
  parsed half a document). The lenient capture verbs (`output_string`/`output_bytes`) are
  unchanged — they still return the result with `truncated()` set for the caller to inspect.
  Only triggers under a non-default bounded *drop* policy.
- **Breaking (behavior):** re-running or retrying a command whose stdin is a **one-shot**
  streaming source (`Stdin::from_reader`/`from_lines`) now fails loud at launch with an
  `Error::Io` (`InvalidInput`) once the source has been consumed (D10), instead of silently
  feeding the re-run empty stdin. Use a re-runnable source
  (`from_bytes`/`from_string`/`from_file`/`from_iter_lines`) to retry or re-run.
- **Breaking:** the streaming verbs are now **fallible** (D2): `RunningProcess::stdout_lines`
  and `output_events` return `Result<StdoutLines>` / `Result<OutputEvents>` instead of the
  bare stream. They `Err` (an `Error::Io` `InvalidInput`) on a non-piped stdout
  (`StdioMode::Inherit`/`Null`) or a second streaming call (stdout streams once) rather than
  handing back a silently-empty stream — mirroring the bulk verbs' loudness and making a
  second `wait_for_line` a clear error instead of a forever-`NotReady` probe. Add `?` /
  `.expect(..)` at the call site.
- **Breaking:** the streaming finishers are **unified** (D3): `finish_streamed()` and
  `finish_events()` collapse into a single `RunningProcess::finish() -> Result<Finished>`,
  and the `StreamedFinish` struct is renamed `Finished` (`{ outcome, stderr }`). After
  `output_events`, `finish().stderr` is empty (stderr was delivered to you as events). `wait()`
  (the discard finisher) is unchanged. Rename `finish_streamed`/`finish_events` → `finish` and
  `StreamedFinish` → `Finished`.
- **Breaking:** `RunningProcess::standard_input()` is renamed `take_stdin()` (D17) — the new
  name signals that it *takes* (consumes) the stdin writer on the first call (returning `None`
  after), and aligns the stdin family's spelling (`stdin` / `keep_stdin_open` / `take_stdin`).
- **Breaking:** `Command::unchecked()` is renamed `unchecked_in_pipe()` (D9/D17) — the name now
  makes the **pipeline-only** scope explicit. It was always a no-op outside a `Pipeline` (a
  single run's status is already data in its `ProcessResult`); the clearer name removes that
  footgun. The `producer.unchecked_in_pipe().pipe(consumer)` shape (suppress a producer's
  `SIGPIPE` under pipefail) is otherwise unchanged.
- (Naming sweep, D17: `OutputBufferPolicy::fail_loud`, `RunningProcess::kills_tree_on_drop`, and
  the deadline lexicon `Error::Timeout` / `Outcome::TimedOut` / `timed_out` / `Error::NotReady`
  were reviewed and **kept** — already clear and well-differentiated (`NotReady` is intentionally
  distinct from `Timeout`), so a rename would be churn on a soon-frozen surface.)
- **Breaking:** `OutputEvent` (yielded by `output_events`) is now `#[non_exhaustive]` — a future
  release may add a third event kind (e.g. a lifecycle marker) without a breaking change, so a
  `match` on it now needs a `_` arm.
- The `mock` feature's `MockRunner` is documented as **semver-exempt**: its `mockall`-generated
  `expect_*` surface (and the opaque expectation types) tracks the `mockall` dependency, not this
  crate's frozen API. `ScriptedRunner` / `RecordingRunner` are the stable, recommended doubles.
- **Breaking:** the test doubles moved from the crate root into a `processkit::testing` module
  (D6): `ScriptedRunner`, `Reply`, `Invocation`, `RecordingRunner`, and (feature-gated)
  `RecordReplayRunner` / `MockRunner` are now `processkit::testing::*`. This keeps the production
  surface focused (they exist only to replace subprocesses in tests). Update imports:
  `use processkit::testing::{ScriptedRunner, Reply};`.

### Fixed

- `Supervisor` no longer panics when a backoff/storm delay approaches `Duration::MAX` with
  jitter enabled (the default): the jittered delay is clamped to the crate's `MAX_DEADLINE`
  ceiling instead of overflowing `Duration::mul_f64`. Reachable via `max_backoff(Duration::MAX)`
  or `storm_pause(Duration::MAX)`.
- `RunningProcess::start_kill` is **documented and guaranteed idempotent** (D20): killing a
  child that has already exited and been reaped (e.g. by a prior readiness probe or `wait_any`
  observation) is a successful no-op — like `kill` on a Unix zombie. (Current tokio/std
  already return `Ok` here; the crate also defensively treats a stray `InvalidInput` from a
  reaped handle as the no-op success it is, and a regression test pins the contract.)
- **Documented (D18):** `Outcome::Signalled` is **Unix-only**. On Windows a killed process
  reports `Outcome::Exited` with a platform code (no signal abstraction) —
  `TerminateJobObject(_, 1)` is `Exited(1)` (indistinguishable from `exit(1)`), `Ctrl-C` is
  `Exited(-1073741510)`. The crate reports the platform truth rather than guessing a
  `Signalled` from an NTSTATUS code; use a deadline or cancellation token when you must *know*
  a run was killed. (See `Outcome` docs and `docs/platform-support.md`.)
- **Pipeline status semantics are unified (D14).** The last stage is now evaluated by the
  same pipefail rule as the inner stages — one `is_clean`, one attribution — fixing two
  inconsistencies:
  - An inner stage that hit its **own** `Command::timeout` now reports **that stage's**
    deadline in the resulting `Error::Timeout`, instead of the chain's timeout or a
    misleading `timed out after 0ns` (B10).
  - The **last** stage's `ok_codes` are now honored (E24e): a last stage with
    `ok_codes([0, 1])` exiting `1` is a clean, successful chain — previously the last
    stage's `ok_codes` were reset to `[0]` while inner stages honored theirs.
  `unchecked_in_pipe` still forgives an exit (preserving the real code) but not a last-stage
  timeout/signal (the chain's output is then broken).
- A `wait_any` / `wait_all` **loser** with `keep_stdin_open` now keeps its stdin usable
  (B15): the race no longer closes an untaken stdin pipe out from under the caller (which
  left `take_stdin()` returning `None` and the child wedged on a premature EOF),
  honoring the documented "losers remain fully usable" guarantee. A `keep_stdin_open` child
  blocked reading stdin is the caller's responsibility, like the existing "no output
  pumping" non-feature — take its writer (or don't keep stdin open) before racing it.
- `output_all`'s cancel-on-drop documentation is corrected (B16): dropping the batch future
  tears down in-flight children only with an **own-group** runner (`JobRunner`); with a
  shared `&ProcessGroup` runner the children live until the caller tears the group down.
- **Non-ASCII-compatible encodings no longer corrupt output.** Bytes are fed through one
  persistent decoder and the *decoded* text is split on newlines, so UTF-16LE/BE (whose
  code units contain `0x0A` bytes that are not line breaks) and stateful encodings decode
  correctly instead of being mangled by a raw-byte split.
- A byte-order mark is handled once at the stream start (the chosen encoding's own BOM
  only), so a legacy line that merely begins with BOM-looking bytes is no longer silently
  re-decoded as UTF-16.
- A CRLF terminator now strips exactly one trailing `\r`, not every trailing `\r`, so
  `"data\r\r\n"` yields `"data\r"`.
- A mid-stream read error now flushes the partial final line instead of dropping it
  (matching the EOF path).
- Sys-layer safety hardening: the POSIX process-group fallback no longer risks signalling
  a **recycled PID**'s group (a latch gates the whole-group fallback) and recovers from a
  poisoned lock instead of panicking; on Windows, `suspend()`/`resume()` no longer return a
  false error when a member thread exits between the snapshot and the walk, and a `Drop`
  that skips the kill now clears the Job Object CPU-rate cap; on Linux, cgroup directory
  names carry a per-process salt so a recycled PID can't collide with a crashed run's
  leftover directory and silently downgrade to the process-group fallback.
- Deadline computations are clamped so a `Duration::MAX`-ish timeout/grace can no longer
  overflow `Instant` arithmetic and panic.
- `Error::Parse`'s `message` is now bounded to a 200-byte preview in both `Display` and
  `Debug` (B14) — a caller-built message that embeds the full unparsed output can no longer
  flood a log line or an `.unwrap()` panic message (the same protection the `Exit` streams
  already had). The complete text stays reachable on the `message` field.
- Test doubles now match the live runner on the contracts they exist to exercise: a
  panicking line handler is isolated on the bulk `ScriptedRunner::output` path (not only
  while streaming); a capture verb on a non-piped stdout errors instead of returning canned
  output; an already-cancelled token short-circuits to `Cancelled` before serving a reply;
  `wait_any`/`wait_all` honor cancellation mid-wait (a pending scripted handle no longer
  hangs forever); a kill landing after a scripted child's natural exit keeps the cached
  outcome (not `Signalled`); the scripted run lifetime accounts for a stderr longer
  than stdout (no truncation); and a scripted `stdout_lines`/`output_events` stream is now
  bounded by the command's `timeout` — the stream ends at the deadline and the run reports
  `TimedOut`, like a real child whose pipes close when its tree is killed (a scripted
  streamed run that previously ran to completion ignoring the timeout now ends early).
- Cassette replay now invokes `on_stdout_line`/`on_stderr_line` (as record mode does), and
  keys on the **stdin content** (hashed, never persisted) so concurrent calls differing
  only in their stdin no longer collide on replay. (A pre-existing cassette recorded *with*
  stdin must be re-recorded to match a stdin invocation again.)
- `ScriptedRunner` warns (under the `tracing` feature) when a rule is unreachable because an
  earlier, broader prefix rule shadows it.

## [0.9.2] - 2026-06-11

### Added

- `Error::Stdin { program, source }` — a non-broken-pipe stdin-writer failure surfaced on an
  otherwise-successful run (see the Phase H stdin fixes below).
- `StdioMode` enum (`Piped` / `Inherit` / `Null`) + `Command::stdout(mode)` /
  `Command::stderr(mode)` builders — control per-stream connection independently.
  `Piped` (the default) captures as before; `Inherit` lets the child share the parent's
  terminal/log; `Null` suppresses output entirely without tying up a pipe.
- `OutputEvent` enum (`Stdout(String)` / `Stderr(String)`) and `OutputEvents` stream —
  merge both stdout and stderr into a single ordered sequence of tagged lines.
  `RunningProcess::output_events()` starts both pumps and returns the stream;
  `RunningProcess::finish_events()` waits for exit and returns the run's `Outcome`.
  Lines interleave in arrival order (best-effort; no kernel timestamp).
- `OverflowMode::Error` variant and `OutputBufferPolicy::fail_loud(n)` builder — a
  fail-loud capture ceiling: once `n` lines are buffered, subsequent lines are counted
  but not retained, and the consuming verb errors with `Error::OutputTooLarge` after the
  run. The pipe is still fully drained so the child never blocks on a full pipe.
  Use this when unbounded output is a misbehavior rather than a policy choice.
- `Error::OutputTooLarge { program, limit, total_lines }` — produced by the fail-loud
  overflow path when the captured line count exceeds the configured ceiling.
- `Command::stdout_tee<W: Write + Send>(writer)` / `Command::stderr_tee<W>(writer)` —
  simultaneously capture *and* write each decoded line to `writer` (a `Vec<u8>`, a
  `File`, a locked stdout — any `std::io::Write + Send`). Replaces any previously set
  per-stream handler; compose inside `on_stdout_line` when multiple sinks are needed.
- `Error::NotFound { program, searched }` — a bare program name (no path separators)
  not found now surfaces a distinct, structured error: `` `git` not found on PATH ``.
  Enriched from the OS's opaque not-found error rather than a `PATH` pre-check, so a
  program the OS resolves by another route (e.g. the application directory on Windows)
  is never falsely reported missing. `Error::is_not_found()` returns `true` for this
  variant (as it does for the existing `Error::Spawn(NotFound)` / missing-cwd case).
  The `searched` field carries the `PATH` directories for programmatic diagnostics.
- `Command::envs([(key, val), …])` — set multiple environment variables in one call.
  Equivalent to chaining `env()` calls; order is preserved and a later entry for the
  same key wins.
- `Error::Signalled { program, signal }` — a process terminated by a signal now surfaces
  a distinct, structured error (was an opaque `Error::Io`). `signal` is the Unix signal
  number when the platform reports it, `None` otherwise (e.g. on Windows). The checking
  verbs (`run`, `exit_code`, `probe`, `ensure_success`, `require_code`) raise it.
- `StreamedFinish { outcome, stderr }` — the named return of
  `RunningProcess::finish_streamed()` (was a bare `(Option<i32>, String)` tuple).
  Derives `Debug`, `Clone`, `PartialEq`, `Eq`.
- `Reply::signalled(Option<i32>)` on the test-double seam — script a signal-killed reply
  so a hermetic test can exercise `Outcome::Signalled` / `Error::Signalled` handling
  without a real subprocess.

### Changed

- **Breaking:** `Outcome::Signalled` now carries the Unix signal number as
  `Signalled(Option<i32>)` (was a unit variant). `Some(n)` is the signal that killed the
  process when the platform reports it; `None` when unavailable (e.g. on Windows).
- **Breaking:** `RunningProcess::wait()`, `wait_any()`, and `wait_all()` now return the
  run's `Outcome` (`Outcome`, `(usize, Outcome)`, and `Vec<Outcome>` respectively) instead
  of the raw `Option<i32>` exit code — distinguishing a clean exit, a signal kill, and a
  timeout instead of collapsing the last two to `None`. A cancelled run raises
  `Error::Cancelled` on every one of these paths.
- **Breaking:** `RunningProcess::finish_streamed()` returns `StreamedFinish { outcome,
  stderr }` instead of `(Option<i32>, String)`; `finish_events()` returns `Outcome`
  instead of `Option<i32>`.
- `Command::current_dir` doc now explicitly calls out that a relative-path program
  (e.g. `"./tool"`) passed to `Command::new` resolves against the *caller's* cwd, not
  the directory set here — use an absolute path for the program when combining
  `current_dir` with a relative-path executable.

### Changed (Phase I — design block)

- `ProcessGroup::spawn` now takes its `tokio::process::Command` **by value** (D8) instead of
  `&mut`: reusing one command across spawns would stack `pre_exec` hooks / re-set creation
  flags, so by-value makes that a compile error rather than a silent footgun. The crate's own
  run helpers already rebuild the command per run, so only direct `spawn` callers are affected.
- `Command::to_tokio_command` is now `#[doc(hidden)]` (D8) — it remains public and callable as
  a raw-tokio bridge to `ProcessGroup::spawn`, but is no longer advertised as 1.0 surface.
- `Invocation::cwd` is now `Option<PathBuf>` instead of `Option<OsString>` (D9) — a working
  directory is a path.
- The bulk capture verbs (`output_string`/`output_bytes`) now **error loudly** when `stdout` was
  set to `StdioMode::Inherit`/`Null` (D5) — there is no pipe to read, so returning silently-empty
  output was a footgun; the streaming verbs document that the stream is empty instead. The
  discard verbs (`wait`/`profile`) are unaffected.
- `OutputBufferPolicy::Error` overflow on an **unbounded** buffer is no longer a silent no-op (D9c):
  `unbounded().with_overflow(Error)` is a misconfiguration (a ceiling with no ceiling), so it now
  fails loud on any **line-pumped** output (`Error::OutputTooLarge`). (`output_bytes` captures stdout
  raw, so its stdout is exempt — only its line-pumped stderr trips the ceiling.) Use `fail_loud(n)`
  for a real cap.
- `Supervisor` now defaults to a **bounded-tail** capture per incarnation (D3) instead of the
  unbounded one-shot default — a long-lived chatty supervised process no longer accumulates its
  entire output in memory. An explicit bounded/`fail_loud` command policy is respected; override
  via the new `Supervisor::capture`.
- `OutputEvents` (the merged stdout+stderr stream) now alternates which stream it polls first (D9d),
  so a continuously-ready stream can't starve the other.
- `Command::first_line`'s predicate now requires `F: Send` (D6) — it delegates through the new
  `ProcessRunnerExt::first_line` seam (see Added).

### Added (Phase I — design block)

- `RunningProcess::kills_tree_on_drop()` (D10) — reports whether dropping the handle tears down
  the process tree: `true` for a private-group handle (kill-on-close leak-safety), `false` for a
  shared-`ProcessGroup` handle (the group owner tears down). Lets a receiving function reason
  about whether dropping the handle is sufficient cleanup.
- `ProcessRunnerExt::first_line` (D6) — the streaming first-matching-line search, routed through
  the `start` seam so it is exercisable with any runner (a `ScriptedRunner` in tests), not just the
  real `JobRunner`. `Command::first_line` now delegates to it.
- `Supervisor::capture(policy)` (D3) — override the per-incarnation output-capture policy (the
  default is a bounded tail; see Changed).
- Documented the deliberate design choices the block confirmed: `ProcessRunner::start` stays a
  defaulted runtime capability (`Error::Unsupported`) rather than a compile-time `ProcessStarter`
  split (D4); the `cli_client!` macro is kept and documented as committed public API (D7); and
  `Command::timeout_signal` stays behind `process-control` because the `Signal` type does — the
  divergence is accepted rather than enlarging the always-on surface (D9b).

### Fixed (Phase H — stdin)

- A stdin-writer failure is no longer silently swallowed: a non-broken-pipe error feeding the
  child's standard input now surfaces as the new `Error::Stdin { program, source }` — **but
  only when the run otherwise succeeded** (a non-zero exit, signal, or timeout is the "realer"
  failure and wins; a broken pipe, the child closing stdin early, never surfaces). Diagnoses a
  silently-truncated input the otherwise-successful child may have acted on.
- `Stdin::write_to` now releases the one-shot source mutex *before* the copy/stream (B17), so a
  concurrent second run on a cloned `Stdin` sees the consumed source and gets prompt EOF instead
  of blocking on the lock for the whole copy.
- `wait_any` / `wait_all` now close an untaken `keep_stdin_open` pipe (L5), matching the bulk
  verbs — a stdin-reading child joined via the race path sees EOF instead of blocking forever
  (the race path applies no timeout).
- Doc fixes (L12): `run` / `run_unit` document that `ok_codes` widens the accepted exit set;
  `Command::env`'s doc no longer falsely claims a `None` value removes a variable (use
  `env_remove`).

### Security (Phase G — security / hygiene)

- `Command`, `CliClient`, and `Invocation` now have a redacted `Debug`: it surfaces the
  argument *count* and the env variable *names* (sorted), never argv or env *values* — so a
  `{cmd:?}` log line or an `assert_eq!` failure can't leak a secret. `command_line()` stays
  the documented, explicitly-secret-bearing escape hatch for the real argv.
- `Error` now has a manual `Debug` (was derived): the `Exit` variant's captured streams are
  bounded to a 200-byte preview (mirroring the `Display` tail cap) so `{e:?}` / `.unwrap()`
  can't dump a multi-MiB stream, and `NotFound`'s `searched` (the `PATH` env value) is
  redacted to a directory count rather than logged. The size-bound is deliberately
  `Error`-only — the reflexive `{e:?}` / `.unwrap()` logging vector; `ProcessResult` keeps
  full streams in its `Debug` for test inspection (and its stdout/stderr are policy-verbatim
  regardless).
- Cassette (`RecordReplayRunner`) hardening: the file is written owner-only (`0600`) on Unix;
  the best-effort drop-flush is skipped while unwinding, so a panic mid-recording no longer
  persists a surprise cassette; and the docs now scope the "no secrets" guarantee to env
  *values* only — argv, cwd, stdout, and stderr are stored verbatim and may carry secrets.
- Documented the cassette's lossy-key limitation: two distinct non-UTF-8 invocations that
  differ only in their invalid bytes decode to the same match key and collide on replay
  (valid-UTF-8 invocations never collide).

### Fixed (Phase F — group / limits / sys layer)

- Linux cgroup resource limits (B13): made the `cgroup.subtree_control` controller-enable
  conditional (it now writes only the controllers not *already* enabled, skipping a redundant
  write) and corrected the previously **misleading** error/docs. The honest story: the crate
  creates the limit cgroup as a child of this process's own cgroup and enables the controllers
  there, which cgroup v2's "no internal processes" rule permits only at the **real cgroup-v2
  hierarchy root** (the one exempt cgroup) — so limits apply only when this process is a direct
  member of that real root, and fail fast (`Error::ResourceLimit`) under a systemd
  session/scope/service or an ordinary (private-cgroupns) container, both of which place it in a
  non-root cgroup. A cgroup *namespace* root does **not** count. The crate deliberately does not
  migrate your process into a sub-cgroup to work around the rule. (The previous error/docs
  recommended `Delegate=yes` / `systemd-run --scope` and a "delegated leaf", which all still
  `EBUSY` — that advice is removed.) Docs (`ResourceLimits`, README, platform-support,
  process-groups) corrected to match.
- Documented the Linux `max_processes` cross-platform divergence (B14): the kernel checks
  `pids.max` only for forks *inside* the cgroup, so on Linux the cap bounds a contained tree's own
  forks but does not reject additional `ProcessGroup::start` calls that each add a top-level child
  (Windows' `ActiveProcessLimit` does). `ResourceLimits::max_processes` now spells this out.
- Documented the POSIX process-group graceful-shutdown zombie caveat (B16): on the
  `ProcessGroup` mechanism (macOS/BSD, Linux fallback) an unreaped zombie still answers the
  liveness probe, so `ProcessGroup::shutdown` burns the full `shutdown_timeout` on a child that
  exited on `SIGTERM` but whose handle was never awaited — await each child you start into the
  group. The Job Object / cgroup mechanisms are immune.
- `ProcessGroup::shutdown` with `escalate_to_kill(false)` now actually preserves survivors:
  the `Drop` impls for all three backends (Linux cgroup, POSIX process-group, Windows Job
  Object) no longer hard-kill the tree when `graceful_shutdown` was invoked with
  `escalate=false`. Previously, the per-platform `Drop` backstop unconditionally killed
  regardless of the escalation setting. (The run-level `timeout_grace` path always escalates,
  so it is unaffected.)
- Fixed a provenance UB in the Windows `job_member_pids` helper: the flexible-array
  `ProcessIdList` field in `JOBOBJECT_BASIC_PROCESS_ID_LIST` is now addressed via
  `std::ptr::addr_of!((*list).ProcessIdList[0])` instead of `.as_ptr()` on the `[ULONG_PTR;1]`
  field, which previously created a reference with incorrect provenance over the out-of-bounds
  elements.
- `ProcessGroupStats::total_cpu_time` doc now explains the semantic divergence: the Windows
  Job Object accumulates CPU time historically (including terminated processes), while the
  Linux cgroup path sums only currently-live processes' `/proc` counters.
- POSIX process-group `exists()` probe no longer permanently prunes a just-spawned pid
  whose process group does not yet exist: `ESRCH` on the negative group-id probe now falls
  back to a direct pid probe, so a child between fork and its `setpgid(0,0)` call is not
  incorrectly evicted from the tracking set. The teardown sweep mirrors this — when
  `killpg` finds no group it falls back to a direct pid signal, so such an entry is actually
  delivered to and drains instead of being retained-but-never-signalled (which would have
  stalled `shutdown` to its full timeout).

### Fixed

- `ProcessResult::combined()` now inserts a `\n` separator between stdout and stderr when
  stdout is non-empty and does not already end with a newline, preventing the last stdout
  line from being glued to the first stderr line.
- Pipeline `pipefail` attribution now honors per-stage `ok_codes`: an inner stage that
  exits with a code in its `ok_codes` set is considered clean and does not trigger
  attribution, instead of checking only for `Exited(0)`.
- Pipeline `pipefail` now attributes to the first **non-SIGPIPE** checked failure rather
  than the first checked failure of any kind. A SIGPIPE-killed upstream stage is typically
  a victim of a downstream failure; the downstream culprit is now correctly attributed.
  When all failures are SIGPIPE, the leftmost is still attributed as before.
- Pipeline `pipefail` now preserves the real exit code of an `unchecked()` last stage
  instead of fabricating `Exited(0)`. `is_success()` remains `true` and `ensure_success()`
  still passes; `code()` now returns the actual exit code for callers that inspect it.
- `Error::NotFound` `Display` no longer includes the raw `PATH` environment value
  (e.g. `searched: /usr/bin:/usr/local/bin`). The `searched` field remains accessible for
  programmatic use. `PATH` is an environment value and must not appear in logs.
- When a bare program name is on `PATH` but the OS cannot execute it directly (e.g. a
  `.cmd`/`.bat` script on Windows that requires `cmd.exe`), the error is now the raw
  `Error::Spawn` rather than the misleading `Error::NotFound` — the program was found.
- `is_bare_name("git/")` now correctly returns `false`; a trailing path separator makes
  a name path-ish and it should not be looked up on `PATH` as a bare name.
- Windows `command_line()` display: a path argument ending with a backslash (e.g.
  `C:\my tools\`) now doubles the trailing backslash before the closing `"` so it does
  not escape the closing quote (was: `"C:\my tools\"`, now: `"C:\my tools\\"`).
- A signal-killed process is no longer reported as a generic `Error::Io("terminated by
  signal")`; the checking verbs now raise the structured `Error::Signalled` (carrying the
  signal number on Unix), and `Outcome::Signalled` preserves it for inspection.
- `finish_streamed` and `finish_events` previously drained an untaken stdout pipe into an
  unbounded `Vec`, bypassing any configured `OutputBufferPolicy`. They now route the pipe
  through the normal pumping path, respecting the buffer policy (including `fail_loud`).
- `wait` and `profile` previously accumulated all output in the user-configured buffer even
  though output is discarded on those paths, causing O(total-lines) peak heap use. Both now
  use a retain-nothing sink that keeps the pipe drained without buffering any lines.
  **Behavior note:** `OverflowMode::Error` (via `fail_loud`) no longer fires during `wait`
  or `profile` — it fires only on the capturing verbs (`output_string`, `output_bytes`,
  `finish_streamed`, `finish_events`). If you need the DoS guard on a run you don't capture,
  use a capturing verb.
- `output_string` / `output_bytes` called after `stdout_lines` previously returned empty
  output because they created fresh empty sinks and ignored the running streaming pump.
  They now reuse the existing pump's sink and join its handle, capturing all buffered and
  in-flight output correctly.
- Calling `stdout_lines` or `output_events` a second time on the same `RunningProcess` now
  returns an empty stream instead of silently replacing the first call's sink reference,
  which previously caused the overflow flag to be lost.
- A second `output_events` call no longer shares the same stderr `SharedLines` as the first;
  it receives a fresh already-closed sink, preventing a `notify_one` race that could leave
  the first consumer's internal task permanently parked.
- Pump task handles previously held in a frame-local `Vec` were leaked (left as detached
  tasks) if an early `?` exit occurred between the pump spawns and the explicit join. Handles
  are now stored on `RunningProcess` fields and aborted by `Drop`, bounding the leak to
  the process handle's lifetime.

## [0.9.1] - 2026-06-09

### Added

- `Command::ok_codes([..])` — treat the given exit codes (not just `0`) as success for
  the checking verbs (`run`/`run_unit` and `ProcessResult::is_success`/`ensure_success`),
  for tools whose non-zero exit is a normal result — `grep` (1 = no match), `diff`
  (1 = differs), rsync's code families. `exit_code` (raw code) and `probe` (0/1
  convention) are unchanged; an empty set is ignored.
- `ProcessResult::duration()` — the run's wall-clock time (spawn → exit/kill), carried
  on the result instead of making callers wrap each run in their own `Instant::now()`.
  `Duration::ZERO` for synthetic results (scripted/replayed bulk `output`).
- `ProcessResult::truncated()` — whether a bounded `OutputBufferPolicy` dropped captured
  output lines, so a caller that bounds the buffer can tell when output was lost
  (the unbounded default never truncates).
- `Command::command_line()` — render the command as a single shell-quoted line for
  logs, error messages, or a dry-run echo (per-platform quoting; **display only** —
  the crate never invokes a shell). It includes argv (which may carry secrets), so —
  unlike the `tracing` feature, which never logs argv — it is opt-in.
- A `current_dir` that does not exist now fails with a clear *"working directory does
  not exist"* error (`Error::is_not_found()` is `true`) instead of the opaque `ENOENT`
  that looked like the program itself was missing.
- `Command::timeout_grace(Duration)` + `Command::timeout_signal(Signal)` — a **graceful
  run-level timeout**: at the deadline the tree is signalled (`SIGTERM` by default, or
  the chosen signal), given up to the grace window to exit, then `SIGKILL`ed — instead
  of the immediate hard kill. Reuses the `ProcessGroup::shutdown` tier and reaps
  concurrently, so a signal-handling child ends the grace early. Applies to bulk and
  streaming runs, own- and shared-group; `timed_out()` stays `true`. Windows has no
  signal tier (atomic kill at the deadline). `timeout_signal` needs `process-control`.

### Changed

- **Breaking:** `RestartPolicy`, `OverflowMode`, `OutputBufferPolicy`, `ResourceLimits`,
  and `ProcessGroupOptions` are now `#[non_exhaustive]` — they may gain variants/fields
  later without another breaking change. Build the structs via their
  constructors/builders (`ProcessGroupOptions::default()`, `OutputBufferPolicy::bounded(..)`,
  …) instead of struct literals.
- `ProcessGroupOptions::shutdown_timeout(Duration)` / `escalate_to_kill(bool)` builders —
  the grace-window fields now have builders, matching the `limits` knobs.

### Fixed

- `Error::Exit` now carries the **full** captured `stdout`/`stderr` instead of truncating
  each to 4 KiB. Truncation happened before the caller could classify on the streams
  (grep for a marker, parse a sub-code), silently destroying the data they needed. The
  one-line `Display` message is still bounded, so logs stay tidy — only the fields grew.

## [0.9.0] - 2026-06-08

### Added

- `Error::is_not_found()` / `is_permission_denied()` / `is_transient()` — io-level
  classifiers over the `Spawn`/`Io` error: distinguish a missing binary (`ENOENT`),
  a permission denial (`EACCES`/`EPERM`), and a transient condition a bare retry can
  clear (`EINTR`/`EAGAIN`/busy, `ETXTBSY`, Windows sharing/lock violation) without
  matching raw `io::ErrorKind`. Pairs with `Command::retry(.., |e| e.is_transient())`.
  Scope is io/spawn-level only — exit-code retryability stays the caller's domain,
  and `Error::Timeout` is excluded (compose it explicitly if wanted).
- `Command::groups([gid, ..])` — set the child's supplementary groups (Unix
  privilege drop), the missing third leg beside `uid`/`gid`: dropping the uid alone
  leaves the child holding the parent's (often root's) supplementary groups. The OS
  applies `setgroups → setgid → setuid`. POSIX-only — non-Unix fails with
  `Error::Unsupported`, never a silent skip.

### Changed
-

### Fixed
-

## [0.8.2] - 2026-06-08

### Added

- `wait_all(&mut [&mut RunningProcess])` — the join companion to `wait_any`:
  drives every handle to exit and returns the exit codes in input order (an
  empty slice resolves to an empty `Vec`). Cancel-safe and borrow-only, like
  `wait_any`.
- `output_all(commands, concurrency, runner)` — run a batch of commands with a
  concurrency cap, collecting every `Result<ProcessResult<String>>` in input
  order (collect-all: a non-zero exit is data, never a short-circuit). The
  back-pressure the one-shot verbs lack when fanning out many commands. Pass
  `&group` to share one kill-on-drop group, or `&JobRunner` for private groups.
  Not a pool/scheduler/retrier by design.

### Changed
-

### Fixed
-

## [0.8.1] - 2026-06-08

### Fixed

- fix(readme): use direct raw.githubusercontent URL for cover so crates.io stops generating a CSP-blocked github.com/raw redirect

## [0.8.0] - 2026-06-07

### Added

- `ProcessRunner::start` — the live-handle half of a run joins the seam (with
  an `Error::Unsupported` default, so `output`-only runners keep compiling).
  `ScriptedRunner::start` returns a **scripted `RunningProcess`** whose canned
  output flows through the same pump machinery as a real child: streaming
  (`stdout_lines`), readiness probes, and `finish_streamed` are now
  hermetically testable. `Reply::lines([...])` scripts the lines;
  `Reply::with_line_delay(d)` paces them (paused-clock friendly);
  `RecordingRunner` records `start` invocations. Scripted handles have no pid,
  don't compose into a real `Pipeline`, and don't model interactive stdin
  (documented). Cassette record/replay does not yet cover streaming runs.
- `ScriptedRunner::output` now replays canned stdout/stderr through the
  command's `on_stdout_line`/`on_stderr_line` handlers, so progress-reporting
  wrappers test hermetically (requested by a downstream wrapper crate's
  streaming spec).
- `ProcessRunnerExt::run_unit` — run for the side effect, require a zero
  exit, discard the output (the verb `CliClient::run_unit` delegates to).
- More `tracing` events (behind the `tracing` feature, `processkit` target):
  child spawn (program/pid/mechanism), timeout and cancellation firing, group
  terminate/shutdown, retry attempts, stdin-writer failures, output-pump
  panics and teardown overruns, and `adopt`. Still never logs argv or
  environment values.
- `ProcessResult::outcome() -> Outcome` — how the run ended as an explicit
  `Exited(i32) | Signalled | TimedOut` enum, now the internal representation
  behind the `code()`/`timed_out()`/`is_success()` accessors (which are
  unchanged, derived, and remain the everyday surface). `Outcome` is
  `#[non_exhaustive]`. Cassette wire format is untouched.
- `CliClient::default_cancel_on(token)` (`cancellation` feature) — a
  client-level cancellation default, completing the run-control default set
  (`default_timeout`/`default_env`): every command the client builds carries
  the token, so cancelling it kills all of that client's in-flight runs. A
  per-command `cancel_on` *replaces* the default (explicit beats default).
  The `cli_client!` macro re-emits the builder on generated wrappers.
  Requested by a downstream wrapper crate.
- `Reply::pending()` (`cancellation` feature) — a `ScriptedRunner` reply that
  parks the call until the command's cancellation token fires, then resolves
  with `Error::Cancelled`, making cancellation *behaviour* (not just its
  aftermath) hermetically testable. With no token it parks forever, like a
  hung child.
- `Command::kill_on_parent_death()` — opt-in hardening so an abruptly-dying
  parent (`SIGKILL`, where `Drop` never runs) still takes its child down:
  Linux arms `PR_SET_PDEATHSIG(SIGKILL)` on the direct child (the
  parent-died-first race is closed by re-checking `getppid` against the
  spawner's pre-fork pid — PID-1-entrypoint-safe); Windows already
  guarantees the whole tree via the job handle closing; macOS/BSD have no
  equivalent (documented no-op). Idea borrowed from `execa`'s
  cleanup-on-exit, mapped to native primitives.
- `Command::unchecked()` — exempt a pipeline stage from pipefail attribution
  (design borrowed from `duct`): its unclean exit (non-zero, signal kill
  including SIGPIPE, or its per-stage-timeout kill) is skipped when blaming
  the chain, fixing the `producer | head -1` false failure. Checked failures
  always trump unchecked ones; a chain whose only failures are unchecked
  reports success. No-op outside a pipeline; never relaxes a whole-chain
  `Pipeline::timeout`.
- `|` operator on `Command`/`Pipeline` — `a | b | c` is sugar for
  `a.pipe(b).pipe(c)`: the same shell-free, one-group, pipefail pipeline.
  Parenthesize the chain before a terminal verb.
- `Supervisor::storm_pause` / `failure_decay` / `failure_threshold` — an
  opt-in failure-storm guard (design borrowed from Go's `suture`): each
  failure feeds a score that halves every `failure_decay`
  (`score = score × 0.5^(Δt/decay) + 1`); past `failure_threshold` the
  supervisor takes one jittered `storm_pause` and resets the score,
  distinguishing "fails rarely" from "crash-looping". Off by default;
  pauses taken are reported in `SupervisionOutcome::storm_pauses`.

### Changed

- A **panicking line handler no longer poisons the run**: the panic is caught,
  the handler is disabled for the rest of the run (surfaced as a `tracing`
  warn), and the child keeps being drained — the final result still carries
  every line. Previously the pump died with the panic and capture was cut at
  that point. The `on_stdout_line`/`on_stderr_line` docs now also state the
  ordering guarantees: FIFO within a stream, no cross-stream order, and all
  handler calls happen-before the consuming verb resolves (requested by a
  downstream wrapper crate's streaming spec).
- **Breaking**: `CliClient`'s run helpers renamed to the crate-wide verb
  vocabulary — `text → run`, `capture → output`, `unit → run_unit`,
  `code → exit_code` (`probe`/`parse`/`try_parse` unchanged). The same verb
  now means the same thing on `Command`, `ProcessRunnerExt`, and `CliClient`;
  `ProcessRunnerExt` gained `run_unit` for full symmetry. No deprecated
  aliases (pre-1.0). `ProcessResult::code()` — the plain accessor — is
  unrelated and unchanged.
- `Error::Exit`'s `Display` now appends a bounded diagnostic excerpt — the
  last non-empty line of stderr (or stdout as fallback), capped at 200
  bytes: `` `git` exited with code 2: fatal: boom `` (idea borrowed from
  `execa`'s error messages). Display text is not part of the semver
  contract; the carried `stdout`/`stderr` fields and `diagnostic()` are
  unchanged.
- `SupervisionOutcome` is now `#[non_exhaustive]` (it gained the
  `storm_pauses` field; like `ProcessGroupStats`/`RunProfile` it is a
  read-only report the crate produces, so future telemetry can be added
  without another breaking change). **Breaking** for exhaustive
  destructuring or struct-literal construction outside the crate.

### Fixed

- `keep_stdin_open` combined with a **bulk** verb (`output_string`/`run`/…)
  no longer hangs a stdin-reading child: a consuming verb now closes an
  **untaken** interactive stdin pipe (nothing could ever write to it again),
  so the child sees EOF instead of blocking to its timeout. A writer taken
  via `standard_input()` is unaffected. The `keep_stdin_open` docs previously
  claimed bulk helpers "always close stdin" — now they actually do.

## [0.7.1] - 2026-06-06

### Fixed

- fix: repair main after the v0.7.0 release commit was dropped (manifest, changelog, release guard)


### Added

- Add cover art to the project overview

## [0.6.2] - 2026-06-06 [YANKED]

- **Yanked on crates.io — use 0.7.0.** A force-push had dropped the
  `Release v0.7.0` commit from `main` before this patch release ran, so the
  release workflow computed the next version from the stale `0.6.1` manifest
  and published the **entire 0.7.0 content below under a `^0.6`-compatible
  patch version** — including the changes that are breaking for
  `default-features = false` consumers. The `v0.6.2` tag and GitHub Release
  remain for the record; the crates.io version is yanked. (The release
  workflow now refuses to run when the manifest is behind the latest release
  tag, so this failure mode is caught before publishing.)

## [0.7.0] - 2026-06-06

> **Release note:** this cycle contains a **breaking** change for
> `default-features = false` consumers (resource measurement moved behind the
> now-default `stats` feature — see *Changed*).

### Changed
- The tree-control surface is now behind a **default-on** `process-control`
  feature: `Signal` and
  `ProcessGroup::{signal, suspend, resume, members, adopt}`. The flag is
  additive and gates *visibility only* — the kill-on-drop tree guarantee
  (and `terminate_all`/`shutdown`) is unconditional in every configuration.
  **Migration note** for `default-features = false` consumers: previously
  that disabled only `stats`; now it also hides the surface above —
  re-enable it explicitly. (A broader visibility split — gating
  pipelines/supervisor/CliClient/test doubles too — was implemented and
  deliberately rolled back: those gates removed no dependencies while
  costing cfg noise and doc quality; see `ideas/three-layer-resource-split.md`
  for the full decision record.)
- `windows-sys` bumped 0.59 → 0.61 to dedup with the copy tokio/mio already
  ship — the lockfile now carries a single `windows-sys`.
- Every public type now implements `Debug` (enforced by a crate lint), and
  `Command` is `#[must_use]` — building one and dropping it unused now warns.
- Resource measurement (`ProcessGroupStats`, `ProcessGroup::stats`,
  `RunningProcess::cpu_time`/`peak_memory_bytes`) now sits behind a default-on
  `stats` Cargo feature: `default-features = false` compiles the accounting code
  (and its Windows ProcessStatus FFI) out. Consumers on default features see no
  change; consumers who already set `default-features = false` must add
  `features = ["stats"]` to keep that API.
- `ProcessGroupStats` and `RunProfile` are now `#[non_exhaustive]`: they are
  read-only outputs the crate produces, so future metrics can be added without
  a breaking change. Reading fields is unaffected; struct-literal construction
  and exhaustive destructuring outside the crate no longer compile.
  (`ProcessGroupOptions`, `ResourceLimits`, and `Invocation` deliberately stay
  exhaustive — constructing them is their intended use.)

### Fixed
- POSIX process-group liveness probes treated `EPERM` as "process gone": a
  live tree whose members the caller may no longer signal (e.g. after a
  third-party uid change) was silently pruned from tracking — and therefore
  never killed on drop. Probes now distinguish `ESRCH` (gone — prune) from
  `EPERM` (exists — keep and still attempt the best-effort signal).
- `output_bytes` awaited an **unbounded** raw stdout drain: on a shared-group
  handle whose timeout/cancel kills only the direct child, a surviving
  descendant holding the pipe could park the call forever. The drain is now
  bounded by the same pump-teardown grace as every other consumer, aborting
  the straggler and returning the partial bytes read so far.
- The streaming deadline/cancel watchdog tasks are now stopped as soon as the
  child's fate is settled (not only on handle drop), closing a narrow window
  where a late firing could signal an already-reaped pid.
- POSIX process-group `ProcessGroup::adopt` was a silent no-op for any child
  that had already `exec`'d (the normal case): POSIX refuses `setpgid` there
  (`EACCES`), and the pid was recorded as a process-*group* id that doesn't
  exist, so teardown never reached the child. Such children are now tracked
  and signalled individually — the adopted child is contained (killed with the
  group), though its future forks are not (unlike Windows/cgroup adoption).
  Adopting a child the group already tracks (a self-spawned leader, or a
  repeated adopt) is also de-duplicated now, so `members()`/`stats()` no
  longer over-report or grow per call.
- The streaming deadline/cancellation kill paths now also kill the **direct
  child by pid** after the group teardown — parity with the run-to-completion
  path's `start_kill` + `terminate_all` pairing, so a group-kill miss on the
  direct child can't leave a bounded stream running. Safe against pid reuse:
  the tasks are aborted when the handle drops, so they can only fire while
  the child is live or an unreaped zombie (its pid still held). (Note: this
  cannot rescue a *grandchild* forked mid-broadcast — the POSIX group
  broadcast is documented best-effort against a forking tree, which is what
  one macOS CI run actually hit.)

### Added
- `ProcessResult::program()` — the program a result is attributed to (for a
  `Pipeline` outcome, the pipefail-attributed stage). Previously the name was
  only recoverable by failing the result and matching the error.
- `docs/` guide set — eight cross-linked, per-topic guides (running commands,
  process groups, streaming & interactive I/O, pipelines, timeouts/retries/
  cancellation, supervision, testing, platform support) with richer examples
  and all capability matrices and platform caveats collected in one place;
  linked from the README's new Documentation section.
- Record/replay cassettes (`record` feature, off by default, pulls optional
  `serde` + `serde_json`): `RecordReplayRunner::record(path, inner)` captures
  real `Invocation → ProcessResult` pairs through any inner runner and writes
  a human-diffable JSON cassette (`save()`, or best-effort on drop);
  `RecordReplayRunner::replay(path)` serves them back hermetically — no
  subprocess. Matching is by program + args + cwd + has-stdin; env override
  values are never written (sorted names only — a committed fixture can't
  leak secrets) and env is not part of the match key. Duplicates of one
  invocation replay in capture order, then the last entry repeats. A miss in
  replay is a strict `Error::Spawn` (NotFound) — replay never spawns. The
  cassette carries a format `version` for forward evolution; non-UTF-8
  program/args/cwd are stored lossily (documented).
- Cancellation (`cancellation` feature, off by default, pulls optional
  `tokio-util`): `Command::cancel_on(token)` ties a run to a re-exported
  `CancellationToken` — cancelling it kills the process tree and every
  consuming path (`run`/`output_string`/`output_bytes`/`wait`/`profile`/
  `finish_streamed`) reports the new `Error::Cancelled`. Asymmetric with
  timeout by design: a timeout is *captured* in the result (`timed_out`), a
  cancellation is always an error; when both land, cancellation wins. A token
  cancelled before launch short-circuits without spawning. On a shared
  `ProcessGroup` handle, cancel kills the child only — siblings are untouched
  (same scope as timeout). A `stdout_lines` stream ends on cancel (own-group
  runs); the raw `wait_any`/`first_line` primitives don't synthesize the error
  for a mid-run cancel. A cancelled run is never re-attempted: `retry` policies
  and `Supervisor` restarts both treat it as terminal — no retry into a
  still-cancelled token.
- Environment and privilege builders on `Command`: `inherit_env([names])`
  (allow-list on a cleared environment, copied from the parent at each spawn;
  explicit `env`/`env_remove` still win), `uid(u32)`/`gid(u32)` (Unix privilege
  drop; gid applied before uid; on the Linux cgroup mechanism the spawn
  currently fails with a permission error — the cgroup join runs after the
  drop — while the process-group mechanism composes cleanly), `setsid()`
  (Unix new session — containment is
  preserved, the group tracks the new session's process group), and
  `create_no_window()` (Windows `CREATE_NO_WINDOW`, now OR'd with the group's
  `CREATE_SUSPENDED` on the Command-driven launch paths instead of being
  clobbered; harmless no-op elsewhere). On non-Unix targets `uid`/`gid`/
  `setsid` fail the run with `Error::Unsupported` — a requested privilege drop
  is never silently skipped.
- Shell-free pipelines: `Command::pipe(next)` starts a `Pipeline` (extend with
  `.pipe(...)`, bound with `.timeout(...)`, drive with `output_string()` /
  `run()`). Stages connect stdout→stdin through native pipes — no shell, no
  quoting/injection surface — and all run inside one shared kill-on-drop group.
  Pipefail outcome: stdout is the last stage's, while code/stderr/program are
  attributed to the first stage that didn't exit cleanly; `run()` requires
  every stage to succeed.
- Readiness probes on `RunningProcess` — wait until a started child is
  actually ready instead of sleeping: `wait_for_line(predicate, within)`
  (stream stdout until a line matches, returning it; consumes stdout up to the
  match), `wait_for_port(addr, within)` (until a TCP connect is accepted), and
  `wait_for(check, within)` (until any async predicate passes; ~50 ms cadence).
  All three fail with the new `Error::NotReady` when the deadline elapses — or
  immediately once readiness can no longer happen (the child exits; for
  `wait_for_line`, its stdout closes) — and never kill the child (a probe
  deadline is separate from `Command::timeout`).
- `Supervisor` — keep a child alive: restart per `RestartPolicy`
  (`Always`/`OnCrash`/`Never`, where a crash is any run without a clean exit —
  non-zero, timeout, signal, or spawn failure), bounded by `max_restarts`, with
  exponential backoff (`backoff(base, factor)`, capped by `max_backoff`,
  jittered ×[0.5, 1.5) by default — `jitter(false)` for determinism) and a
  `stop_when` predicate that ends supervision regardless of policy. `run()`
  reports a `SupervisionOutcome` (final result, restart count, `StopReason`).
  Platform-agnostic, built on the `ProcessRunner` seam: `with_runner(&group)`
  supervises inside one shared kill-on-drop group; doubles make it hermetic.
- Stats sampling over time (`stats` feature): `ProcessGroup::sample_stats(every)`
  yields a `Stream` of `ProcessGroupStats` snapshots (first sample immediate,
  missed ticks skipped, a zero interval clamped to 1 ms, series ends when the
  group can no longer report), and
  `RunningProcess::profile(every)` runs a child to completion while sampling it,
  returning a `RunProfile` summary (exit code, wall duration, last CPU reading,
  peak RSS, sample count, derived `avg_cpu()`).
- Tree inspection: `ProcessGroup::members()` snapshots the live member pids
  (whole tree via the Windows Job Object pid list / Linux `cgroup.procs`;
  tracked group leaders only on the POSIX process-group backends; always empty
  with no containment), and a free `wait_any` races several `RunningProcess`es
  and returns the index + exit code of whichever exits first — contenders are
  only borrowed (the race is cancel-safe), so losers stay fully usable.
- Whole-tree signals and suspend/resume: `ProcessGroup::signal(Signal)` broadcasts
  a signal to every member (new `Signal` enum — `Term`/`Kill`/`Int`/`Hup`/`Quit`/
  `Usr1`/`Usr2` plus an `Other(i32)` escape hatch), and
  `ProcessGroup::suspend`/`resume` freeze and thaw the tree. Per backend: Linux
  cgroup uses a single whole-subtree `cgroup.freeze` write (falling back to
  per-process `SIGSTOP`/`SIGCONT` on kernels without it), the POSIX process-group
  backends
  broadcast to each group, and Windows suspends/resumes every member thread
  (best-effort; suspend counts nest; the walks are mutually exclusive with a
  concurrent `spawn`'s assign-and-resume, so a mid-spawn child can't be
  stranded suspended). On Windows only `Signal::Kill` is
  deliverable (Job Object terminate); any other signal — and these operations on
  the no-containment target — return the new typed `Error::Unsupported`.
- `ProcessGroupOptions` resource limits (behind the new, off-by-default `limits`
  Cargo feature) — `memory_max`, `max_processes`, and `cpu_quota` cap a group's
  whole tree at creation, plus a public `limits:
  ResourceLimits` field. Enforced by the Windows Job Object (job memory limit,
  active-process limit, hard CPU-rate cap) and Linux cgroup v2 (`memory.max` /
  `pids.max` / `cpu.max`, enabling the matching controllers). `cpu_quota` is a
  fraction of one core (`0.5` = half a core); on Windows it is converted against the
  host CPU count and is approximate. Where no real container exists (macOS/BSD, the
  Linux process-group fallback, the no-containment target) — or a Linux cgroup lacks
  controller delegation — `ProcessGroup::with_options` fails fast with the new
  `Error::ResourceLimit` rather than handing back an unbounded group.

## [0.6.1] - 2026-06-03

### Added
-

### Changed
- Move the Testing and Releasing guides out of `README.md` into a dedicated
  `CONTRIBUTING.md`, keeping the README focused on usage.

### Fixed
-

## [0.6.0] - 2026-06-03

### Added
- `probe` — run a predicate command and read its exit code as a `bool`: exit `0` →
  `Ok(true)`, exit `1` → `Ok(false)`, anything else → `Err` (other code / timeout /
  signal-kill). On `Command`, `CliClient`, and `ProcessRunnerExt`. Collapses the
  `match code { 0 => …, 1 => …, _ => Err }` idiom (`git diff --quiet`, `grep -q`, …).
- `Command::retry(max_attempts, backoff, retry_if)` — replay the run while
  `retry_if(&Error)` accepts the failure, with fixed backoff. Honored by the
  success-checking helpers (`run`/`exit_code`/`probe` and the `CliClient`
  `text`/`unit`/`code`/`parse`/`try_parse` helpers); the non-erroring `output_string`/
  `output_bytes`/`capture` paths don't retry. One-shot stdin sources can't replay.

### Changed
- `RunningProcess::stdout_lines` now honors the command's `timeout`: at the deadline
  the process tree is killed and the stream ends, so a streamed run can no longer hang
  past its timeout (`finish_streamed` then reports the kill — `code` is `None` on a Unix
  signal-kill, a platform code on a Windows Job kill). Previously the timeout applied
  only to the run-to-completion helpers.

### Fixed
- Linux (cgroup backend): `Drop` no longer leaks the cgroup directory. `cgroup.kill`
  is asynchronous, so the immediate `rmdir` used to race the still-draining members
  and fail with `EBUSY`; `Drop` now waits (bounded) for the subtree to drain first.
- Linux (cgroup backend, pre-5.14 kernels): the per-pid SIGKILL fallback no longer
  busy-spins — it sleeps briefly between sweeps.
- Streaming: a panicking `on_stdout_line` / `on_stderr_line` handler no longer hangs a
  `stdout_lines` consumer. The pump now closes its sink on any exit (including a panic
  unwind), so the stream always ends instead of parking forever.
- Streaming: a second `stdout_lines()` call no longer silently discards the first call's
  stderr (it previously overwrote the stderr sink, so `finish_streamed` returned empty).
- Test double: `Reply::timeout()` now reports the command's real configured deadline in
  `Error::Timeout` (it previously surfaced a zero duration, diverging from the live runner).

## [0.5.2] - 2026-06-03

### Changed

- ci(release): push the release commit via a GitHub App token (App bypasses branch protection; no PAT expiry); attribute commit to owner (#1)

## [0.5.1] - 2026-06-02

### Added
-

### Changed
- `Error::diagnostic()` and `ProcessResult::diagnostic()` now return the message
  trimmed of surrounding whitespace (the trailing newline a tool leaves on its
  output is noise for a human-facing message). For the raw streams, match
  `Error::Exit`'s fields or use `ProcessResult::stdout`/`stderr`.

### Fixed
-

## [0.5.0] - 2026-06-02

### Added
- `Error::Exit` now carries `stdout` alongside `stderr` (each truncated to 4 KiB),
  so a failed `git`/`jj` run's stdout diagnostics (`CONFLICT (content): …`,
  `nothing to commit, working tree clean`) survive the typed error instead of
  being dropped.
- `Error::diagnostic()` and `ProcessResult::diagnostic()` — the best human message
  for a failed run: standard error if it has text, otherwise standard output.
- `CliClient::default_env` / `default_env_remove` (and matching `cli_client!`
  macro methods): set an environment variable on every command the client builds
  (e.g. `GIT_TERMINAL_PROMPT=0`) instead of repeating it per call.

### Changed
- `ProcessResult::exit_code() -> i32` is replaced by `code() -> Option<i32>`:
  a run that yields no code (killed by its timeout, or by a signal on Unix) is
  `None` — the synthetic `-1` sentinel is gone. `RunningProcess::wait` and
  `finish_streamed` likewise return `Option<i32>`. The `exit_code` convenience
  helpers (`Command`/`ProcessRunnerExt`/`CliClient`) still return `Result<i32>`,
  now surfacing a signal-kill as an IO error rather than `-1`.
- `CliClient::text` trims trailing whitespace only (`trim_end`), matching
  `run` — previously it trimmed both ends.

### Fixed
- Windows: closed the spawn→assign race in the kill-on-close guarantee. A child
  is now created `CREATE_SUSPENDED`, assigned to the Job Object, then resumed, so
  a fast-forking child can no longer escape containment in the window between
  spawn and assignment.

## [0.4.1] - 2026-06-02

### Changed

- review: harden macOS/BSD process-group containment

## [0.4.0] - 2026-06-01

### Added
- macOS and the BSDs now contain process trees with a POSIX process group
  (`killpg` on drop) instead of a plain, uncontained spawn — `mechanism()`
  reports `ProcessGroup` there rather than `None`. The shared backend is the same
  one Linux already uses when no cgroup is writable.

### Changed
-

### Fixed
-

## [0.3.4] - 2026-06-01

### Changed

- Release: reject dispatch from any ref other than main
- Stop tracking agent-instruction files (AGENTS.md, CLAUDE.md, .claude/) — keep them local only

## [0.3.3] - 2026-06-01

### Changed

- Release: always target main (check out + push main regardless of the dispatch ref)

## [0.3.2] - 2026-06-01

### Changed

- Release: publish to crates.io before tagging + retry/idempotent publish & GitHub Release, --locked

## [0.3.1] - 2026-06-01

### Added
- Async stdin/stdout usage examples on `RunningProcess::standard_input` and
  `RunningProcess::stdout_lines`, plus a `StreamExt` re-export so callers can
  consume the `stdout_lines` stream with `use processkit::StreamExt;` (no direct
  `tokio-stream` dependency).

### Changed
-

### Fixed
- `Command::first_line` now honors the command's `timeout` while streaming. It
  previously enforced the deadline only on the run-to-completion path, so a
  command that produced no matching line (e.g. a silent long-running process)
  could hang forever; it now returns `Error::Timeout` once the deadline elapses.

## [0.3.0] - 2026-06-01

### Changed
- **Timeouts are now a first-class `Error::Timeout`** on the success-checking
  helpers. `ProcessResult::ensure_success` (hence `ProcessRunnerExt::run`/`checked`,
  `CliClient::text`/`unit`/`parse`/`try_parse`, and `Command::run`) and
  `ProcessRunnerExt::exit_code` / `CliClient::code` / `Command::exit_code` now return
  `Error::Timeout` for a run killed by its deadline, instead of folding it into
  `Error::Exit { code: -1 }` / a synthetic `-1`.
  `capture`/`output` still expose the inspectable `ProcessResult::timed_out()`
  without erroring. **Breaking:** a timeout that previously surfaced as `Error::Exit`
  is now `Error::Timeout` (the variant was formerly unreachable).

### Added
- `Reply::timeout()` — a canned `ScriptedRunner` reply that drives the timeout
  path, so tests can assert that a command exceeding its deadline surfaces as
  `Error::Timeout`.

## [0.2.0] - 2026-06-01

### Changed
- Release workflow: pick the version bump from a menu, with auto-increment.
  (Release tooling only — no changes to the published library.)

## [0.1.2] - 2026-05-31

_No functional changes — republished to recover a failed crates.io upload; the
first version to actually reach crates.io._

## [0.1.1] - 2026-05-31

_No functional changes — republished to recover a failed crates.io upload._

## [0.1.0] - 2026-05-31

### Added
- `ProcessGroup` — a kill-on-drop container for a child-process tree, backed by
  Windows Job Objects, Linux cgroup v2 (with a POSIX process-group fallback), or
  no containment elsewhere. Async `shutdown` performs a graceful
  SIGTERM → wait → SIGKILL teardown on Unix; the mechanism in effect is
  observable via `Mechanism`.
- `Command` builder and async run-and-capture helpers: `output_string`,
  `output_bytes`, `exit_code`, `run`, `first_line`, and `start` (live handle).
- `RunningProcess` handle with incremental `stdout_lines` streaming (stderr
  drained in the background), `output_string`/`output_bytes`/`wait`, and process
  metadata.
- `ProcessResult<T>` with `is_success` / `ensure_success`, and a structured
  `Error` (`Spawn` / `Exit` / `Timeout` / `Io`).
- `Stdin` sources: `empty`, `from_string`, `from_bytes`, `from_file`,
  `from_iter_lines`, `from_reader`, and `from_lines` (async stream).
- `ProcessRunner` mock seam with `JobRunner`, `ScriptedRunner`,
  `RecordingRunner`, and a `mock`-feature `MockRunner`.
- Interactive stdin: `Command::keep_stdin_open` plus `RunningProcess::standard_input`
  returning a `ProcessStdin` writer (`write`/`write_line`/`flush`/`finish`).
- Push line-handlers: `Command::on_stdout_line` / `on_stderr_line`, invoked per
  decoded line as it is read.
- Output-buffer policy: `OutputBufferPolicy` (`bounded`/`unbounded`) with
  `OverflowMode::{DropOldest, DropNewest}`, plus exact `RunningProcess::stdout_line_count`
  / `stderr_line_count` (count survives dropped lines).
- Encoding overrides: `Command::stdout_encoding` / `stderr_encoding` / `encoding`
  to decode non-UTF-8 legacy output (via `encoding_rs`); default stays UTF-8.
- Diagnostics: `ProcessGroup::stats` → `ProcessGroupStats` (active count, and
  CPU/peak-memory where the platform reports them), and per-process
  `RunningProcess::cpu_time` / `peak_memory_bytes` / `elapsed`.
- `CliClient<R>` + the `cli_client!` macro — a reusable core for building typed
  wrappers around an external CLI tool (`command`/`command_in` builders;
  `text`/`capture`/`unit`/`code`/`parse`/`try_parse` run helpers), with the
  runner injectable for hermetic tests.
- Top-level `processkit::run` / `processkit::output` free functions.
- Public `Command` accessors (`program`/`arguments`/`working_dir`/
  `env_overrides`/`stdin_source`/`configured_timeout`) so external
  `ScriptedRunner::when` predicates can inspect a command; plus public
  `Command::to_tokio_command`.
- `ProcessRunnerExt::checked`, `ProcessResult::combined`, `Invocation::args_str`,
  `RunningProcess::finish_streamed` (exit code + collected stderr after
  streaming) and `RunningProcess::start_kill`.
- `Error::Parse { program, message }` for fallible output parsing.
- The `tracing` feature emits a per-run `debug` event (program, exit code,
  timed-out, elapsed) on the `processkit` target.

### Changed
- Output capture is line-oriented (pumped): captured text is normalized to
  `\n` line endings. `output_bytes` still returns exact raw stdout.

[Unreleased]: https://github.com/ZelAnton/ProcessKit-rs/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.11.1...v1.0.0
[0.11.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.10.2...v0.11.0
[0.10.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.8.2...v0.9.0
[0.8.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.7.0...v0.7.1
[0.6.2]: https://github.com/ZelAnton/ProcessKit-rs/releases/tag/v0.6.2
[0.7.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.5.2...v0.6.0
[0.5.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.3.4...v0.4.0
[0.3.4]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ZelAnton/ProcessKit-rs/releases/tag/v0.1.0
