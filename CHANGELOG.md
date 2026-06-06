# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Add entries to `[Unreleased]` as you work — manual bullets always win over the
git-cliff auto-fill (config: `cliff.toml`). On release, promote `[Unreleased]`
to a dated version section.

## [Unreleased]

> **Release note:** this cycle contains a **breaking** change for
> `default-features = false` consumers (resource measurement moved behind the
> now-default `stats` feature — see *Changed*). Release it as **0.7.0**
> (`minor`), not a patch.

### Added
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

### Changed
- Every public type now implements `Debug` (enforced by a crate lint), and
  `Command` is `#[must_use]` — building one and dropping it unused now warns.
- Resource measurement (`ProcessGroupStats`, `ProcessGroup::stats`,
  `RunningProcess::cpu_time`/`peak_memory_bytes`) now sits behind a default-on
  `stats` Cargo feature: `default-features = false` compiles the accounting code
  (and its Windows ProcessStatus FFI) out. Consumers on default features see no
  change; consumers who already set `default-features = false` must add
  `features = ["stats"]` to keep that API.

### Fixed
- POSIX process-group `ProcessGroup::adopt` was a silent no-op for any child
  that had already `exec`'d (the normal case): POSIX refuses `setpgid` there
  (`EACCES`), and the pid was recorded as a process-*group* id that doesn't
  exist, so teardown never reached the child. Such children are now tracked
  and signalled individually — the adopted child is contained (killed with the
  group), though its future forks are not (unlike Windows/cgroup adoption).
  Adopting a child the group already tracks (a self-spawned leader, or a
  repeated adopt) is also de-duplicated now, so `members()`/`stats()` no
  longer over-report or grow per call.

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

[Unreleased]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.6.1...HEAD
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
