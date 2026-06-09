# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Add entries to `[Unreleased]` as you work — manual bullets always win over the
git-cliff auto-fill (config: `cliff.toml`). On release, promote `[Unreleased]`
to a dated version section.

## [Unreleased]

### Added
-

### Changed
-

### Fixed
-

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

[Unreleased]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.9.1...HEAD
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
