# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Add entries to `[Unreleased]` as you work — manual bullets always win over the
git-cliff auto-fill (config: `cliff.toml`). On release, promote `[Unreleased]`
to a dated version section.

## [Unreleased]

### Added

- Add a Windows named-pipe readiness probe with busy-server detection and a
  symmetric `Unsupported` result on other platforms.
- Add a resettable per-command output-inactivity watchdog with distinct result
  classification across capture, streaming, PTY, doubles, and cassettes.
- Add stderr line and partial-tail readiness probes with the same non-killing
  deadline semantics as their stdout counterparts.
- Add an opt-in cassette scrub hook that redacts arguments, working directories,
  and captured output before persistence while keeping replay keys symmetric.
- Add runnable examples for PTY dialogs, lifecycle events, deliberately detached
  children, and completion-ordered batch streaming.

### Changed

- Add the CLI runner documentation to the Pages implementation switcher.
- Simplify oversized-line pump tracking by removing unused length accumulation.
- Add the VT sanitizer to the scheduled fuzz tier and strengthen its property
  tests around control removal, idempotence, and output length.
- Verify the BSD process-group backend with a FreeBSD cross-check and a focused
  real-VM smoke tier.
- Exercise Windows-specific code on hosted ARM64 runners in both clippy and
  real-subprocess test matrices.
- Expand mutation testing to the hermetic backoff, digest, and resource-limit
  modules while preserving per-shard runtime with a ten-way matrix.

### Fixed

- Keep Windows graceful shutdown on its prompt atomic path when all recorded
  CTRL_BREAK leaders are stale.
- Surface genuine Windows ConPTY termination failures while preserving
  idempotent kills for already-exited children.

## [3.0.2] - 2026-07-25

### Added
-

### Changed

- Redesign the Pages landing page around the illustrated cover, live project
  badges, a concise no-orphan introduction, and direct install, guide, and API
  entry points.
- Add the native .NET sibling to the repository's cross-language links while
  keeping language alternatives out of the Rust Pages landing page.
- Expand the README and Pages 3.0 highlights with the release's significant
  lifecycle streams and live sessions, process-tree controls, completion-order
  batch capture, redaction/raw-output seams, and runtime introspection.

### Fixed

- Prevent the closing untrusted-process note in the containers guide from
  rendering as an oversized Setext heading.
- Keep inline code in every Pages table header aligned with the header's own
  background, color, and typography instead of inheriting code-block styling
  from highlight.js.

## [3.0.1] - 2026-07-25

### Added
-

### Changed

- Refresh the README and Pages landing page for the 3.0.0 release: surface the
  new PTY backend and `Error`/`ErrorReason` split, add the missing `pty` feature
  entry, and align the upgrading guide with the current 3.x line.

### Fixed
-

## [3.0.0] - 2026-07-25

### Added

- Give `use_pty()` children a coherent spawn-time terminal identity. Unix PTYs
  default `TERM=xterm-256color`; Unix and Windows set `COLUMNS`/`LINES` from
  `pty_size(cols, rows)` or the same shared 80×24 fallback used by the PTY
  backend. Windows deliberately does not synthesize `TERM` because ConPTY
  exposes VT handling through console APIs. Explicit `env`/`env_remove`
  operations for all three names remain authoritative across `env_clear` and
  `inherit_env` layering.
- Add Linux I/O scheduling for child processes: `Command::io_priority(IoPriority)`
  applies `ioprio_set(2)` in `pre_exec`, with `Idle`, `BestEffort(0..=7)`, and
  `RealTime(0..=7)` classes. The request fails loudly with `ErrorReason::Unsupported`
  outside Linux and is refused for `spawn_detached`; invalid data values and
  kernel privilege rejections fail before or during spawn rather than silently
  inheriting a different priority.
- Add pseudo-terminal **window-size control** for `use_pty` runs (the `pty`
  feature): `Command::pty_size(cols, rows)` sets the terminal's initial geometry
  at spawn (replacing the previously hard-coded 80×24 — still the default when
  unset), and `RunningProcess::resize_pty(cols, rows)` resizes a **live** session
  so a host window resize can be propagated to the child. On Unix `resize_pty`
  issues `TIOCSWINSZ` on the master, delivering `SIGWINCH` to the child's
  foreground process group; on Windows it calls `ResizePseudoConsole` (no
  `SIGWINCH` — the client observes the new geometry on its next console query, and
  conhost may reflow asynchronously). `resize_pty` returns a typed
  `ErrorReason::Unsupported` — never a panic or a silent no-op — on a non-PTY run
  or once the child has exited; `pty_size` on a non-`use_pty` command is a
  documented no-op. The PTY-variant `ScriptedRunner` models both hermetically (a
  configured spawn size and live resizes) with no real pseudo-terminal, so both
  are unit-testable. Additive: an existing PTY run that sets neither keeps the
  byte-identical 80×24 behavior
- Make PTY (and any terminal-driven) **merged output line-consumable** with two
  decisions on the line-oriented capture path. (1) `Command::use_pty()` now
  defaults the **effective** `line_terminator` to `LineTerminator::CarriageReturn`
  instead of `Newline`, so a child's bare-`\r` progress redraws stream as
  individual frames/lines rather than piling into one ever-growing line only seen
  at EOF; it is a *non-destructive* reframing (`\r\n` stays one terminator), applied
  only when the caller has not pinned a terminator — an explicit
  `line_terminator(...)`/`stdout_line_terminator(...)`/`stderr_line_terminator(...)`
  (even `Newline`) always wins, order-independently. (2) Add the opt-in VT/ANSI
  **output sanitizer** `Command::sanitize_vt()` (plus per-stream
  `stdout_sanitize_vt()` / `stderr_sanitize_vt()`): each captured line is stripped
  of CSI (`ESC [ … final`), OSC (`ESC ] … BEL/ST`), DCS/SOS/PM/APC string escapes,
  other `ESC` escapes, and lone C0 control codes / `DEL` — keeping the horizontal
  tab — so `output_string`, `wait_for_line`/`first_line`, `ProcessResult`, and the
  streaming verbs carry readable text instead of `\x1b[…m`-mucked strings. Kept
  opt-in because stripping is *destructive*. Sanitization shapes **only the capture
  backlog**, the same boundary as `capture_policy`: the per-line handlers, decoded
  tees, byte-plane `raw_tee`, and `output_bytes` still observe the raw bytes, and a
  line past an `OutputBufferPolicy` byte cap is judged on its raw length and never
  reaches the sanitizer. When both are set, sanitization runs **before**
  `capture_policy` so a secret-scrubbing policy matches on already-cleaned text. An
  escape split across pump reads is stripped whole (the line is reassembled before
  the per-line transform runs). The raw-pipe-byte accounting (`seen_bytes`,
  `Error::OutputTooLarge.total_bytes`), the line/`dropped` counters, and the
  `DropNewest` seal are unaffected. Off by default and strictly additive: a run that
  calls neither knob captures byte-for-byte as before
- Add `Command::spawn_detached` — the crate's **one deliberate, opt-in escape**
  from kill-on-drop containment, for the legitimate handoff cases (daemonizing, a
  `nohup`-style helper meant to *outlive* its launcher). It spawns the child
  **outside** this crate's containment — a **new session** (`setsid`) on Unix, and
  **not assigned** to the Job Object on Windows — and hands back a new
  `DetachedChild` type carrying only the `pid`: no `kill`, no `wait`, no timeout,
  no capture, no teardown verbs, because it is no longer contained. Dropping the
  handle does **nothing** to the child. It **inverts the headline guarantee on
  purpose**, so it is a separate, non-interchangeable type (never a
  `RunningProcess`) and: (1) refuses any owner-dependent knob — a timeout, capture
  wiring (`on_stdout_line`/tees/`capture_policy`), an interactive stdin
  (`keep_stdin_open`/`inherit_stdin`/a `stdin` source), `retry`, `cancel_on`,
  `kill_on_parent_death`, `windows_graceful_ctrl_break`, Linux `io_priority`,
  `use_pty` — with a loud,
  typed `ErrorReason::Unsupported` naming it, never a silent drop; (2) allows only
  **null (default) or a file redirect** for stdio, never a pipe (which would
  deadlock a child once its owner is gone); and (3) deliberately does **not** break
  a child out of a *host* Job Object / cgroup it inherits (a CI runner, a
  `systemd` scope, this crate's own supervisor) — it escapes only *this crate's*
  per-run containment. Program/args/env/working-directory and the privilege-drop
  knobs (`uid`/`gid`/`groups`/`umask`/`priority`) are honored. Additive: every
  existing verb keeps its kill-on-drop guarantee unchanged
- Extend the `events()` stream (see the breaking rename under **Changed**) with
  two **lifecycle** event kinds so one asynchronous stream now carries a process's
  whole life — `Started` → interleaved `Stdout`/`Stderr` → `Exited` — instead of
  three separate channels. `ProcessEvent::Started { pid }` leads the stream,
  emitted as soon as the pid is known (before any output; `pid` is `None` for a
  scripted double). `ProcessEvent::Exited(Outcome)` ends it, emitted when the run
  is reaped, carrying the **same** `Outcome` `finish()` reports (not a parallel
  type). Both are observed by the running layer itself — no live sink is threaded
  through the teardown backends. A new `ProcessEvent::name()` gives a stable
  snake_case tag (`"started"`/`"stdout"`/`"stderr"`/`"exited"`), by the same
  convention as `Outcome::name()`. The graceful-teardown transitions
  (`soft_signal`/`grace_started`/`drained`/`escalated`/`spared`) are deliberately
  **not** in this enum yet — with no per-call consumer they stay on the `tracing`
  seam and in `ShutdownReport`; because `ProcessEvent` is `#[non_exhaustive]` they
  can be added here additively once a consumer needs them, without a breaking
  change. Works identically on the piped and `Backend::Pty` pumps.
- Add an opt-in **PTY launch mode** behind the new `pty` feature:
  `Command::use_pty()` spawns the child under a real pseudo-terminal — `openpty`
  on Unix, `CreatePseudoConsole` (ConPTY) on Windows — instead of three pipes, so
  a tool that *demands* a controlling terminal (an `isatty()`-gated agentic CLI,
  an `ssh`/`sudo` password prompt) works. A new `Backend::Pty` feeds the child's
  **merged** stdout+stderr through the same output pump as a piped run over a
  single master, so in this mode the `on_stderr_line`/`stderr_tee` split collapses
  and `ProcessResult::stderr` is empty (documented on `use_pty`). Interactive
  input runs over the same master (`keep_stdin_open` + `take_stdin`); on Unix
  terminal echo is disabled (termios) so a written secret is not echoed back into
  the merged output. **Containment is unchanged** — the PTY child is placed in the
  **same** Job Object / cgroup / process group as any other run (K-032), so
  whole-tree kill-on-drop, timeouts, and cancellation behave identically. Off by
  default and strictly additive: with the feature off (or on but `use_pty` unset)
  the existing three-pipe behavior is byte-for-byte unchanged. `ScriptedRunner`
  gains a PTY variant (it models the stderr→stdout merge) so hermetic tests need
  no real tty. Resolves the PTY deferral in
  `decisions/permissions-privileges-pty-network.md` §4
- Add `OwnedStatsSampler` — an owning, `'static` twin of the borrowing
  `StatsSampler` (feature `stats`). Built from an `&Arc<ProcessGroup>` via
  `OwnedStatsSampler::new`, it is `Send + 'static`, so — unlike the sampler
  `ProcessGroup::sample_stats` returns, which is pinned to the borrow — it can be
  moved into a `tokio::spawn`ed task or across an FFI boundary and sampled there,
  the shape an `Arc`-held group (a long-lived service, a supervisor, a
  cross-language wrapper) needs. It drives the **same** interval engine as
  `StatsSampler` (first sample immediate, then one per interval, missed ticks
  skipped) rather than forking the cadence, and holds the group only **weakly**:
  like the borrowing sampler it never keeps the group or its kill-on-drop
  guarantee alive. Its end-of-series behaviour is therefore well-defined — the
  stream yields `None` (fused) on the first tick that can't produce a snapshot,
  whether the container was torn down (a failed `stats()`) or every strong `Arc`
  to the group was released mid-series (the weak handle stops upgrading) — never
  silently repeating the last snapshot and never hanging the caller. Additive:
  `sample_stats` / `StatsSampler` are unchanged
- Give the **control predicates a fallible channel**: `Supervisor::try_stop_when`
  / `try_give_up_when` / `try_health_check` and `ScriptedRunner::try_when` — the
  `try_*` twins of the existing infallible predicates, each taking a predicate that
  returns `Result<bool, E>` (async `Result<bool, E>` for `try_health_check`) for any
  `E: Into<Box<dyn Error + Send + Sync>>`. `Ok(true)`/`Ok(false)` behave **exactly**
  as the infallible sibling's `true`/`false`; an `Err` **aborts** the operation and
  surfaces to the caller as a new `ErrorReason::Predicate` (kind `ErrorKind::Predicate`)
  carrying the predicate's own error **verbatim** as its `source` — never a fabricated
  stop/give-up/unhealthy verdict. A failing `try_health_check` probe still tears the
  live incarnation down by the same drop-kill a liveness failure uses, so aborting on a
  probe error leaks no child. For a wrapper over a language where any callback can throw
  (e.g. a `processkit-py` binding) this replaces two independent error-smuggling
  machines with one typed channel. Additive: the infallible `stop_when`/`give_up_when`/
  `health_check`/`when` are unchanged, and setting a `try_*` twin replaces its sibling on
  the same slot.
- Add the `ErrorReason::Predicate { predicate, source }` variant and its
  `ErrorKind::Predicate` classification (both `#[non_exhaustive]`, unconditional) — the
  failure mode a fallible control predicate (above) raises. `predicate` is a stable
  identifier (`"stop_when"`/`"give_up_when"`/`"health_check"`/`"when"`); `source` is the
  predicate's own boxed error. It joins the total `ErrorKind` classification (T-174) as
  its own routing bucket, distinct from `Other`, so a consumer can tell "the caller's
  control callback failed" apart from a backend/IO failure.
- Narrate the **graceful-teardown transitions** on the `tracing` seam (feature
  `tracing`): the shared teardown driver now emits a `debug` event per transition on
  the `processkit` target — `soft_signal` (the SIGTERM / CTRL_BREAK was issued) →
  `grace_started` (the grace window opened, with `grace_ms`) → one of `drained` (the
  tree/child exited in time), `escalated` (the grace elapsed and the tree was
  hard-killed), or `spared` (a non-escalating stop left survivors) — each in a stable
  `phase` field, joining the existing spawn/exit events for one uniform, timestamped
  lifecycle timeline. Emitted the same way for **every** graceful path
  (`ProcessGroup::stop` / `shutdown` / `shutdown_ref`, a run-level
  `Command::timeout_grace`, a `Supervisor`'s graceful stop, and the single-child
  streaming teardown), so a consumer running its own end-of-run race can stamp each
  transition the instant the layer that observed it crossed it — the **live**
  counterpart of the after-the-fact facts on `ShutdownReport`. Observation only
  (no way to influence the teardown), zero cost when the feature or a subscriber is
  absent, and never carrying argv/env. No new public API (`public-api.txt`
  unchanged); the typed, `select!`-able event *stream* stays deferred to the unified
  output/lifecycle-stream design
- Add `Error::kind()` / `ErrorReason::kind()` returning a new `#[non_exhaustive]`
  `ErrorKind` — a **total** classification of every failure into one coarse routing
  bucket (`NotFound`, `Spawn`, `PermissionDenied`, `ResourceLimit` (`limits`),
  `Unsupported`, `Timeout`, `Cancelled`, `Exit`, `Signalled`, `Other`). Where
  `ErrorReason` is the structured failure mode, `ErrorKind` is what a consumer needs
  when it maps failures onto its **own** shape — a CLI folding each disposition into a
  distinct exit code, a cross-language binding raising a matching exception class, a
  router picking a retry policy — instead of matching `NotFound`/`Spawn` by hand and
  dumping everything else into one "backend failure" bucket, which just defers the same
  catch-all past a `#[non_exhaustive]` match. The mapping is **derived** from each
  variant's existing semantics (not invented) and is an exhaustive `match` inside the
  crate with **no** catch-all, so a future `ErrorReason` variant cannot ship without a
  deliberate kind. It stays consistent with the point classifiers (`is_not_found()` ⇔
  `kind() == NotFound`, `is_permission_denied()` ⇔ `PermissionDenied`, `is_timeout()`
  ⇔ `Timeout`, …), which keep their exact behavior. `ErrorKind::name()` gives a stable
  `snake_case` machine identifier (`"not_found"`, `"permission_denied"`, …); like
  `Outcome`, it is reported-only, with no `from_name` inverse
- Add payload accessors that previously required destructuring the
  `#[non_exhaustive]` variant by hand: `Error::timeout_duration()` /
  `ErrorReason::timeout_duration()` (the run deadline of a `Timeout`; `None`
  elsewhere, including `NotReady` whose probe deadline is a separate clock),
  `Error::unsupported_operation()` / `ErrorReason::unsupported_operation()` (the
  operation description of an `Unsupported`), and `Error::output_overflow()` /
  `ErrorReason::output_overflow()` returning a new `#[non_exhaustive]` `OutputOverflow`
  snapshot with `total_lines()` / `total_bytes()` / `max_lines()` / `max_bytes()`
  accessors for an `OutputTooLarge` failure (a grouped snapshot rather than four scalar
  `Option<usize>` accessors, so the two optional ceilings stay unambiguous). Each
  follows the existing `code()` / `signal()` pattern — an exhaustive match, `None` for
  irrelevant variants. Purely additive; no existing accessor or classifier changes
  behavior
- Add `output_stream(commands, concurrency, runner)` / `output_stream_bytes(...)` — the
  **streaming** siblings of `output_all` / `output_all_bytes`. Same bounded fan-out, but
  each result is yielded the moment its command finishes (a `Stream` of `(input index,
  Result<ProcessResult<_>>)` pairs) instead of one `Vec` at the very end, so a fast
  command never waits behind a slow one and you can act on the first finisher
  immediately. Results arrive in **completion order**, each tagged with its input index
  so it stays traceable to its source command; the fan-out never short-circuits (a
  command's failure is just its own yielded item, never a cancellation of its siblings);
  and the concurrency cap is honored exactly. Dropping the stream mid-fan-out tears down
  every still-live process tree with no orphans (own-group runner) and drops every
  command still waiting for a slot **without ever spawning it**, while every result
  already handed to the consumer survives — closing `output_all`'s "no partial results
  on cancellation" gap. Chosen as a borrowing `Stream` (not a channel + spawned task):
  it keeps the same `&JobRunner` / `&ProcessGroup` runner ergonomics as `output_all`,
  needs no `'static` bound, and makes "drop cancels the whole fan-out" fall out of
  ownership rather than manual wiring. Strictly additive: `output_all` /
  `output_all_bytes` keep their exact signatures and behavior — they are now literally
  this same engine driven to exhaustion and reassembled into input order (one scheduler,
  two presentations), so their concurrency and no-short-circuit semantics can no longer
  drift from the streaming path
- Add `ProcessResult::configured_timeout()` / `ok_codes()` — accessors for the
  two configuration fields (the run's timeout, the accepted exit-code set) that
  participate in `ProcessResult`'s `PartialEq` but previously had no way to be
  read back. Add the `Command` twin `configured_ok_codes()`, alongside the
  already-existing `configured_timeout()`. Add `#[doc(hidden)]` `from_parts`
  constructors — insulated, off the documented surface but `pub` and
  semver-covered, by the same precedent as `Error::exit`/`timeout`/… — for
  `ProcessResult`, `Finished`, `RunProfile`, and `SupervisionOutcome`, so a
  wrapper/serialization layer (e.g. a cross-language binding) can reconstruct
  one of these values directly instead of programming a test double just to
  produce one. (`Outcome` needs no equivalent: `#[non_exhaustive]` on a plain
  enum does not block constructing an existing tuple/unit variant from outside
  the crate, so `Outcome::Exited(0)` already works anywhere.) Purely
  additive — no change to `PartialEq`/`Hash`/`Debug` semantics on any of these
  types
- Add free-standing `process_info(pid) -> Result<Option<MemberInfo>>` and
  `process_is_alive(pid, start_time) -> Result<bool>` (needs `process-control`) —
  the standalone twin of `ProcessGroup::members_info` for a pid held **outside** any
  group (a pid saved to disk across runs, a launch registry checking a
  crash-surviving owner, an e2e probe watching a process from outside its
  container). `process_info` returns the same best-effort `MemberInfo` a group
  member carries — parent pid, image name, and the start-time identity token — read
  through the crate's existing per-platform readers (`/proc/<pid>/stat` on Linux,
  `proc_pidinfo` on macOS, `Toolhelp32` + the creation `FILETIME` on Windows, a
  `kill(pid, 0)` existence probe on the bare BSDs), never a second implementation
  and never reading argv/environment. It answers with a deliberate three-way
  contract: `Ok(Some(info))` when the process exists, `Ok(None)` when the pid names
  **no** process (an honest negative — the "it's gone" answer), and `Err` when the
  process may exist but couldn't be inspected (no permission — a Windows
  protected/`System` process, a Linux `hidepid` mount, a macOS restricted process —
  or an OS read error), so "not allowed to look" is never mistaken for "dead".
  `process_is_alive` pairs the pid with the saved start-time token for **reuse-safe**
  liveness: `Ok(true)` only when the process exists *and* its current start time
  matches the saved one, so a recycled number (same pid, different start time) reads
  as `Ok(false)` rather than a false "alive"; where the platform reports no start
  time (the bare BSDs) it degrades to number-only liveness, no weaker than a
  hand-written check and never a false "dead". Purely additive
- Add `processkit::host_containment() -> HostContainment` — a **spawn-free,
  side-effect-free** host capability query: it reports how process containment
  behaves on this host *without creating a container or spawning anything*, so a
  consumer's preflight (a *doctor* / host-check command whose contract is "no side
  effects") can state the real story up front instead of having to build a
  `ProcessGroup` just to read `mechanism()`. The new `#[non_exhaustive]`
  `HostContainment` carries: `mechanism()` — which `Mechanism` a group created here
  and now would use, determined by a read-only probe (a fixed constant on
  Windows/macOS/BSD; on Linux a cheap read-only check of cgroup v2 availability and
  writability that agrees with a real `ProcessGroup::new`, best-effort in the rare
  window where a writable-looking cgroup then rejects creation); `soft_stop_scope()`
  (needs `process-control`) — the host-level reach of a soft stop (`WholeTree` on the
  Unix backends, `OptInMembers` on Windows; a *specific* group still reads its own,
  possibly narrower, scope from `ProcessGroup::soft_stop_scope()`);
  `parent_death_cleanup()` — the same `ParentDeathCleanup` that
  `Command::kill_on_parent_death_scope()` reports, reused not recomputed; and
  `crate_version()`. The mechanism detection is lifted out of the group-creation path
  so the query and the real selection share one source of truth. Purely additive
- Add `RunningProcess::stdout_bytes_seen()` and
  `RunningProcess::stderr_bytes_seen()` live monotonic counters. They report
  raw bytes read from each pipe before decoding, including bytes discarded by
  buffer overflow or oversized-line handling, and report `0` for streams that
  are not pumped.
- Add `Command::stdout_raw_tee(writer)` / `stderr_raw_tee(writer)` — a
  **byte-accurate** tee that writes each chunk to `writer` *exactly as read from
  the child's pipe*, before any decoding or line splitting. Where
  `stdout_tee`/`stderr_tee` write decoded lines (each plus a `\n`), the raw tee
  preserves the child's bytes verbatim: non-UTF-8 output survives (no U+FFFD
  replacement, so `git archive`/`tar -cz -`/`ffmpeg … -` are teed intact), CRLF
  is not normalized, a missing final newline is not fabricated, an unterminated
  prompt (`Password: `) reaches the sink the moment it is read rather than at EOF,
  and even a line the buffer policy drops (past a `with_max_bytes` cap, or under
  `DropOldest`/`DropNewest`/`Error`) is still teed whole. Strictly additive — the
  decoded-line path (capture buffer, `on_*_line` handlers, `stdout_tee`,
  truncation accounting) is unchanged, and both tees fire independently. The write
  is awaited on the capture pump (the same backpressure seam as the line tee, so a
  slow raw sink cannot grow unbounded in-flight memory), flushed at stream end, and
  isolated on a write error. Fires from the line/streaming verbs (`output_string`,
  `start` + `stdout_lines`/`output_events`, `wait`/`drain`); a no-op under
  `Inherit`/`Null`/a file redirect (no capture pump) and under `output_bytes`
  (whose own return value already is the raw stdout). Lets a transparent wrapper
  forward a stream live *and* hash the exact bytes the child wrote
- Add `ProcessGroup::soft_stop_scope() -> SoftStopScope` (needs `process-control`),
  an honest, side-effect-free capability report of how far a **soft stop**
  (`signal(Signal::Term)` / `Signal::Int`) reaches on *this* group — queried
  **before** the attempt so a caller cancelling on its own schedule (a UI Cancel, a
  control-socket command, its own timeout) can decide up front and state the real
  reach, instead of firing a `signal`, catching `Error::Unsupported`, and
  reverse-engineering the scope. The new `#[non_exhaustive]` `SoftStopScope` reports
  `WholeTree` on the Unix backends (cgroup v2 and the POSIX process-group fallback
  reach the whole tree), and on Windows `OptInMembers` when a live console-CTRL
  leader (`windows_graceful_ctrl_break`) or a windowed member exists (a curated
  subset) or `Unsupported` when neither does — exactly the split where
  `signal(Int/Term)` returns `Ok` versus `Error::Unsupported`. It reads the same
  live-membership primitives `signal` acts on (delivering no signal, posting no
  `WM_CLOSE`, spawning nothing, mutating nothing), so its report is consistent with
  the real soft-stop outcome by construction. Carries the usual stable machine
  identifier (`name()` / `from_name()`: `whole_tree` / `opt_in_members` / `none`).
  The group-axis sibling of `Command::kill_on_parent_death_scope()`, but read at
  runtime rather than fixed per platform. Purely additive
- Add `RunningProcess::drain()` — a discard-style wait that **respects the
  configured `Command::output_buffer` byte cap**. It drains both pipes (the child
  never blocks on a full one), feeds every fitting line to the configured
  `stdout_tee`/`stderr_tee` and `on_stdout_line`/`on_stderr_line` sinks, retains
  nothing itself, and returns the same `Outcome` classification as `wait`. Unlike
  `wait` (which pins a fixed internal in-flight cap and ignores `output_buffer`),
  `drain` bounds held memory by the configured `max_bytes`, so a
  hundreds-of-megabytes build log already being teed to a file streams through
  without ever being held in memory — no more capturing with `output_string` only
  to throw the result away
- Add `ProcessGroup::stop(grace, escalate)` (needs `process-control`), the
  observable sibling of `shutdown_ref`: the same graceful teardown
  (`SIGTERM` / `CTRL_BREAK` / `WM_CLOSE` → wait → escalate) with an explicit grace
  and escalation flag, returning a new `#[non_exhaustive]` `ShutdownReport` of what
  the kernel *observed* — the attempted soft signal via the new `SoftSignal` enum
  (`Sent`/`Unsupported`/`Failed`, so a windowless Windows Job Object honestly
  reports "no soft-signal tier"), the live member counts before and after, whether
  the tree drained within the grace or was escalated to a hard kill, and the actual
  elapsed. `stop(Duration::ZERO, true)` is a "kill and wait for the tree to actually
  empty" that bare `kill_all` (which returns as soon as the kill is *issued*) does
  not offer. Purely additive — `shutdown`/`shutdown_ref` and every teardown
  guarantee (kill-on-drop, the spawn/adopt re-arm race, no extra wait without a
  grace) are unchanged
- Windows graceful shutdown now posts `WM_CLOSE` (best-effort, *posted* not sent)
  to every top-level window a live job member owns before the hard
  `TerminateJobObject`, so a windowed child (Electron app, desktop tool, windowed
  service) can close cleanly within the grace. Automatic — no opt-in; a windowless
  tree with no `windows_graceful_ctrl_break` opt-in is still hard-killed promptly
  at the deadline, so its timings are unchanged
- Add `Pipeline::start()` returning a live `PipelineSession` — the multi-stage
  analogue of `RunningProcess`: stream the last stage's stdout
  (`stdout_lines`/`output_events`), wait for a readiness line (`wait_for_line`),
  and `finish()` folds the same pipefail outcome (the culprit stage's outcome and
  its own stderr) as the buffering verbs. Whole-chain `start_kill`, kill-on-drop,
  and chain-wide `timeout`/`cancel_on` bound the live session
- Add a seeded randomized-interleaving stress harness for the process lifecycle
- Add "Running untrusted children" hardening guide (`docs/untrusted-children.md`)
- Add comparative benchmarks (`benches/compare.rs`) and the
  [benchmarking guide](docs/comparison.md) for end-to-end capture, streaming,
  and concurrent fan-out comparisons with plain Tokio and standard-library
  process APIs
- Add `Supervisor::start()` returning a live `SupervisionSession` (status
  snapshot, graceful stop, wait)
- Add `RunningProcess::wait_for_socket` for Unix domain socket readiness probes
- Add `ProcessGroup::update_limits` to re-apply `ResourceLimits` to a live group
  (full replacement; Windows Job Object / Linux cgroup v2, typed refusal on the
  process-group mechanism)
- Publish build-provenance attestations for release artifacts (the packaged
  `.crate` and its `SHA256SUMS`, attached to each GitHub Release); see
  "Verifying provenance" in README.md / SECURITY.md
- Add stable machine identifiers to the reporting and configuration enums.
  `Mechanism`, `Outcome`, `ParentDeathCleanup`, `StopReason`, `LimitKind`,
  `LimitReason`, `StdioMode`, `LineTerminator`, `OverflowMode`, `Priority`,
  `RestartPolicy`, and `Signal` each gain a `name()` returning a short,
  lowercase `snake_case` identifier held stable as part of the compatibility
  surface — a diagnostic name, not a wire format: a new variant gets a new name,
  and an existing name is never renamed without a major release. Every enum
  whose value can arrive from outside (config / CLI / another language) also
  gains `from_name(&str)`, an honest inverse that returns `None` on an
  unrecognized name rather than defaulting silently. `Mechanism` and
  `ParentDeathCleanup` use the spellings downstream tools already publish
  (`job_object`/`cgroup_v2`/`process_group`,
  `whole_tree`/`direct_child_only`/`none`), so adopting them needs no migration.
  `Outcome::name()` names the disposition only (`exited`/`signalled`/`timed_out`)
  and has no inverse — the name alone cannot carry the exit code / signal number.
  `Signal::name()` returns `Option`, `None` for the raw-number `Other` escape
  hatch. No optional `serde` feature ships for these enums (deliberately
  declined; rationale in `decisions/serde-reporting-enums-2026-07.md`): the
  string methods already let consumers drop their hand-maintained tables without
  committing the crate to a second serialized-shape compatibility surface or an
  extra optional dependency
- Add `Command::capture_policy(...)` plus the `CapturePolicy` trait and the
  `OutputStream` enum it takes — a typed **redaction-at-capture** seam. Its
  `on_capture(OutputStream, &str) -> Cow<str>` transforms each captured line at the
  single point the crate retains it (before it enters the backlog / `ProcessResult`),
  so a secret can be scrubbed from the retained capture. Deliberate **security
  boundary**: the live paths — the `on_stdout_line`/`on_stderr_line` handlers,
  `stdout_tee`/`stderr_tee`, `stdout_raw_tee`/`stderr_raw_tee`, and the `output_bytes`
  return value — all still see the **unredacted** text; only the retained capture is
  rewritten. A panicking policy fails **closed** (the line is dropped, the secret never
  leaks). Orthogonal to the overflow/eviction buffer policy
  (`OutputBufferPolicy`/`OverflowMode`), which bounds *how much* is kept, not *what* the
  content is; the capture invariants (`DropNewest` seal-latch, raw pre-decode
  `seen_bytes`, `count`/`dropped`/`overflowed`) are untouched. `CapturePolicy::name()`
  is introspectable and shows in `Debug`. Purely additive

### Changed

- **Breaking:** the merged output-event stream is now a full **process-lifecycle**
  stream. `RunningProcess::output_events()` is renamed **`events()`** and the
  event enum `OutputEvent` is renamed **`ProcessEvent`** (the stream type
  `OutputEvents` → `ProcessEvents`); the old names are removed outright (no
  deprecated alias — a deliberate 3.0 major break). The contract widened from
  "which output line" to "an event in the process's life", so the old names had
  become a lie. `ProcessEvent::Stdout`/`Stderr` carry the same `OutputLine`
  payload with unchanged semantics, and `ProcessEvent::text()` still returns
  `Some` for those and `None` for a non-line event. `PipelineSession::output_events`
  is renamed to `events` in lockstep. Migration: rename the verb (`output_events`
  → `events`) and the type (`OutputEvent` → `ProcessEvent`), add a `_` arm for the
  new non-line variants (the enum stays `#[non_exhaustive]`), and — because the
  new terminal `Exited` event is delivered when the run is reaped — drive the
  stream and its `finish()`/`wait()` finisher **together** (e.g. `tokio::join!`)
  rather than draining the stream to completion and only then finishing.
- `ErrorReason::OutputTooLarge.total_bytes` and the `OverflowMode::Error` plus
  `max_bytes` ceiling now count raw bytes read from the output pipe, including
  line terminators and invalid UTF-8 bytes, rather than decoded line-content
  bytes
- **Breaking:** `Error` is now a **pointer-sized wrapper** around a boxed
  `ErrorReason` (`struct Error { .. }` holding a `Box<ErrorReason>`) instead of a
  large enum, mirroring `std::io::Error` / `ErrorKind`. This shrinks `Error` from
  100+ bytes to one pointer, so every `Result<T, Error>` on the run path — and any
  enum that embeds one (e.g. a caller's `vcs_core::Error`) — stays small, and the
  default `result_large_err` / `large_enum_variant` clippy lints no longer fire on
  the crate's public path. The former enum, with **every variant and field
  unchanged** (`Spawn`, `NotFound`, `CassetteMiss`, `Exit`, `Timeout`,
  `OutputTooLarge`, `NotReady`, `Parse`, `ResourceLimit`, `Unsupported`,
  `Cancelled`, `Signalled`, `Stdin`, `Io`), is now the re-exported `ErrorReason`,
  reached via `err.reason() -> &ErrorReason` (or `err.into_reason() -> ErrorReason`
  to take ownership). A `From<ErrorReason> for Error` is provided. All read
  accessors (`code()`, `program()`, `stdout()`/`stderr()`/`stdout_bytes()`,
  `diagnostic()`, `combined()`, `is_not_found()`/`is_timeout()`/`is_cancelled()`/
  `is_signalled()`/`is_transient()`/`is_permission_denied()`, `signal()`,
  `limit_kind()`/`limit_reason()`), `Display`, `Debug` (with its unchanged
  200-byte stream previews, `PATH` redaction, and control-/bidi-sanitizing), and
  `source()` work on `Error` directly as before — only a **direct variant match**
  needs updating: `match err { Error::Exit { .. } => … }` becomes
  `match err.reason() { ErrorReason::Exit { .. } => … }`. The `#[doc(hidden)]`
  constructors (`Error::exit`/`timeout`/`signalled`/`spawn`/`not_found`/`stdin`)
  and the public `Error::parse(..)` are unchanged and still return an `Error`. A
  compile-time assertion pins `size_of::<Error>()` to a pointer. See
  [Upgrading](docs/upgrading.md) (GitHub issue #21)
- Release publishing now uses crates.io Trusted Publishing — a short-lived token
  minted over GitHub OIDC per run — instead of a stored long-lived
  `CRATES_IO_TOKEN` secret
- `ProcessGroup::signal` for `Signal::Int` / `Signal::Term` on Windows now
  best-effort soft-closes the tree (console `CTRL_BREAK` to
  `windows_graceful_ctrl_break` leaders plus `WM_CLOSE` to windowed members)
  instead of always returning `Error::Unsupported`; it returns `Unsupported` only
  when the group has neither a console-CTRL leader nor a windowed member
- `ProcessGroup::signal` on the POSIX process-group mechanism (macOS/BSD and the
  Linux process-group fallback) now reports a genuinely failed send as
  `Error::Io` instead of swallowing every error behind a false `Ok`, matching the
  cgroup mechanism: an `EINVAL` (an out-of-range `Signal::Other(n)`) and an
  `EPERM` from a live, non-zombie member that rejects the signal now surface,
  while an `ESRCH` (a member already exited) and a harmless zombie-only `EPERM`
  stay swallowed. An empty group is still a trivial success, and
  `Signal::Other(0)` remains the POSIX existence probe — it returns `Ok` having
  delivered nothing. Signatures are unchanged; this only makes the error report
  on these edge inputs truthful (not a breaking API change)
- `Command::to_tokio_command()` is no longer `#[doc(hidden)]` — it is now a
  documented, honest low-level escape hatch for a platform knob the high-level builder
  doesn't model. Paired with `ProcessGroup::spawn` it keeps containment (the child is
  still assigned to the Job Object / cgroup / process group) while giving up the
  high-level verbs, output pump, capture, and teardown machinery; a new "Escape hatch"
  section in `docs/commands.md` documents the path. The method was already `pub` and
  semver-covered — only its documentation visibility changed (the storable
  `before_spawn` raw-`Command` mutator hook was assessed for 3.0 and declined; see
  `decisions/before-spawn-hook-2026-07.md`)

### Removed

- **Breaking:** the pre-3.0 output-stream names, removed outright with **no** deprecated
  alias (they were renamed — see the **Changed** entry above for the migration and the
  new concurrent-drive requirement): `RunningProcess::output_events()` and
  `PipelineSession::output_events()` (now `events()`), the `OutputEvent` event enum (now
  `ProcessEvent`), and the `OutputEvents` stream type (now `ProcessEvents`)

### Fixed

- Make Windows ConPTY launches explicitly clear all three inherited standard
  handles with `STARTF_USESTDHANDLES`, allowing the pseudoconsole to install its
  own stdin/stdout/stderr even when ProcessKit itself runs under a debugger,
  test runner, or an existing console. This prevents child output from escaping
  to the launcher's terminal while only conhost's VT frames reach the PTY master.
  `ProcessStdin::write_line` now also sends the ConPTY Enter sequence (`\r`), so
  cooked prompt/response reads complete; raw `write` remains byte-exact
- `OverflowMode::DropNewest` with a `max_bytes` cap now keeps a true contiguous
  **prefix** (head) of the output: once a line is dropped (an over-cap line, or
  one over the remaining byte budget) the head is sealed and every later line is
  dropped too. Previously a shorter line arriving after a dropped longer line
  could still be retained, leaving a non-contiguous set that skipped the dropped
  line — so the retained buffer was not a prefix of the process's output.
  `DropOldest`/`Error` are unaffected. (Audit also confirmed `max_bytes = 0`
  never delivers a phantom empty segment to line handlers, the streaming verbs,
  or the seen-byte accounting before real output — behavior already correct,
  now pinned by regression and property tests.)

## [2.3.2] - 2026-07-22

### Added

- Add capability-reporting query for Unix parent-death cleanup scope
- Add capability-reporting query for Unix parent-death cleanup scope


### Changed

- Clamp a zero Supervisor::health_check probe interval
- Inline dead ideas/*.md rustdoc references
- Start integration branch for batch B-20260721T152828Z
- Inline dead ideas/*.md rustdoc references
- Clamp a zero Supervisor::health_check probe interval
- Prune stale ctrl_break_leaders entries on opt-in spawn
- Dedup ctrl_break_leaders entry on recycled-pid opt-in spawn
- Start integration branch for batch B-20260721T234309Z
- Prune stale ctrl_break_leaders entries in the Windows Job

## [2.3.1] - 2026-07-20

### Added

- Add #[must_use] to cli_client! macro-generated builder methods
- Add #[must_use] to cli_client! macro-generated builder methods
- Add nested Job Object containment integration test
- Add typos spell-checker to CI and justfile
- Add "Running many at once" batch fan-out guide
- Add fuzz targets for cassette parsing and replay
- Add opt-in liveness health checks to Supervisor
- Add spawn-time stdout/stderr file redirect to Command
- Add opt-in Windows graceful shutdown via console CTRL_BREAK
- Add opt-in Windows graceful shutdown via console CTRL_BREAK
- Add spawn-time stdout/stderr file redirect to Command
- Add opt-in liveness health checks to Supervisor
- Add fuzz targets for cassette parsing and replay
- Add "Running many at once" batch fan-out guide
- Add typos spell-checker to CI and justfile
- Add nested Job Object containment integration test
- Add loom model-checking tier for pid_gate/deadline/SkipDropKill
- Add proptest suite for OutputBufferPolicy invariants
- Add proptest suite for OutputBufferPolicy invariants
- Add loom model-checking tier for pid_gate/deadline/SkipDropKill
- Add ProcessGroup::members_info() enriched member snapshot
- Add ProcessGroup::members_info() enriched member snapshot


### Changed

- Start integration branch for batch B-20260719T111659Z
- Give gh run rerun explicit repo context in mutants-retry
- Clamp graceful::run poll sleep to remaining deadline
- Drain stderr in poll_until independently of stdout streamability
- Narrow one-shot stdin retry gate to pre-child launch failures
- Start integration branch for batch B-20260719T213134Z
- Narrow one-shot stdin retry gate to pre-child launch failures
- Drain stderr in poll_until independently of stdout streamability
- Clamp graceful::run poll sleep to remaining deadline
- Start integration branch for batch B-20260719T230600Z
- Document composite pipeline name in ProcessResult::program rustdoc
- Start integration branch for batch B-20260720T081656Z
- Document composite pipeline name in ProcessResult::program rustdoc
- Raise honest EPERM for live pgroup members on SIGKILL teardown
- Extract shared probe() 0/1 exit-code helper on ProcessResult<String>
- Start integration branch for batch B-20260720T192446Z
- Extract shared probe() 0/1 exit-code helper on ProcessResult<String>
- Raise honest EPERM for live pgroup members on SIGKILL teardown
- Start integration branch for batch B-20260720T211640Z


### Fixed

- Fix mutants-retry gh run rerun missing repo context
- Fix broken rustdoc intra-doc links under --no-default-features
- Fix rustfmt formatting in sys::pgroup live/zombie state check


### Removed

- Remove unreachable stdout/stderr sink guards, document invariant with debug_assert
- Remove unreachable stdout/stderr sink guards, document invariant with debug_assert

## [2.3.0] - 2026-07-19

### Added

- Add hermetic tests for pump oversized-line skip/guard paths, mark equivalent mutants
- Add scoped retry controller for mutants shard runner reclaims
- Add auto-retry controller for mutants-shard runner reclaims


### Changed

- Consolidate one-shot-stdin predicate on Command::effective_stdin_source
- Pin surviving mutation-test boundaries in error.rs redaction/truncation
- Pin surviving capacity-boundary mutants in pump.rs with boundary tests
- Pin surviving capacity-boundary mutants in buffer.rs with boundary tests
- Initialize integration workspace for batch B-20260716T110651Z
- Pin surviving capacity-boundary mutants in buffer.rs with boundary tests
- Pin surviving capacity-boundary mutants in pump.rs with boundary tests
- Pin surviving mutation-test boundaries in error.rs redaction/truncation
- Consolidate one-shot-stdin predicate on Command::effective_stdin_source
- Align record_oversized_line with discarding-contract of SharedLines::push
- Initialize integration workspace for batch B-20260716T223538Z
- Align record_oversized_line with the discarding contract of SharedLines::push
- Substitute resolved path for non-.exe PATHEXT bare-name matches on Windows
- Route DryRunRunner stdin validation through take_stdin_for_run
- 
- Reject invalid stdin configs in DryRunRunner via take_stdin_for_run
- Substitute resolved path for non-.exe PATHEXT bare-name matches on Windows
- Mirror stdin_inherit into Command::effective_stdin_source
- Initialize integration workspace for batch B-20260717T012348Z
- Mirror stdin_inherit into Command::effective_stdin_source
- Gate mutants CI on missed.txt content instead of cargo-mutants exit code
- Validate outcome files before tolerating exit codes 2/3, enforce generous timeouts
- Replace timing-based mutant proofs with deterministic assertions
- ci(deps): bump actions/deploy-pages from 4 to 5 (#15)
- ci(deps): bump actions/cache from 4 to 6
- ci(deps): bump actions/upload-artifact from 4 to 7
- ci(deps): bump actions/upload-pages-artifact from 3 to 5
- Initialize integration workspace for batch B-20260717T123847Z (re-anchored on updated main)
- Close remaining MISSED mutants in pump.rs/buffer.rs mutation scope
- Gate mutants CI on MISSED only, tolerate TIMEOUT
- Start integration branch for batch B-20260718T103516Z


### Fixed

- Fix outdated Pipeline crate-doc: per-stage sub-groups, not one shared group
- Fix outdated Pipeline crate-doc: per-stage sub-groups, not one shared group

## [2.2.5] - 2026-07-13

### Added

- Add Command::effective_stdin_source to unify what stdin the child gets


### Changed

- Deduplicate FNV-1a hashing behind cassette match keys
- Consolidate line-handler panic isolation into a shared helper
- Deduplicate /proc/<pid>/stat starttime parsing into sys::procfs
- Align rustdoc for stdout_lines/wait_for/wait_for_port with actual retention
- Only treat backslash as a path separator in is_bare_name on Windows
- 
- Only treat backslash as a path separator in is_bare_name on Unix
- Align rustdoc for stdout_lines/wait_for/wait_for_port with actual retention
- Deduplicate /proc/<pid>/stat starttime parsing into sys::procfs
- Consolidate line-handler panic isolation into a shared helper
- Deduplicate FNV-1a hashing behind cassette match keys
- Unify effective stdin source across command, doubles, and cassette

## [2.2.4] - 2026-07-12

### Added
- Spawn-free program resolution (a *doctor* / preflight check): the crate-level
  `processkit::which(program)`, `Command::resolve_program()`, and
  `CliClient::resolve_program()` resolve a program to its absolute path **without
  launching it** (no side effects), returning the typed `Error::NotFound`
  (`is_not_found()`, `searched` diagnostic) when it can't be located — the same
  error a real run would raise. Resolution reuses the crate's own launch-path
  logic — the same `PATH`/PATHEXT/execute-bit resolution and `prefer_local`
  handling a spawn performs, not a separate copy — so preflight never disagrees
  with the actual launch (a command that relocates the child's `PATH` via
  `env`/`env_clear`/`inherit_env` is resolved against that effective child
  `PATH`). Synchronous and cheap (a few `stat`s); no async runtime required. For
  early, friendly "is this tool installed?" diagnostics in wrapper apps without
  starting a process.
- `Command::inherit_stdin()` — give the child the parent's own standard input
  (`Stdio::inherit`), so it reads directly from the parent's terminal/file/pipe
  instead of a crate-managed pipe. The stdin counterpart of
  `stdout(StdioMode::Inherit)`, for children that must talk to the real tty
  (`git commit` opening `$EDITOR`, a tool prompting for input) or to forward the
  parent's piped stdin straight through. Portable across Linux, macOS, and
  Windows. It is a dedicated verb (not a `Stdin` source or a mode enum) so that
  its incompatibility with a mediated stdin is representable and enforced:
  combining `inherit_stdin()` with `keep_stdin_open()` or a configured
  `stdin(Stdin::…)` source (including `Stdin::empty()`) is refused at the launch
  boundary with a typed `Error::Io` (`InvalidInput`) — mirroring the crate's
  existing one-shot-source refusal — rather than silently letting one setting
  win. The rejection is enforced on the shared launch seam, so the hermetic test
  doubles (`ScriptedRunner`) reject the same conflict a live run does.
- CI now runs the real-subprocess test suite inside a real Alpine/musl
  container (`test-musl` job), in addition to the existing glibc/Windows/macOS
  legs; reproduce locally with `just test-musl` (requires Docker)
- CI now also runs the `test` and `clippy` matrices on a native `ubuntu-24.04-arm`
  (Linux aarch64) runner, in addition to the existing x86_64 Linux/Windows/macOS
  (and Darwin arm64) legs, giving the native Linux syscall/signal/cgroup layer in
  `src/sys/{linux,pgroup,unix,pid_gate}.rs` real Linux/aarch64 test coverage
  instead of only Darwin-arm64 `cargo check`

### Changed
-

### Fixed
-

## [2.2.3] - 2026-07-10

### Changed

- Settle the trap before signalling in matching_identity_group_is_kept_and_signalled

## [2.2.2] - 2026-07-10

### Added
-

### Changed
-

### Fixed
- Close a re-arm race between a non-escalating `ProcessGroup::shutdown`/
  `shutdown_ref` (`escalate_to_kill = false`) and a concurrent `start`/`adopt`.
  A child spawned or adopted into the group while the shutdown was still in
  flight could be silently spared by the shutdown's stale kill-on-drop request
  and then leak as an orphan on `Drop`. The skip-on-drop latch now carries a
  generation the re-arm bumps, so a concurrent spawn/adopt always wins and the
  fresh child keeps its Drop-kill backstop across all three backends (POSIX
  process group, Linux cgroup, Windows Job Object).

## [2.2.1] - 2026-07-08

### Changed

- Unhide and reformat doctest boilerplate in GitHub-facing docs


### Fixed

- Fix factual errors in documentation

## [2.2.0] - 2026-07-08

### Added
- `Command::prefer_local(dir)` — a directory to probe **before** the system
  `PATH` when resolving a bare-name program for this one run (a project's
  `node_modules/.bin`, `target/debug`, a vendored toolchain). Repeated calls
  accumulate in priority order, ahead of the `PATH` fallback; resolution
  reuses the existing PATHEXT-aware `PATH` lookup, so a `.exe`/`.cmd`/`.bat`
  on Windows resolves the same way it would on `PATH`. Only affects a
  bare-name program (a path-form program is unaffected, unchanged) and never
  rewrites the child's own `PATH`; `Error::NotFound`'s `searched` includes
  these directories too when resolution fails. See `docs/commands.md`.
- `ProcessStdin::send_control(char)` — validates and writes a single control
  byte (e.g. `send_control('c')` for Ctrl-C, `send_control('d')` for Ctrl-D)
  to a child's stdin still held open via `keep_stdin_open()`/`take_stdin()`,
  so driving REPLs and other interactive tools no longer requires manually
  writing raw `\x03`/`\x04` bytes. This is just a byte written into a plain
  pipe, not a real terminal signal — genuine `SIGINT`/`SIGTSTP`-style delivery
  still requires a pseudo-terminal, which this crate does not yet provide.

### Changed
-

### Fixed
- `Priority` docs — the Unix privilege caveat ("lowering `nice` below its
  inherited value needs `CAP_SYS_NICE`/root, or the spawn fails as
  `Error::Spawn`") previously called out only `Priority::High`, but
  `Priority::AboveNormal` (`nice(-5)`) raises priority identically and was
  silently missing the same warning. `Priority::Normal`'s doc claimed setting
  it was unconditionally a no-op, which is false under a positively-niced
  parent (e.g. a `nice`d CI/batch launcher): `nice(0)` there is itself a
  privileged decrease from the inherited value. Docs on `Priority` and
  `Command::priority` now state the caveat accurately across all three
  variants; no behavior change.
- Windows `ProcessGroup::{suspend, resume}` no longer risk freezing an unrelated
  process when a job member's pid is recycled. The member-pid snapshot is taken
  before the system-wide thread snapshot, so a member (typically a handle-less
  grandchild) could exit and its pid be reused by a foreign process in that gap;
  its threads then surfaced under a pid still in the member set and passed the
  existing owner check. Each thread's live owner is now re-verified as *still a
  member of this job* (`IsProcessInJob`) immediately before `SuspendThread`/
  `ResumeThread`, closing the query→snapshot recycle window; any failure to open
  or query the owner is fail-safe (the thread is left alone).
- `wait_for` / `wait_for_port` now background-drain the child's piped
  stdout/stderr while polling, matching `wait_for_line`. Previously a child
  that wrote more than one OS pipe buffer (~64 KiB on Linux) of startup output
  before becoming ready would block in `write()`, and the probe would spin
  until its deadline and fail with a spurious `Error::NotReady` even though
  the child was alive and about to become ready. `wait` / `output_string`
  after a probe still see the full output; `output_bytes` and a fresh
  `stdout_lines` / `output_events` no longer compose with any of the three
  probes (same restriction `wait_for_line` already had).

## [2.1.1] - 2026-07-06

### Added
- `Error::{spawn, not_found, stdin}` — the remaining `#[doc(hidden)]` insulated
  constructors for the `#[non_exhaustive]` data-bearing variants that didn't get
  one alongside `Error::{exit, timeout, signalled}` in 2.1.0: `Spawn` and `Stdin`
  carry a `std::io::Error` and there is deliberately no `From<std::io::Error>`, so
  without a constructor a custom `ProcessRunner` double or cassette replay outside
  this crate could not build them at all. `Error::parse(program, message) ->
  Error::Parse` is added too, left **on the documented public surface** (not
  `#[doc(hidden)]`) since building an `Error::Parse` from a caller's own output
  parser — outside this crate's `try_parse` helpers — is a normal production path,
  not just a test-doubling convenience.

### Changed
-

### Fixed
-

## [2.1.0] - 2026-07-06

### Added
- `Command::timeout_opt(Option<Duration>)` — a composable timeout verb for
  config-driven call sites: `Some(d)` is exactly `timeout(d)`, `None` is exactly
  `no_timeout()` (deliberately unbounded, opting out of a client `default_timeout`
  gap-fill), folding the `match cfg { Some(d) => c.timeout(d), None => c.no_timeout() }`
  dance into one call. Internally the timeout is now modeled as a three-case type
  (unset / explicitly unbounded / a deadline) instead of a `bool` maintained next
  to an `Option<Duration>`.
- `Command::retry_never()` — an explicit per-command opt-out of a client
  `default_retry`, symmetric with `no_timeout()`. Runs the command exactly once
  and suppresses the gap-fill; tidier than, and behaviorally identical to, the
  `retry(1, Duration::ZERO, |_| false)` idiom.
- `Error::stdout_bytes() -> Option<&[u8]>` — the **exact** captured stdout bytes
  for a checking-verb error (`Error::Exit` / `Timeout` / `Signalled`) built over
  `output_bytes` (e.g. `output_bytes().await?.ensure_success()?`). Previously
  those bytes existed nowhere after a consuming verb ran: `stdout` on the error
  is a lossy UTF-8 decode, and the source `ProcessResult<Vec<u8>>` was already
  gone. `None` on the text path (`output_string`/`run`/`checked`/…), where the
  decoded `stdout` text is already complete and there is no separate raw form
  to recover.
- `LimitKind` / `LimitReason` (`limits` feature) — classify an
  `Error::ResourceLimit` failure by *which* limit (`Memory`/`Processes`/`Cpu`)
  and *why* (`Invalid`/`Unsupported`/`Unenforceable`) without parsing English
  text, via the new `Error::limit_kind()` / `limit_reason()` accessors.
  `reason` reflects a real backend signal: `Unsupported` when the platform has
  no whole-tree containment mechanism at all (macOS/BSD, or Linux with no
  cgroup v2 mounted), `Unenforceable` when a capable mechanism exists but this
  request was rejected (Linux cgroup delegation missing, a Windows Job Object
  call failing), `Invalid` for a nonsensical value caught before the OS is ever
  touched.
- `impl IntoCommand<R> for &[S; N]` — a reference to a fixed-size argument
  array (`args: [&str; N]`) now passes directly to a `CliClient` verb
  (`client.run(&args)`), matching the existing `[S; N]`/`Vec<S>`/`&[S]` impls.
  A `&[S; N]` doesn't unsize-coerce to `&[S]` in generic-parameter position, so
  the natural call previously failed to compile and needed a manual
  `&args[..]`. Purely additive.
- `Command::priority(Priority)` / `Priority` — launch a child at a lower (or
  higher) CPU-scheduling priority, for background/batch work that shouldn't
  starve the foreground (or a task that should win over it): `Idle` /
  `BelowNormal` / `Normal` / `AboveNormal` / `High`, mapped to
  `nice`/`setpriority` on Unix and a priority-class creation flag on Windows.
  Unlike the privilege builders (`uid`/`gid`), this never yields
  `Error::Unsupported` — both platforms cover every variant (`Priority::High`
  on Unix needs `CAP_SYS_NICE`/root to actually raise priority; the request
  itself is never rejected as unsupported).
- `Command::umask(u32)` — set the child's file-mode creation mask
  (`umask(2)`), controlling the default permissions of files it creates.
  Unix-only via `pre_exec`; `Error::Unsupported` on other targets rather than
  silently ignoring the requested mask (matching `uid`/`gid`/`setsid`).
- `LineTerminator` (`Newline` / `CarriageReturn`) plus
  `Command::line_terminator` / `stdout_line_terminator` /
  `stderr_line_terminator` — pick `\r` as the line boundary instead of `\n`
  (a bare `\r` terminates in `CarriageReturn` mode; a `\r\n` pair still counts
  as one terminator, whole or split across reads) for carriage-return progress
  output (`curl`/`pip`/`apt`-style `\rProgress: 50%\rProgress: 100%`), which
  previously accumulated as one ever-growing unstreamed line and could be
  dropped whole under a byte cap. Threaded through every sink that consumes "a
  line" — the streaming verbs, `stdout_tee`/`stderr_tee`,
  `on_stdout_line`/`on_stderr_line`, and `output_string`. Default (`Newline`,
  `\n`-only) is unchanged.
- `testing::DryRunRunner` — a `ProcessRunner` double that never spawns: it
  renders each command through the crate's own `Command::command_line`
  quoting and returns a synthetic successful result, the seam behind a tool's
  own `--dry-run`/`--echo` mode. Rendered invocations are available as a
  `RecordingRunner`-style collected snapshot (`commands()`/`only_command()`)
  and/or a live `on_invocation` callback — usable together or alone.
- `testing::Reply::with_stderr(text)` — attach stderr to a scripted reply,
  including a **successful** one (`Reply::ok("out").with_stderr("warning\n")`),
  so a test can model a CLI (`git`, a compiler, a linter) that writes warnings
  to stderr even on exit 0 — previously only expressible through the
  misleading `Reply::fail(0, "warning")`. Composes with every `Reply`
  constructor; on a failing reply it overrides the `fail`-supplied stderr.
- `Supervisor::give_up_when(classifier)` / `GiveUpAttempt` /
  `StopReason::GaveUp` — classify a crash (`GiveUpAttempt::Crashed`, a
  completed run) or a spawn/IO failure that never produced a result
  (`GiveUpAttempt::Failed`, e.g. `ENOENT` from a typo'd program name) as
  **permanent**, so the supervisor gives up instead of restarting it forever.
  Consulted only for a crash the policy would otherwise restart, ahead of
  `max_restarts` and the failure-storm guard. A `Failed` verdict has no
  `ProcessResult` to report and surfaces the classified error directly as
  `run()`'s `Err`; a `Crashed` verdict reports `StopReason::GaveUp`. Default:
  unset — a permanent failure restarts forever, matching prior behavior.
- Cassette (`record` feature) now records a **failed** invocation too: an
  `Err` from `output_string`/`start` (`Error::Spawn`/`NotFound`/`Stdin`/
  `OutputTooLarge`/`Unsupported`/`Io` — with its `ErrorKind` preserved by name
  — plus an `Other` fallback) is captured and reconstructed on replay, instead
  of recording nothing and surfacing `Error::CassetteMiss` in place of the
  original error. `Error::Cancelled` is deliberately never recorded — replay
  short-circuits on the replaying command's own token first, before ever
  consulting the cassette. The version check now accepts any format up to the
  one this build writes rather than requiring an exact match, so a cassette
  written before this field existed still loads and replays unchanged.

### Changed
- **Breaking:** the data-carrying struct variants of `Error` — `Exit`, `Timeout`,
  `Signalled`, `Spawn`, `NotFound`, `Parse`, `OutputTooLarge`, `Stdin`, and
  `ResourceLimit` (`limits` feature) — are now individually `#[non_exhaustive]`.
  A struct-literal construction or a field-exhaustive destructuring of any of
  these variants outside the crate no longer compiles; match on the variant
  (with `..` in the pattern) and read fields through the existing accessors
  (`program()`, `stdout()`/`stderr()`/`combined()`, `code()`, `signal()`,
  `is_*()`) or the `#[doc(hidden)]` constructors (`Error::exit`/`timeout`/
  `signalled`) instead. This is prep work for 2.0: it lets a future release add
  fields to any of these variants (e.g. a structured `ResourceLimit`, or raw
  bytes alongside the lossy-UTF-8 `stdout`/`stderr` strings) without another
  breaking change.
- **Breaking:** `Error::Exit`, `Error::Timeout`, and `Error::Signalled` each gain
  a new field, `stdout_bytes: Option<Vec<u8>>` — read it through
  `Error::stdout_bytes`, not by destructuring the variant directly (all three
  are `#[non_exhaustive]` — see above). The `#[doc(hidden)]` constructors
  (`Error::exit`/`timeout`/`signalled`) always build a text-path error
  (`stdout_bytes: None`); only a real checking verb over `output_bytes`
  populates it.
- **Breaking:** `Error::ResourceLimit { message: String }` (`limits` feature) is
  now `{ kind: LimitKind, reason: LimitReason, detail: String }` — fix a match
  `Error::ResourceLimit { message }` → `Error::ResourceLimit { detail, .. }` (or
  use the new `limit_kind()`/`limit_reason()` accessors instead of
  destructuring the `#[non_exhaustive]` variant at all).
- **Breaking:** an API-consistency batch —
  - `Error::OutputTooLarge`'s fields `line_limit`/`byte_limit` are renamed
    `max_lines`/`max_bytes`, matching the `OutputBufferPolicy` knobs they
    report (what you configure is now what you read back).
  - `ResourceLimits::memory_max` (field and builder, `limits` feature) is
    renamed `max_memory`, matching the `max_processes` word order.
  - `ProcessResult::output_contains_any` now takes
    `impl IntoIterator<Item = impl AsRef<str>>` instead of `&[&str]`, matching
    the crate's other multi-input builders (`Command::args`/`envs`,
    `Command::ok_codes`) — a bare array (`["a", "b"]`), a `Vec<String>`, or a
    slice all work directly, without an explicit `&`.
- **Breaking:** the 1.1.0-deprecated forwarding aliases are removed: fix
  `ProcessGroup::terminate_all` → `kill_all`, and `RunProfile::avg_cpu` →
  `avg_cpu_cores`.
- **Breaking:** `RunProfile::exit_code` (the field) is removed — it duplicated
  `outcome.code()`, which the crate's `code()` method already exposes; use
  `profile.code()` instead of `profile.exit_code`.
- **Breaking:** the flat crate-root re-exports of two `0.x` dependencies'
  vocabulary types — `Encoding` (from `encoding_rs`) and `StreamExt` (from
  `tokio-stream`) — move behind a new `processkit::prelude` module: fix
  `use processkit::Encoding` → `use processkit::prelude::Encoding`, and
  `use processkit::StreamExt` → `use processkit::prelude::StreamExt`. Keeps
  `use processkit::*` from pulling in a trait that collides with
  `futures::StreamExt`, and contains a future `0.x` major bump of either
  dependency to the `prelude` module instead of the whole crate surface.
- **Breaking:** `output_bytes` now honors the `OutputBufferPolicy` **byte**
  ceiling (`max_bytes`) on its raw stdout capture, not just the line-pumped
  stderr: `OverflowMode::Error` past the cap errors with
  `Error::OutputTooLarge` (`max_lines: None` — raw bytes have no lines), and
  the drop modes bound the retained bytes to a head/tail and set
  `ProcessResult::truncated`. The default (no byte cap) is unchanged — capture
  stays unbounded as before; bound a flooding child with `with_max_bytes(..)`
  or a `timeout`.
- `RecordReplayRunner` (`record` feature) no longer matches on `cwd`: a
  cassette recorded from one absolute working directory (a dev box, a tempdir)
  now replays against the same invocation run from a different one (a CI
  workspace, a teammate's checkout) instead of `CassetteMiss`ing — the leading
  blocker to recording on one machine and replaying on another. `cwd` is still
  stored on each entry, verbatim, for visibility; it just no longer
  discriminates two otherwise-identical recorded runs (an existing cassette
  that relied on that — two entries differing only in `cwd` — now collides,
  with the first-recorded entry answering for both). The on-disk format
  revision bumped to `3`, but this is not a compatibility gate: a cassette
  written by a previous build still loads and replays fine.

### Fixed
- `ScriptedRunner`'s bulk verbs (`output_string` and the helpers over it) now
  bound a `Reply::pending` call by the command's `timeout`, matching the live
  runner and the scripted `start` path: a pending reply with a `Command::timeout`
  but no cancel token resolves timed-out (`Outcome::TimedOut`) at the deadline
  instead of parking forever. A pending reply with neither a token nor a timeout
  still parks forever (a hung child no one can cancel and no deadline bounds), and
  the token path is unchanged. This makes the `Reply::timeout` doc's advice —
  "script `pending` and set a `Command::timeout` to model a deadline hang" — hold
  on the bulk verbs, not only `start`.
- `RecordingRunner::output_bytes` no longer falls through to the trait's
  `start`-based default: it now records the `Invocation` and delegates
  directly to the inner runner's own `output_bytes`, so wrapping a runner
  whose `output_bytes` behaves differently from its `start` (e.g.
  `RecordReplayRunner`'s honest `Error::Unsupported` for a lossy-UTF-8
  fixture) is honored instead of silently replaying through `start` and
  returning lossily re-encoded bytes.
- Pipeline stages now each spawn into their **own** kill-on-drop
  `ProcessGroup` sub-group instead of sharing one group across the whole
  chain: previously a per-stage `Command::timeout` reached only that stage's
  direct child, so a forking stage's grandchildren (`sh -c …`) survived the
  kill, kept the stdout pipe open, and stalled the downstream stage — the
  chain-level `Pipeline::timeout` was the only real backstop. Both a
  per-stage deadline and the chain-wide teardown now tear down the stage's
  whole subtree; behavior without a per-stage timeout is unchanged.
- A pipeline's checked stage failure now tears the rest of the chain down
  **proactively** instead of only passively through pipe EOF: previously a
  quiet upstream producer that never writes (and so never dies of a broken
  pipe) could hold a run open indefinitely after a downstream failure, while
  `collect()` awaited stages strictly in input order. The first checked
  failure now fires an internal teardown that kills every stage concurrently
  with the ordered gather; killed siblings are flagged `torn_down` and
  de-prioritized in the pipefail fold, so the stage that actually failed
  keeps the blame.
- A bare `finish()` (no preceding `stdout_lines()`) no longer pumps stdout
  into a sink built from the command's `OutputBufferPolicy` and enforces its
  overflow cap over output nobody asked to capture: it could fail loud with
  `Error::OutputTooLarge` on a run that `wait()` reports as successful for the
  same command. Likewise, `wait()`/`profile()` called after a dropped
  `stdout_lines()` stream no longer keep reusing the prior user-policy sink;
  both paths now route through an internal discard sink that neither retains
  lines nor enforces a cap.
- A shared-group streaming deadline watchdog's final `SIGKILL` no longer
  loses a race against `RunningProcess::Drop`: if a timed-out child caught
  the graceful signal, closed stdout, and kept running, the closed stream let
  the consumer drop its handle mid-grace, aborting the in-flight watchdog
  before the hard kill fired — the child then survived until the shared
  group itself was dropped. The graceful kill-and-reap now runs as a detached
  task that no `Drop`-triggered abort can reach, so the final `SIGKILL`
  always lands. Own-group and Windows paths are unaffected.

## [1.2.1] - 2026-07-04

### Added
-

### Changed
-

### Fixed
-

### Documentation
- Noted [`processkit-py`](https://pypi.org/project/processkit-py/), a Python
  (PyO3/asyncio) wrapper over this crate's core, in the README, the docs guide
  index, and the crate-level rustdoc.

## [1.2.0] - 2026-07-03

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
  duplicate key **last-registration-wins *within each channel*** (the static
  `default_env`/`default_env_remove` list, and the dynamic `default_env_fn` list),
  matching `Command::env`'s later-wins — previously first-registered won, opposite
  of the builder intuition. A later registration for a key supersedes and drops the
  earlier one in the same channel (a superseded `default_env_fn` resolver never
  runs). The cross-channel rule is unchanged and orthogonal: a static `default_env`
  for a key always beats a `default_env_fn` for it, regardless of order.

### Fixed
- Windows `suspend`/`resume` now verifies a thread's **owning process** (via
  `GetProcessIdOfThread`) before touching it, so a thread id recycled between the
  system-wide snapshot and `OpenThread` can no longer land a suspend/resume on an
  unrelated process's thread (C11).
- Linux cgroup-v2 detection also probes the systemd **hybrid** mount
  (`/sys/fs/cgroup/unified`), not only `/sys/fs/cgroup` — a hybrid host no longer
  falls back to the weaker process-group mechanism despite a usable v2 hierarchy (C5).
- A cgroup→process-group **containment downgrade** (unprivileged container,
  read-only `/sys/fs/cgroup`) now emits one `warn!` per process (`tracing` feature)
  — previously only debug traces + `mechanism()` polling surfaced it, so a
  `setsid`-escaping child could slip past an operator watching warn-level logs (C4).
- `ProcessResult`'s `Debug` no longer dumps stdout/stderr in full: a
  `panic!("{result:?}")` or tracing line on a multi-MiB capture previously printed
  it all. Now bounded to a preview (text) / byte-length summary (raw bytes), the
  same "no unbounded text in `Debug`" rule `Error` follows. (`Debug` output is not
  semver-covered; the impl is now concrete for `ProcessResult<String>` /
  `ProcessResult<Vec<u8>>`.)
- `Error::Timeout`'s `Display` no longer renders the misleading "timed out after
  0ns" when the deadline is unknown to the checking verb (a scripted / cassette-
  replayed timeout whose command carried no `timeout`) — it now reads just "timed
  out". A real, known deadline still shows the duration.
- `Outcome::code`/`signal` (and the cassette recorder, and the pipeline's clean/
  culprit classification) now match `Outcome` **exhaustively** instead of with a
  `_ => None`/`_ => false` wildcard, so a future `#[non_exhaustive]` variant is a
  compile error forcing a decision rather than silently misclassifying.
- The one-line error diagnostic tail no longer mangles a legitimate **tab** (`\t`)
  to `U+FFFD` — tabs are common column separators in tool output (TSV, `git diff`,
  `ls -l`) and are harmless in a one-line context; only genuinely display-unsafe
  controls (escapes, `CR`, bidi overrides) are still replaced.
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
- Streaming/pipeline honesty: documented that **carriage-return progress output**
  (`curl`/`pip`/`apt` — a `\r`-redrawn bar with no `\n`) is a single growing line
  that doesn't stream and is dropped whole under a byte cap (B4); that a pipeline
  **stage failure** tears the other stages down only *passively* (pipe EOF) and is
  collected in order, so a quiet upstream stage can delay a downstream failure —
  while token *cancellation* is proactive (F2); and that `output_all` yields **no
  partial results** on a mid-batch drop (F3).
- Teardown honesty (process-group / platform caveats): documented the adopt
  **pid-reuse hazard** (an individually-tracked adopted child remembered by pid can,
  if reaped elsewhere, alias a recycled pid at teardown — C3); that a graceful
  `shutdown` of a **suspended** group can't drain (frozen members don't run signal
  handlers — C7); that a **nested-PID-namespace** cgroup member reads as pid `0`,
  skips the graceful `SIGTERM` tier, and is reaped only by the final `cgroup.kill`
  (C8); that `PR_SET_PDEATHSIG` (`kill_on_parent_death`) is **cleared across a
  setuid/setgid `execve`**, so it's void for a `sudo …` child (C9); the Windows
  **spawn→assign window** that can leak a suspended orphan on abrupt parent death
  (C10); and that macOS/BSD `process_metrics` are un-implemented (not impossible —
  `libproc`/`proc_pidinfo` exist) rather than absent (C12). Also clarified that a
  Windows graceful `shutdown` is a **prompt hard kill** at the deadline — `signal`
  and the grace `timeout` are both ignored (no soft-signal tier to trigger a
  drain), so the "signal → grace → kill" tiers are Unix-only (C6).
- Guide/README/rustdoc sweep — corrected several inaccuracies verified against the
  source: `wait_any` **does** surface `Err(Cancelled)` mid-run; the ext verbs
  (`parse`/`try_parse`/`first_line`) are callable on `&dyn ProcessRunner` (they
  just can't form a `dyn ProcessRunnerExt` object); pipefail prefers a real culprit
  over a downstream `SIGPIPE` victim; an unchecked *last* stage preserves its real
  exit code (not a fabricated `0`) but a signal/timeout still surfaces; the
  `cli_client!` `core` field is module-private (not public); `profile` discards
  output like `wait`; `members()` is `process-control`-gated (not `stats`);
  Windows graceful shutdown honors `escalate_to_kill=false` (spares survivors);
  the retry verb list is seven (`run`/`run_unit`/`exit_code`/`probe`/`checked`/
  `parse`/`try_parse`); `run` requires an *accepted* exit (widened by `ok_codes`),
  not strictly `0`; `stats` adds no crate dependency (only a `windows-sys`
  sub-feature) while `mock`/`tracing`/`record` do; `Mechanism::CgroupV2` teardown
  falls back to a per-pid `SIGKILL` sweep pre-5.14 / on write failure; the
  crate-root "never leaks" guarantee is qualified (rides on `Drop`; `setsid`
  escapes the process-group mechanism); and a cookbook snippet shows the explicit
  `io::Error` → `Error::Io` map (no blanket `From<io::Error>`).
- `Error::Exit`/`Timeout`/`Signalled` stdout docs: corrected the claim that a
  raw-bytes error's exact bytes "remain on the originating `ProcessResult`" — a
  consuming checking verb (`run`/`ensure_success`) drops the result, so the exact
  bytes are not preserved on that path; inspect the `ProcessResult` yourself if you
  need them on failure.
- `Error::Signalled`: clarified it is **Unix-only** for real runs — a killed
  Windows process reports `Exit`, and a `Signalled` there arises only from a test
  double / cassette replay.
- `Signal::Other`: documented that `Other(0)` is the POSIX existence probe (not an
  `EINVAL`), and that an out-of-range signal's `EINVAL` surfaces only on the cgroup
  mechanism — the process-group mechanism (macOS/BSD, Linux fallback) swallows it.
- `ProcessResult::combined`: corrected the `\n`-separator condition (both streams
  non-empty) and clarified it is stdout-then-stderr **concatenation**, not a
  temporal interleaving.
- Fixed a self-contradiction in the cgroup zombie docs: an unreaped zombie does
  **not** appear in `cgroup.procs` (it leaves on exit, before reap — per the
  kernel's cgroup-v2 docs), so the stats backend does not zombie-over-count.
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

[Unreleased]: https://github.com/ZelAnton/ProcessKit-rs/compare/v3.0.2...HEAD
[3.0.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v3.0.1...v3.0.2
[3.0.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.3.2...v3.0.0
[2.3.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.3.1...v2.3.2
[2.3.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.3.0...v2.3.1
[2.3.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.2.5...v2.3.0
[2.2.5]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.2.4...v2.2.5
[2.2.4]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.2.3...v2.2.4
[2.2.3]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.2.2...v2.2.3
[2.2.2]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.2.1...v2.2.2
[2.2.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.1.1...v2.2.0
[2.1.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v1.2.1...v2.1.0
[1.2.1]: https://github.com/ZelAnton/ProcessKit-rs/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/ZelAnton/ProcessKit-rs/compare/v1.1.0...v1.2.0
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
