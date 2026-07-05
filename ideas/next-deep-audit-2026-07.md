# Deep audit 2026-07 — findings & fix plan

> **Status:** plan only — nothing fixed yet (по запросу: «не исправляй, только составь план»).
> **Method:** 9 parallel deep-audit passes over every `src/` module plus the full
> `docs/` guide set, followed by a manual verification pass on every serious
> finding (code read / refutation). Findings below are only those that survived
> verification; each carries a confidence marker:
> **[V]** = verified by direct code read during synthesis, **[VV]** = found
> independently by ≥2 auditors, **[C]** = compile-verified in a scratch crate,
> **[A]** = single-auditor find with quoted code (spot-checked plausible).
>
> Severity: **S** serious (wrong behavior / broken guarantee / hang / OOM),
> **M** moderate (real-world trap, doc-vs-code lie, silent degradation),
> **m** minor (polish, edge cosmetics).
>
> Breaking-change policy unchanged: anything breaking goes to
> `v2-breaking-changes.md`, additive/doc fixes land in 1.x.

---

## Theme A — shared-group handles: the `own_group: None` family

One root cause, three user-visible failures. `launch` (`src/runner.rs:557`)
sets `own_group: None` for every `ProcessGroup::start` run, and several
teardown paths silently require an owned group.

- **A1 [S][V]** `Command::timeout` is not enforced while streaming on a
  shared-group handle. `arm_stream_deadline` (`src/running/stream.rs:129-134`)
  arms the watchdog only when `backend.own_group()` is `Some`; the scripted
  fallback covers scripted backends only. `group.start(&cmd.timeout(5s))` →
  `stdout_lines()` → a quiet never-exiting child pends the loop **forever**,
  contradicting the `stdout_lines` doc ("at the deadline the process tree is
  killed, pipes close", stream.rs:60-63). The cancel watchdog *does* cover
  shared handles via a pid-only kill (mod.rs:400-413) — the asymmetry marks
  this an oversight, not a design choice.
  *Fix direction:* mirror the cancel watchdog — arm a pid-only deadline kill
  (`kill_direct_child`/`start_kill`) when `own_group` is `None`, and state the
  "direct child only, not the whole tree" limitation in the shared-group docs.
- **A2 [S][V]** `ProcessRunnerExt::first_line` timeout on a shared group kills
  nothing: on `Err(_elapsed)` it returns `Error::Timeout` and drops the
  `RunningProcess` (`src/runner.rs:269-279`), whose `Drop` only aborts pump
  tasks. The in-code comment (`runner.rs:261` "drop on timeout tears the tree
  down") and the doc (`runner.rs:233-234` "and tears the tree down", explicitly
  recommending `&ProcessGroup`) are both false for shared groups. Scenario:
  readiness-probing a server — probe times out, caller believes the server was
  torn down, it keeps holding its port.
  *Fix direction:* same seam as A1; also fix the two comments.
- **A3 [S][A]** Pipeline per-stage timeout/cancel kills **only the direct
  child** in a shared group (`src/running/mod.rs:1304-1316` "Shared group:
  terminate only our direct child"). A stage child that forks (`sh -c …`)
  leaves a grandchild holding the stdout write-end → downstream never sees EOF
  → `collect` (awaits stages in input order) never completes →
  `Pipeline::output_string` hangs indefinitely unless a whole-chain
  timeout/token was also set.
  *Fix direction:* per-stage sub-groups, or document loudly that per-stage
  deadlines in pipelines reach the direct child only and recommend a chain
  timeout; at minimum make `collect` bail out once a stage reports
  timeout/cancel instead of awaiting upstream EOF propagation.

## Theme B — unbounded memory on "bounded" paths

- **B1 [S][V]** `output_bytes` accumulates stdout in an **uncapped**
  `Vec<u8>` (`src/running/mod.rs:650-673`); the configured
  `OutputBufferPolicy` (incl. `fail_loud`) is applied to **stderr only**
  (line 632). Multi-GB stdout → parent OOM; the fail-loud ceiling never fires
  for the very stream the caller is capturing. The verb doc never states the
  exemption.
  *Fix direction:* honor `max_bytes` in the byte pump (count + stop/error), or
  document the exemption in `output_bytes` and `OutputBufferPolicy` docs as an
  explicit contract. Honoring it is preferable and non-breaking (new failure
  only where OOM loomed).
- **B2 [S][V]** The internal discard sink for `wait()`/`profile()` is
  `OutputBufferPolicy::bounded(0)` **without `max_bytes`**
  (`src/running/mod.rs:932`), and the pump's in-flight cap comes from
  `byte_cap()` = `max_bytes` only (`src/pump.rs:170-172, 423`); with `cap ==
  None` the over-cap skip guard (pump.rs:493) is dead and a newline-free
  stream grows `pending` without bound (`None => break`, pump.rs:500).
  `cmd.wait()` on `base64 -w0`-style output → O(total) heap, contradicting
  the in-code intent comment (mod.rs:929-931).
  *Fix direction:* give the discard policy a small `max_bytes` (e.g. 64 KiB).
- **B3 [M][A]** Bare `finish()` without prior streaming pumps stdout into a
  sink built from the **user's policy** (default unbounded) and retains every
  line nobody will read (`src/running/stream.rs:194-204`); under `fail_loud`
  it errors `OutputTooLarge` for output the caller never asked to capture —
  where `wait()` succeeds. Related: **[m]** after `stdout_lines()` → drop
  stream → `wait()`/`profile()`, the existing user-policy sink is reused
  (mod.rs:937-944), defeating the discard optimization.
  *Fix direction:* `finish()` without a live stream should use the discard
  sink (it returns no stdout); reset the sink when the stream is dropped.
- **B4 [M][A]** `\r`-only progress output (curl/pip/apt) is a single
  ever-growing "line" (split is `pending.find('\n')` only, pump.rs:468):
  nothing streams live, the whole progress stream is one string in memory,
  and with a byte cap the whole line is over-cap → dropped wholesale (user
  sees zero progress output). `buffer.rs` documents newline-free floods but
  never the very common CR-progress case.
  *Fix direction (1.x):* document in `buffer.rs`/`stdout_lines`; *(v2 or
  additive knob):* optional `\r` as line terminator.
- **B5 [M][A]** Per-line extraction `pending.drain(..=nl).collect::<String>()`
  (pump.rs:474) memmoves the entire remaining tail per line and builds the
  `String` char-by-char: ~2000× write amplification on short-line floods.
  *Fix direction:* two-pass split per chunk / index-based subslice copies.

## Theme C — pgroup/teardown honesty (macOS primary, Linux fallback)

- **C1 [S][VV→V]** `escalate_to_kill(false)` + `shutdown_ref()` permanently
  disarms kill-on-drop **even when the tree fully drained**:
  `else if !escalate { skip_drop_kill.request(); }`
  (`src/sys/graceful.rs:74-78`) — one-shot latch, no reset, honored by every
  backend `Drop`. The doc (`group.rs:429-430`) promises "the group stays
  usable … and its `Drop` still backstops any straggler": children spawned
  *after* that shutdown orphan silently. Confirmed by two auditors + code read.
  *Fix direction:* set the latch only when survivors actually remain
  (`!target.is_drained()`), and/or clear it on the next spawn into the group.
- **C2 [S][V]** `Tracked::signal_all` ignores every `killpg`/`kill` errno
  except the ESRCH-fallback branch (`src/sys/pgroup.rs:151-181`); `kill_all()`
  is `broadcast(SIGKILL); Ok(())`. An **EPERM** delivery failure (tree ran
  `sudo`/`pkexec` — real+saved uid changed) reads as success from `kill_all`,
  graceful escalation *and* `Drop`. The cgroup backend deliberately surfaces
  exactly this (`linux.rs:585-593`) — the pgroup backend contradicts it.
  Consequence: silent live orphaned tree on macOS/BSD (primary mechanism) and
  the Linux fallback. `group.rs:257-261` frames the pgroup gap as
  drain-reporting only — not delivery failure.
  *Fix direction:* collect non-ESRCH errnos in `signal_all`, surface the last
  one from `kill_all`/`hard_kill` like the cgroup path; document EPERM.
- **C3 [M][VV]** Post-reap pid reuse at Drop: an entry whose tree exited long
  ago (never pruned — no intervening probe) passes the `kill(-id, 0)` probe if
  the pid was recycled into a new group leader, and gets SIGKILLed; `solos`
  (adopted children) alias on **any** reuse. The internal comment admits it
  ("likelier on macOS's small pid space"), but public `adopt`/`kill_all` docs
  warn only about the unreaped-zombie case, never the post-reap recycled-pid
  kill, and the stale-entry window is unbounded, not "a few instructions".
  *Fix direction:* document honestly on `adopt`/`kill_all`/Drop; consider
  pruning `solos` entries once the caller reaps (hook into handle reap), or
  pid+start-time identity where available.
- **C4 [M][A]** Silent mechanism degradation: cgroup→pgroup fallback (the
  default inside unprivileged containers — read-only `/sys/fs/cgroup`) is
  reported only via `mechanism()` polling and **debug**-level traces
  (runner.rs:523-527, group.rs:437-443). A daemonizing child then escapes
  teardown and warn-level logging captures nothing.
  *Fix direction:* one `warn!` per process (once-latch) on first degradation,
  keep per-spawn at debug.
- **C5 [M][A]** cgroup-v2 detection is hardcoded to
  `/sys/fs/cgroup/cgroup.controllers` (`linux.rs:391-397`) — systemd
  **hybrid** hosts (`/sys/fs/cgroup/unified`) and nonstandard v2 mounts fall
  back to pgroup despite a usable v2 hierarchy.
  *Fix direction:* also probe `/sys/fs/cgroup/unified`; or parse
  `/proc/self/mountinfo` for a cgroup2 mount.
- **C6 [M][A]** Windows `graceful_shutdown` gives **zero** grace: `escalate=
  true` → `TerminateJobObject` immediately (no drain-poll up to `timeout`);
  `escalate=false` → no-op + (C1) permanent Drop disarm. Cross-platform
  "TERM → grace → KILL" code silently becomes "instant kill" — data-losing
  for children that flush on shutdown.
  *Fix direction:* poll for natural exit up to `timeout` before
  `TerminateJobObject` (honors grace for self-exiting trees); docs already
  admit the no-soft-signal gap but not the no-drain-wait gap.
- **C7 [m][A]** Graceful shutdown of a `suspend()`-ed group can never drain
  (frozen tasks don't run TERM handlers; SIGSTOP'd members keep it pending) —
  always burns full timeout then hard-kills (or spares frozen survivors under
  `escalate=false`). *Fix:* thaw before `signal_all`, or document.
- **C8 [m][A]** Nested-pid-namespace members read as pid `0` in
  `cgroup.procs` and are filtered out (`linux.rs:546-558`) — they get no
  graceful signal, only the final `cgroup.kill`. *Fix:* document.
- **C9 [m][A]** `PR_SET_PDEATHSIG` is cleared on exec of setuid/setgid
  binaries — `kill_on_parent_death` silently void for `sudo …` children; the
  otherwise thorough caveat list (command.rs:305-324) omits it. *Fix:* doc.
- **C10 [m][A]** Windows spawn-to-assign window: abrupt parent death between
  `CREATE_SUSPENDED` spawn and job assignment leaks a permanently-suspended
  orphan. Inherent; absent from the "kernel kills the tree even on abrupt
  parent death" headline (windows.rs:61-66). *Fix:* one honest sentence.
- **C11 [m][A]** Windows suspend/resume walk: tid recycled between snapshot
  and `OpenThread` can suspend a foreign process's thread (guard covers only
  stale-unrecycled tids). Low probability, high blast radius. *Fix:* verify
  thread's owning pid via `GetProcessIdOfThread` before acting.
- **C12 [m][A]** macOS `process_metrics` returns `None` claiming "no /proc",
  but `proc_pidinfo(PROC_PIDTASKINFO)` exists — capability gap presented as
  impossibility (`unix.rs:110-114`). *Fix (later):* implement via libproc, or
  reword docs to "not implemented".

## Theme D — cassette / doubles fidelity (tests pass, production fails)

- **D1 [S][VV→V]** Cassette `Entry` stores no `truncated`/totals
  (`cassette.rs:143-165`), replay rebuilds via `ProcessResult::new`
  (`truncated: false`, result.rs:155): a recording clipped by a bounded
  buffer replays as un-truncated — record-time `run()`/`parse()` fails loud
  `OutputTooLarge`, replay silently feeds the clipped tail. `PartialEq`
  excluding `truncated` masks it. *Fix:* record `truncated` (+ totals) with
  `#[serde(default)]` — old cassettes stay readable.
- **D2 [S][V]** Replay mode never checks the cancellation token (both verbs
  go straight to the slot, `cassette.rs:475-504`); real runner and
  `ScriptedRunner` both short-circuit pre-spawn. "Token already fired ⇒ must
  cancel" logic passes its replay test and fails live. *Fix:* mirror the
  pre-spawn check in both `Mode::Replay` arms.
- **D3 [S][A]** `Reply::into_result` hands canned stdout **verbatim** while
  the real bulk path joins decoded lines (strips trailing `\n`, normalizes
  CRLF): `Reply::ok("done\n")` ⇒ `"done\n"` via fake bulk verb, `"done"` via
  real — and the crate's own tests show the same reply differing **by verb**
  on one double (`doubles.rs:1015`). *Fix:* normalize canned text through the
  same line-join in `into_result`, or document loudly.
- **D4 [S][A]** Start-based double paths double-decode canned text:
  `from_scripted` wires `command.out_encoding()` into the pumps while the
  feeder writes the canned `String`'s UTF-8 bytes — `.stdout_encoding(UTF_16LE)`
  + cassette/scripted `start` ⇒ garbage; same cassette via `output_string` is
  correct. *Fix:* feeder must encode canned text with the command's encoding,
  or scripted pumps must force UTF-8.
- **D5 [S][V]** Cassette match key includes **verbatim `cwd`** and the
  `from_file` stdin digest hashes the **absolute path**
  (`cassette.rs:150-153, 179`): recordings made in a tempdir/CI workspace
  miss on every other machine (and every next run for tempdirs) with opaque
  `CassetteMiss`. *Fix (1.x):* document + recommend stable relative cwd;
  *(design, maybe 1.x additive):* opt-in key normalization
  (e.g. `match_cwd(false)` builder on the runner).
- **D6 [M][A]** `write_cassette` truncates in place, drop-path flush swallows
  errors (`let _ = self.save();`): crash mid-save destroys the previously
  good cassette. *Fix:* temp file + atomic rename; surface drop-save failure
  at least via tracing.
- **D7 [M][A]** `ScriptedRunner` never consumes `stdin_source()` — an
  app-level re-run of a one-shot-stdin command passes the fake, fails live
  with `OneShotConsumed`. *Fix:* fake should call `take_for_run` too.
- **D8 [M][A]** Doubles can't express spawn-side failures: rule miss ⇒
  `Spawn{NotFound}` with `is_not_found() == false`; `Reply` can't script
  `Error::NotFound`/bad-cwd. "Tool not installed → fallback" branches are
  untestable. *Fix:* `Reply::not_found()` / `Reply::spawn_error(..)` ctors.
- **D9 [M][A]** Replay `output_string` skips the non-piped-stdout guard that
  both the real path and `ScriptedRunner` enforce — and is internally
  inconsistent with its own `start()` path. *Fix:* add the guard.
- **D10 [M][A]** Env excluded from the match key ⇒ recordings differing only
  in env (e.g. `LC_ALL`) collide on one slot and serve by call order —
  silently wrong output. *Fix:* document the hazard prominently; consider
  opt-in env-sensitive keying.
- **D11 [M][A]** Record-mode errors record nothing ⇒ replay yields
  `CassetteMiss` where record-time code handled `NotFound` etc. — error
  *variant* changes between record and replay. *Fix:* document; consider
  recording typed errors (v2-ish design).
- **D12 [M][VV]** Zero duration fidelity: replayed `duration() == ZERO`, a
  recorded `TimedOut` resolves instantly, and without `.timeout()` on the
  replaying command `ensure_success` renders "timed out after **0ns**"
  (`result.rs:237` `timeout.unwrap_or_default()`). *Fix:* store elapsed in
  `Entry` (`serde(default)`), apply via `with_duration`; render "timed out
  (deadline unknown)" when `timeout` is `None`.
- **D13 [m]** Batch of small ones: version gate runs after full deserialize
  (friendly message lost); no `deny_unknown_fields` note for forward compat;
  keep-stdin-open recording doc describes a hang that actually completes with
  EOF; `Rule::Prefix` exact-`OsStr` match vs Windows case/extension
  resolution (`git` vs `git.exe`); shadowed-rule diagnostic only under
  `tracing` feature; record entries land in completion order + two recorders
  on one path clobber each other. *Fix:* docs + cheap guards.

## Theme E — retry / supervisor / cancellation interplay

- **E1 [M][VV]** Backoff sleeps are not raced against the cancel token —
  `retrying`'s `tokio::time::sleep(delay)` (`runner.rs:335`) and the
  supervisor's `sleep_backoff`/storm pause (`supervisor.rs:402-486`).
  Cancellation is noticed only at the *next* attempt's pre-spawn check: a
  30-60 s backoff delays `Err(Cancelled)` by that much, despite docs advising
  "bound the total with `cancel_on`" and supervisor's "returns that
  `Error::Cancelled` immediately". *Fix:* `select!` the sleep against the
  token in both places.
- **E2 [S→doc][VV×3 → V]** `Command::retry` doc (command.rs:515-522) promises
  a one-shot-stdin retry "fails loud with `Error::Io` (InvalidInput) on the
  second attempt" — in reality `retrying` returns the **first attempt's error
  with zero retries** (`runner.rs:302-320`, test pins it). Retry policy is
  silently inert for `from_reader`/`from_lines`, including for retryable
  spawn failures where the payload was never touched. Guides repeat the wrong
  claim (`docs/timeouts-and-cancellation.md:126-129`, `docs/commands.md:108`).
  *Fix:* correct rustdoc + both guides ("never retried; first error returned
  as-is"); add a `tracing` note when retry is suppressed; consider allowing
  retry when the failure was pre-consumption (spawn error) — additive.
- **E3 [M][A]** Supervisor backoff exponent = lifetime restart count, never
  resets after a healthy run: an hourly-clean-exit worker under
  `RestartPolicy::Always` reaches the 30 s cap in ~8 cycles and stays there
  forever. systemd/suture (whose storm design is borrowed) reset on healthy
  runtime. *Fix:* reset the exponent when a run outlives `failure_decay` (or
  a dedicated `healthy_after` knob) — additive behavior change, document it.
- **E4 [M][A]** Permanent errors restart forever under defaults (`OnCrash` +
  `max_restarts: None`): ENOENT spawn errors hammer at the cap indefinitely;
  the storm guard paces but never ends. *Fix:* document prominently; consider
  an additive `give_up_when(classifier)` — record in ideas if deferred.
- **E5 [m][A]** Supervisor jitter applied **after** the cap ⇒ a single delay
  reaches 1.5×`max_backoff` (45 s at defaults), contradicting "cap any single
  backoff delay"; `RetryPolicy`'s jitter never exceeds its cap. *Fix:* clamp
  after jitter, or fix the doc; align the two engines' documented behavior.
- **E6 [m][A]** `multiplier = +∞`: `RetryPolicy` passes it through (every
  delay ⇒ cap; a test pins it) while its own doc says "non-finite treated as
  1.0", and the Supervisor maps the same input to 1.0 (constant base). Same
  knob, contradictory doc, opposite schedules. *Fix:* make the doc match the
  (reasonable) `RetryPolicy` behavior and align the Supervisor, or vice
  versa — pick one semantic.
- **E7 [M][V]** `first_line` re-evaluates the token **after** the stream
  ends (`runner.rs:285-288`): a no-match run whose child exited cleanly is
  reclassified `Err(Cancelled)` if the token fires between stream end and the
  check — the exact hazard the crate's own `wait_exit` regression forbids.
  *Fix:* snapshot cancellation state before/at stream end (mirror
  `cancel_at_exit`).

## Theme F — pipeline correctness

- **F1 [S][V]** `pipefail` attribution drops the failing stage's `ok_codes`:
  rebuilt via `ProcessResult::new` ⇒ default `vec![0]`
  (`pipeline.rs:427-440`, `result.rs:158`). A rejected-zero stage
  (`ok_codes([1])`, exited 0) is *classified* failed but the rebuilt result
  reports `is_success() == true` — `run`/`checked`/`probe` return Ok for a
  chain the fold deemed failed. The sibling no-failure branch (446-449)
  handles `ok_codes` carefully — clear oversight. *Fix:*
  `.with_ok_codes(stage.ok_codes.clone())` in the attribution branch.
- **F2 [M][A]** Stage failure/cancel teardown is entirely passive (EOF /
  broken-pipe propagation); `collect` awaits stages in input order, so a
  cancelled/failed stage's error surfaces only after upstream stages exit
  naturally — a quiet producer keeps the run pending arbitrarily long, and
  the `cancel_on` doc claims proactive teardown that doesn't happen
  (`pipeline.rs:129-137`). *Fix:* on first stage error, drop/kill the group
  (or at least the upstream handles); fix the doc meanwhile.
- **F3 [m][A]** `output_all` mid-batch drop discards results of
  already-completed commands; no partial recovery. *Fix:* document (the
  cancel-safety note covers processes, not results).

## Theme G — API design gaps (all additive fixes)

- **G1 [S][C]** No `impl ProcessRunner for Box<dyn ProcessRunner>` /
  `Arc<dyn ProcessRunner>` (only `&R`, runner.rs:86) — a runtime-selected
  runner (config: real vs cassette) cannot be stored in
  `CliClient`/`Supervisor` state; compile-verified failures. *Fix:* add the
  two forwarding impls (pure additive).
- **G2 [M][A]** Client `default_env`/`default_env_fn` silently overrides a
  per-command `inherit_env(["K"])` and pierces `env_clear()` isolation —
  `has_env_override` scans only `self.envs` (command.rs:848-861), violating
  the documented "its own explicit settings win". *Fix:* teach
  `has_env_override` about the `inherit_env` allow-list and `env_clear`.
- **G3 [M][A]** Duplicate `default_env` registrations resolve
  first-registered-wins — opposite of `Command::env`'s documented
  later-wins; undocumented. *Fix:* make later registration replace earlier
  (matches builder intuition), or document.
- **G4 [M][A]** Gap-fill defaults are impossible to opt out of per command:
  no `no_timeout()`, no way to express "explicitly unbounded" against a
  client `default_timeout`. *Fix:* additive `Command::no_timeout()` (or
  `timeout_opt(Option<Duration>)`); same consideration for
  retry (`retry_never()`) — record whichever is deferred.
- **G5 [M][A]** `stdout_tee`/`stderr_tee` writers are shared through `Clone`
  (Arc<Mutex<…>>) and appended by every retry attempt — three cloned jobs
  interleave into one log; retries fuse failed-attempt output. Undocumented
  (only `cancel_token` sharing is). *Fix:* document on tee + Clone docs;
  consider per-attempt delimiters later.
- **G6 [m][A]** `IntoCommand for Command` grafts a client's defaults onto a
  command for a *different program* (`git_client.run(Command::new("rsync"))`
  compiles and runs). *Fix:* document the trap; optional debug_assert on
  program mismatch.
- **G7 [m][A]** `ok_codes` with an empty set **resets** to `{0}` while the
  doc says "ignored" (command.rs:443-448). *Fix:* make code match doc (keep
  previous set) — doc-compatible bugfix.
- **G8 [m][A]** `pub use tokio_stream::StreamExt` (+ `encoding_rs::Encoding`)
  flat at crate root: glob-import collisions and semver coupling to 0.x deps.
  *1.x:* document; *v2 tracker:* move behind a `prelude` module or re-export
  under a distinct name.
- **G9 [m][A]** Windows `quote_arg` mis-handles backslash-before-embedded
  quote (`a\"b` → re-parses as `a\b`) in `command_line()` display. *Fix:*
  double backslashes preceding an escaped quote.
- **G10 [m][A]** `Command`'s manual `Debug` omits `groups`/`timeout_grace`/
  `timeout_signal`/`ok_codes`/tees yet uses `finish()` — use
  `finish_non_exhaustive()` and add the security-relevant `groups`.
- **G11 [m][A]** `retry(0, …)` runs once (== `retry(1, …)`); doc's "total
  attempts" reading promises zero. *Fix:* one doc sentence.

## Theme H — error/result semantics & Display polish

- **H1 [M][A]** `ProcessResult` derives `Debug` with full stdout/stderr —
  violates the crate's own "no unbounded text in Debug" invariant (the
  stated reason `Error`'s Debug is manual). `panic!("{result:?}")` on a
  100 MB capture dumps it all. *Fix:* manual Debug with `StreamPreview`
  (Debug output is not semver-covered).
- **H2 [M][A]** `Outcome::code()`/`signal()` use `_ => None` wildcards on a
  `#[non_exhaustive]` enum whose docs anticipate new variants — a future
  variant silently returns `None` from both. Same class in cassette
  `Entry::from_result` (a future variant records as "killed by unknown
  signal"). *Fix:* exhaustive matches (mirror `Error`'s accessors).
- **H3 [M][VV]** "timed out after 0ns": `ensure_success`/`require_code`
  render `timeout.unwrap_or_default()` (result.rs:237, 267) — reachable via
  cassette replay & scripted timeouts (see D12). *Fix:* omit the duration
  clause when `timeout` is `None`.
- **H4 [M][A]** `Error` stream-field docs promise "the exact bytes remain on
  the originating `ProcessResult`", but `ensure_success` consumes `self` and
  drops the original on the Err path — after `output_bytes().await?.
  ensure_success()?` fails, the exact bytes exist nowhere. *Fix:* reword the
  promise (or return the result in the error — v2 design note).
- **H5 [m]** Small ones: `combined()` doc omits the `!err.is_empty()`
  condition on the `\n` insert (Error's doc has it right) and neither doc
  says "concatenation, not temporal interleaving"; `Error::Signalled` doc
  implies Windows can produce it live (it can't — doubles/cassette only);
  diagnostic tail uses `str::lines()` so `\r`-progress shows the *oldest*
  frame; `is_display_unsafe` treats `\t` as control (U+FFFD in legit
  output); `Error::diagnostic` doc's `None` list incomplete;
  `ProcessResult::diagnostic` returns `""` where `Error::diagnostic` returns
  `None`; `ensure_success` clones every field it could move;
  `ResourceLimit` Display unbounded vs bounded Debug. *Fix:* doc/impl
  touch-ups, one pass.
- **H6 [M][A]** `Signal::Other` doc promises EINVAL surfacing, but the
  pgroup backend swallows all errno (C2) and `Other(0)` is an existence
  probe that "succeeds" delivering nothing; cgroup backend surfaces errors —
  platform-divergent results for the same call, undocumented
  (`signal.rs:38-42`). *Fix:* piggyback on C2 + docs.
- **H7 [M][A]** Contradictory zombie claims: `group.rs:404-406` ("cgroup
  immune: leaves `cgroup.procs` on exit, before reaping") vs
  `linux.rs:248-254` ("retains unreaped zombie's pid until parent reaps") —
  at most one is true; `stats.rs:29` inherits it. *Fix:* determine
  empirically (kernel: cgroup.procs lists zombies until reap — the
  linux.rs comment is very likely right), fix the wrong doc.

## Theme I — guide/README corrections (docs-only, one sweep)

All verified against source by the docs auditor (two compile-checked):

- **I1 [S]** `docs/timeouts-and-cancellation.md:180` — wait_any mid-run
  cancel row is wrong: it **does** surface `Err(Cancelled)` (regression test
  pins it).
- **I2 [S][C]** `docs/testing.md:43-44` + rustdoc `runner.rs:192-198,242-247`
  — "parse/try_parse/first_line unavailable on `&dyn ProcessRunner`" is
  false (compiles fine); reword to "not callable through a `dyn
  ProcessRunnerExt` object".
- **I3 [M]** `docs/pipelines.md:150-158` — unchecked **last** stage carve-out
  (timeout/signal still surface; real code preserved, not "code 0").
- **I4 [M]** `docs/pipelines.md:84-87` (+README, cookbook, and
  `result.rs:165` rustdoc) — pipefail prefers a non-SIGPIPE culprit over an
  earlier SIGPIPE victim; "first stage that didn't exit cleanly" is
  incomplete.
- **I5 [M]** `docs/testing.md:332` — `cli_client!`'s `core` field is
  module-private, not "public" (compile-verified E0616).
- **I6 [M]** `docs/streaming.md:31` — `profile` discards output (like
  `wait`), doesn't "capture".
- **I7 [M]** `docs/platform-support.md:64-68` — `members()` is
  `process-control`-gated, not under the "(`stats` feature)" heading.
- **I8 [m]** pipelines.md:185 cancel_on is gap-fill not "every stage";
  commands.md:253 retry verb list incomplete (7 verbs);
  process-groups.md:97/platform-support.md:48 Windows "atomic kill"
  unconditional (escalate=false spares); cookbook.md:152/430 snippets use
  `?` on `io::Result` without a compatible error context (no blanket
  `From<io::Error>` — deliberate D13).
- **I9 [m]** lib.rs: crate-root "no orphans"/"never leaks" unqualified
  (panic="abort", setsid-vs-pgroup, abrupt-death is Windows-only — the
  honest versions live on ProcessGroup/Mechanism pages); "stats is the one
  feature with an extra dependency" is wrong both directions (stats adds
  none; mock/tracing/record do); run vocabulary "require a zero exit" vs
  ok_codes widening; `cli_client!` doc points to a crate-root example that
  doesn't exist; `Mechanism::CgroupV2` "torn down via cgroup.kill"
  unqualified (pre-5.14/write-failure sweep fallback).

## Checked and refuted / verified-clean (do not re-investigate)

- **Refuted:** "`group_seen` fork race" (pgroup.rs:262) — std's Unix `spawn()`
  blocks on the CLOEXEC pipe until the child's exec resolves (that's how
  ENOENT is reported synchronously), and `setpgid` runs before exec in
  `do_exec`; by `spawn()`-return the group exists. The latch seeding is
  sound; the in-code "between-fork-and-exec window" applies only to
  observation windows *inside* the child pre-exec sequence, which the parent
  never observes.
- Verified-clean highlights: retry+one-shot-stdin single-attempt behavior is
  tested; timeout arbiter CAS races; SharedLines wakeup protocol;
  PID-reuse guards while a handle is live; OutputTooLarge never kills the
  child and never backpressures; stdin EPIPE handling (incl. Windows 109/232);
  half-close semantics; limits validated before spawn; Windows Job handle
  hygiene (every open closed, error capture before CloseHandle);
  feature-powerset compiles; cassette file-permission story (0600,
  O_NOFOLLOW, 64 MiB cap); supervisor gate ordering; all internal doc links.

---

## Suggested execution order (each stage: plan → implement → ≥2 review passes → push → CI)

1. **Correctness quartet (S, code):** F1 pipefail ok_codes; B1 output_bytes
   cap; B2 discard-sink cap; E7 first_line cancel snapshot. Small, isolated,
   test-backed.
2. **Shared-group deadline family (S, code):** A1 + A2 (one seam: pid-only
   deadline kill when `own_group` is None) + comment/doc fixes; A3 at least
   doc + collect bail-out decision.
3. **Teardown honesty (S/M, code+docs):** C1 latch-only-when-survivors;
   C2 EPERM surfacing; C4 warn-once degradation; C6 Windows drain-poll;
   C3/C7-C11 docs.
4. **Cassette/doubles fidelity (S/M):** D1 truncated field; D2 cancel check;
   D9 non-piped guard; D6 atomic write; D12 duration + H3 0ns rendering;
   D3/D4 normalization decision; D5/D7/D8/D10/D11/D13 docs + small ctors.
5. **Retry/supervisor cancellation (M):** E1 select-vs-token (runner +
   supervisor); E2 doc corrections + tracing note; E3 backoff reset decision;
   E5/E6 jitter/multiplier alignment; E4 doc.
6. **API additions (G, additive):** G1 Box/Arc impls; G2 inherit_env/env_clear
   awareness; G4 no_timeout; G3/G5-G11 per item (docs or tiny code).
7. **Error/result polish (H):** H1 manual Debug; H2 exhaustive Outcome
   accessors; H4/H5/H6/H7 doc+impl sweep.
8. **Docs sweep (I):** all guide/README/rustdoc corrections in one commit.
9. **v2 tracker append:** G8 prelude isolation; D11 typed-error recording;
   H4 error-carries-result design; B4 `\r` line-terminator knob (if not
   additive); anything from above deliberately deferred.

Non-breaking throughout; where a fix changes observable behavior it is a
bugfix toward the documented contract (or the doc is fixed instead —
decided per item at implementation time).
