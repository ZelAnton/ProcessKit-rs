# Whole-codebase inspection — 2026-06-10

Five independent review passes (async/concurrency, platform/sys + unsafe, API semantics,
security, architecture) over `main` @ `rokvrupv` (91c2f9ab). Findings only — **no fixes
applied**. Serious findings were re-verified against the code by hand; file:line refs are
to this revision.

Severity: **SERIOUS** = data loss / hang / kill of an innocent process / secret leak /
silent wrong result. **MODERATE** = wrong result or resource damage in a realistic edge
case. **LOW** = theoretical, hard to hit, or cosmetic-contract. **DESIGN** = pre-1.0
shape decisions (no users yet — cheap now, breaking later).

---

## SERIOUS

### B1. Streaming watchdogs SIGKILL a recycled PID after a non-consuming reap (Unix)
`src/running/stream.rs:404-409` (`kill_via_weak` → `kill_direct_child`), armed at
`stream.rs:102-145`; gap at `src/running/mod.rs:594-596` (`wait_exit`) and
`mod.rs:1018-1027` (`has_exited_now`/`try_wait`).
`stdout_lines`/`output_events` arm deadline/cancel watchdogs holding the **raw pid**.
`wait_any`/`wait_all`/readiness probes **reap** the child without `abort_watchdogs()`
(only `drive_to_exit` at `mod.rs:842-844` and `Drop` abort them). Handle kept alive →
minutes later the deadline elapses → `kill(recycled_pid, SIGKILL)` on an unrelated
process. Unbounded staleness window on Linux/macOS (Windows is shielded by the live
`Child` handle pinning the pid).

### B2. `cancel_on` does not kill the tree unless a consuming verb / stream watchdog is active
Doc contract `src/command.rs:434-437` ("cancelling it kills the process tree…").
Implementation: the token is raced only inside `drive_to_exit_inner`
(`src/running/mod.rs:916-958`) and the stream watchdogs (`stream.rs:134-145`, own-group
only). An idle `.start()`ed handle, `wait_any`/`wait_all` (`wait_exit` → `backend_wait`,
no token race), `wait_for`/`wait_for_port` probes, and shared-group streamed runs all
ignore `token.cancel()` entirely — no kill, no resolution, no `Error::Cancelled`. The
doc's own `wait_any` carve-out ("their stream simply ends") is also wrong: the wait does
not end.

### B3. Stdin-source failures are silently swallowed — success on truncated/empty input
`src/runner.rs:341-362` (writer task), `src/running/mod.rs:787-814` (`observe_stdin_task`
— "diagnostics only"), `src/stdin.rs:114-146`.
`.stdin(Stdin::from_file("data.tx" /* typo */))` → writer fails NotFound → sink drop
sends EOF → child processes nothing, exits 0 → caller gets `Ok("")`. Same for a
mid-stream reader error (partial input, success status). `Error::Io` is documented as
covering "writing stdin" (`src/error.rs:180-183`) but is never produced for it. Silent
data-corruption shape.

### B4. Secret leak: `Debug` impls print env VALUES and full argv
- `src/command.rs:1066-1068` — `Command`'s manual Debug prints `args` and `envs` raw,
  while the same impl deliberately redacts handlers/cancel-token to presence-only.
- `src/client.rs:48` — `CliClient` Debug prints `default_env` values (token redacted).
- `src/doubles.rs:392-404` — `Invocation` derives Debug with env values; a consumer's
  failed `assert_eq!` dumps secrets into CI logs (the default assertion idiom).
Violates the standing rule: argv/env VALUES never logged. One `tracing::debug!(?cmd)` in
consumer code leaks tokens.

---

## MODERATE

### B5. `finish_streamed`/`finish_events` drain stdout via `read_to_end` into an unbounded `Vec`, bypassing `fail_loud`
`src/running/stream.rs:167-174`, `:347-352`. The comment "the bytes are discarded either
way" is wrong about memory: every byte accumulates until EOF. No line policy, no
`OutputTooLarge` — `buffer.rs:40-44` promises the error "fires on every consuming verb".
This is exactly the flooding-child DoS `fail_loud` exists to stop. The drain task is also
never joined/aborted.

### B6. Dropping a `profile()` future leaks an immortal sampler task
`src/running/mod.rs:630-652` (infinite loop, frame-local `JoinHandle`), abort only via
the `on_exit` closure (`:658-665`); `Drop` (`:1061-1096`) doesn't know about it.
`tokio::time::timeout(d, proc.profile(every))` that elapses → task ticks forever,
sampling a recyclable pid. Accumulates per cancelled call.

### B7. `Command::timeout` is anchored to the consuming call, not to spawn; probes never arm it
`src/running/mod.rs:884-958` (deadline from `drive_to_exit` entry), `stream.rs:102-129`
(watchdog from the stream call), `src/running/probes.rs` (never arms). `start()` + 60 s
of `wait_for_port` + `wait()` → kill at t=70 with `timeout(10s)`; probe-only usage never
times out at all. Two clocks (watchdog vs bulk) can also report a deadline kill with
`timed_out() == false` (watchdog killed first, `backend_wait` returns `(None, false)`).
`started: Instant` exists but is unused for the deadline.

### B8. `Error::NotFound` enrichment misdiagnoses when the program IS found
`src/runner.rs:323` — `let (_found, searched) = find_in_path(...)`; the positive result
is discarded and the error always claims "not found on PATH". Windows: `Command::new("npm")`
with only `npm.cmd` on PATH (CreateProcess can't launch `.cmd` directly) → confidently
wrong message for the single most common Windows CLI trip-up. Unix: script with a missing
shebang interpreter (execve ENOENT) → same false diagnosis.

### B9. `wait`/`profile` retain the child's entire output in memory despite "discard" semantics
`src/running/mod.rs:43-50` (`CaptureMode::Discard`), `:696-738` — lines are pushed into
sinks built from the (default unbounded) user policy and discarded only **after** exit.
A long-lived chatty child = O(total output) heap in a verb whose contract is to discard.

### B10. Mixing streaming and bulk verbs silently yields empty results and skips fail-loud/handler guarantees
`src/running/mod.rs:457-568`, `:696-738`. After `stdout_lines()`/`output_events()`/
`wait_for_line`, a subsequent `output_string()` builds fresh empty sinks, spawns zero
pumps, never joins the still-running streaming pumps (aborted only by `Drop`), and runs
the overflow check against the fresh (empty) sinks. Result: `""` output, `truncated() ==
false`, lost `OutputTooLarge`, broken handler happens-before guarantee
(`command.rs:534-541`).

### B11. Pipelines silently strip per-stage `ok_codes`
`src/pipeline.rs:244-247` (unclean = `code != Some(0)`, ignoring stage `ok_codes`),
`:273-278` (last stage reset to `vec![0]`). `producer | grep.ok_codes([0,1])` — grep
exits 1 (no match): standalone success, in-pipeline attributed failure. Documented
nowhere public (`Command::ok_codes`, `Pipeline` docs silent).

### B12. `escalate_to_kill(false)` is unenforceable — `Drop` hard-kills survivors anyway
`src/group.rs:366-385` (`shutdown` consumes `self`; survivors die microseconds later in
the backend `Drop`: `windows.rs:557-563`, `linux.rs:293-318`, `pgroup.rs:309-313`). The
documented "don't kill the stragglers" behavior is impossible to obtain.

### B13. Linux resource limits structurally near-unusable: `subtree_control` write in a populated cgroup → EBUSY
`src/sys/linux.rs:380-405`; parent = the cgroup the calling process lives in
(`:336-342`). cgroup v2's no-internal-process rule makes the controller-enable fail
almost everywhere — **including** the environments the error message recommends
(`Delegate=yes` units hit the same EBUSY). The standard create-leaf→migrate-self→enable
dance is not implemented; `tests/integration/limits.rs:23-25` tolerates the failure,
masking it.

### B14. Linux `max_processes` not enforced for direct spawns (migration is exempt from `pids.max`)
`src/sys/linux.rs:410-412` vs `:104-108`/`:533-564` — children fork in the parent's
cgroup and migrate in pre-exec; the kernel checks `pids.max` only on fork *inside* the
cgroup. `max_processes(1)` admits unlimited `group.start()` calls on Linux while blocking
the (n+1)th on Windows (`limits.rs` test is Windows-only). Cross-platform contract
divergence, undocumented.

### B15. pgroup: unbounded recycled-pid window — no prune-on-reap
`src/sys/pgroup.rs:91-111` (`signal_all`), `:309-313` (`Drop` broadcast). A tracked
leader exits and is reaped; nothing untracks it. Hours later the group drops; the pid was
recycled into an unrelated process-group leader → probe passes → `killpg(P, SIGKILL)`
destroys an innocent tree. Most acute on macOS (~99k pid space, pgroup is the primary
mechanism there).

### B16. pgroup graceful shutdown waits out the full timeout on zombies of its own children
`src/sys/pgroup.rs:276-294` + `:69-77` (a zombie's pgid probes alive). Un-awaited
`RunningProcess` handles → children exit on SIGTERM but stay unreaped → `any_alive()`
true → `shutdown()` burns the entire `shutdown_timeout` + pointless SIGKILL escalation,
every time. The cgroup backend doesn't have this (exit removes from `cgroup.procs`);
docs acknowledge the effect only for adopted children (`group.rs:221-224`).

### B17. `Stdin::write_to` holds the async mutex across the whole copy — concurrent reuse stalls the second child's stdin
`src/stdin.rs:125-144` — the `MutexGuard` lives across `tokio::io::copy(...)`. Two
concurrent runs of a cloned command with `from_reader`: run B parks at `lock().await`,
its child holds an open silent stdin pipe until A's copy fully completes — instead of the
documented prompt-EOF "second run sees empty stdin". Only the `take()` needs the lock.

### B18. Cassette docs overclaim "can't leak secrets"; Drop-flush persists without `save()`
`src/cassette.rs:43-60` (`args`/`cwd`/`stdout`/`stderr` serialized verbatim), `:181-191`
(the claim — true only for env values), `:271-273` (default file permissions), `:397-409`
(best-effort flush on `Drop`, including unwind paths). A recorded `tool --token=abc` or a
tool that echoes credentials lands them in a fixture the docs say is safe to commit.

---

## LOW

- **L1.** Second `stdout_lines()`/`output_events()` call overwrites `self.stdout_sink`
  with a fresh closed sink → first pump's fail-loud flag and live line count silently
  discarded. `src/running/stream.rs:77-94`, `:263-275`; checks at `:205-216`/`:383-394`.
- **L2.** Two concurrently-polled `OutputEvents` share one stderr `SharedLines`;
  `close()`'s single `notify_one` can leave one consumer parked forever.
  `stream.rs:277-292`, `src/pump.rs:95-110`.
- **L3.** Early-`Err` exits / dropped verb futures leak the bulk-path pumps (frame-local
  handles; `Drop` aborts only the streaming-pump fields). On a shared-group handle the
  orphaned pumps buffer the still-running child's output unboundedly.
  `src/running/mod.rs:704-712`, `:525`.
- **L4.** Cancel tying with deadline can route through the *graceful* teardown, delaying
  the promised immediate hard kill by up to `grace` (unbiased `select!`).
  `src/running/mod.rs:934-957` vs `:984-986`.
- **L5.** `wait_any`/`wait_all` never close an untaken `keep_stdin_open` pipe — the child
  blocks on stdin, the race pends forever (the close lives only in `drive_to_exit`).
  `src/running/mod.rs:594-596` vs `:833-841`.
- **L6.** pgroup fallback path: a signal sweep racing the child's between-fork-and-exec
  `setpgid` gets ESRCH → prunes the id → "never re-add" rule forgets the group forever;
  kill-on-drop misses that subtree. `src/sys/pgroup.rs:180-187` + `:91-111`.
- **L7.** Windows `suspend()`/`resume()` can suspend threads of an unrelated process via
  pid reuse between `job_member_pids()` and the thread walk. `src/sys/windows.rs:266-310`.
- **L8.** `job_member_pids` flexible-array read is provenance-UB under Stacked Borrows
  (`(*list).ProcessIdList.as_ptr()` narrows to one element; `from_raw_parts` reads past
  it). Miri-flaggable, works in practice. `src/sys/windows.rs:495`.
- **L9.** `pid()` never becomes `None` after reap (doc says it does); post-reap
  `cpu_time()`/`peak_memory_bytes()` sample a recyclable pid. `src/running/mod.rs:377-404`.
- **L10.** cgroup vs Job `stats()` semantics diverge undocumented: Linux sums live
  members only (exited CPU vanishes; "peak" = sum of survivors' per-process high-water
  marks), Windows job accounting includes terminated processes. `src/sys/linux.rs:208-241`
  vs `src/sys/windows.rs:323-362`; `src/stats.rs:22-37` silent on it.
- **L11.** `graceful_kill_pid` treats `EPERM` as "gone" (early grace exit), inverting
  `Tracked::exists`'s EPERM-means-alive convention. `src/running/stream.rs:436-441` vs
  `src/sys/pgroup.rs:69-77`.
- **L12.** Doc bugs: `run`/`run_unit` say "require a zero exit" though `ok_codes` widens
  them (`src/command.rs:1008-1009`, `src/runner.rs:63,74`); `Command::env` doc claims a
  `None` value removes — impossible with the signature (`command.rs:156`).
- **L13.** Windows `quote_arg`: trailing backslash before the closing quote reads as an
  escaped quote; `\"` isn't cmd's convention; `( ) !` unquoted; `%` quoting doesn't stop
  cmd expansion. Display-only, but "readable and unambiguous" fails.
  `src/command.rs:1127-1147`.
- **L14.** Cancel-after-success destroys a completed result: `checked_outcome` reads the
  token *after* `join_pumps` (up to 5 s) — a child that exited 0 reports `Err(Cancelled)`
  and the captured output is lost. `src/running/mod.rs:773-779`.
- **L15.** Unchecked-last pipefail fabricates `code: Some(0)` — a real exit code (e.g.
  141) is discarded. `src/pipeline.rs:259-272`.
- **L16.** `ProcessResult::combined` glues the last stdout line to the first stderr line
  (`output_string` drops the trailing newline). `src/result.rs:250-255`,
  `src/running/mod.rs:474-475`.
- **L17.** Pump decode edges: terminator strip removes *all* trailing `\r`/`\n` (a real
  `\r` in `"abc\r\r\n"` is lost); `encoding.decode()` BOM-sniffs **per line**, so a line
  starting with BOM bytes flips to UTF-16 regardless of the configured encoding.
  `src/pump.rs:204-207`.
- **L18.** `Error::Timeout` displays "timed out after 0ns" when no deadline was recorded
  (`self.timeout.unwrap_or_default()`, e.g. scripted `Reply::timeout`).
  `src/result.rs:174,206`.
- **L19.** Pipefail attribution blames the first unclean stage in stage order — i.e. the
  upstream SIGPIPE *victim* — instead of the actual culprit (bash reports rightmost for
  this reason). `src/pipeline.rs:244-247`.
- **L20.** `is_bare_name("git/")` → `true` (trailing separator collapses to one Normal
  component) → path-ish spelling gets the "not found on PATH" enrichment.
  `src/command.rs:1160-1164`.
- **L21.** `Error::NotFound` Display embeds the full `PATH` value; the retry loop logs it
  via `tracing` (`error = %err`) — contradicting lib.rs's "never logs argv or environment
  values" claim. `src/error.rs:34-42`, `src/runner.rs:152-159`, `src/lib.rs:137-141`.
- **L22.** `Error`'s derived `Debug` dumps the FULL captured stdout/stderr (`Exit`
  variant), defeating Display's careful 200-byte cap — `.unwrap()` panics and
  `{e:?}` logging dump multi-MiB streams. `src/error.rs:10`, `:61-76`.
- **L23.** Cassette replay keys go through `to_string_lossy` — two distinct non-UTF-8
  invocations can collide on U+FFFD and replay the wrong reply. `src/cassette.rs:107-131`.

---

## DESIGN (pre-1.0 shape; cheap now, breaking later)

- **D1.** `wait`/`finish_streamed`/`finish_events`/`wait_any`/`wait_all` collapse
  `Outcome` to `Option<i32>` — the handle *knows* `timed_out` and throws it away;
  `(Option<i32>, String)` is a self-undescribing tuple. Suggested: return `Outcome` (and
  a named `StreamedFinish { outcome, stderr }`). `src/running/mod.rs:582`,
  `src/running/stream.rs:159`, `:345`, `src/lib.rs:269,328`.
- **D2.** Signal termination is stringly-typed: synthesized
  `Error::Io(io::Error::other("…terminated by a signal…"))`; the signal number is dropped
  at the source (`backend_wait` never reads `ExitStatusExt::signal()`); `Outcome::Signalled`
  is a unit variant so the number can't be added later without breaking matches.
  Suggested: `Outcome::Signalled(Option<i32>)` + `Error::Signalled { program, signal }`.
  `src/result.rs:191-196`, `src/running/mod.rs:866`.
- **D3.** `Supervisor`'s flagship use case ("keep a server alive") composes with the
  default unbounded capture → every incarnation's full stdout/stderr accumulates in RAM
  (`runner.output()` per incarnation; only `stop_when`/`final_result` consume it).
  Needs a bounded-tail default or a loud knob. `src/supervisor.rs:324`, `:81-88`,
  `src/command.rs:111`.
- **D4.** `ProcessRunner::start` defaults to runtime `Err(Unsupported)` — capability
  discovery in production instead of the type system; a runner forgetting the override
  compiles silently. Suggested: split `ProcessStarter: ProcessRunner` (or make `start`
  required). `src/runner.rs:37-43`, `src/cassette.rs:196-200`.
- **D5.** `StdioMode::Inherit`/`Null` gate the capture verbs silently: `stdout_lines`/
  `output_string` quietly yield nothing, indistinguishable from "already consumed" or
  "pipe taken". Make the door honest (error or documented tri-cause). 
  `src/running/stream.rs:78-93`, `:264-274`.
- **D6.** `Command::first_line` hardwires `JobRunner` — the only non-trivial verb
  unreachable through the test seam (no `ProcessRunnerExt` twin). `src/command.rs:1024-1059`.
- **D7.** `cli_client!` macro is evolution-hostile: the `cancellation` feature already
  required a `#[doc(hidden)] #[macro_export]` helper (public API forever, name
  load-bearing); every future default knob = lockstep macro edits. With zero users,
  consider deleting in favor of the documented hand-rolled wrapper.
  `src/client.rs:258-346`.
- **D8.** Tokio types in public *signatures* (beyond the adjudicated re-exports):
  `to_tokio_command()`, `ProcessGroup::spawn(&mut tokio::Command)`, `adopt(&Child)`,
  `Stdin::from_reader<R: tokio::io::AsyncRead>`. Also `spawn(&mut …)` mutates the
  caller's command (stacking pre_exec hooks on reuse) — take by value or accept the
  crate's `Command`. `src/command.rs:836`, `src/group.rs:192,226`, `src/stdin.rs:76`.
- **D9.** Small breaking-to-fix-later items: `Invocation::cwd: Option<OsString>` (should
  be `PathBuf`; public field) `src/doubles.rs:399`; `timeout_signal` gated behind the
  unrelated `process-control` feature `src/command.rs:412`;
  `unbounded().with_overflow(Error)` is a documented no-op (config that silently does
  nothing) `src/buffer.rs:111-121`; `OutputEvents` fixed stdout-first poll priority bakes
  stderr starvation into observable behavior `src/running/stream.rs:570-599`.
- **D10.** No way to ask a `RunningProcess` whether it kills on drop (private-group vs
  shared-group provenance) — a function receiving a handle can't reason about drop
  safety. One accessor would close it.

---

## Cross-corroboration notes

Independently found by ≥2 reviewers: B1 (async+sys), B5 (async+API), B7 (async+API+sys),
B10 (async+API), the timeout/`timed_out` clock split (B7/L-sys), recycled-pid class
(B1/B15/L7/L9). The five reports' "checked and clean" sections collectively cover: pump
notify protocol, drive_to_exit select correctness, graceful-timeout matrix, Drop ordering,
Windows handle hygiene, pre_exec async-signal-safety, cassette deserialization surface,
injection surfaces (none), batch/pipeline/supervisor drop semantics.
