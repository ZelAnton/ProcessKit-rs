# 2026-06-15 bug-hunt inspection

Fresh-eyes inspection of the whole `src/` tree (~19k LOC) focused on **potential
bugs** first, then vulnerabilities, then structure/interface. Seven readers, one
cohesive area each (core lifecycle; unix containment; Windows containment;
buffer/pump/stdin; command/result/error; pipeline/supervisor/batch;
runner/client/cassette). This is a **list for fixing — nothing was changed.**

Pre-1.0, no users → interface/architecture changes are fair game.

Each item: severity · location · what & why · suggested direction (not a patch).
Confidence noted where the reader could not fully prove it from the code.

---

## Priority 1 — real correctness/behavior bugs, fix first

### P1-1 · Windows `spawn` doesn't set `kill_on_drop(true)` — panic before job-assign leaks a suspended orphan
- **High** · `src/sys/windows.rs:175-207`
- The child is created `CREATE_SUSPENDED` and only *then* assigned to the Job. The
  explicit error paths call `start_kill()`, but a panic (or a future fallible edit)
  between `cmd.spawn()` and the assign drops the `Child` without `kill_on_drop`. The
  process is suspended and not yet in the Job, so `KILL_ON_JOB_CLOSE` will never reap
  it → a permanent suspended orphan. The pgroup backend already sets
  `kill_on_drop(true)` (`pgroup.rs:426,460,502,544`); Windows is the odd one out.
- **Direction:** set `kill_on_drop(true)` in the Windows `spawn` for parity and
  unwind-safety before containment is established.

### P1-2 · Linux `Drop` blocks the tokio worker thread with `std::thread::sleep`
- **High** · `src/sys/linux.rs:326-331` (Cgroup Drop drain loop), reachable `:589-600`
- `Job::drop` for the cgroup backend calls `cg.kill()` then busy-waits
  `std::thread::sleep(2ms)` ×50 (~100ms), and the pre-5.14 `kill()` fallback sleeps up
  to another ~100ms. `Drop` runs wherever the group/handle is dropped — for an async
  crate that's routinely a tokio worker thread (e.g. dropping a group inside a task, or
  at the end of `shutdown()` `group.rs:411`). A blocking sleep there stalls the
  executor (the whole runtime on a current-thread flavor). Classic blocking-in-async-Drop.
- **Direction:** inherent to Drop not being able to await; tighten the bound, consider
  `block_in_place`/a detached blocking task for dir reclaim, and at minimum document the
  worst-case worker stall (~200ms on old kernels).

### P1-3 · Supervisor crash-classification ignores the command's `ok_codes`
- **Medium (High confidence it diverges)** · `src/supervisor.rs:393`
  (`let crashed = result.code() != Some(0);`)
- Everywhere else the crate honors `ok_codes` via `is_success()`; the supervisor alone
  decides "crashed" by raw `code() != Some(0)`. A command with `ok_codes([0,2])` exiting
  `2` is treated as a crash → `OnCrash` restarts a *successful* run and the exit feeds the
  failure-storm score. Inverse: `ok_codes([1])` exiting `0` is read as clean and ends
  supervision on a configured failure. `stop_when` sees the real `ProcessResult`, so the
  two halves of the supervisor disagree.
- **Direction:** classify with `!result.is_success()` (timeouts/signals still have
  `code()==None` → stay crashes); align the `RestartPolicy::OnCrash` doc wording.

### P1-4 · `wait_for_line` arms the tree-killing deadline and can flip a run's final outcome to `TimedOut`
- **High** · `src/running/probes.rs:48-73` (calls `stdout_lines()` at `:60`); arming at
  `src/running/stream.rs:144-202`
- The sibling probes (`wait_for`, `wait_for_port`) promise "a failed probe does not kill
  the child." `wait_for_line` calls `stdout_lines()`, which (with a `Command::timeout`
  set) arms the detached watchdog that hard-kills the tree at the deadline — and leaves it
  armed, so a later `wait()`/`finish()` classifies the run as `TimedOut` even if the child
  later exits cleanly. A non-consuming readiness probe thus changes the eventual outcome.
  (It *is* documented on the method, but it's a sharp asymmetry.)
- **Direction:** either drain stdout in `wait_for_line` without installing the killing
  deadline, or make the asymmetry impossible to miss and reconsider whether a probe should
  ever flip the run's `Outcome`.

### P1-5 · Cassette file-stdin "content digest" hashes the path, not the bytes — silent wrong reply on replay
- **High** · `src/stdin.rs:139` (`content_digest`), keyed at `src/cassette.rs:173-177`
- Cassette docs (`cassette.rs:53-57,164-166`) say matching is by stdin **content
  (hashed)**. For `Stdin::from_file` the digest is over the *path string*, not the file's
  bytes. Same path + changed contents → key collision → replay serves a stale/ wrong
  recorded output; same contents at two paths → treated as two invocations. The
  `stdin.rs:121` comment even admits "a file source hashes its path" — directly
  contradicting the cassette-level docs.
- **Direction:** hash file contents at key time for `from_file` (accept the I/O), or
  rename the concept to "stdin identity" and reconcile the docs so callers know file-stdin
  keys on path only.

### P1-6 · Over-cap line byte accounting miscounts CR / across-read content (affects `OverflowMode::Error` byte ceiling)
- **High** · `src/pump.rs:449,455,484,495`; `content_len` at `:401`
- When a line exceeds the byte cap and is skipped across multiple reads, intermediate
  chunks add `pending.len()` while the final segment uses `content_len` (which strips a
  trailing `\r` only in the *final* chunk). So a CRLF whose `\r` landed in a previous chunk
  is counted, but in-one-chunk it's stripped — the reported line length depends on where the
  read boundary fell. This feeds `seen_bytes`, which under `OverflowMode::Error` with a byte
  cap decides whether the ceiling trips → not byte-exact for the path it's meant to drive.
- **Direction:** define one canonical "retained content length" computed identically whether
  the line fits, is dropped whole, or is skipped across reads (carry "last content byte was
  `\r`" across reads); add a cross-read CRLF over-cap test.

---

## Priority 2 — should fix (correctness edges, security surface, hardening)

### P2-1 · cgroup per-pid signal can hit a recycled pid (non-SIGKILL signals)
- **Medium** · `src/sys/linux.rs:530-546`, fallback `:594-599`
- `signal()` snapshots `cgroup.procs` then `kill(pid, sig)` in a loop. A member can exit and
  its pid be recycled outside the cgroup before the `kill` — ESRCH only covers the *gone*
  case, not the *recycled* one. The pgroup backend has explicit recycled-pid hardening (B5
  latch, probe-before-signal, ESRCH pruning); the cgroup per-pid path has none. The atomic
  `cgroup.kill` path (used for `Signal::Kill`) is immune; exposure is `Term`/freeze
  fallback `SIGSTOP`/`SIGCONT` and pre-5.14 kernels. Also a privilege/DoS surface when run
  with `CAP_KILL`/root.
- **Direction:** re-check membership immediately before the kill (smaller window), or
  document that per-pid signalling is best-effort vs the `cgroup.kill` guarantee.

### P2-2 · `skip_drop_kill` uses `Relaxed` with an unsound justification comment
- **Medium** · `src/sys/graceful.rs:73-77`, `linux.rs:317`, `pgroup.rs:402`
- The store→Drop-load happens-before is real (tokio establishes it on task migration), but
  the comment claims it's because of a "single-threaded call boundary," which is wrong. A
  future refactor that drops the Job on a different task/thread than ran `run()` would
  silently break the Relaxed assumption.
- **Direction:** switch to `Release`/`Acquire` (cheap, self-evidently correct regardless of
  drop site) or fix the comment to cite task-migration synchronization.

### P2-3 · Windows `graceful_shutdown(escalate=false)` is best-effort — can still kill survivors
- **Medium** · `src/sys/windows.rs:632-660`
- The skip-kill path clears `KILL_ON_JOB_CLOSE` via `SetInformationJobObject`; if that call
  fails (return ignored via `let _ =`), the subsequent `CloseHandle` kills the survivors,
  contradicting `escalate=false`. Defensible ("unexpected kill > orphan ambiguity") but it
  means survivor-preservation is **not a guarantee** on Windows.
- **Direction:** surface this in the public `graceful_shutdown` rustdoc (`sys/mod.rs:191`).

### P2-4 · Windows TID-reuse race in suspend/resume member walk
- **Medium** · `src/sys/windows.rs:411-445`, `280-324` (via `OpenThread(tid)`)
- The snapshot maps TID→PID but `OpenThread` is called with the TID alone; a recycled TID
  can resume/suspend a thread of an unrelated process. The primary-thread resume right after
  spawn is near-immune (TID can't recycle that fast); the suspend/resume member walk is the
  exposed path.
- **Direction:** after `OpenThread`, verify the thread still belongs to the expected PID
  (`GetProcessIdOfThread`) before acting; or document the residual race.

### P2-5 · Windows `for_each_member_thread` leaves a partially-suspended tree on mid-walk error
- **Medium** · `src/sys/windows.rs:305-323`
- The walk continues on error and returns only `last_err`. A `suspend` that fails partway
  leaves some threads frozen and some not, with no rollback — can deadlock the child app.
- **Direction:** on a mid-walk suspend failure, best-effort resume the already-suspended
  threads before returning Err; or document that a failed `suspend` may half-freeze the tree.

### P2-6 · `Error::Parse` Display does no control-char sanitization (inconsistent with other variants)
- **Medium** · `src/error.rs:615-627` (`display_parse`) vs sanitized `append_diagnostic_tail`
  `:713-729`
- `Exit`/`Timeout`/`Signalled` replace control chars with U+FFFD; `Parse` (which by its own
  docs "routinely embeds the unparsed output in full" — attacker-influenced) only truncates,
  so raw ESC/BEL/CR reach a log line. Consider also bidi-override code points (Trojan-Source
  class), which `is_control()` does not catch in any variant.
- **Direction:** factor the sanitizer into one helper, apply it in `display_parse` too;
  consider extending the predicate to strip bidi controls.

### P2-7 · pump scratch buffer (`pending`) never shrinks — defeats the configured memory bound
- **Medium** · `src/pump.rs:438,456,485`
- `clear()`/`drain` keep capacity and `reserve(need)` grows monotonically. After one huge
  line the pump holds that high-water allocation for life, even under a small byte cap — the
  cap was sold as bounding in-flight memory (buffer.rs H1).
- **Direction:** `shrink_to` a sane bound after emitting/skipping an over-cap line.

### P2-8 · pump in-flight decode buffer is bounded to `cap + one chunk`, not `cap`
- **Medium (doc accuracy)** · `src/pump.rs:437-438,483`
- The cap check runs only after a full chunk is decoded, so `pending` reaches
  `cap + max_utf8_buffer_length(8192)` before truncation. Not an OOM, but "bounds the
  in-flight decode buffer" overstates it.
- **Direction:** check the cap incrementally inside the split loop, or document the
  `cap + chunk` bound precisely.

### P2-9 · stdin `BrokenPipe`/`WriteZero` from an early-exiting child may surface as a spurious error
- **Medium (needs spawn-layer confirmation)** · `src/stdin.rs:209-230`, `src/pump.rs:332`
- `write_to` propagates any `io::Error` verbatim. A child that legitimately closes stdin
  early (`head -1`) yields `BrokenPipe`; if the spawn/wait layer surfaces it, that's a
  spurious run failure. Also the `Lines`/`Reader` copy loops have no cancellation — if not
  aborted on child exit the writer can park.
- **Direction:** confirm the spawn/wait module swallows `BrokenPipe`/`WriteZero` from the
  stdin writer and aborts the writer task on child exit; if not, add it.

### P2-10 · `profile()` sampler can fold a recycled pid's metrics into the result
- **Medium** · `src/running/mod.rs:1088-1141`
- A sample in flight at reap can complete against a just-recycled pid (acknowledged in a
  comment), silently corrupting `peak_memory_bytes`/`cpu_time` — user-visible data, unlike
  the mostly-idempotent kill signals.
- **Direction:** re-check liveness (or snapshot the process start-time) before folding a
  reading; at minimum surface the caveat in the public `profile` doc.

### P2-11 · cgroup `members()` doesn't filter `pid <= 0`
- **Medium (defensive)** · `src/sys/linux.rs:513-521`
- `parse::<i32>()` keeps anything that parses; a malformed/zero line would make `signal()`
  call `kill(0, …)` (whole caller group) or `kill(-n, …)` (a process group). The kernel
  shouldn't emit 0, but the pgroup backend filters and the cgroup path doesn't.
- **Direction:** `filter(|&pid| pid > 0)` in `members()`.

### P2-12 · cassette accepts contradictory entries silently (loses `Signalled` vs `Exited`+signal)
- **Medium** · `src/cassette.rs:469-473` (replay), `154-159` (capture)
- Replay decodes on `(code, timed_out)` only; a hand-edited cassette with both `code:
  Some(0)` and `signal: Some(9)` silently replays `Exited(0)`, ignoring `signal`. Cassettes
  are documented as human-editable, so a malformed-but-parseable entry should fail loud like
  an unknown version does.
- **Direction:** on load, reject entries with contradictory fields
  (`code.is_some() && signal.is_some()`, or `timed_out && (code|signal).is_some()`) as
  `InvalidData`.

---

## Priority 3 — low severity, latent fragility, hardening tests

- **P3-1 · Lost-wakeup safety in `StdoutLines`/`OutputEvents` rests on the re-drain loop +
  single-permit `Notify`** — appears correct but subtle and under-tested for the concurrent
  pump+close coalescing case. `stream.rs:561-583,706-744`, `pump.rs:173-213`. **Direction:**
  add a stress/loom test (slow consumer parking between every line, concurrent pump + close;
  assert no line lost, stream always terminates). The class of bug Issue-7 warned about.
- **P3-2 · Pipeline whole-chain timeout/error detaches in-flight drain tasks** (not aborted/
  awaited). Safe today only via group kill-on-drop. `pipeline.rs:197-212,189-191`.
  **Direction:** hold `AbortHandle`s and abort siblings (mirror `AbortOnDrop` in
  `running/mod.rs:1119`).
- **P3-3 · `SharedLines` poison handling is inconsistent** — `close`/`seen_bytes`/`overflowed`
  recover from poison, but the hot `push`/`try_pop`/`drain` `.expect()`. `pump.rs:110,181,255,262`.
  **Direction:** use the same `unwrap_or_else(|p| p.into_inner())` everywhere.
- **P3-4 · Dead repeat-call branches in `output_events`/`stdout_lines`** — the
  `ensure_stdout_streamable` gate makes the second-call paths unreachable from the public API,
  but comments imply they're live. `stream.rs:337-387`. **Direction:** remove them or comment
  that they're defense-only.
- **P3-5 · `output_bytes` raw reader silently treats a mid-stream IO error as EOF**
  (`Err(_) => break`), no stashing, no `tracing`. `running/mod.rs:822-859`. **Direction:**
  surface the read error (at least under `tracing`); document the returns-bytes-so-far behavior.
- **P3-6 · `ScriptedRunner::on_sequence` ordering is undefined under concurrent calls** (the
  `Relaxed` `fetch_add` is fine as a counter, but the fail-then-succeed contract assumes
  sequential calls). `doubles.rs:468-471`. **Direction:** document the sequential-only contract.
- **P3-7 · `RecordReplayRunner` record mode doesn't impose execution ordering** for concurrent
  recording of a stateful inner runner (`inner.output` runs outside the lock). `cassette.rs:425-442`.
  **Direction:** document record-mode as sequential-per-key.
- **P3-8 · `into_running` line budget (`split_inclusive('\n')`) vs handler replay (`str::lines()`)
  can disagree** on trailing/blank lines → a delayed-line script can truncate the last line under
  tight timeouts. `doubles.rs:223-225` vs `493/498`. **Direction:** one shared counting helper.
- **P3-9 · `Reply::lines` appends a trailing `\n`, `with_stdout` does not** — inconsistent
  line-shaping. `doubles.rs:169-183` vs `199-202`. **Direction:** document or normalize.
- **P3-10 · `matched_reply` indexes `replies[i.min(len-1)]`** — underflow panic if `replies`
  ever empty (invariant currently held only by constructors). `doubles.rs:471`. **Direction:**
  non-empty type or a debug assert at construction.
- **P3-11 · Pipeline `expect("a pipeline has at least two stages")`** reachable only via future
  internal misuse. `pipeline.rs:157,163,325`. **Direction:** encode the ≥2 invariant in the type.
- **P3-12 · `cgroup_name_salt` falls back to `0`** on a `SystemTime` error, weakening E20
  collision protection silently. `linux.rs:38-46`. **Direction:** mix in `process::id()` as
  secondary entropy.
- **P3-13 · Orphaned `processkit-*` cgroup dirs accumulate** after crashes / `escalate=false`
  survivors. `linux.rs:333-342`. **Direction:** best-effort sweep of stale dirs (dead pid) at
  startup, or document.
- **P3-14 · Cassette load buffers the whole file twice with no size cap** (trusted-fixture
  model, but the task asked about untrusted JSON). `cassette.rs:389-420`. **Direction:** size
  guard before `read_to_string`, or document cassettes as trusted.
- **P3-15 · `graceful::run` re-probes `is_drained()` after the loop** — side-effecting on the
  pgroup backend (mutates the liveness latch). `graceful.rs:64-71`. **Direction:** capture the
  loop's exit state in a bool and reuse it; document that `is_drained` may mutate.
- **P3-16 · `Signal::Other(i32)` / `timeout_signal_raw` pass arbitrary/negative numbers
  unvalidated** to the (downstream) kill site; a negative would target a process group.
  `signal.rs:54`, `command.rs:892-898`. **Direction:** reject non-positive raw signals at the
  `Other` boundary if the syscall site doesn't already guard.

---

## Structure / interface observations (non-bugs)

- **S1 · Centralize `MAX_DEADLINE` clamping** — duplicated at every timing site
  (`graceful.rs:63`, `probes.rs:105/134`, `stream.rs:479`, supervisor, doubles). A single
  `clamped_deadline(timeout) -> Instant` helper removes the chance a future site forgets the
  clamp (the exact E15 panic).
- **S2 · Move `skip_drop_kill` into the `Cgroup` struct** — `Job` carries it but it's dead for
  the ProcessGroup arm (which uses the pgroup's own flag); scope should match use.
  `linux.rs:52` / `pgroup.rs:231`.
- **S3 · `GracefulTarget::is_drained` is documented as a query but mutates on the pgroup
  backend** — state the may-probe-and-mutate contract on the trait. `graceful.rs:35-36`.
- **S4 · Split the long `Cgroup::kill` method** (cgroup.kill / freeze / per-pid sweep / drain /
  thaw) into named helpers so the E18 drain contract and freeze bracketing are verifiable.
  `linux.rs:570-621`.
- **S5 · Batch result memory is O(total), not O(concurrency)** — by design, but worth a one-line
  caveat for callers fanning out hundreds of thousands of commands. `batch.rs:92`.
- **S6 · `JobRunner`/`ProcessGroup` have both inherent and trait `start`** — inherent wins for
  concrete-typed callers; safe today (they delegate identically) but a latent footgun if the
  trait method ever gains pre/post behavior. `runner.rs:329/347`.

---

## Verified clean (the high-risk areas the brief flagged, confirmed sound)

- **The Issue-7 arbiter** (`timeout_state` CAS + `ExitCause` carried out of `select!` +
  first-observation cancel snapshot) genuinely closes the post-hoc `is_cancelled()` window;
  no remaining race found in that area. Drop-time tree containment, double-kill idempotency,
  and cancel-watchdog task-abort cleanup all check out.
- **Exit/signal classification** — `status.code()`/`status.signal()`, no 128+signal conflation,
  no `i32` wraparound on Unix. **`ok_codes`** applied consistently across `output_string`,
  `output_bytes`, the checking verbs, doubles, cassette, pipeline; `probe`/`exit_code`
  correctly ignore it.
- **StdioMode gating** — capture verbs error (`Io(InvalidInput)`) when a stream isn't piped;
  never silently empty, never panic.
- **Encoding/BOM** — single persistent BOM-removing decoder per stream; cross-read multibyte,
  UTF-16 `0x0A`, ISO-2022-JP shift state, lone-CR/LF/CRLF, unterminated final line all handled
  and tested.
- **Secret safety** — `Command`/`Invocation`/`CliClient`/`Error` Debug render arg *counts* and
  env *names* only, never argv/env values; `command_line()` is the sole documented
  argv-bearing escape hatch. Cassette stores env names only (tested).
- **Cassette security** — `O_NOFOLLOW` + `0600` correct on all write paths incl. rewrite; no
  open/write TOCTOU (perms on the held fd); panic-skip-on-unwinding-drop; replay-miss is a
  distinct error, never a surprise spawn. Windows ACL caveat documented consistently.
- **Backoff/jitter math** — f64 compute with `is_finite` guard + `MAX_DEADLINE`/cap clamps +
  `try_from_secs_f64`; no Duration overflow; storm-guard window math sound and tested.
- **Pipeline fd wiring** — parent retains only read ends; relays are independent tasks; no
  EOF-starvation hang; SIGPIPE/EPIPE on an early-closing consumer tolerated; pipefail
  attribution + ok_codes-in-pipefail correct.
- **Batch concurrency driver** — tops up to the limit, preserves input ordering via fixed
  slots, no abort-leak, yields `Pending` only when every active future registered a waker.
- **Windows FFI** — struct sizing for every (Set|Query)InformationJobObject class; NULL vs
  INVALID_HANDLE_VALUE sentinels correct per-API; `GetLastError` captured before `CloseHandle`;
  CloseHandle on every path incl. Drop and error; CREATE_SUSPENDED→assign→resume race fix
  sound; variable-length `job_member_pids` growth + provenance correct; `process_metrics`
  access rights (`PROCESS_QUERY_LIMITED_INFORMATION`) sufficient on Win7+.
- **Cross-compile (linux-gnu / aarch64-darwin)** — Linux-only syscalls correctly cfg-gated to
  `linux.rs`; libc constants/types identical across both targets; non-unix/non-windows
  `compile_error!` gate present.
