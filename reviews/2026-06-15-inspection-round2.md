# 2026-06-15 inspection — round 2 (post-P1)

Second fresh-eyes pass over the whole `src/` tree (~19.7k LOC), run after the
Priority-1 fixes from [`2026-06-15-inspection.md`](2026-06-15-inspection.md)
landed. Seven readers, one area each; every reader first read round 1 so this
report concentrates on **what's new or missed**, not a re-listing.

Focus order: potential **bugs** first, then vulnerabilities, then
structure/interface. Pre-1.0, no users — bold changes OK. **List only — nothing
was changed.**

Net: the P1 fixes verify sound, **except one genuine new High bug the P1-6 fix
left adjacent** (N-1). A handful of new Medium/Low items, one useful *downgrade*
of a round-1 item (N-4), and the round-1 P2/P3 backlog is largely still open.

---

## NEW — Priority 1 (fix first)

### R2-1 · High · CRLF line whose content is exactly `max_bytes` is retained in one chunk but DROPPED when its `\r` lands on a read boundary
- `src/pump.rs:503` (the `None if cap.is_some_and(|c| pending.len() > c)` arm), vs the emit decision at `:485-486`.
- **What:** the incremental in-flight cap trip at `:503` compares the **raw** decoded `pending.len()` — which still includes a trailing `\r` — against the cap, but the retain/emit decision at `:485` compares `content_len` (which excludes a CRLF `\r`). For a line whose *content* length equals the cap, the two disagree the instant a CRLF straddles a read.
- **Trace** (cap = 2, input `"ab\r\n"`):
  - One chunk `"ab\r\n"`: `find('\n')` → `content_len = 2`, `2 <= 2` → **retained** as `"ab"`.
  - Split `["ab\r", "\n"]`: chunk 1 has no `\n` and `pending.len() == 3 > 2` → enters the over-cap skip arm, defers the `\r`, sets `oversized`; chunk 2 resolves `content_len = 2` but the line is now on the `record_oversized_line` path → **dropped**.
- **Why it matters:** the retain-vs-drop verdict (and, under `OverflowMode::Error`, whether the fail-loud ceiling trips) depends on where the OS split the read — the *exact* class of chunk-dependence P1-6 set out to eliminate. P1-6 fixed only the **byte-count accounting** of already-over-cap lines; it did not fix the **enter-skip decision** for a line sitting on the boundary. The P1-6 test uses 10 X's vs a 4-byte cap, where the verdict is "drop" regardless, so it never exercises the boundary line.
- **Direction:** at `:503`, don't count a lone *trailing* `\r` toward the cap (it may be a CRLF terminator). Compare `pending.len() - (pending.ends_with('\r') as usize)` against the cap (defer exactly as `skip_over_cap` does); if the `\r` turns out to be content, the next chunk re-evaluates. Add a regression test: a content-exactly-at-cap CRLF line split as `[..\r][\n..]` must be retained, identically to the one-chunk case and to its LF twin.
- Confidence: **high** (traced against current code).

---

## NEW — Priority 2 (should fix)

### R2-2 · Medium · Windows `peak_memory_bytes` means two different things in `stats()` vs `process_metrics()`
- `src/sys/windows.rs:386` (`stats()` → `ext.PeakJobMemoryUsed`) vs `:651` (`process_metrics()` → `counters.PeakWorkingSetSize`).
- `stats()` reports job-wide **committed** peak; `process_metrics()` reports per-process **working set** (resident). These are not comparable quantities, yet both land in a field named `peak_memory_bytes`. A caller aggregating per-process metrics and comparing to group `stats()` gets inconsistent numbers. (Linux per-process likely reports RSS-like too, so the Windows *internal* inconsistency is the issue.)
- **Direction:** document the distinction, or switch `process_metrics` to a commit-based counter (`PrivateUsage` via `PROCESS_MEMORY_COUNTERS_EX`) if the intent is "footprint." Confidence: high it's a semantic mismatch.

### R2-3 · Medium · cgroup `freeze()` silently downgrades to per-pid signalling on ANY write failure, re-opening the recycled-pid window on a modern kernel
- `src/sys/linux.rs:573-580` (freeze), interacts with the per-pid `signal()` path (R1 P2-1).
- `freeze()` tries `cgroup.freeze` and on **any** `is_ok()` failure (incl. a transient `EACCES`/`EBUSY` on a delegated-but-restricted cgroup, not just kernel-too-old) falls back to per-pid `SIGSTOP`/`SIGCONT` via `signal()`. The comment frames the fallback as "kernels without it," but the trigger is *any* write error — so on a modern kernel a non-version failure silently re-exposes the recycled-pid signalling hazard.
- **Direction:** distinguish `ENOENT` (file absent → old kernel → fall back) from `EACCES`/`EBUSY` (present but rejected → surface, don't downgrade). Confidence: medium.

### R2-4 · Medium · graceful poll **prunes** the pgroup tracking set, so `escalate=false` survivors are forgotten (not just spared) and post-shutdown `members()`/`stats()` under-report
- `src/sys/graceful.rs:64-71` driving `pgroup.rs` `any_alive`/`probe_entry` (`:155-156,188-189`).
- The 20 ms drain poll routes through `is_drained()` → `any_alive()` → `probe_entry()`, which **prunes** entries that momentarily probe gone. Under `escalate=false` (B12) the intent is "leave survivors alive and let `Drop` spare them," but a pruned entry is *forgotten*, so a later `members()`/`stats()` under-reports, and the prune is a write happening inside what `GracefulTarget::is_drained` (`graceful.rs:35`) documents as a query. (Related to round-1 P3-15/S3, but the survivor-accounting consequence is newly noted.)
- **Direction:** give the driver a non-pruning liveness check, or document that the graceful poll mutates the tracked set. Confidence: medium.

### R2-5 · Medium (latent) · Windows `resume_process_threads` success criterion (`resumed >= 1`) depends entirely on `suspend_lock`; a multi-suspended primary thread is left frozen yet reported `Ok`
- `src/sys/windows.rs:466-482`.
- `ResumeThread` decrements the suspend count by one; the function returns `Ok` as soon as one thread is resumed once. A `CREATE_SUSPENDED` child has count 1 (fine today), but if a concurrent `suspend()` member-walk ever raised it to 2, one resume leaves it frozen forever while still returning `Ok`. The `suspend_lock` held across assign→resume is the *only* thing preventing this, and it's not documented as load-bearing for correctness here.
- **Direction:** loop `ResumeThread` on the primary thread until its count reaches 0, or document the `suspend_lock` dependency explicitly. Confidence: medium it's currently unreachable; low it stays so under refactor.

### R2-6 · Medium · `output_events` repeat-call branch is dead AND, if ever reached, takes the stdout pipe into an orphaned sink
- `src/running/stream.rs:357-381`.
- `ensure_stdout_streamable` makes a second `output_events` call return `Err` before the body runs (round-1 P3-4), but the body still unconditionally `take_stdout_reader()`s into a fresh local sink that is then **not** stored (`if self.stdout_sink.is_none()` is false) — so on that (currently unreachable) path the pump+pipe are orphaned. Latent footgun if the gate is ever relaxed.
- **Direction:** delete the unreachable repeat-call arms in `output_events`/`drain_stdout_lines`, or `unreachable!()` them. Confidence: medium.

---

## NEW — Priority 3 (low / hardening / parity)

- **R2-7 · Low · `output_bytes` omits `with_overflow_totals`** — `src/running/mod.rs:879-888` (vs `output_string` `:750-760`). A bounded policy that drops stderr lines on the bytes path yields `ProcessResult<Vec<u8>>` with `truncated()==true` but `total_lines`/`total_bytes == 0`, breaking the "totals meaningful when truncated" promise. Invisible to the checking verbs (they're String-only), so Low. Add `.with_overflow_totals(...)` for parity, or document.
- **R2-8 · Low · `output_bytes` raw reader swallows a mid-stream IO error as EOF** — `src/running/mod.rs:830-837` (`Err(_) => break`). Returns bytes-so-far indistinguishably from a clean EOF, no `tracing`. (Round-1 P3-5 recurs on this raw path too.) Surface under `tracing`, or document.
- **R2-9 · Low · cassette `Signalled(None)` is indistinguishable from a malformed/empty-outcome entry** — `src/cassette.rs:476-480`. A hand-written entry omitting `code`/`timed_out`/`signal` decodes to `Signalled(None)` → surfaces as `Error::Signalled`. Combined with round-1 P2-12 (contradictory `code`+`signal` silently picks `Exited`), the outcome decode has **no** validation. Reject contradictory/empty triples on load.
- **R2-10 · Low · file-stdin cassette serves stale output silently when the file's bytes change** — `src/stdin.rs:139` + `cassette.rs:469`. Now that docs match behavior (path-keyed; round-1 P1-5), the residual is a runtime trap: re-recording isn't triggered by content change. Hash file contents at key time, or emit a `tracing` note. (Round-1 H1 re-cast as a runtime footgun, not a docs bug.)
- **R2-11 · Low · `launch` stdin writer is fire-and-forget** — `src/runner.rs:553-560`. `BrokenPipe`/`WriteZero` from `write_to` (child closed stdin early, e.g. `head -1`) is not guarded at the spawn layer and the writer isn't cancelled on reap from here. (Round-1 P2-9 is *mitigated* in `running/mod.rs` via `observe_stdin_task`/`Drop` abort — confirmed by the pump reviewer — so this is a defense-in-depth note, not a live leak.)
- **R2-12 · Low · `process_metrics` CPU add is plain `+`, not `saturating_add`** — `src/sys/windows.rs:642` (contrast `stats()` `:382`). Overflow needs ~58,000 CPU-years (unreachable); flagged only as parity with the guarded sibling.

---

## Useful correction to round 1

- **R2-13 · Downgrade round-1 P3-16 (`Signal::Other` negative/zero).** Verified: the user-supplied value lands in the **signal** argument of `kill(pid, sig)` / `killpg(id, sig)`, never the **pid/target** argument (targets come from `cgroup.procs` / the tracked pgid, always positive). So a negative `Other` yields a confusing `EINVAL`, **not** a process-group broadcast, on both unix backends; `Other(0)` is a harmless liveness probe. P3-16 should be re-scoped to "unvalidated raw signal → confusing `EINVAL`," not a targeting hazard. (`src/signal.rs:54`, `linux.rs:542-563`, `pgroup.rs:171-176`.)

---

## Round-1 backlog — still open (re-confirmed, unchanged code)

Carried verbatim from [`2026-06-15-inspection.md`](2026-06-15-inspection.md); none regressed, none fixed beyond the P1 set:

- **P2-1** cgroup per-pid recycled-pid signal (non-SIGKILL) — stands.
- **P2-2** `skip_drop_kill` `Relaxed` with the unsound "single-threaded boundary" comment — stands, now at **4 sites** (graceful/linux/pgroup + `windows.rs:661-662`).
- **P2-4** Windows TID-reuse race in the `for_each_member_thread` suspend/resume walk — stands.
- **P2-5** Windows partial-suspend-on-mid-walk-error leaves a half-frozen tree — stands (mildly reinforced: `suspend_lock` now makes a half-frozen child more durable).
- **P2-6** `Error::Parse` `Display` un-sanitized; bidi-control (Trojan-Source) not stripped in *any* variant (`is_control()` misses U+202A–U+202E etc.) — stands.
- **P2-7** pump `pending` scratch buffer never shrinks (`skip_over_cap` uses `drain`/`clear`; `reserve` grows monotonically) — stands; defeats the H1 memory bound after one huge line.
- **P2-8** in-flight bound is `cap + one chunk`, not `cap` — stands (doc accuracy).
- **P2-10** `profile()` sampler can fold a recycled pid's metrics — stands.
- **P2-11** cgroup `members()` doesn't filter `pid <= 0` — stands.
- **P2-12** cassette accepts contradictory entries silently — stands (see R2-9).
- **P3-1** lost-wakeup safety in `StdoutLines`/`OutputEvents` rests on the re-drain loop — re-read, still appears correct, still no loom/stress test.
- **P3-2** pipeline whole-chain timeout detaches in-flight drain tasks (safe only via group-kill) — stands.
- **P3-3** `SharedLines` poison handling inconsistent (`push`/`try_pop`/`drain` `.expect`) — stands.
- **P3-6/M3** `ScriptedRunner::on_sequence` ordering undefined under concurrency — stands.
- **P3-7/M4** `RecordReplayRunner` record-mode ordering (output outside lock) — stands.
- **P3-8/M1** `into_running` line-count (`split_inclusive`) vs handler replay (`lines()`) mismatch — stands.
- **P3-10** `matched_reply` `replies[i.min(len-1)]` underflow if ever empty (invariant-held) — stands.
- **P3-11** pipeline `expect("…at least two stages")` — stands.
- **P3-13** orphaned `processkit-*` cgroup dirs accumulate across crashes — stands.
- **P3-14** cassette load unbounded alloc (no size cap) — stands.
- **Windows H1 (round 1)** nested-job / breakaway undocumented; `escalate=false` is best-effort and `sys/mod.rs` rustdoc still presents it as guaranteed — stands.

## Verified clean (re-checked this round)

- **P1-1** Windows `UncontainedChildGuard`: last-error capture vs guard-Drop ordering, drop order vs `suspend_lock`, double-kill/leak, suspended-child reap — all sound.
- **P1-3** supervisor `!is_success()`: handles every `Outcome`; only `crashed` feeds the storm gate; no residual raw-`code()==0` logic; storm math (strict `>`, budget-before-gate, window reset) correct.
- **P1-4** `wait_for_line` no longer arms the deadline: split is correct, no double/missed-arm, deferred-to-`finish` timeout enforced (anchored to spawn) for real **and** scripted backends; cancel watchdog unaffected.
- **P1-5** cassette stdin-digest docs reconciled (path identity) across README/testing.md/cassette.rs/stdin.rs.
- **P1-6** over-cap byte *accounting* (the deferred-`\r`) correct across multi-`\r`, lone-`\r`-at-EOF, `pending=="\r"`, and the `Error` ceiling — **except the adjacent retain-vs-drop gap R2-1**.
- Cassette security (env-name-only persistence, `O_NOFOLLOW`+`0600`, untrusted-JSON typed errors, lock-released-before-handlers, Send+Sync) — clean.
- Command/result/error semantics (ok_codes parity across string/bytes, signal-vs-exit-vs-timeout precedence, no argv/env *value* leak in Debug, StdioMode gating, exit-code casts) — clean.
- Pipeline fd/EOF wiring, batch waker/ordering/abort, backoff/jitter overflow guards — clean.
- Windows FFI (struct sizing, NULL vs INVALID_HANDLE_VALUE, CloseHandle-on-every-path, variable-length pid list growth) — clean.
- Cross-compile cfg gating (linux-only syscalls gated, libc constants uniform, non-unix `SIGTERM_RAW` fallback) — clean.

## Suggested fix order

1. **R2-1** (High) — the one real new correctness bug; small, localized, testable.
2. **R2-3 / R2-4** (cgroup freeze downgrade; graceful-poll pruning) — reliability-core correctness.
3. **R2-2** (Windows memory-semantics mismatch) — user-visible data correctness (doc or align).
4. **R2-6 / R2-9** (dead-branch pipe orphan; cassette outcome validation) — fail-loud / footgun removal.
5. The round-1 P2 backlog (P2-2 ordering comment + Release/Acquire is the cheapest high-value cleanup; P2-7 shrink_to; P2-6 sanitizer+bidi).
