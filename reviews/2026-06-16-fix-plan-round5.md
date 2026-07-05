# 2026-06-16 inspection round 5 — findings + fix plan

Sixth fresh-eyes pass over the whole `src/` tree (~21k LOC) on `main` (v0.11.1
base, round-4 changes unreleased), after five prior rounds
([1](2026-06-15-inspection.md) · [2](2026-06-15-inspection-round2.md) ·
[3](2026-06-15-inspection-round3.md) · [round-4 plan](2026-06-15-fix-plan-round4.md)).
Five readers, each having read every prior report (so this concentrates on what is
genuinely new or still-open). The deferred/accepted backlog from prior rounds
(P2-1, P2-3/4/5, P2-7, P3-3, R2-2, S-3, S-6, NonZeroUsize-for-batch) is
**intentionally not revisited**.

**Headline:** the crate is extremely well-reviewed; four of five areas reported
"sound, nothing new." But the lifecycle and orchestration readers each found **one
genuine bug** that the prior rounds missed — both in the "claim/guard everywhere
consistently" family, both reachable, both traced against current code. Plus one
additive ergonomics gap worth closing before the freeze.

---

## Bugs (the focus)

### R5-1 · Medium · `has_exited_now` reaps the child but never claims the timeout arbiter — a clean exit observed via a readiness probe can be misclassified `TimedOut`
- `src/running/mod.rs:1754-1776` (`has_exited_now`), vs the natural-reap claim in
  `backend_wait` (`mod.rs:1601-1606`) and the classifier `classify_timed_out`
  (`mod.rs:1521-1523`).
- **What:** every other reap path claims the B1 arbiter
  `compare_exchange(TS_PENDING, TS_EXITED, AcqRel, Relaxed)`. `has_exited_now` is
  the one reaper that does **not**: it `try_wait()`s, calls `abort_watchdogs()`
  (an *asynchronous* abort) and takes the `cancel_at_exit` snapshot (B2), but never
  CASes the arbiter — so it stays `TS_PENDING` after a real, clean reap.
- **Trigger (reachable):** a handle with a `Command::timeout` is streamed via
  `stdout_lines()` (which arms the `deadline_task` watchdog), then driven with a
  polling readiness probe (`wait_for` / `wait_for_port`, which call
  `has_exited_now`). The child exits cleanly *before* its deadline; the deadline
  then elapses and the watchdog CASes `PENDING → TS_TIMED_OUT` (nothing claimed
  `EXITED`) and SIGKILLs an already-gone tree (harmless). A later `finish()`/`wait()`
  short-circuits and `classify_timed_out` reads `TS_TIMED_OUT` → reports
  **`Outcome::TimedOut`** for a child that exited cleanly before its deadline. The
  window is wall-clock-wide (any clean exit observed after the deadline elapses),
  not just a scheduler quantum, and it is a user-visible **outcome corruption** —
  the exact class the B1 arbiter exists to prevent (the B2 cancel snapshot already
  guards the *cancel* disposition on this very path; the *timeout* claim was
  omitted). Both backends affected (the scripted `arm_scripted_deadline` path has
  the identical gap).
- **Fix:** in `has_exited_now`, on observing exit, claim
  `compare_exchange(TS_PENDING, TS_EXITED, AcqRel, Relaxed)` at the reap point
  (before `abort_watchdogs()`), mirroring `backend_wait`. Safe: a genuinely
  timed-out child (deadline already claimed `TS_TIMED_OUT`) makes the CAS fail,
  preserving `TimedOut`. Regression test: stream with a timeout, let the child exit
  before the deadline, advance past the deadline, observe via a probe, then
  `finish()` and assert the real exit (not `TimedOut`). Confidence: high.

### R5-2 · Medium · `Pipeline::run` does not fail loud on a truncated last-stage capture — silently returns a clipped tail
- `src/pipeline.rs:369-377` (`Pipeline::run`).
- **What:** every other "present stdout as if complete" verb calls
  `reject_if_truncated` before handing stdout back — `ProcessRunnerExt::run`
  (`runner.rs`, with the explicit "B12: `run` returns stdout as if complete — fail
  loud" comment), `CliClient::run`, and the pipeline's own `parse`/`try_parse`
  (via `reject_if_last_truncated`). `Pipeline::run` alone skips it:
  `output_string().await?.ensure_success()?.into_stdout().trim_end()`.
  `ensure_success()` does **not** treat truncation as a failure, so a bounded last
  stage that dropped lines returns its clipped tail silently as if complete.
- **Reachable:** the truncation flag *is* correctly re-stamped onto the pipeline
  result (`pipeline.rs` `capture`), and `Pipeline::parse` already consumes it — so
  the data is present; `run` just never checks it. A last stage with
  `.output_buffer(OutputBufferPolicy::bounded(n))` produces `truncated() == true`,
  `ensure_success()` passes, and `into_stdout()` returns the partial stdout. (Test
  gap confirms it: there is a `pipeline_parse_fails_loud_on_a_truncated_last_stage`
  test but no `run` equivalent.)
- **Why Medium:** requires an explicit bounded `output_buffer` on the last stage
  (default is unbounded), but the failure shape is a silent wrong answer (clipped
  tail as complete) and it is a clean asymmetry with three sibling verbs.
- **Fix:** rebuild `run` on the shared guard:
  `let out = self.checked().await?; self.reject_if_last_truncated(&out)?;
  Ok(out.into_stdout().trim_end().to_owned())`. Add a regression test mirroring the
  parse one. Confidence: high.

---

## Interface (additive ergonomics, freeze-worthy)

### R5-3 · Low–Medium · `Outcome` carries no accessor methods, unlike `ProcessResult`
- `src/result.rs` (`Outcome` enum: `Exited(i32)` / `Signalled(Option<i32>)` /
  `TimedOut`), returned by `RunningProcess::wait`/`shutdown` and stored in
  `Finished` / `RunProfile`.
- **What:** a user holding a bare `Outcome` (e.g. from `wait()` or
  `Finished::outcome`) must hand-`match` to ask "what exit code?" / "was it
  signalled?" — and because `Outcome` is `#[non_exhaustive]`, downstream matches
  **cannot** be exhaustive (a wildcard is forced). `ProcessResult` offers
  `code()`/`is_success()`/`timed_out()`; `Outcome` offers nothing. For a
  popular crate this is a real, frequently-hit ergonomics gap, and accessors are
  the *correct* path precisely because the enum is non_exhaustive.
- **Decision:** add unambiguous accessors — `Outcome::code() -> Option<i32>`,
  `Outcome::signal() -> Option<i32>`, `Outcome::timed_out() -> bool` (additive,
  non-breaking). **Deliberately NOT `Outcome::is_success()`:** "success" depends on
  `ok_codes`, which lives on `ProcessResult`/`Command`, not on `Outcome` — a bare
  `Outcome::is_success()` could only mean "Exited(0)" and would silently disagree
  with `ProcessResult::is_success()` for a command with custom `ok_codes`. Document
  that "success is `ok_codes`-aware; use `ProcessResult::is_success`."

### R5-4 · Low · the crate-root free-fn tier is an asymmetric half-mirror of the method vocabulary
- `src/lib.rs` exposes only `run` + `output_string` single-command free fns (plus
  batch `output_all`/`output_all_bytes`), so `processkit::output_string` has no
  `output_bytes` sibling and `run` has no `run_unit`.
- **Decision:** keep the free-fn tier deliberately minimal (adding fns is the wrong
  direction — more frozen root surface for marginal gain); add one doc sentence on
  the free fns noting the tier is intentionally a thin shim and the full vocabulary
  (`output_bytes`/`run_unit`/`checked`/…) lives on `Command`. Doc-only.

---

## Areas reported sound (nothing new)
- **sys/ containment** — all leak/recycled-pid/FFI paths map to documented,
  deferred items; round-4's saturating arithmetic verified complete.
- **command/result/error/stdin/buffer/stats/signal/mechanism/limits** — sanitizer
  (incl. round-4 U+2028/U+2029) routes all untrusted-text Display paths; no panic /
  overflow / off-by-one found.
- The two new bugs are both in the running/pipeline orchestration layer.

---

## Execution plan

Each stage: implement → review-loop (≥2 independent passes, fix serious, repeat
until clean) → full gate → push → next.

- **Stage 1 — Bug fixes:** R5-1 (`has_exited_now` arbiter claim + regression test)
  and R5-2 (`Pipeline::run` truncation guard + regression test).
- **Stage 2 — Interface ergonomics:** R5-3 (`Outcome::code`/`signal`/`timed_out`
  accessors, no `is_success`) and R5-4 (free-fn-tier minimal doc note).

Full gate (per stage): `cargo fmt --check`; clippy `--all-targets` ×
{default, `--no-default-features`, `--all-features`} `-D warnings`;
`RUSTDOCFLAGS=-D warnings cargo doc --no-deps --all-features`;
`cargo test --all-features`; cross-compile `cargo check --all-targets
--all-features --target {x86_64-unknown-linux-gnu, aarch64-apple-darwin}`;
`cargo public-api --simplified --all-features | diff public-api.txt -`; plus
`cargo hack --feature-powerset --depth 2 clippy` on the final pass.

After all stages + final push: doc-conformance check, then an overall review
(≥4 passes), then final push and wait for CI.
