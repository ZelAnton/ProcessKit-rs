# 2026-06-15 inspection round 4 — findings + fix plan (post-0.11.1)

Fifth fresh-eyes pass over the whole `src/` tree (~21k LOC) on the released
`v0.11.1`, after four prior rounds
([1](2026-06-15-inspection.md) · [2](2026-06-15-inspection-round2.md) ·
[3](2026-06-15-inspection-round3.md)). Five readers (core lifecycle; `sys/`
containment; command/result/error data types; runner/cassette/pipeline/batch
orchestration; structure/interface), each having read the prior reports so this
concentrates on **what is new or still-open**.

**Headline:** the crate is now exceptionally well-reviewed — no new High/Critical
bug surfaced. There is **one genuine (Low) injection-defense hole** (N4-1), **one
arithmetic-parity gap** (A1), and **one Medium API-consistency issue worth
resolving before the 1.0 freeze** (M-1, the `output` vs `output_string` cross-type
naming split). The rest are doc-accuracy notes and an already-evaluated backlog
left deferred on purpose.

---

## Bugs (the focus)

### N4-1 · Low · `is_display_unsafe` lets Unicode line/paragraph separators (U+2028 / U+2029) through
- `src/error.rs` (`is_display_unsafe`), feeding `push_sanitized_capped` /
  `append_diagnostic_tail` / `display_parse`.
- **What:** the sanitizer neutralizes control chars (`char::is_control`) plus the
  bidi-control set, but `is_control()` is **false** for U+2028 (LINE SEPARATOR)
  and U+2029 (PARAGRAPH SEPARATOR) — verified empirically. Neither is in the bidi
  list. A hostile child's stderr last line (or a `Parse` message) carrying
  U+2028/U+2029 therefore reaches a one-line `{err}` log/terminal render verbatim,
  where many terminals/log viewers treat them as a line break — partially
  defeating the "one actionable line, no injected newlines" intent the `\r`/ESC
  sanitization exists for. (U+0085 NEL *is* caught, since `is_control()` covers
  C1.)
- **Why only Low:** log-hygiene/cosmetic within an injection-defense path, not a
  memory/correctness bug — but it is the same threat model the function targets,
  so it is an honest hole.
- **Fix:** add `'\u{2028}' | '\u{2029}'` to the `matches!` in `is_display_unsafe`;
  add a regression test alongside the existing control/bidi ones. Confidence: high.

### A1 · Low (parity) · Linux per-process CPU compute uses unguarded `+` and a truncating `as u64`
- `src/sys/linux.rs` (`process_metrics` CPU branch): `(utime + stime) as u128 *
  1e9 / hz` then `Duration::from_nanos(nanos as u64)`.
- Round-2/3's N-2 switched the `stats()` *fold* to `saturating_add`, and the
  Windows FILETIME combine saturates — but this **per-process** computation still
  does a plain `u64 +` (debug-panics on overflow) and a silent `as u64` truncation
  of the nanos. Both are unreachable in practice (billions of years of ticks /
  584 years of CPU), so this is parity hardening, not a live bug.
- **Fix:** `utime.saturating_add(stime)` and saturate the final nanos cast.
  Confidence: high it's a parity gap; ~zero it's reachable.

---

## Structure / interface (freeze-readiness)

### M-1 · Medium · The String-capture verb has **two names depending on the type** (`output` vs `output_string`)
- The same operation — "run to completion, capture stdout as text, return
  `ProcessResult<String>`" — is spelled differently across the surface:
  - **`output`**: `ProcessRunner` trait + every runner impl (`JobRunner`,
    `ProcessGroup`, the test doubles), `CliClient::output`, and the free fn
    `processkit::output`.
  - **`output_string`**: `Command`, `Pipeline`, `RunningProcess`.
- The two prior decision records defended the `_string` vs `_bytes` *split*
  (payload-explicit naming) but never addressed that the String-half is named
  differently on different types. A user who learns `client.output(cmd)` then
  writes `cmd.output()` gets a compile error and must discover `output_string`.
- **Extra reason to standardize on `output_string`:** `std::process::Command::output()`
  returns `Output` whose `stdout` is **bytes**. A bare `output()` that returns a
  *text* payload is therefore surprising to anyone coming from std — so the
  `output`-returns-`String` spellings are the genuinely confusing ones.
- **Decision (chosen):** rename the text verb to **`output_string`** everywhere,
  so the whole surface is `output_string` / `output_bytes`. This extends the
  decision records' explicit-naming rationale consistently, removes the
  std-`output()`-returns-bytes footgun, and is a pre-1.0-only window (no users on
  the released crate to migrate; **breaking**, flagged in the changelog). Rejected
  alternatives: collapsing to bare `output`/`output_bytes` (reverses the recorded
  explicit-naming choice and re-introduces the std footgun); adding `output_string`
  as an alias (two names is worse for a frozen surface).
- **Blast radius:** `ProcessRunner::output` (required trait method) →
  `output_string`; all impls + `&R`/`Box`/`&dyn` forwards; `ProcessRunnerExt`
  internal `self.output(...)` calls; `batch::run_all`'s launch closure;
  `CliClient::output`; the free fn `processkit::output`; `cassette`/`doubles`
  impls; the `mockall` `expect_output` → `expect_output_string` (only referenced in
  one doc comment — no test depends on it); all docs/tests/README/doctests;
  regenerate `public-api.txt`. Mechanical; the compiler enforces completeness.

---

## Doc-accuracy notes (small)

- **D1 · Pipeline stages silently ignore per-stage `Command::retry`** — pipeline
  stages run via `group.start(...).finish()`, bypassing the `retrying` wrapper
  (retry lives only in `ProcessRunnerExt`). Consistent with the streaming `start`
  path (also never retries) and arguably correct, but the `Pipeline` rustdoc lists
  which `Command` settings are honored/overridden without mentioning retry. Add one
  line.
- **D2 · cgroup `stats()` counts unreaped zombies** — `cg.members()` reads
  `cgroup.procs`, which retains a zombie's pid until reaped; `process_metrics`
  still reads its final `/proc/<pid>/stat`. So `active_process_count` and the
  CPU/mem fold include unreaped zombies. The pgroup backend documents the
  analogous property; the cgroup `stats()` path should note it too.
- **D3 · `ResourceLimits` derives `PartialEq` but not `Eq`** (it holds
  `cpu_quota: Option<f64>`). Correct and intentional, but sibling config/stats
  types derive `Eq`, so a one-line doc note prevents a "why can't I use it as a
  map key?" surprise.
- **D4 · `output_bytes` post-timeout partial capture** — after the `PUMP_TEARDOWN`
  timeout the raw reader is aborted then `mem::take`'d; a chunk read just before
  the abort can be lost. Within the documented best-effort teardown contract;
  optionally note the boundary in the rustdoc.

---

## Deferred / accepted (evaluated, intentionally not changed)

- **P2-7 (pump `pending` scratch buffer never shrinks after a huge line)** — round
  3 prototyped a `shrink_to` fix and **dropped it** because `reserve(~3·CHUNK)`
  thrashed the hot path. Re-churning the most-reviewed hot path for a Low
  memory-retention issue a prior round already backed off from trades stability
  for little gain; keep deferred (document as accepted).
- **P3-3 (pump poison-recovery: hot-path `push`/`try_pop`/`drain` use `.expect`,
  Drop paths use `into_inner`)** — the hot-path locked sections are short and
  panic-free, so `.expect` there is consistent with the AGENTS.md lock-poison
  policy; the mix is intentional, not a bug.
- **R2-2 (Windows `peak_memory_bytes`: job-committed in `stats()` vs per-process
  working-set in `process_metrics`)** — already documented; semantic, not a bug.
- **S-3 (split `running/mod.rs` god-module)** and **S-6 (`is_drained` rename)** —
  declined/deferred in round 3 for the reasons recorded there; unchanged.
- Inherent platform hazards (P2-1 cgroup recycled-pid window; P2-3/4/5 Windows
  TID reuse in the member walk; P1-2/P3-13 blocking Drop sleep + orphaned dirs on
  host SIGKILL) — inherent, documented, accepted.

---

## Execution plan

Each stage: implement → review-loop (≥2 independent passes, fix serious, repeat
until clean) → full gate → push → next.

- **Stage 1 — Bug fixes:** N4-1 (U+2028/U+2029 sanitization + test) and A1 (linux
  saturating per-process CPU arithmetic).
- **Stage 2 — API consistency (M-1):** rename the text-capture verb to
  `output_string` across the trait, all impls/forwards, `CliClient`, and the free
  fn; achieve `output_string`/`output_bytes` symmetry everywhere. Breaking
  (pre-1.0), flagged in the changelog; regenerate `public-api.txt`.
- **Stage 3 — Doc accuracy:** D1–D4.

Full gate (per stage): `cargo fmt --check`; clippy `--all-targets` ×
{default, `--no-default-features`, `--all-features`} `-D warnings`;
`RUSTDOCFLAGS=-D warnings cargo doc --no-deps --all-features`;
`cargo test --all-features`; cross-compile `cargo check --all-targets
--all-features --target {x86_64-unknown-linux-gnu, aarch64-apple-darwin}`;
`cargo public-api --simplified --all-features | diff public-api.txt -`; plus
`cargo hack --feature-powerset --depth 2 clippy` on the final pass.

After all stages + final push: doc-conformance check, then an overall review
(≥4 passes), then final push and wait for CI.
