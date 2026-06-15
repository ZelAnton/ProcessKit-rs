# 2026-06-15 inspection — round 3 (post round-2 plan + overall review)

Third fresh-eyes pass over the whole `src/` tree (~20.3k LOC), run after the
entire [round-2 plan](2026-06-15-inspection-round2.md) shipped (8 stages,
doc-conformance, a 4-round overall review) and CI went green on `92db1929`.
Five readers, one area each (core lifecycle; `sys/` containment;
command/result/error; runner/cassette/pipeline; structure/interface). Every
reader first read both prior reports, so this one concentrates on **what is new
or was missed**, not a re-listing.

Focus order: potential **bugs** first, then vulnerabilities, then
structure/interface. Pre-1.0, no users — bold interface/architecture changes are
on the table. **List only — nothing was changed.**

**Headline:** the crate is now very heavily reviewed; the readers found little of
genuine substance left. There is **one new bug worth flagging** (N-1, a narrow
Windows leak window), two **Very Low** correctness nits (N-2, L-1), a handful of
doc/footgun Lows, and a set of **structure/interface opportunities** that are the
real remaining value — chiefly the `Pipeline` verb-parity gap and `parse` not
being reachable from `Command`.

---

## NEW — Bugs

### N-1 · Low–Medium · Windows `Job::spawn` has a suspended-orphan leak window before `UncontainedChildGuard::arm`
- `src/sys/windows.rs:175-190`.
- **What:** the child is spawned `CREATE_SUSPENDED` at `:175`, but the reaper
  guard is only armed at `:190` — *after* two fallible calls: `child.id()` (`:176`)
  and `child.raw_handle()` (`:179`). If either returns `None`, the function
  returns `Err(...)` and `child` drops **unguarded**. A tokio `Child` does **not**
  kill-on-drop by default, so a suspended child that dropped here would become a
  **permanent suspended orphan** — never resumed, never reaped, invisible to the
  job (it was never assigned).
- **Why it's only Low–Medium:** the two error paths are framed as "child exited
  before it could be assigned," but a `CREATE_SUSPENDED` child *cannot run and
  therefore cannot exit on its own* before we resume it, so in practice `id()` /
  `raw_handle()` returning `None` here is close to unreachable. The window is
  real in the code's structure but very hard to actually hit. It's flagged
  because the fix is trivial and removes a "prove it can't happen" burden.
- **Direction:** arm the guard immediately after `cmd.spawn()?` (before reading
  `id()`/`raw_handle()`), so *every* subsequent early return reaps the suspended
  child. `id()`/`raw_handle()` then read through the guard. Confidence: high the
  window exists; low it is reachable today.

### N-2 · Very Low · Linux `stats()` aggregates per-process CPU/memory with panic-on-overflow `+`
- `src/sys/linux.rs` (the `stats()` per-member fold — `cpu += c`, `mem += p`).
- **What:** the running totals use plain `+=`. A `Duration + Duration` overflow
  (or `u64 + u64` for memory) panics in debug and wraps/aborts depending on
  profile. Realistically unreachable — it would take an absurd summed CPU-time or
  memory figure — but it is an arithmetic-on-attacker-influenceable-counts spot,
  and the Windows side was already switched to `saturating_add` in round 2 for
  exactly this parity reason.
- **Direction:** use `saturating_add` for both accumulators to match Windows.
  Confidence: high it's a parity gap; very low it's reachable.

---

## NEW — Low / footguns

### L-1 · Low · One-shot streaming-stdin cassette key uses a constant digest, colliding distinct inputs on replay
- `src/cassette.rs` (the streaming-stdin hashing path).
- **What:** when stdin is supplied as a stream rather than an in-memory buffer,
  the cassette key folds in a **constant** `(3, b"<stream>")` placeholder instead
  of a digest of the actual bytes. Two replays with the *same* program/args but
  *different* streamed stdin therefore hash to the **same cassette key** and the
  second silently replays the first's recording.
- **Why Low:** `record`/`mock` are test-only seams, and streamed stdin into a
  recorded command is a narrow combination; but it is a silent-wrong-answer
  rather than an error, which is the worst failure shape for a test double.
- **Direction:** either hash the streamed bytes (buffer-then-hash, accepting the
  memory cost on the record path only) or **reject** streamed stdin in
  record/replace mode with a clear error rather than silently keying them all the
  same. Confidence: high.

### L-2 · Low · `Command::command_line()` is documented as diagnostic-only but is lossy enough to mislead if treated as runnable
- `src/command.rs` (the `command_line()` renderer) + its rustdoc.
- The hand-rolled per-platform quoter is correct for *display*, but a reader may
  copy its output into a shell. The doc says "for diagnostics," yet doesn't say
  *not for re-execution*. Low, doc-only: add an explicit "not guaranteed to round
  trip through a shell; do not re-execute" sentence.

### L-3 · Low · `Command::from_iter_lines` / line-oriented stdin helpers don't document the trailing-newline contract
- `src/command.rs` (the lines→stdin helper) + `src/doubles.rs` (`into_running`
  uses `lines().count()`).
- Whether a final line without `\n` is sent with or without an appended newline
  is left implicit. Pin it in the doc (and a unit test), because a child reading
  line-buffered stdin behaves differently for `"a\nb"` vs `"a\nb\n"`.

### L-4 · Low · `ProcessResult` is not `#[must_use]`
- `src/result.rs`.
- A non-zero exit is carried in the `Ok(ProcessResult)`, so silently dropping a
  result drops the only signal that a command failed. `#[must_use = "a
  ProcessResult carries the exit status; inspect is_success()/code()"]` would
  catch the classic "ran it, ignored whether it worked" bug at compile time.
  (Confirm it doesn't fire on the many internal sites that legitimately discard.)

### L-5 · Very Low · `RunProfile` / `ProcessGroupStats` could derive `Eq`
- `src/stats.rs`.
- Both are integer/`Duration` aggregates with no floats; deriving `Eq` (and
  `Hash` where sensible) lets callers use them as map keys / compare exactly.
  Trivial, non-breaking. (Noted in round 2 as S-A; restated as still open.)

---

## NEW — Structure / interface opportunities (the real remaining value)

### S-1 · Medium–High · `Pipeline` lacks verb/observability parity with the one-shot path
- `src/pipeline.rs`.
- The pipeline builder exposes far less than the single-command surface: no
  `output_bytes` analogue, no `cancel_on`, no per-stage `exit_code`/`probe`, no
  `first_line`. A user who reaches for a pipe loses the ergonomics they have for
  one command. This is the single largest "coverage of real user need" gap left.
- **Direction:** decide a deliberate pipeline verb set (it need not be 1:1, but
  `cancel_on` + a bytes-output form + per-stage exit inspection are the obvious
  ones) and document what is intentionally omitted. Worth a small design note in
  `decisions/` given the API will freeze.

### S-2 · Medium · `parse` / `try_parse` are not reachable from `Command`
- `src/command.rs` vs the parse helpers.
- The typed-output (`parse`) convenience exists but isn't on the `Command` verb
  vocabulary alongside `output_string`/`output_bytes`/`run`/`exit_code`, so the
  most discoverable entry point can't get to it. Either add `Command::parse` /
  `try_parse` or document the intended path. Medium because it's a discoverability
  hole in an otherwise uniform verb surface.

### S-3 · Low–Medium · `running/mod.rs` is a god-module
- `src/running/mod.rs` (~the largest file).
- It carries spawn glue, teardown, the timeout arbiter, the profile sampler, and
  the raw byte-reader. It works and is well-commented, but the breadth makes it
  the hardest file to reason about as a whole. Pre-freeze is the cheap time to
  split (e.g. `teardown.rs`, `profile.rs`) — purely internal, no API impact.

### S-4 · Low · `skip_drop_kill` flag pattern is triplicated across backends
- `src/sys/linux.rs`, `src/sys/pgroup.rs`, `src/sys/windows.rs` each carry their
  own `skip_drop_kill: AtomicBool` with the same Release-on-set / Acquire-on-read
  dance and the same Drop semantics. A shared small helper (or a documented
  trait-default) would remove the "did all three get the ordering right?" review
  burden every time this is touched. Internal-only.

### S-5 · Low · Two near-identical sanitize-and-cap loops + three `200` literals in `error.rs`
- `src/error.rs` (`append_diagnostic_tail` and `display_parse`).
- The bidi/control sanitize-and-cap logic is written twice and the 200-byte cap
  is a bare literal in three places. Factor a single
  `fn push_sanitized_capped(out, s, cap)` and a `const DIAG_CAP: usize = 200`.
  Cosmetic but reduces drift risk between the two display paths. (Noted in round 2
  structure finding #5; restated.)

### S-6 · Low · `is_drained` is named as a query but mutates the tracked set
- `src/sys/graceful.rs` (`GracefulTarget::is_drained`) + `pgroup.rs` prune.
- Round 2's R2-4 documented the prune; the contract is still a `is_*`-named
  method that writes. Consider renaming to something that signals the mutation
  (`drain_and_check` / `poll_drained`) or split the prune out, so the name stops
  lying. Low; mostly a clarity issue now that the behavior is documented.

### S-7 · Low · `output_bytes_all` / batch concurrency argument ergonomics
- `src/batch.rs`.
- `output_all` takes `concurrency: usize` clamped to ≥1; there is no
  bytes-returning batch analogue, mirroring S-1's verb-parity theme on the batch
  side. If a bytes batch is wanted it belongs here. Also consider a named type or
  `NonZeroUsize` for the concurrency knob so "0 means 1" surprise is encoded in
  the type rather than silently clamped.

### S-8 · Low · `Finished` vs `ProcessResult` shapes are close but not unified
- `src/running/stream.rs` (`Finished`) vs `src/result.rs` (`ProcessResult`).
- The streaming finish type and the one-shot result carry overlapping data in
  different shapes. Not necessarily wrong (streaming legitimately has different
  fields), but worth a deliberate note on why they differ so a future reader
  doesn't try to "fix" it — or a shared sub-struct if the overlap is exact.

---

## Verdict

Nothing here rises to the round-1/round-2 severity. **N-1** is the only new bug
and even it is structural-window-not-reachable; **N-2/L-1** are correctness nits
in test-only or unreachable paths. The durable value of this pass is the
**structure/interface list** — especially **S-1 (pipeline verb parity)** and
**S-2 (`parse` on `Command`)**, which are genuine pre-freeze coverage decisions
worth making deliberately rather than by omission.

Per the request: **no code was changed.** This is the fix-list only.
