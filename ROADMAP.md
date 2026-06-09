# ProcessKit roadmap

> **What this is.** The committed near-term work — the **"do now"** bucket of a
> development sweep run 2026-06-09 across user-scenario coverage, simplification,
> stabilization, testing, extensibility, docs, Rust/GitHub conventions, and
> cross-language borrowing. Open ideas not yet committed live in
> [`ideas/`](ideas/) (tagged `next-` = reconsider first, `later-` = further out);
> settled "won't do / won't change" decisions live in
> [`decisions/`](decisions/). See [`ideas/README.md`](ideas/README.md) for the
> taxonomy.
>
> **Pre-1.0 freedom.** The crate is in strong pre-release: structure, architecture,
> and the public interface can change freely. Backward compatibility is **not** a
> constraint on any item below — getting the shape right *before* 1.0 is the point.
>
> Items are roughly ordered by leverage, not strict sequence. Cost is a gut
> estimate (trivial / moderate / major).
>
> **Status (2026-06-09).** Items **1–9 shipped in `0.9.1`** (Phase A: 1–4; Phase B:
> 5–9 — see [`CHANGELOG.md`](CHANGELOG.md) `[0.9.0]`/`[0.9.1]`). **Item 10** (highest-risk
> test gaps) is the only one still open.

## The ten

### 1. ✅ README polish — badges, alternatives comparison, MSRV up front
*Dimension: docs / conventions · Cost: trivial* — **Shipped 0.9.1** (badge block, "How it compares" table, MSRV line).

The README has a strong "why" and a feature table but **no badges** (crates.io,
docs.rs, CI, license, MSRV) and **no positioning vs alternatives**
(`std::process`, `tokio::process`, `duct`, `command-group`, `async-process`). Both
are the first things an evaluating user looks for. Add the badge block, a short
"How it compares" table (the containment guarantee is the differentiator), and an
explicit MSRV line in the intro. The 3.6 MB `cover.png` should also be checked —
consider a lighter asset.

### 2. ✅ Community-health files — SECURITY, CODE_OF_CONDUCT, templates, dependabot
*Dimension: conventions · Cost: trivial* — **Shipped 0.9.1** (`SECURITY.md`, `CODE_OF_CONDUCT.md`, issue/PR templates, `dependabot.yml`).

Missing: `SECURITY.md`, `CODE_OF_CONDUCT.md`, `.github/ISSUE_TEMPLATE/`,
`.github/PULL_REQUEST_TEMPLATE.md`, `.github/dependabot.yml`. `SECURITY.md` is not
boilerplate here — this crate **drops privileges, joins cgroups, and assigns Job
Objects**, so a disclosure path matters. The PR template should carry the existing
pre-PR checklist from `CONTRIBUTING.md` (fmt, clippy `-D warnings`, changelog
bullet). Dependabot complements the manual `cargo deny` scan.

### 3. ✅ Enforce doc coverage + manifest metadata
*Dimension: stabilization / conventions · Cost: trivial* — **Shipped 0.9.1** (`#![warn(missing_docs)]`, `documentation`/`homepage` filled, `unsafe_op_in_unsafe_fn = deny`).

Doc quality is high in practice but **not enforced**. Add `#![warn(missing_docs)]`
(promote to `deny` in CI) to lock it in before 1.0, and fill the `documentation`
and `homepage` `Cargo.toml` fields. Decide and document the unsafe-code posture:
the crate can't `#![forbid(unsafe_code)]` (FFI in `sys/`), so adopt
`#![deny(unsafe_op_in_unsafe_fn)]` + a `# Safety` comment convention instead.

### 4. ✅ `examples/` directory, built in CI
*Dimension: docs / adoption · Cost: moderate* — **Shipped 0.9.1** (`run_and_capture`, `process_group`, `streaming`, `pipeline`, `cli_client`).

There are doc snippets but no `examples/`. Add a handful of runnable, focused
examples (basic run-and-capture, kill-on-drop process group, streaming with a line
handler, a pipeline, a `CliClient` wrapper) and build them in CI so they can't rot.
They double as adoption on-ramps and smoke tests.

### 5. ✅ Pre-1.0 public-API review pass
*Dimension: stabilization / simplification · Cost: moderate* — **Shipped 0.9.1** (`#[non_exhaustive]` added to the five option structs; full sweep in [`decisions/pre-1.0-api-review.md`](decisions/pre-1.0-api-review.md)).

The single highest-leverage pre-1.0 task. A deliberate sweep of the public surface
for: naming consistency, **`#[non_exhaustive]` on every public enum/struct that
might grow** (so 1.0 doesn't freeze them — `Error` already has it; audit the rest),
re-export hygiene (the `tokio_util::CancellationToken` and `encoding_rs::Encoding`
re-exports leak dependency types — decide deliberately), module organization, and
the breadth of the verb surface (`output_string`/`output_bytes`/`run`/`probe`/
`first_line` × the `CliClient` `text`/`capture`/`unit`/`code`/`parse` set — confirm
each earns its place; the audit already rejected collapsing some). Deliverable: a
`decisions/` record + the uncontentious renames/`non_exhaustive` additions applied.

### 6. ✅ Enrich `ProcessResult` — duration, rendered command, truncation flag
*Dimension: user-scenario / honesty · Cost: trivial–moderate* — **Shipped 0.9.1** (`ProcessResult::duration()`, `Command::command_line()`, `ProcessResult::truncated()`).

Three gaps competitors (execa, CliWrap) cover and callers currently hand-roll:
- **`duration`** — `RunningProcess::elapsed()` is live-only; the finished result
  drops it. Cheap timing without wrapping every call in `Instant::now()`.
- **rendered/escaped command accessor** — a copy-pasteable, shell-quoted argv
  string for logs and error messages (opt-in accessor — **not** a `tracing` field;
  honor the existing "never log argv/env" rule).
- **`truncated` flag** — when a bounded `OutputBufferPolicy` drops data, the result
  doesn't *say so*. The audit keeps the unbounded default to avoid silent loss; this
  closes the same gap for callers who *do* bound. (Uses the shared quoting helper
  from item 9.)

### 7. ✅ Tolerant exit-code checking — `ok_codes` / accepted-status set
*Dimension: user-scenario · Cost: trivial* — **Shipped 0.9.1** (`Command::ok_codes([..])` feeding the checking verbs).

`ensure_success` is zero-only; `probe` is 0/1. Tools like `grep` (1 = no match),
`diff` (1 = differs), and rsync code families force callers to hand-match. Add an
accepted-exit-code set (e.g. `Command::ok_codes([0, 1])`) feeding the checking
verbs. Borrowed from mixlib-shellout `valid_exit_codes` / zx `nothrow`.

### 8. ✅ Run-level graceful timeout — SIGTERM-then-force-kill, configurable signal
*Dimension: user-scenario / correctness · Cost: moderate* — **Shipped 0.9.1** (`Command::timeout_grace(Duration)` + `timeout_signal(Signal)`; Windows = atomic kill).

Today `ProcessGroup::shutdown` has the graceful SIGTERM → wait → SIGKILL tier, but
a plain `Command::timeout` **hard-kills** — a child loses in-flight work on
deadline with no chance to flush. Wire the existing graceful tier into the run-level
timeout path (`grace`/`force_kill_after` knob) and allow choosing the signal
(`Signal` already exists). Borrowed from GNU `timeout --kill-after/--signal`, execa
`forceKillAfterDelay`. Containment is unchanged — this is signal policy on teardown.

### 9. ✅ Spawn-error quality — rendered command, cwd pre-check, quoting helper
*Dimension: user-scenario / ergonomics · Cost: trivial* — **Shipped 0.9.1** (`command_line()` quoting helper, `current_dir` pre-check with clear error). Also fixed: `Error::Exit` now carries full streams.

Make failures self-explanatory: embed the rendered (quoted) command in
`Error::Spawn`'s `Display`; validate `current_dir` exists up front (clear error
instead of an opaque spawn failure); and expose the **shell-quoting helper** that
both this and item 6 share (per-platform argv → quoted string, for display only —
the crate stays shell-free). Builds on the already-shipped `is_not_found()` /
`is_permission_denied()` classifiers.

### 10. ⬜ Close the highest-risk test gaps — **OPEN** (the only remaining item)
*Dimension: testing · Cost: moderate*

Two concrete risk zones the suite under-covers (full inventory in
[`ideas/later-advanced-testing.md`](ideas/later-advanced-testing.md)):
- **Cancellation races** — token fired *between spawn and wait* and *mid-pump*; the
  `tokio::select!` arms in `running/mod.rs` are only tested with a token that fires
  well after spawn settles. Risk: double-kill / missed cancel.
- **Pump edge cases** — last line with **no trailing newline**, a multibyte
  sequence **split across read chunks**, and **CRLF normalization** validated on a
  real Windows child. Cheap, unit/integration-level, and currently gaps.

---

## Notes on what did *not* make the cut (and why)

- **CI hardening** (cargo-semver-checks, cargo-hack feature-powerset,
  minimal-versions, coverage+codecov, clippy pedantic) → `ideas/next-`. Valuable,
  but semver-checks pays off *after* 1.0 (pre-1.0 we break freely), and the existing
  3-config × 3-OS matrix is already strong.
- **Stdio inherit/null modes + output tee, merged stdout+stderr ordering, unified
  event stream** → `ideas/next-output-handling.md`. High value but design-heavy
  (capture is load-bearing in the current pump); want them right, not rushed.
- **Runtime-agnostic / lite sync split / PTY** → `ideas/later-*` — all gated on a
  concrete consumer (see those records).
- **Built-in shell mode, IPC/message-passing, detached-as-default, miri** →
  [`decisions/wont-do-2026-06.md`](decisions/wont-do-2026-06.md).
