# Migrating a consumer from processkit 1.2.x → 2.1.0

[‹ docs index](README.md) · prose version: [Upgrading](upgrading.md)

**Audience:** an agent (or human) bumping a project that *depends on*
`processkit` from `1.2.x` to `2.1.0`. This is the mechanical checklist — what
breaks, how to find it, and the exact edit — so you can do the bump and fix the
call sites in one pass. The [CHANGELOG](../CHANGELOG.md) `[2.1.0]` section is the
full record; this page is the "I depend on it, what do I change" view.

## At a glance

- **Bump `2.1.0`, not `2.0.0`.** `2.0.0` and `1.3.0` were **yanked** (a mistaken
  publish and a semver-broken minor, respectively). Upgrade straight from
  `1.2.x` to `2.1.0`:
  ```toml
  processkit = "2.1"   # was "1.2" / "1"
  ```
  `processkit = "1"` will **not** pull `2.x` — the major is an explicit opt-in.
- **MSRV unchanged:** still Rust **1.88**.
- **Most breaks are compiler-caught** (renames + `#[non_exhaustive]`). Do the
  bump, run `cargo build`, and fix what the compiler flags.
- **One break is *not* compiler-caught** — the `output_bytes` byte-cap behavior
  change ([below](#behavior-change-not-compiler-caught-output_bytes-byte-cap)).
  Read that section even if the build is green.
- No feature-flag changes; the default feature set is the same
  (`process-control` on, `stats`/`limits`/`mock`/`tracing`/`record` opt-in).

## Step 1 — find affected call sites

Run these from the consumer repo before bumping; each maps to a fix below.

```bash
# Removed / renamed symbols (mechanical) — any hit needs an edit:
rg -n 'terminate_all|avg_cpu\b|memory_max|\.exit_code\b' --type rust
rg -n 'use\s+processkit::(Encoding|StreamExt)\b' --type rust
rg -n 'output_contains_any' --type rust        # only the &[..] form, see below

# Destructuring/constructing Error variants by fields (non_exhaustive break):
rg -n 'Error::(Exit|Timeout|Signalled|Spawn|NotFound|Parse|OutputTooLarge|Stdin|ResourceLimit)\s*\{' --type rust

# The output_bytes byte-cap behavior change (review, not a mechanical edit):
rg -n 'output_bytes' --type rust
```

## Step 2 — mechanical renames (compiler-caught)

| Before (1.2.x) | After (2.1.0) |
|---|---|
| `ProcessGroup::terminate_all()` | `ProcessGroup::kill_all()` |
| `RunProfile::avg_cpu()` | `RunProfile::avg_cpu_cores()` |
| `profile.exit_code` (field) | `profile.code()` (method, same `Option<i32>`) |
| `ResourceLimits::memory_max` field / `.memory_max(n)` (`limits`) | `max_memory` / `.max_memory(n)` |
| `Error::OutputTooLarge { line_limit, byte_limit, .. }` | `{ max_lines, max_bytes, .. }` |
| `use processkit::Encoding;` | `use processkit::prelude::Encoding;` |
| `use processkit::StreamExt;` | `use processkit::prelude::StreamExt;` |

`terminate_all`/`avg_cpu` were deprecated forwarding aliases since 1.1.0; 2.1.0
removes them. `RunProfile::exit_code` (the field) duplicated `outcome.code()` and
is gone — `profile.code()` is the one accessor now.

`ProcessResult::output_contains_any` now takes
`impl IntoIterator<Item = impl AsRef<str>>` instead of `&[&str]`. The **old call
still compiles** (`result.output_contains_any(&["a", "b"])`), so this is not a
required edit — but you can now drop the `&`: `output_contains_any(["a", "b"])`,
a `Vec<String>`, etc.

## Step 3 — `Error` is now field-sealed (`#[non_exhaustive]` per variant)

The data-carrying `Error` variants — `Exit`, `Timeout`, `Signalled`, `Spawn`,
`NotFound`, `Parse`, `OutputTooLarge`, `Stdin`, and `ResourceLimit` (`limits`) —
are each `#[non_exhaustive]` now. Two consequences for consumer code:

**You can no longer construct these variants with a struct literal**, and **you
can no longer destructure them field-exhaustively.** Both are compile errors
outside the crate. Fixes:

- **Reading fields** → add `..` to the pattern and use accessors instead of
  raw fields:
  ```rust
  // Before:
  if let Error::Exit { code, stderr, program } = &err { /* ... */ }
  // After:
  if let Error::Exit { .. } = &err {
      let code = err.code();        // Option<i32>
      let stderr = err.stderr();    // Option<&str>
      let program = err.program();  // Option<&str>
  }
  ```
- **Prefer the accessors over matching at all** — they read every failure shape
  without a `match`:

  | Accessor | Returns |
  |---|---|
  | `err.program()` | `Option<&str>` |
  | `err.stdout()` / `err.stderr()` | `Option<&str>` |
  | `err.stdout_bytes()` | `Option<&[u8]>` — exact bytes, only on a checking verb over `output_bytes` |
  | `err.combined()` | `Option<String>` (stdout-then-stderr) |
  | `err.code()` / `err.signal()` | `Option<i32>` |
  | `err.is_timeout()` / `is_cancelled()` / `is_signalled()` | `bool` |
  | `err.is_not_found()` / `is_permission_denied()` / `is_transient()` | `bool` |

- **`ResourceLimit` changed shape** (`limits` feature):
  ```rust
  // Before:  Error::ResourceLimit { message }
  // After:   Error::ResourceLimit { detail, .. }
  // Better:  classify without destructuring the non_exhaustive variant —
  err.limit_kind()    // Option<LimitKind>   : Memory | Processes | Cpu
  err.limit_reason()  // Option<LimitReason> : Invalid | Unsupported | Unenforceable
  ```

- **New field on `Exit`/`Timeout`/`Signalled`:** `stdout_bytes: Option<Vec<u8>>`.
  You don't add it anywhere; the `..` pattern absorbs it, and `err.stdout_bytes()`
  reads it. It's populated only when the error came from a checking verb over
  `output_bytes` (`output_bytes().await?.ensure_success()?`); `None` on the text
  path.

If your code builds custom `ProcessRunner` doubles or error-classifier tests
that *constructed* these variants, use the `#[doc(hidden)]` constructors
`Error::exit` / `Error::timeout` / `Error::signalled` instead of struct literals.

## Behavior change (not compiler-caught): `output_bytes` byte cap

`output_bytes` now honors the `OutputBufferPolicy` **byte** ceiling
(`with_max_bytes`) on its raw stdout capture — previously the byte cap bounded
only the line-pumped stderr, and raw stdout was always unbounded.

- If you **do not** set a byte cap: **no change** — capture stays unbounded.
- If you set `with_max_bytes(n)` **and** `OverflowMode::Error`: an `output_bytes`
  run that exceeds `n` now fails with `Error::OutputTooLarge` (`max_lines: None`
  — raw bytes have no line count) where before it silently returned all bytes.
- With a drop `OverflowMode`: retained bytes are now bounded to a head/tail and
  `ProcessResult::truncated` is set.

**Action:** if you capture bytes from a large/flooding child under a byte-capped
policy, decide whether you want the new fail-loud/truncation or an explicit
unbounded capture, and adjust the policy (or add a `timeout`) accordingly.

## Behavior fixes worth knowing (no code change required)

These change *observed runtime behavior* for the better; no edit needed, but if
your tests asserted the old behavior, update them:

- **Pipelines:** each stage now runs in its own kill-on-drop sub-group, so a
  **per-stage `Command::timeout`** tears down that stage's whole subtree
  (grandchildren of a forking `sh -c …` included), not just its direct child. A
  checked stage failure now also tears the chain down **proactively** instead of
  waiting for pipe EOF — a quiet upstream producer can no longer hold a failed
  chain open.
- **Streaming teardown:** a shared-group streaming timeout's final `SIGKILL` no
  longer races `RunningProcess::Drop` — a child that caught the graceful signal,
  closed stdout, and kept running is now reliably killed.
- **Bare `finish()`** (no preceding `stdout_lines()`) no longer enforces the
  capture policy's overflow cap over output you didn't ask to capture (it could
  spuriously `Error::OutputTooLarge`); it discards instead.
- **`ScriptedRunner`** pending replies (`record`/test doubles) now respect
  `Command::timeout` on the bulk verbs, and **`RecordingRunner::output_bytes`**
  forwards to the inner runner instead of the `start`-based default.

## New capabilities you can adopt (optional — nothing breaks if you ignore)

- **`Command::priority(Priority)`** — run a child at reduced/raised CPU priority
  (`nice`/`setpriority` on Unix, priority class on Windows; never `Unsupported`).
- **`Command::umask(u32)`** — Unix file-mode creation mask (`Unsupported`
  elsewhere).
- **`Command::line_terminator(LineTerminator)`** (+ `stdout_`/`stderr_`
  variants) — split streams on `\r` for carriage-return progress output
  (`curl`/`pip`/`apt` bars) instead of accumulating one giant line.
- **`Command::timeout_opt(Option<Duration>)`** — `Some(d)`→`timeout(d)`,
  `None`→`no_timeout()`, for config-driven call sites.
- **`Command::retry_never()`** — explicit one-shot opt-out of a client
  `default_retry`.
- **`client.run(&args)`** now works with a `&[S; N]` array reference directly
  (no more `&args[..]`).
- **`Supervisor::give_up_when(classifier)`** — stop restarting on a *permanent*
  failure (e.g. a missing binary) instead of looping forever; reports
  `StopReason::GaveUp`.
- **`testing::DryRunRunner`** — a no-spawn double that renders commands (the seam
  behind a `--dry-run` mode).
- **`testing::Reply::with_stderr(text)`** — attach stderr to a scripted reply,
  including a successful one, without the misleading `Reply::fail(0, …)`.
- **Cassettes (`record`)** now record & replay *failed* invocations (a spawn
  error, a not-found) instead of missing on them.

## Step 4 — verify

```bash
cargo update -p processkit --precise 2.1.0   # or edit Cargo.toml then `cargo build`
cargo build
cargo clippy --all-targets
cargo test
```

A green build means all the compiler-caught breaks are handled; re-check the
[`output_bytes` byte-cap](#behavior-change-not-compiler-caught-output_bytes-byte-cap)
section for the one that a build won't surface.
