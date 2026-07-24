# Upgrading processkit

Per-version notes for **consumers** moving their dependency forward: what breaks,
who it affects, and the exact change to make. The [CHANGELOG](../CHANGELOG.md) is
the full record; this page is the "I depend on it, what do I do" view.

> **Versioning.** From 1.0.0 onward `processkit` follows
> [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the public API is
> stable, and any breaking change lands only in a new **major** version, so `2.x`
> upgrades are backward-compatible. The default Cargo requirement `processkit = "2"`
> (or `"2.1"` to also require the latest breaking release) already does the right
> thing — it allows `2.*` but not `3.0`. Skim the relevant section here before each
> major bump. (The `mock` feature's `mockall`-generated `expect_*` surface stays
> semver-exempt — it tracks the `mockall` version.)

## 3.0.0 (from 2.x)

### `Error` is now a pointer-sized wrapper over `ErrorReason`

`Error` changed from an enum into a thin `struct Error { .. }` holding a
`Box<ErrorReason>`, so it is one pointer wide instead of ~100 bytes. This
shrinks every `Result<T, Error>` on the run path (and any enum that embeds one)
and silences the default `result_large_err` / `large_enum_variant` clippy lints.
The former enum — with **all** its variants and fields unchanged — is now the
re-exported [`ErrorReason`], reached through `err.reason()`.

**Who it affects:** anyone that pattern-matches an `Error` by variant. The read
accessors (`code()`, `program()`, `diagnostic()`, `is_timeout()`,
`stdout_bytes()`, …), `Display`, `Debug`, and `source()` are **unchanged** and
still work on `Error` directly — only direct variant matches need a fix.

Fix: reach the variant through `reason()` (borrow) or `into_reason()` (own).

Before:

<!-- `text`, not `rust`: the pre-3.0 enum-variant match no longer compiles now
     that `Error` is a struct — this crate's CI runs doctests, which would
     reject it as `rust`. -->
```text
match err {
    Error::Exit { code, .. } => eprintln!("exit {code}"),
    Error::Timeout { .. } => eprintln!("timed out"),
    _ => {}
}
```

After:

```rust,no_run
use processkit::{Error, ErrorReason};
fn handle(err: Error) {
    match err.reason() {
        ErrorReason::Exit { code, .. } => eprintln!("exit {code}"),
        ErrorReason::Timeout { .. } => eprintln!("timed out"),
        _ => {}
    }
    // To move a captured stream or the owned `io::Error` out of the reason,
    // consume the wrapper instead: `match err.into_reason() { .. }`.
}
```

The `#[doc(hidden)]` constructors (`Error::exit`/`timeout`/`signalled`/`spawn`/
`not_found`/`stdin`) and the public `Error::parse(..)` are unchanged and still
return an `Error`.

### The merged output stream is now a process-lifecycle stream (`output_events()` → `events()`, `OutputEvent` → `ProcessEvent`)

The merged output-event stream widened from "which output line" to "an event in the
process's life", so the verb and its enum are renamed to match — a deliberate 3.0
break with **no** deprecated alias:

| Before | After |
|---|---|
| `RunningProcess::output_events()` (verb) | `RunningProcess::events()` |
| `PipelineSession::output_events()` (verb) | `PipelineSession::events()` |
| `OutputEvent` (event enum) | `ProcessEvent` |
| `OutputEvents` (stream type) | `ProcessEvents` |

**Who it affects:** anyone that calls `output_events()` or matches an `OutputEvent`.
The rename is **compiler-caught** — a build after the bump flags every site (*"no
method named `output_events`"* / *"cannot find type `OutputEvent`"*).
`ProcessEvent::Stdout`/`Stderr` carry the same `OutputLine` payload with unchanged
semantics, and `ProcessEvent::text()` still returns `Some` for a line event and
`None` otherwise. The stream also gained two **lifecycle** variants —
`ProcessEvent::Started { pid }` (leads the stream) and `ProcessEvent::Exited(Outcome)`
(ends it) — so one stream now carries `Started` → `Stdout`/`Stderr` → `Exited`. The
enum stays `#[non_exhaustive]`, so a `_` arm covers them (and any future kind).

Fix — rename the verb and the type, and add a `_` arm.

Before:

<!-- `text`, not `rust`: `output_events()`/`OutputEvent` no longer exist (renamed,
     no alias), and `running`/`events` are free variables with no async subprocess
     context here — so this can't compile. This crate's CI runs the doctests in
     this file (`src/doc_examples.rs` includes it), so a `rust` fence would fail. -->
```text
use processkit::OutputEvent;

let mut events = running.output_events()?;
while let Some(ev) = events.next().await {
    match ev {
        OutputEvent::Stdout(line) => println!("out: {}", line.text()),
        OutputEvent::Stderr(line) => eprintln!("err: {}", line.text()),
        _ => {}
    }
}
```

After — the verb is `events()`, the enum is `ProcessEvent`, and the new lifecycle
variants are handled (or fall through the `_` arm):

```rust,no_run
use processkit::ProcessEvent;
fn handle(ev: ProcessEvent) {
    match ev {
        ProcessEvent::Stdout(line) => println!("out: {}", line.text()),
        ProcessEvent::Stderr(line) => eprintln!("err: {}", line.text()),
        ProcessEvent::Started { pid, .. } => eprintln!("started: {pid:?}"),
        ProcessEvent::Exited(_outcome) => eprintln!("exited"),
        _ => {}
    }
}
```

**Behavior change — drive the stream *concurrently* with the finisher (not
compiler-caught).** Because `Exited` is delivered when the run is reaped, the stream
now parks after both pipes close and yields its terminal `Exited` only once the run
is finished. So the old "drain the stream to its end, *then* call `finish()`/`wait()`"
shape deadlocks — the stream is waiting for the reap that `finish()` performs. Drive
the two **together** instead (e.g. `tokio::join!` the stream loop and `finish()`), or
`wait()`/`finish()` on a separate task while you consume the stream. If you only used
`output_events()` for its output lines and always `finish()`ed separately afterward,
switch to consuming both concurrently.

### `output_bytes` and `OutputTooLarge` now count *raw* bytes read from the pipe

Not compiler-caught. The `max_bytes` ceiling (`OverflowMode::Error` and the drop
modes) and the `total_bytes` an `OutputTooLarge` failure reports now count the **raw**
bytes read off the output pipe — including line terminators and invalid-UTF-8 bytes —
rather than the decoded line-content bytes they counted before. For typical ASCII/UTF-8
line output the two are identical; they diverge for output with CRLF terminators or
non-UTF-8 bytes, where the raw count is slightly higher.

**Who it affects:** a caller that set a byte cap (`with_max_bytes`) and depends on the
*exact* threshold at which capture truncates/errors, or that reads
`OutputOverflow::total_bytes()` / the `total_bytes` field and compares it against a
precise expected value. Fix: re-check those thresholds/assertions against the raw-byte
count. If you set no byte cap, nothing changes.

### `ProcessGroup::signal` reports the soft-stop outcome more truthfully

Two behavior changes to `ProcessGroup::signal(Signal::Int | Signal::Term)`, neither
compiler-caught (the signature is unchanged, `Result<()>`):

- **Windows:** it now best-effort soft-closes the tree (a console `CTRL_BREAK` to
  `windows_graceful_ctrl_break` leaders plus `WM_CLOSE` to windowed members) and
  returns `Ok` when it had something to signal, instead of *always* returning
  `Error::Unsupported`. It still returns `Unsupported` only when the group has neither
  a console-CTRL leader nor a windowed member. A caller that treated the old blanket
  `Unsupported` as "Windows never soft-stops" should stop assuming that.
- **POSIX process-group mechanism (macOS/BSD, and the Linux process-group fallback):**
  a genuinely failed send now surfaces as `Error::Io` instead of being swallowed behind
  a false `Ok` — an `EINVAL` (an out-of-range `Signal::Other(n)`) or an `EPERM` from a
  live, non-zombie member now reaches the caller. An already-exited member (`ESRCH`), a
  harmless zombie-only `EPERM`, an empty group, and the `Signal::Other(0)` existence
  probe still report `Ok`. A caller that ignored the return value is unaffected; one
  that inspects it now sees these real failures.

### Also new in 3.0 (additive — nothing to migrate)

These are new capabilities, not migrations — no code changes are forced. Reach for them
if they help:

- `Command::spawn_detached()` → `DetachedChild` — the one deliberate, opt-in escape
  from kill-on-drop containment, for a child meant to *outlive* its launcher
  (daemonize, a `nohup`-style helper). It **inverts the crate's headline guarantee on
  purpose**, so it is a separate, minimal type (just the `pid`) and loudly refuses every
  owner-dependent knob rather than dropping it silently. See its rustdoc before using it.
- `Command::capture_policy(...)` + the `CapturePolicy` trait and `OutputStream` enum — a
  typed redaction-at-capture seam: transform each captured line (e.g. scrub a secret)
  *before* it is retained in the backlog / `ProcessResult`. The handler/tee/`output_bytes`
  paths still see the unredacted text — only the retained capture is rewritten — and a
  panicking policy fails closed (the line is dropped, never leaked).
- `Command::to_tokio_command()` is no longer `#[doc(hidden)]` — it is now a documented,
  honest low-level escape hatch (pair it with `ProcessGroup::spawn` to keep containment
  while dropping the high-level verbs/pump/capture). See the "Escape hatch" section in
  the commands guide.
- An opt-in **PTY launch mode** behind the new `pty` feature (`Command::use_pty()`) for a
  tool that demands a controlling terminal.

### Verify the upgrade

```sh
cargo update -p processkit
cargo build      # the events()/ProcessEvent rename and the Error struct change are compiler-caught
cargo test       # catches the events()-concurrency, output_bytes byte-count, and signal behavior changes if you rely on them
```

## 2.1.0 (from 1.2.x)

> **2.0.0 and 1.3.0 were withdrawn — upgrade straight from 1.2.x to 2.1.0.**
> `2.0.0` was published in error and yanked; `1.3.0` accidentally shipped this
> breaking batch under a *minor* bump and was yanked too. `2.1.0` is the first
> supported release of the changes below — the crate follows semver, so this
> break lands in a major as intended. There is nothing extra to do for the skip;
> the migration from a `1.2.x` dependency is exactly the notes here.

Mostly mechanical renames — **caught by the compiler** — plus two
`#[non_exhaustive]` tightenings on `Error` (also compiler-caught, once you stop
destructuring the affected variants field-exhaustively) and one genuine
**behavior** change on `output_bytes` that a build alone won't surface.

### Renames (mechanical — compiler-caught)

| Before | After |
|---|---|
| `Error::OutputTooLarge { line_limit, byte_limit, .. }` | `Error::OutputTooLarge { max_lines, max_bytes, .. }` |
| `ResourceLimits::memory_max` (field, `limits` feature) / `.memory_max(n)` builder | `ResourceLimits::max_memory` / `.max_memory(n)` |
| `ProcessGroup::terminate_all()` | `ProcessGroup::kill_all()` |
| `RunProfile::avg_cpu()` | `RunProfile::avg_cpu_cores()` |
| `RunProfile::exit_code` (field) | `profile.code()` (method — same `Option<i32>`) |
| `use processkit::Encoding;` | `use processkit::prelude::Encoding;` |
| `use processkit::StreamExt;` | `use processkit::prelude::StreamExt;` |
| `result.output_contains_any(&["a", "b"])` | `result.output_contains_any(["a", "b"])` (now `impl IntoIterator<Item = impl AsRef<str>>` — a bare array, `Vec<String>`, or slice all work directly, without the `&`; the old `&["a", "b"]` call still compiles too) |

The `terminate_all` / `avg_cpu` entries were deprecated forwarding aliases since
1.1.0 (see the [1.1.0 changelog entry](../CHANGELOG.md#110---2026-06-28)); this
release removes them outright. `RunProfile::exit_code` duplicated
`outcome.code()`, which `RunProfile::code()` already exposed — the field is gone,
the method is the one accessor now.

### `Error`'s data-carrying variants are now individually `#[non_exhaustive]`

`Exit`, `Timeout`, `Signalled`, `Spawn`, `NotFound`, `Parse`, `OutputTooLarge`,
`Stdin`, and — with the `limits` feature — `ResourceLimit` can no longer be
struct-literal-constructed or field-exhaustively destructured outside the crate.

Before:

<!-- `text`, not `rust`: the pre-2.1.0 exhaustive-destructure shape this
     section is *about* removing no longer compiles against
     `#[non_exhaustive]` `Error::Exit` — and this crate's CI runs doctests
     with `--include-ignored`, which still compiles `ignore` blocks (only
     `text`/non-`rust` fences are exempt). -->
```text
match err {
    Error::Exit { program, code, stdout, stderr } => { /* ... */ }
    _ => {}
}
```

After — add `..` to the pattern (or, better, use the existing accessors instead
of destructuring at all):

```rust,no_run
use processkit::{Error, ErrorReason};
fn handle(err: Error) {
// Since 3.0 the variants live on `ErrorReason`, reached via `err.reason()`.
match err.reason() {
    ErrorReason::Exit { program, code, stdout, stderr, .. } => { let _ = (program, code, stdout, stderr); }
    _ => {}
}

// or, accessor-based and immune to the next field addition:
if let Some(code) = err.code() {
    // err.program() / err.stdout() / err.stderr() / err.combined() also work
    let _ = code;
}
}
```

This is prep for future field additions to any of these variants without
another breaking change — the `Exit`/`Timeout`/`Signalled` variants already
gained one such field this release (next entry).

### `Error::Exit` / `Timeout` / `Signalled` gain a `stdout_bytes` field

A new field, `stdout_bytes: Option<Vec<u8>>`, carries the **exact** captured
stdout bytes for a checking-verb error built over `output_bytes`
(e.g. `output_bytes().await?.ensure_success()?`); read it through
`Error::stdout_bytes() -> Option<&[u8]>`, not by destructuring the variant
directly (they are `#[non_exhaustive]` — see above). `None` on the text path
(`output_string`/`run`/`checked`/…), where the decoded `stdout` string is
already the whole story.

### `Error::ResourceLimit` is restructured (`limits` feature)

| Before | After |
|---|---|
| `Error::ResourceLimit { message: String }` | `Error::ResourceLimit { kind: LimitKind, reason: LimitReason, detail: String }` |

Fix a match:

<!-- `text`, not `rust`: bare match arms (no enclosing `match`/subject),
     mixing the removed pre-2.1.0 `{ message }` shape with the current one —
     see the note above on why `text` rather than `ignore`. -->
```text
// Before
Error::ResourceLimit { message } => warn!("limit rejected: {message}"),

// After
Error::ResourceLimit { detail, .. } => warn!("limit rejected: {detail}"),

// or, branch on the structured classification instead of parsing text:
if let (Some(kind), Some(reason)) = (err.limit_kind(), err.limit_reason()) {
    match (kind, reason) {
        (LimitKind::Memory, LimitReason::Unsupported) => { /* ... */ }
        _ => {}
    }
}
```

### `output_bytes` now honors the byte cap on stdout too — a behavior change

Not compiler-caught: if you configured an `OutputBufferPolicy` byte ceiling
(`with_max_bytes`) and called `output_bytes`, the cap previously bounded only
the line-pumped **stderr**; raw **stdout** capture was unbounded regardless of
the configured `max_bytes`. It now applies to both streams:

- `OverflowMode::Error` past the cap now errors on stdout overflow too, with
  `Error::OutputTooLarge { max_lines: None, .. }` (raw bytes have no lines).
- The drop modes (head/tail) now bound retained stdout bytes the same way they
  already bounded stderr, and set `ProcessResult::truncated`.

If nothing sets a byte cap, capture stays unbounded exactly as before — nothing
to do. If you do set one and rely on `output_bytes` returning the **full**
stdout regardless, re-check that call site: it now truncates/errors like every
other capture path under the same policy.

### Cassette replay: `cwd` no longer part of the match key — no action needed

`RecordReplayRunner` (`record` feature) replays a cassette recorded from one
absolute working directory against the same invocation run from a different
one, instead of `CassetteMiss`ing — `cwd` is still stored on each entry for
visibility, it just no longer discriminates two otherwise-identical recorded
runs. The on-disk format revision bumped to `3`, but this is not a compatibility
gate: a cassette written by a 1.x build still loads and replays fine. The one
edge case: an existing cassette that had two entries differing *only* in `cwd`
now collides on replay, and the first-recorded entry answers for both —
re-record it if that matters for your fixtures.

### Verify the upgrade

```sh
cargo update -p processkit
cargo build      # the renames and non_exhaustive tightenings are compiler-caught
cargo test       # catches the output_bytes byte-cap behavior change if you rely on it
```

## 1.0.0 (from 0.11.x)

A few breaking changes, all **caught by the compiler** — if it builds after the
bump, you're done.

### `OutputLine.text` is now an accessor

`OutputLine` (the per-line payload of `RunningProcess::output_events`) no longer
exposes `text` as a public field — read it via `line.text() -> &str` (or
`line.into_text() -> String` to take ownership). This frees the line
representation to evolve. Fix: `line.text` → `line.text()`.

### `Error::ResourceLimit` is now a struct variant

`Error::ResourceLimit(String)` became `Error::ResourceLimit { message: String }`
(parity with the other rich variants, room for structured detail later). Fix a
match `Error::ResourceLimit(m)` → `Error::ResourceLimit { message: m }`.
(Only relevant with the `limits` feature.)

### The text-capture verb is renamed `output` → `output_string`

The verb that runs to completion and returns the full `ProcessResult<String>`
is now spelled **`output_string`** on every layer, matching `output_bytes` (and
the spelling `Command`/`Pipeline`/`RunningProcess` already used). Two reasons:
the same operation no longer has two names depending on the type, and a bare
`output` clashed with `std::process::Command::output`, which returns **bytes** —
the explicit name removes that footgun.

**Affected if you call** `ProcessRunner::output`, `CliClient::output`, the free
fn `processkit::output`, or implement a custom `ProcessRunner` / use `MockRunner`.
The symptom is a build error like *"no method named `output`"* /
*"cannot find function `output` in crate `processkit`"*.

**Fix** — rename the calls (mechanical):

| Before | After |
|---|---|
| `runner.output(&cmd)` / `client.output(args)` | `runner.output_string(&cmd)` / `client.output_string(args)` |
| `processkit::output(prog, args)` | `processkit::output_string(prog, args)` |
| `impl ProcessRunner { async fn output(..) }` | `async fn output_string(..)` (the required method) |
| `mock.expect_output()` | `mock.expect_output_string()` |

`output_bytes` is unchanged, and `Command`/`Pipeline`/`RunningProcess` callers
need no change (those already used `output_string`).

## 0.11.0 (from 0.10.x)

Two breaking changes, both small and **caught by the compiler** — if it builds
after the bump, you're done. Plus one internal fix that needs no action.

### 1. `stats` is now opt-in — a `Cargo.toml` change

The default feature set is now just `process-control`; `stats` is no longer on by
default. (It gates a specialized metrics surface the core never needs; on
Windows it links an OS library — the `ProcessStatus` FFI used solely for the
peak-memory readout — but unlike `mock`/`tracing`/`record` it pulls in no extra
crate.)

**Affected if you use any metrics API:** `ProcessGroup::stats` /
`ProcessGroupStats`, `RunningProcess::cpu_time` / `peak_memory_bytes`, or
`RunProfile` / `RunningProcess::profile`. The symptom is a build error like
*"no method named `stats` / `cpu_time` / `peak_memory_bytes` / `profile`"* or
*"cannot find type `ProcessGroupStats` / `RunProfile`"*.

**Fix** — add the feature:

```toml
[dependencies]
processkit = { version = "0.11", features = ["stats"] }
```

If you already enable `limits`, do **nothing** — `limits` still implies `stats`.

**If you don't use metrics:** nothing to do. Your default build is now slightly
leaner (no Windows `ProcessStatus` dependency).

### 2. `OutputEvent` carries `OutputLine` — a code change

Affects only callers of `RunningProcess::output_events` (the ordered
lifecycle+output event stream). The per-line payload changed from a bare `String`
to a `#[non_exhaustive]` `OutputLine` struct with a public `text` field.

Before:

<!-- `text`, not `rust`: `OutputEvent::Stdout`/`Stderr` carrying a bare
     `String` is the pre-0.11 shape this section is *about* removing — it no
     longer matches the crate's current `OutputEvent`/`OutputLine` types
     (`events` is also a free variable); see the note earlier on why `text`
     rather than `ignore` (this crate's CI runs doctests with
     `--include-ignored`, which still compiles `ignore` blocks). -->
```text
use processkit::OutputEvent;

while let Some(ev) = events.next().await {
    match ev {
        OutputEvent::Stdout(s) => println!("out: {s}"),
        OutputEvent::Stderr(s) => eprintln!("err: {s}"),
        _ => {}
    }
}
```

After — read `line.text` (in 1.0 this becomes `line.text()`; see the
[1.0.0 section](#100-from-011x) above):

<!-- `text`, not `rust`: `line.text` as a public field is itself the
     0.11-era shape (1.0 turned it into an accessor, `line.text()` — see
     above); `ev` is a free variable. -->
```text
match ev {
    OutputEvent::Stdout(line) => println!("out: {}", line.text),
    OutputEvent::Stderr(line) => eprintln!("err: {}", line.text),
    _ => {}
}
```

Or, when you don't care which stream produced the line, use the new accessor:

<!-- `text`, not `rust`: `processkit::OutputEvent` was the 0.11-era name for this
     enum; 3.0 renamed it to `ProcessEvent` (see that section above), so this
     historical example no longer compiles against the current type. The
     `ev.text()` accessor it shows still exists on today's `ProcessEvent`. -->
```text
fn handle(ev: processkit::OutputEvent) {
if let Some(text) = ev.text() {
    println!("{text}");
}
}
```

`OutputLine` is `#[non_exhaustive]`: you receive it from the crate and read its
fields — you don't construct it, and a `match` on it should use `..`. The change
exists to reserve room for per-line metadata (e.g. a timestamp or a monotonic line
index) in a later release without another break.

### 3. Cancel-precedence fix ("Issue 7") — no action

A run that reaps on its own is no longer at risk of being misreported as
`Err(Cancelled)` by a cancellation token that fires in the narrow window between
the reap and the disposition check. This is an internal correctness fix with no
public-API change. If you carried a workaround that tolerated a spurious
`Cancelled` on a self-completing run, you can remove it.

### Verify the upgrade

```sh
cargo update -p processkit
cargo build      # both breaking changes are compiler-caught
cargo test
```

## Upgrading from older than 0.10

The jumps below 0.10 predate this guide. Read the dated sections of the
[CHANGELOG](../CHANGELOG.md) for each minor you cross — every breaking entry there
is marked **Breaking** and carries its own migration note. Notable recent
non-breaking additions you gain along the way: `Command::checked` / `run_unit`
(0.10.2) and the `record`-cassette symlink/`Display`-injection hardening (0.10.2).
