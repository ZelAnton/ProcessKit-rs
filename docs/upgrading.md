# Upgrading processkit

Per-version notes for **consumers** moving their dependency forward: what breaks,
who it affects, and the exact change to make. The [CHANGELOG](../CHANGELOG.md) is
the full record; this page is the "I depend on it, what do I do" view.

> **Versioning.** From 1.0.0 onward `processkit` follows
> [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the public API is
> stable, and any breaking change lands only in a new **major** version, so `1.x`
> upgrades are backward-compatible. The default Cargo requirement `processkit = "1"`
> already does the right thing — it allows `1.*` but not `2.0`. Skim the relevant
> section here before each major bump. (The `mock` feature's `mockall`-generated
> `expect_*` surface stays semver-exempt — it tracks the `mockall` version.)

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

```rust
match err {
    Error::Exit { program, code, stdout, stderr } => { /* ... */ }
    _ => {}
}
```

After — add `..` to the pattern (or, better, use the existing accessors instead
of destructuring at all):

```rust
match err {
    Error::Exit { program, code, stdout, stderr, .. } => { /* ... */ }
    _ => {}
}

// or, accessor-based and immune to the next field addition:
if let Some(code) = err.code() {
    // err.program() / err.stdout() / err.stderr() / err.combined() also work
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

```rust
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
default. (It is the one feature carrying an extra build dependency — the Windows
`ProcessStatus` FFI used solely for the peak-memory readout — and it gates a
specialized metrics surface the core never needs.)

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

```rust
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

```rust
match ev {
    OutputEvent::Stdout(line) => println!("out: {}", line.text),
    OutputEvent::Stderr(line) => eprintln!("err: {}", line.text),
    _ => {}
}
```

Or, when you don't care which stream produced the line, use the new accessor:

```rust
if let Some(text) = ev.text() {
    println!("{text}");
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
