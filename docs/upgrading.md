# Upgrading processkit

Per-version notes for **consumers** moving their dependency forward: what breaks,
who it affects, and the exact change to make. The [CHANGELOG](../CHANGELOG.md) is
the full record; this page is the "I depend on it, what do I do" view.

> **Pre-1.0 versioning.** Under Cargo's semver rules a `0.x` crate treats the
> *minor* as the breaking position, so a `0.10 → 0.11` bump can carry breaking
> changes. Pin to a minor range — `processkit = "0.11"` allows `0.11.*` but not
> `0.12` — and skim the relevant section here before each minor bump.

## Unreleased (from 0.11.x)

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

After — read `line.text` (in 0.12 this becomes `line.text()`; see the Unreleased
section):

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
