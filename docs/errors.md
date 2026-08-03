# Errors

[‹ docs index](README.md)

One structured type covers every failure mode — spawn, exit, timeout,
cancellation, IO — so a caller pattern-matches on a typed variant instead of
parsing strings. Since **3.0**, `Error` is a **pointer-sized wrapper** — a
`Box<ErrorReason>` — that keeps `Result<T, Error>` small on the pervasive
run path; the variants live on the re-exported [`ErrorReason`] enum, reached
through [`err.reason()`](processkit::Error::reason) (or moved out with
`err.into_reason()`). The read accessors (`code()`, `is_timeout()`,
`diagnostic()`, …) and `Display`/`Debug` work on `Error` directly, unchanged.
Several variants look alike at a glance but carry different
contracts; this guide is the one page that lays them all out side by side:
which variant fires from where, how to classify it, and what to do about it.

> **Upgrading from 2.x:** a direct `match err { Error::Exit { .. } => … }`
> becomes `match err.reason() { ErrorReason::Exit { .. } => … }`. See
> [Upgrading](upgrading.md).

- [Variant reference](#variant-reference)
- [Variants that look alike but aren't](#variants-that-look-alike-but-arent)
- [Classifiers](#classifiers)
- [Total classification: `kind()`](#total-classification-kind)
- [Stable machine identifiers](#stable-machine-identifiers)
- [Matching under `#[non_exhaustive]`](#matching-under-non_exhaustive)
- [Errors and retries](#errors-and-retries)
- [Errors and supervision](#errors-and-supervision)

## Variant reference

| Variant | Where it comes from | Recommended reaction |
|---|---|---|
| `ErrorReason::Spawn { program, source }` | The program was located but the OS refused to start it — permission denied, a bad working directory, a Windows `.cmd`/`.bat` needing `cmd.exe`, `ETXTBSY`, … | Inspect `source`; `is_permission_denied()` for an ACL/executable-bit problem, `is_transient()` for a bare-retry-clears-it condition. Not `is_not_found()` — the program *was* found. |
| `ErrorReason::NotFound { program, searched }` | The program could not be located at all — not installed, not on `PATH`, or a path that doesn't resolve | `is_not_found()` is `true`; surface a "is it installed?" hint. `searched` (`Some(dirs)` for a bare-name `PATH` lookup, `None` otherwise) is for a diagnostic only — never log it, it echoes the `PATH` value. |
| `ErrorReason::CassetteMiss { program }` (`record` feature) | A cassette replay found no recording matching the invocation — a stale or incomplete cassette, not a missing program | Fix or re-record the cassette. **Not** `is_not_found()` — do not let an "optional dependency" wrapper swallow this as "tool not installed". |
| `ErrorReason::Exit { program, code, stdout, stderr, stdout_bytes }` | The process ran to completion but exited non-zero | Branch on `code()`; `diagnostic()` for the best one-line human message (stderr, else stdout — `git`/`jj` put decisive text on stdout). |
| `ErrorReason::Timeout { program, timeout, stdout, stderr, stdout_bytes }` | `Command::timeout` elapsed and the tree was killed, on a **checking** verb | `is_timeout()`. Whatever was captured before the kill is attached — `diagnostic()` often explains the hang. Consider composing into a retry classifier: `e.is_timeout() \|\| e.is_transient()`. |
| `ErrorReason::NotReady { program, timeout }` | A [readiness probe](streaming.md#readiness-probes) (`wait_for_line` / `wait_for_port` / `wait_for`) did not pass within its own deadline | Not a run failure — the child is still running (a probe deadline never kills it). Decide whether to keep waiting, `shutdown()` the handle, or surface the failure. |
| `ErrorReason::Parse { program, message }` | The run succeeded but `try_parse`, a typed JSON/NDJSON verb, or a caller's own parser feeding `Error::parse` could not make sense of the output | Generic `message` values are caller-built and retained in full; JSON helpers store only bounded, control-escaped error detail and raw fragments. `Display`/`Debug` apply an additional preview cap. |
| `ErrorReason::OutputTooLarge { program, max_lines, max_bytes, total_lines, total_bytes }` | A `fail_loud` capture ceiling (`OutputBufferPolicy::max_lines`/`max_bytes`) was exceeded; the run itself may have succeeded | Raise the ceiling, switch to a lossy/streaming policy, or treat as a genuine failure — the pipe was fully drained either way, so the child never blocked. |
| `ErrorReason::ResourceLimit { kind, reason, detail }` (`limits` feature) | A requested cap on `ProcessGroupOptions` couldn't be enforced — no whole-tree container on this platform, or the OS rejected it | Read `limit_kind()` / `limit_reason()` rather than parsing `detail`; an unenforced limit is no protection, so treat this as a hard stop, not a warning. Admission-time only: whether a cap that *was* applied later fired is a separate, post-run question — see [below](#variants-that-look-alike-but-arent). |
| `ErrorReason::Unsupported { operation }` | An operation isn't supported by the active containment mechanism on this platform (e.g. any `Signal` but `Kill` on Windows Job Objects) | Branch on platform ahead of time (see [Platform support](platform-support.md)), or catch and degrade. |
| `ErrorReason::Cancelled { program }` | The run's `CancellationToken` fired and its tree was killed | `is_cancelled()`. This is an *abandonment*, not a failure to diagnose — the caller already knows why. Never retried (see [Errors and retries](#errors-and-retries)); terminal under a `Supervisor` too. |
| `ErrorReason::Signalled { program, signal, stdout, stderr, stdout_bytes }` | The process was killed by a signal (**Unix only**; a `ScriptedRunner`/cassette replay can also report `Signalled(None)`) | `is_signalled()`. No exit code to check — always a failure. `diagnostic()` surfaces whatever was captured before the crash. |
| `ErrorReason::Stdin { program, source }` | Feeding the child's stdin failed for a reason other than a routine broken pipe, on an **otherwise-successful** run | A diagnostic of a silently-truncated input the child may have already acted on. The io-level classifiers (`is_transient`, `is_not_found`, `is_permission_denied`) deliberately return `false` here — the run already succeeded, so a blanket retry would just re-run a command that worked. |
| `ErrorReason::Io(source)` | A low-level IO error from the crate's own machinery — driving a child, controlling a process group, reading/writing a cassette file | Never an arbitrary foreign `io::Error` (there is deliberately no blanket `From<std::io::Error>`); every `Io` here was raised at a known site inside the crate. |

## Variants that look alike but aren't

- **`Timeout` is *captured*, `Cancelled` is *always an error*.** `output_string`/
  `output_bytes` return `Ok` with `timed_out() == true` on a deadline — the
  caller decides whether that counts as failure. A cancellation, by contrast,
  reports `Err(ErrorReason::Cancelled)` on **every** consuming path, streaming
  included, because it's a deliberate caller action, not run data. When a run
  both hits its deadline and gets cancelled, cancellation wins (checked
  first). See [Precedence and interactions](timeouts-and-cancellation.md#precedence-and-interactions).
- **`NotReady` is not `Timeout`.** `Command::timeout` is the run's own
  contract and kills the tree; a readiness probe's `within` deadline is a
  *separate* clock layered on top of an already-running child, and giving up
  on it never kills anything. `is_timeout()` is `false` for `NotReady`.
- **`NotFound` vs. `Spawn`.** `NotFound` is the single representation of
  "program not found" — bare name or path, any platform, one variant,
  `is_not_found() == true`. `Spawn` is every *other* OS-level launch failure
  once the program **was** located (permissions, a bad `cwd`, a `.cmd`/`.bat`
  needing `cmd.exe`) — `is_not_found()` is `false` there, so a "not
  installed?" hint never fires on the wrong condition (e.g. a bad working
  directory).
- **`CassetteMiss` is not `is_not_found`.** A stale/incomplete cassette and a
  genuinely missing program are different failures a wrapper needs to tell
  apart — treating a cassette miss as "optional tool not installed" would
  silently hide a test-fixture bug.
- **`Exit` vs. `Signalled`.** Both carry captured streams and a `Display`
  diagnostic tail, but `Exit` has a `code()` and may or may not be a failure
  (`is_success()`/`ok_codes` decide); `Signalled` has no code at all —
  `code()` is `None` — and is always terminal.
- **`ResourceLimit` means "the cap couldn't be applied", never "the cap
  fired".** It is an *admission* error: the requested value was nonsensical
  (`Invalid`), the platform has no mechanism with whole-tree resource
  accounting (`Unsupported`), or a capable mechanism rejected this request
  (`Unenforceable`).
  `ProcessGroup::with_options` raises it **before** anything runs and hands
  back no group. `ProcessGroup::update_limits` raises it against an
  **already-running** group, where it does *not* mean "nothing changed": the
  caps are written axis by axis, so a part-way failure can leave a mix of old
  and new in force — see
  [updating a live group](process-groups.md#updating-a-live-group).
  A cap that *was* applied and then actually stopped the tree produces
  **no error at all** — the child just exits non-zero (or dies by
  `SIGKILL`), exactly like a self-inflicted crash. For that question ask the
  group afterwards:
  [`ProcessGroup::limit_evidence()`](process-groups.md#did-the-cap-actually-fire-limit_evidence)
  returns a per-axis `LimitVerdict` — `Tripped` (authoritative kernel/OS
  evidence that this cap fired), `NotTripped`, or `Unknown` (no evidence
  available on this mechanism — deliberately not folded into a "no"). Exit codes
  and signals are never consulted for it, precisely because they cannot tell a
  cap-driven kill from an ordinary failure. So: `ResourceLimit` on the error
  path, `limit_evidence` on the result path — two different questions, and this
  error's semantics are unchanged by it. They can land on the same axis on a
  live group, though: an axis named by a *failed* `update_limits` is still read
  from the counters, because that failure is not a rollback. (Left as a bare
  code span, not a `docs.rs` link: `LimitVerdict` ships in the next release, so
  a `docs.rs` URL would 404 until then.)

## Classifiers

| Classifier | `true` for | Notes |
|---|---|---|
| `is_not_found()` | `NotFound` only | The "is the program installed?" check. `false` for `Spawn`, `CassetteMiss`, and everything else. |
| `is_timeout()` | `Timeout` only | The `Error` twin of `ProcessResult::timed_out()`. `false` for `NotReady`. |
| `is_cancelled()` | `Cancelled` only | A caller that initiated the stop can swallow this rather than log/retry it as a real failure. |
| `is_signalled()` | `Signalled` only | `true` even when the kernel reported no signal number (`signal()` is then `None`) — the reliable "died by a signal?" check. |
| `is_permission_denied()` | `Spawn` / `Io` carrying `PermissionDenied` | IO/spawn-level only. |
| `is_transient()` | `Spawn` / `Io` carrying an interrupted/would-block/busy/lock condition | IO/spawn-level only, **never** exit codes or timeouts by design; compose explicitly: `e.is_transient() \|\| e.is_timeout()`. |
| `code()` | `Exit` (`Some(code)`) | `None` for every other variant — a timeout or signal-kill carries no exit code. Same accessor name as `ProcessResult::code()` / `Outcome::code()`. |
| `signal()` | `Signalled` with a known number | `None` for every other variant, and for a `Signalled` where the kernel reported no number. |
| `program()` | Every variant that names one | `None` only for `Unsupported`, `Io`, and (`limits` feature) `ResourceLimit` — the ones with no single program to attribute. |
| `limit_kind()` / `limit_reason()` (`limits` feature) | `ResourceLimit` only | Read structured fields instead of parsing `detail`'s English text. |
| `diagnostic()` | `Exit` / `Timeout` / `Signalled` (`Some`) | Stderr if it carries text, else stdout (`git`/`jj` put decisive output there), else `None`. |
| `timeout_duration()` | `Timeout` (`Some(dur)`) | The run deadline that elapsed. `None` everywhere else — including `NotReady`, whose probe deadline is a separate clock (matching `is_timeout()`'s scoping). |
| `output_overflow()` | `OutputTooLarge` (`Some(OutputOverflow)`) | The overflow counters as one snapshot — `total_lines()` / `total_bytes()` / `max_lines()` / `max_bytes()` — instead of destructuring the `#[non_exhaustive]` variant. `None` for every other error. |
| `unsupported_operation()` | `Unsupported` (`Some(&str)`) | The operation description (`"signal(Hup)"`, `"suspend"`). `None` for every other variant. |
| `kind()` | Every error (total) | The one coarse **routing** bucket — see [Total classification](#total-classification-kind). Never `None`; every error has a kind. |

## Total classification: `kind()`

The classifiers above answer *one* question each. When you route **every**
failure onto your own shape — a CLI folding each disposition into a distinct
process exit code, a cross-language binding raising a matching exception class, a
router picking a retry policy — you want one **total** classification instead of
a chain of `is_*` checks ending in "everything else". `err.kind()` is that: a
compact [`ErrorKind`]
with one bucket per operational disposition, **derived** from each variant's
existing semantics (not invented), and covering every variant — present and
future — through an exhaustive `match` inside the crate.

| `ErrorKind` | Derived from `ErrorReason` | Machine name |
|---|---|---|
| `NotFound` | `NotFound` | `not_found` |
| `Spawn` | `Spawn` whose `source` is **not** a permission denial | `spawn` |
| `PermissionDenied` | the `PermissionDenied` subset of `Spawn` / `Io` | `permission_denied` |
| `ResourceLimit` (`limits` feature) | `ResourceLimit` | `resource_limit` |
| `Unsupported` | `Unsupported` | `unsupported` |
| `Timeout` | `Timeout` | `timeout` |
| `Cancelled` | `Cancelled` | `cancelled` |
| `Exit` | `Exit` | `exit` |
| `Signalled` | `Signalled` | `signalled` |
| `Other` | `CassetteMiss`, `Parse`, `NotReady`, `OutputTooLarge`, `Stdin`, and a non-`PermissionDenied` `Io` | `other` |

`kind()` is a **routing** answer, deliberately coarser than the variant — it is
**not** a replacement for matching [`reason()`](processkit::Error::reason) when
you need the details (the exit code, the captured streams, the timeout duration,
which limit failed). It stays consistent with the point classifiers:
`is_not_found()` ⇔ `kind() == NotFound`, `is_permission_denied()` ⇔
`PermissionDenied`, `is_timeout()` ⇔ `Timeout`, and so on.

`ErrorKind` mirrors [`std::io::ErrorKind`]: it is `#[non_exhaustive]` and carries
an `Other` bucket, so a downstream `match` needs a catch-all arm — which is
exactly what makes it forward-compatible.

```rust
use processkit::{Error, ErrorKind};

// Fold every failure onto your own exit codes — one arm per kind you care
// about, a catch-all for the rest (kinds are `#[non_exhaustive]`).
fn exit_code(err: &Error) -> i32 {
    match err.kind() {
        ErrorKind::NotFound => 127,
        ErrorKind::PermissionDenied => 126,
        ErrorKind::Timeout => 124,
        ErrorKind::Exit => 1,
        // A future kind (or one behind a feature this build doesn't enable,
        // e.g. ResourceLimit) routes here instead of breaking the build.
        _ => 70,
    }
}

let err = Error::parse("jq", "unexpected token");
assert_eq!(err.kind(), ErrorKind::Other);
assert_eq!(err.kind().name(), "other");
assert_eq!(exit_code(&err), 70);
```

[`std::io::ErrorKind`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html

## Stable machine identifiers

When you publish a machine-readable contract *over* this crate's types — a
CLI's JSONL schema, a cross-language binding, a structured log field — you need
one canonical string per enum variant, not a table you hand-maintain (and that
silently mislabels a new variant as "unknown"). The reporting and configuration
enums carry that table for you:

The language-neutral, canonical dictionary is
[`spec/identifiers.json`](../spec/identifiers.json). The table below explains
the Rust API that produces it; both shapes describe the same contract and must
change together.

| Method | On | Direction |
|---|---|---|
| `name() -> &'static str` | `Mechanism`, `Outcome`, `ErrorKind`, `ParentDeathCleanup`, `SoftStopScope`, `SoftSignal`, `StopReason`, `LimitKind`, `LimitReason`, `LimitVerdict`, `StdioMode`, `LineTerminator`, `OverflowMode`, `OutputStream`, `Priority`, `RestartPolicy`, `RlimitResource`, `ProcessEvent`, `SupervisionEvent` | A short, lowercase `snake_case` identifier for the variant. |
| `name() -> Option<&'static str>` | `Signal` | `Some(id)` for a curated signal; `None` for the raw-number `Signal::Other` (render its `i32` instead). |
| `from_name(&str) -> Option<Self>` | every enum above **except** `Outcome`, `ErrorKind`, `ProcessEvent`, `SupervisionEvent`, and `SoftSignal` — `LimitVerdict` included, so a recorded `tripped` / `not_tripped` / `unknown` parses back | Parse an identifier back into the value; `None` — not a default — for an unrecognized name. |

The identifiers are a **compatibility surface**, held stable like the rest of
the public API: a **new** variant gets a **new** identifier, and an existing
identifier is **never renamed** without a major release. They are *diagnostic*
names — a stable **vocabulary**, not a frozen record schema — and the opt-in
`report-serde` feature puts that very vocabulary on the wire rather than
minting a second one: it implements `serde::Serialize` for the crate's report
types, and every enum in them serializes as its `name()`
(`{"kind": "exited", "code": 0, "signal_number": null}`, never serde's derived
`{"Exited": 0}`), so a consumer that adopted the dictionary and a consumer that
serializes a report read the same strings. The single exception is the one the
table above already records: `Signal::Other` has no curated identifier, so a
`Signal` travels as its identifier string when curated and as a bare `i32` when
not — confined to the `signal` key, while the raw exit-signal number an
`Outcome` carries is the separate key `signal_number`. That feature is
`Serialize`-only
(these values are reported, never supplied back — see the report-only enums
listed below), it never puts captured output, argv or environment values on the
wire, and it promises the *spelling* of what it emits, not a frozen field set:
every report type is `#[non_exhaustive]`, keeps its fields private, or both —
grown, not frozen — so a minor release may add a key, never rename one, and a
consumer must ignore unknown keys. See the crate-root `report-serde` section
for the full rules.
`Mechanism` and `ParentDeathCleanup` use the spellings downstream tools
already publish (`job_object`/`cgroup_v2`/`process_group`,
`whole_tree`/`direct_child_only`/`none`), so adopting them needs no migration;
the FreeBSD reaper mechanism adds `process_reaper` in the same shape.
`SoftStopScope` (the group-axis soft-stop reach, `process-control`) reuses the
same `whole_tree` and `none` spellings for its shared cases, adding
`opt_in_members` for the Windows partial-reach case. `LimitVerdict` (`limits`)
is the one entry that is not an error classification at all: it is the
*post-run* answer to "did this cap fire?" (`tripped` / `not_tripped` /
`unknown`), while `LimitKind` / `LimitReason` describe the admission-time
`ResourceLimit` failure — two different questions, never two spellings of one
answer (see [Variants that look alike
but aren't](#variants-that-look-alike-but-arent)), and both carry the same
stability promise.

```rust
use processkit::{Mechanism, Priority};

// Forward — a stable identifier for machine-readable output:
assert_eq!(Mechanism::CgroupV2.name(), "cgroup_v2");
assert_eq!(Priority::BelowNormal.name(), "below_normal");

// Inverse (a config value, a CLI flag, a call from another language) — an
// honest `None` on an unrecognized name, never a silent default:
assert_eq!(Priority::from_name("below_normal"), Some(Priority::BelowNormal));
assert_eq!(Priority::from_name("turbo"), None);
```

The enums the table above lists as `from_name` exceptions report a `name()` but
take **no** inverse, because they are classifications, events or fates the crate
*reports* and never accepts back. `Outcome::name()`
reports the *disposition* only (`exited` / `signalled` / `timed_out`) — the name
alone can't carry the exit code or signal number (read those from
[`code()`](https://docs.rs/processkit/latest/processkit/struct.ProcessResult.html#method.code)
/ `signal()`), and an `Outcome` is always reported, never supplied. `ErrorKind`
is the same: a total failure classification the crate hands you via
[`Error::kind()`](processkit::Error::kind), never one you supply back in. Read
the fuller payload from the [`ErrorReason`] variant when a name isn't enough.
`ProcessEvent` and `SupervisionEvent` similarly identify lifecycle event kinds;
their payloads carry the process output, outcome, timing, or supervision detail.
`SoftSignal` (`process-control`) names which fate a graceful stop's best-effort
soft-signal tier met — `sent` / `unsupported` / `failed` — an observation made
*after* a teardown, never a request; the `Signal` it concerns travels separately
(`ShutdownReport::attempted_signal`).
`Signal::name()` returns `Option` because the `Other(i32)` escape hatch has no
curated name (it still has a `from_name`).

## Matching under `#[non_exhaustive]`

`Error` — and several of its struct-like variants — are `#[non_exhaustive]`:
a future release can add a variant or field without that being a breaking
change, but it also means every downstream `match` **must** carry a catch-all
arm.

```rust,no_run
use processkit::{Command, ErrorReason};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let err = Command::new("maybe-missing-tool").run().await.unwrap_err();

    match err.reason() {
        ErrorReason::NotFound { .. } => eprintln!("is it installed?"),
        ErrorReason::Timeout { .. } => eprintln!("hit its deadline"),
        ErrorReason::Cancelled { .. } => { /* caller-initiated, nothing to log */ }
        ErrorReason::Exit { code, .. } => eprintln!("exited with {code}"),
        // #[non_exhaustive]: a future variant (or a today's variant behind a
        // feature this build doesn't enable, e.g. ResourceLimit) falls here.
        other => eprintln!("run failed: {other}"),
    }
    Ok(())
}
```

Prefer the classifiers above (`is_not_found()`, `is_timeout()`, …) over
destructuring when all you need is a yes/no answer — they read the variant
without you having to keep the catch-all arm in sync as fields are added.

## Errors and retries

[`Command::retry(max_attempts, backoff, classifier)`](timeouts-and-cancellation.md#retries)
replays a failed run while your classifier accepts the typed error — the
classifier is exactly this guide's variant table, read by the caller:

```rust,no_run
use processkit::{Command, Error};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("curl")
        .args(["-fsS", "https://example.com/api"])
        .timeout(Duration::from_secs(10))
        .retry(3, Duration::from_millis(250), |e: &Error| {
            // Transient: our own deadline, or a spawn/IO condition a bare retry clears.
            e.is_timeout() || e.is_transient()
        })
        .run()
        .await?;
    Ok(())
}
```

`ErrorReason::Cancelled` is **never** retried, whatever the classifier says — the
token stays cancelled forever, so another attempt could only fail the same
way. See [Retries](timeouts-and-cancellation.md#retries) for the full ground
rules (stdin re-use, which verbs retry at all).

## Errors and supervision

A [`Supervisor`](supervision.md) restarts a crashing service rather than
replaying one operation, but it faces the same "is this permanent?"
question — its
[`give_up_when(classifier)`](supervision.md#giving-up-on-permanent-failures)
gate is the same classifiers applied to a `GiveUpAttempt`:

```rust,no_run
use processkit::{Command, GiveUpAttempt, Supervisor};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let outcome = Supervisor::new(Command::new("maybe-typo'd-binary"))
        .give_up_when(|attempt| match attempt {
            // The child never started at all — e.g. is_not_found() for ENOENT.
            GiveUpAttempt::Failed(err) => err.is_not_found(),
            // A completed run your own domain knows is a permanent crash.
            GiveUpAttempt::Crashed(res) => res.code() == Some(78),
            _ => false,
        })
        .run()
        .await?;
    println!("stopped: {:?}", outcome.stopped);
    Ok(())
}
```

`GiveUpAttempt::Failed(&Error)` is the spawn/IO path (no `ProcessResult` was
ever produced); `GiveUpAttempt::Crashed(&ProcessResult<String>)` is a
completed-but-failing run. A recognized-permanent failure reports
`StopReason::GaveUp` instead of restarting forever. `ErrorReason::Cancelled` is
terminal here too — supervision returns `Err(Cancelled)` instead of
restarting into a still-cancelled token, `give_up_when` or not.

`ErrorReason::ResourceLimit` and `ErrorReason::Unsupported` specifically are how a
sandboxing request fails loud instead of silently doing less than asked —
see [Running untrusted children](untrusted-children.md) for the hardening
checklist that relies on exactly that honesty.

---

Next: [Running commands](commands.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Supervision](supervision.md) ·
[Running untrusted children](untrusted-children.md)
