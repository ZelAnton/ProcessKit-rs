# Pipelines

[‹ docs index](README.md)

`a | b | c` **without a shell**. Each stage's stdout feeds the next stage's
stdin through an in-process relay (a `tokio::io::copy` task per boundary) — there
is no shell string anywhere, so no quoting rules, no word splitting, no injection
surface. All stages spawn into one shared kill-on-drop
[process group](process-groups.md), so the chain lives and dies as a unit. (The
relay is an implementation detail, not a kernel splice: a producer whose consumer
exits early stops on a *broken pipe* when the relay's next write fails, rather
than instantly via `SIGPIPE`.)

- [Building and running](#building-and-running)
- [Semantics: pipefail and the ends](#semantics-pipefail-and-the-ends)
- [Unchecked stages](#unchecked-stages)
- [Timeouts](#timeouts)
- [Re-running a pipeline](#re-running-a-pipeline)

## Building and running

`Command::pipe(next)` starts a `Pipeline`; chain more stages with
`Pipeline::pipe`; drive it with `output_string()` or `run()`:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // git log --format=%an | sort | uniq -c
    let authors = Command::new("git").args(["log", "--format=%an"])
        .pipe(Command::new("sort"))
        .pipe(Command::new("uniq").arg("-c"))
        .run()                         // require every stage to succeed
        .await?;
    println!("{authors}");
    Ok(())
}
```

The verbs mirror `Command`'s, each operating on the pipefail outcome:

| Verb | Returns | A failing stage is… |
|---|---|---|
| `output_string()` | `ProcessResult<String>` | …reported in the result (code/stderr/program of the first unclean stage) |
| `output_bytes()` | `ProcessResult<Vec<u8>>` | …same, with the last stage's stdout captured raw (binary pipes) |
| `run()` | trimmed final stdout | …raised as that stage's `Error::Exit`; fails loud on a truncated capture |
| `checked()` | full `ProcessResult<String>` | …raised as `Error::Exit` (untrimmed stdout) |
| `run_unit()` | `()` | …raised as `Error::Exit` (output discarded) |
| `exit_code()` | `i32` | …its attributed code (no code → `Error::Timeout`/`Signalled`) |
| `probe()` | `bool` | `0` → `true`, `1` → `false`, else `Err` |
| `parse(\|s\| …)` / `try_parse(\|s\| …)` | `T` | …raised as `Error::Exit`; fails loud on a truncated capture |

`Err` from `output_string` itself means a stage couldn't be *started or
driven* at all (spawn failure, broken plumbing) — never a mere non-zero exit.

The streaming `first_line` probe is deliberately not a pipeline verb: a chain
consumes its last stage in full to fold the pipefail outcome. To *capture* the
first matching line of a finished chain, add a `| head -n1` (Unix) / `grep -m1`
/ `findstr` stage and capture. This does **not** cover a streaming readiness
probe over a chain that must keep running (e.g. wait for a banner line, then
leave the chain alive) — `| head` would tear it down; use a single `Command`
with `first_line` for that.

The `|` operator is sugar for the same thing — `a | b | c` ≡
`a.pipe(b).pipe(c)`. Parenthesize the chain before a terminal verb, since
method calls bind tighter than `|`:

```rust,no_run
use processkit::Command;

let authors = (Command::new("git").args(["log", "--format=%an"])
    | Command::new("sort")
    | Command::new("uniq").arg("-c"))
    .run()
    .await?;
```

## Semantics: pipefail and the ends

The outcome is **pipefail**, like `set -o pipefail` in a shell:

- `stdout` is always the **last** stage's output — that's what the chain
  produced.
- `code`, `stderr`, and the reported program come from the **culprit** stage:
  the leftmost stage that didn't exit cleanly (non-zero, signal-killed, or timed
  out), but **preferring a real failure over a downstream `SIGPIPE` victim** — a
  stage killed only because a later stage closed the pipe early. If every failure
  is such a broken-pipe victim, the leftmost one wins; when every stage succeeded,
  the last stage speaks.

```rust,no_run
use processkit::Command;

let result = Command::new("cat").arg("data.txt")
    .pipe(Command::new("grep").arg("ERROR"))      // suppose grep exits 2 (bad pattern)
    .pipe(Command::new("wc").arg("-l"))
    .output_string()
    .await?;

// Diagnostics point at grep — the first unclean stage — while stdout is
// whatever wc managed to print:
assert_eq!(result.code(), Some(2));
println!("blamed: {}", result.ensure_success().unwrap_err()); // names `grep`
```

The ends of the chain behave like a single `Command`:

- The **first** stage's configured [`stdin`](commands.md#standard-input)
  source is honored — feed the whole pipeline from a string, file, or stream.
- **Inner** stages read from the pipe, full stop: any `stdin` source or
  `keep_stdin_open` configured on them is overridden.
- Inner stages' **stderr** is captured per-stage for pipefail diagnostics;
  only the last stage's stdout reaches you.

```rust,no_run
use processkit::{Command, Stdin};

let unique_count = Command::new("sort")
    .stdin(Stdin::from_iter_lines(["b", "a", "b", "c"]))
    .pipe(Command::new("uniq"))
    .pipe(Command::new("wc").arg("-l"))
    .run()
    .await?;
assert_eq!(unique_count.trim(), "3");
```

## Unchecked stages

Strict pipefail has one classic false positive: a consumer that legitimately
stops reading early. In `producer | head -1` the consumer exits `0` after one
line and closes the pipe; the producer then stops on a **broken pipe** — its
next write fails once the relay's downstream is gone (a broken-pipe write error,
or `SIGPIPE` where the OS delivers it) — a perfectly normal death that strict
pipefail would blame the chain for. Mark that stage [`unchecked_in_pipe()`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.unchecked_in_pipe):

```rust,no_run
use processkit::Command;

// seq 1 1000000 | head -1 — the producer's broken-pipe death is expected.
let first = (Command::new("seq").args(["1", "1000000"]).unchecked_in_pipe()
    | Command::new("head").args(["-n", "1"]))
    .run()
    .await?;
assert_eq!(first.trim(), "1");
```

The rules (a design borrowed from `duct`'s `unchecked()` — the idea, not the
code):

- An unchecked stage's unclean exit — a non-zero code, a broken-pipe write
  failure (or `SIGPIPE` where the OS delivers it) from a consumer that closed
  early, or its own per-stage timeout kill — is **skipped** when the chain
  decides what to report.
- A **checked** failure always trumps an unchecked one, regardless of
  position: `unchecked` never shields another stage's real failure.
- A chain whose only failures are unchecked reports **success** with the last
  stage's stdout and its **real** exit code preserved (not a fabricated `0` — the
  accepted-code set is widened to include it). The carve-out is for an exit
  *status* only: a last stage killed by a **signal** or its own **timeout** is not
  a status to forgive and still surfaces as the failure.
- `unchecked` forgives exit *status* only — never a whole-chain
  [`Pipeline::timeout`](#timeouts), and it has no effect on a `Command` run
  outside a pipeline (a single run's status is already plain data in its
  `ProcessResult`).

## Timeouts

Two scopes, deliberately distinct:

```rust,no_run
use processkit::Command;
use std::time::Duration;

let out = Command::new("producer")
    .timeout(Duration::from_secs(10))      // per-STAGE: kills just `producer`
    .pipe(Command::new("consumer"))
    .timeout(Duration::from_secs(30))      // whole-CHAIN: Pipeline::timeout
    .output_string()
    .await?;
```

- **`Pipeline::timeout`** bounds the whole chain: at the deadline the shared
  group is torn down and the result reports `timed_out` (no partial stdout —
  unlike a single command's captured timeout).
- A **per-stage `Command::timeout`** kills just that stage. Every stage is
  evaluated by the same pipefail rule (D14): a stage that hit its own deadline —
  inner *or* last — surfaces on `run()` as that stage's `Error::Timeout`,
  reporting **that stage's own deadline** (not the chain's, and never `0ns`).

Cancellation has two forms. **`Pipeline::cancel_on(token)`** is the chain-level
control: the token **gap-fills** into every stage that doesn't already carry its
own [`Command::cancel_on`] (an explicit per-stage token is left intact), so firing
it tears the whole chain down and the run resolves to `Error::Cancelled`. (A
`cancel_on` token on an individual stage `Command` also cancels that stage and
errors the pipeline, but
the pipeline-level builder is the clearer authority.) See
[Timeouts & cancellation](timeouts-and-cancellation.md).

## Re-running a pipeline

A `Pipeline` is `Clone` and re-runnable — stages are re-cloned per run. The
one caveat is inherited from `Command`: a **one-shot** stdin source on the
first stage (`Stdin::from_reader` / `from_lines`) is consumed by the first run;
re-running then **fails loud** (an `Error::Io` at launch, D10) rather than
silently feeding empty stdin. Use the reusable sources
(`from_string` / `from_bytes` / `from_iter_lines` / `from_file`) when a chain
runs more than once.

---

Next: [Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Running commands](commands.md) ·
[Process groups](process-groups.md)
