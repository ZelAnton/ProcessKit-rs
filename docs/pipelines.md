# Pipelines

[‹ docs index](README.md)

`a | b | c` without a shell. Each stage's stdout feeds the next stage's stdin
through a **native pipe** — there is no shell string anywhere, so no quoting
rules, no word splitting, no injection surface. All stages spawn into one
shared kill-on-drop [process group](process-groups.md), so the chain lives and
dies as a unit.

- [Building and running](#building-and-running)
- [Semantics: pipefail and the ends](#semantics-pipefail-and-the-ends)
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

The two verbs mirror `Command`'s:

| Verb | Returns | A failing stage is… |
|---|---|---|
| `output_string()` | `ProcessResult<String>` | …reported in the result (code/stderr/program of the first unclean stage) |
| `run()` | trimmed final stdout | …raised as that stage's `Error::Exit` |

`Err` from `output_string` itself means a stage couldn't be *started or
driven* at all (spawn failure, broken plumbing) — never a mere non-zero exit.

## Semantics: pipefail and the ends

The outcome is **pipefail**, like `set -o pipefail` in a shell:

- `stdout` is always the **last** stage's output — that's what the chain
  produced.
- `code`, `stderr`, and the reported program come from the **first** stage
  that didn't exit cleanly (non-zero, signal-killed, or timed out) — or from
  the last stage when every stage succeeded.

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
- A **per-stage `Command::timeout`** kills just that stage. The stage shows up
  in the pipefail fold as its unclean exit; on `run()` an *inner* stage's
  deadline surfaces as that stage's signal-kill IO error, while the *last*
  stage's own deadline (or the chain deadline) is a proper `Error::Timeout`.

With the `cancellation` feature, a `cancel_on` token on **any** stage cancels
that stage; the cancellation errors the whole pipeline and the private group
tears the other stages down — see
[Timeouts & cancellation](timeouts-and-cancellation.md).

## Re-running a pipeline

A `Pipeline` is `Clone` and re-runnable — stages are re-cloned per run. The
one caveat is inherited from `Command`: a **one-shot** stdin source on the
first stage (`Stdin::from_reader` / `from_lines`) is consumed by the first run
and feeds empty stdin afterwards. Use the reusable sources
(`from_string` / `from_bytes` / `from_iter_lines` / `from_file`) when a chain
runs more than once.

---

Next: [Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Running commands](commands.md) ·
[Process groups](process-groups.md)
