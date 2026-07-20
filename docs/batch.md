# Running many at once

[‹ docs index](README.md)

The batch helpers cover two distinct shapes of concurrent process work:

- `output_all` / `output_all_bytes` start a collection of `Command`s while
  retaining a fixed number of live runs.
- `wait_any` / `wait_all` observe a fixed collection of `RunningProcess`
  handles that you started earlier.

They provide bounded fan-out and joins, not a scheduler or worker-pool API.
Timeouts, retries, streaming, cancellation, and containment remain the normal
`Command` and `ProcessGroup` primitives.

- [Bounded fan-out](#bounded-fan-out)
- [Containment scope](#containment-scope)
- [Racing and joining handles](#racing-and-joining-handles)
- [Timeouts and output](#timeouts-and-output)
- [Common batch shapes](#common-batch-shapes)

## Bounded fan-out

`output_all(commands, concurrency, runner)` consumes any iterator of commands
and returns one result per input command, in input order. At most `concurrency`
runs are live at once; `0` is treated as `1`, and an empty input returns an
empty `Vec`.

It is **collect-all**: a launch or I/O failure fills that command's `Err` slot,
and the remaining commands still run. A non-zero exit is not an `Err`; it is an
`Ok(ProcessResult)` whose `code()` or `is_success()` you inspect. Fold completed
results after the batch rather than expecting the first failed command to stop
it:

```rust,no_run
use processkit::{Command, JobRunner, output_all};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let conversions = (0..200)
        .map(|n| Command::new("convert").args([format!("input-{n}.png"), format!("output-{n}.webp")]));
    let results = output_all(conversions, 8, &JobRunner).await;
    let failed = results
        .iter()
        .filter(|result| !matches!(result, Ok(output) if output.is_success()))
        .count();
    println!("{failed} conversions failed");
    Ok(())
}
```

`output_all_bytes` has identical scheduling and error semantics, but each
`ProcessResult` contains `Vec<u8>` stdout. Use it when stdout is an artifact,
such as an archive, image, or `git cat-file` object:

```rust,no_run
use processkit::{Command, JobRunner, output_all_bytes};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let commands = [
        Command::new("git").args(["cat-file", "blob", "HEAD:logo.png"]),
        Command::new("git").args(["cat-file", "blob", "HEAD:icon.png"]),
    ];
    for blob in output_all_bytes(commands, 2, &JobRunner).await {
        println!("captured {} bytes", blob?.stdout().len());
    }
    Ok(())
}
```

Like one-shot capture verbs, `output_all*` waits for and captures every
command's output. The vector appears only after the entire batch finishes;
dropping its future returns no partial vector. See [Timeouts and
cancellation](timeouts-and-cancellation.md) when cancellation is part of the
design.

## Containment scope

The `runner` argument controls containment, not only testability.

| Pass as `runner` | Use it when | What dropping a live run affects |
|---|---|---|
| `&JobRunner` | Each command should be independent. | Its own fresh private `ProcessGroup`; batch siblings are unaffected. |
| `&group` where `group: ProcessGroup` | The batch is one unit of fate. | The shared group controls every member, so dropping or shutting it down tears down the whole batch. |

Use `&JobRunner` for independent conversions. Use a shared group when every
process must disappear together:

```rust,no_run
use processkit::{Command, ProcessGroup, output_all};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let checks = [
        Command::new("check-shard").arg("a"),
        Command::new("check-shard").arg("b"),
    ];
    let results = output_all(checks, 2, &group).await;
    assert_eq!(results.len(), 2);
    group.shutdown().await?;
    Ok(())
}
```

Do not choose a shared group merely because the commands began together. It
couples their fate: cancelling `output_all` leaves shared-group children under
the group's lifetime, while each `JobRunner` run owns a private group. See
[Process groups](process-groups.md) for the teardown model.

## Racing and joining handles

Use `wait_any` and `wait_all` when you need live handles — for readiness,
incremental stdin, or a decision before workers finish — rather than only final
captured output. Both functions borrow their handles, so those handles remain
usable afterwards.

`wait_any` returns the index and outcome of the first child to exit. A mirror
race can then stop its loser by shutting down the shared group:

```rust,no_run
use processkit::{Command, ProcessGroup, wait_any};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let mut primary = group.start(&Command::new("fetch-from").arg("primary")).await?;
    let mut mirror = group.start(&Command::new("fetch-from").arg("mirror")).await?;
    let (winner, outcome) = wait_any(&mut [&mut primary, &mut mirror]).await?;
    println!("mirror #{winner} finished with {outcome:?}");
    group.shutdown().await?;
    Ok(())
}
```

`wait_all` joins a fixed worker set and returns every outcome in slice order:

```rust,no_run
use processkit::{Command, ProcessGroup, wait_all};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let mut workers = Vec::new();
    for shard in ["a", "b", "c", "d"] {
        workers.push(group.start(&Command::new("quiet-worker").arg(shard)).await?);
    }
    let mut borrowed: Vec<_> = workers.iter_mut().collect();
    let outcomes = wait_all(&mut borrowed).await?;
    println!("joined {} workers: {outcomes:?}", outcomes.len());
    Ok(())
}
```

`wait_any` rejects an empty slice because no child can win; `wait_all` accepts
one and returns an empty vector. A non-zero exit or signal is an `Outcome`, not
an error. `wait_all` can return the first cancellation, stdin, or reaping error;
its remaining handles are still waitable.

## Timeouts and output

`wait_any` and `wait_all` intentionally add neither of these features:

1. **Per-process timeout.** Put `Command::timeout` on each command before
   `start`, or wrap the complete wait in `tokio::time::timeout` when one deadline
   belongs to the collection.
2. **Output pumping.** A chatty child can fill stdout or stderr and block before
   exit. Drain it with `stdout_lines` / `stderr_lines`, a line handler, or a tee;
   use capture-oriented `output_all*` when incremental output is unnecessary.

An individual deadline belongs to its command:

```rust,no_run
use processkit::Command;
use std::time::Duration;

let worker = Command::new("worker").timeout(Duration::from_secs(30));
```

One collection deadline is explicit instead:

```rust,no_run
# use processkit::{Command, ProcessGroup, wait_all};
# use std::time::Duration;
# async fn example() -> processkit::Result<()> {
# let group = ProcessGroup::new()?;
# let mut worker = group.start(&Command::new("quiet-worker")).await?;
let outcome = tokio::time::timeout(
    Duration::from_secs(60),
    wait_all(&mut [&mut worker]),
).await;
# let _ = outcome;
# Ok(())
# }
```

The outer timeout only stops waiting. Choose containment deliberately, then
explicitly shut down or retain children according to your policy. For streaming
patterns, see [Streaming & interactive I/O](streaming.md#streaming-stdout).

## Common batch shapes

- **Bounded file conversion:** generate one `Command` per file and pass the
  iterator to `output_all` with a cap chosen for CPU, memory, file descriptors,
  and the external tool's limits. Fold all results to report the full failure set.
- **Mirror race:** start one `RunningProcess` per endpoint in a shared
  `ProcessGroup`, call `wait_any`, preserve the winner as needed, then shut down
  the group to stop losers.
- **Fixed worker join:** start known quiet or independently-drained workers,
  retain their handles in a `Vec`, and pass mutable references to `wait_all` for
  ordered outcomes.

For single-command recipes, return to the [Cookbook](cookbook.md).