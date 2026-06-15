# Streaming & interactive I/O

[‹ docs index](README.md)

The one-shot verbs in [Running commands](commands.md) buffer the whole output.
For long-running or conversational children, `Command::start()` returns a live
`RunningProcess` you drive yourself: stream stdout as it arrives, write stdin
incrementally, probe for readiness, race several children, or profile a run.

- [Lifecycle](#lifecycle)
- [Streaming stdout](#streaming-stdout)
- [Interactive stdin](#interactive-stdin)
- [Readiness probes](#readiness-probes)
- [Racing children with `wait_any`](#racing-children-with-wait_any)
- [Per-run telemetry](#per-run-telemetry)

## Lifecycle

```rust,no_run
use processkit::Command;

let mut run = Command::new("dev-server").start().await?;

run.pid();        // Option<u32> — None once the child is reaped
run.elapsed();    // time since spawn

// Consume the handle exactly one way:
//   output_string() / output_bytes()  → capture everything (same as the one-shot verbs)
//   wait()                            → just the Outcome; output is discarded
//   finish()                 → after streaming stdout (below)
//   profile(every)                    → capture + resource samples (stats feature)
let outcome = run.wait().await?;   // Outcome: Exited(code) / Signalled(sig) / TimedOut
```

`start()` puts the child in a **private group the handle owns**: dropping the
`RunningProcess` kills the whole tree, exactly like dropping a one-shot run's
future. The shared-group variant — `group.start(&cmd)` — gives the same handle
but the *group* controls the tree's fate (see
[Process groups](process-groups.md#putting-processes-in)).

There is also an explicit `run.start_kill()` for "stop it now, I'll `wait()`
for the code myself".

## Streaming stdout

`stdout_lines()` yields decoded lines as the child produces them — no waiting
for exit, no full-output buffering. `StreamExt` (re-exported from
`tokio-stream`) provides `.next()`:

```rust,no_run
use processkit::{Command, Outcome, StreamExt, Finished};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("cargo")
        .args(["build", "--release"])
        .start()
        .await?;

    let mut lines = run.stdout_lines()?;
    while let Some(line) = lines.next().await {
        println!("build: {line}");
    }

    // The stream ended (stdout closed). Collect the outcome and stderr —
    // stderr was drained in the background the whole time, so a noisy child
    // could never block on a full pipe.
    let Finished { outcome, stderr, .. } = run.finish().await?;
    if outcome != Outcome::Exited(0) {
        eprintln!("build failed ({outcome:?}):\n{stderr}");
    }
    Ok(())
}
```

Things to know:

- **Call `stdout_lines()` once.** It is fallible: a second `stdout_lines` /
  `output_events` call (stdout is consumed once), or a non-piped stdout
  (`StdioMode::Inherit`/`Null`), returns `Err` rather than a silently-empty
  stream.
- **The command's `timeout` bounds the stream** on an own-group handle: at the
  deadline the tree is killed, the pipes close, and the stream ends — a
  streamed run can't hang past its deadline. A `cancel_on` token ends it the
  same way; the following
  `finish` then reports `Error::Cancelled`. Details in
  [Timeouts & cancellation](timeouts-and-cancellation.md).
- Line counters tick live: `run.stdout_line_count()` / `stderr_line_count()`
  are cheap progress gauges even while you stream.
- The [buffer policy and line handlers](commands.md#output-handling) apply to
  streamed runs too — a handler sees each line on the pump, in addition to
  your loop.
- The whole streaming surface is **hermetically testable**: a
  `ScriptedRunner`'s `start()` returns a handle whose canned lines flow
  through the same pump machinery — `stdout_lines`, the readiness probes, and
  `finish` behave identically with no subprocess. See
  [Testing → scripted streaming](testing.md#scripted-streaming).

## Interactive stdin

Conversational tools — write a request, read the response, repeat. Keep stdin
open with `keep_stdin_open()`, take the writer with `take_stdin()`:

```rust,no_run
use processkit::{Command, Outcome, StreamExt, Finished};

// `ProcessStdin`'s writer methods return `std::io::Result`; `Box<dyn Error>`
// mixes them with the crate's `Result` (or `.map_err(processkit::Error::Io)?`).
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `bc` evaluates each stdin line and prints the result.
    let mut run = Command::new("bc").keep_stdin_open().start().await?;
    let mut stdin = run.take_stdin().expect("stdin was kept open");
    let mut answers = run.stdout_lines()?;

    stdin.write_line("2 + 2").await?;             // writes "2 + 2\n", flushed
    println!("= {}", answers.next().await.unwrap());

    stdin.write_line("6 * 7").await?;
    println!("= {}", answers.next().await.unwrap());

    stdin.finish().await?;                        // send EOF — bc exits
    let Finished { outcome, .. } = run.finish().await?;
    assert_eq!(outcome, Outcome::Exited(0));
    Ok(())
}
```

`ProcessStdin` offers `write(&[u8])`, `write_line(&str)` (newline + flush),
`flush()`, and `finish()` (EOF). Dropping the writer — or the whole
`RunningProcess` — closes stdin too; `finish()` just makes the EOF explicit
and awaitable.

**Avoid the full-duplex deadlock.** A child's stdout pipe has a finite OS
buffer; once it fills, the child blocks *writing* stdout until something reads
it. If you push a large interactive stdin while nothing drains the child's
stdout, the child stops reading stdin (blocked on stdout), your `write` parks
waiting for stdin buffer space, and neither side progresses. The `bc` example
above is safe because it interleaves one write with one read; when you both feed
a sizable stdin **and** the child produces output, drain `stdout_lines` from one
task while writing stdin from another. (The non-interactive `Stdin::from_*`
sources are safe — the crate writes them on a background task that runs
concurrently with the output pumps.)

For *one-directional* streamed input (a channel, a file tail) you don't need
interactivity — give the command `Stdin::from_lines(stream)` /
`Stdin::from_reader(reader)` and let the background writer feed it; see the
[stdin source table](commands.md#standard-input).

## Readiness probes

"Start a server, then use it" needs *ready*, not merely *started*. Three
probes replace the arbitrary sleep, each bounded by its own deadline:

```rust,no_run
use processkit::Command;
use std::time::Duration;

let mut run = Command::new("my-server").start().await?;

// 1. A line on stdout (returns the matching line):
let banner = run
    .wait_for_line(|l| l.contains("listening on"), Duration::from_secs(10))
    .await?;

// 2. A TCP port accepting connections:
run.wait_for_port("127.0.0.1:8080".parse().unwrap(), Duration::from_secs(10))
    .await?;

// 3. Any async predicate (an HTTP /health endpoint, a file appearing, …):
run.wait_for(|| async { health_check().await }, Duration::from_secs(10))
    .await?;

// ready — use the server…
```

Probe semantics, deliberately uniform:

- A probe that can't pass within its deadline fails with **`Error::NotReady`**
  — distinct from `Error::Timeout`, which is the run's own deadline.
- A probe also fails *fast* once readiness can no longer happen: the child
  exits, or (for `wait_for_line`) its stdout closes — no waiting out a 30s
  deadline on a dead server.
- A failed probe **never kills the child.** You decide: retry, log and
  continue, or tear down.
- `wait_for_line` consumes stdout up to (and including) the match — continue
  with `finish` or further streaming. `wait_for_port` / `wait_for`
  don't touch the pipes at all.

## Racing children with `wait_any`

The free function `wait_any` races several running processes and reports
whichever exits first — the natural primitive for "restart whatever died" or
"first answer wins":

```rust,no_run
use processkit::{Command, ProcessGroup, wait_any};

let group = ProcessGroup::new()?;
let mut a = group.start(&Command::new("replica-a")).await?;
let mut b = group.start(&Command::new("replica-b")).await?;

let (index, outcome) = wait_any(&mut [&mut a, &mut b]).await?;
println!("contender #{index} exited first with {outcome:?}");

// Only borrows: the loser is still usable.
let survivor = if index == 0 { &mut b } else { &mut a };
```

`wait_any` takes `&mut` borrows, applies no timeout of its own (wrap it in
`tokio::time::timeout` to bound the race), and does no output pumping — drain
chatty children first or give them bounded
[buffer policies](commands.md#output-handling).

## Per-run telemetry

With the opt-in **`stats`** feature, a running child reports its own
resource usage, and `profile()` turns a whole run into a summary:

```rust,no_run
use processkit::Command;
use std::time::Duration;

let run = Command::new("crunch").start().await?;
run.cpu_time();          // Option<Duration> — user+kernel so far
run.peak_memory_bytes(); // Option<u64>

// …or capture + sample on an interval until exit:
let profile = Command::new("crunch")
    .start().await?
    .profile(Duration::from_millis(100))
    .await?;

println!(
    "exit={:?} wall={:?} cpu={:?} peak_rss={:?} avg_cpu={:?} ({} samples)",
    profile.exit_code,
    profile.duration,
    profile.cpu_time,
    profile.peak_memory_bytes,
    profile.avg_cpu(),          // cpu / wall — e.g. Some(1.7) ≈ 1.7 cores busy
    profile.samples,
);
```

These read the *child process itself* (not a whole tree — that's
[`ProcessGroup::stats`](process-groups.md#stats-and-sampling)), and
availability follows the platform: full CPU/memory on Windows and Linux,
`None` where the kernel doesn't account per-process cheaply — see
[Platform support](platform-support.md).

---

Next: [Pipelines](pipelines.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Supervision](supervision.md)
