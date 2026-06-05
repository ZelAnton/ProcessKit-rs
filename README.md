# processkit

Child-process management for Rust, in two layers:

- **Process groups** ([`ProcessGroup`]) — spawn a child as the root of a process
  tree that is killed as a unit when the group is dropped, using Windows **Job
  Objects**, Linux **cgroup v2** (with a POSIX **process-group** fallback) and a
  POSIX **process group** on macOS/BSD, so no descendant ever outlives its owner.
- **Process runner** ([`Command`]) — async (tokio) run-and-capture of a child's
  `stdout`/`stderr` and exit status, built on the group layer, with a mockable
  [`ProcessRunner`] seam for tests.

Async throughout. Errors are structured (`Error`); a non-zero exit is reported in
the result, not raised, until you call `ProcessResult::ensure_success`.

> **Status:** feature-complete — process groups, the runner and capture helpers,
> streaming, interactive stdin, push line-handlers, output-buffer policies,
> encoding overrides, line counters, CPU/memory stats (with a time-series
> sampler and per-run profiles), whole-tree resource limits
> (memory / process-count / CPU), whole-tree signals plus suspend/resume, and
> tree inspection (`members`, `wait_any`). See [`CHANGELOG.md`](CHANGELOG.md).

## Install

```bash
cargo add processkit
```

This crate requires a [tokio](https://tokio.rs/) runtime.

## Usage

```rust,no_run
use processkit::{Command, ProcessGroup, Stdin};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Capture output; a non-zero exit does not error on its own.
    let result = Command::new("git").args(["rev-parse", "HEAD"]).output_string().await?;
    println!("HEAD is {}", result.stdout().trim());

    // Require success and get trimmed stdout directly.
    let version = Command::new("cargo").arg("--version").run().await?;
    println!("{version}");

    // Feed stdin.
    let sorted = Command::new("sort")
        .stdin(Stdin::from_string("banana\napple\n"))
        .output_string()
        .await?;
    println!("{}", sorted.stdout());

    // Share one kill-on-drop group across several children; dropping the group
    // reaps the whole tree.
    let group = ProcessGroup::new()?;
    let _server = group.start(&Command::new("some-server")).await?;
    // ... work ...
    group.shutdown().await?; // graceful SIGTERM → wait → SIGKILL (Unix); atomic on Windows

    Ok(())
}
```

## Capping a group's resources

Requires the **`limits`** feature (off by default) — add it to the dependency:

```toml
processkit = { version = "…", features = ["limits"] }
```

`ProcessGroupOptions` can then bound the whole tree's memory, process count, and CPU
at creation, so a runaway or untrusted child tree can't exhaust the host:

```rust,no_run
use processkit::{Command, ProcessGroup, ProcessGroupOptions};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .memory_max(512 * 1024 * 1024) // 512 MiB across the tree
            .max_processes(64)
            .cpu_quota(0.5),               // half of one core
    )?;
    let _job = group.start(&Command::new("untrusted-tool")).await?;
    // ... work ...
    Ok(())
}
```

`cpu_quota` is a fraction of a **single** core (`0.5` = half a core, `2.0` = two
cores); on Windows it is converted against the host's CPU count and is approximate.

Limits need a real container — a **Windows Job Object** or a **Linux cgroup v2**.
There is no whole-tree limit on macOS/the BSDs, the Linux process-group fallback, or
the no-containment target, and a Linux cgroup must permit controller delegation (run
as root, in a container, or under a systemd unit with `Delegate=yes`). When a
requested limit can't be enforced, `with_options` returns `Error::ResourceLimit`
instead of a silently-unbounded group — an unapplied cap is no protection.

## Signalling and pausing the whole tree

Beyond the kill/shutdown teardown verbs, a group can broadcast a signal to every
member or freeze and thaw the whole tree:

```rust,no_run
use processkit::{Command, ProcessGroup, Signal};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let _server = group.start(&Command::new("my-server")).await?;

    group.signal(Signal::Hup)?; // e.g. "reload configuration"
    group.suspend()?;           // freeze the whole tree…
    group.resume()?;            // …and let it run again
    Ok(())
}
```

Signals are POSIX-only: on Windows just `Signal::Kill` is deliverable (it maps to
the Job Object terminate) and anything else returns `Error::Unsupported`.
`Signal::Kill` always takes the same whole-tree hard-kill path as
`terminate_all()`. Suspend/resume work everywhere a container exists — one
`cgroup.freeze` write covering the subtree on Linux, `SIGSTOP`/`SIGCONT` on
macOS/BSD and the Linux process-group fallback, and per-thread suspension on
Windows (best-effort; nested suspends stack and need matching resumes).

## Inspecting the tree

`members()` snapshots the live member pids, and `wait_any` races several running
processes, reporting whichever exits first — the natural primitive for
supervising a few long-lived children:

```rust,no_run
use processkit::{Command, ProcessGroup, wait_any};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;
    let mut a = group.start(&Command::new("server-a")).await?;
    let mut b = group.start(&Command::new("server-b")).await?;

    println!("live pids: {:?}", group.members()?);

    // Borrows only: the loser stays usable after the race.
    let (idx, code) = wait_any(&mut [&mut a, &mut b]).await?;
    println!("contender #{idx} exited first with {code:?}");
    Ok(())
}
```

`members()` lists the whole tree on Windows (Job Object) and Linux (cgroup); the
POSIX process-group backends list the tracked group *leaders* only. `wait_any`
applies no per-process timeout (bound the race with `tokio::time::timeout`) and
does no output pumping — drain chatty children first.

## Sampling stats over time

A point-in-time `stats()` becomes a series with `sample_stats`, and a single run
can be profiled end-to-end (requires the default-on `stats` feature):

```rust,no_run
use processkit::{Command, ProcessGroup, StreamExt};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // A CPU/RSS/process-count series for a whole group:
    let group = ProcessGroup::new()?;
    let _worker = group.start(&Command::new("worker")).await?;
    let mut samples = group.sample_stats(Duration::from_millis(250));
    if let Some(s) = samples.next().await {
        println!("procs={} rss={:?}", s.active_process_count, s.peak_memory_bytes);
    }
    drop(samples);

    // …or a one-shot summary of a single run:
    let profile = Command::new("crunch")
        .start().await?
        .profile(Duration::from_millis(100)).await?;
    println!(
        "exit={:?} took={:?} peak_rss={:?} avg_cpu={:?}",
        profile.exit_code, profile.duration, profile.peak_memory_bytes, profile.avg_cpu(),
    );
    Ok(())
}
```

The series inherits `stats()`'s platform matrix (full CPU/memory on Windows and
Linux cgroup; counts only on the POSIX process-group backends); `profile`
samples the started child process itself and applies the run's normal
timeout/output handling.

## Supervising a long-lived child

Where `Command::retry` replays one run until it succeeds, a `Supervisor` keeps a
child **alive**: it restarts the command per policy whenever it exits, with
bounded restarts and exponential backoff (jittered by default so a restarted
fleet doesn't stampede):

```rust,no_run
use processkit::{Command, RestartPolicy, Supervisor};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let outcome = Supervisor::new(Command::new("my-server").args(["--port", "8080"]))
        .restart(RestartPolicy::OnCrash)          // Always | OnCrash | Never
        .max_restarts(5)
        .backoff(Duration::from_millis(200), 2.0) // base, multiplier (cap: .max_backoff)
        .stop_when(|res| res.code() == Some(0))   // a clean exit ends supervision
        .run()
        .await?;
    println!("ended after {} restarts: {:?}", outcome.restarts, outcome.stopped);
    Ok(())
}
```

`run()` reports a `SupervisionOutcome` — the final run's result, the restart
count, and why supervision stopped. Supervision is platform-agnostic and runs
through the `ProcessRunner` seam: pass `.with_runner(&group)` to keep every
incarnation in one shared kill-on-drop group, or a `ScriptedRunner` to test
supervision logic hermetically.

## Async streaming and interactive I/O

The one-shot helpers above buffer the whole output. For long-running or
conversational children, `start()` returns a live [`RunningProcess`] you can
drive asynchronously.

### Stream stdout line by line

Process each line as it arrives — no waiting for the child to exit, no buffering
the full output. `StreamExt` (re-exported from `tokio-stream`) provides `.next()`:

```rust,no_run
use processkit::{Command, StreamExt};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("git")
        .args(["log", "--oneline", "-n", "50"])
        .start()
        .await?;

    let mut lines = run.stdout_lines();
    while let Some(line) = lines.next().await {
        println!("commit: {line}");
    }

    // After the stream ends, collect the exit code and whatever went to stderr
    // (drained in the background while you streamed stdout). `code` is `None` if
    // the run was killed (timeout / signal) and so produced no exit code.
    let (code, stderr) = run.finish_streamed().await?;
    if code != Some(0) {
        eprintln!("git exited {code:?}: {stderr}");
    }
    Ok(())
}
```

> Streaming does **not** auto-enforce the command's [`timeout`]: it applies to the
> run-to-completion helpers (`output_string`/`run`/`first_line`). To bound a manual
> stream, wrap your loop in [`tokio::time::timeout`] and drop the handle (which
> kills the tree) on elapse.

### Interactive stdin — write requests, read responses

Keep stdin open with `keep_stdin_open()`, take the writer with
`standard_input()`, then interleave async writes and reads:

```rust,no_run
use processkit::{Command, StreamExt};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // `bc` evaluates each stdin line and prints the result on stdout.
    let mut run = Command::new("bc").keep_stdin_open().start().await?;

    let mut stdin = run.standard_input().expect("stdin was kept open");
    stdin.write_line("2 + 2").await?;
    stdin.write_line("6 * 7").await?;
    stdin.finish().await?; // send EOF so bc finishes

    let mut answers = run.stdout_lines();
    while let Some(answer) = answers.next().await {
        println!("bc says: {answer}");
    }
    Ok(())
}
```

### Feed stdin from an async stream, react to stdout as it's read

`Stdin::from_lines` writes each item of any `Stream<Item = String>` as a line —
back it with a channel, a file tail, or a network source. Pair it with
`on_stdout_line` / `on_stderr_line` to handle output inline (the handler runs on
the read pump, in addition to capture):

```rust,no_run
use processkit::{Command, Stdin};
use tokio_stream::iter; // any `Stream<Item = String>` works

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let input = iter(vec!["banana".to_owned(), "apple".to_owned(), "cherry".to_owned()]);

    let result = Command::new("sort")
        .stdin(Stdin::from_lines(input))
        .on_stdout_line(|line| println!("sorted: {line}"))
        .output_string()
        .await?;
    let _ = result; // already printed line by line above
    Ok(())
}
```

## Wrapping a CLI tool

`CliClient` + the `cli_client!` macro turn a typed wrapper around an external
tool (`git`, `jj`, `gh`, …) into just its parsers — the runner is injectable, so
the wrapper is hermetically testable with a `ScriptedRunner` (no subprocess):

```rust,no_run
use processkit::{cli_client, ProcessRunner, Result};
use std::path::Path;

cli_client!(pub struct Git => "git");

impl<R: ProcessRunner> Git<R> {
    async fn head(&self, dir: &Path) -> Result<String> {
        self.core.text(self.core.command_in(dir, ["rev-parse", "HEAD"])).await
    }
}
```

## Contributing

Running the tests and the (maintainer-only) release process are documented in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [MIT License](LICENSE).

[`ProcessGroup`]: https://docs.rs/processkit/latest/processkit/struct.ProcessGroup.html
[`Command`]: https://docs.rs/processkit/latest/processkit/struct.Command.html
[`ProcessRunner`]: https://docs.rs/processkit/latest/processkit/trait.ProcessRunner.html
[`RunningProcess`]: https://docs.rs/processkit/latest/processkit/struct.RunningProcess.html
[`timeout`]: https://docs.rs/processkit/latest/processkit/struct.Command.html#method.timeout
[`tokio::time::timeout`]: https://docs.rs/tokio/latest/tokio/time/fn.timeout.html
