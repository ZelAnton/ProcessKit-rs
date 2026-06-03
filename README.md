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
> encoding overrides, line counters, and CPU/memory stats. See
> [`CHANGELOG.md`](CHANGELOG.md).

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

## Testing

```bash
cargo test                 # hermetic unit tests (no subprocess)
cargo test -- --ignored    # real-subprocess + kill-on-drop tests
cargo test --features mock  # the generated MockRunner
```

## Releasing

Maintainer-only, via the **Release** GitHub Actions workflow (manual
`workflow_dispatch` — pick `patch` / `minor` / `major`). It bumps the version,
promotes `CHANGELOG.md`, publishes to crates.io, tags `v<version>`, and creates the
GitHub Release. The release commit is pushed to `main` with a dedicated **GitHub App**
token, so it works under branch protection without a personal token (the App is in the
ruleset's bypass list). Contributions land via pull requests into `main`.

## License

Licensed under the [MIT License](LICENSE).

[`ProcessGroup`]: https://docs.rs/processkit/latest/processkit/struct.ProcessGroup.html
[`Command`]: https://docs.rs/processkit/latest/processkit/struct.Command.html
[`ProcessRunner`]: https://docs.rs/processkit/latest/processkit/trait.ProcessRunner.html
[`RunningProcess`]: https://docs.rs/processkit/latest/processkit/struct.RunningProcess.html
[`timeout`]: https://docs.rs/processkit/latest/processkit/struct.Command.html#method.timeout
[`tokio::time::timeout`]: https://docs.rs/tokio/latest/tokio/time/fn.timeout.html
