# processkit

Child-process management for Rust. A port of the .NET ProcessKit library,
providing two layers:

- **Process groups** ([`ProcessGroup`]) — spawn a child as the root of a process
  tree that is killed as a unit when the group is dropped, using Windows **Job
  Objects** and Linux **cgroup v2** (with a POSIX **process-group** fallback), so
  no descendant ever outlives its owner.
- **Process runner** ([`Command`]) — async (tokio) run-and-capture of a child's
  `stdout`/`stderr` and exit status, built on the group layer, with a mockable
  [`ProcessRunner`] seam for tests.

Async throughout. Errors are structured (`Error`); a non-zero exit is reported in
the result, not raised, until you call `ProcessResult::ensure_success`.

> **Status:** early development (foundation + common helpers). Interactive stdin,
> output-buffer policies, encoding overrides, and rich CPU/memory stats are not
> implemented yet — see [`CHANGELOG.md`](CHANGELOG.md).

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

## Testing

```bash
cargo test                 # hermetic unit tests (no subprocess)
cargo test -- --ignored    # real-subprocess + kill-on-drop tests
cargo test --features mock  # the generated MockRunner
```

## License

Licensed under the [MIT License](LICENSE).

[`ProcessGroup`]: https://docs.rs/processkit/latest/processkit/struct.ProcessGroup.html
[`Command`]: https://docs.rs/processkit/latest/processkit/struct.Command.html
[`ProcessRunner`]: https://docs.rs/processkit/latest/processkit/trait.ProcessRunner.html
