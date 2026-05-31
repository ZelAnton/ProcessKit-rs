# processkit

Child-process management for Rust. A port of the .NET ProcessKit library,
providing two layers:

- **Process groups** — spawn a child as the root of a process tree that is
  killed as a unit when the group is dropped, using Windows **Job Objects** and
  POSIX **process groups**, so no descendant ever outlives its owner.
- **Process runner** — async run-and-capture of a child's `stdout`/`stderr` and
  exit status, built on the group layer.

> **Status:** early development. The public API is still being built out; see
> [`CHANGELOG.md`](CHANGELOG.md) for what has landed.

## Install

```bash
cargo add processkit
```

## License

Licensed under the [MIT License](LICENSE).
