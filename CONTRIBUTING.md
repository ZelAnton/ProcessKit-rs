# Contributing

Contributions land via pull requests into `main` (branch-protected).

## Testing

```bash
cargo test                              # hermetic unit tests (no subprocess)
cargo test --all-features -- --ignored  # real-subprocess + kill-on-drop tests
                                        # (--all-features: the `limits` tests are
                                        #  compiled out by default)
cargo test --features mock              # the generated MockRunner
```

Before opening a PR:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## Releasing

Maintainer-only, via the **Release** GitHub Actions workflow (manual
`workflow_dispatch` — pick `patch` / `minor` / `major`). It bumps the version,
promotes `CHANGELOG.md`, publishes to crates.io, tags `v<version>`, and creates the
GitHub Release. The release commit is pushed to `main` with a dedicated **GitHub App**
token, so it works under branch protection without a personal token (the App is in the
ruleset's bypass list).

The docs.rs API reference is published by that same crates.io release. There is
no separate documentation deploy: after `cargo publish` uploads the crate,
docs.rs builds `https://docs.rs/processkit/<version>/processkit/` from the
published package. The manifest's `[package.metadata.docs.rs]` sets
`all-features = true`, and CI already builds the same profile with
`cargo doc --no-deps --all-features` under `RUSTDOCFLAGS=-D warnings`, plus the
minimal `--no-default-features` docs build.
