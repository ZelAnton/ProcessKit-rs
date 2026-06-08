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
`all-features = true` and `rustdoc-args = ["--cfg", "docsrs"]` — the latter
activates `feature(doc_cfg)` (`src/lib.rs`) so each item carries an "Available
on crate feature `X`" badge. CI builds these under `RUSTDOCFLAGS=-D warnings`:
the stable `cargo doc --no-deps --all-features` and `--no-default-features`
builds catch link/warning regressions, and a nightly `cargo doc --all-features`
with `--cfg docsrs` mirrors the exact docs.rs build, so a docs.rs-only failure
(a renamed nightly feature, a bad `doc(cfg)`) surfaces in CI instead of on the
published page.
