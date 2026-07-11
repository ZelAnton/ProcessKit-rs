# Contributing

Contributions land via pull requests into `main` (branch-protected).

New to the codebase? [ARCHITECTURE.md](ARCHITECTURE.md) maps the internal
layers (`Command` → runner/client → running → pump/buffer → `sys` backends),
the test-double seams, the run's data flow, and the invariants (kill-on-drop,
teardown, cancel-vs-timeout) that changes must preserve.

## Testing

```bash
cargo test                              # hermetic unit tests (no subprocess)
cargo test --all-features -- --ignored  # real-subprocess + kill-on-drop tests
                                        # (--all-features: the `limits` tests are
                                        #  compiled out by default)
cargo test --features mock              # the generated MockRunner
```

`cargo test --all-features` also compiles (and, unless a fenced block is
annotated `no_run` or `ignore`, runs) every Rust code sample in `docs/*.md`
and the root `README.md` as an ordinary doctest — via the test-only, hidden
harness in `src/doc_examples.rs` — so a signature change that breaks a
guide's example fails CI instead of silently going stale. The harness only
builds under `--all-features` (the guides collectively exercise every
optional feature), so a plain `cargo test` with the default features does
**not** check the guides — use `--all-features` (as CI does) when touching a
public signature and checking whether a guide's snippet needs the same edit.

Before opening a PR:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

Or run the equivalent [`just`](https://github.com/casey/just) recipes from the
repository root: `just check` for the fast everyday gate (fmt, clippy,
`--include-ignored` tests), or `just ci` for a fuller local mirror of the CI
workflow (fmt, clippy and tests in all three feature configurations, the
`cargo-hack` feature-powerset build, and the doc builds). `just ci` needs
`cargo-hack` installed (`cargo install cargo-hack`); run `just --list` to see
the optional recipes (`just ci-nightly`, `just msrv`, `just public-api-diff`,
`just test-musl`) that mirror the remaining CI jobs but need a nightly
toolchain or extra tools (`test-musl` needs Docker — it runs the
real-subprocess suite inside a real Alpine/musl container, mirroring the CI
`test-musl` job; see [platform-support.md](docs/platform-support.md#ci-coverage)).

## Releasing

Maintainer-only, via the **Release** GitHub Actions workflow (manual
`workflow_dispatch` — pick `patch` / `minor` / `major`). It bumps the version,
promotes `CHANGELOG.md`, publishes to crates.io, tags `v<version>`, and creates the
GitHub Release. The release commit is pushed to `main` with a dedicated **GitHub App**
token, so it works under branch protection without a personal token (the App is in the
ruleset's bypass list).

The docs.rs API reference is published by that same crates.io release: after
`cargo publish` uploads the crate, docs.rs builds
`https://docs.rs/processkit/<version>/processkit/` from the published package.
This is separate from — and coexists with — the narrative documentation site
(the mdBook guide set under `docs/`), which the `docs` GitHub Actions workflow
(`.github/workflows/docs.yml`) publishes to GitHub Pages at
`https://zelanton.github.io/ProcessKit-rs/` on every push to `main` that
touches `docs/**`, `theme/**`, or `book.toml`; it is not tied to a crates.io
release. The manifest's `[package.metadata.docs.rs]` sets
`all-features = true` and `rustdoc-args = ["--cfg", "docsrs"]` — the latter
activates `feature(doc_cfg)` (`src/lib.rs`) so each item carries an "Available
on crate feature `X`" badge. CI builds these under `RUSTDOCFLAGS=-D warnings`:
the stable `cargo doc --no-deps --all-features` and `--no-default-features`
builds catch link/warning regressions, and a nightly `cargo doc --all-features`
with `--cfg docsrs` mirrors the exact docs.rs build, so a docs.rs-only failure
(a renamed nightly feature, a bad `doc(cfg)`) surfaces in CI instead of on the
published page.
