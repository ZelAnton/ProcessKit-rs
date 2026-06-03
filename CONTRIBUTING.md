# Contributing

Contributions land via pull requests into `main` (branch-protected).

## Testing

```bash
cargo test                  # hermetic unit tests (no subprocess)
cargo test -- --ignored     # real-subprocess + kill-on-drop tests
cargo test --features mock  # the generated MockRunner
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
