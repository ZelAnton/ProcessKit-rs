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

### Build-disk maintenance

Cargo does not garbage-collect obsolete incremental sessions under `target/`.
Feature powersets, alternate toolchains, custom `RUSTFLAGS`, and repeated
integration-test binaries can therefore leave many one-use unit hashes behind.
The matrix-style `just` recipes disable incremental compilation; ordinary
`cargo build`, `cargo test`, and `just check` retain it for fast iteration.
This is intentionally per recipe rather than a global Cargo setting: disabling
incremental compilation everywhere would slow normal edit/build cycles, while
using a separate matrix target would duplicate dependency artifacts. CI keeps
its existing settings because hosted runners and their caches have bounded
lifetime; the unbounded growth is specific to persistent developer checkouts.

Run `just clean-disk` when old sessions have accumulated. It removes only
directories named `incremental` beneath the main checkout's `target/` and task
worktree targets, plus disposable `mutants.out{,.old}` reports. It deliberately
keeps rust-analyzer caches, local tools, review-specific target directories,
cross-target artifacts, and other normal build output. The helper deletes
contents through a configured target path without deleting that root, so a
symlink or Windows junction used to relocate `target/` remains intact. Inspect
mutation reports before cleanup if their outcomes are still needed.

### Mutation testing

The scheduled CI workflow runs the sharded mutation tier and reads
`mutants.out/missed.txt` and `mutants.out/timeout.txt` before the runner is
discarded. For a local run, keep cargo-mutants' potentially large scratch tree
outside the checkout:

```bash
scratch="${TMPDIR:-/tmp}/processkit-mutants-$$"
cargo mutants --output "$scratch"
rm -rf "$scratch"                 # also safe after an interrupted run
```

PowerShell equivalent:

```powershell
$scratch = Join-Path ([IO.Path]::GetTempPath()) "processkit-mutants-$PID"
cargo mutants --output $scratch
Remove-Item -LiteralPath $scratch -Recurse -Force -ErrorAction SilentlyContinue
```

The default in-tree `mutants.out/` and its rotated `mutants.out.old/` are ignored as a
safety net, including in Orchestra task worktrees, so an interrupted/default run cannot
be absorbed by a later `jj describe` or `jj new`. They remain disposable; inspect any
`outcomes.json`, `missed.txt`, or `timeout.txt` you need before deleting them.

## Releasing

Maintainer-only, via the **Release** GitHub Actions workflow (manual
`workflow_dispatch` — pick `patch` / `minor` / `major`). It bumps the version,
promotes `CHANGELOG.md`, publishes to crates.io, tags `v<version>`, and creates the
GitHub Release. The release commit is pushed to `main` with a dedicated **GitHub App**
token, so it works under branch protection without a personal token (the App is in the
ruleset's bypass list).

### Publishing to crates.io (Trusted Publishing)

The workflow publishes with **crates.io Trusted Publishing**: it mints a
short-lived token over GitHub OIDC for that run and passes it to `cargo publish`.
No long-lived `CRATES_IO_TOKEN` secret is stored or used — the token is scoped to
the run and auto-revoked when it ends. If it cannot be minted (OIDC unavailable,
or no trusted publisher configured on crates.io), the release **fails loudly**;
it never falls back to a stored secret.

One-time setup (repository owner). On crates.io, add a trusted publisher for the
`processkit` crate under *Settings → Trusted Publishing → GitHub* with exactly:

- Repository owner: `ZelAnton`
- Repository name: `ProcessKit-rs`
- Workflow filename: `release.yml`
- Environment: *(leave empty — this workflow uses no GitHub Environment)*

Post-migration step (repository owner). After the **first** successful release
through this path, delete the now-unused `CRATES_IO_TOKEN` repository secret
(*Settings → Secrets and variables → Actions*). It is intentionally kept until
then so a first-release problem can be diagnosed without racing to restore a
credential; the workflow no longer reads it.

Each release also issues **build-provenance attestations** for the packaged
`.crate` and its `SHA256SUMS` and attaches both files to the GitHub Release, so
consumers can verify the artifacts were built by this repository and workflow —
see [SECURITY.md](SECURITY.md) and the README's *Verifying provenance* section
for the exact `gh attestation verify` command.

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
