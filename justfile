# Local task runner mirroring the CI gate (see `.github/workflows/ci.yml`).
#
# Requires `just` (https://github.com/casey/just) and, for the `ci` recipe,
# `cargo-hack` plus `cargo-nextest`.
#
# `just --list` shows all recipes.

# Fast everyday gate: fmt, clippy (all features), tests (all features,
# including the ignored real-subprocess ones through nextest), and the
# `#[cfg(fuzzing)]` type-check. Not a full CI mirror — use `just ci` before
# opening a PR for that.
check:
    cargo fmt --all --check
    cargo clippy --all-targets --all-features -- -D warnings
    @cargo nextest --version >/dev/null 2>&1 || (echo "cargo-nextest is not installed; see: https://nexte.st/docs/installation/" && exit 1)
    cargo nextest run --profile ci-all --all-features --run-ignored all
    cargo test --all-features --doc
    just fuzz-check

# Full local mirror of the CI workflow's stable-toolchain jobs: fmt, clippy in
# the three feature configurations the CI matrix checks, the feature-powerset
# build via cargo-hack, tests in the three configurations (including the
# ignored real-subprocess tests), and the two stable
# doc builds, and the typos spell check. Does not cover the nightly-only CI
# jobs (docsrs doc, minimal-versions, msrv) or the jobs needing external
# services/tokens (coverage/coveralls, cargo-deny, public-api diff,
# semver-checks) — see the optional recipes below for the ones that can still
# run locally.
ci: fmt-check clippy-all hack test-all doc-all typos fuzz-check

# Mirrors the CI `fmt` job.
fmt-check:
    cargo fmt --all --check

# Mirrors the CI `clippy` job's three feature configurations. Each
# configuration has a distinct unit hash and is normally compiled only once,
# so retaining its incremental state grows `target/` without helping the next
# invocation. The everyday `check` recipe keeps incremental compilation.
clippy-all:
    CARGO_INCREMENTAL=0 cargo clippy --all-targets --no-default-features -- -D warnings
    CARGO_INCREMENTAL=0 cargo clippy --all-targets -- -D warnings
    CARGO_INCREMENTAL=0 cargo clippy --all-targets --all-features -- -D warnings

# Mirrors the CI `hack` job (feature-powerset build). Requires `cargo-hack`;
# fails with a clear message instead of a raw "no such command" error if it
# isn't installed. The powerset creates many one-use unit hashes, so it must
# not retain an incremental session for every feature/target combination.
hack:
    @cargo hack --version >/dev/null 2>&1 || (echo "cargo-hack is not installed; run: cargo install cargo-hack" && exit 1)
    CARGO_INCREMENTAL=0 cargo hack --feature-powerset --depth 2 check --all-targets

# Mirrors the CI `test` job's three feature configurations through nextest,
# including ignored real-subprocess/kill-on-drop tests. Doctests remain
# separate because nextest does not run them. These three cold feature matrices
# are CI mirrors rather than iterative builds, so their incremental caches are
# deliberately disabled.
test-all:
    @cargo nextest --version >/dev/null 2>&1 || (echo "cargo-nextest is not installed; see: https://nexte.st/docs/installation/" && exit 1)
    CARGO_INCREMENTAL=0 cargo nextest run --profile ci-all --all-features --run-ignored all
    CARGO_INCREMENTAL=0 cargo test --all-features --doc
    CARGO_INCREMENTAL=0 cargo nextest run --profile ci-default --run-ignored all
    CARGO_INCREMENTAL=0 cargo test --doc
    CARGO_INCREMENTAL=0 cargo nextest run --profile ci-minimal --no-default-features --run-ignored all
    CARGO_INCREMENTAL=0 cargo test --no-default-features --doc

# Mirrors the CI `doc` job's two stable-toolchain builds (the nightly
# `--cfg docsrs` build is `docsrs-doc` below, since it needs a nightly
# toolchain this recipe doesn't assume is installed). Documentation matrices
# are one-shot outputs, so their incremental state is pure disk overhead.
doc-all:
    @# Restrict --document-private-items to --all-features, just like CI
    CARGO_INCREMENTAL=0 RUSTDOCFLAGS="-D warnings" cargo doc --document-private-items --no-deps --all-features
    CARGO_INCREMENTAL=0 RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --no-default-features

# Optional: the two nightly-toolchain CI jobs that don't need external
# services (docsrs doc, minimal-versions). Requires a nightly toolchain
# (`rustup toolchain install nightly`). Not part of `just ci` since most
# contributors won't have nightly set up locally.
ci-nightly: docsrs-doc minimal-versions

# Mirrors the CI `doc` job's nightly `--cfg docsrs` build (activates
# `feature(doc_cfg)` for the per-item "Available on feature X" badges), which
# the two stable builds in `doc-all` leave inert. Requires a nightly toolchain;
# its separate compiler/configuration hash is not retained after this one-shot
# compatibility check.
docsrs-doc:
    CARGO_INCREMENTAL=0 RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --no-deps --all-features

# Mirrors the CI `minimal-versions` job: re-resolves every direct dependency
# down to the lowest SemVer-compatible version, in a throwaway lockfile (the
# committed Cargo.lock is untouched), and builds against that resolution.
# Requires a nightly toolchain. Its throwaway dependency resolution is not
# reused by normal development, so retaining incremental state only consumes
# disk.
minimal-versions:
    cargo +nightly -Z direct-minimal-versions generate-lockfile
    CARGO_INCREMENTAL=0 cargo +nightly check --all-targets --all-features --locked

# Optional: mirrors the CI `msrv` job. Requires the toolchain pinned below
# (kept in sync with `rust-version` in Cargo.toml) plus the
# `x86_64-pc-windows-msvc` / `aarch64-apple-darwin` targets on it. Not part of
# `just ci` since most contributors won't have this extra toolchain installed.
# The pinned compiler and cross-target hashes are single-use compatibility
# checks, so they do not retain incremental sessions.
msrv:
    CARGO_INCREMENTAL=0 cargo +1.88 check --all-targets --all-features
    CARGO_INCREMENTAL=0 cargo +1.88 check --target x86_64-pc-windows-msvc --lib --bins --all-features
    CARGO_INCREMENTAL=0 cargo +1.88 check --target aarch64-apple-darwin --lib --bins --all-features

# Mirrors the CI `test-musl` job locally: builds and runs the full suite
# (same three feature configurations, including ignored tests) inside a
# real Alpine/musl container — busybox userland, musl libc — not merely a
# cross-compiled musl-target binary run under glibc userland tools. Requires
# Docker. `--init` supplies a real subreaper and `--cap-add=SYS_NICE` restores
# a capability Docker drops by default; see the CI job's comments in
# `.github/workflows/ci.yml` for why both are needed. `procps` swaps in a
# `ps` that supports `-p PID` (busybox's does not), which one test needs.
# The build output goes to a named Docker volume, not `./target`, so this
# never mixes musl-linked artifacts into your native `target/` directory.
# `MSYS_NO_PATHCONV=1` is a no-op outside Git Bash on Windows; there it stops
# Git Bash from mangling the `/work`-style container paths below.
test-musl:
    MSYS_NO_PATHCONV=1 docker run --rm --init --cap-add=SYS_NICE \
        -v "{{ justfile_directory() }}:/work" \
        -v processkit-musl-target:/musl-target \
        -w /work \
        -e CARGO_TARGET_DIR=/musl-target \
        rust:alpine sh -c ' \
            apk add --no-cache curl procps >/dev/null && \
            curl -LsSf https://get.nexte.st/0.9/linux-musl | tar zxf - -C /usr/local/cargo/bin && \
            cargo build --all-targets --all-features && \
            cargo nextest run --profile ci-all --all-features --run-ignored all && \
            cargo test --all-features --doc && \
            cargo nextest run --profile ci-default --run-ignored all && \
            cargo test --doc && \
            cargo nextest run --profile ci-minimal --no-default-features --run-ignored all && \
            cargo test --no-default-features --doc'

# Mirrors the fuzz-check CI job. Type-checks the `#[cfg(fuzzing)]` code
# without actually running `cargo-fuzz` or requiring a nightly toolchain. The
# custom cfg creates a one-use unit hash, so its incremental state is disabled.
fuzz-check:
    CARGO_INCREMENTAL=0 RUSTFLAGS="--cfg fuzzing" cargo check --all-features --lib

# Mirrors the CI `typos` job. Requires the `typos` CLI
# (`cargo install typos-cli`). Config/allow-list is `_typos.toml`.
typos:
    @typos --version >/dev/null 2>&1 || (echo "typos is not installed; run: cargo install typos-cli" && exit 1)
    typos

# Optional: mirrors the CI `public-api` job. Requires a nightly toolchain and
# `cargo-public-api` (`cargo install cargo-public-api --locked`); compares the
# crate's current public surface against the committed `public-api.txt`
# baseline. The nightly/API configuration is a one-shot validation and does
# not retain an incremental session.
public-api-diff:
    CARGO_INCREMENTAL=0 cargo +nightly public-api --simplified --all-features > public-api-current.txt
    diff public-api.txt public-api-current.txt && echo "(no changes)"

# Cargo never garbage-collects obsolete incremental unit hashes under a
# workspace target directory. Remove only those caches (plus disposable
# cargo-mutants reports) while preserving ordinary build outputs, local tools,
# cross-target artifacts, and the `target` directory itself when it is a
# symlink/junction. The helper also covers task-worktree target directories.
[windows]
clean-disk:
    python tools/clean_disk.py

[unix]
clean-disk:
    python3 tools/clean_disk.py

# Compare processkit's end-to-end process handling with the plain Tokio and
# standard-library APIs. This is intentionally local-only: results vary with
# the OS, scheduler, toolchain, and machine, and are not a CI gate.
bench-compare:
    cargo bench --bench compare
