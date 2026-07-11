# Local task runner mirroring the CI gate (see `.github/workflows/ci.yml`).
#
# Requires `just` (https://github.com/casey/just) and, for the `ci` recipe,
# `cargo-hack` (`cargo install cargo-hack`).
#
# `just --list` shows all recipes.

# Fast everyday gate: fmt, clippy (all features), tests (all features,
# including the real-subprocess/`--include-ignored` ones). Not a full CI
# mirror — use `just ci` before opening a PR for that.
check:
    cargo fmt --all --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-features -- --include-ignored

# Full local mirror of the CI workflow's stable-toolchain jobs: fmt, clippy in
# the three feature configurations the CI matrix checks, the feature-powerset
# build via cargo-hack, tests in the three configurations (with
# `--include-ignored` so the real-subprocess tests run), and the two stable
# doc builds. Does not cover the nightly-only CI jobs (docsrs doc,
# minimal-versions, msrv) or the jobs needing external services/tokens
# (coverage/coveralls, cargo-deny, public-api diff, semver-checks) — see the
# optional recipes below for the ones that can still run locally.
ci: fmt-check clippy-all hack test-all doc-all

# Mirrors the CI `fmt` job.
fmt-check:
    cargo fmt --all --check

# Mirrors the CI `clippy` job's three feature configurations.
clippy-all:
    cargo clippy --all-targets --no-default-features -- -D warnings
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-targets --all-features -- -D warnings

# Mirrors the CI `hack` job (feature-powerset build). Requires `cargo-hack`;
# fails with a clear message instead of a raw "no such command" error if it
# isn't installed.
hack:
    @cargo hack --version >/dev/null 2>&1 || (echo "cargo-hack is not installed; run: cargo install cargo-hack" && exit 1)
    cargo hack --feature-powerset --depth 2 check --all-targets

# Mirrors the CI `test` job's three feature configurations, each with
# `--include-ignored` so the real-subprocess/kill-on-drop tests run too.
test-all:
    cargo test --all-features -- --include-ignored
    cargo test -- --include-ignored
    cargo test --no-default-features -- --include-ignored

# Mirrors the CI `doc` job's two stable-toolchain builds (the nightly
# `--cfg docsrs` build is `docsrs-doc` below, since it needs a nightly
# toolchain this recipe doesn't assume is installed).
doc-all:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --no-default-features

# Optional: the two nightly-toolchain CI jobs that don't need external
# services (docsrs doc, minimal-versions). Requires a nightly toolchain
# (`rustup toolchain install nightly`). Not part of `just ci` since most
# contributors won't have nightly set up locally.
ci-nightly: docsrs-doc minimal-versions

# Mirrors the CI `doc` job's nightly `--cfg docsrs` build (activates
# `feature(doc_cfg)` for the per-item "Available on feature X" badges), which
# the two stable builds in `doc-all` leave inert. Requires a nightly toolchain.
docsrs-doc:
    RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --no-deps --all-features

# Mirrors the CI `minimal-versions` job: re-resolves every direct dependency
# down to the lowest SemVer-compatible version, in a throwaway lockfile (the
# committed Cargo.lock is untouched), and builds against that resolution.
# Requires a nightly toolchain.
minimal-versions:
    cargo +nightly -Z direct-minimal-versions generate-lockfile
    cargo +nightly check --all-targets --all-features --locked

# Optional: mirrors the CI `msrv` job. Requires the toolchain pinned below
# (kept in sync with `rust-version` in Cargo.toml) plus the
# `x86_64-pc-windows-msvc` / `aarch64-apple-darwin` targets on it. Not part of
# `just ci` since most contributors won't have this extra toolchain installed.
msrv:
    cargo +1.88 check --all-targets --all-features
    cargo +1.88 check --target x86_64-pc-windows-msvc --lib --bins --all-features
    cargo +1.88 check --target aarch64-apple-darwin --lib --bins --all-features

# Mirrors the CI `test-musl` job locally: builds and runs the full suite
# (same three feature configurations, each with `--include-ignored`) inside a
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
        -v "{{justfile_directory()}}:/work" \
        -v processkit-musl-target:/musl-target \
        -w /work \
        -e CARGO_TARGET_DIR=/musl-target \
        rust:alpine sh -c ' \
            apk add --no-cache procps >/dev/null && \
            cargo build --all-targets --all-features && \
            cargo test --all-features -- --include-ignored && \
            cargo test -- --include-ignored && \
            cargo test --no-default-features -- --include-ignored'

# Optional: mirrors the CI `public-api` job. Requires a nightly toolchain and
# `cargo-public-api` (`cargo install cargo-public-api --locked`); compares the
# crate's current public surface against the committed `public-api.txt`
# baseline.
public-api-diff:
    cargo +nightly public-api --simplified --all-features > public-api-current.txt
    diff public-api.txt public-api-current.txt && echo "(no changes)"
