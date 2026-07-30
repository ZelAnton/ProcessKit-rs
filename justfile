# Local task runner mirroring the CI gate (see `.github/workflows/ci.yml`).
#
# Requires `just` (https://github.com/casey/just). `just setup` installs the
# remaining repository tools; `just doctor` checks them without changing the
# machine.
#
# `just --list` shows all recipes.

# Plain cargo commands run through the system shell. Windows has PowerShell but
# need not have a POSIX `sh`; environment-sensitive recipes use just attributes
# below so the same bodies work in both shells.
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

# Bootstrap every Rust toolchain/CLI used by the local CI mirrors. Versions are
# constrained exactly where CI constrains them (notably mdBook 0.4.40 and the
# nextest 0.9 series); Docker is diagnosed but remains a manual prerequisite
# because installing a host daemon is outside a repository bootstrap.
[script('python')]
[windows]
setup:
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "tools/dev_tools.py", "setup"]))

[script('python3')]
[unix]
setup:
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "tools/dev_tools.py", "setup"]))

# Read-only counterpart to setup: useful before a long gate, or to explain why
# one stopped early. A missing Docker daemon is a warning limited to test-musl;
# missing compilers or CLIs fail the recipe.
[script('python')]
[windows]
doctor:
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "tools/dev_tools.py", "doctor"]))

[script('python3')]
[unix]
doctor:
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "tools/dev_tools.py", "doctor"]))

# Fast everyday gate: fmt, clippy (all features), tests (all features,
# including the ignored real-subprocess ones through nextest), and the
# `#[cfg(fuzzing)]` type-check. Not a full CI mirror — use `just ci` before
# opening a PR for that.
check:
    cargo fmt --all --check
    cargo clippy --all-targets --all-features -- -D warnings
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
[env("CARGO_INCREMENTAL", "0")]
clippy-all:
    cargo clippy --all-targets --no-default-features -- -D warnings
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-targets --all-features -- -D warnings

# Mirrors the CI `hack` job (feature-powerset build). The powerset creates many
# one-use unit hashes, so it must not retain an incremental session for every
# feature/target combination. `just doctor` diagnoses a missing cargo-hack.
[env("CARGO_INCREMENTAL", "0")]
hack:
    cargo hack --feature-powerset --depth 2 check --all-targets

# Mirrors the CI `test` job's three feature configurations through nextest,
# including ignored real-subprocess/kill-on-drop tests. Doctests remain
# separate because nextest does not run them. These three cold feature matrices
# are CI mirrors rather than iterative builds, so their incremental caches are
# deliberately disabled.
[env("CARGO_INCREMENTAL", "0")]
test-all:
    cargo nextest run --profile ci-all --all-features --run-ignored all
    cargo test --all-features --doc
    cargo nextest run --profile ci-default --run-ignored all
    cargo test --doc
    cargo nextest run --profile ci-minimal --no-default-features --run-ignored all
    cargo test --no-default-features --doc

# Mirrors the CI `doc` job's two stable-toolchain builds (the nightly
# `--cfg docsrs` build is `docsrs-doc` below, since it needs a nightly
# toolchain this recipe doesn't assume is installed). Documentation matrices
# are one-shot outputs, so their incremental state is pure disk overhead.
[env("CARGO_INCREMENTAL", "0")]
[env("RUSTDOCFLAGS", "-D warnings")]
doc-all:
    @# Restrict --document-private-items to --all-features, just like CI
    cargo doc --document-private-items --no-deps --all-features
    cargo doc --no-deps --no-default-features

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
[env("CARGO_INCREMENTAL", "0")]
[env("RUSTDOCFLAGS", "--cfg docsrs -D warnings")]
docsrs-doc:
    cargo +nightly doc --no-deps --all-features

# Mirrors the CI `minimal-versions` job: re-resolves every direct dependency
# down to the lowest SemVer-compatible version, in a throwaway lockfile (the
# committed Cargo.lock is untouched), and builds against that resolution.
# Requires a nightly toolchain. Its throwaway dependency resolution is not
# reused by normal development, so retaining incremental state only consumes
# disk.
[env("CARGO_INCREMENTAL", "0")]
[script('python')]
[windows]
minimal-versions:
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "tools/dev_tools.py", "minimal-versions"]))

[env("CARGO_INCREMENTAL", "0")]
[script('python3')]
[unix]
minimal-versions:
    import subprocess, sys
    raise SystemExit(subprocess.call([sys.executable, "tools/dev_tools.py", "minimal-versions"]))

# Optional: mirrors the CI `msrv` job. Requires the toolchain pinned below
# (kept in sync with `rust-version` in Cargo.toml) plus the
# `x86_64-pc-windows-msvc` / `aarch64-apple-darwin` targets on it. Not part of
# `just ci` since most contributors won't have this extra toolchain installed.
# The pinned compiler and cross-target hashes are single-use compatibility
# checks, so they do not retain incremental sessions.
[env("CARGO_INCREMENTAL", "0")]
msrv:
    cargo +1.88 check --all-targets --all-features
    cargo +1.88 check --target x86_64-pc-windows-msvc --lib --bins --all-features
    cargo +1.88 check --target aarch64-apple-darwin --lib --bins --all-features

# Mirrors the CI `test-musl` job locally: builds and runs the full suite
# (same three feature configurations, including ignored tests) inside a
# real Alpine/musl container — busybox userland, musl libc — not merely a
# cross-compiled musl-target binary run under glibc userland tools. Requires
# Docker. `--init` supplies a real subreaper and `--cap-add=SYS_NICE` restores
# a capability Docker drops by default; see the CI job's comments in
# `.github/workflows/ci.yml` for why both are needed. `procps` swaps in a
# `ps` that supports `-p PID` (busybox's does not), which one test needs.
# The complete `./target` path is shadowed by a named Docker volume. This keeps
# both Cargo artifacts and nextest's fixed `target/nextest` report paths off the
# host filesystem, so musl output never mixes with native artifacts and Docker
# Desktop never asks nextest to create its store on a bind-mounted NTFS tree.
# `MSYS_NO_PATHCONV=1` is a no-op outside Git Bash on Windows; there it stops
# Git Bash from mangling the `/work`-style container paths below.
[env("MSYS_NO_PATHCONV", "1")]
[unix]
test-musl:
    docker run --rm --init --cap-add=SYS_NICE \
        -v "{{ justfile_directory() }}:/work" \
        -v processkit-musl-target:/work/target \
        -w /work \
        -e CARGO_TARGET_DIR=/work/target \
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

[windows]
test-musl:
    docker run --rm --init --cap-add=SYS_NICE -v "{{ justfile_directory() }}:/work" -v processkit-musl-target:/work/target -w /work -e CARGO_TARGET_DIR=/work/target rust:alpine sh -c 'apk add --no-cache curl procps >/dev/null && curl -LsSf https://get.nexte.st/0.9/linux-musl | tar zxf - -C /usr/local/cargo/bin && cargo build --all-targets --all-features && cargo nextest run --profile ci-all --all-features --run-ignored all && cargo test --all-features --doc && cargo nextest run --profile ci-default --run-ignored all && cargo test --doc && cargo nextest run --profile ci-minimal --no-default-features --run-ignored all && cargo test --no-default-features --doc'

# Mirrors the fuzz-check CI job. Type-checks the `#[cfg(fuzzing)]` code
# without actually running `cargo-fuzz` or requiring a nightly toolchain. The
# custom cfg creates a one-use unit hash, so its incremental state is disabled.
[env("CARGO_INCREMENTAL", "0")]
[env("RUSTFLAGS", "--cfg fuzzing")]
fuzz-check:
    cargo check --all-features --lib

# Mirrors the CI `typos` job. Requires the `typos` CLI
# (`cargo install typos-cli`). Config/allow-list is `_typos.toml`.
typos:
    typos

# Optional: mirrors the CI `public-api` job. Requires a nightly toolchain and
# `cargo-public-api` (`cargo install cargo-public-api --locked`); compares the
# crate's current public surface against the committed `public-api.txt`
# baseline. The nightly/API configuration is a one-shot validation and does
# not retain an incremental session.
[env("CARGO_INCREMENTAL", "0")]
[unix]
public-api-diff:
    cargo +nightly public-api --simplified --all-features > public-api-current.txt
    diff public-api.txt public-api-current.txt && echo "(no changes)"

[env("CARGO_INCREMENTAL", "0")]
[windows]
public-api-diff:
    cargo +nightly public-api --simplified --all-features | Set-Content -Encoding UTF8 public-api-current.txt
    $difference = Compare-Object (Get-Content public-api.txt) (Get-Content public-api-current.txt) -SyncWindow 0; if ($difference) { $difference; exit 1 } else { Write-Output "(no changes)" }

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
