<!-- Thanks for contributing! Keep the summary focused on *what changed and why*. -->

## What & why

<!-- A real summary of the change and its motivation. Link any related issue. -->

## Checklist

- [ ] `cargo fmt --all`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` is clean
- [ ] `just check` passes, including ignored real-subprocess tests
- [ ] `CHANGELOG.md` `[Unreleased]` updated when the change is user-facing
      (`Added` / `Changed` / `Fixed`)
- [ ] Docs updated (rustdoc and the `docs/` guide set) if behavior or API changed
- [ ] New dependencies carry a "why" comment in `Cargo.toml` (see `CONTRIBUTING.md`)

## Notes for reviewers

<!-- Anything non-obvious: a platform caveat, a trade-off, a follow-up deferred to ideas/. -->
