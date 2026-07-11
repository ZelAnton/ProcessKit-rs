// Test-only harness: not part of the public API (this whole module is
// private and additionally `#[doc(hidden)]`), never rendered by `cargo doc`,
// and irrelevant to `public-api.txt` / docs.rs. Its only job is to make
// `cargo test`'s doctest pass pick up the fenced Rust code blocks in every
// narrative guide (`docs/*.md`) and the root `README.md`, so a signature
// change that silently stops matching a guide's example fails CI instead of
// quietly lying to a reader (see task T-047).
//
// This is deliberately unrelated to `decisions/readme-crate-doc-sourcing.md`
// (which is about *not* pulling `README.md`'s prose into the rendered crate
// doc via `#![doc = include_str!(...)]` on the crate root, to avoid
// duplicating/mismatching the docs.rs landing page). Here the include lands
// on a private, hidden module purely so rustdoc's doctest extractor walks its
// fenced blocks — the text itself is never published anywhere.
//
// Gated on all six optional features, including the default
// `process-control`, so it only builds under `cargo test --all-features`
// (the guides collectively demonstrate the `process-control`/`stats`/
// `limits`/`mock`/`tracing`/`record` surfaces); the default and
// `--no-default-features` CI legs never see this module at all.
#![allow(dead_code)]

/// `README.md` (crate root) — kept **out** of the rendered crate doc
/// (see `decisions/readme-crate-doc-sourcing.md`); included here only so its
/// fenced Rust blocks run as doctests.
#[doc(hidden)]
#[doc = include_str!("../README.md")]
mod readme {}

/// `docs/README.md` — the guide-set index.
#[doc(hidden)]
#[doc = include_str!("../docs/README.md")]
mod docs_readme {}

/// `docs/commands.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/commands.md")]
mod docs_commands {}

/// `docs/cookbook.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/cookbook.md")]
mod docs_cookbook {}

/// `docs/streaming.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/streaming.md")]
mod docs_streaming {}

/// `docs/pipelines.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/pipelines.md")]
mod docs_pipelines {}

/// `docs/supervision.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/supervision.md")]
mod docs_supervision {}

/// `docs/testing.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/testing.md")]
mod docs_testing {}

/// `docs/timeouts-and-cancellation.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/timeouts-and-cancellation.md")]
mod docs_timeouts_and_cancellation {}

/// `docs/errors.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/errors.md")]
mod docs_errors {}

/// `docs/process-groups.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/process-groups.md")]
mod docs_process_groups {}

/// `docs/platform-support.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/platform-support.md")]
mod docs_platform_support {}

/// `docs/upgrading.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/upgrading.md")]
mod docs_upgrading {}

/// `docs/whats-next.md`.
#[doc(hidden)]
#[doc = include_str!("../docs/whats-next.md")]
mod docs_whats_next {}
