# Decision: README and crate-doc stay separate (not `include_str!`)

> **Status:** decision record / closed. Settled 2026-06-10 while executing ROADMAP
> item 6 ("resolve the README ↔ crate-doc duplication"). Recorded because the prior
> state was *drift-by-default with no rationale on file* — this note IS the resolution.

## The observation

`src/lib.rs`'s `//!` crate doc and `README.md` overlap substantially: the two-layer
intro, the run-verb vocabulary, and the feature list appear in both. They can drift.

The common single-sourcing fix is `#![doc = include_str!("../README.md")]`.

## Decision — keep them separate, deliberately

We do **not** adopt `include_str!`, and we do **not** gut the crate doc to "see the
README." Both remain complete, separately-maintained landing pages. Reasons:

1. **`include_str!` is lossy here.** The README carries (a) relative links into
   `docs/*.md`, which 404 from the docs.rs render; (b) a raw-githubusercontent cover
   image; and (c) prose tuned for the GitHub/crates.io reader. Pulling it verbatim into
   rustdoc would surface broken links and an out-of-place asset on docs.rs.
2. **Two audiences, two landings.** docs.rs is where Rust developers actually read the
   API; the README is the GitHub/crates.io shopfront. Each deserves a *complete* landing
   in its own idiom — trimming the crate doc to defer to the README would degrade the
   docs.rs experience (the primary Rust audience) to save duplication.
3. **The crate-doc earns its length.** Its intro, verb vocabulary, and feature list use
   rustdoc intra-doc links (`[`ProcessGroup`]`, `[`Command::output_string`]`) that only
   work — and only matter — in the rendered API docs.

## Consequence / how drift is managed

Drift risk is **accepted** and handled by review discipline: when the verb set or the
feature flags change, update both `README.md` and the `lib.rs` `//!` block (and the
`Cargo.toml` feature comments) together — they are checked side by side in review. The
drift-prone sections are the verb table and the feature list; both are short.

## Revisit when

A tool appears that renders docs.rs-safe links + assets from a single source, or the
maintenance burden of keeping the two in sync becomes real (it is currently trivial —
both are short and change rarely).
