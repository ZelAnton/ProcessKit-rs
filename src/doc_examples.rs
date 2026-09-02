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
// The doctest registrations are gated on the optional features the guides
// demonstrate, including the default `process-control`, because they
// collectively reach the
// `process-control`/`stats`/`limits`/`mock`/`tracing`/`metrics`/`record`/`pty`/`json`
// surfaces (`docs/streaming.md`'s interactive-PTY example needs `pty`;
// `docs/observability.md` covers `tracing`/`metrics`). The `cfg` below is the
// exact gate, and deliberately not "every optional feature": a feature no guide
// demonstrates (`report-serde`) would only narrow this pass for nothing.
// The test below stays
// available to ordinary `cargo test`: it makes a new guide fail CI until it is
// explicitly registered here.
#![allow(dead_code)]

#[cfg(all(
    feature = "process-control",
    feature = "stats",
    feature = "limits",
    feature = "mock",
    feature = "tracing",
    feature = "metrics",
    feature = "record",
    feature = "pty",
    feature = "json"
))]
// These hidden modules exist only to register fenced examples as doctests. Their
// mdBook links are ordinary Markdown URLs (and often use downstream
// `processkit::...` paths), not intra-doc links in this private module hierarchy.
#[allow(rustdoc::broken_intra_doc_links)]
mod guides {
    /// `README.md` (crate root) — kept **out** of the rendered crate doc
    /// (see `decisions/readme-crate-doc-sourcing.md`); included here only so its
    /// fenced Rust blocks run as doctests.
    ///
    /// README.md's relative file links (e.g. to `LICENSE`) aren't intra-doc
    /// links; only `--document-private-items` builds check them.
    #[doc(hidden)]
    #[doc = include_str!("../README.md")]
    mod readme {}

    /// `docs/README.md` — the guide-set index.
    #[doc(hidden)]
    #[doc = include_str!("../docs/README.md")]
    mod docs_readme {}

    /// `docs/observability.md` — the `tracing`/`metrics` seams.
    #[doc(hidden)]
    #[doc = include_str!("../docs/observability.md")]
    mod docs_observability {}

    /// `docs/commands.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/commands.md")]
    mod docs_commands {}

    /// `docs/cli-clients.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/cli-clients.md")]
    mod docs_cli_clients {}

    /// `docs/batch.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/batch.md")]
    mod docs_batch {}

    /// `docs/comparison.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/comparison.md")]
    mod docs_comparison {}

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

    /// `docs/troubleshooting.md` — symptom-indexed diagnostic routes.
    #[doc(hidden)]
    #[doc = include_str!("../docs/troubleshooting.md")]
    mod docs_troubleshooting {}

    /// `docs/process-groups.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/process-groups.md")]
    mod docs_process_groups {}

    /// `docs/platform-support.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/platform-support.md")]
    mod docs_platform_support {}

    /// `docs/containers.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/containers.md")]
    mod docs_containers {}

    /// `docs/upgrading.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/upgrading.md")]
    mod docs_upgrading {}

    /// `docs/whats-next.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/whats-next.md")]
    mod docs_whats_next {}

    /// `docs/runtime-neutral-v4-roadmap.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/runtime-neutral-v4-roadmap.md")]
    mod docs_runtime_neutral_v4_roadmap {}

    /// `docs/dotnet-version.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/dotnet-version.md")]
    mod docs_dotnet_version {}

    /// `docs/python-wrapper.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/python-wrapper.md")]
    mod docs_python_wrapper {}

    /// `docs/untrusted-children.md`.
    #[doc(hidden)]
    #[doc = include_str!("../docs/untrusted-children.md")]
    mod docs_untrusted_children {}
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::Path;

    const NON_CHAPTERS: &[&str] = &["SUMMARY.md"];

    fn narrative_guides(docs_dir: &Path) -> BTreeSet<String> {
        fs::read_dir(docs_dir)
            .expect("read docs directory")
            .map(|entry| entry.expect("read docs directory entry").file_name())
            .filter_map(|name| name.into_string().ok())
            .filter(|name| name.ends_with(".md") && !NON_CHAPTERS.contains(&name.as_str()))
            .collect()
    }

    #[test]
    fn every_narrative_guide_is_registered_for_doctests() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let source = fs::read_to_string(manifest_dir.join("src/doc_examples.rs"))
            .expect("read doc_examples harness source");

        let included_guides = source
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix(r#"#[doc = include_str!("../docs/"#)
                    .and_then(|path| path.strip_suffix(r#"")]"#))
                    .map(str::to_owned)
            })
            .collect::<BTreeSet<_>>();
        let guide_files = narrative_guides(&manifest_dir.join("docs"));

        assert_eq!(
            included_guides, guide_files,
            "docs/*.md and doc_examples.rs registrations diverged; add each new narrative guide with #[doc = include_str!(...)] (or list a known non-chapter in NON_CHAPTERS)"
        );

        let registered_modules = source
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix("mod docs_")
                    .and_then(|name| name.strip_suffix(" {}"))
                    .map(str::to_owned)
            })
            .collect::<BTreeSet<_>>();
        let expected_modules = guide_files
            .iter()
            .map(|file| {
                file.trim_end_matches(".md")
                    .replace('-', "_")
                    .to_lowercase()
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(
            registered_modules, expected_modules,
            "every narrative docs/*.md include must have a matching mod docs_* declaration"
        );
    }

    #[test]
    fn harness_gate_is_owned_by_doc_examples() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let lib_source =
            fs::read_to_string(manifest_dir.join("src/lib.rs")).expect("read crate root source");

        assert!(
            lib_source.contains("#[cfg(any(test, doc))]\nmod doc_examples;"),
            "lib.rs must keep the doc_examples module feature-independent; its feature gate belongs in src/doc_examples.rs"
        );
    }
}
