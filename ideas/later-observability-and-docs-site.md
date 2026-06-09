# later: observability surface + hosted docs site

> **Status:** open idea (later). From the 2026-06-09 sweep. Two unrelated "polish"
> items, both genuinely optional, parked together to keep the backlog tidy.

## A. Metrics emission (a `metrics` feature)
*Borrow: modern infra libs; CliWrap/execa expose duration · Cost: moderate*

A `tracing` feature already emits per-run events (program/exit/duration — never
argv/env). There's no **metrics** surface: counts, duration histograms, exit-code /
timeout / cancel tallies. A thin optional feature over the `metrics` crate (or an
OpenTelemetry bridge) would let production fleets running many subprocesses get
SLO/latency signals without hand-instrumenting every call site — emitting data the
crate **already computes**.

- **Fit:** adjacent. It's additive and feature-gated, like `tracing`.
- **Caution:** same secret-hygiene rule as `tracing` — emit program name / pid /
  mechanism / durations / exit codes, **never** argv or env values as labels (label
  cardinality *and* secrets).
- **Why later:** no consumer asked; the data is already exposed via results + tracing,
  so the gain is convenience, not capability.

## B. mdBook docs site
*Borrow: ubiquitous in mature crates · Cost: moderate*

`docs/` is a flat set of 10 well-written `.md` guides (cookbook, commands, process
groups, streaming, pipelines, timeouts, supervision, testing, platform support).
That works for git-based reading but isn't a navigable, searchable, hosted site. An
mdBook (`book.toml` + `SUMMARY.md` over the existing files) + a GitHub Pages workflow
would give a polished home alongside docs.rs (API) — without rewriting content.

- **Fit:** docs polish. Low risk (wraps existing files).
- **Why later:** the flat guides are already linked from the README and serve their
  purpose; this is presentation, not substance.

## Assessment

Both are nice-to-haves with no urgency and no consumer pull. (A) becomes worthwhile
if ProcessKit lands in a service running subprocesses at volume; (B) if the doc set
grows enough that flat files get unwieldy or a hosted site is wanted for marketing.
Neither blocks anything.

**Revisit:** (A) on a real production/fleet consumer; (B) when the docs outgrow flat
files or a project site is desired.
