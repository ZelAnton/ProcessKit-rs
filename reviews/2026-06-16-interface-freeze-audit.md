# 2026-06-16 pre-release interface/structure freeze audit

A focused **interface-freeze** pass (distinct from the prior bug-focused rounds):
the crate is about to cut its 1.0-track release; with no users yet, interface
changes are free, so the goal is to make every interface decision **now** and
minimize post-release breaking changes. Four readers, one axis each (verb/method
surface; type design & future-proofing; modules/re-exports/features;
construction/ergonomics/conversions), each reading `public-api.txt`, the prior
reports, and the decision records.

**Headline:** the surface is **substantially freeze-ready** — six prior rounds
plus two decision records settled the big calls (the `output_string` rename,
verb parity, `#[non_exhaustive]` coverage, sealed `IntoCommand`, accessor-fronted
`ProcessResult`/`Stdin`, the dependency-leak re-exports). The audit found **two
genuine breaking-to-change-later type-shape fixes** worth making now, and a set of
**deliberate keep-as-is calls** worth *recording* so they aren't re-litigated or
reversed under pressure post-freeze. The feature gating in `group.rs` was checked
and is correct.

---

## Freeze-critical fixes (breaking to change after 1.0 — do now)

### A · `OutputLine.text` is a public `String` field, locking the representation
- `src/running/stream.rs:629-632`: `#[non_exhaustive] pub struct OutputLine { pub text: String }`.
- `OutputLine` was introduced (B1) **explicitly to grow per-line metadata**. `#[non_exhaustive]` already lets new fields be *added* non-breakingly — but a `pub text: String` field locks `text`'s **type** and **publicness** forever: it can never become a `Cow`/lazily-decoded/owned-vs-borrowed accessor, and an external owner can't even cleanly move the `String` out (non_exhaustive blocks the partial move). Every other line/payload type in the crate is accessor-fronted (`ProcessResult`, `Stdin`, and `OutputEvent::text()` itself).
- **Fix:** make `text` private; add `pub fn text(&self) -> &str` and
  `pub fn into_text(self) -> String` (the latter restores owned access the public
  field couldn't give externally). Update `OutputEvent::text()` and the in-crate
  construction sites; update the doc. Breaking-shape, so pre-freeze only.

### B · `Error::ResourceLimit(String)` is a tuple variant that can't gain structure
- `src/error.rs:231`. Every other rich `Error` variant uses **named fields**
  (individually evolvable under `#[non_exhaustive]`); `ResourceLimit(String)` and
  `Io(io::Error)` are the two tuple variants. `Io` is idiomatic, but
  `ResourceLimit(String)` can never gain a `limit_kind`/`value`/structured detail
  without a breaking variant-shape change.
- **Fix:** convert to `ResourceLimit { message: String }` (parity with the
  sibling struct-variants; future-proofs the shape). ~12 construction/match sites
  (`error.rs`, `group.rs`). Breaking-shape, so pre-freeze only.

---

## Deliberate keep-as-is calls (record so they aren't reversed post-freeze)

These were each examined this round and confirmed correct; recording them in a
decision note prevents a future maintainer from "helpfully" changing them (or
re-litigating) after the surface is frozen:

- **Never add `From<&str>` / `From<String>` for `Command`.** The crate is
  deliberately shell-free; a `From<&str>` reads as "parse a command line" — the
  exact injection-shaped footgun the crate refuses. `Command::new("git")` stays
  the unambiguous entry. (Highest-value record — it's the conversion a user *would*
  expect and the most dangerous to add later under ergonomics pressure.)
- **Keep `Stdin::from_*` named constructors; no `From<String>`/`From<Vec<u8>>`.**
  The named forms disambiguate "path vs content" at the call site.
- **Keep `Signal::Other(i32)`; no `From<i32> for Signal`.** The explicit escape
  hatch beats magic-number `.into()`.
- **Batch `concurrency: usize` (clamped ≥1), not `NonZeroUsize`** — already decided
  (round 3 S-7); the clamp is friendly ergonomics for a batch helper. Re-confirmed.
- **`Pipeline::parse`/`try_parse` keep the looser bounds (no `Send`/`T: Send`)** —
  deliberate (round 4): the pipeline runs the closure inline (not across a
  `tokio::spawn`/boxed-future boundary), so it accepts strictly more closures. The
  looser bound is the *more permissive* side, and loosening-is-not-breaking while
  tightening-is — so the current state is the freeze-safe one.
- **Keep `Copy` on the small scalar config/result enums & structs** (`Outcome`,
  `OutputBufferPolicy`, `ResourceLimits`, …) — they are all-scalar value types;
  `Copy` is a genuine ergonomic and the risk of a future non-`Copy` field is low
  and speculative. Not dropping it defensively.
- **`StdoutText` (the `ensure_success` bound) is a sealed impl-detail** —
  `#[doc(hidden)]` in a private module, unnameable downstream; effectively sealed.
- **The tokio/futures-core trait leaks in bounds** (`AsyncRead`/`AsyncWrite`/
  `Stream`) are intentional and covered by the same "tokio-only, leaking tokio's
  currency is fine" reasoning as the recorded `tokio::process` bridge.

## Checked and confirmed sound (no action)
- `group.rs` feature gating: the `limits` field + resource builders are
  `#[cfg(feature = "limits")]`; stats methods `stats`-gated; tree-control methods
  `process-control`-gated. Correct — verified directly (the all-features
  `public-api.txt` can't show gates).
- `#[non_exhaustive]` coverage is complete across every public enum and growable
  struct. Sealing of `IntoCommand` correct; `ProcessRunner` intentionally open
  (the test-double seam). Re-export canonical paths clean; `mock` internals
  semver-exempt and gated. MSRV 1.88 / edition 2024 consistent.

## Deferred / additive (safe after 1.0 — not freeze-critical)
- `#[must_use]` on `Outcome` — declined: `wait()`-then-ignore-outcome is a
  legitimate pattern, so it would false-positive; and it's additive anyway.
- `Clone`/`PartialEq` on `SupervisionOutcome`; `ResourceLimits` builder methods;
  more free fns (`output_bytes`/`run_unit` at root) — all purely additive.

---

## Execution plan

Each stage: implement → review-loop (≥2 independent passes, fix serious, repeat
until clean) → full gate → push → next.

- **Stage 1 — Freeze-proof public type shapes:** A (`OutputLine.text` → private +
  `text()`/`into_text()` accessors) and B (`Error::ResourceLimit(String)` →
  `{ message: String }`). Both breaking-shape; regenerate `public-api.txt`;
  CHANGELOG (Breaking, pre-1.0).
- **Stage 2 — Record the freeze decisions:** a `decisions/` note capturing the
  deliberate keep-as-is calls above, so the frozen surface isn't reversed or
  re-litigated post-release. Doc-only.

Full gate (per stage): `cargo fmt --check`; clippy `--all-targets` ×
{default, `--no-default-features`, `--all-features`} `-D warnings`;
`RUSTDOCFLAGS=-D warnings cargo doc --no-deps --all-features`;
`cargo test --all-features`; cross-compile `cargo check --all-targets
--all-features --target {x86_64-unknown-linux-gnu, aarch64-apple-darwin}`;
`cargo public-api --simplified --all-features | diff public-api.txt -`; plus
`cargo hack --feature-powerset --depth 2 clippy` on the final pass (confirms the
gating).

After all stages + final push: doc-conformance check, then an overall review
(≥4 passes), then final push and wait for CI.
