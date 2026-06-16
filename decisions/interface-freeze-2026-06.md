# Decision record: the pre-release interface-freeze keep-as-is calls (2026-06)

> **Status:** decision record / closed. Captured 2026-06-16 from the pre-release
> interface/structure freeze audit
> ([`../reviews/2026-06-16-interface-freeze-audit.md`](../reviews/2026-06-16-interface-freeze-audit.md)).
> The audit's two breaking-shape fixes shipped in the companion change
> (`OutputLine.text` accessor-fronted; `Error::ResourceLimit { message }`). This
> note records the surface decisions the audit examined and **deliberately left
> as-is**, so a future maintainer doesn't reverse or re-litigate them after the
> surface is frozen, or add one of the rejected footguns under "ergonomics"
> pressure. Sibling: [`pre-1.0-api-review.md`](pre-1.0-api-review.md),
> [`architecture-audit-2026-06.md`](architecture-audit-2026-06.md).

## Conversions deliberately NOT added

- **No `From<&str>` / `From<String>` for `Command`.** This is the highest-stakes
  one. The crate is deliberately shell-free (see
  [`wont-do-2026-06.md`](wont-do-2026-06.md): built-in shell mode rejected). A
  `From<&str> for Command` reads, to many users, as "parse this command line" —
  the exact quoting/injection-shaped footgun the crate refuses — or it silently
  means "program with no args," which is ambiguous. `Command::new("git")` is the
  one unambiguous entry. **Never add it.** (Adding it later is technically
  non-breaking, which is precisely why it must be a recorded *won't-do*: it's the
  conversion a well-meaning maintainer is most likely to add, and the most
  dangerous.)
- **No `From<String>` / `From<&str>` / `From<Vec<u8>>` for `Stdin`.** The named
  constructors (`from_string` / `from_bytes` / `from_file` / `from_reader` /
  `from_lines` / `from_iter_lines` / `empty`) disambiguate "is this a path or
  content?" at the call site; a bare `.into()` would not.
- **No `From<i32>` for `Signal`.** `Signal::Other(i32)` is the explicit escape
  hatch; a `From<i32>` would invite magic-number `9.into()` over `Signal::Kill`.
- **No blanket `From<std::io::Error>` for `Error`** (already recorded as D13) —
  keeps the `Io` classifiers from seeing foreign errors routed through `?`.

## Signature / type shapes deliberately kept

- **Batch `concurrency: usize` (clamped to ≥ 1), not `NonZeroUsize`.** The
  clamp-to-1 is friendly ergonomics for an end-user batch helper (`output_all` /
  `output_all_bytes`); `NonZeroUsize` would push a `.try_into().unwrap()` onto
  every call for a knob whose only invalid value has an obvious sane meaning. The
  clamp is documented. (Originally raised as round-3 S-7; re-confirmed at freeze.)
- **`Pipeline::parse` / `try_parse` keep the looser closure bounds** (no
  `F: Send`, no `T: Send`), unlike the `Command` / `CliClient` / `ProcessRunnerExt`
  versions. The pipeline runs the closure *inline* (after `checked().await`), not
  across a `tokio::spawn` / boxed-future boundary, so it genuinely needs neither
  bound and accepts strictly more closures. Crucially this is the *freeze-safe*
  side: loosening a bound later is non-breaking, tightening is breaking — so the
  permissive state is the right one to lock.
- **Keep `Copy` on the small scalar value types** (`Outcome`,
  `OutputBufferPolicy`, `ResourceLimits`, `ProcessGroupStats`, `RunProfile`, and
  the unit-ish enums). They are all-scalar; `Copy` is a real ergonomic and the
  risk of a future non-`Copy` field is low and speculative. Not dropping it
  defensively. (If one genuinely must grow a `String`/`Vec` field later, that's a
  rare, deliberate minor-version event.)
- **No `#[must_use]` on `Outcome`.** Unlike `ProcessResult` / `Finished` (which
  carry captured output that's usually a bug to drop), waiting for a process and
  *not* caring how it ended (`let _ = proc.wait().await?;` — "just block until
  done") is a legitimate pattern, so `must_use` here would false-positive. (It is
  additive anyway, so this can be revisited without a break.)

## Sealing / leak posture re-confirmed

- **`ProcessRunner` stays open** (user-implementable) — it is the test-double seam
  (`ScriptedRunner` / `MockRunner` / `RecordingRunner` and downstream fakes). The
  discipline this commits us to: post-1.0, only *defaulted* trait methods may be
  added, never a new *required* one (the crate already follows this — `start`
  shipped with a default).
- **`IntoCommand` stays sealed**; **`StdoutText`** (the `ensure_success` bound) is
  a sealed impl-detail (`#[doc(hidden)]` in a private module, unnameable
  downstream — only `String` / `Vec<u8>` impl it).
- **The tokio / futures-core trait leaks in bounds** (`AsyncRead` / `AsyncWrite`
  on the stdin/tee seams, `futures_core::Stream` on the stream types and
  `Stdin::from_lines`) are intentional and covered by the same reasoning as the
  recorded `tokio::process` bridge and the `encoding_rs` / `tokio_stream` /
  `tokio_util` re-exports ([`pre-1.0-api-review.md`](pre-1.0-api-review.md) §3):
  the crate is tokio-only by design, so leaking tokio's currency in a power-user
  seam costs nothing the runtime choice hasn't already spent.

## Verified sound, no change (so a future reviewer needn't re-check)

- `#[non_exhaustive]` covers every public enum and every growable struct.
- `group.rs` feature gating is correct: the `limits` field + resource builders are
  `#[cfg(feature = "limits")]`; stats methods are `stats`-gated; tree-control
  methods are `process-control`-gated. (The all-features `public-api.txt` cannot
  show gates, so this was verified by reading the source.)
- Features are additive; default set is `process-control` only; the `mock`
  expectation surface is semver-exempt and feature-gated. MSRV 1.88 / edition 2024.

## Genuinely deferred (additive — safe to add after 1.0, not freeze-critical)

`Clone`/`PartialEq` on `SupervisionOutcome`; builder methods on `ResourceLimits`;
more crate-root free fns (`output_bytes` / `run_unit`); an `Outcome::signalled() ->
bool` companion. All purely additive — no freeze pressure.
