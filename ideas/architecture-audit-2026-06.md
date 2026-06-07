# Decision record: the 2026-06 fresh-eyes architecture audit

> **Status:** decision record / closed. Captured 2026-06-07 after a full
> three-slice audit (core execution path, public API surface,
> reliability/testability) with an explicit pre-1.0 mandate to break API.
> What was ADOPTED shipped in the 0.8.0 window (see CHANGELOG: `Outcome`,
> the verb unification, the `finish_lines` consolidation, the reliability/
> tracing pass, `ProcessRunner::start` + scripted streaming, handler panic
> isolation). This note records what was REJECTED, and why, so the next
> audit doesn't re-derive it.

## Rejected, with reasons

- **Bounded output buffer by default.** Keeping
  `OutputBufferPolicy::unbounded()` as the default is deliberate: a silent
  `DropOldest` default loses data *quietly*, which is worse than an explicit
  OOM risk the caller can see and bound (`output_buffer`, or `output_bytes`
  for raw payloads). The docs carry the lines-not-bytes caveat instead.
- **A formal (private) `Job` trait over the platform backends.** The
  compiler only checks the trait impl for the platform being compiled, so a
  trait adds no cross-platform compile-time safety over today's duck-typed
  `imp::Job` contract — CI's cross-target clippy (Linux ×2, macOS) is what
  actually catches divergence. A trait would only centralize documentation,
  at the cost of trait-object noise in a seam that is deliberately concrete.
- **A runtime state enum for `RunningProcess`.** Every consuming verb takes
  `self` by value (double consumption is a compile error); the two `&mut`
  entry points (`stdout_lines`, `standard_input`) have explicit, tested,
  non-panicking repeat-call handling. A state machine would add panic paths
  to guard doors the borrow checker already locks. (Comment lives on the
  field cluster.)
- **`Reply::pending()` outside the `cancellation` gate.** A pending reply
  without a token parks forever; its entire purpose is exercising
  cancellation. The gate is correct.
- **Cassette (record/replay) coverage of streaming runs.** A streamed
  recording needs line timing and stream shape in the schema — a real
  format expansion. Deferred until a consumer asks; `RecordReplayRunner`
  inherits the `start` default (`Unsupported`), documented.
- **A bare `Command::output`.** `output_string`/`output_bytes` split the
  verb by payload on purpose; collapsing them loses the symmetry and would
  churn the most-used verb in the crate for cosmetics.
- **`on_command`/`default_map` generic per-command hooks** (vcs-toolkit
  streaming-spec R4 + cancellation-spec R3). Typed, narrow defaults
  (`default_timeout`/`default_env`/`default_cancel_on`) beat a stored
  closure for introspection (`Debug`), docs, and cassette transparency.
  Standing offer: revisit if a *third* typed candidate accumulates, as one
  design.
- **Supervisor containment-awareness** (warning when a shared-group runner
  hosts every incarnation). The shared-group semantics are documented on
  `with_runner`; the supervisor deliberately doesn't introspect its runner —
  the seam is the point. Doc concern, not a design defect.
- **Renaming Windows-flavored knobs** (`create_no_window` →
  `suppress_console_window` etc.). Load-bearing, documented names; the
  Windows-first name says exactly what the OS does. (Earlier audits also
  rejected renaming `Job`/`JobRunner`/`program()`.)

## Confirmed sound (don't re-litigate without new arguments)

- The cfg-split duplication of `drive_to_exit_inner` (readability over
  unification — standing decision, restated).
- Honest platform duplication across `sys/{windows,linux,unix,pgroup}.rs`.
- The pgroup `Tracked` probe-before-signal discipline: the recycled-pid
  window is a POSIX limit, not a fixable bug; kernel-handle mechanisms
  (Job/cgroup) are the real fix and already preferred where available.
- `ProcessRunner` as the seam, now covering both run shapes (`output` +
  `start`).
