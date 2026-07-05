# next: output-handling surface — inherit/null, tee, merged order, event stream

> **Status:** open idea (next — reconsider first). From the 2026-06-09
> cross-language sweep. Grouped because all four touch the pump/capture core and
> share one design question: **capture is currently load-bearing** — both stdout and
> stderr are always piped and line-pumped. These features relax or extend that, so
> they want one coherent design, not four bolt-ons. That coupling is why they're
> `next-` rather than `today`.

## Candidates

### A. `Stdio` inheritance modes (inherit / null / piped)
*Borrow: `std`/`tokio` `Stdio::inherit`, execa, CliWrap, MedallionShell · Cost: moderate*

Today every run pipes stdout/stderr for capture. There's no "let the child write
straight to my terminal" (interactive children — editors, `less`, prompts) or "throw
the output away" (don't pay pump overhead when you don't want the bytes). **Design
tension:** containment + the line pump assume piped streams; inherit/null change the
`RunningProcess` shape (no `stdout_lines`, no captured `ProcessResult` text). Likely a
per-stream `StdioMode` on `Command` with the streaming/capture verbs gated on `piped`.

### B. Output tee — capture *and* inherit simultaneously
*Borrow: execa `['pipe','inherit']`, mixlib `live_stream`, go-cmd · Cost: moderate*

Long-running build/deploy tools want **live progress on the terminal *and* a captured
transcript**. `on_stdout_line` can fake it but forces the caller to re-implement
writing+flushing+coloring. A first-class "tee to writer X while capturing" closes it.

### C. Merged stdout+stderr in arrival order
*Borrow: execa `all`, CliWrap `Merge`, `2>&1` · Cost: moderate*

`ProcessResult::combined()` concatenates stdout-then-stderr — it **scrambles real
interleaving**. Tools that narrate progress across both streams need true arrival
order. Options: a single merged pipe at spawn (loses per-stream attribution) or a
sequenced shared sink across the two pumps (keeps a `which-stream` tag). The latter
fits the existing two-pump model better.

### D. Unified event stream (started → stdout line → stderr line → exited)
*Borrow: CliWrap `ListenAsync`, go-cmd `Status` channel · Cost: moderate*

`stdout_lines()` is stdout-only; stderr is handler-only; there's no single
`select!`-able async stream of typed lifecycle+output events. Natural superset of the
current streaming API; ideal for TUIs/dashboards. Subsumes much of (C) if the event
carries a stream tag and arrives in order.

### E. `OverflowMode::Error` — a fail-loud capture ceiling (fold-in)
*Borrow: GNU `head -c` ceiling semantics; ties to the existing `OverflowMode` · Cost: trivial*

`OverflowMode` today has only `DropOldest`/`DropNewest` (`src/buffer.rs`) — both *silently
lose* lines, surfaced after the fact via `truncated()`. For an **untrusted** child whose
unbounded output is itself a DoS, a consumer wants to **abort at a ceiling**, not drop. A
third `Error` variant (→ a typed `Error::OutputTooLarge`, say) is a different, legitimate
point on the same axis the audit settled (it kept the unbounded default + drop modes;
erroring is the sandbox case). Confirmed missing on 2026-06-10, and now **non-breaking** to
add (`OverflowMode` became `#[non_exhaustive]` in 0.9.1). Fits the `limits`/sandbox theme.
Folded into the design pass because it touches the same buffer/pump core.

> **Decode policy — checked, not a gap.** A sibling idea (typed invalid-UTF-8 handling on
> the string verbs) was assessed and dropped: the pump already decodes **lossily** via
> `encoding_rs` (`src/pump.rs` `encoding.decode`), so `output_string` never hard-errors on
> bad bytes (test: `invalid bytes decode to the replacement char`). A hard-error mode has
> no asking consumer and inverts the safe default — left out. Documented here so it isn't
> re-proposed.

## Assessment

High real-world value (these are among the most-requested subprocess ergonomics),
but the right move is **one design pass** that decides the `StdioMode` model first
(A), then layers tee (B) and an ordered event stream (D, which subsumes C's tagging),
and adds the cheap (E) ceiling. Rushing any one of them risks a second incompatible knob
later. Pre-1.0, we can reshape `RunningProcess`/`ProcessResult` freely — so do it
deliberately, together.

**Revisit:** **now** — the roadmap drained, so this is promoted to ROADMAP item 2.
