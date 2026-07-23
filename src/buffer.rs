//! Policy for capping the in-memory backlog of captured output lines.

use std::sync::atomic::{AtomicBool, Ordering};

/// How a child process's standard output or error stream is connected.
///
/// Set per-stream on [`Command`](crate::Command) via
/// [`Command::stdout`](crate::Command::stdout) /
/// [`Command::stderr`](crate::Command::stderr). For a child-owned file
/// descriptor, use `stdout_file*` / `stderr_file*` instead; paths remain on
/// `Command` so this enum can stay `Copy`. The default is
/// [`Piped`](StdioMode::Piped), matching the crate's pre-1.0 behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum StdioMode {
    /// Capture the stream into a pipe (the default). Enables line buffering,
    /// per-line handlers, and all output-retrieval verbs. Required for
    /// [`stdout_lines`](crate::RunningProcess::stdout_lines),
    /// [`output_events`](crate::RunningProcess::output_events), and
    /// `output_string` / `output_bytes` to see any output.
    #[default]
    Piped,
    /// Let the child share the parent's stream: output appears in the
    /// parent's terminal or log. Cannot be captured.
    Inherit,
    /// Redirect the stream to `/dev/null` (or the OS equivalent), suppressing
    /// output entirely without tying up a pipe.
    Null,
}

impl StdioMode {
    /// This mode's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"piped"`, `"inherit"`, `"null"`) that is part of
    /// the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back —
    /// the direction a config file or CLI flag choosing a mode needs.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            StdioMode::Piped => "piped",
            StdioMode::Inherit => "inherit",
            StdioMode::Null => "null",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `StdioMode` — the
    /// direction a config value or CLI flag selecting a mode needs.
    ///
    /// Accepts only the canonical identifier ([`Piped`](Self::Piped) is
    /// `"piped"`, not `"pipe"`), returning `None` for anything else — an honest
    /// miss, never a silent default. Round-trips with [`name`](Self::name):
    /// `StdioMode::from_name(m.name()) == Some(m)` for every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "piped" => Some(StdioMode::Piped),
            "inherit" => Some(StdioMode::Inherit),
            "null" => Some(StdioMode::Null),
            _ => None,
        }
    }
}

/// How the output pump decides where one captured/streamed line ends.
///
/// Set per stream on [`Command`](crate::Command) via
/// [`line_terminator`](crate::Command::line_terminator) (both streams at once)
/// or [`stdout_line_terminator`](crate::Command::stdout_line_terminator) /
/// [`stderr_line_terminator`](crate::Command::stderr_line_terminator). The
/// default is [`Newline`](LineTerminator::Newline) — the crate's pre-1.0
/// behavior, splitting only on `\n`.
///
/// This is one shared definition of "a line" for the whole line-pumped path:
/// what [`stdout_lines`](crate::RunningProcess::stdout_lines) /
/// [`output_events`](crate::RunningProcess::output_events) yield, what the
/// per-line handlers ([`on_stdout_line`](crate::Command::on_stdout_line)) see,
/// what a [`stdout_tee`](crate::Command::stdout_tee) writes (each line followed
/// by a `\n`), and what [`output_string`](crate::Command::output_string) joins.
/// Choosing a mode moves all of them together — there is never a per-sink
/// disagreement about what a line is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum LineTerminator {
    /// Split on `\n` only (the default, and the crate's pre-1.0 behavior).
    ///
    /// A `\r` immediately before a `\n` is a CRLF terminator and is stripped;
    /// every other `\r` — a lone one, or a run of them — is line *content*.
    /// Carriage-return progress output (a bar redrawn in place with `\r`, no
    /// `\n` until the very end) therefore accumulates as one ever-growing line:
    /// nothing is delivered until the final `\n`, and under a byte cap
    /// ([`with_max_bytes`](OutputBufferPolicy::with_max_bytes)) that single
    /// over-cap line is dropped whole. Reach for
    /// [`CarriageReturn`](LineTerminator::CarriageReturn) when you need such
    /// output live.
    #[default]
    Newline,
    /// Treat a bare `\r` as a line terminator *in addition to* `\n` —
    /// "`\r`-aware" mode, for carriage-return progress output.
    ///
    /// Each carriage-return frame is delivered as its own line the instant it is
    /// seen, so `Progress: 50%\rProgress: 100%\n` streams as the frames
    /// `Progress: 50%` then `Progress: 100%` — live, one at a time — instead of
    /// piling up as a single line that surfaces only at EOF. A `\r\n` pair stays
    /// a **single** terminator (it does not emit a spurious empty line between
    /// the `\r` and the `\n`), so ordinary CRLF text reads identically to
    /// [`Newline`](LineTerminator::Newline) mode; only a `\r` *not* followed by a
    /// `\n` splits a frame.
    ///
    /// The framing is the shared one described on the type: the handlers, the
    /// tee, the streaming verbs, and `output_string` all observe the same
    /// per-frame lines. It composes with the byte cap too — a frame whose
    /// content exceeds [`max_bytes`](OutputBufferPolicy::max_bytes) is skipped as
    /// it arrives (never assembled whole), exactly as an over-cap `\n` line is,
    /// so a runaway `\r`-free frame cannot exhaust memory.
    CarriageReturn,
}

impl LineTerminator {
    /// This terminator's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"newline"`, `"carriage_return"`) that is part of
    /// the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back —
    /// the direction a config file or CLI flag choosing a terminator needs.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            LineTerminator::Newline => "newline",
            LineTerminator::CarriageReturn => "carriage_return",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `LineTerminator` —
    /// the direction a config value or CLI flag selecting a terminator needs.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default. Round-trips with
    /// [`name`](Self::name): `LineTerminator::from_name(t.name()) == Some(t)`
    /// for every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "newline" => Some(LineTerminator::Newline),
            "carriage_return" => Some(LineTerminator::CarriageReturn),
            _ => None,
        }
    }
}

/// What to drop when a bounded output buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverflowMode {
    /// Ring-buffer / "tail" semantics: discard the oldest line so the most
    /// recent output survives.
    DropOldest,
    /// "Head" semantics: keep a contiguous **prefix** (head) of the output.
    ///
    /// Each line is retained while it fits the ceiling(s); the first line that
    /// does not fit *seals* the head, so every line after it is dropped too —
    /// even a shorter later line that would still fit on its own. This keeps the
    /// retained buffer a true prefix of the process's output
    /// (`retained == output[..k]` for some `k`), never a set that skipped an
    /// over-budget line and kept a later shorter one. A single line longer than
    /// [`max_bytes`](OutputBufferPolicy::max_bytes) can never fit, so it seals
    /// the head as well (it is dropped whole, like under
    /// [`DropOldest`](OverflowMode::DropOldest)).
    DropNewest,
    /// Fail-loud ceiling: once the buffer is full, the run errors with
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) rather than
    /// silently dropping lines. The pipe is still drained (so the child never
    /// blocks); excess lines are counted but not retained.
    ///
    /// The **line** ceiling ([`max_lines`](OutputBufferPolicy::max_lines)) applies
    /// to line-pumped output: it fires on the line-capturing verbs —
    /// [`output_string`](crate::Command::output_string) (stdout *and* stderr) and
    /// the streaming [`finish`](crate::RunningProcess::finish). On
    /// [`output_bytes`](crate::Command::output_bytes) stdout is captured **raw**
    /// (no line buffer), so the line ceiling covers only its line-pumped
    /// *stderr*. The **byte** ceiling
    /// ([`max_bytes`](OutputBufferPolicy::max_bytes)) *is* honored on the raw
    /// stdout too — `output_bytes` errors (Error mode) or bounds the retained
    /// bytes (drop modes) exactly as the line verbs do — so a byte cap is a real
    /// memory bound there. Because a fail-loud ceiling for `output_bytes` needs a
    /// *byte* cap (its `max_lines` is meaningless for a non-line stream), pair
    /// Error mode with [`with_max_bytes`](OutputBufferPolicy::with_max_bytes) when
    /// capturing raw bytes; a `timeout` additionally bounds wall-time. Discard-only
    /// verbs ([`wait`](crate::RunningProcess::wait), and `profile` under the `stats`
    /// feature) use a retain-nothing sink internally and are not affected.
    ///
    /// Use this when unbounded *line* output is itself a misbehavior — an
    /// untrusted tool flooding its stdout through the line verbs is a
    /// denial-of-service, not a policy choice.
    ///
    /// **Memory, not wall-time.** The ceiling bounds how much output is
    /// *retained*, and the error is surfaced when the consuming verb finishes —
    /// it does **not** tear the child down the instant it trips. A flooding
    /// child with no [`timeout`](crate::Command::timeout) keeps running (its
    /// pipe is still drained, into nothing) until it exits on its own. Pair the
    /// ceiling with a `timeout` when you need to bound wall-time too.
    ///
    /// **Pair it with a cap.** With a `bounded`/`Some(n)` `max_lines` (or a
    /// [`with_max_bytes`](OutputBufferPolicy::with_max_bytes) byte cap) it fires
    /// when the ceiling is reached — reach for it via
    /// [`fail_loud`](OutputBufferPolicy::fail_loud) (which sets the cap for you)
    /// or [`with_overflow`](OutputBufferPolicy::with_overflow). On an
    /// *unbounded* buffer (no `max_lines` and no `max_bytes`) this mode is a
    /// misconfiguration — a fail-loud ceiling with no ceiling — so it is treated
    /// as **zero-tolerance**: the run errors on *any* line-pumped output
    /// (`ErrorReason::OutputTooLarge`), rather than silently retaining everything. Use
    /// `fail_loud(n)` when you want a real cap.
    ///
    /// **Counts the total, not the backlog.** The ceiling fires on the
    /// *cumulative* output the pump has seen — total lines and, for
    /// `max_bytes`, raw bytes read from the pipe before decoding, including
    /// line terminators and invalid UTF-8 bytes — not on how much is currently
    /// buffered. A streaming consumer
    /// ([`stdout_lines`](crate::RunningProcess::stdout_lines)) draining lines as
    /// they arrive frees buffer space but does **not** reset the ceiling, so
    /// `fail_loud(100)` errors on the 101st line whether it is read through
    /// `output_string` or streamed. (`DropOldest`/`DropNewest` still bound the
    /// retained *backlog*, the ring-buffer semantics they imply.)
    Error,
}

impl OverflowMode {
    /// This mode's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"drop_oldest"`, `"drop_newest"`, `"error"`) that
    /// is part of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back —
    /// the direction a config file or CLI flag choosing a mode needs.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            OverflowMode::DropOldest => "drop_oldest",
            OverflowMode::DropNewest => "drop_newest",
            OverflowMode::Error => "error",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into an `OverflowMode` —
    /// the direction a config value or CLI flag selecting a mode needs.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default. Round-trips with
    /// [`name`](Self::name): `OverflowMode::from_name(m.name()) == Some(m)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "drop_oldest" => Some(OverflowMode::DropOldest),
            "drop_newest" => Some(OverflowMode::DropNewest),
            "error" => Some(OverflowMode::Error),
            _ => None,
        }
    }
}

/// Caps how many captured/streamed output lines are retained in memory.
///
/// The pump *always* drains the OS pipe (so the child never blocks on a full
/// buffer); this policy only bounds the in-memory backlog. The line counters
/// ([`RunningProcess::stdout_line_count`](crate::RunningProcess::stdout_line_count))
/// still count every line, so `count > retained` reveals that lines were
/// dropped.
///
/// Two independent ceilings — **lines** ([`max_lines`](Self::max_lines)) and
/// **bytes** ([`max_bytes`](Self::max_bytes)) — either or both of which may be
/// set; the buffer stays within whichever are present. The line cap alone does
/// not bound memory: a line is held whole until its newline arrives, so one
/// enormous newline-free "line" (e.g. `base64 -w0` output) occupies memory in
/// full under a `max_lines`-only policy. Add
/// [`with_max_bytes`](Self::with_max_bytes) to bound the actual retained memory,
/// or use [`output_bytes`](crate::Command::output_bytes) (raw, no line
/// splitting) when the output is not line-structured.
///
/// A byte cap bounds both the retained backlog **and** the in-flight line the
/// pump is still assembling: a line whose own length exceeds the cap can
/// never be retained whole, so the pump drops it as it arrives — a newline-free
/// flood is held to about `max_bytes` plus one read buffer (the cap is rechecked
/// once per read), never the whole flood, so memory cannot be exhausted even
/// before the (never-arriving) terminator. The
/// ceiling measures the **retained text** — the sum of the decoded lines' UTF-8
/// byte lengths, *excluding* the stripped `\n`/`\r` terminators — not the raw
/// bytes on the pipe. One consequence: an over-cap line, since it is never
/// assembled, is also **not** delivered to a per-line handler or
/// [`stdout_tee`](crate::Command::stdout_tee) (set no byte cap if a tee must see
/// arbitrarily long lines verbatim).
///
/// **A flood of empty (or near-empty) lines is still bounded.** Content-byte
/// sums alone would let a stream of nothing but newlines (`yes ''`-style)
/// contribute `0` bytes per line forever, defeating the cap. Every retained
/// line is therefore also charged against a *derived* per-line minimum, so the
/// backlog (under the drop modes) or the cumulative total (under
/// [`OverflowMode::Error`]) cannot exceed roughly `max_bytes` **lines** even
/// when their content is empty — the byte cap remains a real memory bound, and
/// [`OverflowMode::Error`] genuinely trips, for that degenerate case too.
///
/// **Carriage-return progress output** (`curl`, `pip`, `apt` — a bar redrawn with
/// `\r`, no `\n` until the end) is the common shape of this under the default
/// [`LineTerminator::Newline`]: the pump splits on `\n` only, so the whole
/// progress stream is a *single* growing line. It doesn't stream live (nothing is
/// delivered until a `\n` arrives), and under a byte cap the one over-cap line is
/// dropped whole — so a caller watching `stdout_lines` sees no progress. Set
/// [`Command::line_terminator(LineTerminator::CarriageReturn)`](crate::Command::line_terminator)
/// to split on `\r` too: each frame is then delivered live, and the byte cap
/// bounds an individual runaway frame instead of the whole stream (an over-cap
/// frame is skipped as it arrives, never assembled). Alternatively, keep the
/// default and capture the output through a real pipe with **no byte cap**
/// (accepting the memory), or read the raw stream yourself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputBufferPolicy {
    /// Maximum retained lines: `None` is unbounded; `Some(0)` retains nothing;
    /// `Some(n)` keeps at most `n`.
    pub max_lines: Option<usize>,
    /// Maximum retained bytes (sum of the retained lines' UTF-8 lengths): `None`
    /// is unbounded; `Some(n)` keeps the retained backlog at or under `n` bytes.
    /// A single line longer than `n` cannot fit and is dropped whole under
    /// the drop modes (which sets the truncation signal,
    /// [`ProcessResult::truncated`](crate::ProcessResult::truncated)); under
    /// [`OverflowMode::Error`] it trips the fail-loud ceiling.
    pub max_bytes: Option<usize>,
    /// Which line to drop when full.
    pub overflow: OverflowMode,
}

impl OutputBufferPolicy {
    /// Retain everything (the default).
    pub fn unbounded() -> Self {
        Self {
            max_lines: None,
            max_bytes: None,
            overflow: OverflowMode::DropOldest,
        }
    }

    /// Retain at most `max_lines`, dropping the oldest when full.
    pub fn bounded(max_lines: usize) -> Self {
        Self {
            max_lines: Some(max_lines),
            max_bytes: None,
            overflow: OverflowMode::DropOldest,
        }
    }

    /// Retain at most `max_lines` and error when full — a fail-loud ceiling.
    ///
    /// Equivalent to `bounded(max_lines).with_overflow(OverflowMode::Error)`.
    /// The run errors with [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge)
    /// once this limit is reached; excess lines are counted but not retained.
    pub fn fail_loud(max_lines: usize) -> Self {
        Self {
            max_lines: Some(max_lines),
            max_bytes: None,
            overflow: OverflowMode::Error,
        }
    }

    /// Set the retained-byte ceiling, composable with any policy.
    ///
    /// Bounds the actual memory the buffer holds — the sum of the retained
    /// lines' UTF-8 byte lengths — independently of [`max_lines`](Self::max_lines),
    /// and bounds the pump's in-flight assembly buffer too, so even a single
    /// never-terminated line cannot exhaust memory. Use it to cap a stream
    /// whose line *count* is modest but whose lines can be huge (one `base64 -w0`
    /// line evades a line cap but not a byte cap):
    /// `unbounded().with_max_bytes(1 << 20)` is a 1 MiB byte-bounded ring buffer;
    /// `fail_loud(100).with_max_bytes(1 << 20)` errors on whichever ceiling — 100
    /// lines or 1 MiB — is reached first. Under the drop modes a single line
    /// larger than the cap is dropped whole (it cannot fit); under
    /// [`OverflowMode::Error`] it trips the fail-loud ceiling. The accounting is
    /// intentionally asymmetric: drop-mode retention remains bounded by decoded
    /// line-content byte lengths, while the `Error` ceiling counts raw bytes read
    /// from the pipe, including terminators and invalid UTF-8 bytes, before
    /// decoding. Thus a line whose content exactly fits the cap can still trip
    /// the `Error` ceiling when its terminator is read.
    ///
    /// **This affects every sink, not just the buffer:** a line longer than
    /// `max_bytes` is never assembled by the pump, so it is silently skipped
    /// for the capture buffer *and* for
    /// [`Command::stdout_tee`](crate::Command::stdout_tee)/`stderr_tee` and the
    /// per-line handlers ([`on_stdout_line`](crate::Command::on_stdout_line)/
    /// `on_stderr_line`) alike — counted only via the truncation/`dropped()`
    /// signal. If every line must reach those sinks, leave the byte cap unset
    /// (or bound with [`max_lines`](Self::max_lines) — a line cap — instead).
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    /// Set the overflow behavior.
    ///
    /// With a `bounded` cap, [`OverflowMode::Error`] fires when the buffer fills.
    /// On an *unbounded* buffer (`max_lines: None`) `with_overflow(Error)`
    /// is treated as **zero-tolerance**: the run errors on any line-pumped output
    /// (it is a fail-loud ceiling with no ceiling, i.e. a misconfiguration; see
    /// [`OverflowMode::Error`] for which streams the ceiling covers). For a real
    /// cap use [`fail_loud`](Self::fail_loud), which sets both at once.
    #[must_use]
    pub fn with_overflow(mut self, overflow: OverflowMode) -> Self {
        self.overflow = overflow;
        self
    }
}

impl Default for OutputBufferPolicy {
    fn default() -> Self {
        Self::unbounded()
    }
}

/// Append `chunk` to a raw-byte capture buffer (used by
/// [`output_bytes`](crate::Command::output_bytes)) under an
/// [`OutputBufferPolicy`] byte ceiling ([`max_bytes`](OutputBufferPolicy::max_bytes)),
/// updating the overflow and truncation signals. This mirrors the line pump's
/// byte-cap semantics for a stream that has no lines: `max_lines` does not
/// apply here.
///
/// - No cap (`None`): retain everything (the default — byte-for-byte the old
///   behavior).
/// - [`OverflowMode::Error`]: flag overflow and stop retaining past the cap; the
///   caller drains the pipe to EOF and then raises `ErrorReason::OutputTooLarge`.
/// - [`OverflowMode::DropNewest`]: keep the first `cap` bytes (head), flag
///   truncation.
/// - [`OverflowMode::DropOldest`]: keep roughly the last `cap` bytes (tail). The
///   tail is allowed to grow to `2 * cap` before a single compaction reclaims
///   it, so a multi-GB stream costs O(total) memmoves rather than O(total×cap);
///   [`clamp_dropoldest_tail`] trims the final buffer back to exactly `cap`.
pub(crate) fn push_capped_bytes(
    buf: &mut Vec<u8>,
    chunk: &[u8],
    cap: Option<usize>,
    mode: OverflowMode,
    overflowed: &AtomicBool,
    truncated: &AtomicBool,
) {
    let Some(cap) = cap else {
        buf.extend_from_slice(chunk);
        return;
    };
    match mode {
        OverflowMode::Error => {
            if buf.len() + chunk.len() > cap {
                overflowed.store(true, Ordering::Relaxed);
                let room = cap.saturating_sub(buf.len());
                buf.extend_from_slice(&chunk[..room.min(chunk.len())]);
            } else {
                buf.extend_from_slice(chunk);
            }
        }
        OverflowMode::DropNewest => {
            let room = cap.saturating_sub(buf.len());
            let take = room.min(chunk.len());
            if take < chunk.len() {
                truncated.store(true, Ordering::Relaxed);
            }
            buf.extend_from_slice(&chunk[..take]);
        }
        OverflowMode::DropOldest => {
            buf.extend_from_slice(chunk);
            if buf.len() > cap {
                truncated.store(true, Ordering::Relaxed);
                // Amortize: only compact once the tail has grown to ~2×cap, so
                // the reclaimed prefix is large relative to the memmove cost.
                if buf.len() > cap.saturating_mul(2) {
                    let excess = buf.len() - cap;
                    buf.drain(..excess);
                }
            }
        }
    }
}

/// Trim an [`OverflowMode::DropOldest`] raw-byte capture back to exactly the
/// last `cap` bytes. [`push_capped_bytes`] lets the tail run up to `2 * cap`
/// during streaming to amortize compaction; this clamps the retained result.
///
/// `#[mutants::skip]`: `buf.len() > cap` mutated to `>=` is an equivalent
/// mutant at the one point they diverge — `buf.len() == cap` — since `excess`
/// becomes `0` and `buf.drain(..0)` is a no-op, leaving `buf` unchanged either
/// way. (The attribute is function-scoped, so it also silences the `&&`/`||`
/// and `==`/`<` mutants this tiny function's other unit tests already catch;
/// see the tests below for that coverage.)
#[mutants::skip]
pub(crate) fn clamp_dropoldest_tail(buf: &mut Vec<u8>, cap: Option<usize>, mode: OverflowMode) {
    if let (OverflowMode::DropOldest, Some(cap)) = (mode, cap)
        && buf.len() > cap
    {
        let excess = buf.len() - cap;
        buf.drain(..excess);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags() -> (AtomicBool, AtomicBool) {
        (AtomicBool::new(false), AtomicBool::new(false))
    }

    // ---- Stable machine identifiers (name()/from_name()) ----

    const ALL_STDIO_MODES: &[StdioMode] = &[StdioMode::Piped, StdioMode::Inherit, StdioMode::Null];
    const ALL_LINE_TERMINATORS: &[LineTerminator] =
        &[LineTerminator::Newline, LineTerminator::CarriageReturn];
    const ALL_OVERFLOW_MODES: &[OverflowMode] = &[
        OverflowMode::DropOldest,
        OverflowMode::DropNewest,
        OverflowMode::Error,
    ];

    #[test]
    fn stdio_mode_name_pins_each_variant() {
        assert_eq!(StdioMode::Piped.name(), "piped");
        assert_eq!(StdioMode::Inherit.name(), "inherit");
        assert_eq!(StdioMode::Null.name(), "null");
    }

    #[test]
    fn line_terminator_name_pins_each_variant() {
        assert_eq!(LineTerminator::Newline.name(), "newline");
        assert_eq!(LineTerminator::CarriageReturn.name(), "carriage_return");
    }

    #[test]
    fn overflow_mode_name_pins_each_variant() {
        assert_eq!(OverflowMode::DropOldest.name(), "drop_oldest");
        assert_eq!(OverflowMode::DropNewest.name(), "drop_newest");
        assert_eq!(OverflowMode::Error.name(), "error");
    }

    #[test]
    fn buffer_enum_names_round_trip_every_variant() {
        for &m in ALL_STDIO_MODES {
            assert_eq!(StdioMode::from_name(m.name()), Some(m));
        }
        for &t in ALL_LINE_TERMINATORS {
            assert_eq!(LineTerminator::from_name(t.name()), Some(t));
        }
        for &m in ALL_OVERFLOW_MODES {
            assert_eq!(OverflowMode::from_name(m.name()), Some(m));
        }
    }

    #[test]
    fn buffer_enum_from_name_rejects_unknown_without_defaulting() {
        // The canonical `StdioMode::Piped` identifier is `piped`, not `pipe`:
        // the legacy alias is deliberately not accepted as a stable name.
        assert_eq!(StdioMode::from_name("pipe"), None);
        assert_eq!(StdioMode::from_name("Piped"), None);
        assert_eq!(LineTerminator::from_name("lf"), None);
        assert_eq!(LineTerminator::from_name("cr"), None);
        assert_eq!(OverflowMode::from_name("drop-oldest"), None);
        assert_eq!(OverflowMode::from_name(""), None);
    }

    // ---- OverflowMode::Error: `buf.len() + chunk.len() > cap`, `room = cap.saturating_sub(buf.len())` ----

    #[test]
    fn error_mode_under_cap_does_not_overflow() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcd",
            Some(5),
            OverflowMode::Error,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcd", "cap-1 input must fit whole");
        assert!(!overflowed.load(Ordering::Relaxed));
        assert!(!truncated.load(Ordering::Relaxed));
    }

    #[test]
    fn error_mode_exactly_at_cap_does_not_overflow() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcde",
            Some(5),
            OverflowMode::Error,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcde");
        assert!(
            !overflowed.load(Ordering::Relaxed),
            "buf.len() + chunk.len() == cap must not overflow"
        );
        assert!(!truncated.load(Ordering::Relaxed));
    }

    #[test]
    fn error_mode_one_over_cap_overflows_and_writes_partial_room() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcdef",
            Some(5),
            OverflowMode::Error,
            &overflowed,
            &truncated,
        );
        assert_eq!(
            buf, b"abcde",
            "only room = cap - buf.len() bytes are retained"
        );
        assert!(overflowed.load(Ordering::Relaxed));
    }

    #[test]
    fn error_mode_partial_room_when_buffer_already_holds_some_bytes() {
        let (overflowed, truncated) = flags();
        let mut buf = b"ab".to_vec();
        push_capped_bytes(
            &mut buf,
            b"cdefgh",
            Some(5),
            OverflowMode::Error,
            &overflowed,
            &truncated,
        );
        // room = cap.saturating_sub(buf.len()) = 5 - 2 = 3
        assert_eq!(buf, b"abcde");
        assert!(overflowed.load(Ordering::Relaxed));
    }

    #[test]
    fn error_mode_zero_room_appends_nothing_further() {
        let (overflowed, truncated) = flags();
        let mut buf = b"abcde".to_vec();
        push_capped_bytes(
            &mut buf,
            b"f",
            Some(5),
            OverflowMode::Error,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcde", "room is 0 once already at cap");
        assert!(overflowed.load(Ordering::Relaxed));
    }

    // ---- OverflowMode::DropNewest: `take < chunk.len()` drives `truncated` ----

    #[test]
    fn drop_newest_under_cap_does_not_truncate() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcd",
            Some(5),
            OverflowMode::DropNewest,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcd");
        assert!(!truncated.load(Ordering::Relaxed));
        assert!(!overflowed.load(Ordering::Relaxed));
    }

    #[test]
    fn drop_newest_exact_fit_does_not_truncate() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcde",
            Some(5),
            OverflowMode::DropNewest,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcde");
        assert!(
            !truncated.load(Ordering::Relaxed),
            "take == chunk.len() must not truncate"
        );
    }

    #[test]
    fn drop_newest_one_over_cap_truncates_and_keeps_head() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcdef",
            Some(5),
            OverflowMode::DropNewest,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcde");
        assert!(
            truncated.load(Ordering::Relaxed),
            "take < chunk.len() must truncate"
        );
    }

    #[test]
    fn drop_newest_zero_room_still_truncates() {
        let (overflowed, truncated) = flags();
        let mut buf = b"abcde".to_vec();
        push_capped_bytes(
            &mut buf,
            b"f",
            Some(5),
            OverflowMode::DropNewest,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcde");
        assert!(truncated.load(Ordering::Relaxed));
    }

    // ---- OverflowMode::DropOldest: `buf.len() > cap`, amortized compaction at
    // `buf.len() > cap.saturating_mul(2)`, exact `excess = buf.len() - cap` ----

    #[test]
    fn drop_oldest_under_cap_does_not_truncate() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcd",
            Some(5),
            OverflowMode::DropOldest,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcd");
        assert!(!truncated.load(Ordering::Relaxed));
    }

    #[test]
    fn drop_oldest_exactly_at_cap_does_not_truncate() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcde",
            Some(5),
            OverflowMode::DropOldest,
            &overflowed,
            &truncated,
        );
        assert_eq!(buf, b"abcde");
        assert!(
            !truncated.load(Ordering::Relaxed),
            "buf.len() == cap must not truncate"
        );
    }

    #[test]
    fn drop_oldest_one_over_cap_truncates_without_compacting() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcdef",
            Some(5),
            OverflowMode::DropOldest,
            &overflowed,
            &truncated,
        );
        assert_eq!(
            buf, b"abcdef",
            "below the 2*cap compaction threshold, the tail is left as-is"
        );
        assert!(truncated.load(Ordering::Relaxed));
    }

    #[test]
    fn drop_oldest_exactly_at_double_cap_does_not_compact() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"0123456789",
            Some(5),
            OverflowMode::DropOldest,
            &overflowed,
            &truncated,
        );
        assert_eq!(
            buf, b"0123456789",
            "buf.len() == 2*cap must not trigger compaction"
        );
        assert!(truncated.load(Ordering::Relaxed));
    }

    #[test]
    fn drop_oldest_one_over_double_cap_compacts_to_exact_excess() {
        let (overflowed, truncated) = flags();
        let mut buf = Vec::new();
        push_capped_bytes(
            &mut buf,
            b"abcdefghijk",
            Some(5),
            OverflowMode::DropOldest,
            &overflowed,
            &truncated,
        );
        // excess = 11 - 5 = 6; the first 6 bytes are drained, leaving the last 5.
        assert_eq!(buf, b"ghijk");
        assert!(truncated.load(Ordering::Relaxed));
    }

    // ---- clamp_dropoldest_tail: `buf.len() > cap`, exact `excess = buf.len() - cap` ----

    #[test]
    fn clamp_dropoldest_tail_under_cap_is_unchanged() {
        let mut buf = b"abcd".to_vec();
        clamp_dropoldest_tail(&mut buf, Some(5), OverflowMode::DropOldest);
        assert_eq!(buf, b"abcd");
    }

    #[test]
    fn clamp_dropoldest_tail_exactly_at_cap_is_unchanged() {
        let mut buf = b"abcde".to_vec();
        clamp_dropoldest_tail(&mut buf, Some(5), OverflowMode::DropOldest);
        assert_eq!(buf, b"abcde", "buf.len() == cap must not be clamped");
    }

    #[test]
    fn clamp_dropoldest_tail_one_over_cap_drops_exact_excess() {
        let mut buf = b"abcdef".to_vec();
        clamp_dropoldest_tail(&mut buf, Some(5), OverflowMode::DropOldest);
        assert_eq!(
            buf, b"bcdef",
            "excess = 6 - 5 = 1 byte dropped from the front"
        );
    }

    #[test]
    fn clamp_dropoldest_tail_at_double_cap_drops_exact_excess() {
        let mut buf = b"0123456789".to_vec();
        clamp_dropoldest_tail(&mut buf, Some(5), OverflowMode::DropOldest);
        // excess = 10 - 5 = 5
        assert_eq!(buf, b"56789");
    }

    #[test]
    fn clamp_dropoldest_tail_well_over_cap_drops_exact_excess() {
        let mut buf = b"abcdefghijk".to_vec();
        clamp_dropoldest_tail(&mut buf, Some(5), OverflowMode::DropOldest);
        // excess = 11 - 5 = 6
        assert_eq!(buf, b"ghijk");
    }

    #[test]
    fn clamp_dropoldest_tail_ignores_other_modes() {
        let mut buf = b"abcdefghijk".to_vec();
        clamp_dropoldest_tail(&mut buf, Some(5), OverflowMode::DropNewest);
        assert_eq!(buf, b"abcdefghijk", "only DropOldest is clamped");
    }

    #[test]
    fn clamp_dropoldest_tail_no_cap_is_a_no_op() {
        let mut buf = b"abcdefghijk".to_vec();
        clamp_dropoldest_tail(&mut buf, None, OverflowMode::DropOldest);
        assert_eq!(buf, b"abcdefghijk");
    }

    mod proptests {
        use super::*;
        use crate::pump::SharedLines;
        use proptest::prelude::*;

        #[derive(Default)]
        struct ExpectedLines {
            lines: Vec<String>,
            count: usize,
            seen_bytes: usize,
            dropped: usize,
            overflowed: bool,
            /// `DropNewest` only: the head has been sealed by a first drop, so
            /// every later line is dropped too (keeps the retained set a
            /// contiguous prefix). Mirrors `Inner::dropnewest_sealed`.
            sealed: bool,
        }

        impl ExpectedLines {
            fn push(&mut self, line: String, policy: OutputBufferPolicy) {
                self.count += 1;
                self.seen_bytes += line.len();
                match policy.overflow {
                    OverflowMode::Error => {
                        let over = match (policy.max_lines, policy.max_bytes) {
                            (None, None) => true,
                            (max_lines, max_bytes) => {
                                max_lines.is_some_and(|cap| self.count > cap)
                                    || max_bytes.is_some_and(|cap| {
                                        self.seen_bytes > cap || self.count > cap
                                    })
                            }
                        };
                        if over {
                            self.overflowed = true;
                            self.dropped += 1;
                        } else {
                            self.lines.push(line);
                        }
                    }
                    OverflowMode::DropOldest => {
                        self.lines.push(line);
                        let mut dropped = false;
                        while policy.max_lines.is_some_and(|cap| self.lines.len() > cap)
                            || policy.max_bytes.is_some_and(|cap| {
                                self.lines.iter().map(String::len).sum::<usize>() > cap
                                    || self.lines.len() > cap
                            })
                        {
                            self.lines.remove(0);
                            dropped = true;
                        }
                        if dropped {
                            self.dropped += 1;
                        }
                    }
                    OverflowMode::DropNewest => {
                        let bytes: usize = self.lines.iter().map(String::len).sum();
                        let fits = policy.max_lines.is_none_or(|cap| self.lines.len() < cap)
                            && policy.max_bytes.is_none_or(|cap| {
                                bytes + line.len() <= cap && self.lines.len() < cap
                            });
                        // Seal the head on the first drop, matching the production
                        // contiguous-prefix invariant: once any line is dropped, no
                        // later (possibly shorter) line may be retained.
                        if !self.sealed && fits {
                            self.lines.push(line);
                        } else {
                            self.sealed = true;
                            self.dropped += 1;
                        }
                    }
                }
            }
        }

        fn arb_line() -> impl Strategy<Value = String> {
            prop::collection::vec(any::<char>(), 0..16)
                .prop_map(|chars| chars.into_iter().collect())
        }

        fn arb_cap(limit: usize) -> impl Strategy<Value = Option<usize>> {
            prop_oneof![Just(None), (0usize..=limit).prop_map(Some)]
        }

        fn arb_overflow_mode() -> impl Strategy<Value = OverflowMode> {
            prop_oneof![
                Just(OverflowMode::DropOldest),
                Just(OverflowMode::DropNewest),
                Just(OverflowMode::Error),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn line_buffer_policy_matches_its_model(
                input in prop::collection::vec(arb_line(), 0..24),
                max_lines in arb_cap(12), max_bytes in arb_cap(96), overflow in arb_overflow_mode(),
            ) {
                let policy = OutputBufferPolicy { max_lines, max_bytes, overflow };
                let sink = SharedLines::new(&policy);
                let mut expected = ExpectedLines::default();
                for line in input {
                    expected.push(line.clone(), policy);
                    // `SharedLines::push` is below the pipe-read seam in this
                    // model test; simulate the raw bytes that pump_lines_core
                    // accounts for before it calls push.
                    sink.add_seen_bytes(line.len());
                    sink.push(line);
                    prop_assert_eq!(sink.count(), expected.count);
                    prop_assert_eq!(sink.seen_bytes(), expected.seen_bytes);
                    prop_assert_eq!(sink.dropped(), expected.dropped);
                    prop_assert_eq!(sink.overflowed(), expected.overflowed);
                }
                let retained = sink.drain();
                let retained_bytes: usize = retained.iter().map(String::len).sum();
                if let Some(cap) = policy.max_lines { prop_assert!(retained.len() <= cap); }
                if let Some(cap) = policy.max_bytes {
                    prop_assert!(retained_bytes <= cap);
                    prop_assert!(retained.len() <= cap);
                }
                prop_assert_eq!(retained, expected.lines);
                prop_assert_eq!(sink.dropped() > 0, expected.dropped > 0);
            }

            #[test]
            fn raw_byte_buffer_policy_matches_its_model(
                chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..80), 0..24),
                max_lines in arb_cap(12), max_bytes in arb_cap(96), overflow in arb_overflow_mode(),
            ) {
                let policy = OutputBufferPolicy { max_lines, max_bytes, overflow };
                let input: Vec<u8> = chunks.iter().flatten().copied().collect();
                let (overflowed, truncated) = (AtomicBool::new(false), AtomicBool::new(false));
                let mut actual = Vec::new();
                for chunk in &chunks {
                    let was_overflowed = overflowed.load(Ordering::Relaxed);
                    let was_truncated = truncated.load(Ordering::Relaxed);
                    push_capped_bytes(&mut actual, chunk, policy.max_bytes, policy.overflow, &overflowed, &truncated);
                    prop_assert!(!was_overflowed || overflowed.load(Ordering::Relaxed));
                    prop_assert!(!was_truncated || truncated.load(Ordering::Relaxed));
                }
                clamp_dropoldest_tail(&mut actual, policy.max_bytes, policy.overflow);
                let (expected, expected_overflowed, expected_truncated) = match policy.max_bytes {
                    None => (input, false, false),
                    Some(cap) => match policy.overflow {
                        OverflowMode::DropOldest => {
                            let start = input.len().saturating_sub(cap);
                            (input[start..].to_vec(), false, input.len() > cap)
                        }
                        OverflowMode::DropNewest => (input[..input.len().min(cap)].to_vec(), false, input.len() > cap),
                        OverflowMode::Error => (input[..input.len().min(cap)].to_vec(), input.len() > cap, false),
                    },
                };
                if let Some(cap) = policy.max_bytes { prop_assert!(actual.len() <= cap); }
                prop_assert_eq!(actual, expected);
                prop_assert_eq!(overflowed.load(Ordering::Relaxed), expected_overflowed);
                prop_assert_eq!(truncated.load(Ordering::Relaxed), expected_truncated);
            }
        }
    }
}
