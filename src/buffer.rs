//! Policy for capping the in-memory backlog of captured output lines.

/// How a child process's standard output or error stream is connected.
///
/// Set per-stream on [`Command`](crate::Command) via
/// [`Command::stdout`](crate::Command::stdout) /
/// [`Command::stderr`](crate::Command::stderr). The default is
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

/// What to drop when a bounded output buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverflowMode {
    /// Ring-buffer / "tail" semantics: discard the oldest line so the most
    /// recent output survives.
    DropOldest,
    /// "Head" semantics: keep what is already buffered and discard new lines.
    DropNewest,
    /// Fail-loud ceiling: once the buffer is full, the run errors with
    /// [`Error::OutputTooLarge`](crate::Error::OutputTooLarge) rather than
    /// silently dropping lines. The pipe is still drained (so the child never
    /// blocks); excess lines are counted but not retained.
    ///
    /// The ceiling applies to **line-pumped** output. It fires on the
    /// line-capturing verbs — [`output_string`](crate::Command::output_string)
    /// (stdout *and* stderr) and the streaming
    /// [`finish`](crate::RunningProcess::finish). On
    /// [`output_bytes`](crate::Command::output_bytes) stdout is captured **raw**
    /// (no line buffer), so the cap applies only to its line-pumped *stderr* —
    /// the raw stdout is never line-capped (bound a flooding child with a
    /// [`timeout`](crate::Command::timeout) instead). Discard-only verbs
    /// ([`wait`](crate::RunningProcess::wait), and `profile` under the `stats`
    /// feature) use a retain-nothing sink internally and are not affected.
    ///
    /// Use this when unbounded *line* output is itself a misbehavior — an
    /// untrusted tool flooding its stdout through the line verbs is a
    /// denial-of-service, not a policy choice.
    ///
    /// **Memory, not wall-time (E5).** The ceiling bounds how much output is
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
    /// or [`with_overflow`](OutputBufferPolicy::with_overflow). **D9c:** on an
    /// *unbounded* buffer (no `max_lines` and no `max_bytes`) this mode is a
    /// misconfiguration — a fail-loud ceiling with no ceiling — so it is treated
    /// as **zero-tolerance**: the run errors on *any* line-pumped output
    /// (`Error::OutputTooLarge`), rather than silently retaining everything.
    /// (Previously it was an inert no-op.) Use `fail_loud(n)` when you want a
    /// real cap.
    ///
    /// **Counts the total, not the backlog (E4).** The ceiling fires on the
    /// *cumulative* output the pump has seen — total lines and total bytes — not
    /// on how much is currently buffered. A streaming consumer
    /// ([`stdout_lines`](crate::RunningProcess::stdout_lines)) draining lines as
    /// they arrive frees buffer space but does **not** reset the ceiling, so
    /// `fail_loud(100)` errors on the 101st line whether it is read through
    /// `output_string` or streamed. (`DropOldest`/`DropNewest` still bound the
    /// retained *backlog*, the ring-buffer semantics they imply.)
    Error,
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
/// full under a `max_lines`-only policy (D8). Add
/// [`with_max_bytes`](Self::with_max_bytes) to bound the actual retained memory,
/// or use [`output_bytes`](crate::Command::output_bytes) (raw, no line
/// splitting) when the output is not line-structured.
///
/// A byte cap bounds both the retained backlog **and** the in-flight line the
/// pump is still assembling (H1): a line whose own length exceeds the cap can
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputBufferPolicy {
    /// Maximum retained lines: `None` is unbounded; `Some(0)` retains nothing;
    /// `Some(n)` keeps at most `n`.
    pub max_lines: Option<usize>,
    /// Maximum retained bytes (sum of the retained lines' UTF-8 lengths): `None`
    /// is unbounded; `Some(n)` keeps the retained backlog at or under `n` bytes
    /// (D8). A single line longer than `n` cannot fit and is dropped whole under
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
    /// The run errors with [`Error::OutputTooLarge`](crate::Error::OutputTooLarge)
    /// once this limit is reached; excess lines are counted but not retained.
    pub fn fail_loud(max_lines: usize) -> Self {
        Self {
            max_lines: Some(max_lines),
            max_bytes: None,
            overflow: OverflowMode::Error,
        }
    }

    /// Set the retained-byte ceiling (D8), composable with any policy.
    ///
    /// Bounds the actual memory the buffer holds — the sum of the retained
    /// lines' UTF-8 byte lengths — independently of [`max_lines`](Self::max_lines),
    /// and bounds the pump's in-flight assembly buffer too, so even a single
    /// never-terminated line cannot exhaust memory (H1). Use it to cap a stream
    /// whose line *count* is modest but whose lines can be huge (one `base64 -w0`
    /// line evades a line cap but not a byte cap):
    /// `unbounded().with_max_bytes(1 << 20)` is a 1 MiB byte-bounded ring buffer;
    /// `fail_loud(100).with_max_bytes(1 << 20)` errors on whichever ceiling — 100
    /// lines or 1 MiB — is reached first. Under the drop modes a single line
    /// larger than the cap is dropped whole (it cannot fit); under
    /// [`OverflowMode::Error`] it trips the fail-loud ceiling.
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    /// Set the overflow behavior.
    ///
    /// With a `bounded` cap, [`OverflowMode::Error`] fires when the buffer fills.
    /// On an *unbounded* buffer (`max_lines: None`) — D9c — `with_overflow(Error)`
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
