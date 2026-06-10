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
    /// The error fires on **capturing verbs** —
    /// [`output_string`](crate::Command::output_string),
    /// [`output_bytes`](crate::Command::output_bytes),
    /// [`finish_streamed`](crate::RunningProcess::finish_streamed), and
    /// [`finish_events`](crate::RunningProcess::finish_events). Discard-only
    /// verbs ([`wait`](crate::RunningProcess::wait),
    /// [`profile`](crate::RunningProcess::profile)) use a retain-nothing sink
    /// internally and are not affected — use a capturing verb if you need the
    /// DoS guard on a run you don't otherwise capture.
    ///
    /// Use this when unbounded output is itself a misbehavior — an untrusted
    /// tool flooding its stdout is a denial-of-service, not a policy choice.
    ///
    /// **Requires a cap to do anything.** This mode fires only when the buffer
    /// is *full*, so it is inert on an unbounded buffer (`max_lines: None`) —
    /// there is no ceiling to exceed. Reach for it via
    /// [`fail_loud`](OutputBufferPolicy::fail_loud) (which sets the cap for you),
    /// or pair [`with_overflow`](OutputBufferPolicy::with_overflow) with a
    /// `bounded`/`Some(n)` `max_lines`; `unbounded().with_overflow(Error)` never
    /// errors.
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
/// The unit is **lines, not bytes**: a line is held whole until its newline
/// arrives, so one enormous newline-free "line" (e.g. `base64 -w0` output)
/// occupies memory in full regardless of the policy. Use
/// [`output_bytes`](crate::Command::output_bytes) (raw, no line splitting)
/// when the output is not line-structured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputBufferPolicy {
    /// Maximum retained lines: `None` is unbounded; `Some(0)` retains nothing;
    /// `Some(n)` keeps at most `n`.
    pub max_lines: Option<usize>,
    /// Which line to drop when full.
    pub overflow: OverflowMode,
}

impl OutputBufferPolicy {
    /// Retain everything (the default).
    pub fn unbounded() -> Self {
        Self {
            max_lines: None,
            overflow: OverflowMode::DropOldest,
        }
    }

    /// Retain at most `max_lines`, dropping the oldest when full.
    pub fn bounded(max_lines: usize) -> Self {
        Self {
            max_lines: Some(max_lines),
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
            overflow: OverflowMode::Error,
        }
    }

    /// Set the overflow behavior.
    ///
    /// Note that [`OverflowMode::Error`] only takes effect when `max_lines` is
    /// `Some(_)`: it fires when the buffer is full, so on an unbounded buffer
    /// (`max_lines: None`) it is a no-op. Combine it with a `bounded` cap, or
    /// use [`fail_loud`](Self::fail_loud) which sets both at once.
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
