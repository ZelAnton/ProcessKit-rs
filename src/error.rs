//! The crate's error type.

use std::fmt;
use std::time::Duration;

/// Errors produced when launching or running a child process.
///
/// Spawn failures, a non-zero exit ([`Exit`](Error::Exit)), timeouts, and IO
/// errors fold into one structured enum, so callers can pattern-match on the
/// failure mode instead of parsing strings.
///
/// `Debug` is **manual, not derived**: the [`Exit`](Error::Exit) variant
/// carries both captured streams in full, and a derived `Debug` would dump them
/// — potentially multi-MiB — into a `{e:?}` log line or an `.unwrap()` panic
/// message. The manual impl bounds each stream to a 200-byte preview (mirroring
/// the [`Display`](std::fmt::Display) tail cap) and redacts
/// [`NotFound`](Error::NotFound)'s
/// `searched` (the `PATH` env value) to a directory count, honoring the crate's
/// "never log environment values" rule. The exact streams remain reachable via
/// the public fields.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The child process could not be started (binary not found, permission
    /// denied, …).
    #[error("could not start `{program}`: {source}")]
    #[non_exhaustive]
    Spawn {
        /// The program we tried to launch.
        program: String,
        /// The underlying OS error.
        #[source]
        source: std::io::Error,
    },

    /// The program could not be located, so no child was ever started — it is
    /// not installed, not on `PATH`, or the given path does not resolve to an
    /// executable. The **single** representation of "program not found": the
    /// launch path routes every such failure here regardless of how the program
    /// was named (bare name vs path) or platform, so a caller matches one
    /// variant and [`is_not_found`](Error::is_not_found) classifies it.
    ///
    /// Distinct from [`Spawn`](Error::Spawn), which covers OS-level failures
    /// once the program *is* located (permission denied, busy, a bad working
    /// directory, a `.cmd`/`.bat` on Windows that needs `cmd.exe`, etc.) —
    /// those are **not** `is_not_found`.
    ///
    /// The `searched` cause is structured: `Some(dirs)` when a bare name was
    /// looked up against `PATH` (the directories searched), `None` when the
    /// program was given as a path or `PATH` was customized (no `PATH` search
    /// applied, so there are no directories to name).
    ///
    /// The `Display` message intentionally omits `searched` — `PATH` is an
    /// environment value and must never appear in logs per the crate's
    /// security policy. Access `searched` directly for a diagnostic. The message
    /// says "on PATH" only when a `PATH` search actually happened (`searched` is
    /// `Some`); a path-form or customized-PATH program reads simply "not found".
    #[error("{}", display_not_found(program, searched))]
    #[non_exhaustive]
    NotFound {
        /// The program name that was looked up.
        program: String,
        /// The `PATH` directories searched, joined by the platform separator
        /// (`:` on Unix, `;` on Windows) — `Some` for a bare-name PATH lookup
        /// (empty string when `PATH` is unset), `None` when no PATH search
        /// applied (a path-form program, or a customized PATH). Not included in
        /// `Display`; use it directly when building a user-facing diagnostic.
        searched: Option<String>,
    },

    /// A cassette replay found **no recording** matching the invocation — a
    /// stale or incomplete cassette, not a missing program. Kept distinct
    /// from [`Spawn`](Error::Spawn) / [`NotFound`](Error::NotFound) so a wrapper
    /// that treats "tool not installed" as an *optional* dependency does not
    /// silently swallow a stale cassette as an absent tool.
    ///
    /// [`is_not_found`](Error::is_not_found) returns `false` for this variant.
    #[error(
        "`{program}`: no cassette entry matches this invocation (stale or incomplete cassette)"
    )]
    CassetteMiss {
        /// The program whose invocation found no recording.
        program: String,
    },

    /// The process ran to completion but exited with a non-zero status.
    ///
    /// Produced by the `ensure_success` helpers; the raw exit code is otherwise
    /// reported without erroring (a non-zero exit is not inherently a failure).
    ///
    /// Both captured streams are carried **in full**: `git`/`jj` write decisive
    /// diagnostics to **stdout** on failure (`CONFLICT (content): …`, `nothing to
    /// commit, working tree clean`), so a caller building a user-facing message
    /// wants stdout as a fallback when stderr is empty — see
    /// [`diagnostic`](Self::diagnostic). Consumers also classify on these fields
    /// (grep for a marker, parse a sub-code), so they are never truncated before
    /// the caller sees them; only the `Display` message below is bounded.
    ///
    /// The one-line `Display` message appends the **last non-empty line** of
    /// [`diagnostic`](Self::diagnostic), capped at 200 bytes — `` `git` exited
    /// with code 2: fatal: boom `` — actionable in a log line without dumping
    /// multi-KiB streams into it.
    #[error("{}", display_exit(program, *code, stdout, stderr))]
    #[non_exhaustive]
    Exit {
        /// The program that exited non-zero.
        program: String,
        /// The raw process exit code.
        code: i32,
        /// Captured standard output, in full. Not shown in the `Display`
        /// message; kept for callers that need a stdout-borne failure message.
        /// For the raw-bytes helper (`output_bytes`) this is a **lossy UTF-8
        /// decode** of stdout — see this variant's `stdout_bytes` field for
        /// the exact bytes on that path.
        stdout: String,
        /// Captured standard error, in full. Only its **last non-empty
        /// line** (bounded) appears in the `Display` message — the complete
        /// captured text lives here, never poisoning a log line.
        stderr: String,
        /// The exact captured stdout bytes, when the producing path captured
        /// raw bytes rather than already-decoded text — `Some` for a checking
        /// verb built over [`output_bytes`](crate::Command::output_bytes)
        /// (e.g. `output_bytes().await?.ensure_success()?`), `None` for the
        /// text path (`output_string`/`run`/`checked`/…), where `stdout` above
        /// is already the complete, non-lossy text and there is no separate
        /// "raw" form to recover. When `Some`, these bytes are the exact
        /// pre-decode stdout — `stdout` is a lossy UTF-8 decode of this same
        /// data, so the two may differ when the stream was not valid UTF-8.
        /// Read via [`Error::stdout_bytes`].
        stdout_bytes: Option<Vec<u8>>,
    },

    /// The process exceeded its configured timeout and was killed.
    ///
    /// Carries whatever the run captured **before** the deadline killed it:
    /// a hung tool's partial stderr is frequently the explanation
    /// (`waiting for lock held by pid 4123`, `connecting to db…`), so it is
    /// reachable via [`diagnostic`](Self::diagnostic) and the public fields
    /// rather than lost. Empty when the producing path captured nothing (a
    /// streaming probe such as `first_line`, which never buffers).
    ///
    /// The one-line `Display` message appends the **last non-empty line** of
    /// [`diagnostic`](Self::diagnostic), capped at 200 bytes — just like
    /// [`Exit`](Error::Exit) — so a log line stays actionable without dumping
    /// the captured streams.
    #[error("{}", display_timeout(program, *timeout, stdout, stderr))]
    #[non_exhaustive]
    Timeout {
        /// The program that timed out.
        program: String,
        /// The deadline that elapsed.
        timeout: Duration,
        /// Standard output captured before the kill, in full. Not shown in the
        /// `Display` message (only its bounded diagnostic tail is). Empty when
        /// the path captured nothing. For the raw-bytes helper this is a **lossy
        /// UTF-8 decode** — see this variant's `stdout_bytes` field for the
        /// exact bytes on that path.
        stdout: String,
        /// Standard error captured before the kill, in full — often the
        /// explanation of *why* the tool hung. Only its last non-empty line
        /// (bounded) reaches the `Display` message.
        stderr: String,
        /// The exact captured stdout bytes before the kill, when the producing
        /// path captured raw bytes — see [`Exit`](Error::Exit)'s `stdout_bytes`
        /// field for the full contract. `None` on the text path, or when the
        /// producing path captured nothing (a streaming probe). Read via
        /// [`Error::stdout_bytes`].
        stdout_bytes: Option<Vec<u8>>,
    },

    /// The captured output exceeded the
    /// [`OverflowMode::Error`](crate::OverflowMode::Error) fail-loud ceiling —
    /// a line cap ([`max_lines`](crate::OutputBufferPolicy::max_lines)), a byte
    /// cap ([`max_bytes`](crate::OutputBufferPolicy::max_bytes)), or both. The
    /// run itself may have succeeded; this error is raised by the consuming
    /// path after the run completes.
    ///
    /// The pipe is still fully drained (the child never blocks); output past
    /// the ceiling is counted (in the totals) but not retained.
    #[error(
        "`{program}` output exceeded its capture ceiling ({total_lines} lines, {total_bytes} bytes total)"
    )]
    #[non_exhaustive]
    OutputTooLarge {
        /// The program whose output exceeded the ceiling.
        program: String,
        /// The configured line ceiling, if any
        /// (`OutputBufferPolicy::max_lines`).
        max_lines: Option<usize>,
        /// The configured byte ceiling, if any
        /// (`OutputBufferPolicy::max_bytes`).
        max_bytes: Option<usize>,
        /// Total lines that arrived (retained + dropped).
        total_lines: usize,
        /// Total bytes of decoded line text seen (retained + dropped) — the
        /// same unit [`max_bytes`](crate::OutputBufferPolicy::max_bytes) caps:
        /// the sum of line lengths with the trailing newline (and one `\r`)
        /// stripped, not the raw stream size.
        total_bytes: usize,
    },

    /// A readiness probe ([`RunningProcess::wait_for_line`],
    /// [`wait_for_port`](crate::RunningProcess::wait_for_port),
    /// [`wait_for`](crate::RunningProcess::wait_for)) did not pass within its
    /// deadline — the line never appeared, the port never accepted, the check
    /// never returned `true`, or the child exited before becoming ready.
    ///
    /// Distinct from [`Timeout`](Error::Timeout): a probe deadline is separate
    /// from the run's own [`Command::timeout`](crate::Command::timeout), and a
    /// failed probe does **not** kill the child — the caller decides what
    /// happens next.
    ///
    /// [`RunningProcess::wait_for_line`]: crate::RunningProcess::wait_for_line
    #[error("`{program}` was not ready after {timeout:?}")]
    NotReady {
        /// The program that did not become ready.
        program: String,
        /// The probe deadline that elapsed (or would have — an early child
        /// exit fails the probe immediately).
        timeout: Duration,
    },

    /// The process succeeded but its output could not be parsed into the
    /// expected shape (e.g. malformed `--json`). Produced by the fallible-parse
    /// helpers `try_parse` on [`Command`](crate::Command),
    /// [`ProcessRunnerExt`](crate::ProcessRunnerExt),
    /// [`CliClient`](crate::CliClient), and [`Pipeline`](crate::Pipeline) (or any
    /// parser the caller maps into this variant).
    ///
    /// `message` is caller-built and routinely embeds the unparsed output in
    /// full, so — like the [`Exit`](Error::Exit) streams — both `Display` and
    /// `Debug` bound it to a 200-byte preview; the complete text stays
    /// reachable via the public field.
    #[error("{}", display_parse(program, message))]
    #[non_exhaustive]
    Parse {
        /// The program whose output failed to parse.
        program: String,
        /// What went wrong. Carried in full; only `Display`/`Debug` are bounded.
        message: String,
    },

    /// A requested resource limit could not be enforced.
    ///
    /// Produced by [`ProcessGroup::with_options`](crate::ProcessGroup::with_options)
    /// when a [`ResourceLimits`](crate::ResourceLimits) cap was set but the active
    /// mechanism can't honor it — either the platform has no whole-tree container
    /// (macOS/BSD, the Linux process-group fallback), or
    /// the OS rejected the request (on Linux, the cgroup controllers can't be
    /// enabled — see [`ResourceLimits`](crate::ResourceLimits) for the cgroup-v2
    /// "real root only" requirement). An unenforced limit is no protection, so this
    /// is raised rather than leaving the tree silently unbounded.
    ///
    /// Structured so a caller (e.g. the `processkit-py` binding) can branch on
    /// *which* limit and *why* without parsing `detail`'s English text — see
    /// [`LimitKind`](crate::LimitKind) / [`LimitReason`](crate::LimitReason), and
    /// the [`limit_kind`](Self::limit_kind) / [`limit_reason`](Self::limit_reason)
    /// accessors.
    #[cfg(feature = "limits")]
    #[error("{}", display_resource_limit(*kind, *reason, detail))]
    #[non_exhaustive]
    ResourceLimit {
        /// Which limit this failure is about.
        kind: crate::limits::LimitKind,
        /// Why the limit could not be applied.
        reason: crate::limits::LimitReason,
        /// Human-readable detail — the validation message for
        /// [`LimitReason::Invalid`](crate::LimitReason::Invalid), or the
        /// underlying OS error text otherwise. Bounded like other free-text
        /// fields (see [`Parse`](Error::Parse)'s `message`) in `Display`/`Debug`.
        detail: String,
    },

    /// An operation is not supported by the active containment mechanism on
    /// this platform.
    ///
    /// Raised by `ProcessGroup::signal` for any signal other than
    /// `Signal::Kill` on Windows (Job Objects have no POSIX signals).
    #[error("operation `{operation}` is not supported on this platform")]
    Unsupported {
        /// A short description of the operation, e.g. `"signal(Hup)"` or
        /// `"suspend"`.
        operation: String,
    },

    /// The run was cancelled via its `CancellationToken`
    /// ([`Command::cancel_on`](crate::Command::cancel_on)) and its process
    /// tree was killed.
    ///
    /// Asymmetric with [`Timeout`](Error::Timeout) by design: a timeout is
    /// *captured* (`ProcessResult::timed_out`) on the non-checking paths,
    /// whereas a cancellation is **always** raised on every consuming path.
    /// When a run both times out and is cancelled, cancellation wins (it is
    /// checked first).
    ///
    /// Unlike [`Timeout`](Error::Timeout) / [`Signalled`](Error::Signalled),
    /// this carries **no captured streams**: cancellation is a deliberate
    /// caller action that stops the run *immediately*. On the pre-spawn path (the
    /// token was already cancelled) nothing was captured at all; on the consuming
    /// verbs, any output captured before the kill is **intentionally discarded** —
    /// the caller initiated the stop and knows why, so a partial diagnostic would
    /// be noise. [`diagnostic`](Self::diagnostic) returns `None`.
    #[error("`{program}` was cancelled")]
    Cancelled {
        /// The program that was cancelled.
        program: String,
    },

    /// The process was terminated by a signal (**Unix only**) without producing an
    /// exit code. `signal` carries the signal number when the kernel reports one,
    /// else `None`. On **Windows** a killed process reports [`Exit`](Error::Exit)
    /// with a platform code, never this — a live `Signalled` cannot occur there; it
    /// arises only from a [`ScriptedRunner`](crate::testing::ScriptedRunner) or a
    /// `record`-feature cassette replay, which report `Signalled(None)` to mirror
    /// Unix.
    ///
    /// Distinct from [`Exit`](Error::Exit): a signal-terminated run has no exit
    /// code to check — it is always a failure. Produced by
    /// [`ensure_success`](crate::ProcessResult::ensure_success) and the
    /// `require_code` path when the outcome is
    /// [`Outcome::Signalled`](crate::Outcome::Signalled).
    ///
    /// Carries whatever the run captured before the signal killed it — a
    /// crashing tool's partial stderr is often the diagnostic — reachable via
    /// [`diagnostic`](Self::diagnostic) and the public fields. The one-line
    /// `Display` appends the bounded diagnostic tail, like [`Exit`](Error::Exit).
    #[error("{}", display_signalled(program, *signal, stdout, stderr))]
    #[non_exhaustive]
    Signalled {
        /// The program that was killed by a signal.
        program: String,
        /// The signal number, when reported by the platform.
        signal: Option<i32>,
        /// Standard output captured before the kill, in full. Not shown in the
        /// `Display` message (only its bounded diagnostic tail is). For the
        /// raw-bytes helper this is a lossy UTF-8 decode of stdout — see this
        /// variant's `stdout_bytes` field for the exact bytes on that path.
        stdout: String,
        /// Standard error captured before the kill, in full. Only its last
        /// non-empty line (bounded) reaches the `Display` message.
        stderr: String,
        /// The exact captured stdout bytes before the kill, when the producing
        /// path captured raw bytes — see [`Exit`](Error::Exit)'s `stdout_bytes`
        /// field for the full contract. `None` on the text path. Read via
        /// [`Error::stdout_bytes`].
        stdout_bytes: Option<Vec<u8>>,
    },

    /// The child ran but feeding its standard input failed for a reason other
    /// than the routine broken pipe.
    ///
    /// This is raised by the consuming paths **only when the
    /// run otherwise succeeded** — a non-zero [`Exit`](Error::Exit), a
    /// [`Signalled`](Error::Signalled), or a [`Timeout`](Error::Timeout) is the
    /// "realer" failure and wins (the stdin error is then dropped). A broken
    /// pipe (`EPIPE` / `ERROR_BROKEN_PIPE` — the child closing stdin before
    /// reading all of it) is routine and **never** surfaces. Diagnoses a
    /// silently-truncated input the otherwise-successful child may have acted on.
    /// The stdin source ([`Command::stdin`](crate::Command::stdin))
    /// is written on a background task; this carries that task's failure.
    ///
    /// The io-level classifiers ([`is_transient`](Error::is_transient),
    /// [`is_not_found`](Error::is_not_found),
    /// [`is_permission_denied`](Error::is_permission_denied)) deliberately return
    /// `false` here: they classify spawn/launch conditions, and the run already
    /// *succeeded* — a blanket retry would re-run a command that worked. Inspect
    /// `source` directly if a stdin-specific retry is wanted.
    #[error("failed to write to `{program}` stdin: {source}")]
    #[non_exhaustive]
    Stdin {
        /// The program whose standard-input write failed.
        program: String,
        /// The underlying IO error (never a broken pipe).
        #[source]
        source: std::io::Error,
    },

    /// A low-level IO error from the crate's own machinery — driving a child
    /// (waiting for exit, issuing a kill), controlling a process group
    /// (signalling, reaping, sampling stats), or reading/writing a cassette
    /// file. It is **not** a spawn/launch condition (those are
    /// [`Spawn`](Error::Spawn) / [`NotFound`](Error::NotFound)).
    ///
    /// There is **deliberately no blanket `From<std::io::Error>`**: the
    /// crate never lets an arbitrary foreign `io::Error` fall into this variant
    /// via `?`. Every `Io` is constructed explicitly at a known site, so the
    /// io-level classifiers ([`is_transient`](Error::is_transient),
    /// [`is_permission_denied`](Error::is_permission_denied)) only ever see an
    /// IO error the crate itself produced — never an unrelated one a caller's
    /// `?` happened to route through here.
    #[error(transparent)]
    Io(std::io::Error),
}

impl Error {
    /// The best human-facing message for a failed run, trimmed of surrounding
    /// whitespace: captured standard error if it carries text, otherwise the
    /// captured standard output (where `git` puts `CONFLICT …` and `git commit`
    /// puts `nothing to commit`). Covers the variants that capture streams — a
    /// non-zero [`Exit`](Error::Exit), a [`Timeout`](Error::Timeout) (the partial
    /// output of a hung-then-killed tool), and a [`Signalled`](Error::Signalled)
    /// crash. Returns `None` when there is no captured output to show — a silent
    /// run (both streams blank) or a variant that carries none
    /// ([`Spawn`](Error::Spawn), [`Cancelled`](Error::Cancelled),
    /// [`Parse`](Error::Parse), [`Io`](Error::Io)) — so a caller can fall back to
    /// the [`Display`](std::fmt::Display) message. For the raw, untrimmed stream
    /// match on the variant's fields directly.
    pub fn diagnostic(&self) -> Option<&str> {
        // Exhaustive on purpose: a future stream-carrying variant must add itself
        // here rather than fall through a `_ => None` and be invisible to
        // `diagnostic()`. `#[non_exhaustive]` only constrains downstream matches.
        match self {
            Error::Exit { stdout, stderr, .. }
            | Error::Timeout { stdout, stderr, .. }
            | Error::Signalled { stdout, stderr, .. } => exit_diagnostic(stdout, stderr),
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// The captured standard output, for the variants that carry a stream — a
    /// non-zero [`Exit`](Error::Exit), a [`Timeout`](Error::Timeout) (partial
    /// output before the kill), or a [`Signalled`](Error::Signalled) crash —
    /// `None` for every other variant. The raw stream in full (untrimmed); for
    /// the best one-line message use [`diagnostic`](Self::diagnostic), for both
    /// streams joined [`combined`](Self::combined). Reads the stream off the error
    /// without destructuring a `#[non_exhaustive]` variant.
    pub fn stdout(&self) -> Option<&str> {
        self.streams().map(|(stdout, _)| stdout)
    }

    /// The captured standard error, for the stream-bearing variants (see
    /// [`stdout`](Self::stdout)); `None` otherwise. The raw stream in full.
    pub fn stderr(&self) -> Option<&str> {
        self.streams().map(|(_, stderr)| stderr)
    }

    /// The **exact** captured stdout bytes, when available — `Some` only for a
    /// stream-bearing variant ([`Exit`](Error::Exit) / [`Timeout`](Error::Timeout)
    /// / [`Signalled`](Error::Signalled)) produced by a checking verb built over
    /// [`output_bytes`](crate::Command::output_bytes) (e.g.
    /// `output_bytes().await?.ensure_success()?`); `None` for every other
    /// variant, and for a stream-bearing variant produced on the text path
    /// (`output_string`/`run`/`checked`/…), where [`stdout`](Self::stdout)
    /// above is already the complete, non-lossy text. When `Some`, these bytes
    /// are the exact pre-decode stdout that [`stdout`](Self::stdout) is a lossy
    /// UTF-8 decode of — the two differ when the stream was not valid UTF-8.
    pub fn stdout_bytes(&self) -> Option<&[u8]> {
        // Exhaustive on purpose (like `streams`): a future stream-carrying
        // variant must add itself here, not fall through a `_`.
        match self {
            Error::Exit { stdout_bytes, .. }
            | Error::Timeout { stdout_bytes, .. }
            | Error::Signalled { stdout_bytes, .. } => stdout_bytes.as_deref(),
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// Standard output followed by standard error, joined — the [`Error`] twin of
    /// [`ProcessResult::combined`](crate::ProcessResult::combined). `Some` for the
    /// stream-bearing variants, `None` otherwise. A `\n` is inserted between the
    /// streams only when both are non-empty and stdout doesn't already end in one.
    /// Use when a tool interleaves diagnostics across both streams, so a single
    /// [`diagnostic`](Self::diagnostic) stream (stderr *else* stdout) would miss a
    /// marker on the other.
    pub fn combined(&self) -> Option<String> {
        self.streams()
            .map(|(stdout, stderr)| crate::result::combine_streams(stdout, stderr))
    }

    /// The captured `(stdout, stderr)` for the stream-bearing variants
    /// ([`Exit`](Error::Exit) / [`Timeout`](Error::Timeout) /
    /// [`Signalled`](Error::Signalled)), `None` for the rest — the single
    /// exhaustive match the public stream accessors above derive from.
    fn streams(&self) -> Option<(&str, &str)> {
        // Exhaustive on purpose (like `diagnostic`/`io_source`): a future
        // stream-carrying variant must add itself here, not fall through a `_`.
        match self {
            Error::Exit { stdout, stderr, .. }
            | Error::Timeout { stdout, stderr, .. }
            | Error::Signalled { stdout, stderr, .. } => Some((stdout, stderr)),
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// The program (the CLI tool) this error is attributed to — `Some` for every
    /// variant that names one (all except [`Unsupported`](Error::Unsupported),
    /// [`Io`](Error::Io), and the `limits`-only `ResourceLimit`), `None` otherwise.
    /// The [`Error`] twin of
    /// [`ProcessResult::program`](crate::ProcessResult::program): the one
    /// cross-cutting datum a wrapper routes or logs on, read without destructuring
    /// a `#[non_exhaustive]` variant.
    pub fn program(&self) -> Option<&str> {
        // Exhaustive on purpose (like `diagnostic`/`streams`/`io_source`): a future
        // program-naming variant must add itself here, not fall through a `_`.
        match self {
            Error::Spawn { program, .. }
            | Error::NotFound { program, .. }
            | Error::CassetteMiss { program }
            | Error::Exit { program, .. }
            | Error::Timeout { program, .. }
            | Error::OutputTooLarge { program, .. }
            | Error::NotReady { program, .. }
            | Error::Parse { program, .. }
            | Error::Cancelled { program }
            | Error::Signalled { program, .. }
            | Error::Stdin { program, .. } => Some(program),
            Error::Unsupported { .. } | Error::Io(_) => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// Whether the **program could not be located** — it is not installed, not
    /// on `PATH`, or the given path does not resolve to an executable. True for
    /// [`NotFound`](Error::NotFound) and **only** that variant: the launch
    /// path funnels every program-not-found failure into `NotFound`, so this is
    /// the one check a caller needs to surface a "command not installed?" hint.
    ///
    /// `false` for every other variant — notably it does **not** fire for a
    /// missing or invalid working directory (a [`Spawn`](Error::Spawn) carrying
    /// [`NotFound`](std::io::ErrorKind::NotFound)/`NotADirectory`): a bad `cwd`
    /// is not a missing program, so the hint would mislead. It is also `false`
    /// for a program that *is* installed but can't be executed directly (e.g. a
    /// Windows `.cmd`/`.bat` that needs `cmd.exe` — surfaced as `Spawn`).
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound { .. })
    }

    /// Whether this is a spawn/IO **permission denial** (`EACCES`/`EPERM`): the
    /// binary isn't executable, or the OS refused the launch. True for
    /// [`Spawn`](Error::Spawn) / [`Io`](Error::Io) carrying
    /// [`PermissionDenied`](std::io::ErrorKind::PermissionDenied); `false`
    /// otherwise.
    pub fn is_permission_denied(&self) -> bool {
        self.io_source()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied)
    }

    /// Whether this is a **transient** spawn/IO condition a bare retry can clear
    /// — interrupted (`EINTR`), would-block (`EAGAIN`), a busy resource, a
    /// text-file-busy executable mid-write (`ETXTBSY`), or a Windows sharing/lock
    /// violation. Classifies the [`Spawn`](Error::Spawn)/[`Io`](Error::Io) IO
    /// error only.
    ///
    /// **Scope: IO/spawn-level, never exit codes.** Whether a tool's non-zero
    /// [`Exit`](Error::Exit) is retryable is domain-specific (a `git` 128 is not
    /// generically transient) — that stays the caller's call. [`Timeout`](Error::Timeout)
    /// is also excluded by design; compose it if wanted:
    /// `e.is_transient() || e.is_timeout()`.
    ///
    /// Pairs with [`Command::retry`](crate::Command::retry):
    /// `cmd.retry(3, backoff, |e| e.is_transient())`.
    pub fn is_transient(&self) -> bool {
        self.io_source().is_some_and(is_transient_io)
    }

    /// The process exit code for a non-zero [`Exit`](Error::Exit); `None` for
    /// every other variant (a timeout or a signal kill carries no exit code).
    /// The same `code()` the crate's other disposition types expose
    /// ([`ProcessResult::code`](crate::ProcessResult::code) /
    /// [`Outcome::code`](crate::Outcome::code), and `RunProfile::code` under the
    /// `stats` feature), so a code is one name everywhere. Reads the code off the
    /// error without destructuring the variant.
    pub fn code(&self) -> Option<i32> {
        // Exhaustive on purpose (like `streams`/`program`): a future exit-code-
        // carrying variant must add itself here, not fall through a `_` and be
        // silently invisible to `code()`.
        match self {
            Error::Exit { code, .. } => Some(*code),
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::Timeout { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Signalled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// The signal number for a [`Signalled`](Error::Signalled) run terminated
    /// with a **known** signal (**Unix only**); `None` for every other variant and
    /// for a signal the kernel didn't expose — the [`Error`] twin of
    /// [`ProcessResult::signal`](crate::ProcessResult::signal) /
    /// [`Outcome::signal`](crate::Outcome::signal). Reads the signal off the error
    /// without destructuring the variant.
    pub fn signal(&self) -> Option<i32> {
        // Exhaustive on purpose (like `streams`/`program`): a future signal-
        // carrying variant must add itself here, not fall through a `_`.
        match self {
            Error::Signalled { signal, .. } => *signal,
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::Exit { .. }
            | Error::Timeout { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// Which limit a [`ResourceLimit`](Error::ResourceLimit) failure is about;
    /// `None` for every other variant. Reads the field off the error without
    /// destructuring the `#[non_exhaustive]` variant.
    #[cfg(feature = "limits")]
    pub fn limit_kind(&self) -> Option<crate::limits::LimitKind> {
        // Exhaustive on purpose (like `signal`/`program`): a future variant
        // must add itself here, not fall through a `_`.
        match self {
            Error::ResourceLimit { kind, .. } => Some(*kind),
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::Exit { .. }
            | Error::Timeout { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Signalled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
        }
    }

    /// Why a [`ResourceLimit`](Error::ResourceLimit) failure occurred; `None`
    /// for every other variant. Reads the field off the error without
    /// destructuring the `#[non_exhaustive]` variant.
    #[cfg(feature = "limits")]
    pub fn limit_reason(&self) -> Option<crate::limits::LimitReason> {
        // Exhaustive on purpose (like `signal`/`program`): a future variant
        // must add itself here, not fall through a `_`.
        match self {
            Error::ResourceLimit { reason, .. } => Some(*reason),
            Error::Spawn { .. }
            | Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::Exit { .. }
            | Error::Timeout { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Signalled { .. }
            | Error::Stdin { .. }
            | Error::Io(_) => None,
        }
    }

    /// Whether the run was killed because it exceeded its
    /// [`Command::timeout`](crate::Command::timeout) — i.e. this is a
    /// [`Timeout`](Error::Timeout). First-class here so the
    /// [`is_transient`](Self::is_transient) retry-composition example can read
    /// `e.is_transient() || e.is_timeout()` rather than matching the variant by hand.
    /// The [`Error`] twin of the crate-wide deadline predicate
    /// [`ProcessResult::timed_out`](crate::ProcessResult::timed_out) (named
    /// `is_timeout` here to sit alongside the error's `is_*` predicate family).
    pub fn is_timeout(&self) -> bool {
        matches!(self, Error::Timeout { .. })
    }

    /// Whether the run was deliberately cancelled via its
    /// [`Command::cancel_on`](crate::Command::cancel_on) token — i.e. this is a
    /// [`Cancelled`](Error::Cancelled). A caller that initiated the stop can
    /// swallow it rather than log or retry it as a real failure (the same
    /// disposition [`Supervisor`](crate::Supervisor) treats as terminal), without
    /// destructuring the variant.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Error::Cancelled { .. })
    }

    /// Whether the run was killed by a signal — i.e. this is a
    /// [`Signalled`](Error::Signalled). Distinct from [`signal`](Self::signal):
    /// this is `true` even when the kernel didn't expose a number (a Unix kill
    /// where `signal()` is `None`, or any platform's signal disposition), so it is
    /// the reliable "died by a signal?" check — the predicate twin of
    /// [`is_timeout`](Self::is_timeout) / [`is_cancelled`](Self::is_cancelled),
    /// without destructuring the variant.
    pub fn is_signalled(&self) -> bool {
        matches!(self, Error::Signalled { .. })
    }

    /// The underlying [`std::io::Error`] for the variants that carry one
    /// ([`Spawn`](Error::Spawn), [`Io`](Error::Io)) — the basis for the io-level
    /// classifiers above.
    fn io_source(&self) -> Option<&std::io::Error> {
        // Exhaustive on purpose: a future variant carrying an `io::Error` must
        // add itself here so the io-level classifiers (`is_transient`,
        // `is_permission_denied`) see it, rather than slipping through a wildcard.
        match self {
            Error::Spawn { source, .. } => Some(source),
            Error::Io(source) => Some(source),
            Error::NotFound { .. }
            | Error::CassetteMiss { .. }
            | Error::Exit { .. }
            | Error::Timeout { .. }
            | Error::OutputTooLarge { .. }
            | Error::NotReady { .. }
            | Error::Parse { .. }
            | Error::Unsupported { .. }
            | Error::Cancelled { .. }
            | Error::Signalled { .. }
            | Error::Stdin { .. } => None,
            #[cfg(feature = "limits")]
            Error::ResourceLimit { .. } => None,
        }
    }

    /// Construct an [`Exit`](Error::Exit) — a `#[doc(hidden)]` convenience for
    /// custom [`ProcessRunner`](crate::ProcessRunner) doubles and error-classifier
    /// tests, so they stop spelling out the struct literal (which the variant's
    /// `#[non_exhaustive]` already rejects outside this crate, and which a future
    /// field addition would otherwise break) and go through one insulated
    /// constructor instead. Off the documented surface, but `pub` so downstream
    /// test code can call it; semver-covered like any public item.
    ///
    /// Always builds a text-path error (`stdout_bytes: None`) — this constructor
    /// takes text, not raw bytes. A bytes-path `Exit` (`stdout_bytes: Some(..)`)
    /// only comes from a real checking verb built over
    /// [`output_bytes`](crate::Command::output_bytes)
    /// (e.g. `ProcessResult<Vec<u8>>::ensure_success`).
    #[doc(hidden)]
    pub fn exit(
        program: impl Into<String>,
        code: i32,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Self {
        Error::Exit {
            program: program.into(),
            code,
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_bytes: None,
        }
    }

    /// Construct a [`Timeout`](Error::Timeout) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn timeout(
        program: impl Into<String>,
        timeout: Duration,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Self {
        Error::Timeout {
            program: program.into(),
            timeout,
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_bytes: None,
        }
    }

    /// Construct a [`Signalled`](Error::Signalled) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn signalled(
        program: impl Into<String>,
        signal: Option<i32>,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Self {
        Error::Signalled {
            program: program.into(),
            signal,
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_bytes: None,
        }
    }

    /// Construct a [`Spawn`](Error::Spawn) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn spawn(program: impl Into<String>, source: std::io::Error) -> Self {
        Error::Spawn {
            program: program.into(),
            source,
        }
    }

    /// Construct a [`NotFound`](Error::NotFound) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn not_found(program: impl Into<String>, searched: Option<String>) -> Self {
        Error::NotFound {
            program: program.into(),
            searched,
        }
    }

    /// Construct a [`Stdin`](Error::Stdin) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn stdin(program: impl Into<String>, source: std::io::Error) -> Self {
        Error::Stdin {
            program: program.into(),
            source,
        }
    }

    /// Construct a [`Parse`](Error::Parse) from a caller-supplied parser's own
    /// failure message. Unlike `exit`/`timeout`/`signalled`/`spawn`/`not_found`/
    /// `stdin` above (`#[doc(hidden)]` insulated constructors meant for test
    /// doubles), this one is left on the **documented public surface**: an
    /// external parser that inspects a tool's output outside this crate's own
    /// `try_parse` helpers has no other way to report a parse failure as an
    /// `Error::Parse` once the variant is `#[non_exhaustive]`, and that path is
    /// a normal production use, not just a test-doubling convenience.
    pub fn parse(program: impl Into<String>, message: impl Into<String>) -> Self {
        Error::Parse {
            program: program.into(),
            message: message.into(),
        }
    }
}

/// Manual `Debug`: bounds the [`Exit`](Error::Exit) streams and redacts
/// the `PATH` value, so `{e:?}` / `.unwrap()` neither dumps a multi-MiB stream
/// nor logs an environment value. Every other variant mirrors what the derive
/// would print.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Spawn { program, source } => f
                .debug_struct("Spawn")
                .field("program", program)
                .field("source", source)
                .finish(),
            Error::NotFound { program, searched } => f
                .debug_struct("NotFound")
                .field("program", program)
                // `searched` is the `PATH` env value — never rendered; summarize
                // as a directory count (`None` renders as `None`).
                .field("searched", &searched.as_deref().map(SearchedRedaction))
                .finish(),
            Error::CassetteMiss { program } => f
                .debug_struct("CassetteMiss")
                .field("program", program)
                .finish(),
            Error::Exit {
                program,
                code,
                stdout,
                stderr,
                stdout_bytes,
            } => f
                .debug_struct("Exit")
                .field("program", program)
                .field("code", code)
                .field("stdout", &StreamPreview(stdout))
                .field("stderr", &StreamPreview(stderr))
                .field("stdout_bytes", &BytesPreview(stdout_bytes.as_deref()))
                .finish(),
            Error::Timeout {
                program,
                timeout,
                stdout,
                stderr,
                stdout_bytes,
            } => f
                .debug_struct("Timeout")
                .field("program", program)
                .field("timeout", timeout)
                .field("stdout", &StreamPreview(stdout))
                .field("stderr", &StreamPreview(stderr))
                .field("stdout_bytes", &BytesPreview(stdout_bytes.as_deref()))
                .finish(),
            Error::OutputTooLarge {
                program,
                max_lines,
                max_bytes,
                total_lines,
                total_bytes,
            } => f
                .debug_struct("OutputTooLarge")
                .field("program", program)
                .field("max_lines", max_lines)
                .field("max_bytes", max_bytes)
                .field("total_lines", total_lines)
                .field("total_bytes", total_bytes)
                .finish(),
            Error::NotReady { program, timeout } => f
                .debug_struct("NotReady")
                .field("program", program)
                .field("timeout", timeout)
                .finish(),
            Error::Parse { program, message } => f
                .debug_struct("Parse")
                .field("program", program)
                // Caller-built, often the full unparsed output — bound it.
                .field("message", &StreamPreview(message))
                .finish(),
            #[cfg(feature = "limits")]
            Error::ResourceLimit {
                kind,
                reason,
                detail,
            } => f
                .debug_struct("ResourceLimit")
                .field("kind", kind)
                .field("reason", reason)
                // Bounded like every text-bearing variant to keep the "no
                // unbounded text in Debug" invariant uniform, though `detail`
                // is short today.
                .field("detail", &StreamPreview(detail))
                .finish(),
            Error::Unsupported { operation } => f
                .debug_struct("Unsupported")
                .field("operation", operation)
                .finish(),
            Error::Cancelled { program } => f
                .debug_struct("Cancelled")
                .field("program", program)
                .finish(),
            Error::Signalled {
                program,
                signal,
                stdout,
                stderr,
                stdout_bytes,
            } => f
                .debug_struct("Signalled")
                .field("program", program)
                .field("signal", signal)
                .field("stdout", &StreamPreview(stdout))
                .field("stderr", &StreamPreview(stderr))
                .field("stdout_bytes", &BytesPreview(stdout_bytes.as_deref()))
                .finish(),
            Error::Stdin { program, source } => f
                .debug_struct("Stdin")
                .field("program", program)
                .field("source", source)
                .finish(),
            Error::Io(source) => f.debug_tuple("Io").field(source).finish(),
        }
    }
}

/// `Debug` for a captured stream, bounded to a 200-byte char-boundary preview
/// with a `(+N bytes)` note — the [`Exit`](Error::Exit) streams can be multi-MiB
/// and must never flood a `{e:?}` log line or `.unwrap()` panic message. Mirrors
/// the [`Display`](std::fmt::Display) tail cap in [`display_exit`].
pub(crate) struct StreamPreview<'a>(pub(crate) &'a str);

impl fmt::Debug for StreamPreview<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const CAP: usize = DIAG_CAP;
        let s = self.0;
        if s.len() <= CAP {
            return fmt::Debug::fmt(s, f);
        }
        let mut cut = CAP;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        write!(f, "{:?}… (+{} bytes)", &s[..cut], s.len() - cut)
    }
}

/// `Debug` for the `stdout_bytes` field of [`Exit`](Error::Exit) /
/// [`Timeout`](Error::Timeout) / [`Signalled`](Error::Signalled): never dumps
/// the raw bytes (they may be binary, may carry secrets, and can be
/// multi-MiB — the same "no unbounded payload in Debug" rule
/// [`StreamPreview`] follows for the text streams) — only a length summary
/// when present, `None` otherwise.
struct BytesPreview<'a>(Option<&'a [u8]>);

impl fmt::Debug for BytesPreview<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(bytes) => write!(f, "Some(<{} bytes>)", bytes.len()),
            None => write!(f, "None"),
        }
    }
}

/// `Debug` for [`NotFound`](Error::NotFound)'s `searched`: the `PATH` value is an
/// environment value and must never be logged, so it renders only as a directory
/// count (`<N directories>`) — never the directories themselves.
struct SearchedRedaction<'a>(&'a str);

impl fmt::Debug for SearchedRedaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const SEP: char = if cfg!(windows) { ';' } else { ':' };
        // Count non-empty segments only, so a trailing or doubled separator does
        // not inflate the redacted count.
        let n = self.0.split(SEP).filter(|s| !s.is_empty()).count();
        write!(f, "<{n} directories>")
    }
}

/// `NotFound`'s one-line `Display`. Says "not found on PATH" only when a `PATH`
/// search actually happened (`searched.is_some()` — a bare name looked up against
/// the process `PATH`); a path-form program or a customized `PATH` (`None`) reads
/// `` `{program}` not found ``, since no `PATH` lookup occurred. Never includes
/// the `searched` value itself (the `PATH` env value — the crate's secret rule).
fn display_not_found(program: &str, searched: &Option<String>) -> String {
    match searched {
        Some(_) => format!("`{program}` not found on PATH"),
        None => format!("`{program}` not found"),
    }
}

/// Builds the [`Error::Io`] raised when a capture verb is called on a command
/// whose stdout was not piped (`Command::stdout` set to `Inherit`/`Null`). The
/// live runner (`RunningProcess::ensure_stdout_capturable`) and both test
/// doubles (`ScriptedRunner::output_string`, the `RecordReplayRunner` replay
/// branch) must reject this identically, so they all route through this one
/// constructor instead of hand-rolling the message and `ErrorKind`.
pub(crate) fn stdout_not_piped_error(program: &str) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "`{program}`: stdout is not piped (Command::stdout was set to \
             Inherit/Null), so the capture verbs have nothing to read — \
             use StdioMode::Piped to capture it"
        ),
    ))
}

/// `Parse`'s one-line `Display`: `` failed to parse `{program}` output: {message} ``
/// with the caller-built `message` bounded to a 200-byte char-boundary head
/// — its start carries the actionable detail (`unexpected token at line 3`),
/// unlike a captured stream whose *tail* is quoted — so an embedded multi-KiB
/// unparsed dump can never poison a log line. The full text stays on the field.
///
/// `Parse` messages routinely embed attacker-influenced unparsed output, so each
/// char is passed through [`is_display_unsafe`] and replaced with `U+FFFD` if
/// dangerous — the same control-/bidi-injection defense the captured-stream tails
/// get, which a bare truncation would have skipped.
fn display_parse(program: &str, message: &str) -> String {
    let mut out = format!("failed to parse `{program}` output: ");
    push_sanitized_capped(&mut out, message, DIAG_CAP);
    out
}

/// `ResourceLimit`'s one-line `Display`: `` {kind} limit {reason}: {detail} ``,
/// e.g. `` memory limit could not be enforced: enabling cgroup controllers... ``
/// or `` CPU limit is invalid: cpu_quota must be a finite value greater than 0 ``.
/// `detail` is bounded/sanitized like [`Parse`](Error::Parse)'s `message` — it
/// may embed a raw OS error string, never trusted to be short or clean.
#[cfg(feature = "limits")]
fn display_resource_limit(
    kind: crate::limits::LimitKind,
    reason: crate::limits::LimitReason,
    detail: &str,
) -> String {
    use crate::limits::{LimitKind, LimitReason};
    let kind_str = match kind {
        LimitKind::Memory => "memory limit",
        LimitKind::Processes => "process-count limit",
        LimitKind::Cpu => "CPU limit",
    };
    let reason_str = match reason {
        LimitReason::Invalid => "is invalid",
        LimitReason::Unsupported => "is not supported on this platform",
        LimitReason::Unenforceable => "could not be enforced",
    };
    let mut out = format!("{kind_str} {reason_str}");
    if !detail.is_empty() {
        out.push_str(": ");
        push_sanitized_capped(&mut out, detail, DIAG_CAP);
    }
    out
}

/// `Signalled`'s one-line Display: `` `{program}` was terminated by signal {n} ``
/// when a number is known, `` `{program}` was terminated by a signal `` otherwise,
/// plus the bounded diagnostic tail of the captured streams, like
/// [`display_exit`].
fn display_signalled(program: &str, signal: Option<i32>, stdout: &str, stderr: &str) -> String {
    let mut message = match signal {
        Some(n) => format!("`{program}` was terminated by signal {n}"),
        None => format!("`{program}` was terminated by a signal"),
    };
    append_diagnostic_tail(&mut message, stdout, stderr);
    message
}

/// `Timeout`'s one-line Display: `` `{program}` timed out after {timeout:?} `` plus
/// the bounded diagnostic tail of whatever the run captured before the kill
/// — a hung tool's last stderr line is often the explanation. Same tail cap as
/// [`display_exit`].
///
/// A **zero** `timeout` renders just `` `{program}` timed out `` (no "after 0ns"):
/// a `Duration::ZERO` here means the deadline wasn't known to the checking verb
/// (a scripted / cassette-replayed timeout whose command carried no `timeout`),
/// not that the run was killed at 0ns — "after 0ns" would be actively misleading.
fn display_timeout(program: &str, timeout: Duration, stdout: &str, stderr: &str) -> String {
    let mut message = if timeout.is_zero() {
        format!("`{program}` timed out")
    } else {
        format!("`{program}` timed out after {timeout:?}")
    };
    append_diagnostic_tail(&mut message, stdout, stderr);
    message
}

/// io-level "retry as-is" conditions: transient kernel/filesystem states a bare
/// retry can clear, distinct from a permanent failure (not-found, permission).
/// Kept deliberately narrow.
fn is_transient_io(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    // `ExecutableFileBusy` (std's `ETXTBSY` mapping) clears once the writer
    // closes the executable.
    if matches!(
        e.kind(),
        ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::ResourceBusy
            | ErrorKind::ExecutableFileBusy
    ) {
        return true;
    }
    // std leaves Windows sharing/lock violations `Uncategorized`, so match the
    // raw codes: ERROR_SHARING_VIOLATION (32) / ERROR_LOCK_VIOLATION (33).
    #[cfg(windows)]
    {
        matches!(e.raw_os_error(), Some(32 | 33))
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// The stream a failed run's message should quote: stderr when it carries
/// text, else stdout (where `git` puts `CONFLICT …`), else nothing.
fn exit_diagnostic<'a>(stdout: &'a str, stderr: &'a str) -> Option<&'a str> {
    [stderr, stdout]
        .into_iter()
        .map(str::trim)
        .find(|text| !text.is_empty())
}

/// `Exit`'s one-line `Display`: program + code, plus a bounded excerpt of the
/// diagnostic — its **last** non-empty line (the actionable one: `git push`
/// ends with `remote: permission denied`, not starts), capped at 200 bytes on
/// a char boundary so a binary-garbage or one-enormous-line stream can never
/// poison a log line.
fn display_exit(program: &str, code: i32, stdout: &str, stderr: &str) -> String {
    let mut message = format!("`{program}` exited with code {code}");
    append_diagnostic_tail(&mut message, stdout, stderr);
    message
}

/// The byte budget every one-line `Display`/`Debug` excerpt of caller- or
/// child-influenced text is capped to, so a multi-KiB stream or unparsed dump can
/// never poison a log line. Shared by [`push_sanitized_capped`], the
/// [`StreamPreview`] `Debug`, and the diagnostic-tail/`Parse` displays.
const DIAG_CAP: usize = 200;

/// Append `text` to `out`, replacing any [display-unsafe](is_display_unsafe) char
/// with `U+FFFD` and stopping at `cap` bytes (an `…` marks truncation). The byte
/// budget counts only `text`, never what `out` already holds. The single
/// sanitize-and-cap loop shared by the `Display` paths that embed
/// attacker-influenced text — the diagnostic tail ([`append_diagnostic_tail`])
/// and the [`Parse`](Error::Parse) message head ([`display_parse`]) — so the
/// control-/bidi-injection defense and the cap can't drift apart.
fn push_sanitized_capped(out: &mut String, text: &str, cap: usize) {
    let mut written = 0usize;
    for ch in text.chars() {
        let ch = if is_display_unsafe(ch) {
            '\u{FFFD}'
        } else {
            ch
        };
        if written + ch.len_utf8() > cap {
            out.push('…');
            return;
        }
        out.push(ch);
        written += ch.len_utf8();
    }
}

/// Whether `ch` is unsafe to emit verbatim into a one-line log/terminal from
/// attacker-influenced text: a control character (ANSI `ESC`, `BEL`,
/// `NUL`, `CR`, cursor moves, …), a Unicode **line/paragraph separator** that a
/// terminal or log viewer renders as a newline (breaking the "one actionable
/// line" intent), **or** a Unicode bidirectional-formatting control — the
/// "Trojan Source" class (CVE-2021-42574) that can visually reorder the
/// surrounding text in a terminal or editor.
///
/// **Tab (`\t`) is exempt:** it is a legitimate, common column separator in tool
/// output (TSV, `git diff`, `ls -l`) and is harmless in a one-line context — it
/// advances to a tab stop, it cannot inject an escape sequence or reorder text — so
/// mangling it to `U+FFFD` would corrupt ordinary output for no security gain.
fn is_display_unsafe(ch: char) -> bool {
    (ch.is_control() && ch != '\t')
        || matches!(ch,
            '\u{2028}' | '\u{2029}'   // LS PS (line/paragraph separators — `is_control` misses these)
            | '\u{202A}'..='\u{202E}' // LRE RLE PDF LRO RLO (embeddings/overrides)
            | '\u{2066}'..='\u{2069}' // LRI RLI FSI PDI (isolates)
            | '\u{200E}' | '\u{200F}' // LRM RLM (implicit marks)
            | '\u{061C}'              // ALM (Arabic letter mark)
        )
}

/// Append `: <last non-empty diagnostic line>` to a one-line error `Display`,
/// capped at 200 bytes on a char boundary with an ellipsis. Shared by
/// [`display_exit`], [`display_timeout`], and [`display_signalled`] so a
/// captured-stream error stays one actionable line and a binary-garbage or
/// one-enormous-line stream can never poison a log line. A no-op when both
/// streams are blank.
///
/// Each char is passed through [`is_display_unsafe`] and
/// replaced with `U+FFFD` if dangerous, so a hostile child's stderr cannot inject
/// terminal escape sequences (ANSI, `BEL`, `NUL`, cursor moves) or bidi-reordering
/// controls into an operator's log or terminal through a `{err}` format. (The
/// line was already split on `\n`; this also neutralizes any stray embedded
/// `\r`/`ESC`.)
fn append_diagnostic_tail(message: &mut String, stdout: &str, stderr: &str) {
    let tail = exit_diagnostic(stdout, stderr)
        .and_then(|text| text.lines().rev().map(str::trim).find(|l| !l.is_empty()));
    if let Some(tail) = tail {
        message.push_str(": ");
        push_sanitized_capped(message, tail, DIAG_CAP);
    }
}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_accessors_cover_the_stream_bearing_variants() {
        // Exit/Timeout/Signalled expose stdout/stderr/combined; combined mirrors
        // ProcessResult::combined (a `\n` between only when both are non-empty
        // and stdout doesn't already end in one).
        for err in [
            Error::exit("git", 1, "out", "err"),
            Error::timeout("git", Duration::from_secs(1), "out", "err"),
            Error::signalled("git", Some(9), "out", "err"),
        ] {
            assert_eq!(err.stdout(), Some("out"));
            assert_eq!(err.stderr(), Some("err"));
            assert_eq!(err.combined().as_deref(), Some("out\nerr"));
        }
        // stdout already ending in `\n` is not double-separated.
        let trailing = Error::exit("git", 1, "out\n", "err");
        assert_eq!(trailing.combined().as_deref(), Some("out\nerr"));
        // An empty stream contributes nothing to `combined`, but the raw
        // accessors still report it as present (`Some("")`) — the contract that
        // distinguishes them from `diagnostic()` (which trims to `None`).
        let only_err = Error::exit("git", 1, "", "err");
        assert_eq!(only_err.stdout(), Some(""));
        assert_eq!(only_err.stderr(), Some("err"));
        assert_eq!(only_err.combined().as_deref(), Some("err"));

        // Non-stream variants carry no streams.
        let not_ready = Error::NotReady {
            program: "server".into(),
            timeout: Duration::from_secs(1),
        };
        assert_eq!(not_ready.stdout(), None);
        assert_eq!(not_ready.stderr(), None);
        assert_eq!(not_ready.combined(), None);
    }

    #[test]
    fn disposition_accessors_code_signal_timeout_cancelled() {
        let exit = Error::exit("git", 7, "", "");
        assert_eq!(exit.code(), Some(7));
        assert_eq!(exit.signal(), None);
        assert!(!exit.is_timeout());
        assert!(!exit.is_cancelled());

        let timeout = Error::timeout("git", Duration::from_secs(1), "", "");
        assert_eq!(timeout.code(), None);
        assert_eq!(timeout.signal(), None);
        assert!(timeout.is_timeout());

        let signalled = Error::signalled("git", Some(9), "", "");
        assert_eq!(signalled.code(), None);
        assert_eq!(signalled.signal(), Some(9));
        assert!(!signalled.is_timeout());
        assert!(signalled.is_signalled());
        // A signal kill with no reported number reads as `None`, not a sentinel —
        // but `is_signalled()` still detects it (the reason it exists).
        let unknown_sig = Error::signalled("git", None, "", "");
        assert_eq!(unknown_sig.signal(), None);
        assert!(unknown_sig.is_signalled());
        assert!(!exit.is_signalled() && !timeout.is_signalled());

        let cancelled = Error::Cancelled {
            program: "job".into(),
        };
        assert!(cancelled.is_cancelled());
        assert!(!cancelled.is_signalled());
        assert!(!exit.is_cancelled() && !timeout.is_cancelled());
    }

    #[test]
    fn program_accessor_covers_named_variants_and_skips_the_rest() {
        // Every variant that names a program returns it; the variants that don't
        // (Unsupported, Io) return None.
        assert_eq!(Error::exit("git", 1, "", "").program(), Some("git"));
        assert_eq!(
            Error::NotReady {
                program: "server".into(),
                timeout: Duration::from_secs(1),
            }
            .program(),
            Some("server")
        );
        assert_eq!(
            Error::Cancelled {
                program: "job".into()
            }
            .program(),
            Some("job")
        );
        assert_eq!(
            Error::Unsupported {
                operation: "suspend".into()
            }
            .program(),
            None
        );
        assert_eq!(
            Error::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)).program(),
            None
        );
        #[cfg(feature = "limits")]
        assert_eq!(
            Error::ResourceLimit {
                kind: crate::limits::LimitKind::Memory,
                reason: crate::limits::LimitReason::Unsupported,
                detail: "no container".into()
            }
            .program(),
            None
        );
    }

    #[test]
    fn doc_hidden_constructors_build_the_expected_variants() {
        // The constructors insulate doubles/tests from a future field addition to
        // these variants; round-trip through the accessors confirms the variant.
        assert!(matches!(
            Error::exit("git", 2, "o", "e"),
            Error::Exit { code: 2, .. }
        ));
        assert!(matches!(
            Error::timeout("git", Duration::from_secs(3), "o", "e"),
            Error::Timeout { .. }
        ));
        match Error::signalled("git", None, "o", "e") {
            Error::Signalled { signal, .. } => assert_eq!(signal, None),
            other => panic!("expected Signalled, got {other:?}"),
        }
    }

    #[test]
    fn display_tail_sanitizes_control_chars_against_terminal_injection() {
        // A hostile child's stderr last line carrying ANSI/BEL/NUL must not reach
        // a `{err}` log/terminal verbatim — control bytes become U+FFFD, while
        // printable text survives.
        let err = Error::Exit {
            program: "tool".into(),
            code: 1,
            stdout: String::new(),
            stderr: "boom\x1b[31m\x07\x00danger".into(),
            stdout_bytes: None,
        };
        let msg = err.to_string();
        assert!(!msg.contains('\x1b'), "ESC must be sanitized: {msg:?}");
        assert!(!msg.contains('\x07'), "BEL must be sanitized: {msg:?}");
        assert!(!msg.contains('\x00'), "NUL must be sanitized: {msg:?}");
        assert!(msg.contains("boom"), "printable text is kept: {msg:?}");
        assert!(msg.contains("danger"), "printable text is kept: {msg:?}");
    }

    #[test]
    fn display_tail_strips_bidi_controls_against_trojan_source() {
        // A hostile stderr last line carrying bidi-override controls
        // (CVE-2021-42574) must not reach a `{err}` line and visually reorder it.
        let err = Error::Exit {
            program: "tool".into(),
            code: 1,
            stdout: String::new(),
            stderr: "safe\u{202E}reversed\u{202C}\u{2066}iso".into(),
            stdout_bytes: None,
        };
        let msg = err.to_string();
        assert!(!msg.contains('\u{202E}'), "RLO must be sanitized: {msg:?}");
        assert!(!msg.contains('\u{202C}'), "PDF must be sanitized: {msg:?}");
        assert!(!msg.contains('\u{2066}'), "LRI must be sanitized: {msg:?}");
        assert!(msg.contains("safe"), "printable text is kept: {msg:?}");
    }

    #[test]
    fn display_tail_strips_unicode_line_separators() {
        // U+2028 / U+2029 are NOT `char::is_control()`, yet terminals and log
        // viewers render them as a newline — a hostile last line carrying them
        // must not inject a break into the one-line `{err}` render.
        let err = Error::Exit {
            program: "tool".into(),
            code: 1,
            stdout: String::new(),
            stderr: "before\u{2028}after\u{2029}end".into(),
            stdout_bytes: None,
        };
        let msg = err.to_string();
        assert!(!msg.contains('\u{2028}'), "LS must be sanitized: {msg:?}");
        assert!(!msg.contains('\u{2029}'), "PS must be sanitized: {msg:?}");
        assert!(msg.contains("before"), "printable text is kept: {msg:?}");
        assert!(msg.contains("after"), "printable text is kept: {msg:?}");
    }

    #[test]
    fn parse_display_sanitizes_control_and_bidi_injection() {
        // `Parse` messages routinely embed attacker-influenced unparsed output;
        // the one-line Display must neutralize control AND bidi controls, not
        // just truncate.
        let err = Error::Parse {
            program: "jq".into(),
            message: "bad\x1b[31m\x07token\u{202E}flip\u{2069}sep\u{2028}end".into(),
        };
        let msg = err.to_string();
        assert!(!msg.contains('\x1b'), "ESC must be sanitized: {msg:?}");
        assert!(!msg.contains('\x07'), "BEL must be sanitized: {msg:?}");
        assert!(!msg.contains('\u{202E}'), "RLO must be sanitized: {msg:?}");
        assert!(!msg.contains('\u{2069}'), "PDI must be sanitized: {msg:?}");
        assert!(!msg.contains('\u{2028}'), "LS must be sanitized: {msg:?}");
        assert!(msg.contains("bad"), "printable text is kept: {msg:?}");
        assert!(msg.contains("token"), "printable text is kept: {msg:?}");
    }

    #[test]
    fn debug_bounds_exit_streams_so_unwrap_cannot_dump_them() {
        // A derived Debug would dump both full streams into `{e:?}` /
        // `.unwrap()`. The manual Debug bounds each to a 200-byte preview.
        let huge = "x".repeat(10_000);
        let err = Error::Exit {
            program: "tool".into(),
            code: 1,
            stdout: huge.clone(),
            stderr: huge,
            stdout_bytes: None,
        };
        let dbg = format!("{err:?}");
        assert!(
            dbg.len() < 700,
            "Debug must be bounded (two 200-byte previews + struct), got {} bytes",
            dbg.len()
        );
        assert!(
            !dbg.contains(&"x".repeat(300)),
            "must not dump the full multi-KiB stream"
        );
        // The bounded preview is still present and marked as truncated.
        assert!(dbg.contains("(+9800 bytes)"), "got: {dbg}");
        // A short stream is shown verbatim (no truncation note).
        let small = Error::Exit {
            program: "tool".into(),
            code: 2,
            stdout: "hello".into(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        let dbg = format!("{small:?}");
        assert!(dbg.contains("\"hello\""), "got: {dbg}");
        assert!(
            !dbg.contains("bytes)"),
            "no truncation note for a short stream: {dbg}"
        );
    }

    #[test]
    fn debug_bounds_stdout_bytes_to_a_length_summary() {
        // `stdout_bytes` may carry the same multi-MiB payload as `stdout` (just
        // pre-decode) — Debug must summarize its length, never dump the bytes.
        let huge_bytes = vec![b'y'; 10_000];
        let err = Error::Exit {
            program: "tool".into(),
            code: 1,
            stdout: String::from_utf8_lossy(&huge_bytes).into_owned(),
            stderr: String::new(),
            stdout_bytes: Some(huge_bytes),
        };
        let dbg = format!("{err:?}");
        assert!(
            dbg.contains("stdout_bytes: Some(<10000 bytes>)"),
            "got: {dbg}"
        );
        assert!(
            !dbg.contains(&"y".repeat(300)),
            "must not dump the raw bytes: {dbg}"
        );

        let none_err = Error::Exit {
            program: "tool".into(),
            code: 1,
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        let dbg = format!("{none_err:?}");
        assert!(dbg.contains("stdout_bytes: None"), "got: {dbg}");
    }

    #[test]
    fn debug_redacts_the_path_value_in_not_found() {
        // `searched` is the PATH env value and must never appear in Debug
        // (which feeds `{e:?}` logs and `.unwrap()` panics).
        let err = Error::NotFound {
            program: "tool".into(),
            searched: Some("/secret/bin:/another/private/dir".into()),
        };
        let dbg = format!("{err:?}");
        assert!(
            !dbg.contains("/secret/bin") && !dbg.contains("/another/private/dir"),
            "PATH value must not appear in Debug: {dbg}"
        );
        assert!(
            dbg.contains("directories"),
            "should summarize as a count: {dbg}"
        );
    }

    #[test]
    fn exit_display_appends_a_bounded_diagnostic_tail() {
        // The Display stays one actionable line — program + code + the LAST
        // non-empty diagnostic line — never the full captured streams.
        let err = Error::Exit {
            program: "git".into(),
            code: 2,
            stdout: "CONFLICT (content): merge conflict in a.rs".into(),
            stderr: "warning: something\nfatal: boom\n".into(),
            stdout_bytes: None,
        };
        assert_eq!(err.to_string(), "`git` exited with code 2: fatal: boom");

        // stderr blank → the stdout-borne message (git's CONFLICT) is used.
        let err = Error::Exit {
            program: "git".into(),
            code: 2,
            stdout: "CONFLICT (content): merge conflict in a.rs".into(),
            stderr: "   ".into(),
            stdout_bytes: None,
        };
        assert_eq!(
            err.to_string(),
            "`git` exited with code 2: CONFLICT (content): merge conflict in a.rs"
        );
    }

    #[test]
    fn exit_display_with_blank_streams_has_no_trailing_colon() {
        let err = Error::Exit {
            program: "git".into(),
            code: 2,
            stdout: String::new(),
            stderr: "  \n ".into(),
            stdout_bytes: None,
        };
        assert_eq!(err.to_string(), "`git` exited with code 2");
    }

    #[test]
    fn exit_display_tail_is_capped_and_never_leaks_the_stream() {
        // A multi-KiB single-line stderr must not poison the log line: the
        // tail is cut at 200 bytes on a char boundary, with an ellipsis.
        let huge = "é".repeat(3000); // 2 bytes/char exercises the boundary
        let err = Error::Exit {
            program: "x".into(),
            code: 1,
            stdout: String::new(),
            stderr: huge,
            stdout_bytes: None,
        };
        let message = err.to_string();
        assert!(message.len() < 250, "capped, got {} bytes", message.len());
        assert!(message.ends_with('…'), "got: {message}");
        assert!(message.starts_with("`x` exited with code 1: éé"));
    }

    #[test]
    fn diagnostic_is_none_for_non_exit_variants() {
        // A timeout that captured nothing has no diagnostic (streams-bearing
        // case covered in `timeout_and_signalled_carry_diagnostic_streams`).
        let timeout = Error::Timeout {
            program: "git".into(),
            timeout: Duration::from_secs(1),
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        assert_eq!(timeout.diagnostic(), None);
        let unsupported = Error::Unsupported {
            operation: "suspend".into(),
        };
        assert_eq!(unsupported.diagnostic(), None);
        let not_ready = Error::NotReady {
            program: "server".into(),
            timeout: Duration::from_secs(10),
        };
        assert_eq!(not_ready.diagnostic(), None);
        {
            let cancelled = Error::Cancelled {
                program: "job".into(),
            };
            assert_eq!(cancelled.diagnostic(), None);
        }
        #[cfg(feature = "limits")]
        {
            let limit = Error::ResourceLimit {
                kind: crate::limits::LimitKind::Memory,
                reason: crate::limits::LimitReason::Unenforceable,
                detail: "cgroup controller delegation unavailable".into(),
            };
            assert_eq!(limit.diagnostic(), None);
        }
    }

    #[test]
    fn cancelled_display_names_the_program() {
        let err = Error::Cancelled {
            program: "long-job".into(),
        };
        assert_eq!(err.to_string(), "`long-job` was cancelled");
        // A cancellation deliberately carries no streams, so diagnostic is None.
        assert_eq!(err.diagnostic(), None);
    }

    #[test]
    fn timeout_and_signalled_carry_diagnostic_streams() {
        // A hung-then-killed tool's partial stderr is the explanation —
        // reachable via diagnostic(), and its last line tails the Display.
        let timeout = Error::Timeout {
            program: "db-migrate".into(),
            timeout: Duration::from_secs(30),
            stdout: String::new(),
            stderr: "connecting…\nwaiting for lock held by pid 4123\n".into(),
            stdout_bytes: None,
        };
        assert_eq!(
            timeout.diagnostic(),
            Some("connecting…\nwaiting for lock held by pid 4123")
        );
        assert_eq!(
            timeout.to_string(),
            "`db-migrate` timed out after 30s: waiting for lock held by pid 4123"
        );

        // stderr blank → the stdout-borne message is used (mirrors Exit).
        let signalled = Error::Signalled {
            program: "worker".into(),
            signal: Some(11),
            stdout: "processing batch 7\n".into(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        assert_eq!(signalled.diagnostic(), Some("processing batch 7"));
        assert_eq!(
            signalled.to_string(),
            "`worker` was terminated by signal 11: processing batch 7"
        );
    }

    #[test]
    fn timeout_and_signalled_debug_bounds_their_streams() {
        // Captured streams must be bounded in Debug, exactly like Exit — a
        // multi-MiB partial capture must never flood `{e:?}`.
        let huge = "x".repeat(10_000);
        let timeout = Error::Timeout {
            program: "t".into(),
            timeout: Duration::from_secs(1),
            stdout: huge.clone(),
            stderr: huge.clone(),
            stdout_bytes: None,
        };
        let dbg = format!("{timeout:?}");
        assert!(dbg.len() < 800, "Debug must be bounded, got {}", dbg.len());
        assert!(!dbg.contains(&"x".repeat(300)), "must not dump the stream");
        assert!(dbg.contains("(+9800 bytes)"), "got: {dbg}");

        let signalled = Error::Signalled {
            program: "s".into(),
            signal: None,
            stdout: huge.clone(),
            stderr: huge,
            stdout_bytes: None,
        };
        let dbg = format!("{signalled:?}");
        assert!(dbg.len() < 800, "Debug must be bounded, got {}", dbg.len());
        assert!(!dbg.contains(&"x".repeat(300)), "must not dump the stream");
    }

    #[test]
    fn parse_message_is_bounded_in_display_and_debug() {
        // The `Parse` message is caller-built and routinely embeds the full
        // unparsed output — it must be bounded like the `Exit` streams, never
        // dumped whole into a `{e}` log line or a `{e:?}` panic message.
        let huge = "x".repeat(10_000);
        let err = Error::Parse {
            program: "jq".into(),
            message: huge,
        };
        let display = err.to_string();
        assert!(
            display.len() < 300,
            "Display must be bounded, got {} bytes",
            display.len()
        );
        assert!(display.starts_with("failed to parse `jq` output: "));
        assert!(
            display.ends_with('…'),
            "truncated Display ends with ellipsis"
        );
        let dbg = format!("{err:?}");
        assert!(
            dbg.len() < 400,
            "Debug must be bounded, got {} bytes",
            dbg.len()
        );
        assert!(
            !dbg.contains(&"x".repeat(300)),
            "must not dump the full message: {dbg}"
        );
        assert!(dbg.contains("bytes)"), "truncation note present: {dbg}");

        // A short message is shown verbatim (no truncation, no ellipsis).
        let small = Error::Parse {
            program: "jq".into(),
            message: "unexpected token at line 3".into(),
        };
        assert_eq!(
            small.to_string(),
            "failed to parse `jq` output: unexpected token at line 3"
        );
        assert!(!format!("{small:?}").contains("bytes)"));
    }

    #[test]
    fn not_ready_display_names_program_and_timeout() {
        let err = Error::NotReady {
            program: "my-server".into(),
            timeout: Duration::from_secs(10),
        };
        assert_eq!(err.to_string(), "`my-server` was not ready after 10s");
    }

    #[test]
    fn unsupported_display_names_the_operation() {
        let err = Error::Unsupported {
            operation: "signal(Hup)".into(),
        };
        assert_eq!(
            err.to_string(),
            "operation `signal(Hup)` is not supported on this platform"
        );
    }

    #[cfg(feature = "limits")]
    #[test]
    fn resource_limit_display_carries_kind_and_reason() {
        use crate::limits::{LimitKind, LimitReason};

        let unsupported = Error::ResourceLimit {
            kind: LimitKind::Memory,
            reason: LimitReason::Unsupported,
            detail: "no cgroup or Job Object available".into(),
        };
        assert_eq!(
            unsupported.to_string(),
            "memory limit is not supported on this platform: no cgroup or Job Object available"
        );

        let unenforceable = Error::ResourceLimit {
            kind: LimitKind::Cpu,
            reason: LimitReason::Unenforceable,
            detail: "delegation unavailable".into(),
        };
        assert_eq!(
            unenforceable.to_string(),
            "CPU limit could not be enforced: delegation unavailable"
        );

        let invalid = Error::ResourceLimit {
            kind: LimitKind::Processes,
            reason: LimitReason::Invalid,
            detail: "max_processes must be greater than 0".into(),
        };
        assert_eq!(
            invalid.to_string(),
            "process-count limit is invalid: max_processes must be greater than 0"
        );

        // A blank detail omits the trailing colon.
        let no_detail = Error::ResourceLimit {
            kind: LimitKind::Memory,
            reason: LimitReason::Unsupported,
            detail: String::new(),
        };
        assert_eq!(
            no_detail.to_string(),
            "memory limit is not supported on this platform"
        );
    }

    #[cfg(feature = "limits")]
    #[test]
    fn resource_limit_accessors_read_kind_and_reason_without_destructuring() {
        use crate::limits::{LimitKind, LimitReason};

        let err = Error::ResourceLimit {
            kind: LimitKind::Cpu,
            reason: LimitReason::Invalid,
            detail: "boom".into(),
        };
        assert_eq!(err.limit_kind(), Some(LimitKind::Cpu));
        assert_eq!(err.limit_reason(), Some(LimitReason::Invalid));

        // Every other variant reports None for both accessors.
        let other = Error::exit("git", 1, "", "");
        assert_eq!(other.limit_kind(), None);
        assert_eq!(other.limit_reason(), None);
    }

    #[test]
    fn signalled_display_and_diagnostic() {
        let with_signal = Error::Signalled {
            program: "git".into(),
            signal: Some(9),
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        assert_eq!(with_signal.to_string(), "`git` was terminated by signal 9");
        assert_eq!(with_signal.diagnostic(), None);
        assert!(!with_signal.is_not_found());
        assert!(!with_signal.is_permission_denied());
        assert!(!with_signal.is_transient());

        let no_signal = Error::Signalled {
            program: "git".into(),
            signal: None,
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        assert_eq!(no_signal.to_string(), "`git` was terminated by a signal");
    }

    #[test]
    fn not_found_display_and_classifier() {
        let err = Error::NotFound {
            program: "my-tool".into(),
            searched: Some("/usr/bin:/usr/local/bin".into()),
        };
        // Display must NOT include the raw PATH value (env values are never
        // logged); searched is still accessible.
        let display = err.to_string();
        assert_eq!(display, "`my-tool` not found on PATH");
        assert!(
            !display.contains("/usr/bin"),
            "Display must not expose PATH value: {display}"
        );
        assert!(err.is_not_found(), "NotFound must satisfy is_not_found()");
        assert!(!err.is_permission_denied());
        assert!(!err.is_transient());
        assert_eq!(err.diagnostic(), None);
    }

    #[test]
    fn not_found_without_path_search_omits_on_path() {
        // A path-form program (or a customized PATH) is `NotFound` with
        // `searched: None` — no PATH lookup happened, so the message must not
        // claim "on PATH". Still `is_not_found()`.
        let err = Error::NotFound {
            program: "/no/such/tool".into(),
            searched: None,
        };
        assert_eq!(err.to_string(), "`/no/such/tool` not found");
        assert!(err.is_not_found());
        // The bare-name case (a real PATH search) still says "on PATH".
        let bare = Error::NotFound {
            program: "tool".into(),
            searched: Some("/usr/bin".into()),
        };
        assert_eq!(bare.to_string(), "`tool` not found on PATH");
    }

    fn spawn(kind: std::io::ErrorKind) -> Error {
        Error::Spawn {
            program: "x".into(),
            source: std::io::Error::from(kind),
        }
    }

    #[test]
    fn not_found_and_permission_denied_are_classified_on_spawn_and_io() {
        use std::io::ErrorKind::{NotFound, PermissionDenied};
        // `is_not_found()` is true ONLY for the `NotFound` variant — a
        // `Spawn`/`Io` carrying a `NotFound` io kind (e.g. a bad cwd) is not a
        // missing program, so the "not installed?" hint can't misfire.
        assert!(
            Error::NotFound {
                program: "x".into(),
                searched: None,
            }
            .is_not_found()
        );
        assert!(!spawn(NotFound).is_not_found());
        assert!(!Error::Io(std::io::Error::from(NotFound)).is_not_found());
        assert!(!spawn(NotFound).is_permission_denied());

        assert!(spawn(PermissionDenied).is_permission_denied());
        assert!(!spawn(PermissionDenied).is_not_found());
        // Neither permanent failure counts as transient.
        assert!(!spawn(NotFound).is_transient());
        assert!(!spawn(PermissionDenied).is_transient());
    }

    #[test]
    fn transient_kinds_are_classified() {
        // ExecutableFileBusy is built straight from the kind (no raw errno) —
        // the classifier must recognize it by `ErrorKind` alone.
        for kind in [
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::ResourceBusy,
            std::io::ErrorKind::ExecutableFileBusy,
        ] {
            assert!(spawn(kind).is_transient(), "{kind:?} should be transient");
            assert!(
                Error::Io(std::io::Error::from(kind)).is_transient(),
                "{kind:?} (Io) should be transient"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn etxtbsy_is_transient_on_unix() {
        let err = Error::Spawn {
            program: "busy".into(),
            source: std::io::Error::from_raw_os_error(libc::ETXTBSY),
        };
        assert!(err.is_transient());
        assert!(!err.is_not_found() && !err.is_permission_denied());
    }

    #[cfg(windows)]
    #[test]
    fn sharing_and_lock_violations_are_transient_on_windows() {
        for code in [32, 33] {
            let err = Error::Spawn {
                program: "locked".into(),
                source: std::io::Error::from_raw_os_error(code),
            };
            assert!(
                err.is_transient(),
                "raw os error {code} should be transient"
            );
        }
    }

    #[test]
    fn classifiers_are_false_for_non_io_variants() {
        // A tool's non-zero exit is never an io-level classification (its
        // retryability is the caller's domain), and Timeout is excluded too.
        let exit = Error::Exit {
            program: "git".into(),
            code: 128,
            stdout: String::new(),
            stderr: "could not resolve host".into(),
            stdout_bytes: None,
        };
        assert!(!exit.is_not_found() && !exit.is_permission_denied() && !exit.is_transient());
        let timeout = Error::Timeout {
            program: "x".into(),
            timeout: Duration::from_secs(1),
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        assert!(
            !timeout.is_transient(),
            "Timeout is excluded from is_transient by design"
        );
    }

    #[test]
    fn spawn_not_found_stdin_and_parse_constructors_build_the_expected_variant() {
        let spawn = Error::spawn("git", std::io::Error::from_raw_os_error(2));
        match spawn {
            Error::Spawn { program, source } => {
                assert_eq!(program, "git");
                assert_eq!(source.raw_os_error(), Some(2));
            }
            other => panic!("expected Error::Spawn, got {other:?}"),
        }

        let not_found = Error::not_found("my-tool", Some("/usr/bin".into()));
        assert!(matches!(
            &not_found,
            Error::NotFound { program, searched }
                if program == "my-tool" && searched.as_deref() == Some("/usr/bin")
        ));

        let stdin = Error::stdin("git", std::io::Error::from_raw_os_error(32));
        match stdin {
            Error::Stdin { program, source } => {
                assert_eq!(program, "git");
                assert_eq!(source.raw_os_error(), Some(32));
            }
            other => panic!("expected Error::Stdin, got {other:?}"),
        }

        let parse = Error::parse("git", "unexpected token");
        assert!(matches!(
            &parse,
            Error::Parse { program, message }
                if program == "git" && message == "unexpected token"
        ));
    }
}
