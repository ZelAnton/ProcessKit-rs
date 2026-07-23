//! The crate's error type.

use std::fmt;
use std::time::Duration;

/// The boxed error a **fallible control predicate** may return — a
/// [`Supervisor`](crate::Supervisor)'s `try_*` twin
/// ([`try_stop_when`](crate::Supervisor::try_stop_when),
/// [`try_give_up_when`](crate::Supervisor::try_give_up_when),
/// [`try_health_check`](crate::Supervisor::try_health_check)) or a
/// [`ScriptedRunner::try_when`](crate::testing::ScriptedRunner::try_when). Carried
/// verbatim as the source of an [`ErrorReason::Predicate`]. Crate-internal alias;
/// the public predicate setters accept any `E: Into<Box<dyn Error + Send + Sync>>`
/// and box it into this shape.
pub(crate) type PredicateError = Box<dyn std::error::Error + Send + Sync>;

/// The structured failure mode behind an [`Error`] — the enum you reach through
/// [`Error::reason`].
///
/// Spawn failures, a non-zero exit ([`Exit`](ErrorReason::Exit)), timeouts, and IO
/// errors fold into one structured enum, so callers can pattern-match on the
/// failure mode instead of parsing strings. It is carried behind a pointer-sized
/// [`Error`] wrapper (`Box<ErrorReason>`), so a `Result<T, Error>` never inlines
/// the largest variant's captured streams — match on it via
/// `err.reason()`.
///
/// `Debug` is **manual, not derived**: the [`Exit`](ErrorReason::Exit) variant
/// carries both captured streams in full, and a derived `Debug` would dump them
/// — potentially multi-MiB — into a `{e:?}` log line or an `.unwrap()` panic
/// message. The manual impl bounds each stream to a 200-byte preview (mirroring
/// the [`Display`](std::fmt::Display) tail cap) and redacts
/// [`NotFound`](ErrorReason::NotFound)'s
/// `searched` (the `PATH` env value) to a directory count, honoring the crate's
/// "never log environment values" rule. The exact streams remain reachable via
/// the public fields.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum ErrorReason {
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
    /// variant and [`is_not_found`](ErrorReason::is_not_found) classifies it.
    ///
    /// Distinct from [`Spawn`](ErrorReason::Spawn), which covers OS-level failures
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
    /// from [`Spawn`](ErrorReason::Spawn) / [`NotFound`](ErrorReason::NotFound) so a wrapper
    /// that treats "tool not installed" as an *optional* dependency does not
    /// silently swallow a stale cassette as an absent tool.
    ///
    /// [`is_not_found`](ErrorReason::is_not_found) returns `false` for this variant.
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
        /// Read via [`ErrorReason::stdout_bytes`].
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
    /// [`Exit`](ErrorReason::Exit) — so a log line stays actionable without dumping
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
        /// path captured raw bytes — see [`Exit`](ErrorReason::Exit)'s `stdout_bytes`
        /// field for the full contract. `None` on the text path, or when the
        /// producing path captured nothing (a streaming probe). Read via
        /// [`ErrorReason::stdout_bytes`].
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
        /// Total raw bytes read from the relevant output pipe (retained +
        /// dropped), including line terminators such as LF and CRLF and bytes
        /// that were invalid UTF-8 before decoding. This uses the same raw
        /// pipe-byte accounting exposed by
        /// [`stdout_bytes_seen`](crate::RunningProcess::stdout_bytes_seen)
        /// and [`stderr_bytes_seen`](crate::RunningProcess::stderr_bytes_seen).
        total_bytes: usize,
    },

    /// A readiness probe ([`RunningProcess::wait_for_line`],
    /// [`wait_for_port`](crate::RunningProcess::wait_for_port),
    /// [`wait_for_socket`](crate::RunningProcess::wait_for_socket),
    /// [`wait_for`](crate::RunningProcess::wait_for)) did not pass within its
    /// deadline — the line never appeared, the port or Unix socket never
    /// accepted, the check never returned `true`, or the child exited before
    /// becoming ready.
    ///
    /// Distinct from [`Timeout`](ErrorReason::Timeout): a probe deadline is separate
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
    /// full, so — like the [`Exit`](ErrorReason::Exit) streams — both `Display` and
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
        /// fields (see [`Parse`](ErrorReason::Parse)'s `message`) in `Display`/`Debug`.
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
    /// Asymmetric with [`Timeout`](ErrorReason::Timeout) by design: a timeout is
    /// *captured* (`ProcessResult::timed_out`) on the non-checking paths,
    /// whereas a cancellation is **always** raised on every consuming path.
    /// When a run both times out and is cancelled, cancellation wins (it is
    /// checked first).
    ///
    /// Unlike [`Timeout`](ErrorReason::Timeout) / [`Signalled`](ErrorReason::Signalled),
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
    /// else `None`. On **Windows** a killed process reports [`Exit`](ErrorReason::Exit)
    /// with a platform code, never this — a live `Signalled` cannot occur there; it
    /// arises only from a [`ScriptedRunner`](crate::testing::ScriptedRunner) or a
    /// `record`-feature cassette replay, which report `Signalled(None)` to mirror
    /// Unix.
    ///
    /// Distinct from [`Exit`](ErrorReason::Exit): a signal-terminated run has no exit
    /// code to check — it is always a failure. Produced by
    /// [`ensure_success`](crate::ProcessResult::ensure_success) and the
    /// `require_code` path when the outcome is
    /// [`Outcome::Signalled`](crate::Outcome::Signalled).
    ///
    /// Carries whatever the run captured before the signal killed it — a
    /// crashing tool's partial stderr is often the diagnostic — reachable via
    /// [`diagnostic`](Self::diagnostic) and the public fields. The one-line
    /// `Display` appends the bounded diagnostic tail, like [`Exit`](ErrorReason::Exit).
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
        /// path captured raw bytes — see [`Exit`](ErrorReason::Exit)'s `stdout_bytes`
        /// field for the full contract. `None` on the text path. Read via
        /// [`ErrorReason::stdout_bytes`].
        stdout_bytes: Option<Vec<u8>>,
    },

    /// The child ran but feeding its standard input failed for a reason other
    /// than the routine broken pipe.
    ///
    /// This is raised by the consuming paths **only when the
    /// run otherwise succeeded** — a non-zero [`Exit`](ErrorReason::Exit), a
    /// [`Signalled`](ErrorReason::Signalled), or a [`Timeout`](ErrorReason::Timeout) is the
    /// "realer" failure and wins (the stdin error is then dropped). A broken
    /// pipe (`EPIPE` / `ERROR_BROKEN_PIPE` — the child closing stdin before
    /// reading all of it) is routine and **never** surfaces. Diagnoses a
    /// silently-truncated input the otherwise-successful child may have acted on.
    /// The stdin source ([`Command::stdin`](crate::Command::stdin))
    /// is written on a background task; this carries that task's failure.
    ///
    /// The io-level classifiers ([`is_transient`](ErrorReason::is_transient),
    /// [`is_not_found`](ErrorReason::is_not_found),
    /// [`is_permission_denied`](ErrorReason::is_permission_denied)) deliberately return
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
    /// [`Spawn`](ErrorReason::Spawn) / [`NotFound`](ErrorReason::NotFound)).
    ///
    /// There is **deliberately no blanket `From<std::io::Error>`**: the
    /// crate never lets an arbitrary foreign `io::Error` fall into this variant
    /// via `?`. Every `Io` is constructed explicitly at a known site, so the
    /// io-level classifiers ([`is_transient`](ErrorReason::is_transient),
    /// [`is_permission_denied`](ErrorReason::is_permission_denied)) only ever see an
    /// IO error the crate itself produced — never an unrelated one a caller's
    /// `?` happened to route through here.
    #[error(transparent)]
    Io(std::io::Error),

    /// A **fallible control predicate returned an error**, aborting the operation
    /// instead of yielding a verdict.
    ///
    /// Produced by the `try_*` control-predicate twins — a
    /// [`Supervisor`](crate::Supervisor)'s
    /// [`try_stop_when`](crate::Supervisor::try_stop_when),
    /// [`try_give_up_when`](crate::Supervisor::try_give_up_when), or
    /// [`try_health_check`](crate::Supervisor::try_health_check), or a
    /// [`ScriptedRunner::try_when`](crate::testing::ScriptedRunner::try_when) —
    /// when the caller-supplied predicate returns `Err` rather than `Ok(bool)`.
    /// The predicate's own error is carried **verbatim** as the [`source`], so a
    /// wrapper (e.g. a language binding whose callback raised) recovers exactly
    /// what its predicate produced rather than a fabricated stop/continue verdict.
    ///
    /// The outcome is deliberately **distinct** from a normal predicate-driven
    /// stop: a predicate that returns `Ok(true)` ends supervision with a benign
    /// [`SupervisionOutcome`](crate::SupervisionOutcome), whereas one that returns
    /// `Err` surfaces this error to the caller. Classifies as
    /// [`ErrorKind::Predicate`].
    ///
    /// [`source`]: std::error::Error::source
    #[error("the `{predicate}` control predicate returned an error: {source}")]
    #[non_exhaustive]
    Predicate {
        /// Which control predicate failed — a stable identifier: `"stop_when"`,
        /// `"give_up_when"`, `"health_check"` (the [`Supervisor`](crate::Supervisor)
        /// twins), or `"when"` (the [`ScriptedRunner`](crate::testing::ScriptedRunner)
        /// twin). A diagnostic name for the message, not a value matched on —
        /// route on [`kind`](Self::kind)/[`ErrorKind::Predicate`] instead.
        predicate: &'static str,
        /// The error the predicate returned, carried verbatim.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// The **kind** of an [`Error`] — a total, compact classification of the failure
/// into one bucket per operational disposition, reached through
/// [`Error::kind`] / [`ErrorReason::kind`].
///
/// Where [`ErrorReason`] is the *structured* failure mode (every field of every
/// variant), `ErrorKind` is the *routing* classification a consumer needs when it
/// maps failures onto its **own** shape — a CLI folding each disposition into a
/// distinct process exit code, a cross-language binding raising a matching
/// exception class, a router picking a retry policy. It is **total**: every
/// [`ErrorReason`] variant — present and future — maps to exactly one kind, and
/// the mapping is an exhaustive `match` inside the crate (no catch-all), so a new
/// `ErrorReason` variant cannot ship without a deliberate kind decision.
///
/// It is deliberately **coarser** than [`ErrorReason`] and is **not** a
/// replacement for matching the reason when you need the details: read
/// [`Error::reason`] for the exit code, the captured streams, the timeout
/// duration, the `PATH` searched, and so on. `kind()` answers "which category of
/// failure is this?"; `reason()` answers "what exactly happened?".
///
/// The shape mirrors [`std::io::Error`] / [`std::io::ErrorKind`]: a rich error
/// carrying an open-ended set of coarse kinds (hence [`Other`](ErrorKind::Other)
/// and `#[non_exhaustive]`). A downstream `match` on `ErrorKind` must therefore
/// carry a catch-all arm — prefer that to enumerating every kind, so a future
/// kind routes somewhere sane instead of breaking your build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The program could not be located — from [`ErrorReason::NotFound`]. The
    /// twin of [`Error::is_not_found`]; the "is it installed?" bucket.
    NotFound,
    /// The program was located but the OS refused to start it, for a reason
    /// other than a permission denial — from a non-`PermissionDenied`
    /// [`ErrorReason::Spawn`] (a bad working directory, a `.cmd`/`.bat` needing
    /// `cmd.exe`, a transient `ETXTBSY`/lock, …).
    Spawn,
    /// A permission denial at the spawn/IO layer — the `PermissionDenied` subset
    /// of [`ErrorReason::Spawn`] / [`ErrorReason::Io`]. The twin of
    /// [`Error::is_permission_denied`]; split out of [`Spawn`](ErrorKind::Spawn)
    /// / [`Other`](ErrorKind::Other) because an ACL/executable-bit problem is a
    /// distinct operator action (fix permissions) from a generic launch or IO
    /// failure.
    PermissionDenied,
    /// A requested resource limit could not be enforced — from
    /// [`ErrorReason::ResourceLimit`] (`limits` feature). Gated exactly like the
    /// variant it classifies, so a `--no-default-features` build has neither.
    #[cfg(feature = "limits")]
    ResourceLimit,
    /// An operation is unsupported by the active containment mechanism on this
    /// platform — from [`ErrorReason::Unsupported`] (e.g. any `Signal` but `Kill`
    /// on Windows Job Objects).
    Unsupported,
    /// The run exceeded its [`Command::timeout`](crate::Command::timeout) and was
    /// killed — from [`ErrorReason::Timeout`]. The twin of [`Error::is_timeout`].
    /// A readiness-probe deadline ([`ErrorReason::NotReady`]) is **not** this — it
    /// never kills the child — and classifies as [`Other`](ErrorKind::Other),
    /// matching [`is_timeout`](Error::is_timeout)'s scoping.
    Timeout,
    /// The run was deliberately cancelled via its
    /// [`CancellationToken`](crate::Command::cancel_on) — from
    /// [`ErrorReason::Cancelled`]. The twin of [`Error::is_cancelled`]; a
    /// caller-initiated stop, never retried.
    Cancelled,
    /// A fallible control predicate returned an error — from
    /// [`ErrorReason::Predicate`]. Its own routing bucket, distinct from
    /// [`Other`](ErrorKind::Other): the failure originated in the caller's own
    /// `try_*` control predicate (a [`Supervisor`](crate::Supervisor) twin or
    /// [`ScriptedRunner::try_when`](crate::testing::ScriptedRunner::try_when)), so
    /// a wrapper routing failures onto its own shape (a language binding raising a
    /// matching exception, say) can tell "the callback raised" apart from a
    /// backend/IO failure without matching the reason variant. The predicate's
    /// verbatim error is on the reason
    /// ([`ErrorReason::Predicate::source`](std::error::Error::source)).
    Predicate,
    /// The process ran to completion but exited non-zero — from
    /// [`ErrorReason::Exit`]. The exit code itself is on the reason
    /// ([`Error::code`]).
    Exit,
    /// The process was killed by a signal (**Unix**, or a modelled
    /// double/cassette) — from [`ErrorReason::Signalled`]. The twin of
    /// [`Error::is_signalled`]; the signal number, when known, is on the reason
    /// ([`Error::signal`]).
    Signalled,
    /// The catch-all IO/other bucket — every remaining [`ErrorReason`] variant
    /// that is not one of the categories above:
    /// [`CassetteMiss`](ErrorReason::CassetteMiss),
    /// [`Parse`](ErrorReason::Parse), [`NotReady`](ErrorReason::NotReady),
    /// [`OutputTooLarge`](ErrorReason::OutputTooLarge),
    /// [`Stdin`](ErrorReason::Stdin), and a non-`PermissionDenied`
    /// [`Io`](ErrorReason::Io). Mirrors [`std::io::ErrorKind::Other`]: a genuine
    /// but uncategorized backend/IO failure. Read [`Error::reason`] to tell them
    /// apart when it matters.
    Other,
}

impl ErrorKind {
    /// This kind's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"not_found"`, `"permission_denied"`, `"exit"`, …)
    /// that is part of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per kind instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** kind gets a **new**
    /// identifier, and an existing identifier is **never renamed** without a
    /// major release.
    ///
    /// There is deliberately **no** `from_name` inverse: an `ErrorKind` is a
    /// classification the crate *reports*, never one supplied back to it (the
    /// same asymmetry as [`Outcome::name`](crate::Outcome::name)).
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new kind is a compile error here, so it can never
        // silently ship without a stable identifier.
        match self {
            ErrorKind::NotFound => "not_found",
            ErrorKind::Spawn => "spawn",
            ErrorKind::PermissionDenied => "permission_denied",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::Predicate => "predicate",
            ErrorKind::Exit => "exit",
            ErrorKind::Signalled => "signalled",
            ErrorKind::Other => "other",
            #[cfg(feature = "limits")]
            ErrorKind::ResourceLimit => "resource_limit",
        }
    }
}

/// The overflow counters carried by an
/// [`OutputTooLarge`](ErrorReason::OutputTooLarge) failure, read through
/// [`Error::output_overflow`] / [`ErrorReason::output_overflow`] without
/// destructuring the `#[non_exhaustive]` variant.
///
/// A single grouped snapshot rather than four scalar accessors, because the two
/// ceilings are themselves `Option` (`None` = that axis had no cap): a bare
/// `Option<usize>` accessor could not tell "not an `OutputTooLarge`" apart from
/// "`OutputTooLarge` with no line cap". You only hold an `OutputOverflow` when
/// the error *was* an overflow, so the ceilings read as their honest `Option`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputOverflow {
    total_lines: usize,
    total_bytes: usize,
    max_lines: Option<usize>,
    max_bytes: Option<usize>,
}

impl OutputOverflow {
    /// Total lines that arrived, retained **plus** dropped —
    /// [`OutputTooLarge::total_lines`](ErrorReason::OutputTooLarge).
    pub fn total_lines(&self) -> usize {
        self.total_lines
    }

    /// Total raw bytes read from the output pipe, retained **plus** dropped,
    /// including line terminators and pre-decode invalid-UTF-8 bytes —
    /// [`OutputTooLarge::total_bytes`](ErrorReason::OutputTooLarge). The same raw
    /// pipe-byte accounting as
    /// [`stdout_bytes_seen`](crate::RunningProcess::stdout_bytes_seen) /
    /// [`stderr_bytes_seen`](crate::RunningProcess::stderr_bytes_seen).
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// The configured line ceiling
    /// ([`OutputBufferPolicy::max_lines`](crate::OutputBufferPolicy::max_lines)),
    /// or `None` when only a byte ceiling was set —
    /// [`OutputTooLarge::max_lines`](ErrorReason::OutputTooLarge).
    pub fn max_lines(&self) -> Option<usize> {
        self.max_lines
    }

    /// The configured byte ceiling
    /// ([`OutputBufferPolicy::max_bytes`](crate::OutputBufferPolicy::max_bytes)),
    /// or `None` when only a line ceiling was set —
    /// [`OutputTooLarge::max_bytes`](ErrorReason::OutputTooLarge).
    pub fn max_bytes(&self) -> Option<usize> {
        self.max_bytes
    }
}

impl ErrorReason {
    /// This failure's [`ErrorKind`] — its **total** classification into one
    /// coarse routing bucket. Every variant maps to exactly one kind through an
    /// exhaustive `match` (no catch-all), so a future variant cannot ship
    /// without a deliberate kind. The classification is *derived* from each
    /// variant's existing semantics, not invented:
    ///
    /// - [`NotFound`](ErrorReason::NotFound) → [`ErrorKind::NotFound`];
    /// - [`Spawn`](ErrorReason::Spawn) → [`ErrorKind::PermissionDenied`] when its
    ///   `source` is a `PermissionDenied`, else [`ErrorKind::Spawn`];
    /// - [`Io`](ErrorReason::Io) → [`ErrorKind::PermissionDenied`] when its inner
    ///   error is a `PermissionDenied`, else [`ErrorKind::Other`] (matching
    ///   [`is_permission_denied`](Self::is_permission_denied)'s `Spawn`/`Io`
    ///   scope);
    /// - [`Timeout`](ErrorReason::Timeout) → [`ErrorKind::Timeout`],
    ///   [`Cancelled`](ErrorReason::Cancelled) → [`ErrorKind::Cancelled`],
    ///   [`Predicate`](ErrorReason::Predicate) → [`ErrorKind::Predicate`],
    ///   [`Exit`](ErrorReason::Exit) → [`ErrorKind::Exit`],
    ///   [`Signalled`](ErrorReason::Signalled) → [`ErrorKind::Signalled`],
    ///   [`Unsupported`](ErrorReason::Unsupported) → [`ErrorKind::Unsupported`];
    /// - `ErrorReason::ResourceLimit` → `ErrorKind::ResourceLimit` (`limits`
    ///   feature; bare code spans, not intra-doc links, because the variant is
    ///   absent from a `--no-default-features` build);
    /// - [`CassetteMiss`](ErrorReason::CassetteMiss),
    ///   [`Parse`](ErrorReason::Parse), [`NotReady`](ErrorReason::NotReady),
    ///   [`OutputTooLarge`](ErrorReason::OutputTooLarge),
    ///   [`Stdin`](ErrorReason::Stdin) → [`ErrorKind::Other`].
    ///
    /// This is a *routing* answer, **not** a replacement for matching the
    /// variant: for the exit code, captured streams, timeout duration, or which
    /// limit failed, read the variant (or the payload accessors) directly.
    pub fn kind(&self) -> ErrorKind {
        // Exhaustive on purpose (no `_` arm) though the enum is
        // `#[non_exhaustive]`: within the defining crate a new variant is a
        // compile error here, forcing a deliberate kind rather than silently
        // falling into a catch-all bucket.
        match self {
            ErrorReason::NotFound { .. } => ErrorKind::NotFound,
            ErrorReason::Spawn { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    ErrorKind::PermissionDenied
                } else {
                    ErrorKind::Spawn
                }
            }
            ErrorReason::Io(source) => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    ErrorKind::PermissionDenied
                } else {
                    ErrorKind::Other
                }
            }
            ErrorReason::Timeout { .. } => ErrorKind::Timeout,
            ErrorReason::Cancelled { .. } => ErrorKind::Cancelled,
            ErrorReason::Predicate { .. } => ErrorKind::Predicate,
            ErrorReason::Exit { .. } => ErrorKind::Exit,
            ErrorReason::Signalled { .. } => ErrorKind::Signalled,
            ErrorReason::Unsupported { .. } => ErrorKind::Unsupported,
            ErrorReason::CassetteMiss { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Stdin { .. } => ErrorKind::Other,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => ErrorKind::ResourceLimit,
        }
    }

    /// The best human-facing message for a failed run, trimmed of surrounding
    /// whitespace: captured standard error if it carries text, otherwise the
    /// captured standard output (where `git` puts `CONFLICT …` and `git commit`
    /// puts `nothing to commit`). Covers the variants that capture streams — a
    /// non-zero [`Exit`](ErrorReason::Exit), a [`Timeout`](ErrorReason::Timeout) (the partial
    /// output of a hung-then-killed tool), and a [`Signalled`](ErrorReason::Signalled)
    /// crash. Returns `None` when there is no captured output to show — a silent
    /// run (both streams blank) or a variant that carries none
    /// ([`Spawn`](ErrorReason::Spawn), [`Cancelled`](ErrorReason::Cancelled),
    /// [`Parse`](ErrorReason::Parse), [`Io`](ErrorReason::Io)) — so a caller can fall back to
    /// the [`Display`](std::fmt::Display) message. For the raw, untrimmed stream
    /// match on the variant's fields directly.
    pub fn diagnostic(&self) -> Option<&str> {
        // Exhaustive on purpose: a future stream-carrying variant must add itself
        // here rather than fall through a `_ => None` and be invisible to
        // `diagnostic()`. `#[non_exhaustive]` only constrains downstream matches.
        match self {
            ErrorReason::Exit { stdout, stderr, .. }
            | ErrorReason::Timeout { stdout, stderr, .. }
            | ErrorReason::Signalled { stdout, stderr, .. } => exit_diagnostic(stdout, stderr),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// The captured standard output, for the variants that carry a stream — a
    /// non-zero [`Exit`](ErrorReason::Exit), a [`Timeout`](ErrorReason::Timeout) (partial
    /// output before the kill), or a [`Signalled`](ErrorReason::Signalled) crash —
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
    /// stream-bearing variant ([`Exit`](ErrorReason::Exit) / [`Timeout`](ErrorReason::Timeout)
    /// / [`Signalled`](ErrorReason::Signalled)) produced by a checking verb built over
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
            ErrorReason::Exit { stdout_bytes, .. }
            | ErrorReason::Timeout { stdout_bytes, .. }
            | ErrorReason::Signalled { stdout_bytes, .. } => stdout_bytes.as_deref(),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
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
    /// ([`Exit`](ErrorReason::Exit) / [`Timeout`](ErrorReason::Timeout) /
    /// [`Signalled`](ErrorReason::Signalled)), `None` for the rest — the single
    /// exhaustive match the public stream accessors above derive from.
    fn streams(&self) -> Option<(&str, &str)> {
        // Exhaustive on purpose (like `diagnostic`/`io_source`): a future
        // stream-carrying variant must add itself here, not fall through a `_`.
        match self {
            ErrorReason::Exit { stdout, stderr, .. }
            | ErrorReason::Timeout { stdout, stderr, .. }
            | ErrorReason::Signalled { stdout, stderr, .. } => Some((stdout, stderr)),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// The program (the CLI tool) this error is attributed to — `Some` for every
    /// variant that names one (all except [`Unsupported`](ErrorReason::Unsupported),
    /// [`Io`](ErrorReason::Io), and the `limits`-only `ResourceLimit`), `None` otherwise.
    /// The [`Error`] twin of
    /// [`ProcessResult::program`](crate::ProcessResult::program): the one
    /// cross-cutting datum a wrapper routes or logs on, read without destructuring
    /// a `#[non_exhaustive]` variant.
    pub fn program(&self) -> Option<&str> {
        // Exhaustive on purpose (like `diagnostic`/`streams`/`io_source`): a future
        // program-naming variant must add itself here, not fall through a `_`.
        match self {
            ErrorReason::Spawn { program, .. }
            | ErrorReason::NotFound { program, .. }
            | ErrorReason::CassetteMiss { program }
            | ErrorReason::Exit { program, .. }
            | ErrorReason::Timeout { program, .. }
            | ErrorReason::OutputTooLarge { program, .. }
            | ErrorReason::NotReady { program, .. }
            | ErrorReason::Parse { program, .. }
            | ErrorReason::Cancelled { program }
            | ErrorReason::Signalled { program, .. }
            | ErrorReason::Stdin { program, .. } => Some(program),
            ErrorReason::Unsupported { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// Whether the **program could not be located** — it is not installed, not
    /// on `PATH`, or the given path does not resolve to an executable. True for
    /// [`NotFound`](ErrorReason::NotFound) and **only** that variant: the launch
    /// path funnels every program-not-found failure into `NotFound`, so this is
    /// the one check a caller needs to surface a "command not installed?" hint.
    ///
    /// `false` for every other variant — notably it does **not** fire for a
    /// missing or invalid working directory (a [`Spawn`](ErrorReason::Spawn) carrying
    /// [`NotFound`](std::io::ErrorKind::NotFound)/`NotADirectory`): a bad `cwd`
    /// is not a missing program, so the hint would mislead. It is also `false`
    /// for a program that *is* installed but can't be executed directly (e.g. a
    /// Windows `.cmd`/`.bat` that needs `cmd.exe` — surfaced as `Spawn`).
    pub fn is_not_found(&self) -> bool {
        matches!(self, ErrorReason::NotFound { .. })
    }

    /// Whether this is a spawn/IO **permission denial** (`EACCES`/`EPERM`): the
    /// binary isn't executable, or the OS refused the launch. True for
    /// [`Spawn`](ErrorReason::Spawn) / [`Io`](ErrorReason::Io) carrying
    /// [`PermissionDenied`](std::io::ErrorKind::PermissionDenied); `false`
    /// otherwise.
    pub fn is_permission_denied(&self) -> bool {
        self.io_source()
            .is_some_and(|e| e.kind() == std::io::ErrorKind::PermissionDenied)
    }

    /// Whether this is a **transient** spawn/IO condition a bare retry can clear
    /// — interrupted (`EINTR`), would-block (`EAGAIN`), a busy resource, a
    /// text-file-busy executable mid-write (`ETXTBSY`), or a Windows sharing/lock
    /// violation. Classifies the [`Spawn`](ErrorReason::Spawn)/[`Io`](ErrorReason::Io) IO
    /// error only.
    ///
    /// **Scope: IO/spawn-level, never exit codes.** Whether a tool's non-zero
    /// [`Exit`](ErrorReason::Exit) is retryable is domain-specific (a `git` 128 is not
    /// generically transient) — that stays the caller's call. [`Timeout`](ErrorReason::Timeout)
    /// is also excluded by design; compose it if wanted:
    /// `e.is_transient() || e.is_timeout()`.
    ///
    /// Pairs with [`Command::retry`](crate::Command::retry):
    /// `cmd.retry(3, backoff, |e| e.is_transient())`.
    pub fn is_transient(&self) -> bool {
        self.io_source().is_some_and(is_transient_io)
    }

    /// The process exit code for a non-zero [`Exit`](ErrorReason::Exit); `None` for
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
            ErrorReason::Exit { code, .. } => Some(*code),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// The signal number for a [`Signalled`](ErrorReason::Signalled) run terminated
    /// with a **known** signal (**Unix only**); `None` for every other variant and
    /// for a signal the kernel didn't expose — the [`Error`] twin of
    /// [`ProcessResult::signal`](crate::ProcessResult::signal) /
    /// [`Outcome::signal`](crate::Outcome::signal). Reads the signal off the error
    /// without destructuring the variant.
    pub fn signal(&self) -> Option<i32> {
        // Exhaustive on purpose (like `streams`/`program`): a future signal-
        // carrying variant must add itself here, not fall through a `_`.
        match self {
            ErrorReason::Signalled { signal, .. } => *signal,
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// The run deadline that elapsed for a [`Timeout`](ErrorReason::Timeout);
    /// `None` for every other variant. The payload twin of
    /// [`is_timeout`](Self::is_timeout): reads
    /// [`Timeout::timeout`](ErrorReason::Timeout) off the error without
    /// destructuring the variant.
    ///
    /// Scoped to the **run** timeout only. A readiness-probe deadline
    /// ([`NotReady`](ErrorReason::NotReady)) is a separate clock that never kills
    /// the child, so it reads `None` here — matching
    /// [`is_timeout`](Self::is_timeout)'s scoping; reach its `timeout` field
    /// directly if you need it.
    pub fn timeout_duration(&self) -> Option<Duration> {
        // Exhaustive on purpose (like `code`/`signal`): a future timeout-carrying
        // variant must add itself here, not fall through a `_`.
        match self {
            ErrorReason::Timeout { timeout, .. } => Some(*timeout),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// The overflow counters of an
    /// [`OutputTooLarge`](ErrorReason::OutputTooLarge) failure as an
    /// [`OutputOverflow`] snapshot (total lines/bytes plus the configured
    /// ceilings); `None` for every other variant. Reads the fields off the error
    /// without destructuring the `#[non_exhaustive]` variant.
    pub fn output_overflow(&self) -> Option<OutputOverflow> {
        // Exhaustive on purpose (like `code`/`signal`): a future variant must add
        // itself here, not fall through a `_`.
        match self {
            ErrorReason::OutputTooLarge {
                max_lines,
                max_bytes,
                total_lines,
                total_bytes,
                ..
            } => Some(OutputOverflow {
                total_lines: *total_lines,
                total_bytes: *total_bytes,
                max_lines: *max_lines,
                max_bytes: *max_bytes,
            }),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// The operation description of an
    /// [`Unsupported`](ErrorReason::Unsupported) failure (e.g. `"signal(Hup)"`,
    /// `"suspend"`); `None` for every other variant. Reads
    /// [`Unsupported::operation`](ErrorReason::Unsupported) off the error without
    /// destructuring the variant.
    pub fn unsupported_operation(&self) -> Option<&str> {
        // Exhaustive on purpose (like `code`/`signal`): a future variant must add
        // itself here, not fall through a `_`.
        match self {
            ErrorReason::Unsupported { operation } => Some(operation),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }

    /// Which limit a [`ResourceLimit`](ErrorReason::ResourceLimit) failure is about;
    /// `None` for every other variant. Reads the field off the error without
    /// destructuring the `#[non_exhaustive]` variant.
    #[cfg(feature = "limits")]
    pub fn limit_kind(&self) -> Option<crate::limits::LimitKind> {
        // Exhaustive on purpose (like `signal`/`program`): a future variant
        // must add itself here, not fall through a `_`.
        match self {
            ErrorReason::ResourceLimit { kind, .. } => Some(*kind),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
        }
    }

    /// Why a [`ResourceLimit`](ErrorReason::ResourceLimit) failure occurred; `None`
    /// for every other variant. Reads the field off the error without
    /// destructuring the `#[non_exhaustive]` variant.
    #[cfg(feature = "limits")]
    pub fn limit_reason(&self) -> Option<crate::limits::LimitReason> {
        // Exhaustive on purpose (like `signal`/`program`): a future variant
        // must add itself here, not fall through a `_`.
        match self {
            ErrorReason::ResourceLimit { reason, .. } => Some(*reason),
            ErrorReason::Spawn { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. }
            | ErrorReason::Io(_) => None,
        }
    }

    /// Whether the run was killed because it exceeded its
    /// [`Command::timeout`](crate::Command::timeout) — i.e. this is a
    /// [`Timeout`](ErrorReason::Timeout). First-class here so the
    /// [`is_transient`](Self::is_transient) retry-composition example can read
    /// `e.is_transient() || e.is_timeout()` rather than matching the variant by hand.
    /// The [`Error`] twin of the crate-wide deadline predicate
    /// [`ProcessResult::timed_out`](crate::ProcessResult::timed_out) (named
    /// `is_timeout` here to sit alongside the error's `is_*` predicate family).
    pub fn is_timeout(&self) -> bool {
        matches!(self, ErrorReason::Timeout { .. })
    }

    /// Whether the run was deliberately cancelled via its
    /// [`Command::cancel_on`](crate::Command::cancel_on) token — i.e. this is a
    /// [`Cancelled`](ErrorReason::Cancelled). A caller that initiated the stop can
    /// swallow it rather than log or retry it as a real failure (the same
    /// disposition [`Supervisor`](crate::Supervisor) treats as terminal), without
    /// destructuring the variant.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, ErrorReason::Cancelled { .. })
    }

    /// Whether the run was killed by a signal — i.e. this is a
    /// [`Signalled`](ErrorReason::Signalled). Distinct from [`signal`](Self::signal):
    /// this is `true` even when the kernel didn't expose a number (a Unix kill
    /// where `signal()` is `None`, or any platform's signal disposition), so it is
    /// the reliable "died by a signal?" check — the predicate twin of
    /// [`is_timeout`](Self::is_timeout) / [`is_cancelled`](Self::is_cancelled),
    /// without destructuring the variant.
    pub fn is_signalled(&self) -> bool {
        matches!(self, ErrorReason::Signalled { .. })
    }

    /// The underlying [`std::io::Error`] for the variants that carry one
    /// ([`Spawn`](ErrorReason::Spawn), [`Io`](ErrorReason::Io)) — the basis for the io-level
    /// classifiers above.
    fn io_source(&self) -> Option<&std::io::Error> {
        // Exhaustive on purpose: a future variant carrying an `io::Error` must
        // add itself here so the io-level classifiers (`is_transient`,
        // `is_permission_denied`) see it, rather than slipping through a wildcard.
        match self {
            ErrorReason::Spawn { source, .. } => Some(source),
            ErrorReason::Io(source) => Some(source),
            ErrorReason::NotFound { .. }
            | ErrorReason::CassetteMiss { .. }
            | ErrorReason::Exit { .. }
            | ErrorReason::Timeout { .. }
            | ErrorReason::OutputTooLarge { .. }
            | ErrorReason::NotReady { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::Unsupported { .. }
            | ErrorReason::Cancelled { .. }
            | ErrorReason::Signalled { .. }
            | ErrorReason::Stdin { .. }
            | ErrorReason::Predicate { .. } => None,
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit { .. } => None,
        }
    }
}

/// The crate's error type: a **pointer-sized** handle to a structured
/// [`ErrorReason`].
///
/// `Error` is a thin wrapper around a boxed [`ErrorReason`] — one pointer wide
/// ([`size_of::<Error>()`](std::mem::size_of) equals a pointer) — so a
/// `Result<T, Error>` on the crate's pervasive process-launch path, and any
/// enum that embeds this one (a caller's own `vcs_core::Error`, say), stays
/// small instead of carrying the largest variant's captured streams inline. The
/// shape mirrors [`std::io::Error`] / [`std::io::ErrorKind`].
///
/// Match on the failure mode through [`reason`](Error::reason):
///
/// ```
/// # use processkit::{Error, ErrorReason};
/// # fn classify(err: &Error) {
/// match err.reason() {
///     ErrorReason::NotFound { program, .. } => eprintln!("install `{program}`?"),
///     ErrorReason::Exit { code, .. } => eprintln!("exited with {code}"),
///     _ => {}
/// }
/// # }
/// ```
///
/// or reach for the read accessors ([`code`](Error::code),
/// [`program`](Error::program), [`diagnostic`](Error::diagnostic), the `is_*`
/// predicates, …), which delegate to the inner [`ErrorReason`].
/// [`Display`](std::fmt::Display), [`Debug`](std::fmt::Debug), and
/// [`source`](std::error::Error::source) delegate too, so the manual `Debug`
/// (200-byte stream previews, `PATH` redaction, control-/bidi-sanitizing) and
/// the `thiserror` `Display` behave exactly as when matching the reason
/// directly — the wrapper adds no envelope of its own.
pub struct Error {
    reason: Box<ErrorReason>,
}

impl Error {
    /// The structured [`ErrorReason`] behind this error — the enum to `match`
    /// on for the failure mode. `Error` is a pointer-sized wrapper; this
    /// borrows the boxed reason without moving it.
    pub fn reason(&self) -> &ErrorReason {
        &self.reason
    }

    /// Consume the wrapper and hand back the **owned** [`ErrorReason`] — for when
    /// you need to move a field out of the reason (a captured stream, the owned
    /// `io::Error`) rather than borrow it via [`reason`](Self::reason), or to
    /// `match err.into_reason() { … }` and bind fields by value.
    #[must_use]
    pub fn into_reason(self) -> ErrorReason {
        *self.reason
    }

    /// This failure's [`ErrorKind`] — its total classification into one coarse
    /// routing bucket. See [`ErrorReason::kind`] for the full mapping. A routing
    /// answer, not a replacement for [`reason`](Self::reason) when you need the
    /// details.
    pub fn kind(&self) -> ErrorKind {
        self.reason.kind()
    }

    /// The best human-facing message for a failed run — see
    /// [`ErrorReason::diagnostic`].
    pub fn diagnostic(&self) -> Option<&str> {
        self.reason.diagnostic()
    }

    /// The captured standard output for the stream-bearing variants — see
    /// [`ErrorReason::stdout`].
    pub fn stdout(&self) -> Option<&str> {
        self.reason.stdout()
    }

    /// The captured standard error for the stream-bearing variants — see
    /// [`ErrorReason::stderr`].
    pub fn stderr(&self) -> Option<&str> {
        self.reason.stderr()
    }

    /// The exact captured stdout bytes, when available — see
    /// [`ErrorReason::stdout_bytes`].
    pub fn stdout_bytes(&self) -> Option<&[u8]> {
        self.reason.stdout_bytes()
    }

    /// Standard output followed by standard error, joined — see
    /// [`ErrorReason::combined`].
    pub fn combined(&self) -> Option<String> {
        self.reason.combined()
    }

    /// The program this error is attributed to — see [`ErrorReason::program`].
    pub fn program(&self) -> Option<&str> {
        self.reason.program()
    }

    /// Whether the program could not be located — see
    /// [`ErrorReason::is_not_found`].
    pub fn is_not_found(&self) -> bool {
        self.reason.is_not_found()
    }

    /// Whether this is a spawn/IO permission denial — see
    /// [`ErrorReason::is_permission_denied`].
    pub fn is_permission_denied(&self) -> bool {
        self.reason.is_permission_denied()
    }

    /// Whether this is a transient spawn/IO condition a bare retry can clear —
    /// see [`ErrorReason::is_transient`].
    pub fn is_transient(&self) -> bool {
        self.reason.is_transient()
    }

    /// The process exit code for a non-zero [`Exit`](ErrorReason::Exit) — see
    /// [`ErrorReason::code`].
    pub fn code(&self) -> Option<i32> {
        self.reason.code()
    }

    /// The signal number for a signal-terminated run — see
    /// [`ErrorReason::signal`].
    pub fn signal(&self) -> Option<i32> {
        self.reason.signal()
    }

    /// The run deadline that elapsed for a timeout — see
    /// [`ErrorReason::timeout_duration`].
    pub fn timeout_duration(&self) -> Option<Duration> {
        self.reason.timeout_duration()
    }

    /// The overflow counters of an output-too-large failure — see
    /// [`ErrorReason::output_overflow`].
    pub fn output_overflow(&self) -> Option<OutputOverflow> {
        self.reason.output_overflow()
    }

    /// The operation description of an unsupported-operation failure — see
    /// [`ErrorReason::unsupported_operation`].
    pub fn unsupported_operation(&self) -> Option<&str> {
        self.reason.unsupported_operation()
    }

    /// Which limit a [`ResourceLimit`](ErrorReason::ResourceLimit) failure is
    /// about — see [`ErrorReason::limit_kind`].
    #[cfg(feature = "limits")]
    pub fn limit_kind(&self) -> Option<crate::limits::LimitKind> {
        self.reason.limit_kind()
    }

    /// Why a [`ResourceLimit`](ErrorReason::ResourceLimit) failure occurred —
    /// see [`ErrorReason::limit_reason`].
    #[cfg(feature = "limits")]
    pub fn limit_reason(&self) -> Option<crate::limits::LimitReason> {
        self.reason.limit_reason()
    }

    /// Whether the run was killed for exceeding its timeout — see
    /// [`ErrorReason::is_timeout`].
    pub fn is_timeout(&self) -> bool {
        self.reason.is_timeout()
    }

    /// Whether the run was deliberately cancelled — see
    /// [`ErrorReason::is_cancelled`].
    pub fn is_cancelled(&self) -> bool {
        self.reason.is_cancelled()
    }

    /// Whether the run was killed by a signal — see
    /// [`ErrorReason::is_signalled`].
    pub fn is_signalled(&self) -> bool {
        self.reason.is_signalled()
    }
}

impl From<ErrorReason> for Error {
    fn from(reason: ErrorReason) -> Self {
        Error {
            reason: Box::new(reason),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.reason, f)
    }
}

/// `Debug` delegates to the inner [`ErrorReason`]'s manual `Debug`, so `{e:?}`
/// and `.unwrap()` panic messages keep the same bounded stream previews and
/// `PATH` redaction — the wrapper prints exactly the reason, no envelope.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.reason, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.reason.source()
    }
}

/// Compile-time guard that `Error` stays one pointer wide. A regression here
/// (inlining a variant's fields back into the wrapper) would re-bloat every
/// `Result<T, Error>` and re-trigger the `result_large_err` /
/// `large_enum_variant` lints this `Box<ErrorReason>` wrapper exists to silence.
const _: () = assert!(
    std::mem::size_of::<Error>() == std::mem::size_of::<*const ()>(),
    "Error must remain pointer-sized (a Box<ErrorReason> handle)"
);

impl Error {
    /// Construct an [`Exit`](ErrorReason::Exit) — a `#[doc(hidden)]` convenience for
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
        ErrorReason::Exit {
            program: program.into(),
            code,
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_bytes: None,
        }
        .into()
    }

    /// Construct a [`Timeout`](ErrorReason::Timeout) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn timeout(
        program: impl Into<String>,
        timeout: Duration,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Self {
        ErrorReason::Timeout {
            program: program.into(),
            timeout,
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_bytes: None,
        }
        .into()
    }

    /// Construct a [`Signalled`](ErrorReason::Signalled) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn signalled(
        program: impl Into<String>,
        signal: Option<i32>,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Self {
        ErrorReason::Signalled {
            program: program.into(),
            signal,
            stdout: stdout.into(),
            stderr: stderr.into(),
            stdout_bytes: None,
        }
        .into()
    }

    /// Construct a [`Spawn`](ErrorReason::Spawn) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn spawn(program: impl Into<String>, source: std::io::Error) -> Self {
        ErrorReason::Spawn {
            program: program.into(),
            source,
        }
        .into()
    }

    /// Construct a [`NotFound`](ErrorReason::NotFound) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn not_found(program: impl Into<String>, searched: Option<String>) -> Self {
        ErrorReason::NotFound {
            program: program.into(),
            searched,
        }
        .into()
    }

    /// Construct a [`Stdin`](ErrorReason::Stdin) — see [`exit`](Self::exit).
    #[doc(hidden)]
    pub fn stdin(program: impl Into<String>, source: std::io::Error) -> Self {
        ErrorReason::Stdin {
            program: program.into(),
            source,
        }
        .into()
    }

    /// Wrap a crate-internal [`std::io::Error`] as [`Io`](ErrorReason::Io).
    /// Crate-private on purpose: there is deliberately **no** blanket
    /// `From<std::io::Error>` (see [`Io`](ErrorReason::Io)), so every `Io` is
    /// built at a known site — this is the one insulated way the crate does it,
    /// including point-free `.map_err(Error::io)`.
    pub(crate) fn io(source: std::io::Error) -> Self {
        ErrorReason::Io(source).into()
    }

    /// Wrap the error a fallible control predicate returned as an
    /// [`ErrorReason::Predicate`], tagged with a stable `predicate` identifier
    /// (`"stop_when"`, `"give_up_when"`, `"health_check"`, `"when"`).
    /// Crate-private: only the crate's own predicate-consultation sites (the
    /// `Supervisor` `try_*` twins and `ScriptedRunner::try_when`) produce it.
    pub(crate) fn predicate(predicate: &'static str, source: PredicateError) -> Self {
        ErrorReason::Predicate { predicate, source }.into()
    }

    /// Construct a [`Parse`](ErrorReason::Parse) from a caller-supplied parser's own
    /// failure message. Unlike `exit`/`timeout`/`signalled`/`spawn`/`not_found`/
    /// `stdin` above (`#[doc(hidden)]` insulated constructors meant for test
    /// doubles), this one is left on the **documented public surface**: an
    /// external parser that inspects a tool's output outside this crate's own
    /// `try_parse` helpers has no other way to report a parse failure as an
    /// `ErrorReason::Parse` once the variant is `#[non_exhaustive]`, and that path is
    /// a normal production use, not just a test-doubling convenience.
    pub fn parse(program: impl Into<String>, message: impl Into<String>) -> Self {
        ErrorReason::Parse {
            program: program.into(),
            message: message.into(),
        }
        .into()
    }
}

/// Manual `Debug`: bounds the [`Exit`](ErrorReason::Exit) streams and redacts
/// the `PATH` value, so `{e:?}` / `.unwrap()` neither dumps a multi-MiB stream
/// nor logs an environment value. Every other variant mirrors what the derive
/// would print.
impl fmt::Debug for ErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorReason::Spawn { program, source } => f
                .debug_struct("Spawn")
                .field("program", program)
                .field("source", source)
                .finish(),
            ErrorReason::NotFound { program, searched } => f
                .debug_struct("NotFound")
                .field("program", program)
                // `searched` is the `PATH` env value — never rendered; summarize
                // as a directory count (`None` renders as `None`).
                .field("searched", &searched.as_deref().map(SearchedRedaction))
                .finish(),
            ErrorReason::CassetteMiss { program } => f
                .debug_struct("CassetteMiss")
                .field("program", program)
                .finish(),
            ErrorReason::Exit {
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
            ErrorReason::Timeout {
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
            ErrorReason::OutputTooLarge {
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
            ErrorReason::NotReady { program, timeout } => f
                .debug_struct("NotReady")
                .field("program", program)
                .field("timeout", timeout)
                .finish(),
            ErrorReason::Parse { program, message } => f
                .debug_struct("Parse")
                .field("program", program)
                // Caller-built, often the full unparsed output — bound it.
                .field("message", &StreamPreview(message))
                .finish(),
            #[cfg(feature = "limits")]
            ErrorReason::ResourceLimit {
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
            ErrorReason::Unsupported { operation } => f
                .debug_struct("Unsupported")
                .field("operation", operation)
                .finish(),
            ErrorReason::Cancelled { program } => f
                .debug_struct("Cancelled")
                .field("program", program)
                .finish(),
            ErrorReason::Signalled {
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
            ErrorReason::Stdin { program, source } => f
                .debug_struct("Stdin")
                .field("program", program)
                .field("source", source)
                .finish(),
            ErrorReason::Io(source) => f.debug_tuple("Io").field(source).finish(),
            ErrorReason::Predicate { predicate, source } => f
                .debug_struct("Predicate")
                .field("predicate", predicate)
                // The caller's own error — printed like any other `#[source]`
                // (`Spawn`/`Stdin`/`Io`), not a captured stream, so no length cap.
                .field("source", source)
                .finish(),
        }
    }
}

/// `Debug` for a captured stream, bounded to a 200-byte char-boundary preview
/// with a `(+N bytes)` note — the [`Exit`](ErrorReason::Exit) streams can be multi-MiB
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

/// `Debug` for the `stdout_bytes` field of [`Exit`](ErrorReason::Exit) /
/// [`Timeout`](ErrorReason::Timeout) / [`Signalled`](ErrorReason::Signalled): never dumps
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

/// `Debug` for [`NotFound`](ErrorReason::NotFound)'s `searched`: the `PATH` value is an
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

/// Builds the [`ErrorReason::Io`] raised when a capture verb is called on a command
/// whose stdout was not piped (`Command::stdout` set to `Inherit`/`Null`, or
/// redirected to a file). The
/// live runner (`RunningProcess::ensure_stdout_capturable`) and both test
/// doubles (`ScriptedRunner::output_string`, the `RecordReplayRunner` replay
/// branch) must reject this identically, so they all route through this one
/// constructor instead of hand-rolling the message and `ErrorKind`.
pub(crate) fn stdout_not_piped_error(program: &str) -> Error {
    ErrorReason::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "`{program}`: stdout is not piped (Command::stdout was set to \
             Inherit/Null or stdout_file), so the capture verbs have nothing to read — \
             use StdioMode::Piped to capture it"
        ),
    ))
    .into()
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
/// `detail` is bounded/sanitized like [`Parse`](ErrorReason::Parse)'s `message` — it
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
/// and the [`Parse`](ErrorReason::Parse) message head ([`display_parse`]) — so the
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
    fn error_is_pointer_sized() {
        // The whole point of the `Box<ErrorReason>` wrapper: `Error` stays one
        // pointer wide so `Result<T, Error>` never inlines the largest variant's
        // captured streams (and the `result_large_err`/`large_enum_variant` lints
        // stay quiet). A compile-time `const _` in the module also pins this; this
        // test surfaces the same guarantee in the test report.
        assert_eq!(
            std::mem::size_of::<Error>(),
            std::mem::size_of::<*const ()>(),
            "Error must remain pointer-sized (a Box<ErrorReason> handle)"
        );
        assert_eq!(
            std::mem::size_of::<crate::Result<()>>(),
            std::mem::size_of::<*const ()>(),
            "Result<(), Error> must be pointer-sized too"
        );
    }

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
        let not_ready = ErrorReason::NotReady {
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

        let cancelled = ErrorReason::Cancelled {
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
            ErrorReason::NotReady {
                program: "server".into(),
                timeout: Duration::from_secs(1),
            }
            .program(),
            Some("server")
        );
        assert_eq!(
            ErrorReason::Cancelled {
                program: "job".into()
            }
            .program(),
            Some("job")
        );
        assert_eq!(
            ErrorReason::Unsupported {
                operation: "suspend".into()
            }
            .program(),
            None
        );
        assert_eq!(
            ErrorReason::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)).program(),
            None
        );
        #[cfg(feature = "limits")]
        assert_eq!(
            ErrorReason::ResourceLimit {
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
            Error::exit("git", 2, "o", "e").reason(),
            ErrorReason::Exit { code: 2, .. }
        ));
        assert!(matches!(
            Error::timeout("git", Duration::from_secs(3), "o", "e").reason(),
            ErrorReason::Timeout { .. }
        ));
        match Error::signalled("git", None, "o", "e").into_reason() {
            ErrorReason::Signalled { signal, .. } => assert_eq!(signal, None),
            other => panic!("expected Signalled, got {other:?}"),
        }
    }

    #[test]
    fn display_tail_sanitizes_control_chars_against_terminal_injection() {
        // A hostile child's stderr last line carrying ANSI/BEL/NUL must not reach
        // a `{err}` log/terminal verbatim — control bytes become U+FFFD, while
        // printable text survives.
        let err = ErrorReason::Exit {
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
        let err = ErrorReason::Exit {
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
        let err = ErrorReason::Exit {
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
        let err = ErrorReason::Parse {
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
        let err = ErrorReason::Exit {
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
        let small = ErrorReason::Exit {
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
    fn stream_preview_truncates_on_char_boundary() {
        // Byte 200 is the second byte of `é`; the preview must back up to the
        // preceding character boundary and report every omitted byte.
        let stream = format!("{}éz", "a".repeat(DIAG_CAP - 1));
        let cut = DIAG_CAP - 1;

        assert_eq!(
            format!("{:?}", StreamPreview(&stream)),
            format!("{:?}… (+{} bytes)", &stream[..cut], stream.len() - cut)
        );
    }

    #[test]
    fn searched_redaction_filters_empty_path_segments() {
        let sep = if cfg!(windows) { ';' } else { ':' };

        assert_eq!(
            format!("{:?}", SearchedRedaction(&format!("first{sep}second{sep}"))),
            "<2 directories>"
        );
        assert_eq!(
            format!("{:?}", SearchedRedaction(&format!("first{sep}{sep}second"))),
            "<2 directories>"
        );
    }

    #[test]
    fn push_sanitized_capped_respects_byte_boundaries() {
        let mut out = String::new();
        push_sanitized_capped(&mut out, "abcd", 4);
        assert_eq!(out, "abcd");

        let mut out = String::new();
        push_sanitized_capped(&mut out, "abc", 4);
        assert_eq!(out, "abc");

        let mut out = String::new();
        push_sanitized_capped(&mut out, "abcde", 4);
        assert_eq!(out, "abcd…");

        let mut out = String::new();
        push_sanitized_capped(&mut out, "abé", 3);
        assert_eq!(out, "ab…");

        let mut out = String::new();
        push_sanitized_capped(&mut out, "\x1b", 3);
        assert_eq!(out, "\u{FFFD}");

        let mut out = String::new();
        push_sanitized_capped(&mut out, "\x1ba", 3);
        assert_eq!(out, "\u{FFFD}…");
    }
    #[test]
    fn debug_bounds_stdout_bytes_to_a_length_summary() {
        // `stdout_bytes` may carry the same multi-MiB payload as `stdout` (just
        // pre-decode) — Debug must summarize its length, never dump the bytes.
        let huge_bytes = vec![b'y'; 10_000];
        let err = ErrorReason::Exit {
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

        let none_err = ErrorReason::Exit {
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
        let err = ErrorReason::NotFound {
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
        let err = ErrorReason::Exit {
            program: "git".into(),
            code: 2,
            stdout: "CONFLICT (content): merge conflict in a.rs".into(),
            stderr: "warning: something\nfatal: boom\n".into(),
            stdout_bytes: None,
        };
        assert_eq!(err.to_string(), "`git` exited with code 2: fatal: boom");

        // stderr blank → the stdout-borne message (git's CONFLICT) is used.
        let err = ErrorReason::Exit {
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
        let err = ErrorReason::Exit {
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
        let err = ErrorReason::Exit {
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
        let timeout = ErrorReason::Timeout {
            program: "git".into(),
            timeout: Duration::from_secs(1),
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        };
        assert_eq!(timeout.diagnostic(), None);
        let unsupported = ErrorReason::Unsupported {
            operation: "suspend".into(),
        };
        assert_eq!(unsupported.diagnostic(), None);
        let not_ready = ErrorReason::NotReady {
            program: "server".into(),
            timeout: Duration::from_secs(10),
        };
        assert_eq!(not_ready.diagnostic(), None);
        {
            let cancelled = ErrorReason::Cancelled {
                program: "job".into(),
            };
            assert_eq!(cancelled.diagnostic(), None);
        }
        #[cfg(feature = "limits")]
        {
            let limit = ErrorReason::ResourceLimit {
                kind: crate::limits::LimitKind::Memory,
                reason: crate::limits::LimitReason::Unenforceable,
                detail: "cgroup controller delegation unavailable".into(),
            };
            assert_eq!(limit.diagnostic(), None);
        }
    }

    #[test]
    fn cancelled_display_names_the_program() {
        let err = ErrorReason::Cancelled {
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
        let timeout = ErrorReason::Timeout {
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
        let signalled = ErrorReason::Signalled {
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
        let timeout = ErrorReason::Timeout {
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

        let signalled = ErrorReason::Signalled {
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
        let err = ErrorReason::Parse {
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
        let small = ErrorReason::Parse {
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
        let err = ErrorReason::NotReady {
            program: "my-server".into(),
            timeout: Duration::from_secs(10),
        };
        assert_eq!(err.to_string(), "`my-server` was not ready after 10s");
    }

    #[test]
    fn unsupported_display_names_the_operation() {
        let err = ErrorReason::Unsupported {
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

        let unsupported = ErrorReason::ResourceLimit {
            kind: LimitKind::Memory,
            reason: LimitReason::Unsupported,
            detail: "no cgroup or Job Object available".into(),
        };
        assert_eq!(
            unsupported.to_string(),
            "memory limit is not supported on this platform: no cgroup or Job Object available"
        );

        let unenforceable = ErrorReason::ResourceLimit {
            kind: LimitKind::Cpu,
            reason: LimitReason::Unenforceable,
            detail: "delegation unavailable".into(),
        };
        assert_eq!(
            unenforceable.to_string(),
            "CPU limit could not be enforced: delegation unavailable"
        );

        let invalid = ErrorReason::ResourceLimit {
            kind: LimitKind::Processes,
            reason: LimitReason::Invalid,
            detail: "max_processes must be greater than 0".into(),
        };
        assert_eq!(
            invalid.to_string(),
            "process-count limit is invalid: max_processes must be greater than 0"
        );

        // A blank detail omits the trailing colon.
        let no_detail = ErrorReason::ResourceLimit {
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

        let err = ErrorReason::ResourceLimit {
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
        let with_signal = ErrorReason::Signalled {
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

        let no_signal = ErrorReason::Signalled {
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
        let err = ErrorReason::NotFound {
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
        let err = ErrorReason::NotFound {
            program: "/no/such/tool".into(),
            searched: None,
        };
        assert_eq!(err.to_string(), "`/no/such/tool` not found");
        assert!(err.is_not_found());
        // The bare-name case (a real PATH search) still says "on PATH".
        let bare = ErrorReason::NotFound {
            program: "tool".into(),
            searched: Some("/usr/bin".into()),
        };
        assert_eq!(bare.to_string(), "`tool` not found on PATH");
    }

    fn spawn(kind: std::io::ErrorKind) -> Error {
        ErrorReason::Spawn {
            program: "x".into(),
            source: std::io::Error::from(kind),
        }
        .into()
    }

    #[test]
    fn not_found_and_permission_denied_are_classified_on_spawn_and_io() {
        use std::io::ErrorKind::{NotFound, PermissionDenied};
        // `is_not_found()` is true ONLY for the `NotFound` variant — a
        // `Spawn`/`Io` carrying a `NotFound` io kind (e.g. a bad cwd) is not a
        // missing program, so the "not installed?" hint can't misfire.
        assert!(
            ErrorReason::NotFound {
                program: "x".into(),
                searched: None,
            }
            .is_not_found()
        );
        assert!(!spawn(NotFound).is_not_found());
        assert!(!ErrorReason::Io(std::io::Error::from(NotFound)).is_not_found());
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
                ErrorReason::Io(std::io::Error::from(kind)).is_transient(),
                "{kind:?} (Io) should be transient"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn etxtbsy_is_transient_on_unix() {
        let err = ErrorReason::Spawn {
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
            let err = ErrorReason::Spawn {
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
        let exit = ErrorReason::Exit {
            program: "git".into(),
            code: 128,
            stdout: String::new(),
            stderr: "could not resolve host".into(),
            stdout_bytes: None,
        };
        assert!(!exit.is_not_found() && !exit.is_permission_denied() && !exit.is_transient());
        let timeout = ErrorReason::Timeout {
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
        match spawn.into_reason() {
            ErrorReason::Spawn { program, source } => {
                assert_eq!(program, "git");
                assert_eq!(source.raw_os_error(), Some(2));
            }
            other => panic!("expected ErrorReason::Spawn, got {other:?}"),
        }

        let not_found = Error::not_found("my-tool", Some("/usr/bin".into()));
        assert!(matches!(
            not_found.reason(),
            ErrorReason::NotFound { program, searched }
                if program == "my-tool" && searched.as_deref() == Some("/usr/bin")
        ));

        let stdin = Error::stdin("git", std::io::Error::from_raw_os_error(32));
        match stdin.into_reason() {
            ErrorReason::Stdin { program, source } => {
                assert_eq!(program, "git");
                assert_eq!(source.raw_os_error(), Some(32));
            }
            other => panic!("expected ErrorReason::Stdin, got {other:?}"),
        }

        let parse = Error::parse("git", "unexpected token");
        assert!(matches!(
            parse.reason(),
            ErrorReason::Parse { program, message }
                if program == "git" && message == "unexpected token"
        ));
    }

    fn output_too_large() -> ErrorReason {
        ErrorReason::OutputTooLarge {
            program: "noisy".into(),
            max_lines: Some(100),
            max_bytes: None,
            total_lines: 250,
            total_bytes: 9001,
        }
    }

    #[test]
    fn kind_pins_the_classification_for_every_error_reason_variant() {
        use std::io::ErrorKind as IoKind;

        // One pin per source `ErrorReason` variant — the total classification is
        // derived from each variant's existing semantics, never invented.
        assert_eq!(
            ErrorReason::NotFound {
                program: "x".into(),
                searched: None,
            }
            .kind(),
            ErrorKind::NotFound
        );
        // Spawn splits on its io source: a permission denial is its own kind, any
        // other launch failure (here a bad cwd -> NotFound io kind) is `Spawn`.
        assert_eq!(spawn(IoKind::NotFound).kind(), ErrorKind::Spawn);
        assert_eq!(
            spawn(IoKind::PermissionDenied).kind(),
            ErrorKind::PermissionDenied
        );
        // Io splits the same way: a permission denial is `PermissionDenied`, any
        // other crate-internal IO error is the catch-all `Other`.
        assert_eq!(
            ErrorReason::Io(std::io::Error::from(IoKind::PermissionDenied)).kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            ErrorReason::Io(std::io::Error::from(IoKind::BrokenPipe)).kind(),
            ErrorKind::Other
        );
        assert_eq!(
            ErrorReason::CassetteMiss {
                program: "x".into()
            }
            .kind(),
            ErrorKind::Other
        );
        assert_eq!(Error::exit("git", 1, "", "").kind(), ErrorKind::Exit);
        assert_eq!(
            Error::timeout("git", Duration::from_secs(1), "", "").kind(),
            ErrorKind::Timeout
        );
        assert_eq!(output_too_large().kind(), ErrorKind::Other);
        assert_eq!(
            ErrorReason::NotReady {
                program: "server".into(),
                timeout: Duration::from_secs(1),
            }
            .kind(),
            ErrorKind::Other
        );
        assert_eq!(
            ErrorReason::Parse {
                program: "jq".into(),
                message: "boom".into(),
            }
            .kind(),
            ErrorKind::Other
        );
        assert_eq!(
            ErrorReason::Unsupported {
                operation: "suspend".into(),
            }
            .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            ErrorReason::Cancelled {
                program: "job".into(),
            }
            .kind(),
            ErrorKind::Cancelled
        );
        assert_eq!(
            Error::signalled("git", Some(9), "", "").kind(),
            ErrorKind::Signalled
        );
        assert_eq!(
            Error::stdin("git", std::io::Error::from(IoKind::Other)).kind(),
            ErrorKind::Other
        );

        #[cfg(feature = "limits")]
        assert_eq!(
            ErrorReason::ResourceLimit {
                kind: crate::limits::LimitKind::Memory,
                reason: crate::limits::LimitReason::Unsupported,
                detail: "no container".into(),
            }
            .kind(),
            ErrorKind::ResourceLimit
        );
    }

    #[test]
    fn kind_matches_what_every_crate_error_constructor_produces() {
        use std::io::ErrorKind as IoKind;

        // The crate's own error factories each land in the expected род.
        assert_eq!(Error::exit("git", 2, "o", "e").kind(), ErrorKind::Exit);
        assert_eq!(
            Error::timeout("git", Duration::from_secs(3), "o", "e").kind(),
            ErrorKind::Timeout
        );
        assert_eq!(
            Error::signalled("git", None, "o", "e").kind(),
            ErrorKind::Signalled
        );
        assert_eq!(
            Error::spawn("git", std::io::Error::from(IoKind::NotFound)).kind(),
            ErrorKind::Spawn
        );
        assert_eq!(
            Error::spawn("git", std::io::Error::from(IoKind::PermissionDenied)).kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            Error::not_found("git", Some("/usr/bin".into())).kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            Error::stdin("git", std::io::Error::from(IoKind::BrokenPipe)).kind(),
            ErrorKind::Other
        );
        assert_eq!(Error::parse("git", "boom").kind(), ErrorKind::Other);
        assert_eq!(
            Error::io(std::io::Error::from(IoKind::InvalidInput)).kind(),
            ErrorKind::Other
        );
        assert_eq!(
            Error::io(std::io::Error::from(IoKind::PermissionDenied)).kind(),
            ErrorKind::PermissionDenied
        );
        // The crate's own "stdout not piped" IO helper is a plain Other backend
        // error, not a permission or spawn condition.
        assert_eq!(stdout_not_piped_error("git").kind(), ErrorKind::Other);
    }

    #[test]
    fn kind_stays_consistent_with_the_is_classifiers() {
        use std::io::ErrorKind as IoKind;

        // The new total род must agree with the existing point classifiers — a
        // regression that drifts one from the other is caught here.
        let cases: [Error; 6] = [
            Error::not_found("x", None),
            spawn(IoKind::PermissionDenied),
            Error::timeout("x", Duration::from_secs(1), "", ""),
            ErrorReason::Cancelled {
                program: "x".into(),
            }
            .into(),
            Error::signalled("x", None, "", ""),
            Error::exit("x", 1, "", ""),
        ];
        for err in &cases {
            assert_eq!(err.is_not_found(), err.kind() == ErrorKind::NotFound);
            assert_eq!(
                err.is_permission_denied(),
                err.kind() == ErrorKind::PermissionDenied
            );
            assert_eq!(err.is_timeout(), err.kind() == ErrorKind::Timeout);
            assert_eq!(err.is_cancelled(), err.kind() == ErrorKind::Cancelled);
            assert_eq!(err.is_signalled(), err.kind() == ErrorKind::Signalled);
        }
    }

    #[test]
    fn error_kind_name_is_a_stable_identifier_per_kind() {
        assert_eq!(ErrorKind::NotFound.name(), "not_found");
        assert_eq!(ErrorKind::Spawn.name(), "spawn");
        assert_eq!(ErrorKind::PermissionDenied.name(), "permission_denied");
        assert_eq!(ErrorKind::Unsupported.name(), "unsupported");
        assert_eq!(ErrorKind::Timeout.name(), "timeout");
        assert_eq!(ErrorKind::Cancelled.name(), "cancelled");
        assert_eq!(ErrorKind::Exit.name(), "exit");
        assert_eq!(ErrorKind::Signalled.name(), "signalled");
        assert_eq!(ErrorKind::Other.name(), "other");
        #[cfg(feature = "limits")]
        assert_eq!(ErrorKind::ResourceLimit.name(), "resource_limit");
    }

    #[test]
    fn timeout_duration_reads_only_the_run_timeout() {
        let dur = Duration::from_millis(1500);
        assert_eq!(
            Error::timeout("git", dur, "", "").timeout_duration(),
            Some(dur)
        );
        // A readiness-probe deadline is a separate clock and reads None here,
        // exactly like `is_timeout()` returns false for it.
        let not_ready = ErrorReason::NotReady {
            program: "server".into(),
            timeout: Duration::from_secs(9),
        };
        assert_eq!(not_ready.timeout_duration(), None);
        assert!(!not_ready.is_timeout());
        // Every other variant reports None too.
        assert_eq!(Error::exit("git", 1, "", "").timeout_duration(), None);
        assert_eq!(Error::not_found("git", None).timeout_duration(), None);
    }

    #[test]
    fn output_overflow_snapshots_the_ceiling_counters() {
        let overflow = output_too_large().output_overflow().expect("some");
        assert_eq!(overflow.total_lines(), 250);
        assert_eq!(overflow.total_bytes(), 9001);
        assert_eq!(overflow.max_lines(), Some(100));
        // `None` here is an honest "no byte ceiling was set", distinguishable
        // from a non-overflow error's `output_overflow() == None` (the reason the
        // accessor returns a struct, not four scalar `Option<usize>`s).
        assert_eq!(overflow.max_bytes(), None);

        // Every non-overflow variant reports None.
        assert_eq!(Error::exit("git", 1, "", "").output_overflow(), None);
        assert_eq!(
            Error::timeout("git", Duration::from_secs(1), "", "").output_overflow(),
            None
        );
    }

    #[test]
    fn unsupported_operation_reads_only_the_unsupported_variant() {
        let err = ErrorReason::Unsupported {
            operation: "signal(Hup)".into(),
        };
        assert_eq!(err.unsupported_operation(), Some("signal(Hup)"));
        // Every other variant reports None.
        assert_eq!(Error::exit("git", 1, "", "").unsupported_operation(), None);
        assert_eq!(
            ErrorReason::Cancelled {
                program: "job".into()
            }
            .unsupported_operation(),
            None
        );
    }

    #[test]
    fn error_wrapper_delegates_kind_and_payload_accessors_to_the_reason() {
        // The pointer-sized `Error` wrapper mirrors every new accessor to its
        // inner `ErrorReason`, exactly like the existing `code`/`signal` delegates.
        let timeout: Error = ErrorReason::Timeout {
            program: "git".into(),
            timeout: Duration::from_secs(2),
            stdout: String::new(),
            stderr: String::new(),
            stdout_bytes: None,
        }
        .into();
        assert_eq!(timeout.kind(), timeout.reason().kind());
        assert_eq!(timeout.kind(), ErrorKind::Timeout);
        assert_eq!(
            timeout.timeout_duration(),
            timeout.reason().timeout_duration()
        );

        let overflow: Error = output_too_large().into();
        assert_eq!(
            overflow.output_overflow(),
            overflow.reason().output_overflow()
        );
        assert_eq!(overflow.kind(), ErrorKind::Other);

        let unsupported: Error = ErrorReason::Unsupported {
            operation: "suspend".into(),
        }
        .into();
        assert_eq!(
            unsupported.unsupported_operation(),
            unsupported.reason().unsupported_operation()
        );
        assert_eq!(unsupported.kind(), ErrorKind::Unsupported);
    }
}
