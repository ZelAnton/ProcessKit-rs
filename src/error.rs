//! The crate's error type.

use std::time::Duration;

/// Errors produced when launching or running a child process.
///
/// Spawn failures, a non-zero exit ([`Exit`](Error::Exit)), timeouts, and IO
/// errors fold into one structured enum, so callers can pattern-match on the
/// failure mode instead of parsing strings.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The child process could not be started (binary not found, permission
    /// denied, …).
    #[error("could not start `{program}`: {source}")]
    Spawn {
        /// The program we tried to launch.
        program: String,
        /// The underlying OS error.
        #[source]
        source: std::io::Error,
    },

    /// A bare program name (no path separators) was not found — it is not
    /// installed or the directory holding it is not on `PATH`. Enriched from the
    /// OS's opaque not-found error so the message names the searched directories,
    /// rather than pre-checking `PATH` (which would falsely reject a program the
    /// OS resolves by another route — e.g. the application directory on Windows).
    ///
    /// Distinct from [`Spawn`](Error::Spawn), which covers OS-level failures
    /// once the executable location is known (permission denied, busy, etc.).
    ///
    /// [`is_not_found`](Error::is_not_found) returns `true` for this variant.
    #[error("`{program}` not found on PATH (searched: {searched})")]
    NotFound {
        /// The program name that was looked up.
        program: String,
        /// The `PATH` directories that were searched, joined by the
        /// platform separator (`:` on Unix, `;` on Windows). Empty when
        /// `PATH` is not set.
        searched: String,
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
    Exit {
        /// The program that exited non-zero.
        program: String,
        /// The raw process exit code.
        code: i32,
        /// Captured standard output, in full. Not shown in the `Display`
        /// message; kept for callers that need a stdout-borne failure message.
        /// For the raw-bytes helper (`output_bytes`) this is a lossy UTF-8 decode
        /// of stdout — the exact bytes remain on the originating `ProcessResult`.
        stdout: String,
        /// Captured standard error, in full. Only its **last non-empty
        /// line** (bounded) appears in the `Display` message — the complete
        /// captured text lives here, never poisoning a log line.
        stderr: String,
    },

    /// The process exceeded its configured timeout and was killed.
    #[error("`{program}` timed out after {timeout:?}")]
    Timeout {
        /// The program that timed out.
        program: String,
        /// The deadline that elapsed.
        timeout: Duration,
    },

    /// The captured output exceeded the
    /// [`OutputBufferPolicy::fail_loud`](crate::OutputBufferPolicy::fail_loud)
    /// ceiling — the child produced more lines than allowed. The run itself
    /// may have succeeded; this error is raised by the consuming path after
    /// the run completes.
    ///
    /// The pipe is still fully drained (the child never blocks); excess lines
    /// are counted (in the total) but not retained.
    #[error("`{program}` output exceeded {limit}-line limit ({total_lines} lines total)")]
    OutputTooLarge {
        /// The program whose output exceeded the limit.
        program: String,
        /// The configured cap (`OutputBufferPolicy::max_lines`).
        limit: usize,
        /// Total lines that arrived (retained + dropped).
        total_lines: usize,
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
    /// helpers on [`CliClient`](crate::CliClient).
    #[error("failed to parse `{program}` output: {message}")]
    Parse {
        /// The program whose output failed to parse.
        program: String,
        /// What went wrong.
        message: String,
    },

    /// A requested resource limit could not be enforced.
    ///
    /// Produced by [`ProcessGroup::with_options`](crate::ProcessGroup::with_options)
    /// when a [`ResourceLimits`](crate::ResourceLimits) cap was set but the active
    /// mechanism can't honor it — either the platform has no whole-tree container
    /// (macOS/BSD, the Linux process-group fallback, the no-containment target), or
    /// the OS rejected the request (e.g. a Linux cgroup without controller
    /// delegation). An unenforced limit is no protection, so this is raised rather
    /// than leaving the tree silently unbounded.
    #[cfg(feature = "limits")]
    #[error("could not enforce resource limits: {0}")]
    ResourceLimit(String),

    /// An operation is not supported by the active containment mechanism on
    /// this platform.
    ///
    /// Raised by `ProcessGroup::signal` for any signal other than
    /// `Signal::Kill` on Windows (Job Objects have no POSIX signals), and by
    /// `signal`/`suspend`/`resume` on the no-containment target, which has no
    /// process tree to act on.
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
    #[cfg(feature = "cancellation")]
    #[error("`{program}` was cancelled")]
    Cancelled {
        /// The program that was cancelled.
        program: String,
    },

    /// The process was terminated by a signal (Unix) without producing an exit
    /// code. `signal` carries the signal number when the platform reports one
    /// (`None` on Windows or when the kernel does not expose it).
    ///
    /// Distinct from [`Exit`](Error::Exit): a signal-terminated run has no exit
    /// code to check — it is always a failure. Produced by
    /// [`ensure_success`](crate::ProcessResult::ensure_success) and the
    /// `require_code` path when the outcome is
    /// [`Outcome::Signalled`](crate::Outcome::Signalled).
    #[error("{}", display_signalled(program, *signal))]
    Signalled {
        /// The program that was killed by a signal.
        program: String,
        /// The signal number, when reported by the platform.
        signal: Option<i32>,
    },

    /// An IO error occurred while driving the process (reading a pipe, writing
    /// stdin, waiting for exit).
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    /// The best human-facing message for a failed run, trimmed of surrounding
    /// whitespace: captured standard error if it carries text, otherwise the
    /// captured standard output (where `git` puts `CONFLICT …` and `git commit`
    /// puts `nothing to commit`). Returns `None` when there is no captured output
    /// to show — a silent [`Exit`](Error::Exit) (both streams blank) or any
    /// non-`Exit` variant ([`Spawn`](Error::Spawn), [`Timeout`](Error::Timeout),
    /// [`Parse`](Error::Parse), [`Io`](Error::Io)) — so a caller can fall back to
    /// the [`Display`](std::fmt::Display) message. For the raw, untrimmed stream
    /// match on [`Exit`](Error::Exit)'s fields directly.
    pub fn diagnostic(&self) -> Option<&str> {
        match self {
            Error::Exit { stdout, stderr, .. } => exit_diagnostic(stdout, stderr),
            _ => None,
        }
    }

    /// Whether this is a **"not found"** failure — the program doesn't exist
    /// on `PATH` or a needed path is missing. True for:
    ///
    /// - [`NotFound`](Error::NotFound) (bare name absent from `PATH`),
    /// - [`Spawn`](Error::Spawn) / [`Io`](Error::Io) carrying
    ///   [`NotFound`](std::io::ErrorKind::NotFound) (e.g. missing `cwd`).
    ///
    /// `false` for every other variant. Lets a caller surface a "command not
    /// installed?" hint without matching on the underlying IO error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound { .. })
            || self
                .io_source()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
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
    /// `e.is_transient() || matches!(e, Error::Timeout { .. })`.
    ///
    /// Pairs with [`Command::retry`](crate::Command::retry):
    /// `cmd.retry(3, backoff, |e| e.is_transient())`.
    pub fn is_transient(&self) -> bool {
        self.io_source().is_some_and(is_transient_io)
    }

    /// The underlying [`std::io::Error`] for the variants that carry one
    /// ([`Spawn`](Error::Spawn), [`Io`](Error::Io)) — the basis for the io-level
    /// classifiers above.
    fn io_source(&self) -> Option<&std::io::Error> {
        match self {
            Error::Spawn { source, .. } => Some(source),
            Error::Io(source) => Some(source),
            _ => None,
        }
    }
}

/// `Signalled`'s one-line Display: `` `{program}` was terminated by signal {n} ``
/// when a number is known, `` `{program}` was terminated by a signal `` otherwise.
fn display_signalled(program: &str, signal: Option<i32>) -> String {
    match signal {
        Some(n) => format!("`{program}` was terminated by signal {n}"),
        None => format!("`{program}` was terminated by a signal"),
    }
}

/// io-level "retry as-is" conditions: transient kernel/filesystem states a bare
/// retry can clear, distinct from a permanent failure (not-found, permission).
/// Kept deliberately narrow.
fn is_transient_io(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    // `ExecutableFileBusy` is std's stable mapping of `ETXTBSY` — the executable
    // is being written; the launch clears once the writer closes it.
    if matches!(
        e.kind(),
        ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::ResourceBusy
            | ErrorKind::ExecutableFileBusy
    ) {
        return true;
    }
    // Windows sharing/lock violations std leaves `Uncategorized`, so match the
    // raw codes: ERROR_SHARING_VIOLATION (32) / ERROR_LOCK_VIOLATION (33) — a
    // file the launch needs is briefly locked by another process.
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
    const TAIL_CAP: usize = 200;
    let mut message = format!("`{program}` exited with code {code}");
    let tail = exit_diagnostic(stdout, stderr)
        .and_then(|text| text.lines().rev().map(str::trim).find(|l| !l.is_empty()));
    if let Some(tail) = tail {
        message.push_str(": ");
        if tail.len() <= TAIL_CAP {
            message.push_str(tail);
        } else {
            let mut cut = TAIL_CAP;
            while !tail.is_char_boundary(cut) {
                cut -= 1;
            }
            message.push_str(&tail[..cut]);
            message.push('…');
        }
    }
    message
}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_display_appends_a_bounded_diagnostic_tail() {
        // The policy guard (deliberately rewritten when the tail was added):
        // the Display stays one actionable line — program + code + the LAST
        // non-empty diagnostic line — never the full captured streams.
        let err = Error::Exit {
            program: "git".into(),
            code: 2,
            stdout: "CONFLICT (content): merge conflict in a.rs".into(),
            stderr: "warning: something\nfatal: boom\n".into(),
        };
        assert_eq!(err.to_string(), "`git` exited with code 2: fatal: boom");

        // stderr blank → the stdout-borne message (git's CONFLICT) is used.
        let err = Error::Exit {
            program: "git".into(),
            code: 2,
            stdout: "CONFLICT (content): merge conflict in a.rs".into(),
            stderr: "   ".into(),
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
        };
        assert_eq!(err.to_string(), "`git` exited with code 2");
    }

    #[test]
    fn exit_display_tail_is_capped_and_never_leaks_the_stream() {
        // A multi-KiB single-line stderr must not poison the log line: the
        // tail is cut at 200 bytes on a char boundary, with an ellipsis.
        let huge = "é".repeat(3000); // 2 bytes per char — exercises the boundary
        let err = Error::Exit {
            program: "x".into(),
            code: 1,
            stdout: String::new(),
            stderr: huge,
        };
        let message = err.to_string();
        assert!(message.len() < 250, "capped, got {} bytes", message.len());
        assert!(message.ends_with('…'), "got: {message}");
        assert!(message.starts_with("`x` exited with code 1: éé"));
    }

    #[test]
    fn diagnostic_is_none_for_non_exit_variants() {
        let timeout = Error::Timeout {
            program: "git".into(),
            timeout: Duration::from_secs(1),
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
        #[cfg(feature = "cancellation")]
        {
            let cancelled = Error::Cancelled {
                program: "job".into(),
            };
            assert_eq!(cancelled.diagnostic(), None);
        }
        #[cfg(feature = "limits")]
        {
            let limit = Error::ResourceLimit("cgroup controller delegation unavailable".into());
            assert_eq!(limit.diagnostic(), None);
        }
    }

    #[cfg(feature = "cancellation")]
    #[test]
    fn cancelled_display_names_the_program() {
        let err = Error::Cancelled {
            program: "long-job".into(),
        };
        assert_eq!(err.to_string(), "`long-job` was cancelled");
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
    fn resource_limit_display_carries_reason() {
        let err = Error::ResourceLimit("no cgroup or Job Object available".into());
        assert_eq!(
            err.to_string(),
            "could not enforce resource limits: no cgroup or Job Object available"
        );
    }

    #[test]
    fn signalled_display_and_diagnostic() {
        let with_signal = Error::Signalled {
            program: "git".into(),
            signal: Some(9),
        };
        assert_eq!(with_signal.to_string(), "`git` was terminated by signal 9");
        assert_eq!(with_signal.diagnostic(), None);
        assert!(!with_signal.is_not_found());
        assert!(!with_signal.is_permission_denied());
        assert!(!with_signal.is_transient());

        let no_signal = Error::Signalled {
            program: "git".into(),
            signal: None,
        };
        assert_eq!(no_signal.to_string(), "`git` was terminated by a signal");
    }

    #[test]
    fn not_found_display_and_classifier() {
        let err = Error::NotFound {
            program: "my-tool".into(),
            searched: "/usr/bin:/usr/local/bin".into(),
        };
        assert_eq!(
            err.to_string(),
            "`my-tool` not found on PATH (searched: /usr/bin:/usr/local/bin)"
        );
        assert!(err.is_not_found(), "NotFound must satisfy is_not_found()");
        assert!(!err.is_permission_denied());
        assert!(!err.is_transient());
        assert_eq!(err.diagnostic(), None);
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
        assert!(spawn(NotFound).is_not_found());
        assert!(Error::Io(std::io::Error::from(NotFound)).is_not_found());
        assert!(!spawn(NotFound).is_permission_denied());

        assert!(spawn(PermissionDenied).is_permission_denied());
        assert!(!spawn(PermissionDenied).is_not_found());
        // Neither permanent failure counts as transient.
        assert!(!spawn(NotFound).is_transient());
        assert!(!spawn(PermissionDenied).is_transient());
    }

    #[test]
    fn transient_kinds_are_classified() {
        // Includes ExecutableFileBusy built straight from the kind (no raw
        // errno) — the classifier must recognize it by `ErrorKind` alone.
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
        };
        assert!(!exit.is_not_found() && !exit.is_permission_denied() && !exit.is_transient());
        let timeout = Error::Timeout {
            program: "x".into(),
            timeout: Duration::from_secs(1),
        };
        assert!(
            !timeout.is_transient(),
            "Timeout is excluded from is_transient by design"
        );
    }
}
