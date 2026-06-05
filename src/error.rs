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

    /// The process ran to completion but exited with a non-zero status.
    ///
    /// Produced by the `ensure_success` helpers; the raw exit code is otherwise
    /// reported without erroring (a non-zero exit is not inherently a failure).
    ///
    /// Both captured streams are carried (each truncated to 4 KiB): `git`/`jj`
    /// write decisive diagnostics to **stdout** on failure (`CONFLICT (content):
    /// …`, `nothing to commit, working tree clean`), so a caller building a
    /// user-facing message wants stdout as a fallback when stderr is empty — see
    /// [`diagnostic`](Self::diagnostic).
    #[error("`{program}` exited with code {code}")]
    Exit {
        /// The program that exited non-zero.
        program: String,
        /// The raw process exit code.
        code: i32,
        /// Captured standard output (truncated). Not shown in the `Display`
        /// message; kept for callers that need a stdout-borne failure message.
        /// For the raw-bytes helper (`output_bytes`) this is a lossy UTF-8 decode
        /// of stdout — the exact bytes remain on the originating `ProcessResult`.
        stdout: String,
        /// Captured standard error (truncated). Not shown in the `Display`
        /// message to avoid log poisoning; this field holds what was kept.
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
    /// Raised by [`ProcessGroup::signal`](crate::ProcessGroup::signal) for any
    /// signal other than [`Signal::Kill`](crate::Signal::Kill) on Windows (Job
    /// Objects have no POSIX signals), and by `signal`/`suspend`/`resume` on
    /// the no-containment target, which has no process tree to act on.
    #[error("operation `{operation}` is not supported on this platform")]
    Unsupported {
        /// A short description of the operation, e.g. `"signal(Hup)"` or
        /// `"suspend"`.
        operation: String,
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
            Error::Exit { stderr, .. } if !stderr.trim().is_empty() => Some(stderr.trim()),
            Error::Exit { stdout, .. } if !stdout.trim().is_empty() => Some(stdout.trim()),
            _ => None,
        }
    }
}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_display_omits_both_captured_streams() {
        // Regression guard: adding `stdout` to `Error::Exit` must not change the
        // one-line `Display` message — neither captured stream may leak into it
        // (a multi-KiB dump would poison logs). The text is exactly program+code.
        let err = Error::Exit {
            program: "git".into(),
            code: 2,
            stdout: "CONFLICT (content): merge conflict in a.rs".into(),
            stderr: "fatal: boom".into(),
        };
        assert_eq!(err.to_string(), "`git` exited with code 2");
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
        #[cfg(feature = "limits")]
        {
            let limit = Error::ResourceLimit("cgroup controller delegation unavailable".into());
            assert_eq!(limit.diagnostic(), None);
        }
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
}
