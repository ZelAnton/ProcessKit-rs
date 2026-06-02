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
    }
}
