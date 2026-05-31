//! The captured outcome of a finished process run.

use std::time::Duration;

use crate::error::Error;

/// The captured result of running a process to completion.
///
/// `T` is the standard-output payload: [`String`] for the text helpers
/// (`output_string`) or [`Vec<u8>`] for the raw-bytes helper (`output_bytes`).
/// Standard error is always captured as text. A non-zero exit code is **not**
/// treated as an error on its own — inspect [`exit_code`](Self::exit_code) or
/// call [`ensure_success`](Self::ensure_success).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessResult<T> {
    program: String,
    stdout: T,
    stderr: String,
    exit_code: i32,
    timed_out: bool,
    /// The deadline that elapsed, when `timed_out` — carried so the
    /// success-checking helpers can build a faithful [`Error::Timeout`].
    timeout: Option<Duration>,
}

impl<T> ProcessResult<T> {
    pub(crate) fn new(
        program: String,
        stdout: T,
        stderr: String,
        exit_code: i32,
        timed_out: bool,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            program,
            stdout,
            stderr,
            exit_code,
            timed_out,
            timeout,
        }
    }

    /// The captured standard output (text or bytes depending on `T`).
    pub fn stdout(&self) -> &T {
        &self.stdout
    }

    /// Consume the result and return just the captured standard output.
    pub fn into_stdout(self) -> T {
        self.stdout
    }

    /// The captured standard error.
    pub fn stderr(&self) -> &str {
        &self.stderr
    }

    /// The raw process exit code (whatever the OS reported).
    pub fn exit_code(&self) -> i32 {
        self.exit_code
    }

    /// Whether the run was killed because it exceeded its timeout.
    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    /// Whether the process exited with code 0.
    pub fn is_success(&self) -> bool {
        self.exit_code == 0
    }

    /// Return `self` unchanged when the run succeeded, otherwise the matching
    /// error: [`Error::Timeout`] if the run was killed by its deadline (checked
    /// first — a timed-out run has no meaningful exit code), else
    /// [`Error::Exit`] for a non-zero exit, carrying the code and (truncated)
    /// standard error.
    ///
    /// Mirrors the .NET `EnsureSuccess()` / `ProcessExitException`.
    pub fn ensure_success(self) -> Result<Self, Error> {
        if let Some(err) = self.timeout_error() {
            return Err(err);
        }
        if self.is_success() {
            return Ok(self);
        }
        Err(Error::Exit {
            program: self.program.clone(),
            code: self.exit_code,
            stderr: truncate_stderr(&self.stderr),
        })
    }

    /// The [`Error::Timeout`] this result represents, if it timed out. Lets the
    /// convenience helpers (`ensure_success`, `ProcessRunnerExt::exit_code`)
    /// surface a timeout as a distinct error rather than a synthetic `-1` exit,
    /// while `output`/`capture` keep exposing the [`timed_out`](Self::timed_out)
    /// flag for callers that want to inspect it without erroring.
    pub(crate) fn timeout_error(&self) -> Option<Error> {
        self.timed_out.then(|| Error::Timeout {
            program: self.program.clone(),
            timeout: self.timeout.unwrap_or_default(),
        })
    }
}

impl ProcessResult<String> {
    /// Standard output followed by standard error, concatenated — handy when a
    /// tool interleaves diagnostics across both streams. Mirrors the .NET / vcs
    /// `combined()`.
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout(), self.stderr())
    }
}

/// Cap the stderr carried in an error message so a giant dump can't poison logs
/// (the full text remains available on the [`ProcessResult`]). Matches the
/// 4 KiB cap the .NET library applies.
fn truncate_stderr(stderr: &str) -> String {
    const MAX: usize = 4 * 1024;
    if stderr.len() <= MAX {
        return stderr.to_owned();
    }
    // Truncate on a char boundary at or below the cap.
    let mut end = MAX;
    while !stderr.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… (truncated)", &stderr[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_is_code_zero() {
        let ok = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            0,
            false,
            None,
        );
        assert!(ok.is_success());
        assert!(ok.ensure_success().is_ok());
    }

    #[test]
    fn nonzero_exit_turns_into_error() {
        let bad = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            "boom".to_owned(),
            2,
            false,
            None,
        );
        assert!(!bad.is_success());
        let err = bad.ensure_success().unwrap_err();
        match err {
            Error::Exit {
                program,
                code,
                stderr,
            } => {
                assert_eq!(program, "git");
                assert_eq!(code, 2);
                assert_eq!(stderr, "boom");
            }
            other => panic!("expected Exit, got {other:?}"),
        }
    }

    #[test]
    fn timed_out_takes_precedence_over_exit_code() {
        // A timed-out run carries exit_code -1, but ensure_success must report it
        // as a distinct Timeout, not a non-zero Exit.
        let timed = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            -1,
            true,
            Some(Duration::from_millis(500)),
        );
        assert!(timed.timed_out());
        match timed.ensure_success().unwrap_err() {
            Error::Timeout { program, timeout } => {
                assert_eq!(program, "git");
                assert_eq!(timeout, Duration::from_millis(500));
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn combined_concatenates_stdout_then_stderr() {
        let r = ProcessResult::new(
            "p".into(),
            "out".to_owned(),
            "err".to_owned(),
            0,
            false,
            None,
        );
        assert_eq!(r.combined(), "outerr");
    }

    #[test]
    fn stderr_is_truncated_in_error_only() {
        let big = "x".repeat(10_000);
        let bad = ProcessResult::new("p".into(), Vec::<u8>::new(), big.clone(), 1, false, None);
        let full = bad.stderr().to_owned();
        assert_eq!(full.len(), 10_000);
        let Error::Exit { stderr, .. } = bad.ensure_success().unwrap_err() else {
            panic!("expected Exit");
        };
        assert!(stderr.len() < 10_000);
        assert!(stderr.ends_with("… (truncated)"));
    }
}
