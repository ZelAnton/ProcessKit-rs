//! The captured outcome of a finished process run.

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
}

impl<T> ProcessResult<T> {
    pub(crate) fn new(
        program: String,
        stdout: T,
        stderr: String,
        exit_code: i32,
        timed_out: bool,
    ) -> Self {
        Self {
            program,
            stdout,
            stderr,
            exit_code,
            timed_out,
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

    /// Return `self` unchanged when the exit code is 0, otherwise an
    /// [`Error::Exit`] carrying the code and (truncated) standard error.
    ///
    /// Mirrors the .NET `EnsureSuccess()` / `ProcessExitException`.
    pub fn ensure_success(self) -> Result<Self, Error> {
        if self.is_success() {
            return Ok(self);
        }
        Err(Error::Exit {
            program: self.program.clone(),
            code: self.exit_code,
            stderr: truncate_stderr(&self.stderr),
        })
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
        let ok = ProcessResult::new("git".into(), "out".to_owned(), String::new(), 0, false);
        assert!(ok.is_success());
        assert!(ok.ensure_success().is_ok());
    }

    #[test]
    fn nonzero_exit_turns_into_error() {
        let bad = ProcessResult::new("git".into(), "out".to_owned(), "boom".to_owned(), 2, false);
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
    fn stderr_is_truncated_in_error_only() {
        let big = "x".repeat(10_000);
        let bad = ProcessResult::new("p".into(), Vec::<u8>::new(), big.clone(), 1, false);
        let full = bad.stderr().to_owned();
        assert_eq!(full.len(), 10_000);
        let Error::Exit { stderr, .. } = bad.ensure_success().unwrap_err() else {
            panic!("expected Exit");
        };
        assert!(stderr.len() < 10_000);
        assert!(stderr.ends_with("… (truncated)"));
    }
}
