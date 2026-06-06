//! The captured outcome of a finished process run.

use std::time::Duration;

use crate::error::Error;

/// The captured result of running a process to completion.
///
/// `T` is the standard-output payload: [`String`] for the text helpers
/// (`output_string`) or [`Vec<u8>`] for the raw-bytes helper (`output_bytes`).
/// Standard error is always captured as text. A non-zero exit code is **not**
/// treated as an error on its own — inspect [`code`](Self::code) or call
/// [`ensure_success`](Self::ensure_success).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessResult<T> {
    program: String,
    stdout: T,
    stderr: String,
    /// The exit code, or `None` when the run produced no code — it was killed by
    /// its timeout, or terminated by a signal (Unix). Distinguish the two via
    /// [`timed_out`](Self::timed_out).
    code: Option<i32>,
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
        code: Option<i32>,
        timed_out: bool,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            program,
            stdout,
            stderr,
            code,
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

    /// The process exit code, or `None` when the run yielded no code — killed by
    /// its timeout ([`timed_out`](Self::timed_out) is then `true`) or terminated
    /// by a signal on Unix (`timed_out` is `false`). There is no synthetic
    /// sentinel: a missing code is `None`, never `-1`.
    pub fn code(&self) -> Option<i32> {
        self.code
    }

    /// Whether the run was killed because it exceeded its timeout.
    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    /// Whether the process exited with code 0.
    pub fn is_success(&self) -> bool {
        self.code == Some(0)
    }

    /// Return `self` unchanged when the run succeeded, otherwise the matching
    /// error: [`Error::Timeout`] if the run was killed by its deadline (checked
    /// first), an IO error if it was killed by a signal (no exit code), else
    /// [`Error::Exit`] for a non-zero exit, carrying the code and both
    /// (truncated) captured streams.
    pub fn ensure_success(self) -> Result<Self, Error>
    where
        T: StdoutText,
    {
        if let Some(err) = self.timeout_error() {
            return Err(err);
        }
        match self.code {
            Some(0) => Ok(self),
            // No code, but not a timeout → terminated by a signal. Surface it as
            // an IO error (consistent with `require_code`) rather than a
            // synthetic `Error::Exit { code: -1 }`.
            None => Err(self.signal_error()),
            Some(code) => Err(Error::Exit {
                program: self.program.clone(),
                code,
                stdout: truncate_output(&self.stdout.as_text()),
                stderr: truncate_output(&self.stderr),
            }),
        }
    }

    /// The [`Error::Timeout`] this result represents, if it timed out. Lets the
    /// convenience helpers (`ensure_success`, `ProcessRunnerExt::exit_code`)
    /// surface a timeout as a distinct error rather than a missing code, while
    /// `output`/`capture` keep exposing the [`timed_out`](Self::timed_out) flag
    /// for callers that want to inspect it without erroring.
    pub(crate) fn timeout_error(&self) -> Option<Error> {
        self.timed_out.then(|| Error::Timeout {
            program: self.program.clone(),
            timeout: self.timeout.unwrap_or_default(),
        })
    }

    /// The error for a run that was killed by a signal and so produced no exit
    /// code (a non-timeout `None` code).
    fn signal_error(&self) -> Error {
        Error::Io(std::io::Error::other(format!(
            "`{}` was terminated by a signal without an exit code",
            self.program
        )))
    }

    /// The exit code for the code-returning convenience helpers
    /// (`Command::exit_code`, `ProcessRunnerExt::exit_code`, `CliClient::code`):
    /// a timeout surfaces as [`Error::Timeout`], a signal-kill (no code) as an
    /// IO error, otherwise the code.
    pub(crate) fn require_code(&self) -> Result<i32, Error> {
        if let Some(err) = self.timeout_error() {
            return Err(err);
        }
        self.code.ok_or_else(|| self.signal_error())
    }
}

impl ProcessResult<String> {
    /// Standard output followed by standard error, concatenated — handy when a
    /// tool interleaves diagnostics across both streams.
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout(), self.stderr())
    }

    /// The best human-facing message from a captured run, trimmed of surrounding
    /// whitespace: standard error if it carries text, otherwise standard output —
    /// `git`/`jj` put `CONFLICT …` and `nothing to commit` on stdout, so a probe
    /// that captured the result (rather than erroring) can build the same friendly
    /// message [`Error::diagnostic`](crate::Error::diagnostic) gives the erroring
    /// path. For the raw, untrimmed streams use [`stdout`](Self::stdout) /
    /// [`stderr`](Self::stderr).
    pub fn diagnostic(&self) -> &str {
        if self.stderr.trim().is_empty() {
            self.stdout.trim()
        } else {
            self.stderr.trim()
        }
    }
}

/// Render captured stdout as text for [`Error::Exit`], whatever the payload type:
/// a [`String`] is taken as-is; raw bytes are decoded lossily.
///
/// An implementation detail of [`ensure_success`](ProcessResult::ensure_success):
/// `pub` only to satisfy the bound's visibility (the `result` module is private,
/// so it is unnameable outside the crate) and `#[doc(hidden)]` so it stays off
/// the public docs.
#[doc(hidden)]
pub trait StdoutText {
    fn as_text(&self) -> String;
}

impl StdoutText for String {
    fn as_text(&self) -> String {
        self.clone()
    }
}

impl StdoutText for Vec<u8> {
    fn as_text(&self) -> String {
        String::from_utf8_lossy(self).into_owned()
    }
}

/// Cap a captured stream carried in an error so a giant dump can't poison logs
/// (the full text remains available on the [`ProcessResult`]). Capped at 4 KiB.
fn truncate_output(text: &str) -> String {
    const MAX: usize = 4 * 1024;
    if text.len() <= MAX {
        return text.to_owned();
    }
    // Truncate on a char boundary at or below the cap.
    let mut end = MAX;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… (truncated)", &text[..end])
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
            Some(0),
            false,
            None,
        );
        assert!(ok.is_success());
        assert_eq!(ok.code(), Some(0));
        assert!(ok.ensure_success().is_ok());
    }

    #[test]
    fn nonzero_exit_carries_both_streams() {
        let bad = ProcessResult::new(
            "git".into(),
            "CONFLICT (content): merge conflict in a.rs".to_owned(),
            "boom".to_owned(),
            Some(2),
            false,
            None,
        );
        assert!(!bad.is_success());
        assert_eq!(bad.code(), Some(2));
        let err = bad.ensure_success().unwrap_err();
        match err {
            Error::Exit {
                program,
                code,
                stdout,
                stderr,
            } => {
                assert_eq!(program, "git");
                assert_eq!(code, 2);
                assert_eq!(stdout, "CONFLICT (content): merge conflict in a.rs");
                assert_eq!(stderr, "boom");
            }
            other => panic!("expected Exit, got {other:?}"),
        }
    }

    #[test]
    fn diagnostic_prefers_stderr_then_falls_back_to_stdout() {
        // stderr present → stderr wins, trimmed of surrounding whitespace.
        let with_stderr = ProcessResult::new(
            "git".into(),
            "on stdout".into(),
            "  on stderr \n".into(),
            Some(1),
            false,
            None,
        );
        assert_eq!(with_stderr.diagnostic(), "on stderr");
        assert_eq!(
            with_stderr.ensure_success().unwrap_err().diagnostic(),
            Some("on stderr")
        );

        // stderr blank → stdout (where `git merge` writes CONFLICT) is the message.
        let conflict = ProcessResult::new(
            "git".into(),
            "CONFLICT (content): merge conflict in a.rs".into(),
            "   \n".into(),
            Some(1),
            false,
            None,
        );
        assert_eq!(
            conflict.diagnostic(),
            "CONFLICT (content): merge conflict in a.rs"
        );
        // The erroring path exposes the same rule via Error::diagnostic.
        let Error::Exit { .. } = conflict.clone().ensure_success().unwrap_err() else {
            panic!("expected Exit");
        };
        assert_eq!(
            conflict.ensure_success().unwrap_err().diagnostic(),
            Some("CONFLICT (content): merge conflict in a.rs")
        );

        // Both streams blank → no captured message; the caller falls back to the
        // Display text rather than getting an empty string.
        let silent = ProcessResult::new(
            "git".into(),
            String::new(),
            "  \n".into(),
            Some(1),
            false,
            None,
        );
        assert_eq!(silent.ensure_success().unwrap_err().diagnostic(), None);
    }

    #[test]
    fn timed_out_takes_precedence_over_exit_code() {
        // A timed-out run has no code (None), and ensure_success must report it
        // as a distinct Timeout, not a non-zero Exit.
        let timed = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            None,
            true,
            Some(Duration::from_millis(500)),
        );
        assert!(timed.timed_out());
        assert_eq!(timed.code(), None);
        match timed.ensure_success().unwrap_err() {
            Error::Timeout { program, timeout } => {
                assert_eq!(program, "git");
                assert_eq!(timeout, Duration::from_millis(500));
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn signal_kill_has_no_code_and_never_yields_minus_one() {
        // A signal-terminated run (no code, not a timeout) reports `None`. Both
        // `require_code` and `ensure_success` must surface an IO error — never a
        // synthetic `Error::Exit { code: -1 }`, which would resurrect the sentinel.
        let killed = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            None,
            false,
            None,
        );
        assert_eq!(killed.code(), None);
        assert!(!killed.is_success());
        assert!(matches!(killed.require_code().unwrap_err(), Error::Io(_)));
        match killed.ensure_success().unwrap_err() {
            Error::Io(_) => {}
            other => panic!("expected Io for a signal-kill, got {other:?}"),
        }
    }

    #[test]
    fn combined_concatenates_stdout_then_stderr() {
        let r = ProcessResult::new(
            "p".into(),
            "out".to_owned(),
            "err".to_owned(),
            Some(0),
            false,
            None,
        );
        assert_eq!(r.combined(), "outerr");
    }

    #[test]
    fn output_is_truncated_in_error_only() {
        let big = "x".repeat(10_000);
        let bad = ProcessResult::new(
            "p".into(),
            big.clone().into_bytes(),
            big.clone(),
            Some(1),
            false,
            None,
        );
        assert_eq!(bad.stderr().len(), 10_000);
        let Error::Exit { stdout, stderr, .. } = bad.ensure_success().unwrap_err() else {
            panic!("expected Exit");
        };
        assert!(stderr.len() < 10_000 && stderr.ends_with("… (truncated)"));
        assert!(stdout.len() < 10_000 && stdout.ends_with("… (truncated)"));
    }

    #[test]
    fn truncation_backs_off_to_a_char_boundary() {
        // A 2-byte char straddling the 4 KiB cap: a naive byte slice at the
        // cap would split it and panic. 4095 ASCII bytes put the first `é`
        // at bytes 4095..4097 — exactly across the boundary.
        let text = format!("{}{}", "a".repeat(4_095), "é".repeat(100));
        let truncated = truncate_output(&text);
        assert!(truncated.ends_with("… (truncated)"));
        let kept = truncated.strip_suffix("… (truncated)").unwrap();
        assert_eq!(
            kept.len(),
            4_095,
            "the straddling char is dropped, not split"
        );
        assert!(kept.chars().all(|c| c == 'a'));
    }

    #[test]
    fn truncation_leaves_text_at_the_cap_untouched() {
        let exactly = "x".repeat(4 * 1024);
        assert_eq!(truncate_output(&exactly), exactly, "<= cap is verbatim");
        assert_eq!(truncate_output(""), "");
    }
}
