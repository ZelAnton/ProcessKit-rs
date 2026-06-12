//! The captured outcome of a finished process run.

use std::time::Duration;

use crate::error::Error;

/// How a run ended — the explicit form of the `code()`/`timed_out()` pair.
///
/// Non-exhaustive: a future disposition (e.g. a richer platform-specific
/// termination) can be added without a breaking change. The convenience
/// accessors [`ProcessResult::code`] / [`ProcessResult::timed_out`] /
/// [`ProcessResult::is_success`] derive from this and remain the everyday
/// surface; match on `Outcome` when the three-way distinction matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The process exited on its own with this code.
    Exited(i32),
    /// Terminated by a signal (Unix) — no exit code exists.
    /// The inner value is the signal number when the platform reports one;
    /// `None` on Windows or when the kernel does not expose the signal.
    Signalled(Option<i32>),
    /// Killed because it exceeded its configured timeout.
    TimedOut,
}

/// The captured result of running a process to completion.
///
/// `T` is the standard-output payload: [`String`] for the text helpers
/// (`output_string`) or [`Vec<u8>`] for the raw-bytes helper (`output_bytes`).
/// Standard error is always captured as text. A non-zero exit code is **not**
/// treated as an error on its own — inspect [`code`](Self::code) or call
/// [`ensure_success`](Self::ensure_success).
#[derive(Debug, Clone)]
pub struct ProcessResult<T> {
    program: String,
    stdout: T,
    stderr: String,
    /// How the run ended (see [`Outcome`]); `code()`/`timed_out()` derive
    /// from it.
    outcome: Outcome,
    /// The deadline that elapsed, when timed out — carried so the
    /// success-checking helpers can build a faithful [`Error::Timeout`].
    timeout: Option<Duration>,
    /// Wall-clock duration of the run; `Duration::ZERO` for synthetic results
    /// (a scripted/replayed bulk `output` that didn't time a real process).
    duration: Duration,
    /// Whether a bounded [`OutputBufferPolicy`](crate::OutputBufferPolicy)
    /// dropped captured output lines.
    truncated: bool,
    /// Exit codes treated as success by [`is_success`](Self::is_success) /
    /// [`ensure_success`](Self::ensure_success). Default `[0]`; widened via
    /// [`Command::ok_codes`](crate::Command::ok_codes).
    ok_codes: Vec<i32>,
}

// Equality is over the *logical* outcome, not incidental measurement: `duration`
// (wall-clock timing) and `truncated` (buffer state) are deliberately excluded.
// This keeps the cassette's round-trip contract — a result and its replay compare
// equal — true even when the recording ran on a real runner (non-zero duration)
// while replay rebuilds it as `Duration::ZERO`. See `cassette.rs`.
impl<T: PartialEq> PartialEq for ProcessResult<T> {
    fn eq(&self, other: &Self) -> bool {
        self.program == other.program
            && self.stdout == other.stdout
            && self.stderr == other.stderr
            && self.outcome == other.outcome
            && self.timeout == other.timeout
            && self.ok_codes == other.ok_codes
    }
}

impl<T: Eq> Eq for ProcessResult<T> {}

impl<T> ProcessResult<T> {
    /// Build a result from the [`Outcome`] every producing path has in hand.
    pub(crate) fn new(
        program: String,
        stdout: T,
        stderr: String,
        outcome: Outcome,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            program,
            stdout,
            stderr,
            outcome,
            timeout,
            // Producer-only setters fill these on the real run paths; the
            // defaults keep synthetic results (doubles, pipeline, tests) correct.
            duration: Duration::ZERO,
            truncated: false,
            ok_codes: vec![0],
        }
    }

    /// The program this result is attributed to (lossy UTF-8 of the program
    /// name) — the same value the error variants carry. For a
    /// [`Pipeline`](crate::Pipeline) outcome this is the pipefail-attributed
    /// stage: the first stage that didn't exit cleanly, or the last stage
    /// when every stage succeeded.
    pub fn program(&self) -> &str {
        &self.program
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

    /// How the run ended, as the explicit three-way [`Outcome`].
    pub fn outcome(&self) -> Outcome {
        self.outcome
    }

    /// The process exit code, or `None` when the run yielded no code — killed by
    /// its timeout ([`timed_out`](Self::timed_out) is then `true`) or terminated
    /// by a signal on Unix (`timed_out` is `false`). There is no synthetic
    /// sentinel: a missing code is `None`, never `-1`. Derived from
    /// [`outcome`](Self::outcome).
    pub fn code(&self) -> Option<i32> {
        match self.outcome {
            Outcome::Exited(code) => Some(code),
            _ => None,
        }
    }

    /// Whether the run was killed because it exceeded its timeout. Derived
    /// from [`outcome`](Self::outcome).
    pub fn timed_out(&self) -> bool {
        matches!(self.outcome, Outcome::TimedOut)
    }

    /// Whether the process exited with an **accepted** code — `0` by default, or
    /// any code in the set configured via
    /// [`Command::ok_codes`](crate::Command::ok_codes).
    pub fn is_success(&self) -> bool {
        matches!(self.outcome, Outcome::Exited(code) if self.ok_codes.contains(&code))
    }

    /// Return `self` unchanged when the run succeeded, otherwise the matching
    /// error: [`Error::Timeout`] if the run was killed by its deadline (checked
    /// first), [`Error::Signalled`](crate::Error::Signalled) if it was terminated
    /// by a signal (no exit code), else [`Error::Exit`] for an exit code outside
    /// the accepted set (code `0` by default — see
    /// [`Command::ok_codes`](crate::Command::ok_codes)), carrying the code and
    /// both captured streams in full (the [`Display`](std::fmt::Display) impl
    /// bounds what it prints; the fields stay complete for classification).
    pub fn ensure_success(self) -> Result<Self, Error>
    where
        T: StdoutText,
    {
        match self.outcome {
            Outcome::Exited(code) if self.ok_codes.contains(&code) => Ok(self),
            Outcome::TimedOut => Err(Error::Timeout {
                program: self.program.clone(),
                timeout: self.timeout.unwrap_or_default(),
            }),
            Outcome::Signalled(signal) => Err(Error::Signalled {
                program: self.program.clone(),
                signal,
            }),
            Outcome::Exited(code) => Err(Error::Exit {
                program: self.program.clone(),
                code,
                stdout: self.stdout.as_text(),
                stderr: self.stderr.clone(),
            }),
        }
    }

    /// The exit code for the code-returning convenience helpers
    /// (`Command::exit_code`, `ProcessRunnerExt::exit_code`, `CliClient::exit_code`):
    /// a timeout surfaces as [`Error::Timeout`], a signal-kill (no code) as
    /// [`Error::Signalled`], otherwise the code.
    pub(crate) fn require_code(&self) -> Result<i32, Error> {
        match self.outcome {
            Outcome::Exited(code) => Ok(code),
            Outcome::TimedOut => Err(Error::Timeout {
                program: self.program.clone(),
                timeout: self.timeout.unwrap_or_default(),
            }),
            Outcome::Signalled(signal) => Err(Error::Signalled {
                program: self.program.clone(),
                signal,
            }),
        }
    }

    /// The wall-clock duration of the run — spawn to exit (or kill). It is
    /// `Duration::ZERO` for synthetic results that didn't time a real process
    /// (a scripted/replayed bulk `output`).
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Whether a bounded [`OutputBufferPolicy`](crate::OutputBufferPolicy)
    /// discarded captured output lines (one or more lines were dropped by the
    /// buffer policy). Lines a streaming consumer popped are *not* truncation, so
    /// this stays `false` under the default unbounded policy even after a partial
    /// [`stdout_lines`](crate::RunningProcess::stdout_lines) stream, and for the
    /// raw stdout of [`output_bytes`](crate::Command::output_bytes) (not
    /// line-buffered).
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Stamp the run's wall-clock duration (producer-only).
    pub(crate) fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Record that a bounded buffer dropped captured lines (producer-only).
    pub(crate) fn with_truncated(mut self, truncated: bool) -> Self {
        self.truncated = truncated;
        self
    }

    /// Set the exit codes treated as success (producer-only). An empty set is
    /// ignored — it would make every exit a failure — so the default `[0]` stands.
    pub(crate) fn with_ok_codes(mut self, ok_codes: Vec<i32>) -> Self {
        if !ok_codes.is_empty() {
            self.ok_codes = ok_codes;
        }
        self
    }
}

impl ProcessResult<String> {
    /// Standard output followed by standard error, joined — handy when a tool
    /// interleaves diagnostics across both streams.
    ///
    /// A `\n` separator is inserted between the streams when stdout is
    /// non-empty and does not already end with a newline, preventing the last
    /// stdout line from being glued to the first stderr line.
    pub fn combined(&self) -> String {
        let out = self.stdout();
        let err = self.stderr();
        if !out.is_empty() && !err.is_empty() && !out.ends_with('\n') {
            format!("{out}\n{err}")
        } else {
            format!("{out}{err}")
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_reflects_the_three_terminal_states() {
        let exited = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Exited(3),
            None,
        );
        assert_eq!(exited.outcome(), Outcome::Exited(3));

        let signalled = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Signalled(None),
            None,
        );
        assert_eq!(signalled.outcome(), Outcome::Signalled(None));

        let timed_out = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::TimedOut,
            None,
        );
        assert_eq!(timed_out.outcome(), Outcome::TimedOut);
    }

    #[test]
    fn signalled_carries_signal_number() {
        let killed = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Signalled(Some(9)),
            None,
        );
        assert_eq!(killed.code(), None);
        assert!(!killed.timed_out());
        assert!(!killed.is_success());
        assert_eq!(killed.outcome(), Outcome::Signalled(Some(9)));
        match killed.ensure_success().unwrap_err() {
            Error::Signalled { program, signal } => {
                assert_eq!(program, "git");
                assert_eq!(signal, Some(9));
            }
            other => panic!("expected Signalled, got {other:?}"),
        }
    }

    #[test]
    fn derived_accessors_agree_with_outcome() {
        for outcome in [
            Outcome::Exited(0),
            Outcome::Exited(7),
            Outcome::Signalled(None),
            Outcome::TimedOut,
        ] {
            let r = ProcessResult::new("x".into(), String::new(), String::new(), outcome, None);
            match r.outcome() {
                Outcome::Exited(c) => {
                    assert_eq!(r.code(), Some(c));
                    assert!(!r.timed_out());
                    assert_eq!(r.is_success(), c == 0);
                }
                Outcome::Signalled(_) => {
                    assert_eq!(r.code(), None);
                    assert!(!r.timed_out());
                    assert!(!r.is_success());
                }
                Outcome::TimedOut => {
                    assert_eq!(r.code(), None);
                    assert!(r.timed_out());
                    assert!(!r.is_success());
                }
            }
        }
    }

    #[test]
    fn success_is_code_zero() {
        let ok = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Exited(0),
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
            Outcome::Exited(2),
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
            Outcome::Exited(1),
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
            Outcome::Exited(1),
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
            Outcome::Exited(1),
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
            Outcome::TimedOut,
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
    fn signal_kill_surfaces_as_signalled_error() {
        // A signal-terminated run (no code, not a timeout) reports `None` code.
        // Both `require_code` and `ensure_success` must surface `Error::Signalled`
        // — never a synthetic `Error::Exit { code: -1 }`, which would resurrect
        // the sentinel, and no longer `Error::Io` (which was the old stand-in).
        let killed = ProcessResult::new(
            "git".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Signalled(None),
            None,
        );
        assert_eq!(killed.code(), None);
        assert!(!killed.is_success());
        assert!(matches!(
            killed.require_code().unwrap_err(),
            Error::Signalled { .. }
        ));
        match killed.ensure_success().unwrap_err() {
            Error::Signalled { program, signal } => {
                assert_eq!(program, "git");
                assert_eq!(signal, None);
            }
            other => panic!("expected Signalled for a signal-kill, got {other:?}"),
        }
    }

    #[test]
    fn combined_concatenates_stdout_then_stderr() {
        let r = ProcessResult::new(
            "p".into(),
            "out".to_owned(),
            "err".to_owned(),
            Outcome::Exited(0),
            None,
        );
        // L16: when stdout doesn't end with \n, a newline separator is inserted
        // so the last stdout line is not glued to the first stderr line.
        assert_eq!(r.combined(), "out\nerr");
    }

    #[test]
    fn combined_no_extra_newline_when_stdout_already_ends_with_newline() {
        // When stdout ends with \n, no extra newline is added.
        let r = ProcessResult::new(
            "p".into(),
            "out\n".to_owned(),
            "err".to_owned(),
            Outcome::Exited(0),
            None,
        );
        assert_eq!(r.combined(), "out\nerr");
    }

    #[test]
    fn combined_no_separator_when_one_stream_is_empty() {
        let stdout_only = ProcessResult::new(
            "p".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Exited(0),
            None,
        );
        assert_eq!(stdout_only.combined(), "out");

        let stderr_only = ProcessResult::new(
            "p".into(),
            String::new(),
            "err".to_owned(),
            Outcome::Exited(0),
            None,
        );
        assert_eq!(stderr_only.combined(), "err");
    }

    #[test]
    fn error_exit_carries_full_streams() {
        // The error fields must carry the complete captured streams — consumers
        // classify on them (grep for a marker, parse a code), and truncating
        // before classification destroyed data. The `Display` impl bounds what
        // it *prints*; the fields stay whole.
        let big = "x".repeat(10_000);
        let bad = ProcessResult::new(
            "p".into(),
            big.clone().into_bytes(),
            big.clone(),
            Outcome::Exited(1),
            None,
        );
        assert_eq!(bad.stderr().len(), 10_000);
        let Error::Exit { stdout, stderr, .. } = bad.ensure_success().unwrap_err() else {
            panic!("expected Exit");
        };
        assert_eq!(stdout.len(), 10_000, "full stdout carried, untruncated");
        assert_eq!(stderr.len(), 10_000, "full stderr carried, untruncated");
        assert!(stdout.chars().all(|c| c == 'x'));
        assert!(stderr.chars().all(|c| c == 'x'));
    }

    #[test]
    fn ok_codes_widen_success_without_masking_other_failures() {
        let one = ProcessResult::new(
            "grep".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Exited(1),
            None,
        )
        .with_ok_codes(vec![0, 1]);
        assert!(one.is_success(), "exit 1 is success when accepted");
        assert!(one.ensure_success().is_ok());

        let two = ProcessResult::new(
            "grep".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Exited(2),
            None,
        )
        .with_ok_codes(vec![0, 1]);
        assert!(!two.is_success(), "an unaccepted code is still a failure");
        assert!(matches!(
            two.ensure_success().unwrap_err(),
            Error::Exit { code: 2, .. }
        ));
    }

    #[test]
    fn new_defaults_zero_duration_untruncated_and_zero_only_success() {
        let r = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Exited(1),
            None,
        );
        assert_eq!(r.duration(), Duration::ZERO);
        assert!(!r.truncated());
        assert!(!r.is_success(), "the default accepts only code 0");

        let stamped = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Exited(0),
            None,
        )
        .with_duration(Duration::from_millis(5))
        .with_truncated(true);
        assert_eq!(stamped.duration(), Duration::from_millis(5));
        assert!(stamped.truncated());
        assert!(stamped.is_success());
    }

    #[test]
    fn empty_ok_codes_is_ignored() {
        let r = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Exited(0),
            None,
        )
        .with_ok_codes(vec![]);
        assert!(
            r.is_success(),
            "an empty accepted set falls back to the default [0]"
        );
    }

    #[test]
    fn equality_ignores_duration_and_truncation() {
        // The cassette round-trip relies on this: a recorded result (real,
        // non-zero duration) must compare equal to its replay (Duration::ZERO).
        let base = ProcessResult::new(
            "p".into(),
            "out".to_owned(),
            String::new(),
            Outcome::Exited(0),
            None,
        );
        let measured = base
            .clone()
            .with_duration(Duration::from_secs(5))
            .with_truncated(true);
        assert_eq!(
            base, measured,
            "duration and truncation are not part of identity"
        );

        // …but the logical fields still distinguish results.
        let different = ProcessResult::new(
            "p".into(),
            "DIFF".to_owned(),
            String::new(),
            Outcome::Exited(0),
            None,
        );
        assert_ne!(base, different);
        let widened = base.clone().with_ok_codes(vec![0, 1]);
        assert_ne!(base, widened, "ok_codes is part of identity");
    }
}
