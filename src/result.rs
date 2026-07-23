//! The captured outcome of a finished process run.

use std::fmt;
use std::time::Duration;

use crate::error::Error;

/// How a run ended — the explicit form of the `code()`/`timed_out()` pair.
///
/// Non-exhaustive: a future disposition (e.g. a richer platform-specific
/// termination) can be added without a breaking change — so prefer the
/// accessors [`code`](Self::code) / [`signal`](Self::signal) /
/// [`timed_out`](Self::timed_out) over a `match` with a wildcard when you hold a
/// bare `Outcome` (e.g. from [`RunningProcess::wait`](crate::RunningProcess::wait)).
///
/// There is deliberately **no** `Outcome::is_success`: success depends on the
/// accepted exit codes ([`Command::ok_codes`](crate::Command::ok_codes)), which a
/// bare `Outcome` does not carry — use
/// [`ProcessResult::is_success`](crate::ProcessResult::is_success) (which honors
/// `ok_codes`) rather than assuming `Exited(0)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// The process exited on its own with this code.
    ///
    /// On **Windows** this also covers a *killed* process: the OS has no signal
    /// abstraction, so termination yields an exit code, not a
    /// [`Signalled`](Self::Signalled). A `TerminateProcess`/
    /// `TerminateJobObject(_, 1)` is reported as `Exited(1)` — indistinguishable
    /// from a voluntary `exit(1)` — and `Ctrl-C` surfaces as
    /// `Exited(-1073741510)` (`STATUS_CONTROL_C_EXIT` as a signed `i32`). This is
    /// the platform truth; the crate does not fabricate a `Signalled` from an
    /// NTSTATUS code (the mapping would be a lossy guess). Use a
    /// [`ProcessGroup`](crate::ProcessGroup) deadline / cancellation token when
    /// you need to *know* you killed it.
    Exited(i32),
    /// Terminated by a signal **(Unix only)** — no exit code exists. The inner
    /// value is the signal number when the platform reports one (`None` when the
    /// kernel does not expose it). **Never produced on Windows**, where a killed
    /// process reports [`Exited`](Self::Exited) with a platform code instead.
    /// A [`ScriptedRunner`](crate::testing::ScriptedRunner) reports
    /// `Signalled(None)` for a killed scripted run, matching Unix.
    Signalled(Option<i32>),
    /// Killed because it exceeded its configured timeout.
    TimedOut,
}

impl Outcome {
    /// The exit code if the process [`Exited`](Self::Exited), else `None`
    /// (a signal kill or timeout has no exit code — never a `-1` sentinel).
    ///
    /// The accessor for a bare `Outcome` (e.g. from
    /// [`RunningProcess::wait`](crate::RunningProcess::wait) or
    /// [`Finished::outcome`](crate::Finished)); mirrors
    /// [`ProcessResult::code`](crate::ProcessResult::code). Because `Outcome` is
    /// `#[non_exhaustive]`, prefer these accessors over a `match` with a wildcard.
    pub fn code(&self) -> Option<i32> {
        // Exhaustive (no wildcard) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, forcing a
        // deliberate decision rather than silently returning `None` for it.
        match self {
            Outcome::Exited(code) => Some(*code),
            Outcome::Signalled(_) | Outcome::TimedOut => None,
        }
    }

    /// The signal number if the process was [`Signalled`](Self::Signalled) with a
    /// known number, else `None` (a clean exit, a timeout, or a signal the kernel
    /// did not expose). **Unix only** — a killed process reports
    /// [`Exited`](Self::Exited) on Windows.
    pub fn signal(&self) -> Option<i32> {
        // Exhaustive on purpose (see `code`): a future variant must be classified
        // here rather than defaulting to `None`.
        match self {
            Outcome::Signalled(signal) => *signal,
            Outcome::Exited(_) | Outcome::TimedOut => None,
        }
    }

    /// Whether the run was killed because it exceeded its timeout. Mirrors
    /// [`ProcessResult::timed_out`](crate::ProcessResult::timed_out).
    pub fn timed_out(&self) -> bool {
        matches!(self, Outcome::TimedOut)
    }

    /// This outcome's **stable machine identifier** — the *kind* of
    /// disposition as a short, lowercase `snake_case` string (`"exited"`,
    /// `"signalled"`, `"timed_out"`), part of the crate's compatibility
    /// surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per disposition instead of hand-maintaining its
    /// own mapping table. It is a *diagnostic* name, **not** a wire format, but
    /// it is held stable all the same: a **new** variant gets a **new**
    /// identifier, and an existing identifier is **never renamed** without a
    /// major release.
    ///
    /// This names the disposition **only**; the payload (the exit code of
    /// [`Exited`](Self::Exited), the signal number of [`Signalled`](Self::Signalled))
    /// travels separately via [`code`](Self::code) / [`signal`](Self::signal).
    /// There is deliberately no `from_name` inverse: the name alone cannot
    /// reconstruct that payload, and an `Outcome` is a *reported* result, never
    /// supplied to the crate from the outside.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            Outcome::Exited(_) => "exited",
            Outcome::Signalled(_) => "signalled",
            Outcome::TimedOut => "timed_out",
        }
    }
}

/// The captured result of running a process to completion.
///
/// `T` is the standard-output payload: [`String`] for the text helpers
/// (`output_string`) or [`Vec<u8>`] for the raw-bytes helper (`output_bytes`).
/// Standard error is always captured as text. A non-zero exit code is **not**
/// treated as an error on its own — inspect [`code`](Self::code) or call
/// [`ensure_success`](Self::ensure_success).
///
/// `#[must_use]`: the result *is* the exit status — dropping it unread discards
/// the only signal that the command failed. Call
/// [`is_success`](Self::is_success) / [`code`](Self::code) /
/// [`ensure_success`](Self::ensure_success). If you only need the side effect,
/// prefer a verb that never produces a result to discard —
/// [`run_unit`](crate::Command::run_unit) (error-or-`()`) or
/// [`exit_code`](crate::Command::exit_code) — rather than capturing and binding
/// to `let _`.
#[must_use = "a ProcessResult carries the exit status; inspect is_success()/code()/ensure_success() or it is silently discarded"]
#[derive(Clone)]
pub struct ProcessResult<T> {
    program: String,
    stdout: T,
    stderr: String,
    outcome: Outcome,
    /// Carried so the success-checking helpers can build a faithful [`Error::Timeout`].
    timeout: Option<Duration>,
    /// `Duration::ZERO` for synthetic results that didn't time a real process.
    duration: Duration,
    /// Whether a bounded [`OutputBufferPolicy`](crate::OutputBufferPolicy)
    /// dropped captured output lines.
    truncated: bool,
    /// Retained + dropped, so a checking verb can build a faithful
    /// [`Error::OutputTooLarge`]; meaningful only when `truncated`.
    total_lines: usize,
    total_bytes: usize,
    /// Exit codes treated as success by [`is_success`](Self::is_success) /
    /// [`ensure_success`](Self::ensure_success). Default `[0]`; widened via
    /// [`Command::ok_codes`](crate::Command::ok_codes).
    ok_codes: Vec<i32>,
}

/// Debug wrapper for a raw-bytes stdout: a length summary, never the content —
/// it may be binary, may carry secrets, and can be multi-MiB.
struct BytesLen(usize);
impl fmt::Debug for BytesLen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} bytes>", self.0)
    }
}

// Shared body of the manual `Debug` — `stdout` is pre-wrapped so the caller
// controls how the payload is bounded (a `StreamPreview` for text, a `BytesLen`
// for raw bytes). Concrete impls below (rather than a generic bound) keep the
// private preview types out of the public API surface.
fn fmt_process_result<T>(
    r: &ProcessResult<T>,
    stdout: &dyn fmt::Debug,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    f.debug_struct("ProcessResult")
        .field("program", &r.program)
        .field("stdout", stdout)
        .field("stderr", &crate::error::StreamPreview(r.stderr.as_str()))
        .field("outcome", &r.outcome)
        .field("timeout", &r.timeout)
        .field("duration", &r.duration)
        .field("truncated", &r.truncated)
        .field("total_lines", &r.total_lines)
        .field("total_bytes", &r.total_bytes)
        .field("ok_codes", &r.ok_codes)
        .finish()
}

// Manual `Debug`: bound the captured streams to a preview rather than the derived
// full dump — a `panic!("{result:?}")` (or a tracing line) on a 100 MB capture
// must not print it all, the same "no unbounded text in Debug" rule `Error`
// follows. `Debug` output is not semver-covered. Concrete impls for the only two
// payload types (`String`, `Vec<u8>`).
impl fmt::Debug for ProcessResult<String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_process_result(self, &crate::error::StreamPreview(self.stdout.as_str()), f)
    }
}

impl fmt::Debug for ProcessResult<Vec<u8>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_process_result(self, &BytesLen(self.stdout.len()), f)
    }
}

// Equality is over the *logical* outcome: `duration`, `truncated`, and the
// overflow totals are excluded so a recorded result compares equal to its replay.
// A cassette records the duration at millisecond granularity, so a sub-millisecond
// run's replay won't byte-match its `duration` even though it is logically the
// same run — excluding these keeps the round-trip contract intact.
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
            // Producer-only setters fill these on real runs; defaults keep
            // synthetic results correct.
            duration: Duration::ZERO,
            truncated: false,
            total_lines: 0,
            total_bytes: 0,
            ok_codes: vec![0],
        }
    }

    /// The program this result is attributed to (lossy UTF-8 of the program
    /// name) — the same value the error variants carry. For a
    /// [`Pipeline`](crate::Pipeline) outcome this is usually the
    /// pipefail-attributed stage: the first stage that didn't exit cleanly, or
    /// the last stage when every stage succeeded. If the pipeline's chain-wide
    /// [`Pipeline::timeout`](crate::Pipeline::timeout) elapses instead, this is
    /// the composite pipeline name: all stage names joined by `" | "`.
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
        self.outcome.code()
    }

    /// Whether the run was killed because it exceeded its timeout. Derived
    /// from [`outcome`](Self::outcome).
    pub fn timed_out(&self) -> bool {
        self.outcome.timed_out()
    }

    /// The signal number if the process was terminated by a signal with a known
    /// number (**Unix only**; `None` otherwise — a clean exit, a timeout, or a
    /// signal the kernel did not expose). Derived from [`outcome`](Self::outcome);
    /// the `Outcome`-level twin is [`Outcome::signal`].
    pub fn signal(&self) -> Option<i32> {
        self.outcome.signal()
    }

    /// Whether the process exited with an **accepted** code — `0` by default, or
    /// any code in the set configured via
    /// [`Command::ok_codes`](crate::Command::ok_codes).
    pub fn is_success(&self) -> bool {
        matches!(self.outcome, Outcome::Exited(code) if self.ok_codes.contains(&code))
    }

    /// Return `self` unchanged when the run succeeded — an **accepted** exit code
    /// (`0` by default; see [`Command::ok_codes`](crate::Command::ok_codes)) —
    /// otherwise the matching error.
    ///
    /// # Errors
    ///
    /// - [`Error::Timeout`] if the run was killed by its deadline (checked
    ///   *first*, so a run that both timed out and exited non-zero reports the
    ///   timeout).
    /// - [`Error::Signalled`](crate::Error::Signalled) if it was terminated by a
    ///   signal, with no exit code.
    /// - [`Error::Exit`] for an exit code outside the accepted set, carrying the
    ///   code and both captured streams in full (the
    ///   [`Display`](std::fmt::Display) impl bounds what it prints; the fields
    ///   stay complete for classification).
    pub fn ensure_success(self) -> Result<Self, Error>
    where
        T: StdoutText,
    {
        match self.outcome {
            Outcome::Exited(code) if self.ok_codes.contains(&code) => Ok(self),
            Outcome::TimedOut => Err(Error::Timeout {
                program: self.program.clone(),
                timeout: self.timeout.unwrap_or_default(),
                stdout: self.stdout.as_text(),
                stderr: self.stderr.clone(),
                stdout_bytes: self.stdout.into_raw(),
            }),
            Outcome::Signalled(signal) => Err(Error::Signalled {
                program: self.program.clone(),
                signal,
                stdout: self.stdout.as_text(),
                stderr: self.stderr.clone(),
                stdout_bytes: self.stdout.into_raw(),
            }),
            Outcome::Exited(code) => Err(Error::Exit {
                program: self.program.clone(),
                code,
                stdout: self.stdout.as_text(),
                stderr: self.stderr.clone(),
                stdout_bytes: self.stdout.into_raw(),
            }),
        }
    }

    /// The exit code for the code-returning convenience helpers
    /// (`Command::exit_code`, `ProcessRunnerExt::exit_code`, `CliClient::exit_code`):
    /// a timeout surfaces as [`Error::Timeout`], a signal-kill (no code) as
    /// [`Error::Signalled`], otherwise the code. Takes `self` by value (rather
    /// than `&self`) so the bytes-path payload can move its raw stdout into the
    /// error instead of cloning it — every call site already holds a
    /// freshly-produced, otherwise-unused `ProcessResult`.
    pub(crate) fn require_code(self) -> Result<i32, Error>
    where
        T: StdoutText,
    {
        match self.outcome {
            Outcome::Exited(code) => Ok(code),
            Outcome::TimedOut => Err(Error::Timeout {
                program: self.program.clone(),
                timeout: self.timeout.unwrap_or_default(),
                stdout: self.stdout.as_text(),
                stderr: self.stderr.clone(),
                stdout_bytes: self.stdout.into_raw(),
            }),
            Outcome::Signalled(signal) => Err(Error::Signalled {
                program: self.program.clone(),
                signal,
                stdout: self.stdout.as_text(),
                stderr: self.stderr.clone(),
                stdout_bytes: self.stdout.into_raw(),
            }),
        }
    }

    /// The wall-clock duration of the run — spawn to exit (or kill). It is
    /// `Duration::ZERO` for synthetic results that didn't time a real process
    /// (a scripted/replayed bulk `output_string`).
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

    /// Record the total lines/bytes the streams produced (retained + dropped),
    /// so a checking verb can build a faithful [`Error::OutputTooLarge`] when it
    /// refuses truncated output. Producer-only.
    pub(crate) fn with_overflow_totals(mut self, total_lines: usize, total_bytes: usize) -> Self {
        self.total_lines = total_lines;
        self.total_bytes = total_bytes;
        self
    }

    /// Total lines seen across the captured streams (retained + dropped) —
    /// companion to [`truncated`](Self::truncated) for re-stamping a folded
    /// result (e.g. the pipeline pipefail fold). Producer-only.
    pub(crate) fn total_lines(&self) -> usize {
        self.total_lines
    }

    /// Total bytes seen across the captured streams (retained + dropped).
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Refuse silently-truncated output. The checking verbs that hand back
    /// stdout *as if complete* (`run`/`parse`/`try_parse`) call this so a bounded
    /// drop-policy that discarded lines surfaces as [`Error::OutputTooLarge`]
    /// rather than feeding a parser a truncated tail. The lenient capture verbs
    /// (`output_string`/`output_bytes`/`checked`) do **not** call it — they
    /// return the result with [`truncated`](Self::truncated) set so the caller
    /// decides. `max_lines`/`max_bytes` are the command's configured ceilings.
    pub(crate) fn reject_if_truncated(
        &self,
        max_lines: Option<usize>,
        max_bytes: Option<usize>,
    ) -> Result<(), Error> {
        if self.truncated {
            return Err(Error::OutputTooLarge {
                program: self.program.clone(),
                max_lines,
                max_bytes,
                total_lines: self.total_lines,
                total_bytes: self.total_bytes,
            });
        }
        Ok(())
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
    /// Standard output followed by standard error, **concatenated** — handy when a
    /// tool splits diagnostics across both streams. This is `stdout` then `stderr`
    /// (each captured in full), **not** a temporal interleaving of the two — the
    /// crate captures the streams separately, so their relative ordering is not
    /// preserved.
    ///
    /// A `\n` separator is inserted between them only when **both** streams are
    /// non-empty and stdout does not already end with a newline, preventing the
    /// last stdout line from being glued to the first stderr line. (Matches
    /// [`Error::combined`](crate::Error::combined).)
    pub fn combined(&self) -> String {
        combine_streams(self.stdout(), self.stderr())
    }

    /// The shared `probe` contract — exit `0` → `Ok(true)`, exit `1` →
    /// `Ok(false)`, anything else → `Err` — reused by
    /// [`ProcessRunnerExt::probe`](crate::ProcessRunnerExt::probe) and
    /// [`Pipeline::probe`](crate::Pipeline::probe) so the classification lives in
    /// exactly one place. Resets `ok_codes` to the default `{0}` first: `probe`
    /// keeps its strict 0/1 contract regardless of a command's/stage's
    /// `ok_codes`, and an *accepted* non-{0,1} code would otherwise make
    /// `ensure_success` return `Ok` and panic the `expect_err`.
    pub(crate) fn probe_bool(self) -> Result<bool, Error> {
        match self.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(self
                .with_ok_codes(vec![0])
                .ensure_success()
                .expect_err("a non-{0,1} exit code is never success")),
        }
    }

    /// Whether **either** captured stream contains any of `needles`, matched
    /// **case-insensitively** (ASCII). For the lenient "a specific non-zero exit
    /// is benign when a known stderr/stdout marker is present" idiom — e.g. `gh`'s
    /// `"no checks reported"` or `jj`'s `"no conflicts"` — without re-lowercasing a
    /// stream by hand each time. **Allocation-free** (it never materializes a
    /// lowercased copy of a possibly-large captured stream). The two streams are
    /// searched independently, so a needle never matches across the stdout/stderr
    /// boundary. An empty `needles` iterator is `false`; an empty `""` needle
    /// follows [`str::contains`] (always present).
    ///
    /// Accepts any iterable of string-like items — `&["a", "b"]`, `Vec<String>`,
    /// a single `["a"]` array, … — matching the other multi-input builders
    /// ([`Command::args`](crate::Command::args),
    /// [`Command::envs`](crate::Command::envs)).
    pub fn output_contains_any(&self, needles: impl IntoIterator<Item = impl AsRef<str>>) -> bool {
        needles.into_iter().any(|needle| {
            let needle = needle.as_ref();
            contains_ascii_ci(&self.stdout, needle) || contains_ascii_ci(&self.stderr, needle)
        })
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

/// Join captured stdout and stderr — stdout, then stderr, with a single `\n`
/// inserted between only when both are non-empty and stdout doesn't already end
/// in a newline (so a clean run's two streams read as consecutive lines, and an
/// empty stream adds nothing). The one implementation shared by
/// [`ProcessResult::combined`] and [`Error::combined`](crate::Error::combined),
/// so the two `combined()` views can't drift.
pub(crate) fn combine_streams(out: &str, err: &str) -> String {
    if !out.is_empty() && !err.is_empty() && !out.ends_with('\n') {
        format!("{out}\n{err}")
    } else {
        format!("{out}{err}")
    }
}

/// ASCII-case-insensitive substring test, **allocation-free** — it never builds a
/// lowercased copy of `haystack` (a captured stream the [`Error`] variants carry
/// in full, potentially multi-MiB). An empty `needle` is always present, matching
/// [`str::contains`]. Backs [`ProcessResult::output_contains_any`].
fn contains_ascii_ci(haystack: &str, needle: &str) -> bool {
    let (hay, needle) = (haystack.as_bytes(), needle.as_bytes());
    // `<[u8]>::windows(0)` panics, so the empty needle is handled first.
    needle.is_empty()
        || hay
            .windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle))
}

/// Render captured stdout as text for [`Error::Exit`], whatever the payload type:
/// a [`String`] is taken as-is; raw bytes are decoded lossily. Also exposes the
/// exact captured bytes when the payload type carries them losslessly
/// (`Vec<u8>` — the `output_bytes()` path), so a checking verb
/// (`ensure_success`/`require_code`) can populate the `Exit`/`Timeout`/
/// `Signalled` variants' `stdout_bytes` field without a second lossy round trip.
///
/// An implementation detail of [`ensure_success`](ProcessResult::ensure_success):
/// `pub` only to satisfy the bound's visibility (the `result` module is private,
/// so it is unnameable outside the crate) and `#[doc(hidden)]` so it stays off
/// the public docs.
#[doc(hidden)]
pub trait StdoutText {
    fn as_text(&self) -> String;

    /// The exact captured bytes, when the payload type carries them losslessly
    /// (`Vec<u8>`); `None` for an already-decoded text payload (`String`) —
    /// there is no separate "raw" form to recover there (`as_text` above is
    /// already the complete, non-lossy text).
    fn into_raw(self) -> Option<Vec<u8>>;
}

impl StdoutText for String {
    fn as_text(&self) -> String {
        self.clone()
    }

    fn into_raw(self) -> Option<Vec<u8>> {
        None
    }
}

impl StdoutText for Vec<u8> {
    fn as_text(&self) -> String {
        String::from_utf8_lossy(self).into_owned()
    }

    fn into_raw(self) -> Option<Vec<u8>> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_accessors_match_each_variant() {
        // The bare-`Outcome` accessors, so callers needn't `match` a
        // non_exhaustive enum with a wildcard.
        assert_eq!(Outcome::Exited(7).code(), Some(7));
        assert_eq!(Outcome::Exited(7).signal(), None);
        assert!(!Outcome::Exited(7).timed_out());

        assert_eq!(Outcome::Signalled(Some(9)).signal(), Some(9));
        assert_eq!(Outcome::Signalled(Some(9)).code(), None);
        assert_eq!(Outcome::Signalled(None).signal(), None);
        assert!(!Outcome::Signalled(None).timed_out());

        assert!(Outcome::TimedOut.timed_out());
        assert_eq!(Outcome::TimedOut.code(), None);
        assert_eq!(Outcome::TimedOut.signal(), None);

        // The accessors agree with the ProcessResult-level ones derived from the
        // same outcome.
        let exited = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Exited(2),
            None,
        );
        assert_eq!(exited.outcome().code(), exited.code());
        assert_eq!(exited.outcome().signal(), exited.signal());
        assert_eq!(exited.outcome().timed_out(), exited.timed_out());

        let killed = ProcessResult::new(
            "x".into(),
            String::new(),
            String::new(),
            Outcome::Signalled(Some(9)),
            None,
        );
        assert_eq!(killed.signal(), Some(9));
        assert_eq!(killed.code(), None);
    }

    #[test]
    fn name_pins_each_disposition_regardless_of_payload() {
        // The kind identifier is a compatibility surface and must not drift; it
        // names the disposition only, independent of the carried payload.
        assert_eq!(Outcome::Exited(0).name(), "exited");
        assert_eq!(Outcome::Exited(137).name(), "exited");
        assert_eq!(Outcome::Signalled(Some(9)).name(), "signalled");
        assert_eq!(Outcome::Signalled(None).name(), "signalled");
        assert_eq!(Outcome::TimedOut.name(), "timed_out");
    }

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
            // The captured stdout flows into the error.
            Error::Signalled {
                program,
                signal,
                stdout,
                ..
            } => {
                assert_eq!(program, "git");
                assert_eq!(signal, Some(9));
                assert_eq!(stdout, "out");
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
                ..
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
        let Error::Exit { .. } = conflict.clone().ensure_success().unwrap_err() else {
            panic!("expected Exit");
        };
        assert_eq!(
            conflict.ensure_success().unwrap_err().diagnostic(),
            Some("CONFLICT (content): merge conflict in a.rs")
        );

        // Both streams blank → no captured message; the caller falls back to the
        // Display text rather than an empty string.
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
        // A timed-out run must report as a distinct Timeout, not a non-zero Exit.
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
            Error::Timeout {
                program,
                timeout,
                stdout,
                ..
            } => {
                assert_eq!(program, "git");
                assert_eq!(timeout, Duration::from_millis(500));
                // Partial stdout captured before the kill is carried.
                assert_eq!(stdout, "out");
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn signal_kill_surfaces_as_signalled_error() {
        // A signal-terminated run (no code, not a timeout) must surface
        // `Error::Signalled` from both `require_code` and `ensure_success`,
        // never a synthetic `Error::Exit { code: -1 }` sentinel.
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
            killed.clone().require_code().unwrap_err(),
            Error::Signalled { .. }
        ));
        match killed.ensure_success().unwrap_err() {
            Error::Signalled {
                program, signal, ..
            } => {
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
        // A newline separator is inserted so the last stdout line is not glued
        // to the first stderr line.
        assert_eq!(r.combined(), "out\nerr");
    }

    #[test]
    fn output_contains_any_is_case_insensitive_over_both_streams() {
        let r = ProcessResult::new(
            "gh".into(),
            "No Checks Reported\n".to_owned(),
            "warning: HTTP 401".to_owned(),
            Outcome::Exited(1),
            None,
        );
        // Case-insensitive; matches a marker on stdout OR stderr; any-of semantics.
        assert!(r.output_contains_any(["no checks reported"]));
        assert!(r.output_contains_any(["http 401"]));
        assert!(r.output_contains_any(["missing", "HTTP 401"]));
        assert!(!r.output_contains_any(["nothing to commit"]));
        // An empty needle iterator matches nothing; an empty `""` needle follows
        // `str::contains` (always present).
        assert!(!r.output_contains_any([] as [&str; 0]));
        assert!(r.output_contains_any([""]));
        // A `Vec` and a slice work too — any `IntoIterator<Item: AsRef<str>>` does.
        assert!(r.output_contains_any(vec!["http 401".to_owned()]));
        assert!(r.output_contains_any(["missing", "HTTP 401"].as_slice()));

        // The streams are searched independently — a needle never matches across
        // the stdout/stderr seam.
        let seam = ProcessResult::new(
            "p".into(),
            "foo".to_owned(),
            "bar".to_owned(),
            Outcome::Exited(1),
            None,
        );
        assert!(!seam.output_contains_any(["foobar"]));
        assert!(!seam.output_contains_any(["foo\nbar"]));
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
        // classify on them (grep for a marker, parse a code). The `Display` impl
        // bounds what it *prints*; the fields stay whole.
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
    fn bytes_path_error_carries_the_exact_raw_stdout() {
        // H4: after a failed `output_bytes().await?.ensure_success()?`, the exact
        // raw stdout bytes must be reachable — not just a lossy UTF-8 re-decode.
        // Invalid UTF-8 exercises the case where `stdout` (the lossy text field)
        // and `stdout_bytes` (the exact bytes) necessarily differ.
        let mut raw = b"before-".to_vec();
        raw.extend_from_slice(&[0xFF, 0xFE, 0x00, 0xC3, 0x28]); // not valid UTF-8
        raw.extend_from_slice(b"-after");
        let bad = ProcessResult::new(
            "p".into(),
            raw.clone(),
            String::new(),
            Outcome::Exited(1),
            None,
        );
        let err = bad.ensure_success().unwrap_err();
        assert_eq!(
            err.stdout_bytes(),
            Some(raw.as_slice()),
            "the exact pre-decode bytes must survive, byte for byte"
        );
        match &err {
            Error::Exit { stdout_bytes, .. } => {
                assert_eq!(stdout_bytes.as_deref(), Some(raw.as_slice()));
            }
            other => panic!("expected Exit, got {other:?}"),
        }
        // The lossy text field is still populated (with replacement chars),
        // but is *not* required to byte-match the raw capture.
        assert!(err.stdout().unwrap().contains('\u{FFFD}'));

        // The text path (`String` payload) has no separate raw form to recover:
        // `stdout_bytes` is `None`, never a re-encoding of the text field.
        let text_bad = ProcessResult::new(
            "p".into(),
            "hello".to_owned(),
            String::new(),
            Outcome::Exited(1),
            None,
        );
        let text_err = text_bad.ensure_success().unwrap_err();
        assert_eq!(text_err.stdout_bytes(), None);
        assert_eq!(text_err.stdout(), Some("hello"));
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
    fn checking_verbs_reject_truncated_output() {
        // A checking verb that hands back stdout (run/parse/try_parse) must refuse
        // silently-truncated output, surfacing OutputTooLarge with the configured
        // limits and the totals seen — not feed a parser a tail.
        let truncated = ProcessResult::new(
            "p".into(),
            "tail".to_owned(),
            String::new(),
            Outcome::Exited(0),
            None,
        )
        .with_truncated(true)
        .with_overflow_totals(5000, 1_000_000);
        match truncated.reject_if_truncated(Some(100), None) {
            Err(Error::OutputTooLarge {
                program,
                max_lines,
                max_bytes,
                total_lines,
                total_bytes,
            }) => {
                assert_eq!(program, "p");
                assert_eq!(max_lines, Some(100));
                assert_eq!(max_bytes, None);
                assert_eq!(total_lines, 5000);
                assert_eq!(total_bytes, 1_000_000);
            }
            other => panic!("expected OutputTooLarge, got {other:?}"),
        }

        // A complete (untruncated) result passes.
        let complete = ProcessResult::new(
            "p".into(),
            "full".to_owned(),
            String::new(),
            Outcome::Exited(0),
            None,
        );
        assert!(complete.reject_if_truncated(Some(100), None).is_ok());
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
        // The cassette round-trip relies on this: a recorded result (non-zero
        // duration) must compare equal to its replay (Duration::ZERO).
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

    #[test]
    fn debug_bounds_a_large_capture() {
        // H1: a multi-MiB capture must not be dumped in full by `{result:?}`.
        let big = "x".repeat(10_000);
        let text = ProcessResult::new(
            "tool".into(),
            big.clone(),
            String::new(),
            Outcome::Exited(0),
            None,
        );
        let dbg = format!("{text:?}");
        assert!(
            dbg.len() < 500,
            "Debug must bound the 10k stdout, got {} chars",
            dbg.len()
        );
        assert!(dbg.contains("(+"), "Debug shows a truncation note: {dbg}");
        // A byte payload renders a length summary, never the (possibly binary /
        // secret) content.
        let bytes = ProcessResult::new(
            "tool".into(),
            big.into_bytes(),
            String::new(),
            Outcome::Exited(0),
            None,
        );
        assert!(
            format!("{bytes:?}").contains("<10000 bytes>"),
            "byte stdout is a length summary"
        );
    }

    #[test]
    fn zero_timeout_omits_the_misleading_after_clause() {
        // H3: a TimedOut result whose command carried no `timeout` (a scripted /
        // cassette-replayed timeout) renders "timed out", not "timed out after 0ns".
        let replayed = ProcessResult::new(
            "tool".into(),
            String::new(),
            String::new(),
            Outcome::TimedOut,
            None,
        );
        let msg = replayed
            .ensure_success()
            .expect_err("a timeout is an error")
            .to_string();
        assert!(msg.contains("timed out"), "got {msg}");
        assert!(!msg.contains("0ns"), "must omit the 0ns clause, got {msg}");
        // A known timeout still shows the duration.
        let real = ProcessResult::new(
            "tool".into(),
            String::new(),
            String::new(),
            Outcome::TimedOut,
            Some(Duration::from_secs(5)),
        );
        assert!(
            real.ensure_success()
                .expect_err("timeout")
                .to_string()
                .contains("5s"),
            "a known timeout shows the duration"
        );
    }
}
