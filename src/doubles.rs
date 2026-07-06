//! Test doubles for the [`ProcessRunner`] seam — no real subprocess required.
//!
//! - [`ScriptedRunner`] returns canned output for commands matched by a
//!   program-and-argument prefix (the first element is the program) or a
//!   predicate.
//! - [`RecordingRunner`] wraps another runner and records every [`Invocation`]
//!   so tests can assert exactly what was run.
//! - [`DryRunRunner`] never spawns anything: it renders each command through
//!   [`Command::command_line`], hands the rendered line to a collected
//!   snapshot and/or a callback, and returns a synthetic successful result —
//!   the seam behind a tool's own `--dry-run` echo mode.
//!
//! Behind the `mock` feature, [`mockall`] additionally generates a `MockRunner`
//! for expectation-style mocking. All of these live under
//! [`processkit::testing`](crate::testing), not the crate root.
//!
//! The seam covers **both shapes of a run**. The bulk verbs (`output_string` and the
//! helpers over it) replay canned results — and feed the command's
//! `on_stdout_line`/`on_stderr_line` handlers, so progress-reporting paths
//! test hermetically. A scripted [`start`](crate::ProcessRunner::start) hands
//! back a live [`RunningProcess`](crate::RunningProcess) whose canned output
//! flows through the **same pump machinery** as a real child: `stdout_lines`,
//! `wait_for_line`/`wait_for`, and `finish` all behave identically
//! (per-line pacing via [`Reply::with_line_delay`]). Scripted handles have no
//! OS identity (`pid()` is `None`), don't compose into a real
//! [`Pipeline`](crate::Pipeline), and don't model interactive stdin. The bulk
//! `output_string` replay also does not model a bounded-buffer **truncation** policy
//! (a canned reply is returned whole, `truncated() == false`), so the
//! checking-verb truncation error won't fire against a bulk double — to
//! exercise that, drive output through a scripted [`start`] handle (whose canned
//! lines flow through the real buffer policy).
//!
//! Instant replies never observe a `cancel_on` token (they resolve before any
//! cancellation could race them). To exercise cancellation **behaviour** — a
//! call that genuinely blocks until its token fires — script
//! [`Reply::pending`]: it parks the call (or never
//! "exits", on `start`) until the command's token — per-command or
//! [`CliClient::default_cancel_on`](crate::CliClient::default_cancel_on) —
//! cancels, then resolves with `Err(Error::Cancelled { .. })`, mirroring the
//! live contract. A pending call is likewise bounded by the command's
//! [`Command::timeout`](crate::Command::timeout): with a deadline set it resolves
//! timed-out (`Outcome::TimedOut`) rather than parking, on the bulk verbs and
//! `start` alike, just as a live child killed for overrunning its deadline would.

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::command::Command;
use crate::error::Result;
use crate::result::{Outcome, ProcessResult};
use crate::runner::ProcessRunner;

/// A canned reply: stdout/stderr text plus an exit code (or a timed-out run,
/// or a parked-until-cancelled call).
#[derive(Debug, Clone)]
pub struct Reply {
    stdout: String,
    stderr: String,
    code: i32,
    timed_out: bool,
    /// Set by [`signalled`](Self::signalled): the reply is a signal kill. Kept
    /// separate from `signal` so `signalled(None)` (killed, number unknown) is
    /// distinct from a plain successful reply (both would have `signal: None`).
    signalled: bool,
    /// Signal number for a signal-killed reply (see [`signalled`](Self::signalled)).
    signal: Option<i32>,
    /// Park the call until the command's cancellation token fires or its
    /// `timeout` deadline elapses (see [`pending`](Self::pending)); the other
    /// fields are unused then.
    pending: bool,
    /// On a scripted `start`, sleep this long before each stdout line (see
    /// [`with_line_delay`](Self::with_line_delay)). Bulk `output_string` ignores it.
    line_delay: Option<std::time::Duration>,
    /// Set by [`not_found`](Self::not_found)/[`spawn_error`](Self::spawn_error): the
    /// reply is a spawn-side failure — the match never "runs" at all, so every
    /// other field is unused. Checked before both bulk and streaming consumption.
    spawn_error: Option<SpawnError>,
}

/// A spawn-side failure a [`Reply`] can express (see
/// [`Reply::not_found`]/[`Reply::spawn_error`]) — the fake's analogue of the
/// live launch path failing before a child ever ran, as opposed to a completed
/// run with a failing exit code.
#[derive(Debug, Clone)]
enum SpawnError {
    /// Drives [`Error::NotFound`](crate::Error::NotFound) (`is_not_found() ==
    /// true`).
    NotFound,
    /// Drives [`Error::Spawn`](crate::Error::Spawn) carrying the given
    /// [`std::io::ErrorKind`] and message.
    Spawn {
        kind: std::io::ErrorKind,
        message: String,
    },
}

impl Reply {
    /// A successful reply (exit code 0) producing `stdout`. Pair with
    /// [`with_stderr`](Self::with_stderr) to also give the successful reply
    /// stderr text (e.g. warnings a CLI writes on exit 0).
    pub fn ok(stdout: impl Into<String>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            code: 0,
            timed_out: false,
            signalled: false,
            signal: None,
            pending: false,
            line_delay: None,
            spawn_error: None,
        }
    }

    /// A failing reply with exit `code` and `stderr` text. Use
    /// [`with_stderr`](Self::with_stderr) afterwards to override this
    /// stderr text.
    pub fn fail(code: i32, stderr: impl Into<String>) -> Self {
        Self {
            stdout: String::new(),
            stderr: stderr.into(),
            code,
            timed_out: false,
            signalled: false,
            signal: None,
            pending: false,
            line_delay: None,
            spawn_error: None,
        }
    }

    /// A timed-out reply — drives the timeout path so a test can assert that a
    /// command which exceeds its deadline surfaces as [`Error::Timeout`](crate::Error::Timeout).
    ///
    /// On both the bulk verbs (`output_string` and friends) and a scripted
    /// [`start`](crate::ProcessRunner::start) this resolves **immediately** as
    /// timed-out, so it asserts the timeout *classification* without exercising the
    /// real deadline race. To model "hangs until the command's `timeout` fires,"
    /// script [`pending`](Self::pending) and set a
    /// [`Command::timeout`](crate::Command::timeout): the call then parks until the
    /// deadline and resolves timed-out — on the bulk verbs and `start` alike,
    /// exactly as the watchdog would drive it for a live child.
    pub fn timeout() -> Self {
        Self {
            stdout: String::new(),
            stderr: String::new(),
            // Unused: a timed-out result carries no code.
            code: 0,
            timed_out: true,
            signalled: false,
            signal: None,
            pending: false,
            line_delay: None,
            spawn_error: None,
        }
    }

    /// A signal-killed reply — the process was terminated by a signal. Drives
    /// the `Outcome::Signalled` path so a test can assert signal-kill handling.
    /// Pass `Some(n)` when the specific signal number matters (e.g. `Some(9)`
    /// for SIGKILL); pass `None` when only "killed by a signal" matters.
    pub fn signalled(signal: Option<i32>) -> Self {
        Self {
            stdout: String::new(),
            stderr: String::new(),
            code: 0,
            timed_out: false,
            signalled: true,
            signal,
            pending: false,
            line_delay: None,
            spawn_error: None,
        }
    }

    /// A reply that **parks the call until its cancellation token fires or its
    /// [`Command::timeout`](crate::Command::timeout) deadline elapses** — the
    /// hermetic mirror of a live long-runner that is either cancelled or killed
    /// for overrunning its deadline, for testing that an orchestration genuinely
    /// cancels (and cleans up) or bounds a hang, not just that it formats a canned
    /// error. A firing token resolves with
    /// [`Error::Cancelled`](crate::Error::Cancelled) naming the program; a firing
    /// deadline resolves as a timed-out run (`Outcome::TimedOut`), exactly as a
    /// [`timeout`](Self::timeout) reply would.
    ///
    /// The token is the matched command's — set per command
    /// ([`Command::cancel_on`]) or client-wide
    /// ([`CliClient::default_cancel_on`](crate::CliClient::default_cancel_on)); the
    /// deadline is its [`timeout`](crate::Command::timeout). Whichever fires first
    /// wins (a tie favors cancellation), on both the bulk `output_string` verb and
    /// a scripted [`start`](crate::ProcessRunner::start), just as the live runner
    /// races its cancel token against its deadline. A pending reply for a command
    /// with **neither** a token **nor** a timeout parks forever, like a hung child
    /// no one can cancel and no deadline bounds — deliberate; pair it with a token,
    /// a `Command::timeout` (or a test timeout) by design.
    pub fn pending() -> Self {
        Self {
            stdout: String::new(),
            stderr: String::new(),
            code: 0,
            timed_out: false,
            signalled: false,
            signal: None,
            pending: true,
            line_delay: None,
            spawn_error: None,
        }
    }

    /// A successful reply whose stdout is `lines` joined with `\n` — reads
    /// naturally for scripted **streaming** (`start` → `stdout_lines` yields
    /// exactly these lines), and is equivalent to [`ok`](Self::ok) with the
    /// joined text for the bulk path.
    pub fn lines<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut text = lines
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>()
            .join("\n");
        if !text.is_empty() {
            text.push('\n');
        }
        Self::ok(text)
    }

    /// On a scripted `start`, sleep `delay` before each stdout line — so a
    /// hermetic streaming test can observe genuinely incremental delivery
    /// (deterministic under `#[tokio::test(start_paused = true)]`). The
    /// scripted run "exits" after the last line. Ignored by the bulk `output_string`
    /// path.
    pub fn with_line_delay(mut self, delay: std::time::Duration) -> Self {
        self.line_delay = Some(delay);
        self
    }

    /// Attach stdout to a reply — e.g. the `CONFLICT …` text `git merge` writes
    /// to stdout on a failing reply, so a test can exercise
    /// [`Error::Exit`](crate::Error::Exit)'s stdout field /
    /// [`ProcessResult::diagnostic`](crate::ProcessResult::diagnostic).
    pub fn with_stdout(mut self, stdout: impl Into<String>) -> Self {
        self.stdout = stdout.into();
        self
    }

    /// Attach stderr to a reply — e.g. the warnings a CLI like `git`, a
    /// compiler, or a linter writes to stderr even on a successful (exit 0)
    /// run, so a test can exercise that stderr without misleadingly reaching
    /// for [`fail`](Self::fail).
    pub fn with_stderr(mut self, stderr: impl Into<String>) -> Self {
        self.stderr = stderr.into();
        self
    }

    /// A reply that fails as if the program could not be located at all — not
    /// installed, not on `PATH`, or the given path doesn't resolve — driving
    /// [`Error::NotFound`](crate::Error::NotFound)
    /// (`is_not_found() == true`), the hermetic mirror of a live spawn hitting a
    /// missing binary. A rule miss on the real runner instead lands on
    /// [`Error::Spawn`](crate::Error::Spawn) (`is_not_found() == false` — a
    /// double-specific "no rule matched" config error, not a modeled spawn
    /// failure), so script this reply explicitly to test a "tool not installed →
    /// fallback" branch: `.on(["rg", …], Reply::not_found())` (or
    /// `.fallback(Reply::not_found())`) lets a caller's
    /// `if err.is_not_found() { try_fallback_tool() }` branch run hermetically.
    pub fn not_found() -> Self {
        let mut reply = Self::ok(String::new());
        reply.spawn_error = Some(SpawnError::NotFound);
        reply
    }

    /// A reply that fails at spawn time with a generic OS-level error —
    /// permission denied, a busy executable, and so on — as opposed to
    /// [`not_found`](Self::not_found)'s "the program doesn't exist at all".
    /// Drives [`Error::Spawn`](crate::Error::Spawn) with an `io::Error` of the
    /// given `kind` and `message`, so classifiers built on it
    /// (e.g. [`Error::is_permission_denied`](crate::Error::is_permission_denied))
    /// answer on the fake exactly as they would for a live spawn failure.
    pub fn spawn_error(kind: std::io::ErrorKind, message: impl Into<String>) -> Self {
        let mut reply = Self::ok(String::new());
        reply.spawn_error = Some(SpawnError::Spawn {
            kind,
            message: message.into(),
        });
        reply
    }

    /// The [`Error`](crate::Error) this reply's spawn failure stands for,
    /// attributed to `program` — or `None` for an ordinary (non-spawn-error)
    /// reply. Checked by both `output_string` and `start` before any other
    /// field is consulted: a spawn-side failure means the match never "ran" at
    /// all.
    fn spawn_error_for(&self, program: String) -> Option<crate::error::Error> {
        match &self.spawn_error {
            Some(SpawnError::NotFound) => Some(crate::error::Error::NotFound {
                program,
                searched: None,
            }),
            Some(SpawnError::Spawn { kind, message }) => Some(crate::error::Error::Spawn {
                program,
                source: std::io::Error::new(*kind, message.clone()),
            }),
            None => None,
        }
    }

    /// A reply reconstructed from a recorded outcome — the decode model shared
    /// with the cassette's `Entry`: `timed_out` → timed out; else a present
    /// `code` → exited; else a signal kill (`signal` optionally absent). Lets the
    /// record/replay runner rebuild a streaming handle ([`into_running`](Self::into_running))
    /// from a stored entry without duplicating `Reply`'s private shape.
    #[cfg(feature = "record")]
    pub(crate) fn from_outcome(
        stdout: String,
        stderr: String,
        code: Option<i32>,
        timed_out: bool,
        signal: Option<i32>,
    ) -> Self {
        Self {
            stdout,
            stderr,
            code: code.unwrap_or_default(),
            timed_out,
            // Signalled iff it neither timed out nor carries an exit code.
            signalled: !timed_out && code.is_none(),
            signal,
            pending: false,
            line_delay: None,
            spawn_error: None,
        }
    }

    /// Build a scripted live handle for `command` from this reply — the
    /// `start` analogue of [`into_result`](Self::into_result). The canned
    /// stdout/stderr feed the command's real pump machinery (handlers, buffer
    /// policy, line terminators all apply); the scripted "process" exits with
    /// the canned code after the last delayed line (immediately without
    /// delays), or never for a [`pending`](Self::pending) reply.
    ///
    /// The canned text is the already-*decoded* output the caller should observe,
    /// so the scripted pumps decode it as UTF-8 (the bytes the feeder writes) —
    /// **not** through the command's `stdout_encoding`, which would re-decode
    /// UTF-8 feeder bytes as, say, UTF-16LE and hand back garbage (see
    /// `RunningProcess::from_scripted`).
    ///
    /// `recorded` carries the truncation/overflow/duration a cassette captured, so
    /// a consumed replay reports the recorded signals instead of re-deriving them
    /// from the re-pumped canned output; `None` (a plain `ScriptedRunner` reply)
    /// keeps the derived values.
    pub(crate) fn into_running(
        self,
        command: &Command,
        recorded: Option<crate::running::ScriptedResultInfo>,
    ) -> crate::RunningProcess {
        // A pending reply never exits on its own; everything else exits after
        // its (possibly zero) total line-delay budget. A canned timeout exits
        // immediately as a timed-out outcome, mirroring `into_result`.
        let lifetime = if self.pending {
            None
        } else {
            let per_line = self.line_delay.unwrap_or_default();
            // Both streams feed concurrently at `line_delay`, so the scripted
            // "process" only finishes once the LONGER stream has drained — count
            // the max, or a lagging stderr gets truncated at the shorter
            // stdout-derived lifetime (which a real child never does). `str::lines`
            // matches the count the pumps actually deliver.
            let stdout_lines = self.stdout.lines().count() as u32;
            let stderr_lines = self.stderr.lines().count() as u32;
            let lines = stdout_lines.max(stderr_lines);
            // Saturate and clamp so a `Duration::MAX`-ish `line_delay` can't
            // overflow the multiply or the later `Instant + lifetime` deadline.
            Some(per_line.saturating_mul(lines).min(crate::MAX_DEADLINE))
        };
        let code_for_scripted = if self.timed_out || self.signalled {
            None
        } else {
            Some(self.code)
        };
        let scripted = crate::running::ScriptedProc::new(
            self.stdout,
            self.stderr,
            code_for_scripted,
            self.timed_out,
            self.signal,
            lifetime,
            self.line_delay,
        );
        crate::RunningProcess::from_scripted(command, scripted, recorded)
    }

    /// The bulk-`output_string` analogue of [`into_running`](Self::into_running):
    /// turn this reply into a [`ProcessResult`] that matches what the live bulk
    /// path would hand back for the same output.
    ///
    /// The live bulk path decodes the child's bytes, splits them into lines under
    /// the command's [`LineTerminator`], then rejoins with `\n`
    /// (`stdout_lines.join("\n")`) — so a trailing terminator is dropped and CRLF
    /// is normalized to LF. Canned text gets the *same* treatment here (via
    /// [`split_pump_lines`](crate::running::split_pump_lines)), so
    /// `Reply::ok("done\n")` yields `"done"` on the fake exactly as it does live —
    /// and the bulk and `start` verbs agree on one double.
    fn into_result(self, program: String, command: &Command) -> ProcessResult<String> {
        // Carry the command's configured timeout so a timed-out reply surfaces as
        // `Error::Timeout` with the real deadline, not a zero duration.
        let timeout = command.configured_timeout();
        let outcome = if self.timed_out {
            Outcome::TimedOut
        } else if self.signalled {
            Outcome::Signalled(self.signal)
        } else {
            Outcome::Exited(self.code)
        };
        let stdout = crate::running::split_pump_lines(&self.stdout, command.out_line_terminator())
            .join("\n");
        let stderr = crate::running::split_pump_lines(&self.stderr, command.err_line_terminator())
            .join("\n");
        ProcessResult::new(program, stdout, stderr, outcome, timeout)
    }
}

/// Build a scripted live [`RunningProcess`](crate::RunningProcess) for `command`
/// from recorded outcome parts — the streaming-`start` counterpart of the bulk
/// replay path. Shared by the `record`-feature cassette so a recorded run can be
/// replayed through `start` (its canned output flowing through the command's real
/// pumps), not only `output_string`. The decode model matches the cassette
/// `Entry`'s (see [`Reply::from_outcome`]).
///
/// The recorded `truncated`/`total_lines`/`total_bytes`/`duration` are threaded
/// onto the handle so a consumed replay (`output_string`) reports the same
/// truncation/overflow/duration the bulk `Entry::to_result` path applies —
/// instead of re-deriving them from the (un-truncated, instantly-fed) canned
/// output — keeping the two replay verbs in agreement.
#[cfg(feature = "record")]
#[allow(clippy::too_many_arguments, reason = "flat recorded-entry parts")]
pub(crate) fn scripted_running_from_parts(
    command: &Command,
    stdout: String,
    stderr: String,
    code: Option<i32>,
    timed_out: bool,
    signal: Option<i32>,
    truncated: bool,
    total_lines: usize,
    total_bytes: usize,
    duration: std::time::Duration,
) -> crate::RunningProcess {
    let recorded = crate::running::ScriptedResultInfo {
        truncated,
        total_lines,
        total_bytes,
        duration,
    };
    Reply::from_outcome(stdout, stderr, code, timed_out, signal)
        .into_running(command, Some(recorded))
}

type Predicate = Box<dyn Fn(&Command) -> bool + Send + Sync>;

enum Rule {
    /// Match when the command's **program name followed by its arguments**
    /// starts with this prefix. The first element is the program; the rest
    /// is an argument prefix. An empty prefix is a catch-all.
    Prefix(Vec<OsString>),
    /// Match when the predicate accepts the command.
    Predicate(Predicate),
}

impl Rule {
    fn matches(&self, command: &Command) -> bool {
        match self {
            // Match the program *and* the argument prefix, so `.on(["git",
            // "status"])` answers for `git status …` but not `rm status`. An empty
            // prefix matches any command. The program name itself compares via
            // `program_names_match` (Windows case/extension normalization); the
            // argument prefix is always an exact, case-sensitive comparison.
            Rule::Prefix(prefix) => match prefix.split_first() {
                Some((program, args)) => {
                    program_names_match(program.as_os_str(), command.program())
                        && command.arguments().starts_with(args)
                }
                None => true,
            },
            Rule::Predicate(pred) => pred(command),
        }
    }
}

/// Compare a rule's registered program name against a command's actual program
/// name — exact on every platform except Windows, where it also tolerates the
/// differences the platform's own executable resolution shrugs off: case
/// (`Git` vs `git`) and a trailing executable extension (`git` vs `git.exe`).
/// Without this, `.on(["git", …])` would silently miss a `Command::new("git.exe")`
/// (or vice versa) even though both name the same tool a live spawn would run.
/// Only the program is compared this way — the argument prefix stays an exact,
/// case-sensitive match.
fn program_names_match(rule: &OsStr, actual: &OsStr) -> bool {
    #[cfg(windows)]
    {
        windows_program_key(rule) == windows_program_key(actual)
    }
    #[cfg(not(windows))]
    {
        rule == actual
    }
}

/// Recognized Windows executable extensions a bare tool name commonly carries
/// (mirrors the common subset of `PATHEXT`) — stripped before comparing, so
/// `git` and `git.exe` normalize to the same key. A hardcoded list rather than
/// reading `PATHEXT` itself: this is a *matching-fidelity* normalization for
/// the fake, not a `PATH`-resolution lookup, and must not vary with the test
/// machine's environment.
#[cfg(windows)]
const WINDOWS_EXECUTABLE_EXTENSIONS: &[&str] = &["exe", "cmd", "bat", "com", "ps1"];

/// The case/extension-normalized key `program_names_match` compares on
/// Windows: lowercased, with a recognized executable extension (see
/// [`WINDOWS_EXECUTABLE_EXTENSIONS`]) stripped. A name whose extension isn't
/// one of those (or has none) is just lowercased whole.
#[cfg(windows)]
fn windows_program_key(name: &OsStr) -> String {
    let lossy = name.to_string_lossy();
    let path = std::path::Path::new(lossy.as_ref());
    match (path.file_stem(), path.extension()) {
        (Some(stem), Some(ext))
            if WINDOWS_EXECUTABLE_EXTENSIONS
                .iter()
                .any(|known| ext.eq_ignore_ascii_case(known)) =>
        {
            stem.to_string_lossy().to_ascii_lowercase()
        }
        _ => lossy.to_ascii_lowercase(),
    }
}

/// Collect a program-and-args prefix (`["git", "status"]`) into owned `OsString`s.
fn collect_prefix<I, S>(prefix: I) -> Vec<OsString>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    prefix
        .into_iter()
        .map(|s| s.as_ref().to_os_string())
        .collect()
}

/// A registered rule and the reply (or ordered sequence of replies) it serves.
struct RuleEntry {
    rule: Rule,
    /// One reply (served on every match) or an ordered sequence (each served
    /// once in turn, then the last repeats) — the cassette replay model.
    /// Never empty.
    replies: Vec<Reply>,
    /// How many times this rule has matched — indexes into `replies` (clamped
    /// to the last). Interior-mutable: matching takes `&self`.
    next: std::sync::atomic::AtomicUsize,
}

/// A [`ProcessRunner`] that returns canned [`Reply`]s for matched commands.
///
/// Rules are tried in registration order; the first match wins. With no match,
/// the [`fallback`](Self::fallback) reply is used, or an error is returned.
///
/// # Example
///
/// Drive a command through scripted replies — hermetic (no real subprocess), so
/// this example actually runs in `cargo test` on every OS:
///
/// ```
/// use processkit::{Command, ProcessRunner};
/// use processkit::testing::{Reply, ScriptedRunner};
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
/// rt.block_on(async {
///     let runner = ScriptedRunner::new()
///         .on(["tool", "--version"], Reply::ok("tool 1.2.3"))
///         .fallback(Reply::fail(1, "unexpected command"));
///
///     let out = runner
///         .output_string(&Command::new("tool").arg("--version"))
///         .await
///         .expect("scripted reply");
///     assert!(out.is_success());
///     assert_eq!(out.stdout().trim(), "tool 1.2.3");
/// });
/// ```
#[derive(Default)]
pub struct ScriptedRunner {
    rules: Vec<RuleEntry>,
    fallback: Option<Reply>,
}

// Manual: `Rule` holds an opaque predicate closure.
impl std::fmt::Debug for ScriptedRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedRunner")
            .field("rules", &self.rules.len())
            .field("has_fallback", &self.fallback.is_some())
            .finish_non_exhaustive()
    }
}

impl ScriptedRunner {
    /// An empty runner (every command misses until rules are added).
    pub fn new() -> Self {
        Self::default()
    }

    /// Reply with `reply` when the command's **program name + arguments** start
    /// with `prefix` (the first element is the program). For example
    /// `.on(["git", "status"])` answers for `git status …`, not `rm status`.
    pub fn on<I, S>(mut self, prefix: I, reply: Reply) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let prefix = collect_prefix(prefix);
        self.push_rule(Rule::Prefix(prefix), vec![reply]);
        self
    }

    /// Reply with each of `replies` in turn — the first match gets the first
    /// reply, the second the second, and so on; once exhausted, the **last**
    /// reply repeats forever. The declarative form for retry scenarios
    /// (fail once, then succeed), matching a cassette's "replay in order, then
    /// repeat the last" model. Matches like [`on`](Self::on) (program + arg
    /// prefix).
    ///
    /// The sequence advances once per matching call via a relaxed atomic counter,
    /// so the "first call gets reply 0, second gets reply 1, …" ordering is
    /// well-defined only for **sequential** calls. Concurrent calls to the same
    /// rule still each get a distinct, in-bounds reply, but which call sees which
    /// reply is unspecified — don't rely on the order across overlapping tasks.
    ///
    /// # Panics
    /// If `replies` is empty.
    pub fn on_sequence<I, S, R>(mut self, prefix: I, replies: R) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
        R: IntoIterator<Item = Reply>,
    {
        let prefix = collect_prefix(prefix);
        let replies: Vec<Reply> = replies.into_iter().collect();
        assert!(
            !replies.is_empty(),
            "ScriptedRunner::on_sequence needs at least one reply"
        );
        self.push_rule(Rule::Prefix(prefix), replies);
        self
    }

    /// Reply with `reply` when `predicate` accepts the command.
    pub fn when<F>(mut self, predicate: F, reply: Reply) -> Self
    where
        F: Fn(&Command) -> bool + Send + Sync + 'static,
    {
        self.push_rule(Rule::Predicate(Box::new(predicate)), vec![reply]);
        self
    }

    /// Reply with `reply` for any command no rule matched.
    pub fn fallback(mut self, reply: Reply) -> Self {
        self.fallback = Some(reply);
        self
    }

    /// Register a rule, warning if it is unreachable because an earlier
    /// prefix rule already matches everything it would.
    fn push_rule(&mut self, rule: Rule, replies: Vec<Reply>) {
        // `matched_reply` indexes `replies[i.min(len - 1)]`, which underflows on
        // an empty vec — codify the non-empty invariant at this one choke point.
        debug_assert!(
            !replies.is_empty(),
            "a ScriptedRunner rule needs at least one reply"
        );
        // A new prefix rule is unreachable if an *earlier* prefix rule is a
        // prefix of it (the broader, first-registered rule wins by first-match).
        // Predicate rules are opaque, so only prefix-vs-prefix shadowing is
        // detected. Diagnostic only; register more-specific rules first to silence.
        #[cfg(feature = "tracing")]
        if let Rule::Prefix(new) = &rule {
            for (i, existing) in self.rules.iter().enumerate() {
                if let Rule::Prefix(earlier) = &existing.rule
                    && new.starts_with(earlier)
                {
                    tracing::warn!(
                        target: "processkit",
                        "ScriptedRunner: rule #{} is unreachable — shadowed by the broader \
                         earlier rule #{}; register more-specific rules first",
                        self.rules.len(),
                        i,
                    );
                    break;
                }
            }
        }
        self.rules.push(RuleEntry {
            rule,
            replies,
            next: std::sync::atomic::AtomicUsize::new(0),
        });
    }

    /// The reply matching `command` (rules in registration order, then the
    /// fallback), or the loud not-found spawn error. A matched rule advances
    /// through its reply sequence.
    fn matched_reply(&self, command: &Command, program: &str) -> Result<&Reply> {
        for entry in &self.rules {
            if entry.rule.matches(command) {
                let i = entry
                    .next
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(&entry.replies[i.min(entry.replies.len() - 1)]);
            }
        }
        self.fallback
            .as_ref()
            .ok_or_else(|| crate::error::Error::Spawn {
                program: program.to_owned(),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "ScriptedRunner: no rule matched and no fallback set",
                ),
            })
    }
}

/// Replay `stdout`/`stderr` text through the command's `on_stdout_line` /
/// `on_stderr_line` handlers (panic-isolated), so a wrapper's
/// progress-reporting path is exercised hermetically on the bulk `output_string` verb
/// — both for a [`ScriptedRunner`] reply and a cassette replay. On a
/// scripted/real `start`, the live pumps invoke the handlers instead.
///
/// Splits the canned text with [`split_pump_lines`](crate::running::split_pump_lines)
/// under the command's per-stream [`LineTerminator`], exactly as the live pump
/// does — so the lines a handler sees on the fake match the ones a real run would
/// deliver (and the ones `into_result` joins back into the result).
pub(crate) fn replay_line_handlers(command: &Command, stdout: &str, stderr: &str) {
    let mut stdout_handler = command.stdout_handler();
    for line in crate::running::split_pump_lines(stdout, command.out_line_terminator()) {
        invoke_isolated(&mut stdout_handler, &line);
    }
    let mut stderr_handler = command.stderr_handler();
    for line in crate::running::split_pump_lines(stderr, command.err_line_terminator()) {
        invoke_isolated(&mut stderr_handler, &line);
    }
}

/// Invoke a line handler with the same panic-isolation contract as the live
/// pump: a panicking handler is caught, disabled for the rest of the run,
/// and replay continues. The scripted bulk path must not diverge from the live
/// path on the very contract the doubles exist to exercise.
fn invoke_isolated(handler: &mut Option<crate::pump::LineHandler>, line: &str) {
    if let Some(h) = handler {
        let invoked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(line)));
        if invoked.is_err() {
            *handler = None;
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "processkit",
                "line handler panicked; disabled for the rest of the run"
            );
        }
    }
}

#[async_trait::async_trait]
impl ProcessRunner for ScriptedRunner {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        let program = command.program().to_string_lossy().into_owned();
        // An already-cancelled token short-circuits with `Cancelled`, exactly as
        // the live runner does pre-spawn — not a canned reply.
        if let Some(token) = command.cancel_token()
            && token.is_cancelled()
        {
            return Err(crate::error::Error::Cancelled { program });
        }
        // Consume a one-shot streaming stdin source exactly like the live launch
        // path: a second run of a command whose stdin was `from_reader`/`from_lines`
        // must fail loud here too, not pass silently on the fake and only blow up
        // live. Checked before `matched_reply` so this never advances an
        // `on_sequence` rule's reply cursor either.
        crate::runner::take_stdin_for_run(command).await?;
        // Honor the non-piped-stdout contract: a capture verb on
        // `stdout(Inherit/Null)` errors rather than handing back output it could
        // never have captured. Checked before `matched_reply` so this config error
        // never advances an `on_sequence` rule's reply cursor.
        if !command.stdout_is_piped() {
            return Err(crate::error::stdout_not_piped_error(&program));
        }
        let reply = self.matched_reply(command, &program)?;
        if let Some(err) = reply.spawn_error_for(program.clone()) {
            return Err(err);
        }
        if reply.pending {
            return park_until_cancelled(command, program).await;
        }
        replay_line_handlers(command, &reply.stdout, &reply.stderr);
        Ok(reply
            .clone()
            .into_result(program, command)
            .with_ok_codes(command.ok_codes_vec()))
    }

    /// Start a scripted live handle: the canned stdout/stderr flow through the
    /// command's **real** pump machinery (handlers, encodings, buffer policy),
    /// so `stdout_lines` / `wait_for_line` / `finish` behave exactly
    /// as on a real child — no subprocess involved.
    async fn start(&self, command: &Command) -> Result<crate::RunningProcess> {
        let program = command.program().to_string_lossy().into_owned();
        // Both `output_string` and `start` short-circuit an already-cancelled
        // token, as the live runner does pre-spawn. Without it, `first_line`
        // (which routes through `start`) would stream canned lines instead of
        // cancelling.
        if let Some(token) = command.cancel_token()
            && token.is_cancelled()
        {
            return Err(crate::error::Error::Cancelled { program });
        }
        // See the matching comment in `output_string`: a one-shot streaming stdin
        // source is consumed here too, so a second scripted `start` of the same
        // command fails loud exactly as a second live spawn would.
        crate::runner::take_stdin_for_run(command).await?;
        let reply = self.matched_reply(command, &program)?;
        if let Some(err) = reply.spawn_error_for(program) {
            return Err(err);
        }
        // A plain scripted reply carries no recorded truncation/duration — the
        // handle derives them from its own (instant) run, as a live child would.
        Ok(reply.clone().into_running(command, None))
    }
}

/// Drive a [`Reply::pending`] match on the bulk `output_string` path: race the
/// command's cancellation token against its configured
/// [`timeout`](crate::Command::timeout) and resolve exactly as the live bulk
/// path would for a genuinely hung child.
///
/// - the token fires first → `Err(Error::Cancelled)`, the mirror of cancelling
///   (and cleaning up) a live long-runner;
/// - the `Command::timeout` deadline fires first → a synthesized timed-out
///   [`ProcessResult`] (`Outcome::TimedOut`, empty output — the same shape a
///   [`Reply::timeout`] yields on this verb), mirroring a live watchdog killing a
///   child that overran its deadline, and matching the scripted `start` path's
///   own deadline arbiter (`arm_scripted_deadline` / `drive_to_exit`);
/// - neither knob is set → the call parks forever, like a hung child that no
///   token can cancel and no deadline bounds.
///
/// Cancellation is biased ahead of the deadline so a simultaneous cancel+timeout
/// resolves as `Cancelled`, matching the live `drive_to_exit_inner` select order.
async fn park_until_cancelled(command: &Command, program: String) -> Result<ProcessResult<String>> {
    let token = command.cancel_token();
    let timeout = command.configured_timeout();
    // Unset knobs become never-resolving arms, so a `select!` over both collapses
    // to "park forever" when neither is configured — the documented hung-child case.
    let cancelled = async {
        match &token {
            Some(token) => token.cancelled().await,
            None => std::future::pending::<()>().await,
        }
    };
    let deadline = async {
        match timeout {
            // Clamp like every other `Instant + Duration` deadline site so a
            // `Duration::MAX`-ish timeout can't overflow the sleep.
            Some(limit) => tokio::time::sleep(limit.min(crate::MAX_DEADLINE)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        biased;
        () = cancelled => Err(crate::error::Error::Cancelled { program }),
        // The same construction the bulk verb applies to a `Reply::timeout()`, so a
        // pending-then-timeout result is byte-for-byte a canned timeout on this verb.
        () = deadline => Ok(Reply::timeout()
            .into_result(program, command)
            .with_ok_codes(command.ok_codes_vec())),
    }
}

/// A captured record of one command a runner was asked to run.
///
/// Captures the *routing* knobs — program, args, cwd, env overrides, whether
/// stdin was supplied — not the I/O-shaping ones (`timeout`, encodings, buffer
/// policy, line handlers, `keep_stdin_open`, retry). Tests that need to assert
/// those inspect the built [`Command`] itself.
///
/// `Debug` is **manual, not derived**: it surfaces the argument *count* and
/// the env variable *names* (sorted), never the argv or env *values* — a
/// `{inv:?}` log line or an `assert_eq!` failure must not leak a secret. The
/// public fields stay available for tests that assert on exact values.
#[derive(Clone, PartialEq, Eq)]
pub struct Invocation {
    /// The program name.
    pub program: OsString,
    /// The arguments, in order.
    pub args: Vec<OsString>,
    /// The working directory, if one was set.
    pub cwd: Option<PathBuf>,
    /// Environment overrides (`None` value = removal), in order — the raw list,
    /// preserving order and duplicates for order/duplicate-sensitive checks. For
    /// the **effective override** value (platform case rules, last write wins) use
    /// the [`env`](Self::env) / [`env_is`](Self::env_is) / [`has_env`](Self::has_env)
    /// accessors rather than scanning this by hand.
    pub envs: Vec<(OsString, Option<OsString>)>,
    /// Whether a (non-empty) stdin source was provided.
    pub has_stdin: bool,
}

// Never render argv or env *values* — only the arg count and the sorted env
// names. Mirrors the `Command`/`CliClient` redaction.
impl std::fmt::Debug for Invocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Invocation")
            .field("program", &self.program)
            .field("args", &self.args.len())
            .field("cwd", &self.cwd)
            .field("env_names", &crate::command::redacted_env_names(&self.envs))
            .field("has_stdin", &self.has_stdin)
            .finish()
    }
}

impl Invocation {
    // pub(crate): the cassette runner captures inputs through this same path, so
    // recordings and `RecordingRunner` assertions can never disagree on what an
    // invocation is.
    pub(crate) fn from_command(command: &Command) -> Self {
        Self {
            program: command.program().to_os_string(),
            args: command.arguments().to_vec(),
            cwd: command.working_dir().map(std::path::Path::to_path_buf),
            envs: command.env_overrides().to_vec(),
            has_stdin: command
                .stdin_source()
                .is_some_and(|stdin| !stdin.is_empty()),
        }
    }

    /// Whether `flag` appears among the arguments.
    pub fn has_flag(&self, flag: impl AsRef<OsStr>) -> bool {
        let flag = flag.as_ref();
        self.args.iter().any(|a| a == flag)
    }

    /// The environment override the invocation set for `name`, if any — the env
    /// analogue of [`has_flag`](Self::has_flag), full-fidelity. The **outer**
    /// `Option` is `None` when the invocation didn't touch `name`; the **inner**
    /// `Option` distinguishes a *set* (`Some(Some(value))`) from a *removal* via
    /// [`Command::env_remove`](crate::Command::env_remove) (`Some(None)`). When a
    /// key is overridden more than once the **last** (effective) override is
    /// returned. Key matching follows the platform's environment rules —
    /// **case-insensitive on Windows** (where env names are), case-sensitive
    /// elsewhere — matching the key a spawn resolves. These accessors reflect the
    /// invocation's explicit **per-variable** overrides (`env`/`env_remove`) only,
    /// not whole-environment scoping (`env_clear`/`inherit_env`, which an
    /// `Invocation` doesn't capture). For the common assertions prefer
    /// [`env_is`](Self::env_is) / [`has_env`](Self::has_env).
    pub fn env(&self, name: impl AsRef<OsStr>) -> Option<Option<&OsStr>> {
        let name = name.as_ref();
        self.envs
            .iter()
            .rev()
            .find(|(key, _)| crate::command::env_key_eq(key.as_os_str(), name))
            .map(|(_, value)| value.as_deref())
    }

    /// Whether the invocation **set** `name` to exactly `value` (its effective
    /// override is a matching `Some`). `false` if `name` was untouched, removed,
    /// or set to a different value. `name` is matched per [`env`](Self::env)'s
    /// platform case rules; the `value` comparison is exact on every platform.
    pub fn env_is(&self, name: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> bool {
        self.env(name) == Some(Some(value.as_ref()))
    }

    /// Whether the invocation **set** `name` to some value — the env presence
    /// check for the "we injected `LC_ALL=C` / `GIT_EDITOR=true`" assertion. A
    /// *removal* ([`Command::env_remove`](crate::Command::env_remove)) is not
    /// "having" the variable, so it returns `false`; query that case via
    /// [`env`](Self::env) returning `Some(None)`.
    pub fn has_env(&self, name: impl AsRef<OsStr>) -> bool {
        matches!(self.env(name), Some(Some(_)))
    }

    /// The arguments as lossy UTF-8 strings, for ergonomic assertions
    /// (e.g. `assert_eq!(call.args_str(), ["pr", "create"])`).
    pub fn args_str(&self) -> Vec<String> {
        self.args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }
}

/// Wraps another [`ProcessRunner`], recording every [`Invocation`] before
/// delegating, so tests can assert exactly what was run.
pub struct RecordingRunner<R: ProcessRunner = ScriptedRunner> {
    inner: R,
    calls: Mutex<Vec<Invocation>>,
}

// Manual: the inner runner type parameter carries no `Debug` bound.
impl<R: ProcessRunner> std::fmt::Debug for RecordingRunner<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let calls = self.calls.lock().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("RecordingRunner")
            .field("calls", &calls)
            .finish_non_exhaustive()
    }
}

impl RecordingRunner<ScriptedRunner> {
    /// A recorder whose inner runner replies with `reply` to everything.
    pub fn replying(reply: Reply) -> Self {
        Self::new(ScriptedRunner::new().fallback(reply))
    }
}

impl<R: ProcessRunner> RecordingRunner<R> {
    /// Wrap `inner`, recording all calls.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// A snapshot of every recorded invocation, in order.
    ///
    /// # Panics
    ///
    /// Panics if the recorder's internal mutex is poisoned — which happens only
    /// if a prior recording panicked while holding it (a crate bug), never from
    /// any caller input.
    pub fn calls(&self) -> Vec<Invocation> {
        self.calls.lock().expect("recorder lock poisoned").clone()
    }

    /// The single recorded invocation; panics unless exactly one was made.
    ///
    /// # Panics
    ///
    /// Panics unless **exactly one** invocation was recorded (zero or several is
    /// a test-assertion failure). Also panics if the recorder's mutex is poisoned
    /// (see [`calls`](Self::calls)).
    pub fn only_call(&self) -> Invocation {
        let calls = self.calls();
        assert_eq!(
            calls.len(),
            1,
            "expected exactly one call, got {}",
            calls.len()
        );
        calls.into_iter().next().expect("length checked above")
    }
}

#[async_trait::async_trait]
impl<R: ProcessRunner> ProcessRunner for RecordingRunner<R> {
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        self.calls
            .lock()
            .expect("recorder lock poisoned")
            .push(Invocation::from_command(command));
        self.inner.output_string(command).await
    }

    async fn start(&self, command: &Command) -> Result<crate::RunningProcess> {
        // Recorded before delegating, so a streamed run is captured even if its
        // stream is never consumed.
        self.calls
            .lock()
            .expect("recorder lock poisoned")
            .push(Invocation::from_command(command));
        self.inner.start(command).await
    }

    async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
        // Don't fall through to the `start`-based default: a runner whose
        // `output_bytes` override behaves differently (e.g. rejects with
        // `Error::Unsupported` instead of lossily re-encoding) must be honored,
        // not silently replayed through `start`.
        self.calls
            .lock()
            .expect("recorder lock poisoned")
            .push(Invocation::from_command(command));
        self.inner.output_bytes(command).await
    }
}

/// The [`DryRunRunner::on_invocation`] callback's boxed shape.
type InvocationCallback = Box<dyn Fn(&str) + Send + Sync>;

/// A [`ProcessRunner`] that never spawns a process: it renders each command
/// via [`Command::command_line`] — reusing the crate's own display quoting,
/// not a hand-rolled shell-escaper — and returns a synthetic successful
/// result for every verb. The seam behind a tool's own `--dry-run`/`--echo`
/// mode: production code keeps calling the same [`ProcessRunner`], just wired
/// to this double instead of [`JobRunner`](crate::JobRunner).
///
/// Unlike [`ScriptedRunner`], there is nothing to script — a dry run has no
/// real output to fake, only a command line to show — so every call
/// unconditionally succeeds: empty stdout/stderr, and an exit code drawn
/// from the command's own [`ok_codes`](Command::ok_codes) (`0` by default)
/// so `is_success()`/the Ext verbs agree it succeeded even for a command
/// whose `ok_codes` excludes `0`. Rendered lines are available two ways,
/// usable together or alone:
///
/// - a collected snapshot, in the style of [`RecordingRunner::calls`] —
///   [`commands`](Self::commands) / [`only_command`](Self::only_command);
/// - a live [`on_invocation`](Self::on_invocation) callback, invoked with the
///   rendered line as each call happens (e.g. to print it immediately).
///
/// # Example
///
/// ```
/// use processkit::{Command, ProcessRunner};
/// use processkit::testing::DryRunRunner;
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
/// rt.block_on(async {
///     let runner = DryRunRunner::new();
///     let out = runner
///         .output_string(&Command::new("rm").args(["-rf", "build"]))
///         .await
///         .unwrap();
///     assert!(out.is_success());
///     assert_eq!(runner.only_command(), "rm -rf build");
/// });
/// ```
#[derive(Default)]
pub struct DryRunRunner {
    commands: Mutex<Vec<String>>,
    on_invocation: Option<InvocationCallback>,
}

// Manual: the callback field carries no `Debug` bound. Like `Invocation`'s
// redaction, a rendered line can carry secrets (a `--token=…` argument), so
// this reports only the call count, not the collected lines.
impl std::fmt::Debug for DryRunRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let commands = self.commands.lock().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("DryRunRunner")
            .field("commands", &commands)
            .field("has_on_invocation", &self.on_invocation.is_some())
            .finish_non_exhaustive()
    }
}

impl DryRunRunner {
    /// A dry-run runner with no callback — every call is only collected,
    /// retrievable via [`commands`](Self::commands).
    pub fn new() -> Self {
        Self::default()
    }

    /// Call `f` with each command's rendered line as it is dry-run
    /// "executed" — e.g. printing it to the terminal for a tool's `--dry-run`
    /// echo — **in addition to**, not instead of, the collected snapshot.
    pub fn on_invocation<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.on_invocation = Some(Box::new(f));
        self
    }

    /// The rendered command line for every call so far, in order — each
    /// produced by [`Command::command_line`], the same display quoting a
    /// caller would reach for by hand.
    ///
    /// # Panics
    ///
    /// Panics if the recorder's internal mutex is poisoned — which happens only
    /// if a prior recording panicked while holding it (a crate bug), never from
    /// any caller input.
    pub fn commands(&self) -> Vec<String> {
        self.commands.lock().expect("dry-run lock poisoned").clone()
    }

    /// The single rendered command line; panics unless exactly one call was made.
    ///
    /// # Panics
    ///
    /// Panics unless **exactly one** call was recorded (zero or several is a
    /// test-assertion failure). Also panics if the recorder's mutex is poisoned
    /// (see [`commands`](Self::commands)).
    pub fn only_command(&self) -> String {
        let commands = self.commands();
        assert_eq!(
            commands.len(),
            1,
            "expected exactly one dry-run call, got {}",
            commands.len()
        );
        commands.into_iter().next().expect("length checked above")
    }

    /// Render and record `command`'s line, invoking the callback if one was set.
    fn record(&self, command: &Command) -> String {
        let line = command.command_line();
        if let Some(f) = &self.on_invocation {
            f(&line);
        }
        self.commands
            .lock()
            .expect("dry-run lock poisoned")
            .push(line.clone());
        line
    }
}

/// The exit code to synthesize for `command`'s dry-run "success": the first
/// code in the command's own [`ok_codes`](Command::ok_codes) (falling back
/// to `0`, `ok_codes_vec`'s own default, if unset). `ok_codes` **replaces**
/// the accepted set rather than extending it and need not include `0` (the
/// crate itself has commands configured with e.g. `.ok_codes([2, 3])`), so
/// hardcoding `Exited(0)` would make `is_success()`/`ensure_success()` (and
/// therefore the `run`/`run_unit`/`checked`/`parse` verbs) disagree with a
/// dry run for exactly such a command — the opposite of "unconditionally
/// succeeds". Synthesizing a code the command's own configuration already
/// accepts keeps every verb honestly successful regardless of `ok_codes`.
fn synthetic_success_code(command: &Command) -> i32 {
    command.ok_codes_vec().first().copied().unwrap_or(0)
}

#[async_trait::async_trait]
impl ProcessRunner for DryRunRunner {
    /// Record `command`'s rendered line and return a synthetic successful
    /// result — no process spawned, no output to fake.
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        self.record(command);
        Ok(ProcessResult::new(
            command.program().to_string_lossy().into_owned(),
            String::new(),
            String::new(),
            Outcome::Exited(synthetic_success_code(command)),
            command.configured_timeout(),
        )
        .with_ok_codes(command.ok_codes_vec()))
    }

    /// Record `command`'s rendered line and return a synthetic successful
    /// live handle whose (empty) output flows through the same pump
    /// machinery a scripted [`start`](ScriptedRunner::start) uses, so
    /// `stdout_lines`/`finish` behave consistently with the rest of the seam.
    async fn start(&self, command: &Command) -> Result<crate::RunningProcess> {
        self.record(command);
        let reply = Reply {
            code: synthetic_success_code(command),
            ..Reply::ok(String::new())
        };
        Ok(reply.into_running(command, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::ProcessRunnerExt;

    #[test]
    fn invocation_env_assertions() {
        use crate::Command;
        let cmd = Command::new("git")
            .env("LC_ALL", "C")
            .env("GIT_EDITOR", "true")
            .env_remove("PAGER")
            .env("LC_ALL", "en_US") // set then set: last wins
            .env("TMPVAR", "x")
            .env_remove("TMPVAR") // set then remove: removed wins
            .env_remove("EDVAR")
            .env("EDVAR", "vi") // remove then set: set wins
            .env("EMPTY", ""); // empty value is a set, distinct from a removal
        let inv = Invocation::from_command(&cmd);

        // env(): full fidelity across set / removed / untouched and interleavings.
        assert_eq!(inv.env("GIT_EDITOR"), Some(Some(OsStr::new("true"))));
        assert_eq!(inv.env("LC_ALL"), Some(Some(OsStr::new("en_US"))));
        assert_eq!(inv.env("PAGER"), Some(None));
        assert_eq!(inv.env("TMPVAR"), Some(None));
        assert_eq!(inv.env("EDVAR"), Some(Some(OsStr::new("vi"))));
        assert_eq!(inv.env("EMPTY"), Some(Some(OsStr::new(""))));
        assert_eq!(inv.env("HOME"), None);

        // env_is(): exact effective set value.
        assert!(inv.env_is("GIT_EDITOR", "true"));
        assert!(inv.env_is("LC_ALL", "en_US"));
        assert!(!inv.env_is("LC_ALL", "C")); // shadowed by the later set
        assert!(inv.env_is("EMPTY", "")); // empty value matches
        assert!(!inv.env_is("PAGER", "")); // removed, not set to ""
        assert!(!inv.env_is("HOME", "x"));

        // has_env(): a set (incl. empty value) is "having"; removal / untouched not.
        assert!(inv.has_env("GIT_EDITOR"));
        assert!(inv.has_env("EMPTY"));
        assert!(inv.has_env("EDVAR"));
        assert!(!inv.has_env("PAGER")); // removed
        assert!(!inv.has_env("TMPVAR")); // set then removed
        assert!(!inv.has_env("HOME")); // untouched
    }

    #[test]
    fn invocation_env_lookup_respects_platform_case_rules() {
        use crate::Command;
        // Two case-differing keys: Windows env names are case-insensitive, so the
        // spawn collapses them (last wins); elsewhere they are distinct keys.
        let cmd = Command::new("git").env("Path", "a").env("PATH", "b");
        let inv = Invocation::from_command(&cmd);
        #[cfg(windows)]
        {
            assert_eq!(inv.env("path"), Some(Some(OsStr::new("b"))));
            assert!(inv.env_is("PATH", "b") && inv.env_is("Path", "b"));
        }
        #[cfg(not(windows))]
        {
            assert_eq!(inv.env("Path"), Some(Some(OsStr::new("a"))));
            assert_eq!(inv.env("PATH"), Some(Some(OsStr::new("b"))));
            assert!(inv.env("path").is_none()); // distinct lowercase key is untouched
        }
    }

    #[test]
    fn invocation_debug_redacts_argv_and_env_values() {
        // A `{inv:?}` log line or an `assert_eq!` failure must not leak argv or
        // env values — only the arg count and env names.
        let inv = Invocation {
            program: "git".into(),
            args: vec!["--token=secret123".into(), "another-secret".into()],
            cwd: None,
            envs: vec![
                ("API_KEY".into(), Some("topsecret-value".into())),
                ("GIT_PAGER".into(), None),
            ],
            has_stdin: false,
        };
        let dbg = format!("{inv:?}");
        assert!(
            !dbg.contains("secret123") && !dbg.contains("another-secret"),
            "argv values must not appear: {dbg}"
        );
        assert!(
            !dbg.contains("topsecret-value"),
            "env values must not appear: {dbg}"
        );
        assert!(
            dbg.contains("API_KEY") && dbg.contains("GIT_PAGER"),
            "env names should appear: {dbg}"
        );
        assert!(dbg.contains("args: 2"), "arg count should appear: {dbg}");
    }

    #[tokio::test]
    async fn first_line_is_reachable_through_the_scripted_seam() {
        // `ProcessRunnerExt::first_line` routes through `start`, so it runs
        // hermetically on a `ScriptedRunner` — no real subprocess.
        use crate::runner::ProcessRunnerExt;
        let runner = ScriptedRunner::new().on(
            ["git", "log"],
            Reply::lines(["alpha", "beta ready", "gamma"]),
        );
        let found = runner
            .first_line(&Command::new("git").arg("log"), |l| l.contains("ready"))
            .await
            .expect("first_line");
        assert_eq!(found.as_deref(), Some("beta ready"));

        let none = runner
            .first_line(&Command::new("git").arg("log"), |l| l.contains("zzz"))
            .await
            .expect("first_line");
        assert_eq!(none, None, "no matching line yields None");
    }

    #[tokio::test]
    async fn first_line_reports_cancellation_not_a_missing_line() {
        // When the command's cancel token has fired, a `first_line` whose
        // predicate never matched must surface `Error::Cancelled` — not `Ok(None)`,
        // which a readiness probe would misread as "the line never appeared".
        use crate::runner::ProcessRunnerExt;
        use tokio_util::sync::CancellationToken;
        let runner =
            ScriptedRunner::new().on(["svc", "run"], Reply::lines(["warming up", "still busy"]));
        let token = CancellationToken::new();
        token.cancel(); // e.g. a shutdown signal arrived
        let cmd = Command::new("svc")
            .arg("run")
            .cancel_on(token.child_token());
        let result = runner.first_line(&cmd, |l| l.contains("ready")).await;
        assert!(
            matches!(result, Err(crate::Error::Cancelled { .. })),
            "a cancelled probe must report Cancelled, got {result:?}"
        );

        // Control: without cancellation, a never-matching predicate is still Ok(None).
        let cmd = Command::new("svc").arg("run");
        let none = runner
            .first_line(&cmd, |l| l.contains("ready"))
            .await
            .expect("first_line");
        assert_eq!(
            none, None,
            "no cancellation → a missing line is still Ok(None)"
        );
    }

    #[tokio::test]
    async fn scripted_start_streams_canned_lines_through_real_pumps() {
        use tokio_stream::StreamExt;
        let runner =
            ScriptedRunner::new().on(["git", "log"], Reply::lines(["first", "second", "third"]));
        let cmd = Command::new("git").arg("log");
        let mut run = runner.start(&cmd).await.expect("scripted start");
        assert_eq!(run.pid(), None, "a scripted child has no OS identity");

        let mut lines = run.stdout_lines().unwrap();
        let mut seen = Vec::new();
        while let Some(line) = lines.next().await {
            seen.push(line);
        }
        assert_eq!(seen, ["first", "second", "third"]);

        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::Exited(0));
        assert_eq!(finish.stderr, "");
    }

    #[tokio::test]
    async fn scripted_carriage_return_mode_streams_each_progress_frame() {
        // End-to-end wiring of the `\r`-aware knob: a scripted child emits
        // carriage-return progress, and `line_terminator(CarriageReturn)` splits it
        // into live frames through the same pump the real path uses.
        use crate::LineTerminator;
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new()
            .fallback(Reply::ok("Progress: 0%\rProgress: 50%\rProgress: 100%\n"));
        let cmd = crate::Command::new("dl").line_terminator(LineTerminator::CarriageReturn);
        let mut run = runner.start(&cmd).await.expect("scripted start");
        let mut lines = run.stdout_lines().unwrap();
        let mut seen = Vec::new();
        while let Some(line) = lines.next().await {
            seen.push(line);
        }
        assert_eq!(
            seen,
            ["Progress: 0%", "Progress: 50%", "Progress: 100%"],
            "each `\\r` frame streams as its own line"
        );
        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::Exited(0));
    }

    #[tokio::test]
    async fn scripted_default_newline_keeps_carriage_return_progress_as_one_line() {
        // Without the knob (default `Newline`), the same progress output is one
        // accumulated line — the pre-existing behavior is unchanged.
        let runner = ScriptedRunner::new()
            .fallback(Reply::ok("Progress: 0%\rProgress: 50%\rProgress: 100%\n"));
        let run = runner
            .start(&crate::Command::new("dl"))
            .await
            .expect("scripted start");
        let result = run.output_string().await.expect("consume");
        assert_eq!(
            result.stdout(),
            "Progress: 0%\rProgress: 50%\rProgress: 100%",
            "default mode keeps the `\\r`s as content in one line"
        );
    }

    #[tokio::test]
    async fn scripted_start_supports_probes_and_failing_finish() {
        let runner = ScriptedRunner::new().fallback(
            Reply::fail(7, "boom: detail\n").with_stdout("starting up\nready to serve\n"),
        );
        let cmd = Command::new("server");
        let mut run = runner.start(&cmd).await.expect("scripted start");
        run.wait_for_line(|l| l.contains("ready"), std::time::Duration::from_secs(5))
            .await
            .expect("the canned banner satisfies the probe");
        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::Exited(7));
        assert_eq!(finish.stderr, "boom: detail");
    }

    #[tokio::test]
    async fn scripted_start_consumed_by_output_string() {
        // The whole consuming surface works on a scripted handle, not just
        // streaming: output_string drains the same pumps.
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b"]));
        let run = runner.start(&Command::new("x")).await.expect("start");
        let result = run.output_string().await.expect("consume");
        assert!(result.is_success());
        assert_eq!(result.stdout(), "a\nb");
    }

    #[tokio::test]
    async fn bulk_output_normalizes_canned_text_like_the_live_path() {
        // D3: the bulk `output_string` verb must join decoded lines the way the
        // live runner does — trailing `\n` dropped, CRLF collapsed to LF — so a
        // reply reads identically on the fake and a real run, and identically
        // across the bulk and `start` verbs on one double.
        let runner = ScriptedRunner::new()
            .on(["a"], Reply::ok("done\n"))
            .on(["b"], Reply::ok("one\r\ntwo\r\n"))
            .fallback(Reply::fail(1, "boom\n"));

        // Trailing newline stripped (matches a live `output_string`, and the
        // scripted `start` path which already normalizes through the pumps).
        let a = runner
            .output_string(&Command::new("a"))
            .await
            .expect("run a");
        assert_eq!(a.stdout(), "done");
        let a_start = runner
            .start(&Command::new("a"))
            .await
            .expect("start a")
            .output_string()
            .await
            .expect("consume a");
        assert_eq!(a.stdout(), a_start.stdout(), "bulk and start agree");

        // CRLF normalized to LF, trailing terminator dropped.
        let b = runner
            .output_string(&Command::new("b"))
            .await
            .expect("run b");
        assert_eq!(b.stdout(), "one\ntwo");

        // stderr is normalized the same way.
        let fail = runner
            .output_string(&Command::new("c"))
            .await
            .expect("run c");
        assert_eq!(fail.stderr(), "boom");
    }

    #[tokio::test]
    async fn bulk_output_honors_the_carriage_return_terminator_like_the_pump() {
        // The bulk normalization follows the command's `line_terminator`, exactly
        // as the live pump (and the scripted `start` path) do: in `\r`-aware mode
        // each carriage-return frame is its own line, so the bulk and `start`
        // verbs agree on one double even under a non-default terminator.
        use crate::LineTerminator;
        let runner = ScriptedRunner::new().fallback(Reply::ok("f1\rf2\rf3\r"));
        let cmd = crate::Command::new("dl").line_terminator(LineTerminator::CarriageReturn);

        let bulk = runner.output_string(&cmd).await.expect("bulk run");
        assert_eq!(bulk.stdout(), "f1\nf2\nf3");

        let start = runner
            .start(&cmd)
            .await
            .expect("scripted start")
            .output_string()
            .await
            .expect("consume");
        assert_eq!(
            bulk.stdout(),
            start.stdout(),
            "bulk and start agree under CR"
        );
    }

    #[tokio::test]
    async fn scripted_start_does_not_re_decode_canned_text_through_stdout_encoding() {
        // D4: the scripted feeder writes the canned String's UTF-8 bytes, so the
        // scripted pump reads them back as UTF-8 — it must NOT re-decode them
        // through the command's `stdout_encoding`. Before the fix,
        // `.stdout_encoding(UTF_16LE)` read the UTF-8 feeder bytes as UTF-16LE and
        // handed back garbage; now the canned text round-trips regardless.
        let runner = ScriptedRunner::new().fallback(Reply::lines(["héllo", "wörld"]));
        let cmd = crate::Command::new("tool").stdout_encoding(encoding_rs::UTF_16LE);

        let start = runner
            .start(&cmd)
            .await
            .expect("scripted start")
            .output_string()
            .await
            .expect("consume");
        assert_eq!(
            start.stdout(),
            "héllo\nwörld",
            "canned text must round-trip regardless of the command's stdout_encoding"
        );

        // The bulk verb (which never decodes) agrees, so the two verbs match.
        let bulk = runner.output_string(&cmd).await.expect("bulk run");
        assert_eq!(bulk.stdout(), start.stdout());
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_line_delay_delivers_incrementally() {
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new().fallback(
            Reply::lines(["tick", "tock"]).with_line_delay(std::time::Duration::from_secs(10)),
        );
        let mut run = runner
            .start(&Command::new("clock"))
            .await
            .expect("scripted start");
        let mut lines = run.stdout_lines().unwrap();

        // Nothing arrives before the first delay elapses…
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), lines.next())
                .await
                .is_err(),
            "no line may arrive before its scripted delay"
        );
        // …then the paused clock advances and both lines flow.
        assert_eq!(lines.next().await.as_deref(), Some("tick"));
        assert_eq!(lines.next().await.as_deref(), Some("tock"));
        assert_eq!(lines.next().await, None);
    }

    /// A scripted `stdout_lines` stream is bounded by the command's
    /// `timeout`, exactly like a real child whose pipe closes when the deadline
    /// kills the tree. The script would pace two lines 10s apart (20s total),
    /// but the 3s timeout fires first: the stream ends having delivered nothing,
    /// and `finish` classifies the run `TimedOut`.
    #[tokio::test(start_paused = true)]
    async fn scripted_stream_is_bounded_by_command_timeout() {
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new().fallback(
            Reply::lines(["tick", "tock"]).with_line_delay(std::time::Duration::from_secs(10)),
        );
        let cmd = Command::new("clock").timeout(std::time::Duration::from_secs(3));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let mut lines = run.stdout_lines().unwrap();
        // The 3s deadline fires before the first line's 10s pace: the stream
        // ends at the deadline instead of running the full 20s of output.
        assert_eq!(
            lines.next().await,
            None,
            "the scripted stream must end at the command's deadline, not run to completion"
        );

        let finish = run.finish().await.expect("finish");
        assert_eq!(
            finish.outcome,
            Outcome::TimedOut,
            "a stream killed by its timeout reports TimedOut, like the bulk verbs"
        );
    }

    /// Output produced *before* the deadline survives — a real child's
    /// already-written pipe bytes are readable after its tree is killed, and the
    /// scripted feeder's already-written bytes are likewise drainable after the
    /// abort. Here lines pace 1s apart under a 2.5s timeout, so two lines arrive
    /// before the deadline ends the stream; the run is still `TimedOut`.
    #[tokio::test(start_paused = true)]
    async fn scripted_stream_delivers_output_produced_before_the_deadline() {
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new().fallback(
            Reply::lines(["one", "two", "three", "four"])
                .with_line_delay(std::time::Duration::from_secs(1)),
        );
        let cmd = Command::new("clock").timeout(std::time::Duration::from_millis(2500));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let mut lines = run.stdout_lines().unwrap();
        let mut seen = Vec::new();
        while let Some(line) = lines.next().await {
            seen.push(line);
        }
        assert_eq!(
            seen,
            ["one", "two"],
            "lines produced before the 2.5s deadline survive; later ones are cut off"
        );

        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::TimedOut);
    }

    /// Arming the scripted deadline must NOT spuriously time out a run that
    /// finishes within its timeout — a short script under a long timeout still
    /// reports its natural exit (the watchdog's `PENDING`→`TIMED_OUT` CAS loses
    /// to the natural reap's `PENDING`→`EXITED`).
    #[tokio::test(start_paused = true)]
    async fn scripted_stream_under_a_generous_timeout_reports_natural_exit() {
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new()
            .fallback(Reply::lines(["a", "b"]).with_line_delay(std::time::Duration::from_secs(1)));
        let cmd = Command::new("quick").timeout(std::time::Duration::from_secs(60));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let mut lines = run.stdout_lines().unwrap();
        let mut seen = Vec::new();
        while let Some(line) = lines.next().await {
            seen.push(line);
        }
        assert_eq!(seen, ["a", "b"], "the whole short script is delivered");

        let finish = run.finish().await.expect("finish");
        assert_eq!(
            finish.outcome,
            Outcome::Exited(0),
            "a run that finishes within its timeout is not spuriously TimedOut"
        );
    }

    /// Parity for the merged `output_events` stream: the same deadline bound
    /// applies, so the event stream ends at the timeout and `finish`
    /// reports `TimedOut`.
    #[tokio::test(start_paused = true)]
    async fn scripted_output_events_is_bounded_by_command_timeout() {
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new().fallback(
            Reply::lines(["tick", "tock"]).with_line_delay(std::time::Duration::from_secs(10)),
        );
        let cmd = Command::new("clock").timeout(std::time::Duration::from_secs(3));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let mut events = run.output_events().unwrap();
        assert!(
            events.next().await.is_none(),
            "the merged event stream must end at the command's deadline"
        );

        let outcome = run.finish().await.expect("finish").outcome;
        assert_eq!(outcome, Outcome::TimedOut);
    }

    /// A never-exiting `pending` reply with a timeout — the stream is empty
    /// (no canned output), but `finish` must still resolve at the
    /// deadline as `TimedOut` rather than hanging on the never-resolving wait.
    /// This is a **liveness** guard: with the scripted deadline armed,
    /// `backend_wait` parks on `signal.notified()` and the watchdog's `fire()`
    /// wakes it; if that wake regressed, the bulk deadline arm in
    /// `drive_to_exit_inner` still backstops the classification (so this asserts
    /// "doesn't hang + reports TimedOut", not which of the two paths resolved it).
    #[tokio::test(start_paused = true)]
    async fn scripted_pending_stream_finishes_at_the_deadline() {
        use tokio_stream::StreamExt;
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let cmd = Command::new("hang").timeout(std::time::Duration::from_secs(3));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let mut lines = run.stdout_lines().unwrap();
        assert_eq!(lines.next().await, None, "a pending reply has no output");

        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::TimedOut);
    }

    /// A readiness probe must NOT arm the `Command::timeout` watchdog, so
    /// it can never kill the tree or flip the outcome to `TimedOut`. With a probe
    /// `within` (10s) LONGER than the command timeout (3s) and a child that never
    /// produces the wanted line, `wait_for_line` waits the full `within` rather
    /// than being cut short at ~3s.
    #[tokio::test(start_paused = true)]
    async fn wait_for_line_does_not_arm_the_command_timeout() {
        // A child whose stdout stays OPEN past the probe (lines paced 30s apart,
        // none matching "ready") — so the probe is bounded by `within`, not by
        // stdout closing.
        let runner = ScriptedRunner::new().fallback(
            Reply::lines(["working", "working"])
                .with_line_delay(std::time::Duration::from_secs(30)),
        );
        let cmd = Command::new("server").timeout(std::time::Duration::from_secs(3));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let start = tokio::time::Instant::now();
        let err = run
            .wait_for_line(|l| l.contains("ready"), std::time::Duration::from_secs(10))
            .await
            .expect_err("the line never arrives, so the probe is NotReady");
        let waited = start.elapsed();

        assert!(matches!(err, crate::Error::NotReady { .. }), "got {err:?}");
        assert!(
            waited >= std::time::Duration::from_secs(9),
            "the probe must wait its full `within` (10s), not be cut short by the \
             3s command timeout: waited {waited:?}"
        );
    }

    /// The command timeout isn't lost, just deferred — a short probe
    /// returns `NotReady` without arming anything, and a subsequent `finish`
    /// still enforces the timeout, reporting `TimedOut` at the command deadline.
    #[tokio::test(start_paused = true)]
    async fn finish_after_wait_for_line_still_times_out_at_the_command_deadline() {
        let runner = ScriptedRunner::new().fallback(
            Reply::lines(["working", "working"])
                .with_line_delay(std::time::Duration::from_secs(30)),
        );
        let cmd = Command::new("hang").timeout(std::time::Duration::from_secs(3));
        let mut run = runner.start(&cmd).await.expect("scripted start");

        let err = run
            .wait_for_line(|_| false, std::time::Duration::from_secs(1))
            .await
            .expect_err("nothing matches within 1s");
        assert!(matches!(err, crate::Error::NotReady { .. }), "got {err:?}");

        let finish = run.finish().await.expect("finish");
        assert_eq!(
            finish.outcome,
            Outcome::TimedOut,
            "the command timeout is still enforced by finish after a probe"
        );
    }

    #[tokio::test]
    async fn scripted_timeout_reply_surfaces_through_start() {
        let runner = ScriptedRunner::new().fallback(Reply::timeout());
        let cmd = Command::new("slow").timeout(std::time::Duration::from_secs(9));
        let run = runner.start(&cmd).await.expect("start");
        let result = run.output_string().await.expect("a timeout is captured");
        assert!(result.timed_out());
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn output_replays_canned_lines_through_handlers() {
        // The bulk path fires `on_stdout_line`/`on_stderr_line` for canned
        // replies, so a wrapper's progress reporting tests hermetically.
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let errs = Arc::new(Mutex::new(Vec::new()));
        let runner =
            ScriptedRunner::new().on(["git", "fetch"], Reply::ok("a\nb\n").with_stdout("a\nb\n"));
        let cmd = Command::new("git")
            .arg("fetch")
            .on_stdout_line({
                let seen = seen.clone();
                move |l| seen.lock().unwrap().push(l.to_owned())
            })
            .on_stderr_line({
                let errs = errs.clone();
                move |l| errs.lock().unwrap().push(l.to_owned())
            });
        let result = runner.output_string(&cmd).await.expect("scripted run");
        assert!(result.is_success());
        assert_eq!(*seen.lock().unwrap(), ["a", "b"]);
        assert!(errs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn output_isolates_a_panicking_line_handler() {
        // A panicking handler on the bulk `output_string` path is caught and
        // disabled, and the run still completes — matching the live pump's
        // panic-isolation contract (the doubles must not diverge on it).
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = ScriptedRunner::new().fallback(Reply::ok("one\ntwo\nthree\n"));
        let cmd = Command::new("x").on_stdout_line({
            let calls = calls.clone();
            move |_| {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    panic!("boom on the second line");
                }
            }
        });
        let result = runner
            .output_string(&cmd)
            .await
            .expect("a handler panic must not fail the scripted run");
        assert!(result.is_success());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "handler disabled after its panic (called for lines 1 and 2 only)"
        );
    }

    #[tokio::test]
    async fn output_on_non_piped_stdout_errors_like_the_live_path() {
        // A capture verb on `stdout(Null)` must error (Io(InvalidInput)), not
        // hand back canned output — matching the live bulk path and the scripted
        // `start` path.
        let runner = ScriptedRunner::new().fallback(Reply::ok("canned"));
        let cmd = Command::new("x").stdout(crate::StdioMode::Null);
        let err = runner
            .output_string(&cmd)
            .await
            .expect_err("a non-piped stdout must error on a capture verb");
        match err {
            crate::error::Error::Io(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput)
            }
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn output_with_an_already_cancelled_token_short_circuits() {
        // A token cancelled before the call returns `Cancelled`, exactly as
        // the live runner does pre-spawn — not a canned reply.
        let token = crate::CancellationToken::new();
        token.cancel();
        let runner = ScriptedRunner::new().fallback(Reply::ok("must not be returned"));
        let cmd = Command::new("x").cancel_on(token);
        let err = runner
            .output_string(&cmd)
            .await
            .expect_err("a pre-cancelled token short-circuits");
        assert!(
            matches!(err, crate::error::Error::Cancelled { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn start_with_an_already_cancelled_token_short_circuits() {
        // `start` (the path `first_line` routes through) must short-circuit
        // a pre-cancelled token too, exactly as `output_string` and the live runner do
        // — not hand back a live scripted handle.
        let token = crate::CancellationToken::new();
        token.cancel();
        let runner = ScriptedRunner::new().fallback(Reply::ok("must not start"));
        let cmd = Command::new("x").cancel_on(token);
        let err = runner
            .start(&cmd)
            .await
            .expect_err("a pre-cancelled token short-circuits start");
        assert!(
            matches!(err, crate::error::Error::Cancelled { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_line_delay_does_not_truncate_a_longer_stderr() {
        // stderr is fed concurrently at the same `line_delay`, so the scripted
        // lifetime must cover the LONGER stream. With only 1 stdout line, a
        // stderr that drains past the (short) stdout-derived lifetime would be
        // cut off — count the max.
        let stderr_text = (1..=10)
            .map(|n| format!("e{n}"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let reply = Reply::fail(3, stderr_text)
            .with_stdout("out\n")
            .with_line_delay(std::time::Duration::from_secs(1));
        let runner = ScriptedRunner::new().fallback(reply);
        let run = runner.start(&Command::new("x")).await.expect("start");
        let result = run.output_string().await.expect("consume");
        assert_eq!(
            result.stderr(),
            "e1\ne2\ne3\ne4\ne5\ne6\ne7\ne8\ne9\ne10",
            "all 10 stderr lines survive despite only 1 stdout line"
        );
    }

    #[tokio::test]
    async fn handler_calls_happen_before_the_consuming_verb_resolves() {
        // Pins the documented ordering guarantee: by the time a consuming
        // verb's future resolves, every line handler invocation has happened
        // (the pumps are joined before the result is assembled).
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(0usize));
        let lines: Vec<String> = (1..=100).map(|n| format!("line {n}")).collect();
        let runner = ScriptedRunner::new().fallback(Reply::lines(lines));
        let cmd = Command::new("x").on_stdout_line({
            let seen = seen.clone();
            move |_| *seen.lock().unwrap() += 1
        });
        let run = runner.start(&cmd).await.expect("scripted start");
        let result = run.output_string().await.expect("consume");
        assert!(result.is_success());
        assert_eq!(
            *seen.lock().unwrap(),
            100,
            "all handler calls happen-before the verb resolves"
        );
    }

    #[tokio::test]
    async fn recording_runner_records_start_invocations() {
        let rec = RecordingRunner::new(ScriptedRunner::new().fallback(Reply::lines(["x"])));
        let run = rec
            .start(&Command::new("gh").args(["run", "watch"]))
            .await
            .expect("recorded start");
        drop(run); // recorded even though the stream was never consumed
        assert_eq!(rec.only_call().args_str(), ["run", "watch"]);
    }

    /// An inner runner whose `output_bytes` honestly rejects the call — like
    /// a `record`-feature cassette, which stores lossy-UTF-8 text and cannot
    /// reproduce exact bytes — instead of falling back to a `start`-based
    /// (lossy) capture.
    struct UnsupportedBytesRunner;

    #[async_trait::async_trait]
    impl ProcessRunner for UnsupportedBytesRunner {
        async fn output_string(&self, _command: &Command) -> Result<ProcessResult<String>> {
            unimplemented!("not exercised by this test")
        }

        async fn output_bytes(&self, _command: &Command) -> Result<ProcessResult<Vec<u8>>> {
            Err(crate::error::Error::Unsupported {
                operation: "output_bytes".into(),
            })
        }
    }

    #[tokio::test]
    async fn recording_runner_forwards_inner_output_bytes_override() {
        // `RecordingRunner` must honor an inner override of `output_bytes`
        // rather than silently falling through to the trait's `start`-based
        // default (which would lossily reconstruct bytes from `output_string`).
        let rec = RecordingRunner::new(UnsupportedBytesRunner);
        let err = rec
            .output_bytes(&Command::new("git").args(["cat-file", "blob", "HEAD"]))
            .await
            .expect_err("inner's Unsupported must be forwarded, not masked");
        assert!(
            matches!(err, crate::error::Error::Unsupported { .. }),
            "got {err:?}"
        );
        assert_eq!(rec.only_call().args_str(), ["cat-file", "blob", "HEAD"]);
    }

    #[tokio::test(start_paused = true)]
    async fn scripted_pending_start_is_cancellable() {
        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let cmd = Command::new("watch").cancel_on(token.clone());
        let run = runner.start(&cmd).await.expect("start");
        let consume = run.output_string();
        tokio::pin!(consume);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3600), &mut consume)
                .await
                .is_err(),
            "a pending scripted run must not resolve before cancellation"
        );
        token.cancel();
        let err = tokio::time::timeout(std::time::Duration::from_secs(3600), consume)
            .await
            .expect("the token resolves the run")
            .expect_err("cancellation is always an error");
        assert!(
            matches!(err, crate::error::Error::Cancelled { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn scripted_kill_after_natural_exit_keeps_the_cached_outcome() {
        // A kill that lands AFTER the scripted child already exited must not
        // overwrite the cached exit with `Signalled` — a real child's status
        // survives a post-exit kill, and the double must match.
        let runner = ScriptedRunner::new().fallback(Reply::fail(5, "boom"));
        let mut run = runner.start(&Command::new("x")).await.expect("start");
        // No line_delay → the scripted child's exit instant is `now` (it has
        // already exited); a kill here is post-mortem.
        run.start_kill().expect("kill is best-effort");
        let outcome = run.wait().await.expect("wait after a post-exit kill");
        assert_eq!(
            outcome,
            Outcome::Exited(5),
            "a post-exit kill keeps the cached exit code, not Signalled"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_any_on_a_pending_scripted_handle_is_cancellable() {
        // A never-exiting (pending) scripted handle raced in `wait_any` must
        // resolve to `Cancelled` when the token fires.
        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let cmd = Command::new("watch").cancel_on(token.clone());
        let mut run = runner.start(&cmd).await.expect("start");
        let mut handles = [&mut run];
        let race = crate::wait_any(&mut handles);
        tokio::pin!(race);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3600), &mut race)
                .await
                .is_err(),
            "a pending scripted run must not resolve before cancellation"
        );
        token.cancel();
        let err = tokio::time::timeout(std::time::Duration::from_secs(3600), race)
            .await
            .expect("the token resolves the race")
            .expect_err("a cancelled run surfaces as an error");
        assert!(
            matches!(err, crate::error::Error::Cancelled { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn prefix_rule_matches_and_replies() {
        let runner = ScriptedRunner::new().on(["git", "status"], Reply::ok("clean"));
        let out = runner
            .output_string(&Command::new("git").arg("status"))
            .await
            .unwrap();
        assert_eq!(out.stdout(), "clean");
        assert!(out.is_success());
    }

    #[tokio::test]
    async fn predicate_rule_and_fallback() {
        let runner = ScriptedRunner::new()
            .when(
                |c| c.arguments().iter().any(|a| a == "--version"),
                Reply::ok("v1"),
            )
            .fallback(Reply::fail(1, "unknown"));

        assert_eq!(
            runner
                .output_string(&Command::new("tool").arg("--version"))
                .await
                .unwrap()
                .stdout(),
            "v1"
        );
        let miss = runner
            .output_string(&Command::new("tool").arg("x"))
            .await
            .unwrap();
        assert_eq!(miss.code(), Some(1));
        assert!(!miss.is_success());
    }

    #[tokio::test]
    async fn no_match_without_fallback_is_a_not_found_spawn_error() {
        let runner = ScriptedRunner::new().on(["git", "status"], Reply::ok("clean"));
        let err = runner
            .output_string(&Command::new("git").arg("log"))
            .await
            .expect_err("an unmatched command with no fallback must error");
        match err {
            crate::error::Error::Spawn { program, source } => {
                assert_eq!(program, "git");
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected Error::Spawn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn prefix_matches_whole_elements_not_substrings() {
        let runner = ScriptedRunner::new().on(["tool", "foo"], Reply::ok("hit"));
        // ["tool", "foo", anything…] matches — element-wise prefix.
        assert!(
            runner
                .output_string(&Command::new("tool").args(["foo", "bar"]))
                .await
                .is_ok()
        );
        // ["foobar"] must NOT: "foo" is a substring, not an args prefix.
        assert!(
            runner
                .output_string(&Command::new("tool").arg("foobar"))
                .await
                .is_err(),
            "substring of an element is not a prefix match"
        );
    }

    #[tokio::test]
    async fn on_matches_the_program_not_just_the_args() {
        // The prefix includes the program, so `.on(["git", "status"])` answers
        // for `git status` but not `rm status`.
        let runner = ScriptedRunner::new()
            .on(["git", "status"], Reply::ok("on branch main"))
            .fallback(Reply::fail(1, "unmatched"));
        let hit = runner
            .output_string(&Command::new("git").arg("status"))
            .await
            .unwrap();
        assert_eq!(hit.stdout(), "on branch main");
        let miss = runner
            .output_string(&Command::new("rm").arg("status"))
            .await
            .unwrap();
        assert_eq!(
            miss.code(),
            Some(1),
            "a different program with the same args must NOT match the rule"
        );
    }

    #[tokio::test]
    async fn on_sequence_yields_each_reply_then_repeats_the_last() {
        // A fail-then-succeed retry scenario, scripted declaratively. Each
        // reply is served once in order, then the last repeats.
        let runner = ScriptedRunner::new().on_sequence(
            ["git", "push"],
            [Reply::fail(1, "rejected"), Reply::ok("pushed")],
        );
        assert_eq!(
            runner
                .output_string(&Command::new("git").arg("push"))
                .await
                .unwrap()
                .code(),
            Some(1),
            "1st call: the first reply (fail)"
        );
        for nth in ["2nd", "3rd"] {
            assert_eq!(
                runner
                    .output_string(&Command::new("git").arg("push"))
                    .await
                    .unwrap()
                    .stdout(),
                "pushed",
                "{nth} call: the last reply repeats"
            );
        }
    }

    #[tokio::test]
    async fn timeout_reply_surfaces_as_timeout_error() {
        use crate::error::Error;
        let runner = ScriptedRunner::new().fallback(Reply::timeout());
        // capture/output exposes the flag without erroring …
        let out = runner.output_string(&Command::new("git")).await.unwrap();
        assert!(out.timed_out());
        // … but the success-checking helpers raise a distinct Timeout.
        assert!(matches!(
            runner.run(&Command::new("git")).await.unwrap_err(),
            Error::Timeout { .. }
        ));
        assert!(matches!(
            runner.exit_code(&Command::new("git")).await.unwrap_err(),
            Error::Timeout { .. }
        ));
        // The reply carries the command's *real* configured deadline, matching the
        // live runner — not a zero duration.
        let cmd = Command::new("git").timeout(std::time::Duration::from_secs(7));
        match runner.run(&cmd).await.unwrap_err() {
            Error::Timeout { timeout, .. } => {
                assert_eq!(timeout, std::time::Duration::from_secs(7))
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn scripted_output_bytes_serves_canned_stdout_through_the_seam() {
        // `output_bytes` is on the `ProcessRunner` seam (default impl via
        // `start`), so a byte-producing tool is testable through a scripted
        // runner exactly like a text one — no real subprocess.
        let runner = ScriptedRunner::new().fallback(Reply::ok("raw\u{0}bytes"));
        let result = runner
            .output_bytes(&Command::new("git").args(["cat-file", "blob", "HEAD"]))
            .await
            .expect("scripted output_bytes");
        assert_eq!(result.stdout(), b"raw\x00bytes");
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn with_stderr_attaches_stderr_to_a_successful_reply() {
        // `with_stderr` composes with the success constructors so a scripted
        // reply can model a CLI (e.g. `git`, a compiler, a linter) that writes
        // warnings to stderr even on exit 0 — previously only expressible via
        // the misleading `Reply::fail(0, "warning")`.
        let runner = ScriptedRunner::new().fallback(Reply::ok("out").with_stderr("warning\n"));
        let result = runner
            .output_string(&Command::new("tool"))
            .await
            .expect("scripted output_string");
        assert!(result.is_success(), "exit code stays 0");
        assert_eq!(result.stdout(), "out");
        assert_eq!(result.stderr(), "warning");
    }

    #[tokio::test]
    async fn signalled_reply_carries_signal_number() {
        use crate::error::Error;
        let runner = ScriptedRunner::new().fallback(Reply::signalled(Some(9)));
        let result = runner.output_string(&Command::new("tool")).await.unwrap();
        assert_eq!(result.outcome(), crate::Outcome::Signalled(Some(9)));
        assert!(matches!(
            runner.run(&Command::new("tool")).await.unwrap_err(),
            Error::Signalled {
                signal: Some(9),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn signalled_reply_without_a_number_is_signalled_none() {
        use crate::error::Error;
        let runner = ScriptedRunner::new().fallback(Reply::signalled(None));
        let result = runner.output_string(&Command::new("tool")).await.unwrap();
        assert_eq!(result.outcome(), crate::Outcome::Signalled(None));
        assert!(matches!(
            runner.run(&Command::new("tool")).await.unwrap_err(),
            Error::Signalled { signal: None, .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn pending_parks_until_the_token_fires_then_cancels() {
        use crate::error::Error;
        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().on(["gh", "run", "watch"], Reply::pending());
        let cmd = Command::new("gh")
            .args(["run", "watch"])
            .cancel_on(token.clone());

        let call = runner.output_string(&cmd);
        tokio::pin!(call);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3600), &mut call)
                .await
                .is_err(),
            "a pending reply must not resolve before cancellation"
        );
        token.cancel();
        match call.await {
            Err(Error::Cancelled { program }) => assert_eq!(program, "gh"),
            other => panic!("expected Error::Cancelled, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pending_without_a_token_or_timeout_parks_forever() {
        // Documented: a pending reply for a command with neither a token nor a
        // timeout behaves like a hung child nobody can cancel and no deadline bounds.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let cmd = Command::new("gh");
        let call = runner.output_string(&cmd);
        tokio::pin!(call);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3600), &mut call)
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_pending_with_a_timeout_and_no_token_times_out_at_the_deadline() {
        // The fidelity fix: on the bulk `output_string` verb a pending reply is
        // bounded by the command's `timeout` just like the live bulk path and the
        // scripted `start` path. With no cancel token but a deadline set, the call
        // must resolve `TimedOut` at the deadline instead of parking forever.
        use crate::error::Error;
        use crate::runner::ProcessRunnerExt;
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let cmd = Command::new("hang").timeout(std::time::Duration::from_secs(3));

        // The capture verb surfaces the timed-out flag without erroring…
        let result = runner
            .output_string(&cmd)
            .await
            .expect("a pending call under a timeout resolves, it does not hang");
        assert_eq!(result.outcome(), Outcome::TimedOut);
        assert!(result.timed_out());

        // …and the checking verbs raise `Error::Timeout` carrying the command's
        // real configured deadline — byte-for-byte a `Reply::timeout` on this verb.
        match runner.run(&cmd).await.unwrap_err() {
            Error::Timeout { timeout, .. } => {
                assert_eq!(timeout, std::time::Duration::from_secs(3))
            }
            other => panic!("expected Error::Timeout, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bulk_pending_cancellation_wins_over_a_later_timeout() {
        // When both a token and a timeout bound a pending bulk call, the one that
        // fires first wins — here the token cancels well before the (long) deadline,
        // so the call reports `Cancelled`, not `TimedOut`.
        use crate::error::Error;
        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let cmd = Command::new("watch")
            .timeout(std::time::Duration::from_secs(3600))
            .cancel_on(token.clone());

        let call = runner.output_string(&cmd);
        tokio::pin!(call);
        // The generous deadline hasn't elapsed, so the call is still parked.
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(60), &mut call)
                .await
                .is_err(),
            "the call must not resolve before either knob fires"
        );
        token.cancel();
        match call.await {
            Err(Error::Cancelled { program }) => assert_eq!(program, "watch"),
            other => panic!("expected Error::Cancelled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn probe_reads_exit_code_as_bool() {
        use crate::error::Error;
        let runner = ScriptedRunner::new()
            .on(["t", "yes"], Reply::ok(""))
            .on(["t", "no"], Reply::fail(1, ""))
            .on(["t", "boom"], Reply::fail(2, "bad"))
            .fallback(Reply::timeout());
        // 0 -> true, 1 -> false.
        assert!(runner.probe(&Command::new("t").arg("yes")).await.unwrap());
        assert!(!runner.probe(&Command::new("t").arg("no")).await.unwrap());
        // Any other code -> Exit error; no code (timeout) -> Timeout error.
        assert!(matches!(
            runner
                .probe(&Command::new("t").arg("boom"))
                .await
                .unwrap_err(),
            Error::Exit { code: 2, .. }
        ));
        assert!(matches!(
            runner
                .probe(&Command::new("t").arg("other"))
                .await
                .unwrap_err(),
            Error::Timeout { .. }
        ));
    }

    #[tokio::test]
    async fn run_ext_trims_and_checks_success() {
        let runner = ScriptedRunner::new().fallback(Reply::ok("  hello \n"));
        let trimmed = runner.run(&Command::new("echo")).await.unwrap();
        assert_eq!(trimmed, "  hello");
    }

    #[tokio::test]
    async fn recording_captures_args_cwd_and_absence() {
        let recorder = RecordingRunner::replying(Reply::ok("ok"));
        let _ = recorder
            .output_string(
                &Command::new("gh")
                    .current_dir("/repo")
                    .args(["pr", "create", "--title", "T"]),
            )
            .await
            .unwrap();

        let call = recorder.only_call();
        assert_eq!(call.program, OsString::from("gh"));
        assert_eq!(call.cwd, Some(PathBuf::from("/repo")));
        assert!(call.has_flag("--title"));
        assert!(!call.has_flag("--base"), "no --base flag was passed");
    }

    #[tokio::test]
    async fn output_string_consumes_a_one_shot_stdin_source_like_a_live_run() {
        // A second run of a command whose stdin is `from_reader`/`from_lines`
        // must fail loud on the fake exactly as it does live (D7) — not pass
        // silently a second time.
        let runner = ScriptedRunner::new().fallback(Reply::ok("done"));
        let stdin = crate::Stdin::from_reader(&b"payload"[..]);
        let cmd = Command::new("tool").stdin(stdin);

        let first = runner.output_string(&cmd).await;
        assert!(first.is_ok(), "the first run consumes the source fine");

        let second = runner
            .output_string(&cmd)
            .await
            .expect_err("a re-run of a consumed one-shot stdin source must fail loud");
        match second {
            crate::error::Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn scripted_start_also_consumes_a_one_shot_stdin_source() {
        // Parity for the streaming verb: `start` must take the one-shot source
        // too, so a second scripted `start` observes it consumed.
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b"]));
        let stdin = crate::Stdin::from_reader(&b"payload"[..]);
        let cmd = Command::new("tool").stdin(stdin);

        let first = runner.start(&cmd).await;
        assert!(
            first.is_ok(),
            "the first scripted start consumes the source"
        );

        let second = runner
            .start(&cmd)
            .await
            .expect_err("a re-run of a consumed one-shot stdin source must fail loud");
        match second {
            crate::error::Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reusable_stdin_sources_are_not_consumed_by_the_fake() {
        // Control: a re-runnable source (`from_bytes`/`from_string`/…) must NOT
        // be affected by the new consumption check — every existing test that
        // reuses a `Command` (or clones one) across calls keeps working.
        let runner = ScriptedRunner::new().fallback(Reply::ok("done"));
        let cmd = Command::new("tool").stdin(crate::Stdin::from_string("hi"));
        for _ in 0..3 {
            let _ = runner
                .output_string(&cmd)
                .await
                .expect("a reusable stdin source never gets 'consumed'");
        }
    }

    #[tokio::test]
    async fn not_found_reply_drives_is_not_found() {
        // `Reply::not_found()` fails as a spawn-side "the program doesn't
        // exist", not a completed run with a failing exit code — the fake's
        // analogue of a live spawn hitting a missing binary (D8).
        use crate::error::Error;
        let runner = ScriptedRunner::new().on(["rg", "--version"], Reply::not_found());
        let err = runner
            .output_string(&Command::new("rg").arg("--version"))
            .await
            .expect_err("a not_found reply must error, not return a canned exit code");
        assert!(err.is_not_found(), "expected is_not_found(), got {err:?}");
        match err {
            Error::NotFound { program, .. } => assert_eq!(program, "rg"),
            other => panic!("expected Error::NotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn not_found_reply_lets_a_tool_not_installed_fallback_branch_be_tested() {
        // The scenario D8 calls out by name: "try the fast tool, fall back to
        // the slow one if it's missing" is untestable on the fake without a way
        // to script `is_not_found()`. `Reply::not_found()` closes that gap.
        async fn pick_grep_tool(runner: &dyn crate::ProcessRunner) -> &'static str {
            match runner
                .output_string(&Command::new("rg").arg("--version"))
                .await
            {
                Ok(_) => "rg",
                Err(e) if e.is_not_found() => "grep",
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        let installed = ScriptedRunner::new().on(["rg", "--version"], Reply::ok("13.0.0"));
        assert_eq!(pick_grep_tool(&installed).await, "rg");

        let missing = ScriptedRunner::new().on(["rg", "--version"], Reply::not_found());
        assert_eq!(pick_grep_tool(&missing).await, "grep");
    }

    #[tokio::test]
    async fn not_found_reply_also_surfaces_through_start() {
        let runner = ScriptedRunner::new().fallback(Reply::not_found());
        let err = runner
            .start(&Command::new("missing-tool"))
            .await
            .expect_err("a not_found reply must error on start too");
        assert!(err.is_not_found(), "expected is_not_found(), got {err:?}");
    }

    #[tokio::test]
    async fn spawn_error_reply_carries_the_io_kind() {
        // A generic OS-level spawn failure (as opposed to `not_found`'s "the
        // program doesn't exist at all") — e.g. permission denied — so
        // classifiers built on `Error::Spawn`'s io kind answer on the fake
        // exactly as they would live.
        use crate::error::Error;
        let runner = ScriptedRunner::new().fallback(Reply::spawn_error(
            std::io::ErrorKind::PermissionDenied,
            "eacces",
        ));
        let err = runner
            .output_string(&Command::new("locked-tool"))
            .await
            .expect_err("a spawn_error reply must error");
        assert!(
            err.is_permission_denied(),
            "expected is_permission_denied(), got {err:?}"
        );
        assert!(
            !err.is_not_found(),
            "a generic spawn error is not is_not_found()"
        );
        match err {
            Error::Spawn { program, source } => {
                assert_eq!(program, "locked-tool");
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
                assert_eq!(source.to_string(), "eacces");
            }
            other => panic!("expected Error::Spawn, got {other:?}"),
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn prefix_rule_program_match_is_case_and_extension_insensitive_on_windows() {
        // `git` and `git.exe` (and any casing) name the same tool a live spawn
        // would resolve identically — the fake must agree (D9/D13's Windows gap).
        let runner = ScriptedRunner::new().on(["git", "status"], Reply::ok("clean"));

        for program in ["git.exe", "GIT", "Git.EXE"] {
            let out = runner
                .output_string(&Command::new(program).arg("status"))
                .await
                .unwrap_or_else(|e| panic!("`{program}` should match the `git` rule: {e}"));
            assert_eq!(out.stdout(), "clean");
        }

        // The reverse direction: a rule registered with the `.exe` suffix still
        // answers for the bare name.
        let runner = ScriptedRunner::new().on(["git.exe", "status"], Reply::ok("clean"));
        let out = runner
            .output_string(&Command::new("git").arg("status"))
            .await
            .expect("a bare `git` should match a `git.exe`-registered rule");
        assert_eq!(out.stdout(), "clean");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn prefix_rule_still_requires_the_same_underlying_program_on_windows() {
        // The normalization must not turn the matcher into a free-for-all: a
        // genuinely different program name still misses.
        let runner = ScriptedRunner::new()
            .on(["git", "status"], Reply::ok("clean"))
            .fallback(Reply::fail(1, "unmatched"));
        let miss = runner
            .output_string(&Command::new("gitk.exe").arg("status"))
            .await
            .unwrap();
        assert_eq!(
            miss.code(),
            Some(1),
            "`gitk` is a different tool than `git`"
        );
    }

    #[tokio::test]
    async fn dry_run_renders_via_command_line_quoting() {
        // The rendered line reuses `Command::command_line`'s own display
        // quoting, not a hand-rolled escaper — same expectation as
        // `command_line_quotes_args_for_display` in command.rs.
        let runner = DryRunRunner::new();
        let cmd = Command::new("git").args(["commit", "-m", "hello world"]);
        let out = runner.output_string(&cmd).await.expect("dry run");
        assert!(out.is_success());
        assert_eq!(out.stdout(), "");
        assert_eq!(out.stderr(), "");
        assert_eq!(out.outcome(), Outcome::Exited(0));

        #[cfg(unix)]
        assert_eq!(runner.only_command(), "git commit -m 'hello world'");
        #[cfg(not(unix))]
        assert_eq!(runner.only_command(), "git commit -m \"hello world\"");
    }

    #[tokio::test]
    async fn dry_run_never_spawns_and_always_synthesizes_success() {
        // No rule to miss, no fallback to configure — every command
        // unconditionally succeeds, unlike `ScriptedRunner`.
        let runner = DryRunRunner::new();
        for program in ["rm", "curl", "anything-at-all"] {
            let out = runner
                .output_string(&Command::new(program).arg("--flag"))
                .await
                .unwrap_or_else(|e| panic!("dry run of `{program}` must never fail: {e}"));
            assert!(out.is_success());
        }
        assert_eq!(runner.commands().len(), 3);
    }

    #[tokio::test]
    async fn dry_run_collects_commands_in_order() {
        let runner = DryRunRunner::new();
        assert!(
            runner
                .output_string(&Command::new("a").arg("1"))
                .await
                .unwrap()
                .is_success()
        );
        assert!(
            runner
                .output_string(&Command::new("b").arg("2"))
                .await
                .unwrap()
                .is_success()
        );
        assert_eq!(runner.commands(), ["a 1", "b 2"]);
    }

    #[tokio::test]
    async fn dry_run_start_returns_a_scripted_handle_through_the_real_pumps() {
        // `start` gets the same synthetic-success treatment, flowing through
        // the ordinary scripted pump machinery (no OS identity, clean exit).
        let runner = DryRunRunner::new();
        let run = runner
            .start(&Command::new("deploy").args(["--env", "prod"]))
            .await
            .expect("dry-run start");
        assert_eq!(run.pid(), None);
        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::Exited(0));
        assert_eq!(finish.stderr, "");
        assert_eq!(runner.only_command(), "deploy --env prod");
    }

    #[tokio::test]
    async fn dry_run_succeeds_even_when_ok_codes_excludes_zero() {
        // `ok_codes` REPLACES the accepted set and needn't include `0` — a
        // command configured with `.ok_codes([2, 3])` must still dry-run as
        // an unconditional success on every verb, not just `Exited(0)` (which
        // would fail `is_success()`/`ensure_success()` for this command).
        let command = Command::new("fsck").ok_codes([2, 3]);

        let runner = DryRunRunner::new();
        let out = runner.output_string(&command).await.unwrap();
        assert!(
            out.is_success(),
            "output_string must synthesize an accepted code"
        );
        assert_eq!(out.outcome(), Outcome::Exited(2));

        let run = runner.start(&command).await.expect("dry-run start");
        let finish = run.finish().await.expect("finish");
        assert_eq!(finish.outcome, Outcome::Exited(2));

        // The Ext verb `run()` goes through `checked` → `ensure_success`; it
        // must succeed rather than surface `Error::Exit` for this command.
        use crate::runner::ProcessRunnerExt;
        runner
            .run(&command)
            .await
            .expect("run() must succeed on a dry run regardless of ok_codes");
    }

    #[tokio::test]
    async fn dry_run_invokes_the_callback_with_the_rendered_line() {
        use std::sync::{Arc, Mutex as StdMutex};
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let seen_in_callback = Arc::clone(&seen);
        let runner = DryRunRunner::new().on_invocation(move |line| {
            seen_in_callback.lock().unwrap().push(line.to_owned());
        });

        let out = runner
            .output_string(&Command::new("echo").arg("hi"))
            .await
            .unwrap();
        assert!(out.is_success());

        assert_eq!(*seen.lock().unwrap(), ["echo hi"]);
        // The callback augments, rather than replaces, the collected snapshot.
        assert_eq!(runner.commands(), ["echo hi"]);
    }

    #[tokio::test]
    async fn dry_run_only_command_panics_unless_exactly_one_call() {
        let runner = DryRunRunner::new();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runner.only_command()));
        assert!(result.is_err(), "zero calls must panic, not default");
    }
}
