//! The scripted backend: canned "child" doubles that feed the same pump
//! machinery a real [`RealProc`](super::RealProc) does, so hermetic test doubles
//! exercise the exact code paths a live run does. Only [`crate::doubles`] and
//! `crate::cassette` construct these — they reach in via the narrow surface
//! re-exported from [`super`] ([`ScriptedProc`], [`ScriptedResultInfo`],
//! [`split_pump_lines`], and [`RunningProcess::from_scripted`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

// The shared timeout arbiter is built from the crate's `cfg(loom)`-swappable sync
// layer so its type matches `RunningProcess::timeout_state` under `--cfg loom`
// (`std::sync::atomic::AtomicU8` otherwise). See `crate::sync`.
use crate::sync::atomic::AtomicU8;

use tokio::sync::Notify;
use tokio::task::AbortHandle;

use crate::buffer::LineTerminator;
use crate::group::ProcessGroup;
use crate::result::Outcome;
use crate::sys::pid_gate::PidGate;

use super::{Backend, OutputReader, RunningProcess, TS_PENDING};

/// Recorded result signals a cassette `start`-replay threads onto its scripted
/// handle, so a consumed replay (`output_string`) reports the *recorded*
/// truncation/overflow/duration — matching the bulk `Entry::to_result` path —
/// instead of re-deriving them from the re-pumped canned output. `None` on a
/// plain [`ScriptedRunner`](crate::testing::ScriptedRunner) reply keeps the
/// derived values.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScriptedResultInfo {
    pub(crate) truncated: bool,
    pub(crate) total_lines: usize,
    pub(crate) total_bytes: usize,
    pub(crate) duration: Duration,
}

/// Split already-decoded `text` into the exact lines the streaming pump
/// ([`pump_lines_core`](crate::pump)) would yield for `terminator` — CRLF/CR
/// terminators collapsed and stripped, with no trailing empty line for a final
/// terminator. Joining the result with `\n` reproduces the live bulk path's
/// `stdout_lines.join("\n")` byte-for-byte, so the test doubles' canned text
/// reads identically on the fake and a real run (and across the bulk and `start`
/// verbs). Models no buffer cap — the bulk replay never truncates.
///
/// Mirrors `pump_lines_core`'s terminator handling on whole in-hand text, so a
/// `\r` in `\r`-aware mode is trailing only at the very end. Kept beside the
/// scripted handle it serves rather than reaching into the pump's private,
/// chunk-oriented line splitter.
pub(crate) fn split_pump_lines(text: &str, terminator: LineTerminator) -> Vec<String> {
    match terminator {
        // `str::lines` splits on `\n`, dropping one preceding `\r` (a CRLF
        // terminator) and yielding no trailing empty line — exactly the pump's
        // `Newline` mode. A lone `\r` stays content, as it does there.
        LineTerminator::Newline => text.lines().map(str::to_owned).collect(),
        // `\r`-aware: split on the earliest `\r` or `\n`; a `\r\n` pair is one
        // terminator; a bare `\r` ends a frame; the final un-terminated segment is
        // the last line. (Indices land on ASCII `\r`/`\n`, always char boundaries.)
        LineTerminator::CarriageReturn => {
            let mut lines = Vec::new();
            let bytes = text.as_bytes();
            let (mut start, mut i) = (0usize, 0usize);
            while i < bytes.len() {
                match bytes[i] {
                    b'\n' => {
                        lines.push(text[start..i].to_owned());
                        i += 1;
                    }
                    b'\r' => {
                        lines.push(text[start..i].to_owned());
                        // A `\r\n` pair is a single terminator; drop both.
                        i += if bytes.get(i + 1) == Some(&b'\n') {
                            2
                        } else {
                            1
                        };
                    }
                    _ => {
                        i += 1;
                        continue;
                    }
                }
                start = i;
            }
            if start < bytes.len() {
                lines.push(text[start..].to_owned());
            }
            lines
        }
    }
}

/// Shared kill state for a scripted child, clonable so a detached watchdog can
/// end the run. `fire` hangs up feeders, flags the child dead, and wakes a
/// parked `ScriptedProc::wait_outcome`.
#[derive(Clone)]
pub(super) struct ScriptedKill {
    killed: Arc<AtomicBool>,
    /// `notify_one` stores a permit so a kill before the wait parks is not missed.
    signal: Arc<Notify>,
    /// Aborting a writer drops its end, EOF-ing the reader — as a real tree's
    /// death closes its pipes. `abort` is idempotent.
    feeders: Arc<Vec<AbortHandle>>,
}

impl ScriptedKill {
    fn fire(&self) {
        self.killed.store(true, Ordering::Release);
        for feeder in self.feeders.iter() {
            feeder.abort();
        }
        self.signal.notify_one();
    }
}

/// A scripted child's **natural** (non-kill) exit signal, clonable so a detached
/// dialog feeder can end the run cleanly once it has written its response —
/// distinct from [`ScriptedKill`] (`Signalled`): firing this resolves
/// [`wait_outcome`](ScriptedProc::wait_outcome) to the canned exit code. Never
/// fired for a plain time/`pending`-based reply, so those keep their existing
/// `exit_at`/kill-only lifecycle.
#[derive(Clone)]
struct ScriptedExit {
    /// Latched once the child has exited on its own — read by the non-blocking
    /// [`has_exited_now`](ScriptedProc::has_exited_now).
    flag: Arc<AtomicBool>,
    /// Wakes a parked [`wait_outcome`](ScriptedProc::wait_outcome); `notify_one`
    /// stores a permit so an exit fired before the wait parks is not missed.
    signal: Arc<Notify>,
}

impl ScriptedExit {
    fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            signal: Arc::new(Notify::new()),
        }
    }

    fn fire(&self) {
        self.flag.store(true, Ordering::Release);
        self.signal.notify_one();
    }
}

/// A scripted "child": canned output readers (fed by detached writer tasks so
/// per-line delays work under a paused clock) plus a canned exit.
pub(crate) struct ScriptedProc {
    stdout: Option<tokio::io::DuplexStream>,
    stderr: Option<tokio::io::DuplexStream>,
    /// The write end of a hermetic dialog's stdin duplex — handed to
    /// [`take_stdin`](RunningProcess::take_stdin) so a
    /// [`Reply::dialog`](crate::testing::Reply::dialog) test can answer the
    /// prompt. `None` for a non-dialog reply (which models no interactive stdin),
    /// matching the pre-existing "scripted handles don't model stdin" contract.
    stdin: Option<tokio::io::DuplexStream>,
    kill: ScriptedKill,
    /// Natural (non-kill) exit, fired by a dialog feeder once it has written its
    /// response — see [`ScriptedExit`]. Inert (never fired) for a plain reply.
    exit: ScriptedExit,
    code: Option<i32>,
    timed_out: bool,
    signal: Option<i32>,
    /// When the scripted child "exits": `Some(at)` resolves at that instant
    /// (now = immediately), `None` never exits on its own (`Reply::pending` and a
    /// [`dialog`](Self::new_dialog) both use `None` — cancel/timeout, or the
    /// dialog's own [`exit`](Self::exit) signal, end it).
    exit_at: Option<tokio::time::Instant>,
    /// Whether this double models a [`use_pty`](crate::Command::use_pty) run, set
    /// by [`from_scripted`](RunningProcess::from_scripted). Gates the hermetic
    /// [`RunningProcess::resize_pty`] model so a non-PTY scripted handle refuses a
    /// resize exactly as a real non-PTY one does.
    #[cfg(feature = "pty")]
    pty: bool,
    /// Every `(cols, rows)` accepted by [`RunningProcess::resize_pty`], in call order —
    /// the hermetic model of a live PTY resize (no real pty). Lets a test observe
    /// that a resize was delivered, and in what geometry.
    #[cfg(feature = "pty")]
    resizes: Vec<(u16, u16)>,
}

impl ScriptedProc {
    /// Assemble a scripted child. Each output's text is fed through a duplex
    /// pipe by a detached writer task — with `line_delay`, the writer sleeps
    /// before each line (virtual-time friendly under a paused clock). The
    /// "process" exits after `lifetime` (`None` = never on its own).
    pub(crate) fn new(
        stdout_text: String,
        stderr_text: String,
        code: Option<i32>,
        timed_out: bool,
        signal: Option<i32>,
        lifetime: Option<Duration>,
        line_delay: Option<Duration>,
    ) -> Self {
        let mut feeders = Vec::new();
        let mut feed = |text: String| {
            let (mut tx, rx) = tokio::io::duplex(64 * 1024);
            if text.is_empty() {
                return rx; // dropped tx → immediate EOF
            }
            // Detached: `AbortHandle` is the only way to hang it up early.
            let task = tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                match line_delay {
                    None => {
                        let _ = tx.write_all(text.as_bytes()).await;
                    }
                    Some(delay) => {
                        for line in text.split_inclusive('\n') {
                            tokio::time::sleep(delay).await;
                            if tx.write_all(line.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                // tx drops → EOF
            });
            feeders.push(task.abort_handle());
            rx
        };
        let stdout = feed(stdout_text);
        let stderr = feed(stderr_text);
        Self {
            stdout: Some(stdout),
            stderr: Some(stderr),
            stdin: None,
            kill: ScriptedKill {
                killed: Arc::new(AtomicBool::new(false)),
                signal: Arc::new(Notify::new()),
                feeders: Arc::new(feeders),
            },
            exit: ScriptedExit::new(),
            code,
            timed_out,
            signal,
            exit_at: lifetime.map(|d| tokio::time::Instant::now() + d),
            #[cfg(feature = "pty")]
            pty: false,
            #[cfg(feature = "pty")]
            resizes: Vec::new(),
        }
    }

    /// Assemble a hermetic **interactive dialog** double: write `prompt` to
    /// stdout immediately (typically *without* a trailing newline, so it surfaces
    /// as the un-terminated tail
    /// [`wait_for_output`](RunningProcess::wait_for_output) matches), then block
    /// until the caller writes to stdin (via [`take_stdin`](RunningProcess::take_stdin)),
    /// then write `response` and exit `code`. Models one prompt/answer turn with
    /// no real pipe or pty — the reactive counterpart of the time-driven
    /// [`new`](Self::new) feeder. stderr is empty (PTY mode merges it into the one
    /// master anyway, and a dialog's content rides stdout).
    ///
    /// The `response` is written the moment stdin is received (not on a timer), so
    /// the exchange is deterministic without a paused clock. Aborting the run
    /// (kill/drop) hangs up the feeder like any other scripted child.
    pub(crate) fn new_dialog(prompt: String, response: String, code: i32) -> Self {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        // stdout: the feeder writes the prompt, waits for stdin, writes the
        // response, then drops its end (EOF). stderr: an immediately-dropped tx
        // (empty). stdin: the caller writes the write end; the feeder reads the
        // read end.
        let (mut stdout_tx, stdout_rx) = tokio::io::duplex(64 * 1024);
        let (_stderr_tx, stderr_rx) = tokio::io::duplex(64 * 1024); // dropped → EOF
        let (stdin_write, mut stdin_read) = tokio::io::duplex(64 * 1024);

        let exit = ScriptedExit::new();
        let exit_for_feeder = exit.clone();
        let task = tokio::spawn(async move {
            // 1. Emit the prompt as an un-terminated tail and flush so the pump
            //    (and a `wait_for_output` observer) sees it before we block.
            if stdout_tx.write_all(prompt.as_bytes()).await.is_err() {
                return;
            }
            let _ = stdout_tx.flush().await;
            // 2. Wait for one complete answer line. `ProcessStdin::write_line`
            //    writes the text and Enter separately, so treating the first
            //    fragment as the whole answer can close the duplex between those
            //    writes and race the caller into `BrokenPipe`. EOF after non-empty
            //    input also completes the answer, like a line reader on a pipe.
            let mut answer = Vec::new();
            if let Ok(n) = BufReader::new(&mut stdin_read)
                .read_until(b'\n', &mut answer)
                .await
                && n > 0
            {
                // 3. Emit the continuation (also flushed).
                let _ = stdout_tx.write_all(response.as_bytes()).await;
                let _ = stdout_tx.flush().await;
            }
            // 4. Drop stdout_tx → EOF, then signal a clean natural exit.
            drop(stdout_tx);
            exit_for_feeder.fire();
        });
        let feeders = vec![task.abort_handle()];

        Self {
            stdout: Some(stdout_rx),
            stderr: Some(stderr_rx),
            stdin: Some(stdin_write),
            kill: ScriptedKill {
                killed: Arc::new(AtomicBool::new(false)),
                signal: Arc::new(Notify::new()),
                feeders: Arc::new(feeders),
            },
            exit,
            code: Some(code),
            timed_out: false,
            signal: None,
            // Event-driven exit (via `exit`), so no time-based `exit_at`.
            exit_at: None,
            #[cfg(feature = "pty")]
            pty: false,
            #[cfg(feature = "pty")]
            resizes: Vec::new(),
        }
    }

    /// Take the write end of a dialog double's stdin (see
    /// [`new_dialog`](Self::new_dialog)), for
    /// [`RunningProcess::take_stdin`](crate::RunningProcess::take_stdin)'s scripted
    /// branch. `None` after the first call, or for a non-dialog reply that models
    /// no interactive stdin.
    pub(super) fn take_stdin_writer(&mut self) -> Option<tokio::io::DuplexStream> {
        self.stdin.take()
    }

    pub(super) fn kill(&self) {
        self.kill.fire();
    }

    /// Mark whether this double models a [`use_pty`](crate::Command::use_pty) run
    /// — set once at [`from_scripted`](RunningProcess::from_scripted) time from the
    /// command's PTY flag.
    #[cfg(feature = "pty")]
    pub(super) fn set_pty_mode(&mut self, pty: bool) {
        self.pty = pty;
    }

    /// Whether this double models a PTY run (backs
    /// [`RunningProcess::resize_pty`](crate::RunningProcess)'s scripted branch).
    #[cfg(feature = "pty")]
    pub(super) fn models_pty(&self) -> bool {
        self.pty
    }

    /// Hermetically model a live PTY resize: record the requested `(cols, rows)`.
    /// The caller has already verified this double is a PTY and still "running",
    /// so this only bookkeeps — there is no real pseudo-terminal to resize.
    #[cfg(feature = "pty")]
    pub(super) fn record_resize(&mut self, cols: u16, rows: u16) {
        self.resizes.push((cols, rows));
    }

    /// The resizes recorded by [`record_resize`](Self::record_resize), in order —
    /// a test-only window into the hermetic resize model.
    #[cfg(all(test, feature = "pty"))]
    pub(super) fn recorded_resizes(&self) -> &[(u16, u16)] {
        &self.resizes
    }

    /// The scripted counterpart of `Backend::own_group` — a scripted double has
    /// no OS process group.
    pub(super) fn own_group(&self) -> Option<&Arc<ProcessGroup>> {
        None
    }

    /// A clonable handle onto this script's kill state, for
    /// `RunningProcess::arm_scripted_deadline`'s detached watchdog.
    pub(super) fn kill_handle(&self) -> ScriptedKill {
        self.kill.clone()
    }

    pub(super) fn take_stdout_reader(&mut self) -> Option<OutputReader> {
        self.stdout.take().map(|p| Box::new(p) as OutputReader)
    }

    pub(super) fn take_stderr_reader(&mut self) -> Option<OutputReader> {
        self.stderr.take().map(|p| Box::new(p) as OutputReader)
    }

    /// Raw exit wait — the scripted counterpart of `RunningProcess::backend_wait`'s
    /// real-child branch. Resolves at the canned `exit_at`, or immediately as
    /// `Signalled` if killed.
    ///
    /// A kill AFTER the scripted child already exited naturally must still
    /// report the cached natural outcome — a real child's exit status survives a
    /// post-exit kill. Only an un-exited (or never-exiting `pending`) script that
    /// is killed is `Signalled`. A [`dialog`](Self::new_dialog) exits cleanly via
    /// its own [`exit`](Self::exit) signal, which resolves to the canned code just
    /// like a time-based `exit_at` would.
    pub(super) async fn wait_outcome(&mut self) -> Outcome {
        fn classify(code: Option<i32>, timed_out: bool, signal: Option<i32>) -> Outcome {
            match (code, timed_out) {
                (_, true) => Outcome::TimedOut,
                (Some(code), false) => Outcome::Exited(code),
                (None, false) => Outcome::Signalled(signal),
            }
        }

        let already_exited = matches!(self.exit_at, Some(at) if at <= tokio::time::Instant::now())
            || self.exit.flag.load(Ordering::Acquire);
        if self.kill.killed.load(Ordering::Acquire) && !already_exited {
            Outcome::Signalled(None)
        } else if already_exited {
            // Cached natural outcome (time- or dialog-driven) even if a kill
            // landed afterwards.
            classify(self.code, self.timed_out, self.signal)
        } else {
            match self.exit_at {
                // Race so a streaming `deadline_task` can still end this wait.
                Some(at) => {
                    tokio::select! {
                        biased;
                        () = self.kill.signal.notified() => Outcome::Signalled(None),
                        () = self.exit.signal.notified() => classify(self.code, self.timed_out, self.signal),
                        () = tokio::time::sleep_until(at) => classify(self.code, self.timed_out, self.signal),
                    }
                }
                // A `pending` reply parks on kill only; a `dialog` also resolves
                // once its feeder fires the natural-exit signal.
                None => {
                    tokio::select! {
                        biased;
                        () = self.kill.signal.notified() => Outcome::Signalled(None),
                        () = self.exit.signal.notified() => classify(self.code, self.timed_out, self.signal),
                    }
                }
            }
        }
    }

    /// The scripted counterpart of `RunningProcess::has_exited_now` — a
    /// non-blocking poll of whether the script has already ended (killed,
    /// dialog-exited, or past its time-based `exit_at`).
    pub(super) fn has_exited_now(&self) -> bool {
        self.kill.killed.load(Ordering::Acquire)
            || self.exit.flag.load(Ordering::Acquire)
            || self
                .exit_at
                .is_some_and(|at| tokio::time::Instant::now() >= at)
    }
}

impl RunningProcess {
    /// Build a scripted handle: same handlers/buffer/timeout/token/line-terminators
    /// as a real run so hermetic tests exercise the same pump machinery. `pid()`
    /// is `None`.
    ///
    /// The decode encoding is forced to **UTF-8**, not the command's
    /// `stdout_encoding`/`stderr_encoding`: the scripted feeder writes the canned
    /// `String`'s UTF-8 bytes (the caller's already-*decoded* output), so the pump
    /// must read them back as UTF-8. Re-decoding through the command's encoding
    /// would double-decode — e.g. `.stdout_encoding(UTF_16LE)` would read UTF-8
    /// feeder bytes as UTF-16LE and hand back garbage.
    ///
    /// `recorded` (from a cassette `start`-replay) overrides the
    /// truncation/overflow/duration a consumed replay reports; `None` derives them.
    pub(crate) fn from_scripted(
        command: &crate::command::Command,
        scripted: ScriptedProc,
        recorded: Option<ScriptedResultInfo>,
    ) -> Self {
        // Carry the command's PTY intent onto the double so a scripted
        // `use_pty` handle answers `resize_pty` (and a non-PTY one refuses it)
        // exactly as a real handle would — hermetically, with no real pty.
        #[cfg(feature = "pty")]
        let scripted = {
            let mut scripted = scripted;
            scripted.set_pty_mode(command.wants_pty());
            scripted
        };
        Self {
            program: command.program_name(),
            backend: Backend::Scripted(Box::new(scripted)),
            timeout: command.configured_timeout(),
            timeout_grace: command.configured_timeout_grace(),
            timeout_signal: command.timeout_signal_raw(),
            pid: None,
            // A scripted double owns no OS process, so there is no start identity to
            // capture — metrics are never sampled against it.
            #[cfg(feature = "stats")]
            proc_identity: None,
            // Feeder writes UTF-8 (the canned String's bytes); force UTF-8 decode
            // so the caller sees the exact canned text regardless of the command's
            // configured encoding (see the doc above), while keeping the command's
            // handler/tee/terminator.
            stdout_config: command.stdout_config().with_encoding(encoding_rs::UTF_8),
            stderr_config: command.stderr_config().with_encoding(encoding_rs::UTF_8),
            buffer: command.output_buffer_policy(),
            ok_codes: command.ok_codes_vec(),
            stdout_sink: None,
            stderr_sink: None,
            stdout_pump: None,
            stderr_pump: None,
            stdin_error: None,
            stdout_piped: command.stdout_is_piped(),
            // PTY mode has one merged output reader exposed as stdout, just like
            // the real PTY backend; never advertise a separate stderr probe.
            stderr_piped: command.stderr_is_piped() && !command.wants_pty(),
            deadline_task: None,
            timeout_state: Arc::new(AtomicU8::new(TS_PENDING)),
            // A scripted double owns no OS process, so its gate is pid-less: every
            // raw kill through it is a no-op regardless of retirement.
            pid_gate: Arc::new(PidGate::new(None)),
            cancel_token: command.cancel_token(),
            cancel_task: None,
            cancel_at_exit: None,
            started: Instant::now(),
            // Same spawn instant as `started`, on tokio's clock: the deadline
            // anchor a scripted stream's watchdog (`arm_scripted_deadline`) and a
            // consuming `drive_to_exit_inner` measure the timeout budget from —
            // the crux of a hermetic paused-clock run, where `started` (real
            // clock) barely moves while virtual time advances.
            deadline_anchor: tokio::time::Instant::now(),
            start_time: SystemTime::now(),
            scripted_result: recorded,
            exit_event_tx: None,
            merged_events_stream: false,
        }
    }

    /// Arm a deadline watchdog for a scripted streamed run. A scripted handle has
    /// no process group; this task hangs up the feeders at the deadline instead,
    /// claiming `TIMED_OUT` via the arbiter. No-op for a real backend or when a
    /// watchdog is already armed.
    pub(super) fn arm_scripted_deadline(&mut self) {
        if self.deadline_task.is_some() {
            return;
        }
        let (Some(limit), Some(kill)) = (self.timeout, self.backend.scripted_kill()) else {
            return;
        };
        // Anchor to spawn time so a late stream call can't re-grant the full
        // limit — on `deadline_anchor` (tokio's clock), the crux under a paused
        // runtime: virtual time already burned (e.g. by a `wait_for_line` probe)
        // is charged against the limit, matching a live child's real clock.
        let started = self.deadline_anchor;
        let timeout_state = self.timeout_state.clone();
        self.deadline_task = Some(tokio::spawn(async move {
            if !super::deadline::wait_deadline_and_claim(started, limit, &timeout_state).await {
                return; // the script already exited on its own — no kill
            }
            kill.fire();
        }));
    }
}
