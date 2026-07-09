//! Incremental stdout streaming: [`StdoutLines`], [`OutputEvents`], the
//! watchdog tasks that bound a streamed run (deadline/cancel), and the unified
//! `finish`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use tokio_stream::Stream;

use crate::error::Result;
use crate::group::ProcessGroup;
use crate::pump::{Popped, SharedLines, pump_lines_core};
use crate::result::Outcome;
use crate::sys::pid_gate::{PidGate, force_kill};

use super::RunningProcess;

/// The outcome of a run driven via
/// [`stdout_lines`](RunningProcess::stdout_lines) or
/// [`output_events`](RunningProcess::output_events): how the run ended plus
/// the captured standard error. Returned by
/// [`RunningProcess::finish`].
///
/// `#[non_exhaustive]`: a future release may attach more about the finished run
/// (e.g. a duration, mirroring [`ProcessResult`](crate::ProcessResult))
/// without a breaking change — read the public fields rather than destructuring
/// it exhaustively.
///
/// **Why this is not a [`ProcessResult`](crate::ProcessResult):** `Finished`
/// is the tail of a run whose **stdout the caller already consumed** line by line,
/// so it deliberately carries *no* `stdout` field — there is nothing left to hand
/// back. `ProcessResult` is the *captured* shape (the bulk verbs buffer stdout and
/// return it as `T`). The two are intentionally distinct: a streaming finisher
/// that re-bundled stdout would either double-buffer what the consumer already saw
/// or hand back an empty string pretending to be the output. The overlapping
/// fields (`outcome`, `stderr`) are kept name-compatible so the mental model
/// carries over.
///
/// `#[must_use]`: like [`ProcessResult`](crate::ProcessResult), its
/// [`outcome`](Self::outcome) is the only signal of *how* the run ended —
/// dropping a `Finished` unread discards whether the process succeeded, timed
/// out, or was signal-killed.
#[must_use = "a Finished carries the run's outcome; inspect `outcome` or it is silently discarded"]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Finished {
    /// How the run ended.
    pub outcome: Outcome,
    /// Standard error captured in the background while stdout was streaming.
    /// `String::new()` when no stderr was produced or stderr was not piped.
    pub stderr: String,
    /// Whether a bounded, non-fail-loud [`OutputBufferPolicy`](crate::OutputBufferPolicy)
    /// silently dropped lines from the background stderr drain (`false` when
    /// stderr was not piped, or nothing was dropped). Mirrors
    /// [`ProcessResult::truncated`](crate::ProcessResult::truncated), scoped to
    /// stderr only — `stdout` here was already handed to the caller line by
    /// line and is that caller's own responsibility to track.
    pub stderr_truncated: bool,
}

impl RunningProcess {
    /// Stream the child's standard output line by line. Call this **once**.
    ///
    /// Standard error is drained in the background and discarded — use
    /// [`output_string`](Self::output_string) when you need both. The command's
    /// [`timeout`](crate::Command::timeout) bounds the stream: at the deadline the
    /// process tree is killed, pipes close, and a following
    /// [`finish`](Self::finish) reports [`Outcome::TimedOut`](crate::Outcome::TimedOut)
    /// even if the child caught the signal and exited cleanly within the grace.
    ///
    /// Returns `Err` instead of a silently-empty stream when stdout was not piped
    /// or a streaming verb was already called on this handle.
    ///
    /// # Errors
    ///
    /// [`Error::Io`](crate::Error::Io) when stdout was not piped, or a prior
    /// streaming verb ([`stdout_lines`](Self::stdout_lines) /
    /// [`output_events`](Self::output_events) / `wait_for_line`) already consumed
    /// it — returned instead of a stream that would silently be empty.
    pub fn stdout_lines(&mut self) -> Result<StdoutLines> {
        // Drain stdout AND arm the timeout watchdog. `wait_for_line` instead calls
        // `drain_stdout_lines` directly, so a readiness probe never kills the tree.
        let lines = self.drain_stdout_lines()?;
        self.arm_stream_deadline();
        Ok(lines)
    }

    /// Set up the stdout and background stderr drains without arming the timeout
    /// watchdog. Shared by [`stdout_lines`](Self::stdout_lines) (which arms it)
    /// and `wait_for_line` (which must not, so a failed probe leaves the child
    /// running).
    pub(super) fn drain_stdout_lines(&mut self) -> Result<StdoutLines> {
        self.ensure_stdout_streamable()?;
        // Background-drain stderr; keep the handle so `finish` can await the last
        // line. Set up once — a second call must not overwrite the first sink/pump,
        // or `finish` would return empty stderr.
        if self.stderr_sink.is_none() {
            let stderr_sink = SharedLines::new(&self.buffer);
            if let Some(pipe) = self.backend.take_stderr_reader() {
                self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_config.clone(),
                    stderr_sink.clone(),
                )));
            }
            self.stderr_sink = Some(stderr_sink);
        }

        let stdout_sink = SharedLines::new(&self.buffer);
        match self.backend.take_stdout_reader() {
            Some(pipe) => {
                self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stdout_config.clone(),
                    stdout_sink.clone(),
                )));
            }
            // `ensure_stdout_streamable` already rejected non-piped/consumed stdout;
            // close the sink rather than hang if an internal caller reaches here.
            None => stdout_sink.close_now(),
        }
        // Store only on the first call — overwriting would discard the first pump's
        // overflow flag and count.
        if self.stdout_sink.is_none() {
            self.stdout_sink = Some(stdout_sink.clone());
        }

        Ok(StdoutLines {
            sink: stdout_sink,
            wait: None,
        })
    }

    /// Arm the deadline watchdog for a live stream. At the timeout, claim it via
    /// the shared arbiter and tear the tree down so pipes close and the stream
    /// ends. Armed once; not called by `wait_for_line` so a readiness probe never
    /// kills the child.
    fn arm_stream_deadline(&mut self) {
        // `Weak` so a hard-kill timer never delays kill-on-close when the handle
        // drops early. The graceful branch upgrades across `graceful_terminate`,
        // deferring group-Drop until teardown finishes — benign.
        if self.deadline_task.is_none()
            && let Some(limit) = self.timeout
        {
            // Own-group handles tear down the whole tree; a shared-group handle
            // (`own_group` is `None`) doesn't own the tree, so it reaches only its
            // direct child by pid — the same scope as the cancel watchdog. A Real
            // backend always has a pid; a scripted stream carries neither and is
            // bounded by `arm_scripted_deadline` below instead. Without arming the
            // shared-group case, `Command::timeout` was silently *unenforced*
            // while streaming on a shared group: a quiet, never-exiting child left
            // the stream pending forever (A1).
            let own_group = self.backend.own_group().map(Arc::downgrade);
            let pid = self.pid;
            if own_group.is_some() || pid.is_some() {
                let grace = self.timeout_grace;
                let signal = self.timeout_signal;
                // Anchor to spawn time so a late stream call can't re-grant the full limit.
                let started = self.started;
                // Claim the timeout via the shared arbiter so the finisher
                // classifies `TimedOut` even if the child exits cleanly within the
                // grace. But winning that CAS is NOT proof the pid is still
                // un-reaped: a natural reap (`backend_wait`) frees the pid inside
                // `wait()` and only claims `TS_EXITED` a few frames later, so this
                // task could win `TS_TIMED_OUT` in that gap on an already-freed pid.
                // The `PidGate` is the real gate: a consuming finisher `retire`s it
                // *before* it frees the pid (and reclaims this watchdog), and every
                // raw kill below runs inside the gate lock — so a kill either lands
                // before that retire (pid still valid) or is skipped, never on a
                // freed pid. During pure streaming (no finisher, so nothing retires)
                // the gate stays live and this task is the sole killer, as it must
                // be to bound the timeout.
                let timeout_state = self.timeout_state.clone();
                let gate = self.pid_gate.clone();
                self.deadline_task = Some(tokio::spawn(async move {
                    if !super::deadline::wait_deadline_and_claim(started, limit, &timeout_state)
                        .await
                    {
                        return; // child already exited — no kill
                    }
                    // A `Child`-owning finisher has taken over teardown/reap: it
                    // kills through the owned `Child` (`start_kill`, a no-op post
                    // reap) and retired the gate, so stand down. This early load
                    // only skips the group kill; the raw direct-child kills below
                    // re-check retirement *atomically with the kill* under the gate
                    // lock, so none can race a reap onto a recycled pid.
                    if gate.is_retired() {
                        return;
                    }
                    // This watchdog can't reap the child, so a child that exits on
                    // the signal still waits the full grace before a no-op SIGKILL.
                    match own_group {
                        Some(group) => match grace {
                            Some(grace) => match group.upgrade() {
                                Some(group) => {
                                    let _ = group.graceful_terminate(grace, signal).await;
                                }
                                None => force_kill(&gate), // group already gone
                            },
                            None => kill_via_weak(&group, &gate),
                        },
                        // Shared group: pid-only teardown (grandchildren of a
                        // forking child are the shared-group teardown gap — pair a
                        // per-stage timeout with a whole-chain one).
                        None => match grace {
                            // Detach the graceful kill-and-reap so its final
                            // SIGKILL survives THIS watchdog being aborted by
                            // `RunningProcess::Drop` mid-grace — the case where a
                            // child catches the signal, closes stdout (ending the
                            // stream), and keeps running. See
                            // `spawn_graceful_kill_and_reap`. Its gated ops re-read
                            // retirement each poll, so a finisher that reclaims
                            // mid-grace still stands it down.
                            Some(grace) => spawn_graceful_kill_and_reap(gate, grace, signal),
                            None => force_kill(&gate),
                        },
                    }
                }));
            }
        }

        self.arm_scripted_deadline();
    }

    /// Finish a streamed run: wait for exit and return a [`Finished`]
    /// carrying the [`Outcome`] and the stderr collected in the background by
    /// [`stdout_lines`](Self::stdout_lines). If a bounded
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) silently dropped stderr
    /// lines, [`Finished::stderr_truncated`] is `true` — the streamed stdout
    /// itself is the caller's own responsibility and is not tracked here.
    ///
    /// A run killed by its [`timeout`](crate::Command::timeout) reports
    /// [`Outcome::TimedOut`](crate::Outcome::TimedOut), even if the child caught
    /// the signal and exited cleanly within the grace — matching the bulk verbs.
    ///
    /// Designed to pair with `stdout_lines` (consume the stdout stream first),
    /// but safe to call on its own — any pipe the stream didn't take is drained
    /// here so the child can never block on a full pipe.
    ///
    /// # Errors
    ///
    /// A timeout or signal-kill is *captured* in the returned [`Finished`]'s
    /// [`outcome`](Finished::outcome), not raised. The `Err` cases are
    /// [`Error::Cancelled`](crate::Error::Cancelled) (the run was cancelled via
    /// [`Command::cancel_on`](crate::Command::cancel_on)),
    /// [`Error::OutputTooLarge`](crate::Error::OutputTooLarge) (a fail-loud
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) overflowed on a sink the
    /// caller opted into by streaming — a bare `finish` discards untouched stdout
    /// and never trips it), [`Error::Stdin`](crate::Error::Stdin) (a
    /// non-broken-pipe stdin-source failure on an otherwise-successful run), or
    /// [`Error::Io`](crate::Error::Io) (waiting on the child failed).
    pub async fn finish(mut self) -> Result<Finished> {
        // A bare `finish()` (no prior `stdout_lines`/`output_events` took stdout)
        // still has to drain the leftover stdout so the child can't block on a full
        // pipe — but the caller never asked to capture it. Pump it through the
        // internal retain-nothing discard sink (not the user's `OutputBufferPolicy`):
        // lines nobody will read aren't retained, and a `fail_loud` policy doesn't
        // raise `OutputTooLarge` for output the caller didn't request — matching
        // `wait()`. When stdout was already streamed, the reader is gone and the
        // stream's user-policy sink stays in place (and is overflow-checked below).
        let mut stdout_discarded = false;
        if let Some(pipe) = self.backend.take_stdout_reader() {
            let sink = SharedLines::new(&super::discard_sink_policy());
            self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stdout_config.clone(),
                sink.clone(),
            )));
            self.stdout_sink = Some(sink);
            stdout_discarded = true;
        }
        if self.stderr_pump.is_none()
            && let Some(pipe) = self.backend.take_stderr_reader()
        {
            let sink = SharedLines::new(&self.buffer);
            self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_config.clone(),
                sink.clone(),
            )));
            self.stderr_sink = Some(sink);
        }

        let raw_outcome = self.drive_to_exit().await?;
        self.observe_stdin_task().await;
        let pumps: Vec<_> = [self.stdout_pump.take(), self.stderr_pump.take()]
            .into_iter()
            .flatten()
            .collect();
        super::join_pumps(pumps).await;
        // Re-observe stdin after the pumps drained: a writer that failed inside
        // the teardown window is only visible now (see `finalize_stdin_task`).
        self.finalize_stdin_task().await;
        let outcome = self.checked_outcome(raw_outcome)?;
        // Skip the stdout overflow check when stdout went to the discard sink: the
        // caller didn't ask to capture it, so an over-cap flood is not their error
        // (the discard sink's DropOldest mode never trips it anyway). A stdout sink
        // populated by a prior *stream* is still checked — the caller opted into
        // that capture.
        let stdout_check = (!stdout_discarded)
            .then_some(self.stdout_sink.as_ref())
            .flatten();
        for sink in [stdout_check, self.stderr_sink.as_ref()]
            .into_iter()
            .flatten()
        {
            if sink.overflowed() {
                return Err(crate::Error::OutputTooLarge {
                    program: self.program.clone(),
                    max_lines: self.buffer.max_lines,
                    max_bytes: self.buffer.max_bytes,
                    total_lines: sink.count(),
                    total_bytes: sink.seen_bytes(),
                });
            }
        }
        // `dropped()` = lines the buffer policy discarded from the background
        // stderr drain, not lines this call itself failed to observe — matches
        // the same signal `output_string`/`output_bytes` derive `truncated` from.
        let stderr_truncated = self.stderr_sink.as_ref().is_some_and(|s| s.dropped() > 0);
        let stderr = self
            .stderr_sink
            .as_ref()
            .map(|sink| sink.drain().join("\n"))
            .unwrap_or_default();
        Ok(Finished {
            outcome,
            stderr,
            stderr_truncated,
        })
    }

    /// Stream both stdout and stderr as a single ordered sequence of
    /// [`OutputEvent`] items. Call this **once**.
    ///
    /// Interleaving is best-effort; the two streams are polled **fairly** so a
    /// continuously-ready stream can't starve the other. Returns `Err` instead of
    /// a silently-empty stream when stdout was not piped or was already consumed.
    /// Call [`finish`](Self::finish) after draining — its `stderr` will be empty
    /// since stderr was delivered as events.
    ///
    /// # Errors
    ///
    /// [`Error::Io`](crate::Error::Io) when stdout was not piped, or a prior
    /// streaming verb already consumed it — returned instead of a stream that
    /// would silently be empty.
    pub fn output_events(&mut self) -> Result<OutputEvents> {
        self.ensure_stdout_streamable()?;
        let stdout_sink = SharedLines::new(&self.buffer);
        match self.backend.take_stdout_reader() {
            Some(pipe) => {
                self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stdout_config.clone(),
                    stdout_sink.clone(),
                )));
            }
            None => stdout_sink.close_now(),
        }
        // Store only on the first call — overwriting would discard the first pump's
        // overflow flag and count.
        if self.stdout_sink.is_none() {
            self.stdout_sink = Some(stdout_sink.clone());
        }

        // Set up stderr on the first call only. A repeat call gets its own closed
        // sink — a shared sink's notify_one on close wakes only one waiter.
        let stderr_sink = if self.stderr_sink.is_none() {
            let sink = SharedLines::new(&self.buffer);
            if let Some(pipe) = self.backend.take_stderr_reader() {
                self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_config.clone(),
                    sink.clone(),
                )));
            } else {
                sink.close_now();
            }
            self.stderr_sink = Some(sink.clone());
            sink
        } else {
            let closed = SharedLines::new(&self.buffer);
            closed.close_now();
            closed
        };

        self.arm_stream_deadline();

        Ok(OutputEvents {
            stdout_sink,
            stderr_sink,
            stdout_wait: None,
            stderr_wait: None,
            stdout_done: false,
            stderr_done: false,
            prefer_stdout: true,
        })
    }
}

/// Tear down the group if still alive, then best-effort kill the direct child
/// through the gate (a no-op if the owner already retired the pid).
pub(super) fn kill_via_weak(group: &Weak<ProcessGroup>, gate: &PidGate) {
    if let Some(group) = group.upgrade() {
        // On Linux + legacy/restricted cgroup this can synchronously block
        // this worker thread up to ~100ms — accepted, not routed through
        // `spawn_blocking`; see the sweep loop in `Cgroup::kill`
        // (src/sys/linux.rs) for the full rationale.
        let _ = group.kill_all();
    }
    force_kill(gate);
}

/// Gracefully terminate (and let the runtime reap) a single child by pid — the
/// shared-group timeout path, which owns no group and so reaches only its own
/// direct child. Sends `signal`, polls liveness up to `grace`, then `SIGKILL`s a
/// survivor; a child that exits on the signal ends the poll early and skips the
/// hard kill. Windows has no signal tier — hard kill.
///
/// The signal → poll → kill algorithm lives in the shared unix driver
/// [`crate::sys::graceful::run_pid`], which documents the load-bearing
/// guarantee: the final `SIGKILL` still fires for a child that caught the
/// signal, closed stdout, and kept running. To make that guarantee hold when a
/// streaming consumer drops its handle mid-grace, arm this via
/// [`spawn_graceful_kill_and_reap`] (a detached task) rather than inline in the
/// abortable deadline watchdog.
///
/// The [`PidGate`] retires the pid the instant its owner reaps it: once retired,
/// the driver's liveness poll reports "gone" and the final `SIGKILL` is
/// suppressed, and every raw op runs under the gate lock — so this pid-only kill
/// can never signal a pid the OS recycled after the reap. The streaming watchdog
/// passes the handle's shared gate (a finisher retires it when it reclaims
/// teardown).
pub(crate) async fn graceful_kill_pid(gate: Arc<PidGate>, grace: std::time::Duration, signal: i32) {
    #[cfg(unix)]
    {
        let target = crate::sys::graceful::UnixChild::new(gate);
        crate::sys::graceful::run_pid(&target, signal, grace).await;
    }
    #[cfg(not(unix))]
    {
        // Windows has no graceful tier: force-kill immediately under the gate.
        let _ = (grace, signal);
        force_kill(&gate);
    }
}

/// Run [`graceful_kill_pid`] as a **detached** task, dropping its `JoinHandle`
/// so no abort site tracks it.
///
/// The shared-group streaming deadline watchdog lives in `self.deadline_task`,
/// which `RunningProcess::Drop` aborts. When the timed-out child catches the
/// graceful signal, closes its stdout, and keeps running, the stream ends (EOF)
/// and the streaming consumer (e.g. `first_line`) drops its handle — aborting
/// that watchdog. Running the kill-and-reap **inline** in the watchdog would
/// then cancel the final `SIGKILL` mid-grace, stranding the survivor until the
/// shared group itself is dropped. Detaching it here severs that abort: the task
/// runs to completion independently, delivering the guaranteed `SIGKILL`. The
/// shared-group child carries no `kill_on_drop`, so this kill never races a
/// Drop-triggered kill+reap of a recycled pid; the reap is the runtime's — the
/// dropped `tokio::process::Child` is collected by tokio's orphan reaper once
/// the kill lands.
///
/// The shared [`PidGate`] rides along as the [`graceful_kill_pid`] stand-down: if
/// a consuming finisher reclaims teardown mid-grace (it retires the gate and
/// reaps through the owned `Child`), this detached task's liveness poll stops and
/// its `SIGKILL` is suppressed, so it can't fire on the recycled pid.
fn spawn_graceful_kill_and_reap(gate: Arc<PidGate>, grace: std::time::Duration, signal: i32) {
    // Detached on purpose: dropping the handle lets it outlive the (abortable)
    // deadline watchdog that spawned it.
    drop(tokio::spawn(graceful_kill_pid(gate, grace, signal)));
}

/// Send a raw graceful signal (e.g. `SIGTERM`) to a still-live direct child by
/// pid — the shared-group graceful-timeout teardown's first step. Unix only; the
/// caller must know the pid is un-reaped (the owner sends it before its own reap,
/// with the gate already retired to stand the detached watchdogs down), so this
/// deliberately does not route through the gate. A no-op for a pid-less handle.
#[cfg(unix)]
pub(super) fn signal_direct_child(pid: Option<u32>, signal: i32) {
    let Some(pid) = pid else { return };
    // SAFETY: sending a signal to a live pid is sound; ESRCH (already gone) is
    // ignored — the bounded reap that follows observes the exit regardless.
    unsafe {
        libc::kill(pid as i32, signal);
    }
}

/// A `Stream` of the child's standard-output lines (see
/// [`RunningProcess::stdout_lines`]).
pub struct StdoutLines {
    sink: Arc<SharedLines>,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl std::fmt::Debug for StdoutLines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdoutLines").finish_non_exhaustive()
    }
}

impl Stream for StdoutLines {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<String>> {
        let this = self.get_mut();
        loop {
            match this.sink.try_pop() {
                Popped::Line(line) => {
                    this.wait = None;
                    return Poll::Ready(Some(line));
                }
                Popped::Closed => return Poll::Ready(None),
                Popped::Empty => {
                    if this.wait.is_none() {
                        this.wait = Some(Box::pin(this.sink.clone().changed()));
                    }
                    match this.wait.as_mut().expect("just set").as_mut().poll(cx) {
                        Poll::Ready(()) => {
                            this.wait = None;
                            continue;
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

/// An event produced by a child process: a decoded line from stdout or stderr.
///
/// Yielded by [`RunningProcess::output_events`], which merges both streams into
/// a single, ordered sequence.
///
/// Non-exhaustive: a future release may add a third kind of event (e.g. a
/// lifecycle marker) without a breaking change, so a `match` on `OutputEvent`
/// needs a `_` arm. Each per-stream line is an [`OutputLine`] (rather than a bare
/// `String`) so per-line metadata can be attached there without a breaking change
/// either.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutputEvent {
    /// A line from the child's standard output.
    Stdout(OutputLine),
    /// A line from the child's standard error.
    Stderr(OutputLine),
}

impl OutputEvent {
    /// The decoded text of this event's line, if it carries one. `Some` for
    /// [`Stdout`](OutputEvent::Stdout)/[`Stderr`](OutputEvent::Stderr); `None` for
    /// a future non-line event kind. Convenience for consumers that don't care
    /// which stream a line came from.
    pub fn text(&self) -> Option<&str> {
        match self {
            OutputEvent::Stdout(line) | OutputEvent::Stderr(line) => Some(line.text()),
        }
    }
}

/// One decoded line carried by an [`OutputEvent`].
///
/// `#[non_exhaustive]`: a future release may attach per-line metadata (e.g. a
/// timestamp or a monotonic line index) without a breaking change. The line text
/// is reached through [`text`](Self::text) / [`into_text`](Self::into_text)
/// (accessor-fronted, like [`ProcessResult`](crate::ProcessResult)), so the
/// payload representation can evolve without breaking callers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutputLine {
    /// The decoded line, with its trailing `\n` (and one CRLF `\r`) stripped —
    /// the same line shape [`StdoutLines`] yields.
    text: String,
}

impl OutputLine {
    /// The decoded line text (trailing `\n` / CRLF `\r` already stripped).
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Consume the line and take ownership of its decoded text.
    #[must_use]
    pub fn into_text(self) -> String {
        self.text
    }
}

/// A merged `Stream` of both stdout and stderr lines (see
/// [`RunningProcess::output_events`]).
///
/// Lines are interleaved in the order they arrive at the async runtime.
pub struct OutputEvents {
    stdout_sink: Arc<SharedLines>,
    stderr_sink: Arc<SharedLines>,
    stdout_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    stderr_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    stdout_done: bool,
    stderr_done: bool,
    /// Which stream gets the first look each poll. Flipped after every emitted
    /// line so a continuously-ready stream can't starve the other.
    prefer_stdout: bool,
}

impl std::fmt::Debug for OutputEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputEvents").finish_non_exhaustive()
    }
}

impl Stream for OutputEvents {
    type Item = OutputEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<OutputEvent>> {
        let this = self.get_mut();
        loop {
            for stdout_turn in [this.prefer_stdout, !this.prefer_stdout] {
                if stdout_turn && !this.stdout_done {
                    match this.stdout_sink.try_pop() {
                        Popped::Line(line) => {
                            this.stdout_wait = None;
                            this.prefer_stdout = false; // stderr gets the next first look
                            return Poll::Ready(Some(OutputEvent::Stdout(OutputLine {
                                text: line,
                            })));
                        }
                        Popped::Closed => {
                            this.stdout_done = true;
                            this.stdout_wait = None;
                        }
                        Popped::Empty => {}
                    }
                } else if !stdout_turn && !this.stderr_done {
                    match this.stderr_sink.try_pop() {
                        Popped::Line(line) => {
                            this.stderr_wait = None;
                            this.prefer_stdout = true;
                            return Poll::Ready(Some(OutputEvent::Stderr(OutputLine {
                                text: line,
                            })));
                        }
                        Popped::Closed => {
                            this.stderr_done = true;
                            this.stderr_wait = None;
                        }
                        Popped::Empty => {}
                    }
                }
            }

            if this.stdout_done && this.stderr_done {
                return Poll::Ready(None);
            }

            // Register a wait future for each open stream so a wakeup from either
            // re-enters the loop.
            let mut any_ready = false;
            if !this.stdout_done {
                if this.stdout_wait.is_none() {
                    this.stdout_wait = Some(Box::pin(this.stdout_sink.clone().changed()));
                }
                if this
                    .stdout_wait
                    .as_mut()
                    .expect("just set")
                    .as_mut()
                    .poll(cx)
                    .is_ready()
                {
                    this.stdout_wait = None;
                    any_ready = true;
                }
            }
            if !this.stderr_done {
                if this.stderr_wait.is_none() {
                    this.stderr_wait = Some(Box::pin(this.stderr_sink.clone().changed()));
                }
                if this
                    .stderr_wait
                    .as_mut()
                    .expect("just set")
                    .as_mut()
                    .poll(cx)
                    .is_ready()
                {
                    this.stderr_wait = None;
                    any_ready = true;
                }
            }
            if any_ready {
                continue;
            }
            return Poll::Pending;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::OutputBufferPolicy;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn output_events_interleaves_fairly_between_two_ready_streams() {
        let policy = OutputBufferPolicy::unbounded();
        let stdout_sink = SharedLines::new(&policy);
        let stderr_sink = SharedLines::new(&policy);
        for line in ["o1", "o2", "o3"] {
            stdout_sink.push(line.to_owned());
        }
        for line in ["e1", "e2", "e3"] {
            stderr_sink.push(line.to_owned());
        }
        stdout_sink.close_now();
        stderr_sink.close_now();

        let mut events = OutputEvents {
            stdout_sink,
            stderr_sink,
            stdout_wait: None,
            stderr_wait: None,
            stdout_done: false,
            stderr_done: false,
            prefer_stdout: true,
        };
        let mut seq = Vec::new();
        while let Some(ev) = events.next().await {
            seq.push(match ev {
                OutputEvent::Stdout(l) => format!("O:{}", l.text()),
                OutputEvent::Stderr(l) => format!("E:{}", l.text()),
            });
        }
        assert_eq!(
            seq,
            ["O:o1", "E:e1", "O:o2", "E:e2", "O:o3", "E:e3"],
            "merged stream must interleave, not drain stdout first"
        );
    }

    #[tokio::test]
    async fn output_event_carries_an_output_line_with_a_text_accessor() {
        let policy = OutputBufferPolicy::unbounded();
        let stdout_sink = SharedLines::new(&policy);
        let stderr_sink = SharedLines::new(&policy);
        stdout_sink.push("out".to_owned());
        stderr_sink.push("err".to_owned());
        stdout_sink.close_now();
        stderr_sink.close_now();
        let mut events = OutputEvents {
            stdout_sink,
            stderr_sink,
            stdout_wait: None,
            stderr_wait: None,
            stdout_done: false,
            stderr_done: false,
            prefer_stdout: true,
        };
        let first = events.next().await.expect("a stdout event");
        assert!(
            matches!(&first, OutputEvent::Stdout(line) if line.text() == "out"),
            "stdout event carries an OutputLine: {first:?}"
        );
        assert_eq!(first.text(), Some("out"), "text() reads the line");
        let second = events.next().await.expect("a stderr event");
        assert!(matches!(&second, OutputEvent::Stderr(line) if line.text() == "err"));
        assert_eq!(second.text(), Some("err"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_lines_loses_no_line_under_a_parking_consumer() {
        const N: usize = 5_000;
        let sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let producer = {
            let sink = sink.clone();
            tokio::spawn(async move {
                for i in 0..N {
                    sink.push(i.to_string());
                    if i % 7 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                sink.close_now();
            })
        };
        let mut lines = StdoutLines { sink, wait: None };
        let consume = async {
            let mut seen = 0usize;
            while let Some(line) = lines.next().await {
                assert_eq!(line, seen.to_string(), "lines must arrive in push order");
                seen += 1;
            }
            seen
        };
        let seen = tokio::time::timeout(std::time::Duration::from_secs(30), consume)
            .await
            .expect("consumer hung — possible lost wakeup");
        producer.await.expect("producer task");
        assert_eq!(seen, N, "every pushed line must be received");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn output_events_lose_no_line_under_two_racing_producers() {
        const N: usize = 3_000;
        let stdout_sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let stderr_sink = SharedLines::new(&OutputBufferPolicy::unbounded());
        let feed = |sink: Arc<SharedLines>| {
            tokio::spawn(async move {
                for i in 0..N {
                    sink.push(i.to_string());
                    if i % 5 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                sink.close_now();
            })
        };
        let p_out = feed(stdout_sink.clone());
        let p_err = feed(stderr_sink.clone());
        let mut events = OutputEvents {
            stdout_sink,
            stderr_sink,
            stdout_wait: None,
            stderr_wait: None,
            stdout_done: false,
            stderr_done: false,
            prefer_stdout: true,
        };
        let consume = async {
            let (mut out, mut err) = (0usize, 0usize);
            while let Some(ev) = events.next().await {
                match ev {
                    OutputEvent::Stdout(_) => out += 1,
                    OutputEvent::Stderr(_) => err += 1,
                }
            }
            (out, err)
        };
        let (out, err) = tokio::time::timeout(std::time::Duration::from_secs(30), consume)
            .await
            .expect("consumer hung — possible lost wakeup");
        p_out.await.expect("stdout producer");
        p_err.await.expect("stderr producer");
        assert_eq!(out, N, "every stdout line received");
        assert_eq!(err, N, "every stderr line received");
    }
}
