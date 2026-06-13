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

use super::RunningProcess;

/// The outcome of a run driven via
/// [`stdout_lines`](RunningProcess::stdout_lines) or
/// [`output_events`](RunningProcess::output_events): how the run ended plus
/// the captured standard error. Returned by
/// [`RunningProcess::finish`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finished {
    /// How the run ended.
    pub outcome: Outcome,
    /// Standard error captured in the background while stdout was streaming.
    /// `String::new()` when no stderr was produced or stderr was not piped.
    pub stderr: String,
}

impl RunningProcess {
    /// Stream the child's standard output line by line. Call this **once**.
    ///
    /// Standard error is drained in the background (so the child can't block on a
    /// full stderr pipe) and discarded — use [`output_string`](Self::output_string)
    /// when you need both. Keep this `RunningProcess` in scope while consuming;
    /// dropping it tears the process down.
    ///
    /// The command's [`timeout`](crate::Command::timeout), if set, **bounds the
    /// stream**: at the deadline the real child's process tree is killed
    /// (gracefully if a [`timeout_grace`](crate::Command::timeout_grace) is set),
    /// so the pipes close and this stream ends — a streamed run can't hang past
    /// its timeout. A following [`finish`](Self::finish) then
    /// reports [`Outcome::TimedOut`](crate::Outcome::TimedOut) — deterministically,
    /// and even if the child caught the signal and exited cleanly within the grace
    /// — consistent with the bulk `output_string` path. With no timeout the stream
    /// is unbounded as before.
    /// (For a real child, bounding applies to a run that owns its group — the
    /// [`Command::start`](crate::Command::start) / [`JobRunner`](crate::JobRunner)
    /// path. A handle from [`ProcessGroup::start`](crate::ProcessGroup::start)
    /// shares its group, so the caller bounds the stream. A
    /// [`ScriptedRunner`](crate::ScriptedRunner) handle is bounded too — its
    /// canned feeders are hung up at the deadline — but, having no signal tier
    /// (like Windows), it ignores `timeout_grace` and ends at once.)
    ///
    /// **D2 — fallible, stream once.** Returns `Err` (an
    /// [`Error::Io`](crate::Error::Io) with
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput)) instead of a
    /// silently-empty stream when **(a)** `stdout` was set to
    /// [`Inherit`](crate::StdioMode::Inherit) / [`Null`](crate::StdioMode::Null)
    /// (not the default [`Piped`](crate::StdioMode::Piped) — nothing to read), or
    /// **(b)** a streaming verb (`stdout_lines` / `output_events`) was already
    /// called on this handle (stdout is consumed exactly once). This mirrors the
    /// bulk verbs' loudness — a non-piped or repeated call is a clear error, not
    /// a stream that quietly yields nothing.
    ///
    /// # Example
    ///
    /// Stream stdout line by line as it is produced, then collect the outcome
    /// and stderr:
    ///
    /// ```no_run
    /// use processkit::{Command, StreamExt, Finished};
    ///
    /// # async fn demo() -> processkit::Result<()> {
    /// let mut run = Command::new("git").args(["log", "--oneline", "-n", "20"]).start().await?;
    ///
    /// let mut lines = run.stdout_lines()?;
    /// while let Some(line) = lines.next().await {
    ///     println!("commit: {line}");
    /// }
    ///
    /// let Finished { outcome, stderr } = run.finish().await?;
    /// # let _ = (outcome, stderr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn stdout_lines(&mut self) -> Result<StdoutLines> {
        self.ensure_stdout_streamable()?;
        // Background-drain stderr (counter + handler still apply). The handle is
        // kept so `finish` can await the last line before draining. Only
        // set up once: a second `stdout_lines` call must not overwrite the first
        // call's sink/pump, or `finish` would return empty stderr.
        if self.stderr_sink.is_none() {
            let stderr_sink = SharedLines::new(&self.buffer);
            if let Some(pipe) = self.backend.take_stderr_reader() {
                self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_encoding,
                    self.stderr_handler.clone(),
                    self.stderr_tee.clone(),
                    stderr_sink.clone(),
                )));
            }
            self.stderr_sink = Some(stderr_sink);
        }

        let stdout_sink = SharedLines::new(&self.buffer);
        match self.backend.take_stdout_reader() {
            Some(pipe) => {
                // Store the handle (like `output_events`) so `finish`
                // joins it before the fail-loud overflow check and `Drop` aborts
                // it on a shared-group handle — a discarded handle would leave
                // both as no-ops for the stdout stream.
                self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stdout_encoding,
                    self.stdout_handler.clone(),
                    self.stdout_tee.clone(),
                    stdout_sink.clone(),
                )));
            }
            // Defensive: `ensure_stdout_streamable` (above) already rejects a
            // non-piped or already-consumed stdout with an `Err` (D2), so this
            // arm is effectively unreachable — close the sink so the stream ends
            // at once rather than hanging if an internal caller ever reaches it.
            None => stdout_sink.close_now(),
        }
        // L1: only store on the first call — a repeat call's stdout_sink is a
        // fresh closed empty sink; overwriting self.stdout_sink with it would
        // silently discard the first pump's overflow flag and line count.
        if self.stdout_sink.is_none() {
            self.stdout_sink = Some(stdout_sink.clone());
        }

        // Bound the stream by the command's timeout: kill the tree at the deadline
        // so the pipes close and this stream ends. A `Weak` to the group means a
        // hard-kill timer never delays kill-on-close when the handle is dropped
        // early (the graceful branch below holds the upgraded `Arc` only until its
        // next poll await, so a dropped handle delays the Drop by at most one poll
        // interval). Armed once (a second `stdout_lines` call won't duplicate it).
        if self.deadline_task.is_none()
            && let (Some(limit), Some(group)) = (self.timeout, self.backend.own_group())
        {
            let group = Arc::downgrade(group);
            let pid = self.pid;
            let grace = self.timeout_grace;
            let signal = self.timeout_signal;
            // Anchor to spawn time so a late stream call can't re-grant the
            // full limit (B7 fix). `started` is std::time::Instant (Copy).
            let started = self.started;
            // B1: claim the timeout via the shared arbiter so the finisher
            // classifies the run as `TimedOut` even if the child then exits
            // cleanly within the grace. Only kill if we WON the race against the
            // natural reap (which claims `EXITED` in `backend_wait`): if the child
            // already exited on its own, the CAS fails and we skip the kill —
            // leaving the real exit and avoiding a signal to a recycled pid.
            let timeout_state = self.timeout_state.clone();
            self.deadline_task = Some(tokio::spawn(async move {
                let remaining = limit
                    .checked_sub(started.elapsed())
                    .unwrap_or(std::time::Duration::ZERO);
                tokio::time::sleep(remaining).await;
                if timeout_state
                    .compare_exchange(
                        super::TS_PENDING,
                        super::TS_TIMED_OUT,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_err()
                {
                    return; // the child already exited on its own — no kill
                }
                match grace {
                    // Graceful: signal the (still-owned) group, wait the grace,
                    // then KILL. This detached watchdog doesn't hold the `Child`,
                    // so it can't reap concurrently — a child that exits on the
                    // signal closes its pipes (ending the stream promptly), but this
                    // task still waits the full grace before its no-op SIGKILL. The
                    // early-grace-exit reaping lives in the bulk `finish` /
                    // `drive_to_exit` path (`teardown_on_timeout`), not here.
                    Some(grace) => match group.upgrade() {
                        Some(group) => {
                            let _ = group.graceful_terminate(grace, signal).await;
                        }
                        // Group already gone (handle dropped) → tree torn down.
                        None => kill_direct_child(pid),
                    },
                    None => kill_via_weak(&group, pid),
                }
            }));
        }

        // F2: the scripted analogue — a scripted handle has no group to kill, so
        // bound the stream by hanging up the feeders at the deadline (their EOF
        // ends the pump and this stream, exactly as a real tree's closing pipes
        // do). Claim the timeout via the arbiter first so the finisher classifies
        // `TimedOut`; if the script already exited the CAS fails and we skip.
        self.arm_scripted_deadline();

        // The cancel watchdog is armed at spawn time by `arm_cancel_watchdog`
        // (via `launch`/`attach_group`), so streaming consumers don't need to
        // re-arm it here. The stored `cancel_task` is already `Some` if a token
        // was configured; `abort_watchdogs` will abort it on reap or drop.

        Ok(StdoutLines {
            sink: stdout_sink,
            wait: None,
        })
    }

    /// Finish a streamed run: wait for exit and return a [`Finished`]
    /// carrying the [`Outcome`] and the stderr collected in the background by
    /// [`stdout_lines`](Self::stdout_lines).
    ///
    /// A run killed by its [`timeout`](crate::Command::timeout) reports
    /// [`Outcome::TimedOut`](crate::Outcome::TimedOut), even if the child caught
    /// the signal and exited cleanly within the grace — matching the bulk verbs.
    ///
    /// Designed to pair with `stdout_lines` (consume the stdout stream first),
    /// but safe to call on its own — any pipe the stream didn't take is drained
    /// here so the child can never block on a full pipe.
    pub async fn finish(mut self) -> Result<Finished> {
        // B5: drain an untaken stdout pipe through the policy-aware line pump
        // instead of read_to_end into an unbounded Vec.  This applies the
        // buffer policy (including fail_loud), counts lines, calls handlers,
        // and stores the handle in self.stdout_pump so join_pumps (below)
        // joins it and Drop aborts it on an early-error exit.
        if let Some(pipe) = self.backend.take_stdout_reader() {
            let sink = crate::pump::SharedLines::new(&self.buffer);
            self.stdout_pump = Some(tokio::spawn(crate::pump::pump_lines_core(
                pipe,
                self.stdout_encoding,
                self.stdout_handler.clone(),
                self.stdout_tee.clone(),
                sink.clone(),
            )));
            self.stdout_sink = Some(sink);
        }
        // Likewise start a stderr pump if streaming never did (so its output is
        // still captured and the pipe never fills).
        if self.stderr_pump.is_none()
            && let Some(pipe) = self.backend.take_stderr_reader()
        {
            let sink = SharedLines::new(&self.buffer);
            self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                self.stderr_tee.clone(),
                sink.clone(),
            )));
            self.stderr_sink = Some(sink);
        }

        let raw_outcome = self.drive_to_exit().await?;
        self.observe_stdin_task().await;
        // Join both streaming pumps before the cancellation/overflow checks so
        // their final writes are visible. `join_pumps` bounds the wait by
        // `PUMP_TEARDOWN` and aborts stragglers, so a surviving grandchild
        // holding a pipe open past the child's death can't park this finisher
        // forever (parity with the bulk verbs). The child's own pipes are
        // closed at this point, so the common case completes immediately.
        let pumps: Vec<_> = [self.stdout_pump.take(), self.stderr_pump.take()]
            .into_iter()
            .flatten()
            .collect();
        super::join_pumps(pumps).await;
        let outcome = self.checked_outcome(raw_outcome)?;
        // Fail-loud ceiling check for both line-pumped streams.
        for sink in [self.stdout_sink.as_ref(), self.stderr_sink.as_ref()]
            .into_iter()
            .flatten()
        {
            if sink.overflowed() {
                return Err(crate::Error::OutputTooLarge {
                    program: self.program.clone(),
                    line_limit: self.buffer.max_lines,
                    byte_limit: self.buffer.max_bytes,
                    total_lines: sink.count(),
                    total_bytes: sink.seen_bytes(),
                });
            }
        }
        let stderr = self
            .stderr_sink
            .as_ref()
            .map(|sink| sink.drain().join("\n"))
            .unwrap_or_default();
        Ok(Finished { outcome, stderr })
    }

    /// Stream both stdout and stderr as a single ordered sequence of
    /// [`OutputEvent`] items — each event tagged with its origin stream —
    /// as the child produces them. Call this **once**.
    ///
    /// **D2 — fallible, stream once.** Like [`stdout_lines`](Self::stdout_lines),
    /// returns `Err` (an [`Error::Io`](crate::Error::Io)) rather than a
    /// silently-empty stream when stdout was not piped, or when a streaming verb
    /// already consumed stdout on this handle.
    ///
    /// Interleaving is best-effort (lines are ordered by when they arrive in
    /// the async runtime, not by a kernel timestamp). D9d: the two streams are
    /// polled **fairly** — the first-look alternates each poll, so a
    /// continuously-ready stream can't starve the other (neither monopolizes
    /// while the peer loses lines or trips its
    /// [`fail_loud`](crate::OutputBufferPolicy::fail_loud) ceiling). Use
    /// [`finish`](Self::finish) after draining to collect the run's
    /// [`Outcome`](crate::Outcome) (its `stderr` is empty — stderr was delivered
    /// to you as events).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use processkit::{Command, OutputEvent, StreamExt};
    ///
    /// # async fn demo() -> processkit::Result<()> {
    /// let mut run = Command::new("make").arg("build").start().await?;
    ///
    /// let mut events = run.output_events()?;
    /// while let Some(event) = events.next().await {
    ///     match event {
    ///         OutputEvent::Stdout(line) => println!("out: {line}"),
    ///         OutputEvent::Stderr(line) => eprintln!("err: {line}"),
    ///     }
    /// }
    ///
    /// let outcome = run.finish().await?.outcome;
    /// # let _ = outcome;
    /// # Ok(())
    /// # }
    /// ```
    pub fn output_events(&mut self) -> Result<OutputEvents> {
        self.ensure_stdout_streamable()?;
        // Set up stdout sink + pump. The handle is stored so `finish`
        // can join it before checking overflow (ensuring the pump's last write
        // is visible before `overflowed()` is queried).
        let stdout_sink = SharedLines::new(&self.buffer);
        match self.backend.take_stdout_reader() {
            Some(pipe) => {
                self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stdout_encoding,
                    self.stdout_handler.clone(),
                    self.stdout_tee.clone(),
                    stdout_sink.clone(),
                )));
            }
            None => stdout_sink.close_now(),
        }
        // L1: only store on the first call — a repeat call's stdout_sink is a
        // fresh closed empty sink; overwriting self.stdout_sink would discard
        // the first pump's overflow flag and line count.
        if self.stdout_sink.is_none() {
            self.stdout_sink = Some(stdout_sink.clone());
        }

        // Set up stderr sink + pump on the first call only.  On a repeat call
        // give the returned OutputEvents its own immediately-closed stderr so the
        // two consumers don't share a SharedLines — a shared sink's notify_one
        // on close wakes only one waiter, leaving the other parked forever (L2).
        let stderr_sink = if self.stderr_sink.is_none() {
            let sink = SharedLines::new(&self.buffer);
            if let Some(pipe) = self.backend.take_stderr_reader() {
                self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_encoding,
                    self.stderr_handler.clone(),
                    self.stderr_tee.clone(),
                    sink.clone(),
                )));
            } else {
                sink.close_now();
            }
            self.stderr_sink = Some(sink.clone());
            sink
        } else {
            // Repeat call: return a fresh closed sink so this OutputEvents'
            // stderr stream ends immediately without racing the first sink.
            let closed = SharedLines::new(&self.buffer);
            closed.close_now();
            closed
        };

        // Arm the deadline watchdog (same as stdout_lines — bounds the stream).
        if self.deadline_task.is_none()
            && let (Some(limit), Some(group)) = (self.timeout, self.backend.own_group())
        {
            let group = Arc::downgrade(group);
            let pid = self.pid;
            let grace = self.timeout_grace;
            let signal = self.timeout_signal;
            let started = self.started;
            // B1: see `stdout_lines` — claim the timeout via the arbiter and
            // kill only if we won the race against the natural reap.
            let timeout_state = self.timeout_state.clone();
            self.deadline_task = Some(tokio::spawn(async move {
                let remaining = limit
                    .checked_sub(started.elapsed())
                    .unwrap_or(std::time::Duration::ZERO);
                tokio::time::sleep(remaining).await;
                if timeout_state
                    .compare_exchange(
                        super::TS_PENDING,
                        super::TS_TIMED_OUT,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_err()
                {
                    return; // the child already exited on its own — no kill
                }
                match grace {
                    Some(grace) => match group.upgrade() {
                        Some(group) => {
                            let _ = group.graceful_terminate(grace, signal).await;
                        }
                        None => kill_direct_child(pid),
                    },
                    None => kill_via_weak(&group, pid),
                }
            }));
        }

        // F2: scripted analogue — see `stdout_lines`.
        self.arm_scripted_deadline();

        // Cancel watchdog is now armed at spawn time — no re-arm needed here.

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

/// The shared kill step of the streaming watchdogs (deadline timer and cancel
/// listener): tear down the group if it is still around, then best-effort kill
/// the direct child. The `Weak` means a watchdog never delays the group's
/// kill-on-close when the handle is dropped early.
pub(super) fn kill_via_weak(group: &Weak<ProcessGroup>, pid: Option<u32>) {
    if let Some(group) = group.upgrade() {
        let _ = group.terminate_all();
    }
    kill_direct_child(pid);
}

/// Gracefully terminate a single child by pid — the **shared-group** timeout
/// case (no owned group to tear down). Send `signal`, poll liveness up to
/// `grace`, then `SIGKILL`. The caller reaps the child concurrently, so a child
/// that exits on the signal is collected and the poll ends early instead of
/// seeing an unreaped zombie as still-alive. Windows has no signal tier — hard
/// kill.
///
/// Like any bare-pid signal (cf. `kill_direct_child`), this relies on the
/// concurrent reap winning the race: a pid recycled between the reap and the
/// next poll could in principle receive the final `SIGKILL`. The window is
/// narrow and the alternative (no force-kill) is worse; kernel-handle
/// mechanisms (Job/cgroup) take the own-group path instead.
pub(crate) async fn graceful_kill_pid(pid: Option<u32>, grace: std::time::Duration, signal: i32) {
    #[cfg(unix)]
    {
        let Some(pid) = pid else { return };
        let pid = pid as i32;
        // SAFETY: sending a signal to a pid is safe; ESRCH (gone) is ignored.
        unsafe {
            libc::kill(pid, signal);
        }
        // E15: clamp so a `Duration::MAX`-ish grace can't overflow `Instant + Duration`.
        let deadline = tokio::time::Instant::now() + grace.min(crate::MAX_DEADLINE);
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            // SAFETY: signal 0 only probes existence/permission.
            // ESRCH (non-zero, non-EPERM) → gone: exit the grace early.
            // EPERM → alive but can't be signalled (e.g. after a uid change) —
            // treat as exists, matching `Tracked::exists`'s convention; the
            // final SIGKILL is still sent best-effort. Any other non-zero (rare)
            // is treated conservatively as "still alive".
            let probe = unsafe { libc::kill(pid, 0) };
            if probe != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EPERM) {
                return; // ESRCH: gone
            }
            let poll = std::time::Duration::from_millis(20);
            tokio::time::sleep(poll.min(deadline - now)).await;
        }
        // SAFETY: as above; force the survivor down (a no-op if already gone).
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        // No signal tier off unix: hard kill (Windows TerminateProcess).
        let _ = (grace, signal);
        kill_direct_child(pid);
    }
}

/// Best-effort kill of the direct child by pid, used by the streaming
/// deadline/cancel tasks after the group teardown — parity with
/// `kill_tree`'s `start_kill` + `terminate_all` pairing (the tasks can't
/// reach the `Child` handle, so they signal by pid). The group kill usually
/// makes this a no-op; it exists so a group-kill miss (e.g. a pgroup
/// broadcast racing the tree) still closes the pipes and ends the stream.
/// Also called by the spawn-time cancel watchdog in `mod.rs`.
pub(super) fn kill_direct_child(pid: Option<u32>) {
    let Some(pid) = pid else { return };
    #[cfg(unix)]
    // SAFETY: SIGKILL to a specific live-or-zombie pid; an exited/reaped pid
    // yields ESRCH, which is ignored.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    #[cfg(windows)]
    // SAFETY: opens the process by id with the narrowest right; both calls
    // tolerate an already-exited process (open fails, handle closed once).
    unsafe {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_TERMINATE, TerminateProcess,
        };
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if !handle.is_null() {
            TerminateProcess(handle, 1);
            CloseHandle(handle);
        }
    }
}

/// A `Stream` of the child's standard-output lines (see
/// [`RunningProcess::stdout_lines`]).
pub struct StdoutLines {
    sink: Arc<SharedLines>,
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

// Manual: the sink and the pending-wait future are opaque.
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
                    // `notify_one` stores a permit, so a push between the `try_pop`
                    // above and registering here is not missed.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputEvent {
    /// A line from the child's standard output.
    Stdout(String),
    /// A line from the child's standard error.
    Stderr(String),
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
    /// D9d: which stream gets the first look each poll. Flipped after every
    /// emitted line so a continuously-ready stream can't starve the other.
    prefer_stdout: bool,
}

// Manual: the sinks and pending-wait futures are opaque.
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
            // D9d: give each stream the first look on alternating polls so a
            // continuously-ready stream can't starve the other. `prefer_stdout`
            // flips after every emitted line; `[pref, !pref]` visits both.
            for stdout_turn in [this.prefer_stdout, !this.prefer_stdout] {
                if stdout_turn && !this.stdout_done {
                    match this.stdout_sink.try_pop() {
                        Popped::Line(line) => {
                            this.stdout_wait = None;
                            this.prefer_stdout = false; // stderr gets the next first look
                            return Poll::Ready(Some(OutputEvent::Stdout(line)));
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
                            return Poll::Ready(Some(OutputEvent::Stderr(line)));
                        }
                        Popped::Closed => {
                            this.stderr_done = true;
                            this.stderr_wait = None;
                        }
                        Popped::Empty => {}
                    }
                }
            }

            // Both streams are closed and drained.
            if this.stdout_done && this.stderr_done {
                return Poll::Ready(None);
            }

            // At least one stream is open but currently empty: register wait
            // futures for each open stream and return Pending.  Both futures
            // are polled so wakeups from *either* stream are registered —
            // whichever fires first re-enters the loop above.
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
                // At least one notification arrived: re-enter the loop to
                // drain whatever arrived.
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

    /// D9d: when both streams are continuously ready, the merged event stream
    /// alternates between them rather than draining all of stdout first — so a
    /// chatty stdout can't starve stderr (or vice versa).
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
        // Closed and pre-filled, so every poll finds both ready — deterministic.
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
                OutputEvent::Stdout(l) => format!("O:{l}"),
                OutputEvent::Stderr(l) => format!("E:{l}"),
            });
        }
        assert_eq!(
            seq,
            ["O:o1", "E:e1", "O:o2", "E:e2", "O:o3", "E:e3"],
            "merged stream must interleave, not drain stdout first"
        );
    }
}
