//! Incremental stdout streaming: [`StdoutLines`], the watchdog tasks that
//! bound a streamed run (deadline/cancel), and `finish_streamed`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use tokio::io::AsyncReadExt;
use tokio_stream::Stream;

use crate::error::Result;
use crate::group::ProcessGroup;
use crate::pump::{Popped, SharedLines, pump_lines};

use super::RunningProcess;

impl RunningProcess {
    /// Stream the child's standard output line by line. Call this **once**.
    ///
    /// Standard error is drained in the background (so the child can't block on a
    /// full stderr pipe) and discarded — use [`output_string`](Self::output_string)
    /// when you need both. Keep this `RunningProcess` in scope while consuming;
    /// dropping it tears the process down.
    ///
    /// The command's [`timeout`](crate::Command::timeout), if set, **bounds the
    /// stream**: at the deadline the process tree is killed, so the pipes close
    /// and this stream ends — a streamed run can't hang past its timeout. A
    /// following [`finish_streamed`](Self::finish_streamed) then reports the kill
    /// (no clean exit: `code` is `None` on a Unix signal-kill, a platform code on
    /// a Windows Job kill). With no timeout the stream is unbounded as before.
    /// (Bounding applies to a run that owns its group — the
    /// [`Command::start`](crate::Command::start) / [`JobRunner`](crate::JobRunner)
    /// path. A handle from [`ProcessGroup::start`](crate::ProcessGroup::start)
    /// shares its group, so the caller bounds the stream.)
    ///
    /// # Example
    ///
    /// Stream stdout line by line as it is produced, then collect the exit code
    /// and stderr:
    ///
    /// ```no_run
    /// use processkit::{Command, StreamExt};
    ///
    /// # async fn demo() -> processkit::Result<()> {
    /// let mut run = Command::new("git").args(["log", "--oneline", "-n", "20"]).start().await?;
    ///
    /// let mut lines = run.stdout_lines();
    /// while let Some(line) = lines.next().await {
    ///     println!("commit: {line}");
    /// }
    ///
    /// let (code, stderr) = run.finish_streamed().await?;
    /// # let _ = (code, stderr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn stdout_lines(&mut self) -> StdoutLines {
        // Background-drain stderr (counter + handler still apply). The handle is
        // kept so `finish_streamed` can await the last line before draining. Only
        // set up once: a second `stdout_lines` call must not overwrite the first
        // call's sink/pump, or `finish_streamed` would return empty stderr.
        if self.stderr_sink.is_none() {
            let stderr_sink = SharedLines::new(&self.buffer);
            if let Some(pipe) = self.backend.take_stderr_reader() {
                self.stderr_pump = Some(tokio::spawn(pump_lines(
                    pipe,
                    self.stderr_encoding,
                    self.stderr_handler.clone(),
                    stderr_sink.clone(),
                )));
            }
            self.stderr_sink = Some(stderr_sink);
        }

        let stdout_sink = SharedLines::new(&self.buffer);
        match self.backend.take_stdout_reader() {
            Some(pipe) => {
                tokio::spawn(pump_lines(
                    pipe,
                    self.stdout_encoding,
                    self.stdout_handler.clone(),
                    stdout_sink.clone(),
                ));
            }
            // Called more than once: hand back an immediately-finished stream.
            None => stdout_sink.close_now(),
        }
        self.stdout_sink = Some(stdout_sink.clone());

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
            self.deadline_task = Some(tokio::spawn(async move {
                tokio::time::sleep(limit).await;
                match grace {
                    // Graceful: signal the (still-owned) group, wait the grace,
                    // then KILL. This detached watchdog doesn't hold the `Child`,
                    // so it can't reap concurrently — a child that exits on the
                    // signal closes its pipes (ending the stream promptly), but this
                    // task still waits the full grace before its no-op SIGKILL. The
                    // early-grace-exit reaping lives in the bulk `finish_streamed` /
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

        // Likewise bound it by the cancellation token, so a pure-streaming
        // consumer's stream ends when the run is cancelled (same private-group
        // asymmetry as the deadline timer).
        #[cfg(feature = "cancellation")]
        if self.cancel_task.is_none()
            && let (Some(token), Some(group)) =
                (self.cancel_token.clone(), self.backend.own_group())
        {
            let group = Arc::downgrade(group);
            let pid = self.pid;
            self.cancel_task = Some(tokio::spawn(async move {
                token.cancelled().await;
                kill_via_weak(&group, pid);
            }));
        }

        StdoutLines {
            sink: stdout_sink,
            wait: None,
        }
    }

    /// Finish a streamed run: wait for exit and return the exit code plus the
    /// stderr collected in the background by [`stdout_lines`](Self::stdout_lines).
    ///
    /// Designed to pair with `stdout_lines` (consume the stdout stream first),
    /// but safe to call on its own — any pipe the stream didn't take is drained
    /// here so the child can never block on a full pipe.
    pub async fn finish_streamed(mut self) -> Result<(Option<i32>, String)> {
        // Drain a stdout pipe a prior `stdout_lines` didn't take (and discard
        // it) so the child can't block writing to it while we wait for exit.
        // Deliberately unbounded: a deadline here would cut the drain off
        // under a still-running chatty child and re-create the blocked-pipe
        // hang. The cost of the alternative — a shared-group descendant
        // holding the pipe parks this one idle reader until it exits — is
        // benign.
        if let Some(mut pipe) = self.backend.take_stdout_reader() {
            tokio::spawn(async move {
                let mut sink = Vec::new();
                // Result ignored on purpose: the drain exists only to unblock
                // the child; the bytes are discarded either way.
                let _ = pipe.read_to_end(&mut sink).await;
            });
        }
        // Likewise start a stderr pump if streaming never did (so its output is
        // still captured and the pipe never fills).
        if self.stderr_pump.is_none()
            && let Some(pipe) = self.backend.take_stderr_reader()
        {
            let sink = SharedLines::new(&self.buffer);
            self.stderr_pump = Some(tokio::spawn(pump_lines(
                pipe,
                self.stderr_encoding,
                self.stderr_handler.clone(),
                sink.clone(),
            )));
            self.stderr_sink = Some(sink);
        }

        let outcome = self.drive_to_exit().await?;
        self.observe_stdin_task().await;
        let (code, _timed_out) = self.checked_outcome(outcome)?;
        // The child has exited, so its stderr pipe is closed — await the pump so
        // the final buffered line is captured before we drain. (The pump
        // returns `()`; a JoinError here means it panicked, and its
        // close-on-drop guard already preserved the captured lines.)
        if let Some(pump) = self.stderr_pump.take() {
            let _ = pump.await;
        }
        let stderr = self
            .stderr_sink
            .as_ref()
            .map(|sink| sink.drain().join("\n"))
            .unwrap_or_default();
        Ok((code, stderr))
    }
}

/// The shared kill step of the streaming watchdogs (deadline timer and cancel
/// listener): tear down the group if it is still around, then best-effort kill
/// the direct child. The `Weak` means a watchdog never delays the group's
/// kill-on-close when the handle is dropped early.
fn kill_via_weak(group: &Weak<ProcessGroup>, pid: Option<u32>) {
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
        let deadline = tokio::time::Instant::now() + grace;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            // SAFETY: signal 0 only probes existence/permission. Non-zero => gone.
            if unsafe { libc::kill(pid, 0) } != 0 {
                return;
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
fn kill_direct_child(pid: Option<u32>) {
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
    #[cfg(not(any(unix, windows)))]
    let _ = pid;
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
