//! Readiness probes: `wait_for_line` / `wait_for` / `wait_for_port` and their
//! shared polling loop. All three background-drain stdout/stderr while they
//! poll, so a chatty child can't stall in `write()` on a full OS pipe buffer;
//! only `wait_for_line` hands any of the drained stdout back to the caller.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`) for the poll deadlines
// below: they are slept out on tokio's timer (`tokio::time::sleep` /
// `tokio::time::timeout`), so a deadline computed from `Instant::now() + within`
// must track the same (possibly paused) virtual clock — otherwise a probe under
// a paused runtime would misjudge its remaining budget. Same shared-clock
// rationale as `running::deadline` and `sys::graceful`.
use tokio::time::Instant;

use tokio::net::TcpStream;

use crate::error::{Error, Result};

use super::RunningProcess;

/// How often [`RunningProcess::wait_for`] / [`wait_for_port`]
/// (RunningProcess::wait_for_port) re-check readiness — responsive without
/// busy-spinning; matches the 50 ms liveness-poll cadence used elsewhere.
const READINESS_POLL: Duration = Duration::from_millis(50);

/// Cap on a single `wait_for_port` connect attempt (clamped to the remaining
/// budget), so one stalled connect can't overrun the probe deadline.
const CONNECT_ATTEMPT_CAP: Duration = Duration::from_secs(1);

impl RunningProcess {
    /// Wait until a stdout line matches `predicate` (returning that line), or
    /// fail with [`Error::NotReady`] when `within` elapses — or immediately
    /// when stdout closes before a match (e.g. the child exited and no
    /// descendant kept the pipe open), since no further line can arrive. A
    /// child that exits while a descendant still holds its stdout keeps the
    /// stream open, so that case waits out the deadline — the pipe, not the
    /// process, is what this probe watches.
    ///
    /// The readiness idiom: start a server, wait for its "listening on …"
    /// banner, then use it — no arbitrary sleeps.
    ///
    /// # Caveats
    ///
    /// - **Consumes stdout** up to and including the matching line (the same
    ///   one-shot stdout drain [`stdout_lines`](Self::stdout_lines) uses — if
    ///   stdout was already consumed by an earlier `stdout_lines` /
    ///   `output_events` / `wait_for_line`, or was not piped, this returns an
    ///   `Err` rather than a stream that is forever `NotReady`). Continue
    ///   with [`finish`](Self::finish) for the outcome and stderr;
    ///   [`wait_for`](Self::wait_for) / [`wait_for_port`](Self::wait_for_port)
    ///   background-drain stdout the same way, they just never hand any of it
    ///   back to the caller mid-probe.
    /// - A failed probe does **not** kill the child, and — unlike
    ///   [`stdout_lines`](Self::stdout_lines) — it does **not** arm the
    ///   [`Command::timeout`](crate::Command::timeout) watchdog: this probe is
    ///   bounded only by its own `within`, and the command's timeout is enforced
    ///   later by the consuming verb ([`finish`](Self::finish)). So a probe can
    ///   never tear the tree down or flip the run's outcome to `TimedOut`,
    ///   matching [`wait_for`](Self::wait_for) / [`wait_for_port`](Self::wait_for_port).
    ///
    /// # Errors
    ///
    /// - [`Error::NotReady`] when `within` elapses with no matching line, or
    ///   immediately when stdout closes first (no further line can arrive). This
    ///   is a *probe* deadline — distinct from [`Error::Timeout`], and a failed
    ///   probe neither kills the child nor flips its outcome to `TimedOut`.
    /// - [`Error::Io`] when stdout was not piped, or a prior streaming verb
    ///   already consumed it (so no line stream can be drained).
    pub async fn wait_for_line(
        &mut self,
        predicate: impl Fn(&str) -> bool + Send,
        within: Duration,
    ) -> Result<String> {
        use tokio_stream::StreamExt;

        // `drain_stdout_lines` (not `stdout_lines`) drains stdout WITHOUT arming
        // the `Command::timeout` watchdog, so a readiness probe can never kill the
        // tree or flip the outcome to `TimedOut`. It owns its sink, leaving `self`
        // borrowable after the search, and is fallible (non-piped or
        // already-consumed stdout) rather than forever `NotReady`.
        let mut lines = self.drain_stdout_lines()?;
        let search = async {
            while let Some(line) = lines.next().await {
                if predicate(&line) {
                    return Some(line);
                }
            }
            None // stdout closed before any match — readiness can't happen.
        };
        match tokio::time::timeout(within, search).await {
            Ok(Some(line)) => Ok(line),
            Ok(None) | Err(_) => Err(self.not_ready(within)),
        }
    }

    /// Wait until `check` (re-invoked every ~50 ms, first attempt immediate)
    /// returns `true`, or fail with [`Error::NotReady`] when `within` elapses —
    /// or immediately when the child exits first (a dead process never becomes
    /// ready).
    ///
    /// The check is any async predicate — an HTTP health endpoint, a file
    /// appearing, a database accepting connections. Piped stdout/stderr are
    /// background-drained and retained under the caller's
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) for the duration of the
    /// poll — like [`wait_for_line`](Self::wait_for_line), but the caller never
    /// sees the lines during the probe — so a child with a large startup burst
    /// can't stall in `write()` on a full OS pipe buffer (~64 KiB on Linux).
    /// Retention lets later consumption such as
    /// [`output_string`](Self::output_string) collect the same drained output;
    /// an existing [`stdout_lines`](Self::stdout_lines) /
    /// [`output_events`](Self::output_events) consumer also reads that sink. It
    /// still composes with [`wait`](Self::wait) and other later consumers that
    /// pick up the same background-drained sink — but *not* with
    /// [`output_bytes`](Self::output_bytes) or a fresh
    /// [`stdout_lines`](Self::stdout_lines) / [`output_events`](Self::output_events)
    /// afterward, exactly like calling `wait_for_line` first (see its
    /// "Consumes stdout" caveat). A failed probe does not kill the child. The
    /// deadline bounds the polling loop, not an in-flight check: a slow
    /// `check` future can overrun `within` by its own duration.
    ///
    /// `check` and its future are `Send` (matching
    /// [`wait_for_line`](Self::wait_for_line)'s predicate and
    /// [`Command::first_line`](crate::Command::first_line)'s), so the returned
    /// future is `Send` — it can cross a thread boundary on a multi-threaded
    /// runtime or be bridged onto another async runtime (e.g. a non-Rust binding
    /// that owns the handle), not only `.await`ed in place. The future still
    /// borrows `&mut self`, so spawning it standalone means moving the owned
    /// [`RunningProcess`] into the surrounding `async` block to make it `'static`.
    ///
    /// # Errors
    ///
    /// [`Error::NotReady`] when `within` elapses before `check` returns `true`,
    /// or immediately when the child exits first (a dead process never becomes
    /// ready). This is a *probe* deadline — distinct from [`Error::Timeout`]: a
    /// failed probe does not kill the child or touch its outcome.
    pub async fn wait_for<F, Fut>(&mut self, check: F, within: Duration) -> Result<()>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = bool> + Send,
    {
        self.poll_until(check, within).await
    }

    /// Wait until a TCP connection to `addr` is accepted, or fail with
    /// [`Error::NotReady`] when `within` elapses — or immediately when the
    /// child exits first.
    ///
    /// One connect attempt per ~50 ms tick (each attempt itself bounded so a
    /// stalled connect can't overrun the deadline); the probe connection is
    /// dropped as soon as it succeeds. Piped stdout/stderr are background-drained
    /// and retained under the caller's
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy), like
    /// [`wait_for`](Self::wait_for) — see its documentation for the same retention
    /// and composition semantics. A failed probe does not kill the child.
    ///
    /// # Errors
    ///
    /// [`Error::NotReady`] when `within` elapses before a connection to `addr` is
    /// accepted, or immediately when the child exits first. This is a *probe*
    /// deadline — distinct from [`Error::Timeout`]: a failed probe does not kill
    /// the child or touch its outcome.
    pub async fn wait_for_port(&mut self, addr: SocketAddr, within: Duration) -> Result<()> {
        // Clamp so a `Duration::MAX`-ish `within` can't overflow the deadline.
        let deadline = Instant::now() + within.min(crate::MAX_DEADLINE);
        self.poll_until(
            move || {
                let remaining = deadline.saturating_duration_since(Instant::now());
                async move {
                    // Clamp the attempt to the remaining budget; floor at 1ms so
                    // the final tick still makes a (brief) attempt.
                    let cap = CONNECT_ATTEMPT_CAP
                        .min(remaining)
                        .max(Duration::from_millis(1));
                    matches!(
                        tokio::time::timeout(cap, TcpStream::connect(addr)).await,
                        Ok(Ok(_))
                    )
                }
            },
            within,
        )
        .await
    }

    /// Re-run `check` on the readiness cadence until it passes, the child
    /// exits, or the deadline elapses.
    async fn poll_until<F, Fut>(&mut self, mut check: F, within: Duration) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        // Background-drain stdout (and, via the same call, stderr) so a child
        // that writes a large startup burst before becoming ready can't stall in
        // `write()` on a full OS pipe buffer (~64 KiB on Linux) while we poll —
        // the same pump `wait_for_line` uses, just without a foreground search:
        // nothing here ever pops a line back out, but `pump_lines_core` drains
        // the pipe into the sink regardless of whether anyone reads the sink, so
        // setting the pump up once is enough. Not arming the `Command::timeout`
        // watchdog matches every other probe. Errors are not this probe's
        // concern — stdout not piped, or already drained by an earlier
        // streaming call — either way there's nothing left to do here.
        let _ = self.drain_stdout_lines();

        // Clamp so a `Duration::MAX`-ish `within` can't overflow the deadline.
        let deadline = Instant::now() + within.min(crate::MAX_DEADLINE);
        loop {
            if check().await {
                return Ok(());
            }
            // An exited child can never become ready — fail fast rather than
            // burning the rest of the deadline. (A "couldn't tell" probe keeps
            // polling; the deadline still bounds us.)
            if self.has_exited_now() {
                return Err(self.not_ready(within));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(self.not_ready(within));
            }
            tokio::time::sleep(READINESS_POLL.min(remaining)).await;
        }
    }

    fn not_ready(&self, within: Duration) -> Error {
        Error::NotReady {
            program: self.program.clone(),
            timeout: within,
        }
    }
}

/// Compile-time proof that every readiness probe's future is `Send`. They must
/// cross a `tokio::spawn` / non-Rust-runtime bridge (e.g. a Python async
/// binding), which requires `Send + 'static` futures; a binding that re-derives
/// readiness in its own language is duplicating semantics this crate already
/// owns. Dropping a `+ Send` bound on a probe callback breaks the build *here*,
/// at the crate, rather than silently in a downstream consumer.
#[cfg(test)]
#[allow(dead_code)]
fn probe_futures_are_send(rp: &mut RunningProcess) {
    fn assert_send<T: Send>(_: &T) {}
    assert_send(&rp.wait_for_line(|line| line.is_empty(), Duration::ZERO));
    assert_send(&rp.wait_for(|| async { true }, Duration::ZERO));
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
    assert_send(&rp.wait_for_port(addr, Duration::ZERO));
}
