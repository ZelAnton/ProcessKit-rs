//! Incremental stdout streaming: [`StdoutLines`], [`ProcessEvents`], the
//! watchdog tasks that bound a streamed run (absolute timeout, output inactivity,
//! and cancellation), and the unified
//! `finish`.

use std::future::Future;
#[cfg(feature = "json")]
use std::marker::PhantomData;
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
/// [`events`](RunningProcess::events): how the run ended plus
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

impl Finished {
    /// Build a `Finished` from its fields — a `#[doc(hidden)]` insulated
    /// constructor for a wrapper/serialization layer to reconstruct a value
    /// directly, by the same "one insulated constructor instead of a struct
    /// literal" rationale as [`Error::exit`](crate::Error::exit) — `Finished`'s
    /// own `#[non_exhaustive]` already rejects a struct literal from outside
    /// this crate even though every field is `pub` (see the type's own doc for
    /// why: a future release may attach more fields without a breaking change).
    /// Off the documented surface, but `pub` so downstream code can call it;
    /// semver-covered like any public item.
    ///
    /// Mirrors every field, so a value round-trips through this constructor and
    /// reading the fields back byte-for-byte. No combination of these fields can
    /// be internally contradictory: `outcome` is this crate's own [`Outcome`],
    /// already mutually exclusive by construction (an exit code and a signal
    /// can never both be present), and `stderr`/`stderr_truncated` are
    /// independent telemetry with no cross-field invariant to violate.
    #[doc(hidden)]
    pub fn from_parts(outcome: Outcome, stderr: impl Into<String>, stderr_truncated: bool) -> Self {
        Finished {
            outcome,
            stderr: stderr.into(),
            stderr_truncated,
        }
    }
}

impl RunningProcess {
    /// Stream the child's standard output line by line. Call this **once**.
    ///
    /// Standard error is drained in the background under the caller's
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) (unbounded by default)
    /// and retained in [`Finished::stderr`] for inspection after
    /// [`finish`](Self::finish). With the default policy, stderr from a long-lived
    /// or chatty process grows in memory until `finish`; configure a bounded policy
    /// when that is a concern. The command's [`timeout`](crate::Command::timeout)
    /// bounds the stream: at the deadline the process tree is killed, pipes close,
    /// and a following
    /// [`finish`](Self::finish) reports [`Outcome::TimedOut`]
    /// even if the child caught the signal and exited cleanly within the grace.
    ///
    /// Returns `Err` instead of a silently-empty stream when stdout was not piped
    /// or a readiness/streaming call already started its one line pump.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when stdout was not piped, or a prior
    /// readiness or streaming call already started its line pump — returned
    /// instead of a stream that would silently be empty.
    pub fn stdout_lines(&mut self) -> Result<StdoutLines> {
        // Drain stdout AND arm the timeout watchdog. `wait_for_line` instead calls
        // `drain_stdout_lines` directly, so a readiness probe never kills the tree.
        let lines = self.drain_stdout_lines()?;
        self.arm_stream_deadline();
        Ok(lines)
    }

    /// Stream stdout as one deserialized JSON value per line. Call this once.
    ///
    /// The framing is strict NDJSON over ProcessKit's normalized text lines:
    /// every line, including an empty one, must independently deserialize as
    /// `T`. Each item is a [`Result<T>`](crate::Result); a malformed line yields
    /// [`ErrorReason::Parse`](crate::ErrorReason::Parse) and the stream continues
    /// with the next line. The error identifies the program, one-based NDJSON
    /// line and column plus zero-based byte offset in normalized stdout, and
    /// contains at most a 160-byte, control-escaped fragment of the offending line.
    ///
    /// Like [`stdout_lines`](Self::stdout_lines), creating this stream arms the
    /// command's timeout and leaves stderr draining in the background. Consume
    /// the stream, then call [`finish`](Self::finish) to observe the process
    /// outcome and stderr.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when stdout was not piped or a
    /// prior readiness/streaming call already consumed it. Per-line JSON errors
    /// are stream items, not construction errors. Available with the `json`
    /// feature.
    #[cfg(feature = "json")]
    pub fn stdout_json_lines<T>(&mut self) -> Result<JsonLines<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let program = self.program.clone();
        Ok(JsonLines {
            inner: self.stdout_lines()?,
            program,
            line_number: 0,
            byte_offset: 0,
            value: PhantomData,
        })
    }

    /// Set up the stdout and background stderr drains without arming the timeout
    /// watchdog. Shared by [`stdout_lines`](Self::stdout_lines) (which arms it)
    /// and `wait_for_line` (which must not, so a failed probe leaves the child
    /// running).
    pub(super) fn drain_stdout_lines(&mut self) -> Result<StdoutLines> {
        self.ensure_stdout_streamable()?;
        debug_assert!(
            self.stdout_sink.is_none(),
            "ensure_stdout_streamable rejects a previously consumed stdout stream"
        );
        // Background-drain stderr; `ensure_stderr_drain` is idempotent, retaining
        // its first sink/pump so `finish` can await the last line and return the
        // collected stderr.
        self.ensure_stderr_drain();

        let stdout_sink =
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
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
        self.stdout_sink = Some(stdout_sink.clone());

        Ok(StdoutLines {
            sink: stdout_sink,
            wait: None,
        })
    }

    /// Set up a one-shot stderr line stream for readiness probing while keeping
    /// stdout draining in the background. Like `drain_stdout_lines`, this does
    /// not arm the command timeout watchdog.
    pub(super) fn drain_stderr_lines(&mut self) -> Result<StdoutLines> {
        self.ensure_stderr_streamable()?;
        debug_assert!(
            self.stderr_sink.is_none(),
            "ensure_stderr_streamable rejects a previously consumed stderr stream"
        );
        self.ensure_stdout_drain();

        let stderr_sink =
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
        match self.backend.take_stderr_reader() {
            Some(pipe) => {
                self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_config.clone(),
                    stderr_sink.clone(),
                )));
            }
            None => stderr_sink.close_now(),
        }
        self.stderr_sink = Some(stderr_sink.clone());

        Ok(StdoutLines {
            sink: stderr_sink,
            wait: None,
        })
    }

    /// Start retaining stdout when another stream is handed to a readiness
    /// caller. Non-piped or already-draining stdout needs no action.
    fn ensure_stdout_drain(&mut self) {
        if !self.stdout_piped || self.stdout_sink.is_some() {
            return;
        }
        let stdout_sink =
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
        if let Some(pipe) = self.backend.take_stdout_reader() {
            self.stdout_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stdout_config.clone(),
                stdout_sink.clone(),
            )));
        } else {
            stdout_sink.close_now();
        }
        self.stdout_sink = Some(stdout_sink);
    }

    /// Background-drain stderr under the caller's
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) when it is piped and not
    /// already draining, retaining the sink/pump so [`finish`](Self::finish) can
    /// await the last line and return the collected stderr.
    ///
    /// Deliberately **independent of stdout**: it never consults stdout's
    /// streamability, so a readiness probe can keep stderr flowing even when
    /// stdout is `Inherit`/`Null` (and so has no drain of its own). Idempotent —
    /// a second call is a no-op, because overwriting the first sink/pump would
    /// discard already-drained stderr (and its overflow flag), leaving `finish`
    /// with empty stderr. A non-piped stderr installs an empty sink with no pump,
    /// matching what `finish` expects (empty stderr).
    fn ensure_stderr_drain(&mut self) {
        if self.stderr_sink.is_none() {
            let stderr_sink =
                SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
            if let Some(pipe) = self.backend.take_stderr_reader() {
                self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                    pipe,
                    self.stderr_config.clone(),
                    stderr_sink.clone(),
                )));
            } else {
                stderr_sink.close_now();
            }
            self.stderr_sink = Some(stderr_sink);
        }
    }

    /// Set up the background output drains a readiness probe needs, regardless of
    /// whether stdout is streamable.
    ///
    /// The stderr drain is the liveness-critical part and is armed
    /// unconditionally via `ensure_stderr_drain`: a child that writes a large
    /// startup burst to piped stderr before becoming ready must not stall in
    /// `write()` on a full OS pipe buffer (~64 KiB on Linux), and it must not do
    /// so even when stdout is `Inherit`/`Null`. The stdout drain is best-effort —
    /// `drain_stdout_lines` sets it up only for piped, not-yet-consumed stdout,
    /// and otherwise returns an `Err` that is deliberately ignored here: unlike
    /// `wait_for_line`, a probe never hands stdout back, so a non-streamable
    /// stdout is not its concern. This leaves the `stdout_lines` / `wait_for_line`
    /// contract untouched — neither changes behavior; only the probe now drains
    /// stderr independently of stdout.
    pub(super) fn ensure_background_drains(&mut self) {
        // stderr first and on its own, so it can't hinge on stdout being
        // streamable: the bug this fixes was `drain_stdout_lines` bailing on a
        // non-piped stdout (via `ensure_stdout_streamable`) before ever reaching
        // the stderr pump.
        self.ensure_stderr_drain();
        // stdout is best-effort: only piped, not-yet-consumed stdout gets a pump;
        // the `Err` for a non-piped/consumed stdout is not this probe's concern
        // (`ensure_stderr_drain` above already made stderr's drain idempotent, so
        // the redundant call inside `drain_stdout_lines` is a no-op).
        let _ = self.drain_stdout_lines();
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
                // Anchor to spawn time so a late stream call can't re-grant the
                // full limit — on `deadline_anchor` (tokio's clock), so virtual
                // time a prior probe burned is charged against the limit here too.
                let started = self.deadline_anchor;
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
        self.arm_stream_inactivity();
        self.arm_scripted_inactivity();
    }

    /// Arm the resettable stdout/stderr inactivity watchdog for a real streamed
    /// run. It shares the absolute deadline's arbiter and teardown policy, so a
    /// simultaneous deadline/reap/inactivity event has one winning cause.
    fn arm_stream_inactivity(&mut self) {
        if self.inactivity_task.is_some() {
            return;
        }
        let Some(limit) = self.inactivity_timeout else {
            return;
        };
        let own_group = self.backend.own_group().map(Arc::downgrade);
        let pid = self.pid;
        if own_group.is_none() && pid.is_none() {
            return; // scripted is armed by `arm_scripted_inactivity`
        }
        let grace = self.timeout_grace;
        let signal = self.timeout_signal;
        let activity = self.output_activity.clone();
        let timeout_state = self.timeout_state.clone();
        let gate = self.pid_gate.clone();
        self.inactivity_task = Some(tokio::spawn(async move {
            activity.wait_for_inactivity(limit).await;
            if !super::deadline::claim_inactivity_timed_out(&timeout_state) {
                return;
            }
            if gate.is_retired() {
                return;
            }
            match own_group {
                Some(group) => match grace {
                    Some(grace) => match group.upgrade() {
                        Some(group) => {
                            let _ = group.graceful_terminate(grace, signal).await;
                        }
                        None => force_kill(&gate),
                    },
                    None => kill_via_weak(&group, &gate),
                },
                None => match grace {
                    Some(grace) => spawn_graceful_kill_and_reap(gate, grace, signal),
                    None => force_kill(&gate),
                },
            }
        }));
    }

    /// Finish a streamed run: wait for exit and return a [`Finished`]
    /// carrying the [`Outcome`] and the stderr collected in the background by
    /// [`stdout_lines`](Self::stdout_lines). If a bounded
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) silently dropped stderr
    /// lines, [`Finished::stderr_truncated`] is `true` — the streamed stdout
    /// itself is the caller's own responsibility and is not tracked here.
    ///
    /// A run killed by its [`timeout`](crate::Command::timeout) reports
    /// [`Outcome::TimedOut`], even if the child caught
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
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) (the run was cancelled via
    /// [`Command::cancel_on`](crate::Command::cancel_on)),
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud
    /// [`OutputBufferPolicy`](crate::OutputBufferPolicy) overflowed on a sink the
    /// caller opted into by streaming — a bare `finish` discards untouched stdout
    /// and never trips it), [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) (a
    /// non-broken-pipe stdin-source failure on an otherwise-successful run), or
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) (waiting on the child failed).
    pub async fn finish(mut self) -> Result<Finished> {
        // A bare `finish()` (no prior `stdout_lines`/`events` took stdout)
        // still has to drain the leftover stdout so the child can't block on a full
        // pipe — but the caller never asked to capture it. Pump it through the
        // internal retain-nothing discard sink (not the user's `OutputBufferPolicy`):
        // lines nobody will read aren't retained, and a `fail_loud` policy doesn't
        // raise `OutputTooLarge` for output the caller didn't request — matching
        // `wait()`. When stdout was already streamed, the reader is gone and the
        // stream's user-policy sink stays in place (and is overflow-checked below).
        let mut stdout_discarded = false;
        if let Some(pipe) = self.backend.take_stdout_reader() {
            let sink = SharedLines::new_with_activity(
                &super::discard_sink_policy(),
                self.output_activity.clone(),
            );
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
            let sink = SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
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
                return Err(crate::ErrorReason::OutputTooLarge {
                    program: self.program.clone(),
                    max_lines: self.buffer.max_lines,
                    max_bytes: self.buffer.max_bytes,
                    total_lines: sink.count(),
                    total_bytes: sink.seen_bytes(),
                }
                .into());
            }
        }
        // A first OS read error on either pipe means an incomplete capture:
        // surface it as `ErrorReason::Io` rather than a silently-short "success". Unlike
        // the overflow check above (skipped for a bare finish's discarded stdout,
        // which the caller never asked to capture), a read error is an OS-level
        // failure of the child's pipe, so it is surfaced for the streamed and the
        // bare-finish (discarded) stdout alike — matching `wait`. A broken-pipe
        // read was already folded into a clean EOF by the pump, so a normal
        // writer-closed stream never trips this.
        for sink in [self.stdout_sink.as_ref(), self.stderr_sink.as_ref()]
            .into_iter()
            .flatten()
        {
            if let Some(source) = sink.take_read_error() {
                return Err(crate::Error::io(source));
            }
        }
        // `dropped()` = lines the buffer policy discarded from the background
        // stderr drain, not lines this call itself failed to observe — matches
        // the same signal `output_string`/`output_bytes` derive `truncated` from.
        // (A flag read, safe to take while a live `events()` stream still pops.)
        let stderr_truncated = self.stderr_sink.as_ref().is_some_and(|s| s.dropped() > 0);
        // A live `events()` stream delivers stderr to the caller as
        // `ProcessEvent::Stderr`; do NOT also drain it here, which would race that
        // stream for the lines. `Finished::stderr` is empty by design for an
        // events run (the caller already has stderr as events).
        let stderr = if self.merged_events_stream {
            String::new()
        } else {
            self.stderr_sink
                .as_ref()
                .map(|sink| sink.drain().join("\n"))
                .unwrap_or_default()
        };
        Ok(Finished {
            outcome,
            stderr,
            stderr_truncated,
        })
    }

    /// Stream the child's full lifecycle as a single ordered sequence of
    /// [`ProcessEvent`]s: [`Started`](ProcessEvent::Started) first, then stdout
    /// and stderr lines interleaved, then [`Exited`](ProcessEvent::Exited). Call
    /// this **once**.
    ///
    /// Line interleaving is best-effort; the two streams are polled **fairly** so
    /// a continuously-ready stream can't starve the other. Returns `Err` instead
    /// of a silently-empty stream when stdout was not piped or was already
    /// consumed.
    ///
    /// **Drive it alongside a consuming finisher.** The terminal
    /// [`Exited`](ProcessEvent::Exited) is delivered when the run is reaped, which
    /// is what [`finish`](Self::finish) / [`wait`](Self::wait) do — so poll this
    /// stream and that finisher **together** (e.g. with `tokio::join!`) rather
    /// than draining the stream to completion first, which would park waiting for
    /// an `Exited` no one is producing. After the stream ends,
    /// [`finish`](Self::finish)'s `stderr` is empty since stderr was delivered as
    /// events.
    ///
    /// ```no_run
    /// use processkit::prelude::StreamExt;
    /// use processkit::{Command, ProcessEvent};
    ///
    /// # async fn run() -> processkit::Result<()> {
    /// let mut run = Command::new("build").start().await?;
    /// let mut events = run.events()?;
    /// let render = async {
    ///     while let Some(ev) = events.next().await {
    ///         match ev {
    ///             ProcessEvent::Started { pid } => eprintln!("started {pid:?}"),
    ///             ProcessEvent::Stdout(l) => println!("{}", l.text()),
    ///             ProcessEvent::Stderr(l) => eprintln!("{}", l.text()),
    ///             ProcessEvent::Exited(outcome) => eprintln!("exited {outcome:?}"),
    ///             _ => {}
    ///         }
    ///     }
    /// };
    /// let (_, finished) = tokio::join!(render, run.finish());
    /// finished?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when stdout was not piped, or a prior
    /// readiness/streaming call already started its line pump — returned instead
    /// of a stream that would silently be empty.
    pub fn events(&mut self) -> Result<ProcessEvents> {
        self.ensure_stdout_streamable()?;
        debug_assert!(
            self.stdout_sink.is_none(),
            "ensure_stdout_streamable rejects a previously consumed stdout stream"
        );
        debug_assert!(
            self.stderr_sink.is_none(),
            "a public output stream consumes stdout before it can arm stderr"
        );
        let stdout_sink =
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
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
        self.stdout_sink = Some(stdout_sink.clone());

        let stderr_sink =
            SharedLines::new_with_activity(&self.buffer, self.output_activity.clone());
        if let Some(pipe) = self.backend.take_stderr_reader() {
            self.stderr_pump = Some(tokio::spawn(pump_lines_core(
                pipe,
                self.stderr_config.clone(),
                stderr_sink.clone(),
            )));
        } else {
            stderr_sink.close_now();
        }
        self.stderr_sink = Some(stderr_sink.clone());

        self.arm_stream_deadline();

        // Hand the run's reap (in `on_reaped`) a channel to publish the outcome on
        // so the stream can emit the terminal `Exited`, and mark that stderr is
        // being delivered as events so `finish` doesn't also drain it into
        // `Finished` (racing this live stream for the lines).
        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        self.exit_event_tx = Some(exit_tx);
        self.merged_events_stream = true;

        Ok(ProcessEvents {
            stdout_sink,
            stderr_sink,
            stdout_wait: None,
            stderr_wait: None,
            stdout_done: false,
            stderr_done: false,
            prefer_stdout: true,
            start_pid: self.pid,
            started_emitted: false,
            exit_rx: Some(exit_rx),
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
        // (src/sys/linux.rs) for the full rationale. ~100ms is the ceiling for
        // every backend: FreeBSD's reaper keeps its post-kill corpse drain in
        // `Drop` alone (see `DRAIN_BUDGET`, src/sys/freebsd.rs), so this call
        // does not block there at all.
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
/// `crate::sys::graceful::run_pid`, which documents the load-bearing
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
/// Drop-triggered kill+reap of a recycled pid.
///
/// The graceful **cancellation** watchdog (`RunningProcess::arm_cancel_watchdog`,
/// with `Command::cancel_grace`) reaches the identical situation — its task is
/// aborted by the same `Drop`, and a signal-catching child can end the stream the
/// same way — so it arms this same detached primitive rather than forking a second
/// one, and `Drop`'s child hand-off covers its shape too (there keyed on the
/// *fired* token, since a merely configured `cancel_grace` arms nothing).
///
/// The reap that frees the pid is **never** left to tokio's orphan reaper (which
/// would free it without retiring the gate, leaving this task free to probe or
/// SIGKILL whatever recycled it). It is owned by whoever owns the `Child`: a
/// consuming finisher (which retires the gate and reaps through the owned
/// `Child`), or — when the streaming consumer instead drops its handle mid-grace —
/// the detached gated reaper `RunningProcess::Drop` hands the child to
/// (`gated_reap_and_retire`), which reaps it under the gate and retires
/// atomically. Either way the shared [`PidGate`] is the [`graceful_kill_pid`]
/// stand-down: once the pid's owner retires it, this task's liveness poll reports
/// "gone", its `SIGKILL` is suppressed, and it can never fire on a recycled pid.
pub(super) fn spawn_graceful_kill_and_reap(
    gate: Arc<PidGate>,
    grace: std::time::Duration,
    signal: i32,
) {
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

/// A typed NDJSON stream returned by
/// [`RunningProcess::stdout_json_lines`].
///
/// Each item corresponds to exactly one normalized stdout line. Malformed
/// items are returned as [`ErrorReason::Parse`](crate::ErrorReason::Parse) and
/// do not terminate the stream.
#[cfg(feature = "json")]
pub struct JsonLines<T> {
    inner: StdoutLines,
    program: String,
    line_number: usize,
    byte_offset: usize,
    value: PhantomData<fn() -> T>,
}

#[cfg(feature = "json")]
impl<T> std::fmt::Debug for JsonLines<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonLines")
            .field("program", &self.program)
            .field("line_number", &self.line_number)
            .field("byte_offset", &self.byte_offset)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "json")]
impl<T> Stream for JsonLines<T>
where
    T: serde::de::DeserializeOwned,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(line)) => {
                this.line_number = this.line_number.saturating_add(1);
                let line_offset = this.byte_offset;
                this.byte_offset = this
                    .byte_offset
                    .saturating_add(line.len())
                    .saturating_add(1);
                Poll::Ready(Some(crate::json::decode_line(
                    &this.program,
                    this.line_number,
                    line_offset,
                    &line,
                )))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A lifecycle event produced by a running child process, yielded by
/// [`RunningProcess::events`], which merges the process's lifecycle transitions
/// and its two output streams into a **single ordered sequence**:
///
/// [`Started`](Self::Started) → interleaved [`Stdout`](Self::Stdout) /
/// [`Stderr`](Self::Stderr) lines → [`Exited`](Self::Exited).
///
/// This is the crate's one asynchronous source of process events: a TUI,
/// dashboard, or orchestrator gets `started`, every output line, and the final
/// exit outcome from one stream instead of stitching them together from separate
/// channels.
///
/// # Scope
///
/// This enum currently carries **`Started`, the output lines, and `Exited`** —
/// the facts the running layer observes directly. It deliberately does **not**
/// (yet) carry the graceful-teardown transitions (`soft_signal` →
/// `grace_started` → `drained` / `escalated` / `spared`): with no per-call
/// programmatic consumer, threading a live sink for them through the teardown
/// backends would be blast radius in concurrency-sensitive code for a
/// speculative surface. Those live facts remain available on the `tracing` seam,
/// and as a typed value after the fact via
/// `ShutdownReport`. Because this enum is
/// `#[non_exhaustive]`, those phases (and any other kind) can be added here
/// **additively** later, once a consumer needs them, without a breaking change.
///
/// `#[non_exhaustive]`: a future release may add another kind of event without a
/// breaking change, so a `match` on `ProcessEvent` needs a `_` arm. Each
/// per-stream line is an [`OutputLine`] (rather than a bare `String`) so per-line
/// metadata can be attached there without a breaking change either.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProcessEvent {
    /// The process has started — the **first** event of the stream, emitted as
    /// soon as the pid is known, before any output line. `pid` is `None` for a
    /// scripted double (which owns no OS process) or a platform that could not
    /// report one.
    Started {
        /// The child's OS process id, mirroring [`RunningProcess::pid`] at spawn.
        pid: Option<u32>,
    },
    /// A line from the child's standard output.
    Stdout(OutputLine),
    /// A line from the child's standard error.
    Stderr(OutputLine),
    /// The process has exited — the **last** event of the stream, emitted once
    /// the child is reaped, carrying the run's [`Outcome`]
    /// ([`Exited`](Outcome::Exited) / [`Signalled`](Outcome::Signalled) /
    /// [`TimedOut`](Outcome::TimedOut)) — the same value
    /// [`Finished::outcome`](crate::Finished) reports, not a parallel type. It is
    /// delivered by the consuming finisher ([`finish`](RunningProcess::finish) /
    /// [`wait`](RunningProcess::wait)) reaping the child, so drive the stream and
    /// that finisher together (see [`events`](RunningProcess::events)).
    Exited(Outcome),
}

impl ProcessEvent {
    /// A stable, snake_case identifier for this event's kind
    /// (`"started"` / `"stdout"` / `"stderr"` / `"exited"`), by the same
    /// convention as [`Outcome::name`](crate::Outcome::name) — suitable as a
    /// serde tag or a log field that survives a `Debug`-format change.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            ProcessEvent::Started { .. } => "started",
            ProcessEvent::Stdout(_) => "stdout",
            ProcessEvent::Stderr(_) => "stderr",
            ProcessEvent::Exited(_) => "exited",
        }
    }

    /// The decoded text of this event's line, if it carries one. `Some` for
    /// [`Stdout`](ProcessEvent::Stdout)/[`Stderr`](ProcessEvent::Stderr); `None`
    /// for the non-line lifecycle events
    /// [`Started`](ProcessEvent::Started)/[`Exited`](ProcessEvent::Exited).
    /// Convenience for consumers that don't care which stream a line came from.
    pub fn text(&self) -> Option<&str> {
        match self {
            ProcessEvent::Stdout(line) | ProcessEvent::Stderr(line) => Some(line.text()),
            ProcessEvent::Started { .. } | ProcessEvent::Exited(_) => None,
        }
    }
}

/// One decoded line carried by a [`ProcessEvent`].
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

/// A `Stream` of a running child's [`ProcessEvent`]s (see
/// [`RunningProcess::events`]).
///
/// [`Started`](ProcessEvent::Started) is yielded first, then stdout and stderr
/// lines interleaved in the order they arrive at the async runtime, and finally
/// [`Exited`](ProcessEvent::Exited) once the run is reaped by its consuming
/// finisher.
pub struct ProcessEvents {
    stdout_sink: Arc<SharedLines>,
    stderr_sink: Arc<SharedLines>,
    stdout_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    stderr_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    stdout_done: bool,
    stderr_done: bool,
    /// Which stream gets the first look each poll. Flipped after every emitted
    /// line so a continuously-ready stream can't starve the other.
    prefer_stdout: bool,
    /// The pid captured at [`events`](RunningProcess::events) time, carried in
    /// the leading [`Started`](ProcessEvent::Started) event.
    start_pid: Option<u32>,
    /// Whether the leading [`Started`](ProcessEvent::Started) has been emitted.
    started_emitted: bool,
    /// The channel the consuming finisher publishes the run's [`Outcome`] on when
    /// it reaps the child; drained once, after both output streams close, into
    /// the terminal [`Exited`](ProcessEvent::Exited). `None` once that event has
    /// been emitted (or the sender was dropped without a reap — a handle dropped
    /// without being finished — ending the stream without an `Exited`).
    exit_rx: Option<tokio::sync::oneshot::Receiver<Outcome>>,
}

impl std::fmt::Debug for ProcessEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessEvents").finish_non_exhaustive()
    }
}

impl Stream for ProcessEvents {
    type Item = ProcessEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<ProcessEvent>> {
        let this = self.get_mut();
        // `Started` leads the stream, before any output is polled.
        if !this.started_emitted {
            this.started_emitted = true;
            return Poll::Ready(Some(ProcessEvent::Started {
                pid: this.start_pid,
            }));
        }
        loop {
            for stdout_turn in [this.prefer_stdout, !this.prefer_stdout] {
                if stdout_turn && !this.stdout_done {
                    match this.stdout_sink.try_pop() {
                        Popped::Line(line) => {
                            this.stdout_wait = None;
                            this.prefer_stdout = false; // stderr gets the next first look
                            return Poll::Ready(Some(ProcessEvent::Stdout(OutputLine {
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
                            return Poll::Ready(Some(ProcessEvent::Stderr(OutputLine {
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
                // Both output streams are drained: the child has exited (its
                // pipes are EOF). Deliver the terminal `Exited` once the run's
                // consuming finisher reaps it and publishes the outcome, then end.
                return match this.exit_rx.as_mut() {
                    Some(rx) => match Pin::new(rx).poll(cx) {
                        Poll::Ready(Ok(outcome)) => {
                            this.exit_rx = None;
                            Poll::Ready(Some(ProcessEvent::Exited(outcome)))
                        }
                        // Sender dropped without a reap (the handle was dropped
                        // rather than finished): end without an `Exited`.
                        Poll::Ready(Err(_)) => {
                            this.exit_rx = None;
                            Poll::Ready(None)
                        }
                        Poll::Pending => Poll::Pending,
                    },
                    None => Poll::Ready(None),
                };
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

    /// T-087: after a streamed stdout hit an OS read error mid-stream, the
    /// streaming consumer wakes and ends (the sink closes), and the consuming
    /// `finish` reports the cause as `ErrorReason::Io` — not a silently-short success.
    /// The recorded error stands in for one a real pump would set (the pump seam
    /// is covered in `pump.rs`).
    #[tokio::test]
    async fn finish_surfaces_a_recorded_stream_read_error_as_io() {
        use crate::command::Command;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok(""))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");
        // Stream stdout (takes the reader, installs the stdout sink) then drop the
        // consumer, as `first_line`-style code does once the stream ends.
        drop(run.stdout_lines().expect("stdout_lines"));
        run.stdout_sink
            .as_ref()
            .expect("streaming installed the stdout sink")
            .set_read_error(std::io::Error::other("stream read boom"));
        match run.finish().await.map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Io(e)) => assert_eq!(e.to_string(), "stream read boom"),
            other => panic!("expected Err(Io) for an incomplete streamed capture, got {other:?}"),
        }
    }

    #[cfg(feature = "json")]
    #[tokio::test]
    async fn json_lines_reports_a_bounded_item_error_and_continues() {
        use crate::command::Command;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct Row {
            value: u32,
        }

        let bad = format!(
            "{{\"padding\":\"{}\",\"value\":nope}}",
            "secret".repeat(100)
        );
        let output = format!("{{\"value\":1}}\n{bad}\n{{\"value\":3}}");
        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok(output))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");
        let mut rows = run.stdout_json_lines::<Row>().expect("JSON stream");

        assert_eq!(
            rows.next().await.expect("row 1").expect("valid row"),
            Row { value: 1 }
        );
        let error = rows
            .next()
            .await
            .expect("row 2")
            .expect_err("malformed row");
        let crate::ErrorReason::Parse { program, message } = error.reason() else {
            panic!("expected Parse, got {error:?}");
        };
        assert_eq!(program, "tool");
        assert!(message.contains("NDJSON decode failed at line 2"));
        assert!(message.contains("byte offset"));
        assert!(message.contains("fragment `…"));
        assert!(!message.contains(&"secret".repeat(50)));
        assert_eq!(
            rows.next().await.expect("row 3").expect("valid row"),
            Row { value: 3 }
        );
        assert!(rows.next().await.is_none());
        drop(rows);

        let finished = run.finish().await.expect("finish");
        assert_eq!(finished.outcome, Outcome::Exited(0));
    }

    /// A `ProcessEvents` built with `started_emitted: true` / `exit_rx: None`
    /// isolates the stdout/stderr line-merging behavior (the leading `Started`
    /// and trailing `Exited` are exercised end to end in the scripted tests).
    fn line_only_events(
        stdout_sink: Arc<SharedLines>,
        stderr_sink: Arc<SharedLines>,
    ) -> ProcessEvents {
        ProcessEvents {
            stdout_sink,
            stderr_sink,
            stdout_wait: None,
            stderr_wait: None,
            stdout_done: false,
            stderr_done: false,
            prefer_stdout: true,
            start_pid: None,
            started_emitted: true,
            exit_rx: None,
        }
    }

    #[tokio::test]
    async fn events_interleave_fairly_between_two_ready_streams() {
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

        let mut events = line_only_events(stdout_sink, stderr_sink);
        let mut seq = Vec::new();
        while let Some(ev) = events.next().await {
            seq.push(match ev {
                ProcessEvent::Stdout(l) => format!("O:{}", l.text()),
                ProcessEvent::Stderr(l) => format!("E:{}", l.text()),
                other => panic!("unexpected event: {other:?}"),
            });
        }
        assert_eq!(
            seq,
            ["O:o1", "E:e1", "O:o2", "E:e2", "O:o3", "E:e3"],
            "merged stream must interleave, not drain stdout first"
        );
    }

    #[tokio::test]
    async fn process_event_carries_an_output_line_with_a_text_accessor() {
        let policy = OutputBufferPolicy::unbounded();
        let stdout_sink = SharedLines::new(&policy);
        let stderr_sink = SharedLines::new(&policy);
        stdout_sink.push("out".to_owned());
        stderr_sink.push("err".to_owned());
        stdout_sink.close_now();
        stderr_sink.close_now();
        let mut events = line_only_events(stdout_sink, stderr_sink);
        let first = events.next().await.expect("a stdout event");
        assert!(
            matches!(&first, ProcessEvent::Stdout(line) if line.text() == "out"),
            "stdout event carries an OutputLine: {first:?}"
        );
        assert_eq!(first.text(), Some("out"), "text() reads the line");
        let second = events.next().await.expect("a stderr event");
        assert!(matches!(&second, ProcessEvent::Stderr(line) if line.text() == "err"));
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
    async fn events_lose_no_line_under_two_racing_producers() {
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
        let mut events = line_only_events(stdout_sink, stderr_sink);
        let consume = async {
            let (mut out, mut err) = (0usize, 0usize);
            while let Some(ev) = events.next().await {
                match ev {
                    ProcessEvent::Stdout(_) => out += 1,
                    ProcessEvent::Stderr(_) => err += 1,
                    _ => {}
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

    /// T-184: the full lifecycle stream over the scripted backend (the
    /// two-stream / `Piped` shape) — `Started` leads, both stdout lines and the
    /// stderr line arrive as events, and the terminal `Exited` carries the run's
    /// `Outcome`. Driven the documented way: the events stream and the consuming
    /// `finish` polled together.
    #[tokio::test]
    async fn events_stream_emits_started_lines_and_exited() {
        use crate::command::Command;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok("out-1\nout-2").with_stderr("err-1"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");

        let mut events = run.events().expect("events");
        let collect = async {
            let mut names = Vec::new();
            let mut outcome = None;
            while let Some(ev) = events.next().await {
                names.push(ev.name());
                if let ProcessEvent::Exited(o) = ev {
                    outcome = Some(o);
                }
            }
            (names, outcome)
        };
        let ((names, outcome), finished) = tokio::join!(collect, run.finish());
        let finished = finished.expect("finish");

        assert_eq!(names.first(), Some(&"started"), "Started leads the stream");
        assert_eq!(names.last(), Some(&"exited"), "Exited ends the stream");
        assert_eq!(
            names.iter().filter(|n| **n == "stdout").count(),
            2,
            "both stdout lines arrive as events"
        );
        assert_eq!(
            names.iter().filter(|n| **n == "stderr").count(),
            1,
            "the stderr line arrives as an event"
        );
        assert_eq!(
            outcome,
            Some(Outcome::Exited(0)),
            "Exited carries the run's outcome"
        );
        assert_eq!(finished.outcome, Outcome::Exited(0));
        assert!(
            finished.stderr.is_empty(),
            "stderr was delivered as events, so Finished::stderr is empty"
        );
    }

    /// T-184: the same lifecycle stream over a single, merged output stream (the
    /// `Backend::Pty` shape, modeled here by a scripted reply with no stderr):
    /// `Started`, the stdout lines, then `Exited`, with no stderr events. The
    /// `events` state machine is backend-agnostic, so this is the merged-pump
    /// parity of the two-stream case above.
    #[tokio::test]
    async fn events_stream_over_a_merged_single_stream() {
        use crate::command::Command;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let mut run = ScriptedRunner::new()
            .fallback(Reply::ok("merged-1\nmerged-2\nmerged-3"))
            .start(&Command::new("tool"))
            .await
            .expect("scripted start");

        let mut events = run.events().expect("events");
        let collect = async {
            let mut seq = Vec::new();
            while let Some(ev) = events.next().await {
                match ev {
                    ProcessEvent::Started { pid } => seq.push(format!("started:{pid:?}")),
                    ProcessEvent::Stdout(l) => seq.push(format!("out:{}", l.text())),
                    ProcessEvent::Stderr(l) => seq.push(format!("err:{}", l.text())),
                    ProcessEvent::Exited(o) => seq.push(format!("exited:{}", o.name())),
                }
            }
            seq
        };
        let (seq, finished) = tokio::join!(collect, run.finish());
        let _ = finished.expect("finish");

        assert_eq!(
            seq,
            [
                "started:None",
                "out:merged-1",
                "out:merged-2",
                "out:merged-3",
                "exited:exited",
            ],
            "Started → stdout lines → Exited, with no stderr events on a merged stream"
        );
    }

    /// T-179: a `Finished` built by the `#[doc(hidden)]` `from_parts`
    /// constructor and read back through its (public) fields reproduces the
    /// original, field for field.
    #[test]
    fn finished_from_parts_round_trips_every_field() {
        let original = Finished::from_parts(Outcome::Signalled(Some(9)), "boom", true);
        assert_eq!(original.outcome, Outcome::Signalled(Some(9)));
        assert_eq!(original.stderr, "boom");
        assert!(original.stderr_truncated);

        let rebuilt = Finished::from_parts(
            original.outcome,
            original.stderr.clone(),
            original.stderr_truncated,
        );
        assert_eq!(original, rebuilt);
    }
}
