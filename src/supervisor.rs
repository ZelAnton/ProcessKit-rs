//! [`Supervisor`] — keep a child alive with policy-driven restarts and backoff.
//!
//! [`Command::retry`](crate::Command::retry) / [`retry_with`](crate::Command::retry_with)
//! (and the client-wide [`CliClient::default_retry`](crate::CliClient::default_retry))
//! answer "run this once, replaying on failure" on a
//! [`RetryPolicy`](crate::RetryPolicy). A supervisor answers the different
//! question **"keep this alive"**: restart a child whenever it exits (unless its
//! exit satisfies the policy or a predicate), with bounded restarts and
//! exponential backoff plus jitter — a minimal `runit`/`systemd`-style keeper on
//! top of the runner layer. Its [`RestartPolicy`](crate::RestartPolicy) is the
//! keep-alive twin of that `RetryPolicy`.
//!
//! Built entirely on the [`ProcessRunner`] seam, so supervision logic is
//! hermetically testable with the crate's doubles, and
//! [`with_runner(&group)`](Supervisor::with_runner) runs every incarnation
//! inside one shared kill-on-drop [`ProcessGroup`](crate::ProcessGroup).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::buffer::OutputBufferPolicy;
use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::{Outcome, ProcessResult};
use crate::runner::{JobRunner, ProcessRunner};

/// Default per-incarnation capture tail for a supervised command whose own
/// policy is unbounded. A supervised process can be long-lived and chatty, so
/// capturing its *entire* output risks unbounded heap — keep a bounded tail (the
/// most recent lines, the ones that matter for a crash) by default instead.
const DEFAULT_SUPERVISION_TAIL: usize = 1000;

/// Default number of *consecutive* failed liveness checks tolerated before the
/// supervisor force-restarts the current incarnation (see
/// [`Supervisor::health_check`] / [`Supervisor::health_check_failures`]).
/// Mirrors the Kubernetes container liveness-probe `failureThreshold` default of
/// `3`: a single blip (a slow tick, a momentarily-busy endpoint) is forgiven, a
/// genuinely wedged child is not. No effect unless
/// [`health_check`](Supervisor::health_check) is enabled.
const DEFAULT_HEALTH_FAILURES: u32 = 3;

/// Floor for a [`health_check`](Supervisor::health_check) probe `interval`.
/// `tokio::time::sleep(Duration::ZERO)` resolves immediately, so a zero (or
/// otherwise degenerate) interval would turn `HealthCheck::watch`'s loop into a
/// busy `sleep(0) -> probe()` hot-loop and silently void the documented
/// startup-grace promise (first probe one `interval` after the incarnation
/// starts). Clamp rather than make [`health_check`](Supervisor::health_check)
/// fallible — mirrors `StatsSampler::new`'s clamp in `src/stats.rs`.
const MIN_HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(1);

/// A boxed async liveness probe: called with no arguments (like
/// [`RunningProcess::wait_for`](crate::RunningProcess::wait_for)'s `check`) and
/// resolving to `true` when the child is healthy. Boxed — probe *and* its future
/// — so the [`Supervisor`] can store an arbitrary closure/endpoint check as one
/// opaque field, the async twin of the boxed `stop_when`/`give_up_when`
/// predicates.
type HealthProbe = Box<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// A liveness health check: an async probe re-run on a fixed cadence for the
/// life of the current incarnation. Configured by
/// [`Supervisor::health_check`]; the consecutive-failure threshold lives
/// separately on the supervisor ([`health_check_failures`](Supervisor::health_check_failures))
/// so it can be set in either order.
struct HealthCheck {
    probe: HealthProbe,
    interval: Duration,
}

impl HealthCheck {
    /// Poll the probe on the configured cadence until it fails
    /// `failures_before_unhealthy` times **in a row** — then resolve, signalling
    /// that the incarnation is wedged and must be force-restarted. Any healthy
    /// probe resets the streak, so only a *sustained* failure trips it.
    ///
    /// The first probe fires one `interval` *after* the incarnation starts (not
    /// immediately, unlike the one-shot readiness [`wait_for`](crate::RunningProcess::wait_for)),
    /// giving a booting child that grace before liveness is judged; a healthy
    /// service then loops here for its whole lifetime and this future never
    /// resolves. A slow probe stretches the effective cadence (the period is
    /// `interval` *plus* the probe's own runtime) rather than overlapping checks.
    /// `self.interval` is already clamped to a safe minimum by the
    /// [`health_check`](Supervisor::health_check) builder, so this loop never
    /// degenerates into a `sleep(0)` busy-spin even for a caller-supplied zero
    /// interval.
    async fn watch(&self, failures_before_unhealthy: u32) {
        // A zero threshold would never trip (`consecutive >= 0` can't be the
        // *strict* streak we want); clamp to "one failed probe kills".
        let threshold = failures_before_unhealthy.max(1);
        let mut consecutive: u32 = 0;
        loop {
            tokio::time::sleep(self.interval).await;
            if (self.probe)().await {
                consecutive = 0;
            } else {
                consecutive = consecutive.saturating_add(1);
                if consecutive >= threshold {
                    return;
                }
            }
        }
    }
}

/// One incarnation's end, as seen by [`Supervisor::run_incarnation`]: either it
/// ran to a natural conclusion (exit / crash / spawn failure — a
/// [`ProcessResult`] or an [`Error`](crate::Error)), or a liveness
/// [`health_check`](Supervisor::health_check) judged it wedged and forced it
/// down.
enum Incarnation {
    /// The runner produced a completed result or a spawn/IO error.
    Ran(Result<ProcessResult<String>>),
    /// A liveness check tripped; the in-flight run was abandoned (killed on drop
    /// under the default [`JobRunner`]). Carries the incarnation's uptime so the
    /// backoff escalation can treat a long-lived-then-wedged child as healthy.
    LivenessFailed { uptime: Duration },
}

/// What the supervision loop should do after a restart-eligible incarnation
/// (a real crash, a clean `Always` restart, or a liveness kill), decided by
/// [`Supervisor::gate_restart`].
enum GateOutcome {
    /// Restart — the backoff (and any storm pause) has already been awaited.
    Restart,
    /// [`give_up_when`](Supervisor::give_up_when) classified the crash as
    /// permanent — stop with [`StopReason::GaveUp`].
    GaveUp,
    /// The [`max_restarts`](Supervisor::max_restarts) budget is spent — stop
    /// with [`StopReason::RestartsExhausted`].
    Exhausted,
    /// A cancel token fired during the backoff/storm pause — end supervision
    /// with `ErrorReason::Cancelled`.
    Cancelled,
    /// A [`SupervisionSession::stop`] fired during the backoff/storm pause — end
    /// supervision with [`StopReason::Stopped`], launching no further incarnation.
    Stopped,
}

/// The capture policy to apply to each incarnation: respect an explicit
/// bounded/fail-loud command policy, but bound an unbounded line count to a
/// tail. Only the line cap is filled in — the overflow *mode* and any byte cap
/// the command set are preserved, so an unbounded `Error` ("fail loud") command
/// stays fail-loud rather than silently switching to `DropOldest`, and a
/// byte-capped command keeps its memory bound.
fn default_supervision_capture(command: &Command) -> OutputBufferPolicy {
    let mut policy = command.output_buffer_policy();
    if policy.max_lines.is_none() {
        policy.max_lines = Some(DEFAULT_SUPERVISION_TAIL);
    }
    policy
}

/// When the supervisor restarts an exited child. See each variant; in every
/// case [`stop_when`](Supervisor::stop_when) and
/// [`max_restarts`](Supervisor::max_restarts) can end supervision first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RestartPolicy {
    /// Restart after every completed run, clean or not.
    Always,
    /// Restart only after a *crash* — a run that is **not a success**
    /// ([`ProcessResult::is_success`](crate::ProcessResult::is_success)): an exit
    /// code outside the accepted set (the command's
    /// [`ok_codes`](crate::Command::ok_codes), default `{0}`), a timeout, a signal
    /// kill, or a failure to spawn. A successful run (an accepted exit code) ends
    /// supervision — so a command with `ok_codes([0, 2])` exiting `2` is treated
    /// as clean, not a crash.
    OnCrash,
    /// Never restart: run the child once and report its outcome.
    Never,
}

impl RestartPolicy {
    /// This policy's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"always"`, `"on_crash"`, `"never"`) that is part
    /// of the crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back —
    /// the direction a config file or CLI flag choosing a policy needs.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            RestartPolicy::Always => "always",
            RestartPolicy::OnCrash => "on_crash",
            RestartPolicy::Never => "never",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `RestartPolicy` —
    /// the direction a config value or CLI flag selecting a policy needs.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default, so an unknown
    /// value fails loudly rather than defaulting to some policy the caller
    /// never asked for. Round-trips with [`name`](Self::name):
    /// `RestartPolicy::from_name(p.name()) == Some(p)` for every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "always" => Some(RestartPolicy::Always),
            "on_crash" => Some(RestartPolicy::OnCrash),
            "never" => Some(RestartPolicy::Never),
            _ => None,
        }
    }
}

/// Why supervision ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    /// The [`stop_when`](Supervisor::stop_when) predicate matched a run.
    Predicate,
    /// The [`RestartPolicy`] was satisfied — a clean exit under
    /// [`OnCrash`](RestartPolicy::OnCrash), or the single
    /// [`Never`](RestartPolicy::Never) run completing.
    PolicySatisfied,
    /// The [`give_up_when`](Supervisor::give_up_when) classifier recognized a
    /// crash as **permanent** — the supervisor stopped instead of restarting it
    /// forever. Only reported for a crashed run that produced a
    /// [`ProcessResult`] ([`GiveUpAttempt::Crashed`]); a permanent *spawn*
    /// failure (e.g. `ENOENT`, [`GiveUpAttempt::Failed`]) has no result to
    /// report and instead surfaces directly as `run()`'s `Err` (see
    /// [`give_up_when`](Supervisor::give_up_when) and the `run()` docs'
    /// "Errors" section).
    GaveUp,
    /// The [`max_restarts`](Supervisor::max_restarts) budget ran out while the
    /// policy still wanted another restart.
    RestartsExhausted,
    /// A liveness [`health_check`](Supervisor::health_check) judged the
    /// incarnation unresponsive and forced it down, and the [`RestartPolicy`]
    /// did not call for a restart ([`Never`](RestartPolicy::Never)) — so that
    /// force-killed run is the final one. Under a *restart-wanting* policy a
    /// failed liveness check instead counts as a crash and restarts, surfacing
    /// (if it then ends supervision at all) as the usual
    /// [`GaveUp`](Self::GaveUp) / [`RestartsExhausted`](Self::RestartsExhausted);
    /// either way the number of liveness force-kills is reported in
    /// [`SupervisionOutcome::liveness_kills`], and the final run's
    /// [`ProcessResult`] carries [`Outcome::Signalled`](crate::Outcome::Signalled).
    Unhealthy,
    /// A caller asked the live [`SupervisionSession`] to stop
    /// ([`SupervisionSession::stop`]): the current incarnation (if any) was
    /// stopped through its graceful path and supervision ended deliberately —
    /// distinct from a crash, an exhausted budget, a cancellation
    /// ([`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled)), or a
    /// [`stop_when`](Supervisor::stop_when) match. Only produced by a session
    /// stop; [`run`](Supervisor::run), which exposes no live handle, never
    /// reports it.
    Stopped,
}

impl StopReason {
    /// This reason's **stable machine identifier**: a short, lowercase
    /// `snake_case` string (`"predicate"`, `"policy_satisfied"`, `"gave_up"`,
    /// `"restarts_exhausted"`, `"unhealthy"`, `"stopped"`) that is part of the
    /// crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per variant instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name, **not** a wire/serialization
    /// format, but it is held stable all the same: a **new** variant gets a
    /// **new** identifier, and an existing identifier is **never renamed**
    /// without a major release. [`from_name`](Self::from_name) parses it back.
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            StopReason::Predicate => "predicate",
            StopReason::PolicySatisfied => "policy_satisfied",
            StopReason::GaveUp => "gave_up",
            StopReason::RestartsExhausted => "restarts_exhausted",
            StopReason::Unhealthy => "unhealthy",
            StopReason::Stopped => "stopped",
        }
    }

    /// Parse a [`name`](Self::name) identifier back into a `StopReason`.
    ///
    /// Returns `None` for any string that is not exactly one of the stable
    /// identifiers — an honest miss, never a silent default. Round-trips with
    /// [`name`](Self::name): `StopReason::from_name(r.name()) == Some(r)` for
    /// every variant.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "predicate" => Some(StopReason::Predicate),
            "policy_satisfied" => Some(StopReason::PolicySatisfied),
            "gave_up" => Some(StopReason::GaveUp),
            "restarts_exhausted" => Some(StopReason::RestartsExhausted),
            "unhealthy" => Some(StopReason::Unhealthy),
            "stopped" => Some(StopReason::Stopped),
            _ => None,
        }
    }
}

/// What the [`give_up_when`](Supervisor::give_up_when) classifier inspects: a
/// crashed run that produced a [`ProcessResult`], or a spawn/IO failure that
/// prevented the child from ever starting (e.g. `ENOENT` for a mistyped
/// program name) and so never produced one.
///
/// Non-exhaustive: a future kind of "the child never got a chance to run"
/// failure could be added without a breaking change.
#[derive(Debug)]
#[non_exhaustive]
pub enum GiveUpAttempt<'a> {
    /// A completed run that counts as a crash (see
    /// [`RestartPolicy::OnCrash`]'s definition) — the last full
    /// [`ProcessResult`] the supervisor would otherwise restart.
    Crashed(&'a ProcessResult<String>),
    /// The child could not even be started — the [`Error`](crate::Error) the
    /// runner returned instead of a result.
    Failed(&'a crate::Error),
}

/// What a finished supervision reports — the last run plus the keeper's
/// telemetry.
///
/// Non-exhaustive: a read-only report the crate produces — new telemetry can
/// be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SupervisionOutcome {
    /// The result of the final run (the one that ended supervision).
    pub final_result: ProcessResult<String>,
    /// How many times the child was *re*-run (the first run is not a restart):
    /// `restarts == 2` means three runs happened.
    pub restarts: u32,
    /// Why supervision stopped.
    pub stopped: StopReason,
    /// How many times the failure-storm guard paused restarts (always `0`
    /// unless [`storm_pause`](Supervisor::storm_pause) is set).
    pub storm_pauses: u32,
    /// How many incarnations a liveness [`health_check`](Supervisor::health_check)
    /// force-killed for being unresponsive (always `0` unless a health check is
    /// enabled). Each such kill is treated as a crash for the
    /// [`RestartPolicy`]/backoff/storm guard, so it is *also* reflected in
    /// [`restarts`](Self::restarts) when the policy restarted it.
    pub liveness_kills: u32,
}

/// A consistent, point-in-time snapshot of a live [`SupervisionSession`]'s
/// state — read atomically under the same lock the supervision loop publishes
/// each change under, so every field agrees with the others (no torn read).
/// Only non-secret facts appear here (activity, counts, the current child's
/// pid / start time); argv and environment values never do.
///
/// Non-exhaustive: a read-only snapshot the crate produces — new fields can be
/// added without a breaking change.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SupervisionStatus {
    active: bool,
    restarts: u32,
    storm_paused: bool,
    pid: Option<u32>,
    started_at: Option<SystemTime>,
}

impl SupervisionStatus {
    /// Whether the supervision loop is still running: `true` from
    /// [`start`](Supervisor::start) until supervision ends (on its own, via a
    /// [`stop`](SupervisionSession::stop), or by a cancel-token cancellation),
    /// `false` once the final [`SupervisionOutcome`] (or terminal error) has
    /// been produced.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// How many times the child has been *re*-run so far, live — mirrors
    /// [`SupervisionOutcome::restarts`] but updates as each restart happens
    /// rather than only once supervision ends. The first run is not a restart,
    /// so `0` while the first incarnation is alive.
    #[must_use]
    pub fn restarts(&self) -> u32 {
        self.restarts
    }

    /// Whether restarts are currently paused by the failure-storm guard
    /// ([`storm_pause`](Supervisor::storm_pause)) — `true` only while a storm
    /// pause is being slept out. Always `false` when `storm_pause` is unset.
    #[must_use]
    pub fn is_storm_paused(&self) -> bool {
        self.storm_paused
    }

    /// The OS process id of the current live incarnation, or `None` when no
    /// child is alive right now (between incarnations, during a backoff / storm
    /// pause, or once supervision has ended) or when the runner exposes no live
    /// pid (a capture-only test double).
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// When the current live incarnation started, or `None` when no child is
    /// alive right now (see [`pid`](Self::pid)).
    #[must_use]
    pub fn started_at(&self) -> Option<SystemTime> {
        self.started_at
    }
}

/// A live handle to a running supervision, returned by
/// [`Supervisor::start`]. Unlike [`run`](Supervisor::run) — which only reports
/// its [`SupervisionOutcome`] at the very end — a session lets a caller watch
/// supervision *while it runs* ([`status`](Self::status)), ask it to stop
/// *gracefully* ([`stop`](Self::stop)), and await its eventual outcome
/// ([`wait`](Self::wait)). This is the primitive for building daemons / process
/// managers on top of the runner layer.
///
/// The live status is an **addition** to (never a replacement for) the exit-
/// driven [`RestartPolicy`] and the crate's tracing instrumentation.
///
/// `Send`, like the crate's other handle types. Supervision runs on a detached
/// task; **dropping** the session without [`wait`](Self::wait)/[`stop`](Self::stop)
/// aborts that task (no orphaned supervision task), and under the default
/// [`JobRunner`] the in-flight incarnation is killed on drop (its private group
/// tears down), so no child is orphaned either.
#[derive(Debug)]
pub struct SupervisionSession {
    shared: Arc<SessionShared>,
    /// The final outcome, delivered once the loop task ends. `Option` so
    /// [`wait`](Self::wait)/[`stop`](Self::stop) can take it out under
    /// `&mut self` without moving a field out of a `Drop` type.
    completion: Option<oneshot::Receiver<Result<SupervisionOutcome>>>,
    /// Aborts the detached supervision task on [`Drop`] — no orphaned task.
    abort: tokio::task::AbortHandle,
}

impl SupervisionSession {
    /// A consistent live snapshot of this session's state (activity, restart
    /// count, storm-pause flag, and the current live incarnation's pid / start
    /// time). Cheap and lock-guarded — safe to poll at any time without racing
    /// the supervision loop.
    #[must_use]
    pub fn status(&self) -> SupervisionStatus {
        self.shared.snapshot()
    }

    /// Await the final [`SupervisionOutcome`] (or terminal error) — exactly
    /// what [`Supervisor::run`] would have returned. Blocks until supervision
    /// ends on its own, via [`stop`](Self::stop), or via a cancel-token
    /// cancellation.
    ///
    /// # Errors
    ///
    /// The same surface as [`run`](Supervisor::run) (a terminating spawn/IO
    /// failure, or a cancel-token [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled)).
    pub async fn wait(mut self) -> Result<SupervisionOutcome> {
        Self::await_completion(self.completion.take()).await
    }

    /// Ask supervision to stop gracefully with `grace` and await the final
    /// outcome. Stops the current live incarnation through its graceful path
    /// (honouring the `grace` window — `SIGTERM`, wait `grace`, then `SIGKILL`,
    /// under the default own-group [`JobRunner`]) and ends the loop with
    /// [`StopReason::Stopped`] — reported as a normal [`SupervisionOutcome`],
    /// never a crash or a cancellation error. A stop taken *during a backoff /
    /// storm pause* (no live child right now) interrupts that sleep and ends
    /// supervision at once, launching no further incarnation. `Duration::ZERO`
    /// escalates the child kill immediately.
    ///
    /// A caller who wants the outcome without stopping should
    /// [`wait`](Self::wait) instead.
    ///
    /// # Errors
    ///
    /// The same surface as [`wait`](Self::wait); a graceful stop itself yields a
    /// [`StopReason::Stopped`] outcome (`Ok`), not an error.
    pub async fn stop(mut self, grace: Duration) -> Result<SupervisionOutcome> {
        // Record the request and snapshot the current live child atomically
        // (closing the stop-vs-spawn race, see `SessionShared::publish_current`),
        // then interrupt any in-flight backoff / storm sleep so a between-
        // incarnations stop also ends promptly.
        let child = self.shared.request_stop(grace);
        self.shared.stop.cancel();
        if let Some(stopper) = child {
            // Stop the live child through its graceful path; the in-flight
            // capture then returns and the loop ends with `Stopped`.
            stopper.graceful_stop(grace).await;
        }
        Self::await_completion(self.completion.take()).await
    }

    async fn await_completion(
        completion: Option<oneshot::Receiver<Result<SupervisionOutcome>>>,
    ) -> Result<SupervisionOutcome> {
        match completion {
            Some(rx) => rx.await.unwrap_or_else(|_| {
                Err(crate::Error::io(std::io::Error::other(
                    "supervision task ended without reporting an outcome",
                )))
            }),
            None => Err(crate::Error::io(std::io::Error::other(
                "supervision outcome already taken",
            ))),
        }
    }
}

impl Drop for SupervisionSession {
    fn drop(&mut self) {
        // No orphaned supervision task: aborting drops the loop future, which
        // drops the in-flight incarnation's handle (killed on drop under the
        // default own-group `JobRunner`). A no-op once the task has finished.
        self.abort.abort();
    }
}

/// State shared between a [`SupervisionSession`] handle and its detached
/// supervision loop: the observable snapshot fields (behind `state`) and the
/// stop signal (`stop`) that interrupts an in-flight backoff / storm sleep.
#[derive(Debug)]
struct SessionShared {
    state: Mutex<SessionState>,
    /// Cancelled by [`SupervisionSession::stop`] to cut short a backoff / storm
    /// sleep. Distinct from the command's [`cancel_on`](Command::cancel_on)
    /// token (whose cancellation is an *error*): a stop is not an error.
    stop: CancellationToken,
}

/// The loop-owned mirror fields republished into an atomic [`SupervisionStatus`]
/// snapshot on every change; also the graceful-stop rendezvous.
#[derive(Debug)]
struct SessionState {
    active: bool,
    restarts: u32,
    storm_paused: bool,
    /// Set by [`SupervisionSession::stop`]; read by the loop to end with
    /// [`StopReason::Stopped`] rather than start / restart another incarnation.
    stopping: bool,
    stop_grace: Duration,
    /// The current live incarnation (pid, start time, and how to stop it), or
    /// `None` between incarnations / for a capture-only runner.
    current: Option<CurrentChild>,
}

/// Bookkeeping for the current live incarnation.
#[derive(Debug)]
struct CurrentChild {
    pid: Option<u32>,
    started_at: SystemTime,
    stopper: ChildStopper,
}

/// How to stop the current live incarnation on a graceful session stop, without
/// consuming the handle the incarnation's output verb is draining.
#[derive(Debug, Clone)]
struct ChildStopper {
    /// The incarnation's own private group, when it owns one (the default
    /// [`JobRunner`]): stopped with a real `SIGTERM` → grace → `SIGKILL`.
    group: Option<Arc<ProcessGroup>>,
    /// The incarnation command's cancel token: fired to stop a child with no
    /// own group to shut down gracefully (a shared-group child — just that
    /// child — or a capture-only double), the only stop lever there.
    inc_cancel: CancellationToken,
}

impl ChildStopper {
    /// Stop the current child. An own-group incarnation gets a graceful
    /// `SIGTERM` → `grace` → `SIGKILL`; any other (shared-group or capture-only)
    /// is stopped by firing its cancel token, the only available lever.
    async fn graceful_stop(&self, grace: Duration) {
        match &self.group {
            Some(group) => {
                let _ = group
                    .graceful_terminate(grace, crate::sys::SIGTERM_RAW)
                    .await;
            }
            None => self.inc_cancel.cancel(),
        }
    }
}

impl SessionShared {
    fn new() -> Self {
        SessionShared {
            state: Mutex::new(SessionState {
                active: true,
                restarts: 0,
                storm_paused: false,
                stopping: false,
                stop_grace: Duration::ZERO,
                current: None,
            }),
            stop: CancellationToken::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SessionState> {
        self.state.lock().expect("session state lock")
    }

    /// The atomic snapshot read by [`SupervisionSession::status`].
    fn snapshot(&self) -> SupervisionStatus {
        let state = self.lock();
        SupervisionStatus {
            active: state.active,
            restarts: state.restarts,
            storm_paused: state.storm_paused,
            pid: state.current.as_ref().and_then(|c| c.pid),
            started_at: state.current.as_ref().map(|c| c.started_at),
        }
    }

    fn set_restarts(&self, restarts: u32) {
        self.lock().restarts = restarts;
    }

    fn set_storm_paused(&self, paused: bool) {
        self.lock().storm_paused = paused;
    }

    /// Publish a freshly-spawned child as current and, atomically, learn whether
    /// a graceful stop is already pending — closing the stop-vs-spawn race:
    /// whichever of publish / [`request_stop`](Self::request_stop) takes the
    /// lock second sees the other's write, so the child is stopped exactly once
    /// (here, if the stop landed first, or in `stop` if the publish did).
    fn publish_current(
        &self,
        pid: Option<u32>,
        started_at: SystemTime,
        stopper: ChildStopper,
    ) -> Option<Duration> {
        let mut state = self.lock();
        let grace = state.stop_grace;
        let stopping = state.stopping;
        state.current = Some(CurrentChild {
            pid,
            started_at,
            stopper,
        });
        stopping.then_some(grace)
    }

    fn clear_current(&self) {
        self.lock().current = None;
    }

    fn mark_inactive(&self) {
        let mut state = self.lock();
        state.active = false;
        state.current = None;
    }

    fn is_stopping(&self) -> bool {
        self.lock().stopping
    }

    /// Record a graceful-stop request and snapshot the current live child (its
    /// stopper), atomically — the [`publish_current`](Self::publish_current)
    /// counterpart of the stop-vs-spawn race.
    fn request_stop(&self, grace: Duration) -> Option<ChildStopper> {
        let mut state = self.lock();
        state.stopping = true;
        state.stop_grace = grace;
        state.current.as_ref().map(|c| c.stopper.clone())
    }
}

/// Releases the published live child ([`SessionShared::current`]) when
/// [`run_to_result`](Supervisor::run_to_result) leaves by **any** path — a
/// normal return *or* the future being dropped mid-run, which is exactly what a
/// liveness [`health_check`](Supervisor::health_check) kill does (the losing
/// `run_to_result` in [`run_incarnation`](Supervisor::run_incarnation)'s
/// `select!` is dropped, so a plain `clear_current()` at the end is never
/// reached). Dropping `current` releases the [`ChildStopper`]'s clone of the
/// incarnation's `Arc<ProcessGroup>`; together with the abandoned
/// [`RunningProcess`](crate::RunningProcess)'s own reference dropping in the same
/// unwind, that lets the group's kill-on-drop backstop fire (and tear the wedged
/// child down) *immediately* on the kill — not linger until the next
/// `publish_current` overwrites it, which would keep the force-killed child alive
/// and its now-stale pid observable through the whole restart backoff.
struct CurrentGuard<'a> {
    shared: &'a SessionShared,
}

impl Drop for CurrentGuard<'_> {
    fn drop(&mut self) {
        self.shared.clear_current();
    }
}

/// Why a backoff / storm sleep (or an incarnation) stopped waiting — the shared
/// return of [`Supervisor::sleep_or_cancel`] and its callers.
enum Wake {
    /// The delay elapsed normally.
    Elapsed,
    /// The command's [`cancel_on`](Command::cancel_on) token fired — a terminal
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled).
    Cancelled,
    /// A [`SupervisionSession::stop`] was requested — end with
    /// [`StopReason::Stopped`].
    Stopped,
}

/// Keeps a [`Command`] alive: runs it, classifies every exit against the
/// [`RestartPolicy`] and the [`stop_when`](Self::stop_when) predicate, and
/// restarts it after an exponential-backoff delay until supervision ends.
///
/// Defaults: [`OnCrash`](RestartPolicy::OnCrash), unlimited restarts, backoff
/// `200ms × 2.0` capped at 30 s, jitter on, failure-storm guard off (enable
/// with [`storm_pause`](Self::storm_pause); failure-score half-life 30 s and
/// threshold 5.0 once enabled).
///
/// Runs go through a [`ProcessRunner`] — [`JobRunner`] by default. Override
/// with [`with_runner`](Self::with_runner) to share a [`ProcessGroup`](crate::ProcessGroup)
/// or inject a test double.
pub struct Supervisor<R: ProcessRunner = JobRunner> {
    command: Command,
    runner: R,
    policy: RestartPolicy,
    max_restarts: Option<u32>,
    backoff_base: Duration,
    backoff_factor: f64,
    max_backoff: Duration,
    jitter: bool,
    failure_decay: Duration,
    failure_threshold: f64,
    storm_pause: Option<Duration>,
    #[allow(clippy::type_complexity)]
    stop_when: Option<Box<dyn Fn(&ProcessResult<String>) -> bool + Send + Sync>>,
    /// The permanent-failure classifier; see
    /// [`give_up_when`](Self::give_up_when).
    #[allow(clippy::type_complexity)]
    give_up_when: Option<Box<dyn Fn(&GiveUpAttempt<'_>) -> bool + Send + Sync>>,
    /// The output-capture policy applied to every incarnation. Defaults to a
    /// bounded tail (see [`default_supervision_capture`]); override with
    /// [`capture`](Self::capture).
    capture: OutputBufferPolicy,
    /// The opt-in liveness probe + cadence; `None` (the default) leaves the
    /// supervisor's behavior exactly as it was before health-checking existed.
    /// Enabled by [`health_check`](Self::health_check).
    health_check: Option<HealthCheck>,
    /// Consecutive failed liveness checks tolerated before the incarnation is
    /// force-restarted (default [`DEFAULT_HEALTH_FAILURES`]). No effect unless
    /// [`health_check`](Self::health_check) is set; tunable via
    /// [`health_check_failures`](Self::health_check_failures).
    health_check_failures: u32,
}

// Manual: runner type parameter and boxed predicate are opaque.
impl<R: ProcessRunner> std::fmt::Debug for Supervisor<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor")
            .field("policy", &self.policy)
            .field("max_restarts", &self.max_restarts)
            .field("backoff_base", &self.backoff_base)
            .field("backoff_factor", &self.backoff_factor)
            .field("max_backoff", &self.max_backoff)
            .field("jitter", &self.jitter)
            .field("failure_decay", &self.failure_decay)
            .field("failure_threshold", &self.failure_threshold)
            .field("storm_pause", &self.storm_pause)
            .field("has_stop_when", &self.stop_when.is_some())
            .field("has_give_up_when", &self.give_up_when.is_some())
            .field("capture", &self.capture)
            .field("has_health_check", &self.health_check.is_some())
            .field("health_check_failures", &self.health_check_failures)
            .finish_non_exhaustive()
    }
}

impl Supervisor<JobRunner> {
    /// Supervise `command` with the default [`JobRunner`] (a fresh private
    /// kill-on-drop group per incarnation).
    pub fn new(command: Command) -> Self {
        let capture = default_supervision_capture(&command);
        Supervisor {
            command,
            runner: JobRunner::new(),
            policy: RestartPolicy::OnCrash,
            max_restarts: None,
            backoff_base: Duration::from_millis(200),
            backoff_factor: 2.0,
            max_backoff: Duration::from_secs(30),
            jitter: true,
            failure_decay: Duration::from_secs(30),
            failure_threshold: 5.0,
            storm_pause: None,
            stop_when: None,
            give_up_when: None,
            capture,
            health_check: None,
            health_check_failures: DEFAULT_HEALTH_FAILURES,
        }
    }
}

impl<R: ProcessRunner> Supervisor<R> {
    /// Run every incarnation through `runner` instead of the default
    /// [`JobRunner`] — e.g. a `&ProcessGroup` for one shared kill-on-drop
    /// group, or a test double for hermetic supervision tests.
    ///
    /// With a shared group, the group's *state* applies to every incarnation:
    /// notably, restarting into a `suspend`ed group on the Linux cgroup
    /// mechanism spawns the new child **frozen** (see the
    /// `ProcessGroup::suspend` docs, `process-control` feature) — resume the
    /// group before supervising into it.
    #[must_use]
    pub fn with_runner<R2: ProcessRunner>(self, runner: R2) -> Supervisor<R2> {
        Supervisor {
            command: self.command,
            runner,
            policy: self.policy,
            max_restarts: self.max_restarts,
            backoff_base: self.backoff_base,
            backoff_factor: self.backoff_factor,
            max_backoff: self.max_backoff,
            jitter: self.jitter,
            failure_decay: self.failure_decay,
            failure_threshold: self.failure_threshold,
            storm_pause: self.storm_pause,
            stop_when: self.stop_when,
            give_up_when: self.give_up_when,
            capture: self.capture,
            health_check: self.health_check,
            health_check_failures: self.health_check_failures,
        }
    }

    /// Bound (or widen) the output captured from each incarnation.
    ///
    /// A supervised process is often long-lived and chatty, so the default is a
    /// **bounded tail** ([`OutputBufferPolicy::bounded`] of the most recent lines)
    /// rather than the unbounded capture a one-shot command uses — capturing a
    /// server's entire lifetime of output would grow without bound. An explicit
    /// bounded/`fail_loud` policy on the [`Command`] is respected as-is; an
    /// *unbounded* one is bounded to the tail while **preserving its overflow
    /// mode** (so an `unbounded().with_overflow(Error)` command becomes a bounded
    /// fail-loud, not a silent `DropOldest`). Pass a policy here to override
    /// either (including [`unbounded`](OutputBufferPolicy::unbounded) if you truly
    /// want every line).
    ///
    /// This caps *retention*, not the stdio mode. A piped stdout is retained so
    /// [`stop_when`](Self::stop_when) can inspect it; a non-piped stdout
    /// (`Inherit`/`Null`/a file redirect) is discarded and its final result has
    /// an empty stdout. File redirects therefore remain suitable for a service
    /// whose restart incarnations append to one child-owned log.
    #[must_use]
    pub fn capture(mut self, policy: OutputBufferPolicy) -> Self {
        self.capture = policy;
        self
    }

    /// When to restart (default: [`OnCrash`](RestartPolicy::OnCrash)).
    #[must_use]
    pub fn restart(mut self, policy: RestartPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Restart at most `n` times — `n + 1` total runs (default: unlimited).
    #[must_use]
    pub fn max_restarts(mut self, n: u32) -> Self {
        self.max_restarts = Some(n);
        self
    }

    /// Exponential backoff before each restart: the n-th restart (0-based)
    /// waits `base × factor^n`, capped by [`max_backoff`](Self::max_backoff).
    /// A `factor` below `1.0` (or non-finite) is treated as `1.0`.
    /// Default: `200ms × 2.0`.
    ///
    /// The escalation **resets** after a healthy run — one that stayed up at least
    /// as long as [`max_backoff`](Self::max_backoff) — so a long-lived service that
    /// crashes occasionally isn't pinned at the ceiling by an old crash burst; a
    /// tight loop whose incarnations are each shorter than the ceiling keeps
    /// climbing (the exponent `n` counts restarts *since the last healthy run*, not
    /// lifetime restarts). The floor is on uptime, not exit kind: under
    /// [`Always`](RestartPolicy::Always) a worker that exits — cleanly or not — in
    /// under `max_backoff` is treated as flapping and its restarts escalate, so
    /// loop inside a long-lived process (or lower `max_backoff`) if you want prompt
    /// clean-exit restarts.
    ///
    /// The keep-alive twin of [`RetryPolicy`](crate::RetryPolicy)'s replay-to-success
    /// backoff, which spells these same two knobs `initial_backoff` (`base`) and
    /// `multiplier` (`factor`); this one uses a `[0.5, 1.5)` multiplicative
    /// [`jitter`](Self::jitter) rather than the policy's `[0, delay]` full jitter.
    #[must_use]
    pub fn backoff(mut self, base: Duration, factor: f64) -> Self {
        self.backoff_base = base;
        self.backoff_factor = factor;
        self
    }

    /// Cap any single backoff delay (default: 30 s). With [`jitter`](Self::jitter)
    /// on (the default), this bounds the *pre-jitter* delay — the `[0.5, 1.5)`
    /// jitter is applied afterward, so an individual restart delay can reach up to
    /// `1.5 ×` this cap. (Contrast [`RetryPolicy`](crate::RetryPolicy)'s `[0, delay]`
    /// full jitter, which never exceeds its own cap.)
    #[must_use]
    pub fn max_backoff(mut self, cap: Duration) -> Self {
        self.max_backoff = cap;
        self
    }

    /// Multiply each backoff delay by a uniform factor in `[0.5, 1.5)`
    /// (default: **on**), so a fleet of supervised workers restarted by the
    /// same incident doesn't stampede back in lockstep. Disable for
    /// deterministic delays.
    #[must_use]
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    /// Enable the **failure-storm guard**: when crash-restarts cluster faster
    /// than the failure score can decay (see
    /// [`failure_decay`](Self::failure_decay) /
    /// [`failure_threshold`](Self::failure_threshold)), pause restarts once
    /// for `pause` — jittered into `[0.5, 1.5)` of the nominal value per
    /// [`jitter`](Self::jitter) — then reset the score and resume. Off by
    /// default; this is the master switch, the other two knobs only tune it.
    ///
    /// Each failed run adds `1` to a score that halves every
    /// `failure_decay`: `score = score × 0.5^(Δt / failure_decay) + 1`. A
    /// service that fails *rarely* never accumulates past the threshold; a
    /// *storm* trips it and gets one collective pause instead of hammering
    /// restarts at backoff speed. (Design borrowed from Go's `suture`
    /// supervisor — the idea, not the code.)
    ///
    /// Only failures feed the score: crashes and spawn errors. A clean exit
    /// restarted under [`Always`](RestartPolicy::Always) is not a failure.
    /// The storm pause *stacks with* (runs before) the per-restart backoff,
    /// and [`max_restarts`](Self::max_restarts) is checked first — a storm
    /// pause never resurrects an exhausted budget. Pauses taken are reported
    /// in [`SupervisionOutcome::storm_pauses`].
    #[must_use]
    pub fn storm_pause(mut self, pause: Duration) -> Self {
        self.storm_pause = Some(pause);
        self
    }

    /// Half-life of the failure score used by the storm guard (default: 30 s):
    /// every `decay` seconds without a failure, the accumulated score halves.
    /// A zero half-life keeps no history — every failure scores exactly `1`,
    /// so the guard trips only with a threshold below `1.0`. No effect unless
    /// [`storm_pause`](Self::storm_pause) is set.
    #[must_use]
    pub fn failure_decay(mut self, decay: Duration) -> Self {
        self.failure_decay = decay;
        self
    }

    /// Failure score above which the storm guard trips (default: `5.0` —
    /// roughly "more than five failures inside one half-life"). A non-finite
    /// threshold never trips. No effect unless
    /// [`storm_pause`](Self::storm_pause) is set.
    #[must_use]
    pub fn failure_threshold(mut self, threshold: f64) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// End supervision when `predicate` matches a completed run — checked
    /// before the [`RestartPolicy`] on every exit, clean or not. (It never
    /// sees a run that failed to *start*; spawn errors are classified by the
    /// policy alone.)
    #[must_use]
    pub fn stop_when(
        mut self,
        predicate: impl Fn(&ProcessResult<String>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.stop_when = Some(Box::new(predicate));
        self
    }

    /// Classify a crash — or a spawn failure that never produced a result —
    /// as **permanent**, so the supervisor gives up instead of restarting it
    /// forever (see the "Permanent failures" section of [`run`](Self::run)'s
    /// docs). `classifier` receives a [`GiveUpAttempt`]: [`Crashed`](GiveUpAttempt::Crashed)
    /// for a completed run that counts as a crash, [`Failed`](GiveUpAttempt::Failed)
    /// for a launch that never started the child at all (the ENOENT case —
    /// a mistyped program name — is a [`Failed`](GiveUpAttempt::Failed), not a
    /// `Crashed`, since no [`ProcessResult`] exists to inspect).
    ///
    /// ```
    /// use processkit::GiveUpAttempt;
    ///
    /// let classify = |attempt: &GiveUpAttempt<'_>| match attempt {
    ///     GiveUpAttempt::Failed(err) => err.is_not_found(), // missing binary — never recovers
    ///     GiveUpAttempt::Crashed(_) => false,
    ///     _ => false, // future GiveUpAttempt variants: not permanent until classified
    /// };
    /// # let _ = classify;
    /// ```
    ///
    /// Not checked for a clean exit, nor for a run [`stop_when`](Self::stop_when)
    /// already ended, nor for a crash the [`RestartPolicy`] itself would not have
    /// restarted (e.g. under [`Never`](RestartPolicy::Never)) — those already stop
    /// supervision with a more specific reason. When checked, it runs **before**
    /// [`max_restarts`](Self::max_restarts) and the [failure-storm guard](Self::storm_pause):
    /// a permanent-failure verdict wins over "budget not yet exhausted" and never
    /// pays for a storm pause it was going to end anyway. A `Crashed` match reports
    /// [`StopReason::GaveUp`]; a `Failed` match has no result to report and
    /// surfaces the classified error directly as `run()`'s `Err`, same as an
    /// exhausted budget on that path.
    ///
    /// Default: unset — a permanent failure restarts forever (throttled only by
    /// backoff/`max_restarts`/the storm guard), matching the crate's prior
    /// behavior.
    #[must_use]
    pub fn give_up_when(
        mut self,
        classifier: impl Fn(&GiveUpAttempt<'_>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.give_up_when = Some(Box::new(classifier));
        self
    }

    /// Enable **liveness health-checking** (opt-in; off by default): re-run the
    /// async `probe` every `interval` for the life of each incarnation, and when
    /// it fails a threshold of consecutive checks
    /// ([`health_check_failures`](Self::health_check_failures), default
    /// `3`) **force-restart** the child. This detects the
    /// blind spot a plain [`RestartPolicy`] can't see: a process that is still
    /// *alive* but *wedged* — a deadlocked server, a stuck event loop — which
    /// never exits, so the exit-driven policy would keep it "running" forever.
    /// The analogue of systemd's `WatchdogSec` and a container liveness probe.
    ///
    /// `probe` is any async predicate returning `true` for *healthy* — a TCP
    /// connect, an HTTP `/healthz` request, a file/heartbeat check, a custom
    /// closure — in the same shape as
    /// [`RunningProcess::wait_for`](crate::RunningProcess::wait_for)'s readiness
    /// check (it takes no handle, so it observes the child out-of-band). The
    /// first probe fires one `interval` after the incarnation starts (startup
    /// grace); a healthy child is then never disturbed. A zero (or otherwise
    /// degenerate) `interval` is clamped to a small safe minimum rather than
    /// causing a busy-spin loop or dropping the startup grace.
    ///
    /// A failed liveness check is treated **exactly like a crash**: the wedged
    /// incarnation is dropped (killed on drop under the default [`JobRunner`] —
    /// see [`run`](Self::run)'s cancellation note for the shared-group caveat)
    /// and flows through the [`RestartPolicy`], [`backoff`](Self::backoff), the
    /// [failure-storm guard](Self::storm_pause) and [`max_restarts`](Self::max_restarts)
    /// just as a real crash would — but it does **not** consult
    /// [`stop_when`](Self::stop_when) (there is no cleanly-completed run to
    /// evaluate). Under [`Never`](RestartPolicy::Never) the single force-killed
    /// run is reported with [`StopReason::Unhealthy`]. Each force-kill is counted
    /// in [`SupervisionOutcome::liveness_kills`]; the synthetic final result
    /// carries [`Outcome::Signalled`](crate::Outcome).
    ///
    /// A liveness-killed incarnation counts toward the backoff escalation as any
    /// crash does, using how long it actually stayed up before wedging — so a
    /// service that runs healthy for a long while and only occasionally wedges
    /// isn't pinned at the [`max_backoff`](Self::max_backoff) ceiling, while one
    /// that wedges promptly after each restart self-throttles (same uptime floor
    /// as [`backoff`](Self::backoff)).
    ///
    /// ```
    /// use processkit::{Command, Supervisor};
    /// use std::time::Duration;
    ///
    /// # async fn f() -> processkit::Result<()> {
    /// let outcome = Supervisor::new(Command::new("my-server"))
    ///     .health_check(
    ///         || async { tokio::net::TcpStream::connect("127.0.0.1:8080").await.is_ok() },
    ///         Duration::from_secs(5),
    ///     )
    ///     .max_restarts(10)
    ///     .run()
    ///     .await?;
    /// println!("liveness kills: {}", outcome.liveness_kills);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn health_check<F, Fut>(mut self, probe: F, interval: Duration) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        self.health_check = Some(HealthCheck {
            probe: Box::new(move || Box::pin(probe())),
            // A degenerate (e.g. zero) interval is clamped to a safe minimum
            // rather than passed through as-is, so `watch`'s loop can't
            // degenerate into a busy-spin and the startup-grace promise below
            // stays true even for a zero `interval` (see the clamp constant's
            // doc comment for the full rationale).
            interval: interval.max(MIN_HEALTH_CHECK_INTERVAL),
        });
        self
    }

    /// How many *consecutive* failed liveness checks to tolerate before a
    /// [`health_check`](Self::health_check) force-restarts the incarnation
    /// (default `3`). One healthy probe resets the
    /// streak, so this forgives transient blips; combined with the probe
    /// `interval` it sets the effective grace window (≈ `interval × n`) before a
    /// wedged child is killed. A value of `0` is treated as `1`. No effect unless
    /// [`health_check`](Self::health_check) is set — settable in either order.
    #[must_use]
    pub fn health_check_failures(mut self, n: u32) -> Self {
        self.health_check_failures = n;
        self
    }

    /// Supervise until the policy, the predicate, or the restart budget ends
    /// it, and report the [`SupervisionOutcome`].
    ///
    /// # Permanent failures
    ///
    /// Without [`give_up_when`](Self::give_up_when), the supervisor does **not**
    /// distinguish a transient crash from a permanent one — a command that can
    /// never succeed (a missing binary, a config error that crashes on startup, a
    /// port that is permanently taken) restarts **forever** under the default
    /// unlimited [`OnCrash`](RestartPolicy::OnCrash) policy, throttled only by the
    /// backoff: a fast-failing one climbs to [`max_backoff`](Self::max_backoff)
    /// (each incarnation is shorter than the ceiling, so never healthy), while one
    /// that takes `≥ max_backoff` to fail is throttled by its own runtime instead.
    /// Either way it loops indefinitely — bound it with
    /// [`max_restarts`](Self::max_restarts) and/or a
    /// [`give_up_when`](Self::give_up_when) classifier (or the coarser
    /// [`stop_when`](Self::stop_when) predicate) that recognizes the unrecoverable
    /// case, so supervision gives up.
    ///
    /// # Errors
    ///
    /// Returns `Err` only when the **terminating** attempt failed to produce a
    /// result at all (a spawn/IO failure when no further restart is allowed) —
    /// there is no final [`ProcessResult`] to report in that case. A spawn
    /// failure with restarts remaining counts as a crash and is retried.
    ///
    /// # Cancellation
    ///
    /// Dropping this future mid-run abandons the in-flight incarnation. With
    /// the default [`JobRunner`] it is killed on drop (the incarnation owns a
    /// private group); with a shared-group runner
    /// ([`with_runner(&group)`](Self::with_runner)) the incarnation stays
    /// alive in the caller's group until the group tears it down.
    ///
    /// An incarnation cancelled via its token ([`Command::cancel_on`](crate::Command::cancel_on))
    /// is **terminal**: supervision returns that
    /// `ErrorReason::Cancelled` immediately, regardless of policy or budget — the
    /// token stays cancelled, so a restart would only be cancelled again.
    ///
    /// A [`health_check`](Self::health_check) force-kill relies on this same
    /// drop-kills semantics to end the wedged incarnation, so the shared-group
    /// caveat above applies to it too (a shared-group child is only reliably
    /// stopped when the group is torn down).
    ///
    /// # Live status
    ///
    /// `run` reports its outcome only at the very end and exposes no handle
    /// while it runs. For a live view — watch the restart count / current pid,
    /// or ask supervision to stop gracefully mid-flight — use
    /// [`start`](Self::start), which returns a [`SupervisionSession`]; `run` is a
    /// thin wrapper over `start` + awaiting its outcome, so their behavior is
    /// identical. (`run` therefore never reports [`StopReason::Stopped`], which
    /// only a session stop produces.)
    pub async fn run(self) -> Result<SupervisionOutcome> {
        // A thin wrapper over the shared supervision engine (`drive`), the same
        // one `start`'s detached task runs — so `run` preserves the classic
        // behavior verbatim (outcome classification, cancellation, tracing,
        // liveness/storm accounting). It drives the engine inline rather than
        // spawning, so — unlike `start` — it needs no `'static` runner and keeps
        // working for a borrowed shared group (`with_runner(&group)`); the
        // session status it publishes is simply never observed.
        let shared = Arc::new(SessionShared::new());
        self.drive(shared).await
    }

    /// The shared supervision engine behind [`run`](Self::run) and
    /// [`start`](Self::start): one faithful copy of the classic supervision loop,
    /// extended only with live-status publication and graceful-stop handling
    /// (both no-ops for `run`, whose `shared` is never observed and whose stop is
    /// never fired). Returns the final [`SupervisionOutcome`] exactly as before.
    async fn drive(self, shared: Arc<SessionShared>) -> Result<SupervisionOutcome> {
        // Reject up front a configuration that could genuinely need a second
        // incarnation but only has a one-shot stdin source to feed it: the
        // first incarnation would consume the source, and every restart after
        // it would fail to launch at all (`ErrorReason::Io`, "already consumed" —
        // see `runner::take_stdin_for_run`), which under the default OnCrash
        // policy spins forever as a rapid crash-restart-backoff loop instead
        // of ever making progress. Caught here, before the first run even
        // starts, so the failure is immediate and typed rather than an
        // eventual runtime symptom.
        if self.may_restart() && self.has_unusable_one_shot_stdin() {
            return Err(self.one_shot_restart_err());
        }

        let factor = if self.backoff_factor.is_finite() {
            self.backoff_factor.max(1.0)
        } else {
            1.0
        };

        // Apply the capture policy once; clone so `self` stays intact.
        let command = self.command.clone().output_buffer(self.capture);
        // Latches false the first time the runner proves capture-only (its
        // `start` returns `Unsupported`): the loop then drives incarnations
        // through the capture verb — no live pid / graceful child-stop, but
        // supervision itself is unaffected.
        let spawn_capable = AtomicBool::new(true);

        let mut restarts: u32 = 0;
        // The backoff *exponent* — separate from the lifetime `restarts` count so a
        // run that stayed healthy resets the escalation (E3): otherwise a
        // long-lived service that exits/crashes occasionally would climb to the
        // `max_backoff` ceiling and restart at it forever.
        let mut backoff_restarts: u32 = 0;
        // Lifetime count of incarnations a liveness check force-killed (reported
        // in `SupervisionOutcome::liveness_kills`); always 0 without a health check.
        let mut liveness_kills: u32 = 0;
        let mut storm = StormState::new();
        let mut last_result: Option<ProcessResult<String>> = None;
        loop {
            // A graceful stop requested while between incarnations (a backoff /
            // storm sleep was just interrupted): end now with the last result
            // rather than start another incarnation.
            if shared.is_stopping()
                && let Some(last) = &last_result
            {
                return Ok(self.outcome(
                    last.clone(),
                    restarts,
                    liveness_kills,
                    &storm,
                    StopReason::Stopped,
                ));
            }

            // Each incarnation carries a fresh cancel token so a graceful stop
            // can reach a shared-group / capture-only child (its only lever); it
            // is a *child* of any caller `cancel_on` token, so caller cancellation
            // still propagates and stays a terminal `ErrorReason::Cancelled`.
            let inc_cancel = self
                .command
                .cancel_token()
                .map_or_else(CancellationToken::new, |user| user.child_token());
            let inc_command = command.clone().cancel_on(inc_cancel);

            match self
                .run_incarnation(&inc_command, &shared, &spawn_capable)
                .await
            {
                Incarnation::Ran(Ok(result)) => {
                    last_result = Some(result.clone());
                    if shared.is_stopping() {
                        // The current incarnation was gracefully stopped (or
                        // completed while a stop was pending): end with its honest
                        // result and `Stopped`, which wins over policy/predicate —
                        // the caller explicitly asked to stop.
                        return Ok(self.outcome(
                            result,
                            restarts,
                            liveness_kills,
                            &storm,
                            StopReason::Stopped,
                        ));
                    }
                    if let Some(predicate) = &self.stop_when
                        && predicate(&result)
                    {
                        return Ok(self.outcome(
                            result,
                            restarts,
                            liveness_kills,
                            &storm,
                            StopReason::Predicate,
                        ));
                    }
                    let crashed = !result.is_success();
                    let wants_restart = match self.policy {
                        RestartPolicy::Always => true,
                        RestartPolicy::OnCrash => crashed,
                        RestartPolicy::Never => false,
                    };
                    if !wants_restart {
                        return Ok(self.outcome(
                            result,
                            restarts,
                            liveness_kills,
                            &storm,
                            StopReason::PolicySatisfied,
                        ));
                    }
                    match self
                        .gate_restart(
                            &result,
                            crashed,
                            &mut restarts,
                            &mut backoff_restarts,
                            &mut storm,
                            factor,
                            &shared,
                        )
                        .await
                    {
                        GateOutcome::GaveUp => {
                            return Ok(self.outcome(
                                result,
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::GaveUp,
                            ));
                        }
                        GateOutcome::Exhausted => {
                            return Ok(self.outcome(
                                result,
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::RestartsExhausted,
                            ));
                        }
                        GateOutcome::Cancelled => return Err(self.cancelled_err(&command)),
                        GateOutcome::Stopped => {
                            return Ok(self.outcome(
                                result,
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::Stopped,
                            ));
                        }
                        GateOutcome::Restart => shared.set_restarts(restarts),
                    }
                }
                Incarnation::LivenessFailed { uptime } => {
                    // A failed liveness check is a crash the supervisor induced:
                    // the wedged incarnation was already dropped (killed on drop),
                    // and now flows through the same crash machinery — but skips
                    // `stop_when` (no cleanly-completed run to judge). The stamped
                    // uptime lets the E3 escalation reset for a long-lived child.
                    liveness_kills = liveness_kills.saturating_add(1);
                    let result = self.liveness_kill_result(uptime);
                    last_result = Some(result.clone());
                    if shared.is_stopping() {
                        // A stop landed while this incarnation was being force-killed
                        // for wedging — honor the stop over a restart.
                        return Ok(self.outcome(
                            result,
                            restarts,
                            liveness_kills,
                            &storm,
                            StopReason::Stopped,
                        ));
                    }
                    if matches!(self.policy, RestartPolicy::Never) {
                        // Never won't restart — report the single force-killed run.
                        return Ok(self.outcome(
                            result,
                            restarts,
                            liveness_kills,
                            &storm,
                            StopReason::Unhealthy,
                        ));
                    }
                    match self
                        .gate_restart(
                            &result,
                            /* crashed */ true,
                            &mut restarts,
                            &mut backoff_restarts,
                            &mut storm,
                            factor,
                            &shared,
                        )
                        .await
                    {
                        GateOutcome::GaveUp => {
                            return Ok(self.outcome(
                                result,
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::GaveUp,
                            ));
                        }
                        GateOutcome::Exhausted => {
                            return Ok(self.outcome(
                                result,
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::RestartsExhausted,
                            ));
                        }
                        GateOutcome::Cancelled => return Err(self.cancelled_err(&command)),
                        GateOutcome::Stopped => {
                            return Ok(self.outcome(
                                result,
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::Stopped,
                            ));
                        }
                        GateOutcome::Restart => shared.set_restarts(restarts),
                    }
                }
                Incarnation::Ran(Err(err)) => {
                    if err.is_cancelled() {
                        // A graceful stop of a shared-group / capture-only child
                        // manifests as a `Cancelled` capture (its cancel token is
                        // the only stop lever). Report the deliberate stop as
                        // `Stopped`, not as a cancellation error.
                        if shared.is_stopping() {
                            return Ok(self.outcome(
                                self.stopped_result(),
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::Stopped,
                            ));
                        }
                        return Err(err);
                    }
                    // A stop was requested but this attempt produced no result to
                    // report — surface the honest terminal error (same shape as an
                    // exhausted budget on this path).
                    if shared.is_stopping() {
                        return Err(err);
                    }
                    let wants_restart = !matches!(self.policy, RestartPolicy::Never);
                    if !wants_restart {
                        return Err(err);
                    }
                    if let Some(classifier) = &self.give_up_when
                        && classifier(&GiveUpAttempt::Failed(&err))
                    {
                        return Err(err);
                    }
                    if self.max_restarts.is_some_and(|max| restarts >= max) {
                        return Err(err);
                    }
                    // A spawn-side failure carries no run duration, so it never
                    // counts as healthy — the escalation keeps climbing.
                    match self.storm_gate(&mut storm, &shared).await {
                        Wake::Cancelled => return Err(self.cancelled_err(&command)),
                        Wake::Stopped => {
                            return Ok(self.outcome(
                                self.stopped_result(),
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::Stopped,
                            ));
                        }
                        Wake::Elapsed => {}
                    }
                    match self.sleep_backoff(backoff_restarts, factor, &shared).await {
                        Wake::Cancelled => return Err(self.cancelled_err(&command)),
                        Wake::Stopped => {
                            return Ok(self.outcome(
                                self.stopped_result(),
                                restarts,
                                liveness_kills,
                                &storm,
                                StopReason::Stopped,
                            ));
                        }
                        Wake::Elapsed => {}
                    }
                    restarts = restarts.saturating_add(1);
                    backoff_restarts = backoff_restarts.saturating_add(1);
                    shared.set_restarts(restarts);
                }
            }
        }
    }

    /// Run one incarnation. Without a [`health_check`](Self::health_check) this
    /// is exactly [`run_to_result`](Self::run_to_result) (the pre-feature fast
    /// path — no extra task, timer, or `select!`). With one, race the run
    /// against the liveness watcher: whichever resolves first wins, `biased`
    /// toward a genuine exit/crash so a child that dies on its own the same
    /// instant a probe would have tripped is reported as its real result, not a
    /// liveness kill. When the watcher wins, dropping the losing
    /// [`run_to_result`](Self::run_to_result) future ends the wedged
    /// incarnation (killed on drop under the default [`JobRunner`]).
    async fn run_incarnation(
        &self,
        command: &Command,
        shared: &SessionShared,
        spawn_capable: &AtomicBool,
    ) -> Incarnation {
        let Some(health) = &self.health_check else {
            return Incarnation::Ran(self.run_to_result(command, shared, spawn_capable).await);
        };
        // Anchor uptime on tokio's clock (not `std::time::Instant`) so it shares
        // the timer the liveness sleeps and any paused-runtime test run on — the
        // same clock split `sleep_or_cancel`/probes use.
        let started = tokio::time::Instant::now();
        tokio::select! {
            biased;
            result = self.run_to_result(command, shared, spawn_capable) => Incarnation::Ran(result),
            () = health.watch(self.health_check_failures) => {
                Incarnation::LivenessFailed { uptime: started.elapsed() }
            }
        }
    }

    /// The shared restart gate reached by a restart-eligible incarnation — a real
    /// crash, a clean run restarted under [`Always`](RestartPolicy::Always), or a
    /// liveness kill (`crashed == true`). Consults, in order:
    /// [`give_up_when`](Self::give_up_when) (crashes only),
    /// [`max_restarts`](Self::max_restarts), the E3 healthy-uptime reset, the
    /// [failure-storm guard](Self::storm_pause) (crashes only), and the backoff
    /// sleep; then advances the restart counters. Factoring it here keeps the
    /// `Ran(Ok)` and `LivenessFailed` arms from drifting apart.
    #[allow(clippy::too_many_arguments)]
    async fn gate_restart(
        &self,
        result: &ProcessResult<String>,
        crashed: bool,
        restarts: &mut u32,
        backoff_restarts: &mut u32,
        storm: &mut StormState,
        factor: f64,
        shared: &SessionShared,
    ) -> GateOutcome {
        if crashed
            && let Some(classifier) = &self.give_up_when
            && classifier(&GiveUpAttempt::Crashed(result))
        {
            return GateOutcome::GaveUp;
        }
        if self.max_restarts.is_some_and(|max| *restarts >= max) {
            return GateOutcome::Exhausted;
        }
        // E3: a run is "healthy" only if it stayed up at least as long as the
        // backoff ceiling — a clear "it's stable now" signal — whether it then
        // exited cleanly, crashed, or was liveness-killed. Resetting the
        // escalation there keeps a long-lived service off the ceiling, while a
        // tight loop (clean OR crashing OR promptly-wedging, each incarnation
        // shorter than max_backoff) keeps climbing and self-throttles. A uniform
        // uptime floor — rather than "any clean exit resets" — avoids a footgun:
        // under Always, an instantly-exiting `exit 0` loop would otherwise reset
        // every iteration and spin at the base delay.
        let healthy = result.duration() >= self.max_backoff;
        if healthy {
            *backoff_restarts = 0;
        }
        if crashed {
            match self.storm_gate(storm, shared).await {
                Wake::Cancelled => return GateOutcome::Cancelled,
                Wake::Stopped => return GateOutcome::Stopped,
                Wake::Elapsed => {}
            }
        }
        match self.sleep_backoff(*backoff_restarts, factor, shared).await {
            Wake::Cancelled => return GateOutcome::Cancelled,
            Wake::Stopped => return GateOutcome::Stopped,
            Wake::Elapsed => {}
        }
        *restarts = restarts.saturating_add(1);
        *backoff_restarts = backoff_restarts.saturating_add(1);
        GateOutcome::Restart
    }

    /// The synthetic [`ProcessResult`] for an incarnation a liveness check
    /// force-killed: a non-success [`Signalled`](crate::Outcome::Signalled)
    /// outcome (we killed it) stamped with how long it stayed up before wedging,
    /// so it is a *crash* for `is_success`/policy purposes and drives the E3
    /// backoff reset off its real uptime. Empty stdout/stderr — the wedged run's
    /// captured output was abandoned with the dropped incarnation.
    fn liveness_kill_result(&self, uptime: Duration) -> ProcessResult<String> {
        ProcessResult::new(
            self.command.program_name(),
            String::new(),
            String::new(),
            Outcome::Signalled(None),
            None,
        )
        .with_duration(uptime)
        .with_ok_codes(self.command.ok_codes_vec())
    }

    /// The synthetic [`ProcessResult`] reported as the final result when a
    /// graceful session stop ended an incarnation that produced only a
    /// `Cancelled` capture (a shared-group / capture-only child, whose only stop
    /// lever is its cancel token, so it has no honest exit to report). A
    /// non-success [`Signalled`](crate::Outcome::Signalled) — the child was
    /// stopped, not a clean exit. (An own-group child stopped through its
    /// graceful path *does* return an honest result, reported as-is.)
    fn stopped_result(&self) -> ProcessResult<String> {
        ProcessResult::new(
            self.command.program_name(),
            String::new(),
            String::new(),
            Outcome::Signalled(None),
            None,
        )
        .with_ok_codes(self.command.ok_codes_vec())
    }

    fn outcome(
        &self,
        final_result: ProcessResult<String>,
        restarts: u32,
        liveness_kills: u32,
        storm: &StormState,
        stopped: StopReason,
    ) -> SupervisionOutcome {
        SupervisionOutcome {
            final_result,
            restarts,
            stopped,
            storm_pauses: storm.pauses,
            liveness_kills,
        }
    }

    /// Run one incarnation and produce its [`ProcessResult`], publishing the
    /// live child (pid / start time / how to stop it) into `shared` for the
    /// duration so a [`SupervisionSession`] can observe and gracefully stop it.
    ///
    /// Obtains a live handle via [`start`](ProcessRunner::start) — the lever for
    /// a live pid and a graceful child-stop — and captures its output exactly as
    /// the classic path did: for a piped stdout the same [`output_string`] the
    /// bulk capture verb runs, otherwise a [`finish`] that drains only any
    /// independently-piped stderr and preserves the exit outcome. A **capture-only**
    /// runner (whose `start` is [`Unsupported`](crate::ErrorReason::Unsupported)) latches
    /// `spawn_capable` off and falls back to the plain capture verb — verbatim
    /// classic behavior, minus the live pid / graceful stop. The sole callee of
    /// [`run_incarnation`](Self::run_incarnation), which additionally races this
    /// against the liveness watcher when a [`health_check`](Self::health_check) is
    /// set.
    ///
    /// [`output_string`]: ProcessRunner::output_string
    /// [`finish`]: crate::RunningProcess::finish
    async fn run_to_result(
        &self,
        command: &Command,
        shared: &SessionShared,
        spawn_capable: &AtomicBool,
    ) -> Result<ProcessResult<String>> {
        if !spawn_capable.load(Ordering::Relaxed) {
            return self.capture_only(command).await;
        }
        let started = Instant::now();
        let handle = match self.runner.start(command).await {
            Ok(handle) => handle,
            Err(err) if matches!(err.reason(), crate::ErrorReason::Unsupported { .. }) => {
                // A capture-only runner: it exposes no live handle. Drive this and
                // every later incarnation through the plain capture verb instead —
                // no live pid / graceful stop, but supervision is unaffected.
                spawn_capable.store(false, Ordering::Relaxed);
                return self.capture_only(command).await;
            }
            Err(err) => return Err(err),
        };

        // Publish the live child and learn, atomically, whether a graceful stop
        // already landed (the stop-vs-spawn race). Its cancel token is the only
        // stop lever for a shared-group / capture-only child; an own-group child
        // is stopped gracefully through its private group.
        let stopper = ChildStopper {
            group: handle.own_group_handle(),
            inc_cancel: command.cancel_token().unwrap_or_default(),
        };
        if let Some(grace) =
            shared.publish_current(handle.pid(), handle.start_time(), stopper.clone())
        {
            // A stop was pending before this child became current: stop it now,
            // fire-and-forget — the output verb below observes the exit and the
            // loop ends with `Stopped`.
            tokio::spawn(async move { stopper.graceful_stop(grace).await });
        }
        // Release the published live child on *every* exit from here — a normal
        // return below, but crucially also this future being dropped mid-`await`
        // when a liveness kill wins `run_incarnation`'s `select!`. Without it the
        // dropped incarnation's `current` (and the `Arc<ProcessGroup>` clone its
        // `ChildStopper` holds) would outlive the force-kill until the next
        // `publish_current`, pinning the wedged child's group alive — and its
        // stale pid observable — across the whole restart backoff.
        let _current_guard = CurrentGuard { shared };

        // Returned directly: `_current_guard` above still drops (clearing
        // `current`) as this function unwinds, after the value below is produced.
        if command.stdout_is_piped() {
            handle.output_string().await
        } else {
            match handle.finish().await {
                Ok(crate::Finished {
                    outcome,
                    stderr,
                    stderr_truncated,
                }) => Ok(ProcessResult::new(
                    command.program_name(),
                    String::new(),
                    stderr,
                    outcome,
                    command.configured_timeout(),
                )
                .with_duration(started.elapsed())
                .with_truncated(stderr_truncated)
                .with_ok_codes(command.ok_codes_vec())),
                Err(err) => Err(err),
            }
        }
    }

    /// The classic capture path, unchanged: a piped stdout uses the bulk
    /// [`output_string`](ProcessRunner::output_string) verb; otherwise a
    /// [`start`](ProcessRunner::start) + [`finish`](crate::RunningProcess::finish)
    /// drains only any independently-piped stderr. Reached for a capture-only
    /// runner (whose `start` is `Unsupported`), where no live handle — hence no
    /// live pid or graceful child-stop — is available.
    async fn capture_only(&self, command: &Command) -> Result<ProcessResult<String>> {
        if command.stdout_is_piped() {
            return self.runner.output_string(command).await;
        }

        let started = Instant::now();
        let finished = self.runner.start(command).await?.finish().await?;
        let crate::Finished {
            outcome,
            stderr,
            stderr_truncated,
        } = finished;
        Ok(ProcessResult::new(
            command.program_name(),
            String::new(),
            stderr,
            outcome,
            command.configured_timeout(),
        )
        .with_duration(started.elapsed())
        .with_truncated(stderr_truncated)
        .with_ok_codes(command.ok_codes_vec()))
    }

    /// The terminal `Cancelled` error for supervision cut short by a cancel token
    /// firing during a backoff or storm pause.
    fn cancelled_err(&self, command: &Command) -> crate::Error {
        crate::ErrorReason::Cancelled {
            program: command.program_name(),
        }
        .into()
    }

    /// Whether this supervisor's configuration could genuinely need more than
    /// one run. [`RestartPolicy::Never`] never restarts, and an explicit
    /// [`max_restarts(0)`](Self::max_restarts) budget caps supervision at the
    /// first run regardless of policy — both mean a second incarnation can
    /// never happen, so a one-shot stdin source is perfectly safe for either.
    /// Every other policy/budget combination *could* restart (whether it
    /// actually does depends on the run's outcome, which isn't known yet).
    fn may_restart(&self) -> bool {
        !matches!(self.policy, RestartPolicy::Never) && self.max_restarts != Some(0)
    }

    /// Whether `self.command`'s stdin source is one that only feeds a single
    /// run and can't be replayed into a restart — a one-shot streaming source
    /// ([`Stdin::from_reader`](crate::Stdin::from_reader)/
    /// [`Stdin::from_lines`](crate::Stdin::from_lines)), and only when it is
    /// actually going to be fed to the child at all, as determined by
    /// [`effective_stdin_source`](Command::effective_stdin_source).
    fn has_unusable_one_shot_stdin(&self) -> bool {
        self.command
            .effective_stdin_source()
            .is_some_and(crate::Stdin::is_one_shot)
    }

    /// The typed, early error for [`may_restart`](Self::may_restart) +
    /// [`has_unusable_one_shot_stdin`](Self::has_unusable_one_shot_stdin) both
    /// holding: the same `ErrorReason::Io`/`InvalidInput` shape
    /// `runner::take_stdin_for_run` raises when a later incarnation actually
    /// hits the consumed source, but reported before any incarnation runs at
    /// all instead of after a wasted (and then endlessly repeated) attempt.
    fn one_shot_restart_err(&self) -> crate::Error {
        crate::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "`{}`: this supervisor's restart policy ({:?}, max_restarts: {:?}) may run \
                 the command more than once, but its stdin source is one-shot \
                 (Stdin::from_reader/from_lines) and only feeds a single incarnation — use \
                 Stdin::from_bytes/from_string/from_file/from_iter_lines (re-runnable stdin), \
                 or restrict this supervisor to RestartPolicy::Never/max_restarts(0) for a \
                 single run",
                self.command.program_name(),
                self.policy,
                self.max_restarts,
            ),
        ))
    }

    /// Sleep `delay`, waking early if the supervised command's
    /// [`cancel_on`](crate::Command::cancel_on) token fires ([`Wake::Cancelled`])
    /// or a [`SupervisionSession::stop`] is requested ([`Wake::Stopped`]) — so
    /// either ends supervision promptly instead of waiting out a (possibly long)
    /// delay. Cancellation takes precedence over a stop when both fire (a caller
    /// cancel is a terminal error). Without either, this just sleeps and returns
    /// [`Wake::Elapsed`]. A zero delay still observes an already-fired token so
    /// supervision ends promptly. For [`run`](Self::run) the stop token is never
    /// fired, so this reduces to the classic cancel-or-elapse behavior.
    #[must_use = "the returned Wake signals cancellation/stop — supervision must end unless Elapsed"]
    async fn sleep_or_cancel(&self, delay: Duration, shared: &SessionShared) -> Wake {
        let user = self.command.cancel_token();
        if delay.is_zero() {
            if user.is_some_and(|t| t.is_cancelled()) {
                return Wake::Cancelled;
            }
            if shared.stop.is_cancelled() {
                return Wake::Stopped;
            }
            return Wake::Elapsed;
        }
        let cancelled = async {
            match &user {
                Some(token) => token.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            () = cancelled => Wake::Cancelled,
            () = shared.stop.cancelled() => Wake::Stopped,
            () = tokio::time::sleep(delay) => Wake::Elapsed,
        }
    }

    /// The failure-storm gate, run before the backoff of every *failure*-
    /// driven restart: fold the failure into the decaying score and, past the
    /// threshold, sleep out one jittered [`storm_pause`](Self::storm_pause)
    /// and reset the score (a fresh window — the pause itself must not count
    /// as elapsed decay time for the *next* failure). Returns a non-[`Elapsed`](Wake::Elapsed)
    /// [`Wake`] if a cancel token / session stop fired during the pause
    /// (supervision should end). Brackets the pause window in the live status
    /// [`is_storm_paused`](SupervisionStatus::is_storm_paused).
    #[must_use = "the returned Wake signals cancellation/stop — supervision must end unless Elapsed"]
    async fn storm_gate(&self, storm: &mut StormState, shared: &SessionShared) -> Wake {
        let Some(pause) = self.storm_pause else {
            return Wake::Elapsed;
        };
        let now = tokio::time::Instant::now();
        let elapsed = storm
            .last_failure_at
            .map(|at| now.saturating_duration_since(at))
            .unwrap_or(Duration::ZERO);
        storm.last_failure_at = Some(now);
        storm.score = decayed_failure_score(storm.score, elapsed, self.failure_decay);
        let tripped = storm.score > self.failure_threshold;
        if !tripped {
            return Wake::Elapsed;
        }
        let pause = apply_jitter(pause, self.jitter);
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: "processkit",
            pause_ms = pause.as_millis() as u64,
            "supervisor failure storm — pausing restarts"
        );
        // Bracket exactly the pause window in the live status: paused while the
        // jittered sleep runs, cleared the instant it returns (or is cut short).
        shared.set_storm_paused(true);
        let wake = self.sleep_or_cancel(pause, shared).await;
        shared.set_storm_paused(false);
        if !matches!(wake, Wake::Elapsed) {
            return wake;
        }
        storm.score = 0.0;
        storm.last_failure_at = None;
        storm.pauses = storm.pauses.saturating_add(1);
        Wake::Elapsed
    }

    /// Sleep out the delay before the `restarts`-th (0-based) restart. Returns a
    /// non-[`Elapsed`](Wake::Elapsed) [`Wake`] if a cancel token / session stop
    /// fired during the backoff.
    #[must_use = "the returned Wake signals cancellation/stop — supervision must end unless Elapsed"]
    async fn sleep_backoff(&self, restarts: u32, factor: f64, shared: &SessionShared) -> Wake {
        let delay = backoff_delay(self.backoff_base, factor, restarts, self.max_backoff);
        let delay = apply_jitter(delay, self.jitter);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            restart = restarts + 1,
            delay_ms = delay.as_millis() as u64,
            "supervisor restarting child"
        );
        self.sleep_or_cancel(delay, shared).await
    }
}

impl<R: ProcessRunner + 'static> Supervisor<R> {
    /// Start supervising in the background and return a live
    /// [`SupervisionSession`] — the interactive counterpart to
    /// [`run`](Self::run). Supervision runs on a detached task from the moment
    /// this returns; poll the session's [`status`](SupervisionSession::status)
    /// (restart count, storm-pause flag, the current child's pid / start time),
    /// ask it to [`stop`](SupervisionSession::stop) gracefully, or
    /// [`wait`](SupervisionSession::wait) for the final
    /// [`SupervisionOutcome`] — exactly what [`run`](Self::run) would have
    /// returned.
    ///
    /// The live status is an **addition** to the exit-driven
    /// [`RestartPolicy`]/[`stop_when`](Self::stop_when)/tracing instrumentation,
    /// never a replacement — supervision behaves identically to [`run`](Self::run).
    ///
    /// Requires a `'static` runner because supervision moves onto a spawned task;
    /// the borrowed shared-group form (`with_runner(&group)`) is available only
    /// through [`run`](Self::run). Own it — `with_runner(group)` by value, or an
    /// `Arc<ProcessGroup>` runner — to supervise a shared group via a session.
    ///
    /// Must be called from within a Tokio runtime (it spawns the supervision
    /// task).
    #[must_use = "a dropped SupervisionSession aborts supervision immediately — hold it, then wait()/stop()"]
    pub fn start(self) -> SupervisionSession {
        let shared = Arc::new(SessionShared::new());
        let (tx, rx) = oneshot::channel();
        let task_shared = Arc::clone(&shared);
        let handle = tokio::spawn(async move {
            let outcome = self.drive(Arc::clone(&task_shared)).await;
            // Flip the live status to inactive before the outcome becomes
            // observable, so an awaiter that then reads `status()` never sees
            // `is_active() == true` on a finished session.
            task_shared.mark_inactive();
            let _ = tx.send(outcome);
        });
        SupervisionSession {
            shared,
            completion: Some(rx),
            abort: handle.abort_handle(),
        }
    }
}

struct StormState {
    score: f64,
    last_failure_at: Option<tokio::time::Instant>,
    pauses: u32,
}

impl StormState {
    fn new() -> Self {
        StormState {
            score: 0.0,
            last_failure_at: None,
            pauses: 0,
        }
    }
}

/// Fold one failure into the decaying score: the previous score halves every
/// `half_life` of elapsed time, then the new failure adds `1`. A zero
/// half-life keeps no history (every failure scores exactly `1.0`); a
/// non-finite previous score resets rather than propagating.
fn decayed_failure_score(prev: f64, elapsed: Duration, half_life: Duration) -> f64 {
    if half_life.is_zero() {
        return 1.0;
    }
    let halflives = elapsed.as_secs_f64() / half_life.as_secs_f64();
    let decayed = prev * 0.5_f64.powf(halflives);
    if decayed.is_finite() {
        decayed + 1.0
    } else {
        1.0
    }
}

/// `min(base × factor^n, cap)`, delegating to the shared
/// [`backoff::capped_exponential`](crate::backoff) core (also used by
/// `RetryPolicy::backoff_at`).
fn backoff_delay(base: Duration, factor: f64, n: u32, cap: Duration) -> Duration {
    crate::backoff::capped_exponential(base, factor, n, cap)
}

/// Multiply `delay` by a uniform random factor in `[0.5, 1.5)` when `enabled`.
fn apply_jitter(delay: Duration, enabled: bool) -> Duration {
    if !enabled || delay.is_zero() {
        return delay;
    }
    let scaled = delay.as_secs_f64() * jitter_factor();
    Duration::try_from_secs_f64(scaled)
        .unwrap_or(crate::MAX_DEADLINE)
        .min(crate::MAX_DEADLINE)
}

/// A pseudo-random factor in `[0.5, 1.5)`, built from the shared
/// [`backoff::unit_random_f64`](crate::backoff) source.
fn jitter_factor() -> f64 {
    0.5 + crate::backoff::unit_random_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Stdin;
    use crate::doubles::{Reply, ScriptedRunner};
    use crate::result::Outcome;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    const ALL_RESTART_POLICIES: &[RestartPolicy] = &[
        RestartPolicy::Always,
        RestartPolicy::OnCrash,
        RestartPolicy::Never,
    ];
    const ALL_STOP_REASONS: &[StopReason] = &[
        StopReason::Predicate,
        StopReason::PolicySatisfied,
        StopReason::GaveUp,
        StopReason::RestartsExhausted,
        StopReason::Unhealthy,
        StopReason::Stopped,
    ];

    #[test]
    fn restart_policy_name_pins_each_variant() {
        assert_eq!(RestartPolicy::Always.name(), "always");
        assert_eq!(RestartPolicy::OnCrash.name(), "on_crash");
        assert_eq!(RestartPolicy::Never.name(), "never");
    }

    #[test]
    fn stop_reason_name_pins_each_variant() {
        assert_eq!(StopReason::Predicate.name(), "predicate");
        assert_eq!(StopReason::PolicySatisfied.name(), "policy_satisfied");
        assert_eq!(StopReason::GaveUp.name(), "gave_up");
        assert_eq!(StopReason::RestartsExhausted.name(), "restarts_exhausted");
        assert_eq!(StopReason::Unhealthy.name(), "unhealthy");
        assert_eq!(StopReason::Stopped.name(), "stopped");
    }

    #[test]
    fn supervisor_enum_names_round_trip_every_variant() {
        for &p in ALL_RESTART_POLICIES {
            assert_eq!(RestartPolicy::from_name(p.name()), Some(p));
        }
        for &r in ALL_STOP_REASONS {
            assert_eq!(StopReason::from_name(r.name()), Some(r));
        }
    }

    #[test]
    fn supervisor_enum_from_name_rejects_unknown_without_defaulting() {
        assert_eq!(RestartPolicy::from_name("OnCrash"), None);
        assert_eq!(RestartPolicy::from_name("on-crash"), None);
        assert_eq!(RestartPolicy::from_name(""), None);
        assert_eq!(StopReason::from_name("gaveup"), None);
        assert_eq!(StopReason::from_name("exhausted"), None);
    }

    /// Per-call outcome sequence; panics if exhausted, so an unexpected restart fails loudly.
    struct SeqRunner {
        replies: Mutex<VecDeque<Result<ProcessResult<String>>>>,
    }

    impl SeqRunner {
        fn new(replies: Vec<Result<ProcessResult<String>>>) -> Self {
            SeqRunner {
                replies: Mutex::new(replies.into()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProcessRunner for SeqRunner {
        async fn output_string(&self, _command: &Command) -> Result<ProcessResult<String>> {
            self.replies
                .lock()
                .expect("replies lock")
                .pop_front()
                .expect("SeqRunner ran out of scripted replies")
        }
    }

    fn ok() -> Result<ProcessResult<String>> {
        Ok(ProcessResult::new(
            "fake".into(),
            "out".into(),
            String::new(),
            Outcome::Exited(0),
            None,
        ))
    }

    fn fail(code: i32) -> Result<ProcessResult<String>> {
        Ok(ProcessResult::new(
            "fake".into(),
            String::new(),
            "boom".into(),
            Outcome::Exited(code),
            None,
        ))
    }

    /// A crash whose incarnation reports having stayed up for `uptime` (stamped
    /// on the result the way a real run's wall-clock is), for the E3 uptime path.
    fn fail_after(code: i32, uptime: Duration) -> Result<ProcessResult<String>> {
        Ok(ProcessResult::new(
            "fake".into(),
            String::new(),
            "boom".into(),
            Outcome::Exited(code),
            None,
        )
        .with_duration(uptime))
    }

    fn timeout() -> Result<ProcessResult<String>> {
        Ok(ProcessResult::new(
            "fake".into(),
            String::new(),
            String::new(),
            Outcome::TimedOut,
            Some(Duration::from_secs(1)),
        ))
    }

    fn spawn_err() -> Result<ProcessResult<String>> {
        Err(crate::ErrorReason::Spawn {
            program: "fake".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "no such binary"),
        }
        .into())
    }

    fn supervise(runner: SeqRunner) -> Supervisor<SeqRunner> {
        Supervisor::new(Command::new("fake"))
            .with_runner(runner)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
    }

    /// Like [`supervise`], but with `stdin` configured on the underlying
    /// `Command` — for the one-shot-stdin-vs-restart guard tests below.
    fn supervise_with_stdin(runner: SeqRunner, stdin: crate::Stdin) -> Supervisor<SeqRunner> {
        Supervisor::new(Command::new("fake").stdin(stdin))
            .with_runner(runner)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
    }

    #[tokio::test]
    async fn redirected_stdout_is_discarded_but_still_supervised() {
        let path = std::env::temp_dir().join("processkit-supervisor-file-redirect.log");
        let outcome = Supervisor::new(Command::new("server").stdout_file(path))
            .restart(RestartPolicy::Never)
            .with_runner(ScriptedRunner::new().fallback(Reply::ok("hidden").with_stderr("warn")))
            .run()
            .await
            .expect("a redirected service is supervised through start/finish");

        assert_eq!(outcome.final_result.stdout(), "");
        assert_eq!(outcome.final_result.stderr(), "warn");
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    #[test]
    fn supervision_capture_default_bounds_an_unbounded_command() {
        let unbounded = Command::new("server");
        let policy = default_supervision_capture(&unbounded);
        assert_eq!(
            policy.max_lines,
            Some(DEFAULT_SUPERVISION_TAIL),
            "an unbounded supervised command must default to a bounded tail"
        );
        assert_eq!(policy.overflow, crate::OverflowMode::DropOldest);

        let unbounded_fail_loud = Command::new("server").output_buffer(
            crate::OutputBufferPolicy::unbounded().with_overflow(crate::OverflowMode::Error),
        );
        let policy = default_supervision_capture(&unbounded_fail_loud);
        assert_eq!(policy.max_lines, Some(DEFAULT_SUPERVISION_TAIL));
        assert_eq!(
            policy.overflow,
            crate::OverflowMode::Error,
            "an unbounded+Error command must become a bounded fail-loud"
        );

        let explicit =
            Command::new("server").output_buffer(crate::OutputBufferPolicy::fail_loud(50));
        let policy = default_supervision_capture(&explicit);
        assert_eq!(policy.max_lines, Some(50), "an explicit cap is respected");
        assert_eq!(policy.overflow, crate::OverflowMode::Error);
    }

    #[tokio::test]
    async fn run_applies_the_capture_policy_to_each_incarnation() {
        use std::sync::Arc;

        #[derive(Clone)]
        struct CapturingRunner(Arc<Mutex<Option<OutputBufferPolicy>>>);
        #[async_trait::async_trait]
        impl ProcessRunner for CapturingRunner {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                *self.0.lock().expect("seen lock") = Some(command.output_buffer_policy());
                ok()
            }
        }

        let seen = Arc::new(Mutex::new(None));
        Supervisor::new(Command::new("server"))
            .restart(RestartPolicy::Never)
            .with_runner(CapturingRunner(seen.clone()))
            .run()
            .await
            .expect("supervision");
        assert_eq!(
            seen.lock().unwrap().expect("ran").max_lines,
            Some(DEFAULT_SUPERVISION_TAIL)
        );

        let seen = Arc::new(Mutex::new(None));
        Supervisor::new(Command::new("server"))
            .restart(RestartPolicy::Never)
            .capture(crate::OutputBufferPolicy::unbounded())
            .with_runner(CapturingRunner(seen.clone()))
            .run()
            .await
            .expect("supervision");
        assert_eq!(seen.lock().unwrap().expect("ran").max_lines, None);
    }

    #[tokio::test]
    async fn on_crash_restarts_until_success() {
        let outcome = supervise(SeqRunner::new(vec![fail(1), fail(1), ok()]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        assert!(outcome.final_result.is_success());
    }

    #[tokio::test]
    async fn zero_max_restarts_means_a_single_run() {
        let outcome = supervise(SeqRunner::new(vec![fail(1), ok()]))
            .max_restarts(0)
            .run()
            .await
            .expect("supervision completes with the single run's result");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
        assert_eq!(outcome.final_result.code(), Some(1));
    }

    #[tokio::test]
    async fn on_crash_accepts_a_clean_first_run() {
        let outcome = supervise(SeqRunner::new(vec![ok()]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    #[tokio::test]
    async fn predicate_beats_policy() {
        let outcome = supervise(SeqRunner::new(vec![ok()]))
            .restart(RestartPolicy::Always)
            .stop_when(|res| res.code() == Some(0))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.stopped, StopReason::Predicate);
    }

    #[tokio::test]
    async fn always_restarts_clean_runs_until_predicate() {
        let seen = AtomicU32::new(0);
        let outcome = supervise(SeqRunner::new(vec![ok(), ok(), ok()]))
            .restart(RestartPolicy::Always)
            .stop_when(move |_| seen.fetch_add(1, Ordering::SeqCst) == 2)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2, "third run matched the predicate");
        assert_eq!(outcome.stopped, StopReason::Predicate);
    }

    #[tokio::test]
    async fn never_reports_a_failing_run_without_restarting() {
        let outcome = supervise(SeqRunner::new(vec![fail(3)]))
            .restart(RestartPolicy::Never)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        assert_eq!(outcome.final_result.code(), Some(3));
    }

    #[tokio::test]
    async fn exhausting_the_budget_reports_the_last_failure() {
        let runner = SeqRunner::new(vec![fail(7), fail(7), fail(7)]);
        let outcome = supervise(runner)
            .max_restarts(2)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2, "two restarts = three runs");
        assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
        assert_eq!(outcome.final_result.code(), Some(7));
    }

    #[tokio::test]
    async fn give_up_when_stops_a_permanently_crashing_run() {
        let outcome = supervise(SeqRunner::new(vec![fail(13)]))
            .give_up_when(
                |attempt| matches!(attempt, GiveUpAttempt::Crashed(res) if res.code() == Some(13)),
            )
            .run()
            .await
            .expect("supervision");
        assert_eq!(
            outcome.restarts, 0,
            "must not restart a run the classifier recognized as permanent"
        );
        assert_eq!(outcome.stopped, StopReason::GaveUp);
        assert_eq!(outcome.final_result.code(), Some(13));
    }

    #[tokio::test]
    async fn give_up_when_does_not_affect_an_unrecognized_transient_crash() {
        let outcome = supervise(SeqRunner::new(vec![fail(1), ok()]))
            .give_up_when(
                |attempt| matches!(attempt, GiveUpAttempt::Crashed(res) if res.code() == Some(13)),
            )
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 1, "an unrecognized crash still restarts");
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    #[tokio::test]
    async fn give_up_when_stops_a_permanent_spawn_failure() {
        // Without a classifier this would restart forever (and panic once the
        // scripted single reply is exhausted) — the ENOENT-style case from the
        // task: a mistyped program name never recovers on its own.
        let err = supervise(SeqRunner::new(vec![spawn_err()]))
            .give_up_when(|attempt| match attempt {
                GiveUpAttempt::Failed(err) => {
                    matches!(err.reason(), crate::ErrorReason::Spawn { .. })
                }
                GiveUpAttempt::Crashed(_) => false,
            })
            .run()
            .await
            .expect_err("a classified-permanent spawn failure must not restart forever");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Spawn { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn give_up_when_takes_precedence_over_an_exhausted_budget() {
        let outcome = supervise(SeqRunner::new(vec![fail(13)]))
            .max_restarts(0)
            .give_up_when(
                |attempt| matches!(attempt, GiveUpAttempt::Crashed(res) if res.code() == Some(13)),
            )
            .run()
            .await
            .expect("supervision");
        assert_eq!(
            outcome.stopped,
            StopReason::GaveUp,
            "a permanent-failure verdict wins over an exhausted budget"
        );
    }

    #[tokio::test]
    async fn give_up_when_is_not_consulted_when_the_policy_already_stops() {
        let outcome = supervise(SeqRunner::new(vec![fail(13)]))
            .restart(RestartPolicy::Never)
            .give_up_when(|_| panic!("classifier must not run once the policy already stopped"))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    #[tokio::test]
    async fn a_timeout_counts_as_a_crash() {
        let outcome = supervise(SeqRunner::new(vec![timeout(), ok()]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 1);
        assert!(outcome.final_result.is_success());
    }

    #[tokio::test]
    async fn an_accepted_nonzero_exit_is_not_a_crash() {
        let accepted = Ok(ProcessResult::new(
            "fake".into(),
            "out".into(),
            String::new(),
            Outcome::Exited(2),
            None,
        )
        .with_ok_codes(vec![0, 2]));
        let outcome = supervise(SeqRunner::new(vec![accepted]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 0, "an accepted exit code is not a crash");
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        assert!(outcome.final_result.is_success());
    }

    #[tokio::test]
    async fn a_rejected_zero_exit_is_a_crash() {
        let rejected_zero = Ok(ProcessResult::new(
            "fake".into(),
            String::new(),
            String::new(),
            Outcome::Exited(0),
            None,
        )
        .with_ok_codes(vec![1]));
        let outcome = supervise(SeqRunner::new(vec![rejected_zero, ok()]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 1, "a rejected exit code is a crash");
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        assert!(outcome.final_result.is_success());
    }

    #[tokio::test]
    async fn terminal_spawn_error_surfaces_as_err() {
        let err = supervise(SeqRunner::new(vec![spawn_err(), spawn_err()]))
            .max_restarts(1)
            .run()
            .await
            .expect_err("the budget-exhausting attempt errored");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Spawn { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn spawn_error_is_retried_like_a_crash() {
        let outcome = supervise(SeqRunner::new(vec![spawn_err(), ok()]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 1);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    #[tokio::test]
    async fn cancelled_incarnation_is_terminal_under_always() {
        // Always would restart any failure; Cancelled must end supervision at
        // once — the second reply is never consumed (SeqRunner panics if so).
        let err = supervise(SeqRunner::new(vec![
            Err(crate::ErrorReason::Cancelled {
                program: "fake".into(),
            }
            .into()),
            ok(),
        ]))
        .restart(RestartPolicy::Always)
        .max_restarts(5)
        .run()
        .await
        .expect_err("a cancelled incarnation is terminal");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Cancelled { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn never_returns_a_spawn_error_directly() {
        let err = supervise(SeqRunner::new(vec![spawn_err()]))
            .restart(RestartPolicy::Never)
            .run()
            .await
            .expect_err("Never does not retry a spawn failure");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Spawn { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_doubles_per_restart_without_jitter() {
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1), fail(1), ok()]))
            .backoff(Duration::from_millis(200), 2.0)
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2);
        assert_eq!(start.elapsed(), Duration::from_millis(600)); // 200 + 400
    }

    #[tokio::test(start_paused = true)]
    async fn max_backoff_caps_the_delay() {
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1), fail(1), ok()]))
            .backoff(Duration::from_millis(200), 2.0)
            .max_backoff(Duration::from_millis(300))
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2);
        assert_eq!(start.elapsed(), Duration::from_millis(500)); // 200 + 400→300
    }

    #[tokio::test(start_paused = true)]
    async fn jitter_stays_within_its_band() {
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1), ok()]))
            .backoff(Duration::from_millis(1000), 1.0)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 1);
        let waited = start.elapsed();
        // ns-rounding can push a factor just under 1.5 to exactly 1.5×.
        assert!(
            waited >= Duration::from_millis(500) && waited <= Duration::from_millis(1500),
            "jittered delay out of [0.5, 1.5] band: {waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonsense_backoff_factor_decays_to_constant_delay() {
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1), fail(1), ok()]))
            .backoff(Duration::from_millis(100), 0.0)
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2);
        assert_eq!(start.elapsed(), Duration::from_millis(200));
    }

    #[test]
    fn jitter_factor_is_in_band() {
        for _ in 0..256 {
            let f = jitter_factor();
            assert!((0.5..1.5).contains(&f), "factor out of band: {f}");
        }
    }

    #[test]
    fn decayed_failure_score_math() {
        let hl = Duration::from_secs(30);
        assert_eq!(decayed_failure_score(0.0, Duration::ZERO, hl), 1.0);
        assert_eq!(decayed_failure_score(1.0, Duration::ZERO, hl), 2.0);
        assert_eq!(decayed_failure_score(2.0, hl, hl), 2.0); // one half-life: 2×0.5+1
        assert_eq!(decayed_failure_score(4.0, hl, hl), 3.0);
        let aged = decayed_failure_score(8.0, Duration::from_secs(3000), hl);
        assert!((aged - 1.0).abs() < 1e-9, "got {aged}"); // many half-lives → ≈1
        assert_eq!(
            decayed_failure_score(100.0, Duration::ZERO, Duration::ZERO),
            1.0 // zero half-life keeps no history
        );
        assert_eq!(decayed_failure_score(f64::NAN, Duration::ZERO, hl), 1.0); // poisoned → reset
    }

    #[tokio::test(start_paused = true)]
    async fn storm_guard_is_off_by_default() {
        let start = tokio::time::Instant::now();
        let outcome = supervise(SeqRunner::new(vec![
            fail(1),
            fail(1),
            fail(1),
            fail(1),
            ok(),
        ]))
        .run()
        .await
        .expect("supervision");
        assert_eq!(outcome.storm_pauses, 0);
        assert_eq!(start.elapsed(), Duration::ZERO, "no hidden pauses");
    }

    #[tokio::test(start_paused = true)]
    async fn storm_trips_past_the_threshold() {
        // Zero backoff → zero decay: scores 1, 2, 3; third crosses 2.5 → one pause.
        let start = tokio::time::Instant::now();
        let outcome = supervise(SeqRunner::new(vec![fail(1), fail(1), fail(1), ok()]))
            .storm_pause(Duration::from_secs(1))
            .failure_threshold(2.5)
            .failure_decay(Duration::from_secs(1000))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 3);
        assert_eq!(outcome.storm_pauses, 1);
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn spaced_failures_decay_below_the_threshold() {
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1), fail(1), fail(1), ok()]))
            .backoff(Duration::from_secs(10), 1.0)
            .jitter(false)
            .storm_pause(Duration::from_secs(1))
            .failure_threshold(2.5)
            .failure_decay(Duration::from_secs(1))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 3);
        assert_eq!(outcome.storm_pauses, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn storm_pause_resets_the_score() {
        // Threshold 1.5: scores 1, 2(pause), 1, 2(pause) — reset after each pause.
        let outcome = supervise(SeqRunner::new(vec![
            fail(1),
            fail(1),
            fail(1),
            fail(1),
            ok(),
        ]))
        .storm_pause(Duration::from_secs(1))
        .failure_threshold(1.5)
        .failure_decay(Duration::from_secs(1000))
        .run()
        .await
        .expect("supervision");
        assert_eq!(outcome.restarts, 4);
        assert_eq!(outcome.storm_pauses, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_budget_wins_over_the_storm_gate() {
        let start = tokio::time::Instant::now();
        let outcome = supervise(SeqRunner::new(vec![fail(1), fail(1)]))
            .max_restarts(1)
            .storm_pause(Duration::from_secs(60))
            .failure_threshold(1.5)
            .failure_decay(Duration::from_secs(1000))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
        assert_eq!(outcome.storm_pauses, 0);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn storm_pause_is_jittered_within_the_band() {
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1), ok()]))
            .backoff(Duration::ZERO, 1.0)
            .storm_pause(Duration::from_millis(1000))
            .failure_threshold(0.5)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.storm_pauses, 1);
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(500) && waited <= Duration::from_millis(1500),
            "jittered storm pause out of [0.5, 1.5] band: {waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn clean_restarts_under_always_do_not_feed_the_storm_score() {
        let seen = AtomicU32::new(0);
        let outcome = supervise(SeqRunner::new(vec![ok(), ok(), ok()]))
            .restart(RestartPolicy::Always)
            .storm_pause(Duration::from_secs(60))
            .failure_threshold(1.5)
            .failure_decay(Duration::from_secs(1000))
            .stop_when(move |_| seen.fetch_add(1, Ordering::SeqCst) == 2)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 2);
        assert_eq!(outcome.storm_pauses, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_is_terminal_before_any_storm_pause() {
        let start = tokio::time::Instant::now();
        let err = supervise(SeqRunner::new(vec![Err(crate::ErrorReason::Cancelled {
            program: "fake".into(),
        }
        .into())]))
        .storm_pause(Duration::from_secs(60))
        .failure_threshold(0.0)
        .run()
        .await
        .expect_err("cancelled is terminal");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Cancelled { .. }),
            "got {err:?}"
        );
        assert_eq!(start.elapsed(), Duration::ZERO, "no storm pause was taken");
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_that_outlived_the_backoff_ceiling_resets_the_escalation() {
        // E3 (uptime path): a crash whose incarnation stayed up at least as long as
        // max_backoff is "healthy" — the escalation resets to base, so a long-lived
        // service that crashes occasionally isn't pinned at the ceiling. 5 such
        // crashes at a 1s base × 2 factor, cap 30s: with the reset the total backoff
        // is ≈5s (5 × base); without it the delays climb 1+2+4+8+16 = 31s. Each
        // incarnation *reports* a 40s uptime (the fake returns instantly; only the
        // stamped duration drives the reset), so this exercises the `duration() >=
        // max_backoff` branch, not the fake's zero-duration path.
        let long = Duration::from_secs(40); // ≥ max_backoff (30s)
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![
                fail_after(1, long),
                fail_after(1, long),
                fail_after(1, long),
                fail_after(1, long),
                fail_after(1, long),
                fail_after(1, long),
            ]))
            .restart(RestartPolicy::OnCrash)
            .max_restarts(5)
            .backoff(Duration::from_secs(1), 2.0)
            .max_backoff(Duration::from_secs(30))
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 5);
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "an uptime ≥ max_backoff must reset the backoff (≈5s), not escalate (31s); took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_short_lived_crash_loop_keeps_escalating() {
        // E3 footgun guard: a crash that did NOT stay up as long as max_backoff is
        // not healthy, so a tight loop (here zero-uptime fakes) keeps climbing. 4
        // restarts at a 1s base × 2 factor: delays 1+2+4+8 = 15s (escalating), not
        // 4s (reset). Proves the uptime floor throttles instant loops (clean or
        // crashing) — including `exit 0` spin under Always.
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![
                fail(1),
                fail(1),
                fail(1),
                fail(1),
                fail(1),
            ]))
            .restart(RestartPolicy::OnCrash)
            .max_restarts(4)
            .backoff(Duration::from_secs(1), 2.0)
            .max_backoff(Duration::from_secs(30))
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 4);
        assert!(
            start.elapsed() >= Duration::from_secs(15),
            "a short-lived crash loop must escalate (1+2+4+8=15s), not reset; took {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_cancellable() {
        // E1: a cancel token firing 100ms into a backoff ends supervision promptly
        // with Cancelled. The backoff is a 60s base capped to a 60s max_backoff, so
        // a *broken* cancel would wait the full 60s; the token fires at 100ms, so a
        // working cancel returns in well under 1s (virtual time).
        let token = crate::CancellationToken::new();
        let sv = Supervisor::new(Command::new("fake").cancel_on(token.clone()))
            .with_runner(SeqRunner::new(vec![fail(1), fail(1)]))
            .restart(RestartPolicy::Always)
            .backoff(Duration::from_secs(60), 1.0)
            .max_backoff(Duration::from_secs(60))
            .jitter(false);
        let canceller = tokio::spawn({
            let token = token.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                token.cancel();
            }
        });
        let start = tokio::time::Instant::now();
        let err = sv.run().await.expect_err("cancelled during backoff");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Cancelled { .. }),
            "got {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "backoff must be cancellable promptly (~100ms), took {:?}",
            start.elapsed()
        );
        canceller.await.expect("canceller");
    }

    #[test]
    fn backoff_delay_math() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(30);
        assert_eq!(backoff_delay(base, 2.0, 0, cap), base);
        assert_eq!(backoff_delay(base, 2.0, 1, cap), Duration::from_millis(200));
        assert_eq!(backoff_delay(base, 2.0, 3, cap), Duration::from_millis(800));
        assert_eq!(backoff_delay(base, 2.0, 1_000, cap), cap); // astronomic → cap
        assert_eq!(backoff_delay(Duration::ZERO, 2.0, 5, cap), Duration::ZERO);
    }

    #[test]
    fn apply_jitter_clamps_instead_of_overflowing() {
        // near-Duration::MAX × up-to-1.5x must clamp, not panic in mul_f64.
        let jittered = apply_jitter(Duration::MAX, true);
        assert!(jittered <= crate::MAX_DEADLINE, "clamped, got {jittered:?}");
        assert_eq!(apply_jitter(Duration::MAX, false), Duration::MAX);
        assert_eq!(apply_jitter(Duration::ZERO, true), Duration::ZERO);
        let normal = apply_jitter(Duration::from_secs(10), true);
        assert!(normal >= Duration::from_secs(5) && normal < Duration::from_secs(15));
    }

    // --- One-shot stdin vs. a restart-capable policy (T-086) ---------------

    #[tokio::test(start_paused = true)]
    async fn one_shot_stdin_blocks_an_unlimited_oncrash_supervisor_before_any_run() {
        // OnCrash + unlimited restarts could always need a second incarnation.
        // An empty SeqRunner guarantees a panic if the guard ever lets a run
        // through.
        let start = tokio::time::Instant::now();
        let err = supervise_with_stdin(SeqRunner::new(vec![]), Stdin::from_reader(&b"x"[..]))
            .run()
            .await
            .expect_err("an unlimited OnCrash policy could need a second incarnation");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Io(_)),
            "got {err:?}"
        );
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "must fail before the first run/backoff, not after one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_shot_stdin_blocks_an_always_supervisor_before_any_run() {
        // Always restarts even a clean run, so a one-shot source is just as
        // unusable here as under OnCrash.
        let start = tokio::time::Instant::now();
        let err = supervise_with_stdin(
            SeqRunner::new(vec![]),
            Stdin::from_lines(tokio_stream::iter(vec!["x".to_owned()])),
        )
        .restart(RestartPolicy::Always)
        .run()
        .await
        .expect_err("Always could always need a second incarnation");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Io(_)),
            "got {err:?}"
        );
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn one_shot_stdin_blocks_a_finite_restart_budget_before_any_run() {
        // A finite but nonzero budget still allows a second incarnation. The
        // scripted (would-be) spawn error is never consumed, proving the
        // guard fires ahead of the first attempt regardless of what that
        // attempt would have reported.
        let start = tokio::time::Instant::now();
        let err = supervise_with_stdin(
            SeqRunner::new(vec![spawn_err()]),
            Stdin::from_reader(&b"x"[..]),
        )
        .max_restarts(2)
        .run()
        .await
        .expect_err("max_restarts(2) could still need a second incarnation");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Io(_)),
            "got {err:?}"
        );
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn one_shot_stdin_is_allowed_under_restart_policy_never() {
        // Never runs at most once, so a one-shot source is fine — the guard
        // must not fire, and the single scripted run must actually execute.
        let outcome =
            supervise_with_stdin(SeqRunner::new(vec![fail(3)]), Stdin::from_reader(&b"x"[..]))
                .restart(RestartPolicy::Never)
                .run()
                .await
                .expect("a single permitted run with one-shot stdin must succeed");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        assert_eq!(outcome.final_result.code(), Some(3));
    }

    #[tokio::test(start_paused = true)]
    async fn one_shot_stdin_is_allowed_under_a_zero_restart_budget() {
        // max_restarts(0) also caps supervision at a single run under the
        // default OnCrash policy — same allowance as RestartPolicy::Never.
        let outcome =
            supervise_with_stdin(SeqRunner::new(vec![fail(1)]), Stdin::from_reader(&b"x"[..]))
                .max_restarts(0)
                .run()
                .await
                .expect("a single permitted run with one-shot stdin must succeed");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
    }

    #[tokio::test(start_paused = true)]
    async fn keep_stdin_open_ignores_a_configured_one_shot_source() {
        // keep_stdin_open() hands the pipe to the caller and never feeds the
        // configured Stdin source to the child at all, so it can't be
        // "consumed" by an incarnation — the guard must not fire, and
        // restarts proceed exactly as they would with no stdin configured.
        let outcome = Supervisor::new(
            Command::new("fake")
                .stdin(Stdin::from_reader(&b"x"[..]))
                .keep_stdin_open(),
        )
        .with_runner(SeqRunner::new(vec![fail(1), ok()]))
        .backoff(Duration::ZERO, 1.0)
        .jitter(false)
        .run()
        .await
        .expect("keep_stdin_open bypasses the one-shot guard");
        assert_eq!(outcome.restarts, 1);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    #[tokio::test(start_paused = true)]
    async fn reusable_stdin_sources_still_restart_under_unlimited_oncrash() {
        // Bytes/string/file/iter-lines sources are replayable, so an
        // unlimited restart-capable policy must keep working exactly as it
        // did before this guard existed.
        for stdin in [
            Stdin::from_bytes(b"x".to_vec()),
            Stdin::from_string("x"),
            Stdin::from_iter_lines(["a", "b"]),
        ] {
            let outcome = supervise_with_stdin(SeqRunner::new(vec![fail(1), fail(1), ok()]), stdin)
                .run()
                .await
                .expect("a reusable stdin source must not trip the one-shot guard");
            assert_eq!(outcome.restarts, 2);
            assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        }
    }

    #[test]
    fn one_shot_restart_err_names_the_program_and_is_understandable() {
        let sv = Supervisor::new(Command::new("fake").stdin(Stdin::from_reader(&b"x"[..])))
            .with_runner(SeqRunner::new(vec![]));
        let err = sv.one_shot_restart_err();
        let msg = err.to_string();
        assert!(
            msg.contains("fake"),
            "message should name the program: {msg}"
        );
        assert!(
            msg.contains("one-shot"),
            "message should explain the actual problem: {msg}"
        );
    }

    // --- Liveness health checks (T-141) ------------------------------------

    #[tokio::test(start_paused = true)]
    async fn health_watch_trips_after_the_consecutive_failure_threshold() {
        let hc = HealthCheck {
            probe: Box::new(|| Box::pin(async { false })),
            interval: Duration::from_millis(100),
        };
        let start = tokio::time::Instant::now();
        hc.watch(3).await;
        // First probe fires one interval in (100ms); the third consecutive
        // failure (300ms) trips the watch.
        assert_eq!(start.elapsed(), Duration::from_millis(300));
    }

    #[tokio::test(start_paused = true)]
    async fn health_watch_resets_the_streak_on_a_healthy_probe() {
        let calls = AtomicU32::new(0);
        let hc = HealthCheck {
            probe: Box::new(move || {
                // Healthy only on the 3rd probe; unhealthy otherwise.
                let healthy = calls.fetch_add(1, Ordering::SeqCst) == 2;
                Box::pin(async move { healthy })
            }),
            interval: Duration::from_millis(100),
        };
        let start = tokio::time::Instant::now();
        hc.watch(3).await;
        // Probes: 1(fail) 2(fail) 3(healthy→reset) 4(fail) 5(fail) 6(fail→trip)
        // — a single healthy check in the middle forbids an early trip.
        assert_eq!(start.elapsed(), Duration::from_millis(600));
    }

    #[tokio::test(start_paused = true)]
    async fn health_watch_zero_threshold_is_clamped_to_one() {
        let hc = HealthCheck {
            probe: Box::new(|| Box::pin(async { false })),
            interval: Duration::from_millis(100),
        };
        let start = tokio::time::Instant::now();
        hc.watch(0).await; // 0 is meaningless — treated as "one failed probe kills".
        assert_eq!(start.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn health_check_clamps_a_zero_interval_to_the_safe_minimum() {
        let supervisor =
            Supervisor::new(Command::new("server")).health_check(|| async { true }, Duration::ZERO);
        let interval = supervisor
            .health_check
            .as_ref()
            .expect("health check set")
            .interval;
        assert_eq!(
            interval, MIN_HEALTH_CHECK_INTERVAL,
            "a zero interval must be clamped, not passed through as-is \
             (mirrors StatsSampler::new's clamp in src/stats.rs)"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn health_watch_zero_interval_does_not_busy_loop() {
        // A zero interval clamped to `MIN_HEALTH_CHECK_INTERVAL` costs exactly
        // that much virtual time *per probe*; an unclamped zero interval would
        // instead let the whole loop resolve in zero virtual time (the busy-spin
        // hazard this test guards against).
        let calls = AtomicU32::new(0);
        let hc = Supervisor::new(Command::new("server"))
            .health_check(
                move || {
                    // Healthy for the first 4 probes, unhealthy on the 5th.
                    let healthy = calls.fetch_add(1, Ordering::SeqCst) < 4;
                    async move { healthy }
                },
                Duration::ZERO,
            )
            .health_check
            .expect("health check set");
        let start = tokio::time::Instant::now();
        hc.watch(1).await; // threshold 1: the first failed probe trips it.
        assert_eq!(
            start.elapsed(),
            MIN_HEALTH_CHECK_INTERVAL * 5,
            "5 clamped-interval sleeps (4 healthy + 1 failing probe), not an \
             instant busy-spin"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_kill_with_a_zero_interval_still_grants_startup_grace() {
        // A zero interval is clamped rather than voiding the documented
        // startup-grace promise: the wedged child is force-killed only after the
        // clamped interval elapses, never instantly.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .restart(RestartPolicy::Never)
            .health_check(|| async { false }, Duration::ZERO)
            .health_check_failures(1)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.liveness_kills, 1);
        assert_eq!(
            start.elapsed(),
            MIN_HEALTH_CHECK_INTERVAL,
            "the first probe must fire one (clamped) interval after the \
             incarnation starts, not instantly"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_failure_force_restarts_a_hung_but_alive_child() {
        // The task's headline path. First incarnation: `Reply::pending` → the
        // scripted `output_string` parks forever (a hung-but-alive child that
        // never exits and no token cancels). An always-unhealthy probe trips
        // after one failed check, dropping that pending run (the force-kill —
        // killed on drop under a real JobRunner) and restarting it as a crash
        // under the default OnCrash policy; the second incarnation exits cleanly.
        let runner =
            ScriptedRunner::new().on_sequence(["server"], [Reply::pending(), Reply::ok("up")]);
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .health_check(|| async { false }, Duration::from_millis(50))
            .health_check_failures(1)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(
            outcome.restarts, 1,
            "the wedged incarnation was restarted once"
        );
        assert_eq!(
            outcome.liveness_kills, 1,
            "exactly one incarnation was force-killed by a failed liveness check"
        );
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
        assert!(
            outcome.final_result.is_success(),
            "the restarted incarnation exited cleanly"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_kill_under_never_reports_unhealthy() {
        // Never won't restart, but a health check still bounds a single run: a
        // hung child is force-killed and reported as Unhealthy rather than
        // parking forever.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .restart(RestartPolicy::Never)
            .health_check(|| async { false }, Duration::from_millis(50))
            .health_check_failures(1)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 0);
        assert_eq!(outcome.liveness_kills, 1);
        assert_eq!(outcome.stopped, StopReason::Unhealthy);
        assert!(!outcome.final_result.is_success());
        assert_eq!(
            outcome.final_result.code(),
            None,
            "a liveness kill surfaces as a Signalled(None) crash, no exit code"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_healthy_probe_never_force_restarts_a_long_running_child() {
        // A child that stays up (pending) until its own 250ms timeout, with a
        // probe that always reports healthy: liveness must never trip, so the run
        // ends on its own terms (a timeout) and no incarnation is force-killed.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let outcome = Supervisor::new(Command::new("server").timeout(Duration::from_millis(250)))
            .with_runner(runner)
            .restart(RestartPolicy::Never)
            .health_check(|| async { true }, Duration::from_millis(100))
            .run()
            .await
            .expect("supervision");
        assert_eq!(
            outcome.liveness_kills, 0,
            "a healthy child is never force-killed"
        );
        assert_eq!(outcome.restarts, 0);
        assert!(
            outcome.final_result.outcome().timed_out(),
            "the run ended on its own timeout, not a liveness kill"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_kill_respects_give_up_when() {
        // The synthetic liveness-kill result is a Signalled(None) crash, so
        // give_up_when sees it as `Crashed` and can classify it permanent —
        // stopping without a restart.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .health_check(|| async { false }, Duration::from_millis(50))
            .health_check_failures(1)
            .give_up_when(
                |attempt| matches!(attempt, GiveUpAttempt::Crashed(res) if res.code().is_none()),
            )
            .run()
            .await
            .expect("supervision");
        assert_eq!(
            outcome.restarts, 0,
            "give_up_when stops the liveness-killed run before any restart"
        );
        assert_eq!(outcome.liveness_kills, 1);
        assert_eq!(outcome.stopped, StopReason::GaveUp);
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_kills_can_exhaust_the_restart_budget() {
        // Every incarnation wedges (fallback pending + always-unhealthy probe),
        // so the budget is spent entirely on liveness-driven restarts.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .health_check(|| async { false }, Duration::from_millis(50))
            .health_check_failures(1)
            .max_restarts(1)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.restarts, 1);
        assert_eq!(
            outcome.liveness_kills, 2,
            "the original and its one restart both wedged"
        );
        assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
        assert_eq!(
            outcome.final_result.code(),
            None,
            "the final result is the Signalled liveness kill"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_kills_feed_the_storm_guard() {
        // A liveness kill is a crash, so it feeds the failure-storm score exactly
        // like a real crash. Scores 1, 2, 3 across the first three kills; the
        // third crosses the 2.5 threshold → one collective pause.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .health_check(|| async { false }, Duration::from_millis(1))
            .health_check_failures(1)
            .max_restarts(3)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
            .storm_pause(Duration::from_secs(1))
            .failure_threshold(2.5)
            .failure_decay(Duration::from_secs(1000))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.liveness_kills, 4);
        assert_eq!(outcome.restarts, 3);
        assert_eq!(outcome.storm_pauses, 1);
        assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_lived_then_wedged_incarnation_resets_the_backoff_escalation() {
        // The E3 uptime floor applies to liveness kills via the stamped uptime:
        // each incarnation stays "up" (pending) for 31s before the probe trips —
        // longer than the 30s max_backoff — so every kill counts as healthy and
        // resets the escalation. 5 restarts at base 1s: with the reset the backoff
        // total is ≈5s; without it the delays would climb 1+2+4+8+16 = 31s. The
        // 31s-per-incarnation uptime is virtual under a paused clock.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let start = tokio::time::Instant::now();
        let outcome = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .health_check(|| async { false }, Duration::from_secs(31))
            .health_check_failures(1)
            .max_restarts(5)
            .backoff(Duration::from_secs(1), 2.0)
            .max_backoff(Duration::from_secs(30))
            .jitter(false)
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.liveness_kills, 6);
        assert_eq!(outcome.restarts, 5);
        let total = start.elapsed();
        let uptime_total = Duration::from_secs(31 * 6);
        assert!(
            total < uptime_total + Duration::from_secs(10),
            "healthy-uptime liveness kills must reset the backoff (≈5s), not escalate (31s); \
             total {total:?}, uptime {uptime_total:?}"
        );
    }

    #[tokio::test]
    async fn without_a_health_check_liveness_kills_stays_zero() {
        // The pre-feature fast path: no health check means no force-kills and the
        // new counter stays 0.
        let outcome = supervise(SeqRunner::new(vec![fail(1), ok()]))
            .run()
            .await
            .expect("supervision");
        assert_eq!(outcome.liveness_kills, 0);
        assert_eq!(outcome.restarts, 1);
        assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    }

    // --- Live supervision session (T-158) ----------------------------------

    /// Poll `session.status()` until `pred` holds, yielding to let the detached
    /// supervision loop make progress. Panics rather than spin forever.
    async fn yield_until(
        session: &SupervisionSession,
        pred: impl Fn(&SupervisionStatus) -> bool,
    ) -> SupervisionStatus {
        for _ in 0..2000 {
            let status = session.status();
            if pred(&status) {
                return status;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "session status condition never held: {:?}",
            session.status()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn session_status_snapshots_a_live_incarnation() {
        // A pending first incarnation stays live; the session exposes it while it
        // runs — active, a start time, restarts still 0 (the first run is not a
        // restart). The scripted double has no OS pid, so `pid()` is `None`.
        let session = Supervisor::new(Command::new("server"))
            .with_runner(ScriptedRunner::new().fallback(Reply::pending()))
            .start();
        let status = yield_until(&session, |s| s.started_at().is_some()).await;
        assert!(status.is_active(), "supervision is running");
        assert_eq!(
            status.restarts(),
            0,
            "the first incarnation is not a restart"
        );
        assert!(!status.is_storm_paused());
        assert!(
            status.pid().is_none(),
            "a scripted double exposes no live pid"
        );

        // A stop ends it cleanly so the paused-clock test doesn't park forever.
        let outcome = session
            .stop(Duration::ZERO)
            .await
            .expect("a graceful stop yields an outcome");
        assert_eq!(outcome.stopped, StopReason::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn session_status_tracks_restarts_live() {
        // The first incarnation crashes and restarts; the second is a live
        // pending run. The session's restart count updates as it happens, not
        // only at the end.
        let runner = ScriptedRunner::new()
            .on_sequence(["server"], [Reply::fail(1, "boom"), Reply::pending()]);
        let session = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
            .start();
        let status = yield_until(&session, |s| s.restarts() == 1 && s.started_at().is_some()).await;
        assert!(status.is_active());
        assert_eq!(status.restarts(), 1, "one crash-restart so far, live");

        let outcome = session
            .stop(Duration::ZERO)
            .await
            .expect("a graceful stop yields an outcome");
        assert_eq!(outcome.stopped, StopReason::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn session_stop_during_a_live_child_reports_stopped() {
        // Stopping while a child is alive ends supervision with `Stopped` — a
        // deliberate, honest reason distinct from a crash, a cancellation, or a
        // predicate/policy stop.
        let session = Supervisor::new(Command::new("server"))
            .with_runner(ScriptedRunner::new().fallback(Reply::pending()))
            .restart(RestartPolicy::Always)
            .start();
        yield_until(&session, |s| s.started_at().is_some()).await;

        let outcome = session
            .stop(Duration::ZERO)
            .await
            .expect("a graceful stop yields an outcome");
        assert_eq!(outcome.stopped, StopReason::Stopped);
        assert_eq!(outcome.restarts, 0);
        // A snapshot taken after supervision ended must not claim it is active.
        assert!(!outcome.final_result.is_success());
    }

    #[tokio::test(start_paused = true)]
    async fn session_stop_during_backoff_does_not_wait_or_launch_another() {
        // A stop taken while a backoff sleep is in flight interrupts the sleep and
        // ends supervision at once — it must NOT wait the (60s) delay out nor
        // start the next incarnation. The `SeqRunner` scripts exactly one reply,
        // so a second incarnation would panic ("ran out of scripted replies").
        let start = tokio::time::Instant::now();
        let session = Supervisor::new(Command::new("fake"))
            .with_runner(SeqRunner::new(vec![fail(1)]))
            .backoff(Duration::from_secs(60), 1.0)
            .jitter(false)
            .start();
        // Let the loop run the single crash and park in the 60s backoff sleep.
        yield_until(&session, |s| s.restarts() == 0 && !s.is_storm_paused()).await;
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        let outcome = session
            .stop(Duration::ZERO)
            .await
            .expect("a graceful stop yields an outcome");
        assert_eq!(outcome.stopped, StopReason::Stopped);
        assert_eq!(outcome.restarts, 0, "no further incarnation was launched");
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "the backoff sleep must be cut short by the stop, not waited out: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_liveness_kill_clears_the_live_child_before_the_backoff() {
        // R-01 regression. A liveness `health_check` force-kills a wedged
        // incarnation by *dropping* the in-flight `run_to_result` future (via
        // `run_incarnation`'s `select!`). That drop must release the published
        // live child (`shared.current`) right then — not leave it holding the
        // killed incarnation's start time / pid (and, under a real JobRunner, a
        // clone of its `Arc<ProcessGroup>`, which would defer the group's
        // kill-on-drop teardown) until the *next* `publish_current` overwrites it
        // after the whole restart backoff.
        //
        // A scripted double exposes no OS pid or real group, so this asserts the
        // observable bookkeeping — `started_at()` (Some for a live scripted
        // incarnation) must go back to None the moment the kill drops the run,
        // all through the backoff. Before the fix it stayed Some for the entire
        // backoff window, this test spinning forever waiting for the clear.
        let runner = ScriptedRunner::new().fallback(Reply::pending());
        let session = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            // First probe one interval after the incarnation starts; one failure
            // trips it, so the live child is force-killed at ~50ms.
            .health_check(|| async { false }, Duration::from_millis(50))
            .health_check_failures(1)
            // A long backoff keeps the loop parked *between* incarnations so the
            // just-killed child's bookkeeping is observable there.
            .backoff(Duration::from_secs(60), 1.0)
            .jitter(false)
            .start();

        // Let the (paused) clock auto-advance while this task parks: the first
        // incarnation goes live (`current` published), the ~50ms probe then
        // force-kills it, and the loop parks in the 60s backoff. A short real
        // wait spans the ~50ms kill without reaching the 60s restart.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let in_backoff = session.status();
        assert!(
            in_backoff.is_active(),
            "supervision is still running, parked in the backoff after the kill"
        );
        assert_eq!(
            in_backoff.restarts(),
            0,
            "still before the restart — the loop is in the backoff, not a new incarnation"
        );
        assert!(
            in_backoff.started_at().is_none(),
            "the force-killed incarnation's live-child state must be cleared on the \
             kill, not linger through the backoff"
        );
        assert!(
            in_backoff.pid().is_none(),
            "no live pid is reported during the backoff after a liveness kill"
        );

        // The cleared state is stable, not a momentary blip: the frozen clock
        // keeps the loop parked in the backoff across these yields.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            session.status().started_at().is_none(),
            "the live child stays cleared for the whole backoff window"
        );

        // Stop from the backoff so the paused-clock test doesn't park forever;
        // the outcome confirms exactly one liveness kill and no further restart.
        let outcome = session
            .stop(Duration::ZERO)
            .await
            .expect("a graceful stop yields an outcome");
        assert_eq!(outcome.stopped, StopReason::Stopped);
        assert_eq!(outcome.restarts, 0, "no further incarnation was launched");
        assert_eq!(
            outcome.liveness_kills, 1,
            "the wedged first incarnation was force-killed exactly once"
        );
    }

    #[tokio::test]
    async fn run_and_a_session_agree_on_the_outcome() {
        // `run()` is a thin wrapper over `start()` + awaiting the outcome, so the
        // two must produce an identical `SupervisionOutcome` for the same config.
        let via_run = supervise(SeqRunner::new(vec![fail(1), fail(1), ok()]))
            .run()
            .await
            .expect("run supervision");
        let via_session = supervise(SeqRunner::new(vec![fail(1), fail(1), ok()]))
            .start()
            .wait()
            .await
            .expect("session supervision");
        assert_eq!(
            via_run, via_session,
            "run() and start().wait() must agree on the outcome"
        );
        assert_eq!(via_run.restarts, 2);
        assert_eq!(via_run.stopped, StopReason::PolicySatisfied);
    }

    #[tokio::test]
    async fn dropping_a_session_aborts_supervision_without_leaking_the_task() {
        // A runner that carries a canary `Arc`: while the detached supervision
        // task is alive it holds the runner (hence a clone of the canary). Dropping
        // the session must abort that task — no orphaned supervision task — so the
        // runner drops and the canary's strong count falls back to 1.
        struct CanaryRunner {
            inner: ScriptedRunner,
            _canary: std::sync::Arc<()>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for CanaryRunner {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                self.inner.output_string(command).await
            }
            async fn start(&self, command: &Command) -> Result<crate::RunningProcess> {
                self.inner.start(command).await
            }
        }

        let canary = std::sync::Arc::new(());
        let runner = CanaryRunner {
            inner: ScriptedRunner::new().fallback(Reply::pending()),
            _canary: std::sync::Arc::clone(&canary),
        };
        let session = Supervisor::new(Command::new("server"))
            .with_runner(runner)
            .start();
        // Let the loop spawn the (pending) child so the task is genuinely running.
        yield_until(&session, |s| s.started_at().is_some()).await;
        assert_eq!(
            std::sync::Arc::strong_count(&canary),
            2,
            "the live supervision task holds the runner"
        );

        drop(session);
        // Let the runtime process the abort and drop the task's future (the runner).
        for _ in 0..200 {
            if std::sync::Arc::strong_count(&canary) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            std::sync::Arc::strong_count(&canary),
            1,
            "dropping the session must abort supervision — no orphaned task holding the runner"
        );
    }
}
