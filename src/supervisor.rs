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

use std::time::Duration;

use crate::buffer::OutputBufferPolicy;
use crate::command::Command;
use crate::error::Result;
use crate::result::ProcessResult;
use crate::runner::{JobRunner, ProcessRunner};

/// Default per-incarnation capture tail for a supervised command whose own
/// policy is unbounded. A supervised process can be long-lived and chatty, so
/// capturing its *entire* output risks unbounded heap — keep a bounded tail (the
/// most recent lines, the ones that matter for a crash) by default instead.
const DEFAULT_SUPERVISION_TAIL: usize = 1000;

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
    /// The [`max_restarts`](Supervisor::max_restarts) budget ran out while the
    /// policy still wanted another restart.
    RestartsExhausted,
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
    /// The output-capture policy applied to every incarnation. Defaults to a
    /// bounded tail (see [`default_supervision_capture`]); override with
    /// [`capture`](Self::capture).
    capture: OutputBufferPolicy,
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
            .field("capture", &self.capture)
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
            capture,
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
            capture: self.capture,
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
    /// This caps *retention*, not the stdio mode: supervision captures each
    /// incarnation's output (to evaluate [`stop_when`](Self::stop_when) and the
    /// final result), so the command's `stdout` must stay
    /// [`Piped`](crate::StdioMode::Piped) (the default). A command with a
    /// non-piped `stdout` (`Inherit`/`Null`) errors every incarnation and
    /// would just spin the restart loop.
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

    /// Supervise until the policy, the predicate, or the restart budget ends
    /// it, and report the [`SupervisionOutcome`].
    ///
    /// # Permanent failures
    ///
    /// The supervisor does **not** distinguish a transient crash from a permanent
    /// one — a command that can never succeed (a missing binary, a config error
    /// that crashes on startup, a port that is permanently taken) restarts
    /// **forever** under the default unlimited [`OnCrash`](RestartPolicy::OnCrash)
    /// policy, throttled only by the backoff: a fast-failing one climbs to
    /// [`max_backoff`](Self::max_backoff) (each incarnation is shorter than the
    /// ceiling, so never healthy), while one that takes `≥ max_backoff` to fail is
    /// throttled by its own runtime instead. Either way it loops indefinitely —
    /// bound it with [`max_restarts`](Self::max_restarts) and/or a
    /// [`stop_when`](Self::stop_when) predicate that recognizes the unrecoverable
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
    /// `Error::Cancelled` immediately, regardless of policy or budget — the
    /// token stays cancelled, so a restart would only be cancelled again.
    pub async fn run(self) -> Result<SupervisionOutcome> {
        let factor = if self.backoff_factor.is_finite() {
            self.backoff_factor.max(1.0)
        } else {
            1.0
        };

        // Apply the capture policy once; clone so `self` stays intact.
        let command = self.command.clone().output_buffer(self.capture);

        let mut restarts: u32 = 0;
        // The backoff *exponent* — separate from the lifetime `restarts` count so a
        // run that stayed healthy resets the escalation (E3): otherwise a
        // long-lived service that exits/crashes occasionally would climb to the
        // `max_backoff` ceiling and restart at it forever.
        let mut backoff_restarts: u32 = 0;
        let mut storm = StormState::new();
        loop {
            match self.runner.output_string(&command).await {
                Ok(result) => {
                    if let Some(predicate) = &self.stop_when
                        && predicate(&result)
                    {
                        return Ok(self.outcome(result, restarts, &storm, StopReason::Predicate));
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
                            &storm,
                            StopReason::PolicySatisfied,
                        ));
                    }
                    if self.max_restarts.is_some_and(|max| restarts >= max) {
                        return Ok(self.outcome(
                            result,
                            restarts,
                            &storm,
                            StopReason::RestartsExhausted,
                        ));
                    }
                    // E3: a run is "healthy" only if it stayed up at least as long
                    // as the backoff ceiling — a clear "it's stable now" signal —
                    // whether it then exited cleanly or crashed. Resetting the
                    // escalation there keeps a long-lived service off the ceiling,
                    // while a tight loop (clean OR crashing, each incarnation shorter
                    // than max_backoff) keeps climbing and self-throttles. A uniform
                    // uptime floor — rather than "any clean exit resets" — avoids a
                    // footgun: under Always, an instantly-exiting `exit 0` loop would
                    // otherwise reset every iteration and spin at the base delay.
                    let healthy = result.duration() >= self.max_backoff;
                    if healthy {
                        backoff_restarts = 0;
                    }
                    if crashed && self.storm_gate(&mut storm).await {
                        return Err(self.cancelled_err(&command));
                    }
                    if self.sleep_backoff(backoff_restarts, factor).await {
                        return Err(self.cancelled_err(&command));
                    }
                    restarts = restarts.saturating_add(1);
                    backoff_restarts = backoff_restarts.saturating_add(1);
                }
                Err(err) => {
                    if err.is_cancelled() {
                        return Err(err);
                    }
                    let wants_restart = !matches!(self.policy, RestartPolicy::Never);
                    if !wants_restart || self.max_restarts.is_some_and(|max| restarts >= max) {
                        return Err(err);
                    }
                    // A spawn-side failure carries no run duration, so it never
                    // counts as healthy — the escalation keeps climbing.
                    if self.storm_gate(&mut storm).await {
                        return Err(self.cancelled_err(&command));
                    }
                    if self.sleep_backoff(backoff_restarts, factor).await {
                        return Err(self.cancelled_err(&command));
                    }
                    restarts = restarts.saturating_add(1);
                    backoff_restarts = backoff_restarts.saturating_add(1);
                }
            }
        }
    }

    fn outcome(
        &self,
        final_result: ProcessResult<String>,
        restarts: u32,
        storm: &StormState,
        stopped: StopReason,
    ) -> SupervisionOutcome {
        SupervisionOutcome {
            final_result,
            restarts,
            stopped,
            storm_pauses: storm.pauses,
        }
    }

    /// The terminal `Cancelled` error for supervision cut short by a cancel token
    /// firing during a backoff or storm pause.
    fn cancelled_err(&self, command: &Command) -> crate::Error {
        crate::Error::Cancelled {
            program: command.program_name(),
        }
    }

    /// Sleep `delay`, waking early (returning `true`) if the supervised command's
    /// [`cancel_on`](crate::Command::cancel_on) token fires — so a cancellation
    /// during a backoff or storm pause ends supervision promptly with
    /// `Error::Cancelled` instead of waiting out a (possibly long) delay. Without a
    /// token, this just sleeps and returns `false`. A zero delay still observes an
    /// already-cancelled token (returns `true`) so supervision ends promptly.
    #[must_use = "the returned bool signals cancellation — supervision must end when true"]
    async fn sleep_or_cancel(&self, delay: Duration) -> bool {
        if delay.is_zero() {
            return self
                .command
                .cancel_token()
                .is_some_and(|t| t.is_cancelled());
        }
        match self.command.cancel_token() {
            Some(token) => tokio::select! {
                biased;
                () = token.cancelled() => true,
                () = tokio::time::sleep(delay) => false,
            },
            None => {
                tokio::time::sleep(delay).await;
                false
            }
        }
    }

    /// The failure-storm gate, run before the backoff of every *failure*-
    /// driven restart: fold the failure into the decaying score and, past the
    /// threshold, sleep out one jittered [`storm_pause`](Self::storm_pause)
    /// and reset the score (a fresh window — the pause itself must not count
    /// as elapsed decay time for the *next* failure). Returns `true` if the
    /// cancel token fired during the pause (supervision should end).
    #[must_use = "the returned bool signals cancellation — supervision must end when true"]
    async fn storm_gate(&self, storm: &mut StormState) -> bool {
        let Some(pause) = self.storm_pause else {
            return false;
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
            return false;
        }
        let pause = apply_jitter(pause, self.jitter);
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: "processkit",
            pause_ms = pause.as_millis() as u64,
            "supervisor failure storm — pausing restarts"
        );
        if self.sleep_or_cancel(pause).await {
            return true;
        }
        storm.score = 0.0;
        storm.last_failure_at = None;
        storm.pauses = storm.pauses.saturating_add(1);
        false
    }

    /// Sleep out the delay before the `restarts`-th (0-based) restart. Returns
    /// `true` if the cancel token fired during the backoff.
    #[must_use = "the returned bool signals cancellation — supervision must end when true"]
    async fn sleep_backoff(&self, restarts: u32, factor: f64) -> bool {
        let delay = backoff_delay(self.backoff_base, factor, restarts, self.max_backoff);
        let delay = apply_jitter(delay, self.jitter);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            restart = restarts + 1,
            delay_ms = delay.as_millis() as u64,
            "supervisor restarting child"
        );
        self.sleep_or_cancel(delay).await
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

/// `base × factor^n`, capped at `cap`.
fn backoff_delay(base: Duration, factor: f64, n: u32, cap: Duration) -> Duration {
    if base.is_zero() {
        return Duration::ZERO;
    }
    let scaled = base.as_secs_f64() * factor.powi(n.min(i32::MAX as u32) as i32);
    if !scaled.is_finite() || scaled >= cap.as_secs_f64() {
        return cap;
    }
    Duration::from_secs_f64(scaled).min(cap)
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

/// A pseudo-random factor in `[0.5, 1.5)`: hash a constant through a fresh
/// `RandomState` to get a fresh `u64` per call, no extra dependency.
fn jitter_factor() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0x9E37_79B9_7F4A_7C15);
    let bits = hasher.finish();
    let unit = (bits >> 11) as f64 / (1u64 << 53) as f64; // top 53 bits → [0, 1)
    0.5 + unit
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::Outcome;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

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
        Err(crate::Error::Spawn {
            program: "fake".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "no such binary"),
        })
    }

    fn supervise(runner: SeqRunner) -> Supervisor<SeqRunner> {
        Supervisor::new(Command::new("fake"))
            .with_runner(runner)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
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
        assert!(matches!(err, crate::Error::Spawn { .. }), "got {err:?}");
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
            Err(crate::Error::Cancelled {
                program: "fake".into(),
            }),
            ok(),
        ]))
        .restart(RestartPolicy::Always)
        .max_restarts(5)
        .run()
        .await
        .expect_err("a cancelled incarnation is terminal");
        assert!(matches!(err, crate::Error::Cancelled { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn never_returns_a_spawn_error_directly() {
        let err = supervise(SeqRunner::new(vec![spawn_err()]))
            .restart(RestartPolicy::Never)
            .run()
            .await
            .expect_err("Never does not retry a spawn failure");
        assert!(matches!(err, crate::Error::Spawn { .. }), "got {err:?}");
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
        let err = supervise(SeqRunner::new(vec![Err(crate::Error::Cancelled {
            program: "fake".into(),
        })]))
        .storm_pause(Duration::from_secs(60))
        .failure_threshold(0.0)
        .run()
        .await
        .expect_err("cancelled is terminal");
        assert!(matches!(err, crate::Error::Cancelled { .. }), "got {err:?}");
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
        assert!(matches!(err, crate::Error::Cancelled { .. }), "got {err:?}");
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
}
