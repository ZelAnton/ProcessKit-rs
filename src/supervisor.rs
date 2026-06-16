//! [`Supervisor`] — keep a child alive with policy-driven restarts and backoff.
//!
//! [`Command::retry`](crate::Command::retry) answers "run this once, replaying
//! on failure". A supervisor answers the different question **"keep this
//! alive"**: restart a child whenever it exits (unless its exit satisfies the
//! policy or a predicate), with bounded restarts and exponential backoff plus
//! jitter — a minimal `runit`/`systemd`-style keeper on top of the runner
//! layer.
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

/// D3: default per-incarnation capture tail for a supervised command whose own
/// policy is unbounded. A supervised process can be long-lived and chatty, so
/// capturing its *entire* output risks unbounded heap — keep a bounded tail (the
/// most recent lines, the ones that matter for a crash) by default instead.
const DEFAULT_SUPERVISION_TAIL: usize = 1000;

/// The capture policy to apply to each incarnation: respect an explicit
/// bounded/fail-loud command policy, but bound an unbounded line count to a
/// tail (D3). Only the line cap is filled in — the overflow *mode* and any
/// byte cap ([`with_max_bytes`](OutputBufferPolicy::with_max_bytes), D8) the
/// command set are preserved, so an unbounded `Error` ("fail loud") command
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
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::{Command, RestartPolicy, Supervisor};
/// use std::time::Duration;
///
/// let outcome = Supervisor::new(Command::new("my-server").args(["--port", "8080"]))
///     .restart(RestartPolicy::OnCrash)
///     .max_restarts(5)
///     .backoff(Duration::from_millis(200), 2.0)
///     .stop_when(|res| res.code() == Some(0))
///     .run()
///     .await?;
/// println!("ended after {} restarts: {:?}", outcome.restarts, outcome.stopped);
/// # Ok(())
/// # }
/// ```
///
/// Defaults: [`OnCrash`](RestartPolicy::OnCrash), unlimited restarts, backoff
/// `200ms × 2.0` capped at 30 s, jitter on, failure-storm guard off (enable
/// with [`storm_pause`](Self::storm_pause); once enabled, failure-score
/// half-life 30 s and threshold 5.0).
///
/// Runs go through a [`ProcessRunner`] — [`JobRunner`] by default (each
/// incarnation in its own private kill-on-drop group). Inject another with
/// [`with_runner`](Self::with_runner): a `&ProcessGroup` supervises every
/// incarnation inside one shared group, and a
/// [`ScriptedRunner`](crate::testing::ScriptedRunner) makes supervision logic fully
/// hermetic in tests.
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
    /// D3: the output-capture policy applied to every incarnation. Defaults to a
    /// bounded tail (see [`default_supervision_capture`]); override with
    /// [`capture`](Self::capture).
    capture: OutputBufferPolicy,
}

// Manual: the runner type parameter and the boxed predicate are opaque.
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

    /// Bound (or widen) the output captured from each incarnation (D3).
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
    /// non-piped `stdout` (`Inherit`/`Null`) errors every incarnation (D5) and
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
    #[must_use]
    pub fn backoff(mut self, base: Duration, factor: f64) -> Self {
        self.backoff_base = base;
        self.backoff_factor = factor;
        self
    }

    /// Cap any single backoff delay (default: 30 s).
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
        // Documented tolerance: a sub-1.0 or non-finite factor never shrinks
        // the delay or panics the Duration math — it decays to 1.0.
        let factor = if self.backoff_factor.is_finite() {
            self.backoff_factor.max(1.0)
        } else {
            1.0
        };

        // D3: apply the supervisor's capture policy (a bounded tail by default)
        // to the command once, so a long-lived chatty incarnation can't grow
        // unbounded heap. Cloned, so `self` (and `self.outcome`) stay intact.
        let command = self.command.clone().output_buffer(self.capture);

        let mut restarts: u32 = 0;
        let mut storm = StormState::new();
        loop {
            match self.runner.output_string(&command).await {
                Ok(result) => {
                    if let Some(predicate) = &self.stop_when
                        && predicate(&result)
                    {
                        return Ok(self.outcome(result, restarts, &storm, StopReason::Predicate));
                    }
                    // A crash is any run that is not a success: an exit code
                    // outside the accepted set (`ok_codes`, default `{0}`), a
                    // timeout, or a signal kill (both of the latter have no
                    // code). `is_success` honors `ok_codes` so the supervisor
                    // agrees with the rest of the crate — a command exiting an
                    // accepted non-zero code is clean, not a crash.
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
                    // Only failures feed the storm score: a clean exit
                    // restarted under `Always` is churn, not a failure.
                    if crashed {
                        self.storm_gate(&mut storm).await;
                    }
                    self.sleep_backoff(restarts, factor).await;
                    restarts = restarts.saturating_add(1);
                }
                Err(err) => {
                    // A cancelled incarnation is terminal: the token stays
                    // cancelled, so restarting would spin a futile loop of
                    // instantly-cancelled runs. Ends supervision like `Never`.
                    if matches!(err, crate::Error::Cancelled { .. }) {
                        return Err(err);
                    }
                    // The child never produced a result (spawn/IO failure). The
                    // predicate can't judge it; the policy treats it as a crash.
                    let wants_restart = !matches!(self.policy, RestartPolicy::Never);
                    if !wants_restart || self.max_restarts.is_some_and(|max| restarts >= max) {
                        return Err(err);
                    }
                    self.storm_gate(&mut storm).await;
                    self.sleep_backoff(restarts, factor).await;
                    restarts = restarts.saturating_add(1);
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

    /// The failure-storm gate, run before the backoff of every *failure*-
    /// driven restart: fold the failure into the decaying score and, past the
    /// threshold, sleep out one jittered [`storm_pause`](Self::storm_pause)
    /// and reset the score (a fresh window — the pause itself must not count
    /// as elapsed decay time for the *next* failure).
    async fn storm_gate(&self, storm: &mut StormState) {
        let Some(pause) = self.storm_pause else {
            return;
        };
        let now = tokio::time::Instant::now();
        let elapsed = storm
            .last_failure_at
            .map(|at| now.saturating_duration_since(at))
            .unwrap_or(Duration::ZERO);
        storm.last_failure_at = Some(now);
        storm.score = decayed_failure_score(storm.score, elapsed, self.failure_decay);
        // A non-finite threshold never trips (NaN comparisons are false).
        let tripped = storm.score > self.failure_threshold;
        if !tripped {
            return;
        }
        let pause = apply_jitter(pause, self.jitter);
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: "processkit",
            pause_ms = pause.as_millis() as u64,
            "supervisor failure storm — pausing restarts"
        );
        if !pause.is_zero() {
            tokio::time::sleep(pause).await;
        }
        storm.score = 0.0;
        storm.last_failure_at = None;
        storm.pauses = storm.pauses.saturating_add(1);
    }

    /// Sleep out the delay before the `restarts`-th (0-based) restart.
    async fn sleep_backoff(&self, restarts: u32, factor: f64) {
        let delay = backoff_delay(self.backoff_base, factor, restarts, self.max_backoff);
        let delay = apply_jitter(delay, self.jitter);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "processkit",
            restart = restarts + 1,
            delay_ms = delay.as_millis() as u64,
            "supervisor restarting child"
        );
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

/// The storm guard's running state — one per `run()` call.
struct StormState {
    /// The decaying failure score (see [`decayed_failure_score`]).
    score: f64,
    /// When the previous failure was folded in (`None` = fresh window).
    last_failure_at: Option<tokio::time::Instant>,
    /// How many storm pauses were taken (reported in the outcome).
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

/// `base × factor^n`, capped — computed in `f64` and clamped into a domain
/// where the `Duration` conversion cannot panic.
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

/// Multiply `delay` by a uniform factor in `[0.5, 1.5)` when enabled.
fn apply_jitter(delay: Duration, enabled: bool) -> Duration {
    if !enabled || delay.is_zero() {
        return delay;
    }
    // Clamp the jittered delay to `MAX_DEADLINE`: `Duration::mul_f64` *panics* on
    // overflow, and the up-to-1.5× factor can push a near-`Duration::MAX` delay
    // (reachable via `max_backoff(Duration::MAX)` or `storm_pause(Duration::MAX)`,
    // jitter on by default) past `Duration`'s range. Mirrors the crate-wide
    // `MAX_DEADLINE` clamp used on every other timing path (E15).
    let scaled = delay.as_secs_f64() * jitter_factor();
    Duration::try_from_secs_f64(scaled)
        .unwrap_or(crate::MAX_DEADLINE)
        .min(crate::MAX_DEADLINE)
}

/// A pseudo-random factor in `[0.5, 1.5)` with no extra dependency: every
/// `RandomState` is constructed with fresh random keys, so hashing a constant
/// through it yields a fresh `u64` per call.
fn jitter_factor() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0x9E37_79B9_7F4A_7C15);
    let bits = hasher.finish();
    // Take the top 53 bits → uniform in [0, 1) at f64 precision.
    let unit = (bits >> 11) as f64 / (1u64 << 53) as f64;
    0.5 + unit
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::Outcome;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A scripted sequence of per-call outcomes — covers the `Err` cases the
    /// reply-matching `ScriptedRunner` can't produce. Running out of replies
    /// panics, so a supervisor looping more than scripted fails loudly.
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
        // Zero backoff keeps the hermetic tests instant; timing-sensitive
        // cases use the paused clock below instead.
        Supervisor::new(Command::new("fake"))
            .with_runner(runner)
            .backoff(Duration::ZERO, 1.0)
            .jitter(false)
    }

    /// D3: the capture default — an unbounded command is bounded to a tail
    /// (preserving its overflow mode); an explicit bounded/fail-loud policy is
    /// respected.
    #[test]
    fn supervision_capture_default_bounds_an_unbounded_command() {
        // Default unbounded (DropOldest) → bounded tail, DropOldest preserved.
        let unbounded = Command::new("server");
        let policy = default_supervision_capture(&unbounded);
        assert_eq!(
            policy.max_lines,
            Some(DEFAULT_SUPERVISION_TAIL),
            "an unbounded supervised command must default to a bounded tail"
        );
        assert_eq!(policy.overflow, crate::OverflowMode::DropOldest);

        // Unbounded + Error → bounded fail-loud (overflow mode preserved, not
        // silently dropped to DropOldest).
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

        // An explicit bounded fail_loud policy is respected as-is.
        let explicit =
            Command::new("server").output_buffer(crate::OutputBufferPolicy::fail_loud(50));
        let policy = default_supervision_capture(&explicit);
        assert_eq!(policy.max_lines, Some(50), "an explicit cap is respected");
        assert_eq!(policy.overflow, crate::OverflowMode::Error);
    }

    /// D3: `run` actually applies the capture policy to the incarnation — by
    /// default a bounded tail, overridable via `capture()`.
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

        // Default: an unbounded command is bounded to the tail.
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

        // `capture()` overrides — here back to unbounded.
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
        // `max_restarts(0)` = a zero restart budget: one run, reported as
        // exhausted when the policy wanted more. A restart slipping through
        // would consume the second (clean) reply and report PolicySatisfied
        // with restarts=1 — the assertions below rule that out.
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
        // Always would restart a clean run; the predicate ends it first.
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
        // P1-3: a supervised command with `ok_codes([0, 2])` that exits 2 is a
        // success everywhere else in the crate (`is_success() == true`), so
        // OnCrash must NOT restart it. The real runner stamps the command's
        // `ok_codes` onto the result, so model that here. Only one reply is
        // scripted: a spurious restart would deplete the SeqRunner and panic.
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
        // Inverse of the above: `ok_codes([1])` makes 0 a failure. OnCrash must
        // restart it rather than reading raw code 0 as clean. The follow-up
        // clean run (default ok_codes {0}, exit 0) satisfies the policy.
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
        // once — the second scripted reply is never consumed (SeqRunner would
        // panic on depletion if a restart happened past it).
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
        // 200ms before the first restart + 400ms before the second.
        assert_eq!(start.elapsed(), Duration::from_millis(600));
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
        // 200ms, then 400ms clamped to 300ms.
        assert_eq!(start.elapsed(), Duration::from_millis(500));
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
        // `<=` upper bound: the factor is in [0.5, 1.5), but `mul_f64` rounds
        // to the nearest nanosecond — a factor just under 1.5 can round the
        // delay up to exactly 1.5× (observed as a rare flake).
        assert!(
            waited >= Duration::from_millis(500) && waited <= Duration::from_millis(1500),
            "jittered delay out of [0.5, 1.5] band: {waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonsense_backoff_factor_decays_to_constant_delay() {
        // factor 0.0 must not shrink the delay or panic — it is treated as
        // 1.0, so both restarts wait the base delay.
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
        // First failure on a fresh window.
        assert_eq!(decayed_failure_score(0.0, Duration::ZERO, hl), 1.0);
        // Back-to-back failures accumulate undecayed.
        assert_eq!(decayed_failure_score(1.0, Duration::ZERO, hl), 2.0);
        // Exactly one half-life: the previous score halves, then +1.
        assert_eq!(decayed_failure_score(2.0, hl, hl), 2.0);
        assert_eq!(decayed_failure_score(4.0, hl, hl), 3.0);
        // Many half-lives: history all but vanishes.
        let aged = decayed_failure_score(8.0, Duration::from_secs(3000), hl);
        assert!((aged - 1.0).abs() < 1e-9, "got {aged}");
        // Zero half-life keeps no history.
        assert_eq!(
            decayed_failure_score(100.0, Duration::ZERO, Duration::ZERO),
            1.0
        );
        // A poisoned previous score resets instead of propagating.
        assert_eq!(decayed_failure_score(f64::NAN, Duration::ZERO, hl), 1.0);
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
        // Zero backoff → zero decay time between failures: scores run 1, 2, 3;
        // the third crosses 2.5 and takes exactly one 1 s pause.
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
        // The 10 s backoff between failures is 10 half-lives of decay — each
        // failure scores ≈1, never reaching 2.5: same failure count as the
        // tripping test above, zero pauses.
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
        // Threshold 1.5, no meaningful decay: failures score 1, 2(pause),
        // 1, 2(pause) — the reset after each pause is what keeps the second
        // failure from tripping immediately.
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
        // The budget check runs first: the second failure terminates before
        // its storm bookkeeping, so no pause is taken or reported.
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
        // `<=` upper bound: ns-rounding can land exactly on 1.5× (see
        // jitter_stays_within_its_band).
        assert!(
            waited >= Duration::from_millis(500) && waited <= Duration::from_millis(1500),
            "jittered storm pause out of [0.5, 1.5] band: {waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn clean_restarts_under_always_do_not_feed_the_storm_score() {
        // Three clean exits restarted by Always would trip threshold 1.5 if
        // they counted as failures; they must not.
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

    #[test]
    fn backoff_delay_math() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(30);
        assert_eq!(backoff_delay(base, 2.0, 0, cap), base);
        assert_eq!(backoff_delay(base, 2.0, 1, cap), Duration::from_millis(200));
        assert_eq!(backoff_delay(base, 2.0, 3, cap), Duration::from_millis(800));
        // Saturation: an astronomic exponent clamps to the cap, no panic.
        assert_eq!(backoff_delay(base, 2.0, 1_000, cap), cap);
        assert_eq!(backoff_delay(Duration::ZERO, 2.0, 5, cap), Duration::ZERO);
    }

    #[test]
    fn apply_jitter_clamps_instead_of_overflowing() {
        // Regression: the up-to-1.5x jitter factor on a near-`Duration::MAX`
        // delay (reachable via `max_backoff(Duration::MAX)` / `storm_pause`) must
        // NOT panic in `Duration::mul_f64` — it clamps to `MAX_DEADLINE`.
        let jittered = apply_jitter(Duration::MAX, true);
        assert!(jittered <= crate::MAX_DEADLINE, "clamped, got {jittered:?}");
        // Jitter disabled or zero delay passes through untouched (no clamp).
        assert_eq!(apply_jitter(Duration::MAX, false), Duration::MAX);
        assert_eq!(apply_jitter(Duration::ZERO, true), Duration::ZERO);
        // A normal delay still gets a factor in [0.5, 1.5).
        let normal = apply_jitter(Duration::from_secs(10), true);
        assert!(normal >= Duration::from_secs(5) && normal < Duration::from_secs(15));
    }
}
