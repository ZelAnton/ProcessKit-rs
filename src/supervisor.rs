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

use crate::command::Command;
use crate::error::Result;
use crate::result::ProcessResult;
use crate::runner::{JobRunner, ProcessRunner};

/// When the supervisor restarts an exited child. See each variant; in every
/// case [`stop_when`](Supervisor::stop_when) and
/// [`max_restarts`](Supervisor::max_restarts) can end supervision first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Restart after every completed run, clean or not.
    Always,
    /// Restart only after a *crash* — a non-zero exit, a timeout, a signal
    /// kill, or a failure to spawn. A clean exit (code `0`) ends supervision.
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
#[derive(Debug)]
pub struct SupervisionOutcome {
    /// The result of the final run (the one that ended supervision).
    pub final_result: ProcessResult<String>,
    /// How many times the child was *re*-run (the first run is not a restart):
    /// `restarts == 2` means three runs happened.
    pub restarts: u32,
    /// Why supervision stopped.
    pub stopped: StopReason,
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
/// `200ms × 2.0` capped at 30 s, jitter on.
///
/// Runs go through a [`ProcessRunner`] — [`JobRunner`] by default (each
/// incarnation in its own private kill-on-drop group). Inject another with
/// [`with_runner`](Self::with_runner): a `&ProcessGroup` supervises every
/// incarnation inside one shared group, and a
/// [`ScriptedRunner`](crate::ScriptedRunner) makes supervision logic fully
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
    #[allow(clippy::type_complexity)]
    stop_when: Option<Box<dyn Fn(&ProcessResult<String>) -> bool + Send + Sync>>,
}

impl Supervisor<JobRunner> {
    /// Supervise `command` with the default [`JobRunner`] (a fresh private
    /// kill-on-drop group per incarnation).
    pub fn new(command: Command) -> Self {
        Supervisor {
            command,
            runner: JobRunner::new(),
            policy: RestartPolicy::OnCrash,
            max_restarts: None,
            backoff_base: Duration::from_millis(200),
            backoff_factor: 2.0,
            max_backoff: Duration::from_secs(30),
            jitter: true,
            stop_when: None,
        }
    }
}

impl<R: ProcessRunner> Supervisor<R> {
    /// Run every incarnation through `runner` instead of the default
    /// [`JobRunner`] — e.g. a `&ProcessGroup` for one shared kill-on-drop
    /// group, or a test double for hermetic supervision tests.
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
            stop_when: self.stop_when,
        }
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
    pub async fn run(self) -> Result<SupervisionOutcome> {
        // Documented tolerance: a sub-1.0 or non-finite factor never shrinks
        // the delay or panics the Duration math — it decays to 1.0.
        let factor = if self.backoff_factor.is_finite() {
            self.backoff_factor.max(1.0)
        } else {
            1.0
        };

        let mut restarts: u32 = 0;
        loop {
            match self.runner.output(&self.command).await {
                Ok(result) => {
                    if let Some(predicate) = &self.stop_when
                        && predicate(&result)
                    {
                        return Ok(self.outcome(result, restarts, StopReason::Predicate));
                    }
                    // A crash is any run without a clean exit: non-zero code,
                    // timeout, or signal kill (both of the latter have no code).
                    let crashed = result.code() != Some(0);
                    let wants_restart = match self.policy {
                        RestartPolicy::Always => true,
                        RestartPolicy::OnCrash => crashed,
                        RestartPolicy::Never => false,
                    };
                    if !wants_restart {
                        return Ok(self.outcome(result, restarts, StopReason::PolicySatisfied));
                    }
                    if self.max_restarts.is_some_and(|max| restarts >= max) {
                        return Ok(self.outcome(result, restarts, StopReason::RestartsExhausted));
                    }
                    self.sleep_backoff(restarts, factor).await;
                    restarts += 1;
                }
                Err(err) => {
                    // The child never produced a result (spawn/IO failure). The
                    // predicate can't judge it; the policy treats it as a crash.
                    let wants_restart = !matches!(self.policy, RestartPolicy::Never);
                    if !wants_restart || self.max_restarts.is_some_and(|max| restarts >= max) {
                        return Err(err);
                    }
                    self.sleep_backoff(restarts, factor).await;
                    restarts += 1;
                }
            }
        }
    }

    fn outcome(
        &self,
        final_result: ProcessResult<String>,
        restarts: u32,
        stopped: StopReason,
    ) -> SupervisionOutcome {
        SupervisionOutcome {
            final_result,
            restarts,
            stopped,
        }
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
    delay.mul_f64(jitter_factor())
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
        async fn output(&self, _command: &Command) -> Result<ProcessResult<String>> {
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
            Some(0),
            false,
            None,
        ))
    }

    fn fail(code: i32) -> Result<ProcessResult<String>> {
        Ok(ProcessResult::new(
            "fake".into(),
            String::new(),
            "boom".into(),
            Some(code),
            false,
            None,
        ))
    }

    fn timeout() -> Result<ProcessResult<String>> {
        Ok(ProcessResult::new(
            "fake".into(),
            String::new(),
            String::new(),
            None,
            true,
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
        assert!(
            waited >= Duration::from_millis(500) && waited < Duration::from_millis(1500),
            "jittered delay out of [0.5, 1.5) band: {waited:?}"
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
}
