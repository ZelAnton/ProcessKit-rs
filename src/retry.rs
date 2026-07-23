//! [`RetryPolicy`] — an exponential-backoff (+ cap + jitter) retry schedule, and
//! the internal [`RetryConfig`] that pairs it with a retry classifier.

use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;

/// How a retryable run is re-attempted: a bounded number of retries with
/// **exponential backoff**, a per-delay **cap**, and optional **full jitter**.
///
/// Used two ways:
/// - **Client-wide** via
///   [`CliClient::default_retry`](crate::CliClient::default_retry) — every verb of
///   the client retries on a shared policy + classifier, instead of repeating a
///   per-command [`Command::retry`](crate::Command::retry) (which is fixed-backoff
///   and per-call).
/// - As the schedule behind [`Command::retry`](crate::Command::retry), whose
///   `(max_attempts, backoff)` form is the fixed-backoff special case
///   (`multiplier` `1.0`, no growth, no jitter).
///
/// The `n`-th retry (0-based) waits `min(initial_backoff × multiplier^n,
/// max_backoff)`; with [`jitter`](Self::jitter) the actual wait is a uniform
/// random value in `[0, that]` (AWS-style **full jitter**), which decorrelates a
/// fleet of clients all backing off at once. Jitter uses a small per-thread PRNG
/// seeded from system entropy — no extra dependency, and no crypto-grade
/// randomness is needed for backoff.
///
/// Distinct from [`RestartPolicy`](crate::RestartPolicy): a `RetryPolicy` **replays
/// a verb to success** (re-running on a classified [`Error`]), while a
/// `RestartPolicy` is the [`Supervisor`](crate::Supervisor)'s **keep-alive restart**
/// schedule for a long-lived process — different subsystems, different intent.
///
/// Non-exhaustive: construct via [`RetryPolicy::new`] / [`Default`] + the builder
/// methods (new knobs can be added without a breaking change). Intentionally
/// **not** `Copy` — that would pin every future field to a `Copy` type (a boxed
/// custom-backoff fn, a list of retryable codes), and shedding `Copy` later is the
/// breaking change; `Clone` covers every real need (all reads are by-`&`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Retries **after** the first attempt — `0` never retries; `3` (the default)
    /// means up to 4 total attempts. Counts retries (like
    /// [`Supervisor::max_restarts`](crate::Supervisor::max_restarts)), **not** total
    /// attempts — in contrast to [`Command::retry`](crate::Command::retry)'s
    /// `max_attempts`.
    pub max_retries: u32,
    /// Backoff before the **first** retry (and the whole delay when `multiplier`
    /// is `1.0`). Default 100 ms. `Duration::ZERO` retries immediately. (The
    /// `base` of [`Supervisor::backoff`](crate::Supervisor::backoff), spelled in
    /// full here.)
    pub initial_backoff: Duration,
    /// Exponential growth factor per retry. Default `2.0`; `1.0` is fixed backoff.
    /// Values below `1.0` (or non-finite) are treated as `1.0` (backoff never
    /// shrinks). (The `factor` of [`Supervisor::backoff`](crate::Supervisor::backoff).)
    pub multiplier: f64,
    /// Upper bound on a single delay, so exponential growth can't run away.
    /// Default 30 s; set `Duration::MAX` to effectively disable the cap.
    pub max_backoff: Duration,
    /// Apply **full jitter** — spread the actual wait uniformly over `[0, delay]`,
    /// so a retry may fire almost immediately (AWS-style; decorrelates a fleet all
    /// backing off at once). This is *full* jitter, distinct from the multiplicative
    /// band the supervisor's [`RestartPolicy`](crate::RestartPolicy) backoff uses.
    /// Default `true`.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(30),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// A policy with the [default](Default) schedule (3 retries, 100 ms initial,
    /// ×2 growth, 30 s cap, jitter on). Tune with the builder methods.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of retries after the first attempt (`0` disables retrying).
    #[must_use]
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the backoff before the first retry (the base of the exponential).
    #[must_use]
    pub fn initial_backoff(mut self, backoff: Duration) -> Self {
        self.initial_backoff = backoff;
        self
    }

    /// Set the exponential growth factor (`1.0` = fixed backoff).
    #[must_use]
    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Cap a single delay at `max` (after growth, before jitter).
    #[must_use]
    pub fn max_backoff(mut self, max: Duration) -> Self {
        self.max_backoff = max;
        self
    }

    /// Toggle full jitter (spread the wait over `[0, delay]`).
    #[must_use]
    pub fn jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Total attempts (`max_retries + 1`), saturating.
    pub(crate) fn max_attempts(&self) -> u32 {
        self.max_retries.saturating_add(1)
    }

    /// The **deterministic** capped exponential delay before the `retry_index`-th
    /// retry (0-based) — `min(initial × multiplier^index, max_backoff)`, before any
    /// jitter. Delegates to the shared [`backoff::capped_exponential`](crate::backoff)
    /// core (also used by `Supervisor`'s restart backoff), which folds a
    /// sub-unit/non-finite multiplier to `1.0` and saturates to `max_backoff`
    /// rather than overflowing `Duration`.
    pub(crate) fn backoff_at(&self, retry_index: u32) -> Duration {
        crate::backoff::capped_exponential(
            self.initial_backoff,
            self.multiplier,
            retry_index,
            self.max_backoff,
        )
    }

    /// The actual wait before the `retry_index`-th retry: [`backoff_at`](Self::backoff_at)
    /// with full jitter applied when [`jitter`](Self::jitter) is set.
    pub(crate) fn delay_for(&self, retry_index: u32) -> Duration {
        let base = self.backoff_at(retry_index);
        if self.jitter && !base.is_zero() {
            jittered(base, crate::backoff::unit_random_f64())
        } else {
            base
        }
    }
}

/// Scale `base` by a `[0, 1]` draw (full jitter) without panicking.
///
/// Computes the scaled delay via `try_from_secs_f64`, falling back to `base` on
/// overflow/non-finite — `Duration::mul_f64` (which routes through the panicking
/// `Duration::from_secs_f64`) would otherwise panic when `base` is saturated near
/// `Duration::MAX` (e.g. `max_backoff` set to `Duration::MAX` to disable the cap,
/// the documented way to do so) and the draw rounds the product back up past
/// `Duration::MAX`. Saturate, don't panic — same discipline as
/// [`backoff::capped_exponential`](crate::backoff::capped_exponential). Taking an
/// explicit draw (rather than calling [`unit_random_f64`](crate::backoff::unit_random_f64)
/// internally) keeps this deterministically testable at the boundary (`draw ==
/// 1.0`), where the panic used to be reachable.
fn jittered(base: Duration, draw: f64) -> Duration {
    let secs = base.as_secs_f64() * draw;
    Duration::try_from_secs_f64(secs).unwrap_or(base)
}

/// A [`RetryPolicy`] schedule paired with the classifier that decides which
/// errors are retryable — the internal shape carried by a [`Command`](crate::Command)
/// and a [`CliClient`](crate::CliClient) default. `Clone` (the classifier is
/// `Arc`-shared) so both stay `Clone`.
#[derive(Clone)]
pub(crate) struct RetryConfig {
    pub(crate) policy: RetryPolicy,
    pub(crate) classifier: Arc<dyn Fn(&Error) -> bool + Send + Sync>,
}

impl RetryConfig {
    /// The fixed-backoff config behind [`Command::retry`](crate::Command::retry)'s
    /// released `(max_attempts, backoff, retry_if)` form: `multiplier 1.0`, cap at
    /// `backoff`, no jitter — so every delay is exactly `backoff`.
    pub(crate) fn fixed(
        max_attempts: u32,
        backoff: Duration,
        retry_if: impl Fn(&Error) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            policy: RetryPolicy {
                max_retries: max_attempts.saturating_sub(1),
                initial_backoff: backoff,
                multiplier: 1.0,
                max_backoff: backoff,
                jitter: false,
            },
            classifier: Arc::new(retry_if),
        }
    }

    /// A config from an explicit [`RetryPolicy`] + classifier (the client-wide path).
    pub(crate) fn new(
        policy: RetryPolicy,
        retry_if: impl Fn(&Error) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            policy,
            classifier: Arc::new(retry_if),
        }
    }

    /// A config that never retries: exactly one run, and a classifier that
    /// rejects every error. Behind [`Command::retry_never`](crate::Command::retry_never)
    /// — the explicit form of the `retry(1, ZERO, |_| false)` opt-out
    /// (`max_attempts` 1 is a single run, and setting *any* config suppresses a
    /// client `default_retry` gap-fill).
    pub(crate) fn never() -> Self {
        Self::fixed(1, Duration::ZERO, |_| false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_schedule_is_sensible() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_retries, 3);
        assert_eq!(p.max_attempts(), 4);
        assert_eq!(p.initial_backoff, Duration::from_millis(100));
        assert_eq!(p.multiplier, 2.0);
        assert!(p.jitter);
    }

    #[test]
    fn backoff_at_is_exponential_and_capped() {
        let p = RetryPolicy::new()
            .initial_backoff(Duration::from_millis(100))
            .multiplier(2.0)
            .max_backoff(Duration::from_millis(450));
        assert_eq!(p.backoff_at(0), Duration::from_millis(100)); // 100
        assert_eq!(p.backoff_at(1), Duration::from_millis(200)); // 200
        assert_eq!(p.backoff_at(2), Duration::from_millis(400)); // 400
        assert_eq!(p.backoff_at(3), Duration::from_millis(450)); // 800 → capped
        assert_eq!(p.backoff_at(50), Duration::from_millis(450)); // far past cap, no overflow
    }

    #[test]
    fn multiplier_one_is_fixed_backoff() {
        let p = RetryPolicy::new()
            .initial_backoff(Duration::from_millis(50))
            .multiplier(1.0)
            .max_backoff(Duration::from_secs(10))
            .jitter(false);
        for i in 0..5 {
            assert_eq!(p.backoff_at(i), Duration::from_millis(50));
            assert_eq!(p.delay_for(i), Duration::from_millis(50));
        }
    }

    #[test]
    fn sub_unit_multiplier_does_not_shrink_backoff() {
        let p = RetryPolicy::new()
            .initial_backoff(Duration::from_millis(100))
            .multiplier(0.5); // nonsensical; treated as 1.0
        assert_eq!(p.backoff_at(3), Duration::from_millis(100));
    }

    #[test]
    fn fixed_config_preserves_command_retry_semantics() {
        // Command::retry(3, 20ms) → 2 retries, every delay exactly 20ms.
        let cfg = RetryConfig::fixed(3, Duration::from_millis(20), |_| true);
        assert_eq!(cfg.policy.max_attempts(), 3);
        for i in 0..4 {
            assert_eq!(cfg.policy.delay_for(i), Duration::from_millis(20));
        }
    }

    #[test]
    fn jitter_stays_within_zero_and_base() {
        let p = RetryPolicy::new()
            .initial_backoff(Duration::from_millis(1000))
            .multiplier(1.0)
            .max_backoff(Duration::from_secs(10))
            .jitter(true);
        let base = p.backoff_at(0);
        // Many draws: every one within [0, base], and not all identical.
        let mut seen_low = false;
        let mut seen_high = false;
        for _ in 0..200 {
            let d = p.delay_for(0);
            assert!(d <= base, "jitter exceeded base: {d:?} > {base:?}");
            if d < base / 2 {
                seen_low = true;
            }
            if d > base / 2 {
                seen_high = true;
            }
        }
        assert!(
            seen_low && seen_high,
            "jitter did not spread across the range"
        );
    }

    #[test]
    fn zero_backoff_never_waits_even_with_jitter() {
        let p = RetryPolicy::new()
            .initial_backoff(Duration::ZERO)
            .multiplier(2.0)
            .jitter(true);
        assert_eq!(p.delay_for(0), Duration::ZERO);
        assert_eq!(p.delay_for(5), Duration::ZERO);
        // A huge index that would overflow the factor to `inf` must still be ZERO
        // (a zero base × inf is NaN — the guard short-circuits before that).
        assert_eq!(p.backoff_at(2000), Duration::ZERO);
        assert_eq!(p.backoff_at(u32::MAX), Duration::ZERO);
    }

    #[test]
    fn jitter_over_max_backoff_cap_never_panics() {
        // max_backoff ≈ Duration::MAX (the documented "effectively disable the
        // cap" configuration) saturates `backoff_at` to `Duration::MAX`.
        let p = RetryPolicy::new()
            .initial_backoff(Duration::from_secs(1))
            .multiplier(2.0)
            .max_backoff(Duration::MAX)
            .jitter(true);
        let base = p.backoff_at(u32::MAX);
        assert_eq!(base, Duration::MAX);

        // The real jitter draw (unit_random_f64, always < 1.0) exercised many
        // times over the saturated base — must never panic.
        for _ in 0..1000 {
            let d = p.delay_for(u32::MAX);
            assert!(d <= Duration::MAX);
        }

        // The exact upper bound of the draw range (1.0) is the one value that
        // used to round `Duration::MAX.mul_f64(1.0)` back up past
        // `Duration::MAX` and panic in `Duration::from_secs_f64` — exercise it
        // directly rather than relying on the RNG to ever land there.
        assert_eq!(jittered(Duration::MAX, 1.0), Duration::MAX);
        // A handful of draws spanning the range, including values right at the
        // top of it, all saturate rather than panic.
        for draw in [0.0, 0.25, 0.5, 0.75, 0.999_999_999, 1.0] {
            let d = jittered(Duration::MAX, draw);
            assert!(d <= Duration::MAX, "draw {draw} produced {d:?} > MAX");
        }
    }

    #[test]
    fn non_finite_or_nonpositive_multiplier_is_treated_as_fixed() {
        let cap = Duration::from_secs(5);
        // Non-finite (NaN, ±∞), non-positive, and sub-unit multipliers all fold to
        // a fixed 1.0 backoff — matching the field doc and `Supervisor::backoff`.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 0.0, 0.5] {
            let p = RetryPolicy::new()
                .initial_backoff(Duration::from_millis(100))
                .multiplier(bad)
                .max_backoff(cap);
            // Treated as 1.0 → fixed backoff, never shrinking, never exploding.
            assert_eq!(p.backoff_at(0), Duration::from_millis(100), "bad = {bad}");
            assert_eq!(p.backoff_at(3), Duration::from_millis(100), "bad = {bad}");
        }
    }

    #[test]
    fn fixed_config_attempt_count_boundaries() {
        // The released `Command::retry(max_attempts, …)` round-trips through
        // max_retries: 0 and 1 both mean a single attempt (no retry).
        assert_eq!(
            RetryConfig::fixed(0, Duration::ZERO, |_| true)
                .policy
                .max_attempts(),
            1
        );
        assert_eq!(
            RetryConfig::fixed(1, Duration::ZERO, |_| true)
                .policy
                .max_attempts(),
            1
        );
        assert_eq!(
            RetryConfig::fixed(5, Duration::ZERO, |_| true)
                .policy
                .max_attempts(),
            5
        );
    }
}
