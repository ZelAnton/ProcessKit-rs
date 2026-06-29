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
/// a verb to success** (re-running on a classified [`Error`](crate::Error)), while a
/// `RestartPolicy` is the [`Supervisor`](crate::Supervisor)'s **keep-alive restart**
/// schedule for a long-lived process — different subsystems, different intent.
///
/// Non-exhaustive: construct via [`RetryPolicy::new`] / [`Default`] + the builder
/// methods (new knobs can be added without a breaking change).
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Retries **after** the first attempt — `0` never retries; `3` (the default)
    /// means up to 4 total attempts. Counts retries (like
    /// [`Supervisor::max_restarts`](crate::Supervisor::max_restarts)), **not** total
    /// attempts — in contrast to [`Command::retry`](crate::Command::retry)'s
    /// `max_attempts`.
    pub max_retries: u32,
    /// Backoff before the **first** retry (and the whole delay when `multiplier`
    /// is `1.0`). Default 100 ms. `Duration::ZERO` retries immediately.
    pub initial_backoff: Duration,
    /// Exponential growth factor per retry. Default `2.0`; `1.0` is fixed backoff.
    /// Values below `1.0` are treated as `1.0` (backoff never shrinks).
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
    /// jitter. Computed in `f64` seconds so a large index/multiplier saturates to
    /// the cap rather than overflowing `Duration`.
    pub(crate) fn backoff_at(&self, retry_index: u32) -> Duration {
        // A zero base never waits, whatever the index/multiplier — and short-circuits
        // the `0.0 * inf = NaN` an overflowing factor would otherwise produce.
        if self.initial_backoff.is_zero() {
            return Duration::ZERO;
        }
        // Treat a sub-unit, NaN, or non-positive multiplier as `1.0` (backoff never
        // shrinks). The fixed case (and the first retry) is the byte-exact `initial`,
        // capped — no f64 round-trip.
        let multiplier = if self.multiplier > 1.0 {
            self.multiplier
        } else {
            1.0
        };
        if multiplier == 1.0 || retry_index == 0 {
            return self.initial_backoff.min(self.max_backoff);
        }
        // Clamp the exponent so `retry_index as i32` can't wrap negative (which would
        // *shrink* the backoff) for an absurd retry count; any realistic index is far
        // below the cap anyway. A finite non-zero base × `inf` factor saturates to
        // `inf`, which `.min(cap)` reduces to the cap — no NaN.
        let exponent = retry_index.min(i32::MAX as u32) as i32;
        let secs = (self.initial_backoff.as_secs_f64() * multiplier.powi(exponent))
            .min(self.max_backoff.as_secs_f64());
        // `try_from_secs_f64` rejects negative/NaN/overflow — fall back to the cap.
        Duration::try_from_secs_f64(secs).unwrap_or(self.max_backoff)
    }

    /// The actual wait before the `retry_index`-th retry: [`backoff_at`](Self::backoff_at)
    /// with full jitter applied when [`jitter`](Self::jitter) is set.
    pub(crate) fn delay_for(&self, retry_index: u32) -> Duration {
        let base = self.backoff_at(retry_index);
        if self.jitter && !base.is_zero() {
            base.mul_f64(jitter_scale())
        } else {
            base
        }
    }
}

/// A uniform random `f64` in `[0, 1)` from a per-thread xorshift PRNG, seeded once
/// from system entropy. For backoff jitter only — not cryptographic, and chosen to
/// avoid pulling in a PRNG dependency.
fn jitter_scale() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(jitter_seed());
    }
    STATE.with(|state| {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        // Top 53 bits → a uniform double in [0, 1).
        (x >> 11) as f64 / (1u64 << 53) as f64
    })
}

/// A nonzero per-thread seed from `RandomState` (system-entropy-seeded, the same
/// source `HashMap` uses), so threads decorrelate without a dependency.
fn jitter_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(0x9E37_79B9_7F4A_7C15);
    hasher.finish() | 1 // xorshift needs nonzero state
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
    fn non_finite_or_nonpositive_multiplier_is_treated_as_fixed() {
        let cap = Duration::from_secs(5);
        for bad in [f64::NAN, -1.0, 0.0, 0.5] {
            let p = RetryPolicy::new()
                .initial_backoff(Duration::from_millis(100))
                .multiplier(bad)
                .max_backoff(cap);
            // Treated as 1.0 → fixed backoff, never shrinking, never NaN.
            assert_eq!(p.backoff_at(0), Duration::from_millis(100));
            assert_eq!(p.backoff_at(3), Duration::from_millis(100));
        }
        // An infinite multiplier saturates to the cap past the first retry.
        let p = RetryPolicy::new()
            .initial_backoff(Duration::from_millis(100))
            .multiplier(f64::INFINITY)
            .max_backoff(cap);
        assert_eq!(p.backoff_at(0), Duration::from_millis(100));
        assert_eq!(p.backoff_at(1), cap);
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
