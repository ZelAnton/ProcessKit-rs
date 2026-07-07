//! Shared backoff math: the capped-exponential delay formula and a source of
//! unit-interval randomness, both used by [`RetryPolicy`](crate::RetryPolicy)'s
//! `backoff_at`/`delay_for` and [`Supervisor`](crate::Supervisor)'s restart
//! backoff. The two callers deliberately apply *different* jitter models on
//! top (full jitter `[0, delay]` vs. multiplicative `[0.5, 1.5) × delay`) —
//! that difference is intentional and lives in each caller, not here.

use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// `min(base × factor^n, cap)` — the capped-exponential backoff shared by
/// [`RetryPolicy::backoff_at`](crate::retry::RetryPolicy) and
/// [`Supervisor`](crate::Supervisor)'s restart backoff.
///
/// - A zero `base` never waits, whatever `n`/`factor` — this short-circuits
///   the `0.0 * inf = NaN` an overflowing factor would otherwise produce.
/// - A `factor` that is non-finite (NaN, ±∞) or `<= 1.0` is treated as `1.0`
///   (backoff never shrinks, and never explodes on the very next step).
/// - `n` is clamped so `n as i32` can't wrap negative for an absurd count;
///   any realistic `n` is far below the cap anyway.
/// - The fixed case (folded `factor == 1.0`, or `n == 0`) returns the
///   byte-exact `base` (capped) rather than round-tripping through `f64`.
/// - Otherwise the result saturates to `cap` on overflow/non-finite rather
///   than panicking.
pub(crate) fn capped_exponential(base: Duration, factor: f64, n: u32, cap: Duration) -> Duration {
    if base.is_zero() {
        return Duration::ZERO;
    }
    let factor = if factor.is_finite() && factor > 1.0 {
        factor
    } else {
        1.0
    };
    if factor == 1.0 || n == 0 {
        return base.min(cap);
    }
    // Clamp the exponent so `n as i32` can't wrap negative (which would
    // *shrink* the backoff) for an absurd retry/restart count.
    let exponent = n.min(i32::MAX as u32) as i32;
    let secs = (base.as_secs_f64() * factor.powi(exponent)).min(cap.as_secs_f64());
    // `try_from_secs_f64` rejects negative/NaN/overflow — fall back to the cap.
    Duration::try_from_secs_f64(secs).unwrap_or(cap)
}

/// A uniform random `f64` in `[0, 1)` from a per-thread xorshift PRNG, seeded
/// once from system entropy. For backoff jitter only — not cryptographic, and
/// chosen to avoid pulling in a PRNG dependency.
pub(crate) fn unit_random_f64() -> f64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(seed());
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

/// A nonzero per-thread seed from `RandomState` (system-entropy-seeded, the
/// same source `HashMap` uses), so threads decorrelate without a dependency.
fn seed() -> u64 {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(0x9E37_79B9_7F4A_7C15);
    hasher.finish() | 1 // xorshift needs nonzero state
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_base_never_waits() {
        assert_eq!(
            capped_exponential(Duration::ZERO, 2.0, 0, Duration::from_secs(1)),
            Duration::ZERO
        );
        assert_eq!(
            capped_exponential(Duration::ZERO, 2.0, u32::MAX, Duration::from_secs(1)),
            Duration::ZERO
        );
    }

    #[test]
    fn exponential_growth_and_cap() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_millis(450);
        assert_eq!(capped_exponential(base, 2.0, 0, cap), base);
        assert_eq!(
            capped_exponential(base, 2.0, 1, cap),
            Duration::from_millis(200)
        );
        assert_eq!(
            capped_exponential(base, 2.0, 2, cap),
            Duration::from_millis(400)
        );
        assert_eq!(capped_exponential(base, 2.0, 3, cap), cap); // 800 → capped
        assert_eq!(capped_exponential(base, 2.0, 50, cap), cap); // far past cap
    }

    #[test]
    fn non_finite_or_sub_unit_factor_is_fixed() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(5);
        for bad in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -1.0,
            0.0,
            0.5,
            1.0,
        ] {
            assert_eq!(capped_exponential(base, bad, 0, cap), base, "bad = {bad}");
            assert_eq!(capped_exponential(base, bad, 3, cap), base, "bad = {bad}");
        }
    }

    #[test]
    fn unit_random_f64_is_in_range() {
        for _ in 0..256 {
            let u = unit_random_f64();
            assert!((0.0..1.0).contains(&u), "out of [0, 1): {u}");
        }
    }
}
