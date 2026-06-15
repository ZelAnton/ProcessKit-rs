//! The shared unix graceful-shutdown driver.
//!
//! Both unix containment backends — the Linux cgroup and the POSIX
//! process-group fallback — escalate teardown the same way: send a graceful
//! signal to the whole tree, poll until it drains or a deadline passes, then
//! either hard-kill the survivors (`escalate`) or leave them running and tell
//! `Drop` to keep its hands off (`!escalate`, B12). Only the mechanics differ
//! (a cgroup signals and kills through the cgroup file API; a process group via
//! `killpg`), so each backend supplies those primitives through
//! [`GracefulTarget`] and they share the escalation algorithm in [`run`].
//!
//! Windows has no graceful tier — its Job Object kill is atomic — so it does
//! not use this module.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`): the deadline must share the
// same clock as the `sleep` below, so it tracks tokio's virtual time under a
// paused runtime — both for the hermetic tests here and to match the inline
// loops this driver replaced byte-for-byte.
use tokio::time::{Instant, sleep};

/// How often the graceful tier re-checks whether the tree has drained.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The per-backend primitives behind the shared escalation algorithm: a
/// teardown target the [`run`] driver can signal, observe, and hard-kill.
pub(crate) trait GracefulTarget {
    /// Best-effort graceful signal to every process in the tree. Failures are
    /// swallowed — the driver proceeds to poll regardless.
    fn signal_all(&self, signal: i32);

    /// Whether the tree has fully drained (no tracked process remains alive).
    /// May refresh a backend's internal liveness cache (e.g. the pgroup
    /// `group_seen` latch), but must NOT prune the tracked set: [`run`] polls
    /// this repeatedly, and forgetting a survivor would corrupt a later
    /// `members()`/`stats()` under `escalate = false`.
    fn is_drained(&self) -> bool;

    /// Forcibly kill any survivors. Called only when escalation is requested
    /// and the tree has not drained by the deadline.
    fn hard_kill(&self) -> io::Result<()>;
}

/// Drive a graceful shutdown of `target`: signal the tree, poll until it drains
/// or the deadline passes, then escalate or stand down.
///
/// - `signal` is the graceful signal (usually `SIGTERM`).
/// - `timeout` bounds the polling wait; it is clamped to [`crate::MAX_DEADLINE`]
///   (E15) so a `Duration::MAX`-ish value can't overflow `Instant + Duration`
///   and panic mid-teardown.
/// - `escalate`: on `true`, hard-kill any survivors once the deadline passes; on
///   `false`, leave them running and set `skip_drop_kill` so the backend's
///   `Drop` won't kill them either (B12).
pub(crate) async fn run(
    target: &impl GracefulTarget,
    skip_drop_kill: &AtomicBool,
    signal: i32,
    timeout: Duration,
    escalate: bool,
) -> io::Result<()> {
    // Best-effort: the graceful tier proceeds to polling regardless.
    target.signal_all(signal);
    // E15: clamp so a `Duration::MAX`-ish timeout can't overflow.
    let deadline = Instant::now() + timeout.min(crate::MAX_DEADLINE);
    while !target.is_drained() {
        if Instant::now() >= deadline {
            break;
        }
        sleep(POLL_INTERVAL).await;
    }
    if escalate && !target.is_drained() {
        target.hard_kill()?;
    } else if !escalate {
        // B12: tell Drop not to hard-kill the survivors the caller chose to
        // leave alive. `Release` here pairs with the `Acquire` load in each
        // backend's `Drop`, so the store is visible whichever thread runs Drop:
        // a tokio task can migrate across the `.await`s between this call and the
        // owner dropping the backend, so a "single-threaded boundary" can't be
        // assumed (P2-2).
        skip_drop_kill.store(true, Ordering::Release);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A scriptable `GracefulTarget` that counts the driver's calls and reports
    /// "alive" for the first `alive_polls` drain checks, then "drained".
    struct FakeTarget {
        signals: AtomicUsize,
        hard_kills: AtomicUsize,
        alive_polls: AtomicUsize,
        fail_hard_kill: bool,
    }

    impl FakeTarget {
        /// Reports alive for `alive_polls` drain checks, then drained forever.
        /// `alive_polls == 0` means drained on the very first check.
        fn new(alive_polls: usize) -> Self {
            Self {
                signals: AtomicUsize::new(0),
                hard_kills: AtomicUsize::new(0),
                alive_polls: AtomicUsize::new(alive_polls),
                fail_hard_kill: false,
            }
        }
    }

    impl GracefulTarget for FakeTarget {
        fn signal_all(&self, _signal: i32) {
            self.signals.fetch_add(1, Ordering::Relaxed);
        }

        fn is_drained(&self) -> bool {
            // Single-threaded in tests; a plain load/store decrement is fine.
            let remaining = self.alive_polls.load(Ordering::Relaxed);
            if remaining == 0 {
                return true;
            }
            self.alive_polls.store(remaining - 1, Ordering::Relaxed);
            false
        }

        fn hard_kill(&self) -> io::Result<()> {
            self.hard_kills.fetch_add(1, Ordering::Relaxed);
            if self.fail_hard_kill {
                Err(io::Error::other("hard_kill failed"))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn drained_before_deadline_does_not_escalate() {
        let target = FakeTarget::new(0); // drained on first check
        let skip = AtomicBool::new(false);
        run(&target, &skip, 15, Duration::from_secs(10), true)
            .await
            .expect("graceful run");
        assert_eq!(target.signals.load(Ordering::Relaxed), 1, "signalled once");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            0,
            "no escalation"
        );
        assert!(
            !skip.load(Ordering::Relaxed),
            "escalate path leaves skip clear"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drains_mid_poll_does_not_escalate() {
        // Alive for three drain checks, then drained — the loop polls, sleeps
        // (auto-advanced under start_paused), and exits before the deadline.
        let target = FakeTarget::new(3);
        let skip = AtomicBool::new(false);
        run(&target, &skip, 15, Duration::from_secs(10), true)
            .await
            .expect("graceful run");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            0,
            "drained in time"
        );
        assert!(!skip.load(Ordering::Relaxed));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_elapses_after_polling_then_escalates() {
        // Stays alive past the timeout: the loop polls and sleeps (virtual time
        // auto-advances) until `now >= deadline`, then escalates. This only
        // terminates because the deadline shares tokio's virtual clock with the
        // sleeps — a regression to `std::time::Instant` would hang here.
        let target = FakeTarget::new(usize::MAX);
        let skip = AtomicBool::new(false);
        run(&target, &skip, 15, Duration::from_millis(50), true)
            .await
            .expect("graceful run");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            1,
            "escalated after the deadline elapsed"
        );
        assert!(!skip.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn not_drained_by_deadline_escalates_when_asked() {
        // Never drains within the test; a zero timeout makes the deadline pass
        // on the first check, so the loop breaks without sleeping.
        let target = FakeTarget::new(usize::MAX);
        let skip = AtomicBool::new(false);
        run(&target, &skip, 15, Duration::ZERO, true)
            .await
            .expect("graceful run");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            1,
            "escalated once"
        );
        assert!(
            !skip.load(Ordering::Relaxed),
            "escalation does not set skip"
        );
    }

    #[tokio::test]
    async fn not_drained_without_escalation_sets_skip_and_spares_survivors() {
        let target = FakeTarget::new(usize::MAX);
        let skip = AtomicBool::new(false);
        run(&target, &skip, 15, Duration::ZERO, false)
            .await
            .expect("graceful run");
        assert_eq!(target.hard_kills.load(Ordering::Relaxed), 0, "no hard kill");
        assert!(
            skip.load(Ordering::Relaxed),
            "skip set so Drop spares survivors"
        );
    }

    #[tokio::test]
    async fn hard_kill_error_propagates() {
        let mut target = FakeTarget::new(usize::MAX);
        target.fail_hard_kill = true;
        let skip = AtomicBool::new(false);
        let err = run(&target, &skip, 15, Duration::ZERO, true)
            .await
            .expect_err("hard_kill failure surfaces");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(!skip.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn saturating_timeout_does_not_panic() {
        // E15: Duration::MAX must be clamped before the `Instant + Duration`.
        let target = FakeTarget::new(0); // drained immediately so we don't wait
        let skip = AtomicBool::new(false);
        run(&target, &skip, 15, Duration::MAX, true)
            .await
            .expect("graceful run with saturating timeout");
    }
}
