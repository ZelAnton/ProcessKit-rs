//! The shared unix graceful-shutdown driver.
//!
//! Both unix containment backends — the Linux cgroup and the POSIX
//! process-group fallback — escalate teardown the same way: send a graceful
//! signal to the whole tree, poll until it drains or a deadline passes, then
//! either hard-kill the survivors (`escalate`) or leave them running and tell
//! `Drop` to keep its hands off (`!escalate`). Only the mechanics differ
//! (a cgroup signals and kills through the cgroup file API; a process group via
//! `killpg`), so each backend supplies those primitives through
//! [`GracefulTarget`] and they share the escalation algorithm in [`run`].
//!
//! Windows has no graceful tier — its Job Object kill is atomic — so it does
//! not use this module.

use std::io;
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`): the deadline must share the
// same clock as the `sleep` below so it tracks tokio's virtual time under a
// paused runtime, which the hermetic tests here rely on.
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
    /// `group_seen` latch), but must NOT prune the tracked set: forgetting a
    /// survivor would corrupt a later `members()`/`stats()` under
    /// `escalate = false`.
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
///   so a `Duration::MAX`-ish value can't overflow `Instant + Duration` and
///   panic mid-teardown.
/// - `escalate`: on `true`, hard-kill any survivors once the deadline passes; on
///   `false`, leave them running and `request()` the `skip_drop_kill` latch so the
///   backend's `Drop` won't kill them either.
pub(crate) async fn run(
    target: &impl GracefulTarget,
    skip_drop_kill: &super::SkipDropKill,
    signal: i32,
    timeout: Duration,
    escalate: bool,
) -> io::Result<()> {
    // Best-effort: the graceful tier proceeds to polling regardless.
    target.signal_all(signal);
    // Clamp so a `Duration::MAX`-ish timeout can't overflow the `Instant` add.
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
        // Tell Drop not to hard-kill the survivors the caller chose to leave
        // alive; the latch makes the decision visible whichever thread runs Drop.
        skip_drop_kill.request();
    }
    Ok(())
}

/// The per-target primitives behind the **single-child** graceful kill-and-reap:
/// signal one shared-group child, observe its liveness, and force-kill a
/// survivor. Distinct from [`GracefulTarget`], which tears down a whole tree
/// (a cgroup or a POSIX process group): a **shared-group** run does not own its
/// group, so its teardown reaches only its own direct child, by pid — the
/// child's own descendants are the documented shared-group teardown gap.
pub(crate) trait PidTarget {
    /// Best-effort graceful signal to the child. A delivery failure (the child
    /// already exited, `EPERM`) is swallowed — the driver proceeds to poll.
    fn signal(&self, signal: i32);

    /// Whether the child is still alive — i.e. not yet exited *and reaped*.
    /// Returning `false` both ends the grace early and suppresses the final
    /// hard kill, so a reaped-and-recycled pid is never signalled.
    fn is_alive(&self) -> bool;

    /// Force a surviving child down (`SIGKILL`). Best-effort; a no-op if it is
    /// already gone.
    fn hard_kill(&self);
}

/// Drive a graceful kill-**and-reap** of a single shared-group child: signal it,
/// poll its liveness until it exits or `grace` elapses, then hard-kill a
/// survivor.
///
/// The final [`hard_kill`](PidTarget::hard_kill) is the load-bearing guarantee.
/// A child that catches the signal, closes its stdout, and keeps running is
/// polled `is_alive == true` for the whole grace and then forced down — even
/// though the streaming consumer already saw EOF on the closed stdout and
/// dropped its handle. The caller therefore runs this **detached** (its
/// `JoinHandle` untracked) so `RunningProcess::Drop` aborting the deadline
/// watchdog cannot cancel the kill mid-grace; and the shared-group child carries
/// no `kill_on_drop`, so this `SIGKILL` never races a Drop-triggered kill+reap
/// of a recycled pid. The reap itself is the runtime's: dropping the child's
/// `tokio::process::Child` hands it to tokio's orphan reaper, which collects it
/// once this kill lands.
///
/// When the child instead exits *on* the signal, [`is_alive`](PidTarget::is_alive)
/// flips to `false` and the driver returns **without** the hard kill: the reap
/// has already reclaimed the pid, so a `SIGKILL` there could hit an unrelated
/// process that recycled it.
///
/// `grace` is clamped to [`crate::MAX_DEADLINE`] so a `Duration::MAX`-ish value
/// can't overflow `Instant + Duration` and panic mid-teardown.
pub(crate) async fn run_pid(target: &impl PidTarget, signal: i32, grace: Duration) {
    // Best-effort: the driver proceeds to polling regardless of delivery.
    target.signal(signal);
    // Clamp so a `Duration::MAX`-ish grace can't overflow the `Instant` add.
    let deadline = Instant::now() + grace.min(crate::MAX_DEADLINE);
    loop {
        let now = Instant::now();
        if now >= deadline {
            break; // grace elapsed with the child still around → hard kill below
        }
        if !target.is_alive() {
            return; // exited (and reaped) within the grace → skip the SIGKILL
        }
        // Never oversleep past the deadline, however large `POLL_INTERVAL` is
        // relative to the remaining grace.
        sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
    target.hard_kill();
}

/// The real single-child target: a live pid signalled, probed, and killed via
/// `libc`. Wraps only the pid; the graceful signal is passed per call so the
/// value stays stateless (mirroring [`GracefulTarget::signal_all`]).
#[cfg(unix)]
pub(crate) struct UnixChild(i32);

#[cfg(unix)]
impl UnixChild {
    pub(crate) fn new(pid: i32) -> Self {
        Self(pid)
    }
}

#[cfg(unix)]
impl PidTarget for UnixChild {
    fn signal(&self, signal: i32) {
        // SAFETY: sending a signal to a pid is sound; `ESRCH` (already gone) is
        // ignored — the poll below observes the drain regardless.
        unsafe {
            libc::kill(self.0, signal);
        }
    }

    fn is_alive(&self) -> bool {
        // SAFETY: signal 0 is a pure existence probe. `ESRCH` → gone; `EPERM` →
        // alive but unsignallable (a uid-changed child) — treat as exists so a
        // still-live tree is not abandoned; any other rc is treated as alive.
        let rc = unsafe { libc::kill(self.0, 0) };
        rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    fn hard_kill(&self) {
        // SAFETY: `SIGKILL` to the pid; a no-op `ESRCH` if it is already gone.
        unsafe {
            libc::kill(self.0, libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        let skip = crate::sys::SkipDropKill::new();
        run(&target, &skip, 15, Duration::from_secs(10), true)
            .await
            .expect("graceful run");
        assert_eq!(target.signals.load(Ordering::Relaxed), 1, "signalled once");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            0,
            "no escalation"
        );
        assert!(!skip.is_set(), "escalate path leaves skip clear");
    }

    #[tokio::test(start_paused = true)]
    async fn drains_mid_poll_does_not_escalate() {
        // Alive for three drain checks, then drained — the loop polls, sleeps
        // (auto-advanced under start_paused), and exits before the deadline.
        let target = FakeTarget::new(3);
        let skip = crate::sys::SkipDropKill::new();
        run(&target, &skip, 15, Duration::from_secs(10), true)
            .await
            .expect("graceful run");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            0,
            "drained in time"
        );
        assert!(!skip.is_set());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_elapses_after_polling_then_escalates() {
        // Stays alive past the timeout: only terminates because the deadline
        // shares tokio's virtual clock with the sleeps — a regression to
        // `std::time::Instant` would hang here.
        let target = FakeTarget::new(usize::MAX);
        let skip = crate::sys::SkipDropKill::new();
        run(&target, &skip, 15, Duration::from_millis(50), true)
            .await
            .expect("graceful run");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            1,
            "escalated after the deadline elapsed"
        );
        assert!(!skip.is_set());
    }

    #[tokio::test]
    async fn not_drained_by_deadline_escalates_when_asked() {
        // Never drains within the test; a zero timeout makes the deadline pass
        // on the first check, so the loop breaks without sleeping.
        let target = FakeTarget::new(usize::MAX);
        let skip = crate::sys::SkipDropKill::new();
        run(&target, &skip, 15, Duration::ZERO, true)
            .await
            .expect("graceful run");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            1,
            "escalated once"
        );
        assert!(!skip.is_set(), "escalation does not set skip");
    }

    #[tokio::test]
    async fn not_drained_without_escalation_sets_skip_and_spares_survivors() {
        let target = FakeTarget::new(usize::MAX);
        let skip = crate::sys::SkipDropKill::new();
        run(&target, &skip, 15, Duration::ZERO, false)
            .await
            .expect("graceful run");
        assert_eq!(target.hard_kills.load(Ordering::Relaxed), 0, "no hard kill");
        assert!(skip.is_set(), "skip set so Drop spares survivors");
    }

    #[tokio::test]
    async fn hard_kill_error_propagates() {
        let mut target = FakeTarget::new(usize::MAX);
        target.fail_hard_kill = true;
        let skip = crate::sys::SkipDropKill::new();
        let err = run(&target, &skip, 15, Duration::ZERO, true)
            .await
            .expect_err("hard_kill failure surfaces");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(!skip.is_set());
    }

    #[tokio::test]
    async fn saturating_timeout_does_not_panic() {
        // Duration::MAX must be clamped before the `Instant + Duration`.
        let target = FakeTarget::new(0); // drained immediately so we don't wait
        let skip = crate::sys::SkipDropKill::new();
        run(&target, &skip, 15, Duration::MAX, true)
            .await
            .expect("graceful run with saturating timeout");
    }

    /// A scriptable [`PidTarget`] for the single-child driver: records the
    /// graceful signal and hard kills, and reports "alive" for the first
    /// `alive_polls` liveness checks, then "gone".
    struct FakePid {
        signals: AtomicUsize,
        last_signal: std::sync::atomic::AtomicI32,
        hard_kills: AtomicUsize,
        alive_polls: AtomicUsize,
    }

    impl FakePid {
        /// Reports alive for `alive_polls` liveness checks, then gone forever.
        /// `usize::MAX` models a child that catches the signal and keeps running
        /// for the whole grace; a small value models one that exits mid-grace.
        fn new(alive_polls: usize) -> Self {
            Self {
                signals: AtomicUsize::new(0),
                last_signal: std::sync::atomic::AtomicI32::new(0),
                hard_kills: AtomicUsize::new(0),
                alive_polls: AtomicUsize::new(alive_polls),
            }
        }
    }

    impl PidTarget for FakePid {
        fn signal(&self, signal: i32) {
            self.signals.fetch_add(1, Ordering::Relaxed);
            self.last_signal.store(signal, Ordering::Relaxed);
        }

        fn is_alive(&self) -> bool {
            let remaining = self.alive_polls.load(Ordering::Relaxed);
            if remaining == 0 {
                return false;
            }
            self.alive_polls.store(remaining - 1, Ordering::Relaxed);
            true
        }

        fn hard_kill(&self) {
            self.hard_kills.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn pid_child_that_catches_the_signal_is_still_hard_killed() {
        // The child stays alive the whole grace (caught the signal, closed
        // stdout, kept running): the driver polls, sleeps (auto-advanced under
        // start_paused), reaches the deadline, and must deliver the final kill.
        let target = FakePid::new(usize::MAX);
        run_pid(&target, 15, Duration::from_millis(100)).await;
        assert_eq!(target.signals.load(Ordering::Relaxed), 1, "signalled once");
        assert_eq!(
            target.last_signal.load(Ordering::Relaxed),
            15,
            "the configured graceful signal is delivered, not a hard-coded one"
        );
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            1,
            "a survivor that rode out the grace is force-killed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pid_child_that_exits_within_grace_skips_the_hard_kill() {
        // Alive for two polls, then gone — the child exited on the signal within
        // the grace. The driver must NOT hard-kill: the pid may already be
        // reaped and recycled, so a SIGKILL there could hit a stranger.
        let target = FakePid::new(2);
        run_pid(&target, 15, Duration::from_secs(10)).await;
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            0,
            "a child gone within the grace is not force-killed (no recycled-pid SIGKILL)"
        );
    }

    #[tokio::test]
    async fn pid_saturating_grace_does_not_panic() {
        // Duration::MAX must be clamped before the `Instant + Duration` add.
        let target = FakePid::new(0); // gone on the first poll so we don't wait
        run_pid(&target, 15, Duration::MAX).await;
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            0,
            "an already-gone child is not force-killed"
        );
    }
}
