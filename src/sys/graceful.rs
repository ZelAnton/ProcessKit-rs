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

#[cfg(unix)]
use std::sync::Arc;

#[cfg(unix)]
use super::pid_gate::PidGate;

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
///
/// The `skip_drop_kill` spare is keyed to a generation snapshotted **before**
/// signalling or polling: a `spawn`/`adopt` that re-arms the backstop while this
/// shutdown is in flight (the task may migrate across the poll `.await`s and land
/// the final `request` on another thread) bumps that generation, so the stale
/// `request` no-ops and the freshly-spawned child keeps its Drop-kill backstop.
pub(crate) async fn run(
    target: &impl GracefulTarget,
    skip_drop_kill: &super::SkipDropKill,
    signal: i32,
    timeout: Duration,
    escalate: bool,
) -> io::Result<()> {
    // Snapshot the re-arm generation up front: any spawn/adopt that re-arms the
    // backstop after this point must win over this shutdown's later spare, so the
    // window has to cover the signal + poll below, not just the final `request`.
    let epoch = skip_drop_kill.begin_shutdown();
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
        // Keyed to `epoch`, so a spawn/adopt that re-armed mid-shutdown wins and
        // this spare becomes a no-op — the fresh child is still torn down.
        skip_drop_kill.request(epoch);
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
/// of a recycled pid. The reap that frees the pid is owned by whoever owns the
/// `Child` — a consuming finisher, or (when the consumer dropped its handle) the
/// detached gated reaper `RunningProcess::Drop` hands the child to — and *that*
/// reap retires the shared `PidGate` atomically, standing this driver down before
/// its `SIGKILL`/liveness probe could touch the freed pid. The reap is never left
/// to tokio's orphan reaper, which would free the pid without retiring the gate.
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
/// `libc`, every raw operation routed through a shared [`PidGate`]. The graceful
/// signal is passed per call so the value stays stateless (mirroring
/// [`GracefulTarget::signal_all`]); the gate carries the pid and the "retired"
/// state, so each syscall and the retired check happen in one indivisible step —
/// a reap that frees the pid cannot slip between them and leave a `SIGKILL` to
/// land on a recycled pid.
#[cfg(unix)]
pub(crate) struct UnixChild {
    /// The gate shared with the pid's owner: it holds the pid and the retired
    /// latch. Once the owner reaps (retires the gate),
    /// [`signal`](Self::signal)/[`hard_kill`](Self::hard_kill) become no-ops and
    /// [`is_alive`](Self::is_alive) reports "gone", so the driver ends the grace
    /// early and skips the final `SIGKILL` — never signalling a pid the OS may
    /// have recycled. This is the pid-teardown use of the same [`PidGate`] the
    /// detached deadline/cancel watchdogs kill through.
    gate: Arc<PidGate>,
}

#[cfg(unix)]
impl UnixChild {
    pub(crate) fn new(gate: Arc<PidGate>) -> Self {
        Self { gate }
    }
}

#[cfg(unix)]
impl PidTarget for UnixChild {
    fn signal(&self, signal: i32) {
        self.gate.with_live_pid((), |pid| {
            // SAFETY: sending a signal to a pid is sound; `ESRCH` (already gone)
            // is ignored — the poll below observes the drain regardless. Runs
            // under the gate lock, so a retired pid is never signalled.
            unsafe {
                libc::kill(pid as i32, signal);
            }
        });
    }

    fn is_alive(&self) -> bool {
        // A retired pid is gone by definition, whatever `kill(pid, 0)` says about
        // whoever recycled it — `with_live_pid` returns the `false` default when
        // the gate is retired, which is the check that stops a recycled-pid
        // `SIGKILL`.
        self.gate.with_live_pid(false, |pid| {
            // SAFETY: signal 0 is a pure existence probe. `ESRCH` → gone; `EPERM`
            // → alive but unsignallable (a uid-changed child) — treat as exists so
            // a still-live tree is not abandoned; any other rc is treated as
            // alive.
            let rc = unsafe { libc::kill(pid as i32, 0) };
            rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        })
    }

    fn hard_kill(&self) {
        self.gate.with_live_pid((), |pid| {
            // SAFETY: `SIGKILL` to the pid; a no-op `ESRCH` if it is already gone.
            // Runs under the gate lock, so a retired pid is never force-killed.
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
        });
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

    // T-079: a spawn/adopt that re-arms the backstop while a non-escalating
    // shutdown is mid-poll must win — the shutdown's final (now stale) request
    // must not re-spare the fresh child. Deterministic via the paused clock: the
    // fake target re-arms the shared latch on a poll, standing in for a concurrent
    // spawn/adopt that lands during the drain wait.
    #[tokio::test(start_paused = true)]
    async fn a_concurrent_rearm_wins_over_a_stale_non_escalating_request() {
        // A target that re-arms the shared latch on its second drain check, then
        // keeps reporting "not drained" so the loop runs to the deadline and issues
        // its (stale) request.
        struct RacingRearm<'a> {
            latch: &'a crate::sys::SkipDropKill,
            polls: AtomicUsize,
        }
        impl GracefulTarget for RacingRearm<'_> {
            fn signal_all(&self, _signal: i32) {}
            fn is_drained(&self) -> bool {
                if self.polls.fetch_add(1, Ordering::Relaxed) == 1 {
                    // A concurrent spawn/adopt re-arms the backstop for a fresh
                    // child, exactly as `ProcessGroup::spawn`/cgroup spawn would.
                    self.latch.clear();
                }
                false
            }
            fn hard_kill(&self) -> io::Result<()> {
                Ok(())
            }
        }

        let skip = crate::sys::SkipDropKill::new();
        // A live reused group: an earlier spawn re-armed once, so the shutdown
        // starts from a non-zero generation just like a real group.
        skip.clear();
        let target = RacingRearm {
            latch: &skip,
            polls: AtomicUsize::new(0),
        };
        run(&target, &skip, 15, Duration::from_millis(100), false)
            .await
            .expect("graceful run");
        assert!(
            !skip.is_set(),
            "a spawn/adopt that re-armed mid-shutdown must not be re-spared by the \
             shutdown's stale request — the fresh child keeps its Drop-kill backstop"
        );
    }

    // The no-race counterpart: with nothing re-arming during the drain wait, a
    // non-escalating shutdown still spares the survivors it set out to.
    #[tokio::test(start_paused = true)]
    async fn a_non_escalating_shutdown_without_a_race_still_spares() {
        let target = FakeTarget::new(3); // alive for a few polls, then drained
        let skip = crate::sys::SkipDropKill::new();
        skip.clear(); // a pre-existing survivor set (non-zero generation)
        run(&target, &skip, 15, Duration::from_secs(10), false)
            .await
            .expect("graceful run");
        assert!(
            skip.is_set(),
            "an unraced non-escalating shutdown spares its survivors on Drop"
        );
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

    // A `UnixChild` whose owner has retired its gate (it reaped the pid) must
    // report the pid *gone* regardless of what `kill(pid, 0)` says about whoever
    // the OS recycled the pid to. This is the load-bearing gate behind T-066/T-078:
    // winning the timeout arbiter's CAS is not proof the pid is un-reaped, so the
    // pid-only graceful kill leans on the gate instead. We probe our own pid —
    // unquestionably alive — so a broken gate would report `true` (and, in
    // `run_pid`, would try to signal us) rather than silently pass against an
    // already-dead ESRCH pid.
    #[cfg(unix)]
    #[test]
    fn a_retired_unix_child_reports_gone_even_for_a_live_pid() {
        let gate = std::sync::Arc::new(PidGate::new(Some(std::process::id())));
        gate.retire();
        let child = UnixChild::new(gate);
        assert!(
            !child.is_alive(),
            "a retired pid must report gone, not probe the recycled pid alive"
        );
        // Sanity: it is the gate, not a broken probe, that flips liveness — the
        // same live pid probes alive through a fresh, un-retired gate.
        let live_gate = std::sync::Arc::new(PidGate::new(Some(std::process::id())));
        let live_child = UnixChild::new(live_gate);
        assert!(
            live_child.is_alive(),
            "a live, un-retired pid still probes alive"
        );
    }

    // End to end through the driver: a target retired before `run_pid` runs is
    // left completely alone — no graceful signal, no final `SIGKILL` — even though
    // its pid resolves to a live process (ours). `signal`/`hard_kill` are guarded
    // by the same gate, so a regression is caught by the timing assertion below,
    // never by signalling the test runner.
    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn run_pid_leaves_a_retired_live_pid_untouched() {
        let gate = std::sync::Arc::new(PidGate::new(Some(std::process::id())));
        gate.retire();
        let target = UnixChild::new(gate);
        let start = Instant::now();
        run_pid(&target, 15, Duration::from_secs(10)).await;
        // `is_alive` returns false on the first poll, so the driver returns
        // without consuming any of the grace: no sleep is awaited, so paused
        // virtual time never advances. A regression that kept polling a retired
        // pid would instead burn the whole 10 s grace before its (guarded) hard
        // kill, tripping this bound.
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "a retired target ends the grace immediately, before any hard kill"
        );
    }

    // T-082 (Window 2): the detached shared-group graceful kill-and-reap must stand
    // down the instant the pid's owner reaps — including when that reap is the
    // detached Drop reaper landing *mid-grace* (the streaming consumer dropped its
    // handle, so `RunningProcess::Drop` handed the child to a gated reaper that
    // reaps under the gate and retires). Modelled deterministically: a target that
    // retires the shared gate right after the driver's first liveness poll (standing
    // in for that mid-grace reap). The very next poll must report "gone", so the
    // driver returns WITHOUT its final `SIGKILL` — never signalling a pid the OS may
    // have recycled. We probe our own (unquestionably live) pid, so a regression
    // that kept polling/killing would either ride out the whole grace (tripping the
    // timing bound) or fire a real, gate-guarded — hence no-op — hard kill (tripping
    // the count). Under the paused clock, standing down burns no virtual time. The
    // model's `signal` is a counting no-op (see the method) so the up-front graceful
    // signal — issued before the first poll retires the gate — never lands on our own
    // still-live pid and terminates the runner.
    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn a_reap_landing_mid_grace_stands_the_detached_kill_down() {
        struct RetireAfterFirstPoll {
            inner: UnixChild,
            gate: std::sync::Arc<PidGate>,
            signals: AtomicUsize,
            polls: AtomicUsize,
            hard_kills: AtomicUsize,
        }
        impl PidTarget for RetireAfterFirstPoll {
            fn signal(&self, _signal: i32) {
                // A counting no-op — deliberately NOT delegating to
                // `self.inner.signal`. `run_pid` issues the graceful signal up
                // front, *before* the first liveness poll retires the gate (the
                // retire fires only inside `is_alive`). At that instant the gate is
                // still live and its pid is our own (`std::process::id()`), so a real
                // `UnixChild::signal` would `kill(getpid(), SIGTERM)` and terminate
                // the un-retired test runner. What this test exercises is the gate's
                // liveness/hard-kill behaviour, not signal delivery, so we merely
                // record that the driver issued the graceful signal.
                self.signals.fetch_add(1, Ordering::SeqCst);
            }
            fn is_alive(&self) -> bool {
                let alive = self.inner.is_alive();
                // After the first live poll, the pid's owner reaps and retires the
                // gate (the Drop reaper), so the next poll must see "gone".
                if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.gate.retire();
                }
                alive
            }
            fn hard_kill(&self) {
                self.hard_kills.fetch_add(1, Ordering::SeqCst);
                self.inner.hard_kill();
            }
        }

        let gate = std::sync::Arc::new(PidGate::new(Some(std::process::id())));
        let target = RetireAfterFirstPoll {
            inner: UnixChild::new(gate.clone()),
            gate,
            signals: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            hard_kills: AtomicUsize::new(0),
        };
        let start = Instant::now();
        run_pid(&target, 15, Duration::from_secs(10)).await;
        assert_eq!(
            target.signals.load(Ordering::SeqCst),
            1,
            "the driver issues the graceful signal once, up front, before it polls"
        );
        assert_eq!(
            target.hard_kills.load(Ordering::SeqCst),
            0,
            "a reap landing mid-grace must suppress the final SIGKILL (no recycled-pid kill)"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "the driver stands down on the next poll, not after riding out the grace"
        );
    }
}
