//! The shared graceful-shutdown driver.
//!
//! Every containment backend escalates teardown the same way: send a graceful
//! signal to the whole tree, poll until it drains or a deadline passes, then
//! either hard-kill the survivors (`escalate`) or leave them running and tell
//! `Drop` to keep its hands off (`!escalate`). Only the mechanics differ
//! (a Linux cgroup signals and kills through the cgroup file API; a POSIX
//! process group via `killpg`; a Windows Job Object via a console CTRL_BREAK and
//! `TerminateJobObject`), so each backend supplies those primitives through
//! [`GracefulTarget`] and they share the escalation algorithm in [`run`].
//!
//! Windows has no *soft signal* tier by default — its Job Object kill is atomic —
//! but the opt-in `windows_graceful_ctrl_break` path (a direct child spawned
//! `CREATE_NEW_PROCESS_GROUP`) does drive [`run`] with a Job-backed
//! `GracefulTarget`, so [`run`] and [`GracefulTarget`] are cross-platform. The
//! single-child kill-and-reap primitives below ([`PidTarget`]/[`run_pid`]/
//! [`UnixChild`]) lean on `PidGate`/`libc` and stay unix-only.

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

/// What became of a graceful teardown's best-effort **soft-signal** tier — the
/// observed fate of the `SIGTERM` / `CTRL_BREAK` / `WM_CLOSE` request the driver
/// issues before the grace window. The always-available, feature-agnostic core
/// behind the public `SoftSignal` report enum: it carries no public `Signal`, so
/// the unconditional `shutdown`/`shutdown_ref` paths can produce it too (and
/// simply discard it).
///
/// A target that actually reaches [`run`] always *has* a soft-signal tier (unix
/// always signals; the Windows atomic branch that has neither a console-CTRL
/// leader nor a windowed member never drives the driver), so
/// [`signal_all`](GracefulTarget::signal_all) only ever returns [`Sent`](Self::Sent)
/// or [`Failed`](Self::Failed). [`Unsupported`](Self::Unsupported) is synthesised
/// by that atomic branch alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SoftDelivery {
    /// The soft signal was delivered best-effort to the tree (a POSIX signal on
    /// unix; at least one `CTRL_BREAK`/`WM_CLOSE` trigger posted on the Windows
    /// soft tier).
    Sent,
    /// This platform/target has no soft-signal tier for the group, so the teardown
    /// could only hard-kill: a windowless Windows Job Object with no console-CTRL
    /// leader. Constructed only by that Windows atomic branch — every Unix backend
    /// always has a real `SIGTERM` tier — so it is dead on Unix (the variant stays
    /// in the cross-platform enum the public `SoftSignal::Unsupported` maps from).
    #[cfg_attr(not(windows), allow(dead_code))]
    Unsupported,
    /// A soft-signal tier exists but the best-effort delivery failed for every
    /// target (a uid-changed member rejecting the signal on unix; no live
    /// console/window target reached on Windows).
    Failed,
}

/// The observed facts of one whole-tree graceful teardown, surfaced by the shared
/// driver [`run`] (and synthesised by the Windows atomic branch that bypasses it).
/// The always-available core the public `ShutdownReport` is built from; kept
/// feature-agnostic so the unconditional `shutdown`/`shutdown_ref` paths produce it
/// and discard it, while the `process-control`-gated public method reads it.
#[derive(Debug, Clone, Copy)]
// Only the `process-control`-gated public report reads these fields; the
// unconditional shutdown paths construct and discard the value.
#[cfg_attr(not(feature = "process-control"), allow(dead_code))]
pub(crate) struct GracefulOutcome {
    /// The fate of the best-effort soft-signal tier.
    pub soft: SoftDelivery,
    /// Live members observed just before the soft signal, or `None` if the
    /// membership could not be read.
    pub members_before: Option<usize>,
    /// Live members observed after the grace window and any hard kill, or `None`
    /// if the membership could not be read.
    pub members_after: Option<usize>,
    /// Whether the tree drained within the grace window, before any hard kill.
    pub drained: bool,
    /// Whether the driver escalated to a hard kill.
    pub escalated: bool,
    /// How long the teardown actually took. Measured on the tokio clock, so the
    /// hermetic paused-clock tests observe virtual time (an early drain reports a
    /// short duration, not the whole grace).
    pub elapsed: Duration,
}

/// The per-backend primitives behind the shared escalation algorithm: a
/// teardown target the [`run`] driver can signal, observe, count, and hard-kill.
pub(crate) trait GracefulTarget {
    /// Best-effort graceful signal to every process in the tree, reporting whether
    /// the tier actually delivered ([`SoftDelivery::Sent`]) or failed for every
    /// target ([`SoftDelivery::Failed`]). Delivery failures never stop the driver —
    /// it proceeds to poll regardless; the verdict is recorded for the report only.
    /// A target reaching this method always has a soft-signal tier, so it never
    /// returns [`SoftDelivery::Unsupported`].
    fn signal_all(&self, signal: i32) -> SoftDelivery;

    /// Whether the tree has fully drained (no tracked process remains alive).
    /// May refresh a backend's internal liveness cache (e.g. the pgroup
    /// `group_seen` latch), but must NOT prune the tracked set: forgetting a
    /// survivor would corrupt a later `members()`/`stats()` under
    /// `escalate = false`.
    fn is_drained(&self) -> bool;

    /// How many tracked members are currently alive, or `None` if the membership
    /// could not be read (e.g. an unreadable `cgroup.procs` or a failed Job Object
    /// query). Probe-only, like [`is_drained`](Self::is_drained): it must **not**
    /// prune the tracked set (the report's before/after counts are observations,
    /// not a teardown side-effect). Counts the same member set the backend's
    /// `members()` reports (the whole tree on the cgroup/Job Object mechanisms, the
    /// tracked group leaders on the POSIX process-group fallback).
    fn alive_count(&self) -> Option<usize>;

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
) -> io::Result<GracefulOutcome> {
    // Anchor the reported duration on the tokio clock (like the deadline below), so
    // a hermetic paused-clock test sees virtual time: an early drain reports the few
    // polls it actually slept, not the whole grace.
    let started = Instant::now();
    // The membership *before* the soft signal — one of the report's headline facts.
    // A probe-only read (no pruning), like `is_drained`.
    let members_before = target.alive_count();
    // Snapshot the re-arm generation up front: any spawn/adopt that re-arms the
    // backstop after this point must win over this shutdown's later spare, so the
    // window has to cover the signal + poll below, not just the final `request`.
    let epoch = skip_drop_kill.begin_shutdown();
    // Best-effort: the graceful tier proceeds to polling regardless of the verdict,
    // which is recorded for the report only.
    let soft = target.signal_all(signal);
    // The soft-signal transition — narrated live on the single `tracing` seam so a
    // consumer can stamp it the instant it happens (the same facts `ShutdownReport`
    // carries after the fact; see decisions/completion-phase-observability-2026-07).
    // The `phase` field is a stable snake_case identifier (like `RestartPolicy::name`);
    // argv/env never appear here, consistent with the crate's tracing redaction rule.
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        phase = "soft_signal",
        signal,
        delivery = ?soft,
        members_before = ?members_before,
        escalate,
        "graceful teardown: soft signal issued"
    );
    // Clamp so a `Duration::MAX`-ish timeout can't overflow the `Instant` add.
    let deadline = started + timeout.min(crate::MAX_DEADLINE);
    // The grace window opens now — the driver will poll up to `timeout` for a drain.
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        phase = "grace_started",
        grace_ms = timeout.min(crate::MAX_DEADLINE).as_millis() as u64,
        "graceful teardown: grace window opened"
    );
    while !target.is_drained() {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        // Never oversleep past the deadline, however large `POLL_INTERVAL` is
        // relative to the remaining grace.
        sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
    // Whether the tree drained within the grace, before any hard kill — read once
    // and reused for both the escalation decision and the report.
    let drained = target.is_drained();
    let escalated = escalate && !drained;
    let kill_result = if escalated {
        target.hard_kill()
    } else {
        if !escalate {
            // Tell Drop not to hard-kill the survivors the caller chose to leave
            // alive; the latch makes the decision visible whichever thread runs
            // Drop. Keyed to `epoch`, so a spawn/adopt that re-armed mid-shutdown
            // wins and this spare becomes a no-op — the fresh child is still torn
            // down.
            skip_drop_kill.request(epoch);
        }
        Ok(())
    };
    // The membership *after* everything — survivors spared (`!escalate`), zombies a
    // pgroup `SIGKILL` left unreaped, or zero on an atomic drain. Read after the
    // kill so the report reflects the final state.
    let members_after = target.alive_count();
    let elapsed = started.elapsed();
    // The terminal teardown transition, narrated live on the same seam(s): one of
    // `drained` (exited within the grace), `escalated` (grace elapsed → hard kill),
    // or `spared` (grace elapsed, a non-escalating stop left survivors alive). Reads
    // the driver's own already-computed facts — no second source (single seam,
    // K-032/K-054) — and holds no handle across an await (K-044 is not implicated:
    // this is a plain synchronous emit at a discrete transition point, not in the
    // poll loop). `elapsed`'s anchor is the tokio clock, like `ShutdownReport`'s
    // (K-007 — a single-function reporting anchor; the metrics histogram reuses this
    // same `elapsed`, adding no third clock). `phase` is a stable snake_case token
    // shared verbatim by both the `tracing` event and the `metrics` teardown tally.
    #[cfg(any(feature = "tracing", feature = "metrics"))]
    let phase = if escalated {
        "escalated"
    } else if drained {
        "drained"
    } else {
        "spared"
    };
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        phase,
        drained,
        escalated,
        members_after = ?members_after,
        elapsed_ms = elapsed.as_millis() as u64,
        "graceful teardown: grace window closed"
    );
    #[cfg(feature = "metrics")]
    {
        crate::metrics::record_teardown(phase);
        crate::metrics::record_teardown_duration(phase, elapsed);
    }
    kill_result.map(|()| GracefulOutcome {
        soft,
        members_before,
        members_after,
        drained,
        escalated,
        elapsed,
    })
}

/// The per-target primitives behind the **single-child** graceful kill-and-reap:
/// signal one shared-group child, observe its liveness, and force-kill a
/// survivor. Distinct from [`GracefulTarget`], which tears down a whole tree
/// (a cgroup or a POSIX process group): a **shared-group** run does not own its
/// group, so its teardown reaches only its own direct child, by pid — the
/// child's own descendants are the documented shared-group teardown gap.
///
/// Unix-only: it is signal/`PidGate`-based, and the shared-group streaming
/// timeout on Windows force-kills through the gate instead (no soft-signal tier).
#[cfg(unix)]
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
#[cfg(unix)]
pub(crate) async fn run_pid(target: &impl PidTarget, signal: i32, grace: Duration) {
    // Best-effort: the driver proceeds to polling regardless of delivery.
    target.signal(signal);
    // The single-child teardown transitions are narrated on the same `tracing` seam
    // and share the whole-tree driver's stable `phase` vocabulary, so a consumer sees
    // one uniform lifecycle timeline whichever teardown path drove it (see
    // decisions/completion-phase-observability-2026-07).
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        phase = "soft_signal",
        signal,
        "graceful child teardown: soft signal issued"
    );
    // Clamp so a `Duration::MAX`-ish grace can't overflow the `Instant` add.
    let deadline = Instant::now() + grace.min(crate::MAX_DEADLINE);
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        phase = "grace_started",
        grace_ms = grace.min(crate::MAX_DEADLINE).as_millis() as u64,
        "graceful child teardown: grace window opened"
    );
    loop {
        let now = Instant::now();
        if now >= deadline {
            break; // grace elapsed with the child still around → hard kill below
        }
        if !target.is_alive() {
            // exited (and reaped) within the grace → skip the SIGKILL
            #[cfg(feature = "metrics")]
            crate::metrics::record_teardown("drained");
            #[cfg(feature = "tracing")]
            tracing::debug!(
                target: "processkit",
                phase = "drained",
                "graceful child teardown: child exited within grace"
            );
            return;
        }
        // Never oversleep past the deadline, however large `POLL_INTERVAL` is
        // relative to the remaining grace.
        sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
    #[cfg(feature = "metrics")]
    crate::metrics::record_teardown("escalated");
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "processkit",
        phase = "escalated",
        "graceful child teardown: grace elapsed, hard kill"
    );
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
        fn signal_all(&self, _signal: i32) -> SoftDelivery {
            self.signals.fetch_add(1, Ordering::Relaxed);
            SoftDelivery::Sent
        }

        fn is_drained(&self) -> bool {
            let remaining = self.alive_polls.load(Ordering::Relaxed);
            if remaining == 0 {
                return true;
            }
            self.alive_polls.store(remaining - 1, Ordering::Relaxed);
            false
        }

        fn alive_count(&self) -> Option<usize> {
            // Model a live count that tracks the drain: `alive_polls` counts down to
            // 0 as `is_drained` is polled, so the "before" read (pre-drain) reports
            // the initial members and the "after" read (post-drain) reports 0. A
            // saturating `usize::MAX` (the never-drains cases) is reported as-is —
            // those tests assert on hard-kill counts, not the member tally.
            Some(self.alive_polls.load(Ordering::Relaxed))
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
            fn signal_all(&self, _signal: i32) -> SoftDelivery {
                SoftDelivery::Sent
            }
            fn is_drained(&self) -> bool {
                if self.polls.fetch_add(1, Ordering::Relaxed) == 1 {
                    // A concurrent spawn/adopt re-arms the backstop for a fresh
                    // child, exactly as `ProcessGroup::spawn`/cgroup spawn would.
                    self.latch.clear();
                }
                false
            }
            fn alive_count(&self) -> Option<usize> {
                None
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

    // The headline property of the whole task: a tree that drains early does NOT
    // burn the whole grace, and the reported `elapsed` proves it (measured on the
    // tokio clock, so the paused runtime advances only by the polls actually slept).
    #[tokio::test(start_paused = true)]
    async fn outcome_reports_an_early_drain_without_spending_the_whole_grace() {
        let target = FakeTarget::new(3); // alive for a few polls, then drained
        let skip = crate::sys::SkipDropKill::new();
        let outcome = run(&target, &skip, 15, Duration::from_secs(30), true)
            .await
            .expect("graceful run");
        assert_eq!(
            outcome.soft,
            SoftDelivery::Sent,
            "the soft signal was issued"
        );
        assert!(outcome.drained, "the tree drained within the grace");
        assert!(!outcome.escalated, "an in-time drain needs no hard kill");
        assert_eq!(
            outcome.members_before,
            Some(3),
            "three members before the signal"
        );
        assert_eq!(outcome.members_after, Some(0), "none left after the drain");
        assert!(
            outcome.elapsed < Duration::from_secs(30),
            "an early drain must not spend the whole grace window (took {:?})",
            outcome.elapsed
        );
    }

    // Escalation is reflected in the report: a tree that never drains within the
    // grace is hard-killed, and `escalated` says so.
    #[tokio::test]
    async fn outcome_reports_escalation_when_the_tree_does_not_drain() {
        let target = FakeTarget::new(usize::MAX); // never drains
        let skip = crate::sys::SkipDropKill::new();
        let outcome = run(&target, &skip, 15, Duration::ZERO, true)
            .await
            .expect("graceful run");
        assert!(!outcome.drained, "the tree never drained");
        assert!(outcome.escalated, "escalation to the hard kill is reported");
        assert_eq!(
            target.hard_kills.load(Ordering::Relaxed),
            1,
            "the hard kill actually fired"
        );
        assert!(outcome.members_before.is_some(), "a member count was read");
    }

    // A non-escalating shutdown that leaves survivors reports `drained = false`,
    // `escalated = false`, and sets the skip latch so Drop spares them.
    #[tokio::test]
    async fn outcome_reports_spared_survivors_without_escalating() {
        let target = FakeTarget::new(usize::MAX); // never drains
        let skip = crate::sys::SkipDropKill::new();
        let outcome = run(&target, &skip, 15, Duration::ZERO, false)
            .await
            .expect("graceful run");
        assert!(!outcome.drained, "the tree was left running, not drained");
        assert!(
            !outcome.escalated,
            "a non-escalating shutdown never hard-kills"
        );
        assert_eq!(target.hard_kills.load(Ordering::Relaxed), 0, "no hard kill");
        assert!(skip.is_set(), "survivors are spared on Drop");
    }

    /// A scriptable [`PidTarget`] for the single-child driver: records the
    /// graceful signal and hard kills, and reports "alive" for the first
    /// `alive_polls` liveness checks, then "gone".
    #[cfg(unix)]
    struct FakePid {
        signals: AtomicUsize,
        last_signal: std::sync::atomic::AtomicI32,
        hard_kills: AtomicUsize,
        alive_polls: AtomicUsize,
    }

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    // --- Teardown transition narration (single `tracing` seam, T-176) ---------
    //
    // These pin that the shared driver narrates its teardown transitions, in order,
    // on the `processkit` tracing target — the live, timestamped counterpart of the
    // after-the-fact `ShutdownReport` (T-167), reading the driver's own facts (single
    // seam, not a second source). A dependency-free capturing `Subscriber` records
    // each event's stable `phase` field, so no `tracing-subscriber` dev-dep is pulled
    // in (the crate carries zero other tracing-capture tests; `tracing` is a
    // best-effort narration seam everywhere else). See
    // decisions/completion-phase-observability-2026-07.md for the design pass.

    /// A minimal [`tracing::Subscriber`] that records the `phase` field of every
    /// event, in order — enough to assert the teardown driver's transition sequence
    /// without depending on `tracing-subscriber`.
    #[cfg(feature = "tracing")]
    #[derive(Clone, Default)]
    struct PhaseCapture {
        phases: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    // Keep one subscriber registered for the whole test process. `tracing` caches
    // callsite interest globally, so installing per-test subscribers lets parallel
    // tests invalidate each other's capture setup.
    #[cfg(feature = "tracing")]
    static INSTALL_PHASE_CAPTURE_SUBSCRIBER: std::sync::Once = std::sync::Once::new();

    #[cfg(feature = "tracing")]
    std::thread_local! {
        static ACTIVE_PHASE_CAPTURE: std::cell::RefCell<Option<PhaseCapture>> = const {
            std::cell::RefCell::new(None)
        };
    }

    #[cfg(feature = "tracing")]
    impl PhaseCapture {
        fn phases(&self) -> Vec<String> {
            self.phases.lock().expect("phase capture lock").clone()
        }
    }

    /// Pulls the stable `phase` field (a `&str`) out of one event; ignores the rest.
    #[cfg(feature = "tracing")]
    struct PhaseVisitor<'a>(&'a mut Option<String>);

    #[cfg(feature = "tracing")]
    impl tracing::field::Visit for PhaseVisitor<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "phase" {
                *self.0 = Some(value.to_owned());
            }
        }
        // The other fields (signal, member counts, bools) are irrelevant to the
        // sequence assertion — a no-op required by the trait.
        fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    }

    #[cfg(feature = "tracing")]
    struct PhaseCaptureSubscriber;

    #[cfg(feature = "tracing")]
    impl tracing::Subscriber for PhaseCaptureSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut phase = None;
            event.record(&mut PhaseVisitor(&mut phase));
            if let Some(phase) = phase {
                ACTIVE_PHASE_CAPTURE.with(|active| {
                    if let Some(capture) = active.borrow().as_ref() {
                        capture
                            .phases
                            .lock()
                            .expect("phase capture lock")
                            .push(phase);
                    }
                });
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[cfg(feature = "tracing")]
    struct ActivePhaseCapture;

    #[cfg(feature = "tracing")]
    impl ActivePhaseCapture {
        fn install(capture: PhaseCapture) -> Self {
            ACTIVE_PHASE_CAPTURE.with(|active| {
                assert!(
                    active.borrow().is_none(),
                    "phase capture scopes must not nest on one thread"
                );
                active.replace(Some(capture));
            });
            Self
        }
    }

    #[cfg(feature = "tracing")]
    impl Drop for ActivePhaseCapture {
        fn drop(&mut self) {
            ACTIVE_PHASE_CAPTURE.with(|active| {
                active.replace(None);
            });
        }
    }

    /// Route events to a fresh [`PhaseCapture`] on this thread, drive `body` to
    /// completion on a current-thread runtime (so every event lands on this thread),
    /// and return the ordered `phase` values.
    #[cfg(feature = "tracing")]
    fn capture_teardown_phases<F, Fut>(body: F) -> Vec<String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        INSTALL_PHASE_CAPTURE_SUBSCRIBER.call_once(|| {
            tracing::subscriber::set_global_default(PhaseCaptureSubscriber)
                .expect("install phase capture subscriber");
        });
        let capture = PhaseCapture::default();
        let _active_capture = ActivePhaseCapture::install(capture.clone());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime");
        rt.block_on(body());
        capture.phases()
    }

    // The headline: a штатный graceful teardown narrates the FULL transition
    // sequence live — soft signal → grace window → drained — in order.
    #[cfg(feature = "tracing")]
    #[test]
    fn teardown_narrates_the_full_transition_sequence_on_a_clean_drain() {
        let phases = capture_teardown_phases(|| async {
            let target = FakeTarget::new(0); // drained on the first check
            let skip = crate::sys::SkipDropKill::new();
            run(&target, &skip, 15, Duration::from_secs(10), true)
                .await
                .expect("graceful run");
        });
        assert_eq!(
            phases,
            vec!["soft_signal", "grace_started", "drained"],
            "a clean graceful teardown narrates soft signal → grace → drain, in order"
        );
    }

    // The escalation branch: a tree that rides out the grace narrates soft signal →
    // grace window → escalated (to the hard kill).
    #[cfg(feature = "tracing")]
    #[test]
    fn teardown_narrates_the_escalation_branch() {
        let phases = capture_teardown_phases(|| async {
            let target = FakeTarget::new(usize::MAX); // never drains
            let skip = crate::sys::SkipDropKill::new();
            // Zero grace: the deadline passes on the first check (no sleep awaited).
            run(&target, &skip, 15, Duration::ZERO, true)
                .await
                .expect("graceful run");
        });
        assert_eq!(
            phases,
            vec!["soft_signal", "grace_started", "escalated"],
            "a tree that rides out the grace narrates the escalation to the hard kill"
        );
    }

    // The distinct third terminal transition: a non-escalating stop that leaves
    // survivors narrates them as spared, not drained or escalated.
    #[cfg(feature = "tracing")]
    #[test]
    fn teardown_narrates_survivors_spared_by_a_non_escalating_stop() {
        let phases = capture_teardown_phases(|| async {
            let target = FakeTarget::new(usize::MAX); // never drains
            let skip = crate::sys::SkipDropKill::new();
            run(&target, &skip, 15, Duration::ZERO, false)
                .await
                .expect("graceful run");
        });
        assert_eq!(
            phases,
            vec!["soft_signal", "grace_started", "spared"],
            "a non-escalating stop that leaves survivors narrates them as spared"
        );
    }

    // The single-child driver shares the whole-tree phase vocabulary: a child gone
    // within the grace narrates the same soft signal → grace → drained sequence.
    #[cfg(all(unix, feature = "tracing"))]
    #[test]
    fn pid_teardown_narrates_a_clean_exit_within_grace() {
        let phases = capture_teardown_phases(|| async {
            let target = FakePid::new(0); // gone on the first poll
            run_pid(&target, 15, Duration::from_secs(10)).await;
        });
        assert_eq!(
            phases,
            vec!["soft_signal", "grace_started", "drained"],
            "a child gone within the grace narrates the same drain transition"
        );
    }

    // And the single-child escalation branch.
    #[cfg(all(unix, feature = "tracing"))]
    #[test]
    fn pid_teardown_narrates_the_hard_kill_branch() {
        let phases = capture_teardown_phases(|| async {
            let target = FakePid::new(usize::MAX); // rides out the grace
            run_pid(&target, 15, Duration::ZERO).await;
        });
        assert_eq!(
            phases,
            vec!["soft_signal", "grace_started", "escalated"],
            "a child that survives the grace narrates the escalation to the hard kill"
        );
    }
}
