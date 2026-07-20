//! [`PidGate`] — the linearizable gate that serializes every raw `kill(pid)` a
//! detached teardown watchdog might issue against the reap that frees (and lets
//! the OS recycle) the pid.
//!
//! # Why a gate, not a flag
//!
//! The teardown watchdogs (the cancellation watchdog, the streaming deadline
//! watchdog, and the shared-group graceful-timeout kill) are *detached* tasks
//! that reach a shared-group child only by its **raw pid** — they hold no owned
//! `Child` and no process group. The consuming finisher, meanwhile, reaps that
//! same child through the owned `Child`, which frees the pid; the OS may then
//! recycle it for an unrelated process. A watchdog that kills *after* that reap
//! would signal the recycled pid — a stranger.
//!
//! The previous design guarded this with a bare `AtomicBool` "handed off" latch:
//! the reaper stored `true` and each watchdog *loaded* it before killing. That
//! is a check-then-act race — the load and the `kill` are two steps, so a reap
//! could land between them and the `kill` still fire on the freed pid. The load
//! observing `false` proved the child was un-reaped *at the instant of the load*,
//! never at the instant of the kill.
//!
//! [`PidGate`] closes that window by making the state read and the raw kill **one
//! indivisible step**: the kill runs inside [`with_live_pid`](PidGate::with_live_pid)
//! *while holding the gate lock*, and the reaper takes the *same* lock to
//! [`retire`](PidGate::retire) the pid. A kill therefore either linearizes
//! entirely before the retire (pid still valid) or is skipped (retired first) —
//! there is no interleaving in which a kill touches a freed pid. The mutex is the
//! "single owner": whoever holds it is the sole actor allowed to decide "kill" vs
//! "already retired", and it decides both atomically.
//!
//! Cross-platform: the gate's state machine is platform-agnostic; only the raw
//! kill syscall behind [`force_kill`] differs (SIGKILL on unix, `OpenProcess` +
//! `TerminateProcess` on Windows).

// The mutex comes from the crate's `cfg(loom)`-swappable sync layer: `std::sync`
// in ordinary builds (byte-for-byte the same gate), loom's model under a
// `--cfg loom` test build so the `loom_model` suite below can permute the
// reaper-vs-watchdog interleavings this gate linearizes. See `crate::sync`.
use crate::sync::{Mutex, MutexGuard};

/// A gate around a direct child's raw pid that linearizes raw kills against the
/// reap that retires the pid. Shared (`Arc`) between the owning handle's reap
/// paths and its detached teardown watchdogs.
pub(crate) struct PidGate {
    inner: Mutex<GateState>,
}

/// The gate's protected state: the pid to kill and whether it has been retired.
struct GateState {
    /// The direct child's pid, or `None` for a pid-less handle (a scripted
    /// double, which owns no OS process). A raw kill is issued only while this is
    /// `Some` *and* `retired` is `false`, both checked under the lock.
    pid: Option<u32>,
    /// Set the instant the pid's owner retires it — reaps it, or is about to reap
    /// it in the same critical section (see [`reap_under_lock`](PidGate::reap_under_lock)).
    /// Once set, no [`with_live_pid`](PidGate::with_live_pid) closure runs, so no
    /// raw kill can land on the (possibly recycled) pid.
    retired: bool,
}

impl PidGate {
    /// A fresh gate for `pid` (`None` for a pid-less handle). Not yet retired, so
    /// raw kills are permitted until the owner reaps.
    pub(crate) fn new(pid: Option<u32>) -> Self {
        Self {
            inner: Mutex::new(GateState {
                pid,
                retired: false,
            }),
        }
    }

    /// Run `f` with the still-live pid, holding the gate lock so the "not
    /// retired" check and `f` are one indivisible step: a concurrent
    /// [`retire`](Self::retire) is serialized entirely before this (then `f` does
    /// not run) or entirely after it (then `f` ran on a pid that was still
    /// un-reaped when the lock was held). Returns `f`'s result, or `default` when
    /// the gate is retired or pid-less.
    ///
    /// `f` runs under the lock, so it must do only a bounded, non-blocking
    /// operation — a single kill/liveness syscall — and never `.await` or block
    /// on I/O.
    pub(crate) fn with_live_pid<R>(&self, default: R, f: impl FnOnce(u32) -> R) -> R {
        let guard = self.lock();
        if guard.retired {
            return default;
        }
        match guard.pid {
            Some(pid) => f(pid),
            None => default,
        }
    }

    /// Retire the pid: after this returns, no [`with_live_pid`](Self::with_live_pid)
    /// closure will run. Called by the owner **before** it frees the pid on the
    /// paths that drive the kill themselves (so a racing watchdog stands down),
    /// or immediately **after** the reap on the passive backstop paths (where the
    /// async wait itself frees the pid). Idempotent.
    pub(crate) fn retire(&self) {
        self.lock().retired = true;
    }

    /// Whether the pid has been retired. A cheap early-out for a watchdog to skip
    /// even the *group* teardown once the owner has taken over — the subsequent
    /// [`with_live_pid`](Self::with_live_pid) re-checks it atomically with the
    /// raw kill, so this load is only an optimization, never the safety gate.
    pub(crate) fn is_retired(&self) -> bool {
        self.lock().retired
    }

    /// Run a **synchronous, non-blocking** reap probe under the gate lock and, if
    /// it reports the child reaped, [`retire`](Self::retire) the pid in the *same*
    /// critical section — so the pid-freeing reap and the retire are atomic and no
    /// gated kill can slip between them. Returns whether the probe reaped.
    ///
    /// This is the fully-linearized reap path for any reaper whose pid-freeing step
    /// is a synchronous, non-blocking call. Two shapes use it: a direct `try_wait`
    /// exit probe (the readiness-probe path), and a **single poll of
    /// `Child::wait()`** — tokio frees the pid via that same `try_wait` *inside* the
    /// poll, so polling it through this closure runs the pid-free under the lock too
    /// (the `backend_wait` backstop and the detached Drop reaper drive their
    /// `Child::wait()` this way). Because the reap that frees the pid happens under
    /// the same lock a watchdog's kill must take, a kill can never observe the pid
    /// as live after this reap freed it. `reap` must therefore stay bounded and
    /// non-blocking — a `try_wait`/liveness syscall or one leaf-future poll — and
    /// must never block on I/O.
    pub(crate) fn reap_under_lock(&self, reap: impl FnOnce() -> bool) -> bool {
        let mut guard = self.lock();
        let reaped = reap();
        if reaped {
            guard.retired = true;
        }
        reaped
    }

    /// Lock the gate, recovering the guard from a poisoned mutex rather than
    /// panicking. The only code under the lock is a trivial state assignment or a
    /// single kill syscall, so poisoning cannot happen in practice; recovering
    /// keeps a hypothetical panic in one detached watchdog from poisoning every
    /// other teardown path.
    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Force-kill the gated pid: `SIGKILL` on unix, `OpenProcess` +
/// `TerminateProcess` on Windows. A no-op if the gate is retired or pid-less.
///
/// This is the single choke point every teardown watchdog funnels its raw
/// direct-child kill through, so routing it through [`PidGate::with_live_pid`]
/// guarantees the kill can never land after the owner retired the pid.
///
/// `cfg(not(loom))`: this is the only impure part of the gate (a raw `libc` /
/// `windows-sys` syscall), and it is never exercised by the loom models — they
/// drive the gate's *state machine* directly. Gating it out lets the standalone
/// loom harness (`loom/`) `#[path]`-include this file without libc/windows-sys.
#[cfg(not(loom))]
pub(crate) fn force_kill(gate: &PidGate) {
    gate.with_live_pid((), raw_force_kill);
}

/// The raw force-kill syscall for one pid. Called only from
/// [`force_kill`] under the gate lock (never directly), so it never runs on a
/// retired/recycled pid.
#[cfg(not(loom))]
fn raw_force_kill(pid: u32) {
    #[cfg(unix)]
    // SAFETY: SIGKILL to a pid; `ESRCH` (already exited/reaped) is ignored. The
    // gate guarantees the pid is not yet reaped by *this* handle's owner.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    #[cfg(windows)]
    // SAFETY: opens by pid with the narrowest right; tolerates an already-exited
    // process (a null handle is skipped).
    unsafe {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_TERMINATE, TerminateProcess,
        };
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if !handle.is_null() {
            TerminateProcess(handle, 1);
            CloseHandle(handle);
        }
    }
}

// These deterministic real-thread tests construct a `PidGate` and drive it
// directly; under `--cfg loom` the gate's mutex is a loom model that may only be
// used inside `loom::model`, so this suite is compiled out there and the
// exhaustive-interleaving equivalents live in `loom_model` below.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A retired gate runs no raw kill: `with_live_pid`'s closure never fires, so
    /// `force_kill` (which *is* `with_live_pid` + the kill syscall) is a no-op.
    /// This is the load-bearing guarantee for all three teardown watchdogs — they
    /// funnel every raw pid kill through this gate.
    #[test]
    fn a_retired_gate_runs_no_raw_kill() {
        let gate = PidGate::new(Some(4321));
        gate.retire();
        let mut fired = false;
        let ran = gate.with_live_pid(false, |_pid| {
            fired = true;
            true
        });
        assert!(!ran, "with_live_pid must report the closure did not run");
        assert!(!fired, "no raw kill may run once the pid is retired");
    }

    /// A live (un-retired) gate does run the kill closure with the pid.
    #[test]
    fn a_live_gate_runs_the_kill_with_the_pid() {
        let gate = PidGate::new(Some(4321));
        let seen = gate.with_live_pid(None, Some);
        assert_eq!(seen, Some(4321), "a live gate hands the pid to the closure");
    }

    /// A pid-less gate (a scripted double) never runs a raw kill, retired or not.
    #[test]
    fn a_pid_less_gate_never_kills() {
        let gate = PidGate::new(None);
        let mut fired = false;
        gate.with_live_pid((), |_| fired = true);
        assert!(!fired, "a pid-less gate has no OS process to kill");
    }

    /// `reap_under_lock` sets `retired` in the *same* critical section as the
    /// reap, so a kill attempted afterward is a no-op.
    #[test]
    fn reap_under_lock_retires_in_one_critical_section() {
        let gate = PidGate::new(Some(4321));
        assert!(
            gate.reap_under_lock(|| true),
            "the probe reported the child reaped"
        );
        assert!(gate.is_retired(), "a successful reap retires the pid");
        let mut fired = false;
        gate.with_live_pid((), |_| fired = true);
        assert!(!fired, "no kill runs after reap_under_lock retired the pid");
    }

    /// Models the **poll-driven** reap the `backend_wait` backstop and the
    /// detached Drop reaper drive `Child::wait()` through: a run of non-reaping
    /// polls (the child is still alive, `Child::wait()` `Pending`) must leave the
    /// gate live and killable so a genuine timeout can still tear the child down,
    /// and only the poll that finally reaps — freeing the pid, exactly as tokio's
    /// `try_wait` does *inside* `Child::wait()`'s poll — retires the gate, in the
    /// *same* critical section, so no gated kill can land on the freed pid.
    /// Deterministic: it steps the poll sequence explicitly (no threads, no timing).
    #[test]
    fn a_poll_driven_reap_retires_only_when_the_poll_reaps() {
        let gate = PidGate::new(Some(4321));
        // Two non-reaping polls (`Child::wait()` still `Pending`): the gate stays
        // live and a watchdog can still fire, matching a live child.
        for _ in 0..2 {
            assert!(
                !gate.reap_under_lock(|| false),
                "a Pending poll does not reap"
            );
            assert!(
                !gate.is_retired(),
                "an un-reaped child leaves the gate live"
            );
            let mut fired = false;
            gate.with_live_pid((), |_| fired = true);
            assert!(fired, "a live child stays killable between reap polls");
        }
        // The reaping poll (`Child::wait()` `Ready`) frees the pid; the retire
        // happens in the same locked step, never a beat later.
        let freed = AtomicBool::new(false);
        let reaped = gate.reap_under_lock(|| {
            freed.store(true, Ordering::SeqCst); // stands in for tokio's `try_wait`
            true
        });
        assert!(
            reaped && freed.load(Ordering::SeqCst),
            "the reaping poll reaps"
        );
        assert!(
            gate.is_retired(),
            "the reaping poll retires the gate atomically"
        );
        // No kill runs now: the closure never fires once retired, so a raw kill can
        // never touch the freed (possibly OS-recycled) pid.
        let mut fired_after = false;
        gate.with_live_pid((), |_| fired_after = true);
        assert!(
            !fired_after,
            "no kill runs after the reaping poll freed the pid"
        );
    }

    /// A probe that does not reap (child still running) leaves the gate live, so
    /// a genuine timeout escalation is not defeated.
    #[test]
    fn reap_under_lock_leaves_a_live_child_killable() {
        let gate = PidGate::new(Some(4321));
        assert!(
            !gate.reap_under_lock(|| false),
            "the probe reported the child still running"
        );
        assert!(!gate.is_retired(), "an un-reaped child is not retired");
        let mut fired = false;
        gate.with_live_pid((), |_| fired = true);
        assert!(fired, "a still-live child stays killable by the watchdog");
    }

    /// The core linearizability invariant, exercised under real thread contention
    /// but asserted **deterministically** (no sleeps, no timing guesses): a gated
    /// kill NEVER runs after the pid has been freed. A barrier releases a reaper
    /// (which frees the pid and retires it in one critical section via
    /// `reap_under_lock`) and a killer (which force-kills through the gate) at the
    /// same instant to maximize contention; whichever wins the lock, the killer's
    /// closure can only ever see the pid *un-freed*, because the free and the
    /// retire are inseparable. This is exactly the reaper-vs-watchdog race the
    /// cancellation watchdog, the streaming deadline watchdog, and the graceful
    /// timeout teardown all reduce to.
    #[test]
    fn no_gated_kill_ever_lands_after_the_pid_is_freed() {
        for _ in 0..2_000 {
            let gate = Arc::new(PidGate::new(Some(4321)));
            let freed = Arc::new(AtomicBool::new(false));
            let killed_after_free = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(Barrier::new(2));

            let reaper = {
                let gate = gate.clone();
                let freed = freed.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    // "Reap": free the pid and retire it in one critical section.
                    gate.reap_under_lock(|| {
                        freed.store(true, Ordering::SeqCst);
                        true
                    });
                })
            };
            let killer = {
                let gate = gate.clone();
                let freed = freed.clone();
                let killed_after_free = killed_after_free.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    gate.with_live_pid((), |_pid| {
                        // Runs only under the lock and only while not retired; if
                        // the gate linearizes correctly the pid is never freed here.
                        if freed.load(Ordering::SeqCst) {
                            killed_after_free.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                })
            };
            reaper.join().expect("reaper thread");
            killer.join().expect("killer thread");
            assert_eq!(
                killed_after_free.load(Ordering::SeqCst),
                0,
                "a raw kill must never run after the pid was freed"
            );
        }
    }

    /// T-092: the **structural-drop** discipline `RunningProcess::drop` uses on the
    /// branches that hand the child to no detached reaper (an own-group handle, or a
    /// shared group without a grace window). There the owner does NOT fuse the reap
    /// and the retire the way [`reap_under_lock`](PidGate::reap_under_lock) does:
    /// `drop()` calls [`retire`](PidGate::retire) synchronously, and only *afterwards*
    /// — as a **separate** later step — does the owned `Child` drop and the pid get
    /// freed (by tokio's orphan reaper, or as the owned group tears the tree down).
    /// This proves that split is still race-free: because the `retire` fully precedes
    /// the free in program order AND both contend on the same lock a watchdog's kill
    /// must take, an aborted-but-mid-poll watchdog's gated raw kill can only ever
    /// observe the pid *un-freed*.
    ///
    /// A barrier releases the "drop" thread (retire, then a beat later mark the pid
    /// freed) and a "watchdog" thread (kill through the gate) at the same instant to
    /// maximize contention. Whichever wins the lock, the killer's closure never runs
    /// after the free: it either linearizes before the retire (pid still valid) or is
    /// skipped once retired. Deterministic (a barrier + a hard assertion, no sleeps or
    /// timing guesses) and platform-agnostic — the exact reaper-vs-watchdog race the
    /// two structural-drop Drop branches reduce to.
    #[test]
    fn a_retire_before_a_separate_pid_free_still_bars_a_racing_kill() {
        for _ in 0..2_000 {
            let gate = Arc::new(PidGate::new(Some(4321)));
            let freed = Arc::new(AtomicBool::new(false));
            let killed_after_free = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(Barrier::new(2));

            // The Drop path: retire the gate, THEN (strictly afterwards, a separate
            // step) free the pid — exactly as the owned `Child` is dropped only once
            // `drop()` has already retired the gate.
            let dropper = {
                let gate = gate.clone();
                let freed = freed.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    gate.retire();
                    freed.store(true, Ordering::SeqCst);
                })
            };
            // A deadline/cancel watchdog aborted mid-poll that still reaches its
            // gated raw kill.
            let killer = {
                let gate = gate.clone();
                let freed = freed.clone();
                let killed_after_free = killed_after_free.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    gate.with_live_pid((), |_pid| {
                        // Runs only under the lock and only while not retired. A
                        // closure that runs here must have won the lock *before* the
                        // dropper's `retire` — hence before the free that follows it —
                        // so `freed` is never observed set.
                        if freed.load(Ordering::SeqCst) {
                            killed_after_free.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                })
            };
            dropper.join().expect("dropper thread");
            killer.join().expect("killer thread");
            assert_eq!(
                killed_after_free.load(Ordering::SeqCst),
                0,
                "a raw kill must never run after the structural drop freed the pid"
            );
        }
    }
}

/// Loom model-checking suite for the [`PidGate`] linearization (run under
/// `--cfg loom`; see [`crate::sync`]).
///
/// Where the `tests` module above pins the invariants with real threads and a
/// barrier — which exercises *some* interleavings but can never prove it hit the
/// racy one — loom **exhaustively** permutes every thread interleaving and every
/// permitted memory ordering of the reaper-vs-watchdog race and fails if any one
/// violates the gate's contract: a gated raw kill never runs after the pid is
/// freed (no use of a freed/recycled pid slot), yet a still-live child stays
/// killable (no missed kill). These are the exact races the cancellation
/// watchdog, the streaming deadline watchdog, and the graceful pid-timeout
/// teardown (whose detached final SIGKILL stands down only through this gate) all
/// reduce to.
#[cfg(all(test, loom))]
mod loom_model {
    use super::PidGate;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    /// The core linearizability invariant, checked over every interleaving: a
    /// reaper frees the pid and retires it in one critical section
    /// ([`reap_under_lock`](PidGate::reap_under_lock)) while a watchdog kills
    /// through the gate ([`with_live_pid`](PidGate::with_live_pid)). The kill
    /// closure runs only under the lock and only while un-retired, so it must
    /// never observe the pid freed — no gated kill lands on a freed (possibly
    /// OS-recycled) pid.
    #[test]
    fn a_gated_kill_never_lands_after_a_fused_reap_freed_the_pid() {
        loom::model(|| {
            let gate = Arc::new(PidGate::new(Some(4321)));
            let freed = Arc::new(AtomicBool::new(false));

            let reaper = {
                let gate = gate.clone();
                let freed = freed.clone();
                loom::thread::spawn(move || {
                    // "Reap": free the pid and retire it in one critical section.
                    gate.reap_under_lock(|| {
                        freed.store(true, Ordering::SeqCst);
                        true
                    });
                })
            };

            // The watchdog kills on the model's main thread. If the closure runs
            // at all, the gate held the lock un-retired — so the fused reap has
            // not run, and `freed` must be false.
            gate.with_live_pid((), |_pid| {
                assert!(
                    !freed.load(Ordering::SeqCst),
                    "a gated kill ran after the fused reap freed the pid"
                );
            });

            reaper.join().unwrap();
            // Once the fused reap has run, the gate is retired for good — the
            // `is_retired` early-out a watchdog reads before even the group kill
            // now reports "stood down" on every interleaving.
            assert!(
                gate.is_retired(),
                "a fused reap must leave the gate retired"
            );
        });
    }

    /// The T-092 structural-drop discipline: the owner
    /// [`retire`](PidGate::retire)s the gate and only *afterwards*, as a separate
    /// step, frees the pid. Loom proves that split is still race-free across every
    /// interleaving — a racing gated kill either linearizes before the retire (pid
    /// valid) or is skipped, never after the free.
    #[test]
    fn a_retire_before_a_separate_free_still_bars_a_racing_kill() {
        loom::model(|| {
            let gate = Arc::new(PidGate::new(Some(4321)));
            let freed = Arc::new(AtomicBool::new(false));

            let dropper = {
                let gate = gate.clone();
                let freed = freed.clone();
                loom::thread::spawn(move || {
                    // Retire first (takes the lock), THEN — strictly afterwards, a
                    // separate step — free the pid.
                    gate.retire();
                    freed.store(true, Ordering::SeqCst);
                })
            };

            gate.with_live_pid((), |_pid| {
                assert!(
                    !freed.load(Ordering::SeqCst),
                    "a gated kill ran after the structural drop freed the pid"
                );
            });

            dropper.join().unwrap();
        });
    }

    /// The other half of the contract — no *missed* kill: a non-reaping poll
    /// ([`reap_under_lock`] returning `false`, the child still alive) never
    /// retires the gate, so a genuinely-live child stays killable on every
    /// interleaving. This is what keeps a real streaming/deadline timeout from
    /// being silently swallowed.
    #[test]
    fn a_live_child_stays_killable_across_a_non_reaping_poll() {
        loom::model(|| {
            let gate = Arc::new(PidGate::new(Some(4321)));
            let killed = Arc::new(AtomicBool::new(false));

            let poller = {
                let gate = gate.clone();
                loom::thread::spawn(move || {
                    // A `Pending` poll: the child is still running, so this does
                    // not reap and must not retire the gate.
                    assert!(!gate.reap_under_lock(|| false), "a Pending poll reaped");
                })
            };

            gate.with_live_pid((), |_pid| {
                killed.store(true, Ordering::SeqCst);
            });

            poller.join().unwrap();
            assert!(
                killed.load(Ordering::SeqCst),
                "a live child was left un-killable — the watchdog's kill was dropped"
            );
        });
    }
}
