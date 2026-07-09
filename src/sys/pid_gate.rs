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

use std::sync::{Mutex, MutexGuard};

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
    /// This is the fully-linearized reap path for a synchronous reaper (a
    /// `try_wait` exit probe): because the reap that frees the pid happens under
    /// the same lock a watchdog's kill must take, a kill can never observe the pid
    /// as live after this reap freed it. An *async* reaper (`Child::wait().await`)
    /// cannot hold the lock across its `.await`; it frees the pid first and then
    /// [`retire`](Self::retire)s, leaving a bounded reap→retire residual that the
    /// gate still keeps a raw kill from *completing* against a freed pid on the
    /// owner-driven paths (which retire before that reap).
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
pub(crate) fn force_kill(gate: &PidGate) {
    gate.with_live_pid((), raw_force_kill);
}

/// The raw force-kill syscall for one pid. Called only from
/// [`force_kill`] under the gate lock (never directly), so it never runs on a
/// retired/recycled pid.
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

#[cfg(test)]
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
}
