//! Platform job layer — one `imp::Job` per target, all exposing the same shape.
//!
//! A `Job` is the kernel object that contains a process tree so the whole tree
//! dies with its owner:
//!
//! - **Windows** — a [Job Object] with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
//! - **Linux** — a [cgroup v2] killed via `cgroup.kill`, falling back to a POSIX
//!   process group when no writable cgroup is available.
//! - **macOS / the BSDs** — a POSIX process group (`killpg` the tree on drop);
//!   no cgroups or Job Objects exist there. See [`pgroup`].
//!
//! Only Unix and Windows are supported; other targets fail to compile (see the
//! `compile_error!` below).
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::Mechanism;
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;

/// The raw `SIGTERM` signal number — the default signal for the graceful
/// teardown tier (`graceful_shutdown` and the run-level graceful timeout).
/// Defined cross-platform; only *used* on unix (Windows' graceful tier ignores
/// the signal and kills the job atomically).
#[cfg(unix)]
pub(crate) const SIGTERM_RAW: i32 = libc::SIGTERM;
#[cfg(not(unix))]
pub(crate) const SIGTERM_RAW: i32 = 15;

/// A "don't kill on Drop" latch shared by every backend, hardened against the
/// spawn/shutdown re-arm race.
///
/// `graceful_shutdown(escalate = false)` [`request`](Self::request)s the latch to
/// spare the survivors the caller chose to leave running; each backend's `Drop`
/// [reads](Self::is_set) it and skips the hard kill when the latch is set.
/// Spawning or adopting a fresh child into a reused group [`clear`](Self::clear)s
/// the latch so the newcomer is not silently spared.
///
/// # The re-arm race this guards
///
/// A non-escalating `graceful_shutdown` runs *concurrently* with `spawn`/`adopt`:
/// it can be mid-poll (or merely between deciding to spare and finishing) while a
/// fresh child is spawned into the same group and re-arms the backstop. A tokio
/// task can migrate across threads at every `.await`, so the shutdown's final
/// `request()` can land **after** the spawn's `clear()`. A plain boolean would let
/// that stale `request()` re-set the skip flag and silently strip the fresh child
/// of its Drop-kill backstop — the exact orphan-leak this type exists to prevent.
///
/// The guard is a **generation counter packed with the skip flag into one atomic
/// word**: bit 0 is the skip flag, bits `1..` are the generation. Every `clear()`
/// bumps the generation and clears the skip bit; a shutdown snapshots the
/// generation up front with [`begin_shutdown`](Self::begin_shutdown) and its later
/// [`request`](Self::request) spares the survivors **only if the generation is
/// unchanged**, expressed as a single compare-exchange. A `clear()` that raced the
/// shutdown has bumped the generation, so the stale `request` compare-exchange
/// fails and the backstop stays armed for the new child. Because the flag and the
/// generation live in one word, the check and the set are indivisible — no store
/// can slip between them (the flaw a separate flag + counter would reintroduce on
/// weakly-ordered hardware).
///
/// Centralizing this keeps the load-bearing memory ordering correct in one place:
/// the `Release` stores pair with the `Acquire` load so the decision — and the
/// generation it is keyed to — is visible to whichever thread runs `Drop`.
#[derive(Debug, Default)]
pub(crate) struct SkipDropKill(std::sync::atomic::AtomicUsize);

/// The re-arm generation snapshotted at the **start** of a non-escalating
/// shutdown, handed back to [`SkipDropKill::request`] so a spawn/adopt that
/// re-armed the backstop in the meantime wins over the shutdown's stale spare.
/// Opaque: only [`begin_shutdown`](SkipDropKill::begin_shutdown) mints one and only
/// [`request`](SkipDropKill::request) consumes it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ShutdownEpoch(usize);

impl SkipDropKill {
    /// Bit 0 of the packed word: set means `Drop` must skip its hard kill.
    const SKIP_BIT: usize = 1;
    /// Added to the packed word to bump the generation (bits `1..`) by one
    /// without disturbing the skip bit.
    const GEN_STEP: usize = Self::SKIP_BIT << 1;

    /// A fresh latch — generation 0, `Drop` will hard-kill until
    /// [`request`](Self::request).
    pub(crate) fn new() -> Self {
        Self(std::sync::atomic::AtomicUsize::new(0))
    }

    /// Snapshot the re-arm generation at the **start** of a non-escalating
    /// shutdown, before it signals or polls the tree. Hand the returned
    /// [`ShutdownEpoch`] to [`request`](Self::request) once the shutdown finishes:
    /// a `spawn`/`adopt` that [`clear`](Self::clear)s the latch after this snapshot
    /// bumps the generation, so the later `request` no-ops and the fresh child
    /// keeps its Drop-kill backstop.
    ///
    /// The skip bit is masked out of the snapshot so the epoch names the *armed*
    /// state at the current generation — precisely the state `request`
    /// compare-exchanges away from. `Acquire` pairs with the `Release` stores.
    pub(crate) fn begin_shutdown(&self) -> ShutdownEpoch {
        use std::sync::atomic::Ordering;
        ShutdownEpoch(self.0.load(Ordering::Acquire) & !Self::SKIP_BIT)
    }

    /// Mark that `Drop` must **not** hard-kill the survivors — but only if no
    /// `spawn`/`adopt` re-armed the backstop since `epoch` was taken. Implemented
    /// as one compare-exchange from "armed at the snapshot generation" to "spared
    /// at that generation": if a racing [`clear`](Self::clear) bumped the
    /// generation (or already spared it), the exchange fails and the latch is left
    /// in the already-correct state — armed at a newer generation, so the fresh
    /// child is still torn down on Drop. `Release` (on success) pairs with the
    /// `Acquire` in [`is_set`](Self::is_set).
    pub(crate) fn request(&self, epoch: ShutdownEpoch) {
        use std::sync::atomic::Ordering;
        // A failed exchange is the point of the guard (a concurrent re-arm won),
        // so the result is deliberately ignored — on failure the latch is already
        // correct.
        let _ = self.0.compare_exchange(
            epoch.0,
            epoch.0 | Self::SKIP_BIT,
            Ordering::Release,
            Ordering::Relaxed,
        );
    }

    /// Re-arm `Drop`'s hard kill for a reused group and **bump the generation** so
    /// an in-flight non-escalating shutdown's later [`request`](Self::request)
    /// cannot re-spare the fresh member. Spawning/adopting a child into a group
    /// that was gracefully shut down with `escalate = false` calls this so the
    /// child (and the rest of the reused group) is not silently spared by a stale
    /// latch — a group left with the latch set but never reused keeps its spared
    /// survivors. `Release` pairs with the `Acquire` in [`is_set`](Self::is_set).
    pub(crate) fn clear(&self) {
        use std::sync::atomic::Ordering;
        // Bump the generation and clear the skip bit as one atomic step. The CAS
        // loop composes with concurrent `clear`s (each retries against the other's
        // bump) and with a racing `request` (whose compare-exchange keys off the
        // exact generation this steps past). The generation wraps harmlessly after
        // `usize::MAX >> 1` re-arms; an ABA there would need that many spawns
        // inside a single shutdown's window, which is not reachable — hence
        // `wrapping_add` rather than a checked add that could panic in a debug
        // build.
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let next = cur.wrapping_add(Self::GEN_STEP) & !Self::SKIP_BIT;
            match self
                .0
                .compare_exchange_weak(cur, next, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Whether `Drop` should skip the kill. `Acquire` pairs with the `Release`
    /// in [`request`](Self::request) / [`clear`](Self::clear).
    pub(crate) fn is_set(&self) -> bool {
        use std::sync::atomic::Ordering;
        (self.0.load(Ordering::Acquire) & Self::SKIP_BIT) != 0
    }
}

#[cfg(test)]
mod skip_drop_kill_tests {
    use super::SkipDropKill;

    #[test]
    fn request_then_clear_re_arms_the_drop_kill() {
        let latch = SkipDropKill::new();
        assert!(!latch.is_set(), "a fresh latch does not skip Drop's kill");
        let epoch = latch.begin_shutdown();
        latch.request(epoch);
        assert!(latch.is_set(), "request() spares survivors on Drop");
        // Spawning into a reused group calls clear() so a fresh child is not
        // spared by the stale latch.
        latch.clear();
        assert!(
            !latch.is_set(),
            "clear() re-arms Drop's kill for a reused group"
        );
    }

    // The re-arm race (T-079): a spawn/adopt that clears the latch AFTER a
    // non-escalating shutdown snapshotted its generation but BEFORE that shutdown's
    // `request` lands must win — the stale request must not silently re-spare the
    // fresh child. This is the single-word generation guard doing its job; a plain
    // boolean latch would fail this.
    #[test]
    fn a_stale_request_does_not_override_a_concurrent_clear() {
        let latch = SkipDropKill::new();
        // A live reused group: an earlier spawn armed the backstop.
        latch.clear();
        // A non-escalating shutdown begins and snapshots the generation…
        let epoch = latch.begin_shutdown();
        // …then a concurrent spawn/adopt re-arms the backstop for a fresh child…
        latch.clear();
        // …and only now does the shutdown's (stale) request land.
        latch.request(epoch);
        assert!(
            !latch.is_set(),
            "a stale non-escalating request must not re-spare a child that a \
             concurrent spawn/adopt already re-armed the backstop for"
        );
    }

    // Without an intervening clear, the shutdown's request still spares survivors:
    // the generation is unchanged, so the compare-exchange succeeds. A later
    // spawn/adopt then re-arms the backstop for the newcomer, as before.
    #[test]
    fn request_spares_survivors_when_no_clear_intervenes() {
        let latch = SkipDropKill::new();
        latch.clear(); // a spawned survivor
        let epoch = latch.begin_shutdown();
        latch.request(epoch);
        assert!(
            latch.is_set(),
            "an unraced non-escalating request spares survivors"
        );
        latch.clear();
        assert!(
            !latch.is_set(),
            "a spawn after the spare re-arms Drop's kill"
        );
    }

    // Generations are monotonic: an epoch captured before a `clear` can never
    // match again, so its request stays a no-op even as a *fresh* epoch at the new
    // generation spares normally.
    #[test]
    fn a_stale_epoch_never_matches_a_later_generation() {
        let latch = SkipDropKill::new();
        let stale = latch.begin_shutdown();
        latch.clear(); // generation advances past `stale`
        let fresh = latch.begin_shutdown();
        latch.request(stale);
        assert!(
            !latch.is_set(),
            "a stale epoch cannot spare at a newer generation"
        );
        latch.request(fresh);
        assert!(
            latch.is_set(),
            "a current-generation request spares as usual"
        );
    }
}

/// Per-process resource metrics sampled from the OS.
#[cfg(feature = "stats")]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProcMetrics {
    pub cpu_time: Option<Duration>,
    pub peak_memory_bytes: Option<u64>,
}

/// An opaque snapshot of a process's OS-reported **start identity** — a start
/// timestamp captured once so a later metrics read can prove a pid still names the
/// *same* process instance, not one that recycled the number after the original
/// was reaped. Only ever compared for equality, never interpreted: the units are
/// platform-specific — Windows uses the process-creation `FILETIME` (100 ns units)
/// and Linux uses `/proc/<pid>/stat` field 22 (`starttime`, clock ticks since
/// boot); the POSIX fallback (macOS/BSD) reports none. This is the per-process
/// analogue of the pgroup backend's start-time identity token (see
/// [`pgroup::read_identity`](crate::sys::pgroup)); it exists to keep a pid-reuse
/// race from folding an unrelated process's CPU/memory into a sample.
#[cfg(feature = "stats")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcIdentity(u64);

// Constructed and read only by the Linux (`/proc` starttime) and Windows
// (creation `FILETIME`) backends; the POSIX fallback (macOS/BSD, `unix.rs`)
// reports no identity and ignores the anchor, leaving both associated items
// unused there — allow it on exactly that target rather than deleting methods
// the other two backends need (mirrors the `SpawnOptions` field pattern above).
#[cfg(feature = "stats")]
#[cfg_attr(all(unix, not(target_os = "linux")), allow(dead_code))]
impl ProcIdentity {
    /// Wrap a platform-specific raw start-time token (constructed only by the
    /// platform backend that knows its units).
    pub(crate) fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw token, for a platform backend's own read-time equality re-check.
    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

/// Sample CPU time and peak memory for a single process by pid, but only when its
/// current OS identity still matches `expected` (see [`ProcIdentity`]). If the pid
/// was recycled by an unrelated process — its start identity no longer matching —
/// the read yields defaults (all `None`) rather than that stranger's counters, so
/// a sample can never be misattributed after PID reuse. `expected == None` skips
/// the check (no identity was captured, or the platform can't report one),
/// preserving the number-only behavior with no weakening. Returns defaults if the
/// process is gone or the platform can't report.
#[cfg(feature = "stats")]
pub(crate) fn process_metrics(pid: u32, expected: Option<ProcIdentity>) -> ProcMetrics {
    imp::process_metrics(pid, expected)
}

/// Capture the OS start-time identity anchor of the *live* process at `pid`, or
/// `None` if the platform can't report one (macOS/BSD) or the process is already
/// gone. Captured once — at spawn for the per-process sampler, or when a cgroup
/// member is first read for a group-stats fold — and handed back to
/// [`process_metrics`] so a later reading taken against a recycled pid is
/// rejected rather than folded in.
#[cfg(feature = "stats")]
pub(crate) fn process_identity(pid: u32) -> Option<ProcIdentity> {
    imp::process_identity(pid)
}

// Shared POSIX process-group backend for both the Linux fallback and macOS/BSD.
#[cfg(unix)]
pub(crate) mod pgroup;

// Shared graceful-shutdown escalation driver for both unix backends. Windows'
// atomic Job kill has no graceful tier, so it is unix-only. `pub(crate)` so the
// shared-group single-child kill-and-reap primitive ([`graceful::run_pid`]) is
// reachable from `crate::running`, which drives the streaming-timeout teardown.
#[cfg(unix)]
pub(crate) mod graceful;

// The linearizable pid gate: serializes every raw direct-child kill a detached
// teardown watchdog issues against the reap that frees (and lets the OS recycle)
// the pid. Cross-platform (the state machine is platform-agnostic; only the kill
// syscall behind `force_kill` differs); driven by `crate::running`.
pub(crate) mod pid_gate;

/// Per-spawn knobs that must reach the platform backend (the
/// `tokio::process::Command` can't carry them: creation flags have no getter,
/// and the pgroup backend must know about `setsid` *before* it sets a process
/// group).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SpawnOptions {
    /// The command carries a `setsid()` pre-exec hook: the pgroup backend must
    /// skip its `process_group(0)` (std applies setpgid before pre-exec hooks,
    /// and `setsid` fails `EPERM` for a process that is already a group
    /// leader); the new session's group (pgid == pid) is tracked instead.
    /// Only unix backends consult it — non-unix launches reject `setsid`
    /// upstream before a `SpawnOptions` is ever built.
    #[cfg_attr(not(unix), allow(dead_code))]
    pub setsid: bool,
    /// Extra Windows creation flags (e.g. `CREATE_NO_WINDOW`), OR'd with the
    /// containment-required `CREATE_SUSPENDED` on the Windows backend. Only
    /// the Windows backend consults it — elsewhere the flag is a documented
    /// no-op.
    #[cfg_attr(not(windows), allow(dead_code))]
    pub creation_flags: u32,
    /// Arm `PR_SET_PDEATHSIG(SIGKILL)` on the direct child. Only the Linux
    /// backend consults it — Windows already kills the tree when the parent
    /// dies (job handle closes), macOS/BSD have no equivalent; both are
    /// documented on [`Command::kill_on_parent_death`](crate::Command::kill_on_parent_death).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub kill_on_parent_death: bool,
}

// processkit supports only Unix and Windows: it relies on `tokio::process` and
// on OS job / process-group primitives (cgroups, setpgid, Job Objects) that have
// no equivalent on bare targets like wasm. Fail with a clear message rather than
// a cascade of missing-symbol errors from a containment-less fallback.
#[cfg(not(any(unix, windows)))]
compile_error!(
    "processkit supports only Unix and Windows targets — it requires tokio::process \
     and OS job/process-group primitives unavailable on this target."
);

// Exactly one platform module is compiled per target. Each defines an `imp::Job`
// with the same inherent methods plus a kill-on-close `Drop`.
#[cfg_attr(windows, path = "windows.rs")]
#[cfg_attr(target_os = "linux", path = "linux.rs")]
#[cfg_attr(all(unix, not(target_os = "linux")), path = "unix.rs")]
mod imp;

/// A handle to an OS job owning a tree of child processes.
///
/// Dropping the `Job` hard-kills every process still inside it, so an exiting or
/// panicking owner never leaks subprocesses.
pub(crate) struct Job(imp::Job);

impl Job {
    /// Create a fresh, empty job, applying any resource `limits`.
    ///
    /// Errors if `limits` requests a cap the target's mechanism can't enforce (no
    /// cgroup/Job Object, or a Linux cgroup whose controllers can't be enabled —
    /// see `ResourceLimits` for the cgroup-v2 real-root requirement).
    #[cfg(feature = "limits")]
    pub(crate) fn new(limits: &ResourceLimits) -> io::Result<Self> {
        imp::Job::new(limits).map(Job)
    }

    /// Create a fresh, empty job.
    #[cfg(not(feature = "limits"))]
    pub(crate) fn new() -> io::Result<Self> {
        imp::Job::new().map(Job)
    }

    /// Spawn `cmd` as a member of this job, honoring the per-spawn `opts`.
    ///
    /// The child — and any process it later spawns — belongs to the job and is
    /// reaped when the job is killed or dropped.
    pub(crate) fn spawn(&self, cmd: &mut Command, opts: &SpawnOptions) -> io::Result<Child> {
        self.0.spawn(cmd, opts)
    }

    /// Attach an already-started child to this job.
    ///
    /// Only the child itself is moved into the job; descendants it already
    /// spawned keep their original containment.
    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        self.0.adopt(child)
    }

    /// Immediately hard-kill every process in the job. Idempotent.
    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.0.kill_all()
    }

    /// Broadcast `sig` to every process in the job. On Windows only
    /// [`Signal::Kill`] is deliverable (job terminate); other signals yield
    /// `ErrorKind::Unsupported`.
    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        self.0.signal(sig)
    }

    /// Freeze the whole tree (cgroup.freeze / SIGSTOP / per-thread suspend).
    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.0.suspend()
    }

    /// Thaw a tree frozen by [`suspend`](Self::suspend).
    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.0.resume()
    }

    /// Snapshot the live member pids (whole tree on Windows/cgroup; tracked
    /// group leaders on the POSIX fallback).
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        self.0.members()
    }

    /// Ask the tree to exit, then escalate.
    ///
    /// On Unix: send `signal` (typically `SIGTERM`), wait up to `timeout` for the
    /// members to leave, then `SIGKILL` survivors when `escalate` is set. On
    /// Windows the job kill is atomic, so `signal` and `timeout` are **both
    /// ignored**: `escalate = true` kills the tree immediately (equivalent to
    /// [`kill_all`](Self::kill_all)), and `escalate = false` leaves survivors alive
    /// (`Drop` then closes the handle without `KILL_ON_JOB_CLOSE`). Windows has no
    /// way to *trigger* a graceful exit (no soft signal), so it does not wait out
    /// the grace `timeout` as a drain window either — doing so would only delay the
    /// kill of a child that ignores the (absent) signal by the whole grace. The
    /// "signal → grace → kill" tiers are Unix-only; on Windows the deadline is a
    /// prompt hard kill.
    ///
    /// `escalate = false` survivor-sparing is **best-effort on Windows**: `Drop`
    /// clears `KILL_ON_JOB_CLOSE` before closing the handle, but if that
    /// `SetInformationJobObject` call fails the handle close still kills the tree
    /// (a deliberate fail-safe — an unexpected kill is preferred over ambiguous
    /// orphaning). On Unix the spare is unconditional once the flag is set.
    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        self.0.graceful_shutdown(signal, timeout, escalate).await
    }

    /// Snapshot the group's resource usage.
    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        self.0.stats()
    }

    /// The containment mechanism actually in effect.
    pub(crate) fn mechanism(&self) -> Mechanism {
        self.0.mechanism()
    }
}
