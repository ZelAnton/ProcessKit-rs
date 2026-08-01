//! Platform job layer — one `imp::Job` per target, all exposing the same shape.
//!
//! A `Job` is the kernel object that contains a process tree so the whole tree
//! dies with its owner:
//!
//! - **Windows** — a [Job Object] with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
//! - **Linux** — a [cgroup v2] killed via `cgroup.kill`, falling back to a POSIX
//!   process group when no writable cgroup is available.
//! - **macOS / the BSDs** — a POSIX process group (`killpg` the tree on drop);
//!   no cgroups or Job Objects exist there. See `pgroup`.
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
use crate::limits::{CappedAxes, LimitEvidence, ResourceLimits};
#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
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

/// Serialize every Windows child-creation call made by ProcessKit.
///
/// Headless ConPTY launch briefly replaces this process's standard-handle slots
/// with null so the pseudoconsole, rather than redirected launcher stdio, owns
/// the child's handles. Ordinary and detached ProcessKit spawns must not observe
/// that process-global window. Code outside this crate cannot share this lock;
/// the remaining limitation is documented on [`crate::Command::use_pty`].
#[cfg(windows)]
static PROCESS_SPAWN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(windows)]
pub(crate) fn process_spawn_lock() -> std::sync::MutexGuard<'static, ()> {
    // A panic while an OS spawn is in flight gives callers no useful recovery
    // action; the ConPTY guard still restores stdio during unwinding.
    PROCESS_SPAWN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// The generation-guarded "don't kill on Drop" latch. Split into its own module so
// the standalone loom harness (`loom/`) can `#[path]`-include just that pure core
// and model-check the spawn/shutdown re-arm race (T-079) — see `skip_drop_kill.rs`
// and `crate::sync`.
mod skip_drop_kill;
pub(crate) use skip_drop_kill::SkipDropKill;

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
/// `pgroup::read_identity`); it exists to keep a pid-reuse
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

/// Identity + best-effort metadata for an **arbitrary** pid (not one tracked by
/// any group) — the platform-dispatching core of the public
/// [`process_info`](crate::process_info) query. Returns the same fields a
/// [`MemberInfo`] carries for a group member, read through the
/// **same** per-platform readers (`/proc/<pid>/stat` on Linux, `proc_pidinfo` on
/// macOS, `Toolhelp32` + creation `FILETIME` on Windows, a `kill(pid, 0)` existence
/// probe on the bare BSDs).
///
/// The three-way contract every backend upholds:
/// - `Ok(Some(info))` — the process exists; each enriching field is honestly
///   `Option` (`None` where the platform can't report it).
/// - `Ok(None)` — the process definitively does **not** exist (an honest negative,
///   never an error).
/// - `Err` — the process may exist but couldn't be inspected (a permission denial
///   or other OS error), so a caller never mistakes "not allowed to look" for
///   "dead".
///
/// Reads no argv/environment on any platform — the crate's standing "never
/// argv/env" rule.
#[cfg(feature = "process-control")]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<crate::member::MemberInfo>> {
    imp::process_info(pid)
}

// Shared POSIX process-group backend for both the Linux fallback and macOS/BSD.
#[cfg(unix)]
pub(crate) mod pgroup;

// Shared `/proc/<pid>/stat` parser for the Linux/Android backends. The pgroup
// liveness identity token (`pgroup::read_identity`) and the Linux per-process
// metrics sampler (`imp::process_metrics` / `process_identity`) all pull the same
// start-time field from the same file with the same "skip past the comm's last
// ')', then field 22 = whitespace index 19" convention; centralizing it here keeps
// that convention from drifting between them and silently weakening the
// anti-pid-reuse identity gate.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) mod procfs;

// Shared graceful-shutdown escalation driver. The whole-tree
// signal → poll → escalate loop ([`graceful::run`]) and its [`GracefulTarget`]
// trait are cross-platform: both unix backends drive it (SIGTERM → grace →
// SIGKILL), and the Windows Job Object drives it too for the opt-in
// console-CTRL graceful path (CTRL_BREAK → grace → `TerminateJobObject`). The
// single-child kill-and-reap primitive ([`graceful::run_pid`]/[`PidTarget`]/
// [`UnixChild`]) stays unix-only — it leans on `PidGate`/`libc` and drives the
// shared-group streaming-timeout teardown from `crate::running`. `pub(crate)`
// so both are reachable from those callers.
pub(crate) mod graceful;

// The linearizable pid gate: serializes every raw direct-child kill a detached
// teardown watchdog issues against the reap that frees (and lets the OS recycle)
// the pid. Cross-platform (the state machine is platform-agnostic; only the kill
// syscall behind `force_kill` differs); driven by `crate::running`.
pub(crate) mod pid_gate;

// The opt-in PTY launch backend (`Command::use_pty`): `openpty` (Unix) /
// `CreatePseudoConsole` ConPTY (Windows) instead of three pipes, wired into the
// SAME per-platform containment path as `Job::spawn` (K-032). Compiled only with
// the `pty` feature; the merged-stream `Backend::Pty` in `crate::running`
// consumes what it hands back.
#[cfg(feature = "pty")]
pub(crate) mod pty;

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
    /// Windows process-affinity mask applied while the freshly-contained child
    /// is still suspended. Unix applies affinity through the command's pre-exec
    /// hook, so only the Windows backend consumes this value.
    #[cfg_attr(not(windows), allow(dead_code))]
    pub cpu_affinity: Option<usize>,
    /// Spawn the direct child in its own console process group
    /// (`CREATE_NEW_PROCESS_GROUP`) so the opt-in graceful teardown can address
    /// it with `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` before the
    /// grace window (see [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break)).
    /// Only the Windows backend consults it — elsewhere it is a documented
    /// no-op (Unix graceful teardown already has a real signal tier).
    #[cfg_attr(not(windows), allow(dead_code))]
    pub windows_new_process_group: bool,
    /// Arm `PR_SET_PDEATHSIG(SIGKILL)` on the direct child. Only the Linux
    /// backend consults it — Windows already kills the tree when the parent
    /// dies (job handle closes), macOS/BSD have no equivalent; both are
    /// documented on [`Command::kill_on_parent_death`](crate::Command::kill_on_parent_death).
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub kill_on_parent_death: bool,
    /// Spawn the child under a pseudo-terminal instead of three independent
    /// pipes (`openpty` on Unix, `CreatePseudoConsole` ConPTY on Windows) — the
    /// opt-in `Command::use_pty` mode. Consulted only
    /// by the PTY spawn path (`crate::sys::pty`), which is compiled only with the
    /// `pty` feature; without the feature the field is always `false` and the
    /// spawn is byte-identical to the three-pipe path. Containment is unchanged —
    /// the PTY child is assigned to the same job/cgroup/process group as any
    /// other child.
    #[cfg_attr(not(feature = "pty"), allow(dead_code))]
    pub use_pty: bool,
    /// The pseudo-terminal window size (`(cols, rows)`) for a `use_pty` spawn, or
    /// `None` to fall back to the backend default (`pty::DEFAULT_PTY_SIZE`).
    /// Set from `Command::pty_size`. Consulted only by
    /// the PTY spawn path (`crate::sys::pty`, `pty`-feature only) — without
    /// `use_pty` the launch never routes there, so a size configured on a non-PTY
    /// command is a documented no-op (never applied to the three-pipe spawn).
    #[cfg_attr(not(feature = "pty"), allow(dead_code))]
    pub pty_size: Option<(u16, u16)>,
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

    /// Replace the live job's resource limits — a **full replacement**, so an axis
    /// left `None` becomes unbounded again. Fails like [`new`](Self::new)'s limit
    /// application: `ErrorKind::Unsupported` when the mechanism has no whole-tree
    /// accounting at all (the POSIX process-group fallback), otherwise a
    /// mechanism-specific failure to apply the request (a Linux cgroup whose
    /// controllers can't be enabled, a Windows Job Object call rejected). Applies
    /// to the same live handle/cgroup the tree-control verbs use.
    #[cfg(feature = "limits")]
    pub(crate) fn update_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        self.0.update_limits(limits)
    }

    /// Post-run evidence about the caps this job carries: did an applied cap
    /// actually fire? See
    /// [`ProcessGroup::limit_evidence`](crate::ProcessGroup::limit_evidence) for the
    /// contract and [`LimitVerdict`](crate::LimitVerdict) for what counts as
    /// evidence on each axis.
    ///
    /// `capped` names the axes that have carried a cap at any point in this job's
    /// life, so a backend reads **only** those (an uncapped axis is
    /// `NotTripped` by construction — no cap, nothing to fire — and costs no I/O).
    /// Infallible by design: an unreadable counter is `Unknown`, never an error,
    /// because "we could not look" and "it did not fire" must not collapse into one
    /// answer. Reads only; it never signals, kills, or mutates the container, so it
    /// leaves teardown/kill-on-drop ordering untouched no matter when it is called.
    #[cfg(feature = "limits")]
    pub(crate) fn limit_evidence(&self, capped: CappedAxes) -> LimitEvidence {
        self.0.limit_evidence(capped)
    }

    /// Spawn `cmd` as a member of this job, honoring the per-spawn `opts`.
    ///
    /// The child — and any process it later spawns — belongs to the job and is
    /// reaped when the job is killed or dropped.
    pub(crate) fn spawn(&self, cmd: &mut Command, opts: &SpawnOptions) -> io::Result<Child> {
        self.0.spawn(cmd, opts)
    }

    /// Spawn `cmd` under a pseudo-terminal as a member of this job — the
    /// [`Command::use_pty`](crate::Command::use_pty) backend. `env` is the child's
    /// resolved environment for the Windows raw-`CreateProcessW` path (ignored on
    /// Unix, whose pty child keeps the tokio `Command`'s env). The child joins the
    /// **same** job as [`spawn`](Self::spawn), so containment is unchanged.
    #[cfg(feature = "pty")]
    pub(crate) fn spawn_pty(
        &self,
        cmd: &mut Command,
        opts: &SpawnOptions,
        env: Option<Vec<(std::ffi::OsString, std::ffi::OsString)>>,
    ) -> io::Result<pty::PtySpawn> {
        // The launch seam routes here only for `use_pty`; the flag on the options
        // records that intent for the platform backend.
        debug_assert!(opts.use_pty, "spawn_pty requires SpawnOptions::use_pty");
        self.0.spawn_pty(cmd, opts, env)
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

    /// The reach of a soft `Int`/`Term` stop on this job right now, read
    /// side-effect-free from live membership (whole tree on the Unix backends;
    /// opt-in console/windowed members, or none, on the Windows Job Object). See
    /// [`ProcessGroup::soft_stop_scope`](crate::ProcessGroup::soft_stop_scope).
    #[cfg(feature = "process-control")]
    pub(crate) fn soft_stop_scope(&self) -> crate::SoftStopScope {
        self.0.soft_stop_scope()
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

    /// Snapshot the live members enriched with ppid / image name / start time
    /// (same member set as [`members`](Self::members); honest `Option` where a
    /// field is unavailable, vanished members skipped).
    #[cfg(feature = "process-control")]
    pub(crate) fn members_info(&self) -> io::Result<Vec<MemberInfo>> {
        self.0.members_info()
    }

    /// Ask the tree to exit, then escalate.
    ///
    /// On Unix: send `signal` (typically `SIGTERM`), wait up to `timeout` for the
    /// members to leave, then `SIGKILL` survivors when `escalate` is set.
    ///
    /// On Windows a Job Object has no POSIX signal, so `signal` is ignored. The
    /// grace `timeout` is used as a drain window **only** when there is a way to
    /// *trigger* a soft exit — a windowed member (`WM_CLOSE`) or a console-CTRL
    /// leader (see below). For a windowless tree with neither, the job kill is
    /// atomic and `timeout` is ignored too: `escalate = true` kills the tree
    /// immediately (equivalent to [`kill_all`](Self::kill_all)), and
    /// `escalate = false` leaves survivors alive (`Drop` then closes the handle
    /// without `KILL_ON_JOB_CLOSE`). It does not wait out the grace as a drain
    /// window there, since polling for a natural exit nothing triggered would only
    /// delay the kill of a child that ignores the (absent) signal by the whole
    /// grace.
    ///
    /// **Windows soft tier.** When a live member owns a top-level window, or a
    /// direct child was spawned with [`SpawnOptions::windows_new_process_group`]
    /// (via [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break)),
    /// this drives the *same* shared [`graceful::run`]
    /// loop the unix backends use: post `WM_CLOSE` to each member window **and**
    /// `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` to each console leader, poll
    /// the job's active-process count up to `timeout`, then `TerminateJobObject`
    /// survivors when `escalate` is set (or spare them when not). `signal` is still
    /// ignored — Windows delivers `WM_CLOSE`/`CTRL_BREAK`, not a POSIX signal. The
    /// console event only reaches children that share this process's console; a
    /// child spawned
    /// [`create_no_window`](crate::Command::create_no_window)/`DETACHED_PROCESS`
    /// cannot receive it, and a windowless member owns no window to close — either
    /// simply rides the grace to the `TerminateJobObject` fallback.
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
    ) -> io::Result<graceful::GracefulOutcome> {
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

/// Read-only prediction of the containment [`Mechanism`] a fresh [`Job`] would use
/// on this host **right now**, computed without creating any OS object or spawning
/// a process — the detection extracted from the group-creation path so it can back
/// the public `host_containment()` query as well.
///
/// A fixed constant on Windows ([`Mechanism::JobObject`]) and macOS/BSD
/// ([`Mechanism::ProcessGroup`]); on Linux a best-effort read-only probe of cgroup
/// v2 availability and writability that agrees with [`Job::new`]'s selection on any
/// real host, differing only in the rare window where a writable-looking cgroup then
/// rejects leaf creation (see the Linux backend's `detect_mechanism`).
pub(crate) fn detect_mechanism() -> Mechanism {
    imp::detect_mechanism()
}

#[cfg(all(test, windows))]
mod process_spawn_lock_tests {
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    #[test]
    fn process_spawn_lock_excludes_a_concurrent_spawn_window() {
        let first = super::process_spawn_lock();
        let ready = Arc::new(Barrier::new(2));
        let worker_ready = Arc::clone(&ready);
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            worker_ready.wait();
            let _second = super::process_spawn_lock();
            acquired_tx.send(()).expect("report lock acquisition");
        });

        ready.wait();
        assert!(
            acquired_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "a concurrent Windows spawn must remain outside the guarded window"
        );
        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the next spawn proceeds once the guarded window closes");
        worker.join().expect("spawn-lock worker must not panic");
    }
}
