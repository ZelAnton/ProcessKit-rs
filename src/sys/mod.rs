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

/// Per-process resource metrics sampled from the OS.
#[cfg(feature = "stats")]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProcMetrics {
    pub cpu_time: Option<Duration>,
    pub peak_memory_bytes: Option<u64>,
}

/// Sample CPU time and peak memory for a single process by pid. Returns
/// defaults (all `None`) if the process is gone or the platform can't report.
#[cfg(feature = "stats")]
pub(crate) fn process_metrics(pid: u32) -> ProcMetrics {
    imp::process_metrics(pid)
}

// The shared POSIX process-group backend, used by both the Linux fallback and
// the macOS/BSD `imp`. Compiled on every unix target.
#[cfg(unix)]
pub(crate) mod pgroup;

// The shared graceful-shutdown escalation driver, used by both unix backends
// (the Linux cgroup and the process-group fallback). Windows' atomic Job kill
// has no graceful tier, so it is unix-only.
#[cfg(unix)]
mod graceful;

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
    /// Windows the job kill is atomic, so `signal` and `timeout` are ignored, but
    /// `escalate` is still honored: `true` kills the tree immediately (equivalent
    /// to [`kill_all`](Self::kill_all)), while `false` leaves survivors alive
    /// (`Drop` then closes the handle without `KILL_ON_JOB_CLOSE`).
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
