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
//! - **other** (non-unix, non-Windows — e.g. wasm) — a plain spawn with no
//!   kernel containment.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::Mechanism;
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;

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

// Exactly one platform module is compiled per target. Each defines an `imp::Job`
// with the same inherent methods plus a kill-on-close `Drop`.
#[cfg_attr(windows, path = "windows.rs")]
#[cfg_attr(target_os = "linux", path = "linux.rs")]
#[cfg_attr(all(unix, not(target_os = "linux")), path = "unix.rs")]
#[cfg_attr(not(any(windows, unix)), path = "other.rs")]
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
    /// cgroup/Job Object, or a cgroup without controller delegation).
    #[cfg(feature = "limits")]
    pub(crate) fn new(limits: &ResourceLimits) -> io::Result<Self> {
        imp::Job::new(limits).map(Job)
    }

    /// Create a fresh, empty job.
    #[cfg(not(feature = "limits"))]
    pub(crate) fn new() -> io::Result<Self> {
        imp::Job::new().map(Job)
    }

    /// Spawn `cmd` as a member of this job.
    ///
    /// The child — and any process it later spawns — belongs to the job and is
    /// reaped when the job is killed or dropped.
    pub(crate) fn spawn(&self, cmd: &mut Command) -> io::Result<Child> {
        self.0.spawn(cmd)
    }

    /// Attach an already-started child to this job.
    ///
    /// Only the child itself is moved into the job; descendants it already
    /// spawned keep their original containment. On targets without a job
    /// mechanism (non-unix, non-Windows) this is a no-op.
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        self.0.adopt(child)
    }

    /// Immediately hard-kill every process in the job. Idempotent.
    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.0.kill_all()
    }

    /// Broadcast `sig` to every process in the job. On Windows only
    /// [`Signal::Kill`] is deliverable (job terminate); other signals — and any
    /// signal on the no-containment target — yield `ErrorKind::Unsupported`.
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        self.0.signal(sig)
    }

    /// Freeze the whole tree (cgroup.freeze / SIGSTOP / per-thread suspend).
    /// `ErrorKind::Unsupported` on the no-containment target.
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.0.suspend()
    }

    /// Thaw a tree frozen by [`suspend`](Self::suspend).
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.0.resume()
    }

    /// Snapshot the live member pids (whole tree on Windows/cgroup; tracked
    /// group leaders on the POSIX fallback; always empty with no containment).
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        self.0.members()
    }

    /// Ask the tree to exit, then escalate.
    ///
    /// On Unix: signal `SIGTERM`, wait up to `timeout` for the members to leave,
    /// then `SIGKILL` survivors when `escalate` is set. On Windows the job kill is
    /// atomic, so this is equivalent to [`kill_all`](Self::kill_all) and the
    /// arguments are ignored.
    pub(crate) async fn graceful_shutdown(
        &self,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        self.0.graceful_shutdown(timeout, escalate).await
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
