//! [`ProcessGroup`] — a kill-on-drop container for a tree of child processes.

use std::time::Duration;

use tokio::process::{Child, Command};

use crate::error::{Error, Result};
use crate::mechanism::Mechanism;
use crate::stats::ProcessGroupStats;
use crate::sys::Job;

/// Tuning for a [`ProcessGroup`]'s graceful shutdown.
///
/// These knobs only affect the Unix graceful path
/// ([`ProcessGroup::shutdown`]): give the tree `shutdown_timeout` to exit after
/// `SIGTERM`, then `SIGKILL` survivors if `escalate_to_kill` is set. On Windows
/// the job kill is atomic, so they are ignored.
#[derive(Debug, Clone)]
pub struct ProcessGroupOptions {
    /// How long to wait after `SIGTERM` before escalating. Default: 2 seconds.
    pub shutdown_timeout: Duration,
    /// Whether to `SIGKILL` processes that outlive `shutdown_timeout`.
    /// Default: `true`.
    pub escalate_to_kill: bool,
}

impl Default for ProcessGroupOptions {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(2),
            escalate_to_kill: true,
        }
    }
}

/// A container that ties the lifetime of a child-process tree to its own.
///
/// Every process spawned into the group — and everything *those* processes
/// spawn — is killed when the group is dropped (kill-on-close), so an exiting or
/// panicking owner never leaks subprocesses. The containment mechanism is
/// platform-specific and observable via [`mechanism`](Self::mechanism).
///
/// Dropping the group performs an immediate **hard** kill. For a graceful
/// `SIGTERM` → wait → `SIGKILL` teardown (Unix), call
/// [`shutdown`](Self::shutdown) instead — `Drop` cannot `await`, so the graceful
/// tier lives in that async method.
pub struct ProcessGroup {
    job: Job,
    options: ProcessGroupOptions,
}

impl ProcessGroup {
    /// Create an empty group with [default options](ProcessGroupOptions).
    pub fn new() -> Result<Self> {
        Self::with_options(ProcessGroupOptions::default())
    }

    /// Create an empty group with the given options.
    pub fn with_options(options: ProcessGroupOptions) -> Result<Self> {
        let job = Job::new()?;
        Ok(Self { job, options })
    }

    /// Spawn `cmd` as a member of this group.
    ///
    /// The returned [`Child`] — and any process it later spawns — belongs to the
    /// group and is reaped when the group is killed or dropped. The caller is
    /// responsible for configuring `cmd`'s stdio; the group only handles
    /// containment.
    ///
    /// **Windows:** to make containment race-free the child is created
    /// `CREATE_SUSPENDED`, assigned to the job, then resumed. This **overwrites**
    /// any process-creation flags the caller set on `cmd` (e.g.
    /// `CREATE_NO_WINDOW`) — Win32 exposes no way to read them back and OR the
    /// suspend bit in. Set such flags through a different launch path if you need
    /// them, or accept that this escape hatch forces only `CREATE_SUSPENDED`.
    /// **Unix:** the group likewise installs a `pre_exec` hook on `cmd` to join
    /// the cgroup / process group.
    pub fn spawn(&self, cmd: &mut Command) -> Result<Child> {
        let child = self.job.spawn(cmd).map_err(|source| Error::Spawn {
            program: program_name(cmd),
            source,
        })?;
        Ok(child)
    }

    /// Attach an already-started [`Child`] to this group.
    ///
    /// Only the child itself is moved into the group; processes it has *already*
    /// spawned keep their original containment (future forks are captured). On
    /// targets without a job mechanism (non-unix, non-Windows) this is a no-op.
    pub fn adopt(&self, child: &Child) -> Result<()> {
        self.job.adopt(child)?;
        Ok(())
    }

    /// Immediately hard-kill every process currently in the group. Idempotent;
    /// the group remains usable for further spawns afterwards.
    pub fn terminate_all(&self) -> Result<()> {
        self.job.kill_all()?;
        Ok(())
    }

    /// Gracefully tear the group down, consuming it.
    ///
    /// On Unix: `SIGTERM` the tree, wait up to `shutdown_timeout`, then `SIGKILL`
    /// survivors when `escalate_to_kill` is set. On Windows the kill is atomic.
    /// Dropping the group instead (without calling this) performs only the hard
    /// kill.
    pub async fn shutdown(self) -> Result<()> {
        self.job
            .graceful_shutdown(self.options.shutdown_timeout, self.options.escalate_to_kill)
            .await?;
        // `self` drops here; the job's Drop hard-kills any straggler (a no-op
        // after a successful graceful shutdown) and frees the OS handle/cgroup.
        Ok(())
    }

    /// Snapshot the group's resource usage (active process count and, where the
    /// platform supports it, total CPU time and peak memory). See
    /// [`ProcessGroupStats`].
    pub fn stats(&self) -> Result<ProcessGroupStats> {
        let stats = self.job.stats()?;
        Ok(stats)
    }

    /// The containment mechanism actually in effect (see [`Mechanism`]).
    pub fn mechanism(&self) -> Mechanism {
        self.job.mechanism()
    }
}

/// Best-effort program name for error messages.
fn program_name(cmd: &Command) -> String {
    cmd.as_std().get_program().to_string_lossy().into_owned()
}
