//! Implementation for unix targets without cgroups or Job Objects (macOS, the
//! BSDs): a [`ProcessGroup`](super::pgroup::ProcessGroup) per the shared POSIX
//! backend. Every child leads its own process group, so dropping the job
//! `killpg`s the whole tree — a real kill-on-close guarantee, weaker only
//! against children that `setsid` away. Surfaced as [`Mechanism::ProcessGroup`].
//!
//! These targets have no `/proc`, so per-process CPU/memory metrics are not
//! available; [`process_metrics`] returns defaults.

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::Mechanism;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
#[cfg(feature = "stats")]
use crate::sys::ProcMetrics;
use crate::sys::pgroup::ProcessGroup;

pub(crate) struct Job {
    group: ProcessGroup,
}

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // A POSIX process group has no resource accounting — there is no whole-tree
        // memory/pids/cpu primitive here, so a requested limit can't be honored.
        #[cfg(feature = "limits")]
        if limits.any() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "resource limits require a cgroup or Job Object; unavailable on this target",
            ));
        }
        Ok(Job {
            group: ProcessGroup::new(),
        })
    }

    pub(crate) fn spawn(&self, cmd: &mut Command) -> io::Result<Child> {
        self.group.spawn(cmd)
    }

    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        self.group.adopt(child)
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.group.kill_all()
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        self.group.graceful_shutdown(timeout, escalate).await
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        self.group.stats()
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        Mechanism::ProcessGroup
    }
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(_pid: u32) -> ProcMetrics {
    // No `/proc` on these targets; per-process accounting is not available.
    ProcMetrics::default()
}
