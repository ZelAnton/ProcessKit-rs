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
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
use crate::sys::pgroup::ProcessGroup;
#[cfg(feature = "stats")]
use crate::sys::{ProcIdentity, ProcMetrics};

pub(crate) struct Job {
    group: ProcessGroup,
}

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // A POSIX process group has no resource accounting, so a requested limit
        // can't be honored — fail rather than hand back an unbounded tree.
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

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        self.group.spawn(cmd, opts)
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        self.group.adopt(child)
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.group.kill_all()
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        self.group.signal(sig.raw())
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.group.suspend()
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.group.resume()
    }

    /// Tracked group leaders only — see the pgroup backend.
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        Ok(self
            .group
            .members()
            .into_iter()
            .map(|pid| pid as u32)
            .collect())
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        self.group
            .graceful_shutdown(signal, timeout, escalate)
            .await
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
pub(crate) fn process_metrics(_pid: u32, _expected: Option<ProcIdentity>) -> ProcMetrics {
    // Not *implemented* on these targets (returns the empty default), rather than
    // impossible: macOS/BSD have no `/proc`, so the Linux `/proc/<pid>/stat` path
    // doesn't apply, but per-process CPU/memory IS obtainable here via
    // `libproc`/`proc_pidinfo` (macOS) or `kvm`/`sysctl` (BSD) — just not wired up
    // (C12). Group-level `stats()` is likewise unavailable on the process-group
    // mechanism; the count is all it can report. The `expected` identity is
    // irrelevant while no metrics are reported — an all-`None` default can never
    // misattribute a recycled pid's counters, so it is honestly ignored.
    ProcMetrics::default()
}

#[cfg(feature = "stats")]
pub(crate) fn process_identity(_pid: u32) -> Option<ProcIdentity> {
    // No wired-up per-process metrics here (see `process_metrics`), so there is no
    // reading to identity-gate and thus no anchor to capture. Honest `None` — never
    // a fabricated token — degrades callers to the number-only path, which on this
    // backend already reports no metrics. (The pgroup backend that actually tracks
    // liveness DOES read a start-time identity where the platform allows it — see
    // `pgroup::read_identity`; this is only the metrics-side stub.)
    None
}
