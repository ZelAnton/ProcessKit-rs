//! Fallback implementation for non-unix, non-Windows targets without any
//! supported job mechanism (e.g. wasm): the child is spawned directly with no
//! kernel containment, so there is no kill-on-close guarantee. [`Mechanism::None`]
//! makes this observable to callers. (macOS and the BSDs use the process-group
//! backend in `sys::unix` instead, not this.)

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::Mechanism;
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
#[cfg(feature = "stats")]
use crate::sys::ProcMetrics;

pub(crate) struct Job;

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // No kernel containment here, so there is nothing to enforce a limit with.
        #[cfg(feature = "limits")]
        if limits.any() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "resource limits require a cgroup or Job Object; unavailable on this target",
            ));
        }
        Ok(Job)
    }

    pub(crate) fn spawn(&self, cmd: &mut Command) -> io::Result<Child> {
        // No containment available — a plain spawn.
        cmd.spawn()
    }

    pub(crate) fn adopt(&self, _child: &Child) -> io::Result<()> {
        // No containment available; nothing to attach the child to.
        Ok(())
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        // Nothing is tracked, so there is nothing to kill here. Individual
        // children are still killed via their own handles by the higher layers.
        Ok(())
    }

    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        // No containment and no signal facility — report it rather than letting
        // the caller believe the tree was signalled.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("signal({sig:?})"),
        ))
    }

    pub(crate) fn suspend(&self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "suspend"))
    }

    pub(crate) fn resume(&self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "resume"))
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        _timeout: Duration,
        _escalate: bool,
    ) -> io::Result<()> {
        Ok(())
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        // No containment, so no group accounting is available.
        Ok(ProcessGroupStats {
            active_process_count: 0,
            total_cpu_time: None,
            peak_memory_bytes: None,
        })
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        Mechanism::None
    }
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(_pid: u32) -> ProcMetrics {
    ProcMetrics::default()
}
