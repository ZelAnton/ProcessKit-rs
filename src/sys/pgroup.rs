//! Shared POSIX process-group job.
//!
//! Each spawned child becomes the leader of its own process group, so signalling
//! the negative group id (`killpg`) reaps the child *and* every descendant it
//! forked. This backs two callers:
//!
//! - **Linux** — the fallback when no writable cgroup is available (e.g. a CI
//!   runner without cgroup delegation).
//! - **macOS / the BSDs** — the primary mechanism, since those targets have
//!   neither cgroups nor Job Objects.
//!
//! Weaker than a cgroup or Job Object: a child that calls `setsid` starts a new
//! session and escapes the group. Callers surface this as
//! [`Mechanism::ProcessGroup`](crate::Mechanism::ProcessGroup) so it is never a
//! silent downgrade.

use std::io;
use std::os::unix::process::CommandExt;
use std::sync::Mutex;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep};

#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;

/// How often the graceful path re-checks whether the tree has drained.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// A set of process groups, one per spawned (or adopted) child.
///
/// Tracks the group ids (each == its leader child's pid) so teardown can signal
/// them. Its [`Drop`] hard-kills every still-live group, so an exiting or
/// panicking owner never leaks subprocesses.
pub(crate) struct ProcessGroup {
    /// Group ids we own. A group id is the leader child's pid.
    pgids: Mutex<Vec<i32>>,
}

impl ProcessGroup {
    pub(crate) fn new() -> Self {
        ProcessGroup {
            pgids: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn spawn(&self, cmd: &mut Command) -> io::Result<Child> {
        // Own process group per child → killpg reaps it and its descendants.
        // `process_group(0)` == setpgid(0, 0): the child becomes its own group
        // leader.
        cmd.as_std_mut().process_group(0);
        let child = cmd.spawn()?;
        if let Some(pid) = child.id()
            && let Ok(mut g) = self.pgids.lock()
        {
            retain_live(&mut g);
            g.push(pid as i32);
        }
        Ok(child)
    }

    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        let pid = child
            .id()
            .ok_or_else(|| io::Error::other("child has no pid (already exited?)"))?
            as i32;
        // Make the external child its own group leader and track it. Only the
        // child itself is moved — already running descendants keep their group.
        // SAFETY: setpgid on a live pid is a sound call.
        let rc = unsafe { libc::setpgid(pid, 0) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            // Benign races/permissions (process gone, already a session leader,
            // cross-session) are not fatal — swallow them.
            let code = err.raw_os_error().unwrap_or(0);
            if code != libc::ESRCH && code != libc::EPERM && code != libc::EACCES {
                return Err(err);
            }
        }
        if let Ok(mut g) = self.pgids.lock() {
            retain_live(&mut g);
            g.push(pid);
        }
        Ok(())
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        signal_groups(&self.pgids, libc::SIGKILL);
        Ok(())
    }

    /// Broadcast `sig` to every tracked process group. Best-effort: groups that
    /// already drained are skipped (and pruned); an empty set is a no-op.
    pub(crate) fn signal(&self, sig: i32) -> io::Result<()> {
        signal_groups(&self.pgids, sig);
        Ok(())
    }

    /// Freeze every tracked group (`SIGSTOP` — unblockable, idempotent).
    pub(crate) fn suspend(&self) -> io::Result<()> {
        signal_groups(&self.pgids, libc::SIGSTOP);
        Ok(())
    }

    /// Thaw every tracked group (`SIGCONT`).
    pub(crate) fn resume(&self) -> io::Result<()> {
        signal_groups(&self.pgids, libc::SIGCONT);
        Ok(())
    }

    /// The live tracked group **leaders** (one pid per spawned/adopted child) —
    /// descendants inside those groups are not enumerated here. Dead groups are
    /// pruned on the way.
    pub(crate) fn members(&self) -> Vec<i32> {
        match self.pgids.lock() {
            Ok(mut g) => {
                retain_live(&mut g);
                g.clone()
            }
            Err(_) => Vec::new(),
        }
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        signal_groups(&self.pgids, libc::SIGTERM);
        let deadline = Instant::now() + timeout;
        while groups_alive(&self.pgids) {
            if Instant::now() >= deadline {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
        if escalate && groups_alive(&self.pgids) {
            signal_groups(&self.pgids, libc::SIGKILL);
        }
        Ok(())
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        // We track group ids, not individual pids, so report the number of
        // still-live groups and leave cpu/memory absent.
        let active = match self.pgids.lock() {
            Ok(g) => g
                .iter()
                // SAFETY: signal 0 is a sound existence probe.
                .filter(|&&pgid| unsafe { libc::kill(-pgid, 0) == 0 })
                .count(),
            Err(_) => 0,
        };
        Ok(ProcessGroupStats {
            active_process_count: active,
            total_cpu_time: None,
            peak_memory_bytes: None,
        })
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        signal_groups(&self.pgids, libc::SIGKILL);
    }
}

/// Send `sig` to every still-live tracked process group, dropping the ones that
/// have already drained.
///
/// A group id is the leader's pid, so a stale id whose leader was reaped and
/// whose pid got recycled could in theory address an unrelated group. Probing
/// liveness (`kill(-pgid, 0)`) immediately before `killpg` keeps that window just
/// a few instructions wide, and pruning the dead ids stops the set from growing
/// without bound over a long-lived group's lifetime. (Still best-effort against a
/// child that `setsid`s out of its group entirely.)
fn signal_groups(pgids: &Mutex<Vec<i32>>, sig: i32) {
    if let Ok(mut g) = pgids.lock() {
        g.retain(|&pgid| {
            // SAFETY: signal 0 to a negative pid is a sound existence probe.
            if unsafe { libc::kill(-pgid, 0) } != 0 {
                return false; // ESRCH: the group is gone — forget it.
            }
            // SAFETY: killpg on a positive group id is always a sound call; a
            // group that exits between the probe and here simply returns ESRCH.
            unsafe { libc::killpg(pgid, sig) };
            true
        });
    }
}

/// Whether any tracked process group still has at least one live member.
fn groups_alive(pgids: &Mutex<Vec<i32>>) -> bool {
    let Ok(g) = pgids.lock() else {
        return false;
    };
    g.iter().any(|&pgid| {
        // `kill(-pgid, 0)` performs no signal but reports existence: 0 if the
        // group has a member, ESRCH otherwise.
        // SAFETY: signal 0 to a negative pid is a sound existence probe.
        unsafe { libc::kill(-pgid, 0) == 0 }
    })
}

/// Drop process groups that have already drained. An empty group can never
/// regain members (new members only fork from existing ones), so an `ESRCH`
/// probe is terminal — forgetting the id is sound and keeps a recyclable dead pid
/// from later being mistaken for a live group.
fn retain_live(pgids: &mut Vec<i32>) {
    // SAFETY: signal 0 to a negative pid is a sound existence probe.
    pgids.retain(|&pgid| unsafe { libc::kill(-pgid, 0) == 0 });
}
