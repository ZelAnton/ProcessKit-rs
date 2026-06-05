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
    /// Adopted children that could not be re-grouped: POSIX forbids
    /// `setpgid` on a child that has already `exec`'d (`EACCES`) — the common
    /// case for [`adopt`](Self::adopt). These are tracked and signalled
    /// *individually*: the child itself is contained, but unlike a group
    /// leader, descendants it forks are not.
    solo_pids: Mutex<Vec<i32>>,
}

impl ProcessGroup {
    pub(crate) fn new() -> Self {
        ProcessGroup {
            pgids: Mutex::new(Vec::new()),
            solo_pids: Mutex::new(Vec::new()),
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
        // Try to make the external child its own group leader. Only the child
        // itself is moved — already running descendants keep their group.
        // SAFETY: setpgid on a live pid is a sound call.
        let rc = unsafe { libc::setpgid(pid, 0) };
        if rc == 0 {
            // It now leads group `pid` — track the group; future forks inherit
            // it and are reaped with it. Dedup: adopting a child this group
            // itself spawned (already a leader — setpgid is a no-op success)
            // must not double-track it, or members()/stats() over-report.
            if let Ok(mut g) = self.pgids.lock() {
                retain_live(&mut g);
                if !g.contains(&pid) {
                    g.push(pid);
                }
            }
            return Ok(());
        }

        let err = io::Error::last_os_error();
        match err.raw_os_error().unwrap_or(0) {
            // The child already exited — nothing to contain.
            code if code == libc::ESRCH => Ok(()),
            // POSIX forbids re-grouping a child once it has `exec`'d (EACCES) —
            // the NORMAL case for adopting a running process — and a session
            // leader / cross-session child can't be moved either (EPERM).
            // Recording `pid` as a *group* id would make teardown a silent
            // no-op (no group `pid` exists); track it individually instead:
            // the child is contained, its future forks are not.
            code if code == libc::EACCES || code == libc::EPERM => {
                if let Ok(mut solos) = self.solo_pids.lock() {
                    retain_live_pids(&mut solos);
                    // Dedup a repeated adopt of the same child.
                    if !solos.contains(&pid) {
                        solos.push(pid);
                    }
                }
                Ok(())
            }
            _ => Err(err),
        }
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.broadcast(libc::SIGKILL);
        Ok(())
    }

    /// Broadcast `sig` to every tracked process group and solo-adopted child.
    /// Best-effort: entries that already drained are skipped (and pruned); an
    /// empty set is a no-op.
    pub(crate) fn signal(&self, sig: i32) -> io::Result<()> {
        self.broadcast(sig);
        Ok(())
    }

    /// Freeze every tracked group (`SIGSTOP` — unblockable, idempotent).
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.broadcast(libc::SIGSTOP);
        Ok(())
    }

    /// Thaw every tracked group (`SIGCONT`).
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.broadcast(libc::SIGCONT);
        Ok(())
    }

    /// One signal sweep over both tracking sets.
    fn broadcast(&self, sig: i32) {
        signal_groups(&self.pgids, sig);
        signal_pids(&self.solo_pids, sig);
    }

    /// Whether anything tracked is still alive.
    fn any_alive(&self) -> bool {
        groups_alive(&self.pgids) || pids_alive(&self.solo_pids)
    }

    /// The live tracked group **leaders** (one pid per spawned child) plus the
    /// solo-adopted pids — descendants inside the groups are not enumerated
    /// here. Dead entries are pruned on the way.
    pub(crate) fn members(&self) -> Vec<i32> {
        let mut members = match self.pgids.lock() {
            Ok(mut g) => {
                retain_live(&mut g);
                g.clone()
            }
            Err(_) => Vec::new(),
        };
        if let Ok(mut solos) = self.solo_pids.lock() {
            retain_live_pids(&mut solos);
            members.extend_from_slice(&solos);
        }
        members
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        self.broadcast(libc::SIGTERM);
        let deadline = Instant::now() + timeout;
        while self.any_alive() {
            if Instant::now() >= deadline {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
        if escalate && self.any_alive() {
            self.broadcast(libc::SIGKILL);
        }
        Ok(())
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        // We track group ids (plus solo-adopted pids), not every individual
        // process, so report the number of live entries and leave cpu/memory
        // absent.
        let active = match self.pgids.lock() {
            Ok(g) => g
                .iter()
                // SAFETY: signal 0 is a sound existence probe.
                .filter(|&&pgid| unsafe { libc::kill(-pgid, 0) == 0 })
                .count(),
            Err(_) => 0,
        };
        let active_solo = match self.solo_pids.lock() {
            Ok(solos) => solos
                .iter()
                // SAFETY: signal 0 is a sound existence probe.
                .filter(|&&pid| unsafe { libc::kill(pid, 0) == 0 })
                .count(),
            Err(_) => 0,
        };
        Ok(ProcessGroupStats {
            active_process_count: active + active_solo,
            total_cpu_time: None,
            peak_memory_bytes: None,
        })
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.broadcast(libc::SIGKILL);
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

/// `signal_groups`, but for the solo-adopted pids: probe each individually and
/// signal the live ones, pruning the dead.
///
/// The recycled-pid hazard here is *stronger* than for groups: a stale group id
/// only aliases if the recycled pid happens to become a group **leader**,
/// whereas a solo entry is a plain pid — any unrelated process that reuses it
/// (likelier on macOS's small pid space) would be probed "alive" and signalled.
/// The exposure is the gap between the child's exit-and-reap and our next
/// sweep; probe-then-signal keeps the in-sweep window a few instructions wide,
/// and a pruned pid is never re-added. Note: a solo pid stays tracked until
/// *reaped by its owner* (adopt borrows the child); an unreaped zombie probes
/// alive, exactly like a zombie group leader.
fn signal_pids(pids: &Mutex<Vec<i32>>, sig: i32) {
    if let Ok(mut p) = pids.lock() {
        p.retain(|&pid| {
            // SAFETY: signal 0 to a positive pid is a sound existence probe.
            if unsafe { libc::kill(pid, 0) } != 0 {
                return false; // ESRCH: gone — forget it.
            }
            // SAFETY: a plain signal to a probed-live pid; an exit between the
            // probe and here just yields ESRCH.
            unsafe { libc::kill(pid, sig) };
            true
        });
    }
}

/// Whether any solo-adopted pid is still alive.
fn pids_alive(pids: &Mutex<Vec<i32>>) -> bool {
    let Ok(p) = pids.lock() else {
        return false;
    };
    // SAFETY: signal 0 to a positive pid is a sound existence probe.
    p.iter().any(|&pid| unsafe { libc::kill(pid, 0) == 0 })
}

/// Drop solo-adopted pids that have already exited (and been reaped).
fn retain_live_pids(pids: &mut Vec<i32>) {
    // SAFETY: signal 0 to a positive pid is a sound existence probe.
    pids.retain(|&pid| unsafe { libc::kill(pid, 0) == 0 });
}
