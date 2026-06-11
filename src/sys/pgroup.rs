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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep};

#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;

/// How often the graceful path re-checks whether the tree has drained.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// One tracked id-set with its probe/signal primitives — either process
/// **groups** (each id is a leader child's pid, probed and signalled
/// negatively: `kill(-id, 0)` / `killpg`) or **solo** pids (adopted children
/// that could not be re-grouped, probed and signalled directly).
///
/// This is the single place the recycled-pid hazard is reasoned about. A
/// stale id whose process was reaped and whose pid got recycled could address
/// an unrelated process: for a group entry the alias additionally requires
/// the recycled pid to become a group *leader*, while a solo entry is a plain
/// pid — any reuse aliases it (likelier on macOS's small pid space). The
/// mitigations are uniform for both kinds:
///
/// - probe existence immediately before signalling, so the in-sweep window is
///   a few instructions wide;
/// - prune on `ESRCH` and never re-add a pruned id — an empty group can never
///   regain members (new members only fork from existing ones), so the probe
///   is terminal and a recyclable dead id is forgotten promptly;
/// - treat `EPERM` as **exists**: the process/group is alive but may not be
///   signalled (e.g. after a third-party uid change) — pruning it would
///   silently orphan a live tree, so it is kept and signalled best-effort.
///
/// A tracked id stays until its process is *reaped* — an unreaped zombie
/// probes alive (relevant for adopted children, which the caller reaps).
struct Tracked {
    ids: Mutex<Vec<i32>>,
    /// Probe/signal the whole process group (negative id) instead of one pid.
    group: bool,
}

impl Tracked {
    const fn new(group: bool) -> Self {
        Tracked {
            ids: Mutex::new(Vec::new()),
            group,
        }
    }

    /// Whether `id` still exists (see the type doc for the `EPERM` rule).
    fn exists(&self, id: i32) -> bool {
        let probe = if self.group { -id } else { id };
        // SAFETY: signal 0 is a sound existence probe (a negative target
        // probes the process group).
        if unsafe { libc::kill(probe, 0) } == 0 {
            return true;
        }
        let err = std::io::Error::last_os_error().raw_os_error();
        if err == Some(libc::EPERM) {
            return true;
        }
        // L6 — group-mode ESRCH on the negative group-id does not prove the
        // process is gone: a just-forked child may not have called setpgid(0,0)
        // yet (the between-fork-and-exec window, reachable on the `setsid` spawn
        // path). Fall back to a direct pid probe so we don't permanently prune a
        // still-live entry. `signal_all` mirrors this with a direct-pid *signal*
        // fallback, so an entry kept alive here is still delivered to and drains
        // — without that companion it would be probed-by-pid but signalled-by-
        // group (`killpg` → ESRCH), retained forever and stalling shutdown.
        //
        // TOCTOU note: the pid could be reaped and recycled between the two
        // probes (the same sub-µs window documented in the `Tracked` type doc
        // for all pid probes). The hazard is the same as the existing solo-pid
        // tracking and is accepted there; this adds no new risk surface.
        if self.group && err == Some(libc::ESRCH) {
            // SAFETY: probing pid directly; EPERM means alive-but-unsignallable.
            if unsafe { libc::kill(id, 0) } == 0 {
                return true;
            }
            return std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
        }
        false
    }

    /// Track `id`, pruning drained entries and de-duplicating (re-adopting a
    /// child this set already tracks must not make `members()`/`stats()`
    /// over-report).
    fn track(&self, id: i32) {
        if let Ok(mut ids) = self.ids.lock() {
            ids.retain(|&id| self.exists(id));
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }

    /// Send `sig` to every still-existing entry, pruning the drained ones.
    fn signal_all(&self, sig: i32) {
        if let Ok(mut ids) = self.ids.lock() {
            ids.retain(|&id| {
                if !self.exists(id) {
                    return false; // ESRCH: gone — forget it.
                }
                // SAFETY: killpg/kill to a probed-existing id; an exit between
                // the probe and here just yields ESRCH, and EPERM stays
                // best-effort — either way the sweep continues.
                unsafe {
                    if self.group {
                        // killpg reaches the leader and every descendant. But if
                        // the group doesn't exist yet — a child kept alive by the
                        // L6 direct-pid fallback in `exists` (forked but not yet
                        // `setpgid`'d), or a recycled pid — killpg yields ESRCH
                        // and reaches nothing. Fall back to a direct pid signal so
                        // the entry actually drains; otherwise it stays alive to
                        // `exists` (by pid) yet is never delivered to, pinning it
                        // in the set and stalling `graceful_shutdown` to its full
                        // timeout. The not-yet-`setpgid`'d child has no
                        // descendants, so a pid signal fully contains it; the
                        // recycled-pid case is the same sub-µs window the type doc
                        // already accepts for every probe/signal here.
                        if libc::killpg(id, sig) == -1
                            && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                        {
                            libc::kill(id, sig);
                        }
                    } else {
                        libc::kill(id, sig);
                    }
                }
                true
            });
        }
    }

    /// Whether any tracked entry still exists.
    fn any_alive(&self) -> bool {
        self.ids
            .lock()
            .map(|ids| ids.iter().any(|&id| self.exists(id)))
            .unwrap_or(false)
    }

    /// The still-existing entries, pruning the drained ones on the way.
    #[cfg(feature = "process-control")]
    fn live_snapshot(&self) -> Vec<i32> {
        match self.ids.lock() {
            Ok(mut ids) => {
                ids.retain(|&id| self.exists(id));
                ids.clone()
            }
            Err(_) => Vec::new(),
        }
    }

    /// How many tracked entries still exist (probe-only; no pruning — stats
    /// must not mutate tracking state).
    #[cfg(feature = "stats")]
    fn count_alive(&self) -> usize {
        self.ids
            .lock()
            .map(|ids| ids.iter().filter(|&&id| self.exists(id)).count())
            .unwrap_or(0)
    }
}

/// A set of process groups, one per spawned (or adopted) child.
///
/// Tracks the group ids (each == its leader child's pid) so teardown can signal
/// them. Its [`Drop`] hard-kills every still-live group, so an exiting or
/// panicking owner never leaks subprocesses.
pub(crate) struct ProcessGroup {
    /// Group ids we own. A group id is the leader child's pid.
    groups: Tracked,
    /// Adopted children that could not be re-grouped: POSIX forbids
    /// `setpgid` on a child that has already `exec`'d (`EACCES`) — the common
    /// case for [`adopt`](Self::adopt). These are tracked and signalled
    /// *individually*: the child itself is contained, but unlike a group
    /// leader, descendants it forks are not.
    solos: Tracked,
    /// B12: set by `graceful_shutdown(escalate=false)` to tell `Drop` not to
    /// hard-kill survivors (the caller deliberately chose not to escalate).
    skip_drop_kill: AtomicBool,
}

impl ProcessGroup {
    pub(crate) fn new() -> Self {
        ProcessGroup {
            groups: Tracked::new(true),
            solos: Tracked::new(false),
            skip_drop_kill: AtomicBool::new(false),
        }
    }

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        // Own process group per child → killpg reaps it and its descendants.
        // `process_group(0)` == setpgid(0, 0): the child becomes its own group
        // leader. EXCEPT when the command carries a `setsid()` pre-exec hook:
        // std applies setpgid *before* pre-exec hooks, and setsid fails EPERM
        // for a process that is already a group leader — so skip setpgid and
        // let setsid create the session + group (pgid == pid). The tracking
        // below is identical either way.
        if !opts.setsid {
            cmd.as_std_mut().process_group(0);
        }
        let child = cmd.spawn()?;
        if let Some(pid) = child.id() {
            self.groups.track(pid as i32);
        }
        Ok(child)
    }

    #[cfg(feature = "process-control")]
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
            // it and are reaped with it. (`track` de-duplicates an adopt of a
            // child this group itself spawned — setpgid is a no-op success
            // for an existing leader.)
            self.groups.track(pid);
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
                self.solos.track(pid);
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
    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: i32) -> io::Result<()> {
        self.broadcast(sig);
        Ok(())
    }

    /// Freeze every tracked group (`SIGSTOP` — unblockable, idempotent).
    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.broadcast(libc::SIGSTOP);
        Ok(())
    }

    /// Thaw every tracked group (`SIGCONT`).
    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.broadcast(libc::SIGCONT);
        Ok(())
    }

    /// One signal sweep over both tracking sets.
    fn broadcast(&self, sig: i32) {
        self.groups.signal_all(sig);
        self.solos.signal_all(sig);
    }

    /// Whether anything tracked is still alive.
    fn any_alive(&self) -> bool {
        self.groups.any_alive() || self.solos.any_alive()
    }

    /// The live tracked group **leaders** (one pid per spawned child) plus the
    /// solo-adopted pids — descendants inside the groups are not enumerated
    /// here. Dead entries are pruned on the way.
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> Vec<i32> {
        let mut members = self.groups.live_snapshot();
        members.extend_from_slice(&self.solos.live_snapshot());
        members
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        self.broadcast(signal);
        let deadline = Instant::now() + timeout;
        while self.any_alive() {
            if Instant::now() >= deadline {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
        if escalate && self.any_alive() {
            self.broadcast(libc::SIGKILL);
        } else if !escalate {
            // B12: tell Drop not to hard-kill the survivors the caller chose
            // to leave alive. Relaxed is sufficient: this store happens-before
            // Drop runs via the single-threaded call boundary.
            self.skip_drop_kill.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        // We track group ids (plus solo-adopted pids), not every individual
        // process, so report the number of live entries and leave cpu/memory
        // absent.
        Ok(ProcessGroupStats {
            active_process_count: self.groups.count_alive() + self.solos.count_alive(),
            total_cpu_time: None,
            peak_memory_bytes: None,
        })
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if !self.skip_drop_kill.load(Ordering::Relaxed) {
            self.broadcast(libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::process::Command;

    use super::*;

    /// B12: `graceful_shutdown(escalate=false)` must not kill survivors — neither
    /// during the call nor when the `ProcessGroup` itself drops.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn escalate_false_does_not_kill_survivors() {
        let pg = ProcessGroup::new();
        let opts = crate::sys::SpawnOptions::default();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("trap '' TERM; while :; do :; done");
        // Reap the child on any early panic path so the test never orphans it.
        cmd.kill_on_drop(true);
        let mut child = pg.spawn(&mut cmd, &opts).unwrap();
        let pid = child.id().unwrap() as i32;
        tokio::time::sleep(Duration::from_millis(50)).await;

        pg.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .unwrap();
        // Drop the group explicitly — this is where the bug fires.
        drop(pg);

        let alive = unsafe { libc::kill(pid, 0) } == 0;
        // Cleanup the orphaned child regardless.
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(alive, "child must survive when escalate_to_kill=false");
    }

    /// L6: a pid that exists as a process but not as a process-group leader must
    /// not be pruned from a group-mode `Tracked` set — ESRCH on the group probe
    /// does not mean the process is gone.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn esrch_on_group_probe_does_not_prune_a_live_pid() {
        let tracked = Tracked::new(true);

        // Spawn without `process_group(0)` so the child inherits the current
        // process group and is NOT its own leader — kill(-pid,0) is ESRCH.
        // `kill_on_drop` reaps it on any early panic path (e.g. the `pid_ok`
        // assert) so the test never orphans the `sleep 60`.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        // Verify precondition: group probe is ESRCH, pid probe is alive.
        let group_ok = unsafe { libc::kill(-pid, 0) } == 0;
        let pid_ok = unsafe { libc::kill(pid, 0) } == 0;
        if group_ok {
            // Pid happened to become a group leader (process_group set elsewhere).
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        }
        assert!(pid_ok, "spawned child must be alive");

        // The fixed `exists()` must return true — the pid is alive as a process.
        let exists = tracked.exists(pid);

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            exists,
            "a process that exists as a pid but not as a group leader \
             must be considered alive by exists()"
        );
    }
}
