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

#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;

/// One tracked id (a group leader pid or a solo pid) plus its liveness latch.
struct Entry {
    id: i32,
    /// Latched `true` once the group probe (`kill(-id, 0)`) has succeeded — the
    /// child has called `setpgid` and the fork→exec window is closed. After that,
    /// an `ESRCH` on the group probe means the group is *genuinely gone*, so the
    /// direct-pid fallback is disabled: a reaped-and-recycled pid is pruned (and
    /// never signalled) instead of being kept alive forever, which would let
    /// `Drop`/`kill_all` SIGKILL an unrelated process that recycled the pid.
    /// Unused for solo (non-group) sets, whose probe is always a direct pid.
    group_seen: bool,
}

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
///   is terminal and a recyclable dead id is forgotten promptly (and, once the
///   group has been seen alive, the [`group_seen`](Entry::group_seen) latch
///   disables the direct-pid fallback so a recycled pid is never revived);
/// - treat `EPERM` as **exists**: the process/group is alive but may not be
///   signalled (e.g. after a third-party uid change) — pruning it would
///   silently orphan a live tree, so it is kept and signalled best-effort.
///
/// A tracked id stays until its process is *reaped* — an unreaped zombie
/// probes alive (relevant for adopted children, which the caller reaps).
struct Tracked {
    ids: Mutex<Vec<Entry>>,
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

    /// Core liveness probe for `id` given the entry's latch state `group_seen`.
    /// Returns `(alive, group_seen_after)`. See [`Entry::group_seen`] and the
    /// type doc for the direct-pid fallback rule and why the latch disables it.
    fn probe_raw(&self, id: i32, group_seen: bool) -> (bool, bool) {
        let probe = if self.group { -id } else { id };
        // SAFETY: signal 0 is a sound existence probe (a negative target
        // probes the process group).
        if unsafe { libc::kill(probe, 0) } == 0 {
            // Alive. For a group, latch: the leader exists, so it has `setpgid`'d
            // and the fork→exec window is closed.
            return (true, group_seen || self.group);
        }
        let err = std::io::Error::last_os_error().raw_os_error();
        if err == Some(libc::EPERM) {
            // Alive but unsignallable — keep it (pruning would orphan a live tree).
            return (true, group_seen || self.group);
        }
        // Group-mode ESRCH on the negative group-id does not prove the process is
        // gone *while the group has never been seen alive*: a just-forked child
        // may not have called setpgid(0,0) yet (the between-fork-and-exec window,
        // reachable on the `setsid` spawn path), so fall back to a direct pid
        // probe rather than permanently prune a still-live entry. ONCE
        // `group_seen` has latched, the child long since `setpgid`'d, so an ESRCH
        // means the group genuinely drained — do NOT fall back, or a direct probe
        // would keep a reaped-and-recycled pid alive forever. `signal_all` mirrors
        // this latch-gated fallback.
        if self.group && !group_seen && err == Some(libc::ESRCH) {
            // SAFETY: probing pid directly; EPERM means alive-but-unsignallable.
            if unsafe { libc::kill(id, 0) } == 0 {
                return (true, false);
            }
            let alive = std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
            return (alive, false);
        }
        (false, group_seen)
    }

    /// Probe a stored entry, updating its [`group_seen`](Entry::group_seen) latch.
    fn probe_entry(&self, entry: &mut Entry) -> bool {
        let (alive, group_seen) = self.probe_raw(entry.id, entry.group_seen);
        entry.group_seen = group_seen;
        alive
    }

    /// Whether `id` is currently tracked (cheap membership check — no probe/prune).
    /// Only the `process-control`-gated `adopt` de-dup uses this.
    #[cfg(feature = "process-control")]
    fn contains(&self, id: i32) -> bool {
        self.ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .any(|e| e.id == id)
    }

    /// Track `id`, pruning drained entries and de-duplicating (re-adopting a
    /// child this set already tracks must not make `members()`/`stats()`
    /// over-report). `group_seen` seeds the latch: `true` when the group is
    /// already known to exist (a non-`setsid` spawn — `setpgid` ran before exec —
    /// or a successful `adopt` `setpgid`), `false` only on the `setsid` path where
    /// the group is created after fork (so the fallback window is still open).
    fn track(&self, id: i32, group_seen: bool) {
        // Recover a poisoned lock instead of dropping the child from tracking,
        // which would void the kill-on-drop guarantee.
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));
        if !ids.iter().any(|e| e.id == id) {
            ids.push(Entry { id, group_seen });
        }
    }

    /// Send `sig` to every still-existing entry, pruning the drained ones.
    /// Returns the last **delivery failure** (e.g. `EPERM` against a tree that
    /// changed uid), or `Ok(())` if every live entry was signalled. `ESRCH` — the
    /// process is gone — is not a failure: the entry drained. This lets the hard
    /// teardown (`kill_all`/`hard_kill`) report an *un*killed tree instead of a
    /// false success, matching the cgroup backend's `signal`.
    fn signal_all(&self, sig: i32) -> io::Result<()> {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        let mut last_err: Option<io::Error> = None;
        ids.retain_mut(|e| {
            if !self.probe_entry(e) {
                return false; // gone — forget it.
            }
            let id = e.id;
            // SAFETY: killpg/kill to a probed-existing id; an exit between the
            // probe and here just yields ESRCH and the sweep continues.
            unsafe {
                if self.group {
                    // killpg reaches the leader and every descendant. While the
                    // group has never been seen alive (a forked-but-not-yet-
                    // `setpgid`'d child), killpg yields ESRCH; fall back to a
                    // direct pid signal so the entry drains. ONCE `group_seen`
                    // latched (`probe_entry` set it above), an ESRCH means the
                    // group is genuinely gone — do NOT direct-signal, or that
                    // would SIGKILL a process that recycled the pid.
                    if libc::killpg(id, sig) == -1 {
                        let err = io::Error::last_os_error();
                        match err.raw_os_error() {
                            Some(libc::ESRCH) if !e.group_seen => {
                                if libc::kill(id, sig) == -1 {
                                    let e2 = io::Error::last_os_error();
                                    if e2.raw_os_error() != Some(libc::ESRCH) {
                                        last_err = Some(e2);
                                    }
                                }
                            }
                            // `group_seen` + ESRCH: genuinely gone — not a failure.
                            Some(libc::ESRCH) => {}
                            // EPERM etc.: alive but not signalled — a real failure.
                            _ => last_err = Some(err),
                        }
                    }
                } else if libc::kill(id, sig) == -1 {
                    let err = io::Error::last_os_error();
                    if err.raw_os_error() != Some(libc::ESRCH) {
                        last_err = Some(err);
                    }
                }
            }
            true
        });
        match last_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Whether any tracked entry still exists.
    fn any_alive(&self) -> bool {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.iter_mut().any(|e| self.probe_entry(e))
    }

    /// The still-existing entries, pruning the drained ones on the way.
    #[cfg(feature = "process-control")]
    fn live_snapshot(&self) -> Vec<i32> {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.retain_mut(|e| self.probe_entry(e));
        ids.iter().map(|e| e.id).collect()
    }

    /// How many tracked entries still exist (probe-only; no pruning — stats
    /// must not mutate the *set* of tracked ids, though it may refresh the
    /// `group_seen` latch, which is a benign monotonic cache).
    #[cfg(feature = "stats")]
    fn count_alive(&self) -> usize {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        let mut alive = 0;
        for e in ids.iter_mut() {
            if self.probe_entry(e) {
                alive += 1;
            }
        }
        alive
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
    /// Set by `graceful_shutdown(escalate=false)` to tell `Drop` not to
    /// hard-kill survivors (the caller deliberately chose not to escalate).
    skip_drop_kill: super::SkipDropKill,
}

impl ProcessGroup {
    pub(crate) fn new() -> Self {
        ProcessGroup {
            groups: Tracked::new(true),
            solos: Tracked::new(false),
            skip_drop_kill: super::SkipDropKill::new(),
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
        // Re-arm the kill-on-drop backstop now that a child has actually joined:
        // a prior graceful_shutdown(escalate=false) latched skip_drop_kill to
        // spare survivors; a fresh member must not be spared by that stale latch.
        // Done *after* the spawn so a failed spawn leaves the spared survivors
        // untouched (it added no member).
        self.skip_drop_kill.clear();
        if let Some(pid) = child.id() {
            // A non-`setsid` spawn is already its own group leader (`setpgid` ran
            // before exec), so seed the latch true. On the `setsid` path the group
            // is created after fork, so leave it false (the fallback window is open
            // until setsid runs).
            self.groups.track(pid as i32, !opts.setsid);
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
            // it and are reaped with it. The group exists (setpgid succeeded), so
            // seed the latch true. `track` de-duplicates a re-adopt.
            self.groups.track(pid, true);
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
                // A child THIS group already spawned is already tracked as a group
                // leader; its `setpgid` fails EACCES because it has exec'd. Don't
                // also solo-track it (that would double-count in `members()`/
                // `stats()` and double-deliver every broadcast) — only solo-track a
                // genuinely external child.
                if !self.groups.contains(pid) {
                    self.solos.track(pid, false);
                }
                Ok(())
            }
            _ => Err(err),
        }
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.broadcast(libc::SIGKILL)
    }

    /// Broadcast `sig` to every tracked process group and solo-adopted child.
    /// Surfaces a delivery failure (e.g. `EPERM` against a uid-changed member) so
    /// a "reload" signal that silently didn't land isn't reported as success;
    /// entries that already drained are skipped (and pruned), an empty set is a
    /// no-op.
    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: i32) -> io::Result<()> {
        self.broadcast(sig)
    }

    /// Freeze every tracked group (`SIGSTOP` — unblockable, idempotent).
    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.broadcast(libc::SIGSTOP)
    }

    /// Thaw every tracked group (`SIGCONT`).
    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.broadcast(libc::SIGCONT)
    }

    /// One signal sweep over both tracking sets. Runs both sweeps, then surfaces
    /// the first delivery failure (a swallowed `EPERM` on `SIGKILL` would be a
    /// silent orphaned tree reported as a clean kill).
    fn broadcast(&self, sig: i32) -> io::Result<()> {
        let groups = self.groups.signal_all(sig);
        let solos = self.solos.signal_all(sig);
        groups.and(solos)
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
        super::graceful::run(self, &self.skip_drop_kill, signal, timeout, escalate).await
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

impl super::graceful::GracefulTarget for ProcessGroup {
    fn signal_all(&self, signal: i32) {
        // The graceful tier is best-effort — the driver polls for drain
        // regardless, so a failed SIGTERM is swallowed here (the escalation's
        // `hard_kill` is where a delivery failure must surface).
        let _ = self.broadcast(signal);
    }

    fn is_drained(&self) -> bool {
        !self.any_alive()
    }

    fn hard_kill(&self) -> io::Result<()> {
        // Surface a `SIGKILL` delivery failure (e.g. `EPERM` against a tree that
        // changed uid via sudo/setuid): a swallowed failure here would report a
        // still-alive orphaned tree as a clean teardown.
        self.broadcast(libc::SIGKILL)
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if !self.skip_drop_kill.is_set() {
            // Drop can't surface an error; best-effort SIGKILL.
            let _ = self.broadcast(libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::process::Command;

    use super::*;

    /// `graceful_shutdown(escalate=false)` must not kill survivors — neither
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

    /// A pid that exists as a process but not as a process-group leader must
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

        // The probe (no latch → fallback applies) must return true — the pid is
        // alive as a process even though it is not a group leader.
        let exists = tracked.probe_raw(pid, false).0;

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            exists,
            "a process that exists as a pid but not as a group leader \
             must be considered alive (L6 fallback, pre-latch)"
        );
    }

    /// Once the group has been seen alive (the `group_seen` latch), the
    /// direct-pid fallback is disabled — a not-a-group-leader pid (standing in
    /// for a reaped-and-recycled pid) is treated as GONE, instead of being kept
    /// alive (and later signalled) forever, which would SIGKILL an innocent
    /// process that recycled the pid.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn group_seen_latch_disables_l6_fallback() {
        let tracked = Tracked::new(true);
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 60")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        // Skip if the pid happens to be a group leader (then kill(-pid,0) would
        // succeed and there is no fallback case to exercise).
        if unsafe { libc::kill(-pid, 0) } == 0 {
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        }

        // Before the group was seen, the fallback keeps a live-but-not-a-leader
        // pid alive (the fork→exec window semantics).
        assert!(
            tracked.probe_raw(pid, false).0,
            "pre-latch: L6 keeps a live pid"
        );
        // After the latch the same pid is GONE: the fallback is disabled, so a
        // recycled pid is pruned rather than kept and signalled.
        assert!(
            !tracked.probe_raw(pid, true).0,
            "post-latch: L6 disabled — a not-a-group-leader pid is treated as gone (B5)"
        );

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;
    }

    /// Adopting a child this group already spawned must not double-track it.
    /// The child has exec'd, so its `setpgid` fails `EACCES`; without the dedup it
    /// would land in `solos` while still in `groups`, double-counting in
    /// `members()`/`stats()` and double-delivering every broadcast.
    #[cfg(feature = "process-control")]
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn adopt_of_an_already_spawned_child_does_not_double_track() {
        let pg = ProcessGroup::new();
        let opts = crate::sys::SpawnOptions::default();
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 60");
        cmd.kill_on_drop(true);
        let mut child = pg.spawn(&mut cmd, &opts).unwrap();
        let pid = child.id().unwrap() as i32;

        // Re-adopt the same child: its `setpgid` fails EACCES (it has exec'd).
        pg.adopt(&child).unwrap();

        let members = pg.members();
        assert_eq!(
            members.iter().filter(|&&m| m == pid).count(),
            1,
            "an already-spawned child must be tracked once, not double-tracked"
        );

        drop(pg);
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;
    }
}
