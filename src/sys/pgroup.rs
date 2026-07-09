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
        // may not have called setpgid(0,0)/setsid yet (the between-fork-and-exec
        // window, reachable right after *any* spawn until the first successful
        // group probe — every spawn seeds the latch `false`), so fall back to a
        // direct pid probe rather than permanently prune a still-live entry. ONCE
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
    /// over-report). `group_seen` seeds the latch: `true` only when *this process
    /// itself* created the group synchronously — a successful `adopt`, whose
    /// `setpgid` the parent ran before this call. Every `spawn` seeds `false`: the
    /// child runs its own `setpgid`/`setsid` after fork, so the group is not proven
    /// to exist until the first successful probe latches it, and the direct-pid
    /// fallback must stay armed across the not-yet-`setpgid`'d window (for the
    /// non-`setsid` fork path too — see `ProcessGroup::spawn`).
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
    ///
    /// Best-effort: delivery failures are **not** surfaced. On the process-group
    /// mechanism's own platforms (macOS/BSD) `EPERM` is ambiguous — it is returned
    /// both for a genuinely-alive tree that changed uid (a `sudo`/setuid child, a
    /// real "couldn't kill" case) *and* for a `killpg` against a group whose only
    /// member is an unreaped **zombie** (dead, harmless). We can't tell them apart
    /// from the errno alone, so surfacing `EPERM` here would falsely fail a
    /// perfectly normal `kill_all`/`shutdown` of a group with unreaped children.
    /// The uid-changed limitation is documented on `ProcessGroup::kill_all`.
    fn signal_all(&self, sig: i32) {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
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
                    if libc::killpg(id, sig) == -1
                        && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                        && !e.group_seen
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
        // Guard the window between a live child and its registration in `groups`:
        // until `track` records it, nothing owns its teardown, so an early return
        // or panic here would leak a live self-grouped child — a silent
        // kill-on-drop violation. The guard hard-kills the not-yet-tracked child
        // on unwind and is disarmed once tracking succeeds; it is the pgroup
        // analogue of the Windows backend's `UncontainedChildGuard`. Today the
        // steps between spawn and `track` are infallible, but the guard keeps that
        // fragile invariant from silently regressing if a fallible step is ever
        // inserted here.
        let guard = UntrackedChildGuard::arm(cmd.spawn()?);
        if let Some(pid) = guard.child().id() {
            // Seed the liveness latch `false` on *every* spawn — the child runs
            // its own `setpgid(0, 0)` (or `setsid`) after fork, so the group is
            // not proven to exist until the first successful group probe latches
            // it. Seeding `true` for a non-`setsid` spawn would be safe only on
            // the posix_spawn fast path (setpgid applied atomically before the pid
            // is returned); with a `pre_exec` hook std falls back to
            // fork→setpgid→exec, and a group probe in the not-yet-`setpgid`'d
            // window would ESRCH and — with the latch wrongly seeded `true` —
            // wrongly prune (and never signal) the live child. `false` keeps the
            // direct-pid fallback armed until the group is first seen, matching
            // the `setsid` path. `adopt`, whose `setpgid` the parent itself runs
            // synchronously before tracking, still seeds `true`.
            self.groups.track(pid as i32, false);
        }
        // Re-arm the kill-on-drop backstop now that a child has actually joined
        // and been tracked: a prior graceful_shutdown(escalate=false) latched
        // skip_drop_kill to spare survivors; a fresh member must not be spared by
        // that stale latch. Done *after* tracking (and after spawn) so a failed
        // spawn — whose guard reaps the child, adding no member — leaves the
        // spared survivors untouched.
        self.skip_drop_kill.clear();
        Ok(guard.disarm())
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
            // A new killable member joined — re-arm Drop's backstop so a prior
            // graceful_shutdown(escalate=false) latch doesn't spare it.
            self.skip_drop_kill.clear();
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
                    // A new killable solo member joined — re-arm Drop's backstop.
                    self.skip_drop_kill.clear();
                    self.solos.track(pid, false);
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
        self.broadcast(signal);
    }

    fn is_drained(&self) -> bool {
        !self.any_alive()
    }

    fn hard_kill(&self) -> io::Result<()> {
        // Best-effort `SIGKILL` sweep. A delivery `EPERM` is not surfaced: on
        // macOS/BSD it is indistinguishable from a `killpg` against an unreaped
        // zombie, so surfacing it would falsely fail a normal shutdown (see
        // `Tracked::signal_all`). The uid-changed limitation is documented on
        // `ProcessGroup::kill_all`.
        self.broadcast(libc::SIGKILL);
        Ok(())
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if !self.skip_drop_kill.is_set() {
            self.broadcast(libc::SIGKILL);
        }
    }
}

/// Reaps a freshly-spawned, not-yet-tracked child if [`ProcessGroup::spawn`]
/// unwinds (an early `Err` or a panic) before the child is registered in
/// `groups`. Until `track` records it the child is owned by nothing that would
/// tear it down, so dropping it un-disarmed would leak a live self-grouped child
/// — a silent kill-on-drop violation. [`disarm`](Self::disarm) hands the child
/// back once it is tracked, after which `groups`/`Drop` own teardown.
///
/// The pgroup analogue of the Windows backend's `UncontainedChildGuard`: same
/// arm/disarm shape, but it hard-kills the child's process *group* (with a
/// direct-pid fallback for the not-yet-`setpgid`'d window) rather than the lone
/// process, so any descendant the child managed to fork in the window is reaped
/// too.
struct UntrackedChildGuard {
    /// `None` only after [`disarm`](Self::disarm) has taken the child.
    child: Option<Child>,
}

impl UntrackedChildGuard {
    fn arm(child: Child) -> Self {
        Self { child: Some(child) }
    }

    /// Borrow the guarded child (present from `arm` until `disarm`) to read its
    /// `id()` while the reaper is armed.
    fn child(&self) -> &Child {
        self.child
            .as_ref()
            .expect("the guarded child is present until disarm")
    }

    /// Tracking succeeded: stop guarding and return the child unharmed.
    fn disarm(mut self) -> Child {
        self.child
            .take()
            .expect("the guarded child is taken exactly once")
    }
}

impl Drop for UntrackedChildGuard {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return; // disarmed — the child is tracked, teardown is owned elsewhere.
        };
        if let Some(pid) = child.id() {
            let pid = pid as i32;
            // Best-effort hard kill of the still-untracked child. It is its own
            // group leader (or, on the `setsid` path, a session leader), so killpg
            // reaps it *and* any descendant it forked in this tiny window; if it
            // has not `setpgid`'d yet the group id does not exist (killpg → ESRCH),
            // so fall back to a direct pid kill. `pid` was just spawned — never a
            // recycled alias — so the kill is safe.
            // SAFETY: killpg/kill delivering SIGKILL to a freshly-spawned id.
            unsafe {
                if libc::killpg(pid, libc::SIGKILL) == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
        // Dropping the tokio `Child` hands the killed process to tokio's orphan
        // reaper, so it is waited (no zombie leak) without this guard blocking.
        drop(child);
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

    /// T-079 (pgroup re-arm race): a `spawn`/`adopt` that re-arms the backstop
    /// while a `graceful_shutdown(escalate=false)` is mid-poll must win — the
    /// shutdown's final (stale) `request` must not re-spare the fresh child.
    ///
    /// Deterministic on the paused clock (no real subprocess): a fake
    /// [`GracefulTarget`](crate::sys::graceful::GracefulTarget) re-arms the
    /// ProcessGroup's **own** latch during the drain wait, standing in for the
    /// concurrent spawn/adopt, and the real [`graceful::run`](crate::sys::graceful::run)
    /// driver — the exact call `ProcessGroup::graceful_shutdown` makes — is exercised
    /// against that latch. The final `is_set() == false` is the load-bearing
    /// outcome: `ProcessGroup::drop` then SIGKILLs the tracked groups rather than
    /// sparing the newcomer.
    #[tokio::test(start_paused = true)]
    async fn shutdown_request_does_not_override_a_concurrent_rearm() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct RacingRearm<'a> {
            latch: &'a crate::sys::SkipDropKill,
            polls: AtomicUsize,
        }
        impl crate::sys::graceful::GracefulTarget for RacingRearm<'_> {
            fn signal_all(&self, _signal: i32) {}
            fn is_drained(&self) -> bool {
                // Re-arm on the second poll (the concurrent spawn/adopt landing
                // mid-shutdown), then keep reporting "not drained" so the driver
                // runs to the deadline and issues its stale request.
                if self.polls.fetch_add(1, Ordering::Relaxed) == 1 {
                    self.latch.clear();
                }
                false
            }
            fn hard_kill(&self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let pg = ProcessGroup::new();
        // A live reused group: its backstop is already armed by an earlier spawn.
        pg.skip_drop_kill.clear();
        let target = RacingRearm {
            latch: &pg.skip_drop_kill,
            polls: AtomicUsize::new(0),
        };
        crate::sys::graceful::run(
            &target,
            &pg.skip_drop_kill,
            libc::SIGTERM,
            Duration::from_millis(100),
            false,
        )
        .await
        .expect("graceful run");
        assert!(
            !pg.skip_drop_kill.is_set(),
            "a child spawned/adopted mid-shutdown must keep the group's Drop-kill \
             backstop — the stale request must not re-spare it"
        );
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

    /// Every `spawn` seeds the group-liveness latch `false` — on the non-`setsid`
    /// path too (it used to seed `true`). The child runs its own
    /// `setpgid`/`setsid` after fork, so the group is not proven to exist until
    /// the first successful probe; seeding `false` keeps the direct-pid fallback
    /// armed across the not-yet-`setpgid`'d window for BOTH paths, so a fast
    /// probe/sweep right after spawn (before the child is scheduled) never wrongly
    /// prunes the still-live child.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn spawn_seeds_group_seen_false_on_both_paths() {
        for setsid in [false, true] {
            let pg = ProcessGroup::new();
            let opts = crate::sys::SpawnOptions {
                setsid,
                ..Default::default()
            };
            let mut cmd = Command::new("sh");
            cmd.arg("-c").arg("sleep 60");
            cmd.kill_on_drop(true);
            let mut child = pg.spawn(&mut cmd, &opts).unwrap();
            let pid = child.id().unwrap() as i32;

            // Inspect the freshly-pushed entry *before* any probe runs on it:
            // `track` pushes without probing the new id, so the seeded latch is
            // observable directly.
            let seeded_false = {
                let ids = pg.groups.ids.lock().unwrap_or_else(|e| e.into_inner());
                ids.iter()
                    .find(|e| e.id == pid)
                    .map(|e| !e.group_seen)
                    .unwrap_or(false)
            };

            drop(pg);
            let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;

            assert!(
                seeded_false,
                "spawn (setsid={setsid}) must seed group_seen=false so the fallback \
                 window stays open until the first successful group probe",
            );
        }
    }

    /// A freshly-spawned child seeded `group_seen = false` that is a live process
    /// but not yet its own group leader (the not-yet-`setpgid`'d window) must be
    /// KEPT and SIGNALLED by a teardown sweep — via the direct-pid fallback — not
    /// pruned as a drained group and silently left unsignalled. Seeding the latch
    /// `false` on every spawn is what keeps this window covered for the
    /// non-`setsid` fork path too.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn signal_all_keeps_and_signals_a_not_yet_grouped_child() {
        let tracked = Tracked::new(true);
        // Spawn WITHOUT process_group(0): the child inherits the parent's group
        // and is not its own leader, so kill(-pid, 0) is ESRCH — the exact shape
        // of the not-yet-`setpgid`'d window a spawn seeds `group_seen=false` to
        // survive. The child traps TERM, so a delivered SIGTERM does NOT kill it:
        // we assert it stayed alive (signalled, kept), then SIGKILL it.
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("trap '' TERM; while :; do :; done")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;

        // Skip if the pid happens to already be a group leader — then kill(-pid,0)
        // succeeds and there is no not-yet-grouped window to exercise.
        if unsafe { libc::kill(-pid, 0) } == 0 {
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let _ = child.wait().await;
            return;
        }

        // Seed exactly as a spawn does now: a group entry with the latch `false`.
        tracked.track(pid, false);
        // A teardown sweep must keep and signal it via the direct-pid fallback.
        tracked.signal_all(libc::SIGTERM);

        let still_tracked = {
            let ids = tracked.ids.lock().unwrap_or_else(|e| e.into_inner());
            ids.iter().any(|e| e.id == pid)
        };
        let alive = unsafe { libc::kill(pid, 0) } == 0;

        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = child.wait().await;

        assert!(
            still_tracked,
            "a not-yet-grouped live child must not be pruned by a teardown sweep"
        );
        assert!(
            alive,
            "the child must survive a trapped SIGTERM — it was signalled, not lost"
        );
    }

    /// An armed [`UntrackedChildGuard`] dropped without `disarm` (the spawn→track
    /// unwind path) must hard-kill the still-untracked child, so a panic or early
    /// error there never leaks a live self-grouped child.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn untracked_guard_reaps_the_child_on_an_armed_drop() {
        use std::os::unix::process::CommandExt as _;

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 60");
        // Own group leader, so the guard's killpg reaches it (the primary path,
        // not just the ESRCH direct-pid fallback).
        cmd.as_std_mut().process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "the child is alive right after spawn"
        );

        drop(UntrackedChildGuard::arm(child)); // armed → reaps on drop

        // A zombie still probes alive via kill(pid,0), so death is only observable
        // once the exited child is *waited*: reap it with a WNOHANG loop,
        // cooperating with tokio's orphan reaper (an ECHILD means it already
        // waited the child — also dead).
        let mut dead = false;
        for _ in 0..200 {
            // SAFETY: waitpid on a pid we spawned; a null status pointer is valid
            // (we don't inspect the exit status) and WNOHANG never blocks.
            let r = unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
            if r == pid
                || (r == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD))
            {
                dead = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Cleanup regardless of the outcome.
        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };

        assert!(
            dead,
            "an armed guard drop must terminate the untracked child"
        );
    }

    /// `disarm` hands back the same child, still running, for `groups` to own —
    /// the guard must not kill a tracked (disarmed) child.
    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn untracked_guard_disarm_hands_back_a_live_child() {
        use std::os::unix::process::CommandExt as _;

        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 60");
        cmd.as_std_mut().process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = child.id().unwrap() as i32;

        let mut kept = UntrackedChildGuard::arm(child).disarm();
        assert!(
            unsafe { libc::kill(pid, 0) } == 0,
            "disarm must leave the child running"
        );

        // Clean up the child the guard handed back.
        let _ = unsafe { libc::killpg(pid, libc::SIGKILL) };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = kept.wait().await;
    }
}
