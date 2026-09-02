//! Linux implementation: a [cgroup v2] killed via `cgroup.kill`, with a POSIX
//! process-group fallback when no writable cgroup is available (e.g. a CI runner
//! without cgroup delegation).
//!
//! Each spawn is routed into a **per-spawn leaf sub-cgroup** of that cgroup where
//! the host allows one, which is what makes a selective "kill exactly this spawn's
//! tree" possible at all (see [`Leaves`]); the job's own cgroup remains the
//! whole-job handle, since `cgroup.kill`/`cgroup.freeze` written there act on it
//! *and* every descendant.
//!
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::Mechanism;
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::{CappedAxes, LimitEvidence, LimitKind, LimitVerdict, ResourceLimits};
#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
use crate::sys::pgroup::ProcessGroup;
#[cfg(feature = "stats")]
use crate::sys::{ProcIdentity, ProcMetrics};

/// Process-wide counter so concurrent jobs get distinct cgroup names.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// The initial interval at which the process-wide cgroup reclaimer retries a
/// directory that the kernel still reports as busy. A single manager keeps a
/// spared job from retaining one thread of its own, while still retrying until
/// the last survivor has left its cgroup.
const CGROUP_RECLAIM_POLL: Duration = Duration::from_millis(10);
/// A survivor can remain in a cgroup for an arbitrarily long time. Cap the
/// retry delay in seconds so eventual cleanup stays bounded without keeping a
/// reclaimer thread busy at millisecond cadence for the whole survivor lifetime.
const CGROUP_RECLAIM_MAX_POLL: Duration = Duration::from_secs(1);

/// A cgroup tree handed to the reclaimer after `Job::drop` could not remove it
/// synchronously. The paths are deliberately owned by this request: once the
/// `Job` is gone there is no registry left to consult, and forgetting a leaf
/// would make the parent impossible to remove and would hide a still-contained
/// survivor from later whole-tree operations before the handoff.
struct CgroupReclaim {
    parent: PathBuf,
    leaves: Vec<PathBuf>,
    attempts: u64,
}

/// The process-wide handoff state keeps a request alive until the manager has
/// accepted it. In particular, a failed thread start or a broken channel must
/// not turn a still-contained survivor into an untracked cgroup tree.
struct CgroupReclaimerState {
    sender: Option<Sender<CgroupReclaim>>,
    pending: Vec<CgroupReclaim>,
}

static CGROUP_RECLAIMER: OnceLock<Mutex<CgroupReclaimerState>> = OnceLock::new();
#[cfg(test)]
static CGROUP_RECLAIMER_TEST_LOCK: Mutex<()> = Mutex::new(());

fn cgroup_reclaimer_state() -> &'static Mutex<CgroupReclaimerState> {
    CGROUP_RECLAIMER.get_or_init(|| {
        Mutex::new(CgroupReclaimerState {
            sender: None,
            pending: Vec::new(),
        })
    })
}

fn lock_cgroup_reclaimer(
    reclaimer: &Mutex<CgroupReclaimerState>,
) -> std::sync::MutexGuard<'_, CgroupReclaimerState> {
    // `Job::drop` cannot propagate poison; the inner queue still owns cleanup.
    reclaimer
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CgroupReclaimBackoff {
    delay: Duration,
}

impl CgroupReclaimBackoff {
    fn new() -> Self {
        Self {
            delay: CGROUP_RECLAIM_POLL,
        }
    }

    fn delay(self) -> Duration {
        self.delay
    }

    fn reset(&mut self) {
        self.delay = CGROUP_RECLAIM_POLL;
    }

    fn increase(&mut self) {
        self.delay = self
            .delay
            .checked_mul(2)
            .unwrap_or(CGROUP_RECLAIM_MAX_POLL)
            .min(CGROUP_RECLAIM_MAX_POLL);
    }
}

fn report_cgroup_reclaim_failure(scope: &'static str, kind: io::ErrorKind, attempt: u64) {
    // A busy cgroup is expected while a non-escalating survivor is alive, so do
    // not emit one warning per 10 ms poll. The first refusal and periodic
    // reminders make a permanently unreadable/undeletable hierarchy visible
    // without turning a long-lived survivor into a log flood.
    if attempt != 1 && !attempt.is_multiple_of(1_000) {
        return;
    }

    #[cfg(feature = "tracing")]
    {
        tracing::warn!(
            target: "processkit",
            operation = "cgroup_reclaim",
            scope,
            error_kind = ?kind,
            attempt,
            "cgroup cleanup is pending; the directory remains registered for retry"
        );
    }
    #[cfg(not(feature = "tracing"))]
    {
        // A library must not claim ownership of the host process's stderr. Keep
        // the arguments in the no-op branch so this function remains the single
        // report hook and diagnostics stay opt-in through `tracing`.
        let _ = (scope, kind);
    }
}

impl CgroupReclaim {
    /// Try one depth-first reclaim pass. A directory is removed from this
    /// request only after `rmdir` confirms success (or `NotFound` confirms that
    /// somebody else already removed it). Every other error keeps the path for
    /// the next pass, preserving the survivor's containment and enumeration.
    fn reclaim_once(&mut self) -> bool {
        self.reclaim_once_with(report_cgroup_reclaim_failure)
    }

    fn reclaim_once_with(
        &mut self,
        mut report: impl FnMut(&'static str, io::ErrorKind, u64),
    ) -> bool {
        self.attempts = self.attempts.saturating_add(1);
        let attempt = self.attempts;
        let mut pending_leaf = false;
        self.leaves.retain(|leaf| match std::fs::remove_dir(leaf) {
            Ok(()) => false,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => {
                pending_leaf = true;
                report("leaf", error.kind(), attempt);
                true
            }
        });

        // A parent with any registered leaf still present cannot be removed;
        // avoiding that call also keeps an expected `ENOTEMPTY` from obscuring
        // the leaf error that is actually blocking progress. If an unknown
        // child exists, the parent rmdir below remains the safe retry guard.
        if pending_leaf || !self.leaves.is_empty() {
            return false;
        }

        match std::fs::remove_dir(&self.parent) {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(error) => {
                report("parent", error.kind(), attempt);
                false
            }
        }
    }
}

impl CgroupReclaimerState {
    /// Send every queued request, retaining the request that a broken receiver
    /// returns (and all requests after it) for a fresh manager attempt.
    fn send_pending(&mut self, sender: &Sender<CgroupReclaim>) -> io::Result<()> {
        let pending = std::mem::take(&mut self.pending);
        let mut pending = pending.into_iter();
        while let Some(request) = pending.next() {
            match sender.send(request) {
                Ok(()) => {}
                Err(mpsc::SendError(request)) => {
                    self.pending.push(request);
                    self.pending.extend(pending);
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "cgroup reclaimer channel disconnected",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Start (or reuse) the process-wide manager and hand it every queued request.
/// A failed start leaves the queue untouched; a later cgroup drop gets another
/// chance to start the manager instead of inheriting a permanently cached error.
fn start_cgroup_reclaimer(state: &mut CgroupReclaimerState) -> io::Result<()> {
    if state.sender.is_none() {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("processkit-cgroup-reclaimer".into())
            .spawn(move || cgroup_reclaim_loop(receiver))
            .map_err(|source| {
                io::Error::other(format!("could not start cgroup reclaimer: {source}"))
            })?;
        state.sender = Some(sender);
    }

    let sender = state
        .sender
        .as_ref()
        .expect("cgroup reclaimer sender installed")
        .clone();
    if let Err(error) = state.send_pending(&sender) {
        // Let the old manager observe the disconnected channel and finish any
        // request it already owns; a fresh manager gets the retained queue.
        state.sender = None;
        return Err(error);
    }
    Ok(())
}

fn accept_cgroup_reclaim(
    pending: &mut Vec<CgroupReclaim>,
    backoff: &mut CgroupReclaimBackoff,
    request: CgroupReclaim,
) {
    pending.push(request);
    backoff.reset();
}

fn cgroup_reclaim_loop(receiver: Receiver<CgroupReclaim>) {
    let mut pending = Vec::new();
    let mut backoff = CgroupReclaimBackoff::new();
    loop {
        while let Ok(request) = receiver.try_recv() {
            accept_cgroup_reclaim(&mut pending, &mut backoff, request);
        }

        let mut index = 0;
        while index < pending.len() {
            if pending[index].reclaim_once() {
                pending.swap_remove(index);
            } else {
                index += 1;
            }
        }

        if pending.is_empty() {
            match receiver.recv() {
                Ok(request) => accept_cgroup_reclaim(&mut pending, &mut backoff, request),
                Err(_) => return,
            }
        } else {
            match receiver.recv_timeout(backoff.delay()) {
                Ok(request) => accept_cgroup_reclaim(&mut pending, &mut backoff, request),
                Err(mpsc::RecvTimeoutError::Timeout) => backoff.increase(),
                // The sender is process-global and should not disconnect, but
                // retaining pending requests is safer than abandoning paths if
                // that invariant ever changes.
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    std::thread::sleep(backoff.delay());
                    backoff.increase();
                }
            }
        }
    }
}

/// Transfer the paths that a dropped cgroup still owns to the process-wide
/// retry manager. `Drop` cannot report an error, so a manager-start/send failure
/// is recorded in the durable queue and made observable while the paths remain
/// untouched for a later retry.
fn enqueue_cgroup_reclaim(parent: PathBuf, leaves: Vec<PathBuf>) {
    #[cfg(test)]
    // Deliberate unwind regressions may poison their serialization gate too.
    let _test_guard = CGROUP_RECLAIMER_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    enqueue_cgroup_reclaim_with_state(cgroup_reclaimer_state(), parent, leaves);
}

fn enqueue_cgroup_reclaim_with_state(
    reclaimer: &Mutex<CgroupReclaimerState>,
    parent: PathBuf,
    leaves: Vec<PathBuf>,
) {
    let request = CgroupReclaim {
        parent,
        leaves,
        attempts: 0,
    };

    let start_error = {
        let mut state = lock_cgroup_reclaimer(reclaimer);
        state.pending.push(request);

        // A transient thread-resource failure must not be cached forever. One
        // enqueue performs one start attempt; subsequent drops retry without ever
        // discarding the requests already in `pending`.
        start_cgroup_reclaimer(&mut state).err()
    };

    // An embedding tracing subscriber may panic; never let it poison handoff state.
    if let Some(error) = start_error {
        report_cgroup_reclaim_failure("handoff", error.kind(), 1);
    }
}

/// A per-process salt mixed into the cgroup dir name so a pid recycled long after
/// a *crashed* ProcessKit process (whose `Drop` never cleaned up its
/// `processkit-<pid>-…` dirs) does not collide with those leftovers and silently
/// downgrade to the process-group fallback. Derived from the wall-clock time of
/// its first use (effectively per-process, computed once via `OnceLock`);
/// concurrent jobs / two crate versions in one process share the salt but differ
/// by the monotonic counter.
///
/// Leftover dirs from a *hard-killed* ProcessKit process accumulate (its `Drop`
/// never ran). A `SIGKILL` of the host is the one case the kill-on-drop guarantee
/// cannot cover, and a cgroup — unlike a Windows Job Object — is **not** torn down
/// by the kernel when its creator dies, so such a leftover dir may still contain a
/// live, orphaned tree (only the opt-in `kill_on_parent_death` /
/// `PR_SET_PDEATHSIG` propagates host death, and only to the direct child). The
/// salt keeps these leftovers from ever affecting a *future* run. A startup sweep
/// is deliberately NOT done: it would have to scan the delegated hierarchy and
/// could race another live ProcessKit instance's dirs. Operators who churn through
/// many crashes can reclaim stale `processkit-*` dirs out of band — depth-first,
/// since such a directory holds the per-spawn leaf sub-cgroups ([`Leaves`]) of the
/// spawns that were live when the host died, and a cgroup directory with children
/// cannot be `rmdir`ed before them.
fn cgroup_name_salt() -> u64 {
    static SALT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SALT.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    })
}

pub(crate) struct Job {
    backend: Backend,
    /// Set by `graceful_shutdown(escalate=false)` so `Drop` skips the hard kill
    /// when the caller chose not to escalate.
    skip_drop_kill: super::SkipDropKill,
}

enum Backend {
    /// All children live in this cgroup; killed via `cgroup.kill`.
    Cgroup(Cgroup),
    /// Fallback when no writable cgroup is available: the shared POSIX
    /// process-group backend (each child leads its own group). Its own `Drop`
    /// hard-kills the tracked groups.
    ProcessGroup(ProcessGroup),
}

/// Warn **once per process** that containment degraded from cgroup to the POSIX
/// process-group fallback (C4). A latch keeps a chatty spawner from flooding logs;
/// the per-spawn detail stays at `debug`. No-op without the `tracing` feature.
fn warn_containment_degraded_once() {
    #[cfg(feature = "tracing")]
    {
        use std::sync::Once;
        static WARNED: Once = Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                target: "processkit",
                "cgroup v2 unavailable — containment degraded to the POSIX \
                 process-group fallback; a child that calls setsid can escape \
                 teardown. Fires once per process (per-spawn detail is at debug)."
            );
        });
    }
}

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // Prefer a cgroup; degrade to a process group if we can't make one
        // (no cgroup v2, no delegation, read-only fs, …). The choice is
        // observable via `mechanism()` — never silent.
        let backend = match Cgroup::create(
            #[cfg(feature = "limits")]
            limits,
        ) {
            Ok(cg) => Backend::Cgroup(cg),
            // The error is only consulted with `limits` on, hence the `_e` binding.
            Err(_e) => {
                // The process-group fallback has no resource accounting, so it
                // cannot honor a requested limit. Fail fast rather than hand back
                // an unbounded tree the caller believes is capped.
                #[cfg(feature = "limits")]
                if limits.any() {
                    return Err(_e);
                }
                // C4: surface the containment *downgrade* once at warn level. A
                // cgroup→pgroup fallback (unprivileged container, read-only
                // `/sys/fs/cgroup`, no delegation) weakens teardown — a `setsid`
                // child then escapes it — and per-spawn `debug` traces plus
                // `mechanism()` polling don't make that visible to an operator who
                // only watches warn-level logs.
                warn_containment_degraded_once();
                Backend::ProcessGroup(ProcessGroup::new())
            }
        };
        Ok(Job {
            backend,
            skip_drop_kill: super::SkipDropKill::new(),
        })
    }

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        // The spare the re-arm below displaces interests only a launch that can
        // still be undone; a spawn that hands back a live child never puts it back.
        self.spawn_displacing_spare(cmd, opts)
            .map(|(child, _displaced)| child)
    }

    /// [`spawn`](Self::spawn), also handing back the [`DisplacedSpare`](super::DisplacedSpare)
    /// its kill-on-drop re-arm took away — the token
    /// [`rollback_pty_spawn`](Self::rollback_pty_spawn) needs if the PTY setup that
    /// follows this spawn fails. One body serves both, so the re-arm cannot drift
    /// between the plain and the undoable launch path; the cgroup arm re-arms this
    /// `Job`'s own latch, while the fallback arm's token comes from the
    /// `ProcessGroup`'s (each backend owns exactly the latch its `Drop` reads).
    pub(crate) fn spawn_displacing_spare(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<(Child, super::DisplacedSpare)> {
        // Arm the parent-death signal last, after containment hooks: pre-exec
        // hooks run in registration order, and a child that dies unprotected
        // inside its container beats one protected outside it. The spawner's
        // pid is captured HERE, pre-fork, so the child can detect a parent
        // that died before the prctl ran (see `arm_pdeathsig`).
        // SAFETY: see `arm_pdeathsig` — async-signal-safe calls only.
        //
        // NOTE: PR_SET_PDEATHSIG tracks the death of *this calling thread*,
        // not the process — see the caveat on `arm_pdeathsig`. `spawner_pid`
        // guards only against the parent process already being dead before
        // arming; it does not protect against this specific thread exiting
        // later while the process lives on.
        let arm = |cmd: &mut Command| {
            if opts.kill_on_parent_death {
                let spawner_pid = std::process::id();
                unsafe {
                    cmd.as_std_mut()
                        .pre_exec(move || arm_pdeathsig(spawner_pid));
                }
            }
        };
        match &self.backend {
            Backend::Cgroup(cg) => {
                // The cgroup path never touches process groups, so a setsid
                // pre-exec hook needs no coordination here.
                //
                // Reserve this spawn's own leaf sub-cgroup, so a later rollback can
                // kill exactly this tree (see `Leaves`). The slot removes the
                // directory again on every path out of here that never produces a
                // child, and hands back the job's own `cgroup.procs` on a host where
                // no leaf could be made — the child is contained either way.
                let leaf = cg.open_leaf();
                let procs =
                    CString::new(leaf.procs_path().into_os_string().into_vec()).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "cgroup path contains NUL")
                    })?;
                // Join the cgroup in the forked child *before* exec, so there is
                // no window in which the child (or its children) escape it. The
                // closure makes only async-signal-safe libc calls.
                // SAFETY: see `write_self_pid`.
                unsafe {
                    cmd.as_std_mut()
                        .pre_exec(move || write_self_pid(procs.as_c_str()));
                }
                arm(cmd);
                let child = cmd.spawn()?;
                // The launch produced a child, so the leaf is now this job's to
                // enumerate, kill and reclaim; keyed by the pid a rollback would
                // name it with.
                leaf.commit(child.id().map(|pid| pid as i32));
                // Re-arm the kill-on-drop backstop now a child has joined: a
                // prior graceful_shutdown(escalate=false) latched this flag to
                // spare survivors; a fresh member must not be spared by it. Done
                // after the spawn so a failed spawn leaves the survivors alone.
                // What the re-arm displaced travels back with the child, for the
                // one caller that may still have to undo this spawn.
                let displaced = self.skip_drop_kill.clear();
                Ok((child, displaced))
            }
            Backend::ProcessGroup(pg) => {
                arm(cmd);
                // `pg.spawn_displacing_spare` re-arms the ProcessGroup's own latch
                // on success, and reports what that re-arm displaced there.
                pg.spawn_displacing_spare(cmd, opts)
            }
        }
    }

    /// Spawn `cmd` under a pseudo-terminal, reusing this backend's normal
    /// cgroup / process-group containment for the actual spawn (K-032). `env` is
    /// unused on Unix — the pty child keeps the tokio `Command`'s env.
    #[cfg(feature = "pty")]
    pub(crate) fn spawn_pty(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
        _env: Option<Vec<(std::ffi::OsString, std::ffi::OsString)>>,
    ) -> io::Result<crate::sys::pty::PtySpawn> {
        // Carries the spare the spawn's kill-on-drop re-arm displaced over to the
        // rollback. Both closures run inside this one call, on this thread, and the
        // rollback only ever runs after the spawn returned — so a `Cell` is the
        // whole hand-off, and an untouched one ("nothing to restore") is exactly
        // right when the spawn never ran.
        let displaced = std::cell::Cell::new(super::DisplacedSpare::default());
        crate::sys::pty::spawn_pty(
            cmd,
            opts,
            |c, o| {
                let (child, spare) = self.spawn_displacing_spare(c, o)?;
                displaced.set(spare);
                Ok(child)
            },
            |pid| self.rollback_pty_spawn(pid, displaced.take()),
        )
    }

    /// Undo a PTY spawn whose master setup failed, **killing before dropping any
    /// bookkeeping** — the contract the shared `sys::pty::spawn_pty` rollback guard
    /// states in full.
    ///
    /// The process-group fallback delegates to the shared backend's own
    /// kill-then-forget. The cgroup backend aims at **this spawn's own leaf
    /// sub-cgroup** (see [`Leaves`]): `cgroup.kill` written there SIGKILLs that
    /// leaf's whole subtree atomically — the direct child, whatever it forked in
    /// the setup window, and a descendant that `setsid`'d away too, since cgroup
    /// membership is inherited across `fork` and `setsid` does not change it —
    /// while the rest of the job, living in its own leaves, is untouched. That is
    /// the selective subtree kill cgroup v2 has no other way to express: written in
    /// the job's own cgroup the same file kills **every** member, i.e. unrelated
    /// runs of the same `ProcessGroup`, which a single failed spawn must not do.
    ///
    /// It is aimed by the pid this spawn returned, which the caller still owns
    /// un-reaped, so no number recycled since can steer it: registering a leaf
    /// retires any older one still claiming the same pid, this call retires the pid
    /// it consumes, and a leaf leaves the registry only once the kernel has
    /// confirmed its directory is gone (see [`Cgroup::kill_leaf_of`]).
    ///
    /// **The two conditions that gate that kill** — neither of which gates the
    /// job's own whole-tree teardown:
    ///
    /// - this spawn has **a leaf at all**: a host that refused the `mkdir`, or a
    ///   fresh directory the child could not have joined, makes the spawn join the
    ///   job's own cgroup instead ([`Cgroup::open_leaf`]);
    /// - the `cgroup.kill` write is **accepted**: a kernel < 5.14 has no such file,
    ///   and a restricted delegated cgroup may refuse the write.
    ///
    /// Failing either, this falls back to what this arm did before leaves existed:
    /// `killpg` over the pty child's session (it is a session leader, pgid == pid)
    /// with the usual direct-pid fallback, reaching this spawn's descendants without
    /// touching the rest of the job. Honest about what *that* leaves: a descendant
    /// that forked and called `setsid` inside the setup window is out of `killpg`'s
    /// reach, but it has **not** left the cgroup, so it stays a member and is still
    /// reported by `members()`. Nothing on this failure path moves it out of the
    /// cgroup tree this job holds — a leaf is a *descendant* of the job's cgroup, so
    /// a whole-job `cgroup.kill` still covers it — and every kill this job *does*
    /// perform therefore reaches it. But the two entry points do not perform the
    /// same kills: `kill_all` performs one whenever it is called, for as long as the
    /// job is alive, while `Drop` performs one only while the kill-on-drop backstop
    /// is armed — and the restore below is entitled to leave it disarmed.
    ///
    /// Either way the kill-on-drop backstop comes last: `displaced` is the spare
    /// this spawn's own re-arm took away, and putting it back leaves the latch as a
    /// `graceful_shutdown(escalate = false)` had left it, so a launch that failed
    /// after its child existed does not hand `Drop` a licence to kill survivors the
    /// caller deliberately spared. It takes only while no other `spawn`/`adopt` has
    /// re-armed the backstop since (see
    /// [`SkipDropKill::restore`](super::SkipDropKill::restore)); each arm restores
    /// on the latch its own `Drop` reads.
    ///
    /// A restore that takes is what narrows the **fallback** paragraph above, and it
    /// narrows it for the whole cgroup tree: as long as that spare stands (no later
    /// `spawn`/`adopt` re-arms the backstop), `Drop` runs no `cgroup.kill` at all —
    /// so a `setsid` escapee the fallback could not reach is not killed there
    /// either. It is left running inside the cgroup dirs `Drop` then leaves behind
    /// (that `rmdir` fails `EBUSY` while members remain), on the same terms as the
    /// survivors the caller chose not to escalate against, and `kill_all` is what
    /// still kills it while the job lives. Where the leaf kill *did* run, this
    /// spawn's whole subtree was SIGKILLed before the restore, so the spare is not
    /// what leaves any of it running. Re-arming the backstop here to catch what the
    /// fallback missed would revoke the `escalate = false` decision for every other
    /// member too — a failed spawn overturning a shutdown call that was not its to
    /// make, which is the trade this rollback declines.
    #[cfg(feature = "pty")]
    pub(crate) fn rollback_pty_spawn(&self, pid: u32, displaced: super::DisplacedSpare) {
        match &self.backend {
            Backend::Cgroup(cg) => {
                if !cg.kill_leaf_of(pid as i32) {
                    crate::sys::pgroup::hard_kill_fresh_spawn(pid as i32);
                }
                self.skip_drop_kill.restore(displaced);
            }
            Backend::ProcessGroup(pg) => pg.rollback_pty_spawn(pid, displaced),
        }
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        let pid = child
            .id()
            .ok_or_else(|| io::Error::other("child has no pid (already exited?)"))?
            as i32;
        match &self.backend {
            Backend::Cgroup(cg) => {
                // Moving a pid into the cgroup is a single write to cgroup.procs;
                // the kernel re-parents that process (its existing descendants are
                // not retroactively pulled in — only future forks).
                //
                // Into the job's own cgroup, not a per-spawn leaf of its own: a leaf
                // buys selectivity for an undo that KILLS, and nothing ever aims a
                // kill at one adopted child alone. (`adopt_external` does have an
                // undo, but it moves the number back out rather than killing it, and
                // that works from the job's own cgroup just as well.) The job cgroup
                // may hold these members and the leaf directories at once because
                // this backend never enables controllers in its **own**
                // `cgroup.subtree_control` (see `Cgroup::create`), which is the only
                // thing cgroup v2's "no internal processes" rule forbids combining
                // with child cgroups.
                match cgroup_write(&cg.path.join("cgroup.procs"), pid.to_string().as_bytes()) {
                    Ok(()) => {
                        // A new killable member joined the cgroup — re-arm Drop's
                        // backstop so a prior graceful_shutdown(escalate=false)
                        // latch doesn't spare it.
                        self.skip_drop_kill.clear();
                        Ok(())
                    }
                    // The child already exited (a zombie pid) — the write fails
                    // ESRCH. Nothing to contain, so return Ok, matching the
                    // process-group backend (which maps ESRCH→Ok).
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            // `pg.adopt` re-arms the ProcessGroup's own latch on success.
            Backend::ProcessGroup(pg) => pg.adopt(child),
        }
    }

    /// Adopt an **external** process named only by `pid` — the Linux backend of
    /// [`ProcessGroup::adopt_external`](crate::ProcessGroup::adopt_external).
    ///
    /// The cgroup arm is the same single `cgroup.procs` write [`adopt`](Self::adopt)
    /// makes, with the identity work a bare number needs around it:
    ///
    /// 1. **Anchor first** ([`capture_adoption_anchor`]): one `/proc/<pid>/stat`
    ///    read yields the process's `starttime` token and, in the same read, proves
    ///    the process exists — an `ENOENT` is the honest "no such pid" and any other
    ///    read failure (a `hidepid` mount) is surfaced, never mistaken for "dead".
    /// 2. **The migration.** The kernel re-parents *that task* into this job's
    ///    cgroup; its existing descendants are not pulled in, only future forks.
    ///    It also takes the task **out** of the cgroup it was in — v2 membership is
    ///    exclusive — so whatever teardown and limits that cgroup carried stop
    ///    applying to it; the kernel does not report what it left, and nothing here
    ///    can put it back. An `ESRCH` means the process was exiting or gone by the
    ///    time the write landed — nothing left to contain, so `Ok`, exactly as
    ///    [`adopt`](Self::adopt) answers for a zombie.
    /// 3. **Re-read the anchor, and undo the migration if it moved.** A `starttime`
    ///    that has positively changed means the number was recycled inside this
    ///    call's own window. Detecting that is not the same as undoing it here: the
    ///    write has already put whoever held the number into the cgroup this job
    ///    kills, so the call runs the best-effort undo
    ///    ([`Cgroup::evict_recycled`]) — which first establishes whether the number
    ///    is a member at all, since the recycle may equally have happened *after* a
    ///    correct migration — and reports what that undo left behind
    ///    ([`recycled_during_cgroup_adoption`]). The kill-on-drop backstop is not
    ///    re-armed on this path, for the plain reason that no member the caller
    ///    asked for joined; that is bookkeeping, not a mitigation — an un-latched
    ///    backstop (the normal state) kills the cgroup's members on `Drop` whether
    ///    or not this path touched it, which is exactly why the undo above has to
    ///    do the real work.
    ///
    /// After a successful write there is no number-keyed bookkeeping to poison:
    /// membership is the kernel's own, per task, and every later verb — `members`,
    /// the graceful tier, `cgroup.kill`, `Drop` — reads or acts on *that*. A number
    /// recycled later cannot appear in this cgroup unless the kernel put the task
    /// there, and the per-pid delivery path pins each member with a pidfd and
    /// reconfirms its membership before sending (see
    /// [`deliver_pinned`]).
    ///
    /// Into the job's own cgroup, not a per-spawn leaf, as [`adopt`](Self::adopt)
    /// also does. A leaf exists to make a *selective kill* expressible (a
    /// `cgroup.kill` aimed at one spawn's subtree), and the one undo this path can
    /// need is not a kill but a move back out — which works from the job's own
    /// cgroup exactly as it would from a leaf, and costs no directory per adoption.
    #[cfg(feature = "process-control")]
    pub(crate) fn adopt_external(&self, pid: u32) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => {
                let anchor = capture_adoption_anchor(pid)?;
                match cgroup_write(&cg.path.join("cgroup.procs"), pid.to_string().as_bytes()) {
                    Ok(()) => {
                        // The same "positive proof of a recycle" rule the pgroup
                        // backend gates every probe on, not a second comparison of
                        // this backend's own.
                        if crate::sys::pgroup::is_recycled(
                            Some(anchor),
                            crate::sys::procfs::read_starttime(pid),
                        ) {
                            // The write is already in the kernel, so this path owes
                            // the caller an attempt to take it back out — and an
                            // honest report of what that attempt achieved.
                            return Err(recycled_during_cgroup_adoption(
                                pid,
                                cg.evict_recycled(pid),
                            ));
                        }
                        // A new killable member joined the cgroup — re-arm Drop's
                        // backstop so a prior graceful_shutdown(escalate=false)
                        // latch doesn't spare it.
                        self.skip_drop_kill.clear();
                        Ok(())
                    }
                    // The process exited between the anchor read and the write (a
                    // zombie, or gone): nothing to contain, so `Ok` — the same
                    // answer `adopt` gives for an exited-but-unreaped child.
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            // `pg.adopt_external` captures its own anchor and re-arms the
            // ProcessGroup's own latch on success.
            Backend::ProcessGroup(pg) => pg.adopt_external(pid),
        }
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.kill(),
            Backend::ProcessGroup(pg) => pg.kill_all(),
        }
    }

    /// Replace the live limits on the already-created container (full replacement).
    ///
    /// The cgroup arm rewrites the `*.max` files in the existing cgroup dir. The
    /// process-group fallback has no whole-tree resource accounting, so a request
    /// carrying any cap is refused with `ErrorKind::Unsupported` — the same typed
    /// refusal creation gives (`Job::new` propagates the cgroup-create error, which
    /// is `Unsupported` when there is no cgroup mechanism, when `limits.any()`).
    /// An empty set (all `None`) is a trivially-satisfiable no-op there: the tree is
    /// already unbounded on the fallback, so "remove all limits" needs nothing done.
    #[cfg(feature = "limits")]
    pub(crate) fn update_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.update_limits(limits),
            Backend::ProcessGroup(_) => {
                if limits.any() {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "resource limits require a cgroup or Job Object; this group fell back to \
                         a POSIX process group, which has no whole-tree resource accounting",
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Post-run evidence for the caps this job carries.
    ///
    /// Only the cgroup backend has whole-tree resource accounting; the POSIX
    /// process-group fallback has none at all, so it reports an honest all-`Unknown`
    /// report rather than a "no". (That fallback can never carry a cap in the first
    /// place — `Job::new` fails fast when `limits.any()` and no cgroup could be
    /// created — so `Unknown` there means "this mechanism has no evidence apparatus",
    /// not "a cap may have fired unseen".)
    #[cfg(feature = "limits")]
    pub(crate) fn limit_evidence(&self, capped: CappedAxes) -> LimitEvidence {
        match &self.backend {
            Backend::Cgroup(cg) => cg.limit_evidence(capped),
            Backend::ProcessGroup(_) => LimitEvidence::unknown(),
        }
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        match &self.backend {
            // SIGKILL takes the atomic `cgroup.kill` path so `signal(Kill)` gives
            // the same whole-tree guarantee as `kill_all` — the per-pid loop
            // below could miss processes forked mid-broadcast.
            Backend::Cgroup(cg) if sig.raw() == libc::SIGKILL => cg.kill(),
            Backend::Cgroup(cg) => cg.signal(sig.raw()),
            Backend::ProcessGroup(pg) => pg.signal(sig.raw()),
        }
    }

    /// Both Linux backends deliver a soft `Int`/`Term` to the **whole tree**,
    /// matching `signal`'s reach: the cgroup backend writes the signal to every
    /// member of the cgroup, and the process-group fallback `killpg`s every tracked
    /// leader's group (see the pgroup backend). Neither has an opt-in subset or an
    /// `Unsupported` case — `signal(Int/Term)` never returns `Unsupported` here —
    /// so the scope is `WholeTree` for either backend.
    #[cfg(feature = "process-control")]
    pub(crate) fn soft_stop_scope(&self) -> crate::SoftStopScope {
        crate::SoftStopScope::WholeTree
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.freeze(true),
            Backend::ProcessGroup(pg) => pg.suspend(),
        }
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.freeze(false),
            Backend::ProcessGroup(pg) => pg.resume(),
        }
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        let pids = match &self.backend {
            // Whole tree: every pid in the job's own `cgroup.procs` and in each of
            // its per-spawn leaves' (`Leaves`).
            Backend::Cgroup(cg) => cg.members()?,
            // Fallback tracks group leaders only.
            Backend::ProcessGroup(pg) => pg.members(),
        };
        Ok(pids.into_iter().map(|pid| pid as u32).collect())
    }

    /// The same members as [`members`](Self::members), enriched from `/proc`.
    ///
    /// The cgroup arm reads the whole tree (`cgroup.procs`); the fallback arm the
    /// tracked group leaders. Either way each pid's ppid / `comm` / start time come
    /// from a single `/proc/<pid>/stat` read, and a pid gone before that read is
    /// skipped (never a fabricated record).
    #[cfg(feature = "process-control")]
    pub(crate) fn members_info(&self) -> io::Result<Vec<MemberInfo>> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.members_info(),
            // The pgroup enumeration is an in-memory tracked list — infallible.
            Backend::ProcessGroup(pg) => Ok(pg.members_info()),
        }
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<super::graceful::GracefulOutcome> {
        match &self.backend {
            // The cgroup signals/observes/kills the tree through the cgroup file
            // API; the shared driver owns the poll-and-escalate algorithm.
            Backend::Cgroup(cg) => {
                super::graceful::run(cg, &self.skip_drop_kill, signal, timeout, escalate).await
            }
            // The ProcessGroup backend carries its own `skip_drop_kill` flag;
            // `pg.graceful_shutdown` sets it when `escalate=false`. `Job::drop`
            // for the ProcessGroup arm does nothing — the pgroup's own `Drop`
            // fires when the `Backend` enum is dropped.
            Backend::ProcessGroup(pg) => pg.graceful_shutdown(signal, timeout, escalate).await,
        }
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.stats(),
            Backend::ProcessGroup(pg) => pg.stats(),
        }
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        match &self.backend {
            Backend::Cgroup(_) => Mechanism::CgroupV2,
            Backend::ProcessGroup(_) => Mechanism::ProcessGroup,
        }
    }
}

/// Identity + best-effort metadata for an **arbitrary** pid (not one tracked by a
/// group) — the Linux backend of the standalone [`process_info`](crate::process_info)
/// query. Reads the same single `/proc/<pid>/stat` line the group snapshot uses
/// (ppid = field 4, `comm` = field 2, start time = field 22), through the shared
/// `sys::procfs` parser, so it can't drift from the member-snapshot path.
///
/// `Ok(None)` when the pid is genuinely gone (`ENOENT`), `Err` when it can't be
/// looked at (a permission denial, e.g. a `hidepid` mount — never mistaken for
/// "dead"), `Ok(Some(_))` otherwise. `/proc/<pid>/stat` is world-readable for
/// other users' processes on a default mount, so a foreign process is reported,
/// not denied.
#[cfg(feature = "process-control")]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<MemberInfo>> {
    Ok(crate::sys::procfs::read_stat_meta_checked(pid)?
        .map(|m| MemberInfo::new(pid, m.ppid, m.comm, m.starttime)))
}

/// Capture the start-time identity anchor of the live process at `pid` for a
/// **bare-pid adoption** into a cgroup ([`Job::adopt_external`]), from a single
/// `/proc/<pid>/stat` read.
///
/// The same `starttime` token [`process_info`] reports and the pgroup backend
/// anchors its entries on, read through the same shared parser — but *required*
/// rather than best-effort, because a bare number with no `Child` behind it has
/// nothing else that could tell the named process apart from a later occupant of
/// the number. The read doubles as the existence check, and keeps
/// [`read_stat_meta_checked`](crate::sys::procfs::read_stat_meta_checked)'s
/// distinction intact: `ENOENT` is the honest "no such pid" (`NotFound`), any
/// other failure means the process may well exist but could not be looked at
/// (a `hidepid` mount) and is surfaced as itself, never as "dead".
#[cfg(feature = "process-control")]
fn capture_adoption_anchor(pid: u32) -> io::Result<u64> {
    match crate::sys::procfs::read_stat_meta_checked(pid)? {
        None => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no process with pid {pid} to adopt"),
        )),
        Some(meta) => meta.starttime.ok_or_else(|| {
            io::Error::other(format!(
                "cannot adopt pid {pid}: /proc/{pid}/stat yielded no start-time identity, and \
                 this group will not track an external process by number alone"
            ))
        }),
    }
}

/// What the best-effort undo of a recycled bare-pid adoption found and did — the
/// outcome of [`Cgroup::evict_recycled`], reported to the caller rather than
/// collapsed, because the three states differ in the one way a caller has to know
/// about: whether this group's teardown still reaches whoever holds the number.
#[cfg(feature = "process-control")]
enum RecycleUndo {
    /// The number is **not** a member of this job's cgroup — either the process the
    /// call migrated has since exited (the recycle happened after the write) or the
    /// number names nothing at all now. Nothing to undo, and nothing here for this
    /// group's teardown to reach.
    NotAMember,
    /// The number **was** a member and was moved back out, into the cgroup this
    /// job's own directory lives in. This group's teardown no longer reaches it.
    Evicted,
    /// The number could not be shown to be out of this job's cgroup: the membership
    /// read failed, or the move-out write was refused. Whoever holds the number may
    /// still be a member of this job, and this group's teardown would kill it.
    Stuck(io::Error),
}

/// The verdict a cgroup-arm bare-pid adoption reports when its closing identity
/// re-read proves the number was recycled while the call ran.
///
/// It states the **aftermath**, not a single sweeping claim, because this arm's
/// state after the error genuinely varies: the migration had already happened, so
/// what the caller is left with depends on whether it could be reversed
/// ([`Cgroup::evict_recycled`]). That is also where this arm parts company with the
/// process-group one, whose after-error state is uniform (an entry that its own
/// identity gate prunes without ever signalling it — see
/// `sys::pgroup::recycled_during_adoption`).
#[cfg(feature = "process-control")]
fn recycled_during_cgroup_adoption(pid: u32, undo: RecycleUndo) -> io::Error {
    let aftermath = match undo {
        RecycleUndo::NotAMember => "the number is not a member of this group's cgroup, so this \
                                    group's teardown will not reach whoever holds it now"
            .to_string(),
        RecycleUndo::Evicted => "the migration was undone — the number was moved back out of this \
                                 group's cgroup, into the cgroup this group's own directory lives \
                                 in — so this group's teardown will not reach it; the cgroup it \
                                 was in before this call is NOT restored, because cgroup v2 \
                                 membership is exclusive and the kernel does not report what a \
                                 task left behind"
            .to_string(),
        RecycleUndo::Stuck(e) => format!(
            "the number could NOT be moved back out of this group's cgroup ({e}), so whoever \
             holds it is a member of this group and this group's teardown — kill_all, shutdown, \
             Drop — will kill it"
        ),
    };
    io::Error::other(format!(
        "pid {pid} was recycled while it was being adopted: its start-time identity differs from \
         the one captured at the start of the call, so the process the caller named is not the \
         one this call acted on — {aftermath}"
    ))
}

/// Read `/proc/<pid>/stat`'s `starttime` (field 22) — the process's start-time
/// identity anchor. `starttime` is fixed at process creation and distinct for a pid
/// recycled by a later process, so it tells a reused number apart from the original.
/// Thin Linux-side alias for the shared parser (`crate::sys::procfs::read_starttime`)
/// so this metrics path and the pgroup liveness path (`sys/pgroup.rs::read_identity`)
/// stay bit-identical. `None` if the process is gone or the stat is unparsable.
#[cfg(feature = "stats")]
fn read_proc_starttime(pid: u32) -> Option<u64> {
    crate::sys::procfs::read_starttime(pid)
}

/// Capture the `/proc/<pid>/stat` starttime of the live process at `pid` as its
/// [`ProcIdentity`] token, or `None` if it is gone / unreadable.
#[cfg(feature = "stats")]
pub(crate) fn process_identity(pid: u32) -> Option<ProcIdentity> {
    read_proc_starttime(pid).map(ProcIdentity::from_raw)
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(pid: u32, expected: Option<ProcIdentity>) -> ProcMetrics {
    process_metrics_with_seams(
        pid,
        expected,
        |pid| std::fs::read_to_string(format!("/proc/{pid}/stat")).ok(),
        |pid| std::fs::read_to_string(format!("/proc/{pid}/status")).ok(),
    )
}

#[cfg(feature = "stats")]
fn process_metrics_with_seams(
    pid: u32,
    expected: Option<ProcIdentity>,
    mut read_stat: impl FnMut(u32) -> Option<String>,
    read_status: impl FnOnce(u32) -> Option<String>,
) -> ProcMetrics {
    let mut metrics = ProcMetrics::default();

    // CPU *and* the identity anchor both come from a *single* /proc/<pid>/stat read
    // — one read so the identity gate and the CPU sample describe the same instant
    // (a second read could straddle a pid recycle). Every field access goes through
    // the shared `sys::procfs` parser (skip past the comm's last ')', then
    // whitespace index 0 is field 3), so this parse cannot drift from the pgroup
    // liveness path in `sys/pgroup.rs::read_identity` that shares it.
    let stat = read_stat(pid);

    // Identity gate: compare the captured identity against this read's `starttime`
    // (field 22) via the shared parser. If the caller captured an identity and this
    // read's starttime differs — or the stat could not be read/parsed at all — the
    // pid names a *different* process (recycled) or is gone: return the all-`None`
    // default and do NOT fall through to the memory read, which would otherwise fold
    // a stranger's RSS. Without a demanded identity (`None`), every read is
    // best-effort as before, with no weakening.
    if let Some(expected) = expected {
        let current = stat
            .as_deref()
            .and_then(crate::sys::procfs::starttime_from_stat);
        if current != Some(expected.raw()) {
            return ProcMetrics::default();
        }
    }

    // The whitespace fields after the comm feed the CPU sample below; the shared
    // `after_comm` cut is the same one the identity gate used above.
    let fields: Option<Vec<&str>> = stat
        .as_deref()
        .and_then(crate::sys::procfs::after_comm)
        .map(|after| after.split_whitespace().collect());

    if let Some(fields) = &fields {
        // After ')', index 0 is field 3 (state); utime=field14→idx11, stime→idx12.
        if fields.len() > 12
            && let (Ok(utime), Ok(stime)) = (fields[11].parse::<u64>(), fields[12].parse::<u64>())
        {
            // SAFETY: sysconf is a pure query with no preconditions.
            let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
            if hz > 0 {
                // Saturating throughout: the add and the final `u64` cast clamp
                // rather than debug-panic / silently wrap on an implausibly large
                // tick count.
                let ticks = utime.saturating_add(stime);
                let nanos = ticks as u128 * 1_000_000_000u128 / hz as u128;
                metrics.cpu_time = Some(Duration::from_nanos(nanos.min(u64::MAX as u128) as u64));
            }
        }
    }

    // Peak memory: /proc/<pid>/status VmHWM (high-water resident set, in kB).
    if let Some(status) = read_status(pid) {
        // The process can exit and its pid can be recycled after the first stat
        // snapshot but before this status snapshot. Reconfirm after the status read
        // and fail both samples together rather than pairing the original CPU time
        // with a replacement process's memory. Without a demanded identity, retain
        // the number-only best-effort behavior and avoid an extra stat read.
        if let Some(expected) = expected
            && read_stat(pid)
                .as_deref()
                .and_then(crate::sys::procfs::starttime_from_stat)
                != Some(expected.raw())
        {
            return ProcMetrics::default();
        }

        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    // Saturating: kB→bytes can't wrap on an implausible VmHWM.
                    metrics.peak_memory_bytes = Some(kb.saturating_mul(1024));
                }
                break;
            }
        }
    }

    metrics
}

impl Drop for Job {
    fn drop(&mut self) {
        match &self.backend {
            Backend::Cgroup(cg) => {
                if !self.skip_drop_kill.is_set() {
                    // Only hard-kill when the caller didn't choose escalate=false.
                    let _ = cg.kill();
                    // `cgroup.kill` is asynchronous: the kernel SIGKILLs the subtree,
                    // but `rmdir` returns `EBUSY` until the members have actually left
                    // (a process leaves `cgroup.procs` when it *exits*, before it is
                    // reaped — so this drains within milliseconds, independent of the
                    // async reaper). Wait bounded so we don't leak the dir.
                    //
                    // `Drop` can't await, so this blocking sleep runs synchronously
                    // wherever the `Job` is dropped — often a tokio worker thread —
                    // stalling that thread's executor for the wait. Bounded: ~100ms
                    // here plus ~100ms from the pre-5.14 `cg.kill()` SIGKILL-sweep
                    // fallback; on a modern kernel `cgroup.kill` is atomic and the
                    // loop usually exits on the first check. Accepted cost of a
                    // synchronous leak-safe teardown.
                    for _ in 0..50 {
                        if let Ok(true) = cg.is_empty() {
                            break;
                        }
                        // `Ok(false)` or `Err(_)`: an unreadable member list is
                        // unknown, not empty. Keep waiting best-effort; Drop
                        // must not panic.
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
                // The per-spawn leaves go first and unconditionally: a cgroup
                // directory that still has child directories cannot be removed at
                // all (`rmdir` answers `ENOTEMPTY`), so an unreclaimed leaf would
                // keep the whole job directory alive. A non-escalating shutdown is
                // allowed to leave a survivor in its leaf and parent, but that is
                // only temporary containment: once this synchronous pass has done
                // everything it can, the remaining paths are handed to the
                // process-wide reclaimer, which retries without issuing a kill.
                cg.reclaim_leaves();
                let parent = cg.path.clone();
                match std::fs::remove_dir(&parent) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => enqueue_cgroup_reclaim(parent, cg.leaf_dirs()),
                }
            }
            // The `ProcessGroup` field hard-kills its tracked groups in its own
            // `Drop`, which runs as this `Job` is torn down — nothing to do here.
            Backend::ProcessGroup(_) => {}
        }
    }
}

/// The cgroup v2 (unified) mount root, if one is present (C5). Checks the pure-v2
/// location (`/sys/fs/cgroup`) first, then the systemd **hybrid** location
/// (`/sys/fs/cgroup/unified`); the presence of `cgroup.controllers` at the root is
/// the v2 marker. Returns `None` when no v2 hierarchy is mounted (v1-only or no
/// cgroups), which routes to the process-group fallback.
fn cgroup2_root() -> Option<PathBuf> {
    for candidate in ["/sys/fs/cgroup", "/sys/fs/cgroup/unified"] {
        let root = Path::new(candidate);
        if root.join("cgroup.controllers").exists() {
            return Some(root.to_path_buf());
        }
    }
    None
}

/// This process's **own** cgroup directory under the v2 `root` — the parent under
/// which a fresh leaf cgroup would be created. On v2, `/proc/self/cgroup` is a
/// single `0::<path>` line; the path is joined onto `root` (a missing/unparsable
/// file falls back to the root itself, `rel = "/"`). Shared by [`Cgroup::create`]
/// (which then `mkdir`s a leaf here) and the read-only [`detect_mechanism`] (which
/// only *probes* whether a leaf could be created), so the "where is our cgroup"
/// resolution is single-sourced and cannot drift between the two paths.
fn cgroup2_self_dir(root: &Path) -> io::Result<PathBuf> {
    let self_cgroup = std::fs::read_to_string("/proc/self/cgroup")?;
    let rel = self_cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap_or("/")
        .trim();
    Ok(root.join(rel.trim_start_matches('/')))
}

/// Whether a new sub-directory (a leaf cgroup) could be created inside `dir` right
/// now, decided by a **pure permission probe that creates nothing**. `mkdir`ing an
/// entry inside `dir` needs write + search (execute) permission on `dir` itself, so
/// that is exactly what is checked, via `faccessat(…, AT_EACCESS)` on the effective
/// ids (matching the ids the real `mkdir` in [`Cgroup::create`] would run under — a
/// read-only mount fails this the same `EROFS` way `mkdir` would). This is the
/// read-only stand-in for the authoritative `mkdir` the group-creation path
/// performs: best-effort, so the rare window where a writable-looking `dir` then
/// rejects creation (a race, an LSM policy) is where [`detect_mechanism`]'s
/// prediction may differ from the mechanism `Job::new` ultimately falls back to.
fn dir_allows_subdir_creation(dir: &Path) -> bool {
    access_ok(dir, libc::W_OK | libc::X_OK)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HardKillPrimitive {
    CgroupKill,
    Pidfd,
}

/// The primitive that makes a cgroup job safe to hand back as kill-on-drop.
///
/// `cgroup.kill` is path-local, while pidfd support is process-wide. Keeping the
/// decision in one seam lets the read-only host query and real creation apply the
/// same fallback rule without making tests depend on the running kernel or its
/// seccomp profile.
fn hard_kill_primitive_with(
    cgroup_dir: &Path,
    cgroup_kill_exists: impl FnOnce(&Path) -> bool,
    pidfd_probe: impl FnOnce(i32) -> io::Result<()>,
) -> Option<HardKillPrimitive> {
    hard_kill_primitive_from(
        cgroup_kill_exists(&cgroup_dir.join("cgroup.kill")),
        pidfd_probe,
    )
}

fn hard_kill_primitive_from(
    cgroup_kill_available: bool,
    pidfd_probe: impl FnOnce(i32) -> io::Result<()>,
) -> Option<HardKillPrimitive> {
    if cgroup_kill_available {
        return Some(HardKillPrimitive::CgroupKill);
    }
    pidfd_probe(std::process::id() as i32)
        .ok()
        .map(|()| HardKillPrimitive::Pidfd)
}

fn hard_kill_primitive(cgroup_dir: &Path) -> Option<HardKillPrimitive> {
    hard_kill_primitive_with(cgroup_dir, Path::exists, |pid| pidfd_open(pid).map(drop))
}

/// Read-only prediction of whether a child created under `parent` will expose
/// `cgroup.kill`.
///
/// Every non-root cgroup on a supporting kernel exposes the file, so the
/// parent's own file is normally authoritative. The hierarchy root is the one
/// exception: it cannot be killed and therefore has no `cgroup.kill`, while its
/// children do. In that case the kernel release is the only read-only answer
/// when the root has no existing child to inspect. An existing child remains
/// useful evidence for vendor backports to pre-5.14 kernels.
fn child_cgroup_kill_available(root: &Path, parent: &Path) -> bool {
    if parent.join("cgroup.kill").exists() {
        return true;
    }
    if parent != root {
        return false;
    }
    root_has_cgroup_kill_child(root) || kernel_supports_cgroup_kill()
}

fn root_has_cgroup_kill_child(root: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    entries
        .filter_map(Result::ok)
        .any(|entry| entry.path().join("cgroup.kill").exists())
}

fn kernel_supports_cgroup_kill() -> bool {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .is_ok_and(|release| kernel_release_supports_cgroup_kill(release.trim()))
}

fn kernel_release_supports_cgroup_kill(release: &str) -> bool {
    let mut components = release.split('.');
    let Some(major) = components.next().and_then(parse_kernel_version_component) else {
        return false;
    };
    let Some(minor) = components.next().and_then(parse_kernel_version_component) else {
        return false;
    };
    (major, minor) >= (5, 14)
}

fn parse_kernel_version_component(component: &str) -> Option<u32> {
    let digit_count = component
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    component.get(..digit_count)?.parse().ok()
}

fn predicted_hard_kill_primitive(root: &Path, parent: &Path) -> Option<HardKillPrimitive> {
    predicted_hard_kill_primitive_with(root, parent, child_cgroup_kill_available, |pid| {
        pidfd_open(pid).map(drop)
    })
}

fn predicted_hard_kill_primitive_with(
    root: &Path,
    parent: &Path,
    child_cgroup_kill_probe: impl FnOnce(&Path, &Path) -> bool,
    pidfd_probe: impl FnOnce(i32) -> io::Result<()>,
) -> Option<HardKillPrimitive> {
    hard_kill_primitive_from(child_cgroup_kill_probe(root, parent), pidfd_probe)
}

fn hard_kill_unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "cgroup v2 cannot guarantee hard teardown: neither cgroup.kill nor pidfd_open is available",
    )
}

/// Whether `path` exists and grants `mode` (`W_OK`, `X_OK`, …) to the **effective**
/// ids right now — the shared `faccessat(…, AT_EACCESS)` primitive behind
/// [`dir_allows_subdir_creation`] and the leaf-joinability probe in
/// [`Cgroup::open_leaf`]. Creates and modifies nothing, and is best-effort by
/// nature: a `false` (including a path that is simply not there) is what both
/// callers act on, and neither treats a `true` as a promise that the real
/// `mkdir`/`write` cannot still be refused.
fn access_ok(path: &Path, mode: libc::c_int) -> bool {
    let Ok(c_path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `faccessat` is a pure permission query — it creates and modifies
    // nothing. `c_path` is a valid NUL-terminated path; the mode/flags are
    // constants. `AT_EACCESS` checks the effective uid/gid, matching the real
    // `mkdir`/`open` the caller is predicting.
    let rc = unsafe { libc::faccessat(libc::AT_FDCWD, c_path.as_ptr(), mode, libc::AT_EACCESS) };
    rc == 0
}

/// Read-only prediction of the [`Mechanism`] a fresh [`Job`] would use on this host
/// right now, **without creating any cgroup directory or spawning a process** —
/// the detection extracted from the group-creation path so the public
/// `host_containment()` query and `Job::new` agree.
///
/// Reports [`Mechanism::CgroupV2`] when a cgroup v2 hierarchy is mounted
/// ([`cgroup2_root`]), this process's own cgroup dir ([`cgroup2_self_dir`]) would
/// accept a new leaf ([`dir_allows_subdir_creation`]), and that prospective leaf
/// can be torn down by either `cgroup.kill` or pidfd-backed member signalling.
/// Creation verifies the same capability against the child it actually made;
/// otherwise this reports
/// [`Mechanism::ProcessGroup`] (the POSIX process-group fallback). The cgroup
/// branch is **best-effort**: at the hierarchy root, which has no `cgroup.kill`
/// of its own, it predicts the child's interface from the kernel release or an
/// existing child. A later `mkdir` refusal or changed interface view can still
/// make `Job::new` fall back.
pub(crate) fn detect_mechanism() -> Mechanism {
    let Some(root) = cgroup2_root() else {
        return Mechanism::ProcessGroup;
    };
    match cgroup2_self_dir(&root) {
        Ok(parent)
            if dir_allows_subdir_creation(&parent)
                && predicted_hard_kill_primitive(&root, &parent).is_some() =>
        {
            Mechanism::CgroupV2
        }
        _ => Mechanism::ProcessGroup,
    }
}

/// The single boundary every cgroup interface-file write in this backend passes
/// through — the one primitive by which this backend changes kernel state
/// (`memory.max`, `pids.max`, `cpu.max`, `cgroup.procs`, `cgroup.freeze`,
/// `cgroup.kill`, and the parent's `cgroup.subtree_control`).
///
/// Behaviorally it is exactly [`std::fs::write`]. Funnelling every write through one
/// place is what lets a `cfg(test)` rule order the write of *one named* control file
/// to fail with a specific errno: "the second of the three sequential limit writes
/// fails" and "`cgroup.freeze` is rejected on a kernel that *has* the file" become
/// deterministic unit tests instead of states only a delegated, restricted or
/// otherwise degraded cgroup host produces. The target label is the file name.
///
/// Reads deliberately keep their existing `*_with(read: impl Fn(&Path) -> …)`
/// closure seams ([`Cgroup::members_with`] and friends) — that is the right tool
/// where the primitive is already a parameter. See the `sys::fault_injection`
/// module (test builds only, hence the bare reference — an intra-doc link to a
/// `cfg(test)` item breaks the rustdoc build) for why the write side needed a
/// different one.
fn cgroup_write(path: &Path, contents: impl AsRef<[u8]>) -> io::Result<()> {
    #[cfg(test)]
    if let Some(injected) = crate::sys::fault_injection::check(
        crate::sys::fault_injection::Site::CgroupWrite,
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default(),
    ) {
        return Err(injected);
    }
    std::fs::write(path, contents)
}

/// The live member pids listed by **one** cgroup directory's own `cgroup.procs`,
/// through the caller's reader seam. The building block of
/// [`Cgroup::members_with`], which folds the job's cgroup and each of its per-spawn
/// leaves ([`Leaves`]) into one set.
///
/// A directory that is gone is empty (`NotFound` → no members — a removed cgroup
/// holds nothing); any other read failure leaves its membership unknown and is
/// surfaced to the caller rather than silently shortening the job's.
fn read_member_pids(
    dir: &Path,
    read: impl Fn(&Path) -> io::Result<String>,
) -> io::Result<Vec<i32>> {
    match read(&dir.join("cgroup.procs")) {
        Ok(procs) => Ok(procs
            .lines()
            // Keep only real pids: a `0`/negative line would otherwise reach
            // `kill(pid, …)` as "the caller's whole process group" (0) or "a
            // process group" (negative) — never a single tracked member. Note
            // a `0` here is not only the (never-emitted) kernel guard: a member
            // living in a **nested PID namespace** not mapped into the reader's
            // namespace reads as `0` in `cgroup.procs`, so it is dropped here
            // and thus skips the per-pid graceful `SIGTERM` tier (C8) — the
            // final `cgroup.kill`, which acts on the whole cgroup regardless of
            // pid visibility, still reaps it.
            .filter_map(|l| l.trim().parse::<i32>().ok())
            .filter(|&pid| pid > 0)
            .collect()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

struct Cgroup {
    path: PathBuf,
    /// The per-spawn leaf sub-cgroups created under [`path`](Self::path).
    leaves: Mutex<Leaves>,
}

/// The per-spawn **leaf sub-cgroups** of one job, and when to next reclaim their
/// directories.
///
/// cgroup v2 offers no way to kill *part* of a cgroup: `cgroup.kill` takes the
/// whole subtree of the directory it is written in. Giving every spawn a leaf
/// directory of its own under the job's cgroup is therefore what turns "kill
/// exactly this spawn's tree, and nothing else of the job" into a single atomic
/// kernel operation — [`Job::rollback_pty_spawn`] is its one user today. It is also
/// the canonical cgroup v2 shape: the "no internal processes" rule wants processes
/// in leaves as soon as a cgroup distributes resources to children.
///
/// What does **not** change shape: the job's own cgroup stays the whole-job handle.
/// `cgroup.kill` and `cgroup.freeze` written there act on it *and every descendant*,
/// so `kill_all`, the graceful tier, `suspend`/`resume` and `Drop` each reach every
/// leaf with the same single write they already made — none of them gained a
/// per-leaf walk, and none of them changed *when* it writes (`Drop` still writes only
/// while the kill-on-drop backstop is armed, as before). `cgroup.procs`, in contrast,
/// lists only the directory's **own** members and never a descendant's, so membership
/// enumeration — and everything built on it — reads the job's cgroup plus each live
/// leaf ([`Cgroup::members_with`]).
///
/// Resource limits stay on the job's cgroup and are inherited: this backend never
/// writes its **own** `cgroup.subtree_control` (only the *parent*'s, to make the
/// limit interface files appear — see [`Cgroup::enable_controllers`]), so no
/// controller is enabled *in* a leaf, and a leaf is transparent for accounting — the
/// charges of its members land on the job cgroup's own counters, which is what the
/// caps there constrain and what [`Cgroup::limit_evidence`] reads back.
///
/// A `Job` never learns that a child exited (the `Child` handles belong to the
/// caller), so a leaf outlives the process that lived in it and its directory is
/// reclaimed lazily: [`Cgroup::reclaim_leaves`] `rmdir`s every leaf the kernel lets
/// go of, from teardown, from a selective kill, and opportunistically from a launch
/// once [`reclaim_at`](Self::reclaim_at) is reached.
struct Leaves {
    /// One entry per leaf directory this job has created and not seen removed.
    live: Vec<Leaf>,
    /// The `live.len()` at which the next launch pays for a reclaim pass. Each pass
    /// costs one `rmdir` per live leaf and then re-arms this at twice what it left
    /// behind (never below [`LEAF_RECLAIM_FLOOR`]), which keeps the reclaim
    /// amortized O(1) per spawn while bounding the leftover directories to roughly
    /// twice the job's peak concurrency.
    reclaim_at: usize,
}

/// One per-spawn leaf: the directory a single launch routed its child into.
struct Leaf {
    /// The pid that launch returned, while this leaf is still the one that pid
    /// names. `None` once that spawn is over — its rollback consumed it, or a later
    /// spawn was handed the same number — or when the launch never returned a pid at
    /// all and its leaf was registered by [`LeafSlot`]'s drop instead. Either way it
    /// leaves a directory to reclaim and members to enumerate, but nothing a selective
    /// kill may aim at: a pid the kernel has since recycled, or one this job never
    /// held, must never steer a kill at a leaf.
    pid: Option<i32>,
    dir: PathBuf,
}

/// The smallest number of leaf directories a job keeps before a launch pays for a
/// reclaim pass — see [`Leaves::reclaim_at`].
const LEAF_RECLAIM_FLOOR: usize = 16;

impl Leaves {
    const fn new() -> Self {
        Leaves {
            live: Vec::new(),
            reclaim_at: LEAF_RECLAIM_FLOOR,
        }
    }

    /// `rmdir` every leaf directory the kernel lets go of, dropping exactly those
    /// entries. A directory that is *not* removable still holds members (cgroup v2
    /// answers `EBUSY` while it does, `ENOTEMPTY` while it has children of its own),
    /// so its entry stays — an entry is released only on the kernel's own
    /// confirmation that there is nothing left to enumerate or kill there, never on
    /// an assumption, which is what keeps a reclaim from narrowing what the job can
    /// still reach. A directory already gone (`NotFound`) is equally confirmed.
    fn reclaim(&mut self) {
        self.live
            .retain(|leaf| match std::fs::remove_dir(&leaf.dir) {
                Ok(()) => false,
                Err(e) => e.kind() != io::ErrorKind::NotFound,
            });
        self.reclaim_at = self.live.len().saturating_mul(2).max(LEAF_RECLAIM_FLOOR);
    }
}

/// The leaf sub-cgroup reserved for a launch that has not happened yet, handed out
/// by [`Cgroup::open_leaf`] and turned over to the job's registry by
/// [`commit`](Self::commit) once a child exists.
///
/// Its `Drop` is what keeps a launch that never reached `commit` — a `cgroup.procs`
/// path that cannot be a `CString`, a `Command::spawn` that fails, a panic — from
/// leaving the directory it reserved behind: it `rmdir`s it. Until then nothing else
/// can be looking at that directory, since a leaf becomes visible to the rest of this
/// backend only once it is registered.
///
/// **A launch that fails can still have left a child in it**, so it is that `rmdir`'s
/// answer — not the failure — that decides the leaf's fate. `Ok`/`NotFound` is the
/// kernel's own confirmation that there is nothing in there, and releases it; any
/// other answer (`EBUSY` while it holds members, `ENOTEMPTY` while it has children of
/// its own) hands the directory to the registry instead
/// ([`register_leaf`](Cgroup::register_leaf)) — the same "release only on a confirmed
/// answer" rule [`Leaves::reclaim`] follows. What this must not do is *forget* a
/// directory the kernel refused to take: the registry is what
/// [`members_with`](Cgroup::members_with) reads and what
/// [`reclaim_leaves`](Cgroup::reclaim_leaves) revisits, so a forgotten leaf would take
/// a live member out of every whole-job verb that enumerates — `members`, `stats`,
/// `is_empty`, the graceful tier, and the per-pid sweep the pre-5.14 teardown falls
/// back to ([`kill_with_seams`](Cgroup::kill_with_seams), which would then report a
/// job drained that it never killed) — and would leak both directories, since a job
/// cgroup that still has a child directory cannot be removed at all.
///
/// That a failed launch can leave a live child is tokio's post-fork setup:
/// `Command::spawn` there is `std::process::Command::spawn` followed by steps that can
/// fail *after* it — registering the child's stdio with the reactor, opening its pidfd
/// — and those return the error while dropping the `std::process::Child`, which does
/// not kill it. The pre-exec hook put that child in the leaf before its `exec`, so it
/// is alive and contained there, with the launch reporting only an `Err`.
///
/// Such a leaf is registered with **no pid**: the launch returned none, and a
/// selective kill may never be aimed at a leaf on a number nothing can vouch for
/// (see [`Leaf::pid`]). Enumerating it, killing the whole job, and reclaiming it once
/// it drains need no pid — and are exactly what registering it preserves.
struct LeafSlot<'a> {
    cg: &'a Cgroup,
    /// `None` when no leaf could be reserved (the launch joins the job's own cgroup)
    /// or once [`commit`](Self::commit) has handed it over.
    dir: Option<PathBuf>,
}

impl LeafSlot<'_> {
    /// The `cgroup.procs` the child must write itself into: the reserved leaf's, or
    /// the job cgroup's own when there is no leaf.
    fn procs_path(&self) -> PathBuf {
        self.dir
            .as_deref()
            .unwrap_or(&self.cg.path)
            .join("cgroup.procs")
    }

    /// The launch produced a child (`pid`, or `None` if it was already gone by the
    /// time the spawn returned): hand the leaf to the job's registry, where it is
    /// enumerated, selectively killable and eventually reclaimed.
    fn commit(mut self, pid: Option<i32>) {
        if let Some(dir) = self.dir.take() {
            self.cg.register_leaf(pid, dir);
        }
    }
}

impl Drop for LeafSlot<'_> {
    fn drop(&mut self) {
        if let Some(dir) = self.dir.take() {
            match std::fs::remove_dir(&dir) {
                // Confirmed empty (or already gone): there is nothing left to
                // enumerate, kill or reclaim, so the job need never hear of it.
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                // Refused: something is in there — a child of a launch that failed
                // after its fork. Hand it over, pid-less, rather than forget it.
                Err(_) => self.cg.register_leaf(None, dir),
            }
        }
    }
}

impl Cgroup {
    /// A handle over an existing cgroup directory, with no leaves yet.
    fn at(path: PathBuf) -> Self {
        Cgroup {
            path,
            leaves: Mutex::new(Leaves::new()),
        }
    }

    /// The lock over this job's leaf registry, recovered rather than propagated if a
    /// panic poisoned it: every path that takes it is a short, allocation-light
    /// critical section, and the ones that matter run inside `Drop` and a rollback,
    /// where dropping the registry would leak directories or void a kill.
    fn leaves(&self) -> std::sync::MutexGuard<'_, Leaves> {
        self.leaves.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The live leaf directories, snapshotted so the membership reads that follow
    /// hold no lock while they touch the filesystem.
    fn leaf_dirs(&self) -> Vec<PathBuf> {
        self.leaves()
            .live
            .iter()
            .map(|leaf| leaf.dir.clone())
            .collect()
    }

    /// Reserve the leaf sub-cgroup for one launch — the `mkdir`, and the check that
    /// a child could actually join what it created.
    ///
    /// Best-effort by design: a host that refuses the `mkdir` (a `cgroup.max.depth`
    /// or `cgroup.max.descendants` cap on the delegated subtree, a filesystem
    /// remounted read-only since the job was created) hands back a slot with no leaf,
    /// and the launch joins the job's own cgroup exactly as it did before leaves
    /// existed. Containment is identical either way — only the *selectivity* of a
    /// later rollback is lost (see [`Job::rollback_pty_spawn`]).
    ///
    /// The joinability probe is the reason this cannot cost a spawn: the child joins
    /// by writing its own pid to `cgroup.procs` **after** the fork, where a failure
    /// can do nothing but fail the whole launch, so the parent asks here — while
    /// falling back to the job's cgroup is still possible — whether that file is
    /// there and writable. On cgroupfs the kernel populates a fresh cgroup with its
    /// interface files, so this passes whenever the `mkdir` did; what it rules out is
    /// routing a child into a directory that is not a cgroup at all.
    fn open_leaf(&self) -> LeafSlot<'_> {
        // The job dir is fresh (`create` retries a name collision away), and the
        // counter is process-wide and monotonic, so a name collision inside it is
        // not a case to retry — an error here simply means no leaf for this spawn.
        let dir = self
            .path
            .join(format!("spawn-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed)));
        if std::fs::create_dir(&dir).is_err() {
            return LeafSlot {
                cg: self,
                dir: None,
            };
        }
        if !access_ok(&dir.join("cgroup.procs"), libc::W_OK) {
            let _ = std::fs::remove_dir(&dir);
            return LeafSlot {
                cg: self,
                dir: None,
            };
        }
        LeafSlot {
            cg: self,
            dir: Some(dir),
        }
    }

    /// Take `dir` into the registry as the leaf of the spawn that returned `pid`,
    /// and pay for a reclaim pass if enough leaves have accumulated.
    ///
    /// A pid the kernel recycled onto this new spawn cannot leave two leaves
    /// answering to one number: any older entry still claiming it is retired here
    /// (its directory stays registered until the kernel confirms it is gone — see
    /// [`Leaves::reclaim`]), so a lookup by pid can only ever find this one.
    fn register_leaf(&self, pid: Option<i32>, dir: PathBuf) {
        let mut leaves = self.leaves();
        if let Some(pid) = pid {
            for leaf in &mut leaves.live {
                if leaf.pid == Some(pid) {
                    leaf.pid = None;
                }
            }
        }
        leaves.live.push(Leaf { pid, dir });
        if leaves.live.len() >= leaves.reclaim_at {
            leaves.reclaim();
        }
    }

    /// SIGKILL the subtree of the spawn that returned `pid` — that spawn's leaf and
    /// nothing else of this job — reporting whether that kill was performed.
    ///
    /// `false` means no selective kill happened and the caller must fall back to its
    /// own (see [`Job::rollback_pty_spawn`]), for one of two reasons kept
    /// deliberately distinct from anything about the job's whole-tree teardown: the
    /// spawn has no leaf (this job never made one for that pid, or the pid was
    /// retired), or the `cgroup.kill` write was refused (a kernel < 5.14 has no such
    /// file; a restricted delegated cgroup can reject the write). In the second case
    /// the leaf keeps holding whatever it held, still enumerated by
    /// [`members`](Self::members) and still covered by the job's own recursive
    /// `cgroup.kill`.
    ///
    /// The pid is retired either way: the spawn it named is over, and letting a
    /// number the kernel may recycle keep steering a kill here would be strictly
    /// worse than the fallback. The directory is *not* released on that account —
    /// only [`Leaves::reclaim`]'s `rmdir` releases one, and `cgroup.kill` is
    /// asynchronous (the kernel has delivered SIGKILL when the write returns; the
    /// members leave `cgroup.procs` as they exit), so a leaf that is still draining
    /// simply survives this reclaim and is taken by the next one, or by `Drop`.
    #[cfg(feature = "pty")]
    fn kill_leaf_of(&self, pid: i32) -> bool {
        let dir = {
            let mut leaves = self.leaves();
            let Some(leaf) = leaves.live.iter_mut().find(|leaf| leaf.pid == Some(pid)) else {
                return false;
            };
            leaf.pid = None;
            leaf.dir.clone()
        };
        if cgroup_write(&dir.join("cgroup.kill"), b"1").is_err() {
            return false;
        }
        self.reclaim_leaves();
        true
    }

    /// Reclaim every leaf directory the kernel lets go of — see [`Leaves::reclaim`]
    /// for why that is the only condition under which one is dropped.
    fn reclaim_leaves(&self) {
        self.leaves().reclaim();
    }

    /// Best-effort undo of the `cgroup.procs` write a bare-pid adoption made, run
    /// only once the closing identity re-read has *proved* the number was recycled
    /// somewhere inside that call ([`Job::adopt_external`]).
    ///
    /// **Why an undo is needed here and not on the process-group arm.** That arm's
    /// only durable trace is an entry carrying the captured token, which the
    /// identity gate prunes unsignalled — detection is enough there. Here the write
    /// has already changed kernel state that outlives the call: membership of *this
    /// job's cgroup* is precisely where `kill_all`, the graceful tier and `Drop`
    /// aim, so a stranger the write pulled in would be SIGKILLed later by a group
    /// that was never given it. Reporting the recycle without moving it back out
    /// would be reporting a state this call created and left in place.
    ///
    /// **The membership pass comes first, and it is not a formality.** A recycle
    /// detected *after* the write has two shapes the tokens alone cannot tell apart,
    /// and they want opposite actions:
    ///
    /// - the number changed hands **before** the write, so the write moved a
    ///   stranger in — it is a member here now, and moving it out is the fix;
    /// - the number changed hands **after** the write, i.e. the process the caller
    ///   named really was migrated here and then exited, was reaped, and its number
    ///   was handed on. Nothing of this job's remains (an exited task leaves the
    ///   cgroup), and the number now names a process this call never touched.
    ///   Moving *that* one out would take an innocent process out of its own
    ///   cgroup — the same harm, aimed the other way.
    ///
    /// Reading this cgroup's own `cgroup.procs` separates them: the number is listed
    /// only in the first shape. The read is one pass and the write that follows is
    /// again by number, so a further recycle in between is possible and irreducible
    /// (`cgroup.procs` accepts numbers, and no pinning primitive changes what a
    /// *write* resolves); the residue is bounded to moving some process out of its
    /// cgroup rather than into a group that kills it, which is the direction this
    /// backend errs in everywhere else. The same bound covers the one other way a
    /// listed number can be a member honestly — a descendant the adopted process
    /// forked here, which then took the freed number: it is moved out and this group
    /// loses containment of that one process, which is a loss the caller is told
    /// about, not a stranger killed silently.
    ///
    /// **Destination.** The directory this job's cgroup was created under — this
    /// process's own cgroup ([`cgroup2_self_dir`]), which is where a member of this
    /// group would have lived had the crate never made a sub-cgroup for it. It is
    /// the one destination this call can name without guessing, it exists (the job's
    /// own directory was created inside it), and it is outside everything this job
    /// kills. Two things it is **not**: a promise that the write is permitted there
    /// (`mkdir` rights on a directory are not write rights on its `cgroup.procs` —
    /// hence the outcome below), and the cgroup the process came from, which v2's
    /// exclusive membership has already discarded and the kernel does not report.
    /// On a host where this process itself lives in the hierarchy root, that
    /// destination *is* the root, with the weaker containment that implies.
    ///
    /// **What it cannot promise.** The move-out is a write like any other and can be
    /// refused (a delegated cgroup that will not take it, an `EBUSY` from the "no
    /// internal processes" rule where the destination distributes resources). The
    /// caller is told which of the three outcomes it got, because they differ in the
    /// only way that matters: whether this group's teardown still reaches the number.
    #[cfg(feature = "process-control")]
    fn evict_recycled(&self, pid: u32) -> RecycleUndo {
        let members = match read_member_pids(&self.path, |path| std::fs::read_to_string(path)) {
            Ok(members) => members,
            // Membership unreadable: nothing here can show the number is out, so say
            // so rather than act on a guess in either direction.
            Err(e) => return RecycleUndo::Stuck(e),
        };
        if !members
            .iter()
            .any(|&member| u32::try_from(member).is_ok_and(|member| member == pid))
        {
            return RecycleUndo::NotAMember;
        }
        let Some(parent) = self.path.parent() else {
            return RecycleUndo::Stuck(io::Error::other(
                "this group's cgroup has no parent directory to move the number back out into",
            ));
        };
        match cgroup_write(&parent.join("cgroup.procs"), pid.to_string().as_bytes()) {
            Ok(()) => RecycleUndo::Evicted,
            // The number names nothing any more, so it is a member of nothing —
            // including this cgroup.
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => RecycleUndo::NotAMember,
            Err(e) => RecycleUndo::Stuck(e),
        }
    }

    fn create(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // Locate the cgroup v2 (unified) mount root. The common case is
        // `/sys/fs/cgroup` (pure v2), but a systemd **hybrid** host mounts the v2
        // hierarchy at `/sys/fs/cgroup/unified` — checking only the former (C5)
        // would fall back to pgroup despite a usable v2 tree.
        let root = cgroup2_root()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "cgroup v2 not mounted"))?;
        let root = root.as_path();

        // Our own cgroup (the parent the leaf is created under), resolved by the
        // shared helper the read-only `detect_mechanism` query also uses so the two
        // can never disagree on *where* this process's cgroup is.
        let parent = cgroup2_self_dir(root)?;

        // Without limits, no controllers are enabled — `cgroup.kill` needs none,
        // and that sidesteps the "no internal processes" rule. Even *with* limits,
        // the controllers are enabled in the PARENT's `cgroup.subtree_control` (see
        // `enable_controllers`), never in this cgroup's own, which is what lets it
        // hold adopted members and the per-spawn leaf directories (`Leaves`) at the
        // same time: the rule forbids only distributing resources to children while
        // holding processes. mkdir is the permission gate that triggers the
        // process-group fallback when delegation is absent.
        //
        // Retry with a fresh counter when the dir already exists — a leftover from
        // a crashed run whose pid was recycled, or two crate versions sharing the
        // namespace — rather than letting `EEXIST` masquerade as a delegation
        // failure and silently downgrade. The salt makes a real collision
        // astronomically unlikely; the bounded retry is the backstop. A genuine
        // permission failure (`EACCES`/`EPERM`) is NOT retried — it propagates and
        // triggers the process-group fallback promptly.
        let salt = cgroup_name_salt();
        let mut created = None;
        for _ in 0..32 {
            let name = format!(
                "processkit-{}-{:x}-{}",
                std::process::id(),
                salt,
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            );
            let path = parent.join(name);
            match std::fs::create_dir(&path) {
                Ok(()) => {
                    created = Some(path);
                    break;
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        let path = created.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "could not create a unique cgroup directory after retries",
            )
        })?;
        let cg = Cgroup::at(path);

        // This created non-root cgroup is the authoritative capability gate.
        // The hierarchy root deliberately has no `cgroup.kill` of its own even
        // when every child exposes it, so checking the parent before `mkdir`
        // would reject a backend with working atomic teardown. If this child has
        // neither primitive, remove it before Job::new falls back to a pgroup.
        if hard_kill_primitive(&cg.path).is_none() {
            let _ = std::fs::remove_dir(&cg.path);
            return Err(hard_kill_unavailable());
        }

        // With limits, enable the matching controllers and write the caps. If that
        // fails (no delegation, or the parent holds processes so it can't carry
        // subtree_control), don't leak the dir we just made — remove it and report.
        #[cfg(feature = "limits")]
        if limits.any()
            && let Err(e) = cg.apply_limits(&parent, limits)
        {
            let _ = std::fs::remove_dir(&cg.path);
            return Err(e);
        }
        Ok(cg)
    }

    /// Enable the controllers each requested limit needs — but only the ones not
    /// *already* enabled — in `parent`'s `cgroup.subtree_control` (which is what
    /// makes the limit interface files appear in our child cgroup), then write the
    /// limit values. Here `parent` is this process's own cgroup (the child is
    /// created under it), so per cgroup v2's "no internal processes" rule the
    /// enable succeeds only when `parent` is the *real* cgroup-v2 hierarchy root (a
    /// cgroup namespace root does not count); otherwise it fails fast with an
    /// honest error. The crate does not migrate this process out of its cgroup to
    /// work around the rule.
    ///
    /// Any controller enablement is deliberately NOT reverted on `Drop`: the
    /// parent cgroup is shared (sibling groups, other processes of this same
    /// user), so disabling controllers there could yank the interface files out
    /// from under unrelated trees. Enabled-but-unused controllers cost nothing.
    #[cfg(feature = "limits")]
    fn apply_limits(&self, parent: &Path, limits: &ResourceLimits) -> io::Result<()> {
        // Enable the controllers each requested limit needs (the "no internal
        // processes" gate — fails fast off the real hierarchy root), then write the
        // requested caps. At creation the limit files default to `max`, so only the
        // Some axes are written; the None-axis reset lives in `update_limits`.
        self.enable_controllers(parent, &needed_controllers(limits))?;
        if let Some(bytes) = limits.max_memory {
            cgroup_write(&self.path.join("memory.max"), bytes.to_string())?;
        }
        if let Some(n) = limits.max_processes {
            cgroup_write(&self.path.join("pids.max"), n.to_string())?;
        }
        if let Some(cores) = limits.cpu_quota {
            cgroup_write(&self.path.join("cpu.max"), cpu_max_value(cores))?;
        }
        Ok(())
    }

    /// Apply a fresh [`ResourceLimits`] set to this **already-created** cgroup — the
    /// backend for [`ProcessGroup::update_limits`](crate::ProcessGroup::update_limits).
    ///
    /// A **full replacement** of the live caps: each of `memory.max` / `pids.max` /
    /// `cpu.max` is overwritten with the new value, and an axis left `None` is
    /// written back to `max` (unbounded) — but only when its interface file exists.
    /// A controller that was never enabled has no file and is already unbounded, so
    /// there is nothing to reset; a newly-requested axis whose controller isn't yet
    /// enabled enables it here first (and, off the real hierarchy root, fails fast
    /// with the same honest error `apply_limits` raises at creation).
    ///
    /// `parent` is derived from this cgroup's own path — the dir it was created
    /// under, i.e. this process's own cgroup — the same `parent` `create` computed,
    /// so no `/proc/self/cgroup` re-derivation is needed and the write targets the
    /// live cgroup rather than re-resolving a possibly-stale one.
    #[cfg(feature = "limits")]
    fn update_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "cgroup directory has no parent — cannot resolve subtree_control",
            )
        })?;
        // Enable controllers for the newly-requested (Some) axes not already
        // enabled — the same off-root fail-fast gate as creation. A None axis needs
        // no controller (it is being cleared, not enforced).
        self.enable_controllers(parent, &needed_controllers(limits))?;
        // Full replacement: set each axis, or reset a removed one to `max`.
        write_limit_reset(
            &self.path.join("memory.max"),
            limits.max_memory.map(|b| b.to_string()),
        )?;
        write_limit_reset(
            &self.path.join("pids.max"),
            limits.max_processes.map(|n| n.to_string()),
        )?;
        write_limit_reset(
            &self.path.join("cpu.max"),
            limits.cpu_quota.map(cpu_max_value),
        )?;
        Ok(())
    }

    /// Post-run evidence for the caps applied to this cgroup, read from the
    /// kernel's own event counters — the authoritative answer to "did the cap
    /// actually fire?", never an inference from an exit code or signal.
    ///
    /// Reads only the axes `capped` says have carried a cap (an uncapped axis has
    /// nothing to fire, so it is `NotTripped` without touching the filesystem), and
    /// only ever *reads*: no signal, no kill, no write, so calling this cannot
    /// perturb teardown or kill-on-drop whenever the caller asks. Counters live in
    /// the cgroup dir, which survives until `Drop` removes it, so the evidence
    /// outlives the tree that produced it.
    ///
    /// Which counter, and why exactly that one:
    ///
    /// - **memory** — `memory.events`' `oom`: the number of times *this* cgroup's
    ///   usage reached **its own** `memory.max` and an allocation was about to fail.
    ///   Deliberately **not** `oom_kill`, which the kernel documents as processes of
    ///   this cgroup "killed by **any** kind of OOM killer" — a *global* (host
    ///   out-of-memory) kill of our child raises `oom_kill` here while our cap never
    ///   engaged, so keying the verdict on it would manufacture exactly the false
    ///   "your cap killed it" this type must never produce. `max` alone is also not
    ///   a trip: it means reclaim absorbed the pressure at the boundary — the cap
    ///   working *without* stopping anything.
    /// - **processes** — `pids.events`' `max`: the number of times a fork was
    ///   refused because the process cap was hit. There is no non-cgroup way for
    ///   this counter to move.
    /// - **cpu** — `cpu.stat`'s `nr_throttled`: how many periods the quota made this
    ///   tree wait. A CPU cap throttles rather than kills, so this *is* the cap
    ///   engaging.
    ///
    /// Each counter is read from the `.local` file first (`memory.events.local`,
    /// `pids.events.local`, kernels that have them), falling back to the
    /// hierarchical file. Both are correct here. The per-spawn leaf sub-cgroups
    /// below this one ([`Leaves`]) do not change that: this backend enables no
    /// controller *in* them (it writes only its **parent**'s `subtree_control`, see
    /// [`enable_controllers`](Self::enable_controllers)), so a leaf carries no
    /// counters of its own and its members' charges — and the events they raise —
    /// land on this cgroup, which is exactly what the `.local` file reports. An
    /// *ancestor* cap cannot be misattributed to it either, because applying a cap at
    /// all requires our parent to be the real cgroup-v2 hierarchy root (see
    /// [`apply_limits`](Self::apply_limits)), which carries no caps of its own — but
    /// preferring the strictly-local file keeps the verdict sound even if a contained
    /// child manages to nest a cgroup of its own, with controllers, inside ours.
    ///
    /// A file or key that isn't there (an older kernel, a controller without
    /// bandwidth accounting, an unreadable cgroup) yields `Unknown`, never a "no".
    #[cfg(feature = "limits")]
    fn limit_evidence(&self, capped: CappedAxes) -> LimitEvidence {
        self.limit_evidence_with(capped, |path| std::fs::read_to_string(path))
    }

    /// [`limit_evidence`](Self::limit_evidence) parametrized over the counter-file
    /// reader — the injectable seam that lets tests drive every
    /// present/absent/unparsable combination without a real cgroup v2 mount, in the
    /// same style as [`members_with`](Self::members_with).
    #[cfg(feature = "limits")]
    fn limit_evidence_with(
        &self,
        capped: CappedAxes,
        read: impl Fn(&Path) -> io::Result<String>,
    ) -> LimitEvidence {
        let axis = |kind: LimitKind, files: &[&str], key: &str| -> LimitVerdict {
            // Never capped on this axis: nothing could have fired, and no read is
            // performed — the cost of evidence stays off groups that asked for no
            // caps at all.
            if !capped.has(kind) {
                return LimitVerdict::NotTripped;
            }
            for file in files {
                // The first file that reads decides: a present-but-zero counter is
                // an authoritative "did not fire", not a reason to try the next one.
                if let Ok(text) = read(&self.path.join(file)) {
                    return match flat_keyed_value(&text, key) {
                        Some(0) => LimitVerdict::NotTripped,
                        Some(_) => LimitVerdict::Tripped,
                        // The file exists but has no such key (a kernel that doesn't
                        // account it) — an honest gap, not a "no".
                        None => LimitVerdict::Unknown,
                    };
                }
            }
            LimitVerdict::Unknown
        };
        LimitEvidence::new(
            axis(
                LimitKind::Memory,
                &["memory.events.local", "memory.events"],
                "oom",
            ),
            axis(
                LimitKind::Processes,
                &["pids.events.local", "pids.events"],
                "max",
            ),
            axis(LimitKind::Cpu, &["cpu.stat"], "nr_throttled"),
        )
    }

    /// Enable each controller in `needed` that is not already present in `parent`'s
    /// `cgroup.subtree_control`, making the matching limit interface files
    /// (`memory.max`, …) appear in this child cgroup. Shared by
    /// [`apply_limits`](Self::apply_limits) (creation) and
    /// [`update_limits`](Self::update_limits) (live update) so the "no internal
    /// processes" gate and its honest off-root error stay identical on both paths.
    #[cfg(feature = "limits")]
    fn enable_controllers(&self, parent: &Path, needed: &[&str]) -> io::Result<()> {
        // Enable only the controllers not ALREADY in the parent's
        // `subtree_control`. When they are present (the parent is the *real*
        // cgroup-v2 hierarchy root — the one cgroup that may carry controllers
        // despite holding this process), the write is skipped, and that is also
        // the only way the limit interface files (`memory.max`, …) can already
        // exist in our child. Otherwise the write below enables them. Writing
        // `subtree_control` while the parent holds member processes (this process
        // lives there) is forbidden by cgroup v2's "no internal processes" rule
        // and fails `EBUSY` for any non-root cgroup — a cgroup *namespace* root
        // does NOT count (it only virtualizes the view; the cgroup still isn't the
        // real root), so a private-cgroupns container EBUSYs just like a systemd
        // scope. processkit does not migrate this process out of its cgroup to
        // work around that, so when controllers are missing the write fails
        // loudly with an honest error.
        let enabled =
            std::fs::read_to_string(parent.join("cgroup.subtree_control")).unwrap_or_default();
        let to_enable = controllers_to_enable(needed, &enabled);
        if !to_enable.is_empty() {
            let spec = to_enable
                .iter()
                .map(|c| format!("+{c}"))
                .collect::<Vec<_>>()
                .join(" ");
            let file = parent.join("cgroup.subtree_control");
            cgroup_write(&file, &spec).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "enabling cgroup controllers ({spec}) in {} failed: {e}. cgroup v2's \
                         'no internal processes' rule forbids enabling controllers in a cgroup \
                         that holds member processes (except the real hierarchy root), and this \
                         process is a member of that cgroup — so processkit's resource limits \
                         apply only when this process runs at the real cgroup-v2 root, not under \
                         a systemd session/scope/service nor an ordinary (private-cgroupns) \
                         container, both of which place it in a non-root cgroup. (A cgroup \
                         namespace root does not count — it only virtualizes the view.) processkit \
                         does not migrate your process into a sub-cgroup to satisfy the rule; \
                         arrange that externally (the create-leaf/migrate-self/enable dance) if \
                         you need limits there.",
                        file.display()
                    ),
                )
            })?;
        }
        Ok(())
    }

    /// Read the live member pids of the **whole job** — this cgroup's own members
    /// plus those of every live per-spawn leaf under it ([`Leaves`]). A removed
    /// cgroup is empty; other read failures leave its state unknown and are surfaced
    /// to the caller.
    fn members(&self) -> io::Result<Vec<i32>> {
        self.members_with(|path| std::fs::read_to_string(path))
    }

    /// `members()` parametrized over the `cgroup.procs` reader — the injectable
    /// seam that lets tests exercise the success/`NotFound`/`PermissionDenied`/I/O
    /// error mapping below, and that every other fail-safe decision in this type
    /// (`is_empty`, `signal`, `kill`, `stats`) is threaded through so *their* tests
    /// can drive the same error paths without a real cgroup filesystem. `Fn` (not
    /// `FnOnce`): the legacy kill sweep below calls this in a bounded retry loop.
    ///
    /// **One pass over the whole job.** `cgroup.procs` lists a cgroup's *own*
    /// members and never a descendant's, so the job's membership is the union of its
    /// own file and one per live per-spawn leaf ([`Leaves`]) — one read per cgroup
    /// of the job, for the whole snapshot. That keeps the batched identity-safe
    /// disciplines built on this (pin every member, then take **one** membership
    /// pass, then reconfirm each pinned member against it — see
    /// [`signal_with_seams`](Self::signal_with_seams) and
    /// [`stats_with_seams`](Self::stats_with_seams)) at two passes for a whole
    /// broadcast rather than two per pid, and rescans nothing per pid.
    ///
    /// The result is a **set**: sorted, and de-duplicated because the union is
    /// several files read in sequence rather than one atomic snapshot, so a member
    /// that moves between two of this job's own cgroups mid-pass must not be folded
    /// twice by `stats` or signalled twice by a broadcast. A leaf whose directory is
    /// already gone reads as empty (`NotFound`), exactly as a removed job cgroup
    /// does; any other read failure propagates, since an unreadable membership is
    /// unknown, not "no processes".
    fn members_with(&self, read: impl Fn(&Path) -> io::Result<String>) -> io::Result<Vec<i32>> {
        let mut pids = read_member_pids(&self.path, &read)?;
        for dir in self.leaf_dirs() {
            pids.extend(read_member_pids(&dir, &read)?);
        }
        pids.sort_unstable();
        pids.dedup();
        Ok(pids)
    }

    /// The live members enriched with ppid / `comm` / start time, each read from a
    /// single `/proc/<pid>/stat` (see [`crate::sys::procfs::read_stat_meta`]) so
    /// the three fields describe one consistent instant. A member gone before its
    /// stat read is skipped — a vanished process is omitted, not a fabricated
    /// record, and never fails the whole snapshot. The `cgroup.procs` read failing
    /// still propagates as `Err` (via [`members`](Self::members)): an unreadable
    /// membership is unknown, not "no processes".
    ///
    /// Unlike the identity-safe [`stats`](Self::stats) fold — which pins and
    /// reconfirms each pid against a re-read of `cgroup.procs` before folding its
    /// *numeric* counters, so a recycled pid's CPU/RSS is never misattributed —
    /// this snapshot follows the point-in-time contract of
    /// [`members`](Self::members): the ppid/comm/start-time it reports are advisory
    /// metadata, and a pid recycled between the `cgroup.procs` read and its stat
    /// read carries the same best-effort exposure `members` already has. The single
    /// atomic stat read keeps the *three fields of one pid* internally consistent.
    #[cfg(feature = "process-control")]
    fn members_info(&self) -> io::Result<Vec<MemberInfo>> {
        let pids = self.members()?;
        Ok(pids
            .into_iter()
            .filter_map(|pid| {
                crate::sys::procfs::read_stat_meta(pid as u32)
                    .map(|m| MemberInfo::new(pid as u32, m.ppid, m.comm, m.starttime))
            })
            .collect())
    }

    /// `is_drained` (the [`GracefulTarget`](super::graceful::GracefulTarget) impl
    /// below) maps a read failure here to "not drained" (`unwrap_or(false)`), and
    /// `Job::drop`'s bounded wait treats it the same way — neither can take an
    /// injected reader (both signatures are fixed), so both are exercised
    /// directly against a real, permission-denied temporary directory in
    /// `fail_safe_tests` below rather than through the `_with` seam.
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.members()?.is_empty())
    }

    /// Sum per-process `/proc` counters (cpu time, peak memory) over the live
    /// members, **identity-safe against pid recycling**. Our cgroup has no
    /// controllers enabled (so `cgroup.kill` works without the "no internal
    /// processes" rule), so cpu/memory aren't available from the cgroup itself.
    ///
    /// Note: `cgroup.procs` lists only *live* members — a process leaves it on
    /// **exit**, before it is reaped, so an unreaped zombie never appears there
    /// (per the kernel's cgroup-v2 docs: "a zombie process does not appear in
    /// cgroup.procs"). The count and the summed `/proc` counters therefore reflect
    /// live processes, not dead ones.
    ///
    /// The dangerous TOCTOU window is between reading `cgroup.procs` and reading a
    /// member's `/proc/<pid>/stat`: the member can exit, be reaped, and its pid be
    /// recycled by a process *outside* the cgroup, whose CPU/RSS would then be
    /// folded into the group snapshot. Each member is therefore folded through
    /// [`sample_member_identity_safe`], which pins the pid's start-time identity,
    /// reconfirms it is *still* a cgroup member, and reads the counters gated on
    /// that identity — so only data for members whose original identity **and**
    /// current membership are both confirmed at read time is summed. A member that
    /// merely exits (no recycle) is skipped cleanly, not folded as a stale value.
    ///
    /// A `cgroup.procs` read failure (EACCES/EIO/…) propagates as `Err` here — the
    /// initial member-list read via `?`, and a per-member membership reconfirm read
    /// via `MemberSample::Failed` — rather than being reported as an empty/partial
    /// group; an unreadable member list is unknown, not "no processes".
    #[cfg(feature = "stats")]
    fn stats(&self) -> io::Result<ProcessGroupStats> {
        self.stats_with(|path| std::fs::read_to_string(path))
    }

    /// `stats()` parametrized over the `cgroup.procs` reader — see
    /// [`members_with`](Self::members_with) — wired to the real `/proc` identity
    /// and metrics reads. The fold logic lives in
    /// [`stats_with_seams`](Self::stats_with_seams) so a seam test can drive the
    /// whole batch (pin → reconfirm → read) with injected identity/metrics seams
    /// instead of a real `/proc`.
    #[cfg(feature = "stats")]
    fn stats_with(
        &self,
        read: impl Fn(&Path) -> io::Result<String>,
    ) -> io::Result<ProcessGroupStats> {
        let mut stats = self.stats_with_seams(
            &read,
            |p| process_identity(p as u32),
            |p, id| process_metrics(p as u32, Some(id)),
        )?;
        // The counters the cgroup keeps itself, layered onto the per-member fold.
        // Deliberately after it: they are best-effort `Option`s (see
        // `container_counters_with`), so an absent controller file must not turn a
        // successful snapshot into an error, and the membership read — where an
        // unreadable answer *is* an error — has already had its say.
        let counters = self.container_counters_with(&read);
        stats.io_read_bytes = counters.io_read_bytes;
        stats.io_write_bytes = counters.io_write_bytes;
        stats.peak_process_count = counters.peak_process_count;
        Ok(stats)
    }

    /// The whole-tree counters the **cgroup itself** keeps, as opposed to the
    /// per-member `/proc` sums [`stats_with_seams`](Self::stats_with_seams) folds:
    /// `io.stat`'s read/write bytes and `pids.peak`'s high-water task count. Read
    /// through the same injectable reader as everything else here, so a test can
    /// drive every present/absent/unparsable combination without a real cgroup v2
    /// mount — the shape `limit_evidence_with` already uses for the events files.
    ///
    /// **Each field is a best-effort `Option`, never an error.** A controller file
    /// this backend never asked for is normally *absent*: the interface files of a
    /// controller appear in this cgroup only once that controller is enabled in the
    /// parent's `cgroup.subtree_control`, and this backend enables exactly the
    /// controllers a requested `ResourceLimits` needs (`memory`/`pids`/`cpu`, via
    /// `enable_controllers`) — never `io`, and never `pids` for a group that asked
    /// for no process cap. (Both are `limits`-gated, hence bare spans from this
    /// `stats`-gated doc.) Reporting `None` for what the host does not account is
    /// the whole contract; failing the snapshot over it would deny the caller the
    /// counts and sums that *did* read.
    ///
    /// Both files are read from this job's **own** cgroup directory and not from its
    /// per-spawn leaves ([`Leaves`]), which is what makes them whole-job numbers: no
    /// controller is enabled inside a leaf, so a leaf holds no counters of its own
    /// and its members' charges land here — the same reasoning `limit_evidence`
    /// records for the events files. For `pids.peak` the kernel is explicit about
    /// the scope on top of that: the pids controller counts this cgroup *and its
    /// descendants*.
    #[cfg(feature = "stats")]
    fn container_counters_with(
        &self,
        read: impl Fn(&Path) -> io::Result<String>,
    ) -> ContainerCounters {
        // `io.stat` is nested-keyed: one line per device, `<major:minor>` followed by
        // `key=value` pairs. A tree that has touched several block devices has a line
        // for each, so the tree's bytes are the sum down the column.
        let io = read(&self.path.join("io.stat")).ok();
        let (io_read_bytes, io_write_bytes) = io.as_deref().map_or((None, None), |text| {
            (
                nested_keyed_sum(text, "rbytes"),
                nested_keyed_sum(text, "wbytes"),
            )
        });
        // `pids.peak` is a single number (the pids controller's high-water mark).
        // Not `pids.current`, which is *now* — the same distinction
        // `active_process_count` already covers.
        let peak_process_count = read(&self.path.join("pids.peak"))
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .and_then(|peak| usize::try_from(peak).ok());
        ContainerCounters {
            io_read_bytes,
            io_write_bytes,
            peak_process_count,
        }
    }

    /// The batched identity-safe stats fold, factored over *all* its seams (the
    /// `cgroup.procs` reader, the identity capture, the metrics read) so a seam
    /// test can drive the full pin → reconfirm → read path — and count reads —
    /// without a real `/proc` or cgroup.
    ///
    /// Batched exactly like [`signal_with_seams`](Self::signal_with_seams): pin
    /// (capture the start-time identity of) **every** member first, then take
    /// exactly **one** membership pass ([`members_with`](Self::members_with) — one
    /// `cgroup.procs` read per cgroup of the job, its own plus each live per-spawn
    /// leaf), then reconfirm each pinned member against that single snapshot and read
    /// its counters gated on the pinned identity (`sample_pinned`). The lone
    /// reconfirm pass lands after every capture, so it is after *each* member's pin —
    /// the same race-freedom order the per-member [`sample_member_identity_safe`]
    /// enforces, now at a constant number of passes over the membership instead of
    /// one per pid.
    ///
    /// `active_process_count` reflects the *initial* member list, as before: a
    /// member that later turns out gone/recycled still counted as live at snapshot
    /// time. An unreadable membership — the initial read (via `?`) or the single
    /// reconfirm read — surfaces as `Err` rather than a silently-short sum.
    ///
    /// This is the **per-member fold only**: the fields fed by the cgroup's own
    /// counters (I/O bytes, peak process count) come back `None` from here and are
    /// layered on by [`stats_with`](Self::stats_with) from
    /// [`container_counters_with`](Self::container_counters_with), which is where
    /// their tests live. Keeping them out of this function is what leaves its read
    /// count — the O(1)-passes-per-tree property asserted below — about `cgroup.procs`
    /// alone.
    #[cfg(feature = "stats")]
    fn stats_with_seams(
        &self,
        read: impl Fn(&Path) -> io::Result<String>,
        capture_identity: impl Fn(i32) -> Option<ProcIdentity>,
        read_metrics: impl Fn(i32, ProcIdentity) -> ProcMetrics,
    ) -> io::Result<ProcessGroupStats> {
        let pids = self.members_with(&read)?;
        let active = pids.len();
        // 1. Pin (capture the start-time identity of) each member before the
        //    reconfirm read. A member gone/unreadable before its pin (None) is a
        //    benign skip that contributes nothing.
        let mut pinned: Vec<(i32, ProcIdentity)> = Vec::new();
        for pid in pids {
            if let Some(id) = capture_identity(pid) {
                pinned.push((pid, id));
            }
        }
        let mut cpu = Duration::ZERO;
        let mut have_cpu = false;
        let mut mem = 0u64;
        let mut have_mem = false;
        let mut last_err = None;
        // 2. One reconfirm pass for the whole fold — one per batch, not one per
        //    pinned pid — taken after every capture above. Skipped when nothing was
        //    pinned (an all-gone or empty group), matching the old per-member path.
        if !pinned.is_empty() {
            match self.members_with(&read) {
                Ok(snapshot) => {
                    let snapshot: std::collections::HashSet<i32> = snapshot.into_iter().collect();
                    // 3. Reconfirm each pinned member against the single snapshot,
                    //    then read its counters gated on the pinned identity.
                    for (pid, id) in pinned {
                        match sample_pinned(pid, id, |p| Ok(snapshot.contains(&p)), &read_metrics) {
                            MemberSample::Folded(m) => {
                                if let Some(c) = m.cpu_time {
                                    // Saturating: summing many members' CPU time
                                    // could in principle overflow `Duration`; clamp
                                    // rather than panic.
                                    cpu = cpu.saturating_add(c);
                                    have_cpu = true;
                                }
                                if let Some(p) = m.peak_memory_bytes {
                                    mem = mem.saturating_add(p);
                                    have_mem = true;
                                }
                            }
                            // Gone, or its pid left the cgroup (possibly recycled
                            // outside) — contributes nothing, but is not a failure.
                            MemberSample::Skipped => {}
                            // A membership reconfirm read failed: the snapshot is
                            // unreliable. (Infallible against the in-memory snapshot
                            // here; the reconfirm-read failure is caught below.)
                            MemberSample::Failed(e) => last_err = Some(e),
                        }
                    }
                }
                // Reconfirm membership unknown: surface it rather than a
                // silently-short sum, mirroring the initial `members_with(&read)?`.
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            return Err(e);
        }
        Ok(ProcessGroupStats {
            active_process_count: active,
            total_cpu_time: have_cpu.then_some(cpu),
            peak_memory_bytes: have_mem.then_some(mem),
            // The cgroup's own counters are not part of this per-member fold;
            // `stats_with` layers them on (see `container_counters_with`).
            io_read_bytes: None,
            io_write_bytes: None,
            peak_process_count: None,
        })
    }

    /// Send `sig` to every current member (the graceful SIGTERM tier and the
    /// public signal broadcast). Best-effort in *aggregate*: an empty cgroup is
    /// trivially signalled, and a member that exits mid-broadcast is a benign
    /// no-op — but each individual delivery is **identity-safe** against pid
    /// recycling (see [`signal_with`](Self::signal_with) and
    /// [`deliver_identity_safe`]).
    ///
    /// The old raw `kill(pid, sig)` had a destructive TOCTOU window: between
    /// reading `cgroup.procs` and the `kill`, a member could exit, be reaped, and
    /// its pid be recycled by an unrelated process *outside* the cgroup, which then
    /// received `sig`. That is now closed by pinning each pid with a pidfd
    /// (`pidfd_open`) and delivering through `pidfd_send_signal`, which can only
    /// ever reach the pinned task — never a recycled pid — after reconfirming the
    /// pid is still a cgroup member. `cgroup.kill` (whole-subtree SIGKILL, used by
    /// [`kill`](Self::kill)) stays the path for SIGKILL teardown because a
    /// broadcast — however identity-safe per pid — can still miss a process forked
    /// after the membership snapshot; only the atomic whole-subtree operation
    /// covers that.
    fn signal(&self, sig: i32) -> io::Result<()> {
        self.signal_with(sig, |path| std::fs::read_to_string(path))
    }

    /// `signal()` parametrized over the `cgroup.procs` reader — see
    /// [`members_with`](Self::members_with) — wired to the real pidfd syscalls.
    /// The delivery logic lives in [`signal_with_seams`](Self::signal_with_seams)
    /// so a seam test can drive the whole batch (pin → reconfirm → send) with
    /// injected `pidfd_open`/`pidfd_send_signal` instead of touching real
    /// processes.
    fn signal_with(&self, sig: i32, read: impl Fn(&Path) -> io::Result<String>) -> io::Result<()> {
        self.signal_with_seams(sig, read, pidfd_open, pidfd_send_signal)
    }

    /// The batched identity-safe broadcast, factored over *all three* seams (the
    /// `cgroup.procs` reader plus the pidfd `open`/`send` syscalls) so tests can
    /// exercise the full pin → reconfirm → send path — and count reads — without a
    /// real pidfd or cgroup. A member-list read failure returns `Err` (via `?`)
    /// *before* anything is pinned, so no signal is ever sent when the initial
    /// membership is unknown.
    ///
    /// **Why one membership pass for the whole batch, not one per pid.** The
    /// identity-safe argument (see [`deliver_identity_safe`]) needs only that each
    /// pid's membership reconfirm happens *after* that pid was pinned — not that
    /// every pid gets its own fresh read. So this pins **every** current member first
    /// (`pin_member`/`pidfd_open`), then takes exactly **one** membership pass
    /// ([`members_with`](Self::members_with) — one `cgroup.procs` read per cgroup of
    /// the job, its own plus each live per-spawn leaf), and reconfirms each pinned
    /// pid against that single snapshot before sending
    /// (`deliver_pinned`/`pidfd_send_signal`). The lone reconfirm pass lands strictly
    /// after every pin, so it is after *each* pid's pin — the race-freedom order is
    /// preserved verbatim, at a constant number of passes over an O(n)-pid membership
    /// instead of one pass per pid.
    ///
    /// Holding all N pidfds open across the single pass (rather than one at a time)
    /// is the deliberate cost of that ordering: a recycled pid must not be pinnable
    /// between the read and the send, so the pin has to precede the shared read.
    /// A process tree's N is bounded by `pids.max`, well under `RLIMIT_NOFILE`.
    ///
    /// A kernel without pidfd (< 5.3) makes `pin_member` fail safe with an honest
    /// error rather than silently downgrading to a racy raw kill.
    fn signal_with_seams<H>(
        &self,
        sig: i32,
        read: impl Fn(&Path) -> io::Result<String>,
        open: impl Fn(i32) -> io::Result<H>,
        send: impl Fn(&H, i32) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut last_err = None;
        // 1. Pin every current member *before* the reconfirm read below, so that
        //    read lands after each pid's pin (the race-freedom order). A pin that
        //    races the member's exit (ESRCH) is a benign no-op; a kernel without
        //    pidfd (ENOSYS) or another error is surfaced.
        let mut pinned: Vec<(i32, H)> = Vec::new();
        for pid in self.members_with(&read)? {
            match pin_member(pid, &open) {
                Pinned::Handle(handle) => pinned.push((pid, handle)),
                Pinned::Gone => {}
                Pinned::Failed(err) => last_err = Some(err),
            }
        }
        // 2. One reconfirm membership pass for the whole batch — one per batch, not
        //    one per pinned pid — taken after every pin above. Skipped when nothing
        //    was pinned (an all-gone or empty group), matching the old per-pid path,
        //    which only re-read once it had a live pin to reconfirm.
        if !pinned.is_empty() {
            match self.members_with(&read) {
                Ok(snapshot) => {
                    let snapshot: std::collections::HashSet<i32> = snapshot.into_iter().collect();
                    // 3. Reconfirm each pinned pid against the single snapshot, then
                    //    send through its pinned handle. A pid absent from the
                    //    snapshot left the cgroup (possibly recycled outside) and is
                    //    skipped without a send.
                    for (pid, handle) in pinned {
                        match deliver_pinned(
                            pid,
                            sig,
                            &handle,
                            |p| Ok(snapshot.contains(&p)),
                            &send,
                        ) {
                            Delivery::Delivered | Delivery::Skipped => {}
                            Delivery::Failed(err) => last_err = Some(err),
                        }
                    }
                }
                // Reconfirm membership unknown (an unreadable `cgroup.procs`): fail
                // safe — never send when we cannot confirm the pinned pids still
                // belong — and surface the error rather than a false success.
                Err(err) => last_err = Some(err),
            }
        }
        match last_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Freeze (`true`) or thaw (`false`) the whole subtree.
    ///
    /// Prefers `cgroup.freeze` (cgroup v2 core file, kernel ≥ 5.2): one write covers
    /// the whole subtree — this cgroup *and every descendant*, so the per-spawn
    /// leaves ([`Leaves`]) need no write of their own — and needs no controllers, the
    /// same family as the `cgroup.kill` file used for teardown. (The kernel applies
    /// the freeze shortly after the write returns.) On kernels without it, fall back
    /// to per-pid `SIGSTOP`/`SIGCONT`, mirroring the `cgroup.kill` fallback idiom.
    ///
    /// The fallback routes through [`signal`](Self::signal), so it inherits the
    /// same identity-safe pidfd delivery — a recycled pid outside the cgroup is
    /// never `SIGSTOP`/`SIGCONT`'d, exactly as for `SIGTERM`. The only kernels that
    /// need this fallback (< 5.2, no `cgroup.freeze`) also lack `pidfd_open`
    /// (< 5.3), so there the primitive fails safe with an honest error rather than
    /// a racy raw kill — suspend/resume via the per-pid tier is unavailable on such
    /// ancient kernels, by design.
    #[cfg(feature = "process-control")]
    fn freeze(&self, frozen: bool) -> io::Result<()> {
        let val: &[u8] = if frozen { b"1" } else { b"0" };
        match cgroup_write(&self.path.join("cgroup.freeze"), val) {
            Ok(()) => return Ok(()),
            // Only the file being ABSENT means "kernel < 5.2" → fall back to the
            // per-pid SIGSTOP/SIGCONT path. Any other error (EACCES/EBUSY on a
            // restricted delegated cgroup, EIO, …) is a real failure on a file
            // that *exists*: surface it rather than silently degrading to the
            // racy per-pid path on a modern kernel.
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            Err(_) => {} // NotFound → no cgroup.freeze; use the fallback below.
        }
        let sig = if frozen { libc::SIGSTOP } else { libc::SIGCONT };
        self.signal(sig)
    }

    fn kill(&self) -> io::Result<()> {
        self.kill_with(|path| std::fs::read_to_string(path))
    }

    /// `kill()` parametrized over the `cgroup.procs` reader used by the legacy
    /// (pre-5.14) SIGKILL-sweep fallback below — see [`members_with`](Self::members_with).
    /// A persistent read error keeps the bounded sweep from ever observing an
    /// empty member list, so it runs to the deadline and the final drain check
    /// below propagates that error instead of a false `Ok(())`.
    fn kill_with(&self, read: impl Fn(&Path) -> io::Result<String>) -> io::Result<()> {
        self.kill_with_seams(read, pidfd_open, pidfd_send_signal)
    }

    /// The legacy/restricted fallback factored over the pidfd seams so its
    /// destructive SIGKILL path uses the same identity-safe gate as the
    /// graceful per-member signal path. A raw `kill(pid, SIGKILL)` is not a
    /// safe fallback here: the pid may have been recycled after the initial
    /// `cgroup.procs` snapshot.
    fn kill_with_seams<H>(
        &self,
        read: impl Fn(&Path) -> io::Result<String>,
        open: impl Fn(i32) -> io::Result<H>,
        send: impl Fn(&H, i32) -> io::Result<()>,
    ) -> io::Result<()> {
        // `cgroup.kill` (kernel ≥ 5.14): write "1" to SIGKILL the whole subtree
        // atomically — this cgroup *and every descendant*, so one write covers every
        // per-spawn leaf (`Leaves`) as well, which is why the whole-job verbs never
        // walk them.
        //
        // Unlike `freeze` (which surfaces a non-`NotFound` write error rather than
        // silently degrading a *suspend* to the racy per-pid path), `kill` falls
        // back on *any* failure here on purpose: the fallback below is a *complete*
        // alternative teardown (freeze + per-pid SIGKILL sweep) that ends in the
        // drain check and surfaces a genuine failure itself. So on
        // a non-version write error (e.g. EACCES on a restricted delegated cgroup)
        // attempting the sweep maximizes the chance of actually killing the tree,
        // and a truly un-killable tree is still reported by the drain check — there
        // is no silent degrade to document away.
        if cgroup_write(&self.path.join("cgroup.kill"), b"1").is_ok() {
            return Ok(());
        }
        // Older kernels (no `cgroup.kill`): a per-pid SIGKILL sweep. First FREEZE
        // the subtree (cgroup v2 `cgroup.freeze`, kernel ≥ 5.2; best-effort — the
        // write is a no-op if absent) so a fork bomb can't out-spawn the sweep:
        // frozen tasks can't fork. Crucially this relies on the cgroup *v2*
        // freezer being killable — "processes in the frozen cgroup can be killed
        // by a fatal signal" (kernel cgroup-v2 docs), so each SIGKILL'd task wakes,
        // takes the fatal signal, and leaves `cgroup.procs` even while the subtree
        // is still frozen (the sweep below therefore drains and breaks normally).
        // This is the deliberate v2 redesign: the v1 freezer blocked SIGKILL until
        // thaw — that hazard does NOT apply to `cgroup.freeze`.
        // Sleep between sweeps rather than busy-spin while the kernel reaps, and
        // bound it so teardown (incl. Drop) can never hang on un-reaped zombies.
        //
        // This fallback — hence this blocking `sleep` — is reachable only on a
        // kernel < 5.14 (no `cgroup.kill` file) or a write-restricted delegated
        // cgroup (the `cgroup.kill` write above fails with e.g. EACCES); on a
        // modern, non-restricted cgroup the atomic write above already returned.
        // `kill_all`/`Job::kill_all` is called synchronously from four ASYNC
        // paths — `stream::kill_via_weak` (streaming deadline),
        // `RunningProcess::arm_cancel_watchdog`'s cancel task,
        // `kill_tree`/`teardown_on_timeout` (bulk deadline/cancel), and
        // `Pipeline`'s `kill_all_stage_groups` (the chain-wide teardown killer
        // fired on cancellation and on `Pipeline::timeout` elapsing,
        // `pipeline.rs`) — none of which route through `spawn_blocking`, so on
        // a reachable config this loop stalls whatever tokio worker thread is
        // running the caller for up to ~100ms (this loop) plus the ~100ms
        // drain wait in `Job::drop` below if the same `Job` is then also
        // dropped synchronously.
        //
        // Accepted as a bounded, rare-path cost rather than routed through
        // `spawn_blocking`: on the vastly common case (kernel ≥ 5.14, standard
        // delegated cgroup) this branch is never taken at all, so
        // unconditionally wrapping every `kill_all()` call in `spawn_blocking`
        // would tax the atomic fast path (extra thread-pool dispatch latency,
        // plus a new call pattern with no existing precedent in this codebase)
        // to guard a ~100ms stall reachable only on legacy/restricted setups.
        // Unlike `Job::drop` (which *cannot* await — Rust's `Drop` is
        // inherently synchronous, so blocking there is unavoidable regardless
        // of caller), all four call sites above run inside `async fn`s/futures
        // and *could* in principle `.await` a `spawn_blocking` wrapper; this is
        // a deliberate choice to keep those paths simple, not a hard constraint
        // like `Job::drop`'s. Revisit (route through `spawn_blocking`) if a
        // legacy/restricted-cgroup deployment reports worker-thread starvation
        // under load.
        //
        // Whether the guard actually went in is remembered rather than discarded:
        // it is the thaw's *last-resort* answer to "is this cgroup frozen" — used
        // only on a host that will not let it read the freezer state back, since
        // reading `cgroup.freeze` answers that question for any freeze, not just
        // one this call put there (see `thaw_after_kill_sweep`). The freeze itself
        // stays best-effort for the sweep's own purposes — an absent file
        // (kernel < 5.2) or a refused write only means the fork-bomb guard is
        // unavailable, and the bounded sweep runs either way.
        let froze = cgroup_write(&self.path.join("cgroup.freeze"), b"1").is_ok();
        let mut last_delivery_error = None;
        for _ in 0..50 {
            if let Err(err) = self.signal_with_seams(libc::SIGKILL, &read, &open, &send) {
                // Keep trying: a member can disappear while another delivery
                // fails, and the final membership read remains authoritative
                // about whether teardown actually completed.
                last_delivery_error = Some(err);
            }
            if let Ok(members) = self.members_with(&read)
                && members.is_empty()
            {
                break;
            }
            // `Err(_)`: unknown state must not look drained. Continue the
            // bounded fallback in case the read failure is transient.
            std::thread::sleep(Duration::from_millis(2));
        }
        // Thaw: the freeze only halted forking DURING the sweep. Restore the cgroup
        // unfrozen so it stays reusable for further spawns (`kill_all` keeps the
        // group usable; a child spawned into a frozen cgroup would itself start
        // frozen and the spawn could block) — and so a SIGKILL'd-but-frozen
        // straggler can run its pending fatal signal and exit.
        // (This unconditionally clears any freeze a prior `suspend()` set; a kill
        // verb resurrecting-then-killing a deliberately-suspended group is benign.)
        //
        // Best-effort in *timing* exactly as before — one bounded retry (a further
        // ~2ms on this thread, plus a read of `cgroup.freeze`, and only on the
        // refused-thaw path), never a wait loop — but no longer best-effort in
        // *reporting*: a cgroup left frozen travels to the drain check below
        // instead of being dropped on the floor.
        let left_frozen = self.thaw_after_kill_sweep(froze);
        // Report a real drain failure instead of a false success, so the caller
        // knows the tree may still be alive — a fork bomb still out-spawning, or
        // un-reapable zombies (a D-state task ignores SIGKILL until it unblocks).
        match self.members_with(&read) {
            // Drained. Still not a success if the cgroup is left frozen: that is
            // not the reusable group `kill_all` promises, and an empty
            // `cgroup.procs` says nothing about the freezer. The report is confined
            // to this arm on purpose — with members still alive, the delivery/drain
            // failure below is both the more severe and the more actionable answer,
            // so the two pre-existing arms keep deciding exactly what they did.
            Ok(members) if members.is_empty() => match left_frozen {
                Some(err) => Err(err),
                None => Ok(()),
            },
            // The two "still populated" cases share one arm: an `if let` guard
            // would read more directly, but `if_let_guard` is unstable on this
            // crate's MSRV (`rust-version = "1.88"`), and the floor is verified
            // by the `msrv` CI job. Surface the last real delivery failure when
            // the sweep hit one, and the generic drain failure otherwise.
            Ok(_) => Err(last_delivery_error.unwrap_or_else(|| {
                io::Error::other(
                    "cgroup did not drain after the bounded SIGKILL sweep (kernel < 5.14 fallback)",
                )
            })),
            Err(e) => Err(e),
        }
    }

    /// Clear the freeze [`kill_with_seams`](Self::kill_with_seams) put on the
    /// subtree to protect its SIGKILL sweep, and report back the one outcome that
    /// must not be dressed up as a clean kill: a cgroup left **frozen**.
    ///
    /// What separates a refusal that matters from one that does not is whether the
    /// freezer is actually on when the clear is refused — which this asks the
    /// kernel, by reading `cgroup.freeze` back ([`frozen_now`]), rather than infers
    /// from `froze` (whether this call's own `cgroup.freeze` → 1 write landed):
    ///
    /// - **Refused over a cgroup that reads frozen.** The group stays frozen, and
    ///   that is not a group the caller can spawn into: cgroup v2 freezes a task
    ///   that joins a frozen cgroup, and this backend joins in a `pre_exec` hook, so
    ///   the next child would stop before it can `exec` rather than run. Reported —
    ///   whoever set that freeze. A freeze this call put in force is the common
    ///   case, but the same answer is owed for one an earlier
    ///   [`suspend`](Self::freeze) left standing, and for one this call reported
    ///   already: `ProcessGroup::kill_all` is documented idempotent, so a repeat
    ///   call over a still-frozen group — where this call's own freeze write is
    ///   refused too, exactly as the thaw is — has to reach the same verdict rather
    ///   than answer `Ok(())` over the state its predecessor refused to call clean.
    /// - **Refused over a cgroup that reads unfrozen** (a write-restricted
    ///   delegated cgroup refusing every control write — the very host whose refused
    ///   `cgroup.kill` selects this fallback — where the paired freeze never landed
    ///   either). Nothing is frozen, so a clear the same host refuses for the same
    ///   reason leaves the group exactly as this call found it. Not reported: the
    ///   per-pid sweep needs no cgroup write at all, so it is a complete teardown
    ///   there, and failing it would make `kill_all` permanently `Err` on those
    ///   hosts over a state that is not there.
    ///
    /// Only when that read gives no usable answer at all (the state is unreadable,
    /// or holds something neither `0` nor `1`) does `froze` decide, on the
    /// assumption that a freeze this call landed and could not clear is still in
    /// force. That fallback is the one place a verdict here rests on an inference
    /// rather than on the kernel's own word, and it keeps the pre-existing
    /// behaviour: an unreadable freezer is no reason to start reporting less than
    /// before.
    ///
    /// A `NotFound` from the *write* is excused ahead of any of that: either there
    /// is no `cgroup.freeze` file (kernel < 5.2, where the paired freeze was the
    /// same no-op) or the cgroup directory is already gone — neither leaves a freeze
    /// in force, and a state read would fail with the same `NotFound` and fall
    /// through to `froze`, which for a vanished cgroup would be exactly the wrong
    /// answer. This is the same absent-versus-refused discrimination
    /// [`freeze`](Self::freeze) makes, for the same reason.
    ///
    /// One retry, then report. What refuses this write does not heal by being
    /// waited on (a revoked delegation stays revoked), and the caller's thread is
    /// blocked throughout — `Job::drop` reaches here too — so the second attempt is
    /// a single extra write on the sweep's own 2ms cadence rather than a loop.
    fn thaw_after_kill_sweep(&self, froze: bool) -> Option<io::Error> {
        let path = self.path.join("cgroup.freeze");
        // The refusals that leave a freeze standing, told apart from those that
        // leave nothing behind to tell the caller about. The state is re-read per
        // refusal rather than once up front: each read is the freshest answer
        // available to the write it judges, and there are at most two of them, both
        // on a path that has already failed.
        let unrecovered = |written: io::Result<()>| match written {
            Ok(()) => None,
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => frozen_now(std::fs::read_to_string(&path), froze).then_some(e),
        };
        // Note the inversion the `?`s carry: `None` is the *good* answer here — no
        // freeze left in force, whether because it cleared, because there is no
        // such file, or because the freezer reads off — and it is both nothing to
        // report and nothing a retry could improve, so it short-circuits out of
        // this function before the sleep and the second write ever happen.
        unrecovered(cgroup_write(&path, b"0"))?;
        // Only a freeze still standing reaches here, and gets the one extra try.
        std::thread::sleep(Duration::from_millis(2));
        let err = unrecovered(cgroup_write(&path, b"0"))?;
        // Keep the refusal's own `ErrorKind` so the crate classifies a permission
        // problem as one (`ErrorReason::Io` → `ErrorKind::PermissionDenied`),
        // matching what a refused `suspend` publishes for the identical write, and
        // carry its `Display` — which spells out `os error <n>` — inside the
        // explanation of what the caller is now holding.
        Some(io::Error::new(
            err.kind(),
            format!(
                "the process tree was killed and the cgroup drained, but the freezer — set to keep \
                 the sweep ahead of new forks — could not be cleared ({err}); the cgroup at {} is \
                 left FROZEN and is not usable for further spawns — cgroup v2 \
                 freezes a task that joins a frozen cgroup, and this backend joins one before \
                 `exec`, so the next child would stop instead of running. Clear `cgroup.freeze` \
                 (the write `ProcessGroup::resume` makes, where that feature is enabled) before \
                 spawning into this group again",
                self.path.display()
            ),
        ))
    }
}

/// Is the freezer on? `state` is a read of this cgroup's own `cgroup.freeze`, and
/// `froze` the caller's fallback belief for when that read does not answer.
///
/// Used by [`thaw_after_kill_sweep`](Cgroup::thaw_after_kill_sweep) on the one path
/// where the answer changes what the caller is told, so the cost — a single extra
/// `read` — is paid only after a write has already been refused.
///
/// `cgroup.freeze` is this cgroup's **own** freezer setting, which is exactly the
/// state a refused thaw fails to change and the one the error's remedy names. It is
/// deliberately not `cgroup.events`' `frozen` field: that reports the *effective*
/// state, which an ancestor's freeze also turns on — a condition no teardown here
/// set, none can clear, and which the atomic `cgroup.kill` path does not report
/// either. Reading it would make this fallback answer for a different, wider
/// question than the one it can act on.
///
/// A value that is neither `0` nor `1` is treated exactly like an unreadable one:
/// this is a two-valued kernel file, so anything else means the answer did not come
/// from the file this code thinks it is reading.
fn frozen_now(state: io::Result<String>, froze: bool) -> bool {
    match state.as_deref().map(str::trim) {
        Ok("1") => true,
        Ok("0") => false,
        _ => froze,
    }
}

impl super::graceful::GracefulTarget for Cgroup {
    fn signal_all(&self, signal: i32) -> super::graceful::SoftDelivery {
        // Best-effort: a delivery failure (a member that exited, EPERM) doesn't
        // stop the graceful tier from proceeding to poll — the verdict is recorded
        // only for the report. An `Ok` sweep (including an empty cgroup) is `Sent`;
        // a surfaced send failure is `Failed`.
        match self.signal(signal) {
            Ok(()) => super::graceful::SoftDelivery::Sent,
            Err(_) => super::graceful::SoftDelivery::Failed,
        }
    }

    fn is_drained(&self) -> bool {
        self.is_empty().unwrap_or(false)
    }

    fn alive_count(&self) -> Option<usize> {
        // The whole tree's live members (the job's `cgroup.procs` plus each leaf's),
        // matching `members()`. A removed cgroup reads empty (`Some(0)`); an
        // unreadable membership is unknown, reported `None` rather than a false 0 —
        // the same fail-safe `is_drained` applies (there mapped to "not drained").
        self.members().ok().map(|members| members.len())
    }

    fn hard_kill(&self) -> io::Result<()> {
        self.kill()
    }
}

/// The classified outcome of one identity-safe per-member delivery attempt (see
/// [`deliver_identity_safe`]). Not a bare `io::Result`: "the member is gone" and
/// "the pid left the cgroup, so it was deliberately skipped" are both success for
/// the broadcast, yet must be distinguishable from a real delivery failure that
/// has to surface.
enum Delivery {
    /// The signal reached the confirmed member, or a benign exit race made it a
    /// no-op — either the target exited before we could pin it, or the *pinned*
    /// task exited before the send (an ESRCH that pidfd guarantees is our target's
    /// own exit, never a signal leaked to a recycled pid). The intended end state
    /// holds; nothing to surface.
    Delivered,
    /// The pinned pid was no longer a member when we reconfirmed: its number may
    /// have been recycled by a process *outside* the cgroup, so we refused to
    /// signal it. No signal was sent.
    Skipped,
    /// A real failure to surface: `EPERM` (a member that changed uid, or a
    /// seccomp/container policy), an unreadable membership (fail-safe: never signal
    /// when we cannot confirm the target still belongs), or a kernel lacking pidfd
    /// (fail-safe: refuse to downgrade to a racy raw kill).
    Failed(io::Error),
}

/// The outcome of pinning a single member with [`pin_member`] — step 1 of the
/// identity-safe delivery, split out so the batched broadcast
/// ([`signal_with_seams`](Cgroup::signal_with_seams)) can pin **every** member
/// *before* the one shared reconfirm read.
enum Pinned<H> {
    /// The exact task currently at `pid` was pinned; its handle drives the send.
    Handle(H),
    /// The member was already gone before we could pin it (an `ESRCH` from
    /// `open`/`pidfd_open`) — the intended end state (gone) already holds, benign,
    /// exactly like an `ESRCH` from the old raw `kill`. No send, and membership is
    /// not even consulted.
    Gone,
    /// A real pin failure to surface: no pidfd on this kernel (< 5.3) or a seccomp
    /// filter blocking the syscall (`ENOSYS` → the honest [`pidfd_unsupported`]
    /// error rather than a racy raw-kill downgrade), or any other `open` error.
    Failed(io::Error),
}

/// Step 1 of the identity-safe delivery: **pin** the exact task currently running
/// as `pid` (a pidfd in production). From here a later send through the returned
/// handle can only ever reach *this* task — never a process that recycles the
/// number. Split from the reconfirm+send ([`deliver_pinned`]) so the batched
/// broadcast pins all members first and then reads `cgroup.procs` once, keeping
/// the race-freedom order (each reconfirm strictly after that pid's pin) at O(1)
/// reads instead of O(n).
fn pin_member<H>(pid: i32, open: impl Fn(i32) -> io::Result<H>) -> Pinned<H> {
    match open(pid) {
        Ok(handle) => Pinned::Handle(handle),
        Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Pinned::Gone,
        Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => Pinned::Failed(pidfd_unsupported()),
        Err(e) => Pinned::Failed(e),
    }
}

/// Steps 2–3 of the identity-safe delivery, against a pid already pinned by
/// [`pin_member`]: **reconfirm** membership (read *after* the pin — the caller
/// guarantees that order, whether one read per pid or one shared read for a whole
/// batch), then **send** through the pinned `handle`.
///
/// If the pin captured a process that had already recycled `pid` (the original
/// member exited in the snapshot→pin window), that impostor is not a member of our
/// cgroup, so `still_member` reports `false` and we skip without sending. A send
/// reaches a live process only if the pinned task is still alive, in which case it
/// has held `pid` continuously since the pin (a live process keeps its pid), so it
/// *is* the process the reconfirm read at `pid` — and the reconfirm only let us
/// proceed if that process was a member. If the pinned task instead exited, the
/// send is a benign `ESRCH`, never a hit on whoever recycled the number.
fn deliver_pinned<H>(
    pid: i32,
    sig: i32,
    handle: &H,
    still_member: impl Fn(i32) -> io::Result<bool>,
    send: impl Fn(&H, i32) -> io::Result<()>,
) -> Delivery {
    // 2. Reconfirm membership *after* pinning.
    match still_member(pid) {
        Ok(true) => {}
        // The pinned pid left the cgroup — its number may have been recycled by a
        // process outside our tree. Refuse to signal it.
        Ok(false) => return Delivery::Skipped,
        // Membership unknown (an unreadable `cgroup.procs`): never signal when we
        // cannot confirm the target still belongs to the cgroup.
        Err(e) => return Delivery::Failed(e),
    }
    // 3. Deliver through the pinned handle — the pinned task or nothing.
    match send(handle, sig) {
        Ok(()) => Delivery::Delivered,
        // The pinned target exited between the reconfirm and the send. pidfd
        // guarantees this `ESRCH` is *our* target's exit, never a signal that
        // leaked to a recycled pid — so it is benign.
        Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Delivery::Delivered,
        Err(e) if e.raw_os_error() == Some(libc::ENOSYS) => Delivery::Failed(pidfd_unsupported()),
        // A real delivery failure (EPERM, …): surface it, never read as success.
        Err(e) => Delivery::Failed(e),
    }
}

/// The identity-safe per-member signal primitive: pin → reconfirm → send for a
/// *single* pid, the composition of [`pin_member`] and [`deliver_pinned`]. The
/// order is what makes it race-free; see those two for the full argument. The
/// production broadcast batches the pins ahead of one shared reconfirm read
/// ([`signal_with_seams`](Cgroup::signal_with_seams)); this single-pid composition
/// keeps the race-freedom logic exercised end-to-end by the seam tests — its only
/// caller — so it carries `allow(dead_code)` outside `cfg(test)`.
#[cfg_attr(not(test), allow(dead_code))]
fn deliver_identity_safe<H>(
    pid: i32,
    sig: i32,
    open: impl Fn(i32) -> io::Result<H>,
    still_member: impl Fn(i32) -> io::Result<bool>,
    send: impl Fn(&H, i32) -> io::Result<()>,
) -> Delivery {
    // 1. Pin the exact task currently at `pid`.
    let handle = match pin_member(pid, open) {
        Pinned::Handle(handle) => handle,
        Pinned::Gone => return Delivery::Delivered,
        Pinned::Failed(e) => return Delivery::Failed(e),
    };
    // 2–3. Reconfirm membership *after* the pin, then send.
    deliver_pinned(pid, sig, &handle, still_member, send)
}

/// The classified outcome of one identity-safe per-member metrics fold (see
/// [`sample_member_identity_safe`]) — the stats analogue of [`Delivery`]. "The
/// member is gone / its pid left the cgroup" is a benign skip that contributes
/// nothing to the sum, distinct from a real membership-read failure that must
/// surface rather than silently shorten the aggregate.
#[cfg(feature = "stats")]
enum MemberSample {
    /// The pinned member was confirmed still present in the cgroup as the same
    /// process; fold these counters (themselves possibly all-`None` for a member
    /// whose `/proc` counters could not be read).
    Folded(ProcMetrics),
    /// The member was gone, or its pid left the cgroup (possibly recycled by a
    /// process *outside* the tree) — no counters folded, but not a failure.
    Skipped,
    /// A membership reconfirm read failed: never fold when the membership is
    /// unknown; surface it so the snapshot is not a silently-short sum.
    Failed(io::Error),
}

/// Steps 2–3 of the identity-safe fold, against a pid whose start-time identity
/// `id` was already pinned by `capture_identity`: **reconfirm** membership (read
/// *after* the pin — the caller guarantees that order, whether one read per pid or
/// one shared read for a whole batch), then read the counters **gated on the
/// pinned identity**. The stats analogue of [`deliver_pinned`], split out so the
/// batched fold ([`stats_with_seams`](Cgroup::stats_with_seams)) can capture every
/// member's identity *before* the one shared reconfirm read.
///
/// A recycle *after* the reconfirm makes the identity no longer match, so
/// `read_metrics` (production `process_metrics`) returns the all-`None` default
/// (contributing nothing) rather than a stranger's CPU/RSS. The folded counters
/// therefore only carry non-default values while the pid still carries the pinned
/// identity — i.e. the same process the reconfirm confirmed was a member.
#[cfg(feature = "stats")]
fn sample_pinned(
    pid: i32,
    id: ProcIdentity,
    still_member: impl Fn(i32) -> io::Result<bool>,
    read_metrics: impl Fn(i32, ProcIdentity) -> ProcMetrics,
) -> MemberSample {
    // 2. Reconfirm membership *after* pinning.
    match still_member(pid) {
        Ok(true) => {}
        // Left the cgroup — its number may have been recycled by a process outside
        // the tree; refuse to fold its counters.
        Ok(false) => return MemberSample::Skipped,
        // Membership unknown (an unreadable `cgroup.procs`): never fold when we
        // cannot confirm the target still belongs to the cgroup.
        Err(e) => return MemberSample::Failed(e),
    }
    // 3. Read the counters gated on the pinned identity.
    MemberSample::Folded(read_metrics(pid, id))
}

/// The identity-safe per-member metrics fold: pin → reconfirm → read for a
/// *single* member, the composition of an identity capture and [`sample_pinned`].
/// The order is what makes it race-free; see [`sample_pinned`] for the argument.
/// The stats analogue of [`deliver_identity_safe`]. The production fold batches
/// the identity captures ahead of one shared reconfirm read
/// ([`stats_with_seams`](Cgroup::stats_with_seams)); this single-member
/// composition keeps the race-freedom logic exercised end-to-end by the seam
/// tests.
///
/// `capture_identity(pid)` pins the start-time identity of whoever holds `pid` now
/// (a `/proc/<pid>/stat` starttime in production); `None` (gone / unreadable) is a
/// benign skip — there is nobody we can vouch for, and membership is not consulted.
/// The seam tests are its only caller, so it carries `allow(dead_code)` outside
/// `cfg(test)`.
#[cfg(feature = "stats")]
#[cfg_attr(not(test), allow(dead_code))]
fn sample_member_identity_safe(
    pid: i32,
    capture_identity: impl Fn(i32) -> Option<ProcIdentity>,
    still_member: impl Fn(i32) -> io::Result<bool>,
    read_metrics: impl Fn(i32, ProcIdentity) -> ProcMetrics,
) -> MemberSample {
    // 1. Pin the identity of the process currently at `pid`.
    let Some(id) = capture_identity(pid) else {
        // Gone (or no readable identity) before we could pin it — the counters
        // would belong to nobody we can vouch for. Benign skip.
        return MemberSample::Skipped;
    };
    // 2–3. Reconfirm membership *after* the pin, then read gated on the identity.
    sample_pinned(pid, id, still_member, read_metrics)
}

/// The honest error returned when the kernel lacks pidfd support, so per-member
/// signalling refuses to fall back to a racy `kill(pid, …)`.
fn pidfd_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "identity-safe per-member signalling needs pidfd (pidfd_open/pidfd_send_signal, \
         Linux >= 5.3); this kernel lacks it, so processkit refuses to fall back to a racy \
         kill(pid, ...) that could hit a pid recycled by a process outside the cgroup — use \
         SIGKILL teardown (atomic cgroup.kill) or run on a >= 5.3 kernel",
    )
}

/// `pidfd_open(2)` (Linux >= 5.3): return an owned fd that pins the *exact* task
/// currently running as `pid`. Unlike the bare pid, this fd never refers to a
/// later process that recycles the number — the identity anchor the per-member
/// signal path relies on. A kernel without the syscall answers `ENOSYS`, which
/// the caller turns into an honest error rather than a racy raw-kill fallback.
fn pidfd_open(pid: i32) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes (pid, flags) by value and shares no memory with the
    // kernel; on success it returns a fresh file descriptor this process owns.
    let rc = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `rc` is a fresh fd we exclusively own; wrap it so it is closed on drop.
    Ok(unsafe { OwnedFd::from_raw_fd(rc as RawFd) })
}

/// `pidfd_send_signal(2)` (Linux >= 5.1): deliver `sig` to the task pinned by
/// `fd`. Because the fd names a specific task, the signal can only ever reach
/// that task — never a process that later reused its pid — which is what makes
/// per-member signalling race-free against pid recycling. A null `siginfo` and
/// zero flags ask the kernel to behave exactly like `kill(2)`.
fn pidfd_send_signal(fd: &OwnedFd, sig: i32) -> io::Result<()> {
    // SAFETY: `fd` is a live pidfd we own; a null siginfo pointer with 0 flags is
    // the documented "behave like kill(2)" form and shares no memory with the
    // kernel.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            sig,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Which of the `needed` cgroup controllers are not already present in a
/// `cgroup.subtree_control` value (a space-separated list of enabled controller
/// names). Returns the ones that still need enabling — so the caller writes
/// `subtree_control` only when something is missing, never redundantly (a
/// redundant write can spuriously `EBUSY` under the no-internal-process rule, so
/// skipping it is what lets limits work in an already-delegated environment).
#[cfg(feature = "limits")]
fn controllers_to_enable<'a>(needed: &[&'a str], subtree_control: &str) -> Vec<&'a str> {
    let already: std::collections::HashSet<&str> = subtree_control.split_whitespace().collect();
    needed
        .iter()
        .copied()
        .filter(|c| !already.contains(c))
        .collect()
}

/// Format a per-core CPU fraction as a cgroup v2 `cpu.max` value (`"quota period"`,
/// microseconds). `0.5` → `"50000 100000"`, `2.0` → `"200000 100000"`.
#[cfg(feature = "limits")]
fn cpu_max_value(cores: f64) -> String {
    const PERIOD: u64 = 100_000;
    let quota = (cores * PERIOD as f64).round().max(1.0) as u64;
    format!("{quota} {PERIOD}")
}

/// The whole-tree counters a cgroup keeps for itself, as read by
/// [`Cgroup::container_counters_with`] and layered onto the per-member fold — the
/// three [`ProcessGroupStats`] fields whose source is the container rather than a
/// sum over `/proc`. Each is `None` where this host does not account it; see the
/// reader for why that is normal rather than a failure.
#[cfg(feature = "stats")]
struct ContainerCounters {
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
    peak_process_count: Option<usize>,
}

/// Sum one nested key down a cgroup v2 **nested-keyed** file — `io.stat`'s
/// `"<major:minor> rbytes=… wbytes=… rios=… wios=…"`, one line per device — over
/// every device listed.
///
/// The three answers are deliberately distinct, in the same spirit as
/// [`flat_keyed_value`]'s "absent key is not a zero":
///
/// - **no lines at all** — `Some(0)`. The file exists, so the controller is
///   accounting; it lists a device only once that device has been touched, so
///   nothing listed means nothing transferred. That is a measured zero.
/// - **lines, none carrying `key`** — `None`. This kernel's `io.stat` does not
///   account that key, and a sum over nothing would fabricate the same `0` the
///   honest measurement above earns.
/// - **lines carrying `key`** — `Some(sum)`, saturating (a tree's lifetime byte
///   count across devices could in principle overflow, and clamping beats
///   wrapping into a small plausible number).
///
/// A value that does not parse is skipped rather than failing the whole read: one
/// malformed device line must not erase the bytes the others reported. The device
/// token itself is never inspected — every device the tree touched counts, and this
/// makes no claim about which.
#[cfg(feature = "stats")]
fn nested_keyed_sum(text: &str, key: &str) -> Option<u64> {
    let mut saw_line = false;
    let mut saw_key = false;
    let mut sum = 0u64;
    for line in text.lines() {
        // The first token is the `<major:minor>` device the line is keyed by; the
        // `key=value` pairs follow it.
        let mut fields = line.split_whitespace();
        if fields.next().is_none() {
            continue;
        }
        saw_line = true;
        for field in fields {
            let Some((name, value)) = field.split_once('=') else {
                continue;
            };
            if name == key
                && let Ok(value) = value.parse::<u64>()
            {
                saw_key = true;
                sum = sum.saturating_add(value);
            }
        }
    }
    match (saw_line, saw_key) {
        // No device has been touched: an accounted zero, not a missing measurement.
        (false, _) => Some(0),
        (true, true) => Some(sum),
        // The file accounts something, but not this key.
        (true, false) => None,
    }
}

/// Read one counter out of a cgroup v2 **flat-keyed** file — the
/// `"<key> <value>"`-per-line format shared by `memory.events`, `pids.events` and
/// `cpu.stat` (`"oom 1"`, `"max 3"`, `"nr_throttled 21"`).
///
/// `None` when the key is absent or its value doesn't parse as a count, so a caller
/// can tell "the kernel does not account this" apart from "the kernel accounts it
/// and it is zero" — the difference between an honest `Unknown` and a decisive
/// `NotTripped`. Keys are matched whole (`split_whitespace`), never by prefix, so
/// `oom` can't be satisfied by `oom_kill` / `oom_group_kill` sitting in the same
/// file.
#[cfg(feature = "limits")]
fn flat_keyed_value(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == key).then(|| fields.next()?.parse::<u64>().ok())?
    })
}

/// The cgroup v2 controllers a limit set needs enabled — one per **requested**
/// (`Some`) axis, in `memory` / `pids` / `cpu` order. A `None` axis needs no
/// controller (it carries no cap to enforce). Shared by the creation
/// (`apply_limits`) and live-update (`update_limits`) paths so both gate on the
/// same controller set.
#[cfg(feature = "limits")]
fn needed_controllers(limits: &ResourceLimits) -> Vec<&'static str> {
    let mut needed: Vec<&'static str> = Vec::new();
    if limits.max_memory.is_some() {
        needed.push("memory");
    }
    if limits.max_processes.is_some() {
        needed.push("pids");
    }
    if limits.cpu_quota.is_some() {
        needed.push("cpu");
    }
    needed
}

/// Write one cgroup limit interface file for the `update_limits` full replacement:
/// `Some(v)` sets the axis to `v`; `None` resets it to `max` (unbounded) — but only
/// when the file exists. A controller that was never enabled has no interface file
/// and the axis is already unbounded, so a `None` reset there is a no-op success
/// rather than a spurious `NotFound` write error.
#[cfg(feature = "limits")]
fn write_limit_reset(path: &Path, value: Option<String>) -> io::Result<()> {
    match value {
        Some(v) => cgroup_write(path, v),
        None if path.exists() => cgroup_write(path, "max"),
        None => Ok(()),
    }
}

/// Arm `PR_SET_PDEATHSIG(SIGKILL)` so the kernel kills this child when the
/// spawning thread dies, then close the parent-died-before-arming race: if
/// `getppid()` no longer reports `spawner_pid` (captured in the parent before
/// the fork), the parent died in the window and the signal will never fire —
/// exit immediately instead. Comparing against the captured pid (never the
/// literal `1`) keeps the guard correct when the spawner itself *is* PID 1 —
/// a container entrypoint, exactly where this hardening matters most.
/// Runs in the forked child after `fork()` and before `exec()`.
///
/// # Caveat: thread death, not process death
///
/// `PR_SET_PDEATHSIG` fires when the *thread* that called `fork()` dies, not
/// when the parent *process* exits. The `getppid()` guard above only closes
/// the "parent process already dead before arming" race — it does nothing
/// for the case where the spawning thread itself is later torn down while
/// the ProcessKit process stays alive (e.g. an async runtime retiring the
/// blocking/worker thread that performed the fork). In that scenario the
/// kernel would prematurely `SIGKILL` a still-wanted child. Today's
/// multi-threaded tokio worker threads live for the whole process, so this
/// is latent, but any future spawn path on a transient thread would need to
/// either pin the fork to a long-lived thread or re-derive this guard.
///
/// # Safety
///
/// Must stay async-signal-safe: it calls only `prctl`/`getppid`/`_exit` —
/// no allocation, no locks.
fn arm_pdeathsig(spawner_pid: u32) -> io::Result<()> {
    // SAFETY: prctl(PR_SET_PDEATHSIG)/getppid/_exit are async-signal-safe.
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::getppid() as u32 != spawner_pid {
            libc::_exit(0);
        }
    }
    Ok(())
}

/// Append the calling process's own pid to the opened `cgroup.procs`, joining
/// the cgroup. Runs in the forked child after `fork()` and before `exec()`.
///
/// # Safety
///
/// Must stay async-signal-safe: it calls only `open`/`getpid`/`write`/`close`
/// and formats the pid into a stack buffer — no allocation, no locks.
fn write_self_pid(path: &CStr) -> io::Result<()> {
    // SAFETY: all calls below are async-signal-safe and operate on a valid,
    // NUL-terminated path; the fd is closed on every return path.
    unsafe {
        let fd = libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // Format the (positive) pid as decimal into a stack buffer.
        let mut buf = [0u8; 12];
        let mut i = buf.len();
        let mut v = libc::getpid() as u32;
        loop {
            i -= 1;
            buf[i] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        let bytes = &buf[i..];

        let written = libc::write(fd, bytes.as_ptr().cast(), bytes.len());
        let werr = io::Error::last_os_error();
        libc::close(fd);
        if written < 0 {
            return Err(werr);
        }
        // A short write would leave the child only partially joined to the cgroup
        // — degrading containment silently. Writing a small pid to `cgroup.procs`
        // is atomic in practice, but treat anything less than the full write as a
        // failure (the spawn then surfaces it) rather than a half-join. Use the
        // allocation-free `ErrorKind` form: this runs in the fork→exec window
        // where `io::Error::new(_, msg)` (which boxes `msg`) would not be
        // async-signal-safe.
        if (written as usize) != bytes.len() {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        Ok(())
    }
}

/// Unit tests for the `_with`-suffixed read-seam methods (`members_with`,
/// `signal_with`, `kill_with`, `stats_with`): each takes an injectable
/// `cgroup.procs` reader so the success/`NotFound`/`PermissionDenied`/I/O-error
/// mapping — and the fail-safe decision each caller builds on it — can be driven
/// deterministically without a real cgroup v2 mount. See `fail_safe_tests` below
/// for the two paths whose signature can't take an injected reader
/// (`GracefulTarget::is_drained`, `Job::drop`'s drain wait), which are instead
/// exercised against a real temporary directory.
///
/// [`frozen_now`] is tested here too, for the same reason and in the same shape: it
/// is a decision made on a read (`cgroup.freeze`, on the kill fallback's
/// refused-thaw path), and taking that read's result as its argument is what lets
/// every branch — including the unreadable one no fault-injection site covers — be
/// driven directly.
#[cfg(test)]
mod cgroup_read_seam_tests {
    use std::cell::Cell;
    use std::io;
    use std::path::{Path, PathBuf};

    use super::{Cgroup, Delivery, deliver_identity_safe, frozen_now};

    fn cgroup() -> Cgroup {
        Cgroup::at(PathBuf::from("/mock/processkit"))
    }

    #[test]
    fn members_parses_readable_procs() {
        let members = cgroup()
            .members_with(|path| {
                assert_eq!(path, Path::new("/mock/processkit/cgroup.procs"));
                Ok("12\n0\ninvalid\n-3\n42\n".to_owned())
            })
            .expect("readable member list");

        assert_eq!(members, [12, 42]);
    }

    #[test]
    fn missing_procs_means_empty_cgroup() {
        let members = cgroup()
            .members_with(|_| Err(io::Error::from(io::ErrorKind::NotFound)))
            .expect("a removed cgroup has no members");

        assert!(members.is_empty());
    }

    #[test]
    fn permission_denied_procs_is_unknown() {
        let err = cgroup()
            .members_with(|_| Err(io::Error::from(io::ErrorKind::PermissionDenied)))
            .expect_err("an unreadable cgroup must not look empty");

        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn io_error_procs_is_unknown() {
        let err = cgroup()
            .members_with(|_| Err(io::Error::from_raw_os_error(libc::EIO)))
            .expect_err("an I/O failure must not look empty");

        assert_eq!(err.raw_os_error(), Some(libc::EIO));
    }

    /// The cgroup evidence reader, driven over the same injectable seam: an axis
    /// that carried a cap is decided by the kernel counter, an axis that never did
    /// is `NotTripped` **without any read at all**, and a missing/unparsable counter
    /// is an honest `Unknown` rather than a "no".
    #[cfg(feature = "limits")]
    mod limit_evidence {
        use std::cell::RefCell;
        use std::io;
        use std::path::{Path, PathBuf};

        use crate::limits::{CappedAxes, LimitKind, LimitVerdict, ResourceLimits};

        use super::cgroup;

        /// A `CappedAxes` recording exactly the axes `limits` caps.
        fn capped(limits: ResourceLimits) -> CappedAxes {
            let mut axes = CappedAxes::default();
            axes.record(&limits);
            axes
        }

        const ALL_CAPPED: fn() -> CappedAxes = || {
            capped(ResourceLimits {
                max_memory: Some(1),
                max_processes: Some(1),
                cpu_quota: Some(1.0),
            })
        };

        /// Every counter file present and non-zero: all three axes fired.
        #[test]
        fn non_zero_counters_trip_each_axis() {
            let ev = cgroup().limit_evidence_with(ALL_CAPPED(), |path| {
                Ok(match path.file_name().unwrap().to_str().unwrap() {
                    // Real kernel spellings, extra keys included: the parser must
                    // pick `oom` and not the `oom_kill`/`oom_group_kill` siblings.
                    "memory.events.local" => "low 0\nhigh 0\nmax 50022\noom 1\noom_kill 1\n",
                    "pids.events.local" => "max 3\n",
                    "cpu.stat" => {
                        "usage_usec 105292\nnr_periods 21\nnr_throttled 21\nthrottled_usec 1977211\n"
                    }
                    other => panic!("unexpected evidence read: {other}"),
                }
                .to_owned())
            });

            assert_eq!(ev.memory(), LimitVerdict::Tripped);
            assert_eq!(ev.processes(), LimitVerdict::Tripped);
            assert_eq!(ev.cpu(), LimitVerdict::Tripped);
        }

        /// A cap that was in force and provably never engaged: an authoritative
        /// zero is a decisive "no", never `Unknown`.
        #[test]
        fn zero_counters_are_a_decisive_not_tripped() {
            let ev = cgroup().limit_evidence_with(ALL_CAPPED(), |path| {
                Ok(match path.file_name().unwrap().to_str().unwrap() {
                    "memory.events.local" => "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
                    "pids.events.local" => "max 0\n",
                    "cpu.stat" => "usage_usec 1\nnr_periods 0\nnr_throttled 0\n",
                    other => panic!("unexpected evidence read: {other}"),
                }
                .to_owned())
            });

            assert_eq!(ev.memory(), LimitVerdict::NotTripped);
            assert_eq!(ev.processes(), LimitVerdict::NotTripped);
            assert_eq!(ev.cpu(), LimitVerdict::NotTripped);
        }

        /// A global (host) OOM kill of our child raises `oom_kill` in our cgroup
        /// while OUR cap never engaged (`oom` stays 0). Keying the verdict on
        /// `oom_kill` would manufacture a false "your memory cap killed it"; the
        /// reader must report `NotTripped` here.
        #[test]
        fn an_oom_kill_without_a_local_oom_event_does_not_trip_memory() {
            let ev = cgroup().limit_evidence_with(
                capped(ResourceLimits {
                    max_memory: Some(1),
                    ..ResourceLimits::default()
                }),
                |_| Ok("low 0\nhigh 0\nmax 0\noom 0\noom_kill 4\noom_group_kill 1\n".to_owned()),
            );

            assert_eq!(
                ev.memory(),
                LimitVerdict::NotTripped,
                "a kill by the GLOBAL oom killer is not evidence that this cgroup's own cap fired"
            );
        }

        /// An axis that never carried a cap answers `NotTripped` and performs **no**
        /// read — the "evidence costs nothing when nothing was capped" guarantee.
        #[test]
        fn an_uncapped_axis_is_not_tripped_without_any_read() {
            let reads: RefCell<Vec<PathBuf>> = RefCell::new(Vec::new());
            let ev = cgroup().limit_evidence_with(
                capped(ResourceLimits {
                    max_processes: Some(4),
                    ..ResourceLimits::default()
                }),
                |path| {
                    reads.borrow_mut().push(path.to_path_buf());
                    Ok("max 7\n".to_owned())
                },
            );

            assert_eq!(ev.processes(), LimitVerdict::Tripped);
            assert_eq!(ev.memory(), LimitVerdict::NotTripped);
            assert_eq!(ev.cpu(), LimitVerdict::NotTripped);
            assert_eq!(
                reads.borrow().as_slice(),
                [PathBuf::from("/mock/processkit/pids.events.local")],
                "only the capped axis may be read"
            );
        }

        /// A group with no caps at all touches the filesystem zero times.
        #[test]
        fn an_uncapped_group_performs_no_evidence_io() {
            let reads = std::cell::Cell::new(0usize);
            let ev = cgroup().limit_evidence_with(CappedAxes::default(), |_| {
                reads.set(reads.get() + 1);
                Ok(String::new())
            });

            assert_eq!(reads.get(), 0, "an uncapped group must not read anything");
            for kind in [LimitKind::Memory, LimitKind::Processes, LimitKind::Cpu] {
                assert_eq!(ev.verdict(kind), LimitVerdict::NotTripped);
            }
        }

        /// Kernels without the `.local` files fall back to the hierarchical ones.
        #[test]
        fn a_missing_local_file_falls_back_to_the_hierarchical_counter() {
            let ev = cgroup().limit_evidence_with(ALL_CAPPED(), |path| {
                match path.file_name().unwrap().to_str().unwrap() {
                    // Pre-5.2 / pre-6.9 kernels have no `.local` variants.
                    "memory.events.local" | "pids.events.local" => {
                        Err(io::Error::from(io::ErrorKind::NotFound))
                    }
                    "memory.events" => Ok("max 1\noom 2\noom_kill 2\n".to_owned()),
                    "pids.events" => Ok("max 0\n".to_owned()),
                    "cpu.stat" => Ok("nr_throttled 5\n".to_owned()),
                    other => panic!("unexpected evidence read: {other}"),
                }
            });

            assert_eq!(ev.memory(), LimitVerdict::Tripped);
            assert_eq!(ev.processes(), LimitVerdict::NotTripped);
            assert_eq!(ev.cpu(), LimitVerdict::Tripped);
        }

        /// No readable counter file at all (an unreadable cgroup, a kernel that
        /// accounts none of this): `Unknown` on every capped axis — never a "no".
        #[test]
        fn unreadable_counters_are_unknown_not_a_no() {
            let ev = cgroup().limit_evidence_with(ALL_CAPPED(), |_| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            });

            for kind in [LimitKind::Memory, LimitKind::Processes, LimitKind::Cpu] {
                assert_eq!(ev.verdict(kind), LimitVerdict::Unknown, "axis {kind:?}");
            }
        }

        /// The file exists but the kernel does not account that key (an older
        /// kernel, a `cpu.stat` without bandwidth fields): `Unknown`, not zero.
        #[test]
        fn a_readable_file_without_the_key_is_unknown() {
            let ev = cgroup().limit_evidence_with(ALL_CAPPED(), |path| {
                Ok(match path.file_name().unwrap().to_str().unwrap() {
                    // Every sibling key present EXCEPT the one that decides.
                    "memory.events.local" => "low 0\nhigh 0\nmax 3\n",
                    "pids.events.local" => "not_max 9\n",
                    "cpu.stat" => "usage_usec 42\nuser_usec 40\nsystem_usec 2\n",
                    other => panic!("unexpected evidence read: {other}"),
                }
                .to_owned())
            });

            for kind in [LimitKind::Memory, LimitKind::Processes, LimitKind::Cpu] {
                assert_eq!(ev.verdict(kind), LimitVerdict::Unknown, "axis {kind:?}");
            }
        }

        /// The counter paths are read from this cgroup's own directory.
        #[test]
        fn counters_are_read_from_this_cgroups_directory() {
            let ev = cgroup().limit_evidence_with(
                capped(ResourceLimits {
                    cpu_quota: Some(0.5),
                    ..ResourceLimits::default()
                }),
                |path| {
                    assert_eq!(path, Path::new("/mock/processkit/cpu.stat"));
                    Ok("nr_throttled 1\n".to_owned())
                },
            );

            assert_eq!(ev.cpu(), LimitVerdict::Tripped);
        }
    }

    #[test]
    fn signal_with_propagates_read_error_without_reaching_the_per_pid_loop() {
        // `signal_with` resolves the member list with `?` before the per-pid
        // `libc::kill` loop, so a read failure returns `Err` and no signal is
        // ever sent — the fail-safe this test locks in.
        let err = cgroup()
            .signal_with(libc::SIGTERM, |_| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            })
            .expect_err("an unreadable member list must not look like a successful no-op signal");

        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn signal_with_empty_member_list_is_a_no_op_success() {
        cgroup()
            .signal_with(libc::SIGTERM, |_| Ok(String::new()))
            .expect("no members to signal is trivially successful");
    }

    #[test]
    fn kill_with_persistent_read_error_reports_a_real_drain_failure() {
        // The mock path has no real `cgroup.kill` file, so this always falls
        // into the legacy per-pid SIGKILL sweep; a `cgroup.procs` that never
        // becomes readable must make the sweep propagate that error instead of
        // a false `Ok(())` (a regression here would look like `Err(_) => Ok(())`
        // in the final drain check).
        let err = cgroup()
            .kill_with(|_| Err(io::Error::from_raw_os_error(libc::EIO)))
            .expect_err("a cgroup.procs that never becomes readable must not report as drained");

        assert_eq!(err.raw_os_error(), Some(libc::EIO));
    }

    #[test]
    fn kill_with_empty_member_list_drains_immediately() {
        cgroup()
            .kill_with(|_| Ok(String::new()))
            .expect("an already-empty cgroup is reported as drained by the fallback sweep");
    }

    /// The freezer question the kill fallback answers after a refused thaw: the
    /// kernel's own reading of `cgroup.freeze` decides wherever it gives one, and
    /// what this call happens to know about *its own* freeze write does not.
    #[test]
    fn frozen_now_takes_the_files_answer_over_the_callers_own_freeze_write() {
        // A freeze this call never put in force still counts — one an earlier
        // `suspend` left standing, or one a previous teardown already reported and
        // could not clear. What decides whether the group can be spawned into is
        // its state, not who set it.
        assert!(
            frozen_now(Ok("1\n".to_owned()), false),
            "a group the kernel calls frozen is frozen whoever froze it"
        );
        // And the converse: a group the kernel calls unfrozen is not made frozen by
        // this call having written the freeze — something cleared it in between.
        assert!(
            !frozen_now(Ok("0\n".to_owned()), true),
            "a landed freeze that is no longer in force is nothing to report"
        );
    }

    /// The one place an inference survives: the file gives no usable answer, so the
    /// caller's own freeze write decides after all. This is the pre-existing
    /// behaviour kept unchanged — an unreadable freezer is no reason to start
    /// reporting less than before, nor more.
    #[test]
    fn frozen_now_falls_back_when_the_file_gives_no_usable_answer() {
        // A *read* that fails, including with `NotFound` — the write's own
        // `NotFound` is a separate, earlier decision in `thaw_after_kill_sweep`
        // (a vanished cgroup holds nobody frozen) and never reaches here — and two
        // readings of a two-valued file that are not values of it.
        let no_answer = || {
            [
                Err(io::Error::from(io::ErrorKind::PermissionDenied)),
                Err(io::Error::from(io::ErrorKind::NotFound)),
                Ok("frozen\n".to_owned()),
                Ok(String::new()),
            ]
        };

        for (n, state) in no_answer().into_iter().enumerate() {
            assert!(
                frozen_now(state, true),
                "case {n}: a freeze this call landed and could not clear stands until \
                 the file says otherwise"
            );
        }
        for (n, state) in no_answer().into_iter().enumerate() {
            assert!(
                !frozen_now(state, false),
                "case {n}: with no freeze of this call's own and no answer from the \
                 file, there is nothing to report"
            );
        }
    }

    #[test]
    fn kill_with_skips_a_pid_recycled_between_snapshot_and_kill() {
        struct Handle(i32);

        let reads = Cell::new(0usize);
        let opened = std::cell::RefCell::new(Vec::new());
        let signalled = std::cell::RefCell::new(Vec::new());
        cgroup()
            .kill_with_seams(
                |_: &Path| {
                    let read = reads.get() + 1;
                    reads.set(read);
                    Ok(match read {
                        1 => "1001\n1002\n",
                        // 1002 left the cgroup and its pid was recycled outside
                        // it before the identity-safe delivery step.
                        2 => "1001\n",
                        3 | 4 => "",
                        other => panic!("unexpected cgroup.procs read {other}"),
                    }
                    .to_owned())
                },
                |pid| {
                    opened.borrow_mut().push(pid);
                    Ok(Handle(pid))
                },
                |handle: &Handle, signal| {
                    assert_eq!(signal, libc::SIGKILL);
                    signalled.borrow_mut().push(handle.0);
                    Ok(())
                },
            )
            .expect("a recycled member is a benign skip when the cgroup drains");

        assert_eq!(*opened.borrow(), vec![1001, 1002]);
        assert_eq!(
            *signalled.borrow(),
            vec![1001],
            "the pid absent from the post-pin membership snapshot must not be signalled"
        );
    }

    #[test]
    fn kill_with_delivers_sigkill_to_a_confirmed_member() {
        struct Handle(i32);

        let reads = Cell::new(0usize);
        let signalled = std::cell::RefCell::new(Vec::new());
        cgroup()
            .kill_with_seams(
                |_: &Path| {
                    let read = reads.get() + 1;
                    reads.set(read);
                    Ok(match read {
                        1 | 2 => "1001\n",
                        3 | 4 => "",
                        other => panic!("unexpected cgroup.procs read {other}"),
                    }
                    .to_owned())
                },
                |pid| Ok(Handle(pid)),
                |handle: &Handle, signal| {
                    assert_eq!(signal, libc::SIGKILL);
                    signalled.borrow_mut().push(handle.0);
                    Ok(())
                },
            )
            .expect("a confirmed member must receive SIGKILL through its pinned handle");

        assert_eq!(*signalled.borrow(), vec![1001]);
    }

    #[cfg(feature = "stats")]
    #[test]
    fn stats_with_read_error_is_not_reported_as_zero_active_processes() {
        let err = cgroup()
            .stats_with(|_| Err(io::Error::from(io::ErrorKind::PermissionDenied)))
            .expect_err("an unreadable member list must not look like an empty (0-process) group");

        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[cfg(feature = "stats")]
    #[test]
    fn stats_with_empty_member_list_reports_zero_active_processes() {
        let stats = cgroup()
            .stats_with(|_| Ok(String::new()))
            .expect("an empty member list is a legitimate zero-active-process stats snapshot");

        assert_eq!(stats.active_process_count, 0);
    }

    /// The counters the cgroup keeps itself (`io.stat`, `pids.peak`), driven over
    /// the same injectable reader as the member list: present, absent, empty and
    /// unparsable, plus the end-to-end `stats_with` composition that layers them
    /// onto the per-member fold.
    #[cfg(feature = "stats")]
    mod container_counters {
        use std::cell::Cell;
        use std::io;
        use std::path::Path;

        // `super` is the seam-test module; the backend itself is one further up
        // (the module is path-remapped to `sys::imp`, so no absolute path to it).
        use super::super::nested_keyed_sum;
        use super::cgroup;

        /// A reader over a fixture table keyed by file name: an entry missing from
        /// the table reads as `NotFound`, exactly as a controller file that is not
        /// there does on a host where that controller was never enabled.
        fn files(entries: &[(&'static str, &'static str)]) -> impl Fn(&Path) -> io::Result<String> {
            let entries: Vec<(String, String)> = entries
                .iter()
                .map(|(name, body)| ((*name).to_owned(), (*body).to_owned()))
                .collect();
            move |path: &Path| {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .expect("a cgroup interface file has a name");
                entries
                    .iter()
                    .find(|(entry, _)| entry == name)
                    .map(|(_, body)| body.clone())
                    .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
            }
        }

        /// Real `io.stat` shape: one line per device, nested `key=value` pairs. The
        /// tree's bytes are the sum down the column — a tree that wrote to two disks
        /// wrote the total of both, and the device tokens are not inspected.
        #[test]
        fn io_bytes_are_summed_over_every_device() {
            let counters = cgroup().container_counters_with(files(&[(
                "io.stat",
                "259:0 rbytes=1024 wbytes=2048 rios=4 wios=8 dbytes=0 dios=0\n\
                 8:16 rbytes=512 wbytes=64 rios=1 wios=1 dbytes=0 dios=0\n",
            )]));

            assert_eq!(counters.io_read_bytes, Some(1536));
            assert_eq!(counters.io_write_bytes, Some(2112));
        }

        /// `pids.peak` is the high-water mark; the reader must take it verbatim and
        /// must not confuse it with `pids.current` (which is *now*, and is not what
        /// this field reports).
        #[test]
        fn peak_process_count_reads_pids_peak_not_pids_current() {
            let read = Cell::new(Vec::new());
            let counters = cgroup().container_counters_with(|path: &Path| {
                let mut seen = read.take();
                seen.push(path.to_owned());
                read.set(seen);
                match path.file_name().and_then(|n| n.to_str()) {
                    Some("pids.peak") => Ok("7\n".to_owned()),
                    Some("pids.current") => panic!("the peak must not be read from pids.current"),
                    _ => Err(io::Error::from(io::ErrorKind::NotFound)),
                }
            });

            assert_eq!(counters.peak_process_count, Some(7));
            assert!(
                read.take()
                    .iter()
                    .all(|p| p.starts_with("/mock/processkit")),
                "the counters are the job's own cgroup's, not a leaf's or an ancestor's"
            );
        }

        /// A host that does not account these at all — no `io` controller, a kernel
        /// without `pids.peak` — must report `None`, not a fabricated zero, and must
        /// not fail the snapshot.
        #[test]
        fn absent_controller_files_are_none_never_zero() {
            let counters = cgroup().container_counters_with(files(&[]));

            assert_eq!(counters.io_read_bytes, None);
            assert_eq!(counters.io_write_bytes, None);
            assert_eq!(counters.peak_process_count, None);
        }

        /// An unreadable (rather than absent) counter file is the same honest gap:
        /// a permission denial says nothing about how many bytes moved.
        #[test]
        fn unreadable_counter_files_are_none() {
            let counters = cgroup().container_counters_with(|_: &Path| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            });

            assert_eq!(counters.io_read_bytes, None);
            assert_eq!(counters.io_write_bytes, None);
            assert_eq!(counters.peak_process_count, None);
        }

        /// The short-run case: `io.stat` exists (the controller *is* accounting) but
        /// lists no device, because nothing has reached the block layer yet. That is
        /// a measured zero — distinct from the absent-file `None` above.
        #[test]
        fn an_accounting_cgroup_that_moved_nothing_reports_zero_not_none() {
            let counters = cgroup().container_counters_with(files(&[("io.stat", "")]));

            assert_eq!(counters.io_read_bytes, Some(0));
            assert_eq!(counters.io_write_bytes, Some(0));
        }

        /// A `pids.peak` that does not parse (a kernel spelling this reader does not
        /// know) is `None` rather than a guess.
        #[test]
        fn unparsable_pids_peak_is_none() {
            let counters = cgroup().container_counters_with(files(&[("pids.peak", "max\n")]));

            assert_eq!(counters.peak_process_count, None);
        }

        /// A device line whose value is malformed must not erase what the other
        /// devices reported — one bad line costs its own contribution, nothing more.
        #[test]
        fn a_malformed_device_line_does_not_erase_the_others() {
            assert_eq!(
                nested_keyed_sum(
                    "259:0 rbytes=oops wbytes=1\n8:16 rbytes=10 wbytes=2\n",
                    "rbytes"
                ),
                Some(10)
            );
        }

        /// A file that accounts *something* but not this key is `None`: summing over
        /// no matching key would fabricate the same `0` a real "nothing moved"
        /// earns, and the two must stay distinguishable.
        #[test]
        fn a_key_this_kernel_does_not_account_is_none() {
            assert_eq!(nested_keyed_sum("259:0 rios=4 wios=8\n", "rbytes"), None);
            assert_eq!(nested_keyed_sum("", "rbytes"), Some(0));
        }

        /// End to end through `stats_with`: the per-member fold and the cgroup's own
        /// counters land in one snapshot, each from its own file, and a membership
        /// change between the two `cgroup.procs` reads (pid 1002 recycled out) skips
        /// only that member's `/proc` share — the container counters are the
        /// kernel's and are unaffected by it.
        #[test]
        fn stats_with_layers_the_container_counters_onto_the_member_fold() {
            let procs_reads = Cell::new(0usize);
            let stats = cgroup()
                .stats_with(
                    |path: &Path| match path.file_name().and_then(|n| n.to_str()) {
                        Some("cgroup.procs") => {
                            procs_reads.set(procs_reads.get() + 1);
                            Ok(if procs_reads.get() == 1 {
                                "1001\n1002\n".to_owned()
                            } else {
                                "1001\n".to_owned()
                            })
                        }
                        Some("io.stat") => Ok("259:0 rbytes=4096 wbytes=8192\n".to_owned()),
                        Some("pids.peak") => Ok("5\n".to_owned()),
                        other => panic!("unexpected read of {other:?}"),
                    },
                )
                .expect("a snapshot with both sources present");

            assert_eq!(
                stats.active_process_count, 2,
                "the count still reflects the initial member list"
            );
            assert_eq!(stats.io_read_bytes, Some(4096));
            assert_eq!(stats.io_write_bytes, Some(8192));
            assert_eq!(
                stats.peak_process_count,
                Some(5),
                "the terminal peak is the cgroup's own, higher than the count now"
            );
        }

        /// The composition must not go the other way either: an absent `io.stat` /
        /// `pids.peak` leaves those fields `None` while the member fold's own
        /// numbers still come back.
        #[test]
        fn a_host_without_the_controllers_still_reports_the_member_fold() {
            let stats = cgroup()
                .stats_with(
                    |path: &Path| match path.file_name().and_then(|n| n.to_str()) {
                        Some("cgroup.procs") => Ok("1001\n".to_owned()),
                        _ => Err(io::Error::from(io::ErrorKind::NotFound)),
                    },
                )
                .expect("missing controller files are not a snapshot failure");

            assert_eq!(stats.active_process_count, 1);
            assert_eq!(stats.io_read_bytes, None);
            assert_eq!(stats.io_write_bytes, None);
            assert_eq!(stats.peak_process_count, None);
        }
    }

    // ---- identity-safe per-member delivery (`deliver_identity_safe`) ----
    //
    // These drive the pin → reconfirm-membership → send decision logic through
    // injected syscall closures, so the pid-reuse race is exercised
    // deterministically without a real pidfd or cgroup. The production
    // `signal_with` wires the same logic to the real `pidfd_open`/
    // `pidfd_send_signal`; `pidfd_integration_tests` covers that live path.

    /// A zero-cost stand-in for a pidfd — `deliver_identity_safe` is generic over
    /// the pin handle, so tests pin with a token instead of a real fd.
    struct FakeHandle;

    #[test]
    fn reused_pid_outside_cgroup_is_never_signalled() {
        // The pin succeeds, but by the time membership is reconfirmed the original
        // member has exited and its pid was recycled by a process OUTSIDE the
        // cgroup, so `still_member` reports false. The primitive must skip and
        // never call `send` — the core PID-reuse safety this task adds.
        let sent = Cell::new(false);
        let outcome = deliver_identity_safe(
            1234,
            libc::SIGTERM,
            |_| Ok(FakeHandle),
            |_| Ok(false),
            |_: &FakeHandle, _| {
                sent.set(true);
                Ok(())
            },
        );
        assert!(matches!(outcome, Delivery::Skipped));
        assert!(
            !sent.get(),
            "a pid recycled outside the cgroup must never be signalled"
        );
    }

    #[test]
    fn confirmed_member_is_signalled_with_the_requested_signal() {
        let sent = Cell::new(None);
        let outcome = deliver_identity_safe(
            42,
            libc::SIGTERM,
            |_| Ok(FakeHandle),
            |_| Ok(true),
            |_: &FakeHandle, sig| {
                sent.set(Some(sig));
                Ok(())
            },
        );
        assert!(matches!(outcome, Delivery::Delivered));
        assert_eq!(
            sent.get(),
            Some(libc::SIGTERM),
            "the requested signal reaches a confirmed member"
        );
    }

    #[test]
    fn member_gone_before_pin_is_a_benign_no_op() {
        // `open` (pidfd_open) fails ESRCH: the member exited before we could pin
        // it. Benign — the intended end state (gone) already holds — and no send;
        // membership is not even consulted.
        let sent = Cell::new(false);
        let outcome = deliver_identity_safe(
            7,
            libc::SIGTERM,
            |_| Err::<FakeHandle, _>(io::Error::from_raw_os_error(libc::ESRCH)),
            |_| -> io::Result<bool> {
                panic!("membership must not be checked once the pin fails ESRCH")
            },
            |_: &FakeHandle, _| {
                sent.set(true);
                Ok(())
            },
        );
        assert!(matches!(outcome, Delivery::Delivered));
        assert!(!sent.get());
    }

    #[test]
    fn no_pidfd_support_fails_safe_instead_of_raw_kill() {
        // `open` fails ENOSYS (kernel < 5.3 / seccomp): the primitive must surface
        // an honest Unsupported error, NOT silently fall back to a racy raw kill.
        let sent = Cell::new(false);
        let outcome = deliver_identity_safe(
            7,
            libc::SIGTERM,
            |_| Err::<FakeHandle, _>(io::Error::from_raw_os_error(libc::ENOSYS)),
            |_| Ok(true),
            |_: &FakeHandle, _| {
                sent.set(true);
                Ok(())
            },
        );
        match outcome {
            Delivery::Failed(e) => assert_eq!(e.kind(), io::ErrorKind::Unsupported),
            _ => panic!("a kernel without pidfd must fail safe, not signal"),
        }
        assert!(!sent.get(), "fail-safe must not send any signal");
    }

    #[test]
    fn unreadable_membership_after_pin_fails_safe_without_sending() {
        // Reconfirming membership fails (EACCES): unknown membership must not be
        // signalled — fail safe, surface the error, no send.
        let sent = Cell::new(false);
        let outcome = deliver_identity_safe(
            7,
            libc::SIGTERM,
            |_| Ok(FakeHandle),
            |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            |_: &FakeHandle, _| {
                sent.set(true);
                Ok(())
            },
        );
        match outcome {
            Delivery::Failed(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            _ => panic!("an unreadable membership must fail safe"),
        }
        assert!(!sent.get());
    }

    #[test]
    fn pinned_target_exiting_before_send_is_a_benign_esrch() {
        // Membership is confirmed, but the pinned task exits before the send, so
        // `send` returns ESRCH. pidfd guarantees that ESRCH is our own target's
        // exit (never a recycled pid), so it is benign — reported Delivered.
        let outcome = deliver_identity_safe(
            7,
            libc::SIGTERM,
            |_| Ok(FakeHandle),
            |_| Ok(true),
            |_: &FakeHandle, _| Err(io::Error::from_raw_os_error(libc::ESRCH)),
        );
        assert!(matches!(outcome, Delivery::Delivered));
    }

    #[test]
    fn eperm_on_send_is_a_real_failure_that_surfaces() {
        // A confirmed member that changed uid (or a seccomp/container policy)
        // rejects the signal with EPERM — a real delivery failure that must not
        // read as success.
        let outcome = deliver_identity_safe(
            7,
            libc::SIGTERM,
            |_| Ok(FakeHandle),
            |_| Ok(true),
            |_: &FakeHandle, _| Err(io::Error::from_raw_os_error(libc::EPERM)),
        );
        match outcome {
            Delivery::Failed(e) => assert_eq!(e.raw_os_error(), Some(libc::EPERM)),
            _ => panic!("EPERM is a real delivery failure and must surface"),
        }
    }

    // ---- batched broadcast (`signal_with_seams`): one read for the whole tree ----
    //
    // The production broadcast pins every member first, reads `cgroup.procs`
    // exactly once, then reconfirms each pinned pid against that single snapshot.
    // These drive it through all three injected seams (counting reader + fake
    // pidfd open/send) so both the O(1) read cost and the pid-reuse skip are
    // observable without real processes — the anti-regression for this task's
    // O(n^2)→O(n) change, and proof the single shared snapshot keeps the
    // per-pid `deliver_identity_safe` safety above.

    #[test]
    fn signal_with_reads_cgroup_procs_a_constant_number_of_times_for_a_whole_tree() {
        // A tree of 100 members must still cost a constant number of `cgroup.procs`
        // reads, not one read per pid: the old per-pid reconfirm made this 1 + n
        // (101) reads of an n-line file — the O(n^2) work this task removes.
        let members = (1000..1100)
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let reads = Cell::new(0usize);
        let sends = Cell::new(0usize);
        cgroup()
            .signal_with_seams(
                libc::SIGTERM,
                |_| {
                    reads.set(reads.get() + 1);
                    Ok(members.clone())
                },
                |_| Ok(FakeHandle),
                |_: &FakeHandle, _| {
                    sends.set(sends.get() + 1);
                    Ok(())
                },
            )
            .expect("every confirmed member is signalled");
        assert_eq!(
            reads.get(),
            2,
            "one read for the initial member list + one shared reconfirm read, \
             independent of the 100 members (was 1 + n before this task)"
        );
        assert_eq!(
            sends.get(),
            100,
            "each confirmed member is still signalled exactly once"
        );
    }

    #[test]
    fn signal_with_skips_a_pid_recycled_outside_the_cgroup_via_the_single_snapshot() {
        // Pid 1002 is pinned from the initial list but has left the cgroup by the
        // one reconfirm snapshot (recycled by a process outside the tree). The
        // batched path must skip exactly that pid — never signal it — while still
        // signalling the rest, so the single shared snapshot preserves the
        // pin→reconfirm→send pid-reuse safety.
        struct Handle(i32);
        let reads = Cell::new(0usize);
        let signalled = std::cell::RefCell::new(Vec::new());
        cgroup()
            .signal_with_seams(
                libc::SIGTERM,
                |_| {
                    reads.set(reads.get() + 1);
                    // 1st read: initial member list. 2nd read: reconfirm snapshot,
                    // with 1002 already gone.
                    Ok(if reads.get() == 1 {
                        "1001\n1002\n1003\n".to_owned()
                    } else {
                        "1001\n1003\n".to_owned()
                    })
                },
                |pid| Ok(Handle(pid)),
                |h: &Handle, _| {
                    signalled.borrow_mut().push(h.0);
                    Ok(())
                },
            )
            .expect("a benign recycle race is not a broadcast failure");
        assert_eq!(
            *signalled.borrow(),
            vec![1001, 1003],
            "the pid missing from the single reconfirm snapshot is skipped; the rest are signalled"
        );
        assert_eq!(
            reads.get(),
            2,
            "still exactly two reads for the whole batch"
        );
    }
}

/// Error paths of the cgroup **write** primitive — the side the `_with` read seams
/// above cannot reach. Each test builds a real temporary directory shaped like a
/// cgroup (so an unfaulted write genuinely lands and can be read back) and makes one
/// named control file's write fail on demand via `crate::sys::fault_injection`. All
/// of these states — a limit write rejected part-way through a sequence, a
/// `cgroup.freeze` refused on a kernel that *has* the file — otherwise need a
/// delegated, restricted or ancient cgroup host, which is why none of them had a
/// regression test before.
// Ungated: the kill fallback's freeze/thaw cases below exercise `Cgroup::kill`,
// which every build has (kill-on-drop is unconditional), so nothing here sits
// unused in a feature-less build. The individual cases that do need a feature
// carry their own gate, as does the `read` helper only they use.
#[cfg(test)]
mod cgroup_write_seam_tests {
    use std::io;

    use super::Cgroup;
    use crate::sys::fault_injection::{Faults, Site};

    const SITE: Site = Site::CgroupWrite;

    /// A stand-in cgroup on a real temporary directory: the parent already
    /// delegates every controller (so `enable_controllers` writes nothing and the
    /// tests exercise only the limit writes), the three limit interface files exist
    /// at their kernel default `max`, and `cgroup.procs` is present and empty so the
    /// per-pid fallback paths have an honest, drained member list to read.
    fn temp_cgroup() -> (tempfile::TempDir, Cgroup) {
        let dir = tempfile::tempdir().expect("temp dir");
        let parent = dir.path().join("parent");
        let leaf = parent.join("leaf");
        std::fs::create_dir_all(&leaf).expect("create the cgroup dirs");
        std::fs::write(parent.join("cgroup.subtree_control"), "cpu memory pids\n")
            .expect("seed the parent's delegated controllers");
        for file in ["memory.max", "pids.max", "cpu.max"] {
            std::fs::write(leaf.join(file), "max\n").expect("seed a limit interface file");
        }
        std::fs::write(leaf.join("cgroup.procs"), "").expect("seed an empty member list");
        (dir, Cgroup::at(leaf))
    }

    /// Read a control file back to prove which writes actually landed.
    #[cfg(feature = "limits")]
    fn read(path: &std::path::Path) -> String {
        std::fs::read_to_string(path).expect("read back a control file")
    }

    /// The undo a recycled bare-pid adoption runs: a number this job's cgroup lists
    /// is written back into the directory the job's cgroup lives in, so the group's
    /// teardown — which aims at its own cgroup — stops covering it.
    ///
    /// On a real hierarchy the kernel performs the move and both files change; on a
    /// stand-in directory only the destination write is observable, and that is
    /// exactly the decision this code owns: *which* `cgroup.procs` the number is
    /// handed to, and whether it is handed to one at all. Whether a host then
    /// permits the move is its own policy, and is what the real-cgroup test
    /// (`real_cgroup_adopt_tests`) exists to answer.
    #[cfg(feature = "process-control")]
    #[test]
    fn a_recycled_adoption_hands_the_number_back_to_the_parent_cgroup() {
        let (_dir, cgroup) = temp_cgroup();
        let parent = cgroup
            .path
            .parent()
            .expect("the stand-in cgroup has a parent")
            .to_path_buf();
        std::fs::write(cgroup.path.join("cgroup.procs"), "4321\n").expect("seed the member list");

        assert!(
            matches!(cgroup.evict_recycled(4321), super::RecycleUndo::Evicted),
            "a number this cgroup holds must be moved back out"
        );
        assert_eq!(
            std::fs::read_to_string(parent.join("cgroup.procs"))
                .expect("the destination member list"),
            "4321",
            "the number must be handed to the cgroup this job's directory lives in"
        );
    }

    /// The other shape of a detected recycle: the migration was correct and the
    /// process it moved has since exited, so the number now names a process this
    /// call never touched. Moving *that* one would take an innocent process out of
    /// its own cgroup — the same harm the undo exists to prevent, aimed the other
    /// way — so the membership pass has to stop it.
    #[cfg(feature = "process-control")]
    #[test]
    fn a_recycle_after_a_correct_migration_moves_nobody() {
        let (_dir, cgroup) = temp_cgroup();
        let parent = cgroup
            .path
            .parent()
            .expect("the stand-in cgroup has a parent")
            .to_path_buf();
        // Seeded empty by `temp_cgroup`: nothing of this job's is left.
        assert!(
            matches!(cgroup.evict_recycled(4321), super::RecycleUndo::NotAMember),
            "a number this cgroup does not hold is nothing to undo"
        );
        assert!(
            !parent.join("cgroup.procs").exists(),
            "a number this call never migrated must not be written anywhere"
        );
    }

    /// The undo is a write like any other and a host may refuse it (a delegated
    /// cgroup that will not take the number back, an `EBUSY` from the "no internal
    /// processes" rule). That leaves whoever holds the number a member of this
    /// group — the fail-lethal residue — and the error must say so instead of
    /// claiming the group has no claim on it.
    #[cfg(feature = "process-control")]
    #[test]
    fn an_undo_the_host_refuses_is_reported_as_still_this_groups_to_kill() {
        let (_dir, cgroup) = temp_cgroup();
        std::fs::write(cgroup.path.join("cgroup.procs"), "4321\n").expect("seed the member list");

        let faults = Faults::new()
            .fail_every(SITE, Some("cgroup.procs"), libc::EPERM)
            .arm();
        let undo = cgroup.evict_recycled(4321);
        assert_eq!(
            faults.fired(SITE),
            1,
            "exactly the move-out write was failed"
        );
        assert!(matches!(undo, super::RecycleUndo::Stuck(_)));

        let text = super::recycled_during_cgroup_adoption(4321, undo).to_string();
        assert!(
            text.contains("could NOT be moved back out") && text.contains("will kill it"),
            "the caller must be told the number is still this group's to kill: {text}"
        );
        assert!(
            text.contains(&format!("os error {}", libc::EPERM)),
            "the refusal's own errno must reach the caller: {text}"
        );
    }

    /// The limits are applied as three **sequential** writes, so a failure on the
    /// second leaves the first already in force in the kernel and the third never
    /// attempted. The failure must surface with its errno intact — reporting the
    /// update as applied would hand back a group capped differently than asked.
    #[cfg(feature = "limits")]
    #[test]
    fn a_rejected_limit_write_surfaces_and_leaves_the_later_axes_untouched() {
        use crate::limits::{CappedAxes, ResourceLimits};
        use crate::{ErrorKind, ErrorReason, LimitKind, LimitReason};

        let (_dir, cgroup) = temp_cgroup();
        let limits = ResourceLimits {
            max_memory: Some(64 << 20),
            max_processes: Some(16),
            cpu_quota: Some(0.5),
            ..ResourceLimits::default()
        };

        // `memory.max` (write 1) lands for real; `pids.max` (write 2) is rejected
        // the way a restricted delegated cgroup rejects it.
        let faults = Faults::new()
            .fail_every(SITE, Some("pids.max"), libc::EIO)
            .arm();

        // Driven through the crate's own shared `update_limits` core — the exact
        // classification `ProcessGroup::update_limits` applies — so this asserts the
        // public error contract, not a hand-rolled equivalent of it.
        let mut capped = CappedAxes::default();
        let mut reflected = ResourceLimits::default();
        let err = crate::group::update_limits_with(&mut capped, &mut reflected, limits, |limits| {
            cgroup.update_limits(limits)
        })
        .expect_err("an EIO half-way through must not report the caps as applied");

        assert_eq!(faults.fired(SITE), 1, "exactly one write was failed");
        assert_eq!(err.kind(), ErrorKind::ResourceLimit);
        match err.reason() {
            ErrorReason::ResourceLimit {
                kind,
                reason,
                detail,
            } => {
                assert_eq!(*kind, LimitKind::Memory, "the first requested axis");
                assert_eq!(
                    *reason,
                    LimitReason::Unenforceable,
                    "a cgroup exists and refused the write — not `Unsupported`"
                );
                assert!(
                    detail.contains(&format!("os error {}", libc::EIO)),
                    "the OS errno must reach the caller: {detail}"
                );
            }
            other => panic!("expected a ResourceLimit failure, got {other:?}"),
        }

        // The partial application is real, and is exactly why `update_limits`
        // records the capped axes before applying rather than after succeeding.
        assert_eq!(
            read(&cgroup.path.join("memory.max")),
            (64u64 << 20).to_string(),
            "the write before the failure really reached the kernel"
        );
        assert_eq!(
            read(&cgroup.path.join("cpu.max")),
            "max\n",
            "the write after the failure was never attempted"
        );
    }

    /// `freeze` may degrade to the per-pid `SIGSTOP`/`SIGCONT` sweep for exactly one
    /// reason: the `cgroup.freeze` file is **absent** (kernel < 5.2). A write that is
    /// *refused* — a restricted delegated cgroup, an I/O error — happens on a file
    /// that exists, so it must surface instead of silently downgrading a suspend to
    /// the racy per-pid path on a modern kernel.
    #[cfg(feature = "process-control")]
    #[test]
    fn a_refused_cgroup_freeze_write_surfaces_instead_of_degrading() {
        let (_dir, cgroup) = temp_cgroup();
        let faults = Faults::new()
            .fail_every(SITE, Some("cgroup.freeze"), libc::EACCES)
            .arm();

        let err = cgroup
            .freeze(true)
            .expect_err("a refused freeze on a modern kernel must not look like a suspend");

        assert_eq!(faults.fired(SITE), 1);
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EACCES),
            "the refusal reaches the caller as itself, not as some fallback's error"
        );

        // And what `ProcessGroup::suspend` publishes for it — the same mapping the
        // public verb applies to its backend's `io::Error`.
        let public = crate::group::map_unsupported(err, "suspend");
        assert_eq!(
            public.kind(),
            crate::ErrorKind::PermissionDenied,
            "an EACCES from the freeze write is a permission problem, never a \
             silent success and never `Unsupported`"
        );
    }

    /// The other half of that discrimination: an **absent** `cgroup.freeze` (the
    /// pre-5.2 kernel case, `ENOENT`) is the one write failure that *may* fall back,
    /// and it does — the empty member list then makes the per-pid sweep a trivially
    /// successful no-op.
    #[cfg(feature = "process-control")]
    #[test]
    fn an_absent_cgroup_freeze_file_falls_back_to_the_per_pid_sweep() {
        let (_dir, cgroup) = temp_cgroup();
        let faults = Faults::new()
            .fail_every(SITE, Some("cgroup.freeze"), libc::ENOENT)
            .arm();

        cgroup
            .freeze(true)
            .expect("a missing cgroup.freeze falls back to the per-pid signal path");

        assert_eq!(
            faults.fired(SITE),
            1,
            "the freeze write was attempted first"
        );
    }

    /// The pre-5.14 kill fallback freezes the subtree so a fork bomb cannot
    /// out-spawn its SIGKILL sweep, then thaws it — and a refused **thaw** is the
    /// one failure an empty `cgroup.procs` cannot speak for. The tree really is
    /// dead, but the cgroup this call froze stays frozen, which is not the group
    /// `kill_all` promises to leave behind: cgroup v2 freezes a task that joins a
    /// frozen cgroup, and this backend joins one in a `pre_exec` hook, so the next
    /// spawn's child would stop before `exec` instead of running. Answering `Ok(())`
    /// off the drained member list alone hides exactly that.
    #[test]
    fn a_refused_thaw_after_the_sweep_is_not_reported_as_a_clean_kill() {
        let (_dir, cgroup) = temp_cgroup();
        // `cgroup.kill` refused selects the sweep (the only path that freezes at
        // all — a kernel ≥ 5.14 returns from the atomic write and never gets here).
        // Of the sweep's two `cgroup.freeze` writes the first — the freeze — lands
        // for real, and the second — the thaw — is refused from there on, the way a
        // delegation revoked mid-teardown refuses it.
        let faults = Faults::new()
            .fail_every(SITE, Some("cgroup.kill"), libc::EACCES)
            .fail_from_nth(SITE, Some("cgroup.freeze"), 2, libc::EACCES)
            .arm();

        let err = cgroup
            .kill()
            .expect_err("a cgroup left frozen is not a kill the caller can build on");

        // The state assertion the report exists for: the freeze write really
        // reached the file (so this is not a test of a write that never happened),
        // and nothing cleared it — the group is left frozen, for real, on disk.
        assert_eq!(
            std::fs::read_to_string(cgroup.path.join("cgroup.freeze"))
                .expect("the freeze the sweep wrote"),
            "1",
            "the sweep's freeze landed and no thaw cleared it"
        );
        // 1 × `cgroup.kill` + 2 × `cgroup.freeze`: the thaw and its one retry. The
        // retry is the bounded "try to actually restore the group" step; a third
        // freeze write would mean it had grown into a wait loop on the caller's
        // thread, and a first would mean the sweep never froze anything.
        assert_eq!(faults.fired(SITE), 3, "the thaw was retried exactly once");

        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "the refusal keeps its own kind, as a refused suspend does"
        );
        let text = err.to_string();
        assert!(
            text.contains("FROZEN") && text.contains(&format!("os error {}", libc::EACCES)),
            "the caller must be told the group is frozen, and why: {text}"
        );
        // And what `ProcessGroup::kill_all` publishes for it — the mapping that
        // public verb applies to its backend's `io::Error`.
        assert_eq!(
            crate::Error::io(err).kind(),
            crate::ErrorKind::PermissionDenied,
            "a refused thaw reaches the caller as a permission problem, not a silent success"
        );
    }

    /// That report has to survive being asked twice. `ProcessGroup::kill_all` is
    /// documented **idempotent**, and the error above tells the caller the tree is
    /// dead but the group is frozen — so the most natural next move, calling it
    /// again, must not answer `Ok(())` over the very group its predecessor refused
    /// to call cleanly killed. Nothing the answer depends on changed in between:
    /// the group is still frozen, and the host still refuses to clear it.
    ///
    /// It is precisely the second call that catches an answer inferred from *this
    /// call's* freeze write instead of read from the freezer: on the repeat, the
    /// revoked delegation refuses that write too, so no freeze of this call's own
    /// went in — and the drained member list says nothing about a freeze that was
    /// already there. Only the file itself can tell it the group is unusable.
    #[test]
    fn a_second_kill_over_a_group_left_frozen_reports_it_frozen_again() {
        let (_dir, cgroup) = temp_cgroup();
        // Call one, exactly as above: the freeze lands, the thaw is refused, the
        // group is left frozen and reported.
        let first = Faults::new()
            .fail_every(SITE, Some("cgroup.kill"), libc::EACCES)
            .fail_from_nth(SITE, Some("cgroup.freeze"), 2, libc::EACCES)
            .arm();
        cgroup
            .kill()
            .expect_err("the first call leaves the group frozen and says so");
        drop(first);

        // Call two, under what refused the thaw: a cgroup whose control writes are
        // all refused now, including this call's own freeze.
        let faults = Faults::new().fail_every(SITE, None, libc::EACCES).arm();

        let err = cgroup
            .kill()
            .expect_err("a group that is still frozen is still not one the caller can spawn into");

        assert_eq!(
            std::fs::read_to_string(cgroup.path.join("cgroup.freeze"))
                .expect("the freeze the first call left behind"),
            "1",
            "the group really is still frozen — nothing thawed it between the two calls"
        );
        // 1 × `cgroup.kill` + 3 × `cgroup.freeze`: this call's own (refused) freeze,
        // then the thaw and its one retry. Four, not three: unlike the first call,
        // no write here reaches the file at all.
        assert_eq!(
            faults.fired(SITE),
            4,
            "the repeat's own freeze was refused, and its thaw still retried exactly once"
        );
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "the repeat reports the refusal in its own right, not as a copy of the first"
        );
        assert!(
            err.to_string().contains("FROZEN"),
            "the second answer must name the same state as the first: {err}"
        );
    }

    /// The mirror case, and the reason the report above is conditional on the group
    /// being frozen rather than on the thaw being refused. A write-restricted
    /// delegated cgroup refuses *every* control write — which is what selected this
    /// fallback in the first place — so no freeze went in and the thaw's refusal
    /// changes nothing: the group is left exactly as this call found it, unfrozen.
    /// The per-pid sweep needs no cgroup write at all, so it is a complete teardown
    /// there, and reporting it as a failure would make `kill_all` permanently `Err`
    /// on those hosts over a state that is not there.
    ///
    /// Both ways of asking agree here, which is what keeps this a clean kill on a
    /// host that grants no cgroup access whatsoever: the state read finds no
    /// `cgroup.freeze` to read, and the fallback it then defers to — no freeze of
    /// this call's own landed — says the same.
    #[test]
    fn a_refused_thaw_that_never_froze_anything_still_reports_a_clean_kill() {
        let (_dir, cgroup) = temp_cgroup();
        let faults = Faults::new().fail_every(SITE, None, libc::EACCES).arm();

        cgroup
            .kill()
            .expect("a sweep that drained the tree without ever freezing it is a kill");

        // `cgroup.kill`, the freeze, the thaw — and no fourth write: with nothing
        // frozen there is nothing for a retry to restore, so it is skipped along
        // with its sleep rather than spending the caller's thread on it.
        assert_eq!(faults.fired(SITE), 3, "the refused thaw was not retried");
        assert!(
            !cgroup.path.join("cgroup.freeze").exists(),
            "no freeze was ever put in force"
        );
    }

    /// The third way the thaw can fail, and the one that is never the caller's
    /// problem: `ENOENT`. Here the freeze landed, so this is not the pre-5.2 kernel
    /// whose freeze was a no-op — the cgroup directory itself went away under the
    /// teardown. A removed cgroup holds nobody frozen, so the kill stands, exactly
    /// as [`Cgroup::freeze`] treats an absent file as its own case rather than a
    /// refusal.
    ///
    /// This is also what pins the order in which the two are asked. The freeze
    /// really landed, so the file on this stand-in still reads `1` and this call's
    /// own `froze` is `true` — both the state read and its fallback would call the
    /// group frozen. `ENOENT` from the write outranks them because it says the file
    /// they would speak for is gone.
    #[test]
    fn a_thaw_onto_a_cgroup_that_vanished_still_reports_a_clean_kill() {
        let (_dir, cgroup) = temp_cgroup();
        let faults = Faults::new()
            .fail_every(SITE, Some("cgroup.kill"), libc::EACCES)
            .fail_from_nth(SITE, Some("cgroup.freeze"), 2, libc::ENOENT)
            .arm();

        cgroup
            .kill()
            .expect("a cgroup that no longer exists is holding nothing frozen");

        assert_eq!(
            faults.fired(SITE),
            2,
            "`cgroup.kill` and the thaw; an absent file is not retried either"
        );
    }
}

/// Fail-safe coverage for the two paths that read `cgroup.procs` through the
/// **real** filesystem rather than the `_with` seam above:
/// `GracefulTarget::is_drained` (whose signature is fixed by the trait, so no
/// reader can be injected) and `Job`'s `Drop` drain wait (which calls the
/// zero-arg `Cgroup::is_empty` directly, for the same reason — `Drop::drop`
/// can't take a parameter either). Both build a real temporary "cgroup"
/// directory with an unreadable `cgroup.procs` (`chmod 000`) to reproduce an
/// EACCES read failure without a real cgroup v2 mount, and skip (rather than
/// false-fail) when the environment can read past the permission bits (e.g.
/// running as root).
#[cfg(test)]
mod fail_safe_tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use super::{Backend, Cgroup, Job};
    use crate::sys::SkipDropKill;
    use crate::sys::graceful::GracefulTarget;

    /// A throwaway directory standing in for a cgroup, with an unreadable
    /// `cgroup.procs`. Returns `None` (rather than panicking) when this
    /// environment can read past `chmod 000` (e.g. running as root), since the
    /// fail-safe behaviour under test is not reachable there.
    fn unreadable_procs_cgroup() -> Option<(Cgroup, PathBuf)> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "processkit-failsafe-test-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp cgroup dir");
        let procs = dir.join("cgroup.procs");
        std::fs::write(&procs, b"").expect("create cgroup.procs");
        std::fs::set_permissions(&procs, std::fs::Permissions::from_mode(0o000))
            .expect("revoke read permission on cgroup.procs");

        let cg = Cgroup::at(dir.clone());
        if cg.is_empty().is_ok() {
            let _ = std::fs::remove_dir_all(&dir);
            eprintln!(
                "skipping: this environment can read past chmod 000 (likely running as root) \
                 — the fail-safe path under test is not reachable here"
            );
            return None;
        }
        Some((cg, dir))
    }

    #[test]
    fn is_drained_treats_unreadable_procs_as_not_drained() {
        let Some((cg, dir)) = unreadable_procs_cgroup() else {
            return;
        };

        assert!(
            !cg.is_drained(),
            "an unreadable member list is unknown, not drained — GracefulTarget::is_drained \
             must not treat it as an empty cgroup (doing so would cancel the escalation)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_keeps_waiting_out_the_bounded_drain_when_procs_is_unreadable() {
        let Some((cg, dir)) = unreadable_procs_cgroup() else {
            return;
        };

        // Armed (default `SkipDropKill::new()`): `Drop` must run its ~100ms
        // bounded drain wait, not skip it.
        let job = Job {
            backend: Backend::Cgroup(cg),
            skip_drop_kill: SkipDropKill::new(),
        };
        let start = Instant::now();
        drop(job);
        let elapsed = start.elapsed();

        // The wait is 50 iterations * 2ms = ~100ms; an unreadable `cgroup.procs`
        // must not be mistaken for "drained" (`Ok(true)`) and short-circuit it —
        // a regression here would look like `Ok(false) | Err(_) => break`.
        assert!(
            elapsed >= Duration::from_millis(90),
            "Job::drop exited its drain wait early ({elapsed:?}) — an unreadable member \
             list must not be treated as an empty (drained) cgroup"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(all(test, feature = "limits"))]
mod tests {
    use super::{controllers_to_enable, cpu_max_value, flat_keyed_value};

    #[test]
    fn flat_keyed_value_reads_a_counter_by_whole_key() {
        // Real `memory.events` shape.
        let events = "low 0\nhigh 0\nmax 50022\noom 1\noom_kill 3\noom_group_kill 0\n";
        assert_eq!(flat_keyed_value(events, "oom"), Some(1));
        assert_eq!(flat_keyed_value(events, "oom_kill"), Some(3));
        assert_eq!(flat_keyed_value(events, "max"), Some(50022));
        // Whole-key matching: `oom` must not be satisfied by the `oom_kill` /
        // `oom_group_kill` lines that sit in the same file, in either direction.
        assert_eq!(flat_keyed_value("oom_kill 3\n", "oom"), None);
        assert_eq!(flat_keyed_value("oom 1\n", "oom_kill"), None);
    }

    #[test]
    fn flat_keyed_value_separates_absent_from_zero() {
        // The distinction the whole three-valued verdict rests on: a key that is
        // not accounted (None → Unknown) vs one that is accounted and zero
        // (Some(0) → a decisive NotTripped).
        assert_eq!(flat_keyed_value("max 0\n", "max"), Some(0));
        assert_eq!(flat_keyed_value("", "max"), None);
        assert_eq!(flat_keyed_value("usage_usec 42\n", "nr_throttled"), None);
        // Unparsable or truncated values are an honest miss, never a fabricated 0.
        assert_eq!(flat_keyed_value("max\n", "max"), None);
        assert_eq!(flat_keyed_value("max nan\n", "max"), None);
        assert_eq!(flat_keyed_value("max -1\n", "max"), None);
        // Tolerates the trailing-whitespace / multi-space shapes a sysfs read can
        // hand back, and finds a key on any line.
        assert_eq!(
            flat_keyed_value("a 1\nnr_throttled  21 \n", "nr_throttled"),
            Some(21)
        );
    }

    #[test]
    fn cpu_max_formats_quota_and_period() {
        // quota = cores * period(100000µs); period fixed at 100ms.
        assert_eq!(cpu_max_value(0.5), "50000 100000");
        assert_eq!(cpu_max_value(2.0), "200000 100000");
        // A vanishingly small quota floors at 1µs (a zero quota would be invalid).
        assert_eq!(cpu_max_value(0.000_001), "1 100000");
    }

    #[test]
    fn controllers_to_enable_skips_already_enabled_ones() {
        // Nothing missing → empty (skip the redundant subtree_control write,
        // which is what makes limits work in an already-delegated environment).
        assert!(controllers_to_enable(&["memory", "pids"], "cpu memory pids").is_empty());
        // Only the genuinely-missing controllers are returned, order preserved.
        assert_eq!(
            controllers_to_enable(&["memory", "pids", "cpu"], "memory"),
            ["pids", "cpu"]
        );
        // An empty / absent subtree_control means all are needed.
        assert_eq!(controllers_to_enable(&["memory"], ""), ["memory"]);
        // Extra controllers in subtree_control are ignored.
        assert!(controllers_to_enable(&["pids"], "pids io hugetlb").is_empty());
    }
}

/// T-079 (Linux cgroup re-arm race). The cgroup arm of [`Job::graceful_shutdown`]
/// drives the shared [`graceful::run`](crate::sys::graceful::run) with the `Job`'s
/// own `skip_drop_kill` latch, so a `spawn`/`adopt` that re-arms the backstop while
/// the shutdown is mid-poll must win over the shutdown's stale spare — exactly like
/// the pgroup fallback. Deterministic on the paused clock and *not* limits-gated
/// (so it runs in the default test config, unlike the cgroup-formatting tests
/// above): a fake `GracefulTarget` re-arms the latch during the drain wait, standing
/// in for the concurrent spawn/adopt without needing a real cgroup.
#[cfg(test)]
mod rearm_race_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A target that re-arms the shared latch on its second drain check (the
    /// concurrent spawn/adopt joining the cgroup), then keeps reporting "not
    /// drained" so the driver runs to the deadline and issues its stale request.
    struct RacingRearm<'a> {
        latch: &'a crate::sys::SkipDropKill,
        polls: AtomicUsize,
    }
    impl crate::sys::graceful::GracefulTarget for RacingRearm<'_> {
        fn signal_all(&self, _signal: i32) -> crate::sys::graceful::SoftDelivery {
            crate::sys::graceful::SoftDelivery::Sent
        }
        fn is_drained(&self) -> bool {
            if self.polls.fetch_add(1, Ordering::Relaxed) == 1 {
                self.latch.clear();
            }
            false
        }
        fn alive_count(&self) -> Option<usize> {
            None
        }
        fn hard_kill(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_request_does_not_override_a_concurrent_rearm() {
        // Models the cgroup `Job`: a non-escalating shutdown driving the shared
        // graceful driver against the Job's own `skip_drop_kill`.
        let skip = crate::sys::SkipDropKill::new();
        skip.clear(); // a live reused group — backstop already armed
        let target = RacingRearm {
            latch: &skip,
            polls: AtomicUsize::new(0),
        };
        crate::sys::graceful::run(
            &target,
            &skip,
            libc::SIGTERM,
            Duration::from_millis(100),
            false,
        )
        .await
        .expect("graceful run");
        assert!(
            !skip.is_set(),
            "a child that joined the cgroup mid-shutdown must keep its Drop-kill \
             backstop — the stale request must not re-spare it (Job::drop then \
             cgroup.kill's the tree)"
        );
    }
}

/// Linux integration coverage for the real pidfd mechanism behind the
/// identity-safe per-member signal path ([`deliver_identity_safe`]). These drive
/// the *actual* `pidfd_open`/`pidfd_send_signal` syscalls against real child
/// processes (no cgroup mount needed), and skip — rather than fail — when the
/// kernel lacks pidfd (< 5.3) or a seccomp filter blocks it, since the mechanism
/// under test is then unreachable. Complements the deterministic decision-logic
/// tests in `cgroup_read_seam_tests`, which use injected syscall seams.
#[cfg(test)]
mod pidfd_integration_tests {
    use super::{Delivery, deliver_identity_safe, pidfd_open, pidfd_send_signal};

    /// Whether this kernel/sandbox exposes `pidfd_open` — probed against our own
    /// pid. `ENOSYS`/`EPERM` (old kernel, seccomp) ⇒ the mechanism is unreachable
    /// and these tests skip instead of false-failing.
    fn pidfd_available() -> bool {
        pidfd_open(std::process::id() as i32).is_ok()
    }

    /// Spawn a real, long-lived child to pin. `sleep` is POSIX-standard on any
    /// Linux host; it does not trap `SIGTERM`, so a delivered `SIGTERM` kills it.
    fn spawn_sleeper() -> std::process::Child {
        std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn `sleep 30`")
    }

    #[test]
    fn pidfd_pins_identity_and_reports_exit_via_esrch() {
        if !pidfd_available() {
            eprintln!("skipping: pidfd_open unavailable on this kernel/sandbox");
            return;
        }
        let mut child = spawn_sleeper();
        let pid = child.id() as i32;
        let fd = pidfd_open(pid).expect("pin the live child");
        // Signal 0 is a pure existence/permission probe: the child is alive, so Ok.
        pidfd_send_signal(&fd, 0).expect("null-signal a live pinned child");
        // Kill and reap, then the pinned fd must report the task gone (ESRCH). It
        // can NEVER be revived by a process that later recycles `pid` — the whole
        // point of pinning by pidfd rather than by number.
        child.kill().expect("kill child");
        child.wait().expect("reap child");
        let err =
            pidfd_send_signal(&fd, 0).expect_err("a reaped, pinned task must not be signallable");
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ESRCH),
            "a pinned task that exited must report ESRCH, never signal a recycled pid"
        );
    }

    #[test]
    fn a_live_non_member_is_skipped_by_the_real_primitive() {
        if !pidfd_available() {
            eprintln!("skipping: pidfd_open unavailable on this kernel/sandbox");
            return;
        }
        let mut child = spawn_sleeper();
        let pid = child.id() as i32;
        // Real `pidfd_open`/`pidfd_send_signal`, but the membership reconfirm
        // reports "not a member" (modelling a pid recycled by a process outside
        // the cgroup). The primitive must skip: the would-be-fatal SIGKILL is never
        // sent, so the child stays alive.
        let outcome = deliver_identity_safe(
            pid,
            libc::SIGKILL,
            pidfd_open,
            |_| Ok(false),
            pidfd_send_signal,
        );
        assert!(matches!(outcome, Delivery::Skipped));
        assert!(
            child.try_wait().expect("try_wait").is_none(),
            "a non-member must receive no signal — the live child is untouched"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn a_confirmed_live_member_is_delivered_to() {
        use std::os::unix::process::ExitStatusExt;

        if !pidfd_available() {
            eprintln!("skipping: pidfd_open unavailable on this kernel/sandbox");
            return;
        }
        let mut child = spawn_sleeper();
        let pid = child.id() as i32;
        // Confirmed member + real syscalls: SIGTERM is delivered and the sleeper,
        // which does not trap SIGTERM, exits. Proves the real pidfd send path works
        // end to end, not just the fail-safe branches.
        let outcome = deliver_identity_safe(
            pid,
            libc::SIGTERM,
            pidfd_open,
            |_| Ok(true),
            pidfd_send_signal,
        );
        assert!(matches!(outcome, Delivery::Delivered));
        // `wait` blocks until the child dies, so the SIGTERM has taken effect.
        let status = child.wait().expect("reap the signalled child");
        assert_eq!(
            status.signal(),
            Some(libc::SIGTERM),
            "the child exited on the SIGTERM we delivered through the pidfd"
        );
    }
}

/// Identity-safe group-stats fold (T-090). These drive the pin → reconfirm
/// membership → read-gated-on-identity decision logic of
/// [`sample_member_identity_safe`] through injected seams, so the pid-reuse race in
/// the `Cgroup::stats` window is reproduced deterministically without a real
/// `/proc` or cgroup — the stats analogue of `cgroup_read_seam_tests`'
/// `deliver_identity_safe` coverage. A second group exercises the real
/// `process_identity`/`process_metrics` identity gate against this process itself
/// (a live pid whose start-time is stable), where a deliberately-wrong identity
/// stands in for a recycled pid.
#[cfg(all(test, feature = "stats"))]
mod member_sample_tests {
    use std::cell::Cell;
    use std::io;
    use std::time::Duration;

    use super::{
        Cgroup, MemberSample, ProcIdentity, process_identity, process_metrics,
        process_metrics_with_seams, read_proc_starttime, sample_member_identity_safe,
    };
    use crate::sys::ProcMetrics;

    /// A mock cgroup whose `cgroup.procs` reads come from an injected seam, so the
    /// batched `stats_with_seams` fold can be driven without a real cgroup mount.
    fn cgroup() -> Cgroup {
        Cgroup::at(std::path::PathBuf::from("/mock/processkit"))
    }

    /// A non-empty reading, so a fold that reaches it is observable.
    fn some_metrics() -> ProcMetrics {
        ProcMetrics {
            cpu_time: Some(Duration::from_millis(10)),
            peak_memory_bytes: Some(2048),
        }
    }

    #[test]
    fn reused_pid_outside_cgroup_is_never_folded() {
        // The identity pins, but by reconfirm time the original member has exited
        // and its pid was recycled by a process OUTSIDE the cgroup, so
        // `still_member` reports false. The fold must skip and never read counters —
        // the core group-stats PID-reuse safety.
        let read = Cell::new(false);
        let outcome = sample_member_identity_safe(
            1234,
            |_| Some(ProcIdentity::from_raw(42)),
            |_| Ok(false),
            |_, _| {
                read.set(true);
                some_metrics()
            },
        );
        assert!(matches!(outcome, MemberSample::Skipped));
        assert!(
            !read.get(),
            "a pid recycled outside the cgroup must never have its counters folded"
        );
    }

    #[test]
    fn confirmed_member_is_folded_with_its_counters() {
        let outcome = sample_member_identity_safe(
            42,
            |_| Some(ProcIdentity::from_raw(7)),
            |_| Ok(true),
            |_, _| some_metrics(),
        );
        match outcome {
            MemberSample::Folded(m) => {
                assert_eq!(m.cpu_time, Some(Duration::from_millis(10)));
                assert_eq!(m.peak_memory_bytes, Some(2048));
            }
            _ => panic!("a confirmed member must be folded"),
        }
    }

    #[test]
    fn member_gone_before_pin_is_a_benign_skip() {
        // `capture_identity` fails: the member exited before we could pin it.
        // Benign — membership is not even consulted and no counters are read.
        let read = Cell::new(false);
        let outcome = sample_member_identity_safe(
            7,
            |_| None,
            |_| -> io::Result<bool> { panic!("membership must not be checked once the pin fails") },
            |_, _| {
                read.set(true);
                some_metrics()
            },
        );
        assert!(matches!(outcome, MemberSample::Skipped));
        assert!(!read.get(), "a gone member's counters must not be read");
    }

    #[test]
    fn unreadable_membership_fails_safe_without_reading_counters() {
        // Reconfirming membership fails (EACCES): unknown membership must not be
        // folded — fail safe, surface the error, read nothing.
        let read = Cell::new(false);
        let outcome = sample_member_identity_safe(
            7,
            |_| Some(ProcIdentity::from_raw(1)),
            |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            |_, _| {
                read.set(true);
                some_metrics()
            },
        );
        match outcome {
            MemberSample::Failed(e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            _ => panic!("an unreadable membership must fail safe"),
        }
        assert!(!read.get(), "fail-safe must not read any counters");
    }

    #[test]
    fn recycle_after_reconfirm_folds_nothing() {
        // Membership is confirmed, but the pid is recycled between the reconfirm and
        // the metrics read: `process_metrics(pid, Some(id))` then sees a mismatching
        // identity and returns the all-`None` default. The fold reaches step 3 but
        // sums nothing, so a stranger's counters never enter the aggregate.
        let outcome = sample_member_identity_safe(
            7,
            |_| Some(ProcIdentity::from_raw(1)),
            |_| Ok(true),
            |_, _| ProcMetrics::default(),
        );
        match outcome {
            MemberSample::Folded(m) => {
                assert!(
                    m.cpu_time.is_none() && m.peak_memory_bytes.is_none(),
                    "a recycle caught by the identity-gated read contributes nothing"
                );
            }
            _ => panic!("a confirmed member is folded (with an all-None reading here)"),
        }
    }

    // ---- batched fold (`stats_with_seams`): one read for the whole tree ----
    //
    // The production fold pins (captures the identity of) every member first,
    // reads `cgroup.procs` exactly once, then reconfirms each pinned member
    // against that single snapshot. These drive it through all three injected
    // seams (counting reader + fake identity/metrics) so both the O(1) read cost
    // and the pid-reuse skip are observable — the stats analogue of
    // `cgroup_read_seam_tests`' batched-broadcast coverage.

    #[test]
    fn stats_reads_cgroup_procs_a_constant_number_of_times_for_a_whole_tree() {
        // A tree of 100 members must still cost a constant number of `cgroup.procs`
        // reads, not one per pid: the old per-member reconfirm made this 1 + n
        // (101) reads of an n-line file — the O(n^2) work this task removes.
        let members = (1000..1100)
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let reads = Cell::new(0usize);
        let stats = cgroup()
            .stats_with_seams(
                |_| {
                    reads.set(reads.get() + 1);
                    Ok(members.clone())
                },
                |_| Some(ProcIdentity::from_raw(1)),
                |_, _| some_metrics(),
            )
            .expect("a fully-confirmed tree folds cleanly");
        assert_eq!(
            reads.get(),
            2,
            "one read for the initial member list + one shared reconfirm read, \
             independent of the 100 members (was 1 + n before this task)"
        );
        assert_eq!(stats.active_process_count, 100);
        assert_eq!(
            stats.total_cpu_time,
            Some(Duration::from_millis(1000)),
            "100 members × 10ms folded once each"
        );
        assert_eq!(
            stats.peak_memory_bytes,
            Some(204_800),
            "100 members × 2048 bytes folded once each"
        );
    }

    #[test]
    fn stats_skips_a_pid_recycled_outside_the_cgroup_via_the_single_snapshot() {
        // Pid 1002 is pinned from the initial list but has left the cgroup by the
        // one reconfirm snapshot (recycled outside). Its counters must not be
        // folded, while the rest are — the single shared snapshot preserving the
        // pin→reconfirm→read pid-reuse safety of `sample_member_identity_safe`.
        let reads = Cell::new(0usize);
        let folded = std::cell::RefCell::new(Vec::new());
        let stats = cgroup()
            .stats_with_seams(
                |_| {
                    reads.set(reads.get() + 1);
                    // 1st read: initial member list. 2nd read: reconfirm snapshot,
                    // with 1002 already gone.
                    Ok(if reads.get() == 1 {
                        "1001\n1002\n1003\n".to_owned()
                    } else {
                        "1001\n1003\n".to_owned()
                    })
                },
                |_| Some(ProcIdentity::from_raw(1)),
                |pid, _| {
                    folded.borrow_mut().push(pid);
                    some_metrics()
                },
            )
            .expect("a benign recycle race is not a fold failure");
        assert_eq!(
            *folded.borrow(),
            vec![1001, 1003],
            "only members present in the single reconfirm snapshot have their counters read"
        );
        assert_eq!(
            stats.active_process_count, 3,
            "active count reflects the initial member list, before the recycle"
        );
        assert_eq!(reads.get(), 2, "still exactly two reads for the whole fold");
        assert_eq!(
            stats.total_cpu_time,
            Some(Duration::from_millis(20)),
            "only the two confirmed members (1001, 1003) are folded"
        );
        assert_eq!(stats.peak_memory_bytes, Some(4096));
    }

    // ---- the real /proc identity gate, driven against our own live process ----

    #[test]
    fn process_identity_matches_a_same_process_metrics_read() {
        let me = std::process::id();
        assert!(
            read_proc_starttime(me).is_some(),
            "our own /proc/<pid>/stat starttime must be readable"
        );
        let id = process_identity(me).expect("our own live process has a start identity");
        let gated = process_metrics(me, Some(id));
        assert!(
            gated.cpu_time.is_some(),
            "an identity-matched read of our own process reports CPU time"
        );
    }

    #[test]
    fn identity_change_after_status_read_discards_both_process_metrics() {
        fn stat_with_starttime(starttime: u64) -> String {
            format!("1 (mock) S 0 0 0 0 0 0 0 0 0 0 5 7 0 0 0 0 0 0 {starttime}")
        }

        let original = ProcIdentity::from_raw(100);
        let stat_reads = Cell::new(0);
        let metrics = process_metrics_with_seams(
            42,
            Some(original),
            |_| {
                let read = stat_reads.get();
                stat_reads.set(read + 1);
                Some(stat_with_starttime(if read == 0 { 100 } else { 200 }))
            },
            |_| Some("Name:\trecycled\nVmHWM:\t123 kB\n".to_owned()),
        );

        assert_eq!(
            stat_reads.get(),
            2,
            "identity is checked on both sides of status"
        );
        assert!(
            metrics.cpu_time.is_none() && metrics.peak_memory_bytes.is_none(),
            "a post-status identity mismatch must discard CPU and the replacement process's memory"
        );
    }

    #[test]
    fn a_mismatched_identity_yields_defaults_not_the_live_process_counters() {
        let me = std::process::id();
        let real = process_identity(me).expect("our own live process has a start identity");
        // A wrong starttime models a pid recycled by a different process: even though
        // the pid is alive (it is us), the gate must return the all-`None` default.
        let bogus = ProcIdentity::from_raw(real.raw().wrapping_add(1));
        let gated = process_metrics(me, Some(bogus));
        assert!(
            gated.cpu_time.is_none() && gated.peak_memory_bytes.is_none(),
            "a mismatched identity must yield defaults, never the live process's \
             CPU/memory — the recycled-pid fail-safe"
        );
        // Without a demanded identity the number-only behavior is preserved.
        assert!(
            process_metrics(me, None).cpu_time.is_some(),
            "an unchecked read (identity None) still reports metrics"
        );
    }
}

/// Tests for the read-only mechanism detection (`detect_mechanism`) that backs the
/// public `host_containment()` query: it must never create a cgroup directory, and
/// must agree with a really-created group's mechanism on this same host.
#[cfg(test)]
mod detect_mechanism_tests {
    use std::cell::Cell;
    use std::io;
    use std::path::Path;

    use super::{
        HardKillPrimitive, Job, cgroup2_root, cgroup2_self_dir, child_cgroup_kill_available,
        detect_mechanism, dir_allows_subdir_creation, hard_kill_primitive_with,
        kernel_release_supports_cgroup_kill, predicted_hard_kill_primitive_with,
    };
    use crate::Mechanism;

    /// Build a bare `Job`, papering over the `limits`-feature gate on `Job::new`.
    fn new_job() -> Job {
        #[cfg(feature = "limits")]
        {
            Job::new(&crate::limits::ResourceLimits::default()).expect("create a job")
        }
        #[cfg(not(feature = "limits"))]
        {
            Job::new().expect("create a job")
        }
    }

    #[test]
    fn detection_reports_a_valid_linux_mechanism() {
        // Linux is cgroup v2 or its POSIX process-group fallback — never anything
        // else, and never a silent "unknown".
        assert!(
            matches!(
                detect_mechanism(),
                Mechanism::CgroupV2 | Mechanism::ProcessGroup
            ),
            "linux detection is cgroup v2 or its pgroup fallback"
        );
    }

    #[test]
    fn cgroup_kill_alone_is_a_hard_kill_primitive() {
        let pidfd_calls = Cell::new(0);
        let cgroup = Path::new("/stand-in/job");

        let primitive = hard_kill_primitive_with(
            cgroup,
            |path| {
                assert_eq!(path, cgroup.join("cgroup.kill"));
                true
            },
            |_| {
                pidfd_calls.set(pidfd_calls.get() + 1);
                Err(io::Error::from(io::ErrorKind::Unsupported))
            },
        );

        assert_eq!(primitive, Some(HardKillPrimitive::CgroupKill));
        assert_eq!(pidfd_calls.get(), 0, "cgroup.kill avoids a needless pidfd");
    }

    #[test]
    fn pidfd_alone_is_a_hard_kill_primitive() {
        let cgroup = Path::new("/stand-in/job");

        let primitive = hard_kill_primitive_with(
            cgroup,
            |path| {
                assert_eq!(path, cgroup.join("cgroup.kill"));
                false
            },
            |pid| {
                assert_eq!(pid, std::process::id() as i32);
                Ok(())
            },
        );

        assert_eq!(primitive, Some(HardKillPrimitive::Pidfd));
    }

    #[test]
    fn missing_cgroup_kill_and_pidfd_reject_the_cgroup_backend() {
        let cgroup = Path::new("/stand-in/job");

        let primitive = hard_kill_primitive_with(
            cgroup,
            |_| false,
            |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        );

        assert_eq!(primitive, None);
    }

    #[test]
    fn hierarchy_root_without_cgroup_kill_predicts_and_creates_a_killable_child() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let root = std::env::temp_dir().join(format!(
            "processkit-root-kill-probe-{}-{nanos}",
            std::process::id()
        ));
        let existing_child = root.join("existing-child");
        let created_child = root.join("created-child");
        std::fs::create_dir_all(&existing_child).expect("create existing child stand-in");
        std::fs::write(existing_child.join("cgroup.kill"), b"")
            .expect("seed existing child's cgroup.kill");

        assert!(
            !root.join("cgroup.kill").exists(),
            "the hierarchy root deliberately has no cgroup.kill"
        );
        let predicted =
            predicted_hard_kill_primitive_with(&root, &root, child_cgroup_kill_available, |_| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            });

        std::fs::create_dir(&created_child).expect("create prospective child stand-in");
        std::fs::write(created_child.join("cgroup.kill"), b"")
            .expect("seed created child's cgroup.kill");
        let created = hard_kill_primitive_with(&created_child, Path::exists, |_| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        });
        let _ = std::fs::remove_dir_all(&root);

        assert_eq!(predicted, Some(HardKillPrimitive::CgroupKill));
        assert_eq!(created, Some(HardKillPrimitive::CgroupKill));
    }

    #[test]
    fn kernel_release_predicts_cgroup_kill_for_mainline_versions() {
        assert!(!kernel_release_supports_cgroup_kill("5.13.19"));
        assert!(kernel_release_supports_cgroup_kill("5.14.0"));
        assert!(kernel_release_supports_cgroup_kill("6.17.0-rc1"));
        assert!(!kernel_release_supports_cgroup_kill("not-a-release"));
    }

    #[test]
    fn the_writability_probe_creates_no_filesystem_entry() {
        // The permission half of `detect_mechanism` writes nothing: probe a fresh,
        // empty scratch dir and assert it stays empty afterwards. The hard-kill
        // half uses only an existence check and pidfd_open on this process.
        let tmp =
            std::env::temp_dir().join(format!("processkit-detect-probe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).expect("scratch dir");
        let _ = dir_allows_subdir_creation(&tmp);
        let stayed_empty = std::fs::read_dir(&tmp)
            .expect("read scratch dir")
            .next()
            .is_none();
        let _ = std::fs::remove_dir_all(&tmp);
        assert!(
            stayed_empty,
            "the writability probe must create no filesystem entry"
        );
    }

    #[test]
    fn query_creates_no_cgroup_dir_and_matches_a_real_group() {
        // Count `processkit-*` leaf dirs under this process's own cgroup (if it is
        // resolvable/readable on this host) before and after hammering the read-only
        // query: it must leave that set unchanged — unlike `Cgroup::create`, which
        // `mkdir`s a leaf. The snapshot is taken *before* any group is created below,
        // so this test never races its own `new_job()`.
        let parent = cgroup2_root().and_then(|root| cgroup2_self_dir(&root).ok());
        let count_pk_dirs = |dir: &Path| -> usize {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return 0;
            };
            entries
                .filter_map(Result::ok)
                .filter(|e| e.file_name().to_string_lossy().starts_with("processkit-"))
                .count()
        };
        let before = parent.as_deref().map(count_pk_dirs);
        for _ in 0..32 {
            let _ = detect_mechanism();
        }
        let after = parent.as_deref().map(count_pk_dirs);
        assert_eq!(
            before, after,
            "the read-only host query must create no cgroup directory"
        );

        // And it must agree with a really-created group's mechanism on this host —
        // the core consistency contract (cgroup v2 with or without delegation, or
        // the pgroup fallback, whichever this host actually yields).
        let job = new_job();
        assert_eq!(
            detect_mechanism(),
            job.mechanism(),
            "the read-only mechanism query must match a really-created group's mechanism"
        );
    }
}

/// T-270 (the cgroup arm). This backend re-arms and reads the `skip_drop_kill`
/// latch on the `Job` itself, while the process-group fallback uses the one inside
/// its own `ProcessGroup` — two different objects — so the cgroup arm's PTY rollback
/// has to restore the spare its own spawn displaced on *that* latch.
///
/// Both tests build a `Job` over a throwaway directory standing in for a cgroup,
/// which is enough because the rollback path under test (`hard_kill_fresh_spawn` +
/// restore) reads no cgroup file at all, and the latch is precisely what
/// `Job::drop` consults before killing. What a stand-in directory cannot show is a
/// real cgroup tearing a survivor down, so these assert the latch; the end-to-end
/// "the spared survivor outlives a failed PTY launch" run lives in
/// `sys::pty::imp::tests` and picks up whichever mechanism the host really provides.
#[cfg(all(test, feature = "pty"))]
mod pty_rollback_spare_tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use tokio::process::{Child, Command};

    use super::{Backend, Cgroup, Job};
    use crate::sys::fault_injection::{Faults, Site};
    use crate::sys::{SkipDropKill, SpawnOptions};

    /// A cgroup-arm `Job` over a throwaway directory carrying the one interface file
    /// this backend's spawn path writes (`cgroup.procs`), plus that directory so the
    /// test can remove it — `Job::drop`'s own `rmdir` cannot, the file being there.
    fn cgroup_job(tag: &str) -> (Job, PathBuf) {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "processkit-pty-spare-{tag}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create the stand-in cgroup dir");
        std::fs::write(dir.join("cgroup.procs"), b"").expect("create cgroup.procs");
        let job = Job {
            backend: Backend::Cgroup(Cgroup::at(dir.clone())),
            skip_drop_kill: SkipDropKill::new(),
        };
        (job, dir)
    }

    /// A child that ignores the graceful signal, standing in for a member the
    /// shutdown leaves running. Its liveness is not what these tests read — a
    /// stand-in directory's `cgroup.procs` never drains, so the non-escalating
    /// shutdown reaches its deadline and spares whether or not this child is still
    /// there — it is here so the shutdown has a member to be about at all.
    fn survivor_command() -> Command {
        let mut command = Command::new("sh");
        command
            .args(["-c", "trap '' TERM; while :; do sleep 60; done"])
            .kill_on_drop(true);
        command
    }

    /// A pty child that stays alive until the rollback kills it.
    fn idle_command() -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", "while :; do sleep 60; done"]);
        command
    }

    /// A stand-in cgroup contains nothing, so every child this module starts is this
    /// module's to end.
    async fn reap(mut child: Child) {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    /// A PTY launch that fails after its child exists must leave a
    /// `graceful_shutdown(escalate = false)` standing: its spawn re-armed the
    /// kill-on-drop backstop, and only the rollback can undo that. Driven through
    /// the production wiring (`Job::spawn_pty` threads the token from its spawn
    /// closure to its rollback closure) with the master `dup` fault-injected.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns real subprocesses"]
    async fn a_failed_pty_launch_restores_the_cgroup_arms_spare() {
        let (job, dir) = cgroup_job("restore");
        let survivor = job
            .spawn(&mut survivor_command(), &SpawnOptions::default())
            .expect("spawn a cgroup member");

        job.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .expect("graceful shutdown");
        assert!(
            job.skip_drop_kill.is_set(),
            "precondition: a non-escalating shutdown spares the survivors"
        );

        {
            let _fault = Faults::new()
                .fail_every(Site::PtyMasterClone, Some("writer"), libc::EIO)
                .arm();
            let result = job.spawn_pty(&mut idle_command(), &pty_options(), None);
            assert!(
                result.is_err(),
                "the injected master-clone fault must surface as an error"
            );
        }

        assert!(
            job.skip_drop_kill.is_set(),
            "the rollback must restore the spare its own spawn displaced — otherwise \
             Job::drop hard-kills survivors the caller chose not to escalate against"
        );

        drop(job);
        reap(survivor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The transactional half: a `spawn` joined the same job between the rolled-back
    /// spawn's re-arm and its rollback. That newcomer is a live member nothing chose
    /// to spare, so the restore must lose and the backstop must stay armed.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "spawns real subprocesses"]
    async fn a_spawn_between_the_pty_spawn_and_its_rollback_keeps_the_backstop_armed() {
        let (job, dir) = cgroup_job("newcomer");
        let survivor = job
            .spawn(&mut survivor_command(), &SpawnOptions::default())
            .expect("spawn a cgroup member");

        job.graceful_shutdown(libc::SIGTERM, Duration::from_millis(100), false)
            .await
            .expect("graceful shutdown");

        // The launch that will be rolled back: its spawn re-arms the backstop and
        // hands back the spare it displaced.
        let (pty_child, displaced) = job
            .spawn_displacing_spare(&mut idle_command(), &SpawnOptions::default())
            .expect("spawn the pty child");
        let pty_pid = pty_child.id().expect("the pty child reports a pid");
        // …and a fresh member joins before that launch is undone.
        let newcomer = job
            .spawn(&mut survivor_command(), &SpawnOptions::default())
            .expect("spawn the newcomer");

        job.rollback_pty_spawn(pty_pid, displaced);
        assert!(
            !job.skip_drop_kill.is_set(),
            "a member that joined after the rolled-back spawn must keep its \
             kill-on-drop backstop — restoring the older spare would strip it"
        );

        drop(job);
        reap(pty_child).await;
        reap(newcomer).await;
        reap(survivor).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A pty spawn's options — the launch seam sets nothing else for these tests.
    fn pty_options() -> SpawnOptions {
        SpawnOptions {
            use_pty: true,
            ..SpawnOptions::default()
        }
    }
}

/// T-273 (per-spawn leaf sub-cgroups), driven against **real directories** rather
/// than a mocked filesystem: every `mkdir`/`rmdir`/interface-file write these tests
/// assert on is one the backend really performs, on a temporary directory shaped
/// like a job cgroup.
///
/// What such a directory cannot be is a *cgroup*: the kernel populates a real one
/// with its interface files at `mkdir` time, and a plain directory has none — which
/// is exactly the state [`Cgroup::open_leaf`]'s joinability probe declines to route
/// a child into (asserted below), and why the leaves here are seeded by hand
/// instead. The end-to-end path — a spawn landing in a leaf the kernel made, a
/// rollback killing that leaf's subtree, the directories going away — needs a real
/// cgroup v2 hierarchy and lives in `real_cgroup_leaf_tests`.
#[cfg(test)]
mod leaf_cgroup_tests {
    use std::cell::Cell;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::{
        Backend, CGROUP_RECLAIMER_TEST_LOCK, Cgroup, CgroupReclaim, CgroupReclaimBackoff,
        CgroupReclaimerState, Job, LEAF_RECLAIM_FLOOR, LeafSlot, accept_cgroup_reclaim,
        cgroup_reclaimer_state, enqueue_cgroup_reclaim, enqueue_cgroup_reclaim_with_state,
        lock_cgroup_reclaimer,
    };
    use crate::sys::SkipDropKill;

    /// A stand-in job cgroup on a real temporary directory, carrying the two
    /// interface files this backend writes on it (`cgroup.procs` for an adopted
    /// member, `cgroup.kill` for the whole-job teardown), both empty.
    fn job_cgroup() -> (tempfile::TempDir, Cgroup) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("processkit-job");
        std::fs::create_dir(&path).expect("create the stand-in job cgroup dir");
        write_procs(&path, &[]);
        std::fs::write(path.join("cgroup.kill"), "").expect("seed the whole-job kill file");
        (dir, Cgroup::at(path))
    }

    /// Add a leaf to `cg` by hand — shaped the way the kernel shapes a real one (a
    /// `cgroup.procs` listing `members`, a writable `cgroup.kill`) — and register it
    /// as the leaf of the spawn that returned `pid`.
    fn seed_leaf(cg: &Cgroup, name: &str, pid: Option<i32>, members: &[i32]) -> PathBuf {
        let dir = cg.path.join(name);
        std::fs::create_dir(&dir).expect("create the stand-in leaf dir");
        write_procs(&dir, members);
        std::fs::write(dir.join("cgroup.kill"), "").expect("seed the leaf kill file");
        cg.register_leaf(pid, dir.clone());
        dir
    }

    fn write_procs(dir: &Path, pids: &[i32]) {
        let text: String = pids.iter().map(|pid| format!("{pid}\n")).collect();
        std::fs::write(dir.join("cgroup.procs"), text).expect("seed a member list");
    }

    #[cfg(feature = "pty")]
    fn read_back(path: &Path) -> String {
        std::fs::read_to_string(path).expect("read back a control file")
    }

    /// The whole job's membership is the union of its own `cgroup.procs` and every
    /// leaf's — the reason every whole-job verb keeps its reach once spawns stop
    /// landing in the job cgroup itself.
    #[test]
    fn membership_is_the_union_of_the_job_cgroup_and_every_leaf() {
        let (_tmp, cg) = job_cgroup();
        // An adopted member lives in the job cgroup itself; spawns live in leaves.
        write_procs(&cg.path, &[11]);
        seed_leaf(&cg, "spawn-a", Some(101), &[101, 1011]);
        seed_leaf(&cg, "spawn-b", Some(102), &[102]);

        assert_eq!(
            cg.members().expect("read the job's membership"),
            [11, 101, 102, 1011],
            "a whole-job membership read must see every leaf's members, not just the \
             job cgroup's own"
        );
    }

    /// The two ways a leaf's `cgroup.procs` can fail to read, kept apart: a leaf
    /// whose directory is already gone holds nothing (a removed cgroup is empty),
    /// while an unreadable one makes the *job's* membership unknown rather than
    /// silently short — the fail-safe every kill/signal decision rests on.
    #[test]
    fn a_gone_leaf_is_empty_and_an_unreadable_one_is_unknown() {
        let (_tmp, cg) = job_cgroup();
        let gone = seed_leaf(&cg, "spawn-gone", Some(1), &[]);
        std::fs::remove_dir_all(&gone).expect("remove the leaf directory");
        seed_leaf(&cg, "spawn-live", Some(2), &[42]);

        assert_eq!(cg.members().expect("a removed leaf is not a failure"), [42]);

        let err = cg
            .members_with(|path| {
                if path.starts_with(&gone) {
                    Err(io::Error::from(io::ErrorKind::PermissionDenied))
                } else {
                    std::fs::read_to_string(path)
                }
            })
            .expect_err("an unreadable leaf must not look like a job without those members");
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    /// The batched identity-safe broadcast still costs a **constant number of
    /// membership passes** — one `cgroup.procs` read per cgroup of the job, twice
    /// (pin pass, reconfirm pass) — however many pids those cgroups hold. Leaves add
    /// a read per leaf to a pass; what they must not add is a pass, or a rescan per
    /// pid (K-010's pin-before-reconfirm ordering is per pid, the reads are not).
    #[test]
    fn membership_costs_one_read_per_cgroup_per_pass_whatever_the_pid_count() {
        /// A stand-in for a pidfd: this test counts membership reads, and nothing
        /// here needs to know which pid a pin belongs to.
        struct Handle;

        let (_tmp, cg) = job_cgroup();
        seed_leaf(&cg, "spawn-a", Some(101), &[]);
        seed_leaf(&cg, "spawn-b", Some(102), &[]);

        let reads_for = |pids: &[i32]| -> usize {
            let listing: String = pids.iter().map(|pid| format!("{pid}\n")).collect();
            let reads = Cell::new(0usize);
            cg.signal_with_seams(
                libc::SIGTERM,
                |_: &Path| {
                    reads.set(reads.get() + 1);
                    Ok(listing.clone())
                },
                |_pid| Ok(Handle),
                |_: &Handle, _| Ok(()),
            )
            .expect("a broadcast over confirmed members");
            reads.get()
        };

        // 3 cgroups (the job's own plus two leaves) x 2 passes.
        assert_eq!(reads_for(&[1001, 1002, 1003, 1004]), 6);
        assert_eq!(
            reads_for(&[1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008]),
            6,
            "twice the members must not cost a single extra read"
        );
    }

    /// The point of the whole mechanism: the selective kill lands in the leaf of the
    /// spawn it names, and in no other cgroup of the job — not a sibling spawn's
    /// leaf, and not the job's own cgroup, where the same write would take down
    /// every member of every spawn.
    #[cfg(feature = "pty")]
    #[test]
    fn a_selective_kill_reaches_only_the_leaf_of_the_spawn_it_names() {
        let (_tmp, cg) = job_cgroup();
        let a = seed_leaf(&cg, "spawn-a", Some(101), &[101]);
        let b = seed_leaf(&cg, "spawn-b", Some(102), &[102]);

        assert!(
            cg.kill_leaf_of(101),
            "the spawn's leaf is there to be killed"
        );

        assert_eq!(read_back(&a.join("cgroup.kill")), "1");
        assert_eq!(
            read_back(&b.join("cgroup.kill")),
            "",
            "another spawn's leaf must not be touched by this spawn's rollback"
        );
        assert_eq!(
            read_back(&cg.path.join("cgroup.kill")),
            "",
            "the whole-job kill file must not be written by a per-spawn rollback"
        );
        assert!(
            !cg.kill_leaf_of(101),
            "the pid is consumed: a number the kernel may recycle must not aim a \
             second kill at a leaf that is no longer its spawn's"
        );
    }

    /// A refused `cgroup.kill` (a kernel < 5.14 has no such file; a restricted
    /// delegated cgroup can reject the write) is reported as "no selective kill
    /// happened", which is what makes the caller fall back to `killpg` — and the
    /// leaf stays registered, so what it holds is still the job's to enumerate and
    /// to kill.
    #[cfg(feature = "pty")]
    #[test]
    fn a_refused_leaf_kill_reports_no_kill_and_keeps_the_leaf() {
        use crate::sys::fault_injection::{Faults, Site};

        let (_tmp, cg) = job_cgroup();
        let leaf = seed_leaf(&cg, "spawn-a", Some(101), &[101]);

        let faults = Faults::new()
            .fail_every(Site::CgroupWrite, Some("cgroup.kill"), libc::EACCES)
            .arm();
        let killed = cg.kill_leaf_of(101);
        assert_eq!(faults.fired(Site::CgroupWrite), 1);
        drop(faults);

        assert!(
            !killed,
            "a refused write is not a kill — the caller must fall back rather than \
             believe this spawn's tree is gone"
        );
        assert!(leaf.exists(), "the leaf must not be reclaimed unkilled");
        assert_eq!(
            cg.members().expect("read the job's membership"),
            [101],
            "what the selective kill could not reach is still a member of the job"
        );
    }

    /// A reclaim releases exactly the leaves the kernel lets go of. One it cannot
    /// remove still holds something, so its entry stays — an entry is dropped only
    /// on the kernel's own confirmation, which is what keeps a reclaim from
    /// narrowing what the job can enumerate and kill.
    #[test]
    fn a_reclaim_releases_only_the_directories_the_kernel_lets_go_of() {
        let (_tmp, cg) = job_cgroup();
        // A drained leaf: the kernel removes a cgroup's interface files with it, so
        // an empty directory stands in for one that has nothing left in it.
        let drained = cg.path.join("spawn-drained");
        std::fs::create_dir(&drained).expect("create the drained leaf dir");
        cg.register_leaf(Some(1), drained.clone());
        let busy = seed_leaf(&cg, "spawn-busy", Some(2), &[7]);

        cg.reclaim_leaves();

        assert!(!drained.exists(), "a removable leaf directory is reclaimed");
        assert!(busy.exists(), "a leaf that still holds something is kept");
        assert_eq!(cg.leaf_dirs(), [busy], "and only that one stays registered");
        assert_eq!(
            cg.members().expect("read the job's membership"),
            [7],
            "the kept leaf's members are still enumerated"
        );
    }

    /// A long-lived job does not accumulate the directories of spawns that have
    /// finished. The launches themselves pay for that, without any teardown, and
    /// what they leave behind is **bounded by the job's live leaves** rather than by
    /// how many launches it has made — the amortized reclaim keeps some (that is the
    /// point of not sweeping on every launch), but never a number that grows with
    /// the run. What is still in use survives every one of those passes.
    #[test]
    fn finished_leaves_do_not_pile_up_between_teardowns() {
        let (_tmp, cg) = job_cgroup();
        let busy = seed_leaf(&cg, "spawn-busy", Some(0), &[7]);

        // One live leaf throughout, so the bound stays the floor: anything above it
        // would mean the leftovers track the launch count.
        let register_drained_leaves = |count: usize, tag: &str| {
            for n in 0..count {
                let dir = cg.path.join(format!("spawn-{tag}-{n}"));
                std::fs::create_dir(&dir).expect("create a drained leaf dir");
                cg.register_leaf(Some(n as i32 + 1), dir);
            }
            let registered = cg.leaf_dirs();
            let on_disk = std::fs::read_dir(&cg.path)
                .expect("read the job cgroup dir")
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_dir())
                .count();
            (registered, on_disk)
        };

        let (registered, on_disk) = register_drained_leaves(2 * LEAF_RECLAIM_FLOOR, "a");
        assert!(
            registered.len() <= LEAF_RECLAIM_FLOOR && on_disk == registered.len(),
            "after {} launches: {registered:?} registered, {on_disk} directories",
            2 * LEAF_RECLAIM_FLOOR
        );
        assert!(
            registered.contains(&busy),
            "the live leaf is never reclaimed"
        );

        // Twice as many launches again must not leave twice as much behind.
        let (later, later_on_disk) = register_drained_leaves(4 * LEAF_RECLAIM_FLOOR, "b");
        assert!(
            later.len() <= LEAF_RECLAIM_FLOOR && later_on_disk == later.len(),
            "after {} more launches: {later:?} registered, {later_on_disk} directories — \
             the leftovers must not grow with the number of launches",
            4 * LEAF_RECLAIM_FLOOR
        );
        assert!(later.contains(&busy), "and it still is not");
        assert!(busy.exists());
    }

    /// A directory that cannot host a leaf (no `cgroup.procs` appeared in it, so no
    /// child could join it) is not one this backend routes a child into: the launch
    /// falls back to the job's own cgroup, and the directory it tried is removed
    /// rather than left behind. Losing the leaf costs a later rollback its
    /// selectivity — never the containment of the spawn.
    #[test]
    fn a_directory_that_cannot_host_a_leaf_falls_back_to_the_job_cgroup() {
        let (_tmp, cg) = job_cgroup();

        let slot = cg.open_leaf();
        assert_eq!(
            slot.procs_path(),
            cg.path.join("cgroup.procs"),
            "the child must still join the job's own cgroup"
        );
        drop(slot);

        let leftovers: Vec<_> = std::fs::read_dir(&cg.path)
            .expect("read the job cgroup dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
            .collect();
        assert!(
            leftovers.is_empty(),
            "the directory reserved for a leaf that could not be used must be removed"
        );
        assert!(
            cg.leaf_dirs().is_empty(),
            "and nothing must be registered for it"
        );
    }

    /// The other way a launch ends without committing its leaf: it fails **after** its
    /// child exists (tokio's `Command::spawn` returns `Err` from steps that run after
    /// the fork), so the `rmdir` the slot's drop attempts is refused — the leaf holds a
    /// live member. The record must survive that refusal: a forgotten leaf takes its
    /// member out of the job's membership altogether, and with it out of the per-pid
    /// sweep the pre-5.14 teardown falls back to, which would then call a job drained
    /// that it never killed.
    ///
    /// An un-removable directory is the stand-in here for a populated cgroup: the
    /// kernel answers `EBUSY` for one that still holds members, and `remove_dir`
    /// answers `ENOTEMPTY` for one that still holds this stand-in's `cgroup.procs`.
    #[test]
    fn a_leaf_the_kernel_refuses_to_remove_stays_the_jobs_to_enumerate() {
        let (_tmp, cg) = job_cgroup();
        let dir = cg.path.join("spawn-failed-after-fork");
        std::fs::create_dir(&dir).expect("create the reserved leaf dir");
        write_procs(&dir, &[4242]);

        let slot = LeafSlot {
            cg: &cg,
            dir: Some(dir.clone()),
        };
        assert_eq!(
            slot.procs_path(),
            dir.join("cgroup.procs"),
            "precondition: this launch's child joined the leaf, not the job cgroup"
        );
        // The launch reports `Err`: no `commit`, no pid — and a live member in there.
        drop(slot);

        assert!(
            dir.exists(),
            "the kernel refused the rmdir, so the leaf is still standing"
        );
        assert_eq!(
            cg.leaf_dirs(),
            std::slice::from_ref(&dir),
            "and the job must still know about it"
        );
        assert_eq!(
            cg.members().expect("read the job's membership"),
            [4242],
            "a member the job cannot enumerate is one no per-pid kill can reach — and \
             one that makes an unkilled job look drained"
        );
        #[cfg(feature = "pty")]
        assert!(
            !cg.kill_leaf_of(4242),
            "no pid may steer a selective kill at a leaf this job never got one for"
        );

        // Once that member really is gone — the kernel takes a cgroup's interface
        // files with it, so an empty directory is one with nothing left in it — the
        // leaf is removable again, and the job's teardown is what takes it. A job that
        // had forgotten this leaf could not, and would leak its own directory too.
        std::fs::remove_file(dir.join("cgroup.procs")).expect("drain the leaf");
        drop(Job {
            backend: Backend::Cgroup(cg),
            skip_drop_kill: SkipDropKill::new(),
        });
        assert!(
            !dir.exists(),
            "a drained leaf must not outlive the job that took it back"
        );
    }

    /// Dropping a job reclaims its leaf directories, and does so **before** trying
    /// its own: a cgroup directory that still has child directories cannot be
    /// removed at all, so a leaf left behind would leak the whole job directory with
    /// it. A leaf the kernel does not let go of is kept, exactly as the job's own
    /// directory is when survivors remain.
    #[test]
    fn dropping_a_job_reclaims_its_leaf_directories_first() {
        let (tmp, cg) = job_cgroup();
        let path = cg.path.clone();
        let drained = path.join("spawn-drained");
        std::fs::create_dir(&drained).expect("create the drained leaf dir");
        cg.register_leaf(Some(1), drained.clone());
        // A leaf the kernel would answer `ENOTEMPTY` for: a cgroup a contained child
        // nested inside ours, which no `rmdir` of ours can take.
        let nested = path.join("spawn-nested");
        std::fs::create_dir(&nested).expect("create the nested leaf dir");
        std::fs::create_dir(nested.join("child-of-a-child")).expect("nest a cgroup inside it");
        cg.register_leaf(Some(2), nested.clone());

        drop(Job {
            backend: Backend::Cgroup(cg),
            skip_drop_kill: SkipDropKill::new(),
        });

        assert!(!drained.exists(), "an empty leaf must not outlive the job");
        assert!(
            nested.exists(),
            "a leaf the kernel refuses to remove is left standing, like the job dir itself"
        );
        drop(tmp);
    }

    /// A busy survivor leaf is retained while a drained sibling is reclaimed;
    /// after the survivor leaves, the same request can safely remove both the
    /// leaf and its parent. This is the kernel-confirmed release rule used by
    /// the process-wide reclaimer (a nested directory stands in for a populated
    /// cgroup without requiring delegated cgroup v2 in the test environment).
    #[test]
    fn eventual_reclaim_keeps_busy_leaf_until_the_survivor_drains() {
        let tmp = tempfile::tempdir().expect("create a reclaim test directory");
        let parent = tmp.path().join("processkit-job");
        let busy = parent.join("spawn-busy");
        let drained = parent.join("spawn-drained");
        let survivor = busy.join("survivor");
        std::fs::create_dir(&parent).expect("create parent");
        std::fs::create_dir(&busy).expect("create busy leaf");
        std::fs::create_dir(&survivor).expect("stand in for the live survivor");
        std::fs::create_dir(&drained).expect("create drained leaf");

        let mut request = CgroupReclaim {
            parent: parent.clone(),
            leaves: vec![busy.clone(), drained.clone()],
            attempts: 0,
        };
        assert!(
            !request.reclaim_once(),
            "a busy survivor keeps the request pending"
        );
        assert!(busy.exists(), "the busy leaf stays registered");
        assert!(
            !drained.exists(),
            "the drained sibling is reclaimed immediately"
        );
        assert_eq!(request.leaves, vec![busy.clone()]);

        std::fs::remove_dir(&survivor).expect("the survivor leaves its cgroup");
        assert!(
            request.reclaim_once(),
            "the final reclaim removes leaf and parent"
        );
        assert!(!busy.exists(), "the formerly busy leaf is now reclaimed");
        assert!(
            !parent.exists(),
            "the empty parent is reclaimed after its leaf"
        );
    }

    /// The handoff path is the production shape after a non-escalating `Drop`:
    /// cleanup starts while a survivor keeps the leaf busy, then retries after
    /// the stand-in survivor drains without ever issuing a kill.
    #[test]
    fn process_wide_reclaimer_retries_after_survivor_release() {
        let tmp = tempfile::tempdir().expect("create a reclaim test directory");
        let parent = tmp.path().join("processkit-job");
        let leaf = parent.join("spawn-survivor");
        let survivor = leaf.join("survivor");
        std::fs::create_dir(&parent).expect("create parent");
        std::fs::create_dir(&leaf).expect("create leaf");
        std::fs::create_dir(&survivor).expect("stand in for the live survivor");

        enqueue_cgroup_reclaim(parent.clone(), vec![leaf.clone()]);
        std::thread::sleep(Duration::from_millis(30));
        assert!(
            parent.exists(),
            "a live survivor keeps the handed-off cgroup contained"
        );

        std::fs::remove_dir(&survivor).expect("the survivor leaves its cgroup");
        for _ in 0..200 {
            if !parent.exists() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("the process-wide reclaimer did not remove {parent:?} after the survivor drained");
    }

    #[test]
    fn reclaimer_backoff_is_bounded_and_reset_for_new_work() {
        let mut pending = Vec::new();
        let mut backoff = CgroupReclaimBackoff::new();
        let expected = [10, 20, 40, 80, 160, 320, 640, 1_000];
        for expected_millis in expected {
            assert_eq!(backoff.delay(), Duration::from_millis(expected_millis));
            backoff.increase();
        }
        assert_eq!(
            backoff.delay(),
            Duration::from_secs(1),
            "a long-lived survivor must not make the retry interval unbounded"
        );

        accept_cgroup_reclaim(
            &mut pending,
            &mut backoff,
            CgroupReclaim {
                parent: PathBuf::from("new-parent"),
                leaves: Vec::new(),
                attempts: 0,
            },
        );
        assert_eq!(backoff.delay(), Duration::from_millis(10));
        assert_eq!(pending.len(), 1, "the new request must still be queued");
    }

    /// A broken handoff must return the request to durable state instead of
    /// dropping it after one synchronous reclaim attempt. The next manager can
    /// accept the same request, which then keeps retrying until the survivor's
    /// stand-in leaves the leaf.
    #[test]
    fn refused_reclaimer_handoff_is_retained_for_a_later_manager() {
        let tmp = tempfile::tempdir().expect("create a reclaim test directory");
        let parent = tmp.path().join("processkit-job");
        let leaf = parent.join("spawn-survivor");
        let survivor = leaf.join("survivor");
        std::fs::create_dir(&parent).expect("create parent");
        std::fs::create_dir(&leaf).expect("create leaf");
        std::fs::create_dir(&survivor).expect("stand in for the live survivor");

        let mut state = CgroupReclaimerState {
            sender: None,
            pending: vec![CgroupReclaim {
                parent: parent.clone(),
                leaves: vec![leaf.clone()],
                attempts: 0,
            }],
        };
        let (refused_sender, refused_receiver) = std::sync::mpsc::channel();
        drop(refused_receiver);
        assert_eq!(
            state
                .send_pending(&refused_sender)
                .expect_err("a closed manager must refuse the handoff")
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            state.pending.len(),
            1,
            "SendError must return the request to the durable queue"
        );

        let (sender, receiver) = std::sync::mpsc::channel();
        state
            .send_pending(&sender)
            .expect("a later manager accepts the retained request");
        let mut request = receiver.recv().expect("receive the retained request");
        assert!(
            !request.reclaim_once(),
            "the survivor still keeps the leaf busy"
        );
        assert!(leaf.exists(), "reclaim must not evict a live survivor");

        std::fs::remove_dir(&survivor).expect("the survivor leaves its cgroup");
        assert!(
            request.reclaim_once(),
            "the retained request must remain retryable until removal is confirmed"
        );
        assert!(
            !parent.exists(),
            "the later retry removes the empty hierarchy"
        );
    }

    /// Poisoning cannot turn an already queued cgroup into a lost request or make
    /// the next `Job::drop` abort an outer unwind. The global state is serialized,
    /// snapshotted and restored so parallel tests never inherit the deliberate
    /// poison or this test's synthetic queue.
    #[test]
    fn poisoned_reclaimer_preserves_pending_and_recovers_during_unwind() {
        struct RestoreReclaimerState<'a> {
            reclaimer: &'a Mutex<CgroupReclaimerState>,
            saved: Option<CgroupReclaimerState>,
        }

        impl Drop for RestoreReclaimerState<'_> {
            fn drop(&mut self) {
                let Some(saved) = self.saved.take() else {
                    return;
                };
                let mut state = lock_cgroup_reclaimer(self.reclaimer);
                let test_state = std::mem::replace(&mut *state, saved);
                drop(state);
                drop(test_state);
                self.reclaimer.clear_poison();
            }
        }

        struct EnqueueOnDrop<'a> {
            reclaimer: &'a Mutex<CgroupReclaimerState>,
            parent: Option<PathBuf>,
        }

        impl Drop for EnqueueOnDrop<'_> {
            fn drop(&mut self) {
                if let Some(parent) = self.parent.take() {
                    enqueue_cgroup_reclaim_with_state(self.reclaimer, parent, Vec::new());
                }
            }
        }

        let retained_parent = PathBuf::from("pending-before-poison");
        let unwind_parent = PathBuf::from("enqueued-during-unwind");
        let retry_parent = PathBuf::from("enqueued-on-retry");
        let (refused_sender, refused_receiver) = std::sync::mpsc::channel();
        drop(refused_receiver);
        // A failed assertion must not strand the test gate for later regressions.
        let _serial = CGROUP_RECLAIMER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reclaimer = cgroup_reclaimer_state();
        let saved = {
            let mut state = lock_cgroup_reclaimer(reclaimer);
            std::mem::replace(
                &mut *state,
                CgroupReclaimerState {
                    sender: Some(refused_sender),
                    pending: vec![CgroupReclaim {
                        parent: retained_parent.clone(),
                        leaves: Vec::new(),
                        attempts: 0,
                    }],
                },
            )
        };
        let saved_pending_len = saved.pending.len();
        let saved_had_sender = saved.sender.is_some();
        reclaimer.clear_poison();
        let restore = RestoreReclaimerState {
            reclaimer,
            saved: Some(saved),
        };

        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = reclaimer.lock().expect("lock the fresh reclaimer state");
            panic!("poison the reclaimer state");
        }));
        assert!(poison.is_err(), "the setup panic must poison the mutex");
        assert!(reclaimer.is_poisoned(), "the test must exercise recovery");

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _enqueue_on_drop = EnqueueOnDrop {
                reclaimer,
                parent: Some(unwind_parent.clone()),
            };
            panic!("exercise enqueue while another unwind is active");
        }));
        assert!(unwind.is_err(), "only the deliberate outer panic is caught");

        let (retry_sender, retry_receiver) = std::sync::mpsc::channel();
        {
            let mut state = lock_cgroup_reclaimer(reclaimer);
            assert_eq!(state.pending.len(), 2, "both requests remain durable");
            assert_eq!(state.pending[0].parent, retained_parent);
            assert_eq!(state.pending[1].parent, unwind_parent);
            assert!(
                state.sender.is_none(),
                "a refused sender must be retired for the next retry"
            );
            state.sender = Some(retry_sender);
        }

        enqueue_cgroup_reclaim_with_state(reclaimer, retry_parent.clone(), Vec::new());
        let received = (0..3)
            .map(|_| {
                retry_receiver
                    .recv_timeout(Duration::from_secs(1))
                    .expect("the replacement manager receives every pending request")
                    .parent
            })
            .collect::<Vec<_>>();
        assert_eq!(
            received,
            [retained_parent, unwind_parent, retry_parent],
            "recovery preserves queue order and the retry drains old and new work"
        );
        assert!(
            lock_cgroup_reclaimer(reclaimer).pending.is_empty(),
            "the successful retry must leave no request stranded"
        );

        drop(restore);
        assert!(
            !reclaimer.is_poisoned(),
            "the deliberate poison must not escape this test"
        );
        let restored = lock_cgroup_reclaimer(reclaimer);
        assert_eq!(restored.pending.len(), saved_pending_len);
        assert_eq!(restored.sender.is_some(), saved_had_sender);
    }

    /// Without `tracing`, failed reclaim diagnostics are intentionally a no-op:
    /// library code must not write to the host process's stderr implicitly.
    #[cfg(not(feature = "tracing"))]
    #[test]
    fn cgroup_reclaim_failure_reporting_is_silent_without_tracing() {
        const HELPER_ENV: &str = "PROCESSKIT_CGROUP_RECLAIM_STDERR_HELPER";
        if std::env::var_os(HELPER_ENV).is_some() {
            super::report_cgroup_reclaim_failure("test", io::ErrorKind::Other, 1);
            return;
        }

        let output = std::process::Command::new(
            std::env::current_exe().expect("locate the current test executable"),
        )
        .args([
            "--exact",
            "sys::imp::leaf_cgroup_tests::cgroup_reclaim_failure_reporting_is_silent_without_tracing",
            "--nocapture",
        ])
        .env(HELPER_ENV, "1")
        .output()
        .expect("run the isolated stderr helper");
        assert!(output.status.success(), "stderr helper failed: {output:?}");
        assert!(
            output.stderr.is_empty(),
            "reclaim diagnostics must not write to host stderr: {:?}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// T-273's leaf machinery against a **real cgroup v2 hierarchy** — the half of the
/// contract a stand-in directory cannot show: a spawn landing in a leaf the kernel
/// created, `cgroup.kill` on that leaf really killing its subtree (and nothing
/// else), and the directories really going away.
///
/// Each test skips — with a note, never a false pass — when this host hands
/// `Job::new` the process-group fallback instead of a delegated cgroup v2 (an
/// unprivileged container, no delegation, a read-only `/sys/fs/cgroup`), since none
/// of it is reachable there. Run them as a user who can create cgroups:
/// `cargo test --lib --all-features -- --include-ignored real_cgroup_leaf`.
#[cfg(test)]
mod real_cgroup_leaf_tests {
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use tokio::process::{Child, Command};

    use super::{Backend, Job};
    use crate::sys::SpawnOptions;

    /// A real `Job` on this host plus its cgroup directory, or `None` (with a note)
    /// when the host gave the process-group fallback.
    fn cgroup_job() -> Option<(Job, PathBuf)> {
        #[cfg(feature = "limits")]
        let job = Job::new(&crate::limits::ResourceLimits::default()).expect("create a job");
        #[cfg(not(feature = "limits"))]
        let job = Job::new().expect("create a job");
        let path = match &job.backend {
            Backend::Cgroup(cg) => cg.path.clone(),
            Backend::ProcessGroup(_) => {
                eprintln!(
                    "skipping: this host has no writable cgroup v2 (Job::new fell back to the \
                     process-group backend) — the per-spawn leaf contract is not reachable here"
                );
                return None;
            }
        };
        Some((job, path))
    }

    /// A child that stays alive until something kills it.
    fn sleeper() -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", "while :; do sleep 60; done"]);
        command
    }

    /// The `spawn-*` sub-directories of a job cgroup, sorted — the leaves as the
    /// kernel's own directory listing sees them, not as the registry believes.
    fn leaf_dirs_on_disk(job_dir: &Path) -> Vec<PathBuf> {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(job_dir)
            .expect("read the job cgroup dir")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_dir()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("spawn-"))
            })
            .collect();
        dirs.sort();
        dirs
    }

    /// The pids a cgroup directory lists as its own members.
    fn procs_of(dir: &Path) -> Vec<u32> {
        std::fs::read_to_string(dir.join("cgroup.procs"))
            .expect("read a cgroup.procs")
            .lines()
            .filter_map(|line| line.trim().parse().ok())
            .collect()
    }

    /// Whether `pid` still names a live process. Only ever asked about a pid this
    /// process is **not** the parent of (the escapee below, a grandchild that left
    /// our session — init's to reap, not ours) or about one that is expected to be
    /// alive, so `kill(pid, 0)` is the whole probe: a killed *direct* child would
    /// answer this "alive" as an un-reaped zombie, and is waited on through its own
    /// `Child` handle instead.
    #[cfg(feature = "pty")]
    fn is_alive(pid: u32) -> bool {
        // SAFETY: signal 0 is a pure existence probe.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    #[cfg(feature = "pty")]
    async fn wait_until_gone(pid: u32, what: &str) {
        for _ in 0..600 {
            if !is_alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        panic!("{what}: pid {pid} was still alive after the bounded wait");
    }

    /// Wait (bounded) for a helper to publish its pid into `path`.
    #[cfg(feature = "pty")]
    async fn published_pid(path: &Path) -> u32 {
        for _ in 0..600 {
            if let Ok(text) = std::fs::read_to_string(path) {
                let text = text.trim().to_owned();
                if !text.is_empty() {
                    return text.parse().expect("the helper publishes a pid");
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the helper never published its pid");
    }

    /// A unique-per-process temp path for a helper to publish a pid through.
    #[cfg(feature = "pty")]
    fn pidfile(tag: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("processkit_leaf_{tag}_{}.pid", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    async fn reap(mut child: Child) {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    /// Every spawn lands in a leaf of its **own**, and the whole job still sees all
    /// of them: two spawns never share a leaf (which is what makes a per-spawn kill
    /// selective at all), the job's own cgroup holds none of them, and `members()`
    /// reports the union across the leaves.
    ///
    /// What a leaf holds beyond its spawn is the spawn's *descendants* — a shell's
    /// `sleep`, anything either forks — since cgroup membership is inherited; that is
    /// the subtree a selective kill is meant to take, so this asserts where each
    /// spawn is, not that it is alone there.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "creates real cgroups and spawns real subprocesses"]
    async fn every_spawn_lands_in_its_own_leaf_and_the_job_lists_them_all() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let first = job
            .spawn(&mut sleeper(), &SpawnOptions::default())
            .expect("spawn the first child");
        let second = job
            .spawn(&mut sleeper(), &SpawnOptions::default())
            .expect("spawn the second child");
        let (first_pid, second_pid) = (first.id().expect("a pid"), second.id().expect("a pid"));

        let leaves = leaf_dirs_on_disk(&dir);
        assert_eq!(leaves.len(), 2, "one leaf per spawn, in {dir:?}");
        let leaf_of = |pid: u32| -> PathBuf {
            let mut holding = leaves
                .iter()
                .filter(|leaf| procs_of(leaf).contains(&pid))
                .cloned();
            let leaf = holding
                .next()
                .unwrap_or_else(|| panic!("pid {pid} is in none of the job's leaves: {leaves:?}"));
            assert!(holding.next().is_none(), "a pid is in exactly one cgroup");
            leaf
        };
        assert_ne!(
            leaf_of(first_pid),
            leaf_of(second_pid),
            "two spawns sharing a leaf would make a per-spawn kill hit them both"
        );
        assert!(
            procs_of(&dir).is_empty(),
            "the job's own cgroup holds no spawned member once every spawn has a leaf"
        );

        #[cfg(feature = "process-control")]
        {
            let members = job.members().expect("read the job's members");
            assert!(
                members.contains(&first_pid) && members.contains(&second_pid),
                "a membership read must aggregate the leaves: {members:?}"
            );
        }

        drop(job);
        reap(first).await;
        reap(second).await;
    }

    /// Dropping the job takes every leaf directory with it, and then the job's own —
    /// which cannot happen in the other order, since a cgroup directory with
    /// children cannot be removed at all.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "creates real cgroups and spawns real subprocesses"]
    async fn dropping_a_job_leaves_no_leaf_directory_behind() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let first = job
            .spawn(&mut sleeper(), &SpawnOptions::default())
            .expect("spawn the first child");
        let second = job
            .spawn(&mut sleeper(), &SpawnOptions::default())
            .expect("spawn the second child");
        assert_eq!(leaf_dirs_on_disk(&dir).len(), 2);

        drop(job);

        assert!(
            !dir.exists(),
            "the job directory must be gone — a leaf left behind would keep it \
             (`rmdir` answers ENOTEMPTY) and leak both"
        );
        reap(first).await;
        reap(second).await;
    }

    /// A non-escalating graceful shutdown leaves the direct child alive and in
    /// its leaf after `Job` is dropped. Once the caller later terminates that
    /// survivor, the detached reclaimer removes the leaf and parent rather than
    /// preserving an empty cgroup hierarchy forever.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "creates real cgroups and spawns real subprocesses"]
    async fn non_escalating_shutdown_reclaims_after_the_survivor_exits() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let mut survivor = Command::new("sh");
        // The ignored TERM disposition survives the exec, so the direct child
        // remains a single process in its leaf and can be terminated explicitly
        // after the non-escalating shutdown has handed containment away.
        survivor.args(["-c", "trap '' TERM; exec sleep 60"]);
        let mut child = job
            .spawn(&mut survivor, &SpawnOptions::default())
            .expect("spawn the survivor");
        let pid = child.id().expect("a survivor pid");

        job.graceful_shutdown(libc::SIGTERM, Duration::from_millis(20), false)
            .await
            .expect("non-escalating graceful shutdown");
        assert!(
            child.try_wait().expect("probe survivor").is_none(),
            "the survivor must remain alive after escalate=false"
        );
        let leaves = leaf_dirs_on_disk(&dir);
        assert_eq!(
            leaves.len(),
            1,
            "the survivor still has one containing leaf"
        );
        assert!(
            procs_of(&leaves[0]).contains(&pid),
            "the survivor remains in containment before Job::drop"
        );

        drop(job);
        assert!(
            child.try_wait().expect("probe spared survivor").is_none(),
            "the detached reclaimer must never hard-kill a spared survivor"
        );
        reap(child).await;

        for _ in 0..300 {
            if !dir.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the cgroup hierarchy {dir:?} remained after its survivor exited");
    }

    /// A launch that never produces a child leaves no leaf directory behind, so a
    /// job that fails to spawn repeatedly does not fill its cgroup with empty ones.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "creates real cgroups"]
    async fn a_launch_that_never_started_leaves_no_leaf_behind() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let mut command = Command::new("/nonexistent/processkit-no-such-program");
        job.spawn(&mut command, &SpawnOptions::default())
            .expect_err("the launch itself must fail");

        assert!(
            leaf_dirs_on_disk(&dir).is_empty(),
            "the leaf reserved for a launch that never happened must be removed"
        );
        drop(job);
    }

    /// A launch that fails **after** its child exists keeps that child reachable — the
    /// case a launch failing *before* its fork (above) does not cover, and the one the
    /// leaf directory makes delicate. tokio's `Command::spawn` is
    /// `std::process::Command::spawn` plus post-fork steps (registering the child's
    /// stdio with the reactor, opening its pidfd) that can return `Err` while dropping
    /// the `std::process::Child`, which does not kill it: the child is alive, it has
    /// been in the leaf since before its `exec`, the launch commits nothing and hands
    /// back no pid, and the `rmdir` the slot's drop attempts is refused `EBUSY`.
    ///
    /// The leaf must not be forgotten there. What that would cost is asserted rather
    /// than argued: the member disappears from `members()`, and the pre-5.14 teardown
    /// — a per-pid SIGKILL sweep over exactly that membership, ending in a drain check
    /// over it — reports a drained job while the process runs on.
    ///
    /// The state is built from the backend's own primitives (reserve a leaf, fork a
    /// child that joins it in its own pre-exec, drop the slot uncommitted), which is
    /// precisely what `?` on `cmd.spawn()` leaves behind in `spawn_displacing_spare`.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "creates real cgroups and spawns real subprocesses"]
    async fn a_launch_that_failed_after_forking_keeps_its_child_enumerable_and_killable() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStringExt;
        use std::os::unix::process::CommandExt;

        use crate::sys::fault_injection::{Faults, Site};

        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let Backend::Cgroup(cg) = &job.backend else {
            unreachable!("cgroup_job hands back the cgroup backend or nothing");
        };

        let slot = cg.open_leaf();
        let leaf = slot
            .procs_path()
            .parent()
            .expect("a cgroup.procs has a directory")
            .to_path_buf();
        assert_ne!(
            leaf, dir,
            "precondition: this host let the launch reserve a leaf of its own"
        );
        let procs = CString::new(slot.procs_path().into_os_string().into_vec())
            .expect("a NUL-free cgroup path");
        let mut command = sleeper();
        // SAFETY: `write_self_pid` makes only async-signal-safe calls — this is the
        // very hook `Job::spawn` installs, on the very path it installs it on.
        unsafe {
            command
                .as_std_mut()
                .pre_exec(move || super::write_self_pid(procs.as_c_str()));
        }
        let mut child = command.spawn().expect("the fork itself succeeds");
        let pid = child.id().expect("a pid") as i32;
        // …and here tokio's post-fork setup fails: the `Child` is dropped unkilled,
        // `commit` never runs, and the slot goes out of scope over a populated leaf.
        drop(slot);

        assert!(
            procs_of(&leaf).contains(&(pid as u32)),
            "precondition: the child of the failed launch is in the leaf, not the job \
             cgroup — the whole point of the leaf being the one thing that could lose it"
        );
        assert_eq!(
            cg.leaf_dirs(),
            std::slice::from_ref(&leaf),
            "a leaf the kernel refused to remove must stay the job's"
        );
        assert!(
            cg.members()
                .expect("read the job's membership")
                .contains(&pid),
            "and its member must still be one of the job's"
        );
        #[cfg(feature = "process-control")]
        assert!(
            job.members()
                .expect("read the job's members")
                .contains(&(pid as u32)),
            "as seen through the whole-job verb the caller actually has"
        );

        // The teardown with no atomic `cgroup.kill` to lean on — a kernel < 5.14, or a
        // delegated cgroup that refuses the write — falls back to the per-pid sweep
        // over that membership. It must kill this child, and must not report a drained
        // job without having done so.
        let faults = Faults::new()
            .fail_every(Site::CgroupWrite, Some("cgroup.kill"), libc::EACCES)
            .arm();
        let swept = job.kill_all();
        assert_eq!(faults.fired(Site::CgroupWrite), 1);
        drop(faults);
        swept.expect("the per-pid sweep must drain the job");

        let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("the child of the failed launch must be killed by that sweep")
            .expect("wait for the child of the failed launch");
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "killed by the sweep, not left to exit on its own: {status:?}"
        );

        // Drained at last, the leaf goes with the job — and takes the job's own
        // directory with it, which a job that had forgotten the leaf could not do
        // (`rmdir` answers ENOTEMPTY while a child directory is there).
        drop(job);
        assert!(
            !dir.exists(),
            "the job directory must be gone, leaf and all"
        );
    }

    /// The guarantee this whole mechanism exists for: rolling back one spawn kills
    /// **that spawn's** subtree — including a descendant that `setsid`'d out of its
    /// session, which no `killpg` can reach — while another spawn of the same job
    /// keeps running. The rolled-back spawn's leaf is then reclaimed and the
    /// survivor's is not.
    #[cfg(feature = "pty")]
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "creates real cgroups and spawns real subprocesses incl. a setsid escapee"]
    async fn a_rollback_kills_this_spawns_setsid_escapee_and_spares_the_other_spawns() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        if !has_setsid() {
            eprintln!("skipping: no setsid(1) on this host to build a session escapee with");
            return;
        }
        let file = pidfile("escapee");

        // The spawn that must survive its neighbour's rollback.
        let survivor = job
            .spawn(&mut sleeper(), &SpawnOptions::default())
            .expect("spawn the survivor");
        let survivor_pid = survivor.id().expect("a pid");

        // The spawn to be rolled back: a shell that forks a descendant into a
        // session of its own (so `killpg` over the shell's group cannot reach it)
        // and then stays alive, as the pty child of a failing launch would.
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "setsid sh -c 'echo $$ > \"$PK_PIDFILE\"; exec sleep 300' </dev/null \
                 >/dev/null 2>&1 & while :; do sleep 60; done",
            ])
            .env("PK_PIDFILE", &file);
        let mut victim = job
            .spawn(&mut command, &SpawnOptions::default())
            .expect("spawn the launch that will be rolled back");
        let victim_pid = victim.id().expect("a pid");
        let escapee = published_pid(&file).await;
        // SAFETY: `getsid` only reads the target's session id.
        assert_eq!(
            unsafe { libc::getsid(escapee as libc::pid_t) },
            escapee as libc::pid_t,
            "escapee {escapee} never became a session leader — the test would prove nothing"
        );
        assert_eq!(
            leaf_dirs_on_disk(&dir).len(),
            2,
            "precondition: two spawns, two leaves"
        );

        job.rollback_pty_spawn(victim_pid, crate::sys::DisplacedSpare::default());

        wait_until_gone(
            escapee,
            "the rollback's leaf-scoped cgroup.kill must reach a setsid escapee of \
             the spawn it undoes",
        )
        .await;
        // The rolled-back spawn's own child is a direct child of this process, so it
        // is waited on rather than probed: a `SIGKILL`ed child is an un-reaped zombie
        // until then, and would answer an existence probe "alive".
        let status = tokio::time::timeout(Duration::from_secs(10), victim.wait())
            .await
            .expect("the rolled-back spawn's own child must be killed by the leaf kill")
            .expect("wait for the rolled-back child");
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "and killed by the leaf's SIGKILL, not left to exit on its own: {status:?}"
        );
        assert!(
            is_alive(survivor_pid),
            "another spawn of the same job must not be touched by this spawn's rollback"
        );
        #[cfg(feature = "process-control")]
        assert!(
            job.members()
                .expect("read the job's members")
                .contains(&survivor_pid),
            "and it must still be a member of the job"
        );
        // `cgroup.kill` is asynchronous, so the reclaim the rollback itself runs can
        // still find the leaf draining; the next pass — here, an explicit one, in
        // production the next launch or the teardown — takes it once it has drained,
        // and takes only it.
        if let Backend::Cgroup(cg) = &job.backend {
            cg.reclaim_leaves();
        }
        assert_eq!(
            leaf_dirs_on_disk(&dir).len(),
            1,
            "the killed spawn's leaf is reclaimed; the survivor's stays"
        );

        drop(job);
        reap(survivor).await;
        let _ = std::fs::remove_file(&file);
        // Belt and braces: no path may leave the escapee running.
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(escapee as libc::pid_t, libc::SIGKILL) };
    }

    /// Whether this host has `setsid(1)` for the escapee helper above.
    #[cfg(feature = "pty")]
    fn has_setsid() -> bool {
        std::process::Command::new("sh")
            .args(["-c", "command -v setsid"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
}

/// Bare-pid adoption (`Job::adopt_external`) against a **real cgroup v2
/// hierarchy** — the arm whose anchor is the kernel's own membership rather than a
/// tracked number, so nothing below it can be shown with a stand-in directory.
///
/// Each test skips — with a note, never a false pass — when this host hands
/// `Job::new` the process-group fallback (that arm is covered by the shared
/// backend's own tests in `sys::pgroup`). Run them as a user who can create
/// cgroups: `cargo test --lib --all-features -- --include-ignored
/// real_cgroup_adopt`.
#[cfg(all(test, feature = "process-control"))]
mod real_cgroup_adopt_tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use super::{Backend, Job};
    use crate::sys::fault_injection::{Faults, Site};

    /// A real `Job` on this host plus its cgroup directory, or `None` (with a note)
    /// when the host gave the process-group fallback.
    fn cgroup_job() -> Option<(Job, PathBuf)> {
        #[cfg(feature = "limits")]
        let job = Job::new(&crate::limits::ResourceLimits::default()).expect("create a job");
        #[cfg(not(feature = "limits"))]
        let job = Job::new().expect("create a job");
        match &job.backend {
            Backend::Cgroup(cg) => {
                let path = cg.path.clone();
                Some((job, path))
            }
            Backend::ProcessGroup(_) => {
                eprintln!(
                    "skipping: this host has no writable cgroup v2 (Job::new fell back to the \
                     process-group backend) — the cgroup adoption path is not reachable here"
                );
                None
            }
        }
    }

    /// The pids a cgroup directory lists as its own members.
    fn procs_of(dir: &Path) -> Vec<u32> {
        std::fs::read_to_string(dir.join("cgroup.procs"))
            .expect("read a cgroup.procs")
            .lines()
            .filter_map(|line| line.trim().parse().ok())
            .collect()
    }

    /// Start a process genuinely **foreign** to this one: a shell backgrounds it
    /// and exits, so `init` adopts it and no handle to it exists here — the state
    /// an FFI caller's pid arrives in.
    ///
    /// The backgrounded process gets its own `/dev/null` stdio: inheriting the
    /// launcher's captured pipe would keep it open, and `output()` waits for EOF —
    /// i.e. for the "orphan" to exit — before this function could even return.
    fn spawn_orphan() -> u32 {
        let out = std::process::Command::new("sh")
            .args(["-c", "sleep 60 >/dev/null 2>&1 </dev/null & echo $!"])
            .output()
            .expect("launch the orphan's launcher");
        assert!(out.status.success(), "the orphan's launcher failed");
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .expect("the launcher prints the orphan's pid")
    }

    /// Whether `pid` still names a live process. Only asked about an orphan, which
    /// `init` reaps the moment it dies, so no zombie can be misread as alive.
    fn is_alive(pid: u32) -> bool {
        // SAFETY: signal 0 is a pure existence probe.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }

    async fn wait_until_gone(pid: u32, what: &str) {
        for _ in 0..600 {
            if !is_alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        panic!("{what}: pid {pid} was still alive after the bounded wait");
    }

    /// The positive path: a process this job never started, and holds no handle
    /// for, becomes a member of the job's own cgroup — and the job's teardown
    /// really reaches it.
    #[tokio::test]
    #[ignore = "needs a writable cgroup v2 and spawns a real subprocess"]
    async fn adopt_external_makes_a_foreign_process_a_cgroup_member() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let pid = spawn_orphan();

        job.adopt_external(pid).expect("adopt a foreign pid");
        assert!(
            procs_of(&dir).contains(&pid),
            "the adopted process must be a member of the job's own cgroup"
        );

        job.kill_all().expect("kill the job");
        wait_until_gone(pid, "the adopted foreign process dies with the job").await;
    }

    /// The Linux failure branch the platform matrix names: the `cgroup.procs` write
    /// refused (a process this one may not move, a restricted delegated cgroup).
    /// Injected rather than staged, since reproducing it for real needs a
    /// hand-built host — the assertion is that it surfaces as an error instead of
    /// being reported as containment.
    #[tokio::test]
    #[ignore = "needs a writable cgroup v2 and spawns a real subprocess"]
    async fn a_refused_cgroup_procs_write_is_not_reported_as_containment() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let pid = spawn_orphan();

        let err = {
            let _faults = Faults::new()
                .fail_every(Site::CgroupWrite, Some("cgroup.procs"), libc::EACCES)
                .arm();
            job.adopt_external(pid)
                .expect_err("a refused cgroup.procs write must fail the adoption")
        };
        assert_eq!(err.raw_os_error(), Some(libc::EACCES), "{err:?}");
        assert!(
            !procs_of(&dir).contains(&pid),
            "a refused adoption must not leave the pid a member"
        );
        assert!(
            is_alive(pid),
            "a refused adoption must leave the process alone"
        );

        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        drop(job);
    }

    /// The undo the recycle path leans on, against the real kernel: a process this
    /// job's cgroup really holds is moved back out into the cgroup the job's own
    /// directory lives in, and the job's teardown then does not reach it.
    ///
    /// A real recycle cannot be staged — the pid space would have to wrap inside one
    /// call — so this drives `evict_recycled` directly. What only a real hierarchy
    /// can answer is the part that decides whether the fix is a fix at all: whether
    /// the kernel *permits* the move-out (delegation rules, the "no internal
    /// processes" constraint on the destination), and whether the number really
    /// stops being a member. A stand-in directory can show neither.
    #[tokio::test]
    #[ignore = "needs a writable cgroup v2 and spawns a real subprocess"]
    async fn a_recycled_adoption_is_taken_back_out_of_the_job_cgroup() {
        let Some((job, dir)) = cgroup_job() else {
            return;
        };
        let pid = spawn_orphan();
        job.adopt_external(pid).expect("adopt a foreign pid");
        assert!(
            procs_of(&dir).contains(&pid),
            "the adopted process must be a member before the undo has anything to do"
        );

        let Backend::Cgroup(cg) = &job.backend else {
            unreachable!("cgroup_job only returns the cgroup backend");
        };
        match cg.evict_recycled(pid) {
            super::RecycleUndo::Evicted => {}
            super::RecycleUndo::NotAMember => panic!("the pid was a member a moment ago"),
            super::RecycleUndo::Stuck(e) => {
                // A host that refuses the move-out is a real outcome, not a test
                // failure — but it is the one where the contract's fail-lethal
                // wording applies, so it must be visible rather than silently
                // passing as a green run.
                // SAFETY: a best-effort kill of a pid this test started.
                unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
                panic!(
                    "this host refuses to move an adopted pid back out of the job's cgroup \
                     ({e}) — the undo degrades to the documented 'still a member' outcome here"
                );
            }
        }
        assert!(
            !procs_of(&dir).contains(&pid),
            "an evicted number must no longer be a member of the job's cgroup"
        );

        job.kill_all().expect("kill the job");
        // The whole point of the undo: the group's teardown no longer covers it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let survived = is_alive(pid);
        // SAFETY: a best-effort kill of a pid this test started.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        assert!(
            survived,
            "a process taken back out of the job's cgroup must not die with the job"
        );
        drop(job);
    }

    /// A number that names nothing is refused by the anchor read, before any write
    /// — the same `NotFound` the pgroup arm gives, so the two arms answer a caller
    /// identically.
    #[tokio::test]
    #[ignore = "needs a writable cgroup v2"]
    async fn adopt_external_of_a_pid_that_names_nothing_is_not_found() {
        let Some((job, _dir)) = cgroup_job() else {
            return;
        };
        let err = job
            .adopt_external(2_000_000_000)
            .expect_err("a pid that names nothing is not adoptable");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{err:?}");
    }
}
