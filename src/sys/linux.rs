//! Linux implementation: a [cgroup v2] killed via `cgroup.kill`, with a POSIX
//! process-group fallback when no writable cgroup is available (e.g. a CI runner
//! without cgroup delegation).
//!
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::Mechanism;
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
use crate::sys::pgroup::ProcessGroup;
#[cfg(feature = "stats")]
use crate::sys::{ProcIdentity, ProcMetrics};

/// Process-wide counter so concurrent jobs get distinct cgroup names.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

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
/// many crashes can reclaim stale `processkit-*` dirs out of band.
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
                let procs = CString::new(cg.path.join("cgroup.procs").into_os_string().into_vec())
                    .map_err(|_| {
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
                // Re-arm the kill-on-drop backstop now a child has joined: a
                // prior graceful_shutdown(escalate=false) latched this flag to
                // spare survivors; a fresh member must not be spared by it. Done
                // after the spawn so a failed spawn leaves the survivors alone.
                self.skip_drop_kill.clear();
                Ok(child)
            }
            Backend::ProcessGroup(pg) => {
                arm(cmd);
                // `pg.spawn` re-arms the ProcessGroup's own latch on success.
                pg.spawn(cmd, opts)
            }
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
                match std::fs::write(cg.path.join("cgroup.procs"), pid.to_string().as_bytes()) {
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
            // Whole tree: every pid in cgroup.procs.
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
    ) -> io::Result<()> {
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
    let mut metrics = ProcMetrics::default();

    // CPU *and* the identity anchor both come from a *single* /proc/<pid>/stat read
    // — one read so the identity gate and the CPU sample describe the same instant
    // (a second read could straddle a pid recycle). Every field access goes through
    // the shared `sys::procfs` parser (skip past the comm's last ')', then
    // whitespace index 0 is field 3), so this parse cannot drift from the pgroup
    // liveness path in `sys/pgroup.rs::read_identity` that shares it.
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok();

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

    // Peak memory: /proc/<pid>/status VmHWM (high-water resident set, in kB). Only
    // reached once the identity gate above confirmed the pid (or none was demanded),
    // so this read is bound to the same process the starttime identified.
    if let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
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
                // Best-effort: an emptied cgroup dir is removed here — the common
                // case, plus the escalate=false case where survivors all drained
                // during the grace. When survivors remain under escalate=false
                // this `rmdir` fails with EBUSY and the dir is intentionally left
                // to keep containing the orphaned tree; it is then *not* reclaimed
                // even after that tree later exits, because the owning `Job` is
                // already gone. That permanent empty-dir leak is the accepted cost
                // of choosing not to escalate — symmetric with the Windows backend
                // deliberately orphaning its survivors.
                let _ = std::fs::remove_dir(&cg.path);
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

struct Cgroup {
    path: PathBuf,
}

impl Cgroup {
    fn create(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // Locate the cgroup v2 (unified) mount root. The common case is
        // `/sys/fs/cgroup` (pure v2), but a systemd **hybrid** host mounts the v2
        // hierarchy at `/sys/fs/cgroup/unified` — checking only the former (C5)
        // would fall back to pgroup despite a usable v2 tree.
        let root = cgroup2_root()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "cgroup v2 not mounted"))?;
        let root = root.as_path();

        // Our own cgroup: on v2, `/proc/self/cgroup` is a single `0::<path>` line.
        let self_cgroup = std::fs::read_to_string("/proc/self/cgroup")?;
        let rel = self_cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap_or("/")
            .trim();
        let parent = root.join(rel.trim_start_matches('/'));

        // Without limits, no controllers are enabled — `cgroup.kill` needs none,
        // and that sidesteps the "no internal processes" rule. mkdir is the
        // permission gate that triggers the process-group fallback when delegation
        // is absent.
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
        let cg = Cgroup { path };

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
            std::fs::write(self.path.join("memory.max"), bytes.to_string())?;
        }
        if let Some(n) = limits.max_processes {
            std::fs::write(self.path.join("pids.max"), n.to_string())?;
        }
        if let Some(cores) = limits.cpu_quota {
            std::fs::write(self.path.join("cpu.max"), cpu_max_value(cores))?;
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
            std::fs::write(&file, &spec).map_err(|e| {
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

    /// Read the live member pids. A removed cgroup is empty; other read failures
    /// leave its state unknown and are surfaced to the caller.
    fn members(&self) -> io::Result<Vec<i32>> {
        self.members_with(|path| std::fs::read_to_string(path))
    }

    /// `members()` parametrized over the `cgroup.procs` reader — the injectable
    /// seam that lets tests exercise the success/`NotFound`/`PermissionDenied`/I/O
    /// error mapping below, and that every other fail-safe decision in this type
    /// (`is_empty`, `signal`, `kill`, `stats`) is threaded through so *their* tests
    /// can drive the same error paths without a real cgroup filesystem. `Fn` (not
    /// `FnOnce`): the legacy kill sweep below calls this in a bounded retry loop.
    fn members_with(&self, read: impl Fn(&Path) -> io::Result<String>) -> io::Result<Vec<i32>> {
        match read(&self.path.join("cgroup.procs")) {
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
        self.stats_with_seams(
            read,
            |p| process_identity(p as u32),
            |p, id| process_metrics(p as u32, Some(id)),
        )
    }

    /// The batched identity-safe stats fold, factored over *all* its seams (the
    /// `cgroup.procs` reader, the identity capture, the metrics read) so a seam
    /// test can drive the full pin → reconfirm → read path — and count reads —
    /// without a real `/proc` or cgroup.
    ///
    /// Batched exactly like [`signal_with_seams`](Self::signal_with_seams): pin
    /// (capture the start-time identity of) **every** member first, then read
    /// `cgroup.procs` exactly **once**, then reconfirm each pinned member against
    /// that single snapshot and read its counters gated on the pinned identity
    /// (`sample_pinned`). The lone reconfirm read lands after every capture, so it
    /// is after *each* member's pin — the same race-freedom order the per-member
    /// [`sample_member_identity_safe`] enforces, now at O(1) reads of an
    /// O(n)-line file instead of O(n).
    ///
    /// `active_process_count` reflects the *initial* member list, as before: a
    /// member that later turns out gone/recycled still counted as live at snapshot
    /// time. An unreadable membership — the initial read (via `?`) or the single
    /// reconfirm read — surfaces as `Err` rather than a silently-short sum.
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
        // 2. One reconfirm read for the whole fold — O(1), not O(n) — taken after
        //    every capture above. Skipped when nothing was pinned (an all-gone or
        //    empty group), matching the old per-member path.
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
    /// **Why one read for the whole batch, not one per pid.** The identity-safe
    /// argument (see [`deliver_identity_safe`]) needs only that each pid's
    /// membership reconfirm happens *after* that pid was pinned — not that every
    /// pid gets its own fresh read. So this pins **every** current member first
    /// (`pin_member`/`pidfd_open`), then reads `cgroup.procs` exactly **once**, and
    /// reconfirms each pinned pid against that single snapshot before sending
    /// (`deliver_pinned`/`pidfd_send_signal`). The lone reconfirm read lands strictly
    /// after every pin, so it is after *each* pid's pin — the race-freedom order is
    /// preserved verbatim, at O(1) reads of an O(n)-line file instead of O(n).
    ///
    /// Holding all N pidfds open across the single read (rather than one at a time)
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
        // 2. One reconfirm read of `cgroup.procs` for the whole batch — O(1), not
        //    O(n) — taken after every pin above. Skipped when nothing was pinned
        //    (an all-gone or empty group), matching the old per-pid path, which
        //    only re-read once it had a live pin to reconfirm.
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
    /// Prefers `cgroup.freeze` (cgroup v2 core file, kernel ≥ 5.2): one write
    /// covers the whole subtree (the kernel applies the freeze shortly after the
    /// write returns) and needs no controllers — the same family as the
    /// `cgroup.kill` file used for teardown. On kernels without it, fall back to
    /// per-pid `SIGSTOP`/`SIGCONT`, mirroring the `cgroup.kill` fallback idiom.
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
        match std::fs::write(self.path.join("cgroup.freeze"), val) {
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
        // `cgroup.kill` (kernel ≥ 5.14): write "1" to SIGKILL the whole subtree
        // atomically.
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
        if std::fs::write(self.path.join("cgroup.kill"), b"1").is_ok() {
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
        let _ = std::fs::write(self.path.join("cgroup.freeze"), b"1");
        for _ in 0..50 {
            if let Ok(members) = self.members_with(&read) {
                if members.is_empty() {
                    break;
                }
                for pid in members {
                    // SAFETY: see signal.
                    unsafe {
                        libc::kill(pid, libc::SIGKILL);
                    }
                }
            }
            // `Err(_)`: unknown state must not look drained. Continue the
            // bounded fallback in case the read failure is transient.
            std::thread::sleep(Duration::from_millis(2));
        }
        // Thaw (best-effort): the freeze only halted forking DURING the sweep.
        // Restore the cgroup unfrozen so it stays reusable for further spawns
        // (`kill_all` keeps the group usable; a child spawned into a frozen
        // cgroup would itself start frozen and the spawn could block) — and so a
        // SIGKILL'd-but-frozen straggler can run its pending fatal signal and exit.
        // (This unconditionally clears any freeze a prior `suspend()` set; a kill
        // verb resurrecting-then-killing a deliberately-suspended group is benign.)
        let _ = std::fs::write(self.path.join("cgroup.freeze"), b"0");
        // Report a real drain failure instead of a false success, so the caller
        // knows the tree may still be alive — a fork bomb still out-spawning, or
        // un-reapable zombies (a D-state task ignores SIGKILL until it unblocks).
        match self.members_with(&read) {
            Ok(members) if members.is_empty() => Ok(()),
            Ok(_) => Err(io::Error::other(
                "cgroup did not drain after the bounded SIGKILL sweep (kernel < 5.14 fallback)",
            )),
            Err(e) => Err(e),
        }
    }
}

impl super::graceful::GracefulTarget for Cgroup {
    fn signal_all(&self, signal: i32) {
        // Best-effort: a delivery failure (a member that exited, EPERM) doesn't
        // stop the graceful tier from proceeding to poll.
        let _ = self.signal(signal);
    }

    fn is_drained(&self) -> bool {
        self.is_empty().unwrap_or(false)
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
        Some(v) => std::fs::write(path, v),
        None if path.exists() => std::fs::write(path, "max"),
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
#[cfg(test)]
mod cgroup_read_seam_tests {
    use std::cell::Cell;
    use std::io;
    use std::path::{Path, PathBuf};

    use super::{Cgroup, Delivery, deliver_identity_safe};

    fn cgroup() -> Cgroup {
        Cgroup {
            path: PathBuf::from("/mock/processkit"),
        }
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

        let cg = Cgroup { path: dir.clone() };
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
    use super::{controllers_to_enable, cpu_max_value};

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
        fn signal_all(&self, _signal: i32) {}
        fn is_drained(&self) -> bool {
            if self.polls.fetch_add(1, Ordering::Relaxed) == 1 {
                self.latch.clear();
            }
            false
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
        Cgroup, MemberSample, ProcIdentity, process_identity, process_metrics, read_proc_starttime,
        sample_member_identity_safe,
    };
    use crate::sys::ProcMetrics;

    /// A mock cgroup whose `cgroup.procs` reads come from an injected seam, so the
    /// batched `stats_with_seams` fold can be driven without a real cgroup mount.
    fn cgroup() -> Cgroup {
        Cgroup {
            path: std::path::PathBuf::from("/mock/processkit"),
        }
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
