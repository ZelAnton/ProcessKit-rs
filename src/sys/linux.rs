//! Linux implementation: a [cgroup v2] killed via `cgroup.kill`, with a POSIX
//! process-group fallback when no writable cgroup is available (e.g. a CI runner
//! without cgroup delegation).
//!
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

use std::ffi::{CStr, CString};
use std::io;
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
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
#[cfg(feature = "stats")]
use crate::sys::ProcMetrics;
use crate::sys::pgroup::ProcessGroup;

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
                    Ok(()) => Ok(()),
                    // The child already exited (a zombie pid) — the write fails
                    // ESRCH. Nothing to contain, so return Ok, matching the
                    // process-group backend (which maps ESRCH→Ok).
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Backend::ProcessGroup(pg) => pg.adopt(child),
        }
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.kill(),
            Backend::ProcessGroup(pg) => pg.kill_all(),
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
            Backend::Cgroup(cg) => cg.members(),
            // Fallback tracks group leaders only.
            Backend::ProcessGroup(pg) => pg.members(),
        };
        Ok(pids.into_iter().map(|pid| pid as u32).collect())
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
            Backend::Cgroup(cg) => {
                // Our cgroup has no controllers enabled (so `cgroup.kill` works
                // without the "no internal processes" rule), so cpu/memory aren't
                // available from the cgroup itself — sum per-process /proc
                // counters of the live members instead.
                //
                // Note: `cgroup.procs` retains an unreaped zombie's pid until its
                // parent reaps it, and `/proc/<pid>/stat` still reports the
                // zombie's final counters — so a tree with unreaped zombies can
                // momentarily over-report `active_process_count` and fold dead
                // members' CPU/memory. (The pgroup backend over-reports the count
                // the same way; it reports no CPU/memory at all, so only the count
                // is affected there.)
                let pids = cg.members();
                let active = pids.len();
                let mut cpu = Duration::ZERO;
                let mut have_cpu = false;
                let mut mem = 0u64;
                let mut have_mem = false;
                for pid in pids {
                    let m = process_metrics(pid as u32);
                    if let Some(c) = m.cpu_time {
                        // Saturating: summing many members' CPU time could in
                        // principle overflow `Duration`; clamp rather than panic.
                        cpu = cpu.saturating_add(c);
                        have_cpu = true;
                    }
                    if let Some(p) = m.peak_memory_bytes {
                        mem = mem.saturating_add(p);
                        have_mem = true;
                    }
                }
                Ok(ProcessGroupStats {
                    active_process_count: active,
                    total_cpu_time: have_cpu.then_some(cpu),
                    peak_memory_bytes: have_mem.then_some(mem),
                })
            }
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

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(pid: u32) -> ProcMetrics {
    let mut metrics = ProcMetrics::default();

    // CPU: /proc/<pid>/stat fields utime (14) + stime (15), in clock ticks.
    // The comm field (2) may contain spaces/parens, so parse after the last ')'.
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        && let Some(idx) = stat.rfind(')')
    {
        let fields: Vec<&str> = stat[idx + 1..].split_whitespace().collect();
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
                        if cg.is_empty() {
                            break;
                        }
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

struct Cgroup {
    path: PathBuf,
}

impl Cgroup {
    fn create(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // Only the cgroup v2 unified hierarchy exposes this file at the root.
        let root = Path::new("/sys/fs/cgroup");
        if !root.join("cgroup.controllers").exists() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cgroup v2 not mounted",
            ));
        }

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
        // The controllers each requested limit needs.
        let mut needed: Vec<&str> = Vec::new();
        if limits.memory_max.is_some() {
            needed.push("memory");
        }
        if limits.max_processes.is_some() {
            needed.push("pids");
        }
        if limits.cpu_quota.is_some() {
            needed.push("cpu");
        }

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
        let to_enable = controllers_to_enable(&needed, &enabled);
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

        if let Some(bytes) = limits.memory_max {
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

    /// Read the live member pids (empty if the file is gone).
    fn members(&self) -> Vec<i32> {
        match std::fs::read_to_string(self.path.join("cgroup.procs")) {
            Ok(procs) => procs
                .lines()
                // Keep only real pids: a `0`/negative line (never emitted by the
                // kernel, but cheap to guard) would otherwise reach `kill(pid, …)`
                // as "the caller's whole process group" (0) or "a process group"
                // (negative) — never a single tracked member.
                .filter_map(|l| l.trim().parse::<i32>().ok())
                .filter(|&pid| pid > 0)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.members().is_empty()
    }

    /// Send `sig` to every current member (the graceful SIGTERM tier and the
    /// public signal broadcast). Best-effort: an empty cgroup is trivially
    /// signalled, and a member that exits mid-loop just yields `ESRCH`.
    ///
    /// This per-pid path is inherently best-effort against pid recycling: in the
    /// window between reading `cgroup.procs` and `kill(pid, sig)`, a member can
    /// exit and its pid be reused by an unrelated process, which would then
    /// receive `sig`. The window is tiny, but the kernel offers no atomic
    /// per-pid-within-cgroup signal — only `cgroup.kill` (whole-subtree SIGKILL,
    /// used by [`kill`](Self::kill)) and `cgroup.freeze` (whole-subtree
    /// suspend/resume, preferred by [`freeze`](Self::freeze)) are race-free.
    /// SIGKILL teardown therefore goes through `cgroup.kill`, not this path.
    fn signal(&self, sig: i32) -> io::Result<()> {
        let mut last_err = None;
        for pid in self.members() {
            // SAFETY: a plain signal to a pid read from cgroup.procs.
            let rc = unsafe { libc::kill(pid, sig) };
            if rc != 0 {
                let err = io::Error::last_os_error();
                // A race where the pid already exited (ESRCH) is benign — the
                // member is gone, the intended end state. Any other failure
                // (notably EPERM — a member that changed uid, or a seccomp /
                // container restriction) is a real delivery failure and must not
                // read as success: surface the last one.
                if err.raw_os_error() != Some(libc::ESRCH) {
                    last_err = Some(err);
                }
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
        let _ = std::fs::write(self.path.join("cgroup.freeze"), b"1");
        for _ in 0..50 {
            let members = self.members();
            if members.is_empty() {
                break;
            }
            for pid in members {
                // SAFETY: see `signal`.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
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
        if self.members().is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(
                "cgroup did not drain after the bounded SIGKILL sweep (kernel < 5.14 fallback)",
            ))
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
        self.is_empty()
    }

    fn hard_kill(&self) -> io::Result<()> {
        self.kill()
    }
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

/// Arm `PR_SET_PDEATHSIG(SIGKILL)` so the kernel kills this child when the
/// spawning thread dies, then close the parent-died-before-arming race: if
/// `getppid()` no longer reports `spawner_pid` (captured in the parent before
/// the fork), the parent died in the window and the signal will never fire —
/// exit immediately instead. Comparing against the captured pid (never the
/// literal `1`) keeps the guard correct when the spawner itself *is* PID 1 —
/// a container entrypoint, exactly where this hardening matters most.
/// Runs in the forked child after `fork()` and before `exec()`.
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
