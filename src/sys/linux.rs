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
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep};

use crate::Mechanism;
use crate::stats::ProcessGroupStats;
use crate::sys::ProcMetrics;

/// How often the graceful path re-checks whether the tree has drained.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Process-wide counter so concurrent jobs get distinct cgroup names.
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) struct Job {
    backend: Backend,
}

enum Backend {
    /// All children live in this cgroup; killed via `cgroup.kill`.
    Cgroup(Cgroup),
    /// Fallback: each spawned child leads its own process group; we track the
    /// group ids (== child pids) and signal them on teardown.
    ProcessGroup(Mutex<Vec<i32>>),
}

impl Job {
    pub(crate) fn new() -> io::Result<Self> {
        // Prefer a cgroup; degrade to a process group if we can't make one
        // (no cgroup v2, no delegation, read-only fs, …). The choice is
        // observable via `mechanism()` — never silent.
        let backend = match Cgroup::create() {
            Ok(cg) => Backend::Cgroup(cg),
            Err(_) => Backend::ProcessGroup(Mutex::new(Vec::new())),
        };
        Ok(Job { backend })
    }

    pub(crate) fn spawn(&self, cmd: &mut Command) -> io::Result<Child> {
        match &self.backend {
            Backend::Cgroup(cg) => {
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
                cmd.spawn()
            }
            Backend::ProcessGroup(pgids) => {
                // Own process group per child → killpg reaps it and its
                // descendants. `process_group(0)` == setpgid(0, 0): the child
                // becomes its own group leader.
                cmd.as_std_mut().process_group(0);
                let child = cmd.spawn()?;
                if let Some(pid) = child.id()
                    && let Ok(mut g) = pgids.lock()
                {
                    g.push(pid as i32);
                }
                Ok(child)
            }
        }
    }

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
                std::fs::write(cg.path.join("cgroup.procs"), pid.to_string().as_bytes())
            }
            Backend::ProcessGroup(pgids) => {
                // Make the external child its own group leader and track it. As
                // with the cgroup path, only the child itself is moved — already
                // running descendants keep their original group.
                // SAFETY: setpgid on a live pid is a sound call.
                let rc = unsafe { libc::setpgid(pid, 0) };
                if rc != 0 {
                    let err = io::Error::last_os_error();
                    // Benign races/permissions (process gone, already a session
                    // leader, cross-session) are not fatal — mirror .NET's `Add`.
                    let code = err.raw_os_error().unwrap_or(0);
                    if code != libc::ESRCH && code != libc::EPERM && code != libc::EACCES {
                        return Err(err);
                    }
                }
                if let Ok(mut g) = pgids.lock() {
                    g.push(pid);
                }
                Ok(())
            }
        }
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.kill(),
            Backend::ProcessGroup(pgids) => {
                signal_groups(pgids, libc::SIGKILL);
                Ok(())
            }
        }
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => {
                cg.signal(libc::SIGTERM);
                let deadline = Instant::now() + timeout;
                while !cg.is_empty() {
                    if Instant::now() >= deadline {
                        break;
                    }
                    sleep(POLL_INTERVAL).await;
                }
                if escalate && !cg.is_empty() {
                    cg.kill()?;
                }
                Ok(())
            }
            Backend::ProcessGroup(pgids) => {
                signal_groups(pgids, libc::SIGTERM);
                let deadline = Instant::now() + timeout;
                while groups_alive(pgids) {
                    if Instant::now() >= deadline {
                        break;
                    }
                    sleep(POLL_INTERVAL).await;
                }
                if escalate && groups_alive(pgids) {
                    signal_groups(pgids, libc::SIGKILL);
                }
                Ok(())
            }
        }
    }

    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        match &self.backend {
            Backend::Cgroup(cg) => {
                // Our cgroup has no controllers enabled (so `cgroup.kill` works
                // without the "no internal processes" rule), so cpu/memory aren't
                // available from the cgroup itself — sum per-process /proc
                // counters of the live members instead.
                let pids = cg.members();
                let active = pids.len();
                let mut cpu = Duration::ZERO;
                let mut have_cpu = false;
                let mut mem = 0u64;
                let mut have_mem = false;
                for pid in pids {
                    let m = process_metrics(pid as u32);
                    if let Some(c) = m.cpu_time {
                        cpu += c;
                        have_cpu = true;
                    }
                    if let Some(p) = m.peak_memory_bytes {
                        mem += p;
                        have_mem = true;
                    }
                }
                Ok(ProcessGroupStats {
                    active_process_count: active,
                    total_cpu_time: have_cpu.then_some(cpu),
                    peak_memory_bytes: have_mem.then_some(mem),
                })
            }
            Backend::ProcessGroup(pgids) => {
                // The fallback tracks group ids, not individual pids, so report
                // the number of still-live groups and leave cpu/memory absent.
                let active = match pgids.lock() {
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
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        match &self.backend {
            Backend::Cgroup(_) => Mechanism::CgroupV2,
            Backend::ProcessGroup(_) => Mechanism::ProcessGroup,
        }
    }
}

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
                let nanos = (utime + stime) as u128 * 1_000_000_000u128 / hz as u128;
                metrics.cpu_time = Some(Duration::from_nanos(nanos as u64));
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
                    metrics.peak_memory_bytes = Some(kb * 1024);
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
                let _ = cg.kill();
                // Best-effort: an emptied cgroup dir can be removed.
                let _ = std::fs::remove_dir(&cg.path);
            }
            Backend::ProcessGroup(pgids) => signal_groups(pgids, libc::SIGKILL),
        }
    }
}

/// Send `sig` to every tracked process group.
///
/// Caveat of this fallback (the cgroup path doesn't share it): a group id is the
/// leader's pid, so if the leader was already reaped and its pid recycled before
/// we fire, the signal could in theory hit an unrelated group. The window is a
/// few instructions wide, so this is accepted for the no-cgroup degraded path.
fn signal_groups(pgids: &Mutex<Vec<i32>>, sig: i32) {
    if let Ok(g) = pgids.lock() {
        for &pgid in g.iter() {
            // SAFETY: killpg on a positive group id is always a sound call; a
            // group that is already gone simply returns ESRCH.
            unsafe {
                libc::killpg(pgid, sig);
            }
        }
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

struct Cgroup {
    path: PathBuf,
}

impl Cgroup {
    fn create() -> io::Result<Self> {
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

        let name = format!(
            "processkit-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        let path = parent.join(name);
        // No controllers enabled — `cgroup.kill` needs none, and that sidesteps
        // the "no internal processes" rule. mkdir is the permission gate that
        // triggers the process-group fallback when delegation is absent.
        std::fs::create_dir(&path)?;
        Ok(Cgroup { path })
    }

    /// Read the live member pids (empty if the file is gone).
    fn members(&self) -> Vec<i32> {
        match std::fs::read_to_string(self.path.join("cgroup.procs")) {
            Ok(procs) => procs
                .lines()
                .filter_map(|l| l.trim().parse::<i32>().ok())
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.members().is_empty()
    }

    /// Send `sig` to every current member (used for the graceful SIGTERM tier).
    fn signal(&self, sig: i32) {
        for pid in self.members() {
            // SAFETY: a plain signal to a pid read from cgroup.procs; a race
            // where the pid already exited just yields ESRCH.
            unsafe {
                libc::kill(pid, sig);
            }
        }
    }

    fn kill(&self) -> io::Result<()> {
        // `cgroup.kill` (kernel ≥ 5.14): write "1" to SIGKILL the whole subtree
        // atomically.
        if std::fs::write(self.path.join("cgroup.kill"), b"1").is_ok() {
            return Ok(());
        }
        // Older kernels: SIGKILL each member until the cgroup drains. Bounded so
        // teardown (incl. Drop) can never hang on un-reaped zombies.
        for _ in 0..100 {
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
        }
        Ok(())
    }
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
        Ok(())
    }
}
