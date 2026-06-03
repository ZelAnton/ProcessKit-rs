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
use tokio::time::{Instant, sleep};

use crate::Mechanism;
use crate::stats::ProcessGroupStats;
use crate::sys::ProcMetrics;
use crate::sys::pgroup::ProcessGroup;

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
    /// Fallback when no writable cgroup is available: the shared POSIX
    /// process-group backend (each child leads its own group). Its own `Drop`
    /// hard-kills the tracked groups.
    ProcessGroup(ProcessGroup),
}

impl Job {
    pub(crate) fn new() -> io::Result<Self> {
        // Prefer a cgroup; degrade to a process group if we can't make one
        // (no cgroup v2, no delegation, read-only fs, …). The choice is
        // observable via `mechanism()` — never silent.
        let backend = match Cgroup::create() {
            Ok(cg) => Backend::Cgroup(cg),
            Err(_) => Backend::ProcessGroup(ProcessGroup::new()),
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
            Backend::ProcessGroup(pg) => pg.spawn(cmd),
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
            Backend::ProcessGroup(pg) => pg.adopt(child),
        }
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Cgroup(cg) => cg.kill(),
            Backend::ProcessGroup(pg) => pg.kill_all(),
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
            Backend::ProcessGroup(pg) => pg.graceful_shutdown(timeout, escalate).await,
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
                // `cgroup.kill` is asynchronous: the kernel SIGKILLs the subtree,
                // but `rmdir` returns `EBUSY` until the members have actually left
                // (a process leaves `cgroup.procs` when it *exits*, before it is
                // reaped — so this drains within milliseconds and doesn't depend on
                // the async reaper). Wait, bounded, so we don't leak the dir; sleep
                // rather than busy-spin.
                for _ in 0..50 {
                    if cg.is_empty() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                // Best-effort: an emptied cgroup dir can be removed.
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
        // Older kernels: SIGKILL each member until the cgroup drains. Sleep
        // between sweeps rather than busy-spin while the kernel reaps, and bound
        // it so teardown (incl. Drop) can never hang on un-reaped zombies.
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
