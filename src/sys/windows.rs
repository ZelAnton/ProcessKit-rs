//! Windows implementation: a [Job Object] with kill-on-close.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};
#[cfg(feature = "process-control")]
use windows_sys::Win32::Foundation::ERROR_MORE_DATA;
#[cfg(feature = "stats")]
use windows_sys::Win32::Foundation::FILETIME;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
#[cfg(any(feature = "process-control", feature = "stats"))]
use windows_sys::Win32::System::JobObjects::QueryInformationJobObject;
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
#[cfg(feature = "limits")]
use windows_sys::Win32::System::JobObjects::{
    JOB_OBJECT_CPU_RATE_CONTROL_ENABLE, JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
    JOB_OBJECT_LIMIT_ACTIVE_PROCESS, JOB_OBJECT_LIMIT_JOB_MEMORY,
    JOBOBJECT_CPU_RATE_CONTROL_INFORMATION, JobObjectCpuRateControlInformation,
};
#[cfg(feature = "stats")]
use windows_sys::Win32::System::JobObjects::{
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
};
#[cfg(feature = "process-control")]
use windows_sys::Win32::System::JobObjects::{
    JOBOBJECT_BASIC_PROCESS_ID_LIST, JobObjectBasicProcessIdList,
};
#[cfg(feature = "stats")]
use windows_sys::Win32::System::ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
#[cfg(feature = "process-control")]
use windows_sys::Win32::System::Threading::SuspendThread;
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
};
#[cfg(feature = "stats")]
use windows_sys::Win32::System::Threading::{
    GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};

use crate::Mechanism;
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::ResourceLimits;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
#[cfg(feature = "stats")]
use crate::sys::ProcMetrics;

pub(crate) struct Job {
    handle: HANDLE,
    /// Serializes `spawn`'s create-suspended → assign → resume sequence against
    /// the [`suspend`](Self::suspend)/[`resume`](Self::resume) member-thread
    /// walks. Without it, a walk landing between assign and `spawn`'s resume
    /// double-suspends the new child's primary thread (per-thread suspend
    /// *counts*), and `spawn`'s single resume leaves it suspended forever.
    suspend_lock: std::sync::Mutex<()>,
}

// The handle is owned solely by this struct and every Win32 job API used here is
// thread-safe, so the raw pointer is sound to send/share across threads.
unsafe impl Send for Job {}
unsafe impl Sync for Job {}

impl Job {
    pub(crate) fn new(#[cfg(feature = "limits")] limits: &ResourceLimits) -> io::Result<Self> {
        // SAFETY: null name/attributes request an unnamed job with defaults.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Job {
            handle,
            suspend_lock: std::sync::Mutex::new(()),
        };

        // Kill every process in the job once the last handle closes — i.e. when
        // this struct drops or the owning process dies. This is the Windows
        // analogue of `cgroup.kill` / `killpg`. The memory and process-count caps
        // ride along on the same extended-limit struct.
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        #[cfg(feature = "limits")]
        {
            if let Some(bytes) = limits.memory_max {
                info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
                // `JobMemoryLimit` is SIZE_T; saturate rather than wrap on a 32-bit host.
                info.JobMemoryLimit = usize::try_from(bytes).unwrap_or(usize::MAX);
            }
            if let Some(n) = limits.max_processes {
                info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
                info.BasicLimitInformation.ActiveProcessLimit = n;
            }
        }
        // SAFETY: `info` is a fully-initialised struct matching the info class and
        // its size is passed explicitly.
        let ok = unsafe {
            SetInformationJobObject(
                job.handle,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&info).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            // `job` drops here, closing the handle — no leak.
            return Err(io::Error::last_os_error());
        }

        // CPU quota is a separate info class. The hard cap is expressed in 1/100 of
        // a percent of *total* system CPU (1..=10000), so convert our per-core
        // fraction using the host's processor count.
        #[cfg(feature = "limits")]
        if let Some(cores) = limits.cpu_quota {
            let cpus = std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64);
            let rate = cpu_hard_cap_rate(cores, cpus);
            let mut cpu: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION = unsafe { std::mem::zeroed() };
            cpu.ControlFlags =
                JOB_OBJECT_CPU_RATE_CONTROL_ENABLE | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP;
            cpu.Anonymous.CpuRate = rate;
            // SAFETY: fully-initialised struct matching the CPU-rate info class; size
            // passed explicitly. `job` drops (closing the handle) on the error path.
            let ok = unsafe {
                SetInformationJobObject(
                    job.handle,
                    JobObjectCpuRateControlInformation,
                    std::ptr::from_ref(&cpu).cast(),
                    std::mem::size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>() as u32,
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(job)
    }

    pub(crate) fn spawn(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<Child> {
        // Race-free containment: start the child's primary thread SUSPENDED so no
        // user code runs (and nothing can fork) before the process is in the job;
        // assign it, then resume. This closes the old spawn→assign window in
        // which a fast-forking child could have escaped the job. Win32 exposes
        // no flag getter, so this overwrite is also where the Command-carried
        // extras (e.g. CREATE_NO_WINDOW) are OR'd back in.
        use std::os::windows::process::CommandExt;
        cmd.as_std_mut()
            .creation_flags(CREATE_SUSPENDED | opts.creation_flags);

        let mut child = cmd.spawn()?;
        let pid = child.id().ok_or_else(|| {
            io::Error::other("child exited before it could be assigned to the job")
        })?;
        let handle = child.raw_handle().ok_or_else(|| {
            io::Error::other("child exited before it could be assigned to the job")
        })?;
        // Hold the suspend lock across assign → resume: once assigned, the pid
        // is visible to a concurrent suspend()/resume() member walk, which
        // would otherwise skew the still-suspended primary thread's count
        // (suspend counts nest) and strand or prematurely release the child.
        // Poisoning is impossible to act on here — recover the guard.
        let _guard = self
            .suspend_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: the raw handle is valid until `child` is dropped, well after
        // this call returns.
        let ok = unsafe { AssignProcessToJobObject(self.handle, handle as HANDLE) };
        if ok == 0 {
            let err = io::Error::last_os_error();
            // Don't leak a child we failed to contain (still suspended).
            let _ = child.start_kill();
            return Err(err);
        }

        // Contained — release the primary thread. A failure here would strand a
        // suspended-but-contained process, so kill it rather than leak it.
        if let Err(err) = resume_process_threads(pid) {
            let _ = child.start_kill();
            return Err(err);
        }
        Ok(child)
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        let handle = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("child has no handle (already exited?)"))?;
        // SAFETY: the raw handle is valid while `child` is alive (borrowed here).
        let ok = unsafe { AssignProcessToJobObject(self.handle, handle as HANDLE) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        // SAFETY: `self.handle` is a valid job handle for the lifetime of self.
        let ok = unsafe { TerminateJobObject(self.handle, 1) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// A Job Object has no POSIX signals: only `Kill` is deliverable (it maps
    /// to the job terminate); everything else is reported as unsupported so the
    /// caller never believes a reload/interrupt was delivered.
    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        match sig {
            Signal::Kill => self.kill_all(),
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("signal({other:?})"),
            )),
        }
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn suspend(&self) -> io::Result<()> {
        self.for_each_member_thread(true)
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn resume(&self) -> io::Result<()> {
        self.for_each_member_thread(false)
    }

    /// The pids currently assigned to the job (whole tree).
    #[cfg(feature = "process-control")]
    pub(crate) fn members(&self) -> io::Result<Vec<u32>> {
        job_member_pids(self.handle)
    }

    /// Suspend or resume every thread of every process currently in the job.
    ///
    /// Best-effort, not atomic: the member list and the thread snapshot are
    /// taken once, so threads or processes created mid-walk are missed, and
    /// `SuspendThread`/`ResumeThread` maintain per-thread suspend *counts*
    /// (nested suspends need matching resumes). A per-thread failure (e.g. a
    /// thread exiting mid-walk) does not abort the walk; the last failure is
    /// reported after every member has been attempted.
    #[cfg(feature = "process-control")]
    fn for_each_member_thread(&self, suspend: bool) -> io::Result<()> {
        // Mutually exclusive with `spawn`'s assign → resume window (see the
        // `suspend_lock` field doc); held across the pid query AND the walk so
        // the member set can't include a mid-spawn, still-suspended child.
        let _guard = self
            .suspend_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let members: std::collections::HashSet<u32> =
            job_member_pids(self.handle)?.into_iter().collect();
        if members.is_empty() {
            // An empty job is trivially suspended/resumed.
            return Ok(());
        }

        // SAFETY: TH32CS_SNAPTHREAD always snapshots all threads system-wide;
        // returns INVALID_HANDLE_VALUE on failure.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;

        let mut last_err = None;
        // SAFETY: valid snapshot; `entry` is sized via its `dwSize` field.
        let mut ok = unsafe { Thread32First(snapshot, &mut entry) };
        while ok != 0 {
            if members.contains(&entry.th32OwnerProcessID)
                && let Err(err) = suspend_or_resume_thread(entry.th32ThreadID, suspend)
            {
                last_err = Some(err);
            }
            // SAFETY: same valid snapshot and entry.
            ok = unsafe { Thread32Next(snapshot, &mut entry) };
        }
        // SAFETY: handle came from CreateToolhelp32Snapshot; closed exactly once.
        unsafe { CloseHandle(snapshot) };

        match last_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub(crate) async fn graceful_shutdown(
        &self,
        _timeout: Duration,
        _escalate: bool,
    ) -> io::Result<()> {
        // A Job Object has no graceful tier: closing the handle (or terminating
        // it) kills the tree atomically. The timeout/escalate knobs are Unix-only.
        self.kill_all()
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> io::Result<ProcessGroupStats> {
        let mut acct: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
        // SAFETY: out param matches the accounting info class and its size.
        let ok = unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectBasicAccountingInformation,
                std::ptr::from_mut(&mut acct).cast(),
                std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        let mut ext: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        // SAFETY: out param matches the extended-limit info class and its size.
        let ok = unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectExtendedLimitInformation,
                std::ptr::from_mut(&mut ext).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        // Job accounting times are in 100-ns units.
        let cpu_100ns = (acct.TotalUserTime as u64).saturating_add(acct.TotalKernelTime as u64);
        Ok(ProcessGroupStats {
            active_process_count: acct.ActiveProcesses as usize,
            total_cpu_time: Some(Duration::from_nanos(cpu_100ns.saturating_mul(100))),
            peak_memory_bytes: Some(ext.PeakJobMemoryUsed as u64),
        })
    }

    pub(crate) fn mechanism(&self) -> Mechanism {
        Mechanism::JobObject
    }
}

/// Resume every thread of `pid`. A child spawned `CREATE_SUSPENDED` has exactly
/// one thread (its primary); we walk a thread snapshot because std/tokio surface
/// only the process handle, not the `PROCESS_INFORMATION` thread handle returned
/// by `CreateProcess`.
fn resume_process_threads(pid: u32) -> io::Result<()> {
    // SAFETY: TH32CS_SNAPTHREAD always snapshots all threads system-wide (the
    // pid argument is ignored for the thread list); returns INVALID_HANDLE_VALUE
    // on failure.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }

    let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;

    let mut resumed = 0u32;
    let mut last_err = None;
    // SAFETY: valid snapshot; `entry` is sized via its `dwSize` field.
    let mut ok = unsafe { Thread32First(snapshot, &mut entry) };
    while ok != 0 {
        if entry.th32OwnerProcessID == pid {
            match resume_thread(entry.th32ThreadID) {
                Ok(()) => resumed += 1,
                Err(err) => last_err = Some(err),
            }
        }
        // SAFETY: same valid snapshot and entry.
        ok = unsafe { Thread32Next(snapshot, &mut entry) };
    }
    // SAFETY: handle came from CreateToolhelp32Snapshot; closed exactly once.
    unsafe { CloseHandle(snapshot) };

    if resumed == 0 {
        return Err(last_err
            .unwrap_or_else(|| io::Error::other("no thread found to resume the contained child")));
    }
    Ok(())
}

/// Resume a single thread by id (decrement its suspend count).
fn resume_thread(tid: u32) -> io::Result<()> {
    // SAFETY: opens the thread by id; returns null on failure.
    let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, tid) };
    if thread.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: valid thread handle; a `u32::MAX` return signals failure.
    let prev = unsafe { ResumeThread(thread) };
    // SAFETY: handle came from OpenThread; closed exactly once.
    unsafe { CloseHandle(thread) };
    if prev == u32::MAX {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Suspend (increment) or resume (decrement) a single thread's suspend count.
#[cfg(feature = "process-control")]
fn suspend_or_resume_thread(tid: u32, suspend: bool) -> io::Result<()> {
    // SAFETY: opens the thread by id; returns null on failure (e.g. exited).
    let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, tid) };
    if thread.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: valid thread handle; both calls signal failure with `u32::MAX`.
    let prev = unsafe {
        if suspend {
            SuspendThread(thread)
        } else {
            ResumeThread(thread)
        }
    };
    // SAFETY: handle came from OpenThread; closed exactly once.
    unsafe { CloseHandle(thread) };
    if prev == u32::MAX {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Enumerate the pids currently assigned to the job.
///
/// Best-effort snapshot: a process created or reaped during the query may be
/// briefly missing or present. The pid list is a variable-length struct (a
/// two-`u32` header followed by an inline `usize` array), so query into a
/// `u64`-backed buffer (alignment ≥ the struct's) and grow on `ERROR_MORE_DATA`.
#[cfg(feature = "process-control")]
fn job_member_pids(handle: HANDLE) -> io::Result<Vec<u32>> {
    // Seed generously so the common case is a single query.
    let mut cap: usize = 64;
    loop {
        let bytes = std::mem::size_of::<JOBOBJECT_BASIC_PROCESS_ID_LIST>()
            + cap.saturating_sub(1) * std::mem::size_of::<usize>();
        // u64 alignment (8) ≥ the struct's (usize) on every Windows target, so
        // casting the buffer to the struct pointer below is sound.
        let mut buf = vec![0u64; bytes.div_ceil(std::mem::size_of::<u64>())];
        // SAFETY: `buf` spans at least `bytes` writable bytes, the info class
        // matches the out-struct, and the size is passed explicitly.
        let ok = unsafe {
            QueryInformationJobObject(
                handle,
                JobObjectBasicProcessIdList,
                buf.as_mut_ptr().cast(),
                bytes as u32,
                std::ptr::null_mut(),
            )
        };
        let list = buf.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>();
        if ok == 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_MORE_DATA as i32) {
                // The header is populated even when the list didn't fit — size
                // the retry from it (with headroom for races), and make sure we
                // always grow so the loop can't spin in place.
                // SAFETY: on ERROR_MORE_DATA the fixed header fields are valid.
                let assigned = unsafe { (*list).NumberOfAssignedProcesses } as usize;
                cap = assigned.max(cap).saturating_mul(2);
                continue;
            }
            return Err(err);
        }
        // SAFETY: a successful query wrote the header and `NumberOfProcessIdsInList`
        // pids contiguously from `ProcessIdList[0]`, all within `bytes`.
        let n = unsafe { (*list).NumberOfProcessIdsInList } as usize;
        // SAFETY: see above; `n <= cap` elements were written.
        let ids = unsafe { std::slice::from_raw_parts((*list).ProcessIdList.as_ptr(), n) };
        return Ok(ids.iter().map(|&pid| pid as u32).collect());
    }
}

/// Combine a FILETIME (100-ns units) into nanoseconds.
#[cfg(feature = "stats")]
fn filetime_nanos(ft: FILETIME) -> u64 {
    let units = ((ft.dwHighDateTime as u64) << 32) | ft.dwLowDateTime as u64;
    units.saturating_mul(100)
}

/// Convert a per-core CPU quota into a Job Object hard-cap `CpuRate`: 1/100 of a
/// percent of *total* system CPU, in `1..=10000`. `cores` is a fraction of one core
/// (`0.5` = half a core); `cpus` is the host processor count. A quota meeting or
/// exceeding the core count saturates at 100% (`10000`), and the result floors at
/// `1` since the API rejects a zero rate.
#[cfg(feature = "limits")]
fn cpu_hard_cap_rate(cores: f64, cpus: f64) -> u32 {
    let rate = ((cores / cpus) * 10_000.0).round();
    // `f64 as u32` is saturating, but clamp first so the floor-at-1 (zero is invalid)
    // and the 100% ceiling are explicit rather than relying on cast behaviour.
    rate.clamp(1.0, 10_000.0) as u32
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(pid: u32) -> ProcMetrics {
    let mut metrics = ProcMetrics::default();
    // SAFETY: limited-information access; returns null on failure (e.g. gone).
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return metrics;
    }

    let mut creation = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut exit = creation;
    let mut kernel = creation;
    let mut user = creation;
    // SAFETY: valid handle; all four out params are owned locals.
    let ok = unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
    if ok != 0 {
        metrics.cpu_time = Some(Duration::from_nanos(
            filetime_nanos(kernel) + filetime_nanos(user),
        ));
    }

    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    // SAFETY: valid handle; `counters` sized via its `cb` field.
    let ok = unsafe { K32GetProcessMemoryInfo(handle, &mut counters, counters.cb) };
    if ok != 0 {
        metrics.peak_memory_bytes = Some(counters.PeakWorkingSetSize as u64);
    }

    // SAFETY: handle came from OpenProcess and is closed exactly once.
    unsafe { CloseHandle(handle) };
    metrics
}

impl Drop for Job {
    fn drop(&mut self) {
        // Closing the last handle triggers KILL_ON_JOB_CLOSE → the tree is reaped.
        // SAFETY: handle came from CreateJobObjectW and is closed exactly once.
        unsafe { CloseHandle(self.handle) };
    }
}

#[cfg(all(test, feature = "limits"))]
mod tests {
    use super::cpu_hard_cap_rate;

    #[test]
    fn cpu_rate_maps_per_core_fraction_to_total_system_percent() {
        // Half a core out of eight = 6.25% of the whole machine.
        assert_eq!(cpu_hard_cap_rate(0.5, 8.0), 625);
        // A whole single core on a 1-CPU host = 100%.
        assert_eq!(cpu_hard_cap_rate(1.0, 1.0), 10_000);
        // Asking for every core = 100%.
        assert_eq!(cpu_hard_cap_rate(4.0, 4.0), 10_000);
        // Over-subscribing (more cores than exist) saturates at 100%, never above.
        assert_eq!(cpu_hard_cap_rate(8.0, 4.0), 10_000);
        // A vanishingly small quota floors at 1 — the API rejects a zero rate.
        assert_eq!(cpu_hard_cap_rate(0.0001, 64.0), 1);
    }
}
