//! Windows implementation: a [Job Object] with kill-on-close.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};
use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::Threading::{
    GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};

use crate::Mechanism;
use crate::stats::ProcessGroupStats;
use crate::sys::ProcMetrics;

pub(crate) struct Job {
    handle: HANDLE,
}

// The handle is owned solely by this struct and every Win32 job API used here is
// thread-safe, so the raw pointer is sound to send/share across threads.
unsafe impl Send for Job {}
unsafe impl Sync for Job {}

impl Job {
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: null name/attributes request an unnamed job with defaults.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Job { handle };

        // Kill every process in the job once the last handle closes — i.e. when
        // this struct drops or the owning process dies. This is the Windows
        // analogue of `cgroup.kill` / `killpg`.
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
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
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    pub(crate) fn spawn(&self, cmd: &mut Command) -> io::Result<Child> {
        // Spawn first, then assign. There is a narrow window between spawn and
        // assignment in which the child could spawn its own children outside the
        // job; acceptable for v1 (the binaries we drive don't fork that fast).
        // Hardening via CREATE_SUSPENDED + ResumeThread is a follow-up.
        let mut child = cmd.spawn()?;
        let handle = child.raw_handle().ok_or_else(|| {
            io::Error::other("child exited before it could be assigned to the job")
        })?;
        // SAFETY: the raw handle is valid until `child` is dropped, well after
        // this call returns.
        let ok = unsafe { AssignProcessToJobObject(self.handle, handle as HANDLE) };
        if ok == 0 {
            let err = io::Error::last_os_error();
            // Don't leak a child we failed to contain.
            let _ = child.start_kill();
            return Err(err);
        }
        Ok(child)
    }

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

    pub(crate) async fn graceful_shutdown(
        &self,
        _timeout: Duration,
        _escalate: bool,
    ) -> io::Result<()> {
        // A Job Object has no graceful tier: closing the handle (or terminating
        // it) kills the tree atomically. The timeout/escalate knobs are Unix-only.
        self.kill_all()
    }

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

/// Combine a FILETIME (100-ns units) into nanoseconds.
fn filetime_nanos(ft: FILETIME) -> u64 {
    let units = ((ft.dwHighDateTime as u64) << 32) | ft.dwLowDateTime as u64;
    units.saturating_mul(100)
}

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
