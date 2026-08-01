//! Windows implementation: a [Job Object] with kill-on-close.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects

use std::io;
use std::time::Duration;

use tokio::process::{Child, Command};
// The process-creation `FILETIME` is both the `stats` identity anchor and the
// `process-control` member-snapshot start time, so it (and `GetProcessTimes`
// below) is needed under either feature.
#[cfg(any(feature = "stats", feature = "process-control"))]
use windows_sys::Win32::Foundation::FILETIME;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, HWND, INVALID_HANDLE_VALUE, LPARAM};
#[cfg(feature = "process-control")]
use windows_sys::Win32::Foundation::{ERROR_INVALID_PARAMETER, ERROR_MORE_DATA};
use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
// System-wide process snapshot for `members_info`'s per-member ppid + image name
// (no per-pid Win32 API yields the parent pid without ntdll).
#[cfg(feature = "process-control")]
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    PROCESSENTRY32W, Process32FirstW, Process32NextW, TH32CS_SNAPPROCESS,
};
// Ungated: the opt-in console-CTRL graceful teardown is compiled on every
// Windows feature config, and its drain check (`QueryInformationJobObject` on
// the accounting info) and per-leader recycle guard (`IsProcessInJob`) can't be
// gated behind `process-control`/`stats`.
use windows_sys::Win32::System::JobObjects::IsProcessInJob;
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
use windows_sys::Win32::System::JobObjects::{
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
};
#[cfg(feature = "process-control")]
use windows_sys::Win32::System::JobObjects::{
    JOBOBJECT_BASIC_PROCESS_ID_LIST, JobObjectBasicProcessIdList,
};
#[cfg(feature = "stats")]
use windows_sys::Win32::System::ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
#[cfg(any(feature = "stats", feature = "process-control"))]
use windows_sys::Win32::System::Threading::GetProcessTimes;
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED, OpenThread, ResumeThread, SetProcessAffinityMask,
    THREAD_SUSPEND_RESUME,
};
#[cfg(feature = "process-control")]
use windows_sys::Win32::System::Threading::{
    GetExitCodeProcess, GetProcessIdOfThread, SuspendThread, THREAD_QUERY_LIMITED_INFORMATION,
};
use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};
// EnumWindows + PostMessageW(WM_CLOSE) for the best-effort GUI-graceful tier: a
// windowed member (Electron/desktop tool/windowed service) receives no console
// CTRL event, so WM_CLOSE is the only soft "please close" it can act on before the
// TerminateJobObject fallback. Un-gated (the WM_CLOSE tier is core to graceful
// shutdown, not feature-gated).
use windows_sys::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowThreadProcessId, PostMessageW, WM_CLOSE,
};
use windows_sys::core::BOOL;

use crate::Mechanism;
#[cfg(feature = "process-control")]
use crate::Signal;
#[cfg(feature = "limits")]
use crate::limits::{CappedAxes, LimitEvidence, LimitKind, LimitVerdict, ResourceLimits};
#[cfg(feature = "process-control")]
use crate::member::MemberInfo;
#[cfg(feature = "stats")]
use crate::stats::ProcessGroupStats;
#[cfg(feature = "stats")]
use crate::sys::{ProcIdentity, ProcMetrics};

pub(crate) struct Job {
    /// The job handle — deliberately non-inheritable and never duplicated:
    /// when this process dies (however abruptly), the kernel closes the last
    /// handle and `KILL_ON_JOB_CLOSE` takes the whole tree. That free
    /// kill-on-parent-death guarantee (documented on
    /// `Command::kill_on_parent_death`) breaks if a refactor ever duplicates
    /// or inherits this handle.
    ///
    /// One inherent gap (C10): a child is spawned `CREATE_SUSPENDED` and only then
    /// assigned to the job. If the **parent dies abruptly in that spawn→assign
    /// window** — after `CreateProcess` returns but before `AssignProcessToJobObject`
    /// — the child is not yet a job member, so kill-on-close can't reach it and it
    /// leaks as a permanently-*suspended* orphan (it never ran). The window is a
    /// few instructions wide and the orphan is inert (suspended), but it is not
    /// covered by the "kernel kills the tree even on abrupt parent death" headline.
    handle: HANDLE,
    /// Serializes ordinary and ConPTY create-suspended → assign → resume
    /// sequences against the `suspend`/`resume` member-thread walks. Without it,
    /// a walk landing between assign and launch's resume nests the new child's
    /// per-thread suspend count and can leave it suspended forever.
    suspend_lock: std::sync::Mutex<()>,
    /// Set by `graceful_shutdown(escalate=false)` so `Drop` clears
    /// `KILL_ON_JOB_CLOSE` before closing the handle, leaving survivors alive.
    skip_drop_kill: super::SkipDropKill,
    /// Pids of direct children spawned `CREATE_NEW_PROCESS_GROUP` for the opt-in
    /// console-CTRL graceful path (via
    /// [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break)).
    /// Empty unless a child opted in: `graceful_shutdown` takes the CTRL_BREAK →
    /// grace → `TerminateJobObject` path **iff** this contains a live leader, so
    /// the default atomic-kill behavior is untouched for every other job. Each pid
    /// is a console
    /// **process-group id** (equal to the leader's pid) addressable by
    /// `GenerateConsoleCtrlEvent`; a per-leader `IsProcessInJob` re-check at signal
    /// time keeps a recycled pid from diverting the event onto a stranger's group.
    ///
    /// Bounded, not merely monotonic (T-154): `spawn` prunes exited/non-member
    /// entries — via the same `process_is_in_job` recycle guard `signal_all`
    /// uses at teardown — before recording each new leader, so a long-lived
    /// shared `Job` that repeatedly spawns opt-in children does not grow this
    /// list for the job's whole lifetime; it stays bounded by the count of
    /// concurrently-live opt-in leaders. Between opt-in spawns a dead entry can
    /// sit here until the *next* opt-in spawn prunes it — harmless, since a dead
    /// pid is never a live job member and `signal_all`'s own guard already skips
    /// it at signal time.
    ctrl_break_leaders: std::sync::Mutex<Vec<u32>>,
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
            skip_drop_kill: super::SkipDropKill::new(),
            ctrl_break_leaders: std::sync::Mutex::new(Vec::new()),
        };

        // Kill every process in the job once the last handle closes — i.e. when
        // this struct drops or the owning process dies. This is the Windows
        // analogue of `cgroup.kill` / `killpg`. The memory and process-count caps
        // ride along on the same extended-limit struct.
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        #[cfg(feature = "limits")]
        {
            if let Some(bytes) = limits.max_memory {
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

    /// Re-apply a fresh [`ResourceLimits`] set to the **live** Job Object — the
    /// backend for [`ProcessGroup::update_limits`](crate::ProcessGroup::update_limits).
    ///
    /// Reissues the exact two `SetInformationJobObject` calls
    /// [`new`](Self::new) makes at creation
    /// (`JOBOBJECT_EXTENDED_LIMIT_INFORMATION` + `JOBOBJECT_CPU_RATE_CONTROL_INFORMATION`),
    /// so the semantics are a **full replacement**: an axis left `None` clears its
    /// limit flag and the job is unbounded on that axis again, exactly as if the
    /// group had been created with that axis unset.
    ///
    /// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is deliberately OR'd back into
    /// `LimitFlags`: the extended-limit struct carries *both* the containment flag
    /// and the memory/process caps, and `SetInformationJobObject` overwrites the
    /// whole `LimitFlags`, so re-writing it without kill-on-close would silently
    /// strip the tree's kill-on-drop guarantee. The CPU-rate struct is written
    /// unconditionally — `ControlFlags = 0` (no `ENABLE`) is how a previously-set
    /// hard cap is cleared, so a removed `cpu_quota` reliably lifts the cap rather
    /// than leaving a stale one in place.
    #[cfg(feature = "limits")]
    pub(crate) fn update_limits(&self, limits: &ResourceLimits) -> io::Result<()> {
        // Rebuild the extended-limit struct from scratch, exactly as `new` does:
        // kill-on-close is always present, and only the requested (Some) caps set
        // their flags — a None axis is left with its flag clear, i.e. unbounded.
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if let Some(bytes) = limits.max_memory {
            info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
            info.JobMemoryLimit = usize::try_from(bytes).unwrap_or(usize::MAX);
        }
        if let Some(n) = limits.max_processes {
            info.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            info.BasicLimitInformation.ActiveProcessLimit = n;
        }
        // SAFETY: `info` is a fully-initialised struct matching the info class and
        // its size is passed explicitly. The handle is valid for the lifetime of
        // self.
        let ok = unsafe {
            SetInformationJobObject(
                self.handle,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&info).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("extended-limit reissue: {err}"),
            ));
        }

        // CPU quota is a separate info class. Written unconditionally so a removed
        // cap is actually cleared: `ControlFlags = 0` disables CPU rate control,
        // while `Some(cores)` re-enables the hard cap at the converted rate — the
        // same conversion `new` uses.
        let mut cpu: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION = unsafe { std::mem::zeroed() };
        if let Some(cores) = limits.cpu_quota {
            let cpus = std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64);
            cpu.ControlFlags =
                JOB_OBJECT_CPU_RATE_CONTROL_ENABLE | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP;
            cpu.Anonymous.CpuRate = cpu_hard_cap_rate(cores, cpus);
        }
        // SAFETY: fully-initialised struct matching the CPU-rate info class; size
        // passed explicitly. A zeroed struct (ControlFlags = 0) is the documented
        // way to disable rate control.
        let ok = unsafe {
            SetInformationJobObject(
                self.handle,
                JobObjectCpuRateControlInformation,
                std::ptr::from_ref(&cpu).cast(),
                std::mem::size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>() as u32,
            )
        };
        if ok == 0 {
            let err = io::Error::last_os_error();
            // Clearing a CPU cap (cpu_quota == None → ControlFlags = 0) on a job
            // that never had rate control enabled is rejected with
            // `ERROR_INVALID_PARAMETER` — there is nothing to disable. The desired
            // state (no CPU cap) already holds, so that specific case is a success,
            // not a failure. A real failure while *setting* a cap (cpu_quota
            // Some), or any other error kind, still propagates.
            let benign_clear = limits.cpu_quota.is_none()
                && err.raw_os_error()
                    == Some(windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER as i32);
            if !benign_clear {
                return Err(io::Error::new(
                    err.kind(),
                    format!("cpu-rate reissue: {err}"),
                ));
            }
        }
        Ok(())
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
        //
        // Opt-in graceful CTRL path: OR in CREATE_NEW_PROCESS_GROUP so the direct
        // child becomes its own console process group, addressable by its pid via
        // `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` at graceful teardown.
        // A side effect is that CTRL_C is disabled for the new group by default —
        // CTRL_BREAK, which the teardown sends, is unaffected.
        use std::os::windows::process::CommandExt;
        let mut flags = CREATE_SUSPENDED | opts.creation_flags;
        if opts.windows_new_process_group {
            flags |= CREATE_NEW_PROCESS_GROUP;
        }
        cmd.as_std_mut().creation_flags(flags);

        // Arm a reaper for the window between spawn and containment: the child is
        // suspended and not yet in the job, so until `AssignProcessToJobObject`
        // succeeds nothing would reap it — an early return or panic here would
        // leak a suspended orphan. Disarmed once contained, restoring the normal
        // "the job owns teardown" semantics. (A permanent `kill_on_drop` would
        // instead fight `graceful_shutdown(escalate=false)` survivor-sparing, and
        // tokio can't toggle it off after spawn.) Arm it *before* reading the
        // fallible `id()`/`raw_handle()` so even their `?` early-returns reap.
        let child = {
            // Headless ConPTY temporarily changes process-global std handles;
            // use its shared spawn lock so this ordinary child cannot observe
            // those null slots while std resolves inherited stdio.
            let _spawn_guard = crate::sys::process_spawn_lock();
            cmd.spawn()?
        };
        let guard = UncontainedChildGuard::arm(child);
        let pid = guard.child().id().ok_or_else(|| {
            io::Error::other("child exited before it could be assigned to the job")
        })?;
        let handle = guard.child().raw_handle().ok_or_else(|| {
            io::Error::other("child exited before it could be assigned to the job")
        })?;
        // Hold the suspend lock across assign → resume: once assigned, the pid
        // is visible to a concurrent suspend()/resume() member walk, which
        // would otherwise skew the still-suspended primary thread's count
        // (suspend counts nest) and strand or prematurely release the child.
        // Poisoning is impossible to act on here — recover the guard.
        let _suspend_guard = self
            .suspend_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: the raw handle is valid until the child is dropped; `guard`
        // owns the child for the rest of this scope, well past this call.
        //
        // Nested jobs: if THIS process is itself inside a Job Object that forbids
        // breakaway, the assign can fail with `ERROR_ACCESS_DENIED`. On Windows 8+
        // jobs nest (the child joins our job *and* the outer one), so the common
        // case works; we do not set a breakaway flag (that would let children
        // escape our containment). On failure the suspended child is reaped (the
        // guard) and the error surfaced — we never leak an uncontained child.
        let ok = unsafe { AssignProcessToJobObject(self.handle, handle as HANDLE) };
        if ok == 0 {
            // The reaper kills the still-suspended child as `guard` drops.
            return Err(io::Error::last_os_error());
        }
        if let Some(mask) = opts.cpu_affinity
            && unsafe { SetProcessAffinityMask(handle as HANDLE, mask) } == 0
        {
            // Still suspended and owned by `guard`; a rejected mask cannot leak
            // or briefly run with the inherited affinity.
            return Err(io::Error::last_os_error());
        }
        // Contained — release the primary thread. A failure here would strand a
        // suspended-but-contained process; the reaper kills it as `guard` drops.
        resume_process_threads(pid)?;
        // Re-arm the kill-on-drop backstop now the child is contained: a prior
        // graceful_shutdown(escalate=false) latched skip_drop_kill to spare
        // survivors; a fresh member must not be spared by that stale latch on
        // Drop. Done after successful containment so a failed spawn leaves the
        // spared survivors alone.
        self.skip_drop_kill.clear();
        // Opt-in: record this direct child as a console-CTRL leader so a later
        // graceful_shutdown addresses it with GenerateConsoleCtrlEvent. Recorded
        // only after successful containment (so a failed spawn tracks nothing) and
        // only when the child was actually spawned into its own process group.
        if opts.windows_new_process_group {
            self.record_ctrl_break_leader(pid);
        }
        Ok(guard.disarm())
    }

    /// Complete containment for a raw ConPTY child while applying the same Job
    /// state disciplines as [`spawn`](Self::spawn): serialize against group
    /// suspend/resume walks, resume through the full suspend count, re-arm
    /// kill-on-close, and record an opt-in console process-group leader.
    #[cfg(feature = "pty")]
    pub(crate) fn contain_pty_child(
        &self,
        process: HANDLE,
        primary_thread: HANDLE,
        pid: u32,
        opts: &crate::sys::SpawnOptions,
    ) -> io::Result<()> {
        // Poisoning is unactionable here: the lock protects OS state that still
        // needs a deterministic assign → resume completion on the next attempt.
        let _suspend_guard = self
            .suspend_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: both handles come from the still-live `CreateProcessW` result;
        // the caller retains ownership and performs complete cleanup on error.
        if unsafe { AssignProcessToJobObject(self.handle, process) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if let Some(mask) = opts.cpu_affinity
            && unsafe { SetProcessAffinityMask(process, mask) } == 0
        {
            return Err(io::Error::last_os_error());
        }
        resume_thread_handle(primary_thread)?;

        self.skip_drop_kill.clear();
        if opts.windows_new_process_group {
            self.record_ctrl_break_leader(pid);
        }
        Ok(())
    }

    /// Record a direct child created as a console process-group leader, pruning
    /// exited entries while remaining safe if Windows recycled a pid.
    fn record_ctrl_break_leader(&self, pid: u32) {
        let mut leaders = self
            .ctrl_break_leaders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Reuse the signal path's recycle-safe membership check. A stale pid
        // recycled for this new member is retained and then deduplicated below.
        leaders.retain(|&leader| process_is_in_job(leader, self.handle));
        if !leaders.contains(&pid) {
            leaders.push(pid);
        }
    }

    /// Spawn `cmd` under a ConPTY pseudoconsole and assign the child to **this**
    /// Job Object, so the PTY child is contained identically to
    /// [`spawn`](Self::spawn). `env` is the child's resolved environment for the
    /// raw `CreateProcessW` path (which bypasses `std`'s env handling).
    #[cfg(feature = "pty")]
    pub(crate) fn spawn_pty(
        &self,
        cmd: &mut Command,
        opts: &crate::sys::SpawnOptions,
        env: Option<Vec<(std::ffi::OsString, std::ffi::OsString)>>,
    ) -> io::Result<crate::sys::pty::PtySpawn> {
        crate::sys::pty::spawn_pty(cmd, opts, env, self)
    }

    #[cfg(feature = "process-control")]
    pub(crate) fn adopt(&self, child: &Child) -> io::Result<()> {
        let handle = child
            .raw_handle()
            .ok_or_else(|| io::Error::other("child has no handle (already exited?)"))?;
        // SAFETY: the raw handle is valid while `child` is alive (borrowed here).
        let ok = unsafe { AssignProcessToJobObject(self.handle, handle as HANDLE) };
        if ok == 0 {
            let err = io::Error::last_os_error();
            // The assign fails for an already-terminated process. If the child has
            // in fact exited there is nothing to contain — return Ok (matching the
            // pgroup/cgroup backends); a genuine failure on a still-LIVE process
            // still propagates.
            if process_has_exited(handle as HANDLE) {
                return Ok(());
            }
            return Err(err);
        }
        // A new killable member joined the job — re-arm the kill-on-drop backstop
        // so a prior graceful_shutdown(escalate=false) latch doesn't spare it.
        self.skip_drop_kill.clear();
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

    /// A Job Object has no POSIX signals, but two portable soft triggers exist:
    /// `Kill` maps to the atomic job terminate; `Int`/`Term` get a best-effort
    /// soft close — `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)` to any console
    /// process-group leader (`windows_graceful_ctrl_break`) plus `WM_CLOSE` to any
    /// windowed member. This is the `signal`-verb analogue of the graceful-shutdown
    /// soft tier, but a one-shot broadcast: it TRIGGERS a clean exit without waiting
    /// or escalating. `Unsupported` is returned for `Int`/`Term` only when the group
    /// has NEITHER a CTRL-capable leader NOR a windowed member (nothing a soft close
    /// could reach); every other non-`Kill` signal stays unsupported so the caller
    /// never believes a reload/interrupt was delivered.
    #[cfg(feature = "process-control")]
    pub(crate) fn signal(&self, sig: Signal) -> io::Result<()> {
        match sig {
            Signal::Kill => self.kill_all(),
            Signal::Int | Signal::Term => {
                // Best-effort soft close: CTRL_BREAK to live console leaders plus
                // WM_CLOSE to windowed members. Neither waits — a `signal` is a
                // one-shot broadcast, not a teardown (contrast `graceful_shutdown`,
                // which then polls and escalates).
                let leaders = self
                    .ctrl_break_leaders
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                let signalled = ctrl_break_live_leaders(&leaders, self.handle);
                let closed = close_member_windows(self.handle);
                // Unsupported ONLY when there was nothing to soft-close: no live
                // console leader and no windowed member. A Job Object still has no
                // way to deliver a POSIX Int/Term to such a group.
                if signalled == 0 && closed == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!(
                            "signal({sig:?}): no console-CTRL or windowed member to soft-close"
                        ),
                    ));
                }
                Ok(())
            }
            other => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("signal({other:?})"),
            )),
        }
    }

    /// The reach of a soft `Int`/`Term` stop on this job right now — a
    /// side-effect-free capability probe answering the exact question
    /// `signal(Int/Term)` would act on, but **without** delivering anything.
    ///
    /// A Job Object has no POSIX signal, so a soft stop reaches only members it can
    /// *trigger*: a live recorded console-CTRL leader (opted in via
    /// `windows_new_process_group` / `windows_graceful_ctrl_break`) or any live
    /// member owning a top-level window (`WM_CLOSE`). Reports
    /// [`OptInMembers`](crate::SoftStopScope::OptInMembers) when at least one such
    /// member is live, else [`Unsupported`](crate::SoftStopScope::Unsupported) —
    /// matching where `signal(Int/Term)` returns `Ok` versus `Unsupported`.
    ///
    /// Read from the same live-membership primitives the delivery path uses
    /// (`ctrl_break_leader_is_live` — the shared recycle-safe `IsProcessInJob`
    /// guard — and the probe-mode `job_has_windowed_member`), so it never sends a
    /// `GenerateConsoleCtrlEvent`, never posts a `WM_CLOSE`, and never mutates the
    /// recorded-leader list (no pruning) — asking cannot change the answer to a
    /// later `signal`.
    #[cfg(feature = "process-control")]
    pub(crate) fn soft_stop_scope(&self) -> crate::SoftStopScope {
        let leaders = self
            .ctrl_break_leaders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let has_live_leader = has_live_ctrl_break_leader(&leaders, self.handle);
        if has_live_leader || job_has_windowed_member(self.handle) {
            crate::SoftStopScope::OptInMembers
        } else {
            crate::SoftStopScope::Unsupported
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

    /// The whole tree's members enriched with ppid / image name / start time.
    ///
    /// The member set is the same whole-tree pid list as [`members`](Self::members).
    /// Parent pid and image name come from a single system-wide `Toolhelp32`
    /// process snapshot; the start time (creation `FILETIME`) from a per-pid
    /// handle. A member the snapshot doesn't list exited between the job
    /// enumeration and the snapshot and is skipped (a vanished member, never a
    /// fabricated record). `Err` only if the job membership can't be read *or* the
    /// metadata snapshot can't be created (a total inability to read metadata,
    /// distinct from one pid vanishing).
    #[cfg(feature = "process-control")]
    pub(crate) fn members_info(&self) -> io::Result<Vec<MemberInfo>> {
        let pids = job_member_pids(self.handle)?;
        let meta = snapshot_process_metadata()?;
        let mut out = Vec::with_capacity(pids.len());
        for pid in pids {
            // A member absent from the snapshot exited between the job
            // enumeration and the snapshot — skip it (the documented race), never
            // fabricating ppid/exe for a pid we couldn't observe alive.
            let Some((ppid, exe)) = meta.get(&pid) else {
                continue;
            };
            // Start time via a per-pid handle. `None` if the process vanished in
            // this even-later window: the ppid/exe captured while it was alive
            // still stand, so the record is kept (only the finer start-time anchor
            // is missing), matching the honest-`Option` contract.
            let start_time = process_start_time(pid);
            out.push(MemberInfo::new(
                pid,
                Some(*ppid),
                Some(exe.clone()),
                start_time,
            ));
        }
        Ok(out)
    }

    /// Suspend or resume every thread of every process currently in the job.
    ///
    /// Best-effort, not atomic: the member list and the thread snapshot are
    /// taken once, so threads or processes created mid-walk are missed, and
    /// `SuspendThread`/`ResumeThread` maintain per-thread suspend *counts*
    /// (nested suspends need matching resumes). A thread that exits mid-walk is
    /// vacuously handled (not a failure); a genuine `SuspendThread`/
    /// `ResumeThread` failure on a still-open thread does not abort the walk and
    /// is reported after every member has been attempted.
    ///
    /// Recycle-safe (C13): the member list is captured before the thread
    /// snapshot, so a member could exit and its pid be reused by a foreign
    /// process in that gap. `suspend_or_resume_thread` re-verifies, per thread,
    /// that the live owner is *still a member of this job* (`IsProcessInJob`)
    /// before touching it, so a recycled pid can never divert a suspend/resume
    /// onto an unrelated process.
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
                && let Err(err) = suspend_or_resume_thread(
                    entry.th32ThreadID,
                    entry.th32OwnerProcessID,
                    self.handle,
                    suspend,
                )
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
        signal: i32,
        timeout: Duration,
        escalate: bool,
    ) -> io::Result<super::graceful::GracefulOutcome> {
        // Soft-shutdown tier: a Windows Job Object has no POSIX SIGTERM, but there
        // ARE two best-effort ways to *trigger* a clean exit before the atomic
        // kill —
        //   * a direct child spawned into its own console process group
        //     (`windows_graceful_ctrl_break`), addressable by
        //     `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)`; and
        //   * ANY live member that owns a top-level window (an Electron app, a
        //     desktop tool, a windowed service), addressable by `WM_CLOSE`.
        // When either exists, drive the SAME shared escalation loop the unix
        // backends use: soft-signal (CTRL_BREAK + WM_CLOSE), poll the job's
        // active-process count up to `timeout`, then `TerminateJobObject` survivors
        // (escalate) or spare them (!escalate). The driver owns the
        // `begin_shutdown`/`request` epoch handshake, so the re-arm race is handled
        // there, exactly as on unix. The WM_CLOSE broadcast itself is issued once,
        // inside the target's `signal_all`, so the loop's soft-signal step stays the
        // single source of the trigger.
        let leaders = self
            .ctrl_break_leaders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let has_live_leader = has_live_ctrl_break_leader(&leaders, self.handle);
        if has_live_leader || job_has_windowed_member(self.handle) {
            let target = SoftShutdownTarget { job: self, leaders };
            return super::graceful::run(&target, &self.skip_drop_kill, signal, timeout, escalate)
                .await;
        }

        // Default path — no console-CTRL leader and no windowed member, so nothing
        // can *trigger* a soft exit. A Job Object has no graceful tier: no Windows
        // equivalent of SIGTERM, and the kill is atomic. When `escalate=true`, kill
        // the tree immediately. When `escalate=false`, skip the kill and let
        // survivors run; `Drop` will clear `KILL_ON_JOB_CLOSE` before closing the
        // handle so the tree is not implicitly killed then either.
        //
        // The `timeout` is deliberately NOT used as a drain window (C6): without a
        // soft signal there is nothing to *trigger* a graceful exit, so polling for
        // a natural exit up to `timeout` would, for the common case of a child that
        // ignores the (absent) signal, only delay the inevitable kill by the whole
        // grace — a data-losing 30 s stall, not a graceful drain. Prompt hard-kill
        // at the deadline is the honest behavior; the grace/soft-signal tiers are
        // Unix-only (or the CTRL/WM_CLOSE paths above). Kill-on-drop and the current
        // timings are therefore unchanged for a windowless tree with no CTRL leader:
        // no extra wait is introduced here, exactly as before this WM_CLOSE tier.
        //
        // Report facts for the atomic branch, which bypasses the shared driver: no
        // soft-signal tier exists here (`Unsupported`), the tree was never given a
        // grace window to drain in, and the elapsed is just the synchronous
        // kill/spare below.
        let started = std::time::Instant::now();
        let members_before = job_active_count(self.handle);
        // Snapshot the re-arm generation up front — before the branch — so a
        // `spawn`/`adopt` that re-arms the backstop concurrently with this shutdown
        // wins over the (stale) `request` below. This body does not poll, but the
        // caller's task can migrate across its `.await` and a spawn/adopt on another
        // thread can still interleave between this snapshot and the request; keying
        // the spare to the epoch makes that concurrent re-arm win (the fresh child
        // keeps its kill-on-close backstop), matching the unix backends.
        let epoch = self.skip_drop_kill.begin_shutdown();
        // An already-empty tree "drained" trivially; otherwise the atomic branch
        // never drains softly (there is no soft trigger), so a non-empty tree is
        // either hard-killed (escalate) or spared (!escalate).
        let already_empty = members_before == Some(0);
        let (escalated, result) = if escalate {
            // The immediate kill IS the escalation — unless the tree was already
            // empty, in which case `kill_all` is a no-op and nothing was escalated.
            (!already_empty, self.kill_all())
        } else {
            // Mark Drop to preserve survivors; the latch makes the flag visible
            // whichever thread drops the `Job` (it may differ from the one that
            // ran graceful shutdown, e.g. after a task migrates across `.await`).
            // Keyed to `epoch`, so a concurrent spawn/adopt re-arm wins and this
            // spare no-ops — the fresh child is still killed on job-close.
            self.skip_drop_kill.request(epoch);
            (false, Ok(()))
        };
        let members_after = job_active_count(self.handle);
        result.map(|()| super::graceful::GracefulOutcome {
            soft: super::graceful::SoftDelivery::Unsupported,
            members_before,
            members_after,
            drained: already_empty,
            escalated,
            elapsed: started.elapsed(),
        })
    }

    /// Post-run evidence for the caps this Job Object carries: **`Unknown` on every
    /// capped axis** — a reasoned, measured negative result, deliberately *not*
    /// derived by analogy with the Linux cgroup backend, which has real counters.
    ///
    /// A Job Object simply does not keep a post-mortem record that any of the three
    /// caps this crate applies actually fired. Axis by axis, what was checked and
    /// what it turned out to be worth:
    ///
    /// - **process count** (`ActiveProcessLimit`). The one plausible counter is
    ///   `JOBOBJECT_BASIC_ACCOUNTING_INFORMATION`'s `TotalTerminatedProcesses`,
    ///   documented as "the total number of processes terminated because of a limit
    ///   violation". **Measured on Windows 11 (26200): it stays 0** across both ways
    ///   this cap is actually violated — a fresh child whose
    ///   `AssignProcessToJobObject` is refused because the job is full, and a job
    ///   *member* whose own `CreateProcess` is refused (`ERROR_NOT_ENOUGH_QUOTA`)
    ///   for the same reason. Neither process is ever an accounted member, so
    ///   nothing is counted as terminated; in practice that field tracks the
    ///   end-of-job-time terminations (`JOB_OBJECT_LIMIT_JOB_TIME` /
    ///   `_PROCESS_TIME`), limits this crate never sets. Reporting `NotTripped` off
    ///   a counter that is provably 0 *after a violation that really happened* would
    ///   be a fabricated "no" — the worst failure mode for this report — so the
    ///   honest answer is `Unknown`. (The direct spawn case is not silent either
    ///   way: the caller already gets a spawn error there.)
    /// - **memory** (`JOB_OBJECT_LIMIT_JOB_MEMORY`). This cap does not kill: a
    ///   commit that would exceed it simply *fails* in the child, which then dies
    ///   (or not) by its own error handling — after the fact indistinguishable from
    ///   any other allocation failure. The OS reports it only as a live
    ///   `JOB_OBJECT_MSG_JOB_MEMORY_LIMIT` message on an IO completion port
    ///   associated with the job: a *push* notification that must be drained while
    ///   the tree runs, i.e. a completion port plus a drain thread on every group
    ///   for its whole life — new machinery on the containment object purely for
    ///   reporting, and a lifecycle change to the very object whose teardown
    ///   guarantee must not move. Deliberately not taken. (`PeakJobMemoryUsed` is
    ///   **not** a substitute: a high-water mark landing at or near the cap is an
    ///   inference about a boundary, not a record that the cap fired — exactly the
    ///   guess this report refuses to make. It is already exposed, as a
    ///   measurement, by `stats()`'s `peak_memory_bytes`.)
    /// - **CPU** (`JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP`). Nothing at all: the hard
    ///   cap throttles silently and Windows keeps no throttle counter for it (no
    ///   analogue of cgroup `cpu.stat`'s `nr_throttled`). The
    ///   `JOB_OBJECT_MSG_NOTIFICATION_LIMIT` message and
    ///   `JobObjectLimitViolationInformation` belong to the separate *notification*
    ///   limit API — soft limits this crate does not set, and which would again need
    ///   a completion port to mean anything.
    ///
    /// An axis that never carried a cap is `NotTripped` — nothing was capped, so
    /// nothing could fire — which needs no query at all. So this reads nothing from
    /// the OS in any case: no `TerminateJobObject`, no `SetInformationJobObject`,
    /// not even a query. Teardown and kill-on-drop are untouched.
    #[cfg(feature = "limits")]
    pub(crate) fn limit_evidence(&self, capped: CappedAxes) -> LimitEvidence {
        // A capped axis has no post-mortem evidence on this mechanism (see above) —
        // `Unknown`, never a guessed "no". An uncapped one had no cap to fire.
        let verdict = |kind: LimitKind| {
            if capped.has(kind) {
                LimitVerdict::Unknown
            } else {
                LimitVerdict::NotTripped
            }
        };
        LimitEvidence::new(
            verdict(LimitKind::Memory),
            verdict(LimitKind::Processes),
            verdict(LimitKind::Cpu),
        )
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

/// Read-only prediction of the [`Mechanism`] a fresh [`Job`] would use on this host,
/// computed **without creating a Job Object or spawning anything** — always a
/// Windows [`Mechanism::JobObject`], so there is nothing to probe and nothing is
/// created. Mirrors [`Job::mechanism`]; backs the public `host_containment()` query.
pub(crate) fn detect_mechanism() -> Mechanism {
    Mechanism::JobObject
}

/// The Job-backed [`GracefulTarget`](crate::sys::graceful::GracefulTarget) for the
/// Windows soft-shutdown tier. It plugs the Job Object into the *same*
/// signal → poll → escalate loop the unix backends drive
/// ([`graceful::run`](crate::sys::graceful::run)):
///
/// - `signal_all` fires both best-effort soft triggers —
///   `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` to each recorded console
///   process-group leader, and `WM_CLOSE` to every top-level window owned by a
///   live job member (the GUI-graceful path);
/// - `is_drained` reads the job's active-process count;
/// - `hard_kill` is `TerminateJobObject` (the escalation fallback).
///
/// It borrows the `Job` (already `Sync`) and a snapshot of the leader pids, so it
/// is automatically `Send + Sync` — no raw handle is carried across the driver's
/// `.await`s.
struct SoftShutdownTarget<'a> {
    job: &'a Job,
    /// Snapshot of the console process-group leader pids at shutdown time.
    leaders: Vec<u32>,
}

impl super::graceful::GracefulTarget for SoftShutdownTarget<'_> {
    fn signal_all(&self, _signal: i32) -> super::graceful::SoftDelivery {
        // Windows delivers a console CTRL_BREAK / a window WM_CLOSE, not a POSIX
        // signal — the raw `signal` number (SIGTERM/`timeout_signal`) is meaningless
        // here and ignored. Both are best-effort soft triggers: a console-group
        // leader gets CTRL_BREAK, a windowed member gets WM_CLOSE. Whatever ignores
        // its trigger rides the grace to the `hard_kill` (TerminateJobObject)
        // fallback. Both fire (no short-circuit); the counts they return classify
        // the delivery for the report — at least one live target reached is `Sent`,
        // none reached (the leaders/windows vanished since the branch check) is
        // `Failed`.
        let ctrl = ctrl_break_live_leaders(&self.leaders, self.job.handle);
        let windows = close_member_windows(self.job.handle);
        if ctrl + windows > 0 {
            super::graceful::SoftDelivery::Sent
        } else {
            super::graceful::SoftDelivery::Failed
        }
    }

    fn is_drained(&self) -> bool {
        job_is_drained(self.job.handle)
    }

    fn alive_count(&self) -> Option<usize> {
        // The whole tree's live members (the job's active-process count), matching
        // `members()`; `None` if the accounting query fails, the same fail-safe
        // `is_drained` applies (there mapped to "not drained").
        job_active_count(self.job.handle)
    }

    fn hard_kill(&self) -> io::Result<()> {
        self.job.kill_all()
    }
}

/// Send `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid)` to each recorded console
/// process-group leader that is STILL a live member of `job`, returning the number
/// actually signalled.
///
/// CTRL_BREAK is chosen over CTRL_C deliberately: a process can disable CTRL_C for
/// its group (and `CREATE_NEW_PROCESS_GROUP` does so by default), but CTRL_BREAK is
/// always deliverable. Best-effort — a delivery failure is swallowed; the count is
/// used only to tell whether the group HAD a CTRL-capable member (so a bare
/// `signal(Int/Term)` can report `Unsupported` when it did not).
fn ctrl_break_live_leaders(leaders: &[u32], job: HANDLE) -> usize {
    let mut signalled = 0;
    for &pid in leaders {
        // Skip a pid that is not a live member of THIS job (see
        // `ctrl_break_leader_is_live`): a zero pid would target EVERY process
        // sharing our console, and a gone/recycled leader could divert the event
        // onto a stranger's group.
        if !ctrl_break_leader_is_live(pid, job) {
            continue;
        }
        // SAFETY: a console control event to a process group id; a delivery failure
        // (no shared console, the leader just exited) is swallowed — the poll
        // observes the drain, and the `hard_kill` fallback covers a child that never
        // received it.
        unsafe {
            GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid);
        }
        signalled += 1;
    }
    signalled
}

/// Whether a recorded console-CTRL leader `pid` is STILL a live member of `job`
/// — the recycle-safe predicate the soft-stop paths share so "which leader
/// counts as reachable" never drifts between the side-effect-free capability
/// probe (`Job::soft_stop_scope`) and the actual
/// `GenerateConsoleCtrlEvent` delivery ([`ctrl_break_live_leaders`]).
///
/// A **zero** pid reads false: it is never a real recorded leader, and
/// `GenerateConsoleCtrlEvent(_, 0)` would otherwise target every process sharing
/// this console (including us). A **gone / recycled / access-denied** pid also
/// reads false — `IsProcessInJob` fails safe (a non-member reads "not a member")
/// — so neither path is ever diverted onto an unrelated process's group (mirrors
/// the suspend/resume C13 recycle discipline).
fn ctrl_break_leader_is_live(pid: u32, job: HANDLE) -> bool {
    pid != 0 && process_is_in_job(pid, job)
}

/// Whether a recorded-leader snapshot contains at least one live member of this
/// job. Capability reporting and graceful-teardown branching share this helper so
/// they cannot drift on which stale/recycled pids count as a reachable soft tier.
fn has_live_ctrl_break_leader(leaders: &[u32], job: HANDLE) -> bool {
    leaders
        .iter()
        .any(|&pid| ctrl_break_leader_is_live(pid, job))
}

/// Drives the [`EnumWindows`] top-level-window walk for the GUI-graceful tier: the
/// job whose members' windows are the target, whether to actually POST `WM_CLOSE`
/// (vs merely detect one), and a running count of matched member windows.
struct MemberWindowScan {
    job: HANDLE,
    /// `true`: post `WM_CLOSE` to each matched member window. `false`: only count
    /// (the side-effect-free "does a windowed member exist?" probe).
    close: bool,
    /// Number of top-level windows found owned by a live member of `job`.
    matched: usize,
}

/// The [`EnumWindows`] callback: for one top-level window, post `WM_CLOSE` (or, in
/// probe mode, just count) iff its owning process is a live member of the job
/// carried in `lparam`. Always returns `TRUE` to visit every top-level window.
///
/// SAFETY: registered only by [`scan_member_windows`], which passes a valid
/// `&mut MemberWindowScan` as `lparam` and drives the enumeration synchronously, so
/// the pointer is live for every callback.
unsafe extern "system" fn scan_member_window(hwnd: HWND, lparam: LPARAM) -> BOOL {
    // SAFETY: `lparam` is the `&mut MemberWindowScan` pointer `scan_member_windows`
    // handed to `EnumWindows`; it stays borrowed for the whole synchronous
    // enumeration, so the reference is valid for every callback.
    let ctx = unsafe { &mut *(lparam as *mut MemberWindowScan) };
    let mut pid: u32 = 0;
    // SAFETY: `hwnd` is a valid top-level window handle supplied by `EnumWindows`;
    // `pid` is an owned out-param.
    unsafe { GetWindowThreadProcessId(hwnd, &mut pid) };
    // Recycle-safe, mirroring the CTRL_BREAK / suspend-resume C13 discipline: only
    // touch a window whose owner is STILL a live member of THIS job. A window whose
    // owner exited — its pid perhaps recycled by a stranger sharing our desktop —
    // fails `process_is_in_job`, so a `WM_CLOSE` never lands on an unrelated app's
    // window.
    if pid != 0 && process_is_in_job(pid, ctx.job) {
        ctx.matched += 1;
        if ctx.close {
            // SAFETY: POST (never Send) a close request to a valid window, so a hung
            // window cannot block us. A delivery failure is swallowed — the grace
            // poll observes the drain and the `TerminateJobObject` fallback covers a
            // window that ignores the request.
            unsafe {
                PostMessageW(hwnd, WM_CLOSE, 0, 0);
            }
        }
    }
    // Continue the enumeration (TRUE) to the next top-level window.
    1
}

/// Walk every top-level window on this desktop, applying [`scan_member_window`] to
/// each. Returns the count of windows owned by a live member of `job`; in
/// `close = true` mode each such window is also sent `WM_CLOSE`.
fn scan_member_windows(job: HANDLE, close: bool) -> usize {
    let mut ctx = MemberWindowScan {
        job,
        close,
        matched: 0,
    };
    // SAFETY: `scan_member_window` is a valid `WNDENUMPROC`; `&mut ctx` is passed as
    // the callback `lparam` and stays borrowed for the whole synchronous call. A 0
    // return from `EnumWindows` (no windows, or an internal stop) is not an error
    // here — `ctx.matched` is authoritative.
    unsafe {
        EnumWindows(
            Some(scan_member_window),
            std::ptr::from_mut(&mut ctx) as LPARAM,
        );
    }
    ctx.matched
}

/// Post `WM_CLOSE` to every top-level window owned by a live member of `job`,
/// returning the number of windows messaged.
///
/// Best-effort GUI-graceful trigger: a windowed child (Electron app, desktop tool,
/// windowed service) receives no console CTRL event, so `WM_CLOSE` is the only soft
/// "please close" it can act on before the `TerminateJobObject` fallback.
/// `PostMessageW` (never `SendMessageW`) is used so a hung window can never block
/// teardown. A return of 0 means the job has no windowed member right now — the
/// caller keeps the prompt hard-kill behavior (no grace wait is introduced for a
/// windowless tree).
fn close_member_windows(job: HANDLE) -> usize {
    scan_member_windows(job, true)
}

/// Whether any top-level window is owned by a live member of `job` — the
/// side-effect-free probe [`Job::graceful_shutdown`](Job::graceful_shutdown) uses to
/// decide whether a GUI-graceful drain is even possible (and thus whether to drive
/// the grace loop) before its target's `signal_all` posts the actual `WM_CLOSE`.
fn job_has_windowed_member(job: HANDLE) -> bool {
    scan_member_windows(job, false) > 0
}

/// The job's live active-process count (the whole tree), or `None` if the
/// accounting query fails (a torn-down handle, a transient error). The single
/// membership primitive behind both [`job_is_drained`] (drained ⟺ `Some(0)`) and
/// the graceful teardown report's before/after member counts — so the drain check
/// and the reported counts always read the same `ActiveProcesses` field.
fn job_active_count(handle: HANDLE) -> Option<usize> {
    let mut acct: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: out param matches the accounting info class and its size.
    let ok = unsafe {
        QueryInformationJobObject(
            handle,
            JobObjectBasicAccountingInformation,
            std::ptr::from_mut(&mut acct).cast(),
            std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(acct.ActiveProcesses as usize)
}

/// Whether the job has fully drained — no process is still active in it.
///
/// Best-effort: a failed query (a torn-down handle, a transient error) reports
/// "not drained" so the driver keeps waiting and then takes its escalation
/// (`TerminateJobObject`) / spare decision at the deadline, never a premature
/// "drained" that would skip the fallback kill.
fn job_is_drained(handle: HANDLE) -> bool {
    job_active_count(handle) == Some(0)
}

/// Whether the process behind `handle` has already exited —
/// `GetExitCodeProcess` reports an exit code other than `STILL_ACTIVE` (259).
/// A *live* process always reports `STILL_ACTIVE`, so this never false-positives
/// a live child as exited. The only ambiguity is a child that genuinely exited
/// with code 259: it reads as "still active", so `adopt` surfaces the assign
/// error for it rather than the nothing-to-contain `Ok` — an acceptable rarity.
#[cfg(feature = "process-control")]
fn process_has_exited(handle: HANDLE) -> bool {
    const STILL_ACTIVE: u32 = 259;
    let mut code: u32 = 0;
    // SAFETY: `handle` is a valid process handle borrowed from the live `Child`.
    let ok = unsafe { GetExitCodeProcess(handle, &mut code) };
    ok != 0 && code != STILL_ACTIVE
}

/// Reaps a freshly-spawned, not-yet-contained child if [`Job::spawn`] unwinds
/// (an early `Err` or a panic) before the child is assigned to the job. Until
/// containment succeeds the child — created `CREATE_SUSPENDED` — is reachable by
/// nothing that would reap it, so dropping it un-disarmed would leak a suspended
/// orphan. [`disarm`](Self::disarm) hands the child back once it is contained,
/// after which the job's kill-on-close owns teardown.
struct UncontainedChildGuard {
    // `None` only after `disarm` has taken the child.
    child: Option<Child>,
}

impl UncontainedChildGuard {
    fn arm(child: Child) -> Self {
        Self { child: Some(child) }
    }

    /// Borrow the guarded child (present from `arm` until `disarm`). Used to read
    /// the child's `id()`/`raw_handle()` while the reaper is armed.
    fn child(&self) -> &Child {
        self.child
            .as_ref()
            .expect("the guarded child is present until disarm")
    }

    /// Containment succeeded: stop guarding and return the child unharmed.
    fn disarm(mut self) -> Child {
        self.child
            .take()
            .expect("the guarded child is taken exactly once")
    }
}

impl Drop for UncontainedChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Best-effort: `start_kill` issues `TerminateProcess` on the
            // suspended child; dropping the `Child` then closes its handle. This
            // is the same kill the explicit error paths used to do inline, now
            // also covering an unwind.
            let _ = child.start_kill();
        }
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
    let result = resume_thread_handle(thread);
    // SAFETY: handle came from OpenThread; closed exactly once.
    unsafe { CloseHandle(thread) };
    result
}

/// Resume a still-open thread handle until its suspend count reaches zero.
/// Shared by ordinary spawn's snapshot walk and ConPTY's direct primary-thread
/// handle so their nested-suspend behavior cannot drift apart.
fn resume_thread_handle(thread: HANDLE) -> io::Result<()> {
    loop {
        // SAFETY: the caller supplies a still-open thread handle with resume
        // rights; `u32::MAX` is the documented failure sentinel.
        let previous = unsafe { ResumeThread(thread) };
        if previous == u32::MAX {
            return Err(io::Error::last_os_error());
        }
        if previous <= 1 {
            return Ok(()); // previous 1 → now running; 0 → already running
        }
    }
}

/// Suspend (increment) or resume (decrement) a single thread's suspend count.
///
/// `job` is the job whose members are being walked; it backs the pid-recycle
/// membership re-check (C13) below.
#[cfg(feature = "process-control")]
fn suspend_or_resume_thread(
    tid: u32,
    expected_pid: u32,
    job: HANDLE,
    suspend: bool,
) -> io::Result<()> {
    // Also request QUERY access so we can confirm the thread's owner below (C11).
    // SAFETY: opens the thread by id; returns null on failure (e.g. exited).
    let thread = unsafe {
        OpenThread(
            THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
            0,
            tid,
        )
    };
    if thread.is_null() {
        let err = io::Error::last_os_error();
        // A STALE tid — a thread that exited between the system-wide snapshot and
        // this open — fails `ERROR_INVALID_PARAMETER` and is *vacuously*
        // suspended/resumed, so swallow it: one churning thread must not fail the
        // whole job suspend/resume. ANY OTHER open failure (e.g.
        // `ERROR_ACCESS_DENIED` on a live but protected thread) is genuine and IS
        // reported — a live thread is never silently left suspended.
        if err.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
            return Ok(());
        }
        return Err(err);
    }
    // C11: the system-wide thread snapshot named this tid as owned by a member
    // process, but a tid can be **recycled** between the snapshot and this open —
    // to a thread of an entirely different process. Verify the live owner before
    // touching it, so a suspend/resume never lands on a foreign process's thread.
    // SAFETY: valid thread handle from OpenThread; returns 0 on failure.
    let owner = unsafe { GetProcessIdOfThread(thread) };
    if owner != expected_pid {
        // Recycled (or unqueryable) — not our member's thread; leave it alone.
        // SAFETY: handle came from OpenThread; closed exactly once.
        unsafe { CloseHandle(thread) };
        return Ok(());
    }
    // C13: the owner check above only proves the thread belongs to `expected_pid`
    // *now* — but `expected_pid` itself may be a **recycled** pid. Between
    // `job_member_pids` (member snapshot) and the thread snapshot, a member
    // (typically a handle-less grandchild) can exit and its pid be reused by a
    // FOREIGN process X; X's threads then surface under a pid still in `members`,
    // and the C11 owner check passes because X genuinely owns them — so a bare
    // owner check would `SuspendThread` all of X's threads, freezing (and later
    // decrementing the suspend count of) an unrelated process. Close that window
    // by confirming the owner is STILL a member of THIS job before touching the
    // thread. Fail-safe: any failure to open the process or query membership is
    // treated as "not our member", so an uncertain result never suspends/resumes
    // a foreign thread.
    if !process_is_in_job(owner, job) {
        // SAFETY: handle came from OpenThread; closed exactly once.
        unsafe { CloseHandle(thread) };
        return Ok(());
    }
    // SAFETY: valid thread handle; both calls signal failure with `u32::MAX`.
    let prev = unsafe {
        if suspend {
            SuspendThread(thread)
        } else {
            ResumeThread(thread)
        }
    };
    // Capture the failure BEFORE `CloseHandle`, which can overwrite the
    // thread-local last-error.
    let err = (prev == u32::MAX).then(io::Error::last_os_error);
    // SAFETY: handle came from OpenThread; closed exactly once.
    unsafe { CloseHandle(thread) };
    match err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Whether the process named by `pid` is currently a member of `job` — the
/// pid-recycle guard (C13) shared by the process-control suspend/resume walk and
/// the opt-in console-CTRL teardown's per-leader signal check.
///
/// Fail-safe by construction: a failure to open the process (gone, denied) or to
/// query membership yields `false`, i.e. "treat as NOT our member". A false
/// negative merely skips a suspend/resume for one thread, or a CTRL_BREAK for one
/// leader (both best-effort, with a `TerminateJobObject` backstop), whereas a
/// false positive would freeze a foreign process or divert a console event onto a
/// stranger's group — so uncertainty must resolve to "leave it alone".
fn process_is_in_job(pid: u32, job: HANDLE) -> bool {
    // Least-privilege: `IsProcessInJob` only needs query access.
    // SAFETY: opens the process by id; returns null on failure (e.g. exited).
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return false;
    }
    let mut in_job: i32 = 0;
    // SAFETY: `handle` is a valid process handle from OpenProcess just above,
    // `job` is our live job handle, and `in_job` is an owned out-param. Returns 0
    // on failure, leaving `in_job` untouched (still 0 → treated as not-a-member).
    let ok = unsafe { IsProcessInJob(handle, job, &mut in_job) };
    // SAFETY: handle came from OpenProcess; closed exactly once.
    unsafe { CloseHandle(handle) };
    ok != 0 && in_job != 0
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
        // SAFETY: a successful query wrote `n` pids starting at `ProcessIdList[0]`.
        // `addr_of!` avoids creating a reference to the `[ULONG_PTR;1]` field
        // (which would have incorrect provenance for the out-of-bounds elements),
        // taking the raw address of its first element directly instead.
        // `ProcessIdList[0]` is always within the struct definition (the field
        // is declared as a 1-element array), so `addr_of!` is valid even when
        // `n == 0`; `from_raw_parts(ptr, 0)` is a zero-length slice, which is
        // always sound for any non-null aligned pointer.
        let ids =
            unsafe { std::slice::from_raw_parts(std::ptr::addr_of!((*list).ProcessIdList[0]), n) };
        return Ok(ids.iter().map(|&pid| pid as u32).collect());
    }
}

/// A system-wide `pid -> (ppid, image name)` map from one `Toolhelp32` process
/// snapshot — the source of [`members_info`](Job::members_info)'s parent-pid and
/// executable-name fields (neither is obtainable per-pid without ntdll, so one
/// snapshot is both cheaper and the only way to get the parent pid).
///
/// Best-effort, like the thread walk in [`for_each_member_thread`](Job::for_each_member_thread):
/// a process created or reaped during the walk may be briefly present or missing.
/// `Err` only when the snapshot itself can't be created — a genuine inability to
/// read any metadata, which the caller surfaces rather than reporting a populated
/// job as empty.
#[cfg(feature = "process-control")]
fn snapshot_process_metadata() -> io::Result<std::collections::HashMap<u32, (u32, String)>> {
    // SAFETY: TH32CS_SNAPPROCESS snapshots all processes system-wide; returns
    // INVALID_HANDLE_VALUE on failure.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }

    let mut entry: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;

    let mut map = std::collections::HashMap::new();
    // SAFETY: valid snapshot; `entry` is sized via its `dwSize` field.
    let mut ok = unsafe { Process32FirstW(snapshot, &mut entry) };
    while ok != 0 {
        // `szExeFile` is a NUL-terminated UTF-16 array; decode up to the NUL.
        let len = entry
            .szExeFile
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(entry.szExeFile.len());
        let exe = String::from_utf16_lossy(&entry.szExeFile[..len]);
        map.insert(entry.th32ProcessID, (entry.th32ParentProcessID, exe));
        // SAFETY: same valid snapshot and entry.
        ok = unsafe { Process32NextW(snapshot, &mut entry) };
    }
    // SAFETY: handle came from CreateToolhelp32Snapshot; closed exactly once.
    unsafe { CloseHandle(snapshot) };
    Ok(map)
}

/// The process-creation `FILETIME` of `pid` as its raw
/// [`MemberInfo`] start-time token (100-ns units since
/// 1601-01-01 UTC), or `None` if the process is gone / unqueryable. Fixed at spawn
/// and never reused within a boot, so it tells a recycled pid apart from the
/// original. (The `stats`-gated [`process_identity`] wraps the same read in a
/// `ProcIdentity`; this returns the bare token for the `process-control` snapshot,
/// which has no `stats` dependency.)
#[cfg(feature = "process-control")]
fn process_start_time(pid: u32) -> Option<u64> {
    // SAFETY: limited-information access; returns null on failure (e.g. gone).
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return None;
    }
    let units = creation_time_units(handle);
    // SAFETY: handle came from OpenProcess and is closed exactly once.
    unsafe { CloseHandle(handle) };
    units
}

/// Read a live process's creation `FILETIME` as its raw 100-ns identity token
/// from an already-open query-limited `handle`. `None` if `GetProcessTimes` fails.
///
/// The single `GetProcessTimes` + [`filetime_units`] decode shared by
/// [`process_start_time`] (which opens a fresh per-pid handle) and [`process_info`]
/// (which reuses the handle it already opened as its existence oracle, avoiding a
/// second `OpenProcess` and the recycle window between two opens), so the two can
/// never decode the creation time differently.
#[cfg(feature = "process-control")]
fn creation_time_units(handle: HANDLE) -> Option<u64> {
    let mut creation = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let mut exit = creation;
    let mut kernel = creation;
    let mut user = creation;
    // SAFETY: valid handle; all four out params are owned locals.
    let ok = unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
    (ok != 0).then(|| filetime_units(creation))
}

/// Identity + best-effort metadata for an **arbitrary** pid — the Windows backend
/// of the standalone [`process_info`](crate::process_info) query.
///
/// `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)` is the existence-and-permission
/// oracle: it is the least-privilege query right, grantable across sessions and
/// integrity levels for ordinary processes, so a **null** handle means either the
/// pid does not exist (`ERROR_INVALID_PARAMETER` → `Ok(None)`, an honest negative)
/// or the caller may not query it — a protected / higher-integrity process such as
/// an anti-malware PPL or the `System` process (any other failure, notably
/// `ERROR_ACCESS_DENIED` → `Err`, never a false "dead"). A handle in hand means the
/// process exists and is queryable; its creation `FILETIME` start-time token is
/// read (via [`creation_time_units`]) while the handle is still held.
///
/// Parent pid and image name then come from one system-wide `Toolhelp32` snapshot
/// (the same source [`members_info`](Job::members_info) uses — no per-pid Win32 API
/// yields the parent pid without ntdll); a pid absent from that even-later snapshot
/// vanished in the window and keeps those two fields `None`, while the start time —
/// read above while the process was demonstrably alive — still stands (the honest
/// per-field `Option` contract).
#[cfg(feature = "process-control")]
pub(crate) fn process_info(pid: u32) -> io::Result<Option<MemberInfo>> {
    // SAFETY: opens by pid with the narrowest query right; null on failure.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        let err = io::Error::last_os_error();
        // `ERROR_INVALID_PARAMETER` is the sole "no such pid" answer — a negative
        // result, not an error. Every other failure (`ERROR_ACCESS_DENIED` on a
        // protected process, …) leaves existence undetermined, so it surfaces as
        // `Err` and is never read as "dead".
        if err.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
            return Ok(None);
        }
        return Err(err);
    }
    let start_time = creation_time_units(handle);
    // SAFETY: handle came from OpenProcess and is closed exactly once.
    unsafe { CloseHandle(handle) };
    // A member absent from the snapshot exited in this later window — keep the
    // record (start time above still stands), with ppid/exe honestly `None`.
    let (ppid, exe) = match snapshot_process_metadata()?.get(&pid) {
        Some((ppid, exe)) => (Some(*ppid), Some(exe.clone())),
        None => (None, None),
    };
    Ok(Some(MemberInfo::new(pid, ppid, exe, start_time)))
}

/// A FILETIME as its raw 64-bit 100-ns unit count (high/low halves combined).
/// The process-creation FILETIME serves as the [`ProcIdentity`] anchor (the
/// `stats` metrics gate) and as the [`MemberInfo`] start-time
/// token (the `process-control` snapshot), compared directly in these units;
/// [`filetime_nanos`] scales the CPU-time FILETIMEs to ns.
#[cfg(any(feature = "stats", feature = "process-control"))]
fn filetime_units(ft: FILETIME) -> u64 {
    ((ft.dwHighDateTime as u64) << 32) | ft.dwLowDateTime as u64
}

/// Combine a FILETIME (100-ns units) into nanoseconds.
#[cfg(feature = "stats")]
fn filetime_nanos(ft: FILETIME) -> u64 {
    filetime_units(ft).saturating_mul(100)
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

/// The process-creation `FILETIME` of the process at `pid`, as its raw
/// [`ProcIdentity`] token, or `None` if the process is gone / unqueryable. The
/// creation time is fixed at spawn and never reused within a boot, so it tells a
/// recycled pid apart from the original process (the Windows analogue of Linux's
/// `/proc/<pid>/stat` starttime).
#[cfg(feature = "stats")]
pub(crate) fn process_identity(pid: u32) -> Option<ProcIdentity> {
    // SAFETY: limited-information access; returns null on failure (e.g. gone).
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return None;
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
    // SAFETY: handle came from OpenProcess and is closed exactly once.
    unsafe { CloseHandle(handle) };
    (ok != 0).then(|| ProcIdentity::from_raw(filetime_units(creation)))
}

#[cfg(feature = "stats")]
pub(crate) fn process_metrics(pid: u32, expected: Option<ProcIdentity>) -> ProcMetrics {
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

    // Identity gate. `OpenProcess(pid)` resolves the number to *whatever process
    // holds it now* — possibly one that recycled it after our child was reaped —
    // and the handle then pins that process. Comparing the pinned process's
    // creation time (read via the same handle) against the captured identity proves
    // it is our process before we trust ANY reading from this handle, memory
    // included. If the times read failed we can't verify identity, so when one was
    // demanded that counts as a mismatch: return defaults and touch nothing else.
    if let Some(expected) = expected {
        let confirmed = ok != 0 && filetime_units(creation) == expected.raw();
        if !confirmed {
            // SAFETY: handle came from OpenProcess and is closed exactly once.
            unsafe { CloseHandle(handle) };
            return ProcMetrics::default();
        }
    }

    if ok != 0 {
        metrics.cpu_time = Some(Duration::from_nanos(
            filetime_nanos(kernel).saturating_add(filetime_nanos(user)),
        ));
    }

    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
    // SAFETY: valid handle; `counters` sized via its `cb` field. Reading through the
    // same identity-confirmed handle keeps the memory figure bound to our process,
    // never a recycled stranger's.
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
        if self.skip_drop_kill.is_set() {
            // Clear KILL_ON_JOB_CLOSE so closing the handle does not kill the tree.
            // `SetInformationJobObject` with `JobObjectExtendedLimitInformation`
            // *replaces* the entire extended-limit structure, so a zeroed struct
            // sets `LimitFlags = 0`, dropping `KILL_ON_JOB_CLOSE` and the
            // memory/active-process caps. Intentional — this path only runs under
            // `escalate=false`, so orphaning survivors uncapped is the desired
            // outcome and the caps are no longer meaningful.
            let info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            // Best-effort: if clearing fails the handle close will still kill —
            // a safe fallback (unexpected kill is better than orphaning ambiguity).
            let _ = unsafe {
                SetInformationJobObject(
                    self.handle,
                    JobObjectExtendedLimitInformation,
                    std::ptr::from_ref(&info).cast(),
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            // The CPU hard cap lives in a SEPARATE info class, so zeroing the
            // extended-limit struct above does NOT lift it. Clear it too (zeroed
            // `ControlFlags` = disabled) or orphaned survivors stay CPU-throttled
            // forever, inconsistent with the memory/process caps just dropped.
            // Harmless when no CPU cap was set.
            #[cfg(feature = "limits")]
            {
                let cpu: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION = unsafe { std::mem::zeroed() };
                let _ = unsafe {
                    SetInformationJobObject(
                        self.handle,
                        JobObjectCpuRateControlInformation,
                        std::ptr::from_ref(&cpu).cast(),
                        std::mem::size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>() as u32,
                    )
                };
            }
        }
        // Closing the last handle triggers KILL_ON_JOB_CLOSE → the tree is reaped
        // (unless cleared above). SAFETY: handle came from CreateJobObjectW and is
        // closed exactly once.
        unsafe { CloseHandle(self.handle) };
    }
}

#[cfg(all(test, feature = "process-control"))]
mod thread_tests {
    use super::{process_is_in_job, suspend_or_resume_thread};
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::JobObjects::CreateJobObjectW;
    use windows_sys::Win32::System::Threading::GetCurrentProcessId;

    /// A stale/invalid tid — a thread that exited between the system-wide
    /// snapshot and the `OpenThread` — fails `ERROR_INVALID_PARAMETER` and is
    /// *vacuously* suspended/resumed, not a failure (a single churning thread must
    /// not fail the whole job suspend). `tid = 1` is not a valid thread id (the
    /// kernel allocates thread/process ids as multiples of 4, and 0 is reserved),
    /// so `OpenThread` deterministically fails with `ERROR_INVALID_PARAMETER` and
    /// the fix returns `Ok` — and it can never open or suspend a real thread.
    #[test]
    fn suspend_or_resume_a_stale_tid_is_ok() {
        // `expected_pid`/`job` are irrelevant here: `tid = 1` fails `OpenThread`
        // before the C11 owner check or the C13 membership check ever runs, so a
        // null job handle is never dereferenced.
        let job = std::ptr::null_mut();
        assert!(suspend_or_resume_thread(1, 0, job, true).is_ok());
        assert!(suspend_or_resume_thread(1, 0, job, false).is_ok());
    }

    /// The C13 pid-recycle guard: a process that is NOT a member of *the* job in
    /// question reads as a non-member, so a suspend/resume is skipped. A freshly
    /// created job has no members, so the current process (never assigned to it)
    /// must fail the check — the exact outcome that spares a foreign process whose
    /// pid recycled into a stale member set. Also covers the fail-safe leg: a pid
    /// that cannot be opened yields `false` rather than a spurious "member".
    #[test]
    fn non_member_and_unopenable_pids_are_rejected() {
        // SAFETY: null name/attributes request an unnamed job with defaults;
        // returns null only on failure.
        let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        assert!(!job.is_null(), "failed to create a test job object");

        // The current process is openable but was never assigned to `job`, so
        // `IsProcessInJob` reports it is not a member of THIS job.
        // SAFETY: a plain read of our own pid.
        let me = unsafe { GetCurrentProcessId() };
        assert!(
            !process_is_in_job(me, job),
            "a process not assigned to this job must not read as a member"
        );

        // Fail-safe: `1` is not a valid pid (ids are multiples of 4), so
        // `OpenProcess` fails and the guard returns false — never a stray suspend.
        assert!(
            !process_is_in_job(1, job),
            "an unopenable pid must be treated as a non-member"
        );

        // SAFETY: handle came from CreateJobObjectW; closed exactly once.
        unsafe { CloseHandle(job) };
    }
}

// Un-gated (the guard is a core, non-feature-gated type) so a default
// `cargo test -- --ignored` exercises it, not only `--features limits`.
#[cfg(test)]
mod guard_tests {
    /// Whether `pid` is a live process. Self-contained FFI (independent of the
    /// crate's feature-gated helpers) so the test compiles in any config.
    fn pid_alive(pid: u32) -> bool {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };
        const STILL_ACTIVE: u32 = 259;
        // SAFETY: a plain query open; the handle is closed exactly once below.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return false; // gone (or no longer openable) — dead for our purposes
        }
        let mut code: u32 = 0;
        // SAFETY: `handle` is a valid process handle from OpenProcess.
        let ok = unsafe { GetExitCodeProcess(handle, &mut code) };
        // SAFETY: closing the handle obtained above, exactly once.
        unsafe { CloseHandle(handle) };
        ok != 0 && code == STILL_ACTIVE
    }

    /// A child created `CREATE_SUSPENDED` and never resumed — the exact state
    /// [`Job::spawn`](super::Job) guards: spawned but not yet assigned to the
    /// job. It runs no user code (a suspended process is still "alive" — its
    /// exit code reads `STILL_ACTIVE`); the guard's reap, or the test cleanup,
    /// terminates it via `TerminateProcess`.
    fn spawn_suspended() -> tokio::process::Child {
        use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;
        tokio::process::Command::new("cmd")
            .args(["/C", "ping -n 30 127.0.0.1 > NUL"])
            .creation_flags(CREATE_SUSPENDED)
            .spawn()
            .expect("spawn the suspended child")
    }

    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn uncontained_guard_reaps_the_child_on_an_armed_drop() {
        // An armed guard dropped without disarm must terminate the suspended,
        // not-yet-contained child (the spawn→assign unwind path).
        let child = spawn_suspended();
        let pid = child.id().expect("the child has a pid");
        assert!(
            pid_alive(pid),
            "the suspended child is alive right after spawn"
        );
        drop(super::UncontainedChildGuard::arm(child)); // armed → reaps on drop
        let mut dead = false;
        for _ in 0..200 {
            if !pid_alive(pid) {
                dead = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(dead, "an armed guard drop must terminate the child");
    }

    #[tokio::test]
    #[ignore = "spawns a real subprocess"]
    async fn uncontained_guard_disarm_hands_back_a_live_child() {
        // The success path: disarm returns the same child, still running, for the
        // job to own — the guard must not kill a contained child.
        let child = spawn_suspended();
        let pid = child.id().expect("the child has a pid");
        let mut kept = super::UncontainedChildGuard::arm(child).disarm();
        assert!(pid_alive(pid), "disarm must leave the child running");
        // Clean up the suspended child.
        let _ = kept.start_kill();
        let _ = kept.wait().await;
    }
}

#[cfg(test)]
mod thread_resume_tests {
    #[test]
    fn resume_thread_handle_rejects_an_invalid_handle() {
        let error = super::resume_thread_handle(std::ptr::null_mut())
            .expect_err("ResumeThread failure must never look like a launched child");
        assert_eq!(
            error.raw_os_error(),
            Some(windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE as i32)
        );
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

// Un-gated (`graceful_shutdown` and the latch are core, not feature-gated) so the
// default `cargo test` exercises the Windows re-arm race — no subprocess needed.
#[cfg(test)]
mod rearm_race_tests {
    use std::time::Duration;

    /// Build a bare `Job`, papering over the `limits`-feature gate on `Job::new`.
    fn new_job() -> super::Job {
        #[cfg(feature = "limits")]
        {
            super::Job::new(&crate::limits::ResourceLimits::default()).expect("create a test job")
        }
        #[cfg(not(feature = "limits"))]
        {
            super::Job::new().expect("create a test job")
        }
    }

    /// The documented reuse semantics, through the real `graceful_shutdown`
    /// (`escalate = false`) path: with nothing racing it, the shutdown spares
    /// survivors (Drop clears `KILL_ON_JOB_CLOSE`), and a subsequent spawn/adopt
    /// (which calls `clear()`) re-arms the kill-on-close backstop for the newcomer.
    #[tokio::test]
    async fn non_escalating_shutdown_spares_then_a_rearm_re_arms() {
        let job = new_job();
        job.graceful_shutdown(0, Duration::ZERO, false)
            .await
            .expect("graceful shutdown");
        assert!(
            job.skip_drop_kill.is_set(),
            "escalate=false spares survivors: Drop clears KILL_ON_JOB_CLOSE"
        );
        job.skip_drop_kill.clear();
        assert!(
            !job.skip_drop_kill.is_set(),
            "a member that joined after the spare re-arms Drop's kill-on-close"
        );
    }

    /// T-079 (Windows job re-arm race): a spawn/adopt that re-arms the backstop
    /// between a non-escalating shutdown's epoch snapshot and its final `request`
    /// must win — the stale request must not re-spare the fresh child. The Windows
    /// `graceful_shutdown(escalate = false)` body is exactly `begin_shutdown()` …
    /// `request(epoch)` with the concurrent spawn/adopt's `clear()` interleaving
    /// between; this reproduces that ordering deterministically on the real `Job`'s
    /// latch, no subprocess required.
    #[tokio::test]
    async fn a_concurrent_rearm_wins_over_a_stale_non_escalating_request() {
        let job = new_job();
        job.skip_drop_kill.clear(); // a live reused job — backstop already armed
        // The shutdown snapshots its generation…
        let epoch = job.skip_drop_kill.begin_shutdown();
        // …a concurrent spawn/adopt re-arms the backstop for a fresh child…
        job.skip_drop_kill.clear();
        // …and only now does the shutdown's stale request land.
        job.skip_drop_kill.request(epoch);
        assert!(
            !job.skip_drop_kill.is_set(),
            "a child assigned to the job mid-shutdown must keep its kill-on-close \
             backstop — the stale request must not re-spare it"
        );
    }
}

// T-139: the opt-in console-CTRL graceful path. Un-gated (the leader tracking,
// routing and drain check are core, not feature-gated) so the default
// `cargo test` exercises most of this module without a subprocess, driven
// against an empty job and our own (non-member) pid as a stand-in leader. The
// T-154 pruning regression test is the one exception — it drives the real
// `Job::spawn` against real short-lived children and is `#[ignore]`d
// accordingly (see K-028-adjacent convention in `guard_tests` above).
#[cfg(test)]
mod ctrl_break_tests {
    use std::time::Duration;

    use windows_sys::Win32::System::Threading::GetCurrentProcessId;

    /// Build a bare `Job`, papering over the `limits`-feature gate on `Job::new`.
    fn new_job() -> super::Job {
        #[cfg(feature = "limits")]
        {
            super::Job::new(&crate::limits::ResourceLimits::default()).expect("create a test job")
        }
        #[cfg(not(feature = "limits"))]
        {
            super::Job::new().expect("create a test job")
        }
    }

    /// The drain check reports a fresh, empty job as drained (no active member),
    /// so the CTRL driver's poll ends immediately rather than riding the grace.
    #[test]
    fn an_empty_job_reports_drained() {
        let job = new_job();
        assert!(
            super::job_is_drained(job.handle),
            "an empty job has zero active processes, so it is drained"
        );
    }

    /// The GUI-graceful probe is membership-scoped: a fresh, empty job owns no
    /// top-level window, so `job_has_windowed_member` is false and
    /// `close_member_windows` posts nothing. This is the load-bearing safety
    /// property — a `WM_CLOSE` is never posted onto an unrelated app's window (or
    /// the test runner's own, which is not a member of this job), only onto a live
    /// member's. It runs on every desktop (the enumeration visits all top-level
    /// windows; none pass the `process_is_in_job` filter for this empty job).
    #[test]
    fn an_empty_job_has_no_windowed_member_and_closes_nothing() {
        let job = new_job();
        assert!(
            !super::job_has_windowed_member(job.handle),
            "an empty job owns no top-level window"
        );
        assert_eq!(
            super::close_member_windows(job.handle),
            0,
            "no member window means no WM_CLOSE is posted — never onto an unrelated \
             app's window or the test runner's own"
        );
    }

    /// The narrowed `signal` contract: `Int`/`Term` on a group with neither a
    /// console-CTRL leader nor a windowed member (a fresh empty job is both) still
    /// reports `Unsupported` — a Job Object can't deliver a POSIX Int/Term when
    /// there is nothing to soft-close.
    #[cfg(feature = "process-control")]
    #[test]
    fn signal_int_or_term_without_a_soft_close_target_is_unsupported() {
        let job = new_job();
        for sig in [crate::Signal::Int, crate::Signal::Term] {
            let err = job
                .signal(sig)
                .expect_err("no console/windowed target to reach");
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::Unsupported,
                "{sig:?} on a group with no console/windowed member is Unsupported"
            );
        }
    }

    /// The WM_CLOSE tier narrowed the contract for `Int`/`Term` ONLY: every other
    /// curated non-`Kill` signal stays unconditionally `Unsupported` on Windows.
    #[cfg(feature = "process-control")]
    #[test]
    fn signal_other_curated_variants_stay_unsupported() {
        let job = new_job();
        for sig in [
            crate::Signal::Hup,
            crate::Signal::Quit,
            crate::Signal::Usr1,
            crate::Signal::Usr2,
            crate::Signal::Other(9),
        ] {
            let err = job.signal(sig).expect_err("not a soft-close signal");
            assert_eq!(
                err.kind(),
                std::io::ErrorKind::Unsupported,
                "{sig:?} remains Unsupported on Windows"
            );
        }
    }

    /// The side-effect-free soft-stop capability probe agrees with the narrowed
    /// `signal` contract on the negative case: a fresh empty job has neither a
    /// console-CTRL leader nor a windowed member, so `soft_stop_scope` reports
    /// `Unsupported` — exactly when `signal(Int/Term)` would too. No spawn, no
    /// signal delivered.
    #[cfg(feature = "process-control")]
    #[test]
    fn soft_stop_scope_on_an_empty_job_is_unsupported() {
        let job = new_job();
        assert_eq!(
            job.soft_stop_scope(),
            crate::SoftStopScope::Unsupported,
            "an empty job can soft-stop nothing"
        );
    }

    /// The probe counts only leaders that are STILL live members of THIS job,
    /// exactly as `ctrl_break_live_leaders` does before delivering — so a recorded
    /// leader that is NOT a member (here our own pid, a non-member, mirroring
    /// `a_recorded_non_member_leader_is_never_signalled`) does not fake a soft-stop
    /// capability. The job being otherwise empty (no windowed member either), the
    /// honest answer stays `Unsupported`. Reads state without pruning the recorded
    /// list or delivering anything — the test process reaching its assertions is
    /// proof no CTRL_BREAK was sent to the (handler-less) runner.
    #[cfg(feature = "process-control")]
    #[test]
    fn soft_stop_scope_ignores_a_non_member_leader() {
        let job = new_job();
        // SAFETY: a plain read of our own pid — a non-member of this job.
        let me = unsafe { GetCurrentProcessId() };
        job.ctrl_break_leaders
            .lock()
            .expect("lock leaders")
            .push(me);

        assert_eq!(
            job.soft_stop_scope(),
            crate::SoftStopScope::Unsupported,
            "a recorded but non-member leader is not a reachable soft-stop target"
        );
        // The probe is side-effect-free: it must not have pruned the recorded
        // (stale) leader out of the list.
        assert_eq!(
            job.ctrl_break_leaders.lock().expect("lock leaders").len(),
            1,
            "soft_stop_scope must not mutate the recorded-leader list"
        );
    }

    /// A ~30s console child, spawned into its own process group so a CTRL_BREAK
    /// reaches its group and not the test runner's console.
    #[cfg(feature = "process-control")]
    fn opt_in_sleeper() -> tokio::process::Command {
        let mut cmd = tokio::process::Command::new("ping");
        cmd.args(["-n", "30", "127.0.0.1"]);
        cmd
    }

    /// Poll `process_is_in_job` up to ~5s for the just-spawned child to read as a
    /// live member (it should be immediate) before treating it as a soft-stop
    /// target — the membership the probe/delivery both key off.
    #[cfg(feature = "process-control")]
    async fn wait_until_member(pid: u32, job: &super::Job) {
        for _ in 0..500 {
            if super::process_is_in_job(pid, job.handle) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("pid {pid} never became a live job member");
    }

    /// Item 1 (the closed test gap): `Job::signal(Term)` / `Job::signal(Int)`
    /// against a job holding a REAL, live opt-in console leader returns `Ok(())` —
    /// the direct positive `signal` assertion the existing tests never made (they
    /// drive `graceful_shutdown` with a non-member stand-in leader, or the empty-job
    /// `Unsupported` case only). The child is spawned with
    /// `windows_new_process_group`, so the CTRL_BREAK lands on its own group, not
    /// the runner's console; `ping` ignores it (K-028) and rides on, so the job
    /// stays reapable at the end.
    #[cfg(feature = "process-control")]
    #[tokio::test]
    #[ignore = "spawns a real opt-in console child and soft-signals it"]
    async fn signal_term_and_int_reach_a_live_opt_in_leader() {
        let job = new_job();
        let opts = crate::sys::SpawnOptions {
            windows_new_process_group: true,
            ..Default::default()
        };
        let mut child = job
            .spawn(&mut opt_in_sleeper(), &opts)
            .expect("spawn opt-in console child");
        let pid = child.id().expect("child has a pid");
        wait_until_member(pid, &job).await;

        // The opt-in leader is a live member, so the soft close reaches it:
        // `signal` returns Ok rather than the empty-job Unsupported.
        job.signal(crate::Signal::Term)
            .expect("Term reaches a live opt-in console leader");
        job.signal(crate::Signal::Int)
            .expect("Int reaches a live opt-in console leader");

        // Tear the tree down deterministically regardless of the child's own
        // CTRL_BREAK handling.
        let _ = job.kill_all();
        let _ = child.wait().await;
    }

    /// The Windows positive for the capability probe: with a real, live opt-in
    /// console leader in the job, `soft_stop_scope` reports `OptInMembers`, and it
    /// is consistent with the real `signal` outcome on that same job (both agree a
    /// soft stop is available). The side-effect-free probe is called BEFORE any
    /// signal.
    #[cfg(feature = "process-control")]
    #[tokio::test]
    #[ignore = "spawns a real opt-in console child and probes soft-stop availability"]
    async fn soft_stop_scope_reports_opt_in_members_for_a_live_leader() {
        let job = new_job();
        let opts = crate::sys::SpawnOptions {
            windows_new_process_group: true,
            ..Default::default()
        };
        let mut child = job
            .spawn(&mut opt_in_sleeper(), &opts)
            .expect("spawn opt-in console child");
        let pid = child.id().expect("child has a pid");
        wait_until_member(pid, &job).await;

        // The probe (side-effect-free, before any delivery) sees the live opt-in
        // leader and reports the soft stop as available.
        assert_eq!(
            job.soft_stop_scope(),
            crate::SoftStopScope::OptInMembers,
            "a live opt-in console leader makes a soft stop available"
        );
        // Consistency with the actual verb: a real soft `signal` succeeds on the
        // same job, matching what the probe reported.
        job.signal(crate::Signal::Term)
            .expect("Term reaches the live opt-in leader the probe reported");

        let _ = job.kill_all();
        let _ = child.wait().await;
    }

    /// A recorded pid that is not a live job member cannot manufacture a graceful
    /// tier. The same recycle-safe membership gate used by `signal` and
    /// `soft_stop_scope` keeps teardown on the prompt atomic branch and reports the
    /// soft signal as unsupported. The test process surviving is also proof no
    /// CTRL_BREAK was sent to our own (handler-less) process.
    #[tokio::test]
    async fn a_recorded_non_member_leader_is_never_signalled() {
        let job = new_job();
        // SAFETY: a plain read of our own pid.
        let me = unsafe { GetCurrentProcessId() };
        job.ctrl_break_leaders
            .lock()
            .expect("lock leaders")
            .push(me);

        let start = std::time::Instant::now();
        let outcome = job
            .graceful_shutdown(crate::sys::SIGTERM_RAW, Duration::from_secs(30), true)
            .await
            .expect("ctrl-break graceful shutdown of an empty job");
        assert_eq!(
            outcome.soft,
            crate::sys::graceful::SoftDelivery::Unsupported,
            "a stale leader does not create a soft-shutdown tier"
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a stale leader must not introduce a grace wait (took {:?})",
            start.elapsed()
        );
        assert!(
            job.ctrl_break_leaders.lock().expect("lock leaders").len() == 1,
            "the recorded leader is retained across a shutdown"
        );
    }

    /// The atomic path honors `escalate = false` sparing when a stale recorded
    /// leader is the only putative soft target. It latches `skip_drop_kill` so
    /// `Drop` clears `KILL_ON_JOB_CLOSE`, while a subsequent spawn/adopt still
    /// re-arms the backstop.
    #[tokio::test]
    async fn a_stale_ctrl_leader_uses_atomic_non_escalating_shutdown() {
        let job = new_job();
        // SAFETY: a plain read of our own pid — a non-member leader (skipped by
        // the recycle guard), so no CTRL_BREAK reaches the test runner.
        let me = unsafe { GetCurrentProcessId() };
        job.ctrl_break_leaders
            .lock()
            .expect("lock leaders")
            .push(me);

        job.graceful_shutdown(crate::sys::SIGTERM_RAW, Duration::ZERO, false)
            .await
            .expect("non-escalating ctrl-break shutdown");
        assert!(
            job.skip_drop_kill.is_set(),
            "a non-escalating atomic shutdown spares survivors: Drop clears KILL_ON_JOB_CLOSE"
        );
        // A subsequent spawn/adopt re-arms it, as on every backend.
        job.skip_drop_kill.clear();
        assert!(
            !job.skip_drop_kill.is_set(),
            "a reused job re-arms the backstop"
        );
    }

    /// T-154 regression: `ctrl_break_leaders` must stay bounded across a
    /// long-lived shared job's lifetime, not grow by one per opt-in spawn
    /// regardless of exits. Drives the real `Job::spawn` (unlike the other tests
    /// in this module, which push a stand-in pid directly) across three
    /// spawn+exit cycles of real, short-lived children and asserts each new
    /// opt-in spawn prunes the previous cycle's now-dead leader before recording
    /// itself — the list only ever holds the single currently-live leader, never
    /// the whole spawn history.
    ///
    /// Between "exit" and "prune" the test polls `process_is_in_job` — the same
    /// recycle guard `spawn`'s pruning uses — down to `false` rather than
    /// asserting the very next instruction after `wait()`/`drop`: a just-exited
    /// process's pid can stay openable, and its job association readable, for a
    /// short OS-timed window past our own handle closing (some other observer —
    /// AV/ETW/csrss bookkeeping — can transiently hold the last reference), so
    /// asserting immediately would be a race against Windows process teardown
    /// timing, not against this pruning logic.
    #[tokio::test]
    #[ignore = "spawns real subprocesses"]
    async fn stale_leaders_are_pruned_across_spawn_and_exit_cycles() {
        let job = new_job();
        let opts = crate::sys::SpawnOptions {
            windows_new_process_group: true,
            ..Default::default()
        };

        fn short_lived_cmd() -> tokio::process::Command {
            let mut cmd = tokio::process::Command::new("cmd");
            cmd.args(["/C", "exit 0"]);
            cmd
        }

        /// Waits (bounded) for `pid` to stop reading as a member of `job` via
        /// the same `process_is_in_job` guard `spawn`'s pruning consults, so
        /// the test's own assertions race the pruning logic, not the OS's
        /// process-teardown timing.
        async fn wait_until_pruneable(pid: u32, job: &super::Job) {
            for _ in 0..500 {
                if !super::process_is_in_job(pid, job.handle) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("pid {pid} still reads as a job member after 5s — can't test pruning");
        }

        // Cycle 1: spawn, then wait out and drop the child, and wait for its
        // job membership to actually clear before treating it as prune-able.
        let mut child1 = job
            .spawn(&mut short_lived_cmd(), &opts)
            .expect("spawn first opt-in child");
        let pid1 = child1.id().expect("first child has a pid");
        child1.wait().await.expect("first child exits");
        drop(child1);
        assert_eq!(
            job.ctrl_break_leaders.lock().expect("lock leaders").clone(),
            vec![pid1],
            "the first leader is recorded right after its spawn"
        );
        wait_until_pruneable(pid1, &job).await;

        // Cycle 2: a second opt-in spawn into the SAME job must prune the now-
        // dead first leader before recording itself — the list must not simply
        // grow to two entries.
        let mut child2 = job
            .spawn(&mut short_lived_cmd(), &opts)
            .expect("spawn second opt-in child");
        let pid2 = child2.id().expect("second child has a pid");
        assert_eq!(
            job.ctrl_break_leaders.lock().expect("lock leaders").clone(),
            vec![pid2],
            "spawn prunes the stale first leader and records only the live second one"
        );
        child2.wait().await.expect("second child exits");
        drop(child2);
        wait_until_pruneable(pid2, &job).await;

        // Cycle 3: repeating the pattern proves the list stays bounded across
        // more than one spawn+exit cycle, not just the first pruning.
        let mut child3 = job
            .spawn(&mut short_lived_cmd(), &opts)
            .expect("spawn third opt-in child");
        let pid3 = child3.id().expect("third child has a pid");
        assert_eq!(
            job.ctrl_break_leaders.lock().expect("lock leaders").clone(),
            vec![pid3],
            "the list stays bounded to the single live leader across a second \
             spawn+exit cycle"
        );
        let _ = child3.kill().await;
    }
}

// The per-process identity gate (T-090): a metrics read that names a pid whose
// current OS start identity does not match the captured one must yield defaults,
// never the stranger's counters — the fail-safe that stops a recycled pid from
// corrupting a `profile`/`cpu_time`/`peak_memory_bytes` sample. Driven against our
// OWN live process (`GetCurrentProcessId`), which is guaranteed present and has a
// stable creation time, so a wrong identity deterministically stands in for a
// reused pid — no second process or reuse simulation needed.
#[cfg(all(test, feature = "stats"))]
mod metrics_identity_tests {
    use super::{ProcIdentity, process_identity, process_metrics};
    use windows_sys::Win32::System::Threading::GetCurrentProcessId;

    #[test]
    fn identity_is_captured_and_matches_a_same_process_read() {
        // SAFETY: a plain read of our own pid.
        let me = unsafe { GetCurrentProcessId() };
        let id = process_identity(me).expect("our own live process has a creation time");

        // The captured identity matches on a re-read (a live process keeps its
        // creation time), so the gated read returns real counters.
        let gated = process_metrics(me, Some(id));
        assert!(
            gated.cpu_time.is_some(),
            "an identity-matched read of our own process reports CPU time"
        );
        assert!(
            gated.peak_memory_bytes.is_some(),
            "an identity-matched read of our own process reports peak memory"
        );
    }

    #[test]
    fn a_mismatched_identity_yields_defaults_not_the_live_process_counters() {
        // SAFETY: a plain read of our own pid.
        let me = unsafe { GetCurrentProcessId() };
        let real = process_identity(me).expect("our own live process has a creation time");

        // A wrong identity models a pid recycled by a different process: even though
        // the pid is very much alive (it is us), the gate must refuse to fold ANY of
        // this process's counters, returning the all-`None` default.
        let bogus = ProcIdentity::from_raw(real.raw().wrapping_add(1));
        let gated = process_metrics(me, Some(bogus));
        assert!(
            gated.cpu_time.is_none() && gated.peak_memory_bytes.is_none(),
            "a mismatched identity must yield defaults, never the live process's \
             CPU/memory — the recycled-pid fail-safe"
        );

        // Without a demanded identity the number-only behavior is preserved.
        let ungated = process_metrics(me, None);
        assert!(
            ungated.cpu_time.is_some(),
            "an unchecked read (identity None) still reports metrics"
        );
    }
}
