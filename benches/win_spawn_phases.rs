//! Windows-only attribution of the **fixed start cost** of a ProcessKit run.
//!
//! `benches/compare.rs` answers "what does a whole run cost against a plain
//! process API?". It cannot answer the follow-up question a Windows consumer
//! actually needs: *which part of that difference is ProcessKit's and which part
//! is `CreateProcess` itself?* On a Windows host with a real-time antivirus,
//! creating a process costs tens of milliseconds all by itself, so an absolute
//! "start took N ms" number attributes nothing.
//!
//! This bench splits the start path into phases and measures each against the
//! same child, so a series differs from the one it builds on by exactly one
//! phase:
//!
//! * `os_spawn_plain` — `CreateProcess` + wait, nothing else. The floor: pure OS
//!   cost, present for every process API on this machine.
//! * `os_spawn_suspended_resume_snapshot` / `..._direct` — the same spawn with
//!   `CREATE_SUSPENDED`, resumed the two ways a launcher can find a child's
//!   primary thread: the documented system-wide `TH32CS_SNAPTHREAD` snapshot, and
//!   the per-process `ntdll!NtGetNextThread` walk the crate uses since T-244.
//!   Minus `os_spawn_plain`, each is the cost of ProcessKit's suspend/resume
//!   cycle under that strategy.
//! * `containment_sequence_snapshot` / `..._direct` — Job Object creation,
//!   suspended spawn, assign, resume, wait. Minus the matching series above, this
//!   is the cost of the Job Object itself. These mirror
//!   `sys::windows::Job::{new, spawn}` step for step.
//!
//! A second group measures the primitives behind those phases in isolation, with
//! no child process involved so a spawn outlier cannot hide them: Job Object
//! creation, both thread-lookup strategies, and the crate's `PATH`/PATHEXT
//! program lookup against an absolute-path lookup (the plain baselines in
//! `compare.rs` never perform that lookup, because `CreateProcess` does its own
//! `.exe`-only search inside the kernel).
//!
//! A third group measures what the crate's process-global spawn lock costs a
//! concurrent fan-out, by running the same 16 spawns with and without one shared
//! mutex around the spawn call.
//!
//! Run with `cargo bench --bench win_spawn_phases` or `just bench-win-phases`.
//! On non-Windows targets the bench compiles to a stub that explains itself and
//! exits, so `cargo build --all-targets` stays green everywhere.

#[cfg(windows)]
mod windows_phases {
    use std::io;
    use std::os::windows::io::AsRawHandle as _;
    use std::os::windows::process::CommandExt as _;
    use std::process::{Child, Stdio};
    use std::sync::Mutex;
    use std::time::Duration;

    use criterion::Criterion;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };
    use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
    use windows_sys::Win32::System::Threading::{
        CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
    };

    /// How many children the fan-out series starts per sample.
    const FAN_OUT: usize = 16;

    /// The child every series starts: the command interpreter exiting
    /// immediately. It is named by its **absolute** path so no series pays for a
    /// `PATH` search — program lookup is measured separately and must not leak
    /// into the spawn phases.
    fn fixture_child() -> std::process::Command {
        let comspec = std::env::var_os("ComSpec")
            .unwrap_or_else(|| std::ffi::OsString::from(r"C:\Windows\System32\cmd.exe"));
        let mut cmd = std::process::Command::new(comspec);
        cmd.args(["/c", "exit"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        cmd
    }

    /// A Job Object with the same kill-on-close containment flag
    /// `sys::windows::Job::new` sets, closed on drop.
    #[derive(Debug)]
    struct OwnedJob(HANDLE);

    impl OwnedJob {
        fn create() -> io::Result<Self> {
            // SAFETY: null name/attributes request an unnamed job with defaults.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            let job = Self(handle);
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `info` is a fully-initialised struct matching the info
            // class and its size is passed explicitly.
            let ok = unsafe {
                SetInformationJobObject(
                    job.0,
                    JobObjectExtendedLimitInformation,
                    std::ptr::from_ref(&info).cast(),
                    u32::try_from(std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .expect("the extended-limit struct fits in a u32"),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(job)
        }

        fn assign(&self, child: &Child) -> io::Result<()> {
            // SAFETY: the raw handle is valid while `child` is borrowed here.
            let ok = unsafe { AssignProcessToJobObject(self.0, child.as_raw_handle() as HANDLE) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    impl Drop for OwnedJob {
        fn drop(&mut self) {
            // SAFETY: the handle came from CreateJobObjectW and is closed once.
            unsafe { CloseHandle(self.0) };
        }
    }

    /// Resume every thread of `pid` — the same system-wide thread-snapshot walk
    /// `sys::windows::resume_process_threads` performs, reproduced here so the
    /// measurement tracks the real implementation rather than an idealised one.
    fn resume_process_threads(pid: u32) -> io::Result<()> {
        // SAFETY: TH32CS_SNAPTHREAD snapshots all threads system-wide; returns
        // INVALID_HANDLE_VALUE on failure.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = u32::try_from(std::mem::size_of::<THREADENTRY32>())
            .expect("the thread entry fits in a u32");
        let mut resumed = 0u32;
        // SAFETY: valid snapshot; `entry` is sized via its `dwSize` field.
        let mut ok = unsafe { Thread32First(snapshot, &mut entry) };
        while ok != 0 {
            if entry.th32OwnerProcessID == pid {
                // SAFETY: opens the thread by id; returns null on failure.
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if !thread.is_null() {
                    // SAFETY: valid thread handle with resume rights.
                    if unsafe { ResumeThread(thread) } != u32::MAX {
                        resumed += 1;
                    }
                    // SAFETY: handle came from OpenThread; closed exactly once.
                    unsafe { CloseHandle(thread) };
                }
            }
            // SAFETY: same valid snapshot and entry.
            ok = unsafe { Thread32Next(snapshot, &mut entry) };
        }
        // SAFETY: handle came from CreateToolhelp32Snapshot; closed once.
        unsafe { CloseHandle(snapshot) };
        if resumed == 0 {
            return Err(io::Error::other("no thread resumed"));
        }
        Ok(())
    }

    /// `ntdll!NtGetNextThread` — the per-process thread walk `sys::windows` uses
    /// instead of the system-wide snapshot, mirrored here so the two resume
    /// strategies can be measured against the same child.
    type NtGetNextThread = unsafe extern "system" fn(
        process: HANDLE,
        thread: HANDLE,
        desired_access: u32,
        handle_attributes: u32,
        flags: u32,
        new_thread: *mut HANDLE,
    ) -> i32;

    fn nt_get_next_thread() -> Option<NtGetNextThread> {
        const NTDLL_UTF16: &[u16] = &[
            b'n' as u16,
            b't' as u16,
            b'd' as u16,
            b'l' as u16,
            b'l' as u16,
            b'.' as u16,
            b'd' as u16,
            b'l' as u16,
            b'l' as u16,
            0,
        ];
        // SAFETY: ntdll is mapped into every Win32 process and never unloaded;
        // GetModuleHandleW takes no reference on it.
        let ntdll = unsafe { GetModuleHandleW(NTDLL_UTF16.as_ptr()) };
        if ntdll.is_null() {
            return None;
        }
        // SAFETY: live module handle, NUL-terminated ASCII symbol name.
        let symbol =
            unsafe { GetProcAddress(ntdll, c"NtGetNextThread".to_bytes_with_nul().as_ptr()) };
        // SAFETY: the resolved export has exactly this signature and ntdll stays
        // mapped for the process lifetime.
        symbol.map(|symbol| unsafe {
            std::mem::transmute::<unsafe extern "system" fn() -> isize, NtGetNextThread>(symbol)
        })
    }

    /// Resume every thread of the process behind `process` through
    /// `NtGetNextThread`, the fast path `sys::windows::resume_process_threads`
    /// takes.
    fn resume_process_threads_direct(process: HANDLE) -> io::Result<()> {
        let next_thread = nt_get_next_thread()
            .ok_or_else(|| io::Error::other("ntdll!NtGetNextThread is unavailable"))?;
        let mut resumed = 0u32;
        let mut cursor: HANDLE = std::ptr::null_mut();
        loop {
            let mut thread: HANDLE = std::ptr::null_mut();
            // SAFETY: `process` is a live child handle, `cursor` is null or a
            // handle from this same enumeration, `thread` is an owned out-param.
            let status =
                unsafe { next_thread(process, cursor, THREAD_SUSPEND_RESUME, 0, 0, &mut thread) };
            if !cursor.is_null() {
                // SAFETY: obtained from a previous call; closed exactly once.
                unsafe { CloseHandle(cursor) };
            }
            if status < 0 || thread.is_null() {
                break;
            }
            // SAFETY: valid thread handle with resume rights.
            if unsafe { ResumeThread(thread) } != u32::MAX {
                resumed += 1;
            }
            cursor = thread;
        }
        if resumed == 0 {
            return Err(io::Error::other("no thread resumed"));
        }
        Ok(())
    }

    /// A suspended child that outlives one measurement, so the per-process thread
    /// walk can be timed without resuming (and thus consuming) it.
    fn fixture_long_lived_suspended_child() -> Child {
        fixture_child()
            .creation_flags(CREATE_SUSPENDED)
            .spawn()
            .expect("spawn the long-lived suspended fixture child")
    }

    /// Enumerate (without resuming) the threads of one process — the lookup half
    /// of the direct resume, the counterpart of [`thread_snapshot_walk`].
    fn count_threads_direct(next_thread: NtGetNextThread, process: HANDLE) -> u32 {
        let mut found = 0u32;
        let mut cursor: HANDLE = std::ptr::null_mut();
        loop {
            let mut thread: HANDLE = std::ptr::null_mut();
            // SAFETY: as in `resume_process_threads_direct`.
            let status =
                unsafe { next_thread(process, cursor, THREAD_SUSPEND_RESUME, 0, 0, &mut thread) };
            if !cursor.is_null() {
                // SAFETY: obtained from a previous call; closed exactly once.
                unsafe { CloseHandle(cursor) };
            }
            if status < 0 || thread.is_null() {
                break;
            }
            found += 1;
            cursor = thread;
        }
        found
    }

    /// The snapshot half of the resume walk on its own: take a system-wide thread
    /// snapshot and count the threads belonging to `pid`, resuming nothing. This
    /// is what the walk pays before it can touch a single thread.
    fn thread_snapshot_walk(pid: u32) -> u32 {
        // SAFETY: as in `resume_process_threads`.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return 0;
        }
        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = u32::try_from(std::mem::size_of::<THREADENTRY32>())
            .expect("the thread entry fits in a u32");
        let mut found = 0u32;
        // SAFETY: valid snapshot; `entry` is sized via its `dwSize` field.
        let mut ok = unsafe { Thread32First(snapshot, &mut entry) };
        while ok != 0 {
            if entry.th32OwnerProcessID == pid {
                found += 1;
            }
            // SAFETY: same valid snapshot and entry.
            ok = unsafe { Thread32Next(snapshot, &mut entry) };
        }
        // SAFETY: handle came from CreateToolhelp32Snapshot; closed once.
        unsafe { CloseHandle(snapshot) };
        found
    }

    /// The three cumulative start phases. Each series adds exactly one phase to
    /// the one above it, so the differences attribute the cost.
    pub fn bench_start_phases(c: &mut Criterion) {
        let mut group = c.benchmark_group("windows_start_phases");
        group.bench_function("os_spawn_plain", |b| {
            b.iter(|| {
                let mut child = fixture_child().spawn().expect("spawn the fixture child");
                child.wait().expect("wait for the fixture child");
            });
        });
        group.bench_function("os_spawn_suspended_resume_snapshot", |b| {
            b.iter(|| {
                let mut child = fixture_child()
                    .creation_flags(CREATE_SUSPENDED)
                    .spawn()
                    .expect("spawn the suspended fixture child");
                resume_process_threads(child.id()).expect("resume the fixture child");
                child.wait().expect("wait for the fixture child");
            });
        });
        group.bench_function("os_spawn_suspended_resume_direct", |b| {
            b.iter(|| {
                let mut child = fixture_child()
                    .creation_flags(CREATE_SUSPENDED)
                    .spawn()
                    .expect("spawn the suspended fixture child");
                resume_process_threads_direct(child.as_raw_handle() as HANDLE)
                    .expect("resume the fixture child");
                child.wait().expect("wait for the fixture child");
            });
        });
        group.bench_function("containment_sequence_snapshot", |b| {
            b.iter(|| {
                let job = OwnedJob::create().expect("create the job object");
                let mut child = fixture_child()
                    .creation_flags(CREATE_SUSPENDED)
                    .spawn()
                    .expect("spawn the suspended fixture child");
                job.assign(&child).expect("assign the child to the job");
                resume_process_threads(child.id()).expect("resume the fixture child");
                child.wait().expect("wait for the fixture child");
            });
        });
        group.bench_function("containment_sequence_direct", |b| {
            b.iter(|| {
                let job = OwnedJob::create().expect("create the job object");
                let mut child = fixture_child()
                    .creation_flags(CREATE_SUSPENDED)
                    .spawn()
                    .expect("spawn the suspended fixture child");
                job.assign(&child).expect("assign the child to the job");
                resume_process_threads_direct(child.as_raw_handle() as HANDLE)
                    .expect("resume the fixture child");
                child.wait().expect("wait for the fixture child");
            });
        });
        group.finish();
    }

    /// The individual primitives behind those phases, measured with no child
    /// process involved at all, so an OS-spawn outlier cannot hide them.
    pub fn bench_start_primitives(c: &mut Criterion) {
        let mut group = c.benchmark_group("windows_start_primitives");
        group.bench_function("job_object_create", |b| {
            b.iter(|| {
                let job = OwnedJob::create().expect("create the job object");
                std::hint::black_box(&job);
            });
        });
        group.bench_function("thread_snapshot_walk", |b| {
            let own_pid = std::process::id();
            b.iter(|| {
                std::hint::black_box(thread_snapshot_walk(own_pid));
            });
        });
        group.bench_function("direct_thread_walk", |b| {
            // The same "find this process's threads" question the resume asks,
            // answered per-process instead of machine-wide. Enumerated over a
            // long-lived suspended child so no thread is actually resumed and the
            // series measures only the lookup. The child is put in a kill-on-close
            // job first, so a panic anywhere below still takes it down with this
            // process instead of stranding a suspended orphan — the same guarantee
            // the crate gives its own children, applied to its own benchmark.
            let job = OwnedJob::create().expect("create the job object");
            let mut child = fixture_long_lived_suspended_child();
            job.assign(&child)
                .expect("contain the suspended fixture child");
            let handle = child.as_raw_handle() as HANDLE;
            let next_thread = nt_get_next_thread().expect("ntdll!NtGetNextThread is available");
            b.iter(|| {
                std::hint::black_box(count_threads_direct(next_thread, handle));
            });
            child.kill().expect("kill the suspended fixture child");
            child.wait().expect("reap the suspended fixture child");
            drop(job);
        });
        group.bench_function("program_lookup_bare_name", |b| {
            b.iter(|| {
                let found = processkit::Command::new("cmd")
                    .resolve_program()
                    .expect("resolve the bare command name");
                std::hint::black_box(found);
            });
        });
        group.bench_function("program_lookup_absolute_path", |b| {
            let absolute = processkit::Command::new("cmd")
                .resolve_program()
                .expect("resolve the bare command name once, outside the timed section");
            b.iter(|| {
                let found = processkit::Command::new(&absolute)
                    .resolve_program()
                    .expect("resolve the absolute program path");
                std::hint::black_box(found);
            });
        });
        group.finish();
    }

    /// What serialising the OS spawn call costs a concurrent fan-out. The crate
    /// holds one process-global lock across every child creation (so an ordinary
    /// spawn cannot observe the ConPTY path's temporary process-global standard
    /// handles); these two series measure the same 16 spawns with and without
    /// that serialisation, on the same threads.
    pub fn bench_spawn_serialization(c: &mut Criterion) {
        let mut group = c.benchmark_group("windows_spawn_serialization");
        group.bench_function("fan_out16_parallel", |b| {
            b.iter(|| {
                std::thread::scope(|scope| {
                    for _ in 0..FAN_OUT {
                        scope.spawn(|| {
                            let mut child =
                                fixture_child().spawn().expect("spawn the fixture child");
                            child.wait().expect("wait for the fixture child");
                        });
                    }
                });
            });
        });
        group.bench_function("fan_out16_serialized_spawn", |b| {
            let lock = Mutex::new(());
            b.iter(|| {
                std::thread::scope(|scope| {
                    for _ in 0..FAN_OUT {
                        scope.spawn(|| {
                            let mut child = {
                                let _guard = lock.lock().expect("the spawn lock is never poisoned");
                                fixture_child().spawn().expect("spawn the fixture child")
                            };
                            child.wait().expect("wait for the fixture child");
                        });
                    }
                });
            });
        });
        group.finish();
    }

    /// More and longer samples than `compare.rs`. Every series that creates a
    /// real child is at the mercy of the host's process-creation cost, which on
    /// an antivirus-equipped Windows machine drifts by *multiples* over minutes —
    /// and criterion runs series one after another, so a short window lets that
    /// drift land unevenly across them. A long window per series averages more of
    /// the cycle into each. The series that create no child (the primitives
    /// group) are unaffected and are the reliable attribution instrument; treat
    /// the cumulative spawn phases as corroboration, not precision.
    /// The long warm-up is not decoration either: on this class of host the first
    /// seconds of a run are visibly more expensive than the rest (a freshly
    /// rebuilt binary and a cold scanner), which without it lands entirely on
    /// whichever series criterion happens to run first.
    pub fn configure() -> Criterion {
        Criterion::default()
            .sample_size(50)
            .warm_up_time(Duration::from_secs(15))
            .measurement_time(Duration::from_secs(20))
    }
}

#[cfg(windows)]
criterion::criterion_group! {
    name = windows_start_benches;
    config = windows_phases::configure();
    targets =
        windows_phases::bench_start_phases,
        windows_phases::bench_start_primitives,
        windows_phases::bench_spawn_serialization
}

#[cfg(windows)]
criterion::criterion_main!(windows_start_benches);

#[cfg(not(windows))]
fn main() {
    println!(
        "win_spawn_phases measures Windows Job Object / suspend-resume / PATH-lookup start \
         cost and has no meaning on this target; run benches/compare.rs instead."
    );
}
