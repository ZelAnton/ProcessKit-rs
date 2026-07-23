//! ProcessGroup fundamentals: the platform mechanism, the kill-on-drop tree
//! guarantee (grandchildren included), and teardown idempotency.

use std::time::{Duration, Instant};

#[cfg(windows)]
use processkit::Command;
use processkit::{Mechanism, ProcessGroup};

use crate::common::*;

#[tokio::test]
#[ignore = "creates an OS job/cgroup"]
async fn group_reports_the_platforms_mechanism() {
    let group = ProcessGroup::new().expect("create group");
    let mechanism = group.mechanism();
    // Tightened per platform: a silently-degraded backend (e.g. JobObject
    // creation failing over to nothing) must not pass as "known".
    #[cfg(windows)]
    assert_eq!(mechanism, Mechanism::JobObject);
    #[cfg(target_os = "linux")]
    assert!(
        matches!(mechanism, Mechanism::CgroupV2 | Mechanism::ProcessGroup),
        "linux is cgroup v2 or its pgroup fallback, got {mechanism:?}"
    );
    #[cfg(all(unix, not(target_os = "linux")))]
    assert_eq!(mechanism, Mechanism::ProcessGroup);
}

/// The spawn-free host query
/// ([`host_containment`](processkit::host_containment)) must report the **same**
/// mechanism a really-created [`ProcessGroup`] on this host reports — the core
/// consistency contract: the read-only prediction agrees with the actual selection
/// (Linux with or without cgroup delegation, Windows, macOS). It also reuses the
/// existing `ParentDeathCleanup` capability query, and its soft-stop reach matches a
/// live group's where that is deterministic.
#[tokio::test]
#[ignore = "creates an OS job/cgroup to cross-check the read-only host query"]
async fn host_containment_matches_a_real_group() {
    // The read-only query — no container created, no process spawned.
    let host = processkit::host_containment();

    // A really-created group on this same host.
    let group = ProcessGroup::new().expect("create group");
    assert_eq!(
        host.mechanism(),
        group.mechanism(),
        "the read-only host query must predict the mechanism a real group gets"
    );

    // The parent-death field is exactly the existing capability query's answer.
    assert_eq!(
        host.parent_death_cleanup(),
        processkit::Command::kill_on_parent_death_scope(),
        "the host report reuses Command::kill_on_parent_death_scope()"
    );

    // Version is this crate's version.
    assert_eq!(host.crate_version(), env!("CARGO_PKG_VERSION"));

    // Soft-stop reach: on the Unix backends it is deterministically WholeTree, so it
    // must match an (empty) live group's per-group scope. On Windows the host-level
    // value is the OptInMembers *maximum*; an empty group narrows to Unsupported
    // per-group, so equality is not expected there — assert the host maximum instead.
    #[cfg(feature = "process-control")]
    {
        use processkit::SoftStopScope;
        #[cfg(unix)]
        assert_eq!(
            host.soft_stop_scope(),
            group.soft_stop_scope(),
            "on Unix the host soft-stop reach equals a live group's (WholeTree)"
        );
        #[cfg(unix)]
        assert_eq!(host.soft_stop_scope(), SoftStopScope::WholeTree);
        #[cfg(windows)]
        assert_eq!(
            host.soft_stop_scope(),
            SoftStopScope::OptInMembers,
            "on Windows the host reports the opt-in-members maximum a Job Object can reach"
        );
    }
}

#[tokio::test]
#[ignore = "spawns a long-lived subprocess and asserts kill-on-drop"]
async fn dropping_group_kills_children() {
    // Kill-on-close exists on Windows (Job Object), Linux (cgroup/process group)
    // and other unix (macOS/BSD process group) — i.e. every supported target.

    // Start the sleeper into a *shared* group: the returned handle does not own
    // the group, so we can drop the group out from under it.
    let group = ProcessGroup::new().expect("create group");
    let process = group.start(&sleeper()).await.expect("spawn sleeper");
    let pid = process.pid();
    assert!(
        pid.is_some(),
        "sleeper should report a pid right after spawn"
    );

    drop(group); // kill-on-close should reap the child promptly

    // The kill releases the child's pipes and forces exit, so `wait` returns
    // far sooner than the sleeper's own ~30s runtime. A hang past the timeout
    // (or an elapsed time near 30s) would mean the child outlived its group.
    // The exit code of a job-killed process is platform-dependent (Windows can
    // report 0), so promptness — not the code — is the guarantee under test.
    let start = Instant::now();
    let _exit = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("child outlived its group — kill-on-close did not fire")
        .expect("wait completed");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child was not reaped promptly (took {:?})",
        start.elapsed()
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real process tree; proves a grandchild is contained (race fix)"]
async fn windows_grandchild_is_contained() {
    // A parent that launches a detached grandchild which records its own PID and
    // then sleeps ~30s; the parent exits as soon as the grandchild is launched.
    // Before the CREATE_SUSPENDED fix the grandchild could be created in the
    // spawn→assign window and escape the job; now the parent runs suspended until
    // it is in the job, so whatever it spawns is contained too. Dropping the
    // group must therefore reap the grandchild, not just the parent.
    //
    // Two small .ps1 files avoid nested-quoting fragility: parent.ps1 launches
    // grandchild.ps1 via Start-Process (which returns immediately).
    let tmp = std::env::temp_dir();
    let tag = std::process::id();
    let pidfile = tmp.join(format!("processkit_gc_{tag}.pid"));
    let grandchild_ps1 = tmp.join(format!("processkit_gc_{tag}.ps1"));
    let parent_ps1 = tmp.join(format!("processkit_parent_{tag}.ps1"));
    let _ = std::fs::remove_file(&pidfile);

    std::fs::write(
        &grandchild_ps1,
        format!(
            "$PID | Set-Content -Encoding ascii '{}'\nStart-Sleep -Seconds 30\n",
            pidfile.display()
        ),
    )
    .expect("write grandchild script");
    std::fs::write(
        &parent_ps1,
        format!(
            "Start-Process -WindowStyle Hidden -FilePath powershell \
             -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','{}'\n",
            grandchild_ps1.display()
        ),
    )
    .expect("write parent script");

    let group = ProcessGroup::new().expect("create group");
    group
        .start(&Command::new("powershell").args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &parent_ps1.to_string_lossy(),
        ]))
        .await
        .expect("spawn parent")
        .wait()
        .await
        .expect("parent waits"); // parent exits promptly after launching grandchild

    // Wait for the grandchild to publish its PID.
    let mut grandchild_pid = None;
    poll_until(
        Duration::from_secs(5),
        Duration::from_millis(100),
        "grandchild never recorded its PID",
        || {
            if let Ok(text) = std::fs::read_to_string(&pidfile)
                && let Ok(pid) = text.trim().parse::<u32>()
            {
                grandchild_pid = Some(pid);
                true
            } else {
                false
            }
        },
    )
    .await;
    let pid = grandchild_pid.expect("grandchild never recorded its PID");
    assert!(
        windows_pid_alive(pid),
        "grandchild should be alive before drop"
    );

    drop(group); // kill-on-close must reap the whole tree, grandchild included

    // Give the job a moment to tear the tree down.
    let mut reaped = false;
    for _ in 0..50 {
        if !windows_pid_alive(pid) {
            reaped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = std::fs::remove_file(&pidfile);
    let _ = std::fs::remove_file(&grandchild_ps1);
    let _ = std::fs::remove_file(&parent_ps1);
    assert!(
        reaped,
        "grandchild {pid} outlived its job — containment leaked"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a setsid child that forks a grandchild; proves pgroup containment reaches it"]
async fn unix_setsid_child_forks_grandchild_still_contained() {
    use processkit::Command;

    // Best-effort boundary of `Mechanism::ProcessGroup`. A child spawned under
    // `.setsid()` leads a *new session and process group* (pgid == its pid),
    // which the group tracks. If that child forks a grandchild before exiting,
    // the grandchild INHERITS the session's process group — it did not `setsid`
    // away itself — so `killpg(pgid)` on drop still reaches it even after the
    // direct child is gone. The documented pgroup escape hatch is a process that
    // calls `setsid` *itself*, not one that merely inherits the session. (Under
    // the Linux cgroup mechanism the grandchild is contained a fortiori — it
    // never leaves the cgroup — so this asserts the *weaker* fallback's boundary
    // explicitly, and holds on every unix backend.)
    let tmp = std::env::temp_dir();
    let pidfile = tmp.join(format!("processkit_setsid_gc_{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);

    // The direct child forks a grandchild (`sleep 30`) that records its own pid
    // (`$!` — the backgrounded job), then exits at once, orphaning the grandchild
    // while it stays inside the tracked session process group.
    let group = ProcessGroup::new().expect("create group");
    let child = group
        .start(
            &Command::new("sh")
                .args(["-c", "sleep 30 & echo $! > \"$PK_PIDFILE\"; exit 0"])
                .env("PK_PIDFILE", &pidfile)
                .setsid(),
        )
        .await
        .expect("setsid child spawns (EPERM would mean the pgroup coordination broke)");
    // Reap the direct child so it is genuinely gone, not a lingering zombie that
    // could keep the group probe answering; it exits promptly after the fork.
    completes_within(Duration::from_secs(10), "direct child exit", child.wait())
        .await
        .expect("direct child waits");

    // The grandchild publishes its pid.
    let mut gc_pid = None;
    poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        "grandchild never recorded its pid",
        || {
            if let Ok(text) = std::fs::read_to_string(&pidfile)
                && let Ok(pid) = text.trim().parse::<i32>()
            {
                gc_pid = Some(pid);
                true
            } else {
                false
            }
        },
    )
    .await;
    let gc = gc_pid.expect("grandchild recorded its pid");
    // SAFETY: signal 0 is a sound liveness probe.
    assert!(
        unsafe { libc::kill(gc, 0) } == 0,
        "grandchild {gc} should be alive before the group is dropped"
    );

    drop(group); // killpg the session pgroup — must reach the inherited grandchild

    // The grandchild must die: poll until its pid stops answering the liveness
    // probe (SIGKILL'd, then reaped by init as an orphan).
    poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        "grandchild outlived the group drop — inherited-session containment leaked",
        // SAFETY: signal 0 is a sound liveness probe.
        || unsafe { libc::kill(gc, 0) } != 0,
    )
    .await;
    let _ = std::fs::remove_file(&pidfile);
}

#[tokio::test]
#[ignore = "spawns a real subprocess and kills it twice"]
async fn kill_all_is_idempotent() {
    let group = ProcessGroup::new().expect("create group");
    let child = group.start(&sleep_secs(30)).await.expect("start sleeper");

    group.kill_all().expect("first kill");
    group
        .kill_all()
        .expect("second kill must be a no-op success, not an error");

    // The group stays usable after teardown: a fresh spawn still lands in it.
    // On Windows, `CreateProcess` of a binary just killed via `TerminateJobObject`
    // can transiently fail with `ERROR_ACCESS_DENIED` — the dying image stays
    // briefly locked (the exiting process, or Defender re-scanning it), so
    // re-spawning the *same* binary right away occasionally "Access is denied."s.
    // That is orthogonal to the job-reusability this asserts, and the crate
    // (rightly) treats `PermissionDenied` as permanent for a real launch, so we
    // retry the transient here rather than in the library.
    let mut again = None;
    for attempt in 0..20u32 {
        match group.start(&sleep_secs(1)).await {
            Ok(run) => {
                again = Some(run);
                break;
            }
            Err(e) if e.is_permission_denied() && attempt < 19 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("group usable after terminate: {e:?}"),
        }
    }
    let again = again.expect("group usable after terminate (after transient retries)");
    drop(again);
    let _ = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("child reaped");
}

/// Regression: tearing down a process group whose only member is an unreaped
/// **zombie** must report success, not a false `EPERM`. On the process-group
/// mechanism (macOS/BSD, and the Linux pgroup fallback) `killpg` against such a
/// group returns `EPERM` on macOS/BSD — indistinguishable, from the errno alone,
/// from a genuinely-alive uid-changed child that rejects the signal. A first
/// attempt to surface that `EPERM` was reverted precisely because it falsely failed
/// this normal case; `kill_all` now checks the leader's run state and swallows the
/// harmless zombie `EPERM`, so this teardown must be `Ok`.
///
/// The raw [`ProcessGroup::spawn`](processkit::ProcessGroup::spawn) path hands back
/// the tokio `Child`, so the test owns reaping: the child exits at once but is never
/// `wait`ed until after the teardown, so it lingers as the unreaped zombie the group
/// still tracks. `kill_all` returns `Ok` on every backend here (a zombie is already
/// dead), so the assertion cannot flake — it fails only if the reverted false `EPERM`
/// returns.
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a fast-exiting child and tears the group down while it is a zombie"]
async fn kill_all_on_a_zombie_only_group_reports_success() {
    use std::process::Stdio;

    use processkit::ProcessGroup;
    use tokio::io::AsyncReadExt as _;
    use tokio::process::Command as TokioCommand;

    let group = ProcessGroup::new().expect("create group");
    let mut cmd = TokioCommand::new("sh");
    cmd.arg("-c").arg("exit 0");
    // A piped stdout gives a deterministic exit oracle (EOF on close) without
    // reaping; silence stderr so a failing shell can't spam the harness.
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::null());
    let mut child = group.spawn(cmd).expect("spawn fast-exiting child");
    let pid = child.id().expect("child pid") as i32;

    // Wait for the child to exit WITHOUT reaping it: its stdout pipe closes on
    // exit, so a read to EOF proves it is gone-but-unreaped (a zombie the group
    // still tracks). We never `wait` it before the teardown, so nothing reaps it.
    let mut out = child.stdout.take().expect("piped stdout handle");
    let mut sink = Vec::new();
    completes_within(
        Duration::from_secs(10),
        "child exit (stdout EOF)",
        out.read_to_end(&mut sink),
    )
    .await
    .expect("read child stdout to EOF");
    // Still present as an unreaped zombie (a zombie answers signal 0), not gone.
    // SAFETY: signal 0 is a sound liveness probe.
    assert!(
        unsafe { libc::kill(pid, 0) } == 0,
        "the exited child must still exist as an unreaped zombie"
    );

    // The load-bearing assertion: a zombie-only group's teardown reports success.
    group
        .kill_all()
        .expect("kill_all of a zombie-only group must succeed, not raise a false EPERM");

    // Reap the zombie so the test leaves nothing behind.
    let _ = completes_within(Duration::from_secs(10), "zombie reap", child.wait()).await;
}

/// Nested Job Objects (Windows): a crate `ProcessGroup` built by a process that is
/// **itself** already inside another Job Object. This pins the real
/// agent-orchestrator topology — Windows Terminal, CI runners and IDE agents all
/// put the parent process in a job — where a silently-degraded
/// `AssignProcessToJobObject` (the historical pre-Windows-8 / hostile-outer-job
/// failure) would leave the crate's containment broken with nobody noticing.
/// Covers scenario 7 of the Orchestra containment request.
///
/// Shape (self-re-exec helper, the same trick `stdin_inherit.rs` uses). The
/// harness creates an **outer** job — `KILL_ON_JOB_CLOSE`, and deliberately *no*
/// breakaway flag — then re-executes this very integration-test binary in
/// "helper mode" (an env-gated `#[ignore]` test), assigns that fresh helper
/// process into the outer job, and only then releases it. Now nested, the helper
/// builds the crate's own `ProcessGroup` (an **inner** job), starts a child that
/// launches a grandchild, and publishes their PIDs through files — the pidfile +
/// poll pattern of [`windows_grandchild_is_contained`], never a fixed sleep. The
/// helper is a real child process distinct from the harness, so tearing the outer
/// job down with kill-on-close (scenario (c)) never reaches the harness itself.
#[cfg(windows)]
mod nested_job {
    use std::os::windows::io::AsRawHandle;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use processkit::{Command, Mechanism, ProcessGroup};

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectExtendedLimitInformation, SetInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    use crate::common::{poll_until, windows_pid_alive};

    /// Marker env var: when set, this binary runs as the re-exec'd helper below
    /// instead of an ordinary test. Unset in a normal `--include-ignored` suite
    /// run, so `job_helper_process` is then just an immediate no-op pass.
    const HELPER_FLAG: &str = "PK_NESTED_JOB_HELPER";
    /// Temp dir + unique tag handed to the helper so both sides derive the exact
    /// same coordination-file paths without passing one env var per file.
    const HELPER_DIR: &str = "PK_NESTED_JOB_DIR";
    const HELPER_TAG: &str = "PK_NESTED_JOB_TAG";
    /// The libtest name the harness re-execs (positional filter + `--exact`). Keep
    /// in sync with the module path and `fn job_helper_process` below — a mismatch
    /// surfaces loudly as `wait_ready` timing out with no ready/error file.
    const HELPER_TEST: &str = "groups::nested_job::job_helper_process";

    /// Every coordination-file path, derived identically on the harness and the
    /// re-exec'd helper from a shared temp dir + unique tag.
    struct Paths {
        dir: PathBuf,
        tag: String,
        /// Harness → helper: written *after* the helper is inside the outer job, so
        /// the helper begins its in-the-nesting work only once contained.
        go: PathBuf,
        /// Helper → harness: `helper_pid`/`child_pid`/`grandchild_pid`/`mechanism`.
        ready: PathBuf,
        /// Helper → harness: a hard-failure message (loud, not a silent degrade).
        error: PathBuf,
        /// Harness → helper: tear your own inner group down with `kill_all`.
        kill: PathBuf,
        /// Helper → harness: `kill_all` returned.
        killed: PathBuf,
        /// Harness → helper: you may exit cleanly now.
        exit: PathBuf,
        /// Grandchild → everyone: its own PID (written by the grandchild script).
        gc_pidfile: PathBuf,
        gc_ps1: PathBuf,
        parent_ps1: PathBuf,
    }

    impl Paths {
        fn new(dir: &Path, tag: &str) -> Self {
            let at = |name: &str| dir.join(format!("processkit_nested_{tag}_{name}"));
            Self {
                dir: dir.to_path_buf(),
                tag: tag.to_string(),
                go: at("go"),
                ready: at("ready"),
                error: at("error"),
                kill: at("kill"),
                killed: at("killed"),
                exit: at("exit"),
                gc_pidfile: at("gc.pid"),
                gc_ps1: at("gc.ps1"),
                parent_ps1: at("parent.ps1"),
            }
        }

        fn cleanup(&self) {
            for p in [
                &self.go,
                &self.ready,
                &self.error,
                &self.kill,
                &self.killed,
                &self.exit,
                &self.gc_pidfile,
                &self.gc_ps1,
                &self.parent_ps1,
            ] {
                let _ = std::fs::remove_file(p);
            }
            // The `ready` atomic-write temp, if a crash ever left one behind.
            let _ = std::fs::remove_file(self.ready.with_extension("tmp"));
        }
    }

    /// A RAII **outer** Job Object: `KILL_ON_JOB_CLOSE`, no breakaway. Dropping it
    /// closes the last handle, so kill-on-close reaps every member — both the
    /// cleanup backstop on any panic path and the exact mechanism scenario (c)
    /// exercises.
    struct OuterJob {
        handle: HANDLE,
    }

    // Sound: the guard is the sole owner of the handle and every Win32 job API is
    // thread-safe (mirrors the `unsafe impl Send for Job` in src/sys/windows.rs).
    unsafe impl Send for OuterJob {}

    impl OuterJob {
        fn create() -> OuterJob {
            // SAFETY: null name/attributes request an unnamed job with defaults.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            assert!(
                !handle.is_null(),
                "CreateJobObjectW failed: {}",
                std::io::Error::last_os_error()
            );
            // Kill-on-close with *no* breakaway flag: members (and their nested
            // children) die when this last handle closes and cannot escape — the
            // hostile outer job an orchestrator's parent process already lives in.
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: fully-initialised struct matching the info class; size passed
            // explicitly. On failure the guard is not built, so no handle leaks
            // (we close it here before panicking).
            let ok = unsafe {
                SetInformationJobObject(
                    handle,
                    JobObjectExtendedLimitInformation,
                    std::ptr::from_ref(&info).cast(),
                    std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                )
            };
            if ok == 0 {
                let err = std::io::Error::last_os_error();
                // SAFETY: handle came from CreateJobObjectW; closed exactly once.
                unsafe { CloseHandle(handle) };
                panic!("SetInformationJobObject failed: {err}");
            }
            OuterJob { handle }
        }

        /// Assign an already-running process into this job (the documented `adopt`
        /// shape). `false` on failure — the caller reports the OS error loudly.
        fn assign(&self, process: HANDLE) -> bool {
            // SAFETY: both handles are valid for the duration of the call.
            unsafe { AssignProcessToJobObject(self.handle, process) != 0 }
        }
    }

    impl Drop for OuterJob {
        fn drop(&mut self) {
            // Closing the last handle triggers KILL_ON_JOB_CLOSE, reaping every
            // member: the panic-path backstop and scenario (c)'s teardown.
            // SAFETY: handle came from CreateJobObjectW; closed exactly once.
            unsafe { CloseHandle(self.handle) };
        }
    }

    /// Whether the current process is inside *any* Job Object — the nesting
    /// premise the helper must confirm before its result means anything.
    fn current_process_in_a_job() -> bool {
        let mut in_job: i32 = 0;
        // SAFETY: GetCurrentProcess returns a pseudo-handle; a null job argument
        // asks "in ANY job?"; `in_job` is a valid BOOL out-param.
        let ok = unsafe { IsProcessInJob(GetCurrentProcess(), std::ptr::null_mut(), &mut in_job) };
        ok != 0 && in_job != 0
    }

    /// The PIDs the helper publishes once its nested tree is up.
    struct Ready {
        helper_pid: u32,
        child_pid: u32,
        grandchild_pid: u32,
    }

    impl Ready {
        fn parse(text: &str) -> Option<Ready> {
            let mut helper_pid = None;
            let mut child_pid = None;
            let mut grandchild_pid = None;
            for line in text.lines() {
                let Some((key, val)) = line.split_once('=') else {
                    continue;
                };
                let val = val.trim();
                match key.trim() {
                    "helper_pid" => helper_pid = val.parse().ok(),
                    "child_pid" => child_pid = val.parse().ok(),
                    "grandchild_pid" => grandchild_pid = val.parse().ok(),
                    _ => {}
                }
            }
            Some(Ready {
                helper_pid: helper_pid?,
                child_pid: child_pid?,
                grandchild_pid: grandchild_pid?,
            })
        }
    }

    static TAG_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A per-run unique tag so the two harness tests (which libtest may run in
    /// parallel) never collide on coordination files.
    fn unique_tag(kind: &str) -> String {
        format!(
            "{}_{kind}_{}",
            std::process::id(),
            TAG_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Re-exec this test binary as the nested-job helper (raw `std` spawn: we need
    /// the process handle to assign it into the outer job ourselves, and it must
    /// *not* be wrapped in any crate group).
    fn spawn_helper(paths: &Paths) -> std::process::Child {
        let exe = std::env::current_exe().expect("locate the integration-test binary");
        std::process::Command::new(exe)
            .args([HELPER_TEST, "--exact", "--ignored"])
            .env(HELPER_FLAG, "1")
            .env(HELPER_DIR, &paths.dir)
            .env(HELPER_TAG, &paths.tag)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("re-exec this test binary in nested-job helper mode")
    }

    /// Create the outer job, spawn the helper, nest it into the outer job, release
    /// it, and wait for it to publish its PIDs. Returns the still-open outer job
    /// (as a guard) and the published PIDs.
    async fn setup_nested(kind: &str) -> (Paths, OuterJob, Ready) {
        let paths = Paths::new(&std::env::temp_dir(), &unique_tag(kind));
        paths.cleanup();

        let outer = OuterJob::create();

        let child = spawn_helper(&paths);
        // Assign the (idling) helper into the outer job *before* releasing it, so
        // every process it later creates nests inside the outer job too.
        let raw = child.as_raw_handle() as HANDLE;
        assert!(
            outer.assign(raw),
            "AssignProcessToJobObject(outer job, helper) failed: {} — nested Job \
             Objects appear unsupported on this host (pre-Windows 8, or a \
             breakaway-forbidding outer job that rejects re-nesting)",
            std::io::Error::last_os_error()
        );
        // Release the helper now that it is contained, then drop our handle to it:
        // a later OpenProcess-by-pid liveness probe must see the process gone once
        // it dies, not kept openable by a handle we still hold.
        std::fs::write(&paths.go, b"1").expect("write the go signal");
        drop(child);

        let ready = wait_ready(&paths, Duration::from_secs(40)).await;
        (paths, outer, ready)
    }

    /// Wait for the helper's `ready` file (or fail loudly on its `error` file /
    /// timeout).
    async fn wait_ready(paths: &Paths, timeout: Duration) -> Ready {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(err) = std::fs::read_to_string(&paths.error) {
                let err = err.trim();
                if !err.is_empty() {
                    panic!("nested-job helper reported a hard failure: {err}");
                }
            }
            if let Ok(text) = std::fs::read_to_string(&paths.ready)
                && let Some(ready) = Ready::parse(&text)
            {
                return ready;
            }
            assert!(
                Instant::now() < deadline,
                "nested-job helper never became ready within {timeout:?} (no ready/error \
                 file — the re-exec filter may not have matched `{HELPER_TEST}`)"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // --- Harness tests -----------------------------------------------------

    /// (a)+(b): from inside the outer job the helper establishes the crate's
    /// containment (`mechanism == JobObject`) and a live child+grandchild, then a
    /// `kill_all` on its own inner group reaps that child and grandchild while
    /// leaving the helper itself — not a member of its own job — alive.
    #[tokio::test]
    #[ignore = "re-execs a helper inside an outer job; proves nested spawn/assign + kill_all"]
    async fn windows_nested_job_assign_and_kill_all() {
        let (paths, outer, ready) = setup_nested("kill").await;

        // (a) The helper got past `group.start` from inside the outer job (assign
        //     did not fail) — otherwise it would have failed loudly and `wait_ready`
        //     would have panicked with that message instead of returning here.
        assert!(
            windows_pid_alive(ready.helper_pid),
            "helper should be alive after publishing"
        );
        assert!(
            windows_pid_alive(ready.child_pid),
            "child should be alive before kill_all"
        );
        assert!(
            windows_pid_alive(ready.grandchild_pid),
            "grandchild should be alive before kill_all"
        );

        // (b) Ask the helper to `kill_all` its inner group.
        std::fs::write(&paths.kill, b"1").expect("write the kill signal");
        poll_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            "helper never confirmed kill_all",
            || paths.killed.exists(),
        )
        .await;

        poll_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            "child outlived kill_all — inner-job containment leaked",
            || !windows_pid_alive(ready.child_pid),
        )
        .await;
        poll_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            "grandchild outlived kill_all — nested containment did not reach it",
            || !windows_pid_alive(ready.grandchild_pid),
        )
        .await;

        assert!(
            windows_pid_alive(ready.helper_pid),
            "kill_all on the inner group must not reap the helper itself (it is not a \
             member of its own job)"
        );

        // Let the helper exit cleanly — it stayed alive to the end of the scenario.
        std::fs::write(&paths.exit, b"1").expect("write the exit signal");
        poll_until(
            Duration::from_secs(15),
            Duration::from_millis(100),
            "helper did not exit after the exit signal",
            || !windows_pid_alive(ready.helper_pid),
        )
        .await;

        drop(outer); // close the outer job (cleanup; nothing left to reap)
        paths.cleanup();
    }

    /// (c): closing the outer job's last handle must cascade `KILL_ON_JOB_CLOSE`
    /// through the nesting and reap the helper **and** its whole subtree. The
    /// helper does not touch its own group here — the entire tree rides on the
    /// outer job.
    #[tokio::test]
    #[ignore = "re-execs a helper subtree inside an outer job; proves closing the job reaps it all"]
    async fn windows_nested_job_outer_close_reaps_tree() {
        let (paths, outer, ready) = setup_nested("close").await;

        assert!(
            windows_pid_alive(ready.helper_pid),
            "helper should be alive before the close"
        );
        assert!(
            windows_pid_alive(ready.child_pid),
            "child should be alive before the close"
        );
        assert!(
            windows_pid_alive(ready.grandchild_pid),
            "grandchild should be alive before the close"
        );

        drop(outer); // close the outer job's last handle → kill-on-close cascades

        poll_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            "helper outlived the outer-job close — kill-on-close did not reach it through the nesting",
            || !windows_pid_alive(ready.helper_pid),
        )
        .await;
        poll_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            "child outlived the outer-job close",
            || !windows_pid_alive(ready.child_pid),
        )
        .await;
        poll_until(
            Duration::from_secs(20),
            Duration::from_millis(100),
            "grandchild outlived the outer-job close",
            || !windows_pid_alive(ready.grandchild_pid),
        )
        .await;

        // This harness process is not a member of the outer job, so it is untouched.
        paths.cleanup();
    }

    // --- Helper (re-exec'd) ------------------------------------------------

    /// The re-exec'd helper. Env-gated: unset in an ordinary suite run, so this is
    /// then an immediate no-op pass; only the harness tests above re-exec the
    /// binary with `PK_NESTED_JOB_HELPER` set, at which point it runs the
    /// in-the-nesting workload.
    #[tokio::test]
    #[ignore = "re-exec target: runs its workload only when the harness sets PK_NESTED_JOB_HELPER"]
    async fn job_helper_process() {
        if std::env::var_os(HELPER_FLAG).is_none() {
            return;
        }
        let dir = PathBuf::from(std::env::var(HELPER_DIR).expect("helper dir env"));
        let tag = std::env::var(HELPER_TAG).expect("helper tag env");
        let paths = Paths::new(&dir, &tag);
        if let Err(msg) = run_helper(&paths).await {
            // Publish the failure so the harness surfaces it loudly, then fail this
            // process too (a silent degrade is exactly what this test guards against).
            let _ = std::fs::write(&paths.error, &msg);
            panic!("nested-job helper failed: {msg}");
        }
    }

    async fn run_helper(paths: &Paths) -> Result<(), String> {
        // 1. Wait until the harness has nested us into the outer job.
        wait_for_file(
            &paths.go,
            Duration::from_secs(30),
            "outer-job assignment (go signal)",
        )
        .await?;

        // 2. Confirm the nesting premise actually holds.
        if !current_process_in_a_job() {
            return Err(
                "helper is not inside any Job Object after the harness assign — \
                        the nesting premise is unmet, so this would not test nesting"
                    .into(),
            );
        }

        // 3. Build the crate's ProcessGroup *inside* the outer job (a nested job).
        let group = ProcessGroup::new()
            .map_err(|e| format!("ProcessGroup::new failed inside the outer job: {e}"))?;
        let mechanism = group.mechanism();
        if mechanism != Mechanism::JobObject {
            return Err(format!(
                "expected the JobObject mechanism inside the nesting, got {mechanism:?} — \
                 containment silently degraded"
            ));
        }

        // 4. Scripts: a child that launches a detached grandchild; both record their
        //    PID and idle ~120s (well past the harness's death-detection windows), so
        //    kill_all / job-close always has live members to reap.
        std::fs::write(
            &paths.gc_ps1,
            format!(
                "$PID | Set-Content -Encoding ascii '{}'\nStart-Sleep -Seconds 120\n",
                paths.gc_pidfile.display()
            ),
        )
        .map_err(|e| format!("write grandchild script: {e}"))?;
        std::fs::write(
            &paths.parent_ps1,
            format!(
                "Start-Process -WindowStyle Hidden -FilePath powershell \
                 -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','{}'\n\
                 Start-Sleep -Seconds 120\n",
                paths.gc_ps1.display()
            ),
        )
        .map_err(|e| format!("write parent script: {e}"))?;

        // 5. (a) spawn + AssignProcessToJobObject into the INNER job must not fail —
        //    this is the crate establishing containment from inside the outer job.
        let run = group
            .start(&Command::new("powershell").args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                &paths.parent_ps1.to_string_lossy(),
            ]))
            .await
            .map_err(|e| {
                format!(
                    "group.start (spawn + AssignProcessToJobObject) failed inside the outer \
                     job — nested containment could not be established: {e}"
                )
            })?;
        let child_pid = run
            .pid()
            .ok_or("the child reported no pid right after spawn")?;

        // 6. The grandchild publishes its PID.
        let grandchild_pid =
            wait_for_pid(&paths.gc_pidfile, Duration::from_secs(30), "grandchild pid").await?;

        // 7. Publish everything for the harness (atomic: temp + rename, so a
        //    concurrent read never sees a half-written file).
        let helper_pid = std::process::id();
        write_atomic(
            &paths.ready,
            format!(
                "helper_pid={helper_pid}\nchild_pid={child_pid}\n\
                 grandchild_pid={grandchild_pid}\nmechanism={mechanism:?}\n"
            ),
        )
        .map_err(|e| format!("publish ready file: {e}"))?;

        // 8. The harness's next move:
        //    - (b) a `kill` signal → tear our own inner group down with kill_all
        //      (reaps child+grandchild, not us — we never joined our own job), mark
        //      it done, then idle until the harness lets us exit cleanly;
        //    - (c) no signal → the harness closes the outer job and kill-on-close
        //      terminates us mid-wait. The generous deadline (>> the harness's death
        //      windows) is only a safety net so a harness bug can't strand the helper
        //      *and* can't be mistaken for the job close having worked.
        if wait_for_file_opt(&paths.kill, Duration::from_secs(90)).await {
            group
                .kill_all()
                .map_err(|e| format!("group.kill_all failed: {e}"))?;
            std::fs::write(&paths.killed, b"1").map_err(|e| format!("write killed marker: {e}"))?;
            // Release the now-terminated child's handle so its pid stops being
            // openable and the harness's OpenProcess-by-pid probe reports it dead —
            // an open handle we still held would keep the process object (and thus
            // its pid) alive. (The grandchild has no handle held here, so it frees
            // on its own once reaped.)
            drop(run);
            wait_for_file_opt(&paths.exit, Duration::from_secs(90)).await;
        } else {
            // Scenario (c): held `run` alive so the inner job kept a live member
            // right up to the outer-job close that terminates us mid-wait; this
            // else is only the safety-net fall-through.
            drop(run);
        }

        // `group` is intentionally held until here; it drops at end of scope. The
        // helper never joined its own inner job, so holding it does not keep the
        // helper's pid alive for the harness's probe.
        drop(group);
        Ok(())
    }

    async fn wait_for_file(path: &Path, timeout: Duration, what: &str) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            if path.exists() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("{what} did not arrive within {timeout:?}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Like [`wait_for_file`] but returns whether the file appeared, rather than
    /// failing — for the optional `kill`/`exit` triggers.
    async fn wait_for_file_opt(path: &Path, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if path.exists() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_for_pid(path: &Path, timeout: Duration, what: &str) -> Result<u32, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(text) = std::fs::read_to_string(path)
                && let Ok(pid) = text.trim().parse::<u32>()
            {
                return Ok(pid);
            }
            if Instant::now() >= deadline {
                return Err(format!("{what} was not published within {timeout:?}"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Write `content` to `path` atomically (temp file + rename) so a concurrent
    /// reader never observes a partially written file.
    fn write_atomic(path: &Path, content: String) -> std::io::Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, path)
    }
}
