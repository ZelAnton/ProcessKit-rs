//! `kill_on_parent_death`: the direct child dies when its spawner goes away
//! abruptly — no `Drop` involved (Linux `PR_SET_PDEATHSIG`). Linux-only:
//! Windows gets the whole-tree version from the kernel for free (the job
//! handle closes with the process), macOS/BSD have no equivalent.
//!
//! Two layers of coverage live here:
//!
//! - **`thread_pdeathsig` (Linux)** — the death signal is tied to the spawning
//!   *thread* (the documented caveat), which is exactly what makes it testable
//!   in-process: spawn from a dedicated thread, `mem::forget` the handle so
//!   kill-on-drop can't interfere, and let the thread die.
//! - **`owner_death` (Linux + macOS)** — the real end-to-end contract: a
//!   separate *owner* process spawns a child and a grandchild, is force-killed
//!   from **outside** (`SIGKILL`, so no `Drop` runs), and the cleanup scope is
//!   verified against the honest report from
//!   [`Command::kill_on_parent_death_scope`](processkit::Command::kill_on_parent_death_scope)
//!   — `DirectChildOnly` on Linux (the child dies, the grandchild survives),
//!   `Unsupported` on macOS (both survive).

// --- Linux in-process pdeathsig (thread-scoped) ---------------------------

#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
use processkit::Command;

/// Whether `pid` is still alive (`kill(pid, 0)` succeeds or fails `EPERM`).
#[cfg(target_os = "linux")]
fn pid_alive(pid: i32) -> bool {
    // SAFETY: signal 0 probes existence without sending anything.
    let probed = unsafe { libc::kill(pid, 0) };
    probed == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Whether our direct (forgotten-handle) child `pid` has exited — reaping it
/// if so. A bare `kill(pid, 0)` probe would see the unreaped zombie as alive
/// forever (nobody `wait()`s a forgotten handle, and the kernel's PDEATHSIG
/// kill reaps nothing) — the same trap the crate's own pgroup `Tracked` and
/// the `setsid` test document.
#[cfg(target_os = "linux")]
fn reaped_or_gone(pid: i32) -> bool {
    let mut status = 0i32;
    // SAFETY: WNOHANG never blocks; `pid` is this process's own child.
    let reaped = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    // `pid` = exited and reaped just now; `-1` (ECHILD) = already gone;
    // `0` = still running.
    reaped == pid || reaped == -1
}

/// Spawn a long sleeper on a dedicated thread (current-thread runtime, so the
/// fork happens *on* that thread), leak every handle so no `Drop` can kill
/// it, and return its pid after the spawning thread has fully exited.
#[cfg(target_os = "linux")]
fn spawn_leaked_from_short_lived_thread(armed: bool) -> i32 {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let pid = rt.block_on(async {
            let mut cmd = Command::new("sleep").arg("300");
            if armed {
                cmd = cmd.kill_on_parent_death();
            }
            let process = cmd.start().await.expect("spawn sleeper");
            let pid = process.pid().expect("sleeper pid") as i32;
            // Suppress the baseline kill-on-drop guarantee (handle + private
            // group leak — including, on the cgroup mechanism, its directory):
            // what remains is exactly the knob under test.
            std::mem::forget(process);
            pid
        });
        drop(rt);
        pid
    })
    .join()
    .expect("spawner thread")
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "leaks a real containment group to isolate the pdeathsig knob"]
async fn dead_spawner_takes_its_armed_child_down() {
    let pid = spawn_leaked_from_short_lived_thread(true);

    // The spawning thread is gone; PDEATHSIG must SIGKILL the child without
    // any Drop running. Probe via waitpid (not kill(pid,0)): the kernel kill
    // leaves a zombie only we can reap.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !reaped_or_gone(pid) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        reaped_or_gone(pid),
        "armed child {pid} must die with its spawning thread"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "leaks a real containment group to isolate the pdeathsig knob"]
async fn dead_spawner_leaves_an_unarmed_child_alive() {
    // The control: without the knob, the leaked child survives its spawner —
    // proving the test above observes pdeathsig, not some other teardown.
    let pid = spawn_leaked_from_short_lived_thread(false);

    tokio::time::sleep(Duration::from_secs(1)).await;
    let alive = pid_alive(pid);
    // Clean up the deliberately-leaked sleeper before asserting: kill AND
    // reap (a bare kill would leave a zombie for the test process's lifetime).
    // SAFETY: pid belongs to our leaked child; blocking waitpid returns
    // immediately after SIGKILL.
    unsafe {
        libc::kill(pid, libc::SIGKILL);
        let mut status = 0i32;
        libc::waitpid(pid, &mut status, 0);
    }
    assert!(
        alive,
        "unarmed child {pid} must outlive its spawning thread"
    );
}

// --- End-to-end: an *owner* process is force-killed from outside -----------

/// The real contract, tested across a process boundary: a separate owner
/// process spawns a child (with `kill_on_parent_death`) and a grandchild, the
/// harness force-kills the owner with `SIGKILL` (so its `Drop` never runs —
/// the owner is dead by the time we check), and the observed cleanup scope is
/// asserted against the platform's honest `ParentDeathCleanup` report:
///
/// - **Linux** (`DirectChildOnly`) — the direct child dies (PDEATHSIG); the
///   grandchild survives (nothing tears the leaked cgroup/pgroup down).
/// - **macOS** (`Unsupported`) — both survive (no `pdeathsig` equivalent).
///
/// Uses the self-re-exec harness pattern (see `groups::nested_job`): the owner
/// is this same test binary run with an env marker, coordinating over files in
/// a temp dir.
mod owner_death {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use processkit::{Command, ParentDeathCleanup};

    /// Marker env var: set only when this binary is re-exec'd as the owner.
    /// Unset in an ordinary `--include-ignored` run, so `owner_process` is then
    /// an immediate no-op pass.
    const OWNER_FLAG: &str = "PK_PDEATH_OWNER";
    /// Temp dir + unique tag handed to the owner so both sides derive the same
    /// coordination-file paths.
    const OWNER_DIR: &str = "PK_PDEATH_DIR";
    const OWNER_TAG: &str = "PK_PDEATH_TAG";
    /// The libtest name the harness re-execs (positional filter + `--exact`).
    /// Keep in sync with the module path and `fn owner_process` below — a
    /// mismatch surfaces as `wait_ready` timing out with no ready/error file.
    const OWNER_TEST: &str = "parent_death::owner_death::owner_process";

    static TAG_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A per-run unique tag so parallel test invocations never collide on
    /// coordination files.
    fn unique_tag() -> String {
        format!(
            "{}_{}",
            std::process::id(),
            TAG_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Coordination-file paths, derived identically on the harness and the
    /// re-exec'd owner from a shared temp dir + unique tag.
    struct Paths {
        /// Owner → harness: `owner_pid` / `child_pid` / `grandchild_pid`.
        ready: PathBuf,
        /// Owner → harness: a hard-failure message (loud, not a silent degrade).
        error: PathBuf,
        /// Grandchild → owner: the grandchild's own pid.
        gc_pidfile: PathBuf,
    }

    impl Paths {
        fn new(dir: &Path, tag: &str) -> Self {
            let at = |name: &str| dir.join(format!("processkit_pdeath_{tag}_{name}"));
            Self {
                ready: at("ready"),
                error: at("error"),
                gc_pidfile: at("gc.pid"),
            }
        }

        fn cleanup(&self) {
            for p in [&self.ready, &self.error, &self.gc_pidfile] {
                let _ = std::fs::remove_file(p);
            }
            let _ = std::fs::remove_file(self.ready.with_extension("tmp"));
        }
    }

    /// The pids the owner publishes once its child + grandchild are up.
    struct Ready {
        child_pid: u32,
        grandchild_pid: u32,
    }

    impl Ready {
        fn parse(text: &str) -> Option<Ready> {
            let mut child_pid = None;
            let mut grandchild_pid = None;
            for line in text.lines() {
                let Some((key, val)) = line.split_once('=') else {
                    continue;
                };
                match key.trim() {
                    "child_pid" => child_pid = val.trim().parse().ok(),
                    "grandchild_pid" => grandchild_pid = val.trim().parse().ok(),
                    _ => {}
                }
            }
            Some(Ready {
                child_pid: child_pid?,
                grandchild_pid: grandchild_pid?,
            })
        }
    }

    /// Whether `pid` still exists at all (`kill(pid, 0)`): `Ok`/`EPERM` = alive,
    /// `ESRCH` = gone. A `SIGKILL`ed-but-unreaped zombie still reads "alive"
    /// here — for the Linux case use [`is_running`], which excludes zombies.
    fn pid_alive(pid: u32) -> bool {
        // SAFETY: signal 0 probes existence without sending anything.
        let probed = unsafe { libc::kill(pid as i32, 0) };
        probed == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    /// Best-effort teardown of a pid we do **not** own (can't `wait()` it — it
    /// reparents to init, which reaps it). Idempotent.
    fn kill_pid(pid: u32) {
        // SAFETY: sending SIGKILL to a pid; harmless if already gone (ESRCH).
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
    }

    /// Linux: whether `pid` is a **live, non-zombie** process, read from
    /// `/proc/<pid>/stat`'s state field (the first token after the final `)`).
    /// A `SIGKILL`ed direct child briefly lingers as a zombie until init reaps
    /// it, and a bare `kill(pid, 0)` would report that zombie as alive — so the
    /// "was the child cleaned up?" check must exclude state `Z`.
    #[cfg(target_os = "linux")]
    fn is_running(pid: u32) -> bool {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false; // gone entirely
        };
        // The comm field can contain spaces/parens; the state is the first
        // whitespace token after the *last* ')'.
        let Some((_, after_comm)) = stat.rsplit_once(')') else {
            return false;
        };
        match after_comm.split_whitespace().next() {
            Some("Z") => false, // zombie: killed, awaiting reap
            Some(_) => true,    // R/S/D/T/…: alive and running
            None => false,
        }
    }

    /// Poll `cond` up to `max`, returning whether it became true (never panics,
    /// so cleanup after the call always runs).
    async fn poll_bool(max: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + max;
        loop {
            if cond() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Re-exec this test binary as the owner (raw `std` spawn: the owner must be
    /// a real separate process the harness can `SIGKILL`, not wrapped in any
    /// crate group).
    fn spawn_owner(dir: &Path, tag: &str) -> std::process::Child {
        let exe = std::env::current_exe().expect("locate the integration-test binary");
        std::process::Command::new(exe)
            .args([OWNER_TEST, "--exact", "--ignored"])
            .env(OWNER_FLAG, "1")
            .env(OWNER_DIR, dir)
            .env(OWNER_TAG, tag)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("re-exec this test binary in owner mode")
    }

    /// Wait for the owner's `ready` file (or fail loudly on its `error` file /
    /// timeout).
    async fn wait_ready(paths: &Paths, timeout: Duration) -> Ready {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(err) = std::fs::read_to_string(&paths.error) {
                let err = err.trim();
                if !err.is_empty() {
                    panic!("parent-death owner reported a hard failure: {err}");
                }
            }
            if let Ok(text) = std::fs::read_to_string(&paths.ready)
                && let Some(ready) = Ready::parse(&text)
            {
                return ready;
            }
            assert!(
                Instant::now() < deadline,
                "parent-death owner never became ready within {timeout:?} (no ready/error \
                 file — the re-exec filter may not have matched `{OWNER_TEST}`)"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The end-to-end test: spawn an owner + child + grandchild, `SIGKILL` the
    /// owner from outside, and assert the cleanup scope matches the platform's
    /// honest [`ParentDeathCleanup`] report.
    #[tokio::test]
    #[ignore = "re-execs a real owner process and force-kills it; asserts the parent-death cleanup scope"]
    async fn owner_sigkill_cleanup_matches_reported_scope() {
        let dir = std::env::temp_dir();
        let tag = unique_tag();
        let paths = Paths::new(&dir, &tag);
        paths.cleanup();

        // 1. Spawn the owner and wait until its child + grandchild are up.
        let mut owner = spawn_owner(&dir, &tag);
        let owner_pid = owner.id();
        let ready = wait_ready(&paths, Duration::from_secs(40)).await;

        // 2. Precondition: both live before we kill the owner.
        assert!(
            pid_alive(ready.child_pid),
            "child {} must be alive before the owner is killed",
            ready.child_pid
        );
        assert!(
            pid_alive(ready.grandchild_pid),
            "grandchild {} must be alive before the owner is killed",
            ready.grandchild_pid
        );

        // 3. Force-kill the owner from OUTSIDE (SIGKILL → no Drop runs), then
        //    reap it (we are its parent) so it doesn't linger as a zombie.
        owner.kill().expect("SIGKILL the owner");
        owner.wait().expect("reap the killed owner");
        assert!(
            poll_bool(Duration::from_secs(10), || !pid_alive(owner_pid)).await,
            "owner {owner_pid} must be gone after SIGKILL + reap"
        );

        // 4. Assert the observed scope matches the honest capability report.
        let scope = Command::kill_on_parent_death_scope();
        match scope {
            ParentDeathCleanup::DirectChildOnly => {
                // Linux: the direct child dies with the owner; the grandchild,
                // reparented to init, keeps running in the leaked group.
                #[cfg(target_os = "linux")]
                {
                    let child_cleaned =
                        poll_bool(Duration::from_secs(15), || !is_running(ready.child_pid)).await;
                    let grandchild_survived = is_running(ready.grandchild_pid);
                    // Tear the survivors down before asserting, so a failed
                    // assertion never leaks the tree.
                    kill_pid(ready.child_pid);
                    kill_pid(ready.grandchild_pid);
                    paths.cleanup();
                    assert!(
                        child_cleaned,
                        "direct child {} must die when the owner is SIGKILLed (PDEATHSIG)",
                        ready.child_pid
                    );
                    assert!(
                        grandchild_survived,
                        "grandchild {} must survive direct-child-only cleanup (nothing tears the \
                         leaked cgroup/pgroup down)",
                        ready.grandchild_pid
                    );
                }
            }
            ParentDeathCleanup::Unsupported => {
                // macOS / the BSDs: no pdeathsig equivalent, so the abrupt owner
                // death triggers no cleanup — both survive.
                tokio::time::sleep(Duration::from_secs(2)).await;
                let child_alive = pid_alive(ready.child_pid);
                let grandchild_alive = pid_alive(ready.grandchild_pid);
                kill_pid(ready.child_pid);
                kill_pid(ready.grandchild_pid);
                paths.cleanup();
                assert!(
                    child_alive,
                    "child {} must survive where parent-death cleanup is Unsupported",
                    ready.child_pid
                );
                assert!(
                    grandchild_alive,
                    "grandchild {} must survive too where parent-death cleanup is Unsupported",
                    ready.grandchild_pid
                );
            }
            other => {
                kill_pid(ready.child_pid);
                kill_pid(ready.grandchild_pid);
                paths.cleanup();
                panic!("unexpected parent-death cleanup scope on this unix target: {other:?}");
            }
        }
    }

    // --- Owner (re-exec'd) -------------------------------------------------

    /// The re-exec'd owner. Env-gated: an immediate no-op pass in an ordinary
    /// suite run; only the harness above re-execs the binary with
    /// `PK_PDEATH_OWNER` set, at which point it spawns the child + grandchild
    /// and idles until the harness kills it.
    #[tokio::test]
    #[ignore = "re-exec target: spawns child+grandchild only when the harness sets PK_PDEATH_OWNER"]
    async fn owner_process() {
        if std::env::var_os(OWNER_FLAG).is_none() {
            return;
        }
        let dir = PathBuf::from(std::env::var(OWNER_DIR).expect("owner dir env"));
        let tag = std::env::var(OWNER_TAG).expect("owner tag env");
        let paths = Paths::new(&dir, &tag);
        if let Err(msg) = run_owner(&paths).await {
            // Publish the failure so the harness surfaces it loudly, then fail
            // this process too.
            let _ = std::fs::write(&paths.error, &msg);
            panic!("parent-death owner failed: {msg}");
        }
    }

    async fn run_owner(paths: &Paths) -> Result<(), String> {
        // Direct child: fork a detached grandchild that records its pid and
        // idles, then `wait` so the direct child itself stays alive (with
        // PDEATHSIG armed by `kill_on_parent_death`). The grandchild is a child
        // of the direct child — a real depth-2 descendant of the owner.
        let gc = paths.gc_pidfile.display();
        let script = format!("sleep 300 & echo $! > '{gc}'; wait");
        let running = Command::new("sh")
            .args(["-c", &script])
            .kill_on_parent_death()
            .start()
            .await
            .map_err(|e| format!("start the direct child: {e}"))?;
        let child_pid = running.pid().ok_or("the direct child reported no pid")?;

        // The grandchild publishes its pid into the shared file.
        let grandchild_pid =
            wait_for_pid(&paths.gc_pidfile, Duration::from_secs(30), "grandchild pid").await?;

        // Publish everything for the harness (atomic: temp + rename, so a
        // concurrent read never sees a half-written file).
        let owner_pid = std::process::id();
        write_atomic(
            &paths.ready,
            format!(
                "owner_pid={owner_pid}\nchild_pid={child_pid}\ngrandchild_pid={grandchild_pid}\n"
            ),
        )
        .map_err(|e| format!("publish the ready file: {e}"))?;

        // Idle, holding `running` so its `Drop` never pre-empts the harness's
        // external SIGKILL. The harness kills us mid-sleep; the long deadline is
        // only a safety net so a harness bug can't strand us forever.
        tokio::time::sleep(Duration::from_secs(120)).await;
        drop(running);
        Ok(())
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
