//! `Command::spawn_detached`: the one deliberate opt-in escape from kill-on-drop
//! containment. Three ends of the contract are covered against real processes:
//!
//! - a **detached** child (a new session on Unix / not in this crate's Job Object
//!   on Windows) **survives** dropping its `DetachedChild` handle — the drop does
//!   nothing to it;
//! - an ordinary **contained** `start()` child is still **hard-killed** when its
//!   handle drops (kill-on-drop has not regressed); and
//! - a detached child's stdout can be sent to a **file** redirect (the only
//!   non-null stdio a detached child is allowed — never a pipe).
//!
//! Real subprocesses, so `#[ignore]`d like the rest of this suite. The detached
//! children are self-terminating with a bounded lifetime, and every one is killed
//! before the test returns; on Unix the library's private reaper owns the wait.

#[cfg(unix)]
use std::collections::HashSet;
use std::time::Duration;

use crate::common::{poll_until, sleep_secs, sleeper, two_line_echo};

// --- per-platform liveness + cleanup for a NON-crate-owned (detached) child ---

/// Whether `pid` is still alive. A detached child is not tracked by tokio or any
/// group of ours, so this is a bare existence probe.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // Signal 0 probes existence without sending anything: `Ok`/`EPERM` = alive.
    let probed = unsafe { libc::kill(pid as i32, 0) };
    probed == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    crate::common::windows_pid_alive(pid)
}

/// Terminate a detached child and observe it disappearing. This deliberately
/// observes liveness instead of calling `waitpid`: the reaper owns the only
/// wait-capable child handle on Unix.
#[cfg(unix)]
async fn terminate_and_observe_exit(pid: u32) {
    // SAFETY: `pid` came from a child spawned by this test; SIGKILL is used only
    // for bounded cleanup and has no effect once the child is gone.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "detached child cleanup",
        || !pid_alive(pid),
    )
    .await;
}

#[cfg(windows)]
async fn terminate_and_observe_exit(pid: u32) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess};
    // SAFETY: terminate access only; a null handle means the pid is already gone.
    let handle = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
    if !handle.is_null() {
        // SAFETY: `handle` came from `OpenProcess`; terminated then closed once.
        unsafe {
            TerminateProcess(handle, 1);
            CloseHandle(handle);
        }
    }
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "detached child cleanup",
        || !pid_alive(pid),
    )
    .await;
}

#[tokio::test]
#[ignore = "spawns a real detached child that outlives its owner handle"]
async fn detached_child_survives_dropping_its_handle() {
    // A self-terminating child with a bounded lifetime (CI-safe): it sleeps a few
    // seconds then exits on its own, so even if cleanup were skipped nothing lingers.
    // The DetachedChild handle is scoped so it is *dropped* before the liveness
    // check: dropping the owner handle must NOT kill the child — that is the whole
    // point of a deliberate detach. (A DetachedChild has no kill-on-drop glue by
    // design, so its drop is a no-op; letting it fall out of scope shows exactly
    // that, and contrasts with `contained_start_still_kills_on_drop`.)
    let pid = {
        let detached = sleep_secs(4)
            .spawn_detached()
            .expect("spawn a detached child");
        detached.pid()
    };

    // Well within the child's ~4s lifetime, and with the owner handle already
    // dropped, it must still be alive.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        pid_alive(pid),
        "detached child {pid} must survive dropping its DetachedChild handle"
    );

    terminate_and_observe_exit(pid).await;
}

#[tokio::test]
#[cfg(unix)]
#[ignore = "spawns repeated real detached children to prove Unix reaping"]
async fn repeated_short_lived_detached_children_are_reaped_while_owner_lives() {
    const CHILDREN: usize = 32;
    let mut pids = HashSet::with_capacity(CHILDREN);

    // The integration-test process stays alive for the complete loop. Each
    // child exits immediately, while the private reaper—not this test—owns the
    // wait operation. kill(pid, 0) reports zombies as existing, so observing
    // ESRCH after every launch proves the child was actually reaped.
    for index in 0..CHILDREN {
        let pid = {
            let detached = crate::common::failing_exit(0)
                .spawn_detached()
                .unwrap_or_else(|error| panic!("spawn short-lived child {index}: {error}"));
            let pid = detached.pid();
            assert!(
                pids.insert(pid),
                "pid {pid} was reused during the reaping loop"
            );
            pid
        };

        poll_until(
            Duration::from_secs(2),
            Duration::from_millis(10),
            "short-lived detached child to be reaped",
            || !pid_alive(pid),
        )
        .await;
    }
}

#[tokio::test]
#[ignore = "spawns a real contained child to prove kill-on-drop has not regressed"]
async fn contained_start_still_kills_on_drop() {
    // The control: an ordinary `start()` child stays inside a kill-on-drop group,
    // so dropping its handle hard-kills it — proving the detached path above is the
    // deliberate exception, not a general regression of containment.
    let running = sleeper().start().await.expect("start a contained child");
    let pid = running.pid().expect("the contained child has a pid");

    // Sanity: it is alive while the handle is held.
    assert!(
        pid_alive(pid),
        "contained child {pid} should be alive at start"
    );

    drop(running); // kill-on-drop backstop: synchronous SIGKILL / job kill

    // The kill is immediate; reaping is asynchronous (tokio / OS), and on Windows
    // the pid can stay briefly openable past exit (poll down to gone, don't assert
    // instantly).
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "contained child dies on handle drop",
        || !pid_alive(pid),
    )
    .await;
}

#[tokio::test]
#[ignore = "spawns a real detached child writing to a file redirect"]
async fn detached_child_can_redirect_stdout_to_a_file() {
    // A file redirect is the ONLY non-null stdio a detached child is allowed (a
    // pipe would deadlock it once the owner is gone). Prove the redirect is wired:
    // the child's stdout lands in the file.
    let dir = tempfile::tempdir().expect("temp dir");
    let log = dir.path().join("detached-out.log");

    let pid = two_line_echo()
        .stdout_file(&log)
        .spawn_detached()
        .expect("spawn a detached child with a file redirect")
        .pid();

    // The child writes and exits quickly; poll until its output reaches the file.
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "detached child's stdout reaches the file",
        || {
            std::fs::read_to_string(&log)
                .map(|s| s.contains("first"))
                .unwrap_or(false)
        },
    )
    .await;

    terminate_and_observe_exit(pid).await; // no-op if exited; the library reaps it
}
