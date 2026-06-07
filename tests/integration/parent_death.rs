//! `kill_on_parent_death`: the direct child dies when its spawner goes away
//! abruptly — no `Drop` involved (Linux `PR_SET_PDEATHSIG`). Linux-only:
//! Windows gets the whole-tree version from the kernel for free (the job
//! handle closes with the process), macOS/BSD have no equivalent.
//!
//! The death signal is tied to the spawning *thread* (the documented caveat),
//! which is exactly what makes it testable in-process: spawn from a dedicated
//! thread, `mem::forget` the handle so kill-on-drop can't interfere, and let
//! the thread die.

use std::time::Duration;

use processkit::Command;

/// Whether `pid` is still alive (`kill(pid, 0)` succeeds or fails `EPERM`).
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
