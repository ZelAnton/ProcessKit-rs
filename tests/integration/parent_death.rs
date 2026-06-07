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
    // any Drop running.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while pid_alive(pid) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !pid_alive(pid),
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
    // Clean up the deliberately-leaked sleeper before asserting.
    // SAFETY: pid was alive a moment ago and belongs to our leaked child.
    unsafe { libc::kill(pid, libc::SIGKILL) };
    assert!(
        alive,
        "unarmed child {pid} must outlive its spawning thread"
    );
}
