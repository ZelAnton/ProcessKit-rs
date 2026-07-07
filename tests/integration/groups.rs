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
