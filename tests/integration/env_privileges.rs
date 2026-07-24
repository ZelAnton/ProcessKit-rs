//! Environment and privilege builders: inherit_env, uid/gid, setsid,
//! CPU/I/O priority, umask, and the Windows-only/unix-only unsupported gates.

#[cfg(unix)]
use std::time::{Duration, Instant};

use processkit::Command;
#[cfg(target_os = "linux")]
use processkit::IoPriority;
#[cfg(unix)]
use processkit::Mechanism;
use processkit::Priority;
use processkit::ProcessGroup;

use crate::common::*;

#[tokio::test]
#[ignore = "spawns real subprocesses to compare environments"]
async fn inherit_env_whitelists_parent_env() {
    // Without a whitelist, an explicit marker (and the inherited env) shows up.
    let with_marker = print_env()
        .env("PK_ITEM8_MARKER", "present")
        .output_string()
        .await
        .expect("run env printer");
    assert!(with_marker.is_success());
    assert!(
        with_marker.stdout().contains("PK_ITEM8_MARKER"),
        "explicit env should reach the child"
    );

    // With an allow-list, only the named vars survive: PATH present (needed to
    // even find the shell on unix), the marker absent (never set explicitly,
    // and the inherited env was cleared).
    let whitelisted = print_env()
        .inherit_env(if cfg!(windows) {
            // cmd.exe needs SystemRoot to run at all.
            vec!["PATH", "SystemRoot"]
        } else {
            vec!["PATH"]
        })
        .output_string()
        .await
        .expect("run env printer");
    assert!(whitelisted.is_success(), "result: {whitelisted:?}");
    assert!(
        whitelisted.stdout().to_uppercase().contains("PATH="),
        "whitelisted PATH should be present: {:?}",
        whitelisted.stdout()
    );
    assert!(
        !whitelisted.stdout().contains("PK_ITEM8_MARKER"),
        "non-whitelisted vars must not leak"
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess to compare environments"]
async fn inherit_env_matches_windows_names_case_insensitively() {
    // Pins a Windows guarantee the allow-list inherits from the OS: the
    // parent lookup goes through `GetEnvironmentVariableW`, which is
    // case-insensitive — `inherit_env(["path"])` must copy `Path`/`PATH`
    // whatever the canonical spelling is. (duct's gotchas list flags env-name
    // casing as a classic Windows trap; this is the regression guard.)
    let result = print_env()
        .inherit_env(["path", "systemroot"]) // deliberately the "wrong" case
        .output_string()
        .await
        .expect("run env printer");
    assert!(result.is_success(), "result: {result:?}");
    assert!(
        result.stdout().to_uppercase().contains("PATH="),
        "lowercase allow-list entry must still copy PATH: {:?}",
        result.stdout()
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess in a new session"]
async fn setsid_spawns_and_stays_contained() {
    // THE regression test for the setsid × process-group coordination: with
    // setpgid applied before pre_exec hooks, setsid would fail EPERM and the
    // spawn would error. It must succeed on every unix mechanism…
    let group = ProcessGroup::new().expect("create group");
    let process = group
        .start(&sleep_secs(30).setsid())
        .await
        .expect("setsid child spawns (EPERM would mean the pgroup coordination broke)");
    let pid = process.pid().expect("pid") as i32;

    // …and the new session's process group must still be contained: dropping
    // the group kills the child. Reap it via wait() — a raw pid probe would
    // see the unreaped zombie as alive forever (the handle holds the child).
    drop(group);
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("setsid child outlived the group drop — containment broke")
        .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "setsid child was not reaped promptly (took {:?})",
        start.elapsed()
    );
    // Reaped: the pid is genuinely gone, not a lingering zombie.
    // SAFETY: signal 0 is a sound liveness probe.
    assert!(
        unsafe { libc::kill(pid, 0) != 0 },
        "pid still probes alive after reap"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns real subprocesses to check nice/umask"]
async fn priority_and_umask_apply_before_exec() {
    // Priority: read this child's own nice value back via `ps` (portable
    // across the Linux/macOS/BSD `ps` this crate already targets) — proves
    // `setpriority` landed in pre_exec before the shell execs. No root
    // needed: raising niceness (BelowNormal) never requires a privilege.
    let out = Command::new("sh")
        .args(["-c", "ps -o nice= -p $$"])
        .priority(Priority::BelowNormal)
        .run()
        .await
        .expect("run priority child");
    assert_eq!(
        out.trim().parse::<i32>().expect("ps prints an integer"),
        10,
        "Priority::BelowNormal must map to nice(10)"
    );

    // umask: the shell builtin reports the mask verbatim; parse as octal so
    // the assertion doesn't depend on a shell's exact zero-padding.
    let out = Command::new("sh")
        .args(["-c", "umask"])
        .umask(0o027)
        .run()
        .await
        .expect("run umask child");
    assert_eq!(
        u32::from_str_radix(out.trim(), 8).expect("umask prints octal"),
        0o027,
        "the requested umask must be visible inside the child"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "spawns a real subprocess to check ioprio_set"]
async fn io_priority_applies_before_exec() {
    // `ionice -p $$` reads the Linux kernel's actual I/O priority for this
    // shell, so this proves `ioprio_set` ran in pre_exec before `sh` execed.
    // Idle is available to an unprivileged process and is unambiguous in the
    // tool's output.
    let out = Command::new("sh")
        .args(["-c", "ionice -p $$"])
        .io_priority(IoPriority::Idle)
        .run()
        .await
        .expect("run I/O-priority child");
    assert_eq!(
        out.trim(),
        "idle",
        "the child must retain idle I/O priority"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "drops privileges; meaningful only as root"]
async fn uid_gid_drop_privileges() {
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: privilege drop requires root");
        return;
    }
    let result = Command::new("id").arg("-u").uid(1).gid(1).run().await;
    match ProcessGroup::new().expect("probe group").mechanism() {
        // Documented caveat: under the cgroup mechanism the cgroup join runs
        // after the uid drop and fails with a permission error — the spawn
        // must error, never hand back an uncontained or wrongly-privileged
        // child.
        Mechanism::CgroupV2 => {
            assert!(
                result.is_err(),
                "uid drop on the cgroup mechanism is documented to fail the \
                 spawn, got {result:?}"
            );
        }
        _ => {
            let out = result.expect("run id -u as uid 1");
            assert_eq!(out.trim(), "1", "child should report the dropped uid");
        }
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "raises priority and drops privileges; meaningful only as root"]
async fn priority_high_with_uid_drop_and_no_groups_succeeds() {
    // Regression test for the raise-then-drop ordering guarantee on the
    // `None` (no `groups`) branch: `Priority::High` needs CAP_SYS_NICE, which
    // only the pre-drop (root) process has. Before the fix, the `None`
    // branch let std's own `.uid()`/`.gid()` builder methods perform the drop
    // — those apply *before* any user pre_exec hook, including the priority
    // hook — so this exact combination failed with `ErrorReason::Spawn` (EPERM from
    // setpriority under the already-dropped uid). It must now succeed,
    // identically to the `groups`-present path.
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: privilege drop requires root");
        return;
    }
    let result = Command::new("sh")
        .args(["-c", "ps -o nice= -p $$; id -u"])
        .priority(Priority::High)
        .uid(1)
        .gid(1)
        .run()
        .await;
    match ProcessGroup::new().expect("probe group").mechanism() {
        // Same documented cgroup caveat as plain uid/gid drop: the cgroup
        // join runs after the uid drop and fails EPERM.
        Mechanism::CgroupV2 => {
            assert!(
                result.is_err(),
                "uid drop on the cgroup mechanism is documented to fail the \
                 spawn, got {result:?}"
            );
        }
        _ => {
            let out = result.expect("run priority+uid child");
            let mut lines = out.lines();
            let nice: i32 = lines
                .next()
                .expect("ps output line")
                .trim()
                .parse()
                .expect("ps prints an integer");
            assert_eq!(nice, -10, "Priority::High must map to nice(-10)");
            let uid = lines.next().expect("id -u output line").trim();
            assert_eq!(uid, "1", "child should report the dropped uid");
        }
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "drops privileges; meaningful only as root"]
async fn uid_gid_drop_without_groups_clears_supplementary_groups() {
    // No `.groups(...)` call here — this exercises the `None` branch's
    // manual pre_exec drop, which must reproduce std's own
    // `setgroups(0, ...)` cleanup of supplementary groups before
    // setgid/setuid, not just setgid+setuid (otherwise the child would keep
    // root's supplementary groups — a privilege leak).
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: privilege drop requires root");
        return;
    }
    let result = Command::new("id").arg("-G").uid(1).gid(1).run().await;
    match ProcessGroup::new().expect("probe group").mechanism() {
        Mechanism::CgroupV2 => {
            assert!(
                result.is_err(),
                "uid drop on the cgroup mechanism is documented to fail the \
                 spawn, got {result:?}"
            );
        }
        _ => {
            let out = result.expect("run id -G as uid/gid 1");
            let ids: std::collections::HashSet<&str> = out.split_whitespace().collect();
            assert_eq!(
                ids,
                std::collections::HashSet::from(["1"]),
                "supplementary groups must be cleared, leaving only the \
                 dropped gid: id -G = {out:?}"
            );
        }
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "sets supplementary groups; meaningful only as root"]
async fn groups_set_supplementary_groups() {
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: setting supplementary groups requires root");
        return;
    }
    // setgroups replaces the inherited set; `id -G` lists the egid plus the
    // supplementary groups. No uid drop here, so the cgroup join (written as
    // root) still succeeds on every mechanism — this isolates the setgroups
    // pre_exec from the documented uid-vs-cgroup caveat.
    let out = Command::new("id")
        .arg("-G")
        .groups([1, 2])
        .run()
        .await
        .expect("run id -G with supplementary groups set");
    let ids: std::collections::HashSet<&str> = out.split_whitespace().collect();
    assert!(
        ids.contains("1") && ids.contains("2"),
        "the requested supplementary groups should be present: id -G = {out:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "drops privileges with supplementary groups; meaningful only as root"]
async fn groups_with_uid_drop_respects_the_cgroup_caveat() {
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: privilege drop requires root");
        return;
    }
    // With `groups` present the whole drop (setgroups → setgid → setuid) runs in
    // one pre_exec that *precedes* the cgroup-join hook — so the documented
    // uid×cgroup caveat must apply exactly as it does for uid alone: the spawn
    // fails under cgroup v2 (join as the dropped uid is refused) and succeeds,
    // with the uid dropped, on the process-group mechanism.
    let result = Command::new("id")
        .arg("-u")
        .uid(1)
        .gid(1)
        .groups([1])
        .run()
        .await;
    match ProcessGroup::new().expect("probe group").mechanism() {
        Mechanism::CgroupV2 => assert!(
            result.is_err(),
            "uid drop with groups on the cgroup mechanism must fail the spawn, got {result:?}"
        ),
        _ => {
            let out = result.expect("run id -u as uid 1 with groups");
            assert_eq!(out.trim(), "1", "child should report the dropped uid");
        }
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "exercises the non-unix unsupported gate"]
async fn windows_unix_only_builders_are_unsupported() {
    for (command, what) in [
        (Command::new("cmd").args(["/c", "exit 0"]).uid(1000), "uid"),
        (Command::new("cmd").args(["/c", "exit 0"]).gid(1000), "gid"),
        (
            Command::new("cmd").args(["/c", "exit 0"]).groups([1000]),
            "groups",
        ),
        (
            Command::new("cmd").args(["/c", "exit 0"]).setsid(),
            "setsid",
        ),
        (
            Command::new("cmd").args(["/c", "exit 0"]).umask(0o022),
            "umask",
        ),
        (
            Command::new("cmd")
                .args(["/c", "exit 0"])
                .io_priority(processkit::IoPriority::Idle),
            "io_priority",
        ),
    ] {
        let err = command
            .output_string()
            .await
            .expect_err("a privilege request must not be silently skipped");
        assert!(
            matches!(err.reason(), processkit::ErrorReason::Unsupported { .. }),
            "expected Unsupported for {what}, got {err:?}"
        );
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess with a non-default priority class"]
async fn windows_priority_is_never_unsupported_and_spawns() {
    // Unlike the privilege builders above, `priority` is implemented on
    // Windows too (a priority-class creation flag) — it must never be
    // gated as Unsupported, and the run must actually succeed.
    let result = Command::new("cmd")
        .args(["/c", "exit 0"])
        .priority(Priority::BelowNormal)
        .output_string()
        .await
        .expect("a requested priority must spawn, not error");
    assert!(result.is_success(), "result: {result:?}");
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess with CREATE_NO_WINDOW under a job"]
async fn windows_create_no_window_spawns_in_group() {
    // Window absence isn't assertable headlessly; what this proves is that the
    // extra flag is OR'd with (not clobbering) CREATE_SUSPENDED containment.
    let group = ProcessGroup::new().expect("create group");
    let process = group
        .start(&two_line_echo().create_no_window())
        .await
        .expect("spawn with CREATE_NO_WINDOW");
    let result = process.output_string().await.expect("collect");
    assert!(result.is_success(), "result: {result:?}");
    assert!(result.stdout().contains("first"));
}
