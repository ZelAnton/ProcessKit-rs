//! Environment and privilege builders: inherit_env, uid/gid, setsid, and the
//! Windows-only/unix-only unsupported gates.

#[cfg(unix)]
use std::time::{Duration, Instant};

use processkit::Command;
#[cfg(unix)]
use processkit::Mechanism;
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

#[cfg(windows)]
#[tokio::test]
#[ignore = "exercises the non-unix unsupported gate"]
async fn windows_unix_only_builders_are_unsupported() {
    for (command, what) in [
        (Command::new("cmd").args(["/c", "exit 0"]).uid(1000), "uid"),
        (Command::new("cmd").args(["/c", "exit 0"]).gid(1000), "gid"),
        (
            Command::new("cmd").args(["/c", "exit 0"]).setsid(),
            "setsid",
        ),
    ] {
        let err = command
            .output_string()
            .await
            .expect_err("a privilege request must not be silently skipped");
        assert!(
            matches!(err, processkit::Error::Unsupported { .. }),
            "expected Unsupported for {what}, got {err:?}"
        );
    }
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
