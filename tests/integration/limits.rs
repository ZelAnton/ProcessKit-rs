//! Resource limits (memory / process count / CPU) — `limits`-gated via the
//! `mod` declaration in `main.rs`.

#[cfg(windows)]
use processkit::Command;
use processkit::{Error, Mechanism, ProcessGroup, ProcessGroupOptions};

#[tokio::test]
#[ignore = "creates an OS job/cgroup with a resource limit"]
async fn limits_are_enforced_or_rejected_per_platform() {
    // Setting a limit must either be honored by a real container (Windows Job
    // Object / Linux cgroup) or fail fast with `Error::ResourceLimit` — never
    // silently hand back an unbounded group.
    let res =
        ProcessGroup::with_options(ProcessGroupOptions::default().memory_max(64 * 1024 * 1024));
    if cfg!(windows) {
        let group = res.expect("Windows Job Objects enforce a memory cap");
        assert!(matches!(group.mechanism(), Mechanism::JobObject));
    } else if cfg!(target_os = "linux") {
        match res {
            Ok(group) => assert!(matches!(group.mechanism(), Mechanism::CgroupV2)),
            // Common on dev boxes / CI without cgroup delegation — the fail-fast path.
            Err(Error::ResourceLimit(_)) => {
                eprintln!("skipping cgroup enforcement: controller delegation unavailable");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    } else {
        // macOS/BSD and the no-containment target have no whole-tree cap.
        assert!(
            matches!(res, Err(Error::ResourceLimit(_))),
            "a limit on a container-less target must be rejected, not silently dropped"
        );
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns real subprocesses to prove the active-process cap is enforced"]
async fn windows_process_count_limit_is_enforced() {
    // A single-process sleeper keeps the accounting unambiguous (one process per
    // start), so `max_processes(1)` admits the first and must refuse the second.
    let one_proc_sleeper = || Command::new("ping").args(["-n", "30", "127.0.0.1"]);

    let group = ProcessGroup::with_options(ProcessGroupOptions::default().max_processes(1))
        .expect("create capped group");
    assert!(matches!(group.mechanism(), Mechanism::JobObject));

    let _first = group
        .start(&one_proc_sleeper())
        .await
        .expect("first child fits the cap");
    let second = group.start(&one_proc_sleeper()).await;
    assert!(
        second.is_err(),
        "a second process must not be admitted past max_processes(1)"
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "creates a capped Job Object and runs a small child within it"]
async fn windows_memory_and_cpu_limits_accept_and_run() {
    // A generous memory cap plus a half-core CPU cap must be accepted by the job
    // (both SetInformationJobObject calls succeed) and must not break an ordinary
    // short-lived child.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .memory_max(512 * 1024 * 1024)
            .cpu_quota(0.5),
    )
    .expect("create capped group");
    assert!(matches!(group.mechanism(), Mechanism::JobObject));

    let out = group
        .start(&Command::new("cmd").args(["/c", "echo hi"]))
        .await
        .expect("spawn small child")
        .output_string()
        .await
        .expect("collect");
    assert!(out.is_success(), "exit {:?}", out.code());
    assert!(out.stdout().contains("hi"), "stdout: {:?}", out.stdout());
}
