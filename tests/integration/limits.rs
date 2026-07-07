//! Resource limits (memory / process count / CPU) — `limits`-gated via the
//! `mod` declaration in `main.rs`.

#[cfg(windows)]
use processkit::Command;
use processkit::{Error, LimitKind, LimitReason, Mechanism, ProcessGroup, ProcessGroupOptions};

#[tokio::test]
#[ignore = "creates an OS job/cgroup with a resource limit"]
async fn limits_are_enforced_or_rejected_per_platform() {
    // Setting a limit must either be honored by a real container (Windows Job
    // Object / Linux cgroup) or fail fast with `Error::ResourceLimit` — never
    // silently hand back an unbounded group.
    let res =
        ProcessGroup::with_options(ProcessGroupOptions::default().max_memory(64 * 1024 * 1024));
    if cfg!(windows) {
        let group = res.expect("Windows Job Objects enforce a memory cap");
        assert!(matches!(group.mechanism(), Mechanism::JobObject));
    } else if cfg!(target_os = "linux") {
        match res {
            Ok(group) => assert!(matches!(group.mechanism(), Mechanism::CgroupV2)),
            // Common on dev boxes / CI without cgroup delegation — the fail-fast
            // path. A capable mechanism (cgroup v2 is mounted) exists here; this
            // *specific* request just couldn't be applied — `Unenforceable`, not
            // `Unsupported`.
            Err(Error::ResourceLimit { kind, reason, .. }) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unenforceable);
                eprintln!("skipping cgroup enforcement: controller delegation unavailable");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    } else {
        // macOS/BSD have no whole-tree cap at all — `Unsupported`, not
        // `Unenforceable` (no mechanism exists to even attempt this against).
        match res {
            Err(Error::ResourceLimit { kind, reason, .. }) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unsupported);
            }
            other => panic!(
                "a limit on a container-less mechanism must be rejected, not silently dropped: {other:?}"
            ),
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "creates a group and asserts the cgroup→pgroup fallback contains a real child"]
async fn linux_cgroup_or_pgroup_fallback_is_observable_and_contains() {
    use std::time::{Duration, Instant};

    use crate::common::{completes_within, sleeper};

    // The cgroup→process-group downgrade (no cgroup v2, no delegation, a
    // read-only `/sys/fs/cgroup`, an unprivileged container) must be OBSERVABLE
    // — never a silently uncontained group: `mechanism()` is always one of the
    // two valid Linux values. And whichever mechanism is active, kill-on-drop
    // must still reap a real child. Run non-root without cgroup delegation, this
    // exercises the fallback; with delegation, the primary cgroup path.
    let group = ProcessGroup::new().expect("create group");
    let mech = group.mechanism();
    assert!(
        matches!(mech, Mechanism::CgroupV2 | Mechanism::ProcessGroup),
        "linux mechanism must be cgroup v2 or its pgroup fallback, got {mech:?}"
    );
    if matches!(mech, Mechanism::ProcessGroup) {
        eprintln!("cgroup delegation unavailable — exercising the process-group fallback");
    } else {
        eprintln!("cgroup v2 delegation available — exercising the primary mechanism");
    }

    // Containment holds under the active mechanism: a long sleeper spawned into a
    // *shared* group (the handle does not own it) is reaped promptly when the
    // group drops, far sooner than its ~30s natural runtime.
    let child = group.start(&sleeper()).await.expect("spawn sleeper");
    assert!(
        child.pid().is_some(),
        "sleeper should report a pid after spawn"
    );

    drop(group);
    let start = Instant::now();
    completes_within(
        Duration::from_secs(10),
        "child reap after group drop (kill-on-drop under the active mechanism)",
        child.wait(),
    )
    .await
    .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child was not reaped promptly under {mech:?} (took {:?})",
        start.elapsed()
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "drops privileges under the cgroup mechanism; meaningful only as root with cgroup delegation"]
async fn linux_uid_drop_under_cgroup_fails_the_spawn() {
    use processkit::Command;

    // The documented cgroup×uid incompatibility. Under `Mechanism::CgroupV2` the
    // child joins its cgroup by writing the auto-created (root-owned)
    // `cgroup.procs` *after* the OS has dropped the uid, so the join is refused
    // and the spawn FAILS — rather than handing back an uncontained (or
    // wrongly-privileged) child. Under the process-group fallback a uid drop
    // composes cleanly, so this failure path is cgroup-specific.
    // SAFETY: geteuid is a pure query.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipping: privilege drop requires root");
        return;
    }
    let group = ProcessGroup::new().expect("create group");
    if !matches!(group.mechanism(), Mechanism::CgroupV2) {
        eprintln!(
            "skipping: the cgroup×uid failure path needs the cgroup mechanism \
             (the process-group fallback composes with a uid drop)"
        );
        return;
    }
    let result = group
        .start(&Command::new("id").arg("-u").uid(1).gid(1))
        .await;
    assert!(
        result.is_err(),
        "a uid drop under the cgroup mechanism must fail the spawn (joining the \
         root-owned cgroup.procs as the dropped uid is refused), got {result:?}"
    );
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
            .max_memory(512 * 1024 * 1024)
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
