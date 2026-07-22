//! Resource limits (memory / process count / CPU) — `limits`-gated via the
//! `mod` declaration in `main.rs`.

#[cfg(windows)]
use processkit::Command;
use processkit::{
    Error, LimitKind, LimitReason, Mechanism, ProcessGroup, ProcessGroupOptions, ResourceLimits,
};

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

#[tokio::test]
#[ignore = "creates an OS job/cgroup and reconfigures its resource limits"]
async fn update_limits_applies_or_refuses_per_platform() {
    // A live group starts unbounded; `update_limits` must then either be honored by
    // a real container (Windows Job Object / Linux cgroup) or fail fast with
    // `Error::ResourceLimit` — never silently leave the tree unbounded — exactly as
    // requesting the cap at creation does.
    let mut group = ProcessGroup::new().expect("create group");
    let mut limits = ResourceLimits::default();
    limits.max_memory = Some(64 * 1024 * 1024);
    let res = group.update_limits(limits);
    if cfg!(windows) {
        res.expect("Windows Job Objects enforce a memory cap on a live job");
        assert!(matches!(group.mechanism(), Mechanism::JobObject));
    } else if cfg!(target_os = "linux") {
        // Branch on the active mechanism: a delegated cgroup either applies the cap
        // (at the real hierarchy root) or reports `Unenforceable`; the pgroup
        // fallback (no usable cgroup) has no accounting at all — `Unsupported`.
        match (group.mechanism(), res) {
            (Mechanism::CgroupV2, Ok(())) => {}
            (Mechanism::CgroupV2, Err(Error::ResourceLimit { kind, reason, .. })) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unenforceable);
                eprintln!("cgroup present but controllers can't be enabled off the real root");
            }
            (Mechanism::ProcessGroup, Err(Error::ResourceLimit { kind, reason, .. })) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unsupported);
                eprintln!("no usable cgroup — the fallback has no whole-tree accounting");
            }
            (mech, other) => panic!("unexpected mechanism/result: {mech:?} / {other:?}"),
        }
    } else {
        // macOS/BSD: no whole-tree cap mechanism exists at all — `Unsupported`.
        match res {
            Err(Error::ResourceLimit { kind, reason, .. }) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unsupported);
            }
            other => panic!(
                "a live-group limit on a container-less mechanism must be rejected, not silently dropped: {other:?}"
            ),
        }
    }
}

#[tokio::test]
#[ignore = "creates an OS job/cgroup and reconfigures it after a graceful teardown"]
async fn update_limits_reuses_validation_and_survives_teardown() {
    // `update_limits` reuses `validate_limits`, so a nonsensical value is rejected
    // with the offending axis and `reason: Invalid` before the OS/backend is
    // touched — regardless of the active mechanism.
    let mut group = ProcessGroup::new().expect("create group");
    let mut bad = ResourceLimits::default();
    bad.max_memory = Some(0);
    match group.update_limits(bad) {
        Err(Error::ResourceLimit { kind, reason, .. }) => {
            assert_eq!(kind, LimitKind::Memory);
            assert_eq!(reason, LimitReason::Invalid);
        }
        other => panic!("an invalid value must be rejected as Invalid, got {other:?}"),
    }

    // Lifting every cap (all-`None`) is a trivial success on every mechanism — the
    // tree is unbounded either way, so "remove all limits" is always applicable.
    group
        .update_limits(ResourceLimits::default())
        .expect("lifting all caps must succeed on every mechanism");

    // The lifecycle gate: `update_limits` routes through the same live handle/cgroup
    // the tree-control verbs use, so it stays usable after a non-consuming graceful
    // teardown (the container outlives `shutdown_ref`; only the consuming `shutdown`
    // / `Drop` retires it, after which the group is gone by ownership). It must not
    // panic or touch a freed handle here.
    group.shutdown_ref().await.expect("graceful shutdown");
    let _ = group.update_limits(ResourceLimits::default());
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "reconfigures a live Windows Job Object and runs children under the replaced caps"]
async fn windows_update_limits_full_replacement_reissues_and_clears() {
    // Create with memory + CPU caps, then *replace* with a process-count-only set:
    // memory and CPU are left `None`, so full-replacement semantics must LIFT them,
    // and the freshly-applied `max_processes(1)` must take effect on the live job.
    let mut group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .max_memory(512 * 1024 * 1024)
            .cpu_quota(0.5),
    )
    .expect("create capped group");
    assert!(matches!(group.mechanism(), Mechanism::JobObject));

    let mut replacement = ResourceLimits::default();
    replacement.max_processes = Some(1);
    group
        .update_limits(replacement)
        .expect("reissue caps on the live job");

    // A single-process sleeper keeps the accounting unambiguous: the first admits,
    // the second must be refused past the newly-applied `max_processes(1)` — proving
    // the replacement took effect on the already-created job.
    let one_proc = || Command::new("ping").args(["-n", "30", "127.0.0.1"]);
    let _first = group
        .start(&one_proc())
        .await
        .expect("first child fits the newly-applied max_processes(1)");
    let second = group.start(&one_proc()).await;
    assert!(
        second.is_err(),
        "a second process must be refused past the replaced-in max_processes(1)"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "rewrites cgroup limit files on a live group; the cgroup leg is meaningful only with delegation"]
async fn linux_update_limits_rewrites_cgroup_or_is_observably_refused() {
    // Whichever Linux mechanism is active, a limited `update_limits` is either
    // applied (cgroup at the real root) or fails fast with a typed error — and an
    // all-`None` update (lift everything) always succeeds. Never a silent no-op.
    let mut group = ProcessGroup::new().expect("create group");
    let mut limits = ResourceLimits::default();
    limits.max_processes = Some(32);
    match group.mechanism() {
        Mechanism::CgroupV2 => match group.update_limits(limits) {
            Ok(()) => {
                // Full replacement back to unbounded must also succeed (writes the
                // `*.max` files back to `max`).
                group
                    .update_limits(ResourceLimits::default())
                    .expect("lifting all caps on the cgroup must succeed");
            }
            Err(Error::ResourceLimit {
                kind,
                reason: LimitReason::Unenforceable,
                ..
            }) => {
                assert_eq!(kind, LimitKind::Processes);
                eprintln!(
                    "cgroup controllers can't be enabled off the real root — refused, not silent"
                );
            }
            other => panic!("unexpected cgroup update result: {other:?}"),
        },
        Mechanism::ProcessGroup => {
            match group.update_limits(limits) {
                Err(Error::ResourceLimit { kind, reason, .. }) => {
                    assert_eq!(kind, LimitKind::Processes);
                    assert_eq!(reason, LimitReason::Unsupported);
                }
                other => panic!("the pgroup fallback must refuse a limited update: {other:?}"),
            }
            group
                .update_limits(ResourceLimits::default())
                .expect("lifting all caps on the pgroup fallback is a no-op success");
        }
        other => panic!("unexpected linux mechanism: {other:?}"),
    }
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
