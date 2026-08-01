//! Resource limits (memory / process count / CPU) — `limits`-gated via the
//! `mod` declaration in `main.rs`.

#[cfg(windows)]
use processkit::Command;
use processkit::{
    ErrorReason, LimitKind, LimitReason, LimitVerdict, Mechanism, ProcessGroup,
    ProcessGroupOptions, ResourceLimits,
};

/// Every limit axis, so an evidence assertion can sweep all three rather than
/// naming one and quietly leaving the others unchecked.
const ALL_KINDS: [LimitKind; 3] = [LimitKind::Memory, LimitKind::Processes, LimitKind::Cpu];

#[tokio::test]
#[ignore = "creates an OS job/cgroup with a resource limit"]
async fn limits_are_enforced_or_rejected_per_platform() {
    // Setting a limit must either be honored by a real container (Windows Job
    // Object / Linux cgroup) or fail fast with `ErrorReason::ResourceLimit` — never
    // silently hand back an unbounded group.
    let res =
        ProcessGroup::with_options(ProcessGroupOptions::default().max_memory(64 * 1024 * 1024));
    if cfg!(windows) {
        let group = res.expect("Windows Job Objects enforce a memory cap");
        assert!(matches!(group.mechanism(), Mechanism::JobObject));
    } else if cfg!(target_os = "linux") {
        match res.map_err(|e| e.into_reason()) {
            Ok(group) => assert!(matches!(group.mechanism(), Mechanism::CgroupV2)),
            // Common on dev boxes / CI without cgroup delegation — the fail-fast
            // path. A capable mechanism (cgroup v2 is mounted) exists here; this
            // *specific* request just couldn't be applied — `Unenforceable`, not
            // `Unsupported`.
            Err(ErrorReason::ResourceLimit { kind, reason, .. }) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unenforceable);
                eprintln!("skipping cgroup enforcement: controller delegation unavailable");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    } else {
        // macOS/BSD have no whole-tree cap at all — `Unsupported`, not
        // `Unenforceable` (no mechanism exists to even attempt this against).
        match res.map_err(|e| e.into_reason()) {
            Err(ErrorReason::ResourceLimit { kind, reason, .. }) => {
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
    // `ErrorReason::ResourceLimit` — never silently leave the tree unbounded — exactly as
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
        match (group.mechanism(), res.map_err(|e| e.into_reason())) {
            (Mechanism::CgroupV2, Ok(())) => {}
            (Mechanism::CgroupV2, Err(ErrorReason::ResourceLimit { kind, reason, .. })) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unenforceable);
                eprintln!("cgroup present but controllers can't be enabled off the real root");
            }
            (Mechanism::ProcessGroup, Err(ErrorReason::ResourceLimit { kind, reason, .. })) => {
                assert_eq!(kind, LimitKind::Memory);
                assert_eq!(reason, LimitReason::Unsupported);
                eprintln!("no usable cgroup — the fallback has no whole-tree accounting");
            }
            (mech, other) => panic!("unexpected mechanism/result: {mech:?} / {other:?}"),
        }
    } else {
        // macOS/BSD: no whole-tree cap mechanism exists at all — `Unsupported`.
        match res.map_err(|e| e.into_reason()) {
            Err(ErrorReason::ResourceLimit { kind, reason, .. }) => {
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
    match group.update_limits(bad).map_err(|e| e.into_reason()) {
        Err(ErrorReason::ResourceLimit { kind, reason, .. }) => {
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
            Err(e) if e.limit_reason() == Some(LimitReason::Unenforceable) => {
                assert_eq!(e.limit_kind(), Some(LimitKind::Processes));
                eprintln!(
                    "cgroup controllers can't be enabled off the real root — refused, not silent"
                );
            }
            other => panic!("unexpected cgroup update result: {other:?}"),
        },
        Mechanism::ProcessGroup => {
            match group.update_limits(limits) {
                Err(e) if e.limit_reason() == Some(LimitReason::Unsupported) => {
                    assert_eq!(e.limit_kind(), Some(LimitKind::Processes));
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

// ---------------------------------------------------------------------------
// Post-run limit evidence (`ProcessGroup::limit_evidence`) — did an APPLIED cap
// actually fire? The opposite side of `ErrorReason::ResourceLimit`, which says
// only why a REQUESTED cap could not be applied. Every verdict below must come
// from an authoritative kernel/OS counter; a `Tripped` inferred from an exit
// code or signal is exactly the false verdict this surface exists to prevent.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "creates a real OS container and reads its post-run limit evidence"]
async fn limit_evidence_is_honest_about_what_each_mechanism_can_prove() {
    // An uncapped group on every platform. The answer must be shaped by what the
    // active mechanism can actually witness — never a blanket "no".
    let group = ProcessGroup::new().expect("create group");
    let evidence = group.limit_evidence();
    match group.mechanism() {
        // No whole-tree resource accounting exists here at all (macOS/the BSDs, and
        // the Linux fallback with no usable cgroup v2), so every axis is an explicit
        // `Unknown`. This is the correct answer, not a degraded one: the mechanism
        // also refuses to carry a cap in the first place, so `Unknown` means "no
        // evidence apparatus", never "a cap may have fired unseen".
        Mechanism::ProcessGroup => {
            for kind in ALL_KINDS {
                assert_eq!(
                    evidence.verdict(kind),
                    LimitVerdict::Unknown,
                    "a mechanism without resource accounting must say so on {kind:?}, \
                     not silently report a 'no'"
                );
            }
        }
        // A real container with nothing capped: nothing could have fired, so a
        // decisive `NotTripped` — and no OS query is made to say it.
        Mechanism::CgroupV2 | Mechanism::JobObject => {
            for kind in ALL_KINDS {
                assert_eq!(
                    evidence.verdict(kind),
                    LimitVerdict::NotTripped,
                    "an axis that never carried a cap cannot have fired ({kind:?})"
                );
            }
        }
        other => panic!("unexpected mechanism: {other:?}"),
    }

    // Reading evidence is a pure read: the group is untouched and still tears down
    // normally afterwards.
    group
        .kill_all()
        .expect("teardown is unaffected by an evidence read");
}

/// Build a group whose caps the **cgroup** mechanism really applied, or skip.
///
/// On a dev box / CI runner without cgroup delegation at the real hierarchy root
/// the cap is refused up front (`ErrorReason::ResourceLimit`) — the documented
/// fail-fast path, already covered by `limits_are_enforced_or_rejected_per_platform`
/// — and there is no container to read evidence from, so these tests skip rather
/// than false-fail.
#[cfg(target_os = "linux")]
fn capped_cgroup_or_skip(options: ProcessGroupOptions) -> Option<ProcessGroup> {
    match ProcessGroup::with_options(options) {
        Ok(group) if matches!(group.mechanism(), Mechanism::CgroupV2) => Some(group),
        Ok(group) => panic!(
            "a group that accepted a cap must be running the cgroup mechanism, got {:?}",
            group.mechanism()
        ),
        Err(e) if e.limit_reason().is_some() => {
            eprintln!("skipping cgroup limit-evidence test: the cap was refused ({e})");
            None
        }
        Err(other) => panic!("unexpected error creating a capped group: {other:?}"),
    }
}

/// Take swap off the table for the cgroup owning `pid`, returning whether it
/// worked.
///
/// `memory.max` caps **memory, not memory + swap**: on a host with swap enabled
/// (`memory.swap.max` defaults to `max`) the kernel pages a hog out instead of
/// OOM-killing it, so the cap engages without ever firing — measured on an 8 GiB-swap
/// host, where a 64 MiB hog under a 32 MiB cap survives with `memory.events`' `oom`
/// still 0. That is correct kernel behaviour and a correct `NotTripped` verdict, but
/// it makes the OOM path unreachable, so the test does what a container operator
/// does and disables swap for this cgroup first. The group's own cgroup is found
/// through a live member's `/proc/<pid>/cgroup`, never by guessing the crate's
/// private naming.
#[cfg(target_os = "linux")]
fn disable_swap_for_the_group_cgroup_of(pid: u32) -> bool {
    let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/cgroup")) else {
        return false;
    };
    let Some(rel) = text.lines().find_map(|line| line.strip_prefix("0::")) else {
        return false;
    };
    let path = std::path::Path::new("/sys/fs/cgroup")
        .join(rel.trim().trim_start_matches('/'))
        .join("memory.swap.max");
    std::fs::write(path, "0").is_ok()
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "OOM-kills a real child inside a cgroup v2 memory cap; meaningful only with cgroup delegation at the real hierarchy root"]
async fn linux_cgroup_memory_cap_oom_kill_is_reported_as_tripped() {
    use std::time::Duration;

    use processkit::Command;

    use crate::common::completes_within;

    let Some(group) =
        capped_cgroup_or_skip(ProcessGroupOptions::default().max_memory(32 * 1024 * 1024))
    else {
        return;
    };

    // The cap is in force and the kernel says it has not fired — a decisive "no",
    // which is what makes the "yes" below meaningful.
    assert_eq!(
        group.limit_evidence().memory(),
        LimitVerdict::NotTripped,
        "an untouched memory cap has provably not fired"
    );

    // A throwaway member exposes the group's cgroup so swap can be taken off it
    // before the hog starts — no race with the hog's own allocations, and no
    // dependence on how the crate names its cgroup.
    let probe = group
        .start(&Command::new("sleep").arg("30"))
        .await
        .expect("spawn a cgroup probe member");
    let swapless = probe
        .pid()
        .is_some_and(disable_swap_for_the_group_cgroup_of);

    // A REAL trigger, not a simulation: POSIX `sh` doubling a string to ~64 MiB —
    // twice the cap — so the charge fails and the kernel OOM-kills inside the
    // cgroup. Bounded on purpose: it terminates either way instead of thrashing.
    let hog = "a=x; i=0; while [ $i -lt 26 ]; do a=\"$a$a\"; i=$((i+1)); done; echo survived";
    let child = group
        .start(&Command::new("sh").args(["-c", hog]))
        .await
        .expect("spawn the memory hog");
    let out = completes_within(
        Duration::from_secs(60),
        "the memory hog to finish or be OOM-killed under its cgroup cap",
        child.output_string(),
    )
    .await
    .expect("collect the hog's outcome");

    if !swapless {
        // Swap could not be disabled, so the kernel is free to page the hog out
        // rather than OOM it. Assert only what stays true either way: the verdict
        // must agree with what actually happened, never invent a trip.
        eprintln!(
            "note: could not disable swap for this cgroup — the kernel may page the \
             hog out instead of OOM-killing it, so only verdict/outcome agreement is \
             checked here (the pids-cap test covers a swap-independent real trip)"
        );
        let verdict = group.limit_evidence().memory();
        if out.is_success() {
            assert_eq!(
                verdict,
                LimitVerdict::NotTripped,
                "the hog survived, so the cap did not fire — the verdict must not claim it did"
            );
        } else {
            assert_eq!(
                verdict,
                LimitVerdict::Tripped,
                "the hog died under its cap, and the kernel recorded the OOM"
            );
        }
        return;
    }

    assert!(
        !out.is_success(),
        "a hog that outgrew its swapless memory cap must be killed, not exit successfully: {:?}",
        out.code()
    );

    // The verdict comes from `memory.events`' own `oom` counter — not from the
    // child's exit status, which cannot tell a cap-driven kill from a crash.
    assert_eq!(
        group.limit_evidence().memory(),
        LimitVerdict::Tripped,
        "the kernel recorded an OOM under this cgroup's own memory cap"
    );
    // Axes that carried no cap cannot have fired, even on a group where another
    // axis just did.
    assert_eq!(group.limit_evidence().processes(), LimitVerdict::NotTripped);
    assert_eq!(group.limit_evidence().cpu(), LimitVerdict::NotTripped);

    // Cumulative and not consumed by reading: asking twice gives the same answer.
    assert_eq!(group.limit_evidence().memory(), LimitVerdict::Tripped);
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "forks a real child past a cgroup v2 pids cap; meaningful only with cgroup delegation at the real hierarchy root"]
async fn linux_cgroup_process_cap_denied_fork_is_reported_as_tripped() {
    use std::time::Duration;

    use processkit::Command;

    use crate::common::completes_within;

    // `pids.max` bounds the descendants a contained child forks (the documented
    // Linux semantics), so the child below is the real trigger: it tries to fork
    // far past the cap and the kernel refuses.
    let Some(group) = capped_cgroup_or_skip(ProcessGroupOptions::default().max_processes(4)) else {
        return;
    };
    assert_eq!(
        group.limit_evidence().processes(),
        LimitVerdict::NotTripped,
        "an untouched process cap has provably not fired"
    );

    let child = group
        .start(&Command::new("sh").args([
            "-c",
            "i=0; while [ $i -lt 64 ]; do sleep 1 & i=$((i+1)); done; wait; exit 0",
        ]))
        .await
        .expect("spawn the forking child");
    // The shell keeps going after a refused fork, so its own exit status says
    // nothing useful here — the kernel counter asserted below is the evidence.
    let _out = completes_within(
        Duration::from_secs(60),
        "the forking child to finish under its pids cap",
        child.output_string(),
    )
    .await
    .expect("collect the forker's outcome");

    assert_eq!(
        group.limit_evidence().processes(),
        LimitVerdict::Tripped,
        "the kernel recorded forks refused by this cgroup's own pids cap"
    );
    assert_eq!(group.limit_evidence().memory(), LimitVerdict::NotTripped);
    assert_eq!(group.limit_evidence().cpu(), LimitVerdict::NotTripped);
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "runs a well-behaved child under real cgroup v2 caps; meaningful only with cgroup delegation at the real hierarchy root"]
async fn linux_cgroup_reports_a_decisive_no_for_caps_that_never_fired() {
    use processkit::Command;

    // All three axes capped generously, and a child that comes nowhere near any of
    // them: the report must be a decisive `NotTripped` on every axis — the "we
    // looked and the counters are zero" answer, never `Unknown` (which would mean
    // the evidence could not be read) and never `Tripped`.
    let Some(group) = capped_cgroup_or_skip(
        ProcessGroupOptions::default()
            .max_memory(256 * 1024 * 1024)
            .max_processes(64)
            .cpu_quota(2.0),
    ) else {
        return;
    };

    let out = group
        .start(&Command::new("sh").args(["-c", "echo hi"]))
        .await
        .expect("spawn a small child")
        .output_string()
        .await
        .expect("collect");
    assert!(out.is_success(), "exit {:?}", out.code());

    let evidence = group.limit_evidence();
    for kind in ALL_KINDS {
        assert_eq!(
            evidence.verdict(kind),
            LimitVerdict::NotTripped,
            "a cap the workload never approached must read as a decisive no ({kind:?})"
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "creates a cgroup-backed group and lifts its cap; meaningful only with cgroup delegation at the real hierarchy root"]
async fn linux_lifting_a_cap_does_not_erase_that_it_was_in_force() {
    // The sticky-axis contract: `update_limits` lifting a cap must not turn a
    // readable verdict into `NotTripped`-by-omission. The counters still exist, so
    // the group must keep consulting them.
    let Some(mut group) = capped_cgroup_or_skip(ProcessGroupOptions::default().max_processes(8))
    else {
        return;
    };
    group
        .update_limits(ResourceLimits::default())
        .expect("lifting every cap succeeds on a cgroup");

    // Still read from the kernel (a decisive no here — nothing forked past the cap
    // while it was in force), NOT skipped as "never capped".
    assert_eq!(
        group.limit_evidence().processes(),
        LimitVerdict::NotTripped,
        "an axis that carried a cap stays on the evidence record after the cap is lifted"
    );
}

// The Windows contract is a *negative* one — a Job Object preserves no
// post-mortem record that a cap fired — so these two tests pin the two ways that
// conclusion could go wrong: a fabricated "did not fire" after a violation that
// really happened, and a fabricated "fired" after an ordinary teardown.

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns real subprocesses past a Job Object active-process cap"]
async fn windows_a_real_cap_violation_is_never_reported_as_a_no() {
    // A single-process sleeper keeps the accounting unambiguous (one process per
    // start), so `max_processes(1)` admits the first and the OS refuses the second
    // — a cap violation that demonstrably happened.
    let one_proc_sleeper = || Command::new("ping").args(["-n", "30", "127.0.0.1"]);

    let group = ProcessGroup::with_options(ProcessGroupOptions::default().max_processes(1))
        .expect("create capped group");
    let _first = group
        .start(&one_proc_sleeper())
        .await
        .expect("first child fits the cap");
    let second = group.start(&one_proc_sleeper()).await;
    assert!(
        second.is_err(),
        "a second process must not be admitted past max_processes(1)"
    );

    // The cap provably fired, and the Job Object records nothing about it (the
    // job accounting's "terminated for a limit violation" tally does not move for
    // this class of violation — measured, not assumed). So the only honest verdict
    // is `Unknown`: reporting `NotTripped` here would be a fabricated "no" about an
    // event that really occurred.
    assert_eq!(
        group.limit_evidence().processes(),
        LimitVerdict::Unknown,
        "a Job Object keeps no post-mortem evidence for its process cap — say so \
         rather than claim the cap did not fire when it demonstrably did"
    );
    // An axis that carried no cap is still decisive: nothing was capped there.
    assert_eq!(group.limit_evidence().memory(), LimitVerdict::NotTripped);
    assert_eq!(group.limit_evidence().cpu(), LimitVerdict::NotTripped);
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "hard-kills a real child inside a capped Job Object"]
async fn windows_a_hard_kill_is_never_reported_as_a_cap_trip() {
    // `kill_all` (TerminateJobObject), the graceful escalation and kill-on-drop all
    // terminate members, but none of them is a *limit violation*. None may ever be
    // dressed up as one: a timed-out or cancelled run must never be blamed on the
    // cap it happened to be running under.
    let group = ProcessGroup::with_options(ProcessGroupOptions::default().max_processes(8))
        .expect("create capped group");
    let child = group
        .start(&Command::new("ping").args(["-n", "30", "127.0.0.1"]))
        .await
        .expect("spawn a child well inside the cap");
    group.kill_all().expect("hard-kill the tree");
    let _ = child.output_string().await;

    assert_ne!(
        group.limit_evidence().processes(),
        LimitVerdict::Tripped,
        "a TerminateJobObject teardown is not a limit violation and must never read as a trip"
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "runs a real child under Job Object memory and CPU caps"]
async fn windows_capped_axes_report_unknown_rather_than_a_false_no() {
    // The standing Windows contract on an ordinary, uneventful run: every axis that
    // carries a cap reports `Unknown` (no post-mortem record exists for it), every
    // axis that carries none reports a decisive `NotTripped`.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default()
            .max_memory(512 * 1024 * 1024)
            .cpu_quota(0.5),
    )
    .expect("create capped group");

    let out = group
        .start(&Command::new("cmd").args(["/c", "echo hi"]))
        .await
        .expect("spawn small child")
        .output_string()
        .await
        .expect("collect");
    assert!(out.is_success(), "exit {:?}", out.code());

    let evidence = group.limit_evidence();
    assert_eq!(
        evidence.memory(),
        LimitVerdict::Unknown,
        "a Job Object memory cap leaves no post-mortem evidence — say so, don't guess a no"
    );
    assert_eq!(
        evidence.cpu(),
        LimitVerdict::Unknown,
        "a Job Object CPU hard cap leaves no post-mortem evidence — say so, don't guess a no"
    );
    assert_eq!(
        evidence.processes(),
        LimitVerdict::NotTripped,
        "an axis that never carried a cap is still decisive"
    );

    // A pure read: the group is untouched and still tears down normally.
    group
        .kill_all()
        .expect("teardown is unaffected by an evidence read");
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "creates a capped Job Object and then lifts the cap"]
async fn windows_lifting_a_cap_does_not_erase_that_it_was_in_force() {
    // The sticky-axis contract, on the platform where it is deterministic: after
    // `update_limits` lifts every cap, the memory axis must still be answered as a
    // capped one (`Unknown` — the cap was in force and this mechanism cannot say
    // whether it fired), NOT downgraded to the `NotTripped` an axis that never
    // carried a cap gets. The CPU axis, never capped here, is the control.
    let mut group =
        ProcessGroup::with_options(ProcessGroupOptions::default().max_memory(512 * 1024 * 1024))
            .expect("create capped group");
    assert_eq!(group.limit_evidence().memory(), LimitVerdict::Unknown);

    group
        .update_limits(ResourceLimits::default())
        .expect("lift every cap on the live job");

    assert_eq!(
        group.limit_evidence().memory(),
        LimitVerdict::Unknown,
        "an axis that carried a cap stays on the evidence record after the cap is lifted"
    );
    assert_eq!(
        group.limit_evidence().cpu(),
        LimitVerdict::NotTripped,
        "an axis that never carried a cap is unaffected"
    );
}
