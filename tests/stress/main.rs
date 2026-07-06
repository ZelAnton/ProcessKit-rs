//! Opt-in **stress tier** for `processkit` — behaviour at scale and under
//! multiplicity, the dimension the correctness suite (`tests/integration`)
//! doesn't cover.
//!
//! These spawn many real processes (50–100 at once, 100k-line floods, restart
//! storms), so they are **gated behind the `PROCESSKIT_STRESS` env var** rather
//! than `#[ignore]`: the normal PR matrix still *compiles* this binary (free
//! breakage-checking) but every scenario early-returns instantly when the var
//! is unset. The nightly `stress.yml` workflow sets it and runs them for real:
//!
//! ```text
//! PROCESSKIT_STRESS=1 cargo test --all-features --test stress -- --test-threads=1
//! ```
//!
//! `--test-threads=1` is deliberate: each scenario spawns 50–100 real processes
//! (and a 100k-line flood), so running them in parallel would oversubscribe the
//! machine and make the per-scenario timeouts meaningless.
//!
//! A skipped scenario (var unset) is recorded by libtest as a *pass*, not a
//! skip — there is no stable runtime-skip API. That is fine for the PR matrix
//! (whose only job here is to keep this binary compiling); the nightly is what
//! actually exercises the scenarios.
//!
//! Each scenario asserts *correctness under load* — everything spawns, drains,
//! dies, and is collected — never wall-clock numbers, which are inherently noisy.

mod common;

use std::time::Duration;

use processkit::{Command, JobRunner, ProcessGroup, RunningProcess, output_all, wait_all};

use crate::common::*;

/// 1. Spawn a burst of N children concurrently into a single group; assert all
///    start, all exit cleanly, and the group is still usable afterwards.
#[tokio::test]
async fn concurrent_spawn_into_one_group() {
    if skip_unless_enabled("concurrent_spawn_into_one_group") {
        return;
    }
    for n in [50usize, 100] {
        let group = ProcessGroup::new().expect("create group");
        let cmds: Vec<Command> = (0..n).map(|_| quick_exit()).collect();
        // `output_all` with the cap == N spawns all N at once into the shared group.
        let results = tokio::time::timeout(Duration::from_secs(60), output_all(cmds, n, &group))
            .await
            .expect("the spawn burst finished in time");
        assert_eq!(results.len(), n);
        assert!(
            results.iter().all(|r| matches!(r, Ok(o) if o.is_success())),
            "all {n} children spawn and exit 0"
        );
        // The group must remain healthy after the burst.
        let extra = group
            .start(&quick_exit())
            .await
            .expect("group still usable after the burst");
        let _ = extra.wait().await.expect("reap the extra child");
    }
}

/// 2. Run N commands each in its own private group (the default `JobRunner`),
///    all concurrently; assert no spawn failures and every result collected.
#[tokio::test]
async fn many_private_groups_at_once() {
    if skip_unless_enabled("many_private_groups_at_once") {
        return;
    }
    let n = 100usize;
    let cmds: Vec<Command> = (0..n).map(|_| quick_exit()).collect();
    let results = tokio::time::timeout(Duration::from_secs(60), output_all(cmds, n, &JobRunner))
        .await
        .expect("the batch finished in time");
    assert_eq!(results.len(), n);
    assert!(
        results.iter().all(|r| matches!(r, Ok(o) if o.is_success())),
        "all {n} private-group runs succeed"
    );
}

/// 3. Sustained spawn→reap churn; assert it stays correct and leaks no
///    file descriptors/handles — Linux (`/proc/self/fd`), macOS (`lsof`), and
///    Windows (`GetProcessHandleCount`) each have their own counting
///    mechanism; a platform where none applies (or the mechanism call itself
///    fails) skips only the leak assertion, never the churn itself.
#[tokio::test]
async fn sustained_spawn_reap_churn_does_not_leak() {
    if skip_unless_enabled("sustained_spawn_reap_churn") {
        return;
    }
    // Warm up: the very first spawn lazily initializes some OS/runtime-side
    // handles (observed on Windows — IOCP, DLL loads, cmd.exe path-resolution
    // caches) that are then reused for every later spawn — a one-time jump,
    // not a leak. Take the baseline after that settles so the churn below
    // measures steady-state growth, not cold-start noise.
    let warmup = quick_exit().output_string().await.expect("warm up a child");
    assert!(warmup.is_success());
    let before = open_handle_count();

    for _ in 0..100 {
        let result = quick_exit().output_string().await.expect("run a child");
        assert!(result.is_success());
    }

    match (before, open_handle_count()) {
        (Some(before), Some(after)) => {
            // A real fd/handle leak grows ~linearly with the 100 spawns; a
            // small slack absorbs runtime bookkeeping noise.
            assert!(
                after <= before + 8,
                "fd/handle count grew across 100 spawn/reap cycles: {before} -> {after}"
            );
        }
        _ => eprintln!(
            "[stress] sustained_spawn_reap_churn: no fd/handle counting mechanism available on \
             this platform — skipping the leak assertion (the churn itself still ran)"
        ),
    }
}

/// 4. Tear a large group down — by `drop` and by `shutdown().await` — and prove
///    the whole tree dies *promptly*.
///
///    We await the owned handles rather than probe pids: a returned `wait()` is
///    unambiguous proof the child exited, where an OS pid probe is not. On
///    Windows a pid stays valid to `OpenProcess` until its *handle* closes (so a
///    probe would really be measuring reaping, not termination) and pids are
///    reused aggressively; the equivalent unix probe sees a not-yet-reaped child
///    as a live zombie. Children are ~90s sleepers, so reaping every one inside
///    the 45s grace can only mean teardown killed them — a survivor would run
///    its full ~90s and miss the grace. A multi-thread runtime lets the 50 reaps
///    overlap instead of serializing on one worker (the source of a 15s-grace
///    flake under load).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_group_teardown_kills_the_whole_tree() {
    if skip_unless_enabled("large_group_teardown") {
        return;
    }
    const GRACE: Duration = Duration::from_secs(45);

    // Sub-case A: kill-on-drop. The handles move into the waiters (not dropped),
    // so the group's `Drop` is the only thing that can kill the tree.
    {
        let group = ProcessGroup::new().expect("create group");
        let mut handles = Vec::new();
        for _ in 0..50 {
            handles.push(group.start(&long_sleeper()).await.expect("start child"));
        }
        let waiters: Vec<_> = handles
            .into_iter()
            .map(|h| tokio::spawn(h.wait()))
            .collect();

        drop(group); // kill-on-drop tears down the job / cgroup / process group

        assert_all_reaped_within(waiters, GRACE, "drop").await;
    }

    // Sub-case B: graceful shutdown.
    {
        let group = ProcessGroup::new().expect("create group");
        let mut handles = Vec::new();
        for _ in 0..50 {
            handles.push(group.start(&long_sleeper()).await.expect("start child"));
        }
        let waiters: Vec<_> = handles
            .into_iter()
            .map(|h| tokio::spawn(h.wait()))
            .collect();

        tokio::time::timeout(Duration::from_secs(30), group.shutdown())
            .await
            .expect("shutdown stays bounded")
            .expect("shutdown ok");

        assert_all_reaped_within(waiters, GRACE, "shutdown").await;
    }
}

/// Await every `wait()` task, failing if they don't all return within `grace`.
/// With ~90s children, prompt reaping is the teardown signal: a child the
/// teardown missed would block its waiter past the grace.
async fn assert_all_reaped_within(
    waiters: Vec<tokio::task::JoinHandle<processkit::Result<processkit::Outcome>>>,
    grace: Duration,
    how: &str,
) {
    let reaped = tokio::time::timeout(grace, async {
        for waiter in waiters {
            // Surface a panicked wait() task (a real teardown bug); the exit
            // code itself is irrelevant — we only need each waiter to return.
            let _ = waiter.await.expect("a wait() task panicked");
        }
    })
    .await;
    assert!(
        reaped.is_ok(),
        "{how} must reap all 50 children within {grace:?} (their natural exit is ~90s)"
    );
}

/// 5. Fire `start_kill` at many handles at once, then join them all with
///    `wait_all`; assert every one is reaped promptly.
#[tokio::test]
async fn concurrent_kill_reaps_every_handle() {
    if skip_unless_enabled("concurrent_kill") {
        return;
    }
    let group = ProcessGroup::new().expect("create group");
    let mut handles = Vec::new();
    for _ in 0..20 {
        handles.push(group.start(&long_sleeper()).await.expect("start child"));
    }
    for handle in &mut handles {
        handle.start_kill().expect("start_kill");
    }
    let mut refs: Vec<&mut RunningProcess> = handles.iter_mut().collect();
    let codes = tokio::time::timeout(Duration::from_secs(15), wait_all(&mut refs))
        .await
        .expect("all kills reaped in time")
        .expect("join");
    // Killed children carry no clean code (a signal on unix, the terminate code
    // on Windows); the guarantee under test is that all 20 are reaped at all.
    assert_eq!(codes.len(), 20, "every killed handle is collected");
}

/// 6. A cancellation storm: fire many tokens at once and assert every in-flight
///    run resolves to `Error::Cancelled` (and its tree is torn down).
#[tokio::test]
async fn cancellation_storm_resolves_every_call() {
    use processkit::{CancellationToken, Error};

    if skip_unless_enabled("cancellation_storm") {
        return;
    }
    let n = 50usize;
    let tokens: Vec<CancellationToken> = (0..n).map(|_| CancellationToken::new()).collect();
    let tasks: Vec<_> = tokens
        .iter()
        .map(|token| {
            let cmd = long_sleeper().cancel_on(token.clone());
            tokio::spawn(async move { cmd.run().await })
        })
        .collect();

    // Let the children actually start, then cancel the whole storm at once.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for token in &tokens {
        token.cancel();
    }

    for task in tasks {
        let result = tokio::time::timeout(Duration::from_secs(15), task)
            .await
            .expect("cancelled run resolves in time")
            .expect("task did not panic");
        assert!(
            matches!(result, Err(Error::Cancelled { .. })),
            "every cancelled run resolves to Error::Cancelled, got {result:?}"
        );
    }
}

/// 7. Buffer saturation: a child floods 100k lines under a bounded policy. The
///    pump must keep draining (so the child exits cleanly, never blocked on a
///    full pipe), and the retained lines must be capped per policy.
#[tokio::test]
async fn buffer_saturation_drains_without_blocking_the_child() {
    use processkit::OutputBufferPolicy;

    if skip_unless_enabled("buffer_saturation") {
        return;
    }
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        line_emitter(100_000)
            .output_buffer(OutputBufferPolicy::bounded(1000))
            .output_string(),
    )
    .await
    .expect("the emitter ran to completion")
    .expect("output captured");

    assert!(
        result.is_success(),
        "the child drains and exits cleanly (never blocked on a full pipe): {:?}",
        result.code()
    );
    let retained = result.stdout().lines().count();
    assert!(
        (1..=1000).contains(&retained),
        "the bounded policy caps retained lines at 1000, got {retained}"
    );
}

/// 8. Supervisor restart storm: an always-failing child must trip the failure-
///    storm guard over real restarts (`storm_pauses > 0`).
#[tokio::test]
async fn supervisor_storm_guard_trips_under_real_restarts() {
    use processkit::{RestartPolicy, Supervisor};

    if skip_unless_enabled("supervisor_storm") {
        return;
    }
    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        Supervisor::new(always_fail())
            .restart(RestartPolicy::Always)
            .max_restarts(6)
            .backoff(Duration::from_millis(1), 1.0)
            .storm_pause(Duration::from_millis(50))
            .failure_threshold(1.5)
            .failure_decay(Duration::from_secs(1000))
            .run(),
    )
    .await
    .expect("supervision stayed bounded")
    .expect("supervision ran");

    assert!(
        outcome.storm_pauses > 0,
        "the storm guard must trip under a real restart storm, got {}",
        outcome.storm_pauses
    );
}

/// 9. The cgroup directory backing a group is actually removed on drop — a
///    direct filesystem check, not a best-effort claim taken on faith
///    (Linux only; `sys/linux.rs`'s `Job::drop` best-effort-`rmdir`s it after
///    draining). Skips (not fails) when cgroup v2 isn't mounted/delegated —
///    `ProcessGroup::new` then falls back to the POSIX process-group
///    mechanism and there is no cgroup directory to check.
#[tokio::test]
async fn cgroup_directory_is_removed_on_drop() {
    if skip_unless_enabled("cgroup_directory_is_removed_on_drop") {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        use processkit::Mechanism;

        let Some(parent) = own_cgroup_v2_parent() else {
            eprintln!(
                "[stress] cgroup_directory_is_removed_on_drop: no cgroup v2 mount found — skipping"
            );
            return;
        };
        let before = own_processkit_cgroup_dirs(&parent);

        let group = ProcessGroup::new().expect("create group");
        if group.mechanism() != Mechanism::CgroupV2 {
            eprintln!(
                "[stress] cgroup_directory_is_removed_on_drop: mechanism is {:?}, not CgroupV2 \
                 (no delegation on this runner) — skipping",
                group.mechanism()
            );
            return;
        }
        let child = group.start(&quick_exit()).await.expect("start child");
        let _ = child.wait().await.expect("reap child");

        let after_spawn = own_processkit_cgroup_dirs(&parent);
        let new_dirs: Vec<_> = after_spawn
            .into_iter()
            .filter(|p| !before.contains(p))
            .collect();
        assert_eq!(
            new_dirs.len(),
            1,
            "expected exactly one new processkit cgroup dir under {}, found {new_dirs:?}",
            parent.display()
        );
        let cgroup_dir = &new_dirs[0];
        assert!(
            cgroup_dir.exists(),
            "the cgroup dir must exist while the group is alive"
        );

        drop(group);

        // `Job::drop` synchronously drains the cgroup (bounded ~100ms wait)
        // before `rmdir`-ing it; give a little extra grace beyond that bound
        // before failing on a slow CI runner.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while cgroup_dir.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !cgroup_dir.exists(),
            "cgroup directory {} must be removed on drop, not left behind",
            cgroup_dir.display()
        );
    }
    #[cfg(not(target_os = "linux"))]
    eprintln!("[stress] cgroup_directory_is_removed_on_drop: cgroup v2 is Linux-only — skipping");
}
