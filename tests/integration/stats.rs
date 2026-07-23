//! Group and per-process accounting: stats(), sample_stats(), profile(), and
//! the per-process diagnostics — `stats`-gated via the `mod` declaration in
//! `main.rs`.

use std::time::Duration;

use processkit::ProcessGroup;

use crate::common::*;

#[tokio::test]
#[ignore = "creates an OS job/cgroup and reads accounting"]
async fn group_stats_report_active_processes() {
    let group = ProcessGroup::new().expect("create group");
    let _process = group.start(&sleeper()).await.expect("spawn sleeper");
    let stats = group.stats().expect("stats");
    assert!(
        stats.active_process_count >= 1,
        "expected a live process, got {stats:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and reads per-process metrics"]
async fn process_diagnostics_are_available() {
    // On the containment platforms CPU/memory are reported; elsewhere they may be
    // None, so only assert the pid/elapsed basics universally.
    let mut process = sleeper().start().await.expect("start sleeper");
    assert!(process.pid().is_some());
    assert!(process.elapsed() < Duration::from_secs(5));
    if cfg!(any(windows, target_os = "linux")) {
        // Give the child a moment to accrue something measurable.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            process.peak_memory_bytes().is_some(),
            "peak memory should be readable on this platform"
        );
    }
    let _ = process.take_stdin(); // no-op (stdin not kept open)
    drop(process);
}

#[tokio::test]
#[ignore = "spawns a real subprocess and samples the group's stats"]
async fn sample_stats_yields_a_live_series() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    let _child = group.start(&sleeper()).await.expect("start sleeper");

    let mut samples = group.sample_stats(Duration::from_millis(50));
    for n in 0..3 {
        let snapshot = tokio::time::timeout(Duration::from_secs(5), samples.next())
            .await
            .expect("sample in time")
            .expect("series still live");
        assert!(
            snapshot.active_process_count >= 1,
            "sample #{n} saw no live process: {snapshot:?}"
        );
    }
}

#[tokio::test]
#[ignore = "spawns a real subprocess and samples the group via the owning sampler"]
async fn owned_sample_stats_yields_a_live_series_from_a_spawned_task() {
    use std::sync::Arc;

    use processkit::OwnedStatsSampler;
    use tokio_stream::StreamExt;

    let group = Arc::new(ProcessGroup::new().expect("create group"));
    let _child = group.start(&sleeper()).await.expect("start sleeper");

    // The owning sampler is `Send + 'static`, so — unlike the borrowing
    // `StatsSampler` — it can be *moved into* a spawned task and driven there.
    // That move is itself the runtime proof of the `Send + 'static` pin.
    let mut samples = OwnedStatsSampler::new(&group, Duration::from_millis(50));
    let seen = tokio::spawn(async move {
        let mut out = Vec::new();
        for _ in 0..3 {
            let snapshot = tokio::time::timeout(Duration::from_secs(5), samples.next())
                .await
                .expect("sample in time")
                .expect("series still live");
            out.push(snapshot);
        }
        out
    })
    .await
    .expect("sampler task");

    for (n, snapshot) in seen.iter().enumerate() {
        assert!(
            snapshot.active_process_count >= 1,
            "owned sample #{n} saw no live process: {snapshot:?}"
        );
    }
}

#[tokio::test]
#[ignore = "spawns a real subprocess and samples via both samplers"]
async fn owned_and_borrowing_samplers_agree_on_a_live_group() {
    use std::sync::Arc;

    use processkit::OwnedStatsSampler;
    use tokio_stream::StreamExt;

    // Driven over the *same* live group, the owning sampler yields the same kind
    // of series as the borrowing one (shared `SamplerCore`, not a forked
    // implementation): each tick a live snapshot with the process counted.
    let group = Arc::new(ProcessGroup::new().expect("create group"));
    let _child = group.start(&sleeper()).await.expect("start sleeper");

    let mut borrowing = group.sample_stats(Duration::from_millis(50));
    let mut owning = OwnedStatsSampler::new(&group, Duration::from_millis(50));

    for n in 0..3 {
        let b = tokio::time::timeout(Duration::from_secs(5), borrowing.next())
            .await
            .expect("borrowing sample in time")
            .expect("borrowing series live");
        let o = tokio::time::timeout(Duration::from_secs(5), owning.next())
            .await
            .expect("owning sample in time")
            .expect("owning series live");
        assert!(
            b.active_process_count >= 1 && o.active_process_count >= 1,
            "sample #{n} disagreed on liveness: borrowing={b:?} owning={o:?}"
        );
    }
}

#[tokio::test]
#[ignore = "spawns a real subprocess, then releases the group mid-series"]
async fn owned_sample_stats_ends_when_the_group_is_released() {
    use std::sync::Arc;

    use processkit::OwnedStatsSampler;
    use tokio_stream::StreamExt;

    let group = Arc::new(ProcessGroup::new().expect("create group"));
    let child = group.start(&sleeper()).await.expect("start sleeper");

    let mut samples = OwnedStatsSampler::new(&group, Duration::from_millis(50));
    let first = tokio::time::timeout(Duration::from_secs(5), samples.next())
        .await
        .expect("first sample in time")
        .expect("series live while the group is held");
    assert!(
        first.active_process_count >= 1,
        "expected a live process before release: {first:?}"
    );

    // Release every strong handle to the group: the child was started into the
    // group by `&self` (it holds no `Arc`), so dropping our `Arc` is the last
    // one — the tree is hard-killed on `Drop` and the sampler's `Weak` can no
    // longer upgrade. The series must end honestly (bounded, not hung; not a
    // repeat of `first`).
    drop(child);
    drop(group);
    let after = tokio::time::timeout(Duration::from_secs(5), samples.next())
        .await
        .expect("post-release poll must be bounded, not hung");
    assert!(
        after.is_none(),
        "a released group must end the owning series, got {after:?}"
    );
    // Fused: it stays ended.
    let again = tokio::time::timeout(Duration::from_secs(5), samples.next())
        .await
        .expect("fused poll bounded");
    assert!(
        again.is_none(),
        "the ended series must not resume: {again:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and profiles its run"]
async fn profile_summarizes_a_run() {
    let profile = group_started_short_run()
        .await
        .profile(Duration::from_millis(50))
        .await
        .expect("profile");

    assert_eq!(profile.code(), Some(0), "profile: {profile:?}");
    // `outcome` is wired through a real run (not just hand-built structs): a clean
    // exit reads as `Exited(0)` with no signal and not timed out, and the
    // `code()`/`outcome` projections agree.
    assert_eq!(
        profile.outcome,
        processkit::Outcome::Exited(0),
        "profile: {profile:?}"
    );
    assert_eq!(profile.code(), profile.outcome.code());
    assert_eq!(profile.signal(), None);
    assert!(!profile.timed_out());
    assert!(
        profile.duration >= Duration::from_millis(500),
        "a ~1s child reported {:?}",
        profile.duration
    );
    assert!(profile.samples >= 1, "profile never sampled: {profile:?}");
    if cfg!(any(windows, target_os = "linux")) {
        assert!(
            profile.peak_memory_bytes.is_some(),
            "peak RSS should be readable on this platform: {profile:?}"
        );
    }
}

/// Start a ~1s single-process child directly (its own private group).
async fn group_started_short_run() -> processkit::RunningProcess {
    sleep_secs(1).start().await.expect("start short child")
}
