//! Graceful shutdown: TERM grace window and SIGKILL escalation — unix-only
//! via the `mod` declaration in `main.rs`.

use std::time::{Duration, Instant};

use processkit::{Command, ProcessGroup};

#[tokio::test]
#[ignore = "spawns a real subprocess and shuts it down gracefully"]
async fn shutdown_lets_a_term_handling_child_end_the_grace_early() {
    // The struct update covers the `limits`-gated field; without that feature
    // every field is already named, which clippy would otherwise flag.
    #[allow(clippy::needless_update)]
    let group = ProcessGroup::with_options(processkit::ProcessGroupOptions {
        shutdown_timeout: Duration::from_secs(10),
        ..Default::default()
    })
    .expect("create group");

    // Exits 0 on SIGTERM, parked on an interruptible `read` of a stdin we keep
    // open — deliberately ZERO forks. A background `sleep 30 &` here flaked on
    // CI: the per-member TERM broadcast is documented best-effort against a
    // tree that is forking, and the sleep forked right after the banner could
    // miss the signal and hold the group alive for the whole grace. The
    // `ready` banner still orders the trap installation before the shutdown.
    let mut run = group
        .start(
            &Command::new("sh")
                .args(["-c", "trap 'exit 0' TERM; echo ready; read line"])
                .keep_stdin_open(),
        )
        .await
        .expect("start");
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(10))
        .await
        .expect("trap installed");
    // Reap concurrently: the graceful path's liveness probe sees a zombie as
    // alive, so the child must actually be collected for the early return.
    let waiter = tokio::spawn(run.wait());

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(20), group.shutdown())
        .await
        .expect("shutdown bounded")
        .expect("shutdown ok");
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "a TERM-handling child must end the 10s grace early (took {:?})",
        start.elapsed()
    );

    let code = waiter.await.expect("join").expect("wait");
    assert_eq!(code, Some(0), "the child exited via its TERM trap");
}

#[tokio::test]
#[ignore = "spawns a TERM-ignoring subprocess and escalates to SIGKILL"]
async fn shutdown_escalates_to_kill_after_the_grace_window() {
    // See above: the struct update exists for the `limits`-gated field.
    #[allow(clippy::needless_update)]
    let group = ProcessGroup::with_options(processkit::ProcessGroupOptions {
        shutdown_timeout: Duration::from_millis(500),
        escalate_to_kill: true,
        ..Default::default()
    })
    .expect("create group");

    // Ignores SIGTERM and busy-waits (a foreground `sleep` would itself die to
    // the broadcast TERM and end the script cleanly — defeating the test). The
    // `ready` banner proves the trap is installed before shutdown fires — a
    // TERM landing earlier would kill the child and end the grace instantly.
    let mut run = group
        .start(&Command::new("sh").args(["-c", "trap '' TERM; echo ready; while :; do :; done"]))
        .await
        .expect("start");
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(10))
        .await
        .expect("trap installed");
    let waiter = tokio::spawn(run.wait());

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(15), group.shutdown())
        .await
        .expect("escalation keeps shutdown bounded")
        .expect("shutdown ok");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300),
        "the grace window must be waited out before escalating ({elapsed:?})"
    );

    let code = waiter.await.expect("join").expect("wait");
    assert_eq!(code, None, "SIGKILL leaves no exit code, got {code:?}");
}
