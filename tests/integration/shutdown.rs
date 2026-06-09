//! Graceful shutdown: TERM grace window and SIGKILL escalation — unix-only
//! via the `mod` declaration in `main.rs`.

use std::time::{Duration, Instant};

use processkit::{Command, ProcessGroup};

#[tokio::test]
#[ignore = "spawns a real subprocess and shuts it down gracefully"]
async fn shutdown_lets_a_term_handling_child_end_the_grace_early() {
    let group = ProcessGroup::with_options(
        processkit::ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(10)),
    )
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
    let group = ProcessGroup::with_options(
        processkit::ProcessGroupOptions::default()
            .shutdown_timeout(Duration::from_millis(500))
            .escalate_to_kill(true),
    )
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

// --- Run-level graceful timeout (`Command::timeout` + `timeout_grace`) ---
//
// Children busy-wait in the shell (no separate `sleep` child to die to the
// broadcast signal); the generous deadline (500ms) leaves time for the trap to
// install before it fires.

#[tokio::test]
#[ignore = "spawns a real subprocess and times it out gracefully"]
async fn graceful_timeout_lets_a_term_handling_child_end_the_grace_early() {
    // Deadline fires → SIGTERM → the trap exits the child well within the long
    // grace (concurrent reap ends it early). `timed_out()` is still true.
    let start = Instant::now();
    let result = Command::new("sh")
        .args(["-c", "trap 'exit 0' TERM; while :; do :; done"])
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(10))
        .output_string()
        .await
        .expect("run completes");
    assert!(result.timed_out(), "the deadline fired");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "a TERM-handling child must end the 10s grace early (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a TERM-ignoring subprocess; escalates to SIGKILL after the grace"]
async fn graceful_timeout_escalates_to_kill_after_the_grace() {
    let start = Instant::now();
    let result = Command::new("sh")
        .args(["-c", "trap '' TERM; while :; do :; done"])
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_millis(500))
        .output_string()
        .await
        .expect("run completes");
    assert!(result.timed_out());
    assert!(
        start.elapsed() >= Duration::from_millis(900),
        "must wait the deadline + grace before SIGKILL (took {:?})",
        start.elapsed()
    );
}

#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess; verifies the configurable timeout signal"]
async fn graceful_timeout_uses_the_configured_signal() {
    use processkit::Signal;
    // Traps INT (exits) and ignores TERM: only a *configured* SIGINT ends it
    // early — a default-SIGTERM graceful timeout would wait the full 10s grace.
    let start = Instant::now();
    let result = Command::new("sh")
        .args(["-c", "trap 'exit 0' INT; trap '' TERM; while :; do :; done"])
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(10))
        .timeout_signal(Signal::Int)
        .output_string()
        .await
        .expect("run completes");
    assert!(result.timed_out());
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the INT trap must end it early — the signal is configurable (took {:?})",
        start.elapsed()
    );
}
