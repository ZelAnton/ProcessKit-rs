//! Readiness probes: wait_for_line / wait_for_port / wait_for.

use std::time::{Duration, Instant};

use processkit::Command;

use crate::common::*;

#[tokio::test]
#[ignore = "spawns a real subprocess and waits for its readiness banner"]
async fn wait_for_line_matches_banner_and_leaves_child_running() {
    let mut process = banner_then_idle().start().await.expect("start");
    let line = tokio::time::timeout(
        Duration::from_secs(15),
        process.wait_for_line(|l| l.contains("ready"), Duration::from_secs(10)),
    )
    .await
    .expect("probe finished in time")
    .expect("banner matched");
    assert!(line.contains("ready"), "line: {line:?}");

    // The probe must not have killed the still-idling child.
    assert!(process.pid().is_some());
    process.start_kill().expect("kill");
    let _ = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("reaped promptly");
}

#[tokio::test]
#[ignore = "spawns a silent subprocess; the probe must give up at its deadline"]
async fn wait_for_line_not_ready_when_silent() {
    // Genuinely silent: the plain `sleeper()` ping prints lines on Windows.
    let silent = if cfg!(windows) {
        Command::new("cmd").args(["/c", "ping -n 30 127.0.0.1 >nul"])
    } else {
        Command::new("sleep").arg("30")
    };
    let mut process = silent.start().await.expect("start sleeper");
    let start = Instant::now();
    let err = process
        .wait_for_line(|_| true, Duration::from_millis(300))
        .await
        .expect_err("a silent child never becomes ready");
    assert!(
        matches!(err, processkit::Error::NotReady { .. }),
        "expected NotReady, got {err:?}"
    );
    assert!(
        start.elapsed() >= Duration::from_millis(250),
        "probe gave up before its deadline ({:?})",
        start.elapsed()
    );
    // The probe does not kill: the sleeper is still there to reap ourselves.
    assert!(process.pid().is_some());
    process.start_kill().expect("kill");
}

#[tokio::test]
#[ignore = "spawns a short subprocess; the probe must fail fast once stdout closes"]
async fn wait_for_line_not_ready_fast_when_child_exits_silently() {
    let mut process = two_line_echo().start().await.expect("start echo");
    let start = Instant::now();
    let err = process
        .wait_for_line(|l| l.contains("never-printed"), Duration::from_secs(30))
        .await
        .expect_err("the banner never appears");
    assert!(
        matches!(err, processkit::Error::NotReady { .. }),
        "expected NotReady, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "stdout closed — the probe should not wait out the 30s deadline ({:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and probes a TCP port that opens late"]
async fn wait_for_port_succeeds_against_a_late_listener() {
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
    // The "server" socket opens only after a delay — the probe must poll, not
    // one-shot.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral listener");
        let addr = listener.local_addr().expect("local addr");
        let _ = addr_tx.send(addr);
        // Keep the listener alive past the outer deadline (35s) so a
        // CPU-starved probe still has a target. Deadlines are nested
        // inner(30) < outer(35) < listener(40) < child(45) so no ceiling can
        // fire before the probe under load — the source of the old flake.
        tokio::time::sleep(Duration::from_secs(40)).await;
        drop(listener);
    });

    // The child must outlive the outer deadline too: `wait_for_port` returns
    // early if the child exits first, which would fail the probe assertion.
    let mut process = sleep_secs(45).start().await.expect("start context child");
    let addr = addr_rx.await.expect("listener address");
    tokio::time::timeout(
        Duration::from_secs(35),
        process.wait_for_port(addr, Duration::from_secs(30)),
    )
    .await
    .expect("probe finished in time")
    .expect("port became ready");
}

#[tokio::test]
#[ignore = "spawns a real subprocess and polls an async readiness check"]
async fn wait_for_passes_once_the_check_turns_true() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let mut process = sleeper().start().await.expect("start sleeper");
    let attempts = std::sync::Arc::new(AtomicU32::new(0));
    let seen = std::sync::Arc::clone(&attempts);
    process
        .wait_for(
            move || {
                let n = seen.fetch_add(1, Ordering::SeqCst);
                async move { n >= 2 }
            },
            Duration::from_secs(10),
        )
        .await
        .expect("third attempt passes");
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "the check should have been re-invoked across ticks"
    );
}

#[tokio::test]
#[ignore = "spawns a short subprocess; the probe must fail fast once it exits"]
async fn wait_for_fails_fast_when_child_exits() {
    let mut process = two_line_echo().start().await.expect("start echo");
    let start = Instant::now();
    let err = process
        .wait_for(|| async { false }, Duration::from_secs(30))
        .await
        .expect_err("an exited child never becomes ready");
    assert!(
        matches!(err, processkit::Error::NotReady { .. }),
        "expected NotReady, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "child exited — the probe should not wait out the 30s deadline ({:?})",
        start.elapsed()
    );
}
