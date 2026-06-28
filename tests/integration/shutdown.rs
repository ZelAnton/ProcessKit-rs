//! Graceful shutdown: TERM grace window and SIGKILL escalation — unix-only
//! via the `mod` declaration in `main.rs`.

use std::time::{Duration, Instant};

use processkit::{Command, Finished, Outcome, ProcessGroup};

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

    let outcome = waiter.await.expect("join").expect("wait");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "the child exited via its TERM trap"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and shuts it down gracefully via a shared handle"]
async fn shutdown_ref_gracefully_tears_down_a_group_held_by_shared_reference() {
    use std::sync::Arc;

    // The borrowing twin's reason to exist: a group held behind a shared handle
    // (here an `Arc`, as a long-lived supervisor or an FFI binding would) can't be
    // moved out by value to call `shutdown(self)`, yet must still tear down
    // gracefully rather than fall back to a hard kill.
    let group = Arc::new(
        ProcessGroup::with_options(
            processkit::ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(10)),
        )
        .expect("create group"),
    );

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
    let waiter = tokio::spawn(run.wait());

    // A second owner stays alive across the teardown, so `Arc::try_unwrap` would
    // fail — exactly the case the by-value `shutdown` couldn't serve gracefully.
    let _second_owner = Arc::clone(&group);

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(20), group.shutdown_ref())
        .await
        .expect("shutdown_ref bounded")
        .expect("shutdown_ref ok");
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "a TERM-handling child must end the 10s grace early (took {:?})",
        start.elapsed()
    );

    let outcome = waiter.await.expect("join").expect("wait");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "the shared-handle group still shut down gracefully (TERM trap), not hard-killed"
    );
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

    let outcome = waiter.await.expect("join").expect("wait");
    assert!(
        matches!(outcome, Outcome::Signalled(_)),
        "SIGKILL surfaces as a signal kill, got {outcome:?}"
    );
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

#[tokio::test]
#[ignore = "spawns a real subprocess and times out a streamed run gracefully"]
async fn graceful_timeout_on_a_streamed_run_signals_and_ends_the_stream() {
    use tokio_stream::StreamExt;

    // The streaming deadline path (`stdout_lines` watchdog) arms its OWN graceful
    // branch, distinct from the bulk `teardown_on_timeout`. A `Command::start`
    // handle owns its group, so the deadline bounds the stream: at 500ms the tree
    // is sent SIGTERM, the trap exits the child, its pipes close, and the stream
    // ends — well within the long grace, proving the graceful (not hard) teardown.
    // The trap echoes to stderr *before* exiting 0: that side-effect is the
    // positive proof the graceful SIGTERM was delivered and the handler ran — a
    // hard SIGKILL is uncatchable, so "bye" would never appear.
    let mut run = Command::new("sh")
        .args([
            "-c",
            "trap 'echo bye 1>&2; exit 0' TERM; echo ready; while :; do :; done",
        ])
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(10))
        .start()
        .await
        .expect("start");

    let start = Instant::now();
    let mut lines = run.stdout_lines().unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("the ready banner arrives before the deadline");
    assert_eq!(first.as_deref(), Some("ready"), "trap installed");

    // The deadline fires, SIGTERM hits the trap, the child exits, the pipe closes
    // → the stream must end promptly (this is the primary signal: a wired graceful
    // branch ends it within the grace; an *unwired* one — the busy-loop never
    // signalled — would hang past this 5s bound and fail here). We drain the stream
    // to completion FIRST so the child is already dead before `finish`.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while lines.next().await.is_some() {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the graceful-timeout signal must end the stream well within the grace"
    );

    // B1: the streamed run TIMED OUT — the deadline fired and triggered the
    // teardown — so `finish` reports `Outcome::TimedOut`, matching the
    // bulk `output_string` path (which reports `timed_out()` for this same
    // scenario), not the child's in-grace `exit 0`. This is deterministic now:
    // the streaming watchdog sets a shared `timed_out` flag when it fires, and
    // the finisher classifies from that flag rather than racing the reaped exit.
    let Finished {
        outcome, stderr, ..
    } = run.finish().await.expect("finish");
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "a streamed run whose deadline fired must report TimedOut (consistent with the bulk path)"
    );
    // The graceful-vs-hard distinction now rests on this stderr side-effect, NOT
    // the outcome (both graceful and hard end well under the 8s bound, so timing
    // alone cannot tell them apart): "bye" appears only if the TERM trap ran.
    assert!(
        stderr.contains("bye"),
        "the graceful SIGTERM must have run the trap (stderr: {stderr:?})"
    );
    // The timing assertion proves the teardown is *wired* and the signal was
    // honored — the child didn't ride out the full grace to SIGKILL.
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "a TERM-handling streamed child must end the 10s grace early (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and times out an output_events run gracefully"]
async fn graceful_timeout_on_an_events_run_reports_timed_out() {
    use processkit::OutputEvent;
    use tokio_stream::StreamExt;

    // B1 parity for the events path: `output_events` arms the same deadline
    // watchdog as `stdout_lines`, and `finish` classifies via the same
    // `timed_out` flag, so a graceful streamed timeout must report
    // `Outcome::TimedOut` here too — not the child's in-grace `exit 0`.
    let mut run = Command::new("sh")
        .args(["-c", "trap 'exit 0' TERM; echo ready; while :; do :; done"])
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(10))
        .start()
        .await
        .expect("start");

    let start = Instant::now();
    let mut events = run.output_events().unwrap();
    let mut saw_ready = false;
    let drained = tokio::time::timeout(Duration::from_secs(8), async {
        while let Some(ev) = events.next().await {
            if let OutputEvent::Stdout(l) = ev {
                saw_ready |= l.text().contains("ready");
            }
        }
    })
    .await;
    assert!(
        drained.is_ok(),
        "the events stream must end within the grace"
    );
    assert!(
        saw_ready,
        "the ready banner must arrive before the deadline"
    );

    let outcome = run.finish().await.expect("finish").outcome;
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "an events run whose deadline fired must report TimedOut (parity with finish)"
    );
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "a TERM-handling events child must end the 10s grace early (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a TERM-ignoring child in a SHARED group; escalates to SIGKILL"]
async fn graceful_timeout_in_a_shared_group_escalates_to_kill() {
    // A handle from `ProcessGroup::start` SHARES its group, so the run-level
    // graceful timeout tears down the single child via `graceful_kill_pid`
    // (bare-pid signal → liveness poll → SIGKILL), not the owned-group path. A
    // TERM-ignoring child must be SIGKILL'd only after the deadline + grace.
    let group = ProcessGroup::new().expect("create group");
    let start = Instant::now();
    let result = group
        .start(
            &Command::new("sh")
                .args(["-c", "trap '' TERM; echo ready; while :; do :; done"])
                .timeout(Duration::from_millis(500))
                .timeout_grace(Duration::from_millis(500)),
        )
        .await
        .expect("start")
        .output_string()
        .await
        .expect("run completes");
    assert!(result.timed_out(), "the deadline fired");
    assert!(
        start.elapsed() >= Duration::from_millis(900),
        "must wait deadline + grace before SIGKILL in a shared group (took {:?})",
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

// D4: `RunningProcess::shutdown(grace)` gracefully stops an own-group handle
// (Command::start): SIGTERM, wait the grace, SIGKILL survivors. A TERM-handling
// child exits cleanly within the grace, so the reported Outcome is Exited(0).
#[tokio::test]
#[ignore = "spawns a real subprocess; D4 graceful shutdown of an own-group handle"]
async fn shutdown_gracefully_stops_a_term_handling_child() {
    let mut run = Command::new("sh")
        .args(["-c", "trap 'exit 0' TERM; echo ready; while :; do :; done"])
        .start()
        .await
        .expect("start");
    // Confirm the trap is installed before shutting down (avoids racing SIGTERM
    // in before the handler is set).
    run.wait_for_line(|l| l == "ready", Duration::from_secs(10))
        .await
        .expect("trap installed");
    let start = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        run.shutdown(Duration::from_secs(5)),
    )
    .await
    .expect("shutdown finished in time")
    .expect("shutdown ok");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "the child caught SIGTERM and exited cleanly within the grace"
    );
    // The grace must end *early* (the child exited on TERM): shutdown reaps
    // concurrently, so a pgroup backend's liveness probe sees the child gone
    // rather than riding out the full 5s grace (M1).
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "graceful shutdown must end as soon as the child exits, not ride the grace (took {:?})",
        start.elapsed()
    );
}

// M2: a `Command::timeout` that has already elapsed when `shutdown` is called
// classifies the run as `TimedOut`, and `shutdown`'s own graceful teardown is the
// SINGLE teardown — it ends as soon as the child exits on SIGTERM rather than the
// run's timeout firing a second, overlapping graceful ladder.
#[tokio::test]
#[ignore = "spawns a real subprocess; M2 shutdown with an already-elapsed timeout"]
async fn shutdown_reports_timed_out_when_the_deadline_already_elapsed() {
    let mut run = Command::new("sh")
        .args([
            "-c",
            "trap 'exit 0' TERM; echo ready; while :; do sleep 1; done",
        ])
        .timeout(Duration::from_millis(100))
        .start()
        .await
        .expect("start");
    run.wait_for_line(|l| l == "ready", Duration::from_secs(10))
        .await
        .expect("trap installed");
    // Let the command's deadline elapse in wall-clock — nothing is driving it yet,
    // so it is not enforced until `shutdown` reaps.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let start = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        run.shutdown(Duration::from_secs(5)),
    )
    .await
    .expect("shutdown finished in time")
    .expect("shutdown ok");
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "an already-elapsed deadline classifies the shutdown as TimedOut"
    );
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "a single teardown ends as soon as the child exits on TERM, not the full grace (took {:?})",
        start.elapsed()
    );
}

// D4: a shared-group handle (ProcessGroup::start) does not own its group, so
// `shutdown` refuses with Error::Unsupported — the caller tears the group down
// via ProcessGroup::shutdown instead.
#[tokio::test]
#[ignore = "spawns a real subprocess; D4 shutdown is unsupported on a shared-group handle"]
async fn shutdown_is_unsupported_on_a_shared_group_handle() {
    let group = ProcessGroup::new().expect("group");
    let run = group
        .start(&Command::new("sh").args(["-c", "sleep 30"]))
        .await
        .expect("start");
    let err = run
        .shutdown(Duration::from_secs(1))
        .await
        .expect_err("a shared-group handle cannot be gracefully shut down");
    assert!(
        matches!(err, processkit::Error::Unsupported { .. }),
        "expected Unsupported, got {err:?}"
    );
    // The child survived (shared-group Drop doesn't kill); tear it down here.
    group.shutdown().await.expect("teardown the group");
}
