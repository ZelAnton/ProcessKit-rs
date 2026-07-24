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

#[tokio::test]
#[ignore = "spawns a real subprocess; verifies a reused group re-arms kill-on-drop"]
async fn spawning_after_shutdown_ref_rearms_kill_on_drop() {
    // C1: `shutdown_ref` with `escalate_to_kill = false` latches skip_drop_kill
    // (to spare any survivors). Spawning a fresh child into the still-usable group
    // must RE-ARM Drop's kill backstop, so the new child is torn down on Drop
    // rather than orphaned by the stale latch.
    let group = ProcessGroup::with_options(
        processkit::ProcessGroupOptions::default()
            .escalate_to_kill(false)
            .shutdown_timeout(Duration::from_millis(200)),
    )
    .expect("group");
    // Latch skip_drop_kill via a graceful shutdown of the (empty) group.
    group.shutdown_ref().await.expect("shutdown_ref");

    // Reuse the group: this spawn must clear the latch. (This module is unix-only;
    // the Windows skip-drop-kill/reuse path is covered by the SkipDropKill unit
    // test and the clear-on-spawn call in the Job Object backend.)
    let child = group
        .start(&Command::new("sleep").arg("60"))
        .await
        .expect("start after shutdown_ref");

    // Drop the group — the re-armed backstop must kill the fresh child. If the
    // stale latch orphaned it, `wait()` blocks the full 60s and the timeout fails
    // the test; a re-armed backstop kills it promptly.
    drop(group);
    let outcome = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("a child spawned after shutdown_ref must be killed on group Drop, not orphaned")
        .expect("wait");
    assert!(
        !matches!(outcome, Outcome::Exited(0)),
        "the child was killed by the group's Drop, not a clean exit (got {outcome:?})"
    );
}

// `ProcessGroup::adopt` is gated on `process-control`.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns and adopts a real subprocess; verifies adopt re-arms kill-on-drop"]
async fn adopting_after_shutdown_ref_rearms_kill_on_drop() {
    // C1 (adopt path): `shutdown_ref(escalate=false)` latches skip_drop_kill;
    // ADOPTING a fresh member must re-arm Drop's kill backstop, the same as
    // spawning — otherwise the adopted child is orphaned by the stale latch.
    let group = ProcessGroup::with_options(
        processkit::ProcessGroupOptions::default()
            .escalate_to_kill(false)
            .shutdown_timeout(Duration::from_millis(200)),
    )
    .expect("group");
    group.shutdown_ref().await.expect("shutdown_ref");

    // A long-lived child spawned OUTSIDE the group, then adopted into it (an
    // already-exec'd child is solo-tracked via the EACCES/EPERM path).
    let mut child = tokio::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn external child");
    group.adopt(&child).expect("adopt after shutdown_ref");

    // Drop the group — the re-armed backstop must kill the adopted child. A stale
    // latch would spare it, making `wait()` block the full 60s.
    drop(group);
    tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("an adopted child must be killed on group Drop, not orphaned")
        .expect("wait");
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
#[ignore = "spawns a real subprocess and times out an events run gracefully"]
async fn graceful_timeout_on_an_events_run_reports_timed_out() {
    use processkit::ProcessEvent;
    use tokio_stream::StreamExt;

    // B1 parity for the events path: `events` arms the same deadline watchdog as
    // `stdout_lines`, and `finish` classifies via the same `timed_out` flag, so a
    // graceful streamed timeout must report `Outcome::TimedOut` here too — not the
    // child's in-grace `exit 0`. The stream's terminal `Exited` carries the same
    // classified outcome, delivered by the concurrently-driven `finish`.
    let mut run = Command::new("sh")
        .args(["-c", "trap 'exit 0' TERM; echo ready; while :; do :; done"])
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(10))
        .start()
        .await
        .expect("start");

    let start = Instant::now();
    let mut events = run.events().unwrap();
    // Drive the event stream and the reaping `finish` together: the terminal
    // `Exited` is published when the run is reaped, so draining the stream first
    // would park waiting for it.
    let collect = async {
        let mut saw_ready = false;
        let mut saw_started = false;
        let mut exit_outcome = None;
        while let Some(ev) = events.next().await {
            match ev {
                ProcessEvent::Started { .. } => saw_started = true,
                ProcessEvent::Stdout(l) => saw_ready |= l.text().contains("ready"),
                ProcessEvent::Exited(o) => exit_outcome = Some(o),
                _ => {}
            }
        }
        (saw_started, saw_ready, exit_outcome)
    };
    let drained = tokio::time::timeout(
        Duration::from_secs(8),
        async { tokio::join!(collect, run.finish()) },
    )
    .await;
    let ((saw_started, saw_ready, exit_outcome), finished) =
        drained.expect("the events stream and finish must complete within the grace");
    let outcome = finished.expect("finish").outcome;

    assert!(saw_started, "Started must lead the stream");
    assert!(saw_ready, "the ready banner must arrive before the deadline");
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "an events run whose deadline fired must report TimedOut (parity with finish)"
    );
    assert_eq!(
        exit_outcome,
        Some(Outcome::TimedOut),
        "the terminal Exited event carries the same classified outcome as finish"
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

// T-017: on a SHARED-group STREAMING handle the deadline watchdog runs the
// graceful kill-and-reap DETACHED, so its final SIGKILL survives the handle's
// `Drop` aborting the watchdog mid-grace. This reproduces the exact hazard the
// primitive fixes: a child that catches SIGTERM, closes its stdout (ending the
// stream, which drops the handle — as `first_line` does), and keeps running.
// Before the fix `Drop` aborted the watchdog before the SIGKILL and the child
// leaked until the shared group itself was dropped; now it is forced down (and
// reaped by the runtime) ~grace after the deadline, regardless of the handle.
#[tokio::test]
#[ignore = "spawns a signal-catching child in a SHARED group and streams it to a timeout"]
async fn shared_group_streaming_timeout_kills_a_signal_catching_child_despite_drop() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // On SIGTERM the trap closes stdout (`exec 1>&-`) so our read end sees EOF and
    // the stream ends, then `exec`s a survivor (`sleep 30`) that keeps the SAME
    // pid alive with no stdout holder. The busy-loop main body runs the trap at
    // the next command boundary — promptly, and with no foreground child that
    // could inherit and pin the stdout pipe open.
    let cmd = Command::new("sh")
        .args([
            "-c",
            "trap 'exec 1>&-; exec sleep 30' TERM; echo ready; while :; do :; done",
        ])
        .timeout(Duration::from_millis(400))
        .timeout_grace(Duration::from_millis(600));

    let mut run = group.start(&cmd).await.expect("start");
    let pid = run.pid().expect("a real child has a pid");
    let mut lines = run.stdout_lines().expect("stream stdout");

    // Drain to end-of-stream: the deadline fires → SIGTERM → the trap closes
    // stdout → EOF → the stream ends here (it must not hang).
    let seen = tokio::time::timeout(Duration::from_secs(10), async {
        let mut seen = Vec::new();
        while let Some(line) = lines.next().await {
            seen.push(line);
        }
        seen
    })
    .await
    .expect("the deadline must end the stream, not hang");
    assert!(seen.iter().any(|l| l == "ready"), "saw: {seen:?}");

    // Drop the streaming handle mid-grace (as `first_line` does), which aborts the
    // deadline watchdog. It must return PROMPTLY — the rejected "await the
    // watchdog in Drop" alternative would instead block for the whole grace.
    let dropped_at = Instant::now();
    drop(lines);
    drop(run);
    assert!(
        dropped_at.elapsed() < Duration::from_millis(250),
        "dropping the streaming handle must not block on the grace (took {:?})",
        dropped_at.elapsed()
    );

    // The GROUP is still alive, so the group-drop backstop is NOT what ends the
    // survivor — only the detached primitive's non-abortable SIGKILL can, ~grace
    // after the deadline. Poll until the pid is gone (killed and then reaped by
    // the runtime's orphan reaper).
    let killed = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            // SAFETY: signal 0 is a pure existence probe.
            if unsafe { libc::kill(pid as i32, 0) } != 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    // Never leak a 30s sleeper if the guarantee regressed: clean it up ourselves
    // before asserting, while the group is still around to tear down its cgroup.
    if unsafe { libc::kill(pid as i32, 0) } == 0 {
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    }
    drop(group);

    killed.expect(
        "the detached kill-and-reap must SIGKILL the signal-catching child after the grace, \
         even though Drop aborted the deadline watchdog",
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
// `shutdown` refuses with ErrorReason::Unsupported — the caller tears the group down
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
        matches!(err.reason(), processkit::ErrorReason::Unsupported { .. }),
        "expected Unsupported, got {err:?}"
    );
    // The child survived (shared-group Drop doesn't kill); tear it down here.
    group.shutdown().await.expect("teardown the group");
}

// --- T-167: the observable graceful stop (`ProcessGroup::stop` → `ShutdownReport`) ---
//
// `stop(grace, escalate)` drives the SAME teardown as `shutdown_ref` but returns a
// `ShutdownReport` of what the kernel actually observed. All `process-control`-gated
// with the method (its `SoftSignal` carries a `Signal`).

// The headline property: a TERM-handling child drains early and the report proves
// it by *actual duration*, not a poll count — the whole 10s grace is NOT spent.
// Also exercises the re-stop-after-teardown case at the end.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess; T-167 stop() reports an early drain by actual duration"]
async fn stop_reports_an_early_drain_without_spending_the_whole_grace() {
    use processkit::{Signal, SoftSignal};

    let group = ProcessGroup::new().expect("create group");
    // Exits 0 on SIGTERM, parked on an interruptible `read` — zero forks (see the
    // sibling `shutdown_lets_a_term_handling_child_end_the_grace_early`).
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
    // Reap concurrently: the pgroup liveness probe reads a zombie as alive, so the
    // child must actually be collected for the early drain (and `members_after == 0`).
    let waiter = tokio::spawn(run.wait());

    let report = tokio::time::timeout(
        Duration::from_secs(20),
        group.stop(Duration::from_secs(10), true),
    )
    .await
    .expect("stop bounded")
    .expect("stop ok");

    assert!(
        report.drained_within_grace(),
        "the TERM-handling child drained within the grace (report: {report:?})"
    );
    assert!(!report.escalated(), "an in-time drain needs no hard kill");
    assert!(
        report.elapsed() < Duration::from_secs(8),
        "an early drain must NOT spend the whole 10s grace (report: {report:?})"
    );
    assert_eq!(
        report.attempted_signal(),
        Some(Signal::Term),
        "the graceful soft signal is SIGTERM"
    );
    assert_eq!(
        report.soft_signal(),
        SoftSignal::Sent(Signal::Term),
        "the soft signal was delivered"
    );
    assert!(
        report.members_before().is_some_and(|n| n >= 1),
        "at least the one child was alive before the signal (report: {report:?})"
    );
    assert_eq!(
        report.members_after(),
        Some(0),
        "the tree is empty after the drain (report: {report:?})"
    );

    let outcome = waiter.await.expect("join").expect("wait");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "the child exited via its TERM trap"
    );

    // Call-after-teardown: a second `stop` on the now-drained group must return
    // promptly (a near no-op), not hang or panic.
    let again = tokio::time::timeout(
        Duration::from_secs(5),
        group.stop(Duration::from_secs(5), true),
    )
    .await
    .expect("a second stop on a drained group must not hang")
    .expect("second stop ok");
    assert!(
        again.drained_within_grace(),
        "the empty group is trivially drained"
    );
    assert!(!again.escalated(), "nothing to escalate on an empty group");
    assert_eq!(again.members_before(), Some(0), "no members remain");
    assert!(
        again.elapsed() < Duration::from_secs(2),
        "a re-stop of a drained group is prompt (took {:?})",
        again.elapsed()
    );
}

// A TERM-ignoring child rides the grace and is hard-killed; the report says so
// (`drained_within_grace == false`, `escalated == true`).
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a TERM-ignoring subprocess; T-167 stop() reports escalation"]
async fn stop_reports_escalation_of_a_term_ignoring_child() {
    use processkit::{Signal, SoftSignal};

    let group = ProcessGroup::new().expect("create group");
    let mut run = group
        .start(&Command::new("sh").args(["-c", "trap '' TERM; echo ready; while :; do :; done"]))
        .await
        .expect("start");
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(10))
        .await
        .expect("trap installed");
    let waiter = tokio::spawn(run.wait());

    let report = tokio::time::timeout(
        Duration::from_secs(15),
        group.stop(Duration::from_millis(500), true),
    )
    .await
    .expect("escalation keeps stop bounded")
    .expect("stop ok");

    assert!(
        !report.drained_within_grace(),
        "a TERM-ignoring child does not drain within the grace"
    );
    assert!(
        report.escalated(),
        "the undrained tree was escalated to a hard kill (report: {report:?})"
    );
    assert_eq!(
        report.soft_signal(),
        SoftSignal::Sent(Signal::Term),
        "the SIGTERM was still delivered (the child ignored it)"
    );
    assert!(
        report.elapsed() >= Duration::from_millis(300),
        "the grace is waited out before escalating (report: {report:?})"
    );

    let outcome = waiter.await.expect("join").expect("wait");
    assert!(
        matches!(outcome, Outcome::Signalled(_)),
        "SIGKILL surfaces as a signal kill, got {outcome:?}"
    );
}

// The "kill and wait" path: `stop(Duration::ZERO, true)` kills the tree at once
// (a zero grace elapses immediately) and reports what it observed — unlike bare
// `kill_all`, which returns as soon as the kill is *issued* with no report.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess; T-167 stop(ZERO, true) kills and reports"]
async fn stop_with_zero_grace_kills_and_reports() {
    use processkit::{Signal, SoftSignal};

    let group = ProcessGroup::new().expect("create group");
    let mut run = group
        .start(&Command::new("sh").args(["-c", "trap '' TERM; echo ready; while :; do :; done"]))
        .await
        .expect("start");
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(10))
        .await
        .expect("trap installed");
    let waiter = tokio::spawn(run.wait());

    let report = tokio::time::timeout(Duration::from_secs(10), group.stop(Duration::ZERO, true))
        .await
        .expect("zero-grace stop is prompt")
        .expect("stop ok");

    assert!(
        !report.drained_within_grace(),
        "a zero grace leaves no window to drain in"
    );
    assert!(
        report.escalated(),
        "the live tree is hard-killed at once (report: {report:?})"
    );
    assert_eq!(report.soft_signal(), SoftSignal::Sent(Signal::Term));
    assert!(
        report.members_before().is_some_and(|n| n >= 1),
        "the child was alive before the kill (report: {report:?})"
    );

    let outcome = waiter.await.expect("join").expect("wait");
    assert!(
        matches!(outcome, Outcome::Signalled(_)),
        "hard-killed, got {outcome:?}"
    );
}

// Behavior on a group with NO live members: an empty group drains trivially and
// reports zero members, no escalation, and an immediate return.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "creates a real process group; T-167 stop() on a group with no live members"]
async fn stop_on_an_empty_group_reports_no_members() {
    use processkit::{Signal, SoftSignal};

    let group = ProcessGroup::new().expect("create group");
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        group.stop(Duration::from_millis(200), true),
    )
    .await
    .expect("stop bounded")
    .expect("stop ok");

    assert_eq!(
        report.members_before(),
        Some(0),
        "an empty group has no members"
    );
    assert_eq!(report.members_after(), Some(0), "still none afterwards");
    assert!(
        report.drained_within_grace(),
        "an empty tree is trivially drained"
    );
    assert!(!report.escalated(), "nothing to escalate");
    assert_eq!(
        report.soft_signal(),
        SoftSignal::Sent(Signal::Term),
        "a real SIGTERM tier exists on unix even over an empty group"
    );
    assert!(
        report.elapsed() < Duration::from_secs(1),
        "an empty group drains immediately, not after the grace (took {:?})",
        report.elapsed()
    );
}
