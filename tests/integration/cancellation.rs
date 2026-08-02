//! Cancellation: `Command::cancel_on` and token-driven teardown.

#[cfg(unix)]
use std::path::Path;
use std::time::{Duration, Instant};

use processkit::{CancellationToken, Command, ProcessGroup};

use crate::common::*;

/// Whether a process with `pid` is still alive, per platform.
fn pid_alive(pid: u32) -> bool {
    #[cfg(windows)]
    return windows_pid_alive(pid);
    #[cfg(unix)]
    // SAFETY: signal 0 is a sound liveness probe.
    return unsafe { libc::kill(pid as i32, 0) == 0 };
}

#[tokio::test]
#[ignore = "spawns real subprocesses and cancels one mid-run"]
async fn cancel_mid_run_errors_and_kills_only_the_cancelled_child() {
    let group = ProcessGroup::new().expect("create group");
    let token = CancellationToken::new();

    // A sibling in the same shared group: cancellation must not touch it
    // (same child-only scope as a timeout on a shared-group handle).
    let sibling = group.start(&sleep_secs(30)).await.expect("start sibling");
    let sibling_pid = sibling.pid().expect("sibling pid");

    // Single-process sleeper, deliberately: the cmd-wrapped `sleeper()` is
    // two processes on Windows, and the child-only cancel kill would leave
    // the grandchild holding the stdout pipe — stalling teardown for the
    // full pump grace instead of ending promptly.
    let run = group
        .start(&sleep_secs(30).cancel_on(token.clone()))
        .await
        .expect("start cancellable sleeper");
    let pid = run.pid().expect("pid");

    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            token.cancel();
        }
    });

    // Promptness bound: the sleeper runs ~30s if cancellation is broken.
    // Generous headroom for full-suite load (cf. the widened timeout bounds).
    let err = completes_within(
        Duration::from_secs(10),
        "cancelled run",
        run.output_string(),
    )
    .await
    .expect_err("a cancelled run must error, not produce a result");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
    canceller.await.expect("canceller task");
    // The cancelled child is dead AND reaped by the time `output_string`
    // returned: the cancel arm's kill_tree start-kills and then awaits the
    // child. (No raw post-mortem pid probe here: a dead pid is recycled by
    // a parallel-suite neighbour within seconds on Windows, which made an
    // earlier probe loop flake.) The prompt Err above is the death proof.

    // The shared group's sibling is untouched — probing a process we hold
    // a live handle to is reuse-safe.
    let _ = pid;
    assert!(
        pid_alive(sibling_pid),
        "cancel must kill the child only, not shared-group siblings"
    );
    drop(sibling);
}

#[tokio::test]
#[ignore = "spawns a real subprocess through a client-level cancellation default"]
async fn client_default_cancel_on_cancels_a_real_run() {
    use processkit::CliClient;

    // The client-level default (`default_cancel_on`) acceptance: a hanging
    // child run through a client configured once is killed — tree and all —
    // when the token fires, surfacing ErrorReason::Cancelled to the awaiting call.
    let token = CancellationToken::new();
    let sleeper = sleep_secs(30);
    let client = CliClient::new(sleeper.program()).default_cancel_on(token.clone());
    let cmd = client.command(sleeper.arguments().iter().map(|a| a.to_os_string()));

    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            token.cancel();
        }
    });

    let err = completes_within(
        Duration::from_secs(10),
        "client-default cancel",
        client.output_string(cmd),
    )
    .await
    .expect_err("a cancelled run must error, not produce a result");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
    canceller.await.expect("canceller task");
    // Death proof: the prompt Cancelled return (the cancel arm kills the tree
    // and awaits the child) — same rationale as the per-command test above.
}

#[tokio::test]
#[ignore = "exercises the pre-spawn short-circuit (no real subprocess)"]
async fn pre_cancelled_token_short_circuits_before_spawning() {
    let token = CancellationToken::new();
    token.cancel();

    // A program that doesn't exist: reaching the OS spawn would fail with
    // an Io error, so getting Cancelled proves the short-circuit fired
    // before any spawn was attempted — and immediately (a 2s bound).
    let err = completes_within(
        Duration::from_secs(2),
        "pre-cancelled short-circuit",
        Command::new("processkit-no-such-program-424242")
            .cancel_on(token)
            .run(),
    )
    .await
    .expect_err("a pre-cancelled run must not start");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and cancels it mid-stream"]
async fn cancel_ends_the_stream_and_finish_reports_it() {
    use tokio_stream::StreamExt;

    let token = CancellationToken::new();
    // Windows: the Job kill is atomic — the cmd-wrapped banner child is
    // fine. Unix: deliberately FORK-FREE (`read` parks the shell itself,
    // stdin kept open) — a `sleep 30` forked at cancel time escaped the
    // pgroup broadcast on macOS CI (killpg is documented best-effort
    // against a forking tree) and held the stdout pipe open past the
    // stream bound.
    let child = if cfg!(windows) {
        banner_then_idle()
    } else {
        Command::new("sh")
            .args(["-c", "echo ready; read line"])
            .keep_stdin_open()
    };
    let mut run = child
        .cancel_on(token.clone())
        .start()
        .await
        .expect("start banner child");

    let pid = run.pid().expect("pid");
    let mut lines = run.stdout_lines().unwrap();
    // Wait for the banner so the cancel provably lands mid-stream.
    let first = tokio::time::timeout(Duration::from_secs(15), lines.next())
        .await
        .expect("banner in time")
        .expect("banner line");
    assert!(first.contains("ready"), "line: {first:?}");

    token.cancel();

    // The cancel tears the (handle-owned) tree down, the pipes close, and
    // the stream ends — the child would otherwise idle ~30s. On a timeout,
    // report whether the direct child is even dead — that separates "the
    // kill never landed" from "the pipe stayed open" (seen once on macOS
    // CI; the probe makes the next occurrence diagnosable).
    let start = Instant::now();
    loop {
        match tokio::time::timeout(Duration::from_secs(15), lines.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => panic!(
                "stream did not end within 15s of the cancel \
                 (direct child still alive: {})",
                pid_alive(pid)
            ),
        }
    }
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "stream did not end promptly (took {:?})",
        start.elapsed()
    );

    let err = run
        .finish()
        .await
        .expect_err("finishing a cancelled streamed run must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and cancels a streaming first_line probe"]
async fn first_line_cancel_surfaces_cancelled_promptly() {
    // first_line streams stdout for a match; if the shared token fires before a
    // match appears, it must surface Err(Cancelled) — promptly, not after the
    // child's long idle. (E7 rewrote first_line's cancel path from a post-stream
    // token re-check to a select racing the token against the search; this is the
    // end-to-end guard the scripted unit tests can't provide deterministically.)
    let token = CancellationToken::new();
    // A stdin-INDEPENDENT idle: first_line drops the child's stdin, so a
    // `read`-blocked shell would see EOF and exit before the cancel lands. A
    // plain sleeper idles regardless and is fork-free on Unix (single process,
    // so the pgroup kill has nothing to escape); it produces no output, so the
    // `|_| false` predicate never matches and the search stays pending until the
    // cancel closes it.
    let idle = if cfg!(windows) {
        Command::new("cmd").args(["/c", "ping -n 31 127.0.0.1 >nul"])
    } else {
        Command::new("sleep").arg("30")
    };
    let probe = idle.cancel_on(token.clone());

    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        }
    });

    let result = completes_within(
        Duration::from_secs(15),
        "cancelled first_line probe",
        probe.first_line(|_| false),
    )
    .await;
    canceller.await.expect("canceller task");
    assert!(
        matches!(&result, Err(e) if matches!(e.reason(), processkit::ErrorReason::Cancelled { .. })),
        "a cancelled streaming probe must error Cancelled, got {result:?}"
    );
}

// `members()` (the no-leak assertion) is gated on `process-control`.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess in a shared group and cancels a first_line probe"]
async fn shared_group_first_line_cancel_tears_down_the_child() {
    // first_line on a SHARED group: on cancel the search is drained to its
    // watchdog-closed end before returning, so the child is actually torn down
    // rather than leaked. (A shared-group handle's Drop doesn't kill the tree and,
    // where `kill_on_drop` is disarmed after containment — Windows Job Objects —
    // returning early would abort the cancel watchdog before it fires, stranding
    // the child.) Verify both the Cancelled result and that the group has no
    // surviving member afterward. A single-process idle keeps the watchdog's
    // direct-child kill sufficient (a forking tree is the separate shared-group
    // teardown gap).
    use processkit::ProcessRunnerExt;
    let group = ProcessGroup::new().expect("create group");
    let token = CancellationToken::new();
    let idle = if cfg!(windows) {
        Command::new("ping").args(["-n", "31", "127.0.0.1"])
    } else {
        Command::new("sleep").arg("30")
    };
    let probe = idle.cancel_on(token.clone());

    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        }
    });

    let result = completes_within(
        Duration::from_secs(15),
        "shared-group first_line cancel",
        group.first_line(&probe, |_| false),
    )
    .await;
    canceller.await.expect("canceller task");
    assert!(
        matches!(&result, Err(e) if matches!(e.reason(), processkit::ErrorReason::Cancelled { .. })),
        "a cancelled shared-group probe must error Cancelled, got {result:?}"
    );

    // No leak: the shared group has no surviving member. The old early-return
    // code would (on a job-owned-teardown platform) abort the watchdog on drop
    // and leave the idle alive for its full run.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let members = group.members().expect("members");
        if members.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "first_line leaked a shared-group child: still-live members {members:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real forking child in a shared group and cancels a no-timeout first_line probe"]
async fn shared_group_first_line_cancel_without_timeout_is_bounded_on_a_forking_child() {
    // T-059: the `None`-timeout branch of `first_line`'s cancel drain. On a shared
    // group the cancel watchdog kills only the direct child by pid; a child that
    // forks a grandchild which inherits the piped stdout leaves that pipe open past
    // the kill, so the drain never sees EOF. With NO timeout there is no outer
    // whole-race backstop to lean on, so before the fix this `first_line` hung
    // until the grandchild's own 30s sleep elapsed. Assert it instead returns
    // promptly with the honest disposition — Cancelled, not a false Timeout (there
    // is no timeout configured to report).
    use processkit::ProcessRunnerExt;

    let group = ProcessGroup::new().expect("create group");
    let token = CancellationToken::new();
    // A shell that backgrounds a grandchild (`sleep 30 &`) which inherits the
    // piped stdout and holds it open, then sleeps itself. It writes nothing, so
    // the `|_| false` predicate never matches and the search stays pending until
    // the cancel. No `.timeout(..)` — this is deliberately the `None` branch.
    let forking = Command::new("sh")
        .args(["-c", "sleep 30 & sleep 30"])
        .cancel_on(token.clone());

    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            // Give the shell time to fork the grandchild that pins stdout open, so
            // the cancel provably lands with the teardown gap in effect.
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        }
    });

    // Broken (unbounded drain): hangs until the grandchild's 30s sleep exits.
    // Fixed: the drain backstop (~5s past the cancel) frees it well under this bound.
    let result = completes_within(
        Duration::from_secs(20),
        "no-timeout shared-group first_line cancel on a forking child",
        group.first_line(&forking, |_| false),
    )
    .await;
    canceller.await.expect("canceller task");
    assert!(
        matches!(&result, Err(e) if matches!(e.reason(), processkit::ErrorReason::Cancelled { .. })),
        "a cancelled no-timeout probe on a forking shared-group child must return \
         Cancelled promptly, got {result:?}"
    );

    // Reap the grandchild the pid-only cancel kill left behind: the shared group
    // still contains it, so dropping the group tears the whole subtree down.
    drop(group);
}

// --- T-255: graceful cancellation (`cancel_grace` / `cancel_signal`) ------------
//
// The cancellation mirror of the `timeout_grace` coverage in `shutdown.rs`, and
// modelled on it: children busy-wait in the shell (no separate `sleep` child that
// would defer the trap until it returns), and the proof that the *soft* tier ran is
// a side-effect only a catchable signal can produce — the TERM trap touching a
// marker file. A hard `SIGKILL` is uncatchable, so the marker's presence (or
// absence) separates "graceful" from "hard" without relying on timing alone.
//
// Unix-only: the soft tier is a real POSIX signal. The cross-platform half of the
// contract — the *outcome* is unchanged by the knob — is asserted by
// `cancel_grace_does_not_change_the_outcome` below, which runs everywhere.

/// A child that installs a `TERM` trap touching `term_marker` and exiting cleanly,
/// announces itself on stdout *and* by touching `ready_marker` (so both a streaming
/// and a bulk caller can wait for the trap to be installed before cancelling), then
/// busy-waits.
#[cfg(unix)]
fn term_trapping_child(ready_marker: &Path, term_marker: &Path) -> Command {
    Command::new("sh").args([
        "-c".to_string(),
        format!(
            "trap \"touch '{term}'; exit 0\" TERM; echo ready; touch '{ready}'; \
             while :; do :; done",
            term = term_marker.display(),
            ready = ready_marker.display(),
        ),
    ])
}

/// The same child, but deaf to `TERM` — it can only be forced down by the grace
/// window's final `SIGKILL`.
#[cfg(unix)]
fn term_ignoring_child(ready_marker: &Path) -> Command {
    Command::new("sh").args([
        "-c".to_string(),
        format!(
            "trap '' TERM; echo ready; touch '{ready}'; while :; do :; done",
            ready = ready_marker.display(),
        ),
    ])
}

/// Wait until the child has installed its trap (it touches `ready` immediately
/// after), so a cancellation provably lands on a child that *could* have caught the
/// soft signal — otherwise "the trap never ran" would prove nothing.
#[cfg(unix)]
async fn await_ready_marker(ready: &Path) {
    poll_until(
        Duration::from_secs(15),
        Duration::from_millis(20),
        "the child to install its TERM trap",
        || ready.exists(),
    )
    .await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and cancels it without a grace window"]
async fn cancel_without_grace_hard_kills_immediately() {
    // (a) THE DEFAULT IS UNCHANGED. No `cancel_grace` → the cancel arm hard-kills
    // at once, so the child's TERM trap never runs and the marker never appears.
    // This is the regression guard for "the new knobs are inert unless asked for".
    let dir = tempfile::tempdir().expect("temp dir");
    let (ready, term) = (dir.path().join("ready"), dir.path().join("term"));
    let token = CancellationToken::new();
    let cmd = term_trapping_child(&ready, &term).cancel_on(token.clone());

    let run = tokio::spawn(async move { cmd.output_string().await });
    await_ready_marker(&ready).await;
    token.cancel();

    let err = completes_within(Duration::from_secs(15), "hard cancel", run)
        .await
        .expect("run task")
        .expect_err("a cancelled run must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
    assert!(
        !term.exists(),
        "without cancel_grace the tree must be SIGKILLed outright — an uncatchable \
         signal, so the TERM trap must NOT have run"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and cancels it gracefully"]
async fn cancel_grace_lets_a_term_handling_child_exit_cleanly() {
    // (b) With `cancel_grace`, the cancel arm of `drive_to_exit_inner` drives the
    // same soft-signal → grace → hard-kill ladder the deadline uses: the child
    // catches TERM, touches its marker, and exits well inside the long grace (the
    // concurrent reap ends the window early). The outcome is still `Cancelled`.
    let dir = tempfile::tempdir().expect("temp dir");
    let (ready, term) = (dir.path().join("ready"), dir.path().join("term"));
    let token = CancellationToken::new();
    let cmd = term_trapping_child(&ready, &term)
        .cancel_on(token.clone())
        .cancel_grace(Duration::from_secs(10));

    let run = tokio::spawn(async move { cmd.output_string().await });
    await_ready_marker(&ready).await;
    let cancelled_at = Instant::now();
    token.cancel();

    let err = completes_within(Duration::from_secs(20), "graceful cancel", run)
        .await
        .expect("run task")
        .expect_err("a cancelled run must error however gently it was torn down");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "cancellation stays an error — only the teardown changed; got {err:?}"
    );
    assert!(
        term.exists(),
        "the graceful cancel must have delivered a catchable SIGTERM (the trap \
         writes {}, which a SIGKILL could never allow)",
        term.display()
    );
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(5),
        "a TERM-handling child must end the 10s grace early (took {:?})",
        cancelled_at.elapsed()
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a TERM-ignoring subprocess and cancels it; escalates after the grace"]
async fn cancel_grace_escalates_to_kill_after_the_grace() {
    // (c) A child that ignores the soft signal still dies — after, and only after,
    // the grace window elapses. The lower bound is what proves the grace was
    // actually waited out rather than skipped.
    let dir = tempfile::tempdir().expect("temp dir");
    let ready = dir.path().join("ready");
    let token = CancellationToken::new();
    let cmd = term_ignoring_child(&ready)
        .cancel_on(token.clone())
        .cancel_grace(Duration::from_millis(500));

    let run = tokio::spawn(async move { cmd.output_string().await });
    await_ready_marker(&ready).await;
    let cancelled_at = Instant::now();
    token.cancel();

    let err = completes_within(Duration::from_secs(20), "escalating cancel", run)
        .await
        .expect("run task")
        .expect_err("a cancelled run must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
    assert!(
        cancelled_at.elapsed() >= Duration::from_millis(400),
        "a TERM-ignoring child must ride out the grace before the SIGKILL (took {:?})",
        cancelled_at.elapsed()
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and cancels a streamed run through the cancel watchdog"]
async fn cancel_grace_is_honored_by_the_cancel_watchdog_on_a_streamed_run() {
    use tokio_stream::StreamExt;

    // The `arm_cancel_watchdog` path — the motivating gap. On a live streamed run
    // no finisher owns the child yet, so the DETACHED cancel watchdog is the only
    // thing that can tear the tree down when the token fires. Before this change it
    // unconditionally `kill_all()`ed; it must now drive the group's graceful
    // terminate instead when `cancel_grace` is set.
    //
    // The proof is ordered, not timing-based: the stream can only end once the
    // child's pipes close, i.e. once the child exited — and the marker is asserted
    // BEFORE `finish()` is ever called, so it can only have been the watchdog's
    // signal that produced it.
    let dir = tempfile::tempdir().expect("temp dir");
    let (ready, term) = (dir.path().join("ready"), dir.path().join("term"));
    let token = CancellationToken::new();
    let mut run = term_trapping_child(&ready, &term)
        .cancel_on(token.clone())
        .cancel_grace(Duration::from_secs(10))
        .start()
        .await
        .expect("start");

    let mut lines = run.stdout_lines().expect("stream stdout");
    let first = completes_within(Duration::from_secs(15), "the ready banner", lines.next())
        .await
        .expect("banner line");
    assert_eq!(first, "ready", "the trap is installed before the banner");

    let cancelled_at = Instant::now();
    token.cancel();

    // The graceful teardown ends the stream: the trap exits the child and its pipes
    // close. An unwired graceful branch would leave the busy-loop running until the
    // grace elapsed into a SIGKILL — well past this bound.
    completes_within(
        Duration::from_secs(8),
        "the cancelled stream to end",
        async { while lines.next().await.is_some() {} },
    )
    .await;
    assert!(
        term.exists(),
        "the cancel WATCHDOG (no finisher has run yet) must have delivered the \
         catchable SIGTERM, not a SIGKILL"
    );
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(5),
        "a TERM-handling streamed child must end the 10s grace early (took {:?})",
        cancelled_at.elapsed()
    );

    let err = run
        .finish()
        .await
        .expect_err("finishing a cancelled streamed run must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess in a SHARED group and cancels it gracefully"]
async fn cancel_grace_in_a_shared_group_signals_the_direct_child() {
    // A shared-group handle owns no group, so the graceful cancellation reaches only
    // its own direct child — by pid, through the same `PidGate`-scoped ladder the
    // shared-group graceful *timeout* uses. The sibling must be untouched, exactly
    // as for a hard cancel.
    let group = ProcessGroup::new().expect("create group");
    let sibling = group.start(&sleep_secs(30)).await.expect("start sibling");
    let sibling_pid = sibling.pid().expect("sibling pid");

    let dir = tempfile::tempdir().expect("temp dir");
    let (ready, term) = (dir.path().join("ready"), dir.path().join("term"));
    let token = CancellationToken::new();
    let run = group
        .start(
            &term_trapping_child(&ready, &term)
                .cancel_on(token.clone())
                .cancel_grace(Duration::from_secs(10)),
        )
        .await
        .expect("start cancellable child");

    await_ready_marker(&ready).await;
    let cancelled_at = Instant::now();
    token.cancel();

    let err = completes_within(
        Duration::from_secs(20),
        "shared-group graceful cancel",
        run.output_string(),
    )
    .await
    .expect_err("a cancelled run must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
    assert!(
        term.exists(),
        "the shared-group graceful cancel must signal the direct child, not SIGKILL it"
    );
    assert!(
        cancelled_at.elapsed() < Duration::from_secs(5),
        "a TERM-handling child must end the 10s grace early (took {:?})",
        cancelled_at.elapsed()
    );
    assert!(
        pid_alive(sibling_pid),
        "a graceful cancel keeps the same child-only scope as a hard one"
    );
    drop(sibling);
}

#[tokio::test]
#[ignore = "spawns a real subprocess and cancels it with a grace window configured"]
async fn cancel_grace_does_not_change_the_outcome() {
    // The cross-platform half of the contract, and the only graceful-cancellation
    // test that runs on Windows (where there is no POSIX signal tier, so the ladder
    // degrades to the atomic Job kill and `grace` goes unused — as documented for
    // `timeout_grace`). Either way the run must resolve to `Cancelled`, promptly:
    // riding out the whole grace would fail the bound.
    let token = CancellationToken::new();
    let cmd = sleep_secs(30)
        .cancel_on(token.clone())
        .cancel_grace(Duration::from_secs(10));

    let canceller = tokio::spawn({
        let token = token.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            token.cancel();
        }
    });

    let start = Instant::now();
    let err = completes_within(
        Duration::from_secs(15),
        "cancel with a grace window",
        cmd.output_string(),
    )
    .await
    .expect_err("a cancelled run must error, grace window or not");
    canceller.await.expect("canceller task");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected ErrorReason::Cancelled, got {err:?}"
    );
    // A plain sleeper does not catch the soft signal (and Windows kills atomically),
    // so the grace must end early on every platform rather than being ridden out.
    assert!(
        start.elapsed() < Duration::from_secs(8),
        "the grace must end as soon as the tree drains (took {:?})",
        start.elapsed()
    );
}
