//! Cancellation: `Command::cancel_on` and token-driven teardown.

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
