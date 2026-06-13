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

    let start = Instant::now();
    let err = run
        .output_string()
        .await
        .expect_err("a cancelled run must error, not produce a result");
    assert!(
        matches!(err, processkit::Error::Cancelled { .. }),
        "expected Error::Cancelled, got {err:?}"
    );
    // Promptness: the sleeper runs ~30s if cancellation is broken. Generous
    // headroom for full-suite load (cf. the widened timeout-test bounds).
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "cancel was not prompt (took {:?})",
        start.elapsed()
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
    // when the token fires, surfacing Error::Cancelled to the awaiting call.
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

    let start = Instant::now();
    let err = client
        .output(cmd)
        .await
        .expect_err("a cancelled run must error, not produce a result");
    assert!(
        matches!(err, processkit::Error::Cancelled { .. }),
        "expected Error::Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "client-default cancel was not prompt (took {:?})",
        start.elapsed()
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

    let start = Instant::now();
    // A program that doesn't exist: reaching the OS spawn would fail with
    // an Io error, so getting Cancelled proves the short-circuit fired
    // before any spawn was attempted.
    let err = Command::new("processkit-no-such-program-424242")
        .cancel_on(token)
        .run()
        .await
        .expect_err("a pre-cancelled run must not start");
    assert!(
        matches!(err, processkit::Error::Cancelled { .. }),
        "expected Error::Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "short-circuit was not immediate (took {:?})",
        start.elapsed()
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
        matches!(err, processkit::Error::Cancelled { .. }),
        "expected Error::Cancelled, got {err:?}"
    );
}
