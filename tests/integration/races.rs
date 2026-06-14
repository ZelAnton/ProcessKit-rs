//! Exit races and explicit kills: wait_any and start_kill.

use std::time::{Duration, Instant};

use processkit::{Outcome, ProcessGroup, wait_any};

use crate::common::*;

#[tokio::test]
#[ignore = "spawns real subprocesses and races their exits"]
async fn wait_any_returns_first_finisher() {
    let group = ProcessGroup::new().expect("create group");
    let mut slow = group.start(&sleep_secs(15)).await.expect("start slow");
    let mut fast = group.start(&sleep_secs(1)).await.expect("start fast");

    let (idx, outcome) = tokio::time::timeout(
        Duration::from_secs(10),
        wait_any(&mut [&mut slow, &mut fast]),
    )
    .await
    .expect("race finished in time")
    .expect("race");
    assert_eq!(idx, 1, "the 1-second sleeper should finish first");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "the fast sleeper exits cleanly"
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses; proves the race loser stays usable"]
async fn wait_any_losers_still_waitable() {
    let group = ProcessGroup::new().expect("create group");
    // A single-process sleeper: `start_kill` must hit the process holding the
    // pipes, or `wait` idles out the pump-teardown grace for an orphaned child.
    let mut slow = group.start(&sleep_secs(30)).await.expect("start slow");
    let mut fast = group.start(&sleep_secs(1)).await.expect("start fast");

    let (idx, _outcome) = tokio::time::timeout(
        Duration::from_secs(10),
        wait_any(&mut [&mut slow, &mut fast]),
    )
    .await
    .expect("race finished in time")
    .expect("race");
    assert_eq!(idx, 1);

    // The loser was only borrowed by the race — kill it and reap it promptly to
    // prove the handle still works end-to-end.
    slow.start_kill().expect("kill the loser");
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), slow.wait())
        .await
        .expect("loser reaped in time")
        .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "loser wait was not prompt (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses; proves a keep_stdin_open loser keeps its stdin (B15)"]
async fn wait_any_loser_keeps_its_stdin() {
    let group = ProcessGroup::new().expect("create group");
    // Loser: reads a line from stdin (blocks until a line + EOF, so it never
    // exits on its own) with stdin kept open and NOT taken — the race must not
    // close its stdin pipe out from under the caller.
    let mut waiter = group
        .start(&first_line_consumer().keep_stdin_open())
        .await
        .expect("start waiter");
    let mut fast = group.start(&sleep_secs(1)).await.expect("start fast");

    let (idx, _) = tokio::time::timeout(
        Duration::from_secs(10),
        wait_any(&mut [&mut waiter, &mut fast]),
    )
    .await
    .expect("race finished in time")
    .expect("race");
    assert_eq!(idx, 1, "the fast sleeper wins the race");

    // B15: the loser was only borrowed — its untaken keep_stdin_open pipe must
    // still be present (the race must not have closed it, which would leave
    // take_stdin() returning None and the child wedged on a premature EOF).
    let mut stdin = waiter
        .take_stdin()
        .expect("B15: a wait_any loser's stdin must remain usable");
    // It works end to end: feed the line it's blocked on, then watch it exit.
    let _ = stdin.write_line("hello").await;
    let _ = stdin.finish().await;
    let outcome = tokio::time::timeout(Duration::from_secs(10), waiter.wait())
        .await
        .expect("loser reaped in time")
        .expect("wait");
    assert!(matches!(outcome, Outcome::Exited(0)), "got {outcome:?}");
}

#[tokio::test]
#[ignore = "spawns a long-lived subprocess and kills it early"]
async fn start_kill_terminates_a_running_process() {
    let mut process = sleeper().start().await.expect("start sleeper");
    assert!(process.pid().is_some());
    process.start_kill().expect("start_kill");
    // After an explicit kill, waiting returns far sooner than the sleeper's ~30s
    // runtime. The exit code of a killed process is platform-dependent, so
    // promptness is the guarantee under test.
    let start = Instant::now();
    let _ = tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .expect("killed process should be reaped promptly")
        .expect("wait");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "kill was not prompt (took {:?})",
        start.elapsed()
    );
}
