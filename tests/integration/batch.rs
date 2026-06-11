//! Batch helpers over real subprocesses: wait_all and output_all.

use std::time::Duration;

use processkit::{Command, Outcome, ProcessGroup, output_all, wait_all};

use crate::common::*;

#[tokio::test]
#[ignore = "spawns real subprocesses and joins on all of them"]
async fn wait_all_collects_every_exit_code_in_order() {
    let group = ProcessGroup::new().expect("create group");
    // Mixed finish order: a quick printer between two sleepers, so the join
    // genuinely has to wait on the slow ones, not just the first finisher.
    let mut a = group.start(&sleep_secs(1)).await.expect("start a");
    let mut b = group.start(&five_lines()).await.expect("start b");
    let mut c = group.start(&sleep_secs(2)).await.expect("start c");

    let codes = tokio::time::timeout(
        Duration::from_secs(20),
        wait_all(&mut [&mut a, &mut b, &mut c]),
    )
    .await
    .expect("join finished in time")
    .expect("join");

    assert_eq!(codes.len(), 3, "one code per process, in input order");
    assert!(
        codes.iter().all(|c| *c == Outcome::Exited(0)),
        "every child exits cleanly: {codes:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a stdin-reading subprocess joined via wait_all"]
async fn wait_all_closes_an_untaken_keep_stdin_open_pipe() {
    // L5: a `keep_stdin_open` child whose stdin pipe was never taken must see
    // EOF when joined via `wait_all` (which goes through `wait_exit` and applies
    // no timeout) — otherwise a stdin-reading child (`cat`/`sort`) blocks
    // forever and hangs the join. The race path must close the untaken pipe just
    // as the bulk verbs do.
    let group = ProcessGroup::new().expect("create group");
    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"]).keep_stdin_open()
    } else {
        Command::new("cat").keep_stdin_open()
    };
    let mut child = group.start(&reads_stdin).await.expect("start");
    let outcomes = tokio::time::timeout(Duration::from_secs(15), wait_all(&mut [&mut child]))
        .await
        .expect("wait_all must not hang on an untaken keep_stdin_open pipe")
        .expect("wait_all ok");
    assert_eq!(outcomes.len(), 1);
    assert!(
        matches!(outcomes[0], Outcome::Exited(_)),
        "the stdin-reading child must see EOF and exit, got: {:?}",
        outcomes[0]
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses with a bounded batch"]
async fn output_all_runs_a_bounded_batch_and_collects_all() {
    let group = ProcessGroup::new().expect("create group");
    // Six commands, cap two: the bound throttles, every result still lands.
    let cmds: Vec<Command> = (0..6).map(|_| five_lines()).collect();
    let results = output_all(cmds, 2, &group).await;

    assert_eq!(results.len(), 6);
    for (i, r) in results.iter().enumerate() {
        let out = r
            .as_ref()
            .unwrap_or_else(|e| panic!("command {i} errored: {e}"));
        assert!(
            out.is_success(),
            "command {i} exits 0, got {:?}",
            out.code()
        );
    }
}

#[tokio::test]
#[ignore = "spawns real subprocesses; a non-zero exit is collected, not raised"]
async fn output_all_collects_a_failing_command_as_data() {
    let group = ProcessGroup::new().expect("create group");
    let cmds = vec![five_lines(), failing_exit(3), five_lines()];
    let results = output_all(cmds, 3, &group).await;

    assert_eq!(results.len(), 3);
    assert!(results[0].as_ref().expect("ok").is_success());
    assert_eq!(
        results[1]
            .as_ref()
            .expect("non-zero exit is Ok data")
            .code(),
        Some(3),
        "the failure is collected as data, not short-circuited"
    );
    assert!(results[2].as_ref().expect("ok").is_success());
}
