//! Batch helpers over real subprocesses: wait_all and output_all.

use std::time::Duration;

use processkit::{Command, ProcessGroup, output_all, wait_all};

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
        codes.iter().all(|c| *c == Some(0)),
        "every child exits cleanly: {codes:?}"
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
