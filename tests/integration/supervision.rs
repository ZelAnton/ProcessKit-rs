//! Supervisor: restart policies, shared-group incarnations, and restart
//! exhaustion.

use std::time::Duration;

use processkit::{Command, ProcessGroup};

use crate::common::*;

#[tokio::test]
#[ignore = "spawns real subprocesses under supervision in a shared group"]
async fn supervisor_runs_incarnations_in_a_shared_group() {
    use processkit::{RestartPolicy, StopReason, Supervisor};

    let exits_zero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };

    // The headline `with_runner(&group)` path: every incarnation runs inside
    // one caller-owned kill-on-drop group, and the group stays usable after.
    let group = ProcessGroup::new().expect("create group");
    let outcome = Supervisor::new(exits_zero)
        .with_runner(&group)
        .restart(RestartPolicy::OnCrash)
        .backoff(Duration::from_millis(1), 1.0)
        .jitter(false)
        .run()
        .await
        .expect("supervision completes");
    assert_eq!(outcome.stopped, StopReason::PolicySatisfied);
    assert!(outcome.final_result.is_success());

    // The shared group survived supervision and still works.
    let _after = group
        .start(&sleep_secs(1))
        .await
        .expect("group still usable");
}

#[tokio::test]
#[ignore = "spawns real subprocesses repeatedly under supervision"]
async fn storm_guard_pauses_a_real_failure_storm() {
    use processkit::{RestartPolicy, Supervisor};

    let always_fails = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "1"])
    } else {
        Command::new("sh").args(["-c", "exit 1"])
    };

    // Real-subprocess wiring only — exact timing/decay semantics live in the
    // hermetic unit tests. A crash storm with a low threshold must report at
    // least one storm pause.
    let outcome = Supervisor::new(always_fails)
        .restart(RestartPolicy::OnCrash)
        .max_restarts(4)
        .backoff(Duration::from_millis(1), 1.0)
        .jitter(false)
        .storm_pause(Duration::from_millis(20))
        .failure_threshold(1.5)
        .failure_decay(Duration::from_secs(60))
        .run()
        .await
        .expect("supervision completes with a result");
    assert_eq!(outcome.restarts, 4);
    assert!(
        outcome.storm_pauses >= 1,
        "a real crash storm must trip the guard: {outcome:?}"
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses repeatedly under supervision"]
async fn always_restarts_a_real_successful_child_until_the_budget_runs_out() {
    use processkit::{RestartPolicy, StopReason, Supervisor};

    // F: `RestartPolicy::Always` restarts a CLEAN exit too (unlike `OnCrash`,
    // which only restarts a failure) — real-subprocess wiring for a case
    // covered only at the unit level today. With no predicate to satisfy, the
    // budget is what ends supervision.
    let exits_zero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };

    let outcome = Supervisor::new(exits_zero)
        .restart(RestartPolicy::Always)
        .max_restarts(2)
        .backoff(Duration::from_millis(1), 1.0)
        .jitter(false)
        .run()
        .await
        .expect("supervision completes with a result");

    assert_eq!(
        outcome.restarts, 2,
        "Always restarts a clean exit too — two restarts = three real runs"
    );
    assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
    assert!(
        outcome.final_result.is_success(),
        "the final incarnation was a clean exit: {:?}",
        outcome.final_result
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses repeatedly under supervision"]
async fn supervisor_exhausts_restarts_on_a_crashing_child() {
    use processkit::{RestartPolicy, StopReason, Supervisor};

    let always_fails = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "1"])
    } else {
        Command::new("sh").args(["-c", "exit 1"])
    };

    let outcome = Supervisor::new(always_fails)
        .restart(RestartPolicy::OnCrash)
        .max_restarts(2)
        .backoff(Duration::from_millis(1), 1.0)
        .jitter(false)
        .run()
        .await
        .expect("supervision completes with a result");

    assert_eq!(outcome.restarts, 2, "two restarts = three real runs");
    assert_eq!(outcome.stopped, StopReason::RestartsExhausted);
    assert_eq!(outcome.final_result.code(), Some(1));
}
