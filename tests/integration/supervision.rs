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

#[tokio::test]
#[ignore = "starts a real supervised child and stops it through a live session"]
async fn session_gracefully_stops_a_real_supervised_child() {
    use processkit::{RestartPolicy, StopReason, Supervisor};

    // The live-session path against a REAL long-running child under the default
    // own-group JobRunner: `status()` exposes the running incarnation's real pid,
    // and `stop()` ends it through its group's graceful teardown, reporting a
    // deliberate `StopReason::Stopped` (never a crash/exhaustion/cancellation).
    let session = Supervisor::new(sleep_secs(30))
        .restart(RestartPolicy::Always)
        .start();

    // Wait until the first incarnation is live, with a real OS pid published.
    let mut live_pid = None;
    for _ in 0..400 {
        let status = session.status();
        if status.is_active()
            && let Some(pid) = status.pid()
        {
            live_pid = Some(pid);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        live_pid.is_some(),
        "the supervised child should go live with a real pid"
    );

    let outcome = session
        .stop(Duration::from_secs(2))
        .await
        .expect("a graceful stop yields an outcome");
    assert_eq!(
        outcome.stopped,
        StopReason::Stopped,
        "a session stop ends supervision deliberately: {outcome:?}"
    );
    assert_eq!(
        outcome.restarts, 0,
        "the first incarnation was stopped, not restarted"
    );
}
