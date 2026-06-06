//! Pipelines: data flow between stages, pipefail attribution, whole-chain
//! timeouts, and first-stage stdin.

use std::time::{Duration, Instant};

use processkit::Command;

use crate::common::*;

/// A stage that copies stdin to stdout, per platform (`sort` keeps order-free
/// assertions simple on Windows; `cat` on Unix).
fn sort_stage() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("sort")
    }
}

#[tokio::test]
#[ignore = "spawns a real two-stage pipeline"]
async fn pipeline_flows_data_between_stages() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo delta& echo alpha"])
    } else {
        Command::new("sh").args(["-c", "printf 'delta\\nalpha\\n'"])
    };

    let result = producer
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    let stdout = result.stdout();
    let alpha = stdout.find("alpha").expect("alpha in output");
    let delta = stdout.find("delta").expect("delta in output");
    assert!(alpha < delta, "sort should reorder: {stdout:?}");
}

#[tokio::test]
#[ignore = "spawns a real three-stage pipeline"]
async fn pipeline_three_stages_end_to_end() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo bb& echo aa& echo bb"])
    } else {
        Command::new("sh").args(["-c", "printf 'bb\\naa\\nbb\\n'"])
    };
    let filter = if cfg!(windows) {
        Command::new("findstr").arg("bb")
    } else {
        Command::new("grep").arg("bb")
    };

    let result = producer
        .pipe(sort_stage())
        .pipe(filter)
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    assert!(
        result.stdout().contains("bb"),
        "stdout: {:?}",
        result.stdout()
    );
    assert!(
        !result.stdout().contains("aa"),
        "filter stage should drop aa: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline with a failing inner stage"]
async fn pipeline_pipefail_attributes_the_first_failure() {
    // A tiny producer exits 0 (its few bytes fit the pipe buffer even though
    // the next stage never reads); the middle stage fails with a distinctive
    // code; the final stage succeeds reading EOF.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "3"])
    } else {
        Command::new("sh").args(["-c", "exit 3"])
    };

    let result = producer
        .pipe(failing)
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("pipeline completes with a result");
    assert_eq!(result.code(), Some(3), "pipefail code: {result:?}");
    assert!(!result.is_success());

    // run() surfaces the same attribution as a typed error.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "3"])
    } else {
        Command::new("sh").args(["-c", "exit 3"])
    };
    let err = producer
        .pipe(failing)
        .pipe(sort_stage())
        .run()
        .await
        .expect_err("a failing stage must fail run()");
    assert!(
        matches!(err, processkit::Error::Exit { code: 3, .. }),
        "expected Exit with code 3, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline and kills it at the deadline"]
async fn pipeline_timeout_kills_the_whole_chain() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };

    let start = Instant::now();
    let result = producer
        .pipe(sleep_secs(30))
        .timeout(Duration::from_millis(300))
        .output_string()
        .await
        .expect("a timed-out pipeline still reports a result");
    assert!(result.timed_out(), "result: {result:?}");
    assert!(!result.is_success());
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "pipeline did not honor its timeout (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline fed from a string stdin"]
async fn pipeline_honors_first_stage_stdin() {
    let result = sort_stage()
        .stdin(processkit::Stdin::from_string("delta\nalpha\n"))
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    assert!(
        result.stdout().contains("alpha") && result.stdout().contains("delta"),
        "stdin should flow through both stages: {:?}",
        result.stdout()
    );
}
