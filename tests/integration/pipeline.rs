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
    // A SILENT producer that exits 0: it writes nothing, so it can never die
    // of SIGPIPE when the fast-failing middle stage closes the pipe first —
    // a real race seen on CI (a writing producer is sometimes the first
    // unclean stage, by signal, stealing the attribution this test pins).
    // The middle stage fails with a distinctive code; the final stage
    // succeeds reading EOF.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
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
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
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
#[ignore = "spawns a real producer|head pipeline killed by the closing pipe"]
async fn unchecked_producer_forgives_the_head_pattern() {
    // The motivating case for `unchecked_in_pipe()`: the consumer takes one line and
    // exits, the endless producer dies of the closed pipe — that death must
    // not fail the chain. (The per-stage timeout is a safety net; a healthy
    // run never reaches it, and `unchecked` forgives that kill too.)
    let result = endless_yes()
        .unchecked_in_pipe()
        .timeout(Duration::from_secs(10))
        .pipe(first_line_consumer())
        .output_string()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    assert!(
        result.stdout().contains('y'),
        "the consumed line is the chain's output: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real producer|head pipeline killed by the closing pipe"]
async fn checked_producer_reports_the_head_pattern_as_failure() {
    // The contrast `unchecked_in_pipe()` exists to fix: strict pipefail blames the
    // producer's perfectly normal pipe-closed death.
    let result = endless_yes()
        .timeout(Duration::from_secs(10))
        .pipe(first_line_consumer())
        .output_string()
        .await
        .expect("pipeline completes with a result");
    assert!(
        !result.is_success(),
        "strict pipefail must report the producer's death: {result:?}"
    );
    assert_ne!(result.code(), Some(0));
}

#[tokio::test]
#[ignore = "spawns a real pipeline with a failing consumer"]
async fn unchecked_producer_does_not_mask_a_failing_consumer() {
    let failing_consumer = if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            "$null = [Console]::In.ReadLine(); exit 7",
        ])
    } else {
        Command::new("sh").args(["-c", "head -n 1 >/dev/null; exit 7"])
    };

    let result = endless_yes()
        .unchecked_in_pipe()
        .timeout(Duration::from_secs(10))
        .pipe(failing_consumer)
        .output_string()
        .await
        .expect("pipeline completes with a result");
    assert_eq!(
        result.code(),
        Some(7),
        "the CHECKED consumer's failure must still be reported: {result:?}"
    );
    assert!(!result.is_success());
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
