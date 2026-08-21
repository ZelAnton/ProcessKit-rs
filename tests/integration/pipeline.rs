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

/// A stage that copies stdin to stdout without reordering it.
fn passthrough_stage() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "more"])
    } else {
        Command::new("cat")
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
    assert!(
        result.duration() > Duration::ZERO,
        "T-039: a successful chain must report the measured wall-clock duration, not ZERO: {result:?}"
    );
    let stdout = result.stdout();
    let alpha = stdout.find("alpha").expect("alpha in output");
    let delta = stdout.find("delta").expect("delta in output");
    assert!(alpha < delta, "sort should reorder: {stdout:?}");
}

#[cfg(feature = "pty")]
#[tokio::test]
#[ignore = "spawns a real pipeline whose final stage owns a PTY"]
async fn final_pty_stage_captures_the_upstream_stream() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "echo final-pty-payload"])
    } else {
        Command::new("sh").args(["-c", "printf 'final-pty-payload\\n'"])
    };

    let result = producer
        .pipe(passthrough_stage().use_pty())
        .output_string()
        .await
        .expect("a final PTY stage is a supported capture surface");
    assert!(result.is_success(), "pipeline result: {result:?}");
    assert!(
        result.stdout().contains("final-pty-payload"),
        "the final PTY must receive and capture upstream data: {result:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline with a shared stdout/stderr writer"]
async fn pipeline_can_merge_stage_stderr_into_downstream_stdin_in_write_order() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args([
            "/d",
            "/c",
            "echo stdout-1& 1>&2 echo stderr-1& echo stdout-2& 1>&2 echo stderr-2",
        ])
    } else {
        Command::new("sh").args([
            "-c",
            "printf 'stdout-1\\n'; printf 'stderr-1\\n' >&2; printf 'stdout-2\\n'; printf 'stderr-2\\n' >&2",
        ])
    };

    let result = producer
        .merge_stderr_in_pipe()
        .pipe(passthrough_stage())
        .output_string()
        .await
        .expect("run merged pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");

    let stdout = result.stdout();
    let stdout_1 = stdout.find("stdout-1").expect("first stdout line");
    let stderr_1 = stdout.find("stderr-1").expect("first stderr line");
    let stdout_2 = stdout.find("stdout-2").expect("second stdout line");
    let stderr_2 = stdout.find("stderr-2").expect("second stderr line");
    assert!(
        stdout_1 < stderr_1 && stderr_1 < stdout_2 && stdout_2 < stderr_2,
        "one shared pipe must preserve the child's write order: {stdout:?}"
    );
    assert_eq!(result.stderr(), "", "the last stage emitted no stderr");
}

#[tokio::test]
#[ignore = "spawns a real pipeline whose failing stage merges its diagnostic"]
async fn merged_stage_has_no_separate_pipefail_stderr() {
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "1>&2 echo merged-diagnostic& exit /b 3"])
    } else {
        Command::new("sh").args(["-c", "printf 'merged-diagnostic\\n' >&2; exit 3"])
    };

    let result = failing
        .merge_stderr_in_pipe()
        .pipe(passthrough_stage())
        .output_string()
        .await
        .expect("pipeline failures are captured");
    assert_eq!(result.code(), Some(3), "pipefail result: {result:?}");
    assert!(
        result.stdout().contains("merged-diagnostic"),
        "merged diagnostic must travel through the downstream stage: {result:?}"
    );
    assert_eq!(
        result.stderr(),
        "",
        "the attributed stage no longer owns a separate stderr stream"
    );
}

#[tokio::test]
#[ignore = "spawns real commands to prove final-stage and standalone no-op semantics"]
async fn stderr_merge_marker_is_a_noop_outside_a_non_final_pipeline_stage() {
    let standalone = if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "echo solo-out& 1>&2 echo solo-err"])
    } else {
        Command::new("sh").args(["-c", "printf 'solo-out\\n'; printf 'solo-err\\n' >&2"])
    }
    .merge_stderr_in_pipe()
    .output_string()
    .await
    .expect("run standalone command");
    assert!(standalone.stdout().contains("solo-out"));
    assert!(!standalone.stdout().contains("solo-err"));
    assert!(standalone.stderr().contains("solo-err"));

    let quiet = if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "exit /b 0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };
    let final_stage = if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "echo final-out& 1>&2 echo final-err"])
    } else {
        Command::new("sh").args(["-c", "printf 'final-out\\n'; printf 'final-err\\n' >&2"])
    };
    let result = quiet
        .pipe(final_stage.merge_stderr_in_pipe())
        .output_string()
        .await
        .expect("run pipeline with a marked final stage");
    assert!(result.stdout().contains("final-out"));
    assert!(!result.stdout().contains("final-err"));
    assert!(result.stderr().contains("final-err"));
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
    assert!(
        result.duration() > Duration::ZERO,
        "T-039: a failing chain must also report the measured wall-clock duration: {result:?}"
    );

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
        matches!(err.reason(), processkit::ErrorReason::Exit { code: 3, .. }),
        "expected Exit with code 3, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a pipeline with a quiet upstream and a failing middle stage"]
async fn pipeline_failure_tears_down_a_quiet_upstream_immediately() {
    // A quiet upstream that writes nothing and would otherwise stay alive for ~30s,
    // feeding a middle stage that fails at once. A purely passive teardown would
    // wait on the silent producer — it never writes, so it never dies of a broken
    // pipe — holding the failed chain open. Proactive teardown kills the group the
    // moment the middle stage fails, so the error surfaces without the long wait,
    // and pipefail still blames the genuine failure (exit 3), not the killed
    // producer (a torn-down victim).
    let quiet_upstream = if cfg!(windows) {
        Command::new("powershell").args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"])
    } else {
        Command::new("sleep").arg("30")
    };

    let start = Instant::now();
    let result = quiet_upstream
        .pipe(failing_exit(3))
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("pipeline completes with a result");
    assert_eq!(
        result.code(),
        Some(3),
        "the downstream failure is attributed, not the killed upstream: {result:?}"
    );
    assert!(!result.is_success());
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "a quiet upstream must not hold the failed chain open (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a pipeline with a quiet upstream and a per-stage-cancelled downstream"]
async fn pipeline_failure_tears_down_a_quiet_upstream_on_a_raw_stage_error_too() {
    // T-085: distinct from `pipeline_failure_tears_down_a_quiet_upstream_immediately`
    // above — that test's failure is a *checked* `Outcome` (a plain non-zero
    // exit), which already fired proactive teardown before this fix landed.
    // This one's failure is a *raw* `Err` (`ErrorReason::Cancelled`, via a per-stage
    // `Command::cancel_on` on just the LAST stage — deliberately not the
    // whole-chain `Pipeline::cancel_on`, so the quiet upstream carries no
    // token of its own) surfacing straight out of a stage's task, past the
    // checked-failure attribution logic entirely — before the fix, that path
    // never touched `teardown`, so a quiet upstream (which never writes, and
    // so never dies of a broken pipe) held the chain open until its own
    // unrelated ~30s deadline elapsed instead.
    use tokio_util::sync::CancellationToken;

    let quiet_upstream = sleeper();
    let token = CancellationToken::new();
    let cancels_soon = sleep_secs(30).cancel_on(token.clone());
    let fired = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        fired.cancel();
    });

    let start = Instant::now();
    let err = quiet_upstream
        .pipe(cancels_soon)
        .output_string()
        .await
        .expect_err("a per-stage-cancelled last stage must surface as Err");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "a quiet upstream must not hold a raw-Err chain open (took {:?})",
        start.elapsed()
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
#[ignore = "spawns a real pipeline with a per-stage timeout on a middle stage"]
async fn per_stage_timeout_ends_a_hanging_middle_stage() {
    // F: a per-stage `Command::timeout` — distinct from the chain-wide
    // `Pipeline::timeout` covered below — bounds a single stage. The middle
    // stage hangs well past its own short deadline while the producer and the
    // last stage are near-instant; the stage's own timeout must kill just that
    // subtree and let the chain fold a `TimedOut` result promptly, without a
    // chain-wide `Pipeline::timeout` in play at all.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };
    let slow_stage = sleep_secs(30).timeout(Duration::from_millis(300));

    let start = Instant::now();
    let result = producer
        .pipe(slow_stage)
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("a per-stage-timed-out pipeline still reports a result");
    assert!(result.timed_out(), "result: {result:?}");
    assert!(!result.is_success());
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "the per-stage timeout did not end the chain promptly (took {:?})",
        start.elapsed()
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
    assert!(
        result.duration() > Duration::ZERO,
        "T-039: the chain-wide timeout branch must also report the measured wall-clock duration, \
         not ZERO: {result:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline that emits diagnostics before a chain-wide timeout"]
async fn pipeline_timeout_keeps_output_read_before_the_deadline() {
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo input"])
    } else {
        Command::new("sh").args(["-c", "printf 'input\\n'"])
    };
    let partial_then_idle = if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            "[Console]::Out.WriteLine('partial-stdout'); [Console]::Error.WriteLine('partial-stderr'); Start-Sleep -Seconds 30",
        ])
    } else {
        Command::new("sh").args([
            "-c",
            "printf 'partial-stdout\\n'; printf 'partial-stderr\\n' >&2; sleep 30",
        ])
    };

    // The deadline has to outlast the second stage's *interpreter start-up*, not
    // just its write: what is asserted below is that output the pumps had already
    // read is salvaged, and a deadline that fires before the child writes at all
    // leaves nothing to salvage (an empty result with `total_bytes: 0`, not a
    // duplication or loss bug). A cold Windows PowerShell start on a loaded host
    // routinely eats more than two seconds, so give that platform room; every
    // budget here stays far below the stage's own 30s idle sleep, so the chain
    // still times out on the deadline rather than on the child exiting.
    let chain_timeout = if cfg!(windows) {
        Duration::from_secs(8)
    } else {
        Duration::from_secs(2)
    };

    let result = producer
        .pipe(partial_then_idle)
        .timeout(chain_timeout)
        .output_string()
        .await
        .expect("a timed-out pipeline still reports a result");

    assert!(result.timed_out(), "result: {result:?}");
    assert_eq!(result.stdout(), "partial-stdout", "result: {result:?}");
    assert_eq!(result.stderr(), "partial-stderr", "result: {result:?}");
    assert_eq!(result.configured_timeout(), Some(chain_timeout));
    assert!(
        result.duration() > Duration::ZERO,
        "chain timeout must retain its measured duration: {result:?}"
    );
}

/// Whether a process with `pid` is still alive (Unix `kill(pid, 0)` probe:
/// succeeds while it lives or is an unreaped zombie, fails `ESRCH` once gone).
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    // SAFETY: signal 0 runs the existence/permission check without delivering a signal.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

/// A forking pipeline stage whose **grandchild** (a backgrounded `sleep`)
/// inherits and holds the stdout pipe open while the foreground shell also
/// sleeps — neither writes. It records the grandchild's PID to `pidfile`, then
/// carries an `unchecked_in_pipe` per-stage timeout. `unchecked` is deliberate:
/// the stage's own timeout death is forgiven AND it never triggers the chain's
/// proactive teardown, so the per-stage deadline is the *only* thing that can end
/// the stage. Before T-016 that deadline reached only the shell (the direct
/// child), leaving the grandchild holding stdout; a per-stage sub-group now tears
/// the whole subtree down.
#[cfg(unix)]
fn forking_stage(pidfile: &std::path::Path) -> Command {
    Command::new("sh")
        .args([
            "-c",
            &format!(
                "sleep 30 & printf %s \"$!\" > '{}'; sleep 30",
                pidfile.display()
            ),
        ])
        .unchecked_in_pipe()
        .timeout(Duration::from_millis(500))
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real forking pipeline stage and bounds it with a per-stage timeout"]
async fn per_stage_timeout_on_a_forking_stage_frees_downstream() {
    // The last stage (`cat`, consumed to EOF) can only finish once every writer of
    // its stdin pipe is gone. The producer's foreground shell AND its backgrounded
    // grandchild both hold that pipe and neither writes, so a per-stage deadline
    // that reached only the shell would leave the grandchild holding stdout and
    // `cat` would block until the grandchild's own 30s `sleep` elapsed. There is
    // deliberately NO `Pipeline::timeout` backstop, and the stage is
    // `unchecked_in_pipe` so no proactive teardown fires either: a prompt finish is
    // proof the per-stage deadline alone reaped the grandchild, freeing downstream.
    let pidfile =
        std::env::temp_dir().join(format!("processkit_t016_free_{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);

    let result = completes_within(
        Duration::from_secs(15),
        "forking pipeline stage bounded by a per-stage timeout",
        forking_stage(&pidfile)
            .pipe(Command::new("cat"))
            .output_string(),
    )
    .await
    .expect("a per-stage-timed-out chain still reports a result");
    let _ = std::fs::remove_file(&pidfile);

    // The inner unchecked stage's timeout is forgiven; the clean last stage speaks.
    assert!(
        result.is_success(),
        "unchecked forking producer's per-stage timeout is forgiven, `cat` ends clean: {result:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real forking pipeline stage and asserts its grandchild is reaped"]
async fn per_stage_timeout_reaps_a_forking_stages_grandchild() {
    // The direct-proof companion to the promptness test above: after the per-stage
    // deadline fires, the backgrounded grandchild that held the stdout pipe must be
    // *gone*, not merely detached. Before T-016 the shared-group per-stage kill
    // reached only the shell, so the grandchild survived; a per-stage sub-group
    // tears the whole subtree down.
    let pidfile =
        std::env::temp_dir().join(format!("processkit_t016_reap_{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);

    let _ = completes_within(
        Duration::from_secs(15),
        "forking pipeline stage bounded by a per-stage timeout",
        forking_stage(&pidfile)
            .pipe(Command::new("cat"))
            .output_string(),
    )
    .await
    .expect("a per-stage-timed-out chain still reports a result");

    // The producer wrote its grandchild's PID before its own deadline elapsed.
    let pid = std::fs::read_to_string(&pidfile)
        .ok()
        .and_then(|t| t.trim().parse::<u32>().ok())
        .expect("forking stage recorded its grandchild's PID");

    // The grandchild was killed with the stage subtree; allow a brief window for
    // the reparent-to-init reap to clear the pid.
    let mut reaped = false;
    for _ in 0..80 {
        if !pid_alive(pid) {
            reaped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = std::fs::remove_file(&pidfile);
    assert!(
        reaped,
        "grandchild {pid} of the forking stage outlived the per-stage deadline — the subtree kill leaked"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline and captures raw bytes"]
async fn pipeline_output_bytes_captures_the_last_stage_stdout() {
    // S-1: the binary-capture analogue of output_string. A simple echo|sort
    // chain whose last stage's stdout is captured as raw bytes.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo beta& echo alpha"])
    } else {
        Command::new("sh").args(["-c", "printf 'beta\\nalpha\\n'"])
    };
    let result = producer
        .pipe(sort_stage())
        .output_bytes()
        .await
        .expect("run pipeline");
    assert!(result.is_success(), "pipeline result: {result:?}");
    let bytes = result.stdout();
    let text = String::from_utf8_lossy(bytes);
    assert!(
        text.contains("alpha") && text.contains("beta"),
        "raw bytes carry both lines: {text:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline with a failing inner stage, captured as bytes"]
async fn pipeline_output_bytes_uses_pipefail_attribution() {
    // S-1: output_bytes shares the pipefail fold with output_string — a failing
    // inner stage's code is attributed even though stdout is captured as bytes.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "5"])
    } else {
        Command::new("sh").args(["-c", "exit 5"])
    };
    let result = producer
        .pipe(failing)
        .pipe(sort_stage())
        .output_bytes()
        .await
        .expect("pipeline completes with a result");
    assert_eq!(
        result.code(),
        Some(5),
        "pipefail code on the bytes path: {result:?}"
    );
    assert!(!result.is_success());
}

#[tokio::test]
#[ignore = "spawns real pipelines exercising the parity verbs"]
async fn pipeline_run_verbs_mirror_the_command_vocabulary() {
    // S-1: run_unit / exit_code / checked on a clean two-stage chain.
    let clean = || {
        let producer = if cfg!(windows) {
            Command::new("cmd").args(["/c", "echo hi"])
        } else {
            Command::new("sh").args(["-c", "printf 'hi\\n'"])
        };
        producer.pipe(sort_stage())
    };
    clean().run_unit().await.expect("run_unit on a clean chain");
    assert_eq!(clean().exit_code().await.expect("exit_code"), 0);
    let checked = clean().checked().await.expect("checked");
    assert!(checked.stdout().contains("hi"), "checked: {checked:?}");

    // exit_code surfaces a failing inner stage's attributed code.
    let code = failing_exit(0)
        .pipe(failing_exit(4))
        .pipe(sort_stage())
        .exit_code()
        .await
        .expect("exit_code reports a result");
    assert_eq!(code, 4, "pipefail-attributed exit code");
}

#[tokio::test]
#[ignore = "spawns a real grep -q pipeline for probe"]
async fn pipeline_probe_reads_the_chain_exit_as_a_bool() {
    // S-1: a `producer | grep -q pattern` chain — exit 0 (match) → true,
    // exit 1 (no match) → false.
    let grep_q = |pattern: &str| {
        if cfg!(windows) {
            // findstr has no quiet flag, but pipefail reads its exit code (0 hit
            // / 1 miss) the same way; `/c:<pattern>` must be a single token.
            Command::new("findstr").arg(format!("/c:{pattern}"))
        } else {
            Command::new("grep").args(["-q", pattern])
        }
    };
    let producer = || {
        if cfg!(windows) {
            Command::new("cmd").args(["/c", "echo hello world"])
        } else {
            Command::new("sh").args(["-c", "printf 'hello world\\n'"])
        }
    };
    assert!(
        producer()
            .pipe(grep_q("hello"))
            .probe()
            .await
            .expect("probe match"),
        "grep -q finds the pattern → true"
    );
    assert!(
        !producer()
            .pipe(grep_q("absent"))
            .probe()
            .await
            .expect("probe miss"),
        "grep -q misses → false (exit 1)"
    );
}

#[tokio::test]
#[ignore = "spawns a real pipeline and parses its output"]
async fn pipeline_parse_turns_chain_stdout_into_a_value() {
    // S-1: parse the line count of a sorted producer.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo b& echo a& echo a"])
    } else {
        Command::new("sh").args(["-c", "printf 'b\\na\\na\\n'"])
    };
    let dedup = if cfg!(windows) {
        // `sort` on Windows has no -u; pipe through to keep it simple: count lines.
        Command::new("findstr").arg("a")
    } else {
        Command::new("grep").arg("a")
    };
    let n: usize = producer
        .pipe(dedup)
        .parse(|s| s.lines().count())
        .await
        .expect("parse the count");
    assert_eq!(n, 2, "two 'a' lines");
}

#[tokio::test]
#[ignore = "spawns a pipeline whose last stage truncates its capture"]
async fn pipeline_parse_fails_loud_on_a_truncated_last_stage() {
    // S-1/B12: parse must reject a clipped tail rather than hand the closure a
    // partial capture. The last stage's bounded buffer drops lines; the folded
    // result must carry `truncated()` so parse errors with OutputTooLarge.
    use processkit::OutputBufferPolicy;
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo a& echo b& echo c& echo d"])
    } else {
        Command::new("sh").args(["-c", "printf 'a\\nb\\nc\\nd\\n'"])
    };
    let err = producer
        .pipe(sort_stage().output_buffer(OutputBufferPolicy::bounded(2)))
        .parse(|s| s.to_owned())
        .await
        .expect_err("a truncated last stage must fail loud");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::OutputTooLarge { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a pipeline whose last stage truncates its capture"]
async fn pipeline_run_fails_loud_on_a_truncated_last_stage() {
    // R5-2/B12: `run` presents stdout as if complete, so a clipped last-stage
    // capture must fail loud (OutputTooLarge), not return a partial tail — the
    // same guard `parse`/`try_parse` and the single-command verbs apply.
    use processkit::OutputBufferPolicy;
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo a& echo b& echo c& echo d"])
    } else {
        Command::new("sh").args(["-c", "printf 'a\\nb\\nc\\nd\\n'"])
    };
    let err = producer
        .pipe(sort_stage().output_buffer(OutputBufferPolicy::bounded(2)))
        .run()
        .await
        .expect_err("a truncated last stage must fail loud on run()");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::OutputTooLarge { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real long-running pipeline and cancels it"]
async fn pipeline_cancel_on_tears_the_whole_chain_down() {
    // S-1: a token fired mid-run cancels every stage; the run resolves to
    // ErrorReason::Cancelled rather than hanging on the endless producer.
    use tokio_util::sync::CancellationToken;
    let token = CancellationToken::new();
    let chain = endless_yes()
        .unchecked_in_pipe()
        .pipe(sleep_secs(30))
        .cancel_on(token.clone());
    let fired = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        fired.cancel();
    });
    let start = Instant::now();
    let err = chain
        .output_string()
        .await
        .expect_err("a cancelled chain errors");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "cancellation must be prompt, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real long-running pipeline whose stages have distinct cancellation tokens"]
async fn pipeline_cancel_on_remains_chain_wide_with_distinct_stage_tokens() {
    use tokio_util::sync::CancellationToken;

    let producer_token = CancellationToken::new();
    let consumer_token = CancellationToken::new();
    let chain_token = CancellationToken::new();
    let chain = endless_yes()
        .cancel_on(producer_token.clone())
        .unchecked_in_pipe()
        .pipe(sleep_secs(30).cancel_on(consumer_token.clone()))
        .cancel_on(chain_token.clone());

    let run = tokio::spawn(async move { chain.output_string().await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    chain_token.cancel();

    let err = completes_within(Duration::from_secs(15), "the chain-wide cancellation", run)
        .await
        .expect("capture task")
        .expect_err("the pipeline token must cancel stages that own distinct tokens");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        !producer_token.is_cancelled() && !consumer_token.is_cancelled(),
        "chain cancellation must not fire either stage-owned token"
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

// ---------------------------------------------------------------------------
// T-159: `Pipeline::start()` — the live streaming session (`PipelineSession`).
// ---------------------------------------------------------------------------

/// Whether a stage's process is still alive, per platform: `kill(pid, 0)` on
/// Unix, `OpenProcess` on Windows. Used by the no-orphan proofs below.
fn stage_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        pid_alive(pid)
    }
    #[cfg(windows)]
    {
        windows_pid_alive(pid)
    }
}

/// Poll until `pid` is no longer alive, allowing a brief window for the OS to
/// clear it after a kill (Windows can keep a just-exited pid openable for a short
/// timer past our own handle release — see K-029). Panics if it never clears.
async fn assert_pid_reaped(pid: u32, what: &str) {
    for _ in 0..100 {
        if !stage_pid_alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{what}: pid {pid} outlived the chain teardown — the kill leaked");
}

/// An idle producer that records its **own** PID to `pidfile`, writes nothing to
/// stdout, then idles ~30s — a quiet first stage whose liveness a no-orphan proof
/// can probe by the recorded pid.
fn pid_recording_idle(pidfile: &std::path::Path) -> Command {
    let path = pidfile.display();
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!("$PID | Set-Content -Encoding ascii -Path '{path}'; Start-Sleep -Seconds 30"),
        ])
    } else {
        Command::new("sh").args(["-c", &format!("printf %s \"$$\" > '{path}'; sleep 30")])
    }
}

/// Read the PID a [`pid_recording_idle`] stage wrote, polling briefly for the file
/// to appear (the stage records it right after spawn). Returns `None` if teardown
/// kills the stage before it is scheduled.
async fn read_recorded_pid(pidfile: &std::path::Path) -> Option<u32> {
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(pidfile)
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            return Some(pid);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[tokio::test]
#[ignore = "spawns a real two-stage pipeline and streams the last stage's stdout"]
async fn pipeline_start_streams_last_stage_lines() {
    use processkit::{Finished, Outcome};
    use tokio_stream::StreamExt;

    // A finite chain: the producer's two lines flow through a passthrough stage,
    // and `stdout_lines` on the session yields the *last* stage's stdout live.
    let mut session = two_line_echo()
        .pipe(sort_stage())
        .start()
        .await
        .expect("start the live chain");

    let mut lines = session
        .stdout_lines()
        .expect("stream the last stage's stdout");
    let mut collected = Vec::new();
    while let Some(line) = completes_within(
        Duration::from_secs(15),
        "streaming a line from the live chain",
        lines.next(),
    )
    .await
    {
        collected.push(line);
    }
    drop(lines);

    // A second take of the same stream is a loud error, exactly like `RunningProcess`.
    let reused = session
        .stdout_lines()
        .expect_err("a second stdout_lines must be a loud error");
    assert!(
        matches!(reused.reason(), processkit::ErrorReason::Io(_)),
        "expected ErrorReason::Io, got {reused:?}"
    );

    assert!(
        collected.iter().any(|l| l.contains("first")),
        "streamed lines: {collected:?}"
    );
    assert!(
        collected.iter().any(|l| l.contains("second")),
        "streamed lines: {collected:?}"
    );

    let Finished { outcome, .. } = session.finish().await.expect("finish the chain");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "a clean chain folds to Exited(0)"
    );
}

// Unix-only: a live-chain readiness banner must flush *promptly* out of the last
// stage, which needs a passthrough (`cat`). Windows `findstr`/`more` block-buffer a
// piped stdout, so the single "ready" line would sit unflushed — the same platform
// buffering the forking-stage tests dodge by staying Unix-only. The `wait_for_line`
// delegation itself is platform-agnostic and is covered cross-platform by the
// single-process readiness suite.
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real live chain and waits for a readiness banner on its last stage"]
async fn pipeline_start_wait_for_line_on_a_live_chain() {
    // The producer prints `ready` after ~0.5s then idles ~30s; a passthrough
    // stage passes that line through as the *last* stage's stdout, so
    // `wait_for_line` on the session sees it without tearing the chain down.
    let filter = Command::new("cat");

    let mut session = banner_then_idle()
        .pipe(filter)
        .start()
        .await
        .expect("start the live chain");

    let line = completes_within(
        Duration::from_secs(20),
        "wait_for_line on a live chain",
        session.wait_for_line(|l| l.contains("ready"), Duration::from_secs(15)),
    )
    .await
    .expect("the banner matched before the deadline");
    assert!(line.contains("ready"), "matched line: {line:?}");

    // The probe left the chain alive; the last stage still has a pid.
    assert!(
        session.pid().is_some(),
        "wait_for_line must not kill the chain"
    );

    // Tear it down and confirm the streamed session finishes promptly.
    session.start_kill().expect("stop the whole chain");
    let _ = completes_within(
        Duration::from_secs(15),
        "finish after killing the live chain",
        session.finish(),
    )
    .await;
}

#[tokio::test]
#[ignore = "spawns a real live chain whose non-last stage fails; finish must attribute it"]
async fn pipeline_start_finish_attributes_a_failing_inner_stage() {
    use processkit::{Finished, Outcome};
    use tokio_stream::StreamExt;

    // A silent producer that exits 0, a middle stage that writes to stderr and
    // exits 3, and a clean last stage. Pipefail must blame the middle stage —
    // *not* the last — and surface that stage's own stderr, exactly as the
    // buffering verbs do, even though the last stage's stdout was streamed.
    let producer = failing_exit(0);
    let failing = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo boom 1>&2 & exit 3"])
    } else {
        Command::new("sh").args(["-c", "echo boom 1>&2; exit 3"])
    };

    let mut session = producer
        .pipe(failing)
        .pipe(sort_stage())
        .start()
        .await
        .expect("start the live chain");

    // Drain the last stage's (empty) stdout to EOF.
    let mut lines = session.stdout_lines().expect("stream the last stage");
    while completes_within(
        Duration::from_secs(15),
        "draining the last stage after an inner failure",
        lines.next(),
    )
    .await
    .is_some()
    {}
    drop(lines);

    let Finished {
        outcome, stderr, ..
    } = completes_within(
        Duration::from_secs(15),
        "finishing a chain with a failing inner stage",
        session.finish(),
    )
    .await
    .expect("finish folds a result");
    assert_eq!(
        outcome,
        Outcome::Exited(3),
        "pipefail blames the failing INNER stage, not the last: {outcome:?}"
    );
    assert!(
        stderr.contains("boom"),
        "the culprit inner stage's own stderr is surfaced: {stderr:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real live chain and stops it with start_kill; no stage may survive"]
async fn pipeline_start_kill_reaps_the_whole_chain() {
    let pidfile =
        std::env::temp_dir().join(format!("processkit_t159_kill_{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);

    // First (inner) stage records its pid and idles writing nothing; the last
    // stage passes stdin through and idles too — a quiet, long-lived live chain.
    let passthrough = if cfg!(windows) {
        Command::new("cmd").args(["/c", "more"])
    } else {
        Command::new("cat")
    };
    let mut session = pid_recording_idle(&pidfile)
        .pipe(passthrough)
        .start()
        .await
        .expect("start the live chain");

    let inner_pid = read_recorded_pid(&pidfile)
        .await
        .expect("the idle producer should record its PID");
    let last_pid = session.pid().expect("the last stage has a live pid");
    assert!(
        stage_pid_alive(inner_pid),
        "the inner stage should be alive"
    );

    session.start_kill().expect("stop the whole chain");
    // `finish` folds the killed outcome AND consumes the session — releasing the
    // last stage's child handle, which on Windows would otherwise keep its pid
    // reporting "alive" no matter that the process is gone (K-029). Only then can
    // the last stage's pid be probed honestly.
    let _ = completes_within(
        Duration::from_secs(15),
        "finish after start_kill",
        session.finish(),
    )
    .await;
    assert_pid_reaped(inner_pid, "inner stage after start_kill").await;
    assert_pid_reaped(last_pid, "last stage after start_kill").await;
    let _ = std::fs::remove_file(&pidfile);
}

#[tokio::test]
#[ignore = "spawns a real live chain and drops it unfinished; kill-on-drop must reap every stage"]
async fn pipeline_session_drop_kills_the_whole_chain() {
    let pidfile =
        std::env::temp_dir().join(format!("processkit_t159_drop_{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);

    let passthrough = if cfg!(windows) {
        Command::new("cmd").args(["/c", "more"])
    } else {
        Command::new("cat")
    };
    let session = pid_recording_idle(&pidfile)
        .pipe(passthrough)
        .start()
        .await
        .expect("start the live chain");

    let inner_pid = read_recorded_pid(&pidfile)
        .await
        .expect("the idle producer should record its PID");
    let last_pid = session.pid().expect("the last stage has a live pid");

    // Drop the session unread — kill-on-drop must tear the whole chain down.
    drop(session);
    assert_pid_reaped(inner_pid, "inner stage after session drop").await;
    assert_pid_reaped(last_pid, "last stage after session drop").await;
    let _ = std::fs::remove_file(&pidfile);
}

// ---------------------------------------------------------------------------
// T-283: a live session's LAST stage failing is a whole-chain teardown too, and
// does not wait for the caller to call `finish`.
//
// Every other stage of a `Pipeline::start` chain drains in a background task
// that fires the chain's proactive teardown on that stage's checked failure. The
// last stage has none — the caller streams it, `finish` consumes it — so its
// failure used to be classified only once `finish` ran. A caller holding the
// session unfinished (here: never even taking the stream) was therefore left with
// the whole upstream of an already-dead chain still running.
// ---------------------------------------------------------------------------

/// Long enough for a fired whole-chain teardown to have reached every stage: the
/// killer's own drain grace plus room for the kill to land on a loaded host. Used
/// to give a teardown that must *not* have happened every chance to show itself.
const TEARDOWN_SETTLE: Duration = Duration::from_secs(3);

/// A producer that records its **own** PID to `pidfile`, emits exactly **one**
/// line, then idles ~30s writing nothing more.
///
/// The single line is what lets a downstream stage key its own exit on this
/// producer having actually run, so a teardown proof does not race a freshly
/// spawned child's scheduling (see [`assert_pid_reaped`]'s sibling caveat, K-048).
/// Going quiet immediately afterwards is the leak scenario itself: a producer that
/// never writes again never learns from a broken pipe that its consumer is gone.
fn pid_recording_one_line_then_idle(pidfile: &std::path::Path) -> Command {
    let path = pidfile.display();
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!(
                "$PID | Set-Content -Encoding ascii -Path '{path}'; [Console]::Out.WriteLine('go'); Start-Sleep -Seconds 30"
            ),
        ])
    } else {
        Command::new("sh").args([
            "-c",
            &format!("printf %s \"$$\" > '{path}'; printf 'go\\n'; sleep 30"),
        ])
    }
}

/// A last stage that consumes one line of stdin and then exits with `code`,
/// writing nothing itself — so its exit is causally *after* its producer's first
/// write, not merely soon after the chain started.
fn one_line_then_exit(code: i32) -> Command {
    if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            &format!("[Console]::In.ReadLine() | Out-Null; exit {code}"),
        ])
    } else {
        Command::new("sh").args(["-c", &format!("head -n 1 >/dev/null; exit {code}")])
    }
}

#[tokio::test]
#[ignore = "spawns a real live chain whose last stage fails while the session is held unread"]
async fn pipeline_session_last_stage_failure_tears_down_a_quiet_upstream() {
    let pidfile = std::env::temp_dir().join(format!(
        "processkit_t283_last_fail_{}.pid",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&pidfile);

    // A producer that records its pid, writes one line, then idles ~30s writing
    // nothing, feeding a last stage that exits non-zero as soon as it reads that
    // line. The producer's pid is therefore on disk before the failure can happen.
    let mut session = pid_recording_one_line_then_idle(&pidfile)
        .pipe(one_line_then_exit(7))
        .start()
        .await
        .expect("start the live chain");

    let inner_pid = read_recorded_pid(&pidfile)
        .await
        .expect("the producer records its PID before its consumer can read a line");

    // Deliberately no `stdout_lines`/`events`/`wait_for_line`, no `start_kill`, no
    // `finish` — nothing but holding the session. The chain must still tear itself
    // down off the last stage's own failure.
    assert_pid_reaped(inner_pid, "quiet upstream after the last stage failed").await;

    // The session is still alive and usable at this point — that is the whole
    // point: the teardown happened without the caller doing anything. Taking the
    // stream is still a first take (the consume-once contract is untouched by the
    // watcher), and a second take is still the loud error it always was.
    let lines = session
        .stdout_lines()
        .expect("the stream is still the caller's to take");
    drop(lines);
    let reused = session
        .stdout_lines()
        .expect_err("a second stdout_lines must still be a loud error");
    assert!(
        matches!(reused.reason(), processkit::ErrorReason::Io(_)),
        "expected ErrorReason::Io, got {reused:?}"
    );

    // And the attribution is the one `finish` would have reached on its own: the
    // last stage is the culprit, not the upstream its own teardown killed.
    let processkit::Finished { outcome, .. } = completes_within(
        Duration::from_secs(15),
        "finishing after a proactive last-stage teardown",
        session.finish(),
    )
    .await
    .expect("finish folds a result");
    assert_eq!(
        outcome,
        processkit::Outcome::Exited(7),
        "pipefail blames the failing LAST stage, not the upstream the teardown killed: {outcome:?}"
    );

    let _ = std::fs::remove_file(&pidfile);
}

#[tokio::test]
#[ignore = "spawns a real live chain whose last stage fails, then takes its event stream"]
async fn pipeline_session_events_taken_after_a_proactive_teardown_still_end_in_exited() {
    use processkit::{Outcome, ProcessEvent};
    use tokio_stream::StreamExt;

    // The watcher reaps the last stage where it finds it exited, so a caller that
    // takes the lifecycle stream *afterwards* is arming it over an already-reaped
    // child. The terminal `Exited` is published by the reap choke point every
    // consuming finisher funnels through — including the short-circuit `finish`
    // takes once a child is already reaped — so the stream must still end in one.
    let pidfile =
        std::env::temp_dir().join(format!("processkit_t283_events_{}.pid", std::process::id()));
    let _ = std::fs::remove_file(&pidfile);

    let mut session = pid_recording_one_line_then_idle(&pidfile)
        .pipe(one_line_then_exit(7))
        .start()
        .await
        .expect("start the live chain");

    for _ in 0..300 {
        if session.pid().is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut events = session
        .events()
        .expect("the lifecycle stream is still the caller's to take");
    let render = async {
        let mut seen = Vec::new();
        while let Some(event) = events.next().await {
            seen.push(event);
        }
        seen
    };
    let (seen, finished) = completes_within(
        Duration::from_secs(20),
        "draining a lifecycle stream armed after the last stage was reaped",
        async { tokio::join!(render, session.finish()) },
    )
    .await;

    assert!(
        matches!(seen.last(), Some(ProcessEvent::Exited(Outcome::Exited(7)))),
        "the stream must still end in the run's real Exited event: {seen:?}"
    );
    let processkit::Finished { outcome, .. } = finished.expect("finish folds a result");
    assert_eq!(
        outcome,
        Outcome::Exited(7),
        "and finish reports the same failing last stage: {outcome:?}"
    );
    let _ = std::fs::remove_file(&pidfile);
}

#[tokio::test]
#[ignore = "spawns a real live chain whose last stage exits cleanly while a producer still runs"]
async fn pipeline_session_clean_last_stage_does_not_tear_the_chain_down() {
    // The complement of the test above, and the reason the watcher classifies
    // instead of just noticing an exit: a last stage that exits *cleanly* is not a
    // failed chain, so it must not kill the rest of it. Otherwise the killed
    // upstream would enter the pipefail fold as a checked failure and the chain
    // would report a failure that never happened.
    let pidfile = std::env::temp_dir().join(format!(
        "processkit_t283_last_clean_{}.pid",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&pidfile);

    let session = pid_recording_one_line_then_idle(&pidfile)
        .pipe(one_line_then_exit(0))
        .start()
        .await
        .expect("start the live chain");

    let inner_pid = read_recorded_pid(&pidfile)
        .await
        .expect("the producer records its PID before its consumer can read a line");

    // Wait for the clean exit to have been *observed* (the watcher's reap clears the
    // session's pid), then well past the teardown drain grace: had that clean exit
    // fired teardown, the producer would be gone by the time we look.
    for _ in 0..300 {
        if session.pid().is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(TEARDOWN_SETTLE).await;
    assert!(
        stage_pid_alive(inner_pid),
        "a clean last stage must not tear the chain down"
    );

    // Dropping the session is what finally reaps the producer here; that path has
    // its own proof in `pipeline_session_drop_kills_the_whole_chain`, so this test
    // only has to release it, not re-assert a teardown known to be racy to observe
    // by pid on a loaded Windows host (K-029/K-048).
    drop(session);
    let _ = std::fs::remove_file(&pidfile);
}

#[tokio::test]
#[ignore = "spawns a real live chain that ends on its own, then cancels the chain token"]
async fn pipeline_session_cancel_after_the_chain_ended_does_not_rewrite_the_outcome() {
    use tokio_util::sync::CancellationToken;

    // Every stage's cancel disposition is latched by whoever first observes that
    // stage's exit, so a token fired *afterwards* cannot rewrite a run that had
    // already ended (the rule `Command::cancel_on` documents). An inner stage's
    // background drain does that latching at the moment the stage exits; the last
    // stage had no observer at all until the session's watcher, so a chain that had
    // completely finished still reported `Cancelled` if the caller happened to fire
    // the token before calling `finish`. Both ends of the chain now latch alike.
    // The producer carries a token of its own that never fires, so the chain token
    // gap-fills onto the last stage alone (`Pipeline::cancel_on`'s documented rule).
    // That keeps this test about the *last* stage's disposition instead of also
    // depending on when the producer's own drain happened to reap it.
    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    }
    .cancel_on(CancellationToken::new());

    let token = CancellationToken::new();
    let session = producer
        .pipe(one_line_then_exit(0))
        .cancel_on(token.clone())
        .start()
        .await
        .expect("start the live chain");

    // Fire the token only once the last stage has provably exited *and* been
    // observed — the watcher's reap is exactly what clears the session's pid. A
    // fixed sleep would instead race a slow interpreter start on a loaded host.
    let mut observed = false;
    for _ in 0..300 {
        if session.pid().is_none() {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        observed,
        "the session's watcher should have reaped the exited last stage"
    );
    token.cancel();

    let processkit::Finished { outcome, .. } = completes_within(
        Duration::from_secs(15),
        "finishing a chain that ended before its token fired",
        session.finish(),
    )
    .await
    .expect("a chain that had already ended is not retroactively cancelled");
    assert_eq!(
        outcome,
        processkit::Outcome::Exited(0),
        "the chain's real outcome survives a token fired after it ended: {outcome:?}"
    );
}

/// A producer that fails on its own while a **grandchild** keeps its stderr pipe
/// open: the shell writes one line to each stream and exits non-zero, but the
/// `sleep` it backgrounded inherited stderr and holds that pipe for ~30s, so the
/// stage's stderr drain cannot reach EOF with the shell. The grandchild's *stdout*
/// is redirected to `/dev/null`, so the stdout pipe still closes when the shell
/// exits — which is what fixes the chain's failure order.
#[cfg(unix)]
fn failing_producer_whose_stderr_outlives_it() -> Command {
    Command::new("sh").args([
        "-c",
        "sleep 30 >/dev/null & printf 'boom\\n' >&2; printf 'go\\n'; exit 3",
    ])
}

// Unix-only for the same reason as the forking-stage tests further up: a
// `sh -c '… &'` is the only cheap way to leave a grandchild holding a stage's
// stderr pipe open past its own death. The attribution rule it pins is
// platform-agnostic, and its fold half is covered hermetically by
// `pipeline::tests::a_slow_draining_culprit_is_not_demoted_by_a_later_failure`.
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real chain whose failing producer leaves a grandchild holding its stderr"]
async fn a_slow_draining_culprit_keeps_the_blame_from_the_last_stage() {
    // The failure order is fixed by construction, not by timing: the producer exits
    // 3 → its stdout pipe closes → `cat` reaches EOF → the last stage exits 5. Two
    // genuine checked failures, neither of which died into a teardown, so pipefail
    // blames the leftmost — the producer, with the producer's own stderr.
    //
    // The grandchild is what makes this a regression guard. The producer's stderr
    // drain cannot end until the chain's teardown kills that grandchild, i.e. well
    // after the last stage has failed and the session's watcher has fired teardown
    // for it. A `torn_down` read *then* — after the drain instead of at the exit —
    // marks the producer a teardown victim and hands the chain's exit code and
    // stderr to the last stage, which is the wrong diagnosis: the producer died
    // first, and on its own.
    let session = failing_producer_whose_stderr_outlives_it()
        .pipe(Command::new("sh").args(["-c", "cat >/dev/null; exit 5"]))
        .start()
        .await
        .expect("start the live chain");

    // Let the standing watcher be the one that observes the last stage's failure
    // (its reap is what clears the session's pid), so this exercises the watcher's
    // latch rather than `finish`'s own drive to the same exit.
    poll_until(
        Duration::from_secs(30),
        Duration::from_millis(50),
        "the session's watcher to reap the failed last stage",
        || session.pid().is_none(),
    )
    .await;

    let processkit::Finished {
        outcome, stderr, ..
    } = completes_within(
        Duration::from_secs(30),
        "finishing a chain whose culprit drains slowly",
        session.finish(),
    )
    .await
    .expect("finish folds a result");

    assert_eq!(
        outcome,
        processkit::Outcome::Exited(3),
        "the producer failed first and stays the culprit, however long its drain took: {outcome:?}"
    );
    assert!(
        stderr.contains("boom"),
        "and the chain reports the culprit's own stderr: {stderr:?}"
    );
}

/// A **last** stage that fails on its own while a grandchild keeps its stderr pipe
/// open — the mirror of `failing_producer_whose_stderr_outlives_it`, one stage to
/// the right. It never reads its stdin, so its exit is what closes the chain's pipe
/// and kills the producer; the grandchild takes stdin and stdout from `/dev/null`,
/// so it holds neither that pipe nor this stage's stdout open, only its stderr.
#[cfg(unix)]
fn failing_last_stage_whose_stderr_outlives_it() -> Command {
    Command::new("sh").args([
        "-c",
        "sleep 30 </dev/null >/dev/null & printf 'boom\\n' >&2; exit 3",
    ])
}

// The buffering counterpart of `a_slow_draining_culprit_keeps_the_blame_from_the_
// last_stage`, covering the fourth and last drive that latches a disposition: the
// last stage of `output_string()`. Unix-only for the same two reasons — the forked
// grandchild and a real `SIGPIPE` — and its fold half is covered hermetically by
// `pipeline::tests::a_slow_draining_last_stage_keeps_the_blame_from_a_sigpipe_producer`.
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real chain whose failing last stage leaves a grandchild holding its stderr"]
async fn output_string_blames_a_slow_draining_last_stage_over_a_sigpipe_producer() {
    // The failure order is fixed by construction, not by timing: the last stage
    // exits 3 without reading stdin → that closes the pipe → `yes`, blocked writing
    // into it, dies of SIGPIPE. So the last stage failed *first* and on its own,
    // while the producer's death is merely its consequence — the canonical shape
    // where a SIGPIPE victim must not be blamed.
    //
    // The grandchild is what makes this a regression guard. The producer's SIGPIPE
    // death fires the chain's teardown while the last stage's stderr drain is still
    // wedged on the backgrounded `sleep`, and that drain can only end once the
    // teardown's killer reaps the grandchild. Reading `torn_down` *then* — after the
    // drain instead of at the exit — marks the last stage a victim of a teardown
    // that fired after it had already died, which leaves the fold no genuine culprit
    // at all: it falls back to the leftmost stage and reports the producer's signal
    // and stderr instead of the real failure's exit 3.
    let result = completes_within(
        Duration::from_secs(30),
        "capturing a chain whose last stage drains slowly",
        endless_yes()
            .pipe(failing_last_stage_whose_stderr_outlives_it())
            .output_string(),
    )
    .await
    .expect("a failing stage is captured in the result, not raised");

    assert_eq!(
        result.code(),
        Some(3),
        "the last stage died first and on its own, so it stays the culprit: {result:?}"
    );
    assert!(
        result.stderr().contains("boom"),
        "and the chain reports that stage's own stderr: {:?}",
        result.stderr()
    );
}

#[tokio::test]
#[ignore = "spawns a real live chain bounded by a chain-wide timeout"]
async fn pipeline_start_timeout_kills_the_live_chain() {
    use processkit::{Finished, Outcome};

    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };

    let session = producer
        .pipe(sleep_secs(30))
        .timeout(Duration::from_millis(300))
        .start()
        .await
        .expect("start the live chain");

    let start = Instant::now();
    let Finished {
        outcome,
        stderr,
        stderr_truncated,
        ..
    } = completes_within(
        Duration::from_secs(15),
        "finishing a chain-wide-timed-out live session",
        session.finish(),
    )
    .await
    .expect("finish folds a result");
    assert_eq!(
        outcome,
        Outcome::TimedOut,
        "a chain-wide timeout reports TimedOut: {outcome:?}"
    );
    assert!(stderr.is_empty(), "no diagnostic was collected: {stderr:?}");
    assert!(!stderr_truncated, "no diagnostic was collected: {stderr:?}");
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "the chain-wide timeout must fire promptly, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real live chain that writes stderr before a chain-wide timeout"]
async fn pipeline_start_timeout_preserves_collected_stderr_and_truncation() {
    use processkit::{Finished, Outcome, OutputBufferPolicy};

    let diagnostic_stage = if cfg!(windows) {
        Command::new("powershell").args([
            "-NoProfile",
            "-Command",
            "[Console]::Error.WriteLine('diagnostic-before'); [Console]::Error.WriteLine('diagnostic-after'); [Console]::Out.WriteLine('ready'); Start-Sleep -Seconds 30",
        ])
    } else {
        Command::new("sh").args([
            "-c",
            "printf 'diagnostic-before\\n' >&2; printf 'diagnostic-after\\n' >&2; printf 'ready\\n'; sleep 30",
        ])
    }
    .output_buffer(OutputBufferPolicy::bounded(1));

    let mut session = if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "exit 0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    }
    .pipe(diagnostic_stage)
    .timeout(if cfg!(windows) {
        Duration::from_secs(8)
    } else {
        Duration::from_secs(2)
    })
    .start()
    .await
    .expect("start the live chain");

    // The readiness line is written after both diagnostics. Waiting for it makes
    // the test independent of how quickly a freshly spawned child is scheduled,
    // while stderr remains a background drain owned by the stage's RunningProcess.
    let ready = completes_within(
        Duration::from_secs(10),
        "waiting for the post-diagnostic readiness line",
        session.wait_for_line(|line| line == "ready", Duration::from_secs(8)),
    )
    .await
    .expect("the stage wrote diagnostics before readiness");
    assert_eq!(ready, "ready");
    let Finished {
        outcome,
        stderr,
        stderr_truncated,
        ..
    } = completes_within(
        Duration::from_secs(15),
        "finishing a chain-wide-timed-out live session with stderr",
        session.finish(),
    )
    .await
    .expect("finish folds a timed-out result");

    assert_eq!(outcome, Outcome::TimedOut, "timeout result: {outcome:?}");
    assert_eq!(
        stderr, "diagnostic-after",
        "pipefail keeps the attributed stage's retained stderr"
    );
    assert!(
        stderr_truncated,
        "the dropped earlier diagnostic must remain visible as truncation"
    );
}

#[tokio::test]
#[ignore = "spawns a real live chain cancelled via the chain-wide token"]
async fn pipeline_start_cancel_ends_the_live_chain() {
    use tokio_util::sync::CancellationToken;

    let producer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo x"])
    } else {
        Command::new("sh").args(["-c", "printf 'x\\n'"])
    };

    let token = CancellationToken::new();
    let session = producer
        .pipe(sleep_secs(30))
        .cancel_on(token.clone())
        .start()
        .await
        .expect("start the live chain");

    let fired = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        fired.cancel();
    });

    let start = Instant::now();
    let err = completes_within(
        Duration::from_secs(15),
        "finishing a cancelled live session",
        session.finish(),
    )
    .await
    .expect_err("a cancelled chain surfaces as Err");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "cancellation must be prompt, took {:?}",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real live chain whose stages have distinct cancellation tokens"]
async fn pipeline_session_cancel_on_remains_chain_wide_with_distinct_stage_tokens() {
    use tokio_util::sync::CancellationToken;

    let producer_token = CancellationToken::new();
    let consumer_token = CancellationToken::new();
    let chain_token = CancellationToken::new();
    let session = sleep_secs(30)
        .cancel_on(producer_token.clone())
        .pipe(sleep_secs(30).cancel_on(consumer_token.clone()))
        .cancel_on(chain_token.clone())
        .start()
        .await
        .expect("start the live chain");

    chain_token.cancel();
    let err = completes_within(
        Duration::from_secs(15),
        "finishing the chain-wide-cancelled live session",
        session.finish(),
    )
    .await
    .expect_err("the pipeline token must cancel stages that own distinct tokens");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        !producer_token.is_cancelled() && !consumer_token.is_cancelled(),
        "chain cancellation must not fire either stage-owned token"
    );
}

#[tokio::test]
#[ignore = "starts one real stage then fails to spawn the next; start() must surface the launch error"]
async fn pipeline_start_errors_on_a_partially_started_chain() {
    // The first stage starts fine; the second names a program that does not exist,
    // so `start()` surfaces the launch error instead of a half-built session. The
    // already-started first stage is torn down by kill-on-drop of the partial
    // launch (proven directly, Unix-only, in the pid-probe companion below).
    let bogus = Command::new("processkit-definitely-not-a-real-program-xyz");
    let err = sleeper()
        .pipe(bogus)
        .start()
        .await
        .expect_err("a bogus second stage must fail start()");
    assert!(
        matches!(
            err.reason(),
            processkit::ErrorReason::NotFound { .. } | processkit::ErrorReason::Spawn { .. }
        ),
        "expected NotFound/Spawn, got {err:?}"
    );
}

// Unix-only: proving the partial-launch teardown by pid needs the first stage to
// record its pid *before* the failing spawn tears it down. A `sh -c` does that in
// microseconds; a Windows PowerShell first stage starts too slowly and is killed
// before it can write the file (so only the Err is asserted cross-platform above).
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns one real stage then fails to spawn the next; the partial chain must be reaped"]
async fn pipeline_start_reaps_a_partially_started_chain() {
    let pidfile = std::env::temp_dir().join(format!(
        "processkit_t159_partial_{}.pid",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&pidfile);

    // The first stage starts and records its pid; the second stage names a program
    // that does not exist, so `start()` fails to spawn it — and must tear the
    // already-started first stage down (the partial-launch no-orphan invariant).
    let bogus = Command::new("processkit-definitely-not-a-real-program-xyz");
    let err = pid_recording_idle(&pidfile)
        .pipe(bogus)
        .start()
        .await
        .expect_err("a bogus second stage must fail start()");
    assert!(
        matches!(
            err.reason(),
            processkit::ErrorReason::NotFound { .. } | processkit::ErrorReason::Spawn { .. }
        ),
        "expected NotFound/Spawn, got {err:?}"
    );

    // Under load, the first stage can be killed before it is scheduled to record
    // its pid. In that case no process ran; otherwise the recorded pid must be gone.
    if let Some(inner_pid) = read_recorded_pid(&pidfile).await {
        assert_pid_reaped(inner_pid, "first stage of a partially-started chain").await;
    }
    let _ = std::fs::remove_file(&pidfile);
}

// ---------------------------------------------------------------------------
// T-271 / T-272: the parent-side reader of a `merge_stderr_in_pipe` stage's
// shared stdout+stderr pipe (`sys::merge_pipe`), exercised through a real chain.
//
// The quiet-grandchild test below exists once per platform, because the two
// readers keep that read off the runtime's shared blocking pool by different
// means: Unix drives the fd through the reactor, Windows blocks on a bridge
// thread of the module's own and interrupts it when the reader is dropped. Each
// version therefore builds its grandchild the way its own platform can — a
// forking `sh -c '… &'` on Unix, a re-exec of this test binary on Windows, whose
// `Stdio::inherit` hands the launched process the merge pipe's write end.
//
// The bulk-payload test is Unix-only: its producer is a `sh`/`seq` one-liner
// with no cheap Windows equivalent (a `cmd` loop of this size takes seconds).
// The cross-platform behaviour of a merged stage — write order, the pipefail
// stderr trade-off, the final-stage no-op — is covered by the tests near the top
// of this file, and the reader's own chunking, EOF and error branches (nothing a
// test can do to the pipe from outside makes its read fail, so that one needs a
// substituted source) by the unit tests in `src/sys/merge_pipe.rs`.
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real merged-stderr chain whose payload outgrows the OS pipe buffer"]
async fn merged_stage_delivers_a_multi_read_payload_and_its_eof() {
    // Over 200 KiB across the two merged streams (~112 KiB each), several times
    // the ~64 KiB pipe buffer: the parent-side reader has to park on readiness
    // and reassemble
    // many partial reads instead of taking the whole payload in one. It then has
    // to see the pipe's EOF once the child exits and no one else holds the write
    // end — without it the downstream `cat` would never reach EOF on its stdin
    // and this chain would hang rather than finish.
    const LINES: usize = 20_000;
    let producer = Command::new("sh").args(["-c", &format!("seq 1 {LINES}; seq 1 {LINES} >&2")]);

    let result = completes_within(
        Duration::from_secs(60),
        "a bulk merged-stderr chain",
        producer
            .merge_stderr_in_pipe()
            .pipe(passthrough_stage())
            .output_string(),
    )
    .await
    .expect("run merged pipeline");

    assert!(result.is_success(), "pipeline result: {result:?}");
    assert_eq!(
        result.stdout().lines().count(),
        LINES * 2,
        "every merged line must survive the reader's partial reads"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "spawns a real merged-stderr chain plus a grandchild that holds the pipe open"]
fn merged_stage_reader_parks_no_blocking_pool_thread_for_a_quiet_grandchild() {
    // The regression this is the guard for: the merged pipe's parent end used to
    // be a `tokio::fs::File`, so a read with nothing to read sat on a thread of
    // the runtime's *shared* blocking pool until the pipe closed — and a
    // grandchild that inherited the write end keeps it open long after the
    // direct child is gone, torn-down run or not. Giving the runtime exactly one
    // blocking thread turns that into a hard, deterministic assertion: the probe
    // below can only run if the merged reader is not holding it.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let holding = dir.path().join("holding");

        // The stage exits at once but leaves a grandchild holding the merge
        // pipe's write end (inherited stdout *and* stderr) and writing nothing
        // to it, so the parent-side read stays pending with no EOF in sight. The
        // marker is created by that grandchild, so waiting for it is a fact
        // about the process tree, not a guess about timing.
        let producer = Command::new("sh").args([
            "-c",
            &format!("(touch '{}'; sleep 30) & exit 0", holding.display()),
        ]);
        let token = tokio_util::sync::CancellationToken::new();
        let chain = producer
            .merge_stderr_in_pipe()
            .pipe(passthrough_stage())
            .cancel_on(token.clone());
        let run = tokio::spawn(async move { chain.output_string().await });

        poll_until(
            Duration::from_secs(30),
            Duration::from_millis(25),
            "the grandchild to take over the merge pipe",
            || holding.exists(),
        )
        .await;

        // While that read is pending, the one blocking thread must still be
        // free. (`spawn_blocking` would queue behind the old implementation's
        // parked read, and nothing here is going to end it — the grandchild
        // writes nothing and outlives the whole run.)
        completes_within(
            Duration::from_secs(10),
            "a blocking-pool probe while the merged reader waits",
            tokio::task::spawn_blocking(|| {}),
        )
        .await
        .expect("blocking probe");

        // Teardown resolves in bounded time rather than waiting out the
        // grandchild's own lifetime.
        token.cancel();
        let err = completes_within(Duration::from_secs(30), "the cancelled chain", run)
            .await
            .expect("run task")
            .expect_err("a cancelled chain errors");
        assert!(
            matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
            "expected Cancelled, got {err:?}"
        );

        // ...and the pool is still free afterwards: the torn-down run left
        // nothing of its own running there.
        completes_within(
            Duration::from_secs(10),
            "a blocking-pool probe after the chain was torn down",
            tokio::task::spawn_blocking(|| {}),
        )
        .await
        .expect("blocking probe");
    });
}

/// Set on the middle process of the Windows test below — the merged stage
/// itself, which launches the grandchild and exits at once. Its value is the
/// marker path that grandchild creates.
#[cfg(windows)]
const MERGE_PIPE_STAGE: &str = "PK_T272_MERGE_PIPE_STAGE";
/// Set on the innermost process: the grandchild that inherits the merge pipe's
/// write end, records that it has it, and then holds it in silence. Its value is
/// that same marker path.
#[cfg(windows)]
const MERGE_PIPE_HOLDER: &str = "PK_T272_MERGE_PIPE_HOLDER";
/// The libtest name of the test below, used to re-invoke it as its own stage and
/// grandchild.
#[cfg(windows)]
const MERGE_PIPE_TEST: &str = "pipeline::merged_stage_with_a_quiet_grandchild_tears_down_promptly";

/// A merged stage whose grandchild keeps the pipe open and silent: the chain
/// stays alive on it (no premature EOF), and cancelling then tears the chain
/// down promptly instead of waiting for that grandchild to go away by itself.
///
/// The teardown half is what covers the Windows reader's bridge thread through a
/// real run: cancelling drops the parent-side reader while its thread is parked
/// in a read nothing is going to complete, so a teardown that joined that thread
/// without interrupting it first would take the grandchild's whole lifetime —
/// measured below, because a `Drop` blocking a runtime thread also stops the
/// timer that would otherwise cut the wait short.
///
/// Deliberately *not* asserted here, unlike the Unix version above: that the
/// runtime's shared blocking pool stays free. On Windows `tokio::process`'s own
/// `ChildStdio` is `Blocking<ArcFile>` — every read of a child's stdout takes a
/// pool thread — so a chain occupies that pool whatever this module does, and a
/// pool probe would say nothing about the merge pipe's reader. Nor does this
/// prove the bridge thread ends at all: the chain's containment reaps the
/// grandchild at teardown, which ends any read on that pipe by itself. Both of
/// those are asserted where they can be: `src/sys/merge_pipe.rs`'s unit tests
/// hold the write end open past the drop and watch the thread go.
///
/// The runtime is deliberately the default single-threaded one: it is what makes
/// the measurement above discriminating. Given a second worker, the teardown
/// that a blocking `Drop` is stalling would simply carry on there, reach the
/// containment kill, and end the grandchild — and with it the read — inside the
/// budget, which is not the property this test is trying to pin.
#[cfg(windows)]
#[tokio::test]
#[ignore = "re-execs the test binary as a merged-stderr chain plus a grandchild that holds the pipe open"]
async fn merged_stage_with_a_quiet_grandchild_tears_down_promptly() {
    // Three processes, all of them this binary running this one test, told apart
    // by the two env markers above:
    //
    //   driver (here)  → the merged chain's parent, holding the pipe's read end
    //     stage        → the chain's first stage; launches the holder, exits
    //       holder     → inherited the pipe's write end; records that, then
    //                    sleeps, writing nothing and never closing it
    if let Some(marker) = std::env::var_os(MERGE_PIPE_HOLDER) {
        hold_the_merge_pipe(marker.as_ref());
        return;
    }
    if let Some(marker) = std::env::var_os(MERGE_PIPE_STAGE) {
        launch_the_merge_pipe_holder(marker.as_ref());
        return;
    }

    let dir = tempfile::tempdir().expect("temp dir");
    let holding = dir.path().join("holding");
    let exe = std::env::current_exe().expect("locate the integration-test binary");

    let producer = Command::new(exe)
        .args([MERGE_PIPE_TEST, "--exact", "--ignored"])
        .env(MERGE_PIPE_STAGE, holding.to_string_lossy().as_ref());
    let token = tokio_util::sync::CancellationToken::new();
    let chain = producer
        .merge_stderr_in_pipe()
        .pipe(passthrough_stage())
        .cancel_on(token.clone());
    let run = tokio::spawn(async move { chain.output_string().await });

    // The marker is written by the grandchild itself, so waiting for it is a
    // fact about the process tree, not a guess about timing.
    poll_until(
        Duration::from_secs(60),
        Duration::from_millis(25),
        "the grandchild to take over the merge pipe",
        || holding.exists(),
    )
    .await;

    // The chain is still running, which is what proves the shape this test
    // needs: the last stage is waiting for an EOF only the grandchild's copy of
    // the write end can deliver, and it is not writing one.
    assert!(
        !run.is_finished(),
        "the grandchild must still be holding the merge pipe open"
    );

    // Teardown resolves in bounded time rather than waiting out the grandchild's
    // own lifetime (`HOLD`, far longer than either bound below).
    let cancelled_at = Instant::now();
    token.cancel();
    let err = completes_within(Duration::from_secs(20), "the cancelled chain", run)
        .await
        .expect("run task")
        .expect_err("a cancelled chain errors");
    let teardown = cancelled_at.elapsed();
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    // Measured rather than left to the timeout above: the wait this guards
    // against blocks a runtime thread, and the elapsed reading survives that
    // where a timer may not. The margin is an order of magnitude, not a tight
    // budget — a prompt teardown here takes milliseconds.
    assert!(
        teardown < HOLD / 2,
        "tearing the chain down must not wait out the grandchild: took {teardown:?}"
    );
}

/// How long the grandchild holds the merge pipe: long enough that a teardown
/// waiting it out could not be mistaken for a prompt one.
#[cfg(windows)]
const HOLD: Duration = Duration::from_secs(60);

/// The middle process of the test above: launch the grandchild and exit, leaving
/// it with the merge pipe.
///
/// `std::process::Command` inherits our stdout and stderr — which are the merged
/// pipe's write end — by duplicating them into the new process as inheritable
/// handles, so the grandchild keeps that end open after this process is gone.
// Waiting for the grandchild is exactly what this must not do: this process has
// to exit while that one still holds the pipe. Nothing here outlives it to be
// troubled by an unreaped child either — the chain's containment reaps the whole
// tree at teardown, and on Windows a dropped `Child` leaves no zombie to collect.
#[allow(clippy::zombie_processes)]
#[cfg(windows)]
fn launch_the_merge_pipe_holder(marker: &std::ffi::OsStr) {
    let exe = std::env::current_exe().expect("locate the integration-test binary");
    std::process::Command::new(exe)
        .args([MERGE_PIPE_TEST, "--exact", "--ignored"])
        .env(MERGE_PIPE_HOLDER, marker)
        .env_remove(MERGE_PIPE_STAGE)
        .spawn()
        .expect("launch the grandchild that holds the merge pipe");
}

/// The innermost process of the test above: announce that this process holds the
/// merge pipe's write end, then hold it in silence for longer than the test
/// needs it. The chain's own containment reaps this process at teardown, well
/// before the sleep ends.
#[cfg(windows)]
fn hold_the_merge_pipe(marker: &std::ffi::OsStr) {
    std::fs::write(marker, b"held").expect("record that the grandchild holds the merge pipe");
    std::thread::sleep(HOLD);
}

// ---------------------------------------------------------------------------
// T-287: a non-final stage's stdout belongs to the pipe.
//
// A non-final stage configured `stdout(Null)`/`stdout(Inherit)`/`stdout_file(..)`
// used to leave `take_stdout_pipe` empty, so the next stage started on a silent
// EOF while the producer's bytes went elsewhere — and the chain reported success.
// The launch boundary now gives every non-final stage the internal downstream
// pipe. `Pipeline::launch` is shared by the buffering verbs and the streaming
// session (K-051), but that is exactly the shape a past fix here got wrong on one
// of the two paths (K-116), so every configuration below is asserted through
// *both*: `relayed_through_capture` and `relayed_through_session`.
// ---------------------------------------------------------------------------

/// The payload the non-final stage under test produces. Already in sorted order,
/// so `sort_stage` is a faithful passthrough of it on both platforms.
const RELAY_PAYLOAD: [&str; 3] = ["alpha", "bravo", "charlie"];

/// A producer that writes [`RELAY_PAYLOAD`] to stdout, one line each.
fn relay_producer() -> Command {
    if cfg!(windows) {
        Command::new("cmd").args(["/d", "/c", "echo alpha& echo bravo& echo charlie"])
    } else {
        Command::new("sh").args(["-c", "printf 'alpha\\nbravo\\ncharlie\\n'"])
    }
}

/// The payload lines carried by a final stage's stdout, normalized for the
/// platform's line ending and for the blank line a Windows shell stage may add.
/// These assertions are about *content and order*; that the relay is faithful at
/// the byte level is asserted separately by
/// [`non_final_null_stdout_relays_bytes_verbatim`].
fn relayed_lines<'a>(lines: impl Iterator<Item = &'a str>) -> Vec<String> {
    lines
        .map(|line| line.trim_end().to_owned())
        .filter(|line| !line.is_empty())
        .collect()
}

/// Run `producer | sort` through the **buffering** capture path and return what
/// the final stage received. `producer` is the non-final stage under test.
async fn relayed_through_capture(producer: Command) -> Vec<String> {
    let result = producer
        .pipe(sort_stage())
        .output_string()
        .await
        .expect("run the chain");
    assert!(result.is_success(), "pipeline result: {result:?}");
    relayed_lines(result.stdout().lines())
}

/// Run the same chain through the **streaming** session path and return what the
/// caller streamed off the final stage. The second half of the K-116 pair: this
/// path reaches the same `launch`, and must show the same relay.
async fn relayed_through_session(producer: Command) -> Vec<String> {
    use processkit::{Finished, Outcome};
    use tokio_stream::StreamExt;

    let mut session = producer
        .pipe(sort_stage())
        .start()
        .await
        .expect("start the live chain");
    let mut lines = session
        .stdout_lines()
        .expect("stream the last stage's stdout");
    let mut collected = Vec::new();
    while let Some(line) = completes_within(
        Duration::from_secs(15),
        "streaming a line from the live chain",
        lines.next(),
    )
    .await
    {
        collected.push(line);
    }
    drop(lines);

    let Finished { outcome, .. } = session.finish().await.expect("finish the chain");
    assert_eq!(
        outcome,
        Outcome::Exited(0),
        "a clean chain folds to Exited(0)"
    );
    relayed_lines(collected.iter().map(String::as_str))
}

#[tokio::test]
#[ignore = "spawns a real two-stage pipeline whose producer suppresses its own stdout"]
async fn non_final_null_stdout_still_feeds_the_next_stage() {
    let relayed =
        relayed_through_capture(relay_producer().stdout(processkit::StdioMode::Null)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "a non-final stage's `stdout(Null)` must not swallow the bytes the next stage is owed"
    );
}

#[tokio::test]
#[ignore = "spawns a real two-stage streaming chain whose producer suppresses its own stdout"]
async fn non_final_null_stdout_still_feeds_the_streaming_session() {
    let relayed =
        relayed_through_session(relay_producer().stdout(processkit::StdioMode::Null)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "the streaming path shares `launch`, so it must relay exactly as the buffering path does"
    );
}

#[tokio::test]
#[ignore = "spawns a real two-stage pipeline whose producer inherits the parent's stdout"]
async fn non_final_inherit_stdout_still_feeds_the_next_stage() {
    let relayed =
        relayed_through_capture(relay_producer().stdout(processkit::StdioMode::Inherit)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "a non-final stage's `stdout(Inherit)` must not divert the bytes to the parent's terminal"
    );
}

#[tokio::test]
#[ignore = "spawns a real two-stage streaming chain whose producer inherits the parent's stdout"]
async fn non_final_inherit_stdout_still_feeds_the_streaming_session() {
    let relayed =
        relayed_through_session(relay_producer().stdout(processkit::StdioMode::Inherit)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "the streaming path shares `launch`, so it must relay exactly as the buffering path does"
    );
}

#[tokio::test]
#[ignore = "spawns real two-stage pipelines whose producer carries a stdout file redirect"]
async fn non_final_stdout_file_feeds_the_next_stage_and_leaves_the_file_alone() {
    let dir = tempfile::tempdir().expect("temp dir");
    let seeded = dir.path().join("stage.log");
    let absent = dir.path().join("never-created.log");
    std::fs::write(&seeded, "previous run\n").expect("seed the stage's log");

    let relayed = relayed_through_capture(relay_producer().stdout_file(&seeded)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "a non-final stage's `stdout_file` must not divert the bytes to the file"
    );
    assert_eq!(
        std::fs::read_to_string(&seeded).expect("read the seeded log"),
        "previous run\n",
        "the displaced redirect must be left unopened, so an earlier run's log survives"
    );

    let relayed = relayed_through_capture(relay_producer().stdout_file(&absent)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "same for a redirect to a fresh path"
    );
    assert!(
        !absent.exists(),
        "the displaced redirect must not be created either"
    );
}

#[tokio::test]
#[ignore = "spawns a real two-stage streaming chain whose producer carries a stdout file redirect"]
async fn non_final_stdout_file_feeds_the_streaming_session_and_leaves_the_file_alone() {
    let dir = tempfile::tempdir().expect("temp dir");
    let seeded = dir.path().join("stage.log");
    std::fs::write(&seeded, "previous run\n").expect("seed the stage's log");

    let relayed = relayed_through_session(relay_producer().stdout_file(&seeded)).await;
    assert_eq!(
        relayed, RELAY_PAYLOAD,
        "the streaming path shares `launch`, so it must relay exactly as the buffering path does"
    );
    assert_eq!(
        std::fs::read_to_string(&seeded).expect("read the seeded log"),
        "previous run\n",
        "the displaced redirect must be left unopened on this path too"
    );
}

// `cat` is byte-faithful, so the chain's captured bytes are exactly what the
// producer wrote. Windows has no equally faithful stage in `PATH` (`more` is a
// pager and rewrites line endings), which is why the cross-platform tests above
// assert on lines and this one is Unix-only.
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real two-stage pipeline and compares the relayed bytes exactly"]
async fn non_final_null_stdout_relays_bytes_verbatim() {
    // A bare CR mid-stream and a final line with no terminator: both survive a
    // faithful relay and both would be lost or invented by a line-oriented one.
    let payload = b"alpha\rbravo\ncharlie-without-a-newline".to_vec();
    let result = Command::new("sh")
        .args(["-c", "printf 'alpha\\rbravo\\ncharlie-without-a-newline'"])
        .stdout(processkit::StdioMode::Null)
        .pipe(Command::new("cat"))
        .output_bytes()
        .await
        .expect("run the chain");

    assert!(result.is_success(), "pipeline result: {result:?}");
    assert_eq!(
        result.stdout(),
        &payload,
        "the next stage must receive the producer's bytes verbatim"
    );
}

#[tokio::test]
#[ignore = "spawns a real two-stage pipeline whose final stage suppresses its stdout"]
async fn final_stage_stdout_configuration_is_left_alone() {
    // The override is scoped to non-final stages: the last stage's stdout is the
    // chain's own output, so a capture verb must still refuse a non-piped one
    // loudly instead of being handed a silently-forced pipe.
    let err = relay_producer()
        .pipe(sort_stage().stdout(processkit::StdioMode::Null))
        .output_string()
        .await
        .expect_err("capturing a chain whose last stage discards its stdout must be a loud error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Io(_)),
        "expected ErrorReason::Io, got {err:?}"
    );
    assert!(
        err.to_string().contains("stdout is not piped"),
        "the error must still name the non-piped stdout: {err}"
    );
}
