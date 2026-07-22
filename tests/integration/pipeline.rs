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
    assert!(
        result.duration() > Duration::ZERO,
        "T-039: a successful chain must report the measured wall-clock duration, not ZERO: {result:?}"
    );
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
        matches!(err, processkit::Error::Exit { code: 3, .. }),
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
    // This one's failure is a *raw* `Err` (`Error::Cancelled`, via a per-stage
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
        matches!(err, processkit::Error::Cancelled { .. }),
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
        matches!(err, processkit::Error::OutputTooLarge { .. }),
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
        matches!(err, processkit::Error::OutputTooLarge { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real long-running pipeline and cancels it"]
async fn pipeline_cancel_on_tears_the_whole_chain_down() {
    // S-1: a token fired mid-run cancels every stage; the run resolves to
    // Error::Cancelled rather than hanging on the endless producer.
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
        matches!(err, processkit::Error::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "cancellation must be prompt, took {:?}",
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
/// to appear (the stage records it right after spawn).
async fn read_recorded_pid(pidfile: &std::path::Path) -> u32 {
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(pidfile)
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "the idle producer never recorded its PID to {}",
        pidfile.display()
    );
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
        matches!(reused, processkit::Error::Io(_)),
        "expected Error::Io, got {reused:?}"
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
    assert_eq!(outcome, Outcome::Exited(0), "a clean chain folds to Exited(0)");
}

// Unix-only: a live-chain readiness banner must flush *promptly* out of the last
// stage, which needs a line-buffered passthrough (`grep --line-buffered`). Windows
// `findstr`/`more` block-buffer a piped stdout, so the single "ready" line would
// sit unflushed — the same platform buffering the forking-stage tests dodge by
// staying Unix-only. The `wait_for_line` delegation itself is platform-agnostic and
// is covered cross-platform by the single-process readiness suite.
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real live chain and waits for a readiness banner on its last stage"]
async fn pipeline_start_wait_for_line_on_a_live_chain() {
    // The producer prints `ready` after ~0.5s then idles ~30s; a line-buffered
    // filter passes that line through as the *last* stage's stdout, so
    // `wait_for_line` on the session sees it without tearing the chain down.
    let filter = Command::new("grep").args(["--line-buffered", "ready"]);

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

    let inner_pid = read_recorded_pid(&pidfile).await;
    let last_pid = session.pid().expect("the last stage has a live pid");
    assert!(stage_pid_alive(inner_pid), "the inner stage should be alive");

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

    let inner_pid = read_recorded_pid(&pidfile).await;
    let last_pid = session.pid().expect("the last stage has a live pid");

    // Drop the session unread — kill-on-drop must tear the whole chain down.
    drop(session);
    assert_pid_reaped(inner_pid, "inner stage after session drop").await;
    assert_pid_reaped(last_pid, "last stage after session drop").await;
    let _ = std::fs::remove_file(&pidfile);
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
    let Finished { outcome, .. } = completes_within(
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
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "the chain-wide timeout must fire promptly, took {:?}",
        start.elapsed()
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
        matches!(err, processkit::Error::Cancelled { .. }),
        "expected Cancelled, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "cancellation must be prompt, took {:?}",
        start.elapsed()
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
            err,
            processkit::Error::NotFound { .. } | processkit::Error::Spawn { .. }
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
    let pidfile =
        std::env::temp_dir().join(format!("processkit_t159_partial_{}.pid", std::process::id()));
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
            err,
            processkit::Error::NotFound { .. } | processkit::Error::Spawn { .. }
        ),
        "expected NotFound/Spawn, got {err:?}"
    );

    // The first stage recorded its pid before `start` returned its error; it must
    // now be gone — the partial chain was not left running.
    let inner_pid = read_recorded_pid(&pidfile).await;
    assert_pid_reaped(inner_pid, "first stage of a partially-started chain").await;
    let _ = std::fs::remove_file(&pidfile);
}
