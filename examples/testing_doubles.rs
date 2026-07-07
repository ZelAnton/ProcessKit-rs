//! Drive real orchestration logic through a scripted `ProcessRunner` — no
//! subprocess is ever spawned, so this runs deterministically on every OS.
//! Pairs `ScriptedRunner` (canned replies keyed on a program+argument prefix)
//! with `RecordingRunner` (records every call so tests can assert exactly
//! what was run) — see `docs/testing.md` for the full guide to the doubles.
//!
//! Run with: `cargo run --example testing_doubles`

use processkit::testing::{RecordingRunner, Reply, ScriptedRunner};
use processkit::{Command, ProcessRunner, ProcessRunnerExt, Result};

/// The logic under test: report the working tree's status. Generic over
/// `ProcessRunner`, so the very same function runs against the real `git` in
/// production and against a scripted double in a hermetic test/example.
async fn describe_repo(runner: &impl ProcessRunner) -> Result<String> {
    let clean = runner
        .probe(&Command::new("git").args(["diff", "--quiet"]))
        .await?;
    if !clean {
        return Ok("dirty working tree".to_string());
    }
    let branch = runner
        .run(&Command::new("git").args(["branch", "--show-current"]))
        .await?;
    Ok(format!("clean on {branch}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    // ScriptedRunner: canned replies, matched by program + argument prefix.
    // RecordingRunner wraps it, capturing every Invocation for later assertions.
    let runner = RecordingRunner::new(
        ScriptedRunner::new()
            .on(["git", "diff", "--quiet"], Reply::ok(""))
            .on(["git", "branch", "--show-current"], Reply::ok("main\n"))
            .fallback(Reply::fail(1, "unexpected command")),
    );

    let description = describe_repo(&runner).await?;
    println!("{description}");
    assert_eq!(description, "clean on main");

    // Assert exactly what ran, without a single real process behind it.
    let calls = runner.calls();
    assert_eq!(calls.len(), 2, "expected exactly two scripted calls");
    assert_eq!(calls[0].args_str(), ["diff", "--quiet"]);
    assert!(calls[1].has_flag("--show-current"));

    // `only_call` is handy when exactly one invocation is expected.
    let only = RecordingRunner::replying(Reply::ok("v1.2.3\n"));
    let version = only.run(&Command::new("tool").arg("--version")).await?;
    assert_eq!(version, "v1.2.3");
    assert_eq!(only.only_call().args_str(), ["--version"]);

    println!(
        "recorded {} scripted call(s) across two runners — no subprocess spawned",
        calls.len() + 1
    );
    Ok(())
}
