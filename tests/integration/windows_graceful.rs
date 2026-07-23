//! Windows-only: the opt-in graceful teardown via a console `CTRL_BREAK` event
//! ([`Command::windows_graceful_ctrl_break`]) — gated by the `#[cfg(windows)] mod`
//! declaration in `main.rs`. The unix graceful ladder lives in `shutdown.rs`.
//!
//! The direct child is spawned into its own console process group and, at
//! [`ProcessGroup::shutdown`], sent `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT)`
//! before the grace window, then `TerminateJobObject`'d if it hasn't exited.
//! Three scenarios, driven against a purpose-built PowerShell child that installs
//! a real `SetConsoleCtrlHandler` (system tools like `ping` install their own
//! handlers and ignore `CTRL_BREAK`, so they can't stand in here):
//!
//! - a child whose handler **exits** on the event drains early (exit code 42);
//! - a child whose handler **ignores** it rides the grace to the hard-kill
//!   fallback;
//! - an **off-console** child (`create_no_window`) never receives the event and
//!   likewise falls back — the documented console-only boundary.
//!
//! `GenerateConsoleCtrlEvent` routes through the console the sender shares with
//! the child, so the two delivery tests first ensure this process owns one (a
//! harness launched headless has none). All are `#[ignore]`d real-subprocess
//! tests, run on the Windows CI leg via `--include-ignored`.
//!
//! The **automatic** `WM_CLOSE` tier (no opt-in) is covered by two further tests:
//! a real WinForms-windowed PowerShell child that exits (code 43) on `WM_CLOSE`
//! within the grace, and a windowless `ping` child that has no window to close and
//! so must be hard-killed *promptly* — proving the tier adds no grace wait for a
//! windowless tree.

use std::io::Write;
use std::time::{Duration, Instant};

use processkit::{Command, Outcome, ProcessGroup, ProcessGroupOptions};

use crate::common::completes_within;

/// A PowerShell script that installs a `CTRL_BREAK` handler, prints `ready` once
/// it is armed, then idles ~30s. `HANDLER_BODY` is the C# statement the handler
/// runs on the event (e.g. `Environment.Exit(42)`, or nothing to ignore it).
const PS_HANDLER_TEMPLATE: &str = r#"Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class Ctrl {
  public delegate bool Handler(uint sig);
  [DllImport("kernel32.dll")] public static extern bool SetConsoleCtrlHandler(Handler handler, bool add);
  static Handler _h;
  public static void Arm() { _h = s => { HANDLER_BODY; return true; }; SetConsoleCtrlHandler(_h, true); }
}
'@
[Ctrl]::Arm()
Write-Output 'ready'
Start-Sleep -Seconds 30
"#;

/// Build a `Command` that runs a `CTRL_BREAK`-handling PowerShell child (opted
/// into the graceful console teardown), plus the temp script path that must be
/// kept alive for the child's lifetime. `handler_body` is the C# statement the
/// handler runs on the event.
fn ctrl_break_powershell(handler_body: &str) -> (Command, tempfile::TempPath) {
    let script = PS_HANDLER_TEMPLATE.replace("HANDLER_BODY", handler_body);
    let mut f = tempfile::Builder::new()
        .suffix(".ps1")
        .tempfile()
        .expect("create temp script");
    f.write_all(script.as_bytes()).expect("write temp script");
    f.flush().expect("flush temp script");
    // Close our handle before spawning — Windows blocks PowerShell from reading a
    // file we still hold open — while keeping it on disk until the path drops.
    let temp_path = f.into_temp_path();
    let cmd = Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &temp_path.to_string_lossy(),
        ])
        .windows_graceful_ctrl_break();
    (cmd, temp_path)
}

/// Ensure THIS process owns a console before spawning a child we intend to
/// `CTRL_BREAK`.
///
/// `GenerateConsoleCtrlEvent` routes through the console the *sender* shares with
/// the target group; a harness launched without one (a detached/service context)
/// has nothing to route through and the event silently no-ops. `AllocConsole`
/// attaches one — a harmless no-op (`ERROR_ACCESS_DENIED`) when a console already
/// exists, and it does not disturb std handles already redirected (cargo's output
/// capture). The child, spawned after, inherits the console and can receive the
/// event. This mirrors the "console-only" boundary the mechanism documents.
fn ensure_console() {
    use windows_sys::Win32::System::Console::AllocConsole;
    // SAFETY: a plain console allocation; the result is deliberately ignored (it
    // fails when a console is already present, which is fine).
    unsafe {
        let _ = AllocConsole();
    }
}

#[tokio::test]
#[ignore = "spawns a real console subprocess; opt-in CTRL_BREAK graceful shutdown"]
async fn windows_ctrl_break_lets_a_handling_child_exit_within_the_grace() {
    ensure_console();
    // A generous grace — the child must exit *early* on the CTRL_BREAK, so a
    // regression that failed to deliver it would ride the full grace and trip the
    // bound below.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(25)),
    )
    .expect("create group");

    // The handler exits with the distinctive code 42, so the reported outcome is
    // positive proof the CTRL_BREAK ran the handler — not the TerminateJobObject
    // fallback (which would exit 1).
    let (cmd, _script) = ctrl_break_powershell("Environment.Exit(42)");
    let mut run = group.start(&cmd).await.expect("start");
    // Wait until the handler is armed (Add-Type compiles first — a few seconds on
    // a cold runner) before signalling: a CTRL_BREAK landing earlier would hit the
    // default handler and terminate the child, masking the graceful path.
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(30))
        .await
        .expect("handler armed");

    let start = Instant::now();
    completes_within(
        Duration::from_secs(30),
        "ctrl-break graceful shutdown",
        group.shutdown(),
    )
    .await
    .expect("shutdown ok");
    assert!(
        start.elapsed() < Duration::from_secs(12),
        "a CTRL_BREAK-handling child must exit early, not ride the 25s grace (took {:?})",
        start.elapsed()
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert_eq!(
        outcome,
        Outcome::Exited(42),
        "the child exited via its CTRL_BREAK handler (code 42), not the hard-kill fallback"
    );
}

#[tokio::test]
#[ignore = "spawns a CTRL_BREAK-ignoring subprocess; escalates to TerminateJobObject"]
async fn windows_ctrl_break_escalates_to_terminate_for_an_ignoring_child() {
    ensure_console();
    // A short grace: the child receives the CTRL_BREAK but its handler ignores it
    // (returns handled without exiting), so it must ride the grace to the hard
    // kill.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(1)),
    )
    .expect("create group");

    // An empty handler body: the handler returns `true` (handled) without exiting,
    // so the child keeps running through the grace.
    let (cmd, _script) = ctrl_break_powershell("");
    let mut run = group.start(&cmd).await.expect("start");
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(30))
        .await
        .expect("handler armed");

    let start = Instant::now();
    completes_within(
        Duration::from_secs(15),
        "ctrl-break escalation shutdown",
        group.shutdown(),
    )
    .await
    .expect("shutdown ok");
    assert!(
        start.elapsed() >= Duration::from_millis(800),
        "an ignoring child must ride the ~1s grace before TerminateJobObject (took {:?})",
        start.elapsed()
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert!(
        !matches!(outcome, Outcome::Exited(42)),
        "the child did NOT exit via its handler — it was hard-killed by the fallback (got {outcome:?})"
    );
}

#[tokio::test]
#[ignore = "spawns a real off-console subprocess; CTRL_BREAK falls back to TerminateJobObject"]
async fn windows_ctrl_break_falls_back_to_terminate_for_an_off_console_child() {
    // `create_no_window` detaches the child from this process's console, so the
    // CTRL_BREAK cannot reach it — the documented console-only boundary. It rides
    // the grace to the TerminateJobObject fallback. A plain `ping -n 30` child is
    // fine here: no console event ever reaches it, so its own handler behavior is
    // irrelevant.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_millis(700)),
    )
    .expect("create group");

    let run = group
        .start(
            &Command::new("ping")
                .args(["-n", "30", "127.0.0.1"])
                .windows_graceful_ctrl_break()
                .create_no_window(),
        )
        .await
        .expect("start");
    // The ≈30s child is still running after a brief settle — it will NOT exit on
    // its own within the 700ms grace.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = Instant::now();
    completes_within(
        Duration::from_secs(10),
        "ctrl-break fallback shutdown",
        group.shutdown(),
    )
    .await
    .expect("shutdown ok");
    assert!(
        start.elapsed() >= Duration::from_millis(500),
        "an off-console child rides the grace before the TerminateJobObject fallback (took {:?})",
        start.elapsed()
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert!(
        !matches!(outcome, Outcome::Exited(0)),
        "the off-console child was hard-killed by the fallback (got {outcome:?})"
    );
}

/// A PowerShell script that creates a real WinForms top-level window, prints
/// `ready` once the window is shown, then pumps its message loop. On `WM_CLOSE`
/// (posted by the automatic graceful teardown) the `FormClosing` handler exits with
/// the distinctive code 43 — positive proof the WM_CLOSE path ran the handler, not
/// the `TerminateJobObject` fallback (which would exit 1).
const PS_WINDOW_SCRIPT: &str = r#"Add-Type -AssemblyName System.Windows.Forms
$form = New-Object System.Windows.Forms.Form
$form.Text = 'processkit-wmclose-test'
$form.Add_FormClosing({ [Environment]::Exit(43) })
$form.Add_Shown({ [Console]::Out.WriteLine('ready'); [Console]::Out.Flush() })
[System.Windows.Forms.Application]::Run($form)
"#;

/// Build a `Command` running the WinForms-windowed PowerShell child, plus the temp
/// script path that must be kept alive for the child's lifetime. Deliberately NOT
/// `windows_graceful_ctrl_break` — the WM_CLOSE tier is automatic for any windowed
/// member. `-Sta` keeps the WinForms message pump on a single-threaded apartment
/// (the default for Windows PowerShell, requested explicitly for robustness).
fn wm_close_powershell() -> (Command, tempfile::TempPath) {
    let mut f = tempfile::Builder::new()
        .suffix(".ps1")
        .tempfile()
        .expect("create temp script");
    f.write_all(PS_WINDOW_SCRIPT.as_bytes())
        .expect("write temp script");
    f.flush().expect("flush temp script");
    // Close our handle before spawning — Windows blocks PowerShell from reading a
    // file we still hold open — while keeping it on disk until the path drops.
    let temp_path = f.into_temp_path();
    let cmd = Command::new("powershell").args([
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Sta",
        "-File",
        &temp_path.to_string_lossy(),
    ]);
    (cmd, temp_path)
}

#[tokio::test]
#[ignore = "spawns a real GUI subprocess; automatic graceful WM_CLOSE shutdown"]
async fn windows_wm_close_lets_a_windowed_child_exit_within_the_grace() {
    // A generous grace — the windowed child must exit *early* on the WM_CLOSE, so a
    // regression that failed to post it would ride the full grace and trip the
    // bound below. No `windows_graceful_ctrl_break()`: the WM_CLOSE tier is
    // automatic for any windowed member.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(25)),
    )
    .expect("create group");

    let (cmd, _script) = wm_close_powershell();
    let mut run = group.start(&cmd).await.expect("start");
    // Wait until the window is shown (Add-Type + the WinForms load take a few
    // seconds on a cold runner) before signalling: a WM_CLOSE posted before the
    // window exists would land on nothing and the child would ride the grace.
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(30))
        .await
        .expect("window shown");

    let start = Instant::now();
    completes_within(
        Duration::from_secs(30),
        "wm-close graceful shutdown",
        group.shutdown(),
    )
    .await
    .expect("shutdown ok");
    assert!(
        start.elapsed() < Duration::from_secs(12),
        "a WM_CLOSE-handling windowed child must exit early, not ride the 25s grace (took {:?})",
        start.elapsed()
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert_eq!(
        outcome,
        Outcome::Exited(43),
        "the child exited via its WM_CLOSE FormClosing handler (code 43), not the hard-kill fallback"
    );
}

#[tokio::test]
#[ignore = "spawns a real windowless console subprocess; WM_CLOSE tier must NOT delay its prompt hard kill"]
async fn windows_windowless_child_is_hard_killed_promptly_not_after_the_grace() {
    // A plain console `ping` owns no window and did not opt into
    // `windows_graceful_ctrl_break`, so there is nothing to soft-close: the WM_CLOSE
    // tier must leave its timings unchanged — a prompt atomic hard kill at the
    // deadline, NOT a wait-out of the grace. A large grace makes a regression
    // (driving the grace loop for a windowless tree) unmistakable.
    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(20)),
    )
    .expect("create group");

    let run = group
        .start(&Command::new("ping").args(["-n", "30", "127.0.0.1"]))
        .await
        .expect("start");
    // Let it settle into its ~30s run so it is unambiguously still alive at shutdown.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = Instant::now();
    completes_within(
        Duration::from_secs(10),
        "windowless prompt shutdown",
        group.shutdown(),
    )
    .await
    .expect("shutdown ok");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "a windowless child has no WM_CLOSE target, so it is hard-killed promptly — \
         NOT after the 20s grace (took {:?})",
        start.elapsed()
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert!(
        !matches!(outcome, Outcome::Exited(0)),
        "the windowless child was hard-killed by the atomic fallback (got {outcome:?})"
    );
}

// --- T-167: the observable graceful stop (`ProcessGroup::stop` → `ShutdownReport`) ---

// A windowless child with no console-CTRL opt-in has no soft-signal tier, so the
// Job Object takes the atomic branch: the report says `SoftSignal::Unsupported`
// (no signal attempted), escalates promptly, and does NOT wait out the grace.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real windowless subprocess; T-167 stop() reports an unsupported soft signal"]
async fn windows_windowless_stop_reports_unsupported_soft_signal_and_escalates_promptly() {
    use processkit::SoftSignal;

    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(20)),
    )
    .expect("create group");
    let run = group
        .start(&Command::new("ping").args(["-n", "30", "127.0.0.1"]))
        .await
        .expect("start");
    // Settle into its ~30s run so it is unambiguously alive at stop.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let start = Instant::now();
    let report = completes_within(
        Duration::from_secs(10),
        "windowless stop",
        group.stop(Duration::from_secs(20), true),
    )
    .await
    .expect("stop ok");

    assert_eq!(
        report.soft_signal(),
        SoftSignal::Unsupported,
        "a windowless Job Object with no console-CTRL leader has no soft-signal tier"
    );
    assert_eq!(
        report.attempted_signal(),
        None,
        "nothing soft was attempted"
    );
    assert!(
        report.escalated(),
        "the live tree was hard-killed (TerminateJobObject) (report: {report:?})"
    );
    assert!(
        !report.drained_within_grace(),
        "a live windowless tree does not soft-drain"
    );
    assert!(
        report.members_before().is_some_and(|n| n >= 1),
        "the ping child was alive before the kill (report: {report:?})"
    );
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the atomic branch is prompt — it does NOT wait out the 20s grace (took {:?})",
        start.elapsed()
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert!(
        !matches!(outcome, Outcome::Exited(0)),
        "the windowless child was hard-killed by the atomic fallback (got {outcome:?})"
    );
}

// A windowed member gives the Job Object a soft tier: `stop` drives the shared
// grace loop, posts `WM_CLOSE`, the child closes within the grace, and the report
// says `SoftSignal::Sent(Term)` with `drained_within_grace == true`, no escalation.
#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real GUI subprocess; T-167 stop() reports a WM_CLOSE soft signal that drained"]
async fn windows_wm_close_stop_reports_a_sent_soft_signal_that_drained() {
    use processkit::{Signal, SoftSignal};

    let group = ProcessGroup::with_options(
        ProcessGroupOptions::default().shutdown_timeout(Duration::from_secs(25)),
    )
    .expect("create group");
    let (cmd, _script) = wm_close_powershell();
    let mut run = group.start(&cmd).await.expect("start");
    run.wait_for_line(|l| l.contains("ready"), Duration::from_secs(30))
        .await
        .expect("window shown");

    let report = completes_within(
        Duration::from_secs(30),
        "wm-close stop",
        group.stop(Duration::from_secs(25), true),
    )
    .await
    .expect("stop ok");

    assert_eq!(
        report.soft_signal(),
        SoftSignal::Sent(Signal::Term),
        "the WM_CLOSE soft trigger reached the windowed member"
    );
    assert_eq!(report.attempted_signal(), Some(Signal::Term));
    assert!(
        report.drained_within_grace(),
        "the windowed child closed within the grace (report: {report:?})"
    );
    assert!(
        !report.escalated(),
        "an in-time WM_CLOSE drain needs no TerminateJobObject"
    );
    assert!(
        report.elapsed() < Duration::from_secs(12),
        "an early WM_CLOSE drain must not ride the 25s grace (report: {report:?})"
    );

    let outcome = completes_within(Duration::from_secs(5), "reap", run.wait())
        .await
        .expect("wait");
    assert_eq!(
        outcome,
        Outcome::Exited(43),
        "the child exited via its WM_CLOSE FormClosing handler (code 43), not the hard-kill fallback"
    );
}
