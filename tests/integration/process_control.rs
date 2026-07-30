//! Whole-tree signals, suspend/resume, adoption, and member inspection —
//! everything behind the `process-control` feature (the `mod` declaration in
//! `main.rs` carries the gate).

use std::time::Duration;

#[cfg(target_os = "linux")]
use processkit::Mechanism;
use processkit::{Command, ProcessGroup, Signal};

use crate::common::*;

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and signals it"]
async fn unix_signal_reaches_the_tree() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // Print a readiness marker once the trap is installed, then idle; on SIGHUP
    // the trap fires after the current `sleep` returns (it dies to the HUP too).
    let cmd = Command::new("sh").args([
        "-c",
        "trap 'echo got-hup' HUP; echo ready; while :; do sleep 0.1; done",
    ]);
    let mut process = group.start(&cmd).await.expect("start trap child");
    let mut lines = process.stdout_lines().unwrap();

    let ready = tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("readiness line in time")
        .expect("readiness line");
    assert!(ready.contains("ready"), "line: {ready:?}");

    group.signal(Signal::Hup).expect("broadcast SIGHUP");
    let got = tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("trap line in time")
        .expect("trap line");
    assert!(got.contains("got-hup"), "line: {got:?}");
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess and freezes it"]
async fn unix_suspend_freezes_progress() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // A ticker: one line every ~50ms.
    let cmd = Command::new("sh").args([
        "-c",
        "i=0; while :; do i=$((i+1)); echo $i; sleep 0.05; done",
    ]);
    let mut process = group.start(&cmd).await.expect("start ticker");
    let mut lines = process.stdout_lines().unwrap();

    // Prove it is producing output, then freeze.
    tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("first tick in time")
        .expect("first tick");
    group.suspend().expect("suspend");

    // Drain lines emitted before the freeze landed (pipe buffering), then
    // require silence for a window several ticks long.
    tokio::time::sleep(Duration::from_millis(200)).await;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(100), lines.next()).await {}
    let stalled = tokio::time::timeout(Duration::from_millis(400), lines.next()).await;
    assert!(stalled.is_err(), "frozen tree kept producing output");

    group.resume().expect("resume");
    let resumed = tokio::time::timeout(Duration::from_secs(10), lines.next()).await;
    assert!(
        resumed.is_ok_and(|line| line.is_some()),
        "tree did not resume ticking"
    );
}

#[cfg(unix)]
#[test]
#[ignore = "creates an OS job/cgroup"]
fn signal_on_empty_group_is_ok() {
    // An empty group is trivially signalled/suspended/resumed — load-bearing
    // for callers that broadcast before (or after) any member is alive.
    let group = ProcessGroup::new().expect("create group");
    group.signal(Signal::Term).expect("signal on empty group");
    group.suspend().expect("suspend on empty group");
    group.resume().expect("resume on empty group");
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess; cross-checks soft_stop_scope against signal"]
async fn unix_soft_stop_scope_is_whole_tree_and_matches_signal() {
    use processkit::SoftStopScope;

    // The soft-stop capability report must agree with the real `signal` outcome
    // on the SAME group (the honesty contract: what it advertises is what a soft
    // stop actually reaches). On both Unix mechanisms — cgroup v2 and the POSIX
    // process-group fallback — a soft `Int`/`Term` reaches the whole tree and
    // never reports `Unsupported`, so the report is `WholeTree` and a real `Term`
    // returns `Ok`.
    let group = ProcessGroup::new().expect("create group");

    // Before any member: whole-tree capability, and an empty group accepts a soft
    // signal trivially.
    assert_eq!(
        group.soft_stop_scope(),
        SoftStopScope::WholeTree,
        "a Unix group always offers a whole-tree soft stop"
    );
    group
        .signal(Signal::Term)
        .expect("Term on an empty Unix group is Ok — matching the WholeTree report");

    // With a live child: still whole tree, and a real soft `Term` still succeeds,
    // so the report matches the observed signal outcome.
    let cmd = Command::new("sh").args(["-c", "while :; do sleep 0.1; done"]);
    let _run = group.start(&cmd).await.expect("start sleeper");
    assert_eq!(
        group.soft_stop_scope(),
        SoftStopScope::WholeTree,
        "the POSIX/cgroup soft stop reaches the whole tree with a live member too"
    );
    group
        .signal(Signal::Term)
        .expect("a real Term reaches the tree, matching the whole-tree report");
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a fork storm and broadcasts SIGKILL to the group"]
async fn unix_fork_storm_is_swept_by_group_broadcast() {
    // Best-effort boundary of the pgroup mechanism under a fork storm. A group
    // leader forks a dense burst of grandchildren — each inheriting the leader's
    // process group, none `setsid`-ing away — while we broadcast `Signal::Kill`.
    // `killpg` reaches the whole process group in one sweep (the documented
    // "SIGKILL … cannot miss a process forked mid-broadcast"), and any child
    // forked in the race window is caught by the next sweep, so the storm is
    // fully torn down — the only pgroup escape hatch is a member that `setsid`s
    // into its own session. We record the single-sweep catch count (best-effort,
    // not a strict 100% guarantee) and assert the group drains completely after
    // teardown. (Under the Linux cgroup mechanism the whole tree is contained via
    // `cgroup.kill`; the assertions hold there too.)
    let tmp = std::env::temp_dir();
    let dir = tmp.join(format!("processkit_fork_storm_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create storm dir");

    // The leader forks a grandchild every ~20ms; each records its OWN pid (`$$`
    // of its own `sh -c`) and then sleeps well past the test, so the burst runs
    // concurrently with the broadcast below.
    let script = r#"i=0; while [ "$i" -lt 40 ]; do sh -c 'echo live > "$PK_DIR/$$"; exec sleep 30' & i=$((i + 1)); sleep 0.02; done; wait"#;
    let group = ProcessGroup::new().expect("create group");
    let forker = group
        .start(&Command::new("sh").args(["-c", script]).env("PK_DIR", &dir))
        .await
        .expect("fork-storm leader spawns");

    // Count grandchildren currently registered *and* alive (files are named by
    // pid, so the filename is the pid to probe).
    let alive_registered = || -> usize {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return 0;
        };
        entries
            .flatten()
            .filter_map(|e| e.file_name().to_string_lossy().parse::<i32>().ok())
            // SAFETY: signal 0 is a sound liveness probe.
            .filter(|&pid| unsafe { libc::kill(pid, 0) } == 0)
            .count()
    };

    // Warm up until a real storm is running, so the broadcast races live forks.
    poll_until(
        Duration::from_secs(5),
        Duration::from_millis(20),
        "fork storm never ramped up",
        || alive_registered() >= 6,
    )
    .await;
    let before = alive_registered();
    assert!(
        before >= 4,
        "expected a live fork storm, saw {before} grandchildren"
    );

    // One broadcast sweep, mid-storm.
    group
        .signal(Signal::Kill)
        .expect("broadcast SIGKILL to the group");

    // The sweep must catch the bulk of the group. Poll (not a fixed sleep) so a
    // slow init-reap of the freshly-killed orphans doesn't count lingering
    // zombies as survivors: killpg is best-effort against the concurrent fork
    // race, but a whole-group SIGKILL leaves at most a handful forked inside the
    // syscall's race window — never more than half of a real burst.
    poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        "one whole-group sweep did not catch the bulk of the storm",
        || alive_registered() * 2 <= before,
    )
    .await;
    let survived_one_sweep = alive_registered();

    // Reap the leader (SIGKILL'd above) so the group's liveness probe is driven
    // purely by the grandchildren, then take a final sweep to catch any
    // race-window survivor.
    completes_within(Duration::from_secs(10), "leader reap", forker.wait())
        .await
        .expect("leader waits");
    group.kill_all().expect("final whole-tree sweep");

    // Load-bearing: the whole tracked group must drain. `members()` uses the
    // crate's own recycle-safe probe and reports empty only once the group is
    // genuinely gone — no grandchild permanently escaped the mechanism.
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "fork storm did not fully drain — a grandchild escaped the group",
        || group.members().is_ok_and(|m| m.is_empty()),
    )
    .await;

    eprintln!(
        "fork storm: {before} grandchildren alive before broadcast, \
         {survived_one_sweep} survived one sweep, group fully drained after teardown"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(windows)]
#[test]
#[ignore = "creates an OS job"]
fn windows_signal_non_kill_is_unsupported() {
    // Job Objects have no POSIX signals. `Int`/`Term` get a best-effort soft close
    // (console `CTRL_BREAK` + `WM_CLOSE` to windowed members), but this empty group
    // has neither a console leader nor a windowed member, so they too surface the
    // typed Unsupported error here — as does every other non-Kill signal
    // unconditionally. Never a silent no-op.
    let group = ProcessGroup::new().expect("create group");
    for sig in [Signal::Term, Signal::Hup, Signal::Other(9)] {
        let err = group
            .signal(sig)
            .expect_err("a non-Kill signal with no soft-close target must be rejected on Windows");
        assert!(
            matches!(err.reason(), processkit::ErrorReason::Unsupported { .. }),
            "expected ErrorReason::Unsupported for {sig:?}, got {err:?}"
        );
    }
}

#[cfg(windows)]
#[test]
#[ignore = "creates an OS job"]
fn windows_soft_stop_scope_on_empty_group_is_unsupported() {
    use processkit::SoftStopScope;

    // The soft-stop capability report agrees with the narrowed `signal` contract:
    // a group with neither a console-CTRL leader nor a windowed member can soft-stop
    // nothing, so the report is `Unsupported` and a real `signal(Term)` would return
    // `ErrorReason::Unsupported` — the caller can decide up front without firing (and
    // parsing back) the error. The probe is side-effect-free (no spawn, no signal).
    let group = ProcessGroup::new().expect("create group");
    assert_eq!(
        group.soft_stop_scope(),
        SoftStopScope::Unsupported,
        "an empty Windows group has no soft-stop target"
    );
    let err = group
        .signal(Signal::Term)
        .expect_err("no soft-close target on an empty Windows group");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Unsupported { .. }),
        "the report and the real signal outcome agree on Unsupported, got {err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real opt-in console child; probes soft-stop availability at the ProcessGroup API"]
async fn windows_soft_stop_scope_reports_opt_in_members_for_a_live_child() {
    use processkit::SoftStopScope;

    // End-to-end through the public API (including the `windows_graceful_ctrl_break`
    // → `SpawnOptions::windows_new_process_group` → recorded-leader wiring): a live
    // opt-in console child makes a soft stop available, so the side-effect-free
    // probe reports `OptInMembers` and a real `signal(Term)` then succeeds — the
    // report matches the observed outcome. `ping` ignores CTRL_BREAK (it installs
    // its own handler), so the child stays reapable for the explicit teardown.
    let group = ProcessGroup::new().expect("create group");
    let _run = group
        .start(
            &Command::new("ping")
                .args(["-n", "30", "127.0.0.1"])
                .windows_graceful_ctrl_break(),
        )
        .await
        .expect("start opt-in console child");

    assert_eq!(
        group.soft_stop_scope(),
        SoftStopScope::OptInMembers,
        "a live opt-in console child makes a soft stop available"
    );
    group
        .signal(Signal::Term)
        .expect("a real Term reaches the live opt-in leader the probe reported");

    group.kill_all().expect("tear the tree down");
}

#[cfg(all(windows, feature = "pty"))]
#[tokio::test]
#[ignore = "spawns a real opt-in ConPTY child; probes shared Job bookkeeping"]
async fn windows_conpty_opt_in_child_is_recorded_for_soft_stop() {
    use processkit::SoftStopScope;

    let group = ProcessGroup::new().expect("create group");
    let _run = group
        .start(
            &Command::new("ping")
                .args(["-n", "30", "127.0.0.1"])
                .windows_graceful_ctrl_break()
                .use_pty(),
        )
        .await
        .expect("start opt-in ConPTY child");

    assert_eq!(
        group.soft_stop_scope(),
        SoftStopScope::OptInMembers,
        "ConPTY spawn must register its console process-group leader on the Job"
    );
    group
        .signal(Signal::Term)
        .expect("the registered ConPTY leader must accept the advertised soft stop");
    group.kill_all().expect("tear the ConPTY tree down");
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess and kills it via Signal::Kill"]
async fn windows_signal_kill_kills_tree() {
    let group = ProcessGroup::new().expect("create group");
    let process = group.start(&sleeper()).await.expect("start sleeper");
    assert!(process.pid().is_some());

    group
        .signal(Signal::Kill)
        .expect("Signal::Kill maps to job terminate");

    // The ~30s sleeper waiting out promptly proves the whole tree was killed
    // (pid liveness can't be probed here: our own RunningProcess still holds the
    // child handle, which keeps the terminated process object around).
    completes_within(Duration::from_secs(5), "Signal::Kill reap", process.wait())
        .await
        .expect("wait");
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess and suspends/resumes its threads"]
async fn windows_suspend_resume_stalls_output() {
    use tokio_stream::StreamExt;

    let group = ProcessGroup::new().expect("create group");
    // ping prints one line per second — a slow ticker.
    let cmd = Command::new("ping").args(["-n", "30", "127.0.0.1"]);
    let mut process = group.start(&cmd).await.expect("start ping");
    let mut lines = process.stdout_lines().unwrap();

    tokio::time::timeout(Duration::from_secs(10), lines.next())
        .await
        .expect("first ping line in time")
        .expect("first ping line");
    group.suspend().expect("suspend");

    // Drain pre-freeze buffered lines, then require silence across what would
    // be two ticks.
    tokio::time::sleep(Duration::from_millis(200)).await;
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(100), lines.next()).await {}
    let stalled = tokio::time::timeout(Duration::from_secs(2), lines.next()).await;
    assert!(stalled.is_err(), "suspended tree kept producing output");

    group.resume().expect("resume");
    let resumed = tokio::time::timeout(Duration::from_secs(10), lines.next()).await;
    assert!(
        resumed.is_ok_and(|line| line.is_some()),
        "tree did not resume output"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess outside the group and adopts it"]
async fn adopt_brings_an_external_child_under_containment() {
    // Spawn OUTSIDE any processkit group, adopt, then prove the group's
    // teardown reaps it — the adopt() containment claim, end-to-end.
    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("ping");
        c.args(["-n", "30", "127.0.0.1"]);
        c
    } else {
        let mut c = tokio::process::Command::new("sleep");
        c.arg("30");
        c
    };
    cmd.stdout(std::process::Stdio::null());
    let mut child = cmd.spawn().expect("spawn external child");

    let group = ProcessGroup::new().expect("create group");
    group.adopt(&child).expect("adopt external child");
    group.kill_all().expect("hard-kill the adopted tree");

    // The adopted child must die promptly — well under its ~30s natural run.
    let _ = completes_within(Duration::from_secs(5), "adopted child reap", child.wait()).await;
}

#[tokio::test]
#[ignore = "spawns real subprocesses and lists the group's members"]
async fn members_lists_live_children() {
    let group = ProcessGroup::new().expect("create group");
    let _a = group.start(&sleeper()).await.expect("start first sleeper");
    let _b = group.start(&sleeper()).await.expect("start second sleeper");

    // Windows/cgroup list the whole tree (a started child may be a shell plus
    // its own child); the pgroup backends list one leader per started child.
    // Either way, two started children mean at least two live pids.
    let members = group.members().expect("members");
    assert!(members.len() >= 2, "members: {members:?}");
}

#[tokio::test]
#[ignore = "spawns real subprocesses and watches the member list shrink"]
async fn members_shrinks_when_a_child_dies() {
    let group = ProcessGroup::new().expect("create group");
    // Single-process sleepers, deliberately: the cmd-wrapped `sleeper()` is two
    // processes whose second member spawns asynchronously, so a `before`
    // snapshot can race it — and `start_kill` would hit only the wrapper,
    // leaving its orphan in the job and the count above the threshold forever
    // (seen on a cold CI runner).
    let _keep = group.start(&sleep_secs(30)).await.expect("start survivor");
    let mut dying = group.start(&sleep_secs(30)).await.expect("start victim");
    let before = group.members().expect("members").len();
    assert!(before >= 2, "expected at least two members, got {before}");

    dying.start_kill().expect("kill victim");
    // Reap it (wait consumes the handle) so the kill is visible everywhere —
    // an unreaped zombie still probes as alive on the pgroup backends.
    let _ = completes_within(Duration::from_secs(10), "victim reap", dying.wait()).await;

    poll_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        &format!("member count never dropped below {before}"),
        || group.members().expect("members").len() < before,
    )
    .await;
}

#[tokio::test]
#[ignore = "creates an OS job/cgroup"]
async fn members_on_empty_group_is_empty() {
    let group = ProcessGroup::new().expect("create group");
    let members = group.members().expect("members");
    assert!(members.is_empty(), "fresh group has members: {members:?}");
}

#[tokio::test]
#[ignore = "spawns a real subprocess and reads its enriched member snapshot"]
async fn members_info_enriches_a_live_child() {
    let group = ProcessGroup::new().expect("create group");
    let child = group.start(&sleeper()).await.expect("start sleeper");
    let child_pid = child.pid().expect("child pid");

    let infos = group.members_info().expect("members_info");
    assert!(!infos.is_empty(), "members_info empty for a live child");

    // The started child's real pid must be among the enriched records — proof the
    // snapshot reports genuine member pids (whole tree on Windows/cgroup, the group
    // leader on the pgroup backends, and the direct child is that leader).
    let mine = infos
        .iter()
        .find(|m| m.pid() == child_pid)
        .unwrap_or_else(|| panic!("child pid {child_pid} not in members_info {infos:?}"));

    // Every field this platform declares available (see `MemberInfo`'s matrix) must
    // actually be filled for that live member — not silently `None`.
    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    {
        assert!(
            mine.ppid().is_some(),
            "ppid should be reported here: {mine:?}"
        );
        assert!(
            mine.exe_name().is_some(),
            "exe_name should be reported here: {mine:?}"
        );
        assert!(
            mine.start_time().is_some(),
            "start_time should be reported here: {mine:?}"
        );
    }
    // On the bare BSDs the enriching fields are honestly `None`; the pid being
    // present is the whole guarantee there.
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    let _ = mine;
}

#[tokio::test]
#[ignore = "creates an OS job/cgroup"]
async fn members_info_on_empty_group_is_empty() {
    let group = ProcessGroup::new().expect("create group");
    let infos = group.members_info().expect("members_info");
    assert!(infos.is_empty(), "fresh group has members: {infos:?}");
}

// ── standalone process_info / process_is_alive (T-175) ───────────────────────

// A live child is recognised by the free-standing pid query — outside any group —
// with the same best-effort fields a group member carries, and its saved
// (pid, start-time) pair reports it alive.
#[tokio::test]
#[ignore = "spawns a real subprocess and queries its identity by pid"]
async fn process_info_identifies_a_live_child() {
    let group = ProcessGroup::new().expect("create group");
    let child = group.start(&sleeper()).await.expect("start sleeper");
    let pid = child.pid().expect("child pid");

    let info = processkit::process_info(pid)
        .expect("process_info must not error on a live child we own")
        .expect("a live child must be found by pid");
    assert_eq!(info.pid(), pid, "process_info reported the wrong pid");

    // Every field this platform declares available (see `MemberInfo`'s matrix) must
    // actually be filled for a live process — exactly as `members_info` fills them,
    // proof the standalone query reuses the same readers rather than a stub.
    #[cfg(any(windows, target_os = "linux", target_os = "macos"))]
    {
        assert!(
            info.ppid().is_some(),
            "ppid should be reported here: {info:?}"
        );
        assert!(
            info.exe_name().is_some(),
            "exe_name should be reported here: {info:?}"
        );
        assert!(
            info.start_time().is_some(),
            "start_time should be reported here: {info:?}"
        );
    }

    // The saved (pid, start-time) pair reports the same instance alive. On the bare
    // BSDs `start_time()` is `None`, so this degrades to bare-pid liveness — still
    // `true` for a live child.
    assert!(
        processkit::process_is_alive(pid, info.start_time())
            .expect("liveness query must not error on a live child"),
        "the live child must read as alive by its (pid, start-time) pair",
    );
}

// Models a **recycled pid** without waiting for a real recycle: the pid is a live
// process, but the saved start-time differs — as if a different process had
// reclaimed the number after the original exited. A recycle-aware liveness check
// must report the saved instance gone. Only meaningful where a start-time token is
// reported (Windows / Linux / macOS); the bare BSDs report none and degrade to
// number-only liveness by design.
#[cfg(any(windows, target_os = "linux", target_os = "macos"))]
#[tokio::test]
#[ignore = "spawns a real subprocess to model a recycled pid without waiting for a real recycle"]
async fn process_is_alive_rejects_a_recycled_number() {
    let group = ProcessGroup::new().expect("create group");
    let child = group.start(&sleeper()).await.expect("start sleeper");
    let pid = child.pid().expect("child pid");

    let start = processkit::process_info(pid)
        .expect("process_info")
        .and_then(|i| i.start_time())
        .expect("a start-time token is reported on this platform");

    // The genuine token: same instance, alive.
    assert!(
        processkit::process_is_alive(pid, Some(start)).expect("liveness with the real token"),
        "the real (pid, start-time) pair must read as alive",
    );

    // A stale token on the *same live pid* stands in for the number having been
    // recycled by a different process — the check must call the saved instance gone.
    let stale = start ^ 0x5A5A_5A5A;
    assert_ne!(
        stale, start,
        "the stale token must differ from the real one"
    );
    assert!(
        !processkit::process_is_alive(pid, Some(stale))
            .expect("liveness must not error on a live pid with a stale token"),
        "a mismatched start-time must read as gone — the number was recycled",
    );
}

// Once a child is torn down and reaped, the free-standing query reports its saved
// (pid, start-time) pair gone — the "after it exits, an honest no" half of the
// contract. Uses the same `OpenProcess`-by-pid liveness the group-teardown tests
// rely on, polling to let the async reap complete (and, on Windows, the process
// handle close).
#[tokio::test]
#[ignore = "spawns a real subprocess, tears it down, and confirms its pid reads as gone"]
async fn process_is_alive_reports_gone_after_teardown() {
    let group = ProcessGroup::new().expect("create group");
    let child = group.start(&sleeper()).await.expect("start sleeper");
    let pid = child.pid().expect("child pid");
    let start = processkit::process_info(pid)
        .expect("process_info")
        .and_then(|i| i.start_time());

    assert!(
        processkit::process_is_alive(pid, start).expect("liveness while running"),
        "the child must read alive before teardown",
    );

    group.kill_all().expect("terminate the tree");
    let _ = child.wait().await.expect("reap the killed child");
    drop(group);

    // The pid must now read as gone. Poll: on the pgroup backends the reap frees the
    // number a beat after the kill, and on Windows the last process handle closes as
    // the child and job handles drop.
    poll_until(
        Duration::from_secs(10),
        Duration::from_millis(100),
        "the reaped child's pid still read as alive",
        || {
            !processkit::process_is_alive(pid, start)
                .expect("liveness after teardown must not error")
        },
    )
    .await;

    // A fresh lookup is likewise a clean negative (or, if the number was already
    // recycled by an unrelated process, a *different* instance — never the saved
    // one, which `process_is_alive` above already proved gone).
    match processkit::process_info(pid).expect("process_info after teardown must not error") {
        None => {}
        Some(info) => assert_ne!(
            info.start_time(),
            start,
            "the saved instance is gone; any Some here is a recycled stranger",
        ),
    }
}

// A pid past any valid range names no process: a clean `Ok(None)`, never an error
// or a panic, and never alive. The deterministic, subprocess-free negative.
#[tokio::test]
#[ignore = "queries a pid past any valid range"]
async fn process_info_on_a_nonexistent_pid_is_a_clean_none() {
    // ~2e9 is far above any OS's pid_max yet still fits `pid_t` (`i32`), so it
    // exercises each backend's real "not found" path rather than a range guard.
    let bogus = 2_000_000_000u32;
    assert_eq!(
        processkit::process_info(bogus).expect("a nonexistent pid is not an error"),
        None,
        "a nonexistent pid must be a clean None, not Some/Err",
    );
    assert!(
        !processkit::process_is_alive(bogus, None).expect("liveness on a gone pid"),
        "a nonexistent pid must read as not alive",
    );
    assert!(
        !processkit::process_is_alive(bogus, Some(12_345)).expect("liveness on a gone pid"),
        "a token cannot resurrect a nonexistent pid",
    );
}

// A foreign **privileged** process must never be a false "gone": the documented and
// verified behaviour is `Ok(Some)` when the caller may query it, else a permission
// `Err` — but never `Ok(None)` (which would let a caller conclude a live process is
// dead). Robust to whether the runner is elevated.
#[tokio::test]
#[ignore = "queries a known privileged system process by pid"]
async fn process_info_on_a_privileged_process_is_never_a_false_gone() {
    #[cfg(windows)]
    let pid = 4; // the Windows `System` process — normally not query-openable.
    #[cfg(not(windows))]
    let pid = 1; // pid 1: init/systemd (Linux), launchd (macOS), the container init.

    match processkit::process_info(pid) {
        Ok(Some(info)) => assert_eq!(info.pid(), pid, "reported the wrong pid"),
        Ok(None) => panic!(
            "a live privileged process (pid {pid}) must never read as a nonexistent pid — \
             'can't look' is not 'dead'"
        ),
        // A permission error is the honest "not allowed to look" — expected on the
        // targets/rights where the query is denied, and explicitly not a false gone.
        Err(_) => {}
    }
}

#[tokio::test]
#[ignore = "spawns a short subprocess and adopts it after reaping"]
async fn adopt_of_a_reaped_child_errors_instead_of_tracking_nothing() {
    let group = ProcessGroup::new().expect("create group");

    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("cmd");
        c.args(["/c", "exit", "0"]);
        c
    } else {
        let mut c = tokio::process::Command::new("sh");
        c.args(["-c", "exit 0"]);
        c
    };
    let mut child = cmd.spawn().expect("spawn short child");
    let _ = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("short child exits");

    // A reaped child has no pid/handle left — adopting it must say so loudly
    // rather than silently tracking nothing.
    let err = group
        .adopt(&child)
        .expect_err("adopting a reaped child must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Io(_)),
        "expected the no-pid Io error, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a child, kills it UNREAPED, then adopts the zombie"]
async fn adopt_of_an_exited_unreaped_child_is_ok() {
    // E21: a child that has EXITED but not yet been reaped (a zombie — its
    // handle/pid is still valid while the process is dead, distinct from the
    // reaped case above) has nothing to contain, so `adopt` returns Ok on every
    // backend (cgroup/pgroup `ESRCH` → Ok, Windows `GetExitCodeProcess` → Ok),
    // rather than surfacing the raw backend failure.
    let group = ProcessGroup::new().expect("create group");

    // A long-lived child we control: `start_kill` terminates it WITHOUT reaping,
    // so it is *deterministically* a dead-but-unreaped zombie at adopt time — no
    // reliance on natural-exit timing (a too-short sleep would adopt a still-live
    // child, whose assign succeeds, and never exercise the exited path).
    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("ping");
        c.args(["-n", "60", "127.0.0.1"]);
        c
    } else {
        let mut c = tokio::process::Command::new("sleep");
        c.arg("60");
        c
    };
    let mut child = cmd.spawn().expect("spawn long-lived child");

    child
        .start_kill()
        .expect("kill the child without reaping it");
    // The kill is prompt (SIGKILL / TerminateProcess); give it a moment to become
    // a zombie. We never `wait`, so it stays unreaped (handle/pid still valid).
    tokio::time::sleep(Duration::from_millis(500)).await;

    group
        .adopt(&child)
        .expect("adopting an exited-but-unreaped (zombie) child must be a no-op Ok");

    let _ = child.wait().await;
    drop(group);
}

#[tokio::test]
#[ignore = "creates an OS job/cgroup"]
async fn empty_group_accepts_lifecycle_calls() {
    let group = ProcessGroup::new().expect("create group");

    // Signalling, freezing, and thawing nobody must succeed trivially…
    group.signal(Signal::Kill).expect("Kill on an empty group");
    if cfg!(windows) {
        // …except `Term`/`Int` on Windows: they would soft-close a console or
        // windowed member (`CTRL_BREAK` + `WM_CLOSE`), but an EMPTY group has
        // neither, so they are typed Unsupported here — a Job Object has no POSIX
        // signal to fall back on.
        let err = group
            .signal(Signal::Term)
            .expect_err("Term on an empty Windows group has no soft-close target");
        assert!(
            matches!(err.reason(), processkit::ErrorReason::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    } else {
        group.signal(Signal::Term).expect("Term on an empty group");
    }
    group.suspend().expect("suspend an empty group");
    group.resume().expect("resume an empty group");

    #[cfg(feature = "stats")]
    {
        let stats = group.stats().expect("stats on an empty group");
        assert_eq!(stats.active_process_count, 0);
    }
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "spawns a real subprocess and nests suspend/resume"]
async fn windows_nested_suspend_needs_matching_resumes() {
    use tokio_stream::StreamExt;

    // Documented Windows semantics: suspend/resume are per-thread *counts*, so
    // two suspends need two resumes. A bare ping prints ~one line per second —
    // line flow is the freeze probe.
    let group = ProcessGroup::new().expect("create group");
    let mut run = group
        .start(&Command::new("ping").args(["-n", "31", "127.0.0.1"]))
        .await
        .expect("start ticker");
    let mut lines = run.stdout_lines().unwrap();
    tokio::time::timeout(Duration::from_secs(15), lines.next())
        .await
        .expect("ticker prints")
        .expect("first line");

    group.suspend().expect("suspend #1");
    group.suspend().expect("suspend #2");
    group.resume().expect("resume #1 of 2");

    // Drain lines emitted before the freeze landed; 2s of silence (double the
    // ticker period) means the tree is genuinely frozen.
    loop {
        match tokio::time::timeout(Duration::from_secs(2), lines.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("ticker exited while suspended"),
            Err(_) => break,
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(3), lines.next())
            .await
            .is_err(),
        "one resume must not thaw two suspends"
    );

    group.resume().expect("resume #2 of 2");
    let line = tokio::time::timeout(Duration::from_secs(15), lines.next())
        .await
        .expect("a balanced resume thaws the tree");
    assert!(line.is_some(), "ticker resumed output");
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "adopts a real subprocess into a suspended cgroup"]
async fn linux_cgroup_adopt_into_suspended_group_freezes_the_child() {
    use tokio::io::AsyncBufReadExt;

    // Documented cgroup divergence: the freeze is *group state*, so a child
    // joining while the group is suspended freezes on attach. (Windows/pgroup
    // freeze only the members present at the call.) The join is exercised via
    // `adopt` — the parent writes the pid itself. `group.start()` would test
    // the same kernel behavior but can BLOCK here: the pre-exec cgroup join
    // freezes the child before the spawn handshake completes (see the
    // `suspend` rustdoc), which would hang this very test.
    let group = ProcessGroup::new().expect("create group");
    if !matches!(group.mechanism(), Mechanism::CgroupV2) {
        eprintln!("skipping: needs the cgroup mechanism");
        return;
    }

    // A free-running ticker, spawned OUTSIDE the group.
    let mut ticker = tokio::process::Command::new("sh")
        .args(["-c", "while :; do echo tick; sleep 0.25; done"])
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn ticker");
    let stdout = ticker.stdout.take().expect("ticker stdout");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("ticker prints")
        .expect("read line")
        .expect("a tick before adoption");

    group.suspend().expect("suspend the empty group");
    group
        .adopt(&ticker)
        .expect("adopt the ticker into the frozen cgroup");

    // Drain ticks emitted before the freeze landed; 1s of silence (4× the
    // tick period) means the child is genuinely frozen…
    loop {
        match tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await {
            Ok(Ok(Some(_))) => continue,
            Ok(_) => panic!("ticker exited while frozen"),
            Err(_) => break,
        }
    }
    // …and stays frozen.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .is_err(),
        "a child adopted into a suspended cgroup must freeze on attach"
    );

    group.resume().expect("resume");
    let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("thawed ticker resumes output")
        .expect("read line");
    assert_eq!(line.as_deref(), Some("tick"));

    let _ = ticker.kill().await;
}
