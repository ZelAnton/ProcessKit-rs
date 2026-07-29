//! One-shot capture verbs: output_string/bytes, run, stdin, timeouts, probe,
//! first_line, and the top-level free functions.

use std::time::{Duration, Instant};

use processkit::Command;

use crate::common::*;

#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn missing_working_directory_errors_clearly() {
    // A cwd that doesn't exist must surface as a clear error that names the
    // working directory — not the opaque ENOENT that looks like the program is
    // missing. The check fires before any child is spawned. D11: a bad cwd is
    // NOT a missing program, so `is_not_found()` must be false — the
    // "not installed?" hint would mislead.
    let err = Command::new("echo")
        .arg("hi")
        .current_dir("does-not-exist-processkit-xyz")
        .output_string()
        .await
        .expect_err("a missing cwd must error");
    assert!(
        !err.is_not_found(),
        "a missing cwd is not a missing program: {err:?}"
    );
    assert!(
        format!("{err}").contains("working directory does not exist"),
        "message should name the cwd: {err}"
    );
}

#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn working_directory_that_is_a_file_errors_as_not_a_directory() {
    // A cwd that exists but is a regular file is reported distinctly — it is
    // *found*, just not a directory, so `is_not_found()` must be false. (Cargo.toml
    // exists at the package root, which is the test process's working directory.)
    let err = Command::new("echo")
        .arg("hi")
        .current_dir("Cargo.toml")
        .output_string()
        .await
        .expect_err("a file as cwd must error");
    assert!(!err.is_not_found(), "a file is found, not missing: {err:?}");
    assert!(
        format!("{err}").contains("is not a directory"),
        "message should say not-a-directory: {err}"
    );
}

#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn missing_program_surfaces_not_found_with_searched_path() {
    // A bare program name that isn't on PATH must produce ErrorReason::NotFound
    // with a message that names the searched directories — not the opaque
    // OS ENOENT that would otherwise be indistinguishable from a missing cwd.
    let err = Command::new("processkit-definitely-not-installed-424242")
        .output_string()
        .await
        .expect_err("an unknown program must error");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::NotFound { .. }),
        "expected ErrorReason::NotFound, got {err:?}"
    );
    assert!(err.is_not_found(), "is_not_found() must be true: {err:?}");
    let msg = err.to_string();
    assert!(
        msg.contains("not found on PATH"),
        "message should name the PATH search: {msg}"
    );
}

#[tokio::test]
#[ignore = "exercises the real spawn path with a relocated child PATH"]
async fn missing_program_reports_the_effective_child_path() {
    let path_dir = tempfile::tempdir().expect("temp PATH dir");
    let child_path = std::env::join_paths([path_dir.path()]).expect("single PATH entry");
    let err = Command::new("processkit-definitely-not-installed-child-path")
        .env("PATH", &child_path)
        .output_string()
        .await
        .expect_err("an unknown program must error");

    match err.into_reason() {
        processkit::ErrorReason::NotFound { searched, .. } => assert_eq!(
            searched,
            Some(child_path.to_string_lossy().into_owned()),
            "launch diagnostics must name the same effective child PATH as preflight"
        ),
        other => panic!("expected ErrorReason::NotFound, got {other:?}"),
    }
}

#[cfg(unix)]
fn broken_interpreter_command() -> (tempfile::TempDir, Command) {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("temp PATH dir");
    let name = "processkit-present-with-missing-interpreter";
    let program = dir.path().join(name);
    std::fs::write(&program, b"#!/processkit/definitely/missing/interpreter\n")
        .expect("write executable stub");
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755))
        .expect("mark stub executable");
    let child_path = std::env::join_paths([dir.path()]).expect("single PATH entry");
    (dir, Command::new(name).env("PATH", child_path))
}

#[cfg(unix)]
fn assert_located_spawn_error(err: processkit::Error) {
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Spawn { .. }),
        "a program found on the effective child PATH must remain Spawn, not NotFound: {err:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "exercises pipe and detached real-spawn NotFound enrichment"]
async fn relocated_path_found_program_is_spawn_error_for_pipe_and_detached_launches() {
    let (_dir, command) = broken_interpreter_command();
    let err = command
        .output_string()
        .await
        .expect_err("the missing interpreter prevents exec");
    assert_located_spawn_error(err);

    let err = command
        .spawn_detached()
        .expect_err("the missing interpreter prevents detached exec");
    assert_located_spawn_error(err);
}

#[cfg(all(unix, feature = "pty"))]
#[tokio::test]
#[ignore = "exercises PTY real-spawn NotFound enrichment"]
async fn relocated_path_found_program_is_spawn_error_for_pty_launch() {
    let (_dir, command) = broken_interpreter_command();
    let err = command
        .use_pty()
        .output_string()
        .await
        .expect_err("the missing interpreter prevents PTY exec");
    assert_located_spawn_error(err);
}

// Regression: a bare program name that the OS resolves by a route we don't model
// must NOT be falsely rejected as `NotFound`. On Windows std also searches the
// *application directory* (the running exe's dir), not just PATH — a helper
// shipped beside the binary spawns fine. An earlier PATH-only pre-check rejected
// it before ever attempting the spawn; the rich error is now enriched from the
// OS's actual not-found, so the OS stays the source of truth. (Unix `execvp`
// searches PATH only, so there is no equivalent app-dir case to regress there.)
#[cfg(windows)]
#[tokio::test]
#[ignore = "exercises the real spawn path; writes a temp exe beside the test binary"]
async fn bare_name_in_the_application_directory_is_not_falsely_not_found() {
    // Place a uniquely-named copy of a real system exe next to THIS test binary
    // (the application directory) — deliberately *not* on PATH and *not* in cwd.
    let app_dir = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("exe has a parent dir")
        .to_path_buf();
    let unique = "pk_appdir_regression_probe_77321";
    let dst = app_dir.join(format!("{unique}.exe"));
    std::fs::copy(r"C:\Windows\System32\where.exe", &dst).expect("copy where.exe beside test exe");

    // `where /?` prints help and exits 0 — so a clean success proves the OS both
    // *found* (via the app dir) and *ran* the bare-named program.
    let result = Command::new(unique).arg("/?").output_string().await;
    let _ = std::fs::remove_file(&dst); // best-effort cleanup before asserting

    let result = result.expect("a program in the application dir must spawn, not error");
    assert!(
        result.is_success(),
        "`where /?` exits 0; got {:?}",
        result.code()
    );
}

// T-054: `Command::prefer_local` must be checked BEFORE the system PATH, so a
// program that exists only in a prefer_local directory (deliberately absent
// from PATH) must still be found and spawned.
#[tokio::test]
#[ignore = "exercises the real spawn path; writes a temp executable"]
async fn prefer_local_resolves_the_program_before_the_system_path() {
    let dir = tempfile::tempdir().expect("temp dir");
    let unique = "pk_prefer_local_probe_881234";

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.path().join(unique);
        std::fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("write stub");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod +x");
    }
    #[cfg(windows)]
    {
        let path = dir.path().join(format!("{unique}.exe"));
        std::fs::copy(r"C:\Windows\System32\where.exe", &path).expect("copy where.exe");
    }

    let cmd = Command::new(unique).prefer_local(dir.path());
    let cmd = if cfg!(windows) { cmd.arg("/?") } else { cmd };
    let result = cmd
        .output_string()
        .await
        .expect("prefer_local must resolve the program even though it is not on PATH");
    assert!(result.is_success(), "got {:?}", result.code());
}

// T-054: a `prefer_local` directory that does NOT hold the program must not
// break the existing PATH fallback (the program still resolves normally if
// it happens to be on PATH).
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn prefer_local_miss_falls_back_to_the_system_path() {
    let dir = tempfile::tempdir().expect("temp dir"); // deliberately empty

    let program = if cfg!(windows) { "cmd" } else { "sh" };
    let mut cmd = Command::new(program).prefer_local(dir.path());
    cmd = if cfg!(windows) {
        cmd.args(["/c", "echo hi"])
    } else {
        cmd.args(["-c", "echo hi"])
    };
    let result = cmd
        .output_string()
        .await
        .expect("a prefer_local miss must still fall back to the system PATH");
    assert!(result.is_success(), "got {:?}", result.code());
    assert!(
        result.stdout().contains("hi"),
        "stdout: {:?}",
        result.stdout()
    );
}

// T-110: Unix environment keys are case-sensitive, so a lowercase `path`
// must not suppress the process-PATH diagnostic for a missing program.
#[cfg(unix)]
#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn lowercase_path_env_keeps_the_not_found_searched_path() {
    let system_path = std::env::var_os("PATH")
        .expect("test environment must provide PATH")
        .to_string_lossy()
        .into_owned();
    let err = Command::new("processkit-definitely-not-installed-t110")
        .env("path", "/opt/bin")
        .output_string()
        .await
        .expect_err("an unknown program must error");

    match err.into_reason() {
        processkit::ErrorReason::NotFound { searched, .. } => assert_eq!(
            searched,
            Some(system_path),
            "lowercase `path` must not replace the process PATH on Unix"
        ),
        other => panic!("expected ErrorReason::NotFound, got {other:?}"),
    }
}

// T-054: when resolution fails everywhere, `ErrorReason::NotFound`'s `searched`
// diagnostic must include the `prefer_local` directories too — not just PATH
// — so the caller can tell they were checked.
#[tokio::test]
#[ignore = "exercises the real spawn path"]
async fn missing_program_not_found_searched_includes_prefer_local_dirs() {
    let dir = tempfile::tempdir().expect("temp dir"); // deliberately empty

    let err = Command::new("processkit-definitely-not-installed-565656")
        .prefer_local(dir.path())
        .output_string()
        .await
        .expect_err("an unknown program must error");
    match err.into_reason() {
        processkit::ErrorReason::NotFound { searched, .. } => {
            let searched = searched.expect("a bare-name lookup must report searched dirs");
            assert!(
                searched.contains(&dir.path().to_string_lossy().into_owned()),
                "searched must include the prefer_local directory: {searched}"
            );
        }
        other => panic!("expected ErrorReason::NotFound, got {other:?}"),
    }
}

// R-01 (secondary observation): `prefer_local` resolution is parent-side
// (plain filesystem probes under caller-named directories) and independent
// of the child's own environment, so `ErrorReason::NotFound`'s `searched` must
// still name the `prefer_local` directories even when the command also
// customizes the child's `PATH` (here via `inherit_env`, which clears the
// inherited set) — only the process-`PATH` portion of the diagnostic is
// unsafe to report in that case (it wouldn't match the child's actual PATH),
// not the `prefer_local` portion, which never depended on it.
#[tokio::test]
#[ignore = "exercises the real spawn path"]
async fn missing_program_not_found_searched_includes_prefer_local_dirs_even_with_a_customized_path()
{
    let dir = tempfile::tempdir().expect("temp dir"); // deliberately empty

    let err = Command::new("processkit-definitely-not-installed-565656")
        .prefer_local(dir.path())
        .inherit_env(["HOME"]) // customizes_path(): true
        .output_string()
        .await
        .expect_err("an unknown program must error");
    match err.into_reason() {
        processkit::ErrorReason::NotFound { searched, .. } => {
            let searched = searched.expect(
                "a prefer_local directory was probed even though PATH itself is customized",
            );
            assert_eq!(
                searched,
                dir.path().to_string_lossy().into_owned(),
                "searched must be exactly the prefer_local directory — no process PATH \
                 entries, since those don't apply to the customized child env"
            );
        }
        other => panic!("expected ErrorReason::NotFound, got {other:?}"),
    }
}

// T-101: the spawn-free preflight (`which` / `Command::resolve_program` /
// `CliClient::resolve_program`) must agree with the ACTUAL launch on the same
// inputs — a preflight that "finds" a program the spawn can't locate (or the
// reverse) would lie about availability. Covers a bare name on PATH, a
// `prefer_local` hit (PATHEXT on Windows / the execute bit on Unix through the
// same resolution), a non-executable file (found by neither), and a genuinely
// missing program (NotFound from both).
#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group)"]
async fn preflight_resolution_agrees_with_the_actual_spawn() {
    // Whether a real spawn LOCATED the program: `Ok`, or any error that isn't
    // `NotFound` (e.g. a non-zero exit), means the launch found and ran it;
    // only `ErrorReason::NotFound` means it couldn't be located.
    async fn spawn_located(cmd: Command) -> bool {
        match cmd.output_string().await {
            Ok(_) => true,
            Err(e) => !e.is_not_found(),
        }
    }

    // (1) A bare name that IS on PATH (`cargo`, present in this workspace):
    // `which` resolves it to an existing absolute path, and the spawn locates it.
    let resolved = processkit::which("cargo").expect("cargo resolves on PATH");
    assert!(
        resolved.is_absolute() && resolved.exists(),
        "which must resolve to an existing absolute path: {resolved:?}"
    );
    assert!(
        spawn_located(Command::new("cargo").arg("--version")).await,
        "the spawn must also locate cargo"
    );

    // (2) A genuinely missing bare name: preflight AND spawn must both report
    // NotFound (never a false "found").
    let missing = "pk-preflight-agree-absent-9x8y7";
    let preflight_missing = Command::new(missing).resolve_program();
    assert!(
        preflight_missing
            .as_ref()
            .is_err_and(processkit::Error::is_not_found),
        "preflight must report NotFound: {preflight_missing:?}"
    );
    assert!(
        !spawn_located(Command::new(missing)).await,
        "the spawn must also report NotFound for the same missing program"
    );

    // (3) A program present ONLY in a `prefer_local` directory (not on PATH):
    // preflight resolves it to a path under that directory, and the spawn runs
    // it — exercising PATHEXT (Windows) / the execute bit (Unix) via the SAME
    // resolution both paths share.
    let dir = tempfile::tempdir().expect("temp dir");
    let unique = "pk_preflight_prefer_local_stub";
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.path().join(unique);
        std::fs::write(&p, b"#!/bin/sh\nexit 0\n").expect("write stub");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).expect("chmod +x");
    }
    #[cfg(windows)]
    {
        std::fs::copy(
            r"C:\Windows\System32\where.exe",
            dir.path().join(format!("{unique}.exe")),
        )
        .expect("copy where.exe");
    }
    let make = || {
        let c = Command::new(unique).prefer_local(dir.path());
        if cfg!(windows) { c.arg("/?") } else { c }
    };
    let resolved = make()
        .resolve_program()
        .expect("prefer_local must resolve without spawning");
    assert!(
        resolved.starts_with(dir.path()),
        "resolved path must live under the prefer_local dir: {resolved:?}"
    );
    assert!(
        spawn_located(make()).await,
        "the spawn must locate the same prefer_local program"
    );

    // (4) A file in a `prefer_local` directory that is NOT directly executable
    // (no execute bit on Unix / no PATHEXT-recognized extension on Windows) must
    // be located by NEITHER: preflight falls through to PATH (a miss) and so
    // does the spawn — they agree it is unavailable.
    let noexec = tempfile::tempdir().expect("temp dir");
    let noexec_name = "pk_preflight_noexec_9182";
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = noexec.path().join(noexec_name);
        std::fs::write(&p, b"#!/bin/sh\nexit 0\n").expect("write");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).expect("chmod -x");
    }
    #[cfg(windows)]
    {
        std::fs::write(noexec.path().join(noexec_name), b"not an exe").expect("write");
    }
    let noexec_cmd = || Command::new(noexec_name).prefer_local(noexec.path());
    let preflight_noexec = noexec_cmd().resolve_program();
    assert!(
        preflight_noexec
            .as_ref()
            .is_err_and(processkit::Error::is_not_found),
        "a non-executable prefer_local file must not resolve: {preflight_noexec:?}"
    );
    assert!(
        !spawn_located(noexec_cmd()).await,
        "the spawn must likewise not locate a non-executable prefer_local file"
    );
}

// T-125: the parity above (case 1) covers a bare name that resolves through a
// `.exe` on PATH — the case the OS's own `.exe`-only bare-name search already
// agrees with. This closes the gap the earlier test deliberately side-stepped:
// a bare name that exists on PATH ONLY via a non-`.exe` PATHEXT extension (the
// typical `yarn.cmd`/`npx.cmd` npm/scoop shim). Preflight resolves it (its
// PATHEXT model finds the `.cmd`), and — now that `build_tokio` substitutes the
// resolved absolute path for such a match — the ACTUAL spawn succeeds too,
// instead of failing (before the fix the OS's bare-name search appended only
// `.exe`, missed the `.cmd`, and the launch raised `ErrorReason::Spawn`). A plain
// `spawn_located` check wouldn't prove the fix — the pre-fix failure was a
// non-`NotFound` `Spawn` error, which `spawn_located` counts as "located" — so
// this asserts a genuinely successful run.
#[cfg(windows)]
#[tokio::test]
#[ignore = "exercises the real spawn path (creates a process group); writes a temp .cmd"]
async fn preflight_and_spawn_agree_on_a_non_exe_pathext_program_on_path() {
    // A directory holding ONLY `<unique>.cmd` — no `.exe` — prepended to the
    // command's `PATH`. `env("PATH", …)` relocates the child `PATH`, so both the
    // preflight and `build_tokio` resolve against this exact list (its effective
    // child `PATH`), keeping them in lockstep.
    let dir = tempfile::tempdir().expect("temp dir");
    let unique = "pk_pathext_cmd_shim_t125";
    let cmd_path = dir.path().join(format!("{unique}.cmd"));
    // A trivial batch that exits cleanly. `@echo off` keeps stdout quiet; the
    // exit code (0) is what `is_success()` checks.
    std::fs::write(&cmd_path, "@echo off\r\nexit /b 0\r\n").expect("write .cmd shim");

    let base_path = std::env::var_os("PATH").unwrap_or_default();
    let mut new_path = std::ffi::OsString::from(dir.path());
    new_path.push(";");
    new_path.push(&base_path);

    // (a) Preflight resolves the bare name to the `.cmd` under our directory —
    // its PATHEXT model finds a non-`.exe` match the OS's own search would not.
    let resolved = Command::new(unique)
        .env("PATH", &new_path)
        .resolve_program()
        .expect("a bare name present on PATH only as a .cmd must resolve via PATHEXT");
    assert!(
        resolved.starts_with(dir.path()),
        "resolved path must live under our PATH directory: {resolved:?}"
    );
    assert!(
        resolved
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("cmd")),
        "the match must be the non-.exe PATHEXT extension (.cmd): {resolved:?}"
    );

    // (b) The ACTUAL launch now spawns it successfully — not the pre-fix
    // `ErrorReason::Spawn` from the OS's `.exe`-only bare-name search missing the
    // `.cmd`. `.expect(...)` here is the load-bearing assertion: it fails on the
    // pre-fix behavior and passes only once the resolved absolute path is
    // substituted at spawn.
    let result = Command::new(unique)
        .env("PATH", &new_path)
        .output_string()
        .await
        .expect("a bare name resolvable only via a non-.exe PATHEXT match must now spawn");
    assert!(
        result.is_success(),
        "the resolved .cmd must run and exit 0; got {:?}",
        result.code()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn output_string_captures_stdout() {
    let result = two_line_echo().output_string().await.expect("run echo");
    assert!(result.is_success(), "exit was {:?}", result.code());
    assert!(
        result.stdout().contains("first"),
        "stdout: {:?}",
        result.stdout()
    );
    assert!(
        result.stdout().contains("second"),
        "stdout: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn command_checked_and_run_unit_verbs() {
    // Command-level `checked()` returns the full success-checked result (untrimmed
    // stdout); `run_unit()` requires an accepted exit and discards the output —
    // the same verbs the runner/client families carry (holistic API consistency).
    let result = two_line_echo()
        .checked()
        .await
        .expect("checked on a zero exit");
    assert!(result.is_success(), "exit was {:?}", result.code());
    assert!(
        result.stdout().contains("first"),
        "stdout: {:?}",
        result.stdout()
    );

    two_line_echo()
        .run_unit()
        .await
        .expect("run_unit ok on a zero exit");
}

// StdioMode::Null on stdout: D5 makes the bulk capture verbs ERROR (there is no
// pipe to read, so returning silently-empty output was a footgun). To run a
// command with a suppressed stdout, use a discard verb (`wait`), which captures
// nothing by design.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_null_makes_capture_verbs_error_but_discard_verbs_run() {
    // D5: a capture verb on a non-piped stdout errors loudly.
    let err = two_line_echo()
        .stdout(processkit::StdioMode::Null)
        .output_string()
        .await
        .expect_err("output_string on a non-piped stdout must error (D5)");
    match err.into_reason() {
        processkit::ErrorReason::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }

    // A discard verb still runs the command to completion (nothing to capture is
    // fine there) — the exit code is real.
    let outcome = two_line_echo()
        .stdout(processkit::StdioMode::Null)
        .start()
        .await
        .expect("start")
        .wait()
        .await
        .expect("wait() runs a stdout(Null) command fine");
    assert_eq!(
        outcome,
        processkit::Outcome::Exited(0),
        "the command still ran"
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses that write directly to temp files"]
async fn file_redirection_truncates_or_appends_without_a_stdout_pipe() {
    let dir = tempfile::tempdir().expect("temp dir");
    let stdout_path = dir.path().join("service.log");
    std::fs::write(&stdout_path, "stale\n").expect("seed stale log");

    let first = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo first"])
    } else {
        Command::new("sh").args(["-c", "printf 'first\\n'"])
    };
    first
        .stdout_file(&stdout_path)
        .start()
        .await
        .expect("start with redirected stdout")
        .wait()
        .await
        .expect("first child exits");
    let first_log = std::fs::read_to_string(&stdout_path).expect("read truncated log");
    assert!(first_log.contains("first"), "log: {first_log:?}");
    assert!(
        !first_log.contains("stale"),
        "log was not truncated: {first_log:?}"
    );

    let second = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo second"])
    } else {
        Command::new("sh").args(["-c", "printf 'second\\n'"])
    };
    second
        .stdout_file_append(&stdout_path)
        .start()
        .await
        .expect("start with appended stdout")
        .wait()
        .await
        .expect("second child exits");
    let appended_log = std::fs::read_to_string(&stdout_path).expect("read appended log");
    assert!(appended_log.contains("first") && appended_log.contains("second"));

    let stderr_path = dir.path().join("service.err");
    let stderr_writer = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo warning 1>&2"])
    } else {
        Command::new("sh").args(["-c", "printf 'warning\\n' >&2"])
    };
    stderr_writer
        .stderr_file(&stderr_path)
        .start()
        .await
        .expect("start with redirected stderr")
        .wait()
        .await
        .expect("stderr writer exits");
    assert!(
        std::fs::read_to_string(stderr_path)
            .expect("read stderr log")
            .contains("warning")
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess to verify the file-redirect capture gate"]
async fn file_redirect_rejects_capture_and_streaming_verbs() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("out.log");

    let err = two_line_echo()
        .stdout_file(&path)
        .output_string()
        .await
        .expect_err("a file is not a capture pipe");
    match err.into_reason() {
        processkit::ErrorReason::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }

    let mut run = two_line_echo()
        .stdout_file(&path)
        .start()
        .await
        .expect("start redirected child");
    assert!(
        run.stdout_lines().is_err(),
        "stdout_lines must reject a file redirect"
    );
    run.wait().await.expect("redirected child still exits");
}

#[derive(Clone)]
struct SharedSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl tokio::io::AsyncWrite for SharedSink {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.lock().expect("sink mutex").extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

// A BufWriter keeps this data out of SharedSink until the pump explicitly
// flushes the tee; dropping a tokio BufWriter cannot perform an async flush.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_buffered_tee_flushes_while_capturing() {
    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let result = two_line_echo()
        .stdout_tee(tokio::io::BufWriter::with_capacity(4096, sink.clone()))
        .output_string()
        .await
        .expect("a stdout_tee run completes");

    // Capture still happens alongside the tee.
    assert!(
        result.stdout().contains("first") && result.stdout().contains("second"),
        "capture must still see both lines: {:?}",
        result.stdout()
    );
    // The tee received the same lines (each decoded line + '\n').
    let teed = String::from_utf8(sink.0.lock().expect("sink mutex").clone()).expect("tee is utf-8");
    assert!(
        teed.contains("first") && teed.contains("second"),
        "tee sink must receive both lines: {teed:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stderr_buffered_tee_flushes_while_capturing() {
    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let command = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo err 1>&2"])
    } else {
        Command::new("sh").args(["-c", "echo err 1>&2"])
    };
    let result = command
        .stderr_tee(tokio::io::BufWriter::with_capacity(4096, sink.clone()))
        .output_string()
        .await
        .expect("a stderr_tee run completes");

    assert!(
        result.stderr().contains("err"),
        "capture must still see stderr: {:?}",
        result.stderr()
    );
    let teed = String::from_utf8(sink.0.lock().expect("sink mutex").clone()).expect("tee is utf-8");
    assert!(
        teed.contains("err"),
        "tee sink must receive stderr: {teed:?}"
    );
}

// --- Raw byte tee (T-170) ------------------------------------------------

// End to end, the raw tee must reproduce the child's *exact* stdout bytes — the
// same bytes `output_bytes` (the raw-capture path) returns — whatever line
// endings the platform's `echo` emits. Cross-platform, proving the raw tee is
// wired through the builder and the capture pump.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_raw_tee_reproduces_the_childs_exact_bytes() {
    let expected = two_line_echo()
        .output_bytes()
        .await
        .expect("output_bytes baseline")
        .stdout()
        .to_vec();

    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let result = two_line_echo()
        .stdout_raw_tee(sink.clone())
        .output_string()
        .await
        .expect("a stdout_raw_tee run completes");

    // The decoded capture still works alongside the raw tee (strictly additive).
    assert!(result.stdout().contains("first") && result.stdout().contains("second"));
    assert_eq!(
        &*sink.0.lock().expect("sink mutex"),
        &expected,
        "the raw tee must reproduce the child's exact stdout bytes"
    );
}

// The raw tee fires on the *streaming* path too (start + stdout_lines + finish),
// not just bulk capture — proving the wiring reaches every pump call site.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_raw_tee_feeds_the_streaming_path() {
    use processkit::prelude::StreamExt;

    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let mut run = two_line_echo()
        .stdout_raw_tee(sink.clone())
        .start()
        .await
        .expect("start a raw-tee streamed run");

    let mut seen = Vec::new();
    let mut lines = run.stdout_lines().expect("stream stdout lines");
    while let Some(line) = lines.next().await {
        seen.push(line);
    }
    drop(lines);
    let done = run.finish().await.expect("finish the streamed run");
    assert!(
        matches!(done.outcome, processkit::Outcome::Exited(_)),
        "the streamed child exited: {:?}",
        done.outcome
    );

    assert!(
        seen.iter().any(|l| l.contains("first")) && seen.iter().any(|l| l.contains("second")),
        "the caller streamed both lines: {seen:?}"
    );
    let raw = String::from_utf8(sink.0.lock().expect("sink mutex").clone()).expect("raw is utf-8");
    assert!(
        raw.contains("first") && raw.contains("second"),
        "the raw tee received the streamed stdout bytes: {raw:?}"
    );
}

// The exacting end-to-end byte match: a real child writes CRLF, two non-UTF-8
// bytes, and no final newline. The raw tee must reproduce every byte — no CRLF
// normalization, no U+FFFD replacement, no fabricated trailing newline — while
// the decoded capture is free to be lossy. Unix-only: emitting exact bytes needs
// `printf` octal escapes (cmd.exe cannot).
#[cfg(unix)]
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdout_raw_tee_is_byte_exact_for_non_utf8_crlf_and_missing_newline() {
    let sink = SharedSink(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
    let result = Command::new("sh")
        .args(["-c", r"printf 'plain\r\n\377\376no-nl'"])
        .stdout_raw_tee(sink.clone())
        .output_string()
        .await
        .expect("a non-UTF-8 raw-tee run completes");

    // The decoded capture kept the ASCII prefix (lossy on the invalid bytes).
    assert!(result.stdout().contains("plain"));
    assert_eq!(
        &*sink.0.lock().expect("sink mutex"),
        b"plain\r\n\xff\xfeno-nl",
        "raw tee: CRLF un-normalized, non-UTF-8 bytes intact, no fabricated final newline"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn run_trims_and_requires_success() {
    // `cargo --version` is reliably present in this workspace.
    let out = Command::new("cargo")
        .arg("--version")
        .run()
        .await
        .expect("cargo --version");
    assert!(out.to_lowercase().contains("cargo"), "unexpected: {out}");
    // `run` trims trailing newlines.
    assert_eq!(out, out.trim_end());
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn output_bytes_returns_raw_stdout() {
    let result = two_line_echo().output_bytes().await.expect("run echo");
    assert!(result.is_success());
    let text = String::from_utf8_lossy(result.stdout());
    assert!(text.contains("first") && text.contains("second"));
}

#[tokio::test]
#[ignore = "spawns a real subprocess fed stdin it never reads"]
async fn early_exiting_child_does_not_fail_a_large_stdin_feed() {
    // duct's gotcha #2 as a spec: the child exits without reading stdin while
    // the writer still has ~1 MiB to push — the resulting broken-pipe write
    // (EPIPE / Windows pipe error) must not fail the run or hang the feed.
    let big = "x".repeat(1024 * 1024);
    let exits_zero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "exit", "0"])
    } else {
        Command::new("sh").args(["-c", "exit 0"])
    };
    let result = exits_zero
        .stdin(processkit::Stdin::from_string(big))
        .output_string()
        .await
        .expect("the stdin writer's broken pipe must not surface as Err");
    assert!(result.is_success(), "result: {result:?}");
}

#[tokio::test]
#[ignore = "spawns a real stdin-reading subprocess on the bulk path"]
async fn untaken_keep_stdin_open_pipe_is_closed_by_bulk_verbs() {
    // Regression (audit-found hang): `keep_stdin_open` + a bulk verb used to
    // leave the stdin pipe open forever — a stdin-reading child (`sort`)
    // blocked to its timeout instead of seeing EOF. The consuming verb must
    // close an untaken pipe, so this returns promptly and cleanly.
    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    let start = std::time::Instant::now();
    let result = reads_stdin
        .keep_stdin_open()
        .timeout(std::time::Duration::from_secs(20)) // tripwire, must not be hit
        .output_string()
        .await
        .expect("run completes");
    assert!(result.is_success(), "result: {result:?}");
    assert!(
        !result.timed_out(),
        "the child must see EOF, not hang to the deadline: {result:?}"
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(15),
        "bulk verb did not close the untaken stdin pipe (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess fed a failing stdin source"]
async fn failing_stdin_source_surfaces_as_error_stdin_on_a_successful_run() {
    use processkit::ErrorReason;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A stdin source whose read fails immediately with a non-broken-pipe error.
    struct FailingReader;
    impl tokio::io::AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("stdin source failed")))
        }
    }

    // `sort` (Windows) / `cat` (Unix): both read stdin and exit 0 on EOF.
    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    // B3 (Decision 2): the child sees EOF (the sink is dropped on the writer's
    // error) and exits 0 — a success — so the stashed non-broken-pipe stdin
    // failure now surfaces as `ErrorReason::Stdin` instead of being swallowed.
    let err = reads_stdin
        .stdin(processkit::Stdin::from_reader(FailingReader))
        .output_string()
        .await
        .expect_err("a failed stdin writer on a successful run must surface as ErrorReason::Stdin");
    assert!(
        matches!(err.reason(), ErrorReason::Stdin { .. }),
        "got: {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess fed a panicking stdin source"]
async fn panicking_stdin_source_surfaces_as_error_stdin_not_silent_success() {
    use processkit::ErrorReason;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A stdin source whose read PANICS — a bug in a user-supplied reader.
    struct PanickingReader;
    impl tokio::io::AsyncRead for PanickingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            panic!("stdin source panicked");
        }
    }

    let reads_stdin = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    // L1: the writer task panics; its `JoinError` must be surfaced as
    // `ErrorReason::Stdin` on an otherwise-successful run, not swallowed into a clean
    // success (a panicking source is a real failure the caller must see).
    let err = reads_stdin
        .stdin(processkit::Stdin::from_reader(PanickingReader))
        .output_string()
        .await
        .expect_err(
            "a panicking stdin writer on a successful run must surface as ErrorReason::Stdin",
        );
    assert!(
        matches!(err.reason(), ErrorReason::Stdin { .. }),
        "got: {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess that exits non-zero while its stdin source fails"]
async fn nonzero_exit_wins_over_a_failing_stdin_source() {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Yields one line, then fails (non-broken-pipe) — the child consumes the
    /// line and still exits non-zero.
    struct OneLineThenFail(bool);
    impl tokio::io::AsyncRead for OneLineThenFail {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.0 {
                Poll::Ready(Err(std::io::Error::other("stdin source failed")))
            } else {
                self.0 = true;
                buf.put_slice(b"x\n");
                Poll::Ready(Ok(()))
            }
        }
    }

    // grep/findstr of an absent pattern reads stdin, then exits 1 (no match).
    let nonzero = if cfg!(windows) {
        Command::new("cmd").args(["/c", "findstr", "zzz-no-match"])
    } else {
        Command::new("grep").arg("zzz-no-match")
    };
    // B3 (Decision 2): exit 1 is outside `ok_codes`, so the run is NOT a success
    // — the stdin failure is dropped, not surfaced. `output_string` returns the
    // result carrying the real exit code rather than `Err(ErrorReason::Stdin)`.
    let result = nonzero
        .stdin(processkit::Stdin::from_reader(OneLineThenFail(false)))
        .output_string()
        .await
        .expect("a non-zero exit is a result on output_string, not Err(Stdin)");
    assert_eq!(
        result.code(),
        Some(1),
        "the real exit code is preserved: {result:?}"
    );
    assert!(!result.is_success());
}

#[tokio::test]
#[ignore = "spawns a real subprocess whose stdin source fails during pump teardown"]
async fn stdin_source_failing_during_pump_teardown_still_surfaces_as_error_stdin() {
    use processkit::ErrorReason;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A stdin source that parks (yields no byte) and — only after `delay` —
    /// fails with a non-broken-pipe error. Parking keeps the background writer
    /// task *unfinished* past the child's exit and past the pre-pump stdin peek,
    /// so its failure lands in the post-exit teardown window (`join_pumps` /
    /// `finalize_stdin_task`), not before it. This is the exact race a single
    /// pre-pump observation lost: T-040's regression guard.
    struct DelayedFailingReader {
        delay: Duration,
        timer: Option<Pin<Box<tokio::time::Sleep>>>,
    }
    impl tokio::io::AsyncRead for DelayedFailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let delay = this.delay;
            let timer = this
                .timer
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));
            match timer.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(()) => Poll::Ready(Err(std::io::Error::other(
                    "stdin source failed mid-teardown",
                ))),
            }
        }
    }

    // `echo` prints a line and exits 0 *without* consuming stdin, so the run
    // succeeds while the parked writer is still in flight — a child success plus
    // a swallowed stdin failure is precisely the `ErrorReason::Stdin` case. Its stdout
    // line gives the pumps something to drain during teardown.
    let outputs_and_exits = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo hello"])
    } else {
        Command::new("sh").args(["-c", "echo hello"])
    };

    let err = outputs_and_exits
        .stdin(processkit::Stdin::from_reader(DelayedFailingReader {
            // Comfortably under PUMP_TEARDOWN (5s), so the bounded post-pump wait
            // catches it, yet long enough to land after the pre-pump peek.
            delay: Duration::from_millis(300),
            timer: None,
        }))
        .output_string()
        .await
        .expect_err(
            "a stdin writer that fails during the join_pumps window must still surface as \
             ErrorReason::Stdin, not a silent success",
        );
    assert!(
        matches!(err.reason(), ErrorReason::Stdin { .. }),
        "got: {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess echoing 256 KiB through both pipes"]
async fn large_stdin_and_large_output_do_not_deadlock() {
    // duct's gotcha #10 as a spec: stdin is fed and both outputs are drained
    // concurrently, so a child echoing more than a pipe buffer (~64 KiB) in
    // each direction can neither stall writing (full stdout pipe nobody
    // drains) nor starve reading (stdin writer blocked behind us).
    let line = "0123456789abcdef".repeat(64); // 1 KiB
    let big = format!("{line}\n").repeat(256); // 256 KiB + newlines
    let echo_all = if cfg!(windows) {
        // findstr "^" passes every line through.
        Command::new("cmd").args(["/c", "findstr", "^^"])
    } else {
        Command::new("cat")
    };
    let result = echo_all
        .stdin(processkit::Stdin::from_string(big.clone()))
        .timeout(std::time::Duration::from_secs(60)) // deadlock tripwire
        .output_string()
        .await
        .expect("echo run");
    assert!(result.is_success(), "result: {result:?}");
    assert_eq!(
        result.stdout().lines().count(),
        256,
        "every line must round-trip"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn stdin_is_fed_to_the_child() {
    // `cat` (Unix) / `findstr` echo of stdin (Windows `sort` reads stdin).
    let result = if cfg!(windows) {
        Command::new("cmd")
            .args(["/c", "sort"])
            .stdin(processkit::Stdin::from_string("delta\nalpha\n"))
            .output_string()
            .await
            .expect("run sort")
    } else {
        Command::new("cat")
            .stdin(processkit::Stdin::from_string("hello stdin\n"))
            .output_string()
            .await
            .expect("run cat")
    };
    assert!(result.is_success());
    let expected = if cfg!(windows) {
        "alpha"
    } else {
        "hello stdin"
    };
    assert!(
        result.stdout().contains(expected),
        "stdout: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess and waits for the timeout"]
async fn timeout_kills_and_flags() {
    let result = sleeper()
        .timeout(Duration::from_millis(300))
        .output_string()
        .await
        .expect("timed run still returns a result");
    assert!(result.timed_out(), "should be flagged as timed out");
    assert!(!result.is_success());
}

#[tokio::test]
#[ignore = "spawns a real subprocess and waits for the timeout"]
async fn exit_code_surfaces_timeout_as_error() {
    // `Command::exit_code` must report a timeout as `ErrorReason::Timeout`, not the
    // synthetic `-1` — consistent with the runner/CliClient code paths.
    let err = sleeper()
        .timeout(Duration::from_millis(300))
        .exit_code()
        .await
        .expect_err("a timed-out run has no meaningful exit code");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Timeout { .. }),
        "expected ErrorReason::Timeout, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess that stalls; must not hang past the timeout"]
async fn first_line_honors_timeout_instead_of_hanging() {
    // A long-running command that emits NO stdout: without a timeout `first_line`
    // would block forever waiting for a line. With a deadline it must give up and
    // surface `ErrorReason::Timeout` promptly — never hang.
    let silent = if cfg!(windows) {
        Command::new("powershell").args(["-NoProfile", "-Command", "Start-Sleep -Seconds 30"])
    } else {
        Command::new("sleep").arg("30")
    };
    let start = Instant::now();
    let err = silent
        .timeout(Duration::from_millis(300))
        .first_line(|_| true)
        .await
        .expect_err("a stalled run should time out, not return Ok(None)");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Timeout { .. }),
        "expected ErrorReason::Timeout, got {err:?}"
    );
    // Generous anti-hang bound (the sleeper runs ~30s if the timeout is
    // broken): under full-suite load PowerShell's cold start alone has been
    // seen to push a 300ms-timeout run past 5s.
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "first_line did not honor the timeout (took {:?})",
        start.elapsed()
    );
}

#[cfg(feature = "process-control")]
#[tokio::test]
#[ignore = "spawns a real subprocess in a shared group; first_line must time out and tear it down"]
async fn shared_group_first_line_honors_timeout_and_tears_down() {
    use processkit::{ProcessGroup, ProcessRunnerExt};

    // A2: a first_line probe on a SHARED group must honor the command timeout —
    // surface ErrorReason::Timeout AND tear the child down (the shared-group deadline
    // watchdog now reaches the direct child by pid). A single-process idle emits
    // no matching line and outlives the deadline.
    let group = ProcessGroup::new().expect("group");
    let silent = if cfg!(windows) {
        Command::new("ping").args(["-n", "30", "127.0.0.1"])
    } else {
        Command::new("sleep").arg("30")
    }
    .timeout(Duration::from_millis(300));

    let start = Instant::now();
    let err = completes_within(
        Duration::from_secs(15),
        "shared-group first_line timeout",
        group.first_line(&silent, |_| false),
    )
    .await
    .expect_err("a stalled shared-group probe must time out, not hang or return Ok(None)");
    assert!(
        matches!(err.reason(), processkit::ErrorReason::Timeout { .. }),
        "expected ErrorReason::Timeout, got {err:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "first_line did not honor the timeout (took {:?})",
        start.elapsed()
    );

    // No leak: the deadline watchdog tore the child down.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let members = group.members().expect("members");
        if members.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "first_line timeout leaked a shared-group child: {members:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "spawns a real stdin-reading subprocess via first_line"]
async fn first_line_closes_an_untaken_keep_stdin_open_pipe() {
    // `first_line` gives no way to write stdin, so a `keep_stdin_open` filter
    // must still see EOF — otherwise `cat` blocks reading stdin forever and
    // the stream never yields. The streaming verb closes the untaken pipe.
    let filter = if cfg!(windows) {
        Command::new("cmd").args(["/c", "sort"])
    } else {
        Command::new("cat")
    };
    let start = Instant::now();
    // No timeout: a regression would hang indefinitely, caught by the outer
    // test harness — the assertion below documents the intent.
    let found = filter
        .keep_stdin_open()
        .first_line(|_| true)
        .await
        .expect("first_line completes");
    assert_eq!(
        found, None,
        "an empty stdin filter emits nothing: {found:?}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "first_line hung on an untaken stdin pipe (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses"]
async fn probe_reads_real_exit_codes() {
    // Exit 0 -> Ok(true), exit 1 -> Ok(false), exit 2 -> Err.
    let exits = |code: i32| {
        if cfg!(windows) {
            Command::new("cmd").args(["/c", "exit", &code.to_string()])
        } else {
            Command::new("sh").args(["-c", &format!("exit {code}")])
        }
    };
    assert!(exits(0).probe().await.expect("exit 0 is a clean true"));
    assert!(!exits(1).probe().await.expect("exit 1 is a clean false"));
    assert!(
        exits(2).probe().await.is_err(),
        "any code other than 0/1 must be an error, not a silent bool"
    );
}

#[tokio::test]
#[ignore = "spawns real subprocesses; ok_codes through the real verbs"]
async fn ok_codes_widens_success_through_output_string_and_bytes() {
    // A non-zero exit the caller declares OK must flow through the REAL spawn
    // path to is_success() — for BOTH output_string and output_bytes (the bytes
    // verb carries ok_codes too, and had no test at any layer).
    let s = failing_exit(1)
        .ok_codes([0, 1])
        .output_string()
        .await
        .expect("run completes");
    assert!(
        s.is_success(),
        "exit 1 is success under ok_codes([0,1]) (output_string)"
    );
    assert_eq!(s.code(), Some(1), "the raw code is still reported");

    let b = failing_exit(1)
        .ok_codes([0, 1])
        .output_bytes()
        .await
        .expect("run completes");
    assert!(
        b.is_success(),
        "exit 1 is success under ok_codes([0,1]) (output_bytes)"
    );
    assert_eq!(b.code(), Some(1));

    // The widening is bounded: a code outside the set is still a failure.
    let outside = failing_exit(2)
        .ok_codes([0, 1])
        .output_string()
        .await
        .expect("run completes");
    assert!(
        !outside.is_success(),
        "exit 2 is outside ok_codes([0,1]) — still a failure"
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess that overflows a fail-loud buffer"]
async fn fail_loud_buffer_surfaces_output_too_large() {
    // A child that prints more lines than a fail-loud ceiling must surface
    // ErrorReason::OutputTooLarge through the real spawn+pump path — the fail-loud
    // DoS guard, end-to-end. `five_lines` prints 5 lines; the cap is 2, so the
    // run errors even though the child exited 0. (The pipe is still drained, so
    // the child never blocks.)
    use processkit::OutputBufferPolicy;
    let err = five_lines()
        .output_buffer(OutputBufferPolicy::fail_loud(2))
        .output_string()
        .await
        .expect_err("5 lines over a 2-line fail-loud cap must error");
    match err.reason() {
        processkit::ErrorReason::OutputTooLarge {
            max_lines,
            total_lines,
            ..
        } => {
            assert_eq!(
                *max_lines,
                Some(2),
                "the configured line cap is reported: {err:?}"
            );
            assert!(*total_lines >= 5, "every line is counted: {total_lines}");
        }
        other => panic!("expected ErrorReason::OutputTooLarge, got {other:?}"),
    }

    // A run that stays under the cap must NOT error (control case).
    let ok = two_line_echo()
        .output_buffer(OutputBufferPolicy::fail_loud(10))
        .output_string()
        .await
        .expect("2 lines under a 10-line cap is fine");
    assert!(ok.is_success());
}

#[tokio::test]
#[ignore = "spawns a real subprocess whose raw stdout a byte cap bounds"]
async fn output_bytes_honors_the_byte_cap() {
    // `output_bytes` captures stdout raw, but a byte ceiling on the policy is a
    // real memory bound there too (not just for the line verbs): fail-loud
    // errors, the drop modes bound the retained bytes. `two_line_echo` prints
    // well over 4 bytes.
    use processkit::{OutputBufferPolicy, OverflowMode};

    // Fail-loud: a raw-stdout flood past the byte cap errors with OutputTooLarge,
    // reporting the byte ceiling — raw stdout has no lines, so `max_lines` is
    // `None`.
    let err = two_line_echo()
        .output_buffer(
            OutputBufferPolicy::unbounded()
                .with_max_bytes(4)
                .with_overflow(OverflowMode::Error),
        )
        .output_bytes()
        .await
        .expect_err("raw stdout over a 4-byte fail-loud cap must error");
    match err.into_reason() {
        processkit::ErrorReason::OutputTooLarge {
            max_bytes,
            max_lines,
            total_bytes,
            ..
        } => {
            assert_eq!(max_bytes, Some(4), "the configured byte cap is reported");
            assert_eq!(
                max_lines, None,
                "raw stdout has no lines, so no line cap is reported"
            );
            assert!(
                total_bytes >= 4,
                "every byte seen is counted: {total_bytes}"
            );
        }
        other => panic!("expected ErrorReason::OutputTooLarge, got {other:?}"),
    }

    // Drop mode (the policy default overflow): the retained bytes are bounded
    // and truncation is flagged.
    let capped = two_line_echo()
        .output_buffer(OutputBufferPolicy::unbounded().with_max_bytes(4))
        .output_bytes()
        .await
        .expect("a drop-mode byte cap does not error");
    assert!(
        capped.stdout().len() <= 4,
        "retained bytes are bounded to the cap, got {}",
        capped.stdout().len()
    );
    assert!(capped.truncated(), "dropping raw bytes flags truncation");

    // Control: no cap keeps the full output, unbounded as before.
    let full = two_line_echo()
        .output_bytes()
        .await
        .expect("uncapped output_bytes");
    assert!(full.stdout().len() > 4, "the full output exceeds the cap");
    assert!(!full.truncated(), "no cap set, no truncation");
}

#[tokio::test]
#[ignore = "spawns a real subprocess whose output a bounded drop-policy truncates"]
async fn checking_verbs_reject_truncated_output_e2e() {
    // B12: a bounded *drop* policy silently discards lines. The lenient capture
    // verb (`output_string`) returns the partial result with `truncated()` set;
    // the checking verbs that hand back stdout as if complete (`run`/`parse`)
    // must instead fail loud with `OutputTooLarge` rather than feed a caller a
    // truncated tail. `five_lines` prints 5 lines; the cap keeps 2.
    use processkit::OutputBufferPolicy;

    // Lenient: output_string keeps the (partial) result and flags truncation.
    let lenient = five_lines()
        .output_buffer(OutputBufferPolicy::bounded(2))
        .output_string()
        .await
        .expect("output_string stays lenient under a bounded drop policy");
    assert!(lenient.is_success());
    assert!(lenient.truncated(), "the bounded policy dropped lines");

    // Strict: run() refuses the silently-truncated stdout.
    let err = five_lines()
        .output_buffer(OutputBufferPolicy::bounded(2))
        .run()
        .await
        .expect_err("run must reject truncated stdout (B12)");
    assert!(
        matches!(
            err.reason(),
            processkit::ErrorReason::OutputTooLarge {
                max_lines: Some(2),
                ..
            }
        ),
        "expected OutputTooLarge with the configured cap, got {err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "Windows has no signal tier: timeout_grace must degrade to a prompt atomic kill"]
async fn graceful_timeout_degrades_to_a_prompt_kill_on_windows() {
    // Windows has no SIGTERM tier, so timeout_grace/timeout_signal degrade to the
    // atomic Job kill at the deadline — it must NOT wait out a phantom grace.
    let start = Instant::now();
    let result = sleeper() // ~30s child
        .timeout(Duration::from_millis(500))
        .timeout_grace(Duration::from_secs(30))
        .output_string()
        .await
        .expect("run completes");
    assert!(result.timed_out(), "the deadline fired");
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "Windows must hard-kill promptly at the deadline, not wait the 30s grace (took {:?})",
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess via the top-level free functions"]
async fn top_level_run_and_output() {
    let v = processkit::run("cargo", ["--version"])
        .await
        .expect("run cargo --version");
    assert!(v.to_lowercase().contains("cargo"), "unexpected: {v}");

    let result = processkit::output_string("cargo", ["--version"])
        .await
        .expect("output cargo --version");
    assert!(result.is_success());
    assert!(result.stdout().to_lowercase().contains("cargo"));
}

#[tokio::test]
#[ignore = "spawns a real subprocess driven via interactive stdin"]
async fn send_control_writes_the_exact_control_byte() {
    // `send_control` writes exactly one raw byte to the pipe — no terminal
    // interpretation happens, so a byte-for-byte echo child must receive
    // precisely the mapped control byte (Ctrl-D -> 0x04), not e.g. the text
    // 'd' or an EOF with no byte at all.
    let mut process = raw_stdin_echo()
        .keep_stdin_open()
        .start()
        .await
        .expect("start raw echo");
    let mut stdin = process.take_stdin().expect("stdin kept open");
    stdin.send_control('d').await.expect("send Ctrl-D");
    stdin.finish().await.expect("eof");

    let result = process.output_bytes().await.expect("collect");
    assert!(result.is_success(), "result: {result:?}");
    assert_eq!(
        result.stdout(),
        &[0x04][..],
        "the exact control byte must reach the child: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess driven via interactive stdin"]
async fn send_control_rejects_an_unrecognized_character_without_writing() {
    // An unrecognized character must error loudly and write nothing — not a
    // silent, meaningless byte.
    let mut process = raw_stdin_echo()
        .keep_stdin_open()
        .start()
        .await
        .expect("start raw echo");
    let mut stdin = process.take_stdin().expect("stdin kept open");
    let err = stdin
        .send_control('0')
        .await
        .expect_err("a digit is not a recognized control character");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    stdin.finish().await.expect("eof");

    let result = process.output_bytes().await.expect("collect");
    assert!(result.is_success(), "result: {result:?}");
    assert!(
        result.stdout().is_empty(),
        "a rejected control character must write no byte: {:?}",
        result.stdout()
    );
}

#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn first_line_returns_none_when_the_stream_ends_without_a_match() {
    // stdout closing without a matching line is Ok(None) — not a hang and not
    // an error (the timeout path is covered separately).
    let found = tokio::time::timeout(
        Duration::from_secs(15),
        two_line_echo().first_line(|l| l.contains("never-printed")),
    )
    .await
    .expect("first_line must end when stdout closes, not hang")
    .expect("run succeeds");
    assert_eq!(found, None);
}

/// A whole-command [`processkit::CapturePolicy`] that scrubs every `secret`
/// occurrence as each line is captured — the redaction-at-capture use case.
struct RedactSecret;
impl processkit::CapturePolicy for RedactSecret {
    fn name(&self) -> &str {
        "redact-secret"
    }
    fn on_capture<'a>(
        &self,
        _stream: processkit::OutputStream,
        line: &'a str,
    ) -> std::borrow::Cow<'a, str> {
        if line.contains("secret") {
            std::borrow::Cow::Owned(line.replace("secret", "[REDACTED]"))
        } else {
            std::borrow::Cow::Borrowed(line)
        }
    }
}

/// A real child prints a secret to stdout; a `capture_policy` scrubs it before
/// it settles, so the captured `ProcessResult` never contains the raw secret.
/// End-to-end proof of the Command -> pump -> ProcessResult wiring.
#[tokio::test]
#[ignore = "spawns a real subprocess"]
async fn capture_policy_redacts_captured_output_end_to_end() {
    let cmd = if cfg!(windows) {
        Command::new("cmd").args(["/c", "echo token=secret-abc& echo plain-line"])
    } else {
        Command::new("sh").args(["-c", "printf 'token=secret-abc\nplain-line\n'"])
    };
    let result = cmd
        .capture_policy(RedactSecret)
        .output_string()
        .await
        .expect("run succeeds");
    let stdout = result.stdout();
    assert!(
        stdout.contains("[REDACTED]"),
        "the redaction landed in the captured result: {stdout:?}"
    );
    assert!(
        !stdout.contains("secret"),
        "the raw secret must never reach ProcessResult: {stdout:?}"
    );
    assert!(
        stdout.contains("plain-line"),
        "non-matching lines are captured verbatim: {stdout:?}"
    );
}
