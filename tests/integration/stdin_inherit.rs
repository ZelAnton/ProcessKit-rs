//! Real-subprocess coverage for `Command::inherit_stdin` (T-100): a child
//! launched with an inherited stdin reads the *parent's* standard input, and the
//! incompatible builder combinations are refused at the launch boundary.

use std::io::Write;
use std::time::Duration;

use processkit::{Command, ErrorKind, Stdin};

use crate::common::raw_stdin_echo;

/// Marker env var: when set, this test binary runs as the **inner** process of
/// the self-re-exec below, whose own stdin was fed known data by the outer run.
const INNER_MARKER: &str = "PK_T100_INHERIT_STDIN_INNER";
/// The payload the outer feeds on the inner's stdin — a unique string that
/// cannot collide with libtest's own banner lines.
const PAYLOAD: &str = "processkit-T100-inherited-stdin-payload-9f3a7c\n";
/// Sentinels bracketing the child's echoed bytes in the inner's stdout, so the
/// outer can extract exactly what the inherited-stdin child produced out of the
/// surrounding test-harness noise.
const BEGIN: &str = "<<<PK_T100_BEGIN>>>";
const END: &str = "<<<PK_T100_END>>>";

/// A child launched with `inherit_stdin()` reads the parent process's *own*
/// standard input, byte for byte.
///
/// Proving this needs a parent whose stdin is known data. The test therefore
/// re-execs itself: the **outer** run pipes `PAYLOAD` into an **inner** run of
/// this very test (selected by an env marker); the inner run then launches a
/// byte-for-byte stdin echo child with `inherit_stdin()`, so that child reads the
/// inner process's stdin — the piped `PAYLOAD` — and echoes it. The outer
/// captures the inner's stdout and asserts the child saw exactly `PAYLOAD`.
#[tokio::test]
#[ignore = "re-execs the test binary and spawns a real inherited-stdin child"]
async fn inherited_stdin_is_read_by_the_child() {
    if std::env::var_os(INNER_MARKER).is_some() {
        run_inner().await;
        return;
    }

    // Outer: re-invoke exactly this ignored test as a child, feeding it PAYLOAD on
    // stdin. `--nocapture` lets the inner's stdout (the child's echo) flow back to
    // us; `--exact --ignored` runs just this one test.
    let exe = std::env::current_exe().expect("current_exe for self re-exec");
    let result = Command::new(exe)
        .args([
            "stdin_inherit::inherited_stdin_is_read_by_the_child",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env(INNER_MARKER, "1")
        .stdin(Stdin::from_string(PAYLOAD))
        .timeout(Duration::from_secs(60))
        .output_string()
        .await
        .expect("the inner re-exec must run to completion");

    let out = result.stdout();
    let begin = out
        .find(BEGIN)
        .unwrap_or_else(|| panic!("inner produced no BEGIN sentinel; stdout: {out:?}"));
    let end = out
        .find(END)
        .unwrap_or_else(|| panic!("inner produced no END sentinel; stdout: {out:?}"));
    let echoed = &out[begin + BEGIN.len()..end];
    assert_eq!(
        echoed, PAYLOAD,
        "the inherit_stdin child must have read the parent's piped stdin verbatim; \
         full inner stdout: {out:?}"
    );
}

/// The inner half of [`inherited_stdin_is_read_by_the_child`]: our own stdin is
/// the outer's piped `PAYLOAD`; launch an echo child that inherits it and surface
/// exactly what the child read, bracketed by sentinels.
async fn run_inner() {
    let result = raw_stdin_echo()
        .inherit_stdin()
        .timeout(Duration::from_secs(30))
        .output_bytes()
        .await
        .expect("inner: an inherit_stdin echo child must run");
    assert!(
        result.is_success(),
        "inner: the echo child must exit cleanly; got {:?}",
        result.code()
    );
    let mut stdout = std::io::stdout();
    stdout.write_all(BEGIN.as_bytes()).unwrap();
    stdout.write_all(result.stdout()).unwrap();
    stdout.write_all(END.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

/// `inherit_stdin()` + `keep_stdin_open()` is refused end-to-end through a public
/// run verb, before any child is spawned, as a typed `ErrorKind::Io(InvalidInput)`.
#[tokio::test]
#[ignore = "drives the real launch path (though it rejects before spawning)"]
async fn inherit_stdin_with_keep_stdin_open_is_rejected() {
    let err = raw_stdin_echo()
        .inherit_stdin()
        .keep_stdin_open()
        .output_string()
        .await
        .expect_err("inherit_stdin + keep_stdin_open must be rejected at launch")
        .into_kind();
    assert!(
        matches!(&err, ErrorKind::Io(io) if io.kind() == std::io::ErrorKind::InvalidInput),
        "expected ErrorKind::Io(InvalidInput), got {err:?}"
    );
}

/// `inherit_stdin()` + a configured `stdin(Stdin::…)` source is likewise refused
/// through a public run verb as a typed `ErrorKind::Io(InvalidInput)`.
#[tokio::test]
#[ignore = "drives the real launch path (though it rejects before spawning)"]
async fn inherit_stdin_with_a_source_is_rejected() {
    let err = raw_stdin_echo()
        .stdin(Stdin::from_string("payload"))
        .inherit_stdin()
        .output_string()
        .await
        .expect_err("inherit_stdin + a stdin source must be rejected at launch")
        .into_kind();
    assert!(
        matches!(&err, ErrorKind::Io(io) if io.kind() == std::io::ErrorKind::InvalidInput),
        "expected ErrorKind::Io(InvalidInput), got {err:?}"
    );
}
