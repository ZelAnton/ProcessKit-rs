//! Record/replay cassettes over the [`ProcessRunner`] seam (`record` feature).
//!
//! [`RecordReplayRunner`] closes the gap between the hand-written
//! [`ScriptedRunner`](crate::ScriptedRunner) and the input-asserting
//! [`RecordingRunner`](crate::RecordingRunner): run the real tool **once** with
//! the runner in *record* mode and every `Invocation → ProcessResult` pair is
//! captured to a human-diffable JSON cassette; switch to *replay* mode and the
//! cassette serves results that compare equal to the recorded ones — fast,
//! hermetic, no subprocess in CI.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::doubles::Invocation;
use crate::error::{Error, Result};
use crate::result::{Outcome, ProcessResult};
use crate::runner::{JobRunner, ProcessRunner};

/// The on-disk format revision. Bumped if the cassette schema ever changes
/// incompatibly; loading a cassette with an unknown version fails loudly
/// instead of misreading it.
const CASSETTE_VERSION: u32 = 1;

/// The whole fixture file: a format version plus the entries in capture order.
#[derive(Debug, Serialize, Deserialize)]
struct Cassette {
    version: u32,
    entries: Vec<Entry>,
}

/// One captured `Invocation → ProcessResult` pair.
///
/// Strings are lossy UTF-8 (the cassette is a text fixture). **Only env
/// *values* are redacted** — overrides are stored as variable *names* only.
/// Everything else (`program`, `args`, `cwd`, `stdout`, `stderr`) is stored
/// **verbatim** and can carry secrets — a `--password=…` argv, a token echoed
/// to stdout — so review a cassette before committing it. `timeout` is
/// deliberately absent: it is the *command's* configuration, re-read at replay
/// time, exactly like the live runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    // --- the match key ---
    program: String,
    args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    has_stdin: bool,
    // --- stored for visibility, not matched on ---
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    env_names: Vec<String>,
    // --- the captured output ---
    stdout: String,
    stderr: String,
    code: Option<i32>,
    #[serde(default, skip_serializing_if = "is_false")]
    timed_out: bool,
    // Signal number for Signalled outcomes; absent for Exited/TimedOut and in
    // cassettes written before this field was added (loaded as None).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signal: Option<i32>,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // signature dictated by serde
fn is_false(b: &bool) -> bool {
    !*b
}

/// Write `json` to `path`, restricting the file to owner-only (`0600`) on Unix.
///
/// B18: a cassette redacts env *values* (it stores names only), but argv, cwd,
/// stdout, and stderr are stored **verbatim** — any of which can carry a secret.
/// So the file is created owner-only rather than inheriting a world-readable
/// umask. On Windows it inherits the directory ACL (the unit of access control
/// there); restrict the containing directory if the fixture is sensitive.
fn write_cassette(path: &Path, json: &str) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        // `mode(0o600)` applies only at *creation*, closing the create-time
        // window. For a pre-existing (possibly world-readable) cassette being
        // rewritten, `open` truncates it but keeps its old perms — so tighten
        // the fd *before* writing the content, never holding it at loose perms.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, json)
    }
}

impl Entry {
    /// Capture one record-mode call. Lossy UTF-8 throughout — see the type doc.
    fn from_parts(invocation: &Invocation, result: &ProcessResult<String>) -> Self {
        let mut env_names: Vec<String> = invocation
            .envs
            .iter()
            .map(|(name, _value)| name.to_string_lossy().into_owned())
            .collect();
        // Sorted + deduped: stable diffs, and repeated overrides of one var
        // are one fact ("this var shaped the run"), not a sequence.
        env_names.sort();
        env_names.dedup();
        Self {
            program: invocation.program.to_string_lossy().into_owned(),
            args: invocation
                .args
                .iter()
                .map(|a| a.to_string_lossy().into_owned())
                .collect(),
            cwd: invocation
                .cwd
                .as_ref()
                .map(|c| c.to_string_lossy().into_owned()),
            has_stdin: invocation.has_stdin,
            env_names,
            stdout: result.stdout().clone(),
            stderr: result.stderr().to_owned(),
            code: result.code(),
            timed_out: result.timed_out(),
            signal: match result.outcome() {
                Outcome::Signalled(s) => s,
                _ => None,
            },
        }
    }
}

/// What an invocation is matched on: program + args + cwd + has_stdin. Env
/// overrides are excluded so an irrelevant env difference between the record
/// and replay environments can't cause a spurious miss.
///
/// L23: the string components are *lossy* UTF-8 decodes, so two distinct
/// non-UTF-8 invocations that differ only in their invalid bytes produce the
/// same key and collide on replay (the first recorded one answers for both).
/// Accepted: keying on raw bytes would defeat the human-diffable text fixture,
/// and valid-UTF-8 invocations (the common case) never collide.
type Key = (String, Vec<String>, Option<String>, bool);

/// The key of a live invocation — must decode exactly like
/// [`key_of_entry`] (both sides go through the same lossy conversion).
fn key_of(invocation: &Invocation) -> Key {
    (
        invocation.program.to_string_lossy().into_owned(),
        invocation
            .args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect(),
        invocation
            .cwd
            .as_ref()
            .map(|c| c.to_string_lossy().into_owned()),
        invocation.has_stdin,
    )
}

/// The key of a stored entry (already lossy strings).
fn key_of_entry(entry: &Entry) -> Key {
    (
        entry.program.clone(),
        entry.args.clone(),
        entry.cwd.clone(),
        entry.has_stdin,
    )
}

/// The replay-side state for one key: its entries in capture order plus a
/// cursor implementing the order-then-repeat-last consumption.
#[derive(Debug)]
struct ReplaySlot {
    entries: Vec<Entry>,
    next: usize,
}

impl ReplaySlot {
    /// The entry for this call: in capture order while they last, then the
    /// last one forever — so a sequence of differing outputs replays
    /// faithfully, and a retry/probe loop that re-runs the command after the
    /// sequence is exhausted still gets a stable answer.
    fn play(&mut self) -> &Entry {
        let index = self.next.min(self.entries.len() - 1);
        self.next = self.next.saturating_add(1);
        &self.entries[index]
    }
}

enum Mode<R> {
    Record {
        inner: R,
        path: PathBuf,
        recorded: Mutex<Vec<Entry>>,
        /// Runs recorded since the last successful save — the drop-time flush
        /// fires only when there is something unwritten, so a save-then-record
        /// sequence can't silently lose the late runs.
        dirty: AtomicBool,
    },
    Replay {
        slots: Mutex<HashMap<Key, ReplaySlot>>,
    },
}

/// A [`ProcessRunner`] that records real runs to a JSON cassette, or replays a
/// cassette hermetically (`record` feature).
///
/// **Record** mode wraps a real inner runner (a [`JobRunner`] in production;
/// any runner in tests), captures each call's invocation and result, and
/// writes the cassette on [`save`](Self::save) (or best-effort on drop). Only
/// *completed* runs are captured — a call that returns `Err` (spawn failure,
/// timeout error from a checking helper, …) records nothing; non-zero exits
/// and captured timeouts are results and **are** recorded.
///
/// **Replay** mode loads the cassette and serves results without spawning
/// anything:
///
/// - **Matching** is by program + args + cwd + has-stdin. Env overrides are
///   *not* matched, and their **values** are never written — only the sorted
///   variable *names*. Everything else (argv, cwd, stdout, stderr) is stored
///   **verbatim**, so a cassette can still carry secrets in those fields:
///   review fixtures before committing. The file is written owner-only
///   (`0600`) on Unix.
/// - **Duplicates** of one key replay in capture order, then the last entry
///   repeats forever — a recorded sequence (`git rev-parse HEAD` twice with
///   different heads) replays faithfully, while a retry loop that outlives the
///   sequence keeps getting the final answer.
/// - **A miss is a strict error** ([`Error::CassetteMiss`], distinct from a
///   missing-program error so `is_not_found()` is `false`): replay never spawns
///   a surprise subprocess, and a stale or incomplete cassette fails loudly
///   rather than being mistaken for an absent optional tool.
/// - The replayed [`ProcessResult`] carries the *replaying* command's
///   [`timeout`](Command::timeout) configuration, exactly like the live
///   runner — so a recorded timed-out run surfaces as
///   [`Error::Timeout`](crate::Error::Timeout) with the real deadline.
/// - Covers the **`output`** shape only. The streaming half of the seam
///   ([`start`](crate::ProcessRunner::start)) inherits the default and returns
///   [`Error::Unsupported`](crate::Error::Unsupported) — recording line timing
///   and stream shape is future work; use
///   [`ScriptedRunner`](crate::ScriptedRunner) for hermetic streaming tests.
///
/// Cassettes are pretty-printed JSON with a `version` field; loading an
/// unknown version (or a corrupt file) is an [`Error::Io`] with
/// `InvalidData`. Non-UTF-8 programs/args/paths are stored *lossily* (the
/// fixture is text); both record and replay apply the same conversion, so
/// matching still works — the rare non-UTF-8 byte just round-trips as `�`.
///
/// **Lossy-key limitation (exotic input):** because the match key is the
/// lossy-decoded program/args/cwd, two *distinct* non-UTF-8 invocations that
/// differ only in their invalid bytes decode to the same key and are
/// indistinguishable on replay (the first recorded one answers for both). This
/// affects only commands whose program/args/cwd contain invalid UTF-8; keying
/// on raw bytes is intentionally avoided to keep the fixture human-diffable
/// text, and valid-UTF-8 invocations never collide.
///
/// **Persistence:** [`save`](Self::save) is the explicit write; record mode also
/// flushes best-effort on drop (so a forgotten `save()` still leaves a complete
/// cassette) — **except while unwinding**, so a panic mid-recording never
/// persists a surprise file (it may carry secrets in argv/stdout).
///
/// ```no_run
/// use processkit::{Command, JobRunner, ProcessRunnerExt, RecordReplayRunner};
///
/// # async fn demo() -> processkit::Result<()> {
/// // Record once against the real tool (e.g. an opt-in `--record` test run):
/// let runner = RecordReplayRunner::record("fixtures/git.json", JobRunner::new());
/// let version = runner.run(&Command::new("git").arg("--version")).await?;
/// runner.save()?;
///
/// // Replay everywhere else — no subprocess, identical results:
/// let runner = RecordReplayRunner::replay("fixtures/git.json")?;
/// assert_eq!(
///     runner.run(&Command::new("git").arg("--version")).await?,
///     version,
/// );
/// # Ok(())
/// # }
/// ```
pub struct RecordReplayRunner<R: ProcessRunner = JobRunner> {
    mode: Mode<R>,
}

impl<R: ProcessRunner> RecordReplayRunner<R> {
    /// Record every run through `inner`, to be written to `path` as a JSON
    /// cassette by [`save`](Self::save) (or best-effort when the runner
    /// drops). Nothing touches the filesystem until then.
    pub fn record(path: impl Into<PathBuf>, inner: R) -> Self {
        Self {
            mode: Mode::Record {
                inner,
                path: path.into(),
                recorded: Mutex::new(Vec::new()),
                dirty: AtomicBool::new(false),
            },
        }
    }

    /// Write the cassette now (record mode). This is the error-surfacing path
    /// — the drop-time flush swallows failures. Idempotent (rewrites the full
    /// cassette each time); a no-op `Ok` in replay mode. Runs recorded *after*
    /// a save are still covered: the drop-time flush fires whenever anything
    /// was recorded since the last successful save.
    pub fn save(&self) -> Result<()> {
        let Mode::Record {
            path,
            recorded,
            dirty,
            ..
        } = &self.mode
        else {
            return Ok(());
        };
        // Hold the entries lock until `dirty` is cleared, so a run recorded
        // concurrently with the save can't be marked clean without being in
        // the written file (it blocks, then lands as dirty again).
        // `expect`, not poison-recovery: no user code ever runs under the
        // cassette locks, so poisoning is a logic bug worth failing loudly on
        // (the crate-wide rule lives in AGENTS.md "Code style").
        let entries = recorded.lock().expect("cassette mutex poisoned");
        let cassette = Cassette {
            version: CASSETTE_VERSION,
            entries: entries.clone(),
        };
        let json = serde_json::to_string_pretty(&cassette)
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;
        write_cassette(path, &json)?;
        dirty.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl RecordReplayRunner<JobRunner> {
    /// Load the cassette at `path` and serve its entries hermetically — no
    /// subprocess is ever spawned in replay mode.
    ///
    /// Errors are [`Error::Io`]: a missing file keeps its `NotFound` kind; a
    /// corrupt file or an unknown format `version` is `InvalidData`.
    pub fn replay(path: impl AsRef<Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        // serde_json's From<serde_json::Error> for io::Error keeps the right
        // kind (syntax/data errors become InvalidData).
        let cassette: Cassette =
            serde_json::from_str(&text).map_err(|e| Error::Io(std::io::Error::from(e)))?;
        if cassette.version != CASSETTE_VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cassette version {} is not supported (this build reads version {CASSETTE_VERSION})",
                    cassette.version
                ),
            )));
        }
        let mut slots: HashMap<Key, ReplaySlot> = HashMap::new();
        for entry in cassette.entries {
            slots
                .entry(key_of_entry(&entry))
                .or_insert_with(|| ReplaySlot {
                    entries: Vec::new(),
                    next: 0,
                })
                .entries
                .push(entry);
        }
        Ok(Self {
            mode: Mode::Replay {
                slots: Mutex::new(slots),
            },
        })
    }
}

#[async_trait::async_trait]
impl<R: ProcessRunner> ProcessRunner for RecordReplayRunner<R> {
    async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
        match &self.mode {
            Mode::Record {
                inner,
                recorded,
                dirty,
                ..
            } => {
                let result = inner.output(command).await?;
                let invocation = Invocation::from_command(command);
                let mut entries = recorded.lock().expect("cassette mutex poisoned");
                entries.push(Entry::from_parts(&invocation, &result));
                // Under the same lock `save` holds while clearing the flag, so
                // this entry is either in the file or marked unwritten.
                dirty.store(true, Ordering::SeqCst);
                Ok(result)
            }
            Mode::Replay { slots } => {
                let invocation = Invocation::from_command(command);
                let mut slots = slots.lock().expect("cassette mutex poisoned");
                let Some(slot) = slots.get_mut(&key_of(&invocation)) else {
                    // Ф7: a stale/incomplete cassette is a distinct error, not a
                    // missing-program `Spawn`/`NotFound` (so `is_not_found()` is
                    // false and a wrapper can't mistake it for an absent tool).
                    return Err(Error::CassetteMiss {
                        program: command.program_name(),
                    });
                };
                let entry = slot.play();
                // Ф6: feed the replayed output through the command's
                // `on_stdout_line`/`on_stderr_line` handlers, so a wrapper's
                // progress path is exercised on replay exactly as it is in
                // record mode (real pumps) and on `ScriptedRunner::output`.
                crate::doubles::replay_line_handlers(command, &entry.stdout, &entry.stderr);
                let outcome = match (entry.code, entry.timed_out) {
                    (_, true) => Outcome::TimedOut,
                    (Some(code), false) => Outcome::Exited(code),
                    (None, false) => Outcome::Signalled(entry.signal),
                };
                Ok(ProcessResult::new(
                    // Same value the live runner reports — the lossy program
                    // name — so a round trip is comparison-identical.
                    entry.program.clone(),
                    entry.stdout.clone(),
                    entry.stderr.clone(),
                    outcome,
                    command.configured_timeout(),
                )
                .with_ok_codes(command.ok_codes_vec()))
            }
        }
    }
}

// Manual: no `R: Debug` bound (mirrors `RecordingRunner`), and the recorded
// entries/slots are summarized as counts rather than dumped.
impl<R: ProcessRunner> std::fmt::Debug for RecordReplayRunner<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.mode {
            Mode::Record {
                path,
                recorded,
                dirty,
                ..
            } => f
                .debug_struct("RecordReplayRunner::Record")
                .field("path", path)
                .field(
                    "recorded",
                    &recorded.lock().expect("cassette mutex poisoned").len(),
                )
                .field("dirty", &dirty.load(Ordering::SeqCst))
                .finish_non_exhaustive(),
            Mode::Replay { slots } => f
                .debug_struct("RecordReplayRunner::Replay")
                .field(
                    "keys",
                    &slots.lock().expect("cassette mutex poisoned").len(),
                )
                .finish_non_exhaustive(),
        }
    }
}

impl<R: ProcessRunner> Drop for RecordReplayRunner<R> {
    fn drop(&mut self) {
        // Best-effort flush of anything recorded since the last save, so a
        // record run that forgot `save()` (or recorded more after one) still
        // leaves a complete cassette behind; errors are deliberately swallowed
        // (a Drop can't surface them) — call `save()` to observe failures.
        //
        // B18: skip the flush while *unwinding* (`thread::panicking()`). A test
        // that panics mid-recording should not silently persist a surprise
        // cassette — which stores argv/cwd/stdout/stderr verbatim and may carry
        // secrets — as a side effect of the panic. Normal scope-exit still
        // flushes; call `save()` for explicit control on the panic path.
        if let Mode::Record { dirty, .. } = &self.mode
            && dirty.load(Ordering::SeqCst)
            && !std::thread::panicking()
        {
            let _ = self.save();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doubles::{Reply, ScriptedRunner};
    use crate::result::Outcome;
    use crate::runner::ProcessRunnerExt;
    use std::time::Duration;

    /// A scripted inner runner standing in for the real tool.
    fn scripted() -> ScriptedRunner {
        ScriptedRunner::new()
            .on(["tool", "--version"], Reply::ok("tool 1.2.3\n"))
            .on(["tool", "fail"], Reply::fail(7, "boom"))
    }

    fn temp_cassette() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("cassette.json");
        (dir, path)
    }

    #[tokio::test]
    async fn round_trip_is_identical() {
        let (_dir, path) = temp_cassette();

        let recorder = RecordReplayRunner::record(&path, scripted());
        let ok = recorder
            .output(&Command::new("tool").arg("--version"))
            .await
            .expect("record ok run");
        let fail = recorder
            .output(&Command::new("tool").arg("fail"))
            .await
            .expect("record failing run (non-zero exit is a result, not Err)");
        recorder.save().expect("save cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let ok2 = replayer
            .output(&Command::new("tool").arg("--version"))
            .await
            .expect("replay ok run");
        let fail2 = replayer
            .output(&Command::new("tool").arg("fail"))
            .await
            .expect("replay failing run");
        assert_eq!(ok, ok2, "replay must be identical to the recording");
        assert_eq!(fail, fail2);
        assert_eq!(fail2.code(), Some(7));
        assert_eq!(fail2.stderr(), "boom");
    }

    #[tokio::test]
    async fn duplicate_key_plays_in_order_then_repeats_last() {
        let (_dir, path) = temp_cassette();

        // The same invocation captured twice with different outputs (think
        // `git rev-parse HEAD` before and after a commit). ScriptedRunner
        // replies are stateless, so the sequence is hand-crafted exactly as a
        // real recording of a changing command lays it out.
        let json = serde_json::json!({
            "version": 1,
            "entries": [
                {
                    "program": "git", "args": ["head"],
                    "stdout": "aaa", "stderr": "", "code": 0
                },
                {
                    "program": "git", "args": ["head"],
                    "stdout": "bbb", "stderr": "", "code": 0
                }
            ]
        });
        std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();

        let cmd = Command::new("git").arg("head");
        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let first = replayer.run(&cmd).await.expect("first replay");
        let second = replayer.run(&cmd).await.expect("second replay");
        let third = replayer.run(&cmd).await.expect("third replay repeats last");
        assert_eq!(first, "aaa");
        assert_eq!(second, "bbb");
        assert_eq!(third, "bbb", "exhausted key must repeat the last entry");
    }

    #[tokio::test]
    async fn replay_miss_is_a_distinct_cassette_miss_error() {
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        recorder
            .output(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output(&Command::new("tool").arg("--other"))
            .await
            .expect_err("an unrecorded invocation must not be served");
        match &err {
            Error::CassetteMiss { program } => assert_eq!(program, "tool"),
            other => panic!("expected Error::CassetteMiss, got {other:?}"),
        }
        // Ф7: a stale cassette is NOT mistaken for a missing program.
        assert!(
            !err.is_not_found(),
            "a cassette miss must not read as not-found: {err:?}"
        );
    }

    #[tokio::test]
    async fn replay_invokes_line_handlers() {
        // Ф6: replay feeds the recorded output through `on_stdout_line`, just
        // like record mode (real pumps) and `ScriptedRunner::output` — a
        // wrapper's progress path tests the same hermetically on replay.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        recorder
            .output(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let cmd = Command::new("tool").arg("--version").on_stdout_line({
            let seen = seen.clone();
            move |l| seen.lock().unwrap().push(l.to_owned())
        });
        replayer.output(&cmd).await.expect("replay");
        assert_eq!(
            *seen.lock().unwrap(),
            ["tool 1.2.3"],
            "replay must invoke the command's line handler"
        );
    }

    #[tokio::test]
    async fn replayed_timeout_carries_the_commands_deadline() {
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().on(["tool", "slow"], Reply::timeout()),
        );
        recorder
            .output(&Command::new("tool").arg("slow"))
            .await
            .expect("a captured timeout is a result, not an Err");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .run(
                &Command::new("tool")
                    .arg("slow")
                    .timeout(Duration::from_secs(7)),
            )
            .await
            .expect_err("run() raises the captured timeout");
        match err {
            Error::Timeout { timeout, .. } => assert_eq!(timeout, Duration::from_secs(7)),
            other => panic!("expected Error::Timeout, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn env_values_never_reach_the_file() {
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("done")));
        recorder
            .output(
                &Command::new("tool")
                    .env("API_TOKEN", "hunter2-very-secret")
                    .env("MODE", "fast"),
            )
            .await
            .expect("record");
        recorder.save().expect("save");

        let json = std::fs::read_to_string(&path).expect("read cassette");
        assert!(json.contains("API_TOKEN"), "names are stored: {json}");
        assert!(json.contains("MODE"));
        assert!(
            !json.contains("hunter2-very-secret") && !json.contains("fast"),
            "values must never be written: {json}"
        );

        // And env differences don't affect matching: replay without any env.
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let out = replayer
            .run(&Command::new("tool"))
            .await
            .expect("env is not part of the match key");
        assert_eq!(out, "done");
    }

    #[tokio::test]
    async fn signal_number_survives_round_trip() {
        // Write a cassette JSON that encodes a signal-killed run (signal 9).
        // Then replay it and verify the outcome carries the signal number.
        let (_dir, path) = temp_cassette();
        let json = r#"{"version":1,"entries":[{"program":"tool","args":[],"stdout":"","stderr":"","code":null,"signal":9}]}"#;
        std::fs::write(&path, json).expect("write cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let result = replayer
            .output(&Command::new("tool"))
            .await
            .expect("replay");
        assert_eq!(result.outcome(), Outcome::Signalled(Some(9)));
    }

    #[tokio::test]
    async fn cassette_without_signal_field_loads_as_signalled_none() {
        // Old cassettes have no `signal` field; they should replay as
        // Signalled(None) for a code:null entry, not fail to load.
        let (_dir, path) = temp_cassette();
        let json = r#"{"version":1,"entries":[{"program":"tool","args":[],"stdout":"","stderr":"","code":null}]}"#;
        std::fs::write(&path, json).expect("write cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let result = replayer
            .output(&Command::new("tool"))
            .await
            .expect("replay");
        assert_eq!(result.outcome(), Outcome::Signalled(None));
    }

    #[tokio::test]
    async fn load_errors_are_typed_io() {
        // Missing file keeps NotFound.
        let (_dir, path) = temp_cassette();
        match RecordReplayRunner::replay(&path) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {other:?}"),
        }

        // Corrupt JSON is InvalidData.
        std::fs::write(&path, "{ not json").unwrap();
        match RecordReplayRunner::replay(&path) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData),
            other => panic!("expected Io(InvalidData), got {other:?}"),
        }

        // An unknown format version is InvalidData too.
        std::fs::write(&path, r#"{ "version": 99, "entries": [] }"#).unwrap();
        match RecordReplayRunner::replay(&path) {
            Err(Error::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                assert!(e.to_string().contains("version 99"), "got: {e}");
            }
            other => panic!("expected Io(InvalidData), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drop_without_save_flushes_best_effort() {
        let (_dir, path) = temp_cassette();
        {
            let recorder = RecordReplayRunner::record(&path, scripted());
            recorder
                .output(&Command::new("tool").arg("--version"))
                .await
                .expect("record");
            // No save() — the Drop flush must persist the cassette.
        }
        let replayer = RecordReplayRunner::replay(&path).expect("dropped recorder left a cassette");
        let out = replayer
            .run(&Command::new("tool").arg("--version"))
            .await
            .expect("replay after drop-flush");
        assert_eq!(out, "tool 1.2.3");
    }

    #[tokio::test]
    async fn cwd_is_part_of_the_match_key() {
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("from-a")));
        recorder
            .output(&Command::new("tool").current_dir("dir-a"))
            .await
            .expect("record in dir-a");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        // Same program+args but a different (or no) cwd must not match.
        let err = replayer
            .output(&Command::new("tool").current_dir("dir-b"))
            .await
            .expect_err("a different cwd is a different invocation");
        assert!(matches!(err, Error::CassetteMiss { .. }), "got {err:?}");
        let err = replayer
            .output(&Command::new("tool"))
            .await
            .expect_err("a missing cwd is a different invocation too");
        assert!(matches!(err, Error::CassetteMiss { .. }), "got {err:?}");
        // The recorded cwd still replays.
        let out = replayer
            .run(&Command::new("tool").current_dir("dir-a"))
            .await
            .expect("the recorded cwd matches");
        assert_eq!(out, "from-a");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cassette_file_is_written_owner_only() {
        // B18: a cassette stores argv/cwd/stdout/stderr verbatim, so it must not
        // inherit a world-readable umask — it is created 0600 on Unix.
        use std::os::unix::fs::PermissionsExt;
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        recorder
            .output(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let mode = std::fs::metadata(&path)
            .expect("stat cassette")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "cassette must be owner-only, got {:o}",
            mode & 0o777
        );
    }

    #[tokio::test]
    async fn drop_while_unwinding_does_not_persist_a_surprise_cassette() {
        // B18: a panic mid-recording must not flush a cassette as a side effect
        // (it may carry secrets in argv/stdout). The Drop guard skips on unwind.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        recorder
            .output(&Command::new("tool").arg("--version"))
            .await
            .expect("record (now dirty, unsaved)");

        // Move the dirty recorder into a panicking scope; its Drop runs during
        // unwind and must NOT write the file.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _hold = recorder;
            panic!("boom mid-recording");
        }));
        assert!(outcome.is_err(), "the scope must have panicked");
        assert!(
            !path.exists(),
            "a recorder dropped during unwind must not persist a cassette: {path:?}"
        );
    }

    #[tokio::test]
    async fn save_then_record_more_then_drop_flushes_the_late_runs() {
        let (_dir, path) = temp_cassette();
        {
            let recorder = RecordReplayRunner::record(&path, scripted());
            recorder
                .output(&Command::new("tool").arg("--version"))
                .await
                .expect("record first");
            recorder.save().expect("first save");
            // A run recorded *after* the save must not be lost on drop.
            recorder
                .output(&Command::new("tool").arg("fail"))
                .await
                .expect("record second");
        }
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let result = replayer
            .output(&Command::new("tool").arg("fail"))
            .await
            .expect("the post-save run was flushed by drop");
        assert_eq!(result.code(), Some(7));
    }

    #[tokio::test]
    async fn non_utf8_args_are_recorded_lossily_not_fatally() {
        // A program argument that is not valid Unicode, per platform.
        #[cfg(unix)]
        let bad = {
            use std::os::unix::ffi::OsStringExt;
            std::ffi::OsString::from_vec(vec![b'a', 0xFF, b'b'])
        };
        #[cfg(windows)]
        let bad = {
            use std::os::windows::ffi::OsStringExt;
            // A lone surrogate is valid UTF-16-ish for OsString but not Unicode.
            std::ffi::OsString::from_wide(&[0x61, 0xD800, 0x62])
        };
        // No non-Unicode OsString can be built portably elsewhere; the lossy
        // path is still exercised, just with a clean string.
        #[cfg(not(any(unix, windows)))]
        let bad = std::ffi::OsString::from("ab");

        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("ok")));
        let cmd = Command::new("tool").arg(&bad);
        recorder.output(&cmd).await.expect("record lossily");
        recorder.save().expect("save");

        // Both sides apply the same lossy conversion, so the entry matches.
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let out = replayer.run(&cmd).await.expect("replay matches lossily");
        assert_eq!(out, "ok");
    }
}
