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
use crate::result::ProcessResult;
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
/// Strings are lossy UTF-8 (the cassette is a text fixture); env overrides are
/// stored as **names only** — a committed fixture must never leak secret
/// values. `timeout` is deliberately absent: it is the *command's*
/// configuration, re-read at replay time, exactly like the live runner.
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
}

#[allow(clippy::trivially_copy_pass_by_ref)] // signature dictated by serde
fn is_false(b: &bool) -> bool {
    !*b
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
        }
    }
}

/// What an invocation is matched on: program + args + cwd + has_stdin. Env
/// overrides are excluded so an irrelevant env difference between the record
/// and replay environments can't cause a spurious miss.
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
///   *not* matched (and their values are never written to the file — only the
///   sorted variable names, so a committed fixture can't leak secrets).
/// - **Duplicates** of one key replay in capture order, then the last entry
///   repeats forever — a recorded sequence (`git rev-parse HEAD` twice with
///   different heads) replays faithfully, while a retry loop that outlives the
///   sequence keeps getting the final answer.
/// - **A miss is a strict error** ([`Error::Spawn`] with a not-found source,
///   like a [`ScriptedRunner`](crate::ScriptedRunner) without a fallback):
///   replay never spawns a surprise subprocess, and a stale cassette fails
///   loudly.
/// - The replayed [`ProcessResult`] carries the *replaying* command's
///   [`timeout`](Command::timeout) configuration, exactly like the live
///   runner — so a recorded timed-out run surfaces as
///   [`Error::Timeout`](crate::Error::Timeout) with the real deadline.
///
/// Cassettes are pretty-printed JSON with a `version` field; loading an
/// unknown version (or a corrupt file) is an [`Error::Io`] with
/// `InvalidData`. Non-UTF-8 programs/args/paths are stored *lossily* (the
/// fixture is text); both record and replay apply the same conversion, so
/// matching still works — the rare non-UTF-8 byte just round-trips as `�`.
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
        let entries = recorded.lock().expect("cassette mutex poisoned");
        let cassette = Cassette {
            version: CASSETTE_VERSION,
            entries: entries.clone(),
        };
        let json = serde_json::to_string_pretty(&cassette)
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;
        std::fs::write(path, json)?;
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
                    return Err(Error::Spawn {
                        program: command.program_name(),
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "RecordReplayRunner: no cassette entry matches this invocation",
                        ),
                    });
                };
                let entry = slot.play();
                Ok(ProcessResult::new(
                    // Same value the live runner reports — the lossy program
                    // name — so a round trip is comparison-identical.
                    entry.program.clone(),
                    entry.stdout.clone(),
                    entry.stderr.clone(),
                    entry.code,
                    entry.timed_out,
                    command.configured_timeout(),
                ))
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
        if let Mode::Record { dirty, .. } = &self.mode
            && dirty.load(Ordering::SeqCst)
        {
            let _ = self.save();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doubles::{Reply, ScriptedRunner};
    use crate::runner::ProcessRunnerExt;
    use std::time::Duration;

    /// A scripted inner runner standing in for the real tool.
    fn scripted() -> ScriptedRunner {
        ScriptedRunner::new()
            .on(["--version"], Reply::ok("tool 1.2.3\n"))
            .on(["fail"], Reply::fail(7, "boom"))
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
    async fn replay_miss_is_a_strict_not_found_error() {
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
        match err {
            Error::Spawn { program, source } => {
                assert_eq!(program, "tool");
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected Error::Spawn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replayed_timeout_carries_the_commands_deadline() {
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().on(["slow"], Reply::timeout()));
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
        assert!(matches!(err, Error::Spawn { .. }), "got {err:?}");
        let err = replayer
            .output(&Command::new("tool"))
            .await
            .expect_err("a missing cwd is a different invocation too");
        assert!(matches!(err, Error::Spawn { .. }), "got {err:?}");
        // The recorded cwd still replays.
        let out = replayer
            .run(&Command::new("tool").current_dir("dir-a"))
            .await
            .expect("the recorded cwd matches");
        assert_eq!(out, "from-a");
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
