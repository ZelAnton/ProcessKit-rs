//! Record/replay cassettes over the [`ProcessRunner`] seam (`record` feature).
//!
//! [`RecordReplayRunner`] closes the gap between the hand-written
//! [`ScriptedRunner`](crate::testing::ScriptedRunner) and the input-asserting
//! [`RecordingRunner`](crate::testing::RecordingRunner): run the real tool **once** with
//! the runner in *record* mode and every `Invocation → ProcessResult` pair is
//! captured to a human-diffable JSON cassette; switch to *replay* mode and the
//! cassette serves results that compare equal to the recorded ones — fast,
//! hermetic, no subprocess in CI.
//!
//! **Portability of the match key.** An invocation is matched on `program` +
//! `args` + the stdin digest — **not** `cwd` (see [`CASSETTE_VERSION`] for why),
//! so a cassette recorded in one absolute working directory (a tempdir, a CI
//! workspace like `/home/alice/repo` or `C:\actions\work\…`) replays cleanly in
//! another: the leading portability blocker (`cwd` pinning a cassette to the
//! machine/checkout it was recorded on) is gone. `cwd` is still stored on the
//! entry, verbatim, for visibility — just not matched on. A `from_file` stdin
//! source keys on its **path**, though, so that source is still machine-bound if
//! the path itself is absolute and varies across machines (a tempdir file); a
//! per-run tempdir path will still miss on the very next run too. Prefer
//! `Stdin::from_bytes`/`from_string` over `from_file` when the cassette must
//! travel.

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
/// instead of misreading it. [`RecordReplayRunner::replay`] checks this
/// *before* attempting the full `Cassette` decode, so a future version whose
/// entries this build genuinely can't parse still reports the clear "version N
/// is not supported" message rather than a raw serde type-mismatch error.
///
/// Bumped to `2`: entries may now carry an optional `error` (see
/// [`CassetteError`]) recording an `Err` the inner runner returned in record
/// mode, so replay reproduces that same `Error` instead of missing the
/// cassette. The field is additive and optional at deserialization (`#[serde(default)]`),
/// so a version-1 cassette (no `error` field on any entry) still loads and
/// replays exactly as before — the bump only guards against an *older* build
/// misreading a *newer*-shaped cassette it doesn't understand yet, not the
/// reverse.
///
/// Bumped to `3`: `cwd` is no longer part of the match key (see the type doc's
/// "Portability of the match key"). A cassette recorded with the *previous*
/// (cwd-keying) build and replayed with this one still **loads and replays
/// fine** — the field is untouched on disk, still deserialized, still stored on
/// the entry for visibility, only dropped from the key computation — so this
/// bump is not a compatibility gate the way `2`'s was; it exists purely so a
/// cassette on disk records *which* matching rules produced it, for a human
/// skimming the file. A leading candidate that was *not* taken: normalizing
/// `cwd` to a path relative to a `record_root` (preserves cwd distinctions, but
/// needs a "root" concept the runner doesn't otherwise have, and no in-tree
/// consumer has ever needed two recorded runs to be told apart *only* by their
/// cwd — see `ideas/later-cassette-cwd-portability.md`).
const CASSETTE_VERSION: u32 = 3;

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
    /// FNV-1a digest of the stdin *source identity* — keyed so two invocations
    /// differing only in stdin don't collide on replay. In-memory bytes hash
    /// their content; a `from_file` source hashes its **path** (the file is not
    /// read at key time, so changing the file's bytes does not change the key).
    /// One-shot streaming sources (`from_reader`/`from_lines`) are rejected by
    /// record/replay — their bytes can't be keyed — so this digest only ever
    /// describes a replayable source.
    /// `None` for empty/absent stdin. An older cassette recorded *with* stdin
    /// but no digest loads this as `None` and must be re-recorded to match a
    /// stdin invocation again. See `Stdin::content_digest` for the hashing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stdin_digest: Option<u64>,
    // --- stored for visibility, not matched on ---
    /// The invocation's working directory, verbatim — **not** part of the match
    /// key (see the type doc's "Portability of the match key" / [`CASSETTE_VERSION`]'s
    /// `3` bump). Kept only so a human reviewing the cassette can see where the
    /// recording ran; two entries differing only in `cwd` collide on replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    /// Whether stdin was supplied (human-readable; matching uses `stdin_digest`).
    #[serde(default, skip_serializing_if = "is_false")]
    has_stdin: bool,
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
    /// Whether a bounded `OutputBufferPolicy` clipped the output. Recorded so the
    /// checking verbs' fail-loud-on-truncation (`run`/`parse` reject a clipped
    /// tail) survives replay instead of silently passing a truncated capture. Old
    /// cassettes (no field) load `false` — re-record to reproduce a clipped run.
    #[serde(default, skip_serializing_if = "is_false")]
    truncated: bool,
    /// Cumulative line / byte counts behind an `OutputTooLarge`, so a replayed
    /// rejection reports the same totals as the recording.
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    total_lines: usize,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    total_bytes: usize,
    /// Recorded wall-clock duration (ms), so a replayed `duration()` is the
    /// recording's, not a synthetic `0`. Old cassettes load `0`.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    duration_ms: u64,
    /// An `Err` the inner runner returned in record mode, in place of a
    /// completed output — `None` for the ordinary (successful-call) entry
    /// shape above. `Some` and every other field left at its default (empty
    /// streams, `code: None`, `signal: None`, `timed_out: false`) is the
    /// error-entry shape `Entry::from_error` builds; [`validate_entry_outcome`]
    /// rejects an entry that sets both. Absent on a cassette written before
    /// this field existed (version 1), which loads every entry as `None` —
    /// exactly the old "record nothing for an Err" behavior. See
    /// [`CassetteError`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<CassetteError>,
}

/// A recorded [`Error`] discriminant + payload, so replaying the same
/// invocation raises the *same error* the recording run did, instead of the
/// call silently falling through to a plain [`Entry`] read (or, before this
/// existed, missing the cassette entirely with a misleading
/// [`Error::CassetteMiss`]).
///
/// Deliberately **not** every [`Error`] variant: only the ones the raw
/// [`ProcessRunner::output_string`]/[`ProcessRunner::start`] seam can actually
/// return in record mode land here. A variant produced by a *checking* verb
/// layered over an otherwise-successful [`ProcessResult`] —
/// [`Exit`](Error::Exit), [`Timeout`](Error::Timeout),
/// [`Signalled`](Error::Signalled) — is already reproduced through the
/// existing `code`/`timed_out`/`signal` fields and never needs this; only a
/// call that returned no [`ProcessResult`] at all does.
///
/// [`Cancelled`](Error::Cancelled) is deliberately **excluded** (never
/// recorded, like the pre-this-task "record nothing" behavior): replay
/// already short-circuits a *replaying* command's own cancelled token before
/// ever consulting the cassette (mirroring the live runner's pre-spawn
/// check), so a recorded `Cancelled` entry could only ever be wrong — served
/// to a replaying call that never asked to be cancelled.
///
/// Any other variant this schema doesn't model precisely (a future
/// [`Error`] addition, or one not listed above) still lands here via
/// [`Other`](CassetteError::Other), carrying its `Display` message — so an
/// `Err` is always reproduced as *some* error, never silently dropped back to
/// a [`Error::CassetteMiss`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum CassetteError {
    /// [`Error::Spawn`]: the child could not be started.
    Spawn {
        /// The OS error's [`std::io::ErrorKind`], named — see
        /// [`io_kind_name`]/[`io_kind_from_name`].
        os_kind: String,
        /// The OS error's `Display` text.
        message: String,
    },
    /// [`Error::NotFound`]: the program could not be located.
    NotFound {
        /// The `PATH` directories searched, joined — see
        /// [`Error::NotFound`]'s `searched` field. Never logged elsewhere;
        /// stored here exactly like the rest of a cassette (verbatim,
        /// reviewed before committing).
        searched: Option<String>,
    },
    /// [`Error::Stdin`]: feeding the child's stdin failed for a reason other
    /// than a routine broken pipe.
    Stdin {
        /// See [`Spawn`](CassetteError::Spawn)'s `os_kind`.
        os_kind: String,
        /// The OS error's `Display` text.
        message: String,
    },
    /// [`Error::OutputTooLarge`]: the captured output exceeded its ceiling.
    OutputTooLarge {
        max_lines: Option<usize>,
        max_bytes: Option<usize>,
        total_lines: usize,
        total_bytes: usize,
    },
    /// [`Error::Unsupported`]: the operation is not supported on this
    /// platform/mechanism.
    Unsupported {
        /// A short description of the unsupported operation.
        operation: String,
    },
    /// [`Error::Io`]: a low-level IO error from the crate's own machinery.
    Io {
        /// See [`Spawn`](CassetteError::Spawn)'s `os_kind`.
        os_kind: String,
        /// The error's `Display` text.
        message: String,
    },
    /// Any other `Error` variant, kept only as a `Display` message. Replays
    /// as [`Error::Io`] with [`std::io::ErrorKind::Other`] — not the original
    /// variant, but still a loud, informative `Err` rather than a silent
    /// cassette miss.
    Other {
        /// The original error's `Display` text.
        message: String,
    },
}

impl CassetteError {
    /// Capture the inner runner's `Err` for the cassette, or `None` for
    /// [`Error::Cancelled`] (see the type doc — deliberately never recorded).
    fn from_error(err: &Error) -> Option<Self> {
        Some(match err {
            Error::Cancelled { .. } => return None,
            Error::Spawn { source, .. } => CassetteError::Spawn {
                os_kind: io_kind_name(source.kind()).to_owned(),
                message: source.to_string(),
            },
            Error::NotFound { searched, .. } => CassetteError::NotFound {
                searched: searched.clone(),
            },
            Error::Stdin { source, .. } => CassetteError::Stdin {
                os_kind: io_kind_name(source.kind()).to_owned(),
                message: source.to_string(),
            },
            Error::OutputTooLarge {
                max_lines,
                max_bytes,
                total_lines,
                total_bytes,
                ..
            } => CassetteError::OutputTooLarge {
                max_lines: *max_lines,
                max_bytes: *max_bytes,
                total_lines: *total_lines,
                total_bytes: *total_bytes,
            },
            Error::Unsupported { operation } => CassetteError::Unsupported {
                operation: operation.clone(),
            },
            Error::Io(source) => CassetteError::Io {
                os_kind: io_kind_name(source.kind()).to_owned(),
                message: source.to_string(),
            },
            other => CassetteError::Other {
                message: other.to_string(),
            },
        })
    }

    /// Reconstruct the [`Error`] this cassette error stands for, attributed to
    /// `program` (the replaying command's own [`Command::program_name`]).
    fn to_error(&self, program: &str) -> Error {
        match self {
            CassetteError::Spawn { os_kind, message } => Error::Spawn {
                program: program.to_owned(),
                source: std::io::Error::new(io_kind_from_name(os_kind), message.clone()),
            },
            CassetteError::NotFound { searched } => Error::NotFound {
                program: program.to_owned(),
                searched: searched.clone(),
            },
            CassetteError::Stdin { os_kind, message } => Error::Stdin {
                program: program.to_owned(),
                source: std::io::Error::new(io_kind_from_name(os_kind), message.clone()),
            },
            CassetteError::OutputTooLarge {
                max_lines,
                max_bytes,
                total_lines,
                total_bytes,
            } => Error::OutputTooLarge {
                program: program.to_owned(),
                max_lines: *max_lines,
                max_bytes: *max_bytes,
                total_lines: *total_lines,
                total_bytes: *total_bytes,
            },
            CassetteError::Unsupported { operation } => Error::Unsupported {
                operation: operation.clone(),
            },
            CassetteError::Io { os_kind, message } => Error::Io(std::io::Error::new(
                io_kind_from_name(os_kind),
                message.clone(),
            )),
            CassetteError::Other { message } => Error::Io(std::io::Error::other(message.clone())),
        }
    }
}

/// Name an [`std::io::ErrorKind`] for the cassette text fixture — only the
/// kinds this crate's own error sites actually construct (see
/// `is_transient_io`/spawn/cwd-validation in `runner.rs` and `error.rs`), so
/// the classifiers built on it ([`Error::is_transient`],
/// [`Error::is_permission_denied`]) still work after a round trip. Any other
/// kind falls back to `"Other"` (matching [`io_kind_from_name`]'s fallback),
/// which loses only the exact kind, never the message.
fn io_kind_name(kind: std::io::ErrorKind) -> &'static str {
    use std::io::ErrorKind as K;
    match kind {
        K::NotFound => "NotFound",
        K::PermissionDenied => "PermissionDenied",
        K::Interrupted => "Interrupted",
        K::WouldBlock => "WouldBlock",
        K::InvalidInput => "InvalidInput",
        K::InvalidData => "InvalidData",
        K::TimedOut => "TimedOut",
        K::WriteZero => "WriteZero",
        K::UnexpectedEof => "UnexpectedEof",
        K::ResourceBusy => "ResourceBusy",
        K::ExecutableFileBusy => "ExecutableFileBusy",
        K::NotADirectory => "NotADirectory",
        K::BrokenPipe => "BrokenPipe",
        K::AlreadyExists => "AlreadyExists",
        _ => "Other",
    }
}

/// The inverse of [`io_kind_name`]; an unrecognized name (e.g. a kind this
/// build doesn't name, or `"Other"` itself) decodes as
/// [`std::io::ErrorKind::Other`].
fn io_kind_from_name(name: &str) -> std::io::ErrorKind {
    use std::io::ErrorKind as K;
    match name {
        "NotFound" => K::NotFound,
        "PermissionDenied" => K::PermissionDenied,
        "Interrupted" => K::Interrupted,
        "WouldBlock" => K::WouldBlock,
        "InvalidInput" => K::InvalidInput,
        "InvalidData" => K::InvalidData,
        "TimedOut" => K::TimedOut,
        "WriteZero" => K::WriteZero,
        "UnexpectedEof" => K::UnexpectedEof,
        "ResourceBusy" => K::ResourceBusy,
        "ExecutableFileBusy" => K::ExecutableFileBusy,
        "NotADirectory" => K::NotADirectory,
        "BrokenPipe" => K::BrokenPipe,
        "AlreadyExists" => K::AlreadyExists,
        _ => K::Other,
    }
}

/// The match-key fields (`program`/`args`/`stdin_digest`) plus the
/// visibility-only ones (`cwd`/`has_stdin`/`env_names`) both [`Entry`]
/// constructors derive the same way — see [`Entry::key_fields`]. `cwd` rides
/// along here for storage, not matching (see [`Key`]'s doc).
struct KeyFields {
    program: String,
    args: Vec<String>,
    cwd: Option<String>,
    stdin_digest: Option<u64>,
    has_stdin: bool,
    env_names: Vec<String>,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // signature dictated by serde
fn is_false(b: &bool) -> bool {
    !*b
}

#[allow(clippy::trivially_copy_pass_by_ref)] // signature dictated by serde
fn is_zero_usize(n: &usize) -> bool {
    *n == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)] // signature dictated by serde
fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

/// Write `json` to `path`, restricting the file to owner-only (`0600`) on Unix.
///
/// A cassette redacts env *values* (it stores names only), but argv, cwd,
/// stdout, and stderr are stored **verbatim** — any of which can carry a secret.
/// So the file is created owner-only rather than inheriting a world-readable
/// umask.
///
/// On Unix the open also refuses to follow a symlink at `path` (`O_NOFOLLOW`),
/// so a planted `cassette.json` symlink can't redirect the secret-bearing write
/// (and the `0600`) onto the link's target — it fails loud (`ELOOP`) instead. On
/// Windows the file inherits the directory ACL (the unit of access control
/// there); **restrict the containing directory** (or use a per-user temp dir,
/// not a world-writable shared one) if the fixture can carry secrets.
fn write_cassette(path: &Path, json: &str) -> std::io::Result<()> {
    // Write to a sibling temp file, then atomically `rename` it over the target,
    // so a crash / interrupted write can never truncate or destroy an existing
    // good cassette — the old file survives intact until the rename swaps in the
    // fully-written new one. The temp shares the target's directory so the rename
    // stays on one filesystem (a cross-device rename is not atomic).
    // Defense-in-depth alert: refuse a symlinked cassette path (`O_NOFOLLOW` on
    // the temp can't see the target). The rename below is safe regardless — it
    // replaces the link, never writes *through* it to the secret-bearing target —
    // but a symlink at a cassette path is suspicious, so fail loud (`ELOOP`).
    #[cfg(unix)]
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(std::io::Error::from_raw_os_error(libc::ELOOP));
    }
    let tmp = tmp_sibling(path);
    // Clear a stale temp left by a prior crashed run (a recycled pid could leave
    // one behind) so `create_new` below doesn't spuriously fail; removing a
    // symlink here drops the link, not its target.
    let _ = std::fs::remove_file(&tmp);
    let written = write_new_file(&tmp, json);
    match written.and_then(|()| std::fs::rename(&tmp, path)) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Best-effort cleanup of the temp file; the original cassette (if any)
            // is untouched.
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// A sibling temp path in the same directory as `path` (so a later `rename` is
/// same-filesystem/atomic). The pid disambiguates one process's temp from
/// another's; concurrent recorders to *one* cassette are still unsupported.
fn tmp_sibling(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{}.tmp", std::process::id()));
    std::path::PathBuf::from(name)
}

/// Create-and-write a brand-new file at `path` (owner-only on Unix), fsync'd so
/// the content is durable before the caller renames it into place.
fn write_new_file(path: &Path, json: &str) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        // `create_new` + `O_NOFOLLOW`: a fresh owner-only (`0600`) file — no
        // symlink to follow, no pre-existing perms to inherit. `set_permissions`
        // tightens even if a restrictive umask were somehow looser.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?; // durable before the rename swaps it in
        Ok(())
    }
    #[cfg(not(unix))]
    {
        use std::io::Write;
        // `create_new` so a planted temp can't be written through; the target
        // directory's ACL governs access on Windows (see the type doc).
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}

impl Entry {
    /// The match-key and visibility-only fields shared by both entry shapes
    /// (successful-call and `Err`-call) — everything [`from_parts`](Self::from_parts)
    /// and [`from_error`](Self::from_error) build identically, so the two
    /// constructors can't drift on how the key is derived.
    fn key_fields(invocation: &Invocation, stdin_digest: Option<u64>) -> KeyFields {
        let mut env_names: Vec<String> = invocation
            .envs
            .iter()
            .map(|(name, _value)| name.to_string_lossy().into_owned())
            .collect();
        // Sorted + deduped: stable diffs, and repeated overrides of one var
        // are one fact ("this var shaped the run"), not a sequence.
        env_names.sort();
        env_names.dedup();
        KeyFields {
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
            stdin_digest,
            has_stdin: invocation.has_stdin,
            env_names,
        }
    }

    /// Capture one record-mode call. Lossy UTF-8 throughout — see the type doc.
    fn from_parts(
        invocation: &Invocation,
        result: &ProcessResult<String>,
        stdin_digest: Option<u64>,
    ) -> Self {
        let key = Self::key_fields(invocation, stdin_digest);
        Self {
            program: key.program,
            args: key.args,
            cwd: key.cwd,
            stdin_digest: key.stdin_digest,
            has_stdin: key.has_stdin,
            env_names: key.env_names,
            stdout: result.stdout().clone(),
            stderr: result.stderr().to_owned(),
            code: result.code(),
            timed_out: result.timed_out(),
            // Exhaustive (no wildcard) so a future `Outcome` variant is a compile
            // error here rather than silently recorded as "no signal" (H2).
            signal: match result.outcome() {
                Outcome::Signalled(s) => s,
                Outcome::Exited(_) | Outcome::TimedOut => None,
            },
            truncated: result.truncated(),
            total_lines: result.total_lines(),
            total_bytes: result.total_bytes(),
            duration_ms: result.duration().as_millis() as u64,
            error: None,
        }
    }

    /// Capture one record-mode call that returned `Err` instead of a
    /// [`ProcessResult`] — the match key is derived exactly like
    /// [`from_parts`](Self::from_parts), but every output field is left at its
    /// default and `error` carries the recorded [`CassetteError`].
    fn from_error(
        invocation: &Invocation,
        stdin_digest: Option<u64>,
        error: CassetteError,
    ) -> Self {
        let key = Self::key_fields(invocation, stdin_digest);
        Self {
            program: key.program,
            args: key.args,
            cwd: key.cwd,
            stdin_digest: key.stdin_digest,
            has_stdin: key.has_stdin,
            env_names: key.env_names,
            stdout: String::new(),
            stderr: String::new(),
            code: None,
            timed_out: false,
            signal: None,
            truncated: false,
            total_lines: 0,
            total_bytes: 0,
            duration_ms: 0,
            error: Some(error),
        }
    }

    /// Rebuild the recorded [`ProcessResult`] (shared by both replay verbs), so
    /// the truncation/overflow/duration signals recorded at capture time survive
    /// replay. `timeout` is the *replaying command's* configuration, re-read like
    /// the live runner (never stored on the entry).
    fn to_result(
        &self,
        timeout: Option<std::time::Duration>,
        ok_codes: Vec<i32>,
    ) -> ProcessResult<String> {
        let outcome = match (self.code, self.timed_out) {
            (_, true) => Outcome::TimedOut,
            (Some(code), false) => Outcome::Exited(code),
            (None, false) => Outcome::Signalled(self.signal),
        };
        ProcessResult::new(
            self.program.clone(),
            self.stdout.clone(),
            self.stderr.clone(),
            outcome,
            timeout,
        )
        .with_ok_codes(ok_codes)
        .with_truncated(self.truncated)
        .with_overflow_totals(self.total_lines, self.total_bytes)
        .with_duration(std::time::Duration::from_millis(self.duration_ms))
    }
}

/// What an invocation is matched on: program + args + the stdin source digest
/// (content for in-memory bytes, path for a `from_file` source). Env overrides
/// are excluded — deliberately, so an *irrelevant* env difference between the
/// record and replay environments can't cause a spurious miss — **but this is
/// a real collision risk when the env difference is NOT irrelevant**: two
/// invocations that differ only by an env override that actually changes the
/// tool's behavior/output collide on one entry (see
/// [`RecordReplayRunner`]'s "Env is not part of the match key" for the
/// workarounds; there is no opt-in env-keying knob today). `cwd` is excluded
/// too — see the type doc's "Portability of the match key" /
/// [`CASSETTE_VERSION`]'s `3` bump — so a cassette recorded in one absolute
/// working directory still matches an otherwise-identical invocation run from
/// a different one.
///
/// The string components are *lossy* UTF-8 decodes, so two distinct non-UTF-8
/// invocations that differ only in their invalid bytes produce the same key and
/// collide on replay (the first recorded one answers for both). Accepted: keying
/// on raw bytes would defeat the human-diffable text fixture, and valid-UTF-8
/// invocations (the common case) never collide.
type Key = (String, Vec<String>, bool, Option<u64>);

/// The stdin source digest keyed into a cassette match — `None` for an
/// empty/absent stdin. The digest never persists the stdin payload: in-memory
/// bytes hash their content, a `from_file` source hashes its path.
fn stdin_digest_of(command: &Command) -> Option<u64> {
    command
        .stdin_source()
        .filter(|s| !s.is_empty())
        .map(|s| s.content_digest())
}

/// Reject a one-shot streaming stdin source (`from_reader`/`from_lines`) in
/// record/replay. Such a source's bytes are consumed lazily and never captured
/// into the match key — `content_digest` can only hash a constant discriminant
/// for them — so two invocations differing *only* in streamed stdin would
/// collide on one cassette key and silently replay each other's recording.
/// Failing loud is safer than a silent wrong answer; use a replayable source
/// (`from_bytes`/`from_string`/`from_file`) for a recordable invocation. Applies
/// to both verbs, `output_string` and `start`.
fn reject_unrecordable_stdin(command: &Command) -> Result<()> {
    if command.stdin_source().is_some_and(|s| s.is_one_shot()) {
        return Err(Error::Unsupported {
            operation: "cassette record/replay with one-shot streaming stdin \
                        (from_reader/from_lines); use from_bytes/from_string/from_file"
                .to_string(),
        });
    }
    Ok(())
}

/// The key of a live invocation — must decode exactly like
/// [`key_of_entry`] (both sides go through the same lossy conversion). The
/// `stdin_digest` is computed from the command, not carried on the
/// [`Invocation`] (which records only *whether* stdin was supplied). The
/// `has_stdin` bool is keyed alongside the digest so an older entry that loads
/// `stdin_digest: None` regardless of its stored `has_stdin` cannot match a
/// no-stdin replay — only miss. `invocation.cwd` is deliberately **not**
/// included — see [`Key`]'s doc.
fn key_of(invocation: &Invocation, stdin_digest: Option<u64>) -> Key {
    (
        invocation.program.to_string_lossy().into_owned(),
        invocation
            .args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect(),
        invocation.has_stdin,
        stdin_digest,
    )
}

/// The key of a stored entry (already lossy strings).
fn key_of_entry(entry: &Entry) -> Key {
    (
        entry.program.clone(),
        entry.args.clone(),
        entry.has_stdin,
        entry.stdin_digest,
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
/// **Record** mode wraps a real inner runner, captures each completed call's
/// invocation and result, and writes the cassette on [`save`](Self::save) (or
/// best-effort on drop). Non-zero exits and captured timeouts are results and
/// are recorded as such. An `Err` the inner runner returns instead — a spawn
/// failure, a missing program, … — is recorded too (as a discriminant + payload;
/// an internal, non-public schema detail — not every `Error` variant is
/// representable, an unmodeled one falls back to its `Display` text),
/// **except** [`Error::Cancelled`], which is never recorded (a caller-driven
/// cancellation isn't a fact about the invocation to replay). Either way the
/// real error still propagates to the record-mode caller unchanged.
///
/// **Replay** mode loads the cassette and serves results without spawning:
///
/// - **Matching**: program + args + stdin source digest — **not** `cwd`, so a
///   cassette recorded in one absolute working directory replays against an
///   otherwise-identical invocation run from a different one (see the type
///   doc's "Portability of the match key"). Env override *values* are never
///   written — only sorted variable names. Everything else (argv, cwd, stdout,
///   stderr) is stored verbatim, so review fixtures before committing. File is
///   written owner-only (`0600`) on Unix.
/// - **Env is not part of the match key, at all — not even as variable names.**
///   Two invocations of the same program+args+stdin that differ *only* in an
///   env override (`LC_ALL=C` vs `LC_ALL=en_US`, a feature-flag env var that
///   actually changes the tool's output, …) collide on one cassette entry: the
///   first one recorded silently answers for both on replay. This is a real
///   collision risk, not just a documented quirk — if your invocations vary
///   meaningfully by env, either (a) fold the env-sensitive knob into the
///   command's **args** instead (part of the key), (b) record each env variant
///   into its **own cassette file**, or (c) drop to a
///   [`ScriptedRunner`](crate::testing::ScriptedRunner) rule keyed on a
///   predicate that inspects [`Command::env_overrides`] directly. There is
///   currently no opt-in "key on these env names too" knob — env-keying was
///   considered and deferred (see `ideas/v2-breaking-changes.md`, D10) in favor
///   of this explicit warning, since no in-tree consumer has needed it yet.
/// - **Duplicates** replay in capture order, then the last entry repeats.
/// - **A miss is [`Error::CassetteMiss`]** (not `is_not_found()`): never a
///   surprise subprocess. A **recorded `Err`** replays as that same `Error`
///   rather than a result — this is a genuine behavioral difference from
///   cassettes written before this existed, where such an invocation instead
///   missed the cassette entirely.
/// - The replayed result carries the *replaying* command's
///   [`timeout`](Command::timeout), so a recorded timed-out run surfaces as
///   [`Error::Timeout`](crate::Error::Timeout) with the real deadline.
/// - Covers the **text and streaming verbs**: `output_string` replays the
///   captured result, and [`start`](crate::ProcessRunner::start) replays the
///   recorded output through a scripted [`RunningProcess`](crate::RunningProcess)
///   (its lines flow through the command's real pumps — `stdout_lines` /
///   `wait_for_line` / `finish` — with no subprocess). A cassette is verb-agnostic:
///   record through either, replay through either. **Record-side caveat:**
///   recording a `start` captures the run *whole* — the recording call drives the
///   child to completion (via the inner runner's `output_string`) before returning
///   the handle, so an **interactive** streaming run that must be fed stdin
///   mid-stream can't be recorded this way (it would block waiting for input that
///   never comes; bound it with a [`Command::timeout`](crate::Command::timeout), or
///   script it with a [`ScriptedRunner`](crate::testing::ScriptedRunner) instead).
/// - **The runner's `output_bytes` verb is unsupported**
///   ([`Error::Unsupported`](crate::Error::Unsupported)) in both modes: a cassette
///   stores lossy-UTF-8 text and cannot reproduce the exact raw bytes that verb
///   promises — capture bytes from a real or scripted runner. (This guards the
///   convenient default route, which would otherwise re-encode the recorded text
///   to bytes through `start`; a streaming handle you obtain from `start` will
///   still re-encode on its own `output_bytes` — the same lossy bytes, not the
///   original.)
///
/// Non-UTF-8 programs/args/paths are stored lossily; both sides apply the same
/// conversion, so matching still works. Two distinct non-UTF-8 invocations that
/// differ only in invalid bytes share the same key and collide on replay.
///
/// [`save`](Self::save) is the explicit write; drop flushes best-effort except
/// while unwinding (a panic never silently persists a cassette).
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
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the recorded entries cannot be serialized to JSON, or if
    /// writing the cassette file fails. In replay mode there is nothing to write,
    /// so it returns `Ok(())`.
    ///
    /// # Panics
    ///
    /// Panics if the cassette's internal mutex is poisoned — which happens only
    /// if a prior operation panicked while holding it. No user code runs under
    /// this lock, so poisoning is a crate bug, never reachable from any caller
    /// input.
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
        // cassette locks, so poisoning is a logic bug worth failing loudly on.
        let entries = recorded.lock().expect("cassette mutex poisoned");
        let cassette = Cassette {
            version: CASSETTE_VERSION,
            entries: entries.clone(),
        };
        let json = serde_json::to_string_pretty(&cassette)
            .map_err(|e| Error::Io(std::io::Error::from(e)))?;
        write_cassette(path, &json).map_err(Error::Io)?;
        dirty.store(false, Ordering::SeqCst);
        Ok(())
    }
}

/// Reject a cassette entry whose outcome fields *contradict* each other.
/// The decode model is: an `error` entry replays as that `Err` and carries no
/// outcome at all; otherwise `timed_out` → `TimedOut`; else `code` present →
/// `Exited`; else → `Signalled(signal)` (with `signal` optionally absent, i.e.
/// "killed, signal unknown"). So an `error` entry must set none of
/// `code`/`timed_out`/`signal`, and an outcome entry (`error: None`) may set at
/// most one of them — an entry that sets two or more (e.g. both `code` and
/// `signal`), or both `error` and an outcome indicator, is malformed: the
/// decoder would silently pick one and drop the rest. Fail loud on load, like
/// an unknown `version` does. (An outcome entry that sets *none* is the
/// legitimate `Signalled(None)` and is allowed.)
fn validate_entry_outcome(entry: &Entry) -> Result<()> {
    let indicators = usize::from(entry.code.is_some())
        + usize::from(entry.timed_out)
        + usize::from(entry.signal.is_some());
    if entry.error.is_some() {
        if indicators > 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cassette entry for `{}` carries both a recorded `error` and an outcome \
                     indicator (`code`/`timed_out`/`signal`) — at most one may be set",
                    entry.program
                ),
            )));
        }
        return Ok(());
    }
    if indicators > 1 {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "cassette entry for `{}` has a contradictory outcome: at most one of \
                 `code` (exited), `timed_out`, or `signal` (signalled) may be set — found {indicators}",
                entry.program
            ),
        )));
    }
    Ok(())
}

impl RecordReplayRunner<JobRunner> {
    /// Load the cassette at `path` and serve its entries hermetically — no
    /// subprocess is ever spawned in replay mode.
    ///
    /// # Errors
    ///
    /// Always [`Error::Io`]: a missing file keeps its `NotFound` kind; a corrupt
    /// file, a contradictory entry, an unknown format `version`, or a cassette
    /// over the 64 MiB size limit is `InvalidData`.
    pub fn replay(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        const MAX_CASSETTE_BYTES: u64 = 64 << 20; // 64 MiB
        if let Ok(meta) = std::fs::metadata(path)
            && meta.len() > MAX_CASSETTE_BYTES
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cassette is {} bytes, over the {MAX_CASSETTE_BYTES}-byte limit",
                    meta.len()
                ),
            )));
        }
        let text = std::fs::read_to_string(path).map_err(Error::Io)?;
        // Gate on `version` BEFORE attempting the full `Cassette` decode: every
        // schema bump to date has been additive (a new `#[serde(default)]`
        // field), so this build's `Entry` shape happens to still parse an older
        // cassette — but that is not guaranteed for a *future*, not-yet-understood
        // version, whose entries could carry a field this build's `Entry` can't
        // decode at all (a type change, not just an addition). Deserializing the
        // whole `Cassette` first would then surface a confusing raw serde
        // type-mismatch error instead of the clear "version N is not supported"
        // one below. `CassetteHeader` only names `version` — serde silently skips
        // every other (unknown-shape-tolerant) field — so this first pass can
        // never fail on an entries shape this build doesn't understand.
        #[derive(Deserialize)]
        struct CassetteHeader {
            version: u32,
        }
        let header: CassetteHeader =
            serde_json::from_str(&text).map_err(|e| Error::Io(std::io::Error::from(e)))?;
        if header.version > CASSETTE_VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cassette version {} is not supported (this build reads up to version {CASSETTE_VERSION})",
                    header.version
                ),
            )));
        }
        let cassette: Cassette =
            serde_json::from_str(&text).map_err(|e| Error::Io(std::io::Error::from(e)))?;
        let mut slots: HashMap<Key, ReplaySlot> = HashMap::new();
        for entry in cassette.entries {
            validate_entry_outcome(&entry)?;
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
    async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
        reject_unrecordable_stdin(command)?;
        match &self.mode {
            Mode::Record {
                inner,
                recorded,
                dirty,
                ..
            } => {
                let invocation = Invocation::from_command(command);
                let stdin_digest = stdin_digest_of(command);
                match inner.output_string(command).await {
                    Ok(result) => {
                        let mut entries = recorded.lock().expect("cassette mutex poisoned");
                        entries.push(Entry::from_parts(&invocation, &result, stdin_digest));
                        dirty.store(true, Ordering::SeqCst);
                        Ok(result)
                    }
                    Err(err) => {
                        // Record the `Err` too (see `CassetteError`), so replay
                        // reproduces it instead of missing the cassette; still
                        // surface the real error to this record-mode caller.
                        if let Some(cassette_err) = CassetteError::from_error(&err) {
                            let mut entries = recorded.lock().expect("cassette mutex poisoned");
                            entries.push(Entry::from_error(
                                &invocation,
                                stdin_digest,
                                cassette_err,
                            ));
                            dirty.store(true, Ordering::SeqCst);
                        }
                        Err(err)
                    }
                }
            }
            Mode::Replay { slots } => {
                // Cancellation is terminal on every path — mirror the real
                // runner's pre-spawn short-circuit so replay-driven tests see the
                // same `Cancelled` a live run would, rather than a recorded `Ok` (D2).
                if let Some(token) = command.cancel_token()
                    && token.is_cancelled()
                {
                    return Err(Error::Cancelled {
                        program: command.program_name(),
                    });
                }
                // A capture verb on `stdout(Inherit/Null)` has nothing to read —
                // the real runner and the scripted double both reject it, and the
                // cassette's own `start` replay does too (it carries `stdout_piped`);
                // reject it here so the two replay arms stay symmetric and a config
                // mistake isn't masked by a recorded capture (D9).
                if !command.stdout_is_piped() {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "`{}`: stdout is not piped (Command::stdout was set to \
                             Inherit/Null), so the capture verbs have nothing to read — \
                             use StdioMode::Piped to capture it",
                            command.program_name()
                        ),
                    )));
                }
                let invocation = Invocation::from_command(command);
                let stdin_digest = stdin_digest_of(command);
                // Release the lock before invoking line handlers — a handler that
                // re-enters this replayer would otherwise deadlock.
                let entry = {
                    let mut slots = slots.lock().expect("cassette mutex poisoned");
                    let Some(slot) = slots.get_mut(&key_of(&invocation, stdin_digest)) else {
                        return Err(Error::CassetteMiss {
                            program: command.program_name(),
                        });
                    };
                    slot.play().clone()
                };
                // A recorded `Err` reproduces as that same `Error`, not a result.
                if let Some(cassette_err) = &entry.error {
                    return Err(cassette_err.to_error(&entry.program));
                }
                crate::doubles::replay_line_handlers(command, &entry.stdout, &entry.stderr);
                Ok(entry.to_result(command.configured_timeout(), command.ok_codes_vec()))
            }
        }
    }

    /// Unsupported on a cassette in **either** mode. A cassette is a lossy-UTF-8
    /// text fixture (`stdout`/`stderr` are stored as `String`), so it can neither
    /// record nor replay the *exact* bytes `output_bytes` promises — for a binary
    /// tool the raw bytes were already mangled to `U+FFFD` at record time. Failing
    /// loud here keeps that contract honest rather than handing back silently-lossy
    /// bytes through the defaulted `start` path; capture bytes from a real or
    /// scripted runner instead.
    async fn output_bytes(&self, _command: &Command) -> Result<ProcessResult<Vec<u8>>> {
        Err(Error::Unsupported {
            operation: "output_bytes on a cassette (a lossy-UTF-8 text fixture cannot \
                        reproduce exact bytes; capture them from a real or scripted runner)"
                .to_string(),
        })
    }

    async fn start(&self, command: &Command) -> Result<crate::RunningProcess> {
        reject_unrecordable_stdin(command)?;
        match &self.mode {
            Mode::Record {
                inner,
                recorded,
                dirty,
                ..
            } => {
                // Record a streaming run by capturing it whole — the real child
                // via the inner runner's `output_string` — then hand back a
                // scripted handle that replays the captured output through the
                // command's real pumps. The stored `Entry` is byte-identical to
                // the one `output_string` records, so a cassette is verb-agnostic:
                // record through either verb, replay through either.
                //
                // The capture-whole trade-off on *record*: the child runs to
                // completion before this returns, so a run that must be fed stdin
                // *mid-stream* (interactive streaming) cannot be recorded this way
                // — script those with a [`ScriptedRunner`](crate::testing::ScriptedRunner)
                // instead. *Replay* has no such limit (it never spawns).
                //
                // Capture with the per-line handlers/tees stripped: the scripted
                // handle returned below carries the caller's handlers and fires them
                // once when consumed (as a live `start` would), so this capture pass
                // must stay silent or every handler/tee would fire twice.
                let invocation = Invocation::from_command(command);
                let stdin_digest = stdin_digest_of(command);
                match inner
                    .output_string(&command.without_line_side_effects())
                    .await
                {
                    Ok(result) => {
                        let entry = Entry::from_parts(&invocation, &result, stdin_digest);
                        {
                            let mut entries = recorded.lock().expect("cassette mutex poisoned");
                            entries.push(entry.clone());
                            dirty.store(true, Ordering::SeqCst);
                        }
                        Ok(crate::doubles::scripted_running_from_parts(
                            command,
                            entry.stdout,
                            entry.stderr,
                            entry.code,
                            entry.timed_out,
                            entry.signal,
                            entry.truncated,
                            entry.total_lines,
                            entry.total_bytes,
                            std::time::Duration::from_millis(entry.duration_ms),
                        ))
                    }
                    Err(err) => {
                        // Record the `Err` too (see `CassetteError`), so replay
                        // reproduces it instead of missing the cassette; still
                        // surface the real error to this record-mode caller.
                        if let Some(cassette_err) = CassetteError::from_error(&err) {
                            let mut entries = recorded.lock().expect("cassette mutex poisoned");
                            entries.push(Entry::from_error(
                                &invocation,
                                stdin_digest,
                                cassette_err,
                            ));
                            dirty.store(true, Ordering::SeqCst);
                        }
                        Err(err)
                    }
                }
            }
            Mode::Replay { slots } => {
                // Cancellation is terminal — mirror the real runner's pre-spawn
                // short-circuit (D2), matching `output_string`'s replay arm.
                if let Some(token) = command.cancel_token()
                    && token.is_cancelled()
                {
                    return Err(Error::Cancelled {
                        program: command.program_name(),
                    });
                }
                let invocation = Invocation::from_command(command);
                let stdin_digest = stdin_digest_of(command);
                let entry = {
                    let mut slots = slots.lock().expect("cassette mutex poisoned");
                    let Some(slot) = slots.get_mut(&key_of(&invocation, stdin_digest)) else {
                        return Err(Error::CassetteMiss {
                            program: command.program_name(),
                        });
                    };
                    slot.play().clone()
                };
                // A recorded `Err` reproduces as that same `Error`, not a
                // scripted running handle.
                if let Some(cassette_err) = &entry.error {
                    return Err(cassette_err.to_error(&entry.program));
                }
                // The recorded output flows through the command's real pumps on a
                // scripted handle: `stdout_lines` / `wait_for_line` / `finish`
                // behave as on a live child, with no subprocess.
                Ok(crate::doubles::scripted_running_from_parts(
                    command,
                    entry.stdout,
                    entry.stderr,
                    entry.code,
                    entry.timed_out,
                    entry.signal,
                    entry.truncated,
                    entry.total_lines,
                    entry.total_bytes,
                    std::time::Duration::from_millis(entry.duration_ms),
                ))
            }
        }
    }
}

// Manual: no `R: Debug` bound; entries/slots are summarized as counts.
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
        // Best-effort flush; skip while unwinding so a panic never silently
        // persists a cassette that may carry secrets in argv/stdout.
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

    #[cfg(unix)]
    #[test]
    fn write_cassette_refuses_to_follow_a_symlink() {
        let dir = tempfile::tempdir().expect("temp dir");
        let target = dir.path().join("victim.txt");
        std::fs::write(&target, "original").expect("seed victim");
        let link = dir.path().join("cassette.json");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");

        let err = write_cassette(&link, "{\"secret\":true}")
            .expect_err("writing through a symlink must fail (O_NOFOLLOW)");
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ELOOP),
            "O_NOFOLLOW on a symlink yields ELOOP, got {err:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&target).expect("read victim"),
            "original",
            "the victim file must be untouched"
        );
    }

    #[tokio::test]
    async fn round_trip_is_identical() {
        let (_dir, path) = temp_cassette();

        let recorder = RecordReplayRunner::record(&path, scripted());
        let ok = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record ok run");
        let fail = recorder
            .output_string(&Command::new("tool").arg("fail"))
            .await
            .expect("record failing run (non-zero exit is a result, not Err)");
        recorder.save().expect("save cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let ok2 = replayer
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("replay ok run");
        let fail2 = replayer
            .output_string(&Command::new("tool").arg("fail"))
            .await
            .expect("replay failing run");
        assert_eq!(ok, ok2, "replay must be identical to the recording");
        assert_eq!(fail, fail2);
        assert_eq!(fail2.code(), Some(7));
        assert_eq!(fail2.stderr(), "boom");
    }

    #[tokio::test]
    async fn start_records_then_replays_a_streaming_run() {
        // The streaming verb round-trips like the bulk one: record a `start` run
        // (captured whole), then replay it through `start` — the recorded output
        // flows through the command's real pumps on a scripted handle, with no
        // subprocess. Closes the gap where `start` used to return `Unsupported`.
        let (_dir, path) = temp_cassette();
        let inner = ScriptedRunner::new().on(
            ["server", "--watch"],
            Reply::lines(["starting", "listening on :8080", "ready"]),
        );

        let recorder = RecordReplayRunner::record(&path, inner);
        let mut run = recorder
            .start(&Command::new("server").arg("--watch"))
            .await
            .expect("record start");
        let line = run
            .wait_for_line(|l| l.contains("listening"), Duration::from_secs(5))
            .await
            .expect("readiness line during record");
        assert_eq!(line, "listening on :8080");
        assert_eq!(run.wait().await.expect("record finish"), Outcome::Exited(0));
        recorder.save().expect("save cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let mut replayed = replayer
            .start(&Command::new("server").arg("--watch"))
            .await
            .expect("replay start");
        assert_eq!(replayed.pid(), None, "a replayed handle has no OS identity");
        let line2 = replayed
            .wait_for_line(|l| l.contains("listening"), Duration::from_secs(5))
            .await
            .expect("readiness line during replay");
        assert_eq!(line2, "listening on :8080");
        assert_eq!(
            replayed.wait().await.expect("replay finish"),
            Outcome::Exited(0),
            "replay must reproduce the recorded outcome through the streaming path"
        );
    }

    #[tokio::test]
    async fn start_record_fires_line_side_effects_exactly_once() {
        // Record-mode `start` captures the run whole AND hands back a scripted
        // handle. The caller's per-line side-effects — BOTH `on_stdout_line`
        // handlers and `stdout_tee` sinks — must fire once (on consume), not twice
        // (once for the internal capture, once for the scripted replay). The
        // capture pass runs on a `without_line_side_effects` command for exactly
        // this; this test covers both channels that method strips.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        // A shared in-memory `AsyncWrite` so the test reads back what the tee got
        // (mirrors the `SharedSink` in tests/integration/capture.rs).
        struct SharedSink(Arc<std::sync::Mutex<Vec<u8>>>);
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

        let (_dir, path) = temp_cassette();
        let inner = ScriptedRunner::new().on(["tool"], Reply::lines(["a", "b", "c"]));
        let recorder = RecordReplayRunner::record(&path, inner);

        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let teed = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let cmd = Command::new("tool")
            .on_stdout_line(move |_line| {
                counter.fetch_add(1, AtomicOrdering::SeqCst);
            })
            .stdout_tee(SharedSink(Arc::clone(&teed)));

        let run = recorder.start(&cmd).await.expect("record start");
        let _ = run
            .output_string()
            .await
            .expect("consume the scripted handle");

        assert_eq!(
            hits.load(AtomicOrdering::SeqCst),
            3,
            "stdout handler must fire once per line, not twice (the capture pass must stay silent)"
        );
        assert_eq!(
            teed.lock().expect("teed mutex").as_slice(),
            b"a\nb\nc\n",
            "stdout tee must receive each line once, not twice"
        );
    }

    #[tokio::test]
    async fn start_replay_reproduces_signal_and_timeout_outcomes() {
        // The whole point of `Reply::from_outcome` → `into_running`: the scripted
        // `start` handle must reproduce every non-exit outcome (not just Exited),
        // so a recorded signal kill / timeout replays as one through the streaming
        // path, exactly as the bulk `output_string` path already does.
        let (_dir, path) = temp_cassette();
        let json = serde_json::json!({
            "version": 1,
            "entries": [
                { "program": "killed", "args": [], "stdout": "", "stderr": "", "signal": 9 },
                { "program": "crashed", "args": [], "stdout": "", "stderr": "" },
                { "program": "slow", "args": [], "stdout": "", "stderr": "", "timed_out": true }
            ]
        });
        std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();
        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");

        let signalled = replayer
            .start(&Command::new("killed"))
            .await
            .expect("replay a signalled run through start")
            .wait()
            .await
            .expect("wait the signalled handle");
        assert_eq!(signalled, Outcome::Signalled(Some(9)));

        // An entry with no code/timed_out/signal is the legitimate "killed,
        // signal unknown" — it must decode to `Signalled(None)`, not `Exited`.
        let signalled_unknown = replayer
            .start(&Command::new("crashed"))
            .await
            .expect("replay a signal-unknown run through start")
            .wait()
            .await
            .expect("wait the signal-unknown handle");
        assert_eq!(signalled_unknown, Outcome::Signalled(None));

        let timed_out = replayer
            .start(&Command::new("slow"))
            .await
            .expect("replay a timed-out run through start")
            .wait()
            .await
            .expect("wait the timed-out handle");
        assert_eq!(timed_out, Outcome::TimedOut);
    }

    #[tokio::test]
    async fn output_bytes_is_unsupported_in_both_modes() {
        // A cassette stores lossy-UTF-8 text, so `output_bytes` (exact raw bytes)
        // must be rejected loudly rather than served silently-lossy through the
        // now-implemented `start` path.
        let (_dir, path) = temp_cassette();

        let recorder = RecordReplayRunner::record(&path, scripted());
        let rec_err = recorder
            .output_bytes(&Command::new("tool").arg("--version"))
            .await
            .expect_err("output_bytes must be unsupported in record mode");
        assert!(
            matches!(rec_err, Error::Unsupported { .. }),
            "got {rec_err:?}"
        );
        // The rejected call recorded nothing; a real entry is still needed to load.
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record a real entry");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let rep_err = replayer
            .output_bytes(&Command::new("tool").arg("--version"))
            .await
            .expect_err("output_bytes must be unsupported in replay mode");
        assert!(
            matches!(rep_err, Error::Unsupported { .. }),
            "got {rep_err:?}"
        );
    }

    #[tokio::test]
    async fn duplicate_key_plays_in_order_then_repeats_last() {
        let (_dir, path) = temp_cassette();

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
    async fn replay_rejects_an_entry_with_contradictory_outcome() {
        let (_dir, path) = temp_cassette();
        let json = serde_json::json!({
            "version": 1,
            "entries": [
                { "program": "x", "args": [], "stdout": "", "stderr": "", "code": 0, "signal": 9 }
            ]
        });
        std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();
        let err = RecordReplayRunner::replay(&path)
            .expect_err("a contradictory outcome must be rejected");
        assert!(
            matches!(&err, Error::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn replay_miss_is_a_distinct_cassette_miss_error() {
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool").arg("--other"))
            .await
            .expect_err("an unrecorded invocation must not be served");
        match &err {
            Error::CassetteMiss { program } => assert_eq!(program, "tool"),
            other => panic!("expected Error::CassetteMiss, got {other:?}"),
        }
        // A stale cassette is NOT mistaken for a missing program.
        assert!(
            !err.is_not_found(),
            "a cassette miss must not read as not-found: {err:?}"
        );
    }

    #[tokio::test]
    async fn replay_invokes_line_handlers() {
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let cmd = Command::new("tool").arg("--version").on_stdout_line({
            let seen = seen.clone();
            move |l| seen.lock().unwrap().push(l.to_owned())
        });
        let _ = replayer.output_string(&cmd).await.expect("replay");
        assert_eq!(
            *seen.lock().unwrap(),
            ["tool 1.2.3"],
            "replay must invoke the command's line handler"
        );
    }

    #[tokio::test]
    async fn stdin_content_is_part_of_the_match_key() {
        let (_dir, path) = temp_cassette();
        let inner = ScriptedRunner::new()
            .on_sequence(["tool"], [Reply::ok("out-A\n"), Reply::ok("out-B\n")]);
        let recorder = RecordReplayRunner::record(&path, inner);
        let _ = recorder
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_string("A")))
            .await
            .expect("record A");
        let _ = recorder
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_string("B")))
            .await
            .expect("record B");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        // Replay B FIRST: with stdin in the key it gets out-B. Keying on
        // `has_stdin` alone would collide and return out-A (the first entry).
        let b = replayer
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_string("B")))
            .await
            .expect("replay B");
        assert_eq!(
            b.stdout(),
            // The bulk path strips the recorded trailing newline (line-join
            // normalization), so the canned "out-B\n" replays as "out-B".
            "out-B",
            "stdin B must replay its own recording"
        );
        let a = replayer
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_string("A")))
            .await
            .expect("replay A");
        assert_eq!(a.stdout(), "out-A", "stdin A must replay its own recording");
    }

    #[tokio::test]
    async fn one_shot_streaming_stdin_is_rejected_in_both_modes() {
        let (_dir, path) = temp_cassette();
        let inner = ScriptedRunner::new().fallback(Reply::ok("out\n"));
        let recorder = RecordReplayRunner::record(&path, inner);
        let err = recorder
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_reader(&b"payload"[..])))
            .await
            .expect_err("record must reject a one-shot streaming stdin");
        assert!(matches!(err, Error::Unsupported { .. }), "got {err:?}");

        // Record a plain entry so the cassette loads, then prove replay rejects
        // a streaming stdin too.
        let _ = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect("record a replayable entry");
        recorder.save().expect("save");
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_reader(&b"payload"[..])))
            .await
            .expect_err("replay must reject a one-shot streaming stdin");
        assert!(matches!(err, Error::Unsupported { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn no_stdin_replay_does_not_match_a_stdin_recorded_entry() {
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("out\n")));
        let _ = recorder
            .output_string(&Command::new("tool").stdin(crate::Stdin::from_string("input")))
            .await
            .expect("record with stdin");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect_err("a no-stdin call must not match a stdin-recorded entry");
        assert!(matches!(err, Error::CassetteMiss { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn replayed_timeout_carries_the_commands_deadline() {
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().on(["tool", "slow"], Reply::timeout()),
        );
        let _ = recorder
            .output_string(&Command::new("tool").arg("slow"))
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
        let _ = recorder
            .output_string(
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

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let out = replayer
            .run(&Command::new("tool"))
            .await
            .expect("env is not part of the match key");
        assert_eq!(out, "done");
    }

    #[tokio::test]
    async fn signal_number_survives_round_trip() {
        let (_dir, path) = temp_cassette();
        let json = r#"{"version":1,"entries":[{"program":"tool","args":[],"stdout":"","stderr":"","code":null,"signal":9}]}"#;
        std::fs::write(&path, json).expect("write cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let result = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect("replay");
        assert_eq!(result.outcome(), Outcome::Signalled(Some(9)));
    }

    #[tokio::test]
    async fn cassette_without_signal_field_loads_as_signalled_none() {
        let (_dir, path) = temp_cassette();
        let json = r#"{"version":1,"entries":[{"program":"tool","args":[],"stdout":"","stderr":"","code":null}]}"#;
        std::fs::write(&path, json).expect("write cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let result = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect("replay");
        assert_eq!(result.outcome(), Outcome::Signalled(None));
    }

    #[tokio::test]
    async fn load_errors_are_typed_io() {
        let (_dir, path) = temp_cassette();
        match RecordReplayRunner::replay(&path) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {other:?}"),
        }

        std::fs::write(&path, "{ not json").unwrap();
        match RecordReplayRunner::replay(&path) {
            Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData),
            other => panic!("expected Io(InvalidData), got {other:?}"),
        }

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
    async fn version_gate_fires_before_the_full_entries_decode() {
        // An unsupported future version whose entries wouldn't even decode
        // under this build's `Entry` schema (`code` is normally an integer, not
        // a string) — the version gate must reject this with the clear
        // "version N is not supported" message, not a raw serde type-mismatch
        // error from attempting the full `Cassette` decode first.
        let (_dir, path) = temp_cassette();
        std::fs::write(
            &path,
            r#"{ "version": 99, "entries": [ { "program": "x", "args": [], "code": "not-a-number" } ] }"#,
        )
        .unwrap();
        match RecordReplayRunner::replay(&path) {
            Err(Error::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
                assert!(
                    e.to_string().contains("version 99"),
                    "expected the clear version-gate message, got: {e}"
                );
            }
            other => panic!("expected the version-gate error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replay_output_string_rejects_non_piped_stdout_even_on_a_match() {
        // Mirrors the live path (and `ScriptedRunner`'s
        // `output_on_non_piped_stdout_errors_like_the_live_path`): a capture verb
        // on `stdout(Null)` has nothing to read, so it must error even when a
        // cassette entry genuinely matches the invocation — a config mistake
        // isn't masked by a recorded capture.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record ok run");
        recorder.save().expect("save cassette");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let cmd = Command::new("tool")
            .arg("--version")
            .stdout(crate::StdioMode::Null);
        let err = replayer
            .output_string(&cmd)
            .await
            .expect_err("a non-piped stdout must error, even against a matching entry");
        match err {
            Error::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn drop_without_save_flushes_best_effort() {
        let (_dir, path) = temp_cassette();
        {
            let recorder = RecordReplayRunner::record(&path, scripted());
            let _ = recorder
                .output_string(&Command::new("tool").arg("--version"))
                .await
                .expect("record");
        }
        let replayer = RecordReplayRunner::replay(&path).expect("dropped recorder left a cassette");
        let out = replayer
            .run(&Command::new("tool").arg("--version"))
            .await
            .expect("replay after drop-flush");
        assert_eq!(out, "tool 1.2.3");
    }

    #[tokio::test]
    async fn cwd_is_not_part_of_the_match_key() {
        // The portability fix this task is about: a cassette recorded from one
        // absolute working directory (a dev box, a tempdir) still replays when
        // the same logical invocation runs from a different one (CI, another
        // checkout) — cwd is stored on the entry for visibility but excluded
        // from the match key (CASSETTE_VERSION 3).
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("from-a")));
        let _ = recorder
            .output_string(&Command::new("tool").current_dir("/home/dev/checkout"))
            .await
            .expect("record in one absolute cwd");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let cross_dir = replayer
            .run(&Command::new("tool").current_dir(r"C:\actions\work\checkout"))
            .await
            .expect(
                "a differing absolute cwd (dev box -> CI workspace) must still replay: \
                 cwd is not part of the match key",
            );
        assert_eq!(cross_dir, "from-a");

        let no_cwd = replayer
            .run(&Command::new("tool"))
            .await
            .expect("no cwd at all must replay the same recorded entry too");
        assert_eq!(no_cwd, "from-a");

        // cwd is still stored verbatim on the entry, just not matched on.
        let json = std::fs::read_to_string(&path).expect("read cassette");
        assert!(
            json.contains("/home/dev/checkout"),
            "cwd must still be stored for visibility: {json}"
        );
    }

    #[tokio::test]
    async fn differing_program_or_args_still_miss_with_cwd_excluded() {
        // Guards against a too-broad fix: dropping `cwd` from the key must not
        // also loosen matching on `program`/`args` — the remaining key fields
        // still discriminate as before.
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("from-a")));
        let _ = recorder
            .output_string(&Command::new("tool").arg("build").current_dir("dir-a"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("other").arg("build").current_dir("dir-b"))
            .await
            .expect_err("a different program is still a miss");
        assert!(matches!(err, Error::CassetteMiss { .. }), "got {err:?}");
        let err = replayer
            .output_string(&Command::new("tool").arg("test").current_dir("dir-b"))
            .await
            .expect_err("different args are still a miss");
        assert!(matches!(err, Error::CassetteMiss { .. }), "got {err:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cassette_file_is_written_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
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
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record (now dirty, unsaved)");

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
            let _ = recorder
                .output_string(&Command::new("tool").arg("--version"))
                .await
                .expect("record first");
            recorder.save().expect("first save");
            let _ = recorder
                .output_string(&Command::new("tool").arg("fail"))
                .await
                .expect("record second");
        }
        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let result = replayer
            .output_string(&Command::new("tool").arg("fail"))
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

        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("ok")));
        let cmd = Command::new("tool").arg(&bad);
        let _ = recorder.output_string(&cmd).await.expect("record lossily");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let out = replayer.run(&cmd).await.expect("replay matches lossily");
        assert_eq!(out, "ok");
    }

    /// An inner runner that returns a bounded-buffer-clipped result (truncated,
    /// with overflow totals and a non-zero duration) so record can capture them.
    struct TruncatedInner;
    #[async_trait::async_trait]
    impl ProcessRunner for TruncatedInner {
        async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
            Ok(ProcessResult::new(
                command.program_name(),
                "clipped".to_owned(),
                String::new(),
                Outcome::Exited(0),
                None,
            )
            .with_truncated(true)
            .with_overflow_totals(100, 9999)
            .with_duration(Duration::from_millis(1234)))
        }
    }

    #[tokio::test]
    async fn truncation_and_duration_survive_replay() {
        // D1/D12: a bounded-buffer-clipped recording must replay as truncated
        // (so the checking verbs still fail loud) and carry its recorded
        // duration, not a synthetic zero.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, TruncatedInner);
        let recorded = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect("record");
        assert!(recorded.truncated());
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let replayed = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect("replay");
        assert!(replayed.truncated(), "truncation must survive replay (D1)");
        assert_eq!(
            replayed.duration(),
            Duration::from_millis(1234),
            "the recorded duration must survive replay (D12)"
        );
        // A checking verb must fail loud on the truncated replay, not feed a
        // caller the clipped tail.
        let err = replayer
            .run(&Command::new("tool"))
            .await
            .expect_err("run must reject a truncated replay");
        assert!(matches!(err, Error::OutputTooLarge { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn truncation_and_duration_survive_start_replay() {
        // D4 (Stage-4 related): a caller who consumes a cassette `start` replay via
        // `output_string` must see the *recorded* truncation/overflow/duration,
        // threaded through the scripted handle — not the values re-derived from the
        // (un-truncated, instantly-fed) canned output. This closes the gap the bulk
        // `output_string` replay already covered but `start` used to lose.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, TruncatedInner);
        // Record-mode `start` captures the run whole at `start()`; drop the handle.
        let _ = recorder
            .start(&Command::new("tool"))
            .await
            .expect("record start");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let replayed = replayer
            .start(&Command::new("tool"))
            .await
            .expect("replay start")
            .output_string()
            .await
            .expect("consume the replayed handle");
        assert!(
            replayed.truncated(),
            "recorded truncation must survive a start replay"
        );
        assert_eq!(
            replayed.duration(),
            Duration::from_millis(1234),
            "recorded duration must survive a start replay"
        );
        assert_eq!(replayed.total_lines(), 100);
        assert_eq!(replayed.total_bytes(), 9999);
    }

    #[tokio::test]
    async fn replay_short_circuits_a_cancelled_token() {
        // D2: replay must honor a pre-cancelled `cancel_on` token exactly like the
        // real runner's pre-spawn short-circuit, not hand back a recorded `Ok`.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let token = crate::CancellationToken::new();
        token.cancel();
        let err = replayer
            .output_string(&Command::new("tool").arg("--version").cancel_on(token))
            .await
            .expect_err("a pre-cancelled token must short-circuit replay");
        assert!(matches!(err, Error::Cancelled { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn start_replay_short_circuits_a_cancelled_token() {
        // D2: the `start` replay arm must short-circuit a pre-cancelled token too
        // (it is separate code from the `output_string` arm).
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let token = crate::CancellationToken::new();
        token.cancel();
        let err = replayer
            .start(&Command::new("tool").arg("--version").cancel_on(token))
            .await
            .expect_err("a pre-cancelled token must short-circuit start replay");
        assert!(matches!(err, Error::Cancelled { .. }), "got {err:?}");
    }

    /// An inner runner whose `output_string` always fails with a fixed `Error`
    /// (never a `ProcessResult`) — stands in for a real spawn/lookup failure so
    /// record mode has an `Err` to capture (T-018).
    struct FailingInner(fn(&str) -> Error);
    #[async_trait::async_trait]
    impl ProcessRunner for FailingInner {
        async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
            Err((self.0)(&command.program_name()))
        }
    }

    #[tokio::test]
    async fn record_of_a_not_found_err_replays_the_same_not_found_error() {
        // T-018: a record-mode call that returns `Err` must no longer vanish
        // into thin air — replaying the same invocation must reproduce the
        // recorded `Error::NotFound`, not miss the cassette.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| Error::NotFound {
            program: program.to_owned(),
            searched: Some("/usr/bin:/bin".to_owned()),
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let record_err = recorder
            .output_string(&Command::new("ghost"))
            .await
            .expect_err("the inner runner's Err must still reach the record-mode caller");
        assert!(
            matches!(&record_err, Error::NotFound { .. }),
            "got {record_err:?}"
        );
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let replay_err = replayer
            .output_string(&Command::new("ghost"))
            .await
            .expect_err("replay must reproduce the recorded NotFound, not CassetteMiss");
        match &replay_err {
            Error::NotFound { program, searched } => {
                assert_eq!(program, "ghost");
                assert_eq!(searched.as_deref(), Some("/usr/bin:/bin"));
            }
            other => panic!("expected Error::NotFound, got {other:?}"),
        }
        assert!(
            replay_err.is_not_found(),
            "is_not_found() must still classify the replayed error"
        );

        // The same round trip through `start`, which shares the recording and
        // replay machinery with `output_string`.
        let (_dir2, path2) = temp_cassette();
        let inner2 = FailingInner(|program| Error::NotFound {
            program: program.to_owned(),
            searched: None,
        });
        let recorder2 = RecordReplayRunner::record(&path2, inner2);
        let _ = recorder2
            .start(&Command::new("ghost"))
            .await
            .expect_err("start must also surface the inner runner's Err in record mode");
        recorder2.save().expect("save");
        let replayer2 = RecordReplayRunner::replay(&path2).expect("load cassette");
        let err2 = replayer2
            .start(&Command::new("ghost"))
            .await
            .expect_err("start replay must reproduce the recorded NotFound");
        assert!(matches!(err2, Error::NotFound { .. }), "got {err2:?}");
    }

    #[tokio::test]
    async fn record_of_a_spawn_err_preserves_the_permission_denied_classification() {
        // The `os_kind` roundtrip must survive well enough for the io-level
        // classifiers (`is_permission_denied`) to still work after replay, not
        // just the message text.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| Error::Spawn {
            program: program.to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let _ = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect_err("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect_err("replay reproduces the recorded Spawn error");
        assert!(matches!(err, Error::Spawn { .. }), "got {err:?}");
        assert!(
            err.is_permission_denied(),
            "the recorded os error kind must survive replay: {err:?}"
        );
    }

    #[tokio::test]
    async fn a_cancelled_record_mode_call_is_never_persisted_to_the_cassette() {
        // Error::Cancelled is caller-state, not a fact about the invocation —
        // recording it would make an unrelated future replay wrongly cancelled.
        // It must simply not appear in the cassette (like the old "record
        // nothing for an Err" behavior), so a later real recording of the same
        // invocation can still be captured normally.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| Error::Cancelled {
            program: program.to_owned(),
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let err = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect_err("the cancelled call still surfaces its real error to the caller");
        assert!(matches!(err, Error::Cancelled { .. }));
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect_err("a cassette with no recorded entry for this invocation must miss");
        assert!(
            matches!(err, Error::CassetteMiss { .. }),
            "Cancelled must never be persisted, got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_version_1_cassette_with_no_error_field_still_loads_and_replays() {
        // Backward compatibility: a cassette written before this task (version
        // 1, no `error` key on any entry) must still load under the bumped
        // `CASSETTE_VERSION` and replay exactly as it always did.
        let (_dir, path) = temp_cassette();
        let json = serde_json::json!({
            "version": 1,
            "entries": [
                { "program": "tool", "args": ["--version"], "stdout": "tool 1.2.3\n", "stderr": "", "code": 0 }
            ]
        });
        std::fs::write(&path, serde_json::to_string_pretty(&json).unwrap()).unwrap();

        let replayer = RecordReplayRunner::replay(&path).expect("a version-1 cassette must load");
        let out = replayer
            .run(&Command::new("tool").arg("--version"))
            .await
            .expect("replay a version-1 entry with no error field");
        assert_eq!(out, "tool 1.2.3");
    }

    #[tokio::test]
    async fn unmodeled_error_variant_falls_back_to_other_rather_than_dropping_silently() {
        // `Error::Parse` has no dedicated `CassetteError` arm — `from_error`'s
        // catch-all `other =>` routes it into `CassetteError::Other`, and
        // `to_error` reconstructs that as `Error::Io(ErrorKind::Other)` carrying
        // the original `Display` text, never the original variant. This is the
        // lossy safety-net path: it must still land as *some* Err, never a
        // silent CassetteMiss.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| Error::Parse {
            program: program.to_owned(),
            message: "unexpected token at line 3".to_owned(),
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let record_err = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect_err("record");
        let expected_message = record_err.to_string();
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect_err("replay must reproduce the Other fallback, not miss the cassette");
        match err {
            Error::Io(source) => {
                assert_eq!(source.kind(), std::io::ErrorKind::Other, "got {source:?}");
                assert_eq!(source.to_string(), expected_message);
            }
            other => panic!("expected Error::Io(ErrorKind::Other), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn modeled_unsupported_error_round_trips_exactly_through_replay() {
        // Unlike `Parse` above, `Error::Unsupported` has an explicit
        // `CassetteError::Unsupported` arm in `from_error`/`to_error`, so it
        // must survive record/replay with full fidelity (variant and fields),
        // not just as a lossy `Other` message.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|_program| Error::Unsupported {
            operation: "signal(Hup)".to_owned(),
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let _ = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect_err("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect_err("replay must reproduce Unsupported, not miss the cassette");
        match err {
            Error::Unsupported { operation } => assert_eq!(operation, "signal(Hup)"),
            other => panic!("expected Error::Unsupported, got {other:?}"),
        }
    }
}
