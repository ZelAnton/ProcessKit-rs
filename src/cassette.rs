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
//! **Portability of the match key.** By default an invocation is matched on
//! `program` + `args` + the stdin digest — **not** `cwd` (see [`CASSETTE_VERSION`]
//! for why), so a cassette recorded in one absolute working directory (a tempdir,
//! a CI workspace like `/home/alice/repo` or `C:\actions\work\…`) replays cleanly
//! in another: the leading portability blocker (`cwd` pinning a cassette to the
//! machine/checkout it was recorded on) is gone. `cwd` is still stored on the
//! entry, verbatim, for visibility — just not matched on. When a tool's output
//! genuinely depends on the working directory or on selected environment
//! variables, opt in with [`RecordReplayRunner::match_on_cwd`] /
//! [`match_on_env`](RecordReplayRunner::match_on_env) — those fold the field into
//! the key through a digest, so env *values* still never reach the file. A
//! `from_file` stdin
//! source keys on its **path**, though, so that source is still machine-bound if
//! the path itself is absolute and varies across machines (a tempdir file); a
//! per-run tempdir path will still miss on the very next run too. Prefer
//! `Stdin::from_bytes`/`from_string` over `from_file` when the cassette must
//! travel.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::command::Command;
use crate::doubles::Invocation;
use crate::error::{Error, ErrorKind, Result};
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
/// cwd).
///
/// Bumped to `4`: an entry may now carry an optional `match_digest` (see
/// [`MatchPolicy`]) — the FNV-1a digest that an **opt-in** match policy
/// ([`RecordReplayRunner::match_on_cwd`]/[`match_on_env`](RecordReplayRunner::match_on_env))
/// folds the working directory and/or selected env-variable *values* into. Like
/// the `3` bump this is *not* a compatibility gate: the field is additive and
/// optional (`#[serde(default)]`), so a cassette recorded **without** a policy
/// (the portable default) omits it entirely and stays byte-identical to a
/// version-3 cassette but for the `version` number, and an older (v1..=v3)
/// cassette loads and replays exactly as before (no `match_digest` decodes as
/// `None`, matched by a no-policy replayer). Env *values* are still never
/// persisted — only this opaque digest is (see [`MatchPolicy::digest_of`]). The
/// bump exists so a cassette on disk records that it was keyed under a stricter
/// policy; a *newer*-shaped cassette is still refused by an older build's
/// version gate, exactly as before.
const CASSETTE_VERSION: u32 = 4;

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
    /// FNV-1a digest of the fields an **opt-in** [`MatchPolicy`] folds into the
    /// key — the working directory and/or the *values* of selected env
    /// variables. `None` (absent on disk) for the portable default (no policy),
    /// so a default cassette keys exactly as a version-3 one did and an older
    /// cassette without the field loads as `None`. Env values are **hashed into
    /// this digest, never persisted raw** — the cassette still stores only env
    /// variable *names* (`env_names`), never their values. See
    /// [`MatchPolicy::digest_of`] for the hashing and why a policy digest and a
    /// no-policy `None` are matched separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    match_digest: Option<u64>,
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
/// [`ErrorKind::CassetteMiss`]).
///
/// Deliberately **not** every [`Error`] variant: only the ones the raw
/// [`ProcessRunner::output_string`]/[`ProcessRunner::start`] seam can actually
/// return in record mode land here. A variant produced by a *checking* verb
/// layered over an otherwise-successful [`ProcessResult`] —
/// [`Exit`](ErrorKind::Exit), [`Timeout`](ErrorKind::Timeout),
/// [`Signalled`](ErrorKind::Signalled) — is already reproduced through the
/// existing `code`/`timed_out`/`signal` fields and never needs this; only a
/// call that returned no [`ProcessResult`] at all does.
///
/// [`Cancelled`](ErrorKind::Cancelled) is deliberately **excluded** (never
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
/// a [`ErrorKind::CassetteMiss`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum CassetteError {
    /// [`ErrorKind::Spawn`]: the child could not be started.
    Spawn {
        /// The OS error's [`std::io::ErrorKind`], named — see
        /// [`io_kind_name`]/[`io_kind_from_name`].
        os_kind: String,
        /// The OS error's `Display` text.
        message: String,
    },
    /// [`ErrorKind::NotFound`]: the program could not be located.
    NotFound {
        /// The `PATH` directories searched, joined — see
        /// [`ErrorKind::NotFound`]'s `searched` field. Never logged elsewhere;
        /// stored here exactly like the rest of a cassette (verbatim,
        /// reviewed before committing).
        searched: Option<String>,
    },
    /// [`ErrorKind::Stdin`]: feeding the child's stdin failed for a reason other
    /// than a routine broken pipe.
    Stdin {
        /// See [`Spawn`](CassetteError::Spawn)'s `os_kind`.
        os_kind: String,
        /// The OS error's `Display` text.
        message: String,
    },
    /// [`ErrorKind::OutputTooLarge`]: the captured output exceeded its ceiling.
    OutputTooLarge {
        max_lines: Option<usize>,
        max_bytes: Option<usize>,
        total_lines: usize,
        total_bytes: usize,
    },
    /// [`ErrorKind::Unsupported`]: the operation is not supported on this
    /// platform/mechanism.
    Unsupported {
        /// A short description of the unsupported operation.
        operation: String,
    },
    /// [`ErrorKind::Io`]: a low-level IO error from the crate's own machinery.
    Io {
        /// See [`Spawn`](CassetteError::Spawn)'s `os_kind`.
        os_kind: String,
        /// The error's `Display` text.
        message: String,
    },
    /// Any other `Error` variant, kept only as a `Display` message. Replays
    /// as [`ErrorKind::Io`] with [`std::io::ErrorKind::Other`] — not the original
    /// variant, but still a loud, informative `Err` rather than a silent
    /// cassette miss.
    Other {
        /// The original error's `Display` text.
        message: String,
    },
}

impl CassetteError {
    /// Capture the inner runner's `Err` for the cassette, or `None` for
    /// [`ErrorKind::Cancelled`] (see the type doc — deliberately never recorded).
    fn from_error(err: &Error) -> Option<Self> {
        Some(match err.kind() {
            ErrorKind::Cancelled { .. } => return None,
            ErrorKind::Spawn { source, .. } => CassetteError::Spawn {
                os_kind: io_kind_name(source.kind()).to_owned(),
                message: source.to_string(),
            },
            ErrorKind::NotFound { searched, .. } => CassetteError::NotFound {
                searched: searched.clone(),
            },
            ErrorKind::Stdin { source, .. } => CassetteError::Stdin {
                os_kind: io_kind_name(source.kind()).to_owned(),
                message: source.to_string(),
            },
            ErrorKind::OutputTooLarge {
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
            ErrorKind::Unsupported { operation } => CassetteError::Unsupported {
                operation: operation.clone(),
            },
            ErrorKind::Io(source) => CassetteError::Io {
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
            CassetteError::Spawn { os_kind, message } => ErrorKind::Spawn {
                program: program.to_owned(),
                source: std::io::Error::new(io_kind_from_name(os_kind), message.clone()),
            },
            CassetteError::NotFound { searched } => ErrorKind::NotFound {
                program: program.to_owned(),
                searched: searched.clone(),
            },
            CassetteError::Stdin { os_kind, message } => ErrorKind::Stdin {
                program: program.to_owned(),
                source: std::io::Error::new(io_kind_from_name(os_kind), message.clone()),
            },
            CassetteError::OutputTooLarge {
                max_lines,
                max_bytes,
                total_lines,
                total_bytes,
            } => ErrorKind::OutputTooLarge {
                program: program.to_owned(),
                max_lines: *max_lines,
                max_bytes: *max_bytes,
                total_lines: *total_lines,
                total_bytes: *total_bytes,
            },
            CassetteError::Unsupported { operation } => ErrorKind::Unsupported {
                operation: operation.clone(),
            },
            CassetteError::Io { os_kind, message } => ErrorKind::Io(std::io::Error::new(
                io_kind_from_name(os_kind),
                message.clone(),
            )),
            CassetteError::Other { message } => {
                ErrorKind::Io(std::io::Error::other(message.clone()))
            }
        }
        .into()
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

/// An **opt-in** stricter match policy: which normally-excluded routing fields —
/// the working directory and/or the *values* of selected environment variables —
/// also participate in the cassette match key. Empty by default (the portable
/// `program` + `args` + stdin-digest key documented on [`RecordReplayRunner`]);
/// populated via [`RecordReplayRunner::match_on_cwd`] /
/// [`match_on_env`](RecordReplayRunner::match_on_env).
///
/// **Env values are never persisted.** A policy naming env variables keys on a
/// *digest* of their `(name, value)` pairs (see [`digest_of`](Self::digest_of)),
/// keeping the cassette's non-secret-bearing posture intact — the file still
/// stores only variable *names* (`env_names`), never values. The policy lives on
/// the runner (outside the record/replay [`Mode`]) because both sides consult it:
/// record folds the digest onto each entry, replay recomputes it from the live
/// invocation. Record and replay must therefore use the **same** policy, exactly
/// as they must target the same tool — a mismatched policy simply misses (never
/// serves a wrong entry), because the digests won't be equal.
#[derive(Debug, Clone, Default)]
struct MatchPolicy {
    /// Whether the working directory participates in the match key.
    match_cwd: bool,
    /// Env variable names whose *values* participate — kept sorted + deduped so
    /// the digest is order-independent and stable across builder-call order.
    env_names: Vec<String>,
}

impl MatchPolicy {
    /// No stricter matching requested — the portable default. Keeps
    /// [`digest_of`](Self::digest_of) returning `None` so a no-policy cassette
    /// keys exactly as before (`match_digest` absent).
    fn is_empty(&self) -> bool {
        !self.match_cwd && self.env_names.is_empty()
    }

    /// The policy digest for an invocation, or `None` when the policy is empty.
    ///
    /// FNV-1a (like [`Stdin::content_digest`](crate::Stdin) — stable across Rust
    /// releases, unlike `DefaultHasher`) over a canonical, **self-describing**
    /// serialization: the policy's own shape (whether cwd is keyed, which env
    /// names, in sorted order) is folded in *alongside* the values, with a
    /// per-field tag byte, so (a) two different policies can't collide on one
    /// digest, and (b) a variable being *set to a value*, *removed*
    /// ([`Command::env_remove`](crate::Command::env_remove)), or *untouched* are
    /// three distinct facts. The effective override value is resolved through
    /// [`Invocation::env`](crate::testing::Invocation::env) (last-write-wins,
    /// platform case rules), matching the value a spawn would actually use.
    ///
    /// The env *values* are hashed here but **never persisted** — only the
    /// resulting opaque `u64` reaches the cassette, so a committed fixture leaks
    /// no more than it did before (still just variable names). Two invocations
    /// whose selected values differ produce different digests and thus miss each
    /// other on replay; identical selected values collide (the intended hit).
    fn digest_of(&self, invocation: &Invocation) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        // FNV-1a via the shared `digest::Fnv1a` helper — the single home of the
        // constants + mix loop, shared with `Stdin::content_digest` so the two
        // cassette-key digests reason alike (a constant changed in only one place
        // would silently invalidate recorded cassettes). Stable across releases,
        // unlike `DefaultHasher`.
        let mut h = crate::digest::Fnv1a::new();
        if self.match_cwd {
            // Tag the field so an empty-cwd policy can't alias a same-length env
            // digest; `cwd` is lossless bytes (also stored verbatim on the entry).
            h.mix(b"cwd\0");
            match &invocation.cwd {
                Some(cwd) => {
                    h.mix(&[1]);
                    h.mix(cwd.as_os_str().as_encoded_bytes());
                }
                None => h.mix(&[0]),
            }
        }
        for name in &self.env_names {
            h.mix(b"env\0");
            h.mix(name.as_bytes());
            h.mix(&[0]); // name/value boundary
            match invocation.env(name) {
                Some(Some(value)) => {
                    h.mix(&[1]); // set to a value
                    h.mix(value.as_encoded_bytes());
                }
                Some(None) => h.mix(&[2]), // explicitly removed
                None => h.mix(&[0]),       // untouched / inherited
            }
        }
        Some(h.finish())
    }
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
    // Durability + concurrency contract (documented on `RecordReplayRunner::save`):
    //   1. Refuse a symlinked cassette path (`O_NOFOLLOW`, unix) — fail loud
    //      (`ELOOP`) rather than write the secret-bearing content through a
    //      planted link. The rename below is safe regardless (it replaces the
    //      link, never writes *through* it), but a symlink here is suspicious.
    //   2. Serialize every writer to this one target behind an advisory lock
    //      (`acquire_save_lock`): a concurrent thread/process is refused with an
    //      explicit, transient `WouldBlock` conflict rather than a silent
    //      last-writer-wins clobber of another recorder's records.
    //   3. Write a *uniquely* named sibling temp with `O_EXCL` (never the old
    //      fixed `target + pid` name), so two in-flight saves can't stomp one
    //      temp, and a stale temp left by a crashed writer is a harmless orphan
    //      we neither collide with nor delete (it could be a live writer's temp).
    //   4. fsync the temp (in `write_new_file`), atomically `rename` it over the
    //      target (single-filesystem, so the rename is atomic), then fsync the
    //      parent directory so the rename itself is durable across a power loss on
    //      supporting unix filesystems.
    // Any fsync/rename failure propagates as `Err`; the previous cassette (if any)
    // survives intact until the rename swaps in the fully written new one.
    #[cfg(unix)]
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(std::io::Error::from_raw_os_error(libc::ELOOP));
    }
    // Held for the whole temp-write → rename → dir-fsync critical section; the
    // guard drops (releasing the lock) when this function returns.
    let _lock = acquire_save_lock(path)?;
    // A fresh unique temp, created with `O_EXCL`. The name collision that trips
    // `create_new` is astronomically unlikely (pid + a per-process counter +
    // nanos), but on the off chance a recycled pid left an identically named
    // orphan, retry with a new name rather than delete a file that might be
    // another writer's in-flight temp.
    let mut tmp = tmp_sibling(path);
    let mut attempts = 0u32;
    loop {
        match write_new_file(&tmp, json) {
            Ok(()) => break,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempts < 16 => {
                attempts += 1;
                tmp = tmp_sibling(path);
            }
            Err(e) => return Err(e),
        }
    }
    match std::fs::rename(&tmp, path).and_then(|()| sync_parent_dir(path)) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Best-effort cleanup of the temp *we* created; the original cassette
            // (if any) is untouched. We only ever remove a temp we made.
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// A *uniquely* named sibling temp path in the same directory as `path` (so a
/// later `rename` is same-filesystem/atomic). Uniqueness — pid + a process-wide
/// monotonic counter + wall-clock nanos — is what lets several in-flight saves
/// (two recorder instances in one process, or two processes) coexist without
/// stomping one another's temp: each save creates its own file with `O_EXCL` and
/// never touches, nor deletes, a name it did not create.
fn tmp_sibling(path: &Path) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{}.{}.{}.tmp", std::process::id(), seq, nanos));
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

/// A sibling advisory-lock path (`<path>.lock`) coordinating concurrent saves to
/// one cassette across threads and processes (see [`acquire_save_lock`]).
fn lock_sibling(path: &Path) -> std::path::PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    std::path::PathBuf::from(name)
}

/// The `Err` raised when another writer is saving the same cassette right now.
/// Its [`WouldBlock`](std::io::ErrorKind::WouldBlock) kind makes the wrapping
/// [`ErrorKind::Io`] satisfy [`Error::is_transient`](crate::Error::is_transient) — so
/// the loser can simply retry once the winner's save completes, and the last
/// confirmed-good cassette is preserved rather than silently overwritten.
fn concurrent_save_conflict() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        "another writer is saving this cassette concurrently — concurrent saves to \
         one cassette path are serialized by an advisory lock and the loser is \
         refused (a transient, retryable error) rather than silently overwriting \
         the last good cassette",
    )
}

/// An advisory exclusive lock over concurrent saves to one cassette path, held
/// for the temp-write → rename → dir-fsync critical section. Releasing the lock
/// is dropping the held handle — which also happens on process death, so a
/// crashed writer never wedges future saves.
struct SaveLock {
    /// Held purely so its `Drop` (closing the handle) releases the OS lock; the
    /// value itself is never read after construction.
    #[allow(dead_code)]
    file: std::fs::File,
}

/// Acquire the per-target advisory save lock, **non-blocking**. On success the
/// returned guard holds the lock until dropped; if another thread or process
/// holds it, returns [`concurrent_save_conflict`] rather than blocking or
/// silently proceeding.
///
/// Cross-process **and** cross-thread on both platforms:
/// - **Unix**: a `flock(LOCK_EX | LOCK_NB)` on a sibling `<path>.lock`. A
///   separate `open` per save gives each save its own open-file-description, so
///   two threads of one process contend here just like two processes do. The
///   lock is deliberately never `unlink`ed (unlinking a `flock`'d file races a
///   fresh create+lock of the same name); a leftover 0-byte `<path>.lock` is the
///   intended, harmless artifact.
/// - **Windows**: a deny-all `share_mode(0)` open of the same file — a
///   concurrent open (thread or process) fails with a sharing violation.
#[cfg(unix)]
fn acquire_save_lock(path: &Path) -> std::io::Result<SaveLock> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    // Owner-only, and refuse a symlinked lock path (`O_NOFOLLOW`) for the same
    // defense-in-depth reason the cassette write does. `create` (not
    // `create_new`) so an existing lock file from a prior run is reused — the
    // `flock` below, not the file's existence, is the arbiter.
    let file = std::fs::OpenOptions::new()
        .create(true)
        // Never truncate: the lock file is a 0-byte rendezvous, and truncating it
        // would needlessly touch an inode another holder may be `flock`ing.
        .truncate(false)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(lock_sibling(path))?;
    // SAFETY: `flock` on a valid fd we own; the fd outlives the call.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let err = std::io::Error::last_os_error();
        // std maps both `EWOULDBLOCK` and `EAGAIN` (the errnos `LOCK_NB`
        // contention raises) to `WouldBlock`.
        if err.kind() == std::io::ErrorKind::WouldBlock {
            return Err(concurrent_save_conflict());
        }
        return Err(err);
    }
    Ok(SaveLock { file })
}

#[cfg(windows)]
fn acquire_save_lock(path: &Path) -> std::io::Result<SaveLock> {
    use std::os::windows::fs::OpenOptionsExt;
    // Another handle already holds the deny-all lock (thread or process).
    const ERROR_SHARING_VIOLATION: i32 = 32;
    match std::fs::OpenOptions::new()
        .create(true)
        // Never truncate: the lock file is a 0-byte rendezvous held only for its
        // deny-share handle, not its contents.
        .truncate(false)
        .write(true)
        .share_mode(0)
        .open(lock_sibling(path))
    {
        Ok(file) => Ok(SaveLock { file }),
        Err(err) if err.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => {
            Err(concurrent_save_conflict())
        }
        Err(err) => Err(err),
    }
}

/// fsync the directory containing `path` so a preceding `rename` into it is
/// durable across a power loss. The file's own contents are already fsync'd in
/// [`write_new_file`], but the directory-entry swap `rename` performs is a
/// separate metadata write that must itself be flushed. Unix-only: Windows has
/// no portable directory-fsync, and NTFS metadata journaling plus the file's
/// `FlushFileBuffers` and the atomic `MoveFileEx` replace already provide the
/// durable-replacement guarantee there.
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        // A bare filename (`cassette.json`) has an empty parent — sync `.`.
        let dir = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(parent) => std::fs::File::open(parent)?,
            None => std::fs::File::open(".")?,
        };
        dir.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
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
    /// `match_digest` is the active [`MatchPolicy`]'s digest for this invocation
    /// (`None` for the portable default) — folded onto the entry so replay can
    /// key on it without re-persisting the raw cwd/env values.
    fn from_parts(
        invocation: &Invocation,
        result: &ProcessResult<String>,
        stdin_digest: Option<u64>,
        match_digest: Option<u64>,
    ) -> Self {
        let key = Self::key_fields(invocation, stdin_digest);
        Self {
            program: key.program,
            args: key.args,
            cwd: key.cwd,
            stdin_digest: key.stdin_digest,
            match_digest,
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
        match_digest: Option<u64>,
        error: CassetteError,
    ) -> Self {
        let key = Self::key_fields(invocation, stdin_digest);
        Self {
            program: key.program,
            args: key.args,
            cwd: key.cwd,
            stdin_digest: key.stdin_digest,
            match_digest,
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

/// What an invocation is matched on **by default**: program + args + the stdin
/// source digest (content for in-memory bytes, path for a `from_file` source).
/// Env overrides are excluded — deliberately, so an *irrelevant* env difference
/// between the record and replay environments can't cause a spurious miss —
/// **but this is a real collision risk when the env difference is NOT
/// irrelevant**: two invocations that differ only by an env override that
/// actually changes the tool's behavior/output collide on one entry unless a
/// [`MatchPolicy`] names that variable (see
/// [`RecordReplayRunner`]'s "Env is excluded from the match key by default" for
/// the opt-in [`match_on_env`](RecordReplayRunner::match_on_env) and the other
/// workarounds). `cwd` is excluded too — see the type doc's "Portability of the
/// match key" / [`CASSETTE_VERSION`]'s `3` bump — so a cassette recorded in one
/// absolute working directory still matches an otherwise-identical invocation
/// run from a different one, unless [`match_on_cwd`](RecordReplayRunner::match_on_cwd)
/// opts in. The optional trailing [`MatchPolicy`] digest carries those opt-in
/// fields without persisting raw cwd/env values.
///
/// The string components are *lossy* UTF-8 decodes, so two distinct non-UTF-8
/// invocations that differ only in their invalid bytes produce the same key and
/// collide on replay (the first recorded one answers for both). Accepted: keying
/// on raw bytes would defeat the human-diffable text fixture, and valid-UTF-8
/// invocations (the common case) never collide.
///
/// The trailing `Option<u64>` is the **opt-in** [`MatchPolicy`] digest (the last
/// tuple element; the earlier `Option<u64>` is the stdin digest): `None` for the
/// portable default, so a no-policy live invocation keys the same as a no-policy
/// (or older, field-less) entry. When a policy is active it folds cwd and/or
/// selected env *values* in (see [`MatchPolicy::digest_of`]); a differing
/// selected cwd/env then yields a different digest and a deliberate miss.
type Key = (String, Vec<String>, bool, Option<u64>, Option<u64>);

/// The stdin source digest keyed into a cassette match — `None` for an
/// empty/absent stdin. The digest never persists the stdin payload: in-memory
/// bytes hash their content, a `from_file` source hashes its path.
fn stdin_digest_of(command: &Command) -> Option<u64> {
    command
        .effective_stdin_source()
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
    if command
        .effective_stdin_source()
        .is_some_and(|s| s.is_one_shot())
    {
        return Err(ErrorKind::Unsupported {
            operation: "cassette record/replay with one-shot streaming stdin \
                        (from_reader/from_lines); use from_bytes/from_string/from_file"
                .to_string(),
        }
        .into());
    }
    Ok(())
}

/// The key of a live invocation — must decode exactly like
/// [`key_of_entry`] (both sides go through the same lossy conversion). The
/// `stdin_digest` is computed from the command, not carried on the
/// [`Invocation`] (which records only *whether* stdin was supplied). The
/// `has_stdin` bool is keyed alongside the digest so an older entry that loads
/// `stdin_digest: None` regardless of its stored `has_stdin` cannot match a
/// no-stdin replay — only miss. `invocation.cwd` and env values are excluded
/// from the key **by default** and folded in only under an opt-in `policy` (see
/// [`Key`]'s doc and [`MatchPolicy`]); an empty policy digests to `None`, keying
/// the same as a no-policy / older field-less entry.
fn key_of(invocation: &Invocation, stdin_digest: Option<u64>, policy: &MatchPolicy) -> Key {
    (
        invocation.program.to_string_lossy().into_owned(),
        invocation
            .args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect(),
        invocation.has_stdin,
        stdin_digest,
        policy.digest_of(invocation),
    )
}

/// The key of a stored entry (already lossy strings). `match_digest` was set by
/// the recording runner's policy; a field-less (older / no-policy) entry is `None`.
fn key_of_entry(entry: &Entry) -> Key {
    (
        entry.program.clone(),
        entry.args.clone(),
        entry.has_stdin,
        entry.stdin_digest,
        entry.match_digest,
    )
}

/// The replay-side state for one key: its entries in capture order plus a
/// cursor implementing the order-then-repeat-last consumption.
#[derive(Debug, Clone)]
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
/// **except** [`ErrorKind::Cancelled`], which is never recorded (a caller-driven
/// cancellation isn't a fact about the invocation to replay). Either way the
/// real error still propagates to the record-mode caller unchanged.
///
/// **Replay** mode loads the cassette and serves results without spawning:
///
/// - **Matching (default)**: program + args + stdin source digest — **not**
///   `cwd`, so a cassette recorded in one absolute working directory replays
///   against an otherwise-identical invocation run from a different one (see the
///   type doc's "Portability of the match key"). Env override *values* are never
///   written — only sorted variable names. Everything else (argv, cwd, stdout,
///   stderr) is stored verbatim, so review fixtures before committing. File is
///   written owner-only (`0600`) on Unix.
/// - **Opt-in stricter matching**: [`match_on_cwd`](Self::match_on_cwd) and
///   [`match_on_env`](Self::match_on_env) add the working directory and/or the
///   *values* of named env variables to the key — for a tool whose output truly
///   depends on where it runs or on a given variable. The extra fields key
///   through a single digest folded onto each entry
///   (`MatchPolicy::digest_of`); **env values are still never persisted** (only
///   the digest is), so a stricter policy leaks no more than the default. Set the
///   **same** policy on the record and replay runner — a mismatch misses rather
///   than misserving. Off by default to keep cassettes portable across machines
///   (see "Portability of the match key").
/// - **Env is excluded from the match key by default — not even as variable
///   names.** Two invocations of the same program+args+stdin that differ *only*
///   in an env override (`LC_ALL=C` vs `LC_ALL=en_US`, a feature-flag env var
///   that actually changes the tool's output, …) collide on one cassette entry
///   unless a policy names that variable: the first one recorded otherwise
///   silently answers for both on replay. This is a real collision risk, not
///   just a documented quirk — if your invocations vary meaningfully by env,
///   either (a) opt in with [`match_on_env`](Self::match_on_env), (b) fold the
///   env-sensitive knob into the command's **args** instead (part of the key),
///   (c) record each env variant into its **own cassette file**, or (d) drop to
///   a [`ScriptedRunner`](crate::testing::ScriptedRunner) rule keyed on a
///   predicate that inspects [`Command::env_overrides`] directly. (Env-keying via
///   `match_on_env` keys on a **digest** of the selected values, never the raw
///   values — a broader always-on env-keying alternative was considered and
///   deferred.)
/// - **Duplicates** replay in capture order, then the last entry repeats.
/// - **A miss is [`ErrorKind::CassetteMiss`]** (not `is_not_found()`): never a
///   surprise subprocess. A **recorded `Err`** replays as that same `Error`
///   rather than a result — this is a genuine behavioral difference from
///   cassettes written before this existed, where such an invocation instead
///   missed the cassette entirely.
/// - The replayed result carries the *replaying* command's
///   [`timeout`](Command::timeout), so a recorded timed-out run surfaces as
///   [`ErrorKind::Timeout`](crate::ErrorKind::Timeout) with the real deadline.
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
///   ([`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported)) in both modes: a cassette
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
    /// The opt-in stricter match policy (empty by default). Consulted on both
    /// sides: record folds its digest onto each entry, replay recomputes it from
    /// the live invocation — see [`MatchPolicy`].
    policy: MatchPolicy,
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
            policy: MatchPolicy::default(),
        }
    }

    /// Opt in to matching on the **working directory** too, on top of the
    /// portable default (`program` + `args` + stdin digest). With this set, two
    /// otherwise-identical invocations that ran in different working directories
    /// key to different cassette entries instead of colliding — for a tool whose
    /// output genuinely depends on where it runs.
    ///
    /// Off by default deliberately (see the type doc's "Portability of the match
    /// key"): the default keeps a cassette recorded on a dev box replaying in a
    /// CI workspace. Turn this on only when cwd actually changes the recorded
    /// output, and set the **same** policy on the record and replay runner —
    /// a policy mismatch simply misses (never serves a wrong entry). `cwd` is
    /// stored verbatim on the entry regardless; this only adds it to the *key*
    /// (via the entry's `match_digest`, so no new secret-bearing field appears).
    #[must_use]
    pub fn match_on_cwd(mut self) -> Self {
        self.policy.match_cwd = true;
        self
    }

    /// Opt in to matching on the **values** of the named environment variables,
    /// on top of the portable default. With this set, two invocations of the same
    /// `program`/`args`/stdin that differ only in one of these variables'
    /// override value key to different entries instead of colliding on the first
    /// recording (the collision risk documented under "Env is not part of the
    /// match key").
    ///
    /// Only the *values* named here participate, and only via a **digest**: the
    /// cassette still stores variable *names* only, never values — no env secret
    /// reaches the file. The value compared is the command's effective override
    /// for the variable (`env`/`env_remove`, last-write-wins, platform case
    /// rules); a variable this command never overrides is keyed as "untouched",
    /// distinct from a set or removed one. Names accumulate across calls and are
    /// deduped; set the **same** names on the record and replay runner (a policy
    /// mismatch misses rather than misserving). Unnamed variables stay excluded,
    /// preserving portability for env differences that don't matter.
    #[must_use]
    pub fn match_on_env<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.policy
            .env_names
            .extend(names.into_iter().map(Into::into));
        self.policy.env_names.sort();
        self.policy.env_names.dedup();
        self
    }

    /// Write the cassette now (record mode). This is the error-surfacing path
    /// — the drop-time flush swallows failures. Idempotent (rewrites the full
    /// cassette each time); a no-op `Ok` in replay mode. Runs recorded *after*
    /// a save are still covered: the drop-time flush fires whenever anything
    /// was recorded since the last successful save.
    ///
    /// # Durability
    ///
    /// The write is crash-atomic: the full cassette is written to a uniquely
    /// named sibling temp, fsync'd, then `rename`d over the target (an atomic
    /// replacement on a single filesystem), after which the **parent directory is
    /// fsync'd on Unix** so the rename itself survives a power loss. An
    /// interrupted or crashed save therefore never truncates or corrupts an
    /// existing good cassette — the old file stays intact until the rename swaps
    /// in the fully written new one. `rename`/fsync failures surface as `Err`
    /// (see below); the best-effort drop-time flush ignores them, and `Drop`
    /// never panics.
    ///
    /// # Concurrency
    ///
    /// Concurrent saves to **one** cassette path — two recorder instances in a
    /// process, or separate processes — are serialized by an advisory lock on a
    /// sibling `<path>.lock` (a `flock` on Unix, a deny-share open on Windows;
    /// both released on drop, including on process death). Each save uses its own
    /// `O_EXCL` temp, so writers never stomp one temp or delete another's (a
    /// stale temp from a crashed writer is a harmless orphan). The lock is taken
    /// **non-blocking**: if another writer holds it at that instant, the save is
    /// refused with a *transient* [`ErrorKind::Io`]
    /// ([`WouldBlock`](std::io::ErrorKind::WouldBlock), so
    /// [`is_transient`](crate::Error::is_transient) is `true` — retry once the
    /// other save completes) rather than silently overwriting it. This trades the
    /// old silent last-writer-wins data loss for an explicit, retryable conflict
    /// that always preserves the last confirmed-good cassette. A single recorder
    /// never conflicts with itself: its own saves are internally serialized.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Io`] if the recorded entries cannot be serialized to JSON; if
    /// another writer holds the save lock (a transient
    /// [`WouldBlock`](std::io::ErrorKind::WouldBlock) conflict — see
    /// *Concurrency*); or if writing, renaming, or fsync'ing the cassette (or its
    /// parent directory) fails. In replay mode there is nothing to write, so it
    /// returns `Ok(())`.
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
            .map_err(|e| ErrorKind::Io(std::io::Error::from(e)))?;
        write_cassette(path, &json).map_err(ErrorKind::Io)?;
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
            return Err(ErrorKind::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cassette entry for `{}` carries both a recorded `error` and an outcome \
                     indicator (`code`/`timed_out`/`signal`) — at most one may be set",
                    entry.program
                ),
            ))
            .into());
        }
        return Ok(());
    }
    if indicators > 1 {
        return Err(ErrorKind::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "cassette entry for `{}` has a contradictory outcome: at most one of \
                 `code` (exited), `timed_out`, or `signal` (signalled) may be set — found {indicators}",
                entry.program
            ),
        )).into());
    }
    Ok(())
}

impl RecordReplayRunner<JobRunner> {
    /// Load the cassette at `path` and serve its entries hermetically — no
    /// subprocess is ever spawned in replay mode.
    ///
    /// # Errors
    ///
    /// Always [`ErrorKind::Io`]: a missing file keeps its `NotFound` kind; a corrupt
    /// file, a contradictory entry, an unknown format `version`, or a cassette
    /// over the 64 MiB size limit is `InvalidData`.
    pub fn replay(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        const MAX_CASSETTE_BYTES: u64 = 64 << 20; // 64 MiB
        if let Ok(meta) = std::fs::metadata(path)
            && meta.len() > MAX_CASSETTE_BYTES
        {
            return Err(ErrorKind::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cassette is {} bytes, over the {MAX_CASSETTE_BYTES}-byte limit",
                    meta.len()
                ),
            ))
            .into());
        }
        let text = std::fs::read_to_string(path).map_err(ErrorKind::Io)?;
        Self::from_cassette_text(&text)
    }

    /// Decode a cassette already held in memory into a replay runner. Keeping
    /// the format validation here lets file loading and fuzzing exercise the
    /// same version gate and replay-slot construction without a temp file.
    fn from_cassette_text(text: &str) -> Result<Self> {
        Ok(Self {
            mode: Mode::Replay {
                slots: Mutex::new(replay_slots_from_text(text)?),
            },
            policy: MatchPolicy::default(),
        })
    }
}

/// Drive cassette decoding directly from arbitrary bytes. This is compiled
/// only by cargo-fuzz, so the ordinary crate API remains file-based.
#[cfg(fuzzing)]
pub fn fuzz_cassette_parse(bytes: &[u8]) {
    // JSON is text, but a lossy conversion ensures every fuzzer-generated byte
    // sequence reaches the real parser rather than being discarded as non-UTF-8.
    let text = String::from_utf8_lossy(bytes);
    let _ = RecordReplayRunner::from_cassette_text(&text);
}

/// Check replay's ordered consumption, repeat-last behavior, and miss error
/// against a valid in-memory cassette. This is compiled only by cargo-fuzz.
#[cfg(fuzzing)]
pub fn fuzz_cassette_replay(text: &str, calls: &[(String, Vec<String>)]) {
    let mut expected = match replay_slots_from_text(text) {
        Ok(slots) => slots,
        Err(_) => return,
    };
    let replayer = RecordReplayRunner::from_cassette_text(text)
        .expect("a cassette accepted for expected replay must build a replayer");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create fuzz replay runtime");

    runtime.block_on(async {
        for (program, args) in calls {
            let command = args
                .iter()
                .fold(Command::new(program), |command, arg| command.arg(arg));
            let invocation = Invocation::from_command(&command);
            let key = key_of(
                &invocation,
                stdin_digest_of(&command),
                &MatchPolicy::default(),
            );
            let expected_entry = expected.get_mut(&key).map(|slot| slot.play().clone());
            match expected_entry {
                Some(entry) if entry.error.is_some() => {
                    let err = replayer
                        .output_string(&command)
                        .await
                        .expect_err("a recorded error must replay as an error");
                    assert!(
                        !matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
                        "a matched recorded error must not become a cassette miss"
                    );
                }
                Some(entry) => {
                    let actual = replayer
                        .output_string(&command)
                        .await
                        .expect("a matched cassette entry must replay");
                    let expected_result = entry.to_result(None, Vec::new());
                    assert_eq!(actual.stdout(), expected_result.stdout());
                    assert_eq!(actual.stderr(), expected_result.stderr());
                    assert_eq!(actual.outcome(), expected_result.outcome());
                }
                None => match replayer
                    .output_string(&command)
                    .await
                    .map_err(Error::into_kind)
                {
                    Err(ErrorKind::CassetteMiss { .. }) => {}
                    other => {
                        panic!("an unmatched invocation must be a CassetteMiss, got {other:?}")
                    }
                },
            }
        }
    });
}

/// Parse and validate the textual cassette before arranging entries into their
/// per-key replay slots. Both file loading and fuzz-only callers use this one
/// path so their version-gate and malformed-entry behavior cannot drift.
fn replay_slots_from_text(text: &str) -> Result<HashMap<Key, ReplaySlot>> {
    // Gate on `version` before the full `Cassette` decode, so a future schema
    // reports its unsupported version instead of a misleading entry type error.
    #[derive(Deserialize)]
    struct CassetteHeader {
        version: u32,
    }
    let header: CassetteHeader =
        serde_json::from_str(text).map_err(|e| ErrorKind::Io(std::io::Error::from(e)))?;
    if header.version > CASSETTE_VERSION {
        return Err(ErrorKind::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "cassette version {} is not supported (this build reads up to version {CASSETTE_VERSION})",
                header.version
            ),
        )).into());
    }
    let cassette: Cassette =
        serde_json::from_str(text).map_err(|e| ErrorKind::Io(std::io::Error::from(e)))?;
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
    Ok(slots)
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
                // The active policy's digest (None for the portable default),
                // folded onto the entry so replay keys on cwd/env without
                // re-persisting the raw values.
                let match_digest = self.policy.digest_of(&invocation);
                match inner.output_string(command).await {
                    Ok(result) => {
                        let mut entries = recorded.lock().expect("cassette mutex poisoned");
                        entries.push(Entry::from_parts(
                            &invocation,
                            &result,
                            stdin_digest,
                            match_digest,
                        ));
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
                                match_digest,
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
                    return Err(ErrorKind::Cancelled {
                        program: command.program_name(),
                    }
                    .into());
                }
                // A capture verb on `stdout(Inherit/Null)` has nothing to read —
                // the real runner and the scripted double both reject it, and the
                // cassette's own `start` replay does too (it carries `stdout_piped`);
                // reject it here so the two replay arms stay symmetric and a config
                // mistake isn't masked by a recorded capture (D9).
                if !command.stdout_is_piped() {
                    return Err(crate::error::stdout_not_piped_error(
                        &command.program_name(),
                    ));
                }
                let invocation = Invocation::from_command(command);
                let stdin_digest = stdin_digest_of(command);
                // Release the lock before invoking line handlers — a handler that
                // re-enters this replayer would otherwise deadlock.
                let entry = {
                    let mut slots = slots.lock().expect("cassette mutex poisoned");
                    let slot = match slots.get_mut(&key_of(&invocation, stdin_digest, &self.policy))
                    {
                        Some(slot) => slot,
                        None => {
                            return Err(ErrorKind::CassetteMiss {
                                program: command.program_name(),
                            }
                            .into());
                        }
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
        Err(ErrorKind::Unsupported {
            operation: "output_bytes on a cassette (a lossy-UTF-8 text fixture cannot \
                        reproduce exact bytes; capture them from a real or scripted runner)"
                .to_string(),
        }
        .into())
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
                let match_digest = self.policy.digest_of(&invocation);
                match inner
                    .output_string(&command.without_line_side_effects())
                    .await
                {
                    Ok(result) => {
                        let entry =
                            Entry::from_parts(&invocation, &result, stdin_digest, match_digest);
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
                                match_digest,
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
                    return Err(ErrorKind::Cancelled {
                        program: command.program_name(),
                    }
                    .into());
                }
                let invocation = Invocation::from_command(command);
                let stdin_digest = stdin_digest_of(command);
                let entry = {
                    let mut slots = slots.lock().expect("cassette mutex poisoned");
                    let slot = match slots.get_mut(&key_of(&invocation, stdin_digest, &self.policy))
                    {
                        Some(slot) => slot,
                        None => {
                            return Err(ErrorKind::CassetteMiss {
                                program: command.program_name(),
                            }
                            .into());
                        }
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
            matches!(rec_err.kind(), ErrorKind::Unsupported { .. }),
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
            matches!(rep_err.kind(), ErrorKind::Unsupported { .. }),
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
            matches!(&err.kind(), ErrorKind::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
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
        match &err.kind() {
            ErrorKind::CassetteMiss { program } => assert_eq!(program, "tool"),
            other => panic!("expected ErrorKind::CassetteMiss, got {other:?}"),
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
        assert!(
            matches!(err.kind(), ErrorKind::Unsupported { .. }),
            "got {err:?}"
        );

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
        assert!(
            matches!(err.kind(), ErrorKind::Unsupported { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn keep_stdin_open_omits_ignored_sources_from_the_digest() {
        let replayable = Command::new("tool")
            .stdin(crate::Stdin::from_string("first"))
            .keep_stdin_open();
        let one_shot = Command::new("tool")
            .stdin(crate::Stdin::from_reader(&b"second"[..]))
            .keep_stdin_open();

        assert_eq!(stdin_digest_of(&replayable), None);
        assert_eq!(stdin_digest_of(&one_shot), None);
    }

    #[tokio::test]
    async fn keep_stdin_open_allows_ignored_one_shot_stdin_in_record_and_replay() {
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("out\n")));
        let recorded = Command::new("tool")
            .stdin(crate::Stdin::from_reader(&b"recorded"[..]))
            .keep_stdin_open();
        let result = recorder
            .output_string(&recorded)
            .await
            .expect("record accepts an ignored one-shot source");
        assert_eq!(result.stdout(), "out");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let replayed = Command::new("tool")
            .stdin(crate::Stdin::from_lines(tokio_stream::iter(vec![
                "replayed".to_owned(),
            ])))
            .keep_stdin_open();
        let result = replayer
            .output_string(&replayed)
            .await
            .expect("replay accepts and ignores a different one-shot source");
        assert_eq!(result.stdout(), "out");
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
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );
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
        match err.kind() {
            ErrorKind::Timeout { timeout, .. } => assert_eq!(*timeout, Duration::from_secs(7)),
            other => panic!("expected ErrorKind::Timeout, got {other:?}"),
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
        match RecordReplayRunner::replay(&path).map_err(Error::into_kind) {
            Err(ErrorKind::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
            other => panic!("expected Io(NotFound), got {other:?}"),
        }

        std::fs::write(&path, "{ not json").unwrap();
        match RecordReplayRunner::replay(&path).map_err(Error::into_kind) {
            Err(ErrorKind::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData),
            other => panic!("expected Io(InvalidData), got {other:?}"),
        }

        std::fs::write(&path, r#"{ "version": 99, "entries": [] }"#).unwrap();
        match RecordReplayRunner::replay(&path).map_err(Error::into_kind) {
            Err(ErrorKind::Io(e)) => {
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
        match RecordReplayRunner::replay(&path).map_err(Error::into_kind) {
            Err(ErrorKind::Io(e)) => {
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
        match err.kind() {
            ErrorKind::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
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
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );
        let err = replayer
            .output_string(&Command::new("tool").arg("test").current_dir("dir-b"))
            .await
            .expect_err("different args are still a miss");
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );
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
    async fn concurrent_saves_to_one_path_never_corrupt_the_cassette() {
        // Unique-temp + advisory-lock + crash-atomic write, exercised together:
        // eight *distinct* recorder instances (as two independent recorders in one
        // process would be) all save to ONE path simultaneously. No save may
        // corrupt the file or panic; every save either wins or is refused with the
        // explicit transient conflict; and the survivor must reopen as a whole,
        // valid cassette.
        let (_dir, path) = temp_cassette();
        let mut recorders = Vec::new();
        for _ in 0..8 {
            let r = RecordReplayRunner::record(
                &path,
                ScriptedRunner::new().on(["tool", "ping"], Reply::ok("pong")),
            );
            let _ = r
                .output_string(&Command::new("tool").arg("ping"))
                .await
                .expect("record ping");
            recorders.push(r);
        }
        // Fire every save at once, each on its own thread.
        let results: Vec<Result<()>> = std::thread::scope(|s| {
            let handles: Vec<_> = recorders.iter().map(|r| s.spawn(|| r.save())).collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("a save thread must not panic"))
                .collect()
        });
        // A losing save is *only ever* the transient WouldBlock conflict — never a
        // corrupt/partial-write error of some other kind.
        for res in &results {
            if let Err(err) = res {
                assert!(
                    err.is_transient(),
                    "a losing concurrent save must be the transient WouldBlock \
                     conflict, got {err:?}"
                );
            }
        }
        assert!(
            results.iter().any(|r| r.is_ok()),
            "at least one concurrent save must win"
        );
        // The final cassette reopens and replays the recorded run — proof the
        // winner wrote a whole, valid cassette, not a torn or interleaved one.
        let replayer = RecordReplayRunner::replay(&path).expect("reopen after concurrent writes");
        let out = replayer
            .output_string(&Command::new("tool").arg("ping"))
            .await
            .expect("replay the surviving entry");
        assert_eq!(out.stdout(), "pong");
    }

    #[tokio::test]
    async fn a_save_racing_a_held_lock_is_refused_not_a_silent_clobber() {
        // The cross-process boundary: a competing writer (another process, or
        // another thread) is modeled by holding the very OS advisory lock `save`
        // takes — that lock *is* the cross-process coordination. A save that races
        // it must be refused with the transient conflict and must NOT overwrite
        // the last confirmed-good cassette.
        let (_dir, path) = temp_cassette();

        // A first recorder writes a known-good cassette.
        let winner = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().on(["tool", "keep"], Reply::ok("kept")),
        );
        let _ = winner
            .output_string(&Command::new("tool").arg("keep"))
            .await
            .expect("record the good run");
        winner.save().expect("the first save wins the free lock");

        // Now a competitor holds the lock, exactly as a rival process would.
        let held = acquire_save_lock(&path).expect("model a competing writer holding the lock");

        // A second, different recorder tries to save the same path while it is held.
        let loser = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().on(["tool", "clobber"], Reply::ok("clobbered\n")),
        );
        let _ = loser
            .output_string(&Command::new("tool").arg("clobber"))
            .await
            .expect("record the clobbering run");
        let err = loser
            .save()
            .expect_err("a save racing a held lock must be refused, not silently applied");
        assert!(
            err.is_transient(),
            "the conflict must be the transient WouldBlock error, got {err:?}"
        );

        drop(held); // release the competitor's lock

        // The last confirmed-good cassette survived untouched: `keep` still
        // replays, and the refused save's run never reached the file.
        let replayer = RecordReplayRunner::replay(&path).expect("reopen the preserved cassette");
        assert_eq!(
            replayer
                .output_string(&Command::new("tool").arg("keep"))
                .await
                .expect("the good run is still there")
                .stdout(),
            "kept"
        );
        let miss = replayer
            .output_string(&Command::new("tool").arg("clobber"))
            .await
            .expect_err("the refused save's run must be absent");
        assert!(
            matches!(miss.kind(), ErrorKind::CassetteMiss { .. }),
            "the clobbering entry must be absent (a miss), got {miss:?}"
        );
    }

    #[tokio::test]
    async fn a_stale_temp_from_a_crashed_writer_is_left_untouched_and_does_not_block() {
        // A leftover temp from a prior crashed/interrupted writer must neither
        // block a fresh save (the new temp is uniquely named + `O_EXCL`) nor be
        // deleted (it is indistinguishable from another *live* writer's in-flight
        // temp, so removing it would be the very bug this change removes).
        let (_dir, path) = temp_cassette();
        let stale = {
            let mut name = path.as_os_str().to_owned();
            // Shaped like a real temp but with a pid/seq this run will never mint.
            name.push(".4294967295.0.0.tmp");
            PathBuf::from(name)
        };
        std::fs::write(&stale, "garbage from a crashed writer").expect("seed stale temp");

        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record");
        recorder
            .save()
            .expect("save must succeed despite the stale temp");

        assert!(
            stale.exists(),
            "a stale temp (possibly another writer's live temp) must be left untouched"
        );
        assert_eq!(
            std::fs::read_to_string(&stale).expect("read stale temp"),
            "garbage from a crashed writer",
            "the stale temp's contents must not be disturbed"
        );
        RecordReplayRunner::replay(&path).expect("the cassette is valid and reopens");
    }

    #[tokio::test]
    async fn a_write_failure_surfaces_as_err_and_drop_stays_non_panic() {
        // A failure in the temp→target replacement must propagate out of `save`
        // (never a silent success), and a subsequent best-effort drop-flush over
        // the same failure must not panic. We occupy the target path with a
        // *directory* so the atomic `rename` cannot succeed — cross-platform, and
        // independent of filesystem permissions or the test process's uid.
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("cassette.json");
        std::fs::create_dir(&path).expect("occupy the target path with a directory");

        let recorder = RecordReplayRunner::record(&path, scripted());
        let _ = recorder
            .output_string(&Command::new("tool").arg("--version"))
            .await
            .expect("record (now dirty)");
        let err = recorder
            .save()
            .expect_err("renaming the temp over a directory must fail, not silently succeed");
        assert!(
            matches!(err.kind(), ErrorKind::Io(_)),
            "a write/rename failure is an Io error, got {err:?}"
        );
        // The recorder is still dirty; dropping it re-runs the best-effort flush,
        // which fails the same way and must swallow it without panicking.
        drop(recorder);
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
        assert!(
            matches!(err.kind(), ErrorKind::OutputTooLarge { .. }),
            "got {err:?}"
        );
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
        assert!(
            matches!(err.kind(), ErrorKind::Cancelled { .. }),
            "got {err:?}"
        );
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
        assert!(
            matches!(err.kind(), ErrorKind::Cancelled { .. }),
            "got {err:?}"
        );
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
        // recorded `ErrorKind::NotFound`, not miss the cassette.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| {
            ErrorKind::NotFound {
                program: program.to_owned(),
                searched: Some("/usr/bin:/bin".to_owned()),
            }
            .into()
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let record_err = recorder
            .output_string(&Command::new("ghost"))
            .await
            .expect_err("the inner runner's Err must still reach the record-mode caller");
        assert!(
            matches!(&record_err.kind(), ErrorKind::NotFound { .. }),
            "got {record_err:?}"
        );
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load cassette");
        let replay_err = replayer
            .output_string(&Command::new("ghost"))
            .await
            .expect_err("replay must reproduce the recorded NotFound, not CassetteMiss");
        match &replay_err.kind() {
            ErrorKind::NotFound { program, searched } => {
                assert_eq!(program, "ghost");
                assert_eq!(searched.as_deref(), Some("/usr/bin:/bin"));
            }
            other => panic!("expected ErrorKind::NotFound, got {other:?}"),
        }
        assert!(
            replay_err.is_not_found(),
            "is_not_found() must still classify the replayed error"
        );

        // The same round trip through `start`, which shares the recording and
        // replay machinery with `output_string`.
        let (_dir2, path2) = temp_cassette();
        let inner2 = FailingInner(|program| {
            ErrorKind::NotFound {
                program: program.to_owned(),
                searched: None,
            }
            .into()
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
        assert!(
            matches!(err2.kind(), ErrorKind::NotFound { .. }),
            "got {err2:?}"
        );
    }

    #[tokio::test]
    async fn record_of_a_spawn_err_preserves_the_permission_denied_classification() {
        // The `os_kind` roundtrip must survive well enough for the io-level
        // classifiers (`is_permission_denied`) to still work after replay, not
        // just the message text.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| {
            ErrorKind::Spawn {
                program: program.to_owned(),
                source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
            }
            .into()
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
        assert!(matches!(err.kind(), ErrorKind::Spawn { .. }), "got {err:?}");
        assert!(
            err.is_permission_denied(),
            "the recorded os error kind must survive replay: {err:?}"
        );
    }

    #[tokio::test]
    async fn a_cancelled_record_mode_call_is_never_persisted_to_the_cassette() {
        // ErrorKind::Cancelled is caller-state, not a fact about the invocation —
        // recording it would make an unrelated future replay wrongly cancelled.
        // It must simply not appear in the cassette (like the old "record
        // nothing for an Err" behavior), so a later real recording of the same
        // invocation can still be captured normally.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| {
            ErrorKind::Cancelled {
                program: program.to_owned(),
            }
            .into()
        });
        let recorder = RecordReplayRunner::record(&path, inner);
        let err = recorder
            .output_string(&Command::new("tool"))
            .await
            .expect_err("the cancelled call still surfaces its real error to the caller");
        assert!(matches!(err.kind(), ErrorKind::Cancelled { .. }));
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let err = replayer
            .output_string(&Command::new("tool"))
            .await
            .expect_err("a cassette with no recorded entry for this invocation must miss");
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
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
        // `ErrorKind::Parse` has no dedicated `CassetteError` arm — `from_error`'s
        // catch-all `other =>` routes it into `CassetteError::Other`, and
        // `to_error` reconstructs that as `ErrorKind::Io(ErrorKind::Other)` carrying
        // the original `Display` text, never the original variant. This is the
        // lossy safety-net path: it must still land as *some* Err, never a
        // silent CassetteMiss.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|program| {
            ErrorKind::Parse {
                program: program.to_owned(),
                message: "unexpected token at line 3".to_owned(),
            }
            .into()
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
        match err.kind() {
            ErrorKind::Io(source) => {
                assert_eq!(source.kind(), std::io::ErrorKind::Other, "got {source:?}");
                assert_eq!(source.to_string(), expected_message);
            }
            other => panic!("expected ErrorKind::Io(ErrorKind::Other), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn modeled_unsupported_error_round_trips_exactly_through_replay() {
        // Unlike `Parse` above, `ErrorKind::Unsupported` has an explicit
        // `CassetteError::Unsupported` arm in `from_error`/`to_error`, so it
        // must survive record/replay with full fidelity (variant and fields),
        // not just as a lossy `Other` message.
        let (_dir, path) = temp_cassette();
        let inner = FailingInner(|_program| {
            ErrorKind::Unsupported {
                operation: "signal(Hup)".to_owned(),
            }
            .into()
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
        match err.kind() {
            ErrorKind::Unsupported { operation } => assert_eq!(operation, "signal(Hup)"),
            other => panic!("expected ErrorKind::Unsupported, got {other:?}"),
        }
    }

    // --- T-081: opt-in match policy (cwd / selected env values) ---

    #[tokio::test]
    async fn default_ignores_differing_env_values_without_a_policy() {
        // Criterion (b): with NO policy set, two invocations differing only in an
        // env value still collide on one entry — the portable default is
        // unchanged, env is not part of the key.
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("from-a")));
        let _ = recorder
            .output_string(&Command::new("tool").env("MODE", "a"))
            .await
            .expect("record with MODE=a");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load");
        let out = replayer
            .run(&Command::new("tool").env("MODE", "b"))
            .await
            .expect("default: env is not keyed, so a differing value still hits");
        assert_eq!(out, "from-a");
    }

    #[tokio::test]
    async fn match_on_env_makes_a_differing_value_miss_and_ignores_unnamed_vars() {
        // Criterion (a): with the env policy on, a differing selected env value is
        // a deliberate `CassetteMiss` instead of a silent wrong-entry replay; a
        // variable the policy does NOT name still can't perturb the key
        // (portability preserved for irrelevant env differences).
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().fallback(Reply::ok("recorded")),
        )
        .match_on_env(["MODE"]);
        let _ = recorder
            .output_string(&Command::new("tool").env("MODE", "fast"))
            .await
            .expect("record MODE=fast");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path)
            .expect("load")
            .match_on_env(["MODE"]);
        let hit = replayer
            .run(&Command::new("tool").env("MODE", "fast"))
            .await
            .expect("the same selected env value must replay its recording");
        assert_eq!(hit, "recorded");

        let err = replayer
            .output_string(&Command::new("tool").env("MODE", "slow"))
            .await
            .expect_err("a differing selected env value must miss under the policy");
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );

        let still_hit = replayer
            .run(
                &Command::new("tool")
                    .env("MODE", "fast")
                    .env("UNRELATED", "x"),
            )
            .await
            .expect("an env var the policy does not name must not perturb the key");
        assert_eq!(still_hit, "recorded");
    }

    #[tokio::test]
    async fn match_on_env_records_distinct_entries_per_value() {
        // Two runs differing only in a selected env value must key to SEPARATE
        // entries (not collide on the first recording), so each replays its own.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new()
                .on_sequence(["tool"], [Reply::ok("out-fast\n"), Reply::ok("out-slow\n")]),
        )
        .match_on_env(["MODE"]);
        let _ = recorder
            .output_string(&Command::new("tool").env("MODE", "fast"))
            .await
            .expect("record fast");
        let _ = recorder
            .output_string(&Command::new("tool").env("MODE", "slow"))
            .await
            .expect("record slow");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path)
            .expect("load")
            .match_on_env(["MODE"]);
        // Replay slow FIRST: had the two collided on one key, this would wrongly
        // return the first recording (out-fast).
        let slow = replayer
            .run(&Command::new("tool").env("MODE", "slow"))
            .await
            .expect("replay slow");
        assert_eq!(slow, "out-slow");
        let fast = replayer
            .run(&Command::new("tool").env("MODE", "fast"))
            .await
            .expect("replay fast");
        assert_eq!(fast, "out-fast");
    }

    #[tokio::test]
    async fn match_on_env_never_writes_raw_values_only_a_digest() {
        // Criterion (c): even with the env policy on, the RAW value never reaches
        // the file — only the variable name (as always) and an opaque digest.
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("done")))
                .match_on_env(["API_TOKEN"]);
        let _ = recorder
            .output_string(&Command::new("tool").env("API_TOKEN", "hunter2-very-secret"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let json = std::fs::read_to_string(&path).expect("read cassette");
        assert!(
            json.contains("API_TOKEN"),
            "the name is still stored: {json}"
        );
        assert!(
            !json.contains("hunter2-very-secret"),
            "the raw value must never be written, even under the env match policy: {json}"
        );
        assert!(
            json.contains("match_digest"),
            "the opaque policy digest is what keys the env value: {json}"
        );
    }

    #[tokio::test]
    async fn match_on_env_distinguishes_a_set_var_from_an_untouched_one() {
        // Under the policy, a selected variable that is SET vs left UNTOUCHED are
        // distinct keys — not just two different set values.
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().fallback(Reply::ok("with-flag")),
        )
        .match_on_env(["FLAG"]);
        let _ = recorder
            .output_string(&Command::new("tool").env("FLAG", "on"))
            .await
            .expect("record with FLAG set");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path)
            .expect("load")
            .match_on_env(["FLAG"]);
        let err = replayer
            .output_string(&Command::new("tool")) // FLAG untouched
            .await
            .expect_err("an untouched selected var must not match a set-var recording");
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn match_on_cwd_keys_on_working_directory() {
        // The opt-in cwd policy: the same cwd replays, a differing one misses —
        // for a tool whose output genuinely depends on where it runs. (Contrast
        // `cwd_is_not_part_of_the_match_key`, the portable default.)
        let (_dir, path) = temp_cassette();
        let recorder = RecordReplayRunner::record(
            &path,
            ScriptedRunner::new().fallback(Reply::ok("from-dir-a")),
        )
        .match_on_cwd();
        let _ = recorder
            .output_string(&Command::new("tool").current_dir("/work/a"))
            .await
            .expect("record in /work/a");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path)
            .expect("load")
            .match_on_cwd();
        let hit = replayer
            .run(&Command::new("tool").current_dir("/work/a"))
            .await
            .expect("the same cwd must replay under match_on_cwd");
        assert_eq!(hit, "from-dir-a");
        let err = replayer
            .output_string(&Command::new("tool").current_dir("/work/b"))
            .await
            .expect_err("a differing cwd must miss under match_on_cwd");
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_policy_keyed_cassette_replayed_without_the_policy_misses() {
        // The policy is symmetric by contract: an entry keyed with a `match_digest`
        // can't be matched by a no-policy replayer (digest `None`) — it misses
        // loudly rather than serving a wrong entry. Record and replay must set the
        // same policy.
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("x")))
                .match_on_env(["MODE"]);
        let _ = recorder
            .output_string(&Command::new("tool").env("MODE", "a"))
            .await
            .expect("record under a policy");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path).expect("load"); // no policy
        let err = replayer
            .output_string(&Command::new("tool").env("MODE", "a"))
            .await
            .expect_err("a policy-keyed entry must not match a no-policy replay");
        assert!(
            matches!(err.kind(), ErrorKind::CassetteMiss { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn match_on_env_names_are_order_independent() {
        // Names accumulate + dedup, so builder-call order can't change the key:
        // recording with [A, B] and replaying with [B, A] still matches.
        let (_dir, path) = temp_cassette();
        let recorder =
            RecordReplayRunner::record(&path, ScriptedRunner::new().fallback(Reply::ok("ok")))
                .match_on_env(["ALPHA", "BETA"]);
        let _ = recorder
            .output_string(&Command::new("tool").env("ALPHA", "1").env("BETA", "2"))
            .await
            .expect("record");
        recorder.save().expect("save");

        let replayer = RecordReplayRunner::replay(&path)
            .expect("load")
            .match_on_env(["BETA"])
            .match_on_env(["ALPHA"]);
        let out = replayer
            .run(&Command::new("tool").env("ALPHA", "1").env("BETA", "2"))
            .await
            .expect("policy name order must not affect the key");
        assert_eq!(out, "ok");
    }

    #[test]
    fn digest_of_is_stable_byte_for_byte() {
        // Pin the exact FNV-1a output of `MatchPolicy::digest_of` for fixed
        // policies + invocations. The expected values are computed independently (a
        // standalone FNV-1a-64 over the exact byte sequence the method folds), NOT
        // copied out of the code — so they catch any drift in the constants, the
        // mix loop, or the field-tag order, which would silently invalidate every
        // already-recorded cassette (a baffling `CassetteMiss`, not a build error).

        // Env policy with the var set: folds b"env\0" ++ b"MODE" ++ [0] ++ [1] ++
        // b"fast" onto the offset basis.
        let env_policy = MatchPolicy {
            match_cwd: false,
            env_names: vec!["MODE".to_owned()],
        };
        let set = Invocation::from_command(&Command::new("tool").env("MODE", "fast"));
        assert_eq!(
            env_policy.digest_of(&set),
            Some(0xb9f4_ba02_d660_e742),
            "env-value digest changed — invalidates every recorded cassette"
        );

        // cwd policy with an ASCII cwd (identical OsStr bytes on every platform):
        // folds b"cwd\0" ++ [1] ++ b"/work/a".
        let cwd_policy = MatchPolicy {
            match_cwd: true,
            env_names: Vec::new(),
        };
        let in_dir = Invocation::from_command(&Command::new("tool").current_dir("/work/a"));
        assert_eq!(
            cwd_policy.digest_of(&in_dir),
            Some(0x1926_ae14_ca39_bd8e),
            "cwd digest changed — invalidates every recorded cassette"
        );

        // An empty policy keys to `None` (no `match_digest` on the entry) — the
        // portable default is unchanged by the refactor.
        assert_eq!(MatchPolicy::default().digest_of(&set), None);
    }
}
