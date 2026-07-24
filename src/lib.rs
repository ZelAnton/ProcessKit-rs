// `doc_cfg` (nightly-only) auto-derives the "Available on crate feature" badges
// from `#[cfg]` gates; gated behind `docsrs` so stable/CI `cargo doc` ignores it.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

//! `processkit` — async child-process management for Rust + [tokio]: whole-tree
//! kill-on-drop (no orphaned subprocesses), run-and-capture, streaming,
//! shell-free pipelines, timeouts & cancellation, and supervision.
//!
//! [tokio]: https://tokio.rs/
//!
//! Two layers:
//!
//! - **[`ProcessGroup`]** — a kill-on-drop container for a process *tree*. Every
//!   child spawned into the group, and everything those children spawn, dies
//!   with the group, so an exiting or panicking owner doesn't leak subprocesses.
//!   Containment is a Windows [Job Object], a Linux [cgroup v2] (with a POSIX
//!   process-group fallback), or a POSIX process group on macOS/BSD —
//!   observable via [`Mechanism`]. A spawn-free [`host_containment`] reports
//!   which [`Mechanism`] (and the reach of soft stop / abrupt-owner-death
//!   cleanup) a group *would* get on this host, before any group exists. Two
//!   caveats the [`ProcessGroup`] /
//!   [`Mechanism`] docs spell out: the guarantee rides on `Drop` running (a
//!   `panic = "abort"` process, or a `SIGKILL`/power-loss of the *owner*, skips
//!   it — the OS-owned Job Object / cgroup still reaps on handle close, the POSIX
//!   process-group fallback does not), and on the process-group mechanism a child
//!   that calls `setsid` escapes containment. The whole tree can be
//!   signalled (`ProcessGroup::signal`, see `Signal`), paused/resumed
//!   (`ProcessGroup::suspend` / `ProcessGroup::resume`), and inspected
//!   (`ProcessGroup::members`); [`wait_any`] races several running processes
//!   and reports the first to exit.
//! - **runner** — async run-and-capture built on the group. Describe a run with
//!   [`Command`], then drive it to completion ([`Command::output_string`],
//!   [`Command::run`], …) or [`start`](Command::start) it for streaming and
//!   interactive I/O. The [`ProcessRunner`] trait runs commands to completion
//!   and is the mock seam (see [`ScriptedRunner`](testing::ScriptedRunner)). A
//!   [`Supervisor`] keeps a command *alive* — restarting it per policy with
//!   backoff — where [`Command::retry`] merely replays one run to success.
//!   Readiness probes ([`RunningProcess::wait_for_line`] /
//!   [`wait_for_port`](RunningProcess::wait_for_port) /
//!   [`wait_for`](RunningProcess::wait_for)) wait until a started child is
//!   actually *ready* instead of sleeping. A [`Pipeline`]
//!   ([`Command::pipe`]) chains commands stdout→stdin without a shell — each
//!   stage spawns into its own kill-on-drop `ProcessGroup` sub-group, with
//!   chain-wide teardown fanning the kill across every sub-group, pipefail
//!   outcome. [`Command::cancel_on`] ties a run to a
//!   [`CancellationToken`]: cancelling it kills the tree and every consuming
//!   path resolves to [`ErrorReason::Cancelled`]. Spawn-time sandboxing knobs:
//!   [`Command::inherit_env`] (env allow-list), [`Command::uid`] /
//!   [`Command::gid`] (Unix privilege drop), [`Command::setsid`],
//!   [`Command::create_no_window`], [`Command::priority`] (CPU-scheduling
//!   priority, both platforms), [`Command::umask`] (Unix file-creation mask).
//!
//! Async throughout (tokio). Errors are the structured [`Error`]; a non-zero
//! exit is reported in [`ProcessResult`], not raised, until you call
//! [`ProcessResult::ensure_success`].
//!
//! **Stability.** Since **1.0**, `processkit` follows [Semantic Versioning]: the
//! public API is stable, and any breaking change lands only in a new *major*
//! version, so `2.x` upgrades are backward-compatible (the last breaking release
//! was **2.1.0**). (The lone exception is the `mock` feature's `mockall`-generated
//! `expect_*` surface — see below.)
//!
//! [Semantic Versioning]: https://semver.org/spec/v2.0.0.html
//!
//! **Stable machine identifiers.** The reporting and configuration enums —
//! [`Mechanism`], [`Outcome`], [`ParentDeathCleanup`], [`StopReason`],
//! [`StdioMode`], [`LineTerminator`], [`OverflowMode`], [`Priority`],
//! [`RestartPolicy`], plus the feature-gated `LimitKind` / `LimitReason`
//! (`limits`) and `Signal` / `SoftStopScope` (`process-control`), given as bare
//! names here since this crate-root doc also builds with those features off —
//! each
//! expose a `name()` that returns a short, lowercase `snake_case` identifier
//! for machine-readable output (a CLI's JSONL schema, a cross-language binding,
//! a structured log field), so a consumer publishing a contract over these
//! types has one canonical spelling per variant instead of a hand-maintained
//! table. These identifiers are a *diagnostic* surface, **not** a wire format,
//! but they carry the same stability promise as the rest of the public API: a
//! new variant gets a new identifier, and an existing identifier is never
//! renamed without a major release. Every enum whose value can arrive from
//! outside (config, CLI, another language) also has a `from_name(&str)` inverse
//! that returns `None` — an honest miss, never a silent default — on an
//! unrecognized name. See the [Errors guide]'s "Stable machine identifiers"
//! section for the whole set.
//!
//! [Errors guide]: https://github.com/ZelAnton/ProcessKit-rs/blob/main/docs/errors.md
//!
//! Beyond this page, the repository ships a narrative [guide set] — a
//! task-oriented [cookbook] ("I want to …" → snippet), a deep guide per
//! capability, and every per-platform caveat collected in one place.
//!
//! [guide set]: https://github.com/ZelAnton/ProcessKit-rs/tree/main/docs#readme
//! [cookbook]: https://github.com/ZelAnton/ProcessKit-rs/blob/main/docs/cookbook.md
//!
//! **Run vocabulary** — one verb, one meaning, at every layer ([`Command`],
//! [`ProcessRunner`]/[`ProcessRunnerExt`], [`CliClient`]):
//!
//! - **`run`** — require an **accepted** exit (`0` by default, widened by
//!   [`Command::ok_codes`]) and return stdout as a `String`, trailing whitespace
//!   trimmed (`trim_end`: the final newline is noise, but leading whitespace can
//!   be significant). **`run_unit`** — the same, discarding the output.
//! - **`output_string`** / **`output_bytes`** — return the full
//!   [`ProcessResult`] (stdout as text / raw bytes); a non-zero exit is *not* an
//!   error here. (`output_string`, not a bare `output`, since
//!   `std::process::Command::output` yields *bytes* — the explicit name avoids
//!   that footgun and is spelled the same on every layer.)
//! - **`exit_code`** — the exit code, with a missing code surfaced as an
//!   error. (On a [`ProcessResult`], [`code`](ProcessResult::code) is the
//!   plain `Option<i32>` accessor — `None` for a timeout/signal kill, never a
//!   `-1` sentinel.)
//! - **`probe`** — run a predicate and read its exit code as a `bool`: `0` →
//!   `true`, `1` → `false`, anything else is an error (`git diff --quiet`, …).
//! - **`parse`** / **`try_parse`** — run to a clean success and feed the
//!   captured stdout to a closure: `parse` for an infallible closure,
//!   `try_parse` for one returning [`Result`] (the JSON-deserialization
//!   shape). **`Send`-contract exception:** [`Command::parse`] /
//!   [`CliClient::parse`] require `F: Send` (and `T: Send`), so the returned
//!   future is `Send` and movable into `tokio::spawn`; [`Pipeline::parse`]
//!   deliberately does **not** require `Send` — its closure runs inline on
//!   the awaiting task rather than across a `tokio::spawn` boundary, so it
//!   accepts strictly more closures, but the resulting future is `Send` only
//!   when `F`/`T` happen to be. If you need to move a `Pipeline::parse` /
//!   `Pipeline::try_parse` call into `tokio::spawn`, make sure your closure
//!   and its output are `Send` yourself; the compiler won't require it for
//!   you the way it does for `Command`/`CliClient`.
//!
//! # Features
//!
//! Every flag is *additive* and gates visibility only — the kill-on-drop tree
//! guarantee is unconditional in every configuration.
//!
//! - **`stats`** — resource measurement: `ProcessGroupStats`,
//!   `ProcessGroup::stats` (plus the `sample_stats` time-series sampler and its
//!   owning `'static` twin `OwnedStatsSampler`), the
//!   per-process `RunningProcess::cpu_time`/`peak_memory_bytes` diagnostics,
//!   and the `RunningProcess::profile` run summary. **Opt-in** for its
//!   specialized purpose (on Windows it calls the system `ProcessStatus`/PSAPI
//!   API — a link to an OS library, *not* an added crate dependency); enable with
//!   `features = ["stats"]`, or `limits`, which implies it. (The features that do
//!   pull an extra crate are `mock` → `mockall`, `tracing` → `tracing`, and
//!   `record` → `serde`/`serde_json`.)
//! - **`process-control`** *(default)* — tree control beyond contain+kill:
//!   `Signal` and `ProcessGroup::{signal, suspend, resume, members,
//!   members_info, adopt}`, the enriched `MemberInfo` member snapshot, and the
//!   free-standing `process_info` / `process_is_alive` queries for a pid held
//!   *outside* any group (reuse-safe liveness by the `(pid, start time)` pair).
//! - **`limits`** — whole-tree resource caps: `ResourceLimits`, the
//!   `max_memory`/`max_processes`/`cpu_quota` builders on
//!   [`ProcessGroupOptions`], and `ErrorReason::ResourceLimit`. Implies `stats`.
//! - **`mock`** — the `mockall`-generated `testing::MockRunner` for
//!   consumers' tests. Its
//!   `expect_*` surface is generated by `mockall` and is **exempt from this
//!   crate's semver guarantees** — it tracks the `mockall` version (an
//!   implementation detail) rather than a frozen API. The first-class doubles
//!   ([`ScriptedRunner`](testing::ScriptedRunner) /
//!   [`RecordingRunner`](testing::RecordingRunner)) are the stable, recommended
//!   seam; reach for `mock` only if you specifically want expectation-style
//!   mocking.
//! - **`tracing`** — `tracing` events on the `processkit` target: spawn and
//!   exit (program/pid/mechanism), timeout and cancellation firing, group
//!   terminate/shutdown, retry attempts, supervisor restarts and storm
//!   pauses, and teardown anomalies (stdin-writer failures, pump overruns).
//!   Never logs argv or environment values.
//! - **`record`** — record/replay cassettes over the [`ProcessRunner`] seam:
//!   `RecordReplayRunner` records real `Invocation → ProcessResult` pairs to a
//!   JSON fixture once, then replays them hermetically — no subprocess in CI.
//!   Pulls in `serde` + `serde_json`.
//!
//! # Other languages
//!
//! Not on Rust? [`processkit-py`](https://pypi.org/project/processkit-py/) is a
//! Python wrapper (PyO3 bindings) over this crate's core, with an asyncio-facing
//! API. This crate remains the single source of truth for the containment/runner
//! logic underneath.
//!
//! [Job Object]: https://learn.microsoft.com/windows/win32/procthread/job-objects
//! [cgroup v2]: https://docs.kernel.org/admin-guide/cgroup-v2.html

mod backoff;
mod batch;
mod buffer;
#[cfg(feature = "record")]
mod cassette;
mod client;
mod command;
// FNV-1a helper shared by the two cassette-key digests (`Stdin::content_digest`
// and `MatchPolicy::digest_of`), so their constants + mix loop have one home and
// can't drift apart. Both call sites live under `record`, so the helper does too.
#[cfg(feature = "record")]
mod digest;
// Compiles docs/*.md + README.md's fenced Rust blocks as doctests under
// `--all-features` (see the module's own doc comment). Under `cfg(test)`, the
// sanity test stays available to ordinary `cargo test` with any feature
// configuration, including default and `--no-default-features`.
#[cfg(any(
    test,
    all(
        feature = "process-control",
        feature = "stats",
        feature = "limits",
        feature = "mock",
        feature = "tracing",
        feature = "record"
    )
))]
mod doc_examples;
mod doubles;
mod error;
mod group;
#[cfg(feature = "limits")]
mod limits;
// `process_info` / `process_is_alive` — the free-standing identity & reuse-safe
// liveness queries for an arbitrary pid held outside any group. Gated with the
// `MemberInfo` they return and the `process-control` readers they reuse.
#[cfg(feature = "process-control")]
mod lookup;
mod mechanism;
// `MemberInfo` — the enriched member snapshot returned by
// `ProcessGroup::members_info` and the free-standing `process_info` query. Gated
// with the methods it exists for.
#[cfg(feature = "process-control")]
mod member;
// `ParentDeathCleanup` — the honest per-platform capability report for
// `Command::kill_on_parent_death`. Unconditional, like the knob it describes.
mod parent_death;
mod pipeline;
mod priority;
mod pump;
mod result;
mod retry;
mod runner;
mod running;
// `ShutdownReport` / `SoftSignal` — the observed facts of a graceful
// `ProcessGroup::stop`. Gated with the method (and the `Signal` its `SoftSignal`
// carries).
#[cfg(feature = "process-control")]
mod shutdown_report;
#[cfg(feature = "process-control")]
mod signal;
// `SoftStopScope` — the runtime, per-group reach of a soft stop on the group
// axis, reported by `ProcessGroup::soft_stop_scope`. Gated with the `signal`
// verb it precedes.
#[cfg(feature = "process-control")]
mod soft_stop;
#[cfg(feature = "stats")]
mod stats;
mod stdin;
mod supervisor;
// The `cfg(loom)`-swappable sync layer (std::sync in ordinary builds, loom models
// under `--cfg loom` test builds) that the PID-lifecycle lock-free protocols build
// on. See `sync.rs` for why it gates on `all(loom, test)`.
mod sync;
mod sys;

/// Clamp ceiling for `Instant + Duration` deadline math: a timeout, grace,
/// or `within` longer than this is treated as "effectively forever", so a
/// `Duration::MAX`-ish input can't overflow `Instant + Duration` and panic.
/// ~10 years — far beyond any real process deadline, with ample margin below
/// `Instant`'s representable range on every platform.
pub(crate) const MAX_DEADLINE: std::time::Duration =
    std::time::Duration::from_secs(10 * 365 * 24 * 60 * 60);

pub use batch::{output_all, output_all_bytes, output_stream, output_stream_bytes};
pub use buffer::{
    CapturePolicy, LineTerminator, OutputBufferPolicy, OutputStream, OverflowMode, StdioMode,
};
pub use client::{CliClient, IntoCommand};
pub use command::Command;
pub use error::{Error, ErrorKind, ErrorReason, OutputOverflow, Result};
pub use group::{ProcessGroup, ProcessGroupOptions};
#[cfg(feature = "limits")]
pub use limits::{LimitKind, LimitReason, ResourceLimits};
#[cfg(feature = "process-control")]
pub use lookup::{process_info, process_is_alive};
pub use mechanism::{HostContainment, Mechanism};
#[cfg(feature = "process-control")]
pub use member::MemberInfo;
pub use parent_death::ParentDeathCleanup;
pub use pipeline::{Pipeline, PipelineSession};
pub use priority::Priority;
// Fuzzing-only entry point for `fuzz/fuzz_targets/decode_pump_lines.rs` (see
// `src/pump.rs`). `cfg(fuzzing)` is set automatically by `cargo fuzz build`
// for the whole dependency graph, never in an ordinary build — so this
// never shows up in `cargo public-api`'s (`--all-features`, no `--cfg
// fuzzing`) surface, and thus never touches `public-api.txt`.
#[cfg(fuzzing)]
pub use pump::fuzz_decode_pump_lines;
// Fuzzing-only cassette seams keep the ordinary API file-based while letting
// cargo-fuzz exercise parser and replay state directly from in-memory input.
#[cfg(all(fuzzing, feature = "record"))]
pub use cassette::{fuzz_cassette_parse, fuzz_cassette_replay};
pub use result::{Outcome, ProcessResult};
pub use retry::RetryPolicy;
pub use runner::{JobRunner, ProcessRunner, ProcessRunnerExt};
pub use running::{Finished, OutputLine, ProcessEvent, ProcessEvents, RunningProcess, StdoutLines};
#[cfg(feature = "process-control")]
pub use shutdown_report::{ShutdownReport, SoftSignal};
#[cfg(feature = "process-control")]
pub use signal::Signal;
#[cfg(feature = "process-control")]
pub use soft_stop::SoftStopScope;
#[cfg(feature = "stats")]
pub use stats::{OwnedStatsSampler, ProcessGroupStats, RunProfile, StatsSampler};
pub use stdin::{ProcessStdin, Stdin};
pub use supervisor::{
    GiveUpAttempt, RestartPolicy, StopReason, SupervisionOutcome, SupervisionSession,
    SupervisionStatus, Supervisor,
};

use std::ffi::OsStr;

/// Run `program` with `args` inside a private job and return trimmed stdout, or
/// an [`Error`] on a non-zero exit / spawn failure / timeout. A thin shim over
/// [`Command`]; use the builder for a working directory, env, stdin, timeout, or
/// the full verb vocabulary.
///
/// # Errors
///
/// The same surface as [`Command::run`]: a launch failure ([`ErrorReason::NotFound`] /
/// [`ErrorReason::Spawn`] / [`ErrorReason::Unsupported`] / [`ErrorReason::Io`]), a non-accepted
/// exit ([`ErrorReason::Exit`]), [`ErrorReason::Signalled`], [`ErrorReason::Timeout`], or
/// [`ErrorReason::OutputTooLarge`] on a fail-loud truncation.
pub async fn run<I, S>(program: impl AsRef<OsStr>, args: I) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program).args(args).run().await
}

/// Run `program` with `args` inside a private job and capture the result
/// without erroring on a non-zero exit — for commands whose exit code is meaningful.
///
/// # Errors
///
/// The same surface as [`Command::output_string`]: a non-zero exit, a timeout,
/// and a signal-kill are *captured* in the returned [`ProcessResult`], not
/// raised; beyond a launch failure, only [`ErrorReason::Cancelled`],
/// [`ErrorReason::OutputTooLarge`], [`ErrorReason::Stdin`], and [`ErrorReason::Io`] surface.
pub async fn output_string<I, S>(
    program: impl AsRef<OsStr>,
    args: I,
) -> Result<ProcessResult<String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program).args(args).output_string().await
}

/// Resolve `program` to a concrete executable path **without launching it** — a
/// spawn-free preflight for a *doctor* / early-diagnosis check ("is `git`
/// installed?") that must have **no** side effects. A thin shim over
/// [`Command::new(program).resolve_program()`](Command::resolve_program); use the
/// builder form when you need `prefer_local` directories or a relocated `PATH`
/// honored (the client-level [`CliClient::resolve_program`] does the same for a
/// wrapped tool).
///
/// Resolution reuses the crate's *own* launch-path logic — a bare name is looked
/// up on `PATH` honoring PATHEXT on Windows and the execute bit on Unix; a
/// path-form `program` is probed directly — so a `which` hit is exactly what a
/// real run would spawn, at that same absolute path. On Windows this includes a
/// bare name reachable only through a non-`.exe` PATHEXT extension
/// (`yarn.cmd`/`npx.cmd` and similar shims): the launch substitutes the resolved
/// path, so such a hit spawns rather than failing. Synchronous and cheap (a few
/// `stat`s); no async runtime is required.
///
/// **The reverse holds on Unix, not fully on Windows.** A `which` miss is the
/// exact [`ErrorReason::NotFound`] a run would raise on Unix, where `execvp` searches
/// `PATH` only. On Windows the OS *also* locates a bare name through routes this
/// `PATH`-based preflight deliberately doesn't model — the application directory,
/// the current directory, and the system directories — so a Windows `which` miss
/// is not a guarantee a run couldn't still launch the program by one of those
/// routes.
///
/// # Errors
///
/// [`ErrorReason::NotFound`] when the program can't be located
/// — not installed, not on `PATH`, or a path that doesn't resolve to an
/// executable. Its `searched` field names the directories checked for a
/// bare-name lookup, and [`is_not_found`](crate::Error::is_not_found) classifies
/// it — the same error, with the same classification, a real run would give.
pub fn which(program: impl AsRef<OsStr>) -> Result<std::path::PathBuf> {
    Command::new(program).resolve_program()
}

/// Report how process containment behaves on **this** host **without creating a
/// container or spawning anything** — a spawn-free preflight (a *doctor* /
/// host-check command that must have no side effects) that answers what a
/// [`ProcessGroup`] would otherwise only reveal *after* it exists: which
/// [`Mechanism`] a group created here and now would use, how far a soft stop
/// reaches, what the OS guarantees on abrupt owner death, and this crate's version.
///
/// See [`HostContainment`] for the full contract of each field. In particular the
/// [`mechanism`](HostContainment::mechanism) is determined by a read-only probe
/// (the shared `Mechanism::detect`) that on Linux is **best-effort**: it inspects whether
/// a cgroup could be created rather than creating one, so in a rare window it can
/// differ from the mechanism a real [`ProcessGroup::new`](ProcessGroup::new) falls
/// back to. Like [`which`], no async runtime is required.
///
/// ```
/// let host = processkit::host_containment();
/// // e.g. log the containment story a run *would* get, before starting anything:
/// let _ = (host.mechanism(), host.parent_death_cleanup(), host.crate_version());
/// ```
#[must_use]
pub fn host_containment() -> HostContainment {
    HostContainment::probe()
}

/// Wait for whichever of several running processes exits **first**, returning
/// its index in `processes` and its [`Outcome`] (matching
/// [`RunningProcess::wait`]).
///
/// The processes are only *borrowed*: the race is cancel-safe, so the losers —
/// and the winner, whose exit status tokio caches — remain fully usable
/// afterwards ([`wait`](RunningProcess::wait), another `wait_any`, …).
///
/// Two deliberate non-features:
///
/// - **No per-process [`timeout`](Command::timeout)** — the configured deadline
///   is armed by the consuming wait paths, not here. Bound the whole race with
///   [`tokio::time::timeout`] when a deadline is wanted.
/// - **No output pumping** — a contender that fills its stdout/stderr pipe
///   blocks and never exits. Drain chatty children first (e.g. via
///   [`stdout_lines`](RunningProcess::stdout_lines)) or race low-output ones.
///   Note the interplay: a [`tokio::time::timeout`] bounding the race fires,
///   but leaves such pipe-blocked contenders alive and still wedged — kill or
///   drain them afterwards; the timeout alone is not the mitigation.
/// - **No stdin management** — symmetrically, a contender started with
///   [`keep_stdin_open`](Command::keep_stdin_open) and blocked reading stdin
///   never reaches EOF, so it never exits. The race does **not** close its
///   stdin for it (that would break the "losers remain usable" guarantee):
///   take its writer via [`take_stdin`](RunningProcess::take_stdin)
///   (or don't keep stdin open) before racing it.
///
/// An empty `processes` slice is an error ([`ErrorReason::Io`] with
/// [`InvalidInput`](std::io::ErrorKind::InvalidInput)) rather than a future
/// that never resolves.
///
/// The first finisher's result carries the same errors as a bulk verb:
/// `ErrorReason::Cancelled` for a cancelled run, or [`ErrorReason::Stdin`] when its stdin
/// source failed (non-broken-pipe) on an otherwise-successful exit. A non-zero
/// exit or signal is *not* an error here — it is returned as its [`Outcome`].
///
/// # Errors
///
/// [`ErrorReason::Io`] with [`InvalidInput`](std::io::ErrorKind::InvalidInput) when
/// `processes` is empty. Otherwise the first finisher's error surfaces:
/// [`ErrorReason::Cancelled`] (a cancelled run), [`ErrorReason::Stdin`] (a non-broken-pipe
/// stdin-source failure on an otherwise-successful exit), or [`ErrorReason::Io`] (a
/// failed reap). A non-zero exit or signal is returned as an [`Outcome`], not an
/// error.
pub async fn wait_any(processes: &mut [&mut RunningProcess]) -> Result<(usize, Outcome)> {
    use std::future::Future;

    if processes.is_empty() {
        return Err(Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "wait_any requires at least one process",
        )));
    }
    let mut waits: Vec<_> = processes
        .iter_mut()
        .map(|process| Box::pin(process.wait_exit()))
        .collect();
    // Hand-rolled race (avoids a `futures` dependency): first `Ready` wins, the
    // rest are dropped cancel-safe so the caller can still wait on them.
    std::future::poll_fn(move |cx| {
        for (idx, wait) in waits.iter_mut().enumerate() {
            if let std::task::Poll::Ready(result) = wait.as_mut().poll(cx) {
                return std::task::Poll::Ready(result.map(|outcome| (idx, outcome)));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// Wait for **all** of several running processes to exit, returning their
/// [`Outcome`]s in the same order as `processes`. The processes are only
/// *borrowed* and stay usable afterwards (the exit status tokio caches remains
/// re-readable).
///
/// Same two non-features as [`wait_any`]: **no per-process
/// [`timeout`](Command::timeout)** (bound the whole batch with
/// [`tokio::time::timeout`]) and **no output pumping** (a contender that fills
/// its stdout/stderr pipe blocks forever — drain chatty children first). Unlike
/// `wait_any`, an empty slice resolves immediately to an empty `Vec`: collecting
/// zero outcomes is well-defined, where racing none is not.
///
/// If a contender fails to reap (an OS I/O error), that `Err` is returned and
/// the remaining processes stay waitable (cancel-safe). A contender's
/// `ErrorReason::Cancelled` (cancelled run) or [`ErrorReason::Stdin`] (a non-broken-pipe
/// stdin-source failure on its otherwise-successful exit) likewise short-circuits
/// the join — like the bulk verbs, these surface as an `Err`, not an `Outcome`.
///
/// # Errors
///
/// A contender's [`ErrorReason::Io`] (a failed reap), [`ErrorReason::Cancelled`] (a
/// cancelled run), or [`ErrorReason::Stdin`] (a non-broken-pipe stdin-source failure
/// on its otherwise-successful exit) short-circuits the join; the remaining
/// processes stay waitable (cancel-safe). A non-zero exit or signal is returned
/// as an [`Outcome`], not an error. An empty slice resolves to an empty `Vec`.
///
/// # Panics
///
/// Does not panic on any caller input: the final collection step carries an
/// internal consistency assertion (every outcome slot is filled once all
/// contenders have exited, an invariant the join loop maintains). It is
/// documented only because that assertion is a hard `expect`.
pub async fn wait_all(processes: &mut [&mut RunningProcess]) -> Result<Vec<Outcome>> {
    use std::future::Future;
    use std::task::Poll;

    // A slot goes `None` once resolved so finishers aren't re-polled.
    let mut waits: Vec<_> = processes
        .iter_mut()
        .map(|process| Some(Box::pin(process.wait_exit())))
        .collect();
    // `None` outcome slot = not yet resolved; all are `Some` when `remaining ==
    // 0`, so the final `expect` cannot fire.
    let mut outcomes: Vec<Option<Outcome>> = vec![None; waits.len()];
    let mut remaining = waits.len();

    // Hand-rolled join (avoids a `futures` dependency): store each outcome at its
    // input-order index, resolve once all have exited. Cancel-safe like wait_any.
    std::future::poll_fn(move |cx| {
        for (idx, slot) in waits.iter_mut().enumerate() {
            if let Some(wait) = slot.as_mut()
                && let Poll::Ready(result) = wait.as_mut().poll(cx)
            {
                match result {
                    Ok(outcome) => {
                        outcomes[idx] = Some(outcome);
                        *slot = None;
                        remaining -= 1;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
        }
        if remaining == 0 {
            Poll::Ready(Ok(std::mem::take(&mut outcomes)
                .into_iter()
                .map(|o| o.expect("all slots filled when remaining == 0"))
                .collect()))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Test doubles for the [`ProcessRunner`] seam: a
/// [`ScriptedRunner`](testing::ScriptedRunner) that serves canned replies, a
/// [`RecordingRunner`](testing::RecordingRunner) that asserts on invocations,
/// the [`Invocation`](testing::Invocation) it captures, a
/// [`DryRunRunner`](testing::DryRunRunner) that renders and echoes commands
/// without spawning them, and (behind features) record/replay cassettes and a
/// `mockall` mock.
pub mod testing {
    pub use crate::doubles::{DryRunRunner, Invocation, RecordingRunner, Reply, ScriptedRunner};

    /// Record/replay cassette runner (enabled by the `record` feature).
    #[cfg(feature = "record")]
    pub use crate::cassette::RecordReplayRunner;

    /// The `mockall`-generated mock of [`ProcessRunner`](crate::ProcessRunner)
    /// (enabled by the `mock` feature), re-exported under a friendlier name.
    ///
    /// **Semver-exempt:** the `expect_*` builder surface is generated by
    /// `mockall` and its exact shape (including the opaque expectation types) is
    /// an implementation detail that follows the `mockall` dependency, **not**
    /// part of this crate's frozen public API. For a stable double, prefer
    /// [`ScriptedRunner`] (canned replies) or [`RecordingRunner`] (input
    /// assertions).
    #[cfg(feature = "mock")]
    pub use crate::runner::MockProcessRunner as MockRunner;
}

/// Re-exports of small vocabulary types from the crate's `0.x` dependencies,
/// kept out of the crate root so `use processkit::*` doesn't pull them in (and
/// so a future `0.x` major bump of either dependency stays contained to this
/// module rather than the whole crate surface).
///
/// ```
/// use processkit::prelude::StreamExt;
/// ```
pub mod prelude {
    /// Re-exported from [`encoding_rs`] so a caller can name the type passed to
    /// [`Command::stdout_encoding`](crate::Command::stdout_encoding) /
    /// [`stderr_encoding`](crate::Command::stderr_encoding) without a direct
    /// dependency on `encoding_rs`.
    pub use encoding_rs::Encoding;

    /// Re-exported from [`tokio_stream`] so callers can `.next()` the stdout/event
    /// streams (e.g. [`StdoutLines`](crate::StdoutLines) /
    /// [`ProcessEvents`](crate::ProcessEvents)) without a direct `tokio-stream`
    /// dependency. Collides with `futures::StreamExt` under a glob import — import
    /// by path (`processkit::prelude::StreamExt`) if both traits are in scope.
    pub use tokio_stream::StreamExt;
}

/// Re-exported so callers can `use processkit::CancellationToken;` without a
/// direct `tokio-util` dependency. See [`Command::cancel_on`].
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::Outcome;

    /// The deadline-clamp ceiling must be small enough that
    /// `Instant + MAX_DEADLINE` cannot overflow, and a `Duration::MAX` input must
    /// clamp down to it — so `Instant::now() + within.min(MAX_DEADLINE)` is
    /// panic-free for any timeout/grace, however absurd.
    #[test]
    fn max_deadline_clamp_prevents_instant_overflow() {
        use std::time::{Duration, Instant};
        let _ = Instant::now() + super::MAX_DEADLINE; // must not panic
        assert_eq!(Duration::MAX.min(super::MAX_DEADLINE), super::MAX_DEADLINE);
    }

    // Regression: a bulk verb on the winner after a late cancel must not
    // reclassify its natural exit as Err(Cancelled) (wait_exit must snapshot
    // cancel_at_exit rather than re-evaluate the now-cancelled token).
    #[tokio::test]
    async fn wait_any_winner_natural_exit_preserved_after_late_cancel() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("test-prog").cancel_on(token.clone()))
            .await
            .expect("start scripted process");

        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0), "process exited naturally");

        token.cancel(); // after the natural exit
        let result = process.wait().await.expect("wait after wait_any");
        assert_eq!(
            result,
            Outcome::Exited(0),
            "natural exit must not be converted to Err(Cancelled)"
        );
    }

    // Regression: the same snapshot hazard via a *second* wait_any (the
    // "race them, keep watching the rest" pattern) rather than a bulk verb.
    #[tokio::test]
    async fn wait_any_winner_preserved_after_late_cancel_and_second_wait_any() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("test-prog").cancel_on(token.clone()))
            .await
            .expect("start scripted process");

        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("first wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0));

        token.cancel();

        let (idx2, outcome2) = super::wait_any(&mut [&mut process])
            .await
            .expect("second wait_any must not error after a late cancel");
        assert_eq!(idx2, 0);
        assert_eq!(
            outcome2,
            Outcome::Exited(0),
            "repeat wait_any must preserve the natural exit, not reclassify as Cancelled"
        );
    }

    // Regression for wait_all: a late cancel then a re-join must not error the
    // whole batch (it short-circuits on first Err, discarding every outcome).
    #[tokio::test]
    async fn wait_all_winners_preserved_after_late_cancel_and_re_wait() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut a = runner
            .start(&crate::Command::new("a").cancel_on(token.clone()))
            .await
            .expect("start a");
        let mut b = runner
            .start(&crate::Command::new("b").cancel_on(token.clone()))
            .await
            .expect("start b");

        let outcomes = super::wait_all(&mut [&mut a, &mut b])
            .await
            .expect("first wait_all");
        assert_eq!(outcomes, vec![Outcome::Exited(0), Outcome::Exited(0)]);

        token.cancel();

        let outcomes2 = super::wait_all(&mut [&mut a, &mut b])
            .await
            .expect("re-join after a late cancel must not error");
        assert_eq!(
            outcomes2,
            vec![Outcome::Exited(0), Outcome::Exited(0)],
            "repeat wait_all must preserve natural exits, not reclassify as Cancelled"
        );
    }

    #[tokio::test]
    async fn wait_returns_outcome() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let process = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let outcome = process.wait().await.expect("wait");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    #[tokio::test]
    async fn wait_any_returns_outcome() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let (idx, outcome) = super::wait_any(&mut [&mut process])
            .await
            .expect("wait_any");
        assert_eq!(idx, 0);
        assert_eq!(outcome, Outcome::Exited(0));
    }

    #[tokio::test]
    async fn wait_all_returns_outcomes() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut a = runner
            .start(&crate::Command::new("a"))
            .await
            .expect("start a");
        let mut b = runner
            .start(&crate::Command::new("b"))
            .await
            .expect("start b");
        let outcomes = super::wait_all(&mut [&mut a, &mut b])
            .await
            .expect("wait_all");
        assert_eq!(outcomes, vec![Outcome::Exited(0), Outcome::Exited(0)]);
    }

    #[tokio::test]
    async fn wait_all_collects_a_mix_of_outcomes_in_order() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        // Distinct terminal states must each surface as their own Outcome, in order.
        let runner = ScriptedRunner::new()
            .on(["p", "clean"], Reply::ok(""))
            .on(["p", "fail"], Reply::fail(3, "boom"))
            .on(["p", "killed"], Reply::signalled(Some(9)));
        let mut a = runner
            .start(&crate::Command::new("p").arg("clean"))
            .await
            .expect("start a");
        let mut b = runner
            .start(&crate::Command::new("p").arg("fail"))
            .await
            .expect("start b");
        let mut c = runner
            .start(&crate::Command::new("p").arg("killed"))
            .await
            .expect("start c");
        let outcomes = super::wait_all(&mut [&mut a, &mut b, &mut c])
            .await
            .expect("wait_all");
        assert_eq!(
            outcomes,
            vec![
                Outcome::Exited(0),
                Outcome::Exited(3),
                Outcome::Signalled(Some(9)),
            ]
        );
    }

    // Regression: a run cancelled before exit must surface as Err(Cancelled)
    // from wait_any, not Ok(Signalled(None)).
    #[tokio::test]
    async fn wait_any_cancelled_run_surfaces_as_err_cancelled() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog").cancel_on(token.clone()))
            .await
            .expect("start");

        token.cancel(); // before wait_any
        let err = super::wait_any(&mut [&mut process])
            .await
            .expect_err("cancelled run must error");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Cancelled { .. }),
            "expected ErrorReason::Cancelled, got {err:?}"
        );
    }

    // Symmetry: a genuine cancellation must stay sticky across a re-wait, not
    // just a clean exit — the guard must not make cancellation non-sticky.
    #[tokio::test]
    async fn wait_any_genuine_cancel_stays_cancelled_on_re_wait() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;

        let token = crate::CancellationToken::new();
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut process = runner
            .start(&crate::Command::new("prog").cancel_on(token.clone()))
            .await
            .expect("start");

        token.cancel(); // genuine cancel before the race

        let err = super::wait_any(&mut [&mut process])
            .await
            .expect_err("first wait_any: cancelled run errors");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Cancelled { .. }),
            "got {err:?}"
        );

        let err2 = super::wait_any(&mut [&mut process])
            .await
            .expect_err("re-wait must stay cancelled, not flip to Ok");
        assert!(
            matches!(err2.reason(), crate::ErrorReason::Cancelled { .. }),
            "repeat wait_any must preserve the cancellation, got {err2:?}"
        );
    }

    #[tokio::test]
    async fn wait_any_on_an_empty_slice_errors_instead_of_pending() {
        let err = super::wait_any(&mut [])
            .await
            .expect_err("an empty race must error, not pend forever");
        match err.into_reason() {
            crate::ErrorReason::Io(source) => {
                assert_eq!(source.kind(), std::io::ErrorKind::InvalidInput);
            }
            other => panic!("expected ErrorReason::Io(InvalidInput), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_all_on_an_empty_slice_is_an_empty_vec() {
        // Unlike `wait_any`, joining zero processes is well-defined: it
        // resolves immediately to an empty `Vec`, not an error or a hang.
        let outcomes = super::wait_all(&mut [])
            .await
            .expect("an empty join resolves cleanly");
        assert!(outcomes.is_empty());
    }

    // ── output-capture integrity ─────────────────────────────────────────────

    // A bare finish (no prior stdout_lines) drains untaken stdout through the
    // internal discard sink — the caller never asked to capture it — so it does
    // NOT enforce fail_loud on that output, matching wait() (see below).
    #[tokio::test]
    async fn bare_finish_does_not_enforce_fail_loud_on_untaken_stdout() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let run = runner
            .start(&crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2)))
            .await
            .expect("start");
        let finished = run
            .finish()
            .await
            .expect("a bare finish discards untaken stdout, so fail_loud does not fire");
        assert_eq!(finished.outcome, Outcome::Exited(0));
    }

    // wait discards output, so it must never fire fail_loud (retain-nothing sink).
    #[tokio::test]
    async fn wait_does_not_error_on_fail_loud() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let run = runner
            .start(&crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2)))
            .await
            .expect("start");
        let outcome = run
            .wait()
            .await
            .expect("wait must succeed despite fail_loud");
        assert_eq!(outcome, Outcome::Exited(0));
    }

    // output_string after stdout_lines must see the lines the streaming pump
    // wrote, not silently return empty output.
    #[tokio::test]
    async fn output_string_after_stdout_lines_captures_buffered_output() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["x", "y", "z"]));
        let mut run = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        let _ = run.stdout_lines().unwrap(); // take the pipe, start the streaming pump
        let result = run.output_string().await.expect("output_string");
        assert!(
            !result.stdout().is_empty(),
            "output_string after stdout_lines must not return empty; got {:?}",
            result.stdout()
        );
    }

    // A second stdout_lines call is a loud error (stdout streams once), not a
    // silent empty stream — and the first pump's overflow is still seen by finish.
    #[tokio::test]
    async fn second_stdout_lines_errors_and_first_overflow_is_preserved() {
        use crate::buffer::OutputBufferPolicy;
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::prelude::StreamExt;
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::lines(["a", "b", "c"]));
        let cmd = crate::Command::new("prog").output_buffer(OutputBufferPolicy::fail_loud(2));
        let mut run = runner.start(&cmd).await.expect("start");
        let mut first = run.stdout_lines().expect("first stdout_lines");
        while first.next().await.is_some() {}
        let err = run
            .stdout_lines()
            .expect_err("a second stdout_lines must be a loud error");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Io(_)),
            "got {err:?}"
        );
        let err = run
            .finish()
            .await
            .expect_err("overflow from first pump must still be visible");
        assert!(
            matches!(err.reason(), crate::ErrorReason::OutputTooLarge { .. }),
            "expected OutputTooLarge, got {err:?}"
        );
    }

    // A second events() call is likewise a loud error: the first call consumed
    // stdout, so the second must fail rather than yield a silently-empty stream.
    #[tokio::test]
    async fn second_events_call_is_a_loud_error() {
        use crate::doubles::{Reply, ScriptedRunner};
        use crate::runner::ProcessRunner;
        let runner = ScriptedRunner::new().fallback(Reply::fail(1, "stderr-only"));
        let mut run = runner
            .start(&crate::Command::new("prog"))
            .await
            .expect("start");
        // The first call takes stdout; the second fails immediately on the
        // already-consumed stdout (no need to drain the first stream, which would
        // park awaiting its terminal `Exited` from a not-yet-driven finisher).
        let first = run.events().expect("first events");
        let err = run
            .events()
            .expect_err("a second events call must be a loud error");
        assert!(
            matches!(err.reason(), crate::ErrorReason::Io(_)),
            "got {err:?}"
        );
        drop(first);
        let _ = run.finish().await;
    }
}
