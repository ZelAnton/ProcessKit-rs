//! [`Pipeline`] — `a | b | c` without a shell.
//!
//! Each stage's stdout feeds the next stage's stdin — **no shell string**, so no
//! quoting or injection surface, and no `sh -c`. The connection is a small
//! in-process relay (a `tokio::io::copy` task per boundary), not a kernel pipe
//! spliced fd-to-fd; this has two consequences: a producer whose consumer exits
//! early stops on a [broken pipe](crate::Error) when the relay's next write fails
//! (rather than instantly via SIGPIPE), and the relay's own I/O is plumbing — a
//! closed sibling reads as EOF / writes as a broken pipe, neither reported as a
//! stage's stdin failure. Each stage spawns into its **own** kill-on-drop
//! [`ProcessGroup`](crate::ProcessGroup) sub-group, so a per-stage
//! [`Command::timeout`] tears down that stage's *whole* subtree (grandchildren of
//! a forking `sh -c …` included), while a chain-wide
//! [`Pipeline::timeout`]/teardown fans the kill across every sub-group so the
//! whole chain still dies as a unit. The outcome is **pipefail**: the first stage
//! without a clean exit decides the reported code/diagnostics.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::command::Command;
use crate::error::Result;
use crate::group::ProcessGroup;
use crate::result::{Outcome, ProcessResult};
use crate::running::{Finished, OutputEvents, RunningProcess, StdoutLines};
use crate::sync::atomic::{AtomicU8, Ordering};

/// A chain of [`Command`]s connected stdout→stdin — built with
/// [`Command::pipe`], extended with [`pipe`](Self::pipe), driven with the same
/// verb vocabulary as a single [`Command`]:
/// [`output_string`](Self::output_string) / [`output_bytes`](Self::output_bytes)
/// for capture, [`run`](Self::run) / [`run_unit`](Self::run_unit) /
/// [`checked`](Self::checked) for success-checked runs,
/// [`exit_code`](Self::exit_code) / [`probe`](Self::probe) for the code, and
/// [`parse`](Self::parse) / [`try_parse`](Self::try_parse) for typed output —
/// each operating on the **pipefail** outcome. Bound the whole chain with
/// [`timeout`](Self::timeout) / [`cancel_on`](Self::cancel_on).
///
/// [`first_line`](Command::first_line) is intentionally **not** on `Pipeline`:
/// a chain consumes its last stage in full to fold the pipefail outcome. For a
/// streaming readiness probe that leaves the chain alive, use a single
/// [`Command`] with `first_line` instead.
///
/// Semantics:
///
/// - **Per-stage subtree, one chain fate** — each stage runs in its own
///   kill-on-drop group, so a per-stage [`Command::timeout`] tears down that
///   stage's whole subtree (a forking `sh -c …`'s grandchildren included).
///   Cancelling the future or a chain-wide [`timeout`](Self::timeout) elapsing
///   still tears the *whole* chain down — the kill fans across every stage's
///   sub-group.
/// - **Pipefail** — `stdout` is always the *last* stage's output; `code`,
///   `stderr`, and the reported program come from the **first** stage that
///   didn't exit cleanly, or from the last stage when every stage succeeded.
///   [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are exempt:
///   checked failures always trump unchecked ones; a chain whose only failures
///   are unchecked reports success.
/// - **Stdin/stdout at the ends** — the *first* stage's configured
///   [`stdin`](Command::stdin) is honored; inner stages' stdin is the pipe
///   (any configured source is overridden). Inner stages' stderr is captured
///   per-stage for pipefail diagnostics.
/// - A per-stage [`Command::retry`] is **not** applied inside a pipeline;
///   wrap the `Pipeline` call to retry the whole chain.
/// - A one-shot [`Stdin`](crate::Stdin) source on the *first* stage is
///   consumed by the first run; re-running **fails loud** rather than silently
///   feeding empty stdin.
#[must_use = "a Pipeline does nothing until it is run"]
#[derive(Clone)]
pub struct Pipeline {
    stages: Vec<Command>,
    timeout: Option<Duration>,
    cancel_token: Option<tokio_util::sync::CancellationToken>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("stages", &self.stages.len())
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// One finished stage's data — input to the pipefail fold.
struct StageOutcome {
    program: String,
    outcome: Outcome,
    stderr: String,
    /// Stage opted out of pipefail attribution.
    unchecked: bool,
    /// Exit codes the stage treats as success.
    ok_codes: Vec<i32>,
    /// Stage's own configured timeout — carried so a timed-out stage reports its real deadline.
    timeout: Option<Duration>,
    /// Stage was ended by the chain's proactive teardown (the group was killed
    /// after a *sibling* failed), not by a failure of its own. Treated like a
    /// SIGPIPE victim in attribution: de-prioritized so the real culprit — the
    /// stage that triggered teardown — is the one blamed.
    torn_down: bool,
    /// Whether a bounded, non-fail-loud `OutputBufferPolicy` silently dropped
    /// this stage's stderr. Stamped onto the folded result's `truncated` when
    /// this stage is the one pipefail blames, so an inner stage's clipped
    /// diagnostics are visible even when it isn't the last stage.
    stderr_truncated: bool,
}

/// One stage task's outcome, tagged with enough to fold it back into position
/// after the stages complete in **unordered** (true completion) order — see
/// the comment on `capture`'s task-driving `JoinSet` for why unordered
/// completion, rather than left-to-right positional awaiting, is what keeps
/// the chain live.
enum Joined<T> {
    /// An inner (non-last) stage, tagged with its original index so the fold
    /// can rebuild `stages` in left-to-right order once every task is in.
    Inner(usize, StageOutcome),
    /// The last stage: its captured output (`capture_last`'s result) plus
    /// whether the chain's teardown had already fired when it finished.
    Last(ProcessResult<T>, bool),
}

/// A launched chain, before the last stage is split off — the shared product of
/// [`Pipeline::launch`], consumed by the buffering [`capture`](Pipeline::capture)
/// path and the streaming [`start`](Pipeline::start) path alike. Holding it keeps
/// every stage's kill-on-drop sub-group alive; dropping it (e.g. after a mid-chain
/// spawn failure returned by `launch` via `?`) tears every already-started stage
/// down, so a partially-launched chain never leaks.
struct LaunchedChain {
    /// A strong handle to every stage's kill-on-drop sub-group, so the chain-wide
    /// teardown can fan a hard kill across all of them.
    stage_groups: Vec<Arc<ProcessGroup>>,
    /// Every stage's live handle paired with its `unchecked_in_pipe` flag, in
    /// left-to-right order (the last stage is the final element).
    running: Vec<(RunningProcess, bool)>,
    /// Wall-clock start of the whole chain, captured before the first spawn.
    started: std::time::Instant,
}

/// The last stage split off a launched chain by [`Pipeline::detach_last`]: the
/// live streaming handle plus the pipefail metadata [`PipelineSession::finish`]
/// folds it back into position with.
struct DetachedLast {
    handle: RunningProcess,
    program: String,
    ok_codes: Vec<i32>,
    timeout: Option<Duration>,
    unchecked: bool,
}

impl Pipeline {
    pub(crate) fn new(first: Command, second: Command) -> Self {
        Pipeline {
            stages: vec![first, second],
            timeout: None,
            cancel_token: None,
        }
    }

    /// Append another stage: the current last stage's stdout becomes `next`'s
    /// stdin.
    pub fn pipe(mut self, next: Command) -> Self {
        self.stages.push(next);
        self
    }

    /// Kill the **whole chain** if it exceeds `timeout` (every stage's sub-group
    /// is torn down; the result reports `timed_out`). Unlike a single
    /// [`Command::timeout`] capture, no partial stdout is reported for a
    /// timed-out chain.
    ///
    /// This is the chain-wide backstop. A **per-stage** [`Command::timeout`] now
    /// also bounds a forking stage on its own: each stage runs in its own
    /// kill-on-drop group, so a per-stage deadline tears down that stage's whole
    /// subtree — a grandchild it forked (`sh -c …`) can no longer keep the stdout
    /// pipe open past the kill and stall the downstream stage. Reach for this
    /// whole-chain timeout when you want a single ceiling on the *entire* run
    /// regardless of which stage is slow; use a per-stage timeout to bound an
    /// individual stage. Either bounds a single-process stage.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Cancel the **whole chain** when `token` fires: the token reaches every
    /// stage (see gap-fill below), so each stage's run is cancelled and kills its
    /// own subtree, and the run resolves to
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled). This is **proactive** —
    /// firing the token cancels the stages directly, it does not wait for them to
    /// notice a closed pipe.
    ///
    /// A plain **stage failure** (a stage exits non-zero, is signal-killed, or hits
    /// its own timeout, *without* the chain being cancelled) is **also** proactive:
    /// the first such failure tears the whole group down at once, so a quiet,
    /// still-running sibling — classically an upstream producer that never writes,
    /// and so never dies of a broken pipe — cannot hold the run open. The failure
    /// still keeps its **pipefail** attribution: the stage that triggered teardown
    /// is blamed, while the siblings the teardown killed are treated as victims
    /// (like a downstream `SIGPIPE` death), never stealing the blame. The one
    /// death that does *not* trigger teardown is an
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) stage's — its unclean exit
    /// is forgiven, so it leaves the rest of the chain running.
    ///
    /// The token **gap-fills** — at launch it is applied to every stage that does
    /// not already carry its own [`Command::cancel_on`], leaving an explicit
    /// per-stage token intact (which still cancels the chain, since cancelling one
    /// stage errors the run and the group tears the rest down). This matches
    /// [`CliClient::default_cancel_on`](crate::CliClient::default_cancel_on)
    /// rather than silently overriding a per-stage choice. To have a stage
    /// cancelled by **both** its own token and the chain token, pass a child of
    /// this token as the stage's token (`token.child_token()`).
    ///
    /// Like [`Command::cancel_on`], a cancelled run is terminal: it is not
    /// retried, and the chain cannot be re-run through a token that stays
    /// cancelled.
    pub fn cancel_on(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Launch every stage of the chain — the shared start-up core reused by the
    /// buffering [`capture`](Self::capture) verbs and the streaming
    /// [`start`](Self::start) session, so a change to how a chain comes up (per-stage
    /// sub-groups, the stdout→stdin relay, cancel-token gap-fill) lands on both.
    ///
    /// Each stage spawns into its **own** kill-on-drop sub-group, retained as a
    /// strong handle so a per-stage [`Command::timeout`]/[`cancel_on`](Command::cancel_on)
    /// tears down that stage's *whole* subtree (grandchildren of a forking `sh -c …`
    /// included) and a chain-wide teardown can fan a kill across every one. The
    /// pipeline [`cancel_on`](Self::cancel_on) token gap-fills onto every stage that
    /// carries no token of its own.
    ///
    /// On a mid-chain spawn failure the `?` early-return drops the partially-built
    /// vectors, so kill-on-drop reaps every already-started stage — a partial chain
    /// never leaks (the "partially poured" teardown invariant).
    async fn launch(&self) -> Result<LaunchedChain> {
        // Wall-clock start of the whole chain, before the first spawn.
        let started = std::time::Instant::now();

        let mut stage_groups: Vec<Arc<ProcessGroup>> = Vec::with_capacity(self.stages.len());
        let mut running = Vec::with_capacity(self.stages.len());
        let mut upstream = None;
        for (index, stage) in self.stages.iter().enumerate() {
            let mut command = stage.clone();
            // Gap-fill: apply the pipeline cancel token only where a stage has no token of its own.
            if let Some(token) = &self.cancel_token
                && command.cancel_token().is_none()
            {
                command = command.cancel_on(token.clone());
            }
            if let Some(reader) = upstream.take() {
                command.set_pipe_stdin(reader);
            }
            // Spawn into a fresh per-stage group, then hand it to the stage handle
            // (`attach_group` also upgrades the cancel watchdog to a group+pid kill)
            // and keep a strong clone for the chain-wide teardown.
            let group = ProcessGroup::new()?;
            let mut process = group.start(&command).await?;
            process.attach_group(group);
            if let Some(handle) = process.own_group_handle() {
                stage_groups.push(handle);
            }
            if index + 1 < self.stages.len() {
                upstream = process.take_stdout_pipe();
            }
            // Bundle the unchecked flag with the handle; the last stage is split off by the caller.
            running.push((process, stage.is_unchecked()));
        }

        Ok(LaunchedChain {
            stage_groups,
            running,
            started,
        })
    }

    /// Split the last stage off a launched chain's handles as the streaming
    /// surface, paired with the pipefail metadata [`finish`](PipelineSession::finish)
    /// folds it back with. Kept private so its infallible `pop`/`last` (a
    /// `Pipeline` starts at and never drops below two stages, so `running` is never
    /// empty and `self.stages` is never empty here) stays out of the public
    /// [`start`](Self::start) — matching how the buffering verbs keep their
    /// same-invariant `expect`s inside the private `capture`.
    fn detach_last(&self, running: &mut Vec<(RunningProcess, bool)>) -> DetachedLast {
        let (handle, unchecked) = running.pop().expect("a pipeline has at least two stages");
        let stage = self
            .stages
            .last()
            .expect("a pipeline has at least two stages");
        DetachedLast {
            program: handle.program_name().to_owned(),
            ok_codes: stage.ok_codes_vec(),
            timeout: stage.configured_timeout(),
            unchecked,
            handle,
        }
    }

    /// Start the chain as a **live streaming session** — the multi-stage analogue
    /// of [`Command::start`](crate::Command::start), returning a
    /// [`PipelineSession`] you drive yourself instead of buffering the whole run.
    /// It brings the streaming surface (`journalctl -f | grep …`, `tail -F | jq`,
    /// any long-lived chain you read *from* rather than wait *out*) to pipelines.
    ///
    /// The session gives:
    ///
    /// - the **last** stage's stdout as it arrives —
    ///   [`stdout_lines`](PipelineSession::stdout_lines) /
    ///   [`output_events`](PipelineSession::output_events), with the same
    ///   consume-once contract as [`RunningProcess`](crate::RunningProcess) (a
    ///   second take is a loud `Err`, never a silently-empty stream);
    /// - a readiness wait on that stream —
    ///   [`wait_for_line`](PipelineSession::wait_for_line);
    /// - a [`finish`](PipelineSession::finish) that folds the same **pipefail**
    ///   outcome as the buffering verbs: the culprit stage's outcome and *its own*
    ///   stderr, not just the last stage's;
    /// - whole-chain teardown — [`start_kill`](PipelineSession::start_kill) and
    ///   kill-on-drop — plus the chain-wide [`timeout`](Self::timeout) /
    ///   [`cancel_on`](Self::cancel_on) still bounding the live session.
    ///
    /// # Errors
    ///
    /// A launch failure of any stage
    /// ([`ErrorReason::NotFound`](crate::ErrorReason::NotFound) /
    /// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) /
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported)) or
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) at start-up — every already-started
    /// stage is torn down before the error returns. A failing *stage* is not raised
    /// here; it surfaces at [`finish`](PipelineSession::finish).
    pub async fn start(&self) -> Result<PipelineSession> {
        let LaunchedChain {
            stage_groups,
            mut running,
            // A live session reports no duration (its `Finished` carries none — the
            // caller times the stream itself), so the launch anchor is unused here.
            started: _,
        } = self.launch().await?;

        // Split the last stage off as the live streaming surface; the caller drives
        // it. Carry its pipefail metadata so `finish` can fold it into position.
        // The infallible pop lives in `detach_last` (a `Pipeline` starts at and
        // never drops below two stages), so this public method carries no panic path.
        let DetachedLast {
            handle: last,
            program: last_program,
            ok_codes: last_ok_codes,
            timeout: last_timeout,
            unchecked: last_unchecked,
        } = self.detach_last(&mut running);

        let inner_count = running.len();

        // Proactive teardown token, exactly as in `capture`: an inner stage's first
        // checked failure (or a raw error) fires it, and the standing killer below
        // reacts by tearing the whole live chain down — so a quiet upstream can't
        // hold a failed streaming chain open.
        let teardown = tokio_util::sync::CancellationToken::new();

        // Background-drain every inner stage while the caller streams the last. Each
        // task reuses `finish_inner_stage` (the shared classify-and-teardown body)
        // and *additionally* fires teardown on a raw `Err`, standing in for the
        // central `drain_unordered` backstop the buffering path gets for free —
        // here no central drain runs while the caller streams, so each task must
        // self-report to keep the chain live behind a ready error.
        let mut inner_tasks: tokio::task::JoinSet<Result<(usize, StageOutcome)>> =
            tokio::task::JoinSet::new();
        for (index, ((process, unchecked), stage)) in
            running.into_iter().zip(self.stages.iter()).enumerate()
        {
            let program = process.program_name().to_owned();
            let ok_codes = stage.ok_codes_vec();
            let timeout = stage.configured_timeout();
            let teardown = teardown.clone();
            inner_tasks.spawn(async move {
                let result = finish_inner_stage(
                    process,
                    index,
                    program,
                    ok_codes,
                    timeout,
                    unchecked,
                    teardown.clone(),
                )
                .await;
                if result.is_err() {
                    teardown.cancel();
                }
                result
            });
        }

        // Standing teardown killer: fans a hard kill across every stage's sub-group
        // the instant `teardown` fires, so a failing inner stage tears the *whole*
        // live chain down (last stage included) even while the caller is mid-stream.
        // `Weak` handles so it never pins a group past a session drop; aborted on
        // `finish`/drop.
        let killer = spawn_group_killer(teardown.clone(), &stage_groups);

        // Chain-wide `Pipeline::timeout` on the live session: a background watchdog
        // reusing the shared deadline arbiter (no second implementation — K-034/K-007).
        // At the deadline it claims a fresh chain arbiter and hard-kills every
        // sub-group; `finish` reads the arbiter to report `Outcome::TimedOut`, exactly
        // as the buffering path's `tokio::time::timeout` branch does. Anchored to a
        // `tokio::time::Instant` (the deadline clock), captured now — after launch,
        // matching where the buffering `tokio::time::timeout(limit, collect)` begins.
        let chain_state = Arc::new(AtomicU8::new(crate::running::TS_PENDING));
        let deadline_task = self.timeout.map(|limit| {
            let state = chain_state.clone();
            let groups: Vec<Weak<ProcessGroup>> = stage_groups.iter().map(Arc::downgrade).collect();
            let anchor = tokio::time::Instant::now();
            tokio::spawn(async move {
                if crate::running::deadline::wait_deadline_and_claim(anchor, limit, &state).await {
                    kill_weak_stage_groups(&groups);
                }
            })
        });

        Ok(PipelineSession {
            last: Some(last),
            last_program,
            last_ok_codes,
            last_unchecked,
            last_timeout,
            inner_tasks: Some(inner_tasks),
            inner_count,
            stage_groups,
            teardown,
            timeout: self.timeout,
            chain_state,
            deadline_task,
            killer: Some(killer),
        })
    }

    /// Run the chain to completion and capture the outcome (stdout as text). A
    /// failing stage is **not** an `Err` here — it is reported in the result
    /// (pipefail attribution, see the type docs); `Err` means a stage could not
    /// be started or driven at all.
    ///
    /// # Errors
    ///
    /// A failing stage (and a timeout) is *captured* in the returned
    /// [`ProcessResult`] (pipefail attribution), not raised. `Err` means a stage
    /// could not be *started or driven*: a launch failure
    /// ([`ErrorReason::NotFound`](crate::ErrorReason::NotFound) /
    /// [`ErrorReason::Spawn`](crate::ErrorReason::Spawn) /
    /// [`ErrorReason::Unsupported`](crate::ErrorReason::Unsupported)),
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled),
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud
    /// overflow of the last stage), [`ErrorReason::Stdin`](crate::ErrorReason::Stdin), or
    /// [`ErrorReason::Io`](crate::ErrorReason::Io).
    pub async fn output_string(&self) -> Result<ProcessResult<String>> {
        self.capture(|last| async move { last.output_string().await })
            .await
    }

    /// Run the chain to completion and capture the last stage's stdout as **raw
    /// bytes** (the binary-pipe analogue of [`output_string`](Self::output_string)
    /// — e.g. `curl … | gunzip`). Pipefail attribution is identical; only the
    /// last stage's stdout is captured raw. Stderr (every stage, including the
    /// last) stays decoded text — it is diagnostics, never the binary payload.
    ///
    /// # Errors
    ///
    /// The same surface as [`output_string`](Self::output_string) — a failing
    /// stage (and a timeout) is captured, not raised — with the last stage's
    /// stdout captured as raw bytes.
    pub async fn output_bytes(&self) -> Result<ProcessResult<Vec<u8>>> {
        self.capture(|last| async move { last.output_bytes().await })
            .await
    }

    /// Start and chain every stage, drain concurrently, and fold the pipefail
    /// outcome. `capture_last` decides how the last stage's stdout is captured.
    async fn capture<T, C, F>(&self, capture_last: C) -> Result<ProcessResult<T>>
    where
        T: Default + Send + 'static,
        C: FnOnce(crate::running::RunningProcess) -> F,
        F: std::future::Future<Output = Result<ProcessResult<T>>> + Send + 'static,
    {
        // Launch the whole chain (shared with `start`'s streaming path): every
        // stage in its own kill-on-drop sub-group, stdout→stdin chained, strong
        // sub-group handles retained for the chain-wide teardown fan. `started` is
        // the wall-clock anchor from before the first spawn, so `duration()`
        // reflects the run, not just the last stage.
        let LaunchedChain {
            stage_groups,
            mut running,
            started,
        } = self.launch().await?;

        // Proactive teardown: the first stage to finish with a *checked failure*
        // fires this token; a concurrent killer then tears every stage's sub-group
        // down so a quiet, still-running sibling (classically an upstream producer
        // that never writes, so never dies of a broken pipe) cannot hold the chain
        // open after the failure. Stages ended by that kill are flagged `torn_down` and
        // de-prioritized in the pipefail fold, so the real culprit is still blamed.
        // This is distinct from the user's `cancel_token`: a cancelled stage errors
        // out (`Err(Cancelled)`) before producing a `StageOutcome`, so cancellation
        // never fires this teardown and the cancel path is unchanged.
        let teardown = tokio_util::sync::CancellationToken::new();

        // Drain concurrently: a stderr-chatty inner stage must not block on a full pipe.
        let (last, last_unchecked) = running.pop().expect("a pipeline has at least two stages");
        let last_stage = self
            .stages
            .last()
            .expect("a pipeline has at least two stages");
        let last_ok_codes = last_stage.ok_codes_vec();
        let last_timeout = last_stage.configured_timeout();
        // Drive every stage's task through one `JoinSet`, drained by
        // `drain_unordered` below: a stage's raw `Err` (`Cancelled` / `Stdin` /
        // `Io` / `OutputTooLarge`) or a task panic never reaches the
        // `is_checked_failure` check, so it can't fire `teardown` itself the
        // way a checked failure does — `drain_unordered` fires it centrally
        // for *every* bad completion, in true completion order rather than
        // stage position, so a later stage's ready error can't sit behind an
        // earlier, still-quiet stage forever.
        let inner_count = running.len();
        let mut tasks: tokio::task::JoinSet<Result<Joined<T>>> = tokio::task::JoinSet::new();
        for (index, ((process, unchecked), stage)) in
            running.into_iter().zip(self.stages.iter()).enumerate()
        {
            let program = process.program_name().to_owned();
            let ok_codes = stage.ok_codes_vec();
            let timeout = stage.configured_timeout();
            let teardown = teardown.clone();
            tasks.spawn(async move {
                // `finish_inner_stage` is the shared classify-and-teardown body,
                // reused by `start`'s streaming inner drains — so both paths blame
                // a stage and fire proactive teardown identically.
                let (index, outcome) = finish_inner_stage(
                    process, index, program, ok_codes, timeout, unchecked, teardown,
                )
                .await?;
                Ok(Joined::Inner(index, outcome))
            });
        }
        // Call `capture_last` here (not inside the spawned future): it yields a
        // `Send` future `F`, whereas the closure `C` itself is not `Send` and must
        // not be captured across the `tokio::spawn` boundary.
        let last_future = capture_last(last);
        {
            let teardown = teardown.clone();
            let last_ok_codes = last_ok_codes.clone();
            tasks.spawn(async move {
                let result = last_future.await?;
                // The last stage triggers teardown too (a failing last stage should
                // not wait on a quiet upstream either); torn if a sibling already did.
                let torn_down = teardown.is_cancelled();
                if !torn_down
                    && is_checked_failure(result.outcome(), &last_ok_codes, last_unchecked)
                {
                    teardown.cancel();
                }
                Ok(Joined::Last(result, torn_down))
            });
        }

        let collect = async {
            // On a bad completion `drain_unordered` fires `teardown` itself; the
            // killer arm below wakes and tears every stage's sub-group down,
            // unblocking whichever task was stalled on a quiet sibling, so
            // `gather` (however long the drain takes to notice) still finishes
            // and wins the `select!` rather than hanging next to a pending kill.
            let gather = async {
                let joined = drain_unordered(tasks, &teardown).await?;
                let mut inner_outcomes: Vec<Option<StageOutcome>> =
                    (0..inner_count).map(|_| None).collect();
                let mut last_slot: Option<(ProcessResult<T>, bool)> = None;
                for item in joined {
                    match item {
                        Joined::Inner(index, outcome) => inner_outcomes[index] = Some(outcome),
                        Joined::Last(result, torn_down) => last_slot = Some((result, torn_down)),
                    }
                }
                let outcomes: Vec<StageOutcome> = inner_outcomes
                    .into_iter()
                    .map(|outcome| {
                        outcome.expect("every inner stage slot is filled when every task succeeded")
                    })
                    .collect();
                let (last_result, last_torn_down) =
                    last_slot.expect("last slot is filled when every task succeeded");
                Ok::<_, crate::Error>((outcomes, last_result, last_torn_down))
            };
            tokio::select! {
                collected = gather => collected,
                // Fires once on the first checked failure, then never resolves —
                // it only exists for its kill side effect, letting `gather` finish.
                () = async {
                    teardown.cancelled().await;
                    kill_all_stage_groups(&stage_groups);
                    std::future::pending::<()>().await
                } => unreachable!("the teardown killer pends forever after firing"),
            }
        };

        let (mut stages, last_result, last_torn_down) = match self.timeout {
            None => collect.await?,
            Some(limit) => match tokio::time::timeout(limit, collect).await {
                Ok(collected) => collected?,
                Err(_elapsed) => {
                    // Kill every stage's subtree; `tasks` was moved into `gather`
                    // (via `drain_unordered`), which `collect` (and so this
                    // `tokio::time::timeout`) just dropped — the `JoinSet`'s own
                    // drop aborts every drain task still in flight, the same
                    // guarantee the old explicit abort-on-drop guard gave.
                    kill_all_stage_groups(&stage_groups);
                    return Ok(ProcessResult::new(
                        self.pipeline_name(),
                        T::default(),
                        String::new(),
                        Outcome::TimedOut,
                        Some(limit),
                    )
                    .with_duration(started.elapsed()));
                }
            },
        };

        // `pipefail` rebuilds via `ProcessResult::new` (which defaults `truncated=false`);
        // re-stamp truncation so the `parse`/`try_parse` guard fires correctly. The
        // last stage's own `truncated()` covers both its stdout and stderr drops —
        // reused below as this stage's `stderr_truncated` too, since either one
        // clips the chain's actual captured content regardless of attribution.
        let last_truncated = last_result.truncated();
        let (last_total_lines, last_total_bytes) =
            (last_result.total_lines(), last_result.total_bytes());
        let last_outcome = StageOutcome {
            program: last_result.program().to_owned(),
            outcome: last_result.outcome(),
            stderr: last_result.stderr().to_owned(),
            unchecked: last_unchecked,
            ok_codes: last_ok_codes,
            timeout: last_timeout,
            torn_down: last_torn_down,
            stderr_truncated: last_truncated,
        };
        let last_stdout = last_result.into_stdout();
        stages.push(last_outcome);

        let mut result = pipefail(stages, last_stdout).with_duration(started.elapsed());
        // The attributed stage's own stderr truncation is already folded in by
        // `pipefail` (see below); this additionally ORs in the last stage's own
        // capture truncation, since the chain's actual `stdout` is always the last
        // stage's — a clipped last-stage capture is real regardless of which stage
        // pipefail blames for the failure.
        if last_truncated {
            result = result
                .with_truncated(true)
                .with_overflow_totals(last_total_lines, last_total_bytes);
        }
        Ok(result)
    }

    /// Run the chain, require **every** stage to exit cleanly, and return the
    /// last stage's trimmed stdout. A failure surfaces as the first failing
    /// stage's [`ErrorReason::Exit`](crate::ErrorReason::Exit) (pipefail attribution;
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are exempt, so a chain whose
    /// only failures are unchecked returns `Ok`).
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) is produced by the whole-chain
    /// [`timeout`](Self::timeout) or by **any** stage's own
    /// [`Command::timeout`] — the attributed stage's *own* deadline is reported,
    /// not the chain's.
    ///
    /// # Errors
    ///
    /// The first failing stage's [`ErrorReason::Exit`](crate::ErrorReason::Exit) (pipefail
    /// attribution; [`unchecked_in_pipe`](Command::unchecked_in_pipe) stages are
    /// exempt), [`ErrorReason::Signalled`](crate::ErrorReason::Signalled),
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) (the whole-chain
    /// [`timeout`](Self::timeout) or any stage's own — the attributed stage's
    /// deadline is reported), [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled),
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud
    /// truncation of the last stage), plus any launch failure or
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin).
    pub async fn run(&self) -> Result<String> {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        Ok(out.into_stdout().trim_end().to_owned())
    }

    /// Run the chain, require **every** stage to exit cleanly (pipefail), and
    /// return the full captured [`ProcessResult`] (untrimmed stdout) — the
    /// building block when you need the whole result after success-checking,
    /// rather than trimmed stdout ([`run`](Self::run)). Mirrors
    /// [`Command::checked`](crate::Command::checked).
    ///
    /// # Errors
    ///
    /// The same pipefail surface as [`run`](Self::run) —
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit) /
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) /
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) /
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled), plus launch failures and
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) — but, as the lenient building block,
    /// it does not fail loud on a bounded-buffer truncation.
    pub async fn checked(&self) -> Result<ProcessResult<String>> {
        self.output_string().await?.ensure_success()
    }

    /// Run the chain for its side effect: require a clean pipefail outcome and
    /// discard the output. Mirrors [`Command::run_unit`](crate::Command::run_unit).
    ///
    /// # Errors
    ///
    /// The same surface as [`checked`](Self::checked); only the captured output
    /// is discarded.
    pub async fn run_unit(&self) -> Result<()> {
        self.output_string().await?.ensure_success().map(drop)
    }

    /// Run the chain and return the pipefail-attributed exit code. A chain that
    /// produced no code surfaces as an error — a whole-chain or stage timeout as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout), a signal-kill as
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) — mirroring
    /// [`Command::exit_code`](crate::Command::exit_code).
    ///
    /// # Errors
    ///
    /// A chain that produced no code errors as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) (whole-chain or stage),
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled), or
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled), atop any launch failure. A
    /// non-zero pipefail code is returned, not raised.
    pub async fn exit_code(&self) -> Result<i32> {
        self.output_string().await?.require_code()
    }

    /// Read the chain's pipefail-attributed exit code as a boolean: `0` →
    /// `Ok(true)`, `1` → `Ok(false)`, anything else → `Err` (other code as
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit), a timeout as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout), a signal-kill as
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled)). For a chain whose final
    /// answer is a yes/no exit — `producer | grep -q pattern`. Mirrors
    /// [`Command::probe`](crate::Command::probe) and keeps its strict 0/1
    /// contract regardless of any stage's `ok_codes`.
    ///
    /// The code is the **pipefail** code, so an *inner* stage exiting `1` reads as
    /// `false` even when the predicate is the last stage. If only the final
    /// stage's verdict should decide the boolean, mark the earlier stages
    /// [`unchecked_in_pipe`](Command::unchecked_in_pipe) so they never speak for
    /// the chain.
    ///
    /// # Errors
    ///
    /// A pipefail code other than `0`/`1` becomes
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit); a chain with no code errors as
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout),
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled), or
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled), atop any launch failure.
    pub async fn probe(&self) -> Result<bool> {
        let result = self.output_string().await?;
        result.probe_bool()
    }

    /// Run the chain (requiring a clean pipefail outcome) and feed the last
    /// stage's stdout to an **infallible** `parse` closure. Fails loud on a
    /// bounded-buffer truncation of the last stage so the parser never sees a
    /// clipped tail. Mirrors [`Command::parse`](crate::Command::parse) — except
    /// the closure runs *inline* on the awaiting task (not across a `tokio::spawn`
    /// boundary), so it needs no `Send` bound, accepting strictly more closures
    /// than `Command::parse`.
    ///
    /// # Errors
    ///
    /// The pipefail surface of [`run`](Self::run) (launch failures,
    /// [`ErrorReason::Exit`](crate::ErrorReason::Exit) /
    /// [`ErrorReason::Signalled`](crate::ErrorReason::Signalled) /
    /// [`ErrorReason::Timeout`](crate::ErrorReason::Timeout) /
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) /
    /// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin)), plus
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) when a fail-loud
    /// buffer truncated the last stage's stdout. The `parse` closure is
    /// infallible, so it adds no error.
    pub async fn parse<T, F>(&self, parse: F) -> Result<T>
    where
        F: FnOnce(&str) -> T,
    {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        Ok(parse(out.stdout()))
    }

    /// Run the chain (requiring a clean pipefail outcome) and feed the last
    /// stage's stdout to a *fallible* `parse` closure (the JSON-deserialization
    /// shape; a failure becomes [`ErrorReason::Parse`](crate::ErrorReason::Parse) or whatever
    /// the closure returns). Fails loud on truncation. Mirrors
    /// [`Command::try_parse`](crate::Command::try_parse).
    ///
    /// # Errors
    ///
    /// Everything [`parse`](Self::parse) can return, plus whatever the fallible
    /// `parse` closure yields on malformed output — typically
    /// [`ErrorReason::Parse`](crate::ErrorReason::Parse).
    pub async fn try_parse<T, F>(&self, parse: F) -> Result<T>
    where
        F: FnOnce(&str) -> Result<T>,
    {
        let out = self.checked().await?;
        self.reject_if_last_truncated(&out)?;
        parse(out.stdout())
    }

    fn reject_if_last_truncated(&self, out: &ProcessResult<String>) -> Result<()> {
        let policy = self
            .stages
            .last()
            .expect("a pipeline has at least two stages")
            .output_buffer_policy();
        out.reject_if_truncated(policy.max_lines, policy.max_bytes)
    }

    fn pipeline_name(&self) -> String {
        self.stages
            .iter()
            .map(|stage| stage.program_name())
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

/// A **live streaming session** over a running [`Pipeline`] — the multi-stage
/// analogue of a [`RunningProcess`](crate::RunningProcess), returned by
/// [`Pipeline::start`]. It streams the **last** stage's stdout as it arrives while
/// every inner stage drains in the background, then folds the same **pipefail**
/// outcome as the buffering verbs at [`finish`](Self::finish).
///
/// Drive it like a `RunningProcess`:
///
/// - [`stdout_lines`](Self::stdout_lines) / [`output_events`](Self::output_events)
///   — the last stage's stdout, line by line or as interleaved events, with the
///   same **consume-once** contract (a second take is a loud `Err`).
/// - [`wait_for_line`](Self::wait_for_line) — wait for a readiness banner on that
///   stream without tearing the chain down.
/// - [`finish`](Self::finish) — fold the pipefail outcome (the culprit stage's
///   outcome and *its own* stderr, not just the last stage's) into a
///   [`Finished`].
/// - [`start_kill`](Self::start_kill) — stop the whole chain now.
///
/// **Teardown is whole-chain.** A stage's checked failure proactively tears every
/// stage's sub-group down (so a quiet upstream can't hold a failed live chain
/// open), the chain-wide [`Pipeline::timeout`] / [`Pipeline::cancel_on`] still
/// bound the session, and **dropping** the session hard-kills every stage's tree —
/// the crate's no-orphan invariant holds for a live chain exactly as it does for a
/// single [`RunningProcess`](crate::RunningProcess). A partially-started chain (one
/// stage up, the next failing to spawn) is torn down before [`start`](Pipeline::start)
/// even returns its error.
#[must_use = "a PipelineSession streams a live chain; drop it and the whole chain is killed unread"]
pub struct PipelineSession {
    /// The last stage's live handle — the streaming surface the caller drives.
    /// `Option` so [`finish`](Self::finish) can move it out without a partial move
    /// (the session has a `Drop`); `None` only after `finish` consumed it.
    last: Option<RunningProcess>,
    /// The last stage's pipefail metadata, kept so `finish` can fold it into place.
    last_program: String,
    last_ok_codes: Vec<i32>,
    last_unchecked: bool,
    last_timeout: Option<Duration>,
    /// Background drains of every inner (non-last) stage — each an index-tagged
    /// [`StageOutcome`] once its stage exits, sorted back into left-to-right order
    /// by `finish`. `Option`, taken by `finish`.
    inner_tasks: Option<tokio::task::JoinSet<Result<(usize, StageOutcome)>>>,
    /// How many inner stages there are — the `Debug` summary's stage count.
    inner_count: usize,
    /// Strong handles to every stage's sub-group, for [`start_kill`](Self::start_kill)
    /// and the kill-on-drop backstop.
    stage_groups: Vec<Arc<ProcessGroup>>,
    /// Proactive teardown token, fired by an inner stage's checked/raw failure.
    teardown: tokio_util::sync::CancellationToken,
    /// The chain-wide [`Pipeline::timeout`], if any (gates the arbiter read).
    timeout: Option<Duration>,
    /// The chain-wide deadline arbiter (reusing the shared `running::deadline` CAS
    /// protocol): the watchdog claims `TimedOut`, `finish` claims `Exited`.
    chain_state: Arc<AtomicU8>,
    /// The chain-wide timeout watchdog, aborted on `finish`/drop.
    deadline_task: Option<JoinHandle<()>>,
    /// The standing teardown killer, aborted on `finish`/drop.
    killer: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for PipelineSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineSession")
            .field("last", &self.last_program)
            .field("inner_stages", &self.inner_count)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl PipelineSession {
    /// Stream the **last** stage's standard output line by line, as the chain
    /// produces it — the multi-stage analogue of
    /// [`RunningProcess::stdout_lines`](crate::RunningProcess::stdout_lines). Call
    /// this **once**. Every inner stage's stdout feeds the next stage's stdin (and
    /// its stderr is drained in the background for pipefail diagnostics); only the
    /// last stage's stdout reaches you.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when the last stage's stdout was not piped,
    /// or a prior streaming verb ([`stdout_lines`](Self::stdout_lines) /
    /// [`output_events`](Self::output_events) / [`wait_for_line`](Self::wait_for_line))
    /// already consumed it — returned instead of a silently-empty stream.
    pub fn stdout_lines(&mut self) -> Result<StdoutLines> {
        self.last_mut().stdout_lines()
    }

    /// Stream the **last** stage's stdout **and** stderr as one ordered sequence of
    /// [`OutputEvent`](crate::OutputEvent)s — the multi-stage analogue of
    /// [`RunningProcess::output_events`](crate::RunningProcess::output_events). Call
    /// this **once**. Note this surfaces only the *last* stage's stderr as events;
    /// inner stages' stderr still folds into the pipefail diagnostics at
    /// [`finish`](Self::finish).
    ///
    /// # Errors
    ///
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when the last stage's stdout was not piped,
    /// or a prior streaming verb already consumed it.
    pub fn output_events(&mut self) -> Result<OutputEvents> {
        self.last_mut().output_events()
    }

    /// Wait until a line on the **last** stage's stdout matches `predicate`
    /// (returning that line), or fail with [`ErrorReason::NotReady`](crate::ErrorReason::NotReady)
    /// when `within` elapses — the multi-stage analogue of
    /// [`RunningProcess::wait_for_line`](crate::RunningProcess::wait_for_line). Like
    /// there, a failed probe does **not** kill the chain and does not arm the
    /// chain-wide timeout; continue with [`finish`](Self::finish) for the outcome.
    ///
    /// # Errors
    ///
    /// [`ErrorReason::NotReady`](crate::ErrorReason::NotReady) when `within` elapses with no
    /// matching line (or the last stage's stdout closes first), and
    /// [`ErrorReason::Io`](crate::ErrorReason::Io) when that stdout was not piped or was already
    /// consumed — the same surface as the single-process probe.
    pub async fn wait_for_line(
        &mut self,
        predicate: impl Fn(&str) -> bool + Send,
        within: Duration,
    ) -> Result<String> {
        self.last_mut().wait_for_line(predicate, within).await
    }

    /// The OS process id of the **last** stage, or `None` once it has been reaped —
    /// the stage whose stdout you stream. The inner stages' pids are not surfaced;
    /// the chain is driven and torn down as a unit.
    pub fn pid(&self) -> Option<u32> {
        self.last.as_ref().and_then(RunningProcess::pid)
    }

    /// Stop the **whole chain** now: fan a hard kill across every stage's sub-group.
    /// Idempotent and best-effort (a per-group error is swallowed), mirroring
    /// [`RunningProcess::start_kill`](crate::RunningProcess::start_kill) at chain
    /// scope. After it, the last stage's stream ends and [`finish`](Self::finish)
    /// reports the killed outcome via the usual pipefail fold.
    ///
    /// # Errors
    ///
    /// Currently infallible — returns `Ok(())`; the `Result` mirrors
    /// [`RunningProcess::start_kill`](crate::RunningProcess::start_kill) and leaves
    /// room for a future backend that can report a kill failure.
    pub fn start_kill(&mut self) -> Result<()> {
        kill_all_stage_groups(&self.stage_groups);
        Ok(())
    }

    /// Finish the live chain and fold the **pipefail** outcome, the streaming
    /// analogue of [`Pipeline::output_string`](Pipeline::output_string) /
    /// [`RunningProcess::finish`](crate::RunningProcess::finish). The last stage's
    /// stdout was already streamed to you, so — like [`Finished`] — none is
    /// re-bundled here; what you get back is *how the chain ended*.
    ///
    /// The returned [`Finished`] carries the **pipefail-attributed** stage's
    /// [`outcome`](Finished::outcome) and *its own* [`stderr`](Finished::stderr):
    /// the culprit is chosen by the same rule as the buffering verbs — the leftmost
    /// checked failure, preferring a genuine failure over a SIGPIPE/teardown victim,
    /// or the last stage when every stage exited cleanly. A chain-wide
    /// [`Pipeline::timeout`] that elapsed reports [`Outcome::TimedOut`](crate::Outcome::TimedOut)
    /// regardless of how the individual stages were killed, exactly as the buffering
    /// path's whole-chain timeout does.
    ///
    /// # Errors
    ///
    /// A failing stage (and a chain timeout) is *captured* in the returned
    /// [`Finished`]'s [`outcome`](Finished::outcome), not raised. `Err` mirrors the
    /// buffering verbs' non-outcome failures:
    /// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) (the chain-wide
    /// [`cancel_on`](Pipeline::cancel_on) token fired, or a stage carried one),
    /// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge) (a fail-loud buffer
    /// overflowed on a stage), [`ErrorReason::Stdin`](crate::ErrorReason::Stdin), or
    /// [`ErrorReason::Io`](crate::ErrorReason::Io).
    pub async fn finish(mut self) -> Result<Finished> {
        // The consume-once `take`s live in `take_live_parts` (this method takes
        // `self` by value, so they are always `Some` here), keeping the panic path
        // out of the public `finish`.
        let (last, inner_tasks) = self.take_live_parts();
        let teardown = self.teardown.clone();
        let last_ok_codes = self.last_ok_codes.clone();
        let last_unchecked = self.last_unchecked;

        // Drive the last stage to its `Finished` and drain the inner stages
        // concurrently, with the standing killer + chain-deadline watchdog still
        // armed. A failing/erroring last stage fires `teardown` too (mirroring the
        // buffering path's failing-last-stage teardown), so a quiet upstream can't
        // wedge the finalize; the killer then tears the chain down.
        let last_fut = {
            let teardown = teardown.clone();
            async move {
                let result = last.finish().await;
                // Snapshot `torn_down` *before* the last stage might fire teardown —
                // a teardown already in flight makes it a victim; a last stage that
                // fails on its own fires teardown yet stays the (non-torn) culprit.
                let torn_down = teardown.is_cancelled();
                match &result {
                    Ok(finished) => {
                        if !torn_down
                            && is_checked_failure(finished.outcome, &last_ok_codes, last_unchecked)
                        {
                            teardown.cancel();
                        }
                    }
                    Err(_) if !torn_down => teardown.cancel(),
                    Err(_) => {}
                }
                (result, torn_down)
            }
        };
        let ((last_res, last_torn_down), inner_res) =
            tokio::join!(last_fut, drain_unordered(inner_tasks, &teardown));

        // Both sides settled — stop the background watchdogs.
        self.abort_background();

        // Chain-wide timeout wins over the stages' own (hard-killed) outcomes, just
        // like the buffering path's `tokio::time::timeout` branch. Claiming the
        // arbiter is race-free against the watchdog: whichever of `Exited`/`TimedOut`
        // CASes from `PENDING` first decides.
        if self.chain_timed_out() {
            return Ok(Finished {
                outcome: Outcome::TimedOut,
                stderr: String::new(),
                stderr_truncated: false,
            });
        }

        // Surface a raw `Err` from either side (Cancelled / OutputTooLarge / Stdin / Io).
        let last_finished = last_res?;
        let mut inner = inner_res?;

        // Rebuild the inner stages in left-to-right order (a clean drain returns all
        // `inner_count` items, each tagged with its unique launch index), then append
        // the last. Sorting by index needs no `expect` for a missing slot.
        inner.sort_by_key(|(index, _)| *index);
        let mut stages: Vec<StageOutcome> = inner.into_iter().map(|(_, outcome)| outcome).collect();
        stages.push(StageOutcome {
            program: self.last_program.clone(),
            outcome: last_finished.outcome,
            stderr: last_finished.stderr,
            unchecked: self.last_unchecked,
            ok_codes: self.last_ok_codes.clone(),
            timeout: self.last_timeout,
            torn_down: last_torn_down,
            stderr_truncated: last_finished.stderr_truncated,
        });

        // Reuse the exact pipefail attribution. The last stage's real stdout was
        // already streamed to the caller, so fold with a unit payload and surface
        // only the attributed stage's outcome + stderr as a `Finished`.
        let folded = pipefail(stages, ());
        Ok(Finished {
            outcome: folded.outcome(),
            stderr: folded.stderr().to_owned(),
            stderr_truncated: folded.truncated(),
        })
    }

    fn last_mut(&mut self) -> &mut RunningProcess {
        self.last
            .as_mut()
            .expect("the last stage is live until finish consumes the session")
    }

    /// Take the last stage handle and the inner-stage drains out of the session for
    /// [`finish`](Self::finish). Private so its consume-once `expect`s (both fields
    /// are always `Some` until `finish` — which owns `self` — runs) stay out of the
    /// public API's panic-doc surface.
    #[allow(clippy::type_complexity)]
    fn take_live_parts(
        &mut self,
    ) -> (
        RunningProcess,
        tokio::task::JoinSet<Result<(usize, StageOutcome)>>,
    ) {
        let last = self
            .last
            .take()
            .expect("finish consumes the session exactly once");
        let inner_tasks = self
            .inner_tasks
            .take()
            .expect("finish consumes the session exactly once");
        (last, inner_tasks)
    }

    /// Whether the chain-wide [`Pipeline::timeout`] elapsed: claim the arbiter for a
    /// natural finish (`Exited`); if a fired deadline already claimed `TimedOut`,
    /// report the timeout. Reuses the shared `running::deadline` claim protocol.
    fn chain_timed_out(&self) -> bool {
        self.timeout.is_some()
            && !crate::running::deadline::claim_exited(&self.chain_state)
            && self.chain_state.load(Ordering::Acquire) == crate::running::TS_TIMED_OUT
    }

    /// Abort the standing background watchdog tasks (deadline + teardown killer).
    /// Idempotent — `finish` calls it once the chain has settled, and `Drop` calls
    /// it for the drop-without-finish path.
    fn abort_background(&mut self) {
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
        if let Some(task) = self.killer.take() {
            task.abort();
        }
    }
}

impl Drop for PipelineSession {
    fn drop(&mut self) {
        // Abort the detached watchdogs so a session dropped unfinished leaves no
        // parked killer/deadline task behind. The chain itself is torn down by
        // kill-on-drop as the fields fall: the last stage's `RunningProcess::drop`
        // kills its own tree, every inner stage's handle (moved into the aborted
        // `inner_tasks`) does the same as the `JoinSet` drops, and the retained
        // `stage_groups` are the kill-on-drop backstop for any straggler.
        self.abort_background();
    }
}

/// Fan a hard kill across every stage's sub-group — the chain-wide backstop when
/// a [`Pipeline::timeout`] elapses or a stage's failure triggers proactive
/// teardown, now that each stage owns its own kill-on-drop group rather than
/// sharing one. Best-effort: a per-group error is swallowed, exactly as the old
/// single-group `kill_all()` was.
///
/// Called synchronously from a spawned async task (both the timeout branch and
/// the cancellation killer above), so on a cgroup backend it is one of the four
/// async call sites of the accepted bounded blocking sweep documented on
/// `Cgroup::kill` (`src/sys/linux.rs`) — see that comment for the pre-5.14/
/// restricted-cgroup tradeoff.
fn kill_all_stage_groups(groups: &[Arc<ProcessGroup>]) {
    for group in groups {
        let _ = group.kill_all();
    }
}

/// The [`Weak`] analogue of [`kill_all_stage_groups`], for the detached watchdog
/// tasks a live [`PipelineSession`] holds: `Weak` so a still-parked killer/deadline
/// task can never keep a stage's sub-group (and its kill-on-drop backstop) alive
/// past a session drop — a dropped-away group simply fails to upgrade and is
/// skipped. Best-effort per group, exactly like the strong-handle version.
fn kill_weak_stage_groups(groups: &[Weak<ProcessGroup>]) {
    for group in groups {
        if let Some(group) = group.upgrade() {
            let _ = group.kill_all();
        }
    }
}

/// Spawn the standing teardown killer for [`Pipeline::start`]'s live session: it
/// waits on `teardown` and, the instant it fires, fans a hard kill across every
/// stage's sub-group (the last stage included) so a failing inner stage tears the
/// *whole* live chain down even mid-stream. Holds [`Weak`] handles so it never
/// pins the groups; the session aborts it on `finish`/drop.
fn spawn_group_killer(
    teardown: tokio_util::sync::CancellationToken,
    stage_groups: &[Arc<ProcessGroup>],
) -> JoinHandle<()> {
    let groups: Vec<Weak<ProcessGroup>> = stage_groups.iter().map(Arc::downgrade).collect();
    tokio::spawn(async move {
        teardown.cancelled().await;
        kill_weak_stage_groups(&groups);
    })
}

/// Drive one already-launched **non-last** stage to its [`Finished`] and fold it
/// into a positioned [`StageOutcome`], firing `teardown` on its first *checked*
/// failure (the proactive-teardown trigger). The shared classify-and-teardown body
/// of both the buffering [`capture`](Pipeline::capture) path and the streaming
/// [`start`](Pipeline::start) session's inner drains, so both blame a stage — and
/// decide whether it is a teardown victim — by exactly the same rule.
///
/// The `torn_down` snapshot is taken *before* this stage might fire `teardown`: a
/// teardown already in flight when the stage ended marks it a victim (de-prioritized
/// in the pipefail fold), while the first genuine failure sees no teardown yet, so
/// it fires it and stays the (non-torn) culprit.
#[allow(clippy::too_many_arguments)]
async fn finish_inner_stage(
    process: RunningProcess,
    index: usize,
    program: String,
    ok_codes: Vec<i32>,
    timeout: Option<Duration>,
    unchecked: bool,
    teardown: tokio_util::sync::CancellationToken,
) -> Result<(usize, StageOutcome)> {
    let Finished {
        outcome,
        stderr,
        stderr_truncated,
    } = process.finish().await?;
    let torn_down = teardown.is_cancelled();
    if !torn_down && is_checked_failure(outcome, &ok_codes, unchecked) {
        teardown.cancel();
    }
    Ok((
        index,
        StageOutcome {
            program,
            outcome,
            stderr,
            unchecked,
            ok_codes,
            timeout,
            torn_down,
            stderr_truncated,
        },
    ))
}

/// True for SIGPIPE (Unix signal 13) — the usual victim symptom, not the culprit.
fn is_sigpipe(outcome: &Outcome) -> bool {
    #[cfg(unix)]
    return matches!(outcome, Outcome::Signalled(Some(13)));
    #[cfg(not(unix))]
    let _ = outcome;
    #[cfg(not(unix))]
    false
}

/// Did this outcome count as a *clean* exit given the stage's accepted codes?
/// Exhaustive (no wildcard) so a future [`Outcome`] variant forces a decision
/// here rather than being silently treated as clean (H2).
fn is_clean_exit(outcome: Outcome, ok_codes: &[i32]) -> bool {
    match outcome {
        Outcome::Exited(code) => ok_codes.contains(&code),
        Outcome::Signalled(_) | Outcome::TimedOut => false, // kill/timeout → unclean
    }
}

/// A stage whose unclean, non-forgiven exit makes it a genuine pipefail culprit —
/// the trigger for proactive teardown. An [`unchecked_in_pipe`](Command::unchecked_in_pipe)
/// stage never qualifies (its unclean exit is forgiven, so it must not tear the
/// chain down).
fn is_checked_failure(outcome: Outcome, ok_codes: &[i32], unchecked: bool) -> bool {
    !unchecked && !is_clean_exit(outcome, ok_codes)
}

/// Fold all stages (last included) into one pipefail result.
///
/// Key invariants:
/// - An `unchecked` inner stage is fully exempt from attribution regardless of
///   how it ended. An `unchecked` last stage is the one carve-out: only its
///   non-zero *exit* is forgiven; a last-stage timeout or signal still surfaces.
/// - Among checked failures, a genuine culprit is preferred over a *victim* — a
///   downstream SIGPIPE death, or a stage the chain's proactive teardown killed
///   (`torn_down`) after a sibling failed; otherwise the leftmost wins.
fn pipefail<T>(stages: Vec<StageOutcome>, last_stdout: T) -> ProcessResult<T> {
    let checked_failures: Vec<_> = stages
        .iter()
        .filter(|s| !s.unchecked && !is_clean_exit(s.outcome, &s.ok_codes))
        .collect();

    if let Some(stage) = checked_failures
        .iter()
        // Prefer a genuine culprit over a mere victim (a SIGPIPE death or a stage
        // killed by our own teardown) — a teardown victim never steals the blame
        // from the failure that triggered the teardown.
        .find(|s| !is_sigpipe(&s.outcome) && !s.torn_down)
        .or_else(|| checked_failures.first()) // else leftmost
        .copied()
    {
        // Carry the stage's own `ok_codes` so the rebuilt result classifies
        // success exactly as the stage did. Without this the default `[0]` would
        // make a rejected-zero failure (a stage with `ok_codes` excluding `0`
        // that exited `0`) — which `is_clean` deemed a *failure* above — report
        // `is_success() == true`, so the whole chain would return `Ok`.
        //
        // Also stamp the attributed stage's own stderr truncation: a bounded
        // `OutputBufferPolicy` may have silently dropped this stage's stderr even
        // when it isn't the last stage, and pipefail's diagnostics come from
        // *this* stage — so its truncation must be visible on the folded result,
        // not just the last stage's.
        return ProcessResult::new(
            stage.program.clone(),
            last_stdout,
            stage.stderr.clone(),
            stage.outcome,
            stage.timeout,
        )
        .with_ok_codes(stage.ok_codes.clone())
        .with_truncated(stage.stderr_truncated);
    }

    // No checked failure: the last stage speaks. For an unchecked last stage that
    // exited non-clean, widen ok_codes to include the real code so is_success() is
    // true without fabricating 0. Signal/timeout outcomes stay non-success regardless.
    let last = stages.last().expect("a pipeline has at least two stages");
    let ok_codes = match last.outcome {
        Outcome::Exited(code) if last.unchecked && !last.ok_codes.contains(&code) => vec![code],
        _ => last.ok_codes.clone(),
    };
    ProcessResult::new(
        last.program.clone(),
        last_stdout,
        last.stderr.clone(),
        last.outcome,
        last.timeout,
    )
    .with_ok_codes(ok_codes)
    .with_truncated(last.stderr_truncated)
}

/// `a | b` — sugar for [`Command::pipe`]. Parenthesize the chain before a
/// terminal verb, since method calls bind tighter than `|`.
impl std::ops::BitOr<Command> for Command {
    type Output = Pipeline;

    fn bitor(self, rhs: Command) -> Pipeline {
        self.pipe(rhs)
    }
}

/// `pipeline | c` — sugar for [`Pipeline::pipe`], so `a | b | c` chains
/// left-associatively into one pipeline.
impl std::ops::BitOr<Command> for Pipeline {
    type Output = Pipeline;

    fn bitor(self, rhs: Command) -> Pipeline {
        self.pipe(rhs)
    }
}

fn join_error(err: tokio::task::JoinError) -> crate::Error {
    crate::Error::io(std::io::Error::other(format!(
        "pipeline stage task failed: {err}"
    )))
}

/// Drain every task in `tasks` to completion in **true completion order**
/// (`JoinSet::join_next`, not a left-to-right positional await), firing
/// `teardown` the instant *any* task ends badly — a raw `Err` (a stage's own
/// [`ErrorReason::Cancelled`](crate::ErrorReason::Cancelled) /
/// [`ErrorReason::Stdin`](crate::ErrorReason::Stdin) / [`ErrorReason::Io`](crate::ErrorReason::Io) /
/// [`ErrorReason::OutputTooLarge`](crate::ErrorReason::OutputTooLarge)) or a task panic
/// (surfaced here as a `JoinError`).
///
/// This is `capture`'s liveness fix, factored out so it's testable without
/// spawning a single real process: a positional `for task in tasks { task
/// .await?? }` gather stalls forever on an earlier, still-quiet task once a
/// *later* task is already sitting on a ready `Err` — that `Err` never got a
/// chance to fire `teardown` either, since it happened on a path (`?`) that
/// skips the checked-failure attribution logic entirely. Draining in
/// completion order instead means whichever task is ready first — quiet or
/// not, wherever it sits — is the one examined first, so a bad completion is
/// never stuck behind a pending one.
///
/// A task that resolves to a *checked* failure (an `Outcome` `pipefail`
/// treats as unclean) already fires `teardown` itself, from inside the task
/// — see the spawn bodies in `capture` — since that decision needs
/// domain knowledge (the stage's `ok_codes`/`unchecked` flag) this function
/// doesn't have; this function only has to backstop the paths those spawn
/// bodies *can't* self-report: a raw `Err` return (still inside the task,
/// but past `is_checked_failure`) and a panic (never returns at all).
///
/// On success, every task's `Ok` payload is returned — in **completion**
/// order, not submission order; a payload that must be rebuilt in original
/// stage order (as `capture` does) needs to carry its own position, the way
/// [`Joined::Inner`]'s index does.
async fn drain_unordered<Item: 'static>(
    mut tasks: tokio::task::JoinSet<Result<Item>>,
    teardown: &tokio_util::sync::CancellationToken,
) -> Result<Vec<Item>> {
    let mut collected = Vec::with_capacity(tasks.len());
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(item)) => collected.push(item),
            Ok(Err(err)) => {
                teardown.cancel();
                return Err(err);
            }
            Err(join_err) => {
                teardown.cancel();
                return Err(join_error(join_err));
            }
        }
    }
    Ok(collected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(program: &str, outcome: Outcome) -> StageOutcome {
        StageOutcome {
            program: program.into(),
            outcome,
            stderr: String::new(),
            unchecked: false,
            ok_codes: vec![0],
            timeout: None,
            torn_down: false,
            stderr_truncated: false,
        }
    }

    fn clean(program: &str) -> StageOutcome {
        stage(program, Outcome::Exited(0))
    }

    fn unclean(program: &str, outcome: Outcome, stderr: &str) -> StageOutcome {
        StageOutcome {
            stderr: stderr.into(),
            ..stage(program, outcome)
        }
    }

    fn unchecked_fail(program: &str, outcome: Outcome) -> StageOutcome {
        StageOutcome {
            unchecked: true,
            ..unclean(program, outcome, "forgiven")
        }
    }

    fn last(outcome: Outcome, unchecked: bool) -> StageOutcome {
        StageOutcome {
            program: "last".into(),
            outcome,
            stderr: "last-err".into(),
            unchecked,
            ok_codes: vec![0],
            timeout: None,
            torn_down: false,
            stderr_truncated: false,
        }
    }

    fn pf(mut inner: Vec<StageOutcome>, last: StageOutcome, stdout: &str) -> ProcessResult<String> {
        inner.push(last);
        pipefail(inner, stdout.to_owned())
    }

    fn expect_last(outcome: Outcome, stdout: &str) -> ProcessResult<String> {
        ProcessResult::new(
            "last".into(),
            stdout.into(),
            "last-err".into(),
            outcome,
            None,
        )
    }

    #[test]
    fn all_clean_inner_stages_let_the_last_stage_speak() {
        let ok = pf(
            vec![clean("a"), clean("b")],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert_eq!(ok, expect_last(Outcome::Exited(0), "final"));

        let failing_last = pf(vec![clean("a")], last(Outcome::Exited(3), false), "partial");
        assert_eq!(failing_last, expect_last(Outcome::Exited(3), "partial"));
    }

    #[test]
    fn failing_inner_stage_wins_but_stdout_stays_the_chains() {
        let result = pf(
            vec![clean("a"), unclean("b", Outcome::Exited(2), "b broke")],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert_eq!(result.program(), "b", "diagnostics from the failing stage");
        assert_eq!(result.code(), Some(2));
        assert_eq!(result.stderr(), "b broke");
        assert_eq!(
            result.stdout(),
            "final",
            "stdout is what the chain produced — the last stage's"
        );
        assert!(!result.timed_out());
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Exit {
                program,
                code,
                stdout,
                stderr,
                ..
            }) => {
                assert_eq!(program, "b", "diagnostics from the failing stage");
                assert_eq!(code, 2);
                assert_eq!(stdout, "final");
                assert_eq!(stderr, "b broke");
            }
            other => panic!("expected ErrorReason::Exit, got {other:?}"),
        }
    }

    #[test]
    fn rejected_zero_stage_stays_a_failure_after_attribution() {
        // A stage whose `ok_codes` exclude 0 that nonetheless exits 0 is a
        // *failure* (`is_clean` says so) and wins attribution. The rebuilt result
        // must carry that stage's own `ok_codes`, or the `ProcessResult::new`
        // default `[0]` would make `is_success()` report true for a chain the
        // fold deemed failed — so `run`/`checked`/`probe` would return `Ok`.
        let culprit = StageOutcome {
            ok_codes: vec![1],
            ..unclean("check", Outcome::Exited(0), "rejected zero")
        };
        let result = pf(vec![culprit], last(Outcome::Exited(0), false), "final");
        assert_eq!(result.program(), "check", "the rejected-zero stage wins");
        assert_eq!(result.code(), Some(0));
        assert_eq!(result.stderr(), "rejected zero");
        assert!(
            !result.is_success(),
            "a rejected-zero failure must not report success just because it exited 0"
        );
        assert!(
            result.ensure_success().is_err(),
            "the chain must surface the failure, not swallow it as Ok"
        );
    }

    #[test]
    fn first_of_several_failures_is_attributed() {
        let result = pf(
            vec![
                unclean("a", Outcome::Exited(1), "first"),
                unclean("b", Outcome::Exited(2), "second"),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a", "pipefail blames the FIRST failure");
        assert_eq!(result.code(), Some(1));
        assert_eq!(result.stderr(), "first");
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Exit { program, .. }) => {
                assert_eq!(program, "a", "...and so does the error surface");
            }
            other => panic!("expected ErrorReason::Exit, got {other:?}"),
        }
    }

    #[test]
    fn attributed_inner_stage_truncation_survives_the_fold_not_just_the_last_stage() {
        // T-042: an INNER (non-last) stage's silently-dropped stderr must show up
        // as `truncated()` on the folded result when pipefail blames *that*
        // stage — before this, only the last stage's `Finished` carried a
        // truncation signal, so a bounded-buffer-clipped diagnostic on the real
        // culprit was reported as complete (`truncated() == false`).
        let culprit = StageOutcome {
            stderr_truncated: true,
            ..unclean("b", Outcome::Exited(2), "b broke (clipped)")
        };
        let result = pf(
            vec![clean("a"), culprit],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert_eq!(result.program(), "b", "the inner failing stage is blamed");
        assert!(
            result.truncated(),
            "the attributed inner stage's dropped stderr must be visible: {result:?}"
        );

        // Contrast: the same inner failure WITHOUT truncation must not falsely
        // report truncated (a plain sanity check the flag isn't stuck on).
        let clean_culprit = unclean("b", Outcome::Exited(2), "b broke");
        let untruncated = pf(
            vec![clean("a"), clean_culprit],
            last(Outcome::Exited(0), false),
            "final",
        );
        assert!(
            !untruncated.truncated(),
            "an inner failure with no dropped stderr must not report truncated: {untruncated:?}"
        );
    }

    #[test]
    fn all_unchecked_failures_report_success() {
        // The head-pattern: the producer's SIGPIPE death (Signalled(None)) is
        // forgiven, the chain succeeds with the consumer's output.
        let result = pf(
            vec![unchecked_fail("producer", Outcome::Signalled(None))],
            last(Outcome::Exited(0), false),
            "first line",
        );
        assert!(result.is_success(), "got {result:?}");
        assert_eq!(result.stdout(), "first line");
        assert_eq!(result.program(), "last", "the clean last stage speaks");
    }

    #[test]
    fn checked_failure_trumps_unchecked_regardless_of_order() {
        // unchecked-then-checked: the later checked failure wins.
        let result = pf(
            vec![
                unchecked_fail("a", Outcome::Exited(141)),
                unclean("b", Outcome::Exited(2), "real"),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "b", "unchecked never shields a failure");
        assert_eq!(result.code(), Some(2));

        // checked-then-unchecked: the first (checked) failure wins, as today.
        let result = pf(
            vec![
                unclean("a", Outcome::Exited(1), "real"),
                unchecked_fail("b", Outcome::Exited(2)),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), Some(1));
    }

    #[test]
    fn attribution_skips_unchecked_to_the_first_checked_failure() {
        let result = pf(
            vec![
                clean("a"),
                unchecked_fail("b", Outcome::Exited(1)),
                unclean("c", Outcome::Exited(3), "c broke"),
                unclean("d", Outcome::Exited(4), "d broke"),
            ],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "c", "first CHECKED failure is blamed");
        assert_eq!(result.code(), Some(3));
        assert_eq!(result.stderr(), "c broke");
    }

    #[test]
    fn unchecked_last_stage_failure_is_forgiven() {
        let result = pf(
            vec![clean("a")],
            last(Outcome::Exited(141), true),
            "partial",
        );
        assert!(result.is_success(), "got {result:?}");
        assert_eq!(result.code(), Some(141), "real exit code preserved");
        assert_eq!(result.stdout(), "partial", "output is preserved");
        assert_eq!(result.stderr(), "last-err", "stderr kept for the curious");
        assert!(result.ensure_success().is_ok());
    }

    #[test]
    fn last_stage_ok_codes_are_honoured() {
        let mut last_grep = last(Outcome::Exited(1), false);
        last_grep.program = "grep".into();
        last_grep.ok_codes = vec![0, 1];
        let result = pf(vec![clean("a")], last_grep, "matched");
        assert!(
            result.is_success(),
            "exit 1 in the last stage's ok_codes: {result:?}"
        );
        assert_eq!(result.code(), Some(1), "real code preserved");
        assert_eq!(result.program(), "grep");
    }

    #[test]
    fn inner_stage_ok_codes_are_honoured_in_pipefail_cleanliness() {
        let mut with_ok = stage("grep", Outcome::Exited(1));
        with_ok.ok_codes = vec![0, 1];
        let result = pf(vec![with_ok], last(Outcome::Exited(0), false), "out");
        assert!(
            result.is_success(),
            "exit 1 in ok_codes should be clean: {result:?}"
        );
        assert_eq!(result.program(), "last", "clean inner → last stage speaks");
    }

    #[test]
    fn timed_out_stage_reports_its_own_deadline_not_the_chains() {
        let mut timed = unclean("slow", Outcome::TimedOut, "");
        timed.timeout = Some(Duration::from_millis(500));
        let result = pf(vec![timed], last(Outcome::Exited(0), false), "out");
        assert_eq!(result.program(), "slow");
        assert!(result.timed_out());
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Timeout {
                program, timeout, ..
            }) => {
                assert_eq!(program, "slow");
                assert_eq!(
                    timeout,
                    Duration::from_millis(500),
                    "the stage's own deadline, not the chain's 0ns"
                );
            }
            other => panic!("expected ErrorReason::Timeout, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn sigpipe_victim_not_blamed_when_downstream_non_sigpipe_failure_exists() {
        let sigpipe_victim = unclean("producer", Outcome::Signalled(Some(13)), "pipe broken");
        let real_failure = unclean("consumer", Outcome::Exited(2), "consumer broke");
        let result = pf(
            vec![sigpipe_victim, real_failure],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(
            result.program(),
            "consumer",
            "downstream non-SIGPIPE culprit, not upstream SIGPIPE victim"
        );
        assert_eq!(result.code(), Some(2));
    }

    #[test]
    fn torn_down_victim_does_not_steal_blame_from_the_real_failure() {
        // Proactive teardown: a downstream stage fails, the chain kills the group,
        // and a quiet upstream (and the last stage) come back signal-killed. Those
        // teardown kills are victims — the genuine downstream failure must still be
        // blamed, even though its Signalled(9) siblings are leftward / would-be
        // culprits by position.
        let torn_upstream = StageOutcome {
            torn_down: true,
            ..unclean(
                "upstream",
                Outcome::Signalled(Some(9)),
                "killed by teardown",
            )
        };
        let culprit = unclean("downstream", Outcome::Exited(3), "the real failure");
        let torn_last = StageOutcome {
            torn_down: true,
            ..last(Outcome::Signalled(Some(9)), false)
        };
        let result = pf(vec![torn_upstream, culprit], torn_last, "");
        assert_eq!(
            result.program(),
            "downstream",
            "the failure that triggered teardown wins, not a torn-down victim"
        );
        assert_eq!(result.code(), Some(3));
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Exit { program, code, .. }) => {
                assert_eq!(program, "downstream");
                assert_eq!(code, 3);
            }
            other => panic!("expected ErrorReason::Exit, got {other:?}"),
        }
    }

    #[test]
    fn all_torn_down_failures_fall_back_to_the_leftmost() {
        // Degenerate: every checked failure is a teardown victim (no un-torn
        // culprit survives). Attribution falls back to the leftmost, exactly as it
        // does when every failure is a SIGPIPE victim.
        let first = StageOutcome {
            torn_down: true,
            ..unclean("a", Outcome::Signalled(Some(9)), "first killed")
        };
        let second = StageOutcome {
            torn_down: true,
            ..unclean("b", Outcome::Signalled(Some(9)), "second killed")
        };
        let result = pf(vec![first, second], last(Outcome::Exited(0), false), "");
        assert_eq!(
            result.program(),
            "a",
            "leftmost victim when all are torn down"
        );
        assert!(!result.is_success());
    }

    #[test]
    fn checked_last_stage_failure_still_speaks_verbatim() {
        // Regression guard: a checked unclean last stage must not be discarded.
        let result = pf(vec![clean("a")], last(Outcome::Exited(3), false), "partial");
        assert_eq!(result, expect_last(Outcome::Exited(3), "partial"));
    }

    #[test]
    fn unchecked_never_forgives_a_timeout() {
        // An unchecked LAST stage that timed out still reports the timeout —
        // a deadline violation is not an exit status.
        let result = pf(vec![clean("a")], last(Outcome::TimedOut, true), "");
        assert!(result.timed_out());
        assert!(!result.is_success());
    }

    #[test]
    fn unchecked_never_forgives_a_signal_kill() {
        let result = pf(
            vec![clean("a")],
            last(Outcome::Signalled(Some(9)), true),
            "",
        );
        assert!(matches!(result.outcome(), Outcome::Signalled(Some(9))));
        assert!(!result.is_success());
    }

    #[test]
    fn bitor_chains_like_pipe() {
        let chain = Command::new("a") | Command::new("b") | Command::new("c");
        assert_eq!(chain.stages.len(), 3, "a | b | c is one three-stage chain");
        assert_eq!(chain.pipeline_name(), "a | b | c");
        assert!(chain.timeout.is_none());
    }

    #[test]
    fn signal_killed_inner_stage_counts_as_unclean() {
        let result = pf(
            vec![unclean("a", Outcome::Signalled(None), "killed")],
            last(Outcome::Exited(0), false),
            "out",
        );
        assert_eq!(result.program(), "a");
        assert_eq!(result.code(), None);
        assert_eq!(result.stderr(), "killed");
        assert!(!result.timed_out(), "a stage kill is not a chain timeout");
        match result.ensure_success().map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Signalled {
                program, signal, ..
            }) => {
                assert_eq!(program, "a");
                assert_eq!(signal, None);
            }
            other => panic!("expected ErrorReason::Signalled, got {other:?}"),
        }
    }

    // T-085: `drain_unordered` regression coverage. These are hermetic — no
    // real process is spawned — and exercise the exact liveness bug: a raw
    // `Err` (or a task panic) on one task must fire `teardown` and let the
    // whole drain finish, even while a *sibling* task is deliberately still
    // pending. The quiet sibling below is a "controlled future": it can only
    // resolve once `teardown` actually fires, so if `drain_unordered`
    // regressed to a positional (left-to-right) gather that stalls on a
    // still-pending earlier task, these tests hang instead of failing fast —
    // wrapped in `tokio::time::timeout` so a regression is reported as a
    // failure rather than a wedged test run.

    /// A task that can only ever resolve once `teardown` fires — the
    /// hermetic stand-in for a quiet upstream stage that never writes and so
    /// never dies on its own; only the chain's proactive teardown ends it.
    async fn quiet_until_teardown(
        teardown: tokio_util::sync::CancellationToken,
        item: &'static str,
    ) -> Result<&'static str> {
        teardown.cancelled().await;
        Ok(item)
    }

    #[tokio::test]
    async fn drain_unordered_wakes_a_quiet_task_when_a_later_task_returns_a_raw_error() {
        let teardown = tokio_util::sync::CancellationToken::new();
        let mut tasks: tokio::task::JoinSet<Result<&'static str>> = tokio::task::JoinSet::new();
        // Spawned first (the "earlier, leftmost" stage in `capture`'s terms) —
        // a positional gather would await this one to completion before ever
        // looking at the task below, and it never completes on its own.
        tasks.spawn(quiet_until_teardown(teardown.clone(), "quiet-upstream"));
        // Spawned second (the "later" stage) but ready immediately — the raw
        // `Err` a checked-failure path never produces, so nothing but the
        // centralized `drain_unordered` firing `teardown` on its behalf can
        // wake the sibling above.
        tasks.spawn(async { Err(crate::Error::io(std::io::Error::other("downstream boom"))) });

        let drained =
            tokio::time::timeout(Duration::from_secs(5), drain_unordered(tasks, &teardown))
                .await
                .expect(
                    "drain_unordered must not hang on the still-pending quiet task \
             once the sibling's raw error has fired teardown",
                );

        match drained.map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Io(err)) => {
                assert_eq!(err.to_string(), "downstream boom");
            }
            other => panic!("expected the downstream stage's own Io error, got {other:?}"),
        }
        assert!(
            teardown.is_cancelled(),
            "a raw Err must fire teardown so a quiet sibling is unblocked"
        );
    }

    #[tokio::test]
    async fn drain_unordered_fires_teardown_on_a_task_panic_so_a_quiet_sibling_still_resolves() {
        let teardown = tokio_util::sync::CancellationToken::new();
        // Synchronizes the two tasks so the panic only happens once the quiet
        // task has genuinely started waiting on `teardown` — proving this is
        // a real concurrent-blocking scenario, not a fluke of scheduling
        // order where the quiet task happened to run (and finish) first.
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

        let mut tasks: tokio::task::JoinSet<Result<&'static str>> = tokio::task::JoinSet::new();
        tasks.spawn({
            let teardown = teardown.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                quiet_until_teardown(teardown, "quiet-upstream").await
            }
        });
        tasks.spawn(async move {
            barrier.wait().await;
            panic!("downstream stage task panicked");
        });

        let drained =
            tokio::time::timeout(Duration::from_secs(5), drain_unordered(tasks, &teardown))
                .await
                .expect(
                    "drain_unordered must not hang on the still-pending quiet task \
             once the sibling's panic has fired teardown",
                );

        match drained.map_err(|e| e.into_reason()) {
            Err(crate::ErrorReason::Io(err)) => {
                assert!(
                    err.to_string().contains("pipeline stage task failed"),
                    "expected the wrapped JoinError, got {err}"
                );
            }
            other => panic!("expected a wrapped JoinError, got {other:?}"),
        }
        assert!(
            teardown.is_cancelled(),
            "a task panic must fire teardown so a quiet sibling is unblocked"
        );
    }

    #[tokio::test]
    async fn drain_unordered_returns_every_item_when_the_whole_set_finishes_clean() {
        let teardown = tokio_util::sync::CancellationToken::new();
        let mut tasks: tokio::task::JoinSet<Result<u32>> = tokio::task::JoinSet::new();
        for item in [1u32, 2, 3] {
            tasks.spawn(async move {
                tokio::task::yield_now().await;
                Ok(item)
            });
        }

        let mut drained = drain_unordered(tasks, &teardown)
            .await
            .expect("every task succeeded");
        drained.sort_unstable();
        assert_eq!(
            drained,
            vec![1, 2, 3],
            "every task's payload survives the drain, completion order aside"
        );
        assert!(
            !teardown.is_cancelled(),
            "an all-clean drain must never fire teardown"
        );
    }
}
