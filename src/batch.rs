//! Bounded-concurrency batch execution.
//!
//! Two shapes of bounded fan-out, both built on the **same** engine
//! ([`Fanout`]), so the concurrency cap and the teardown guarantees are single-sourced:
//!
//! - [`output_all`] (text) / [`output_all_bytes`] (raw bytes) — **buffering**: run a
//!   whole slice of commands, capping how many live at once, and collect **all** their
//!   results **in input order** once the batch finishes.
//! - [`output_stream`] (text) / [`output_stream_bytes`] (raw bytes) — **streaming**:
//!   the same bounded fan-out, but each result is yielded the moment it lands
//!   (completion order, tagged with its input index), so a fast command never waits on a
//!   slow sibling and every result already handed to the consumer survives a
//!   mid-fan-out cancellation.
//!
//! The buffering verbs are literally the streaming engine driven to exhaustion and
//! reassembled by index (see [`collect_in_order`]) — one scheduler, two presentations.
//! For awaiting handles you already hold, see [`wait_all`](crate::wait_all).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio_stream::Stream;

use crate::{Command, ProcessResult, ProcessRunner, Result};

/// One completed command from a fan-out: its **input index** (its position in the
/// original `commands` iterator, so a completion-ordered stream stays traceable to
/// its source) paired with that command's independent [`Result`]. An `Err` is a
/// spawn/I/O failure; a non-zero exit is an `Ok(ProcessResult)` whose
/// [`code`](ProcessResult::code) you inspect.
type Completed<T> = (usize, Result<ProcessResult<T>>);

/// The pinned, boxed, `Send` future resolving to one command's [`Completed`]
/// outcome. Each such future **owns** its [`Command`] (moved into an `async move`
/// block that borrows only the runner for `'a`), so the [`Fanout`] driver can hold
/// a `Vec` of them without becoming self-referential — the alternative, storing the
/// owned commands *and* futures borrowing them in one struct, does not compile.
type CompletedFut<'a, T> = Pin<Box<dyn Future<Output = Completed<T>> + Send + 'a>>;

/// The per-command launcher: builds one command's [`CompletedFut`], picking the
/// capture verb (`output_string` for text, `output_bytes` for raw bytes). A plain
/// `fn` pointer (the launchers capture nothing) rather than a generic closure param,
/// so [`Fanout`] stays a three-parameter type — no `impl Fn` in its signature (which
/// `clippy::type_complexity` would flag) and no `Unpin` bound to thread through.
type Launch<'a, R, T> = fn(&'a R, usize, Command) -> CompletedFut<'a, T>;

/// The shared bounded-concurrency engine behind every batch verb. A [`Stream`] that
/// keeps at most `limit` command futures live at once and yields each
/// [`Completed`] result **the instant it resolves** (completion order, not input
/// order).
///
/// **Cancellation / drop.** Dropping the `Fanout` drops its in-flight futures —
/// with an own-group runner ([`JobRunner`](crate::JobRunner)) that kills each live
/// tree, matching [`output_all`]'s teardown — and drops every not-yet-scheduled
/// command **without ever spawning it** (a queued command is just a `Command` value
/// in `pending`; nothing runs until it is given a slot). Results already yielded to
/// the consumer are the consumer's and are unaffected.
struct Fanout<'a, R: ?Sized, T> {
    runner: &'a R,
    launch: Launch<'a, R, T>,
    /// Commands awaiting a concurrency slot, each tagged with its input index.
    /// Drained front-to-back as slots free; dropped un-spawned on cancellation.
    pending: VecDeque<(usize, Command)>,
    /// The at-most-`limit` in-flight command futures.
    active: Vec<CompletedFut<'a, T>>,
    limit: usize,
    /// Original command count, captured up front so [`collect_in_order`] can size
    /// its input-order slot vector before the stream drains `pending`.
    total: usize,
}

impl<'a, R, T> Fanout<'a, R, T>
where
    R: ProcessRunner + ?Sized,
{
    /// Build the engine over `commands`, clamping the concurrency cap to at least 1
    /// (`0` would deadlock a non-empty fan-out). Commands are collected eagerly and
    /// index-tagged, exactly as the old buffering driver collected its input up
    /// front.
    fn new<I>(commands: I, concurrency: usize, runner: &'a R, launch: Launch<'a, R, T>) -> Self
    where
        I: IntoIterator<Item = Command>,
    {
        let pending: VecDeque<(usize, Command)> = commands.into_iter().enumerate().collect();
        let total = pending.len();
        Self {
            runner,
            launch,
            pending,
            active: Vec::new(),
            limit: concurrency.max(1),
            total,
        }
    }
}

impl<'a, R, T> Stream for Fanout<'a, R, T>
where
    R: ProcessRunner + ?Sized,
{
    type Item = Completed<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Sound because `Fanout` is `Unpin`: every field is (the command futures are
        // pinned on the heap, so the `Pin<Box<_>>` handles move freely; the launcher is
        // a plain `fn` pointer), so it projects through a plain `&mut Self`.
        let this = self.get_mut();
        let runner = this.runner;

        // Top up to the concurrency cap: hand every waiting command a slot the moment
        // one frees, never exceeding `limit` live at once. Newly launched futures are
        // polled below in this same call, so their wakers are registered before we
        // ever return `Pending`.
        while this.active.len() < this.limit {
            match this.pending.pop_front() {
                Some((idx, command)) => this.active.push((this.launch)(runner, idx, command)),
                None => break,
            }
        }

        // Poll live commands in order; the FIRST to finish is yielded immediately.
        // We must not poll past it: a future that returned `Ready` must never be
        // polled again, and yielding only one item per `poll_next` means any later
        // ready sibling has to wait for the next call (its waker, registered on the
        // previous `Pending`, stays armed). No short-circuit here — a command's `Err`
        // is just its own yielded item; it neither drops nor cancels its siblings.
        let mut i = 0;
        while i < this.active.len() {
            match this.active[i].as_mut().poll(cx) {
                Poll::Ready(done) => {
                    drop(this.active.swap_remove(i)); // drop the finished (already-resolved) future
                    return Poll::Ready(Some(done));
                }
                Poll::Pending => i += 1,
            }
        }

        // Nothing ready. Done only when the whole fan-out has drained; otherwise every
        // live future was just polled `Pending`, so its waker is armed.
        if this.active.is_empty() && this.pending.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// Drive a [`Fanout`] to exhaustion and reassemble its completion-ordered items back
/// into **input order** — the buffering presentation shared by [`output_all`] /
/// [`output_all_bytes`]. Each command's index routes its result to a fixed slot, so
/// the returned `Vec` matches the input regardless of completion order, and the
/// engine's no-short-circuit / concurrency-cap behavior is inherited unchanged.
async fn collect_in_order<'a, R, T>(mut fanout: Fanout<'a, R, T>) -> Vec<Result<ProcessResult<T>>>
where
    R: ProcessRunner + ?Sized,
{
    use tokio_stream::StreamExt;

    let mut slots: Vec<Option<Result<ProcessResult<T>>>> =
        (0..fanout.total).map(|_| None).collect();
    // `Fanout` is `Unpin`, so `StreamExt::next` drives it through a plain `&mut`.
    while let Some((idx, result)) = fanout.next().await {
        slots[idx] = Some(result);
    }
    slots
        .into_iter()
        .map(|slot| slot.expect("every command fills its slot before the fan-out ends"))
        .collect()
}

/// Build a text-capturing [`Fanout`]: each command runs through
/// [`ProcessRunner::output_string`]. The `async move` block owns its `Command` and
/// borrows only `runner`, keeping the driver free of self-reference (see
/// [`CompletedFut`]).
fn text_fanout<'a, R, I>(commands: I, concurrency: usize, runner: &'a R) -> Fanout<'a, R, String>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    // The non-capturing closure coerces to the `Launch` fn pointer; the explicit
    // annotation forces that coercion.
    let launch: Launch<'a, R, String> =
        |r, idx, command| Box::pin(async move { (idx, r.output_string(&command).await) });
    Fanout::new(commands, concurrency, runner, launch)
}

/// Build a raw-bytes-capturing [`Fanout`]: the byte twin of [`text_fanout`], routing
/// each command through [`ProcessRunner::output_bytes`].
fn bytes_fanout<'a, R, I>(commands: I, concurrency: usize, runner: &'a R) -> Fanout<'a, R, Vec<u8>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    let launch: Launch<'a, R, Vec<u8>> =
        |r, idx, command| Box::pin(async move { (idx, r.output_bytes(&command).await) });
    Fanout::new(commands, concurrency, runner, launch)
}

/// Run every command in `commands`, keeping at most `concurrency` of them live
/// at once, and collect **all** their results in input order.
///
/// `concurrency` is clamped to at least 1. Each element is the independent
/// [`Result`] of one command: an `Err` is a spawn/I/O failure; a non-zero exit
/// is an `Ok(ProcessResult)` whose [`code`](ProcessResult::code) you inspect.
/// The batch never short-circuits.
///
/// Not cancel-safe: dropping the returned future mid-batch drops the in-flight
/// handles. With an own-group runner ([`JobRunner`](crate::JobRunner)) this kills
/// those children; with a shared-group runner (`&ProcessGroup`) they live until
/// the caller tears the group down. **No partial results (F3):** the `Vec` is
/// produced only when the whole batch finishes, so a mid-batch drop also discards
/// the results of commands that *had* already completed — there is no partial
/// recovery. If you need each result **as it lands** (to survive a cancellation, or
/// to act on the first finisher without waiting for the slowest), reach for
/// [`output_stream`] — the same bounded fan-out as a completion-ordered stream —
/// rather than driving the commands yourself.
///
/// This is exactly [`output_stream`] collected back into input order; the two share
/// one engine, so their concurrency and no-short-circuit semantics cannot drift.
pub async fn output_all<R, I>(
    commands: I,
    concurrency: usize,
    runner: &R,
) -> Vec<Result<ProcessResult<String>>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    collect_in_order(text_fanout(commands, concurrency, runner)).await
}

/// The raw-bytes companion to [`output_all`]: captures each command's stdout as
/// [`Vec<u8>`] instead of decoded text. All other semantics are identical — see
/// [`output_all`]. The streaming counterpart is [`output_stream_bytes`].
pub async fn output_all_bytes<R, I>(
    commands: I,
    concurrency: usize,
    runner: &R,
) -> Vec<Result<ProcessResult<Vec<u8>>>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    collect_in_order(bytes_fanout(commands, concurrency, runner)).await
}

/// Run every command in `commands` with at most `concurrency` live at once, yielding
/// each result — an `(input index, `[`Result`]`<`[`ProcessResult`]`<String>>)` pair —
/// **the moment that command finishes**. This is the streaming sibling of
/// [`output_all`]: the same bounded fan-out and the same per-command error semantics
/// (an `Err` is a spawn/I/O failure; a non-zero exit is an `Ok(ProcessResult)`; the
/// fan-out never short-circuits), but presented as a [`Stream`] over completions
/// instead of a single `Vec` at the end.
///
/// Key differences from [`output_all`]:
///
/// - **Completion order, not input order.** Items arrive as commands finish, so a
///   fast command never blocks behind a slow one. Each item carries the command's
///   **input index** (its position in `commands`), so you can still map a result back
///   to its source; if you need the input-order `Vec`, use [`output_all`] (which is
///   this stream collected by index).
/// - **Partial results survive cancellation.** Every result already yielded is owned
///   by the consumer, so dropping the stream mid-fan-out keeps them — unlike
///   [`output_all`], whose `Vec` materializes only at the very end (its "no partial
///   results" limitation).
///
/// `concurrency` is clamped to at least 1. An empty `commands` yields an empty stream
/// (immediately `None`).
///
/// **Cancellation / teardown.** Dropping the stream drops the in-flight command
/// futures — with an own-group runner ([`JobRunner`](crate::JobRunner)) that kills
/// every still-live process tree (no orphans), matching [`output_all`]; with a
/// shared-group runner (`&ProcessGroup`) they live until the group is torn down.
/// Commands still waiting for a concurrency slot are dropped **without ever being
/// spawned** — a queued command runs nothing until it is scheduled, so cancelling the
/// fan-out cancels them for free.
///
/// The returned stream borrows `runner` for `'a`; consume it with
/// [`StreamExt`](crate::prelude::StreamExt) (`while let Some((i, res)) =
/// stream.next().await`).
#[must_use = "a stream does nothing until polled (e.g. with `StreamExt::next`)"]
pub fn output_stream<'a, R, I>(
    commands: I,
    concurrency: usize,
    runner: &'a R,
) -> impl Stream<Item = Completed<String>> + Send + 'a
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command> + 'a,
{
    text_fanout(commands, concurrency, runner)
}

/// The raw-bytes companion to [`output_stream`]: each yielded [`ProcessResult`]
/// captures stdout as [`Vec<u8>`] instead of decoded text (for binary artifacts —
/// `git cat-file`, `tar -c`, an image transcoder). Scheduling, completion ordering,
/// input indexing, no-short-circuit, and cancellation/teardown are identical to
/// [`output_stream`]; the buffering counterpart is [`output_all_bytes`].
#[must_use = "a stream does nothing until polled (e.g. with `StreamExt::next`)"]
pub fn output_stream_bytes<'a, R, I>(
    commands: I,
    concurrency: usize,
    runner: &'a R,
) -> impl Stream<Item = Completed<Vec<u8>>> + Send + 'a
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command> + 'a,
{
    bytes_fanout(commands, concurrency, runner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{Reply, ScriptedRunner};

    #[tokio::test]
    async fn output_all_preserves_input_order() {
        let runner = ScriptedRunner::new()
            .on(["step", "0"], Reply::ok("zero"))
            .on(["step", "1"], Reply::ok("one"))
            .on(["step", "2"], Reply::ok("two"));
        let cmds = vec![
            Command::new("step").arg("0"),
            Command::new("step").arg("1"),
            Command::new("step").arg("2"),
        ];
        let results = output_all(cmds, 2, &runner).await;
        let stdout: Vec<&str> = results
            .iter()
            .map(|r| r.as_ref().expect("ok").stdout().as_str())
            .collect();
        assert_eq!(stdout, ["zero", "one", "two"]);
    }

    #[tokio::test]
    async fn output_all_collects_all_even_with_a_failure_in_the_middle() {
        let runner = ScriptedRunner::new()
            .on(["step", "0"], Reply::ok("ok-0"))
            .on(["step", "1"], Reply::fail(7, "boom"))
            .on(["step", "2"], Reply::ok("ok-2"));
        let cmds = vec![
            Command::new("step").arg("0"),
            Command::new("step").arg("1"),
            Command::new("step").arg("2"),
        ];
        let results = output_all(cmds, 3, &runner).await;
        assert_eq!(results.len(), 3);
        assert!(results[0].as_ref().unwrap().is_success());
        assert_eq!(results[1].as_ref().unwrap().code(), Some(7));
        assert!(results[2].as_ref().unwrap().is_success());
    }

    #[tokio::test]
    async fn output_all_never_exceeds_and_actually_reaches_the_concurrency_cap() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct ConcurrencyProbe {
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for ConcurrencyProbe {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    String::new(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                ))
            }
        }

        let probe = ConcurrencyProbe {
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        };
        let cmds: Vec<Command> = (0..10)
            .map(|i| Command::new("x").arg(i.to_string()))
            .collect();
        let results = output_all(cmds, 3, &probe).await;

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.as_ref().unwrap().is_success()));
        let peak = probe.peak.load(Ordering::SeqCst);
        assert!(peak <= 3, "concurrency cap exceeded: peak {peak} > 3");
        assert_eq!(
            peak, 3,
            "the cap must actually be reached (genuine overlap), got peak {peak}"
        );
        assert_eq!(
            probe.active.load(Ordering::SeqCst),
            0,
            "all futures finished"
        );
    }

    #[tokio::test]
    async fn output_all_bytes_captures_raw_stdout_in_input_order() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct BytesEcho {
            peak: Arc<AtomicUsize>,
            active: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for BytesEcho {
            async fn output_string(&self, _command: &Command) -> Result<ProcessResult<String>> {
                unreachable!("output_all_bytes must use output_bytes, not output_string")
            }
            async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
                let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                let arg = command.arguments()[0].to_string_lossy().into_owned();
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    arg.into_bytes(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                ))
            }
        }

        let runner = BytesEcho {
            peak: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
        };
        let cmds: Vec<Command> = (0..6)
            .map(|i| Command::new("echo").arg(i.to_string()))
            .collect();
        let results = output_all_bytes(cmds, 2, &runner).await;
        let bytes: Vec<Vec<u8>> = results
            .iter()
            .map(|r| r.as_ref().expect("ok").stdout().clone())
            .collect();
        let expected: Vec<Vec<u8>> = (0..6).map(|i| i.to_string().into_bytes()).collect();
        assert_eq!(bytes, expected, "raw bytes preserved in input order");
        let peak = runner.peak.load(Ordering::SeqCst);
        assert!(
            peak <= 2,
            "concurrency cap exceeded for the bytes batch: {peak}"
        );
        assert_eq!(
            peak, 2,
            "the cap must actually be reached (genuine overlap), got {peak}"
        );
    }

    #[tokio::test]
    async fn output_all_on_an_empty_batch_is_an_empty_vec() {
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let results = output_all(Vec::new(), 4, &runner).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn output_all_runs_more_commands_than_the_concurrency_cap() {
        let mut runner = ScriptedRunner::new();
        for i in 0..10 {
            runner = runner.on(["x".to_string(), i.to_string()], Reply::ok(format!("n{i}")));
        }
        let cmds: Vec<Command> = (0..10)
            .map(|i| Command::new("x").arg(i.to_string()))
            .collect();
        let results = output_all(cmds, 2, &runner).await;
        let stdout: Vec<String> = results
            .iter()
            .map(|r| r.as_ref().expect("ok").stdout().clone())
            .collect();
        let expected: Vec<String> = (0..10).map(|i| format!("n{i}")).collect();
        assert_eq!(stdout, expected);
    }

    // ── streaming driver (output_stream / output_stream_bytes) ────────────────

    #[tokio::test]
    async fn output_stream_yields_in_completion_order_not_input_order() {
        use crate::prelude::StreamExt;

        // A runner whose per-command latency is its second arg, expressed as a count
        // of cooperative yields: more yields = later completion.
        #[derive(Clone)]
        struct Paced;
        #[async_trait::async_trait]
        impl ProcessRunner for Paced {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                let yields: usize = command.arguments()[1]
                    .to_string_lossy()
                    .parse()
                    .expect("yield count");
                for _ in 0..yields {
                    tokio::task::yield_now().await;
                }
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    command.arguments()[0].to_string_lossy().into_owned(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                ))
            }
        }

        // Input order: slow (input idx 0, 8 yields) THEN fast (input idx 1, 1 yield).
        // With both live, the fast one must be handed back first — the pin that a fast
        // command does not wait on a slow sibling.
        let cmds = vec![
            Command::new("job").arg("0").arg("8"),
            Command::new("job").arg("1").arg("1"),
        ];
        let mut stream = output_stream(cmds, 2, &Paced);
        let mut order = Vec::new();
        while let Some((idx, result)) = stream.next().await {
            assert!(result.is_ok(), "no command errors in this batch");
            order.push(idx);
        }
        assert_eq!(
            order,
            vec![1, 0],
            "completion order: fast input-idx 1 before slow input-idx 0"
        );
    }

    #[tokio::test]
    async fn output_stream_respects_the_concurrency_cap() {
        use crate::prelude::StreamExt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct ConcurrencyProbe {
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for ConcurrencyProbe {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    String::new(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                ))
            }
        }

        let probe = ConcurrencyProbe {
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        };
        let cmds: Vec<Command> = (0..10)
            .map(|i| Command::new("x").arg(i.to_string()))
            .collect();
        let mut stream = output_stream(cmds, 3, &probe);
        let mut count = 0;
        while let Some((_idx, result)) = stream.next().await {
            assert!(result.unwrap().is_success());
            count += 1;
        }

        assert_eq!(count, 10, "every command is eventually yielded");
        let peak = probe.peak.load(Ordering::SeqCst);
        assert!(peak <= 3, "concurrency cap exceeded: peak {peak} > 3");
        assert_eq!(
            peak, 3,
            "the cap must actually be reached (genuine overlap), got peak {peak}"
        );
        assert_eq!(
            probe.active.load(Ordering::SeqCst),
            0,
            "all futures finished"
        );
    }

    #[tokio::test]
    async fn output_stream_does_not_short_circuit_on_a_failure() {
        use crate::prelude::StreamExt;
        let runner = ScriptedRunner::new()
            .on(["step", "0"], Reply::ok("ok-0"))
            .on(["step", "1"], Reply::fail(7, "boom"))
            .on(["step", "2"], Reply::ok("ok-2"));
        let cmds = vec![
            Command::new("step").arg("0"),
            Command::new("step").arg("1"),
            Command::new("step").arg("2"),
        ];
        let mut stream = output_stream(cmds, 3, &runner);
        let mut codes: std::collections::BTreeMap<usize, Option<i32>> = Default::default();
        while let Some((idx, result)) = stream.next().await {
            codes.insert(idx, result.expect("no spawn failures here").code());
        }
        // The middle failure neither dropped nor cancelled its siblings.
        assert_eq!(codes.len(), 3, "all three commands completed");
        assert_eq!(codes[&0], Some(0));
        assert_eq!(codes[&1], Some(7));
        assert_eq!(codes[&2], Some(0));
    }

    #[tokio::test]
    async fn output_stream_on_an_empty_batch_yields_nothing() {
        use crate::prelude::StreamExt;
        let runner = ScriptedRunner::new().fallback(Reply::ok(""));
        let mut stream = output_stream(Vec::new(), 4, &runner);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn output_stream_drop_cancels_inflight_and_never_spawns_queued_commands() {
        use crate::prelude::StreamExt;
        use std::collections::BTreeSet;
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Decrements the live counter when its command future is dropped — the proof
        /// that cancelling the fan-out actually tore an in-flight command down.
        struct LiveGuard(Arc<AtomicUsize>);
        impl Drop for LiveGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        #[derive(Clone)]
        struct Blocker {
            live: Arc<AtomicUsize>,
            started: Arc<Mutex<BTreeSet<usize>>>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for Blocker {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                let idx: usize = command.arguments()[0]
                    .to_string_lossy()
                    .parse()
                    .expect("index arg");
                self.started.lock().expect("started lock").insert(idx);
                self.live.fetch_add(1, Ordering::SeqCst);
                let _guard = LiveGuard(self.live.clone());
                // Never resolves: the only exit is a drop (the cancellation under test).
                std::future::pending::<()>().await;
                unreachable!("pending() never resolves")
            }
        }

        let blocker = Blocker {
            live: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(Mutex::new(BTreeSet::new())),
        };
        // 5 commands, cap 2: only input idx 0 and 1 can ever be scheduled; 2/3/4 wait
        // for a slot that a never-completing command never frees.
        let cmds: Vec<Command> = (0..5)
            .map(|i| Command::new("block").arg(i.to_string()))
            .collect();
        let mut stream = output_stream(cmds, 2, &blocker);

        // Drive the fan-out: it launches the first two commands (both block forever),
        // so the stream stays pending until we stop waiting.
        let polled =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await;
        assert!(
            polled.is_err(),
            "every launched command blocks, so nothing is yielded"
        );
        assert_eq!(
            blocker.live.load(Ordering::SeqCst),
            2,
            "exactly the cap's worth of commands are live"
        );

        // Cancel: dropping the stream drops the two in-flight futures (their guards
        // fire) and drops the three still-queued commands WITHOUT spawning them.
        drop(stream);

        assert_eq!(
            blocker.live.load(Ordering::SeqCst),
            0,
            "dropping the stream tore down every in-flight command"
        );
        let started = blocker.started.lock().expect("started lock").clone();
        assert_eq!(
            started,
            BTreeSet::from([0, 1]),
            "only the cap's worth of commands ever started; queued 2/3/4 never spawned"
        );
    }

    #[tokio::test]
    async fn output_stream_yielded_results_survive_a_mid_fanout_drop() {
        use crate::prelude::StreamExt;

        // Input idx 0 completes at once; input idx 1 blocks forever. Cap 2 → both launch.
        #[derive(Clone)]
        struct OneFastOneBlocked;
        #[async_trait::async_trait]
        impl ProcessRunner for OneFastOneBlocked {
            async fn output_string(&self, command: &Command) -> Result<ProcessResult<String>> {
                if command.arguments()[0].to_string_lossy() == "0" {
                    return Ok(ProcessResult::new(
                        command.program().to_string_lossy().into_owned(),
                        "fast".to_owned(),
                        String::new(),
                        crate::result::Outcome::Exited(0),
                        None,
                    ));
                }
                std::future::pending::<()>().await;
                unreachable!("the blocked command never resolves")
            }
        }

        let cmds = vec![Command::new("job").arg("0"), Command::new("job").arg("1")];
        let mut stream = output_stream(cmds, 2, &OneFastOneBlocked);

        // The fast command lands while the blocked one is still live.
        let (idx, result) =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
                .await
                .expect("the fast command resolves before the timeout")
                .expect("a yielded result, not end-of-stream");
        assert_eq!(idx, 0, "the fast command is input idx 0");

        // Cancel while input idx 1 is still blocked. The already-yielded result is the
        // consumer's — the cancellation cannot reclaim it.
        drop(stream);
        assert_eq!(
            result.expect("the yielded result is intact").stdout(),
            "fast"
        );
    }

    #[tokio::test]
    async fn output_stream_bytes_captures_raw_stdout_tagged_with_input_index() {
        use crate::prelude::StreamExt;

        #[derive(Clone)]
        struct BytesEcho;
        #[async_trait::async_trait]
        impl ProcessRunner for BytesEcho {
            async fn output_string(&self, _command: &Command) -> Result<ProcessResult<String>> {
                unreachable!("output_stream_bytes must use output_bytes, not output_string")
            }
            async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
                let arg = command.arguments()[0].to_string_lossy().into_owned();
                Ok(ProcessResult::new(
                    command.program().to_string_lossy().into_owned(),
                    arg.into_bytes(),
                    String::new(),
                    crate::result::Outcome::Exited(0),
                    None,
                ))
            }
        }

        let cmds: Vec<Command> = (0..6)
            .map(|i| Command::new("echo").arg(i.to_string()))
            .collect();
        let mut stream = output_stream_bytes(cmds, 2, &BytesEcho);
        let mut got: std::collections::BTreeMap<usize, Vec<u8>> = Default::default();
        while let Some((idx, result)) = stream.next().await {
            got.insert(idx, result.expect("ok").stdout().clone());
        }
        let expected: std::collections::BTreeMap<usize, Vec<u8>> =
            (0..6).map(|i| (i, i.to_string().into_bytes())).collect();
        assert_eq!(got, expected, "each input index maps to its own raw bytes");
    }
}
