//! Bounded-concurrency batch execution.
//!
//! [`output_all`] (text) and [`output_all_bytes`] (raw bytes) run a whole slice
//! of commands while capping how many live at once — the back-pressure the
//! one-shot verbs lack when you fan out hundreds of commands. For awaiting
//! handles you already hold, see [`wait_all`](crate::wait_all).

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use crate::{Command, ProcessResult, ProcessRunner, Result};

/// The boxed future a [`ProcessRunner::output`] / [`ProcessRunner::output_bytes`]
/// call returns (via `async-trait`): already pinned, boxed, and `Send`, so the
/// batch driver stores it as-is. Generic over the captured payload `T` (`String`
/// for [`output_all`], `Vec<u8>` for [`output_all_bytes`]).
type OutputFut<'a, T> = Pin<Box<dyn Future<Output = Result<ProcessResult<T>>> + Send + 'a>>;

/// Run every command in `commands`, keeping at most `concurrency` of them live
/// at once, and collect **all** their results in input order.
///
/// The batch companion to the one-shot verbs. Where [`Command::output_string`]
/// runs a single command, `output_all` runs a whole batch while bounding how many
/// processes exist simultaneously — the missing back-pressure when you spawn
/// hundreds of commands and would otherwise exhaust file descriptors or the
/// process table. `concurrency` is clamped to at least 1.
///
/// `runner` is any [`ProcessRunner`]: pass `&group` (a
/// [`ProcessGroup`](crate::ProcessGroup)) to keep every child in one shared
/// kill-on-drop group, or `&JobRunner` to give each command its own private
/// group.
///
/// **Partial failure is data, not control flow.** Each element is the
/// independent [`Result`] of one command, in input order: an `Err` is a
/// spawn/I/O failure for that command alone, while a non-zero *exit* is an
/// `Ok(ProcessResult)` whose [`code`](ProcessResult::code) /
/// [`is_success`](ProcessResult::is_success) you inspect. The batch never
/// short-circuits — every command runs and the caller folds the outcomes.
///
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::{Command, JobRunner, output_all};
///
/// let cmds = vec![
///     Command::new("convert").args(["a.png", "a.webp"]),
///     Command::new("convert").args(["b.png", "b.webp"]),
///     Command::new("convert").args(["c.png", "c.webp"]),
/// ];
/// // At most two conversions run at once; every result is collected.
/// let results = output_all(cmds, 2, &JobRunner).await;
/// let failed = results
///     .iter()
///     .filter(|r| !matches!(r, Ok(o) if o.is_success()))
///     .count();
/// println!("{failed} of {} commands failed", results.len());
/// # Ok(())
/// # }
/// ```
///
/// Deliberately *not* a process pool, scheduler, or retrier — those are policy.
/// This is the one primitive — bounded fan-out that collects every outcome — in
/// two capture flavors: text here, raw bytes via [`output_all_bytes`] (the same
/// `output_string` vs `output_bytes` split `Command`/`Pipeline` already have).
///
/// Unlike [`wait_all`](crate::wait_all) / [`wait_any`](crate::wait_any), this is
/// **not** cancel-safe: it consumes the `Command`s and owns the handles it
/// spawns, so dropping the returned future mid-batch drops those handles
/// (results already collected are unaffected).
///
/// Whether dropping a handle *kills* its tree depends on the `runner` (B16):
/// with an **own-group** runner ([`JobRunner`](crate::JobRunner) — the common
/// case) each handle owns its group, so its `Drop` tears the tree down. With a
/// **shared-group** runner (`&ProcessGroup`), the handles share the caller's
/// group and their `Drop` deliberately does *not* kill — the still-running
/// children live until the caller tears the group down (its `Drop` or
/// [`shutdown`](crate::ProcessGroup::shutdown)). Use an own-group runner if
/// dropping the batch future must reap in-flight children.
pub async fn output_all<R, I>(
    commands: I,
    concurrency: usize,
    runner: &R,
) -> Vec<Result<ProcessResult<String>>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    run_all(commands, concurrency, runner, |r, c| r.output(c)).await
}

/// The raw-bytes companion to [`output_all`]: the same bounded fan-out, but each
/// command's stdout is captured as [`Vec<u8>`] (via
/// [`ProcessRunner::output_bytes`]) instead of decoded text — for batching
/// binary-producing commands (image conversions, `curl`-into-bytes, …). Every
/// other property is identical: partial failure is per-element data, results are
/// in input order, at most `concurrency` (clamped to ≥ 1) run at once, and
/// dropping the future tears in-flight children down per the `runner`'s grouping
/// (own-group reaps, shared-group defers — see [`output_all`]). Like
/// [`output_all`], it is **not** cancel-safe: it owns the handles it spawns, so
/// dropping the returned future mid-batch drops them (already-collected results
/// are unaffected).
///
/// ```no_run
/// # async fn demo() -> processkit::Result<()> {
/// use processkit::{Command, JobRunner, output_all_bytes};
///
/// let cmds = vec![
///     Command::new("gzip").args(["-c", "a.txt"]),
///     Command::new("gzip").args(["-c", "b.txt"]),
/// ];
/// let results = output_all_bytes(cmds, 2, &JobRunner).await;
/// for r in &results {
///     if let Ok(out) = r {
///         println!("{} compressed bytes", out.stdout().len());
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn output_all_bytes<R, I>(
    commands: I,
    concurrency: usize,
    runner: &R,
) -> Vec<Result<ProcessResult<Vec<u8>>>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    run_all(commands, concurrency, runner, |r, c| r.output_bytes(c)).await
}

/// The shared bounded-fan-out driver behind [`output_all`] /
/// [`output_all_bytes`]. `launch` selects the per-command capture verb (text vs
/// raw bytes); everything else — the `concurrency`-capped active set, the
/// poll-and-harvest loop, and input-order assembly — is identical for both.
async fn run_all<R, I, T, L>(
    commands: I,
    concurrency: usize,
    runner: &R,
    launch: L,
) -> Vec<Result<ProcessResult<T>>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
    L: for<'a> Fn(&'a R, &'a Command) -> OutputFut<'a, T>,
{
    let commands: Vec<Command> = commands.into_iter().collect();
    let n = commands.len();
    let limit = concurrency.max(1);
    // Borrow the owned Vec: the in-flight futures reference `commands` here in
    // the async-fn frame, not a field of the `move` closure — so the closure's
    // `active` set isn't self-referential (the same shape that lets `wait_any`
    // capture its `waits` without borrowing itself).
    let commands = &commands;

    let mut results: Vec<Option<Result<ProcessResult<T>>>> = (0..n).map(|_| None).collect();
    let mut next = 0usize;
    // Active set: (result slot, in-flight capture future). Capped at `limit`.
    let mut active: Vec<(usize, OutputFut<'_, T>)> = Vec::new();

    std::future::poll_fn(move |cx| {
        loop {
            // Top the active set up from the remaining commands.
            while active.len() < limit && next < n {
                let idx = next;
                next += 1;
                active.push((idx, launch(runner, &commands[idx])));
            }

            // Poll every active future once; harvest the finishers in place.
            let mut completed = false;
            let mut i = 0;
            while i < active.len() {
                if let Poll::Ready(result) = active[i].1.as_mut().poll(cx) {
                    let (idx, _) = active.swap_remove(i);
                    results[idx] = Some(result);
                    completed = true;
                    // `swap_remove` moved the last entry into slot `i`; don't
                    // advance — re-poll whatever now sits there.
                } else {
                    i += 1;
                }
            }

            if active.is_empty() && next >= n {
                // Every command has run; assemble the results in input order.
                return Poll::Ready(
                    results
                        .iter_mut()
                        .map(|slot| slot.take().expect("every slot filled before completion"))
                        .collect(),
                );
            }
            if !completed {
                // All active futures are `Pending` and have just registered
                // their wakers; nothing new can start until one completes.
                return Poll::Pending;
            }
            // A completion freed a slot: loop to top up and poll the freshly
            // started futures so they register wakers before we yield.
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{Reply, ScriptedRunner};

    // A scripted runner answers each `output` with a canned reply keyed on the
    // command's args — no subprocess, so these exercise the batch driver itself
    // hermetically. Each batch command carries a distinct arg to route it.

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
        // The middle command exits non-zero — that's an `Ok(ProcessResult)`
        // with a non-zero code, not an `Err`, and must not stop the others.
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
        // A probe runner whose `output` future stays `Pending` across several
        // polls (via `yield_now`), so the batch driver tops the active set up to
        // the cap *before* any future completes — letting us observe the true peak
        // concurrency. The synchronous-`Ready` `ScriptedRunner` can't exercise the
        // `active.len() < limit` ceiling at all (every reply completes on the first
        // poll), so a regression removing the cap would pass against it.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct ConcurrencyProbe {
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for ConcurrencyProbe {
            async fn output(&self, command: &Command) -> Result<ProcessResult<String>> {
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
        // S-7: the bytes companion runs the same bounded fan-out but captures
        // each command's stdout as raw bytes (via `output_bytes`). A runner that
        // echoes the command's first arg as bytes lets us assert order + payload
        // hermetically.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct BytesEcho {
            peak: Arc<AtomicUsize>,
            active: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ProcessRunner for BytesEcho {
            async fn output(&self, _command: &Command) -> Result<ProcessResult<String>> {
                unreachable!("output_all_bytes must use output_bytes, not output")
            }
            async fn output_bytes(&self, command: &Command) -> Result<ProcessResult<Vec<u8>>> {
                let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                // Stay Pending across several polls so the driver tops the active
                // set up to the cap before any future completes — letting us
                // observe the true peak (mirrors the text-path cap test).
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
        // 10 commands, cap 2: every one must still run and land in order.
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
}
