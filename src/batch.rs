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

/// The boxed future a [`ProcessRunner::output_string`] / [`ProcessRunner::output_bytes`]
/// call returns (via `async-trait`): already pinned, boxed, and `Send`, so the
/// batch driver stores it as-is. Generic over the captured payload `T` (`String`
/// for [`output_all`], `Vec<u8>` for [`output_all_bytes`]).
type OutputFut<'a, T> = Pin<Box<dyn Future<Output = Result<ProcessResult<T>>> + Send + 'a>>;

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
/// recovery. If you need each result as it lands (to survive a cancellation),
/// drive the commands yourself (e.g. a `JoinSet`) rather than through this helper.
pub async fn output_all<R, I>(
    commands: I,
    concurrency: usize,
    runner: &R,
) -> Vec<Result<ProcessResult<String>>>
where
    R: ProcessRunner + ?Sized,
    I: IntoIterator<Item = Command>,
{
    run_all(commands, concurrency, runner, |r, c| r.output_string(c)).await
}

/// The raw-bytes companion to [`output_all`]: captures each command's stdout as
/// [`Vec<u8>`] instead of decoded text. All other semantics are identical — see
/// [`output_all`].
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

/// Shared driver behind [`output_all`] / [`output_all_bytes`]. `launch` selects
/// the capture verb; the concurrency cap, poll loop, and input-order assembly
/// are identical for both.
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
    // Borrow into the async-fn frame so in-flight futures don't make `active` self-referential.
    let commands = &commands;

    let mut results: Vec<Option<Result<ProcessResult<T>>>> = (0..n).map(|_| None).collect();
    let mut next = 0usize;
    let mut active: Vec<(usize, OutputFut<'_, T>)> = Vec::new();

    std::future::poll_fn(move |cx| {
        loop {
            while active.len() < limit && next < n {
                let idx = next;
                next += 1;
                active.push((idx, launch(runner, &commands[idx])));
            }

            let mut completed = false;
            let mut i = 0;
            while i < active.len() {
                if let Poll::Ready(result) = active[i].1.as_mut().poll(cx) {
                    let (idx, _) = active.swap_remove(i);
                    results[idx] = Some(result);
                    completed = true;
                    // `swap_remove` moved the last entry here; re-poll it.
                } else {
                    i += 1;
                }
            }

            if active.is_empty() && next >= n {
                return Poll::Ready(
                    results
                        .iter_mut()
                        .map(|slot| slot.take().expect("every slot filled before completion"))
                        .collect(),
                );
            }
            if !completed {
                return Poll::Pending;
            }
            // A slot freed — loop to top up and register wakers on new futures.
        }
    })
    .await
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
}
