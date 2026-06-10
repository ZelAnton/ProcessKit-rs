# Cookbook

[‹ docs index](README.md)

Task-oriented recipes: find the thing you're trying to do, copy the snippet,
follow the link when you need the fine print. Every snippet assumes a tokio
runtime and `use processkit::Command;` unless shown otherwise.

- [Run a command and get its output](#run-a-command-and-get-its-output)
- [Inspect a failure instead of erroring](#inspect-a-failure-instead-of-erroring)
- [Ask a yes/no question](#ask-a-yesno-question)
- [Accept non-zero exit codes as success](#accept-non-zero-exit-codes-as-success)
- [Bound a run with a timeout](#bound-a-run-with-a-timeout)
- [Let a tool clean up on timeout](#let-a-tool-clean-up-on-timeout)
- [Show a useful error message](#show-a-useful-error-message)
- [Feed the child's stdin](#feed-the-childs-stdin)
- [Stream output as it arrives](#stream-output-as-it-arrives)
- [Talk to an interactive child](#talk-to-an-interactive-child)
- [Pipe commands without a shell](#pipe-commands-without-a-shell)
- [Start a server and wait until it's ready](#start-a-server-and-wait-until-its-ready)
- [Tear down several children as a unit](#tear-down-several-children-as-a-unit)
- [React to whichever child exits first](#react-to-whichever-child-exits-first)
- [Sandbox an untrusted tool](#sandbox-an-untrusted-tool)
- [Keep a crash-prone service running](#keep-a-crash-prone-service-running)
- [Retry a flaky command](#retry-a-flaky-command)
- [Cancel runs on shutdown](#cancel-runs-on-shutdown)
- [Measure what a run cost](#measure-what-a-run-cost)
- [Contain a process you didn't spawn](#contain-a-process-you-didnt-spawn)
- [Test code that runs processes — without processes](#test-code-that-runs-processes--without-processes)
- [Test streaming code — without processes](#test-streaming-code--without-processes)
- [Wrap a CLI tool behind a typed API](#wrap-a-cli-tool-behind-a-typed-api)

## Run a command and get its output

```rust,no_run
let head = Command::new("git").args(["rev-parse", "HEAD"]).run().await?;
```

`run()` requires a zero exit and returns stdout with trailing whitespace
trimmed; a non-zero exit, spawn failure, or timeout is a typed `Error`. For a
one-liner without the builder: `processkit::run("git", ["rev-parse", "HEAD"])`.

*Fine print: [Running commands → consuming verbs](commands.md).*

## Inspect a failure instead of erroring

```rust,no_run
let result = Command::new("git").args(["merge", "topic"]).output_string().await?;
if !result.is_success() {
    eprintln!("merge exited {:?}: {}", result.code(), result.stderr());
}
```

`output_string()` (and `output_bytes()` for raw bytes) treats the exit code as
data — `Err` means the run couldn't happen at all. Call
`result.ensure_success()?` later to convert a stored failure into the same
typed error `run()` would have produced.

*Fine print: [Running commands → results and errors](commands.md).*

## Ask a yes/no question

```rust,no_run
let dirty = !Command::new("git").args(["diff", "--quiet"]).probe().await?;
```

`probe()` maps exit 0 → `true`, exit 1 → `false`, and anything else to an
error — the `git diff --quiet` / `grep -q` convention without manual code
matching.

## Accept non-zero exit codes as success

```rust,no_run
// `grep` exits 1 when it finds no match — not a failure for this call.
let found = Command::new("grep")
    .args(["needle", "haystack.txt"])
    .ok_codes([0, 1])
    .output_string()
    .await?;
let matched = found.code() == Some(0); // 0 = matched, 1 = no match (both "success")
```

`ok_codes` widens what the checking verbs (`run`/`run_unit`) and
`is_success`/`ensure_success` treat as success — for tools whose non-zero exit is a
normal result (`grep` 1 = no match, `diff` 1 = differs, rsync's code families). It
does **not** change `exit_code` (always the raw code) or `probe` (always the 0/1
convention). An empty set is ignored, so the default stays exit `0`.

## Bound a run with a timeout

```rust,no_run
use std::time::Duration;

let result = Command::new("slow-tool")
    .timeout(Duration::from_secs(30))
    .output_string()
    .await?;
if result.timed_out() {
    eprintln!("gave up after 30s; partial output: {}", result.stdout());
}
```

At the deadline the whole tree is killed. On the capture verbs the timeout is
*captured* (`timed_out()`, partial output kept); on the success-checking verbs
(`run`, `exit_code`) it surfaces as `Error::Timeout`.

## Let a tool clean up on timeout

```rust,no_run
use std::time::Duration;

let result = Command::new("dev-server")
    .timeout(Duration::from_secs(30))
    .timeout_grace(Duration::from_secs(5)) // SIGTERM, wait up to 5s, then SIGKILL
    .output_string()
    .await?;
```

`timeout_grace` turns the hard deadline kill into a graceful one: `SIGTERM` (or the
signal from `timeout_signal`, with the `process-control` feature), up to the grace
window to exit, then `SIGKILL`. A signal-handling child exits early; `timed_out()`
stays `true`. Windows has no signal tier — the deadline kills atomically.

*Fine print: [Timeouts → graceful timeout](timeouts-and-cancellation.md#graceful-timeout).*

*Fine print: [Timeouts, retries & cancellation](timeouts-and-cancellation.md).*

## Show a useful error message

```rust,no_run
if let Err(e) = Command::new("git").args(["merge", "topic"]).run().await {
    eprintln!("merge failed: {}", e.diagnostic().unwrap_or("(no output)"));
}
```

`Error::diagnostic()` picks the most explanatory captured text — stderr,
falling back to stdout (git writes `CONFLICT …` there) — so callers don't
re-implement the same heuristic.

## Feed the child's stdin

```rust,no_run
use processkit::Stdin;

// A string you already have:
let sorted = Command::new("sort")
    .stdin(Stdin::from_string("banana\napple\n"))
    .run()
    .await?;

// …or any async source: a reader (file, socket) or a stream of lines.
let from_file = Stdin::from_reader(tokio::fs::File::open("input.txt").await?);
let from_chan = Stdin::from_lines(tokio_stream::iter(vec!["one".to_owned()]));
```

One-shot sources (`from_reader`/`from_lines`) feed a single run; re-running the
same `Command` afterwards sees empty stdin. For a conversation, see the next
recipe but one.

*Fine print: [Running commands → standard input](commands.md).*

## Stream output as it arrives

```rust,no_run
use processkit::{StreamExt, StreamedFinish}; // StreamExt re-exported; provides `.next()`

let mut run = Command::new("cargo").args(["build", "--verbose"]).start().await?;
let mut lines = run.stdout_lines();
while let Some(line) = lines.next().await {
    println!("build: {line}");
}
let StreamedFinish { outcome, stderr } = run.finish_streamed().await?; // outcome + buffered stderr
```

No waiting for exit, no full-output buffering; stderr is drained in the
background so the child can't block. A `timeout` on the command bounds the
stream itself. Prefer a callback? `.on_stdout_line(|l| …)` runs one per line
while any capture verb drives the run.

*Fine print: [Streaming & interactive I/O](streaming.md).*

## Talk to an interactive child

```rust,no_run
use processkit::StreamExt;

let mut run = Command::new("bc").keep_stdin_open().start().await?;
let mut stdin = run.standard_input().expect("stdin was kept open");
stdin.write_line("2 + 2").await?;
stdin.finish().await?; // EOF — bc exits

let mut answers = run.stdout_lines();
while let Some(answer) = answers.next().await {
    println!("{answer}");
}
```

`keep_stdin_open()` hands you an async writer instead of closing stdin at
spawn; interleave writes with reads for request/response tools.

*Fine print: [Streaming & interactive I/O → interactive stdin](streaming.md).*

## Pipe commands without a shell

```rust,no_run
let authors = Command::new("git").args(["log", "--format=%an"])
    .pipe(Command::new("sort"))
    .pipe(Command::new("uniq").arg("-c"))
    .output_string()
    .await?;
```

Native pipes — no shell string, no quoting, no injection surface. The outcome
is **pipefail**: stdout comes from the last stage, the reported failure from
the first stage that didn't exit cleanly. All stages share one kill-on-drop
group. The `|` operator is equivalent sugar:
`(a | b | c).output_string()`.

For a consumer that legitimately stops reading early — the `| head -1` shape,
where the producer's `SIGPIPE` death is expected — mark the producer
`unchecked()` so that death doesn't fail the chain:

```rust,no_run
let first = (Command::new("seq").args(["1", "1000000"]).unchecked()
    | Command::new("head").args(["-n", "1"]))
    .run()
    .await?;
```

*Fine print: [Pipelines → unchecked stages](pipelines.md#unchecked-stages).*

## Start a server and wait until it's ready

```rust,no_run
use std::time::Duration;

let mut server = Command::new("my-server").args(["--port", "8080"]).start().await?;

// Pick the probe that matches how the server announces readiness:
server.wait_for_line(|l| l.contains("listening"), Duration::from_secs(10)).await?;
// server.wait_for_port("127.0.0.1:8080".parse().unwrap(), Duration::from_secs(10)).await?;
// server.wait_for(|| async { http_health().await }, Duration::from_secs(10)).await?;

// …use the server; dropping `server` kills its whole tree.
```

A probe that can't succeed fails fast with `Error::NotReady` and never kills
the child — you decide what happens next. No more `sleep(2)` and hoping.

*Fine print: [Streaming & interactive I/O → readiness probes](streaming.md).*

## Tear down several children as a unit

```rust,no_run
use processkit::ProcessGroup;

let group = ProcessGroup::new()?;
let _db = group.start(&Command::new("dev-db")).await?;
let _api = group.start(&Command::new("dev-api")).await?;

// Either: graceful — SIGTERM, bounded wait, optional SIGKILL escalation…
group.shutdown().await?;
// …or just drop(group): hard kill-on-drop of everything, grandchildren included.
```

The group is the unit of fate: a panic or early return anywhere reaps every
member. Configure the grace window via `ProcessGroupOptions`.

*Fine print: [Process groups](process-groups.md).*

## React to whichever child exits first

```rust,no_run
use processkit::{ProcessGroup, wait_any};

let group = ProcessGroup::new()?;
let mut a = group.start(&Command::new("worker-a")).await?;
let mut b = group.start(&Command::new("worker-b")).await?;

let (idx, outcome) = wait_any(&mut [&mut a, &mut b]).await?;
println!("worker #{idx} exited first with {outcome:?}");
// `a` and `b` are only borrowed — the loser is still usable here.
```

*Fine print: [Streaming & interactive I/O → racing children](streaming.md).*

## Sandbox an untrusted tool

```rust,no_run
use processkit::{ProcessGroup, ProcessGroupOptions};

// Cap the whole tree (requires the `limits` feature; Windows Job / Linux cgroup):
let group = ProcessGroup::with_options(
    ProcessGroupOptions::default()
        .memory_max(512 * 1024 * 1024)
        .max_processes(64)
        .cpu_quota(0.5),
)?;

let result = group
    .start(
        &Command::new("untrusted-tool")
            .inherit_env(["PATH"]) // allow-list: everything else is cleared
            .timeout(std::time::Duration::from_secs(60)),
    )
    .await?
    .output_string()
    .await?;
```

Unenforceable limits are a hard `Error::ResourceLimit`, never a silently
unbounded group. On Unix, add `.uid(…)`/`.gid(…)` to drop privileges (note the
cgroup-mechanism caveat in the guide).

*Fine print: [Process groups → resource limits](process-groups.md) ·
[Running commands → privileges](commands.md).*

## Keep a crash-prone service running

```rust,no_run
use processkit::{RestartPolicy, Supervisor};
use std::time::Duration;

let outcome = Supervisor::new(Command::new("my-service"))
    .restart(RestartPolicy::OnCrash)
    .max_restarts(5)
    .backoff(Duration::from_millis(200), 2.0)
    .storm_pause(Duration::from_secs(15)) // crash-loop guard (off by default)
    .run()
    .await?;
println!(
    "stopped after {} restarts ({} storm pauses): {:?}",
    outcome.restarts, outcome.storm_pauses, outcome.stopped
);
```

Exponential backoff with jitter by default; `stop_when(…)` ends supervision on
a condition; `.with_runner(&group)` keeps every incarnation inside one shared
kill-on-drop group. `storm_pause` arms the failure-storm guard: failures feed
a decaying score, and past the threshold the supervisor takes one collective
pause instead of hammering restarts — "fails rarely" and "crash-looping" stop
being the same case.

*Fine print: [Supervision](supervision.md), [failure storms](supervision.md#failure-storms).*

## Retry a flaky command

```rust,no_run
use processkit::Error;
use std::time::Duration;

let fetched = Command::new("git")
    .args(["fetch", "--quiet"])
    .timeout(Duration::from_secs(10))
    .retry(3, Duration::from_millis(200), |e| {
        matches!(e, Error::Timeout { .. })
            || e.diagnostic().is_some_and(|m| m.contains("Could not resolve host"))
    })
    .run()
    .await?;
```

The classifier sees the typed error and decides whether this failure is worth
another attempt; each attempt is a fresh process. `retry` replays a run to
success — for keeping a process *alive*, use a `Supervisor` (previous recipe).

*Fine print: [Timeouts, retries & cancellation → retry](timeouts-and-cancellation.md).*

## Cancel runs on shutdown

```rust,no_run
use processkit::CancellationToken; // `cancellation` feature

let token = CancellationToken::new();

let job = tokio::spawn({
    let token = token.child_token();
    async move { Command::new("long-job").cancel_on(token).run().await }
});

// On Ctrl-C / shutdown signal / sibling failure:
token.cancel(); // kills the tree; the run resolves to Error::Cancelled
let outcome = job.await; // Err(Error::Cancelled { .. }) inside
```

Cancellation is always an error (the run was abandoned, there is no result),
beats a simultaneous timeout, and is terminal for `retry` and `Supervisor`
alike.

For a typed wrapper whose commands never cross your code, set the token once
on the client — every command it builds carries it:

```rust,no_run
use processkit::{CancellationToken, CliClient};

let token = CancellationToken::new();
let gh = CliClient::new("gh").default_cancel_on(token.child_token());
// token.cancel() → every in-flight command of THIS client dies.
```

*Fine print: [Timeouts, retries & cancellation → cancellation](timeouts-and-cancellation.md), [client-level default](timeouts-and-cancellation.md#client-level-default).*

## Measure what a run cost

```rust,no_run
use std::time::Duration;

// One run, summarized (requires the default-on `stats` feature):
let profile = Command::new("crunch").start().await?.profile(Duration::from_millis(100)).await?;
println!("exit={:?} took={:?} peak_rss={:?} avg_cpu={:?}",
    profile.exit_code, profile.duration, profile.peak_memory_bytes, profile.avg_cpu());
```

For a live series over a whole group, `group.sample_stats(every)` yields a
`Stream` of snapshots. CPU/memory need a real container (Windows Job / Linux
cgroup); elsewhere you still get process counts.

*Fine print: [Process groups → stats](process-groups.md) ·
[Streaming → profiling](streaming.md).*

## Contain a process you didn't spawn

```rust,no_run
use processkit::ProcessGroup;

let child = tokio::process::Command::new("legacy-launcher").spawn()?;

let group = ProcessGroup::new()?; // `adopt` is part of `process-control` (default-on)
group.adopt(&child)?;            // from now on the group's teardown covers it
```

Adoption is best-effort by mechanism — on Windows/cgroup the whole running
tree joins; on the POSIX process-group backends an exec'd child is contained
individually (its *future* forks too, where it could be re-grouped). The guide
spells out exactly what each mechanism can promise.

*Fine print: [Process groups → adopt](process-groups.md) ·
[Platform support](platform-support.md).*

## Test code that runs processes — without processes

```rust,no_run
use processkit::{Reply, ScriptedRunner};

// Your code takes any `R: ProcessRunner`; in tests, hand it a script.
// Rules match on a prefix of the *arguments* (the program name is not part
// of the argument list):
let runner = ScriptedRunner::new()
    .on(["rev-parse"], Reply::ok("abc123\n"))
    .on(["push"], Reply::fail(128, "remote: permission denied"))
    .fallback(Reply::ok(""));

// my_deploy(&runner).await? — no subprocess, fully deterministic.
```

`RecordingRunner` wraps any runner and captures every `Invocation` for
assertions; `MockRunner` (feature `mock`) gives `mockall` expectations; and
the `record` feature's `RecordReplayRunner` records real runs into a JSON
cassette once and replays them hermetically in CI.

*Fine print: [Testing your code](testing.md).*

## Test streaming code — without processes

```rust,no_run
use processkit::{Command, Outcome, ProcessRunner, Reply, ScriptedRunner, StreamedFinish};
use std::time::Duration;

let runner = ScriptedRunner::new()
    .on(["run", "watch"], Reply::lines(["queued", "in_progress", "completed"])
        .with_line_delay(Duration::from_millis(50))); // paced delivery

let mut run = runner.start(&Command::new("gh").args(["run", "watch", "123"])).await?;
run.wait_for_line(|l| l.contains("completed"), Duration::from_secs(5)).await?;
let StreamedFinish { outcome, .. } = run.finish_streamed().await?;
assert_eq!(outcome, Outcome::Exited(0));
```

A scripted `start()` feeds the canned lines through the **same pump
machinery** a real child uses, so `stdout_lines`, the readiness probes, and
`finish_streamed` behave identically — and `with_line_delay` is deterministic
under `#[tokio::test(start_paused = true)]`. Canned output also replays
through `on_stdout_line`/`on_stderr_line` handlers on the bulk verbs, so
progress-reporting paths test hermetically too.

*Fine print: [Testing → scripted streaming](testing.md#scripted-streaming).*

## Wrap a CLI tool behind a typed API

```rust,no_run
use processkit::{cli_client, ProcessRunner, Result};

cli_client!(pub struct Git => "git");

impl<R: ProcessRunner> Git<R> {
    pub async fn current_branch(&self) -> Result<String> {
        self.core.run(self.core.command(["branch", "--show-current"])).await
    }
    pub async fn is_clean(&self) -> Result<bool> {
        self.core.probe(self.core.command(["diff", "--quiet"])).await
    }
}
```

The generated struct carries a runner and per-client defaults
(`default_timeout`, `default_env`); your methods are just argument lists and
parsers — and because the runner is injectable, the whole wrapper is testable
with the previous recipe's `ScriptedRunner`.

*Fine print: [Testing your code → CliClient](testing.md).*
