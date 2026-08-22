# Pipelines

[‹ docs index](README.md)

`a | b | c` **without a shell**. Each stage's stdout feeds the next stage's
stdin through an in-process relay (a `tokio::io::copy` task per boundary) — there
is no shell string anywhere, so no quoting rules, no word splitting, no injection
surface. Each stage spawns into its **own** kill-on-drop
[process group](process-groups.md) sub-group, so a per-stage
[`Command::timeout`](#timeouts) tears down that stage's whole subtree
(grandchildren of a forking `sh -c …` included); a chain-wide teardown fans the
kill across every stage's sub-group, so the chain still lives and dies as a
unit. (The relay is an implementation detail, not a kernel splice: a producer
whose consumer exits early stops on a *broken pipe* when the relay's next write
fails, rather than instantly via `SIGPIPE`.)

- [Building and running](#building-and-running)
- [Semantics: pipefail and the ends](#semantics-pipefail-and-the-ends)
- [Merging a stage's stderr into the pipe](#merging-a-stages-stderr-into-the-pipe)
- [Unchecked stages](#unchecked-stages)
- [Timeouts](#timeouts)
- [Streaming a live chain](#streaming-a-live-chain)
- [Re-running a pipeline](#re-running-a-pipeline)

## Building and running

`Command::pipe(next)` starts a `Pipeline`; chain more stages with
`Pipeline::pipe`; drive it with `output_string()` or `run()`:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // git log --format=%an | sort | uniq -c
    let authors = Command::new("git").args(["log", "--format=%an"])
        .pipe(Command::new("sort"))
        .pipe(Command::new("uniq").arg("-c"))
        .run()                         // require every stage to succeed
        .await?;
    println!("{authors}");
    Ok(())
}
```

The verbs mirror `Command`'s, each operating on the pipefail outcome:

| Verb | Returns | A failing stage is… |
|---|---|---|
| `output_string()` | `ProcessResult<String>` | …reported in the result (code/stderr/program of the first unclean stage) |
| `output_bytes()` | `ProcessResult<Vec<u8>>` | …same, with the last stage's stdout captured raw (binary pipes) |
| `run()` | trimmed final stdout | …raised as that stage's `ErrorReason::Exit`; fails loud on a truncated capture |
| `checked()` | full `ProcessResult<String>` | …raised as `ErrorReason::Exit` (untrimmed stdout) |
| `run_unit()` | `()` | …raised as `ErrorReason::Exit` (output discarded) |
| `exit_code()` | `i32` | …its attributed code (no code → `ErrorReason::Timeout`/`Signalled`) |
| `probe()` | `bool` | `0` → `true`, `1` → `false`, else `Err` |
| `parse(\|s\| …)` / `try_parse(\|s\| …)` | `T` | …raised as `ErrorReason::Exit`; fails loud on a truncated capture |

`Err` from `output_string` itself means a stage couldn't be *started or
driven* at all (spawn failure, broken plumbing) — never a mere non-zero exit.

The `first_line` probe is deliberately not a *buffering* pipeline verb: those
verbs consume the last stage in full to fold the pipefail outcome. To *capture*
the first matching line of a finished chain, add a `| head -n1` (Unix) / `grep
-m1` / `findstr` stage and capture. To instead **read a chain that keeps
running** — wait for a banner line, then stream the rest — use
[`Pipeline::start()`](#streaming-a-live-chain), which gives a chain the same live
streaming surface a single `Command::start()` gives a process.

The `|` operator is sugar for the same thing — `a | b | c` ≡
`a.pipe(b).pipe(c)`. Parenthesize the chain before a terminal verb, since
method calls bind tighter than `|`:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let authors = (Command::new("git").args(["log", "--format=%an"])
        | Command::new("sort")
        | Command::new("uniq").arg("-c"))
        .run()
        .await?;
    Ok(())
}
```

## Semantics: pipefail and the ends

The outcome is **pipefail**, like `set -o pipefail` in a shell:

- `stdout` is always the **last** stage's output — that's what the chain
  produced.
- `code`, `stderr`, and the reported program come from the **culprit** stage:
  the leftmost stage that didn't exit cleanly (non-zero, signal-killed, or timed
  out), but **preferring a real failure over a downstream `SIGPIPE` victim** — a
  stage killed only because a later stage closed the pipe early. If every failure
  is such a broken-pipe victim, the leftmost one wins; when every stage succeeded,
  the last stage speaks.

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("cat").arg("data.txt")
        .pipe(Command::new("grep").arg("ERROR"))      // suppose grep exits 2 (bad pattern)
        .pipe(Command::new("wc").arg("-l"))
        .output_string()
        .await?;

    // Diagnostics point at grep — the first unclean stage — while stdout is
    // whatever wc managed to print:
    assert_eq!(result.code(), Some(2));
    println!("blamed: {}", result.ensure_success().unwrap_err()); // names `grep`
    Ok(())
}
```

**Failure tears the chain down proactively.** The moment a stage ends with a
checked failure (a non-zero exit outside its `ok_codes`, a signal kill, or its
own per-stage timeout), every stage's sub-group is torn down at once — the failure does
not wait to trickle out through closing pipes. This matters for a *quiet*
sibling that would otherwise hang: an upstream producer that never writes never
dies of a broken pipe, so under a purely passive teardown a downstream failure
could be held open indefinitely by that silent producer. Now the failure
surfaces immediately, and the killed siblings are treated as victims (like a
downstream `SIGPIPE` death) — the stage that actually failed keeps the blame.
The one death that does **not** trigger this is an
[`unchecked_in_pipe()`](#unchecked-stages) stage's: its unclean exit is forgiven,
so it leaves the rest of the chain running. (A stuck stage that never *fails* —
a healthy producer that simply never finishes — is still bounded only by
[`Pipeline::timeout`](#timeouts) or [cancellation](#timeouts).)

The ends of the chain behave like a single `Command`:

- The **first** stage's configured [`stdin`](commands.md#standard-input)
  source is honored — feed the whole pipeline from a string, file, or stream.
- **Inner** stages read from the pipe, full stop: any `stdin` source or
  `keep_stdin_open` configured on them is overridden.
- **Inner** stages likewise *write* to the pipe, full stop — see below.
- The **last** stage's stdout is the chain's own output and is honored as configured
  when that destination is supported (`stdout(Null)`, `stdout_file(..)` and friends
  behave as they do on a standalone non-PTY command).
- Inner stages' **stderr** is captured per-stage for pipefail diagnostics;
  only the last stage's stdout reaches you. A stage explicitly marked with
  `merge_stderr_in_pipe()` is the exception described below.

```rust,no_run
use processkit::{Command, Stdin};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let unique_count = Command::new("sort")
        .stdin(Stdin::from_iter_lines(["b", "a", "b", "c"]))
        .pipe(Command::new("uniq"))
        .pipe(Command::new("wc").arg("-l"))
        .run()
        .await?;
    assert_eq!(unique_count.trim(), "3");
    Ok(())
}
```

### A non-final stage's stdout belongs to the pipe

Because the link between two stages is unconditional, the chain — not the stage —
owns a non-final stage's stdout. If such a stage carries `stdout(StdioMode::Null)`,
`stdout(StdioMode::Inherit)`, or a
[file redirect](commands.md#redirecting-output-directly-to-a-file),
**the pipe wins and that configuration goes inert for the run**:

- the stage's bytes go to the next stage and nowhere else — not to `/dev/null`,
  not to the parent's terminal, not to the file;
- a configured redirect file is **neither created nor truncated**, so a log a
  previous run left there is left intact;
- the stage's stdout observers (`on_stdout_line`, `stdout_tee`, `stdout_raw_tee`)
  do not fire — as they do not on *any* non-final stage, whose stdout goes to the
  next stage rather than through a pump that could run them;
- **stderr is untouched**: it keeps its own configuration and stays available for
  pipefail diagnostics.

This is the same precedence inner-stage *stdin* has had all along, applied to the
other end of the same relay, and it is what keeps a `Command` that carries a
redirect for standalone use reusable as a pipeline stage. The alternative — treating
the combination as a configuration error — would reject such a stage before spawn;
what neither does is quietly drop the producer's output and start the next stage on
an empty stdin.

The one non-final stdout configuration that *is* rejected rather than overridden is
`use_pty()`: a PTY master carries a merged terminal stream that cannot feed a later
stage at all, so the chain fails before spawn with `ErrorReason::Unsupported`.
The final stage may use a PTY only with its supported merged `Piped` stdout and
`Piped` stderr destination; `Inherit`, `Null`, and file redirects are rejected.
The pipeline checks every PTY stage's destination before it starts any stage, so
an invalid final-stage destination cannot run an upstream command or open a
redirect file first. A non-final PTY retains the more specific pipeline-topology
error because it cannot provide the next stage's stdin pipe at all.

## Merging a stage's stderr into the pipe

Mark a non-final stage with `merge_stderr_in_pipe()` for the shell-free
equivalent of `command 2>&1 | next`:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Feed both compiler diagnostics and ordinary output to the filter.
    let matches = Command::new("cargo").args(["check", "--message-format=short"])
        .merge_stderr_in_pipe()
        .pipe(Command::new("grep").arg("warning"))
        .output_string()
        .await?;
    println!("{}", matches.stdout());
    Ok(())
}
```

The child receives two cloned handles to the **same anonymous-pipe writer** for
stdout and stderr. Their bytes therefore enter one OS pipe in the order the
child writes them; processkit does not merge two independently-read streams in
userspace. The normal pipeline boundary still relays that one reader into the
next stage's stdin, as described at the top of this page.

The marker is opt-in per stage and only applies when that stage has a downstream
neighbor. It is a no-op on a standalone command and on the final pipeline stage.
For an affected stage, the downstream pipe overrides its configured stdout and
stderr destinations. The shared anonymous-pipe wiring is supported on Unix and
Windows; activating it on another target fails before spawn with
`ErrorReason::Unsupported`.

This changes pipefail diagnostics deliberately: the merged stage no longer has
a separate stderr capture. If it is blamed for a failure, the result's `stderr`
is empty; its diagnostic bytes may instead be present in the final stdout after
flowing through the remaining stages. Leave the marker off when retaining the
culprit stage's dedicated stderr is more important than filtering the combined
stream.

## Unchecked stages

Strict pipefail has one classic false positive: a consumer that legitimately
stops reading early. In `producer | head -1` the consumer exits `0` after one
line and closes the pipe; the producer then stops on a **broken pipe** — its
next write fails once the relay's downstream is gone (a broken-pipe write error,
or `SIGPIPE` where the OS delivers it) — a perfectly normal death that strict
pipefail would blame the chain for. Mark that stage [`unchecked_in_pipe()`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.unchecked_in_pipe):

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // seq 1 1000000 | head -1 — the producer's broken-pipe death is expected.
    let first = (Command::new("seq").args(["1", "1000000"]).unchecked_in_pipe()
        | Command::new("head").args(["-n", "1"]))
        .run()
        .await?;
    assert_eq!(first.trim(), "1");
    Ok(())
}
```

The rules (a design borrowed from `duct`'s `unchecked()` — the idea, not the
code):

- An unchecked stage's unclean exit — a non-zero code, a broken-pipe write
  failure (or `SIGPIPE` where the OS delivers it) from a consumer that closed
  early, or its own per-stage timeout kill — is **skipped** when the chain
  decides what to report.
- A **checked** failure always trumps an unchecked one, regardless of
  position: `unchecked` never shields another stage's real failure.
- A chain whose only failures are unchecked reports **success** with the last
  stage's stdout and its **real** exit code preserved (not a fabricated `0` — the
  accepted-code set is widened to include it). The carve-out is for an exit
  *status* only: a last stage killed by a **signal** or its own **timeout** is not
  a status to forgive and still surfaces as the failure.
- `unchecked` forgives exit *status* only — never a whole-chain
  [`Pipeline::timeout`](#timeouts), and it has no effect on a `Command` run
  outside a pipeline (a single run's status is already plain data in its
  `ProcessResult`).

## Timeouts

Two scopes, deliberately distinct:

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("producer")
        .timeout(Duration::from_secs(10))      // per-STAGE: kills just `producer`
        .pipe(Command::new("consumer"))
        .timeout(Duration::from_secs(30))      // whole-CHAIN: Pipeline::timeout
        .output_string()
        .await?;
    Ok(())
}
```

- **`Pipeline::timeout`** bounds the whole chain: at the deadline every
  stage's sub-group is torn down and the result reports `timed_out`. The
  result keeps best-effort stdout and stderr already captured by the final
  stage before teardown, subject to the same buffer/truncation policy as a
  normal capture.
- A **per-stage `Command::timeout`** kills that stage's *whole subtree* — its
  own sub-group, grandchildren of a forking `sh -c …` included, not just its
  direct child. Every stage is evaluated by the same pipefail rule (D14): a
  stage that hit its own deadline — inner *or* last — surfaces on `run()` as
  that stage's `ErrorReason::Timeout`, reporting **that stage's own deadline** (not
  the chain's, and never `0ns`).

Cancellation has two forms. **`Pipeline::cancel_on(token)`** is the chain-level
control: the token **gap-fills** into every stage that doesn't already carry its
own `Command::cancel_on` (an explicit per-stage token is left intact), so firing
it tears the whole chain down and the run resolves to `ErrorReason::Cancelled`. (A
`cancel_on` token on an individual stage `Command` also cancels that stage and
errors the pipeline, but
the pipeline-level builder is the clearer authority.) See
[Timeouts & cancellation](timeouts-and-cancellation.md).

## Streaming a live chain

The verbs above buffer the whole run. For a long-lived chain you read *from*
rather than wait *out* — `journalctl -f | grep ERROR`, `tail -F access.log | jq`
— `Pipeline::start()` returns a `PipelineSession`: the multi-stage analogue of
the [`RunningProcess`](streaming.md) a single `Command::start()` gives you. It
streams the **last** stage's stdout as it arrives while every inner stage drains
in the background, then folds the same pipefail outcome at `finish()`.

```rust,no_run
use processkit::prelude::StreamExt;
use processkit::{Command, Finished, Outcome};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // journalctl -f | grep --line-buffered ERROR
    let mut session = Command::new("journalctl").arg("-f")
        .pipe(Command::new("grep").args(["--line-buffered", "ERROR"]))
        .start()
        .await?;

    let mut lines = session.stdout_lines()?;   // the *last* stage's stdout, live
    while let Some(line) = lines.next().await {
        println!("error: {line}");
        // …break out when you've seen enough, then tear the chain down…
        break;
    }
    drop(lines);

    // Fold the pipefail outcome. The last stage's stdout was already streamed,
    // so — like RunningProcess::finish — none is re-bundled; `outcome` and
    // `stderr` come from the pipefail-attributed (culprit) stage.
    session.start_kill()?;                      // stop the whole chain now
    let Finished { outcome, stderr, .. } = session.finish().await?;
    if outcome != Outcome::Exited(0) {
        eprintln!("chain ended {outcome:?}: {stderr}");
    }
    Ok(())
}
```

The session mirrors `RunningProcess`:

- **`stdout_lines()` / `events()`** — the last stage's stdout (lines) or its full
  lifecycle (`Started` → interleaved stdout+stderr → `Exited`), each
  **consume-once** (a second take is a loud `Err`, never a silently-empty stream).
- **`wait_for_line(pred, within)`** — wait for a readiness banner on that stream
  without tearing the chain down (an `ErrorReason::NotReady` on timeout, like the
  single-process probe).
- **`finish()`** — the streaming analogue of `output_string()`: the
  pipefail-attributed stage's `outcome` and *its own* `stderr` in a `Finished`
  (no stdout — you already streamed it). A chain-wide `Pipeline::timeout` that
  elapsed reports `Outcome::TimedOut`; a `Pipeline::cancel_on` that fired while the
  chain was still running surfaces as `ErrorReason::Cancelled` — exactly as in the
  buffering verbs. As for a single `Command::cancel_on`, that disposition is
  **first-observation-wins**: every stage is observed while the session is live
  (each inner stage by its background drain, the last stage by the session's own
  watcher), so a token fired *after* the chain had already ended does not rewrite
  its real outcome into `Cancelled`, even when `finish()` is called afterwards.
- **`start_kill()`** and **kill-on-drop** — stop the whole chain now, or drop the
  session and every stage's tree dies. The no-orphan invariant holds for a live
  chain (including a partially-started one) just as it does for a single process.

Whole-chain teardown still applies while streaming: **any** stage's checked
failure proactively tears the chain down (so a quiet upstream can't hold a failed
live chain open), and the chain-wide [timeout / cancellation](#timeouts) bounds
the session regardless of which stage is slow. That includes the **last** stage,
and without waiting for `finish()` — a standing watcher observes it for a terminal
outcome, so holding an unfinished session is not a way for a failed chain's
upstream to keep running. The teardown is prompt rather than instantaneous: it
lands within one probe interval of the last stage's failure plus the short drain
grace. A last stage that exits *cleanly* deliberately fires no teardown — a chain
whose stages all succeed is not a failed chain.

## Re-running a pipeline

A `Pipeline` is `Clone` and re-runnable — stages are re-cloned per run. The
one caveat is inherited from `Command`: a **one-shot** stdin source on the
first stage (`Stdin::from_reader` / `from_lines`) is consumed by the first run;
re-running then **fails loud** (an `ErrorReason::Io` at launch, D10) rather than
silently feeding empty stdin. Use the reusable sources
(`from_string` / `from_bytes` / `from_iter_lines` / `from_file`) when a chain
runs more than once.

---

Next: [Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Running commands](commands.md) ·
[Process groups](process-groups.md)
