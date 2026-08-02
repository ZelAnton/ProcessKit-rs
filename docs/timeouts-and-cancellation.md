# Timeouts, retries & cancellation

[‹ docs index](README.md)

Three ways a run ends early, with three different philosophies:

- a **timeout** is *data* — the deadline was part of the run's contract, so
  its expiry is captured in the result (and only the success-checking verbs
  turn it into an error);
- a **retry** is a *policy* — the success-checking verbs replay the run while
  your classifier says the failure is transient;
- a **cancellation** is an *abandonment* — the caller
  changed its mind, so every path reports an error; there is no result worth
  inspecting. (The abandonment is about the *outcome*, not the manner: the
  goodbye can be made soft with [`cancel_grace`](#graceful-cancellation), and it
  is still always an error.)

- [Timeouts](#timeouts)
- [Retries](#retries)
- [Cancellation](#cancellation)
- [Precedence and interactions](#precedence-and-interactions)

## Timeouts

`Command::timeout(d)` kills the **whole process tree** at the absolute deadline —
not just the direct child, so a wrapper script's grandchildren die too.

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Captured: inspect the flag yourself.
    let result = Command::new("slow-tool")
        .timeout(Duration::from_secs(5))
        .output_string()
        .await?;
    if result.timed_out() {
        println!("partial output before the kill: {}", result.stdout());
    }

    // Raised: the checking verbs convert the flag into a typed error.
    let err = Command::new("slow-tool")
        .timeout(Duration::from_secs(5))
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err.reason(), processkit::ErrorReason::Timeout { .. }));
    Ok(())
}
```

Where each verb lands:

| Verb | Deadline expiry becomes |
|---|---|
| `output_string()` / `output_bytes()` | `Ok` result with `timed_out() == true`, `code() == None`, partial output kept |
| `run()` / `exit_code()` / `probe()` / `checked()` | `ErrorReason::Timeout { program, timeout, inactivity, stdout, stderr, .. }` — the partial output captured before the kill is attached (`err.diagnostic()` surfaces a hung tool's last words) |
| `first_line(pred)` | `ErrorReason::Timeout` (the line never arrived in time) |
| `start()` + streaming | the stream **ends** at the deadline (tree killed, pipes closed); `finish` then reports the kill (`outcome == Outcome::TimedOut`) |
| `ensure_success()` on a captured result | `ErrorReason::Timeout`, checked *before* the exit code |
| [`Pipeline`](pipelines.md#timeouts) | chain deadline → `timed_out` result; per-stage deadlines fold into pipefail |

### Output-inactivity watchdog

`Command::inactivity_timeout(d)` kills a single run when neither stdout nor
stderr has produced bytes for `d`. Its clock starts at spawn and resets on every
successful read from either stream, including merged PTY output:

```rust,no_run
use processkit::{Command, Outcome};
use std::time::Duration;

# #[tokio::main]
# async fn main() -> processkit::Result<()> {
let result = Command::new("build-tool")
    .timeout(Duration::from_secs(30 * 60))
    .inactivity_timeout(Duration::from_secs(5 * 60))
    .output_string()
    .await?;

if result.outcome() == Outcome::InactivityTimedOut {
    eprintln!("build produced no output for five minutes");
}
# Ok(())
# }
```

The first watchdog to fire wins. Both use the same whole-tree teardown and honor
`timeout_grace` / `timeout_signal`, but their results remain distinct:
`Outcome::TimedOut` means the absolute runtime expired;
`Outcome::InactivityTimedOut` means the output went quiet. `timed_out()` is true
for either; `inactivity_timed_out()` identifies the latter. Checking verbs turn
both into `ErrorReason::Timeout`; its `inactivity` field carries the distinction
and its `timeout` field is the window that fired.

This first iteration applies to individual command runs, including capture,
streaming, first-line consumption, and PTY mode. Readiness probes drain output
and therefore refresh the activity clock, but do not arm either command
watchdog themselves. Pipeline-wide and supervisor-wide inactivity policies are
separate concerns; commands they launch still keep their own configured
watchdog.

Two distinct deadline families to keep apart:

- `Command::timeout` / `Command::inactivity_timeout` — the run's own contracts,
  this section.
- The [readiness probes](streaming.md#readiness-probes)' `within` parameter —
  gives `ErrorReason::NotReady` and **never kills the child**.

### Graceful timeout

By default a watchdog **hard-kills** at once. Add `timeout_grace(d)` to give the
tree a chance to clean up: when it fires the tree is sent `SIGTERM` (or the signal chosen
with `timeout_signal`, which needs the `process-control` feature), allowed up to the
grace window to exit, then `SIGKILL`ed — the same SIGTERM → wait → SIGKILL tier as
[`ProcessGroup::shutdown`](process-groups.md). A signal-handling child that exits ends
the grace early.

```rust,no_run
use processkit::Command;
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let result = Command::new("slow-tool")
        .timeout(Duration::from_secs(30))
        .timeout_grace(Duration::from_secs(5)) // SIGTERM, wait up to 5s, then SIGKILL
        .output_string()
        .await?;
    Ok(())
}
```

`timed_out()` is `true` regardless of whether the child exited on the signal or was
`SIGKILL`ed after the grace — the deadline is what fired. **Windows** has no signal
tier by default: `timeout_grace` is accepted but the deadline kills the job
atomically. To get a real soft-shutdown window there, add
[`windows_graceful_ctrl_break()`](https://docs.rs/processkit/latest/processkit/struct.Command.html#method.windows_graceful_ctrl_break):
the direct child is spawned in its own console process group and, at the deadline,
sent a console `CTRL_BREAK` before the grace window, then `TerminateJobObject`'d if
it hasn't exited — the same `timeout_grace` window now actually meaning something on
Windows. It works only for a child that shares this process's console (a
`create_no_window` / `DETACHED_PROCESS` child never receives the event and rides
the grace to the hard kill), and sends `CTRL_BREAK` rather than the Unix
`timeout_signal`. See [Process groups → Windows opt-in](process-groups.md#windows-the-graceful-soft-tier-wm_close-opt-in-ctrl_break).

The explicit [`RunningProcess::shutdown(grace)`](streaming.md) verb (stop a started
handle on demand) composes with a `Command::timeout`: its own SIGTERM → grace →
SIGKILL is the **single** teardown (it does not also fire the run's timeout
teardown), and if the deadline has **already elapsed** when you call `shutdown`,
the outcome is reported as `Outcome::TimedOut` — the `grace` you pass governs the
teardown timing.

#### Observing the grace window

With the `tracing` feature on, the teardown driver narrates each grace-window
transition **live** on the `processkit` target — `soft_signal` (the SIGTERM /
CTRL_BREAK was issued), `grace_started` (the wait began, with `grace_ms`), and then
one of `drained` (the tree exited in time), `escalated` (the grace elapsed and the
tree was hard-killed), or `spared` (a non-escalating stop left survivors) — each
carried in a stable `phase` field and stamped by your subscriber at the instant it
happens. This is the same soft-signal → grace → drain/kill ladder every graceful
path drives (`timeout_grace`, [`cancel_grace`](#graceful-cancellation),
`RunningProcess::shutdown`, `ProcessGroup::stop`), so
you get one uniform timeline whichever verb fired it. For the same facts *after* the
teardown returns — as a typed value rather than log events — reach for
[`ProcessGroup::stop`'s `ShutdownReport`](process-groups.md#observing-the-teardown-stop-and-shutdownreport).
Neither is a control surface: both are observation only, and never carry argv/env.

## Retries

`retry(max_attempts, backoff, classifier)` replays a failed run — up to
`max_attempts` **total** attempts, sleeping `backoff` between tries, retrying
only while the classifier accepts the error:

```rust,no_run
use processkit::{Command, ErrorReason};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let out = Command::new("curl")
        .args(["-fsS", "https://example.com/api"])
        .timeout(Duration::from_secs(10))
        .retry(3, Duration::from_millis(250), |e| {
            // transient: network timeouts and curl's "couldn't connect" (7)
            matches!(e.reason(), ErrorReason::Timeout { .. })
                || matches!(e.reason(), ErrorReason::Exit { code: 7, .. })
        })
        .run()
        .await?;
    Ok(())
}
```

Ground rules:

- Retries apply to the **success-checking** paths only (`run`, `exit_code`,
  `probe`, `ProcessRunnerExt::checked` — and everything built on them, e.g.
  `CliClient`). The non-erroring `output_string` capture never retries: it
  didn't fail.
- The classifier sees the typed error — match on variants, codes, even the
  captured stderr.
- Each attempt re-runs the *same* `Command` — so a command whose stdin is a
  **one-shot** source ([table](commands.md#standard-input)), consumed by the
  first run, is **not retried at all**: the first attempt's error is returned
  as-is, since a second attempt could only replay empty stdin. Use a reusable
  stdin source if a stdin-bearing command must retry. (A one-shot source re-run
  *outside* the retry loop — a `Supervisor` incarnation, a pipeline re-run —
  instead fails loud with an `ErrorReason::Io` (`InvalidInput`) at launch.)
- A `Cancelled` error is **never retried**, classifier or not — the token
  stays cancelled forever, so another attempt could only fail the same way.

For "keep it alive" (restart a *service* whenever it exits) rather than
"replay this one operation", use a [`Supervisor`](supervision.md) — same
backoff shape, different loop condition.

## Cancellation

Hand any command a `CancellationToken` (re-exported at the crate root);
cancelling the token kills the run's tree and makes every consuming path
report `ErrorReason::Cancelled`:

```rust,no_run
use processkit::{CancellationToken, Command};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let shutdown = CancellationToken::new();

    // Wire the same parent token into many jobs via child tokens:
    let job = tokio::spawn({
        let token = shutdown.child_token();
        async move {
            Command::new("long-export").cancel_on(token).run().await
        }
    });

    // Ctrl-C handler, sibling failure, UI button, …
    shutdown.cancel();

    assert!(matches!(
        job.await.unwrap().map_err(|e| e.into_reason()),
        Err(processkit::ErrorReason::Cancelled { .. })
    ));
    Ok(())
}
```

The contract, path by path:

| Situation | Behavior |
|---|---|
| Cancel during `run` / `output_string` / `output_bytes` / `wait` / `profile` / `exit_code` / `probe` | tree killed, `ErrorReason::Cancelled { program }` |
| Cancel during streaming (`stdout_lines`) | the stream **ends**; the following `finish` reports `ErrorReason::Cancelled` |
| Token already cancelled before the run | short-circuits **before spawning** — no process is ever created |
| Cancel on a shared-`ProcessGroup` handle | kills the child itself, leaves the group's siblings alone (same scope as a timeout) |
| A `Pipeline` stage's token cancels | that stage dies; the cancellation errors the whole pipeline and the private group reaps the other stages |
| Under `retry` | terminal — never retried, whatever the classifier says |
| Under a [`Supervisor`](supervision.md) | terminal — supervision returns `Err(Cancelled)` instead of restarting into a still-cancelled token |
| `wait_any` mid-run | surfaces `Err(Cancelled)` — each racer's wait path resolves to `Cancelled` when its token fires, the same as a bulk verb (a *pre-cancelled* token still hits the pre-spawn short-circuit) |
| `first_line` mid-run | surfaces `ErrorReason::Cancelled` once the token fires — a cancelled stream that closes without a match is reported as cancellation, not `Ok(None)` |
| Teardown manner | hard kill by default; SIGTERM → grace → SIGKILL with [`cancel_grace`](#graceful-cancellation) (the outcome is `Cancelled` either way) |

### Graceful cancellation

By default a cancellation **hard-kills** at once — the mirror image of a bare
watchdog. Add `cancel_grace(d)` to give the tree a chance to clean up: when the
token fires the tree is sent `SIGTERM` (or the signal chosen with `cancel_signal`,
which needs the `process-control` feature), allowed up to the grace window to
exit, then `SIGKILL`ed. This is the exact same SIGTERM → wait → SIGKILL ladder as
[`timeout_grace`](#graceful-timeout) — the same driver, the same `phase`
observability — just fired by the token instead of the deadline. A
signal-handling child that exits ends the grace early.

```rust,no_run
use processkit::{CancellationToken, Command};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let shutdown = CancellationToken::new();

    let job = tokio::spawn({
        let token = shutdown.child_token();
        async move {
            Command::new("long-export")
                .cancel_on(token)
                // SIGTERM, wait up to 5s for a clean shutdown, then SIGKILL
                .cancel_grace(Duration::from_secs(5))
                .run()
                .await
        }
    });

    shutdown.cancel(); // Ctrl-C, sibling failure, …

    // Still an error — only the manner of the teardown changed.
    assert!(matches!(
        job.await.unwrap().map_err(|e| e.into_reason()),
        Err(processkit::ErrorReason::Cancelled { .. })
    ));
    Ok(())
}
```

This is the knob for the recommended "one shared token for the whole app"
shutdown pattern: without it, a Ctrl-C on the parent `SIGKILL`s every child
outright, with no chance to flush state, finish a transaction, or remove a
pidfile. `RunningProcess::shutdown(grace)` already covered that for handles you
started by hand; `cancel_grace` extends it to the bulk verbs, the streamed runs,
and the [`Supervisor`](supervision.md) — everything the token reaches.

Ground rules:

- **Opt-in, and inert by default.** Without `cancel_grace` every cancellation
  path behaves exactly as it always has (immediate hard kill). It also does
  nothing without a token.
- **The outcome never changes.** Whether the child exited on the soft signal or
  was killed after the grace, every consuming path still reports
  `ErrorReason::Cancelled` — cancellation is still an abandonment, still never
  retried, still terminal under `Supervisor`.
- **Independent of the timeout knobs.** `cancel_grace`/`cancel_signal` do not
  read (and are not filled in by) `timeout_grace`/`timeout_signal`, so a command
  can say farewell differently for "the caller changed its mind" than for "the
  deadline expired". `cancel_signal` defaults to `SIGTERM`, like `timeout_signal`.
- **Same teardown scope as any cancellation.** An own-group run tears down the
  whole tree; a shared-`ProcessGroup` run reaches only its direct child (its
  grandchildren remain the documented shared-group gap).
- **Windows** has no POSIX signal tier — exactly as for `timeout_grace`, the soft
  tier is the best-effort `WM_CLOSE` post plus the opt-in
  `windows_graceful_ctrl_break()` console event; a tree with neither is killed
  atomically and `grace` goes unused.
- **Cancel racing the deadline.** Cancellation still wins the race (the outcome is
  `Cancelled`, never `TimedOut`). Which teardown runs follows the *cancellation*
  policy: without `cancel_grace` that tie hard-kills, as it always did; with
  `cancel_grace` it takes the graceful ladder, because that is what the caller
  asked cancellation to do.

### Client-level default

A typed wrapper built on [`CliClient`](testing.md#wrapping-a-cli-tool) usually constructs
and consumes its `Command`s internally — there is no place to chain a
per-call `cancel_on`. Set the token **once on the client**; every command it
builds carries it:

```rust,no_run
use processkit::{CancellationToken, CliClient};

let token = CancellationToken::new();
let gh = CliClient::new("gh").default_cancel_on(token.child_token());
// ... controller cancels `token` → every in-flight command of THIS client
// dies (whole tree), surfacing ErrorReason::Cancelled to the awaiting call.
```

Clients are cheap — scope cancellation by building **one client per
cancellable scope** with its own (child) token, instead of threading tokens
through call signatures. `cli_client!`-generated wrappers re-emit the builder,
so `Git::new().default_cancel_on(t)` works for downstream crates too.

**Precedence:** a per-command `cancel_on` chained on a built command
*replaces* the client default (explicit beats default, like a per-command
`timeout` after `default_timeout`). To honor **both** sources, wire it
explicitly — `CancellationToken` has no built-in merge: derive a child of the
default (`let c = default.child_token()`), hand the command
`cancel_on(c.clone())`, and have the second source call `c.cancel()`. Or
simpler: build a dedicated client per scope.

## Precedence and interactions

**Timeout vs. cancellation.** A timeout is *captured*; a cancellation is
*always an error*. When both land on the same run, **cancellation wins** —
you asked the run to stop mattering, so no result is synthesized:

```rust,no_run
use processkit::{CancellationToken, Command};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let token = CancellationToken::new();
    token.cancel();

    let err = Command::new("tool")
        .timeout(Duration::from_millis(1))   // would have been a Timeout…
        .cancel_on(token)                    // …but cancellation takes priority
        .run()
        .await
        .unwrap_err();
    assert!(matches!(err.reason(), processkit::ErrorReason::Cancelled { .. }));
    Ok(())
}
```

**Which knob for which job:**

| You want | Reach for |
|---|---|
| "This run may not take longer than X" | `Command::timeout` |
| "This operation is flaky, try a few times" | `Command::retry` |
| "Stop everything when the app shuts down" | `cancel_on` + one shared token |
| "…and let them shut down cleanly first" | `cancel_grace` alongside it |
| "Keep this service alive across crashes" | [`Supervisor`](supervision.md) |
| "Tell me when it's *ready*, don't kill it" | [readiness probes](streaming.md#readiness-probes) |

---

Next: [Supervision](supervision.md) ·
[Streaming & interactive I/O](streaming.md) ·
[Running commands](commands.md)
