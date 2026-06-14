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
  inspecting.

- [Timeouts](#timeouts)
- [Retries](#retries)
- [Cancellation](#cancellation)
- [Precedence and interactions](#precedence-and-interactions)

## Timeouts

`Command::timeout(d)` kills the **whole process tree** at the deadline — not
just the direct child, so a wrapper script's grandchildren die too.

```rust,no_run
use processkit::Command;
use std::time::Duration;

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
assert!(matches!(err, processkit::Error::Timeout { .. }));
```

Where each verb lands:

| Verb | Deadline expiry becomes |
|---|---|
| `output_string()` / `output_bytes()` | `Ok` result with `timed_out() == true`, `code() == None`, partial output kept |
| `run()` / `exit_code()` / `probe()` / `checked()` | `Error::Timeout { program, timeout, stdout, stderr }` — the partial output captured before the kill is attached (`err.diagnostic()` surfaces a hung tool's last words) |
| `first_line(pred)` | `Error::Timeout` (the line never arrived in time) |
| `start()` + streaming | the stream **ends** at the deadline (tree killed, pipes closed); `finish` then reports the kill (`outcome == Outcome::TimedOut`) |
| `ensure_success()` on a captured result | `Error::Timeout`, checked *before* the exit code |
| [`Pipeline`](pipelines.md#timeouts) | chain deadline → `timed_out` result; per-stage deadlines fold into pipefail |

Two distinct deadline families to keep apart:

- `Command::timeout` — the run's own contract, this section.
- The [readiness probes](streaming.md#readiness-probes)' `within` parameter —
  gives `Error::NotReady` and **never kills the child**.

### Graceful timeout

By default the deadline **hard-kills** at once. Add `timeout_grace(d)` to give the
tree a chance to clean up: at the deadline it is sent `SIGTERM` (or the signal chosen
with `timeout_signal`, which needs the `process-control` feature), allowed up to the
grace window to exit, then `SIGKILL`ed — the same SIGTERM → wait → SIGKILL tier as
[`ProcessGroup::shutdown`](process-groups.md). A signal-handling child that exits ends
the grace early.

```rust,no_run
use processkit::Command;
use std::time::Duration;

let result = Command::new("slow-tool")
    .timeout(Duration::from_secs(30))
    .timeout_grace(Duration::from_secs(5)) // SIGTERM, wait up to 5s, then SIGKILL
    .output_string()
    .await?;
```

`timed_out()` is `true` regardless of whether the child exited on the signal or was
`SIGKILL`ed after the grace — the deadline is what fired. **Windows** has no signal
tier: `timeout_grace` is accepted but the deadline kills the job atomically.

The explicit [`RunningProcess::shutdown(grace)`](streaming.md) verb (stop a started
handle on demand) composes with a `Command::timeout`: its own SIGTERM → grace →
SIGKILL is the **single** teardown (it does not also fire the run's timeout
teardown), and if the deadline has **already elapsed** when you call `shutdown`,
the outcome is reported as `Outcome::TimedOut` — the `grace` you pass governs the
teardown timing.

## Retries

`retry(max_attempts, backoff, classifier)` replays a failed run — up to
`max_attempts` **total** attempts, sleeping `backoff` between tries, retrying
only while the classifier accepts the error:

```rust,no_run
use processkit::{Command, Error};
use std::time::Duration;

let out = Command::new("curl")
    .args(["-fsS", "https://example.com/api"])
    .timeout(Duration::from_secs(10))
    .retry(3, Duration::from_millis(250), |e| {
        // transient: network timeouts and curl's "couldn't connect" (7)
        matches!(e, Error::Timeout { .. })
            || matches!(e, Error::Exit { code: 7, .. })
    })
    .run()
    .await?;
```

Ground rules:

- Retries apply to the **success-checking** paths only (`run`, `exit_code`,
  `probe`, `ProcessRunnerExt::checked` — and everything built on them, e.g.
  `CliClient`). The non-erroring `output_string` capture never retries: it
  didn't fail.
- The classifier sees the typed error — match on variants, codes, even the
  captured stderr.
- Each attempt re-runs the *same* `Command`: a one-shot stdin source
  ([table](commands.md#standard-input)) is consumed by attempt #1, so attempt #2
  **fails loud** with an `Error::Io` (`InvalidInput`) at launch (D10) rather than
  silently feeding empty stdin. Use reusable sources for retried commands.
- A `Cancelled` error is **never retried**, classifier or not — the token
  stays cancelled forever, so another attempt could only fail the same way.

For "keep it alive" (restart a *service* whenever it exits) rather than
"replay this one operation", use a [`Supervisor`](supervision.md) — same
backoff shape, different loop condition.

## Cancellation

Hand any command a `CancellationToken` (re-exported at the crate root);
cancelling the token kills the run's tree and makes every consuming path
report `Error::Cancelled`:

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
        job.await.unwrap(),
        Err(processkit::Error::Cancelled { .. })
    ));
    Ok(())
}
```

The contract, path by path:

| Situation | Behavior |
|---|---|
| Cancel during `run` / `output_string` / `output_bytes` / `wait` / `profile` / `exit_code` / `probe` | tree killed, `Error::Cancelled { program }` |
| Cancel during streaming (`stdout_lines`) | the stream **ends**; the following `finish` reports `Error::Cancelled` |
| Token already cancelled before the run | short-circuits **before spawning** — no process is ever created |
| Cancel on a shared-`ProcessGroup` handle | kills the child itself, leaves the group's siblings alone (same scope as a timeout) |
| A `Pipeline` stage's token cancels | that stage dies; the cancellation errors the whole pipeline and the private group reaps the other stages |
| Under `retry` | terminal — never retried, whatever the classifier says |
| Under a [`Supervisor`](supervision.md) | terminal — supervision returns `Err(Cancelled)` instead of restarting into a still-cancelled token |
| `wait_any` mid-run | the raw primitive doesn't synthesize the error — the race just resolves (a *pre-cancelled* token still hits the pre-spawn short-circuit) |
| `first_line` mid-run | surfaces `Error::Cancelled` once the token fires — a cancelled stream that closes without a match is reported as cancellation, not `Ok(None)` |

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
// dies (whole tree), surfacing Error::Cancelled to the awaiting call.
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

let token = CancellationToken::new();
token.cancel();

let err = Command::new("tool")
    .timeout(Duration::from_millis(1))   // would have been a Timeout…
    .cancel_on(token)                    // …but cancellation takes priority
    .run()
    .await
    .unwrap_err();
assert!(matches!(err, processkit::Error::Cancelled { .. }));
```

**Which knob for which job:**

| You want | Reach for |
|---|---|
| "This run may not take longer than X" | `Command::timeout` |
| "This operation is flaky, try a few times" | `Command::retry` |
| "Stop everything when the app shuts down" | `cancel_on` + one shared token |
| "Keep this service alive across crashes" | [`Supervisor`](supervision.md) |
| "Tell me when it's *ready*, don't kill it" | [readiness probes](streaming.md#readiness-probes) |

---

Next: [Supervision](supervision.md) ·
[Streaming & interactive I/O](streaming.md) ·
[Running commands](commands.md)
