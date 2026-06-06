# Supervision

[‹ docs index](README.md)

Where [`retry`](timeouts-and-cancellation.md#retries) answers *"run this once,
replaying on failure"*, a `Supervisor` answers the different question *"keep
this alive"*: restart a child per policy whenever it exits, with bounded
restarts, exponential backoff, and jitter — a minimal `runit`/`systemd`-style
keeper, platform-agnostic because it sits entirely on the
[`ProcessRunner` seam](testing.md#the-processrunner-seam).

- [The shape](#the-shape)
- [Policies: what counts as a crash](#policies-what-counts-as-a-crash)
- [Backoff and jitter](#backoff-and-jitter)
- [Stopping](#stopping)
- [Outcomes](#outcomes)
- [Supervising inside a shared group](#supervising-inside-a-shared-group)
- [Errors and cancellation](#errors-and-cancellation)

## The shape

```rust,no_run
use processkit::{Command, RestartPolicy, Supervisor};
use std::time::Duration;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let outcome = Supervisor::new(Command::new("my-server").args(["--port", "8080"]))
        .restart(RestartPolicy::OnCrash)           // default
        .max_restarts(5)                           // default: unlimited
        .backoff(Duration::from_millis(200), 2.0)  // default: 200ms × 2.0
        .max_backoff(Duration::from_secs(30))      // default: 30s cap
        .jitter(true)                              // default: on
        .stop_when(|res| res.code() == Some(0))    // optional exit condition
        .run()
        .await?;

    println!(
        "ended after {} restarts, reason: {:?}, last exit: {:?}",
        outcome.restarts, outcome.stopped, outcome.final_result.code(),
    );
    Ok(())
}
```

Each *incarnation* is one full captured run of the command (so the command's
own `timeout`, stdin, env, … all apply per run — with the usual
[one-shot-stdin caveat](commands.md#standard-input) for the second run
onward).

## Policies: what counts as a crash

A **crash** is any run without a clean exit: a non-zero code, a timeout, a
signal-kill, or a spawn failure.

| `RestartPolicy` | Restarts after… |
|---|---|
| `OnCrash` *(default)* | crashes only; a clean exit ends supervision (`PolicySatisfied`) |
| `Always` | every completed run, clean or not — pair it with `stop_when`/`max_restarts` or it loops forever |
| `Never` | nothing: one run, reported as-is |

## Backoff and jitter

The *n*-th restart (0-based) sleeps

```text
delay(n) = min(base × factor^n, max_backoff) × jitter
```

with `jitter` drawn uniformly from `[0.5, 1.5)` per restart. Jitter is **on by
default** so a fleet of supervised workers restarted by the same incident
doesn't stampede back in lockstep; `jitter(false)` gives deterministic delays
(useful in tests with a paused tokio clock). A non-finite or `< 1.0` factor is
treated as `1.0` — constant delay, never a shrinking one.

```text
base=200ms, factor=2.0, cap=30s:
restart #0 → ~200ms   #1 → ~400ms   #2 → ~800ms … #7 → ~25.6s   #8+ → 30s (cap)
```

## Stopping

Three gates, checked in this order after every completed run:

1. **`stop_when(predicate)`** — sees the run's `ProcessResult`; returning
   `true` ends supervision *regardless of policy* (→ `StopReason::Predicate`).
   "Exit 0 is done, anything else is a crash" is the classic:
   `stop_when(|res| res.code() == Some(0))` under `RestartPolicy::Always`.
2. **The policy** — `OnCrash` stops on a clean exit (→ `PolicySatisfied`).
3. **`max_restarts(n)`** — at most *n* restarts = *n + 1* total runs; an
   exhausted budget reports the last result (→ `RestartsExhausted`).
   `max_restarts(0)` means exactly one run.

## Outcomes

`run()` resolves to a `SupervisionOutcome`:

```rust,no_run
let outcome = Supervisor::new(Command::new("job")).run().await?;

outcome.final_result; // ProcessResult<String> of the LAST run
outcome.restarts;     // how many restarts happened (not counting run #1)
outcome.stopped;      // StopReason::{Predicate, PolicySatisfied, RestartsExhausted}
```

Note `run()` returning `Ok` does **not** mean the child succeeded — it means
supervision *concluded*. Inspect `final_result` (or `ensure_success()` it) for
the child's own verdict.

## Supervising inside a shared group

The supervisor runs through any `ProcessRunner`. The headline production
variant injects a [`ProcessGroup`](process-groups.md) so every incarnation —
and everything it spawns — lives in one kill-on-drop container:

```rust,no_run
use processkit::{Command, ProcessGroup, RestartPolicy, Supervisor};

let group = ProcessGroup::new()?;

let outcome = Supervisor::new(Command::new("worker"))
    .with_runner(&group)                 // &group is itself a ProcessRunner
    .restart(RestartPolicy::OnCrash)
    .max_restarts(10)
    .run()
    .await?;

// The group outlives supervision: drop it (or shutdown) to reap any strays.
```

Mind one interaction: don't supervise into a group you've
[suspended](process-groups.md#suspending-and-resuming) — under the cgroup
mechanism the restarted child would start frozen (and the spawn itself can
block). Resume first.

The same injection point makes supervision logic **hermetically testable** —
script a sequence of fake results and assert the restart/stop behavior with
no real process; see [Testing your code](testing.md#scripting-replies).

## Errors and cancellation

A run that produces no result at all (spawn/IO failure) can't be judged by
`stop_when`; the policy treats it as a crash and restarts (with backoff)
unless the policy is `Never` or the budget is exhausted — then the error
itself surfaces as `run()`'s `Err`.

With the [`cancellation` feature](timeouts-and-cancellation.md#cancellation),
a cancelled incarnation is **terminal**: `run()` returns
`Err(Error::Cancelled)` immediately. The token never un-cancels, so a restart
could only produce another instantly-cancelled run — the supervisor refuses
the futile loop.

---

Next: [Testing your code](testing.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Process groups](process-groups.md)
