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
- [Failure storms](#failure-storms)
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

A **crash** is any run that is not a *success* (`ProcessResult::is_success`,
which honors the command's `ok_codes`): an exit code outside the accepted set
(default `{0}`), a timeout, a signal-kill, or a spawn failure. A command with
`ok_codes([0, 2])` that exits `2` is a success, so `OnCrash` treats it as clean,
not a crash.

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

## Failure storms

Backoff spaces *individual* restarts; `max_restarts` is a *lifetime* cap.
Neither distinguishes a service that fails once a day from one that is
suddenly crash-looping. The opt-in **storm guard** does (a design borrowed
from Go's [`suture`](https://github.com/thejerf/suture) supervisor — the
idea, not the code):

```rust,no_run
use processkit::{Command, Supervisor};
use std::time::Duration;

let outcome = Supervisor::new(Command::new("worker"))
    .storm_pause(Duration::from_secs(15))     // master switch — off by default
    .failure_decay(Duration::from_secs(30))   // score half-life (default 30s)
    .failure_threshold(5.0)                   // trip point (default 5.0)
    .run()
    .await?;

println!("storm pauses taken: {}", outcome.storm_pauses);
```

Each failed run adds `1` to a score that **halves every `failure_decay`**:

```text
score = score × 0.5^(Δt / failure_decay) + 1
```

- *Fails rarely*: the score decays back toward `1` between failures and never
  reaches the threshold — the guard stays out of the way.
- *Failure storm*: failures arrive faster than the half-life drains them, the
  score climbs past `failure_threshold`, and the supervisor takes **one
  collective pause** of `storm_pause` (jittered into `[0.5, 1.5)` like the
  backoff), resets the score, and resumes.

Only failures feed the score — crashes and spawn errors, not clean exits
restarted under `RestartPolicy::Always`. The pause stacks with (runs before)
the per-restart backoff, and the `max_restarts` budget is checked first, so a
storm pause never extends an exhausted budget. Pauses taken are reported in
`SupervisionOutcome::storm_pauses`.

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
outcome.storm_pauses; // failure-storm pauses taken (0 unless storm_pause is set)
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

A [cancelled](timeouts-and-cancellation.md#cancellation) incarnation is
**terminal**: `run()` returns
`Err(Error::Cancelled)` immediately. The token never un-cancels, so a restart
could only produce another instantly-cancelled run — the supervisor refuses
the futile loop.

---

Next: [Testing your code](testing.md) ·
[Timeouts, retries & cancellation](timeouts-and-cancellation.md) ·
[Process groups](process-groups.md)
