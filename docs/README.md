# processkit — documentation

`processkit` is a child-process toolkit for Rust in two layers:

```text
┌─────────────────────────────────────────────────────────────────┐
│  Runner layer (async, tokio)                                    │
│  Command · RunningProcess · Pipeline · Supervisor · CliClient   │
│  capture / streaming / PTY / interactive stdin / readiness      │
│  testing seam: ProcessRunner → ScriptedRunner / RecordReplay…   │
├─────────────────────────────────────────────────────────────────┤
│  Group layer (kill-on-drop containment)                         │
│  ProcessGroup: spawn / adopt / signal / suspend / members /     │
│  stats / limits / shutdown                                      │
├─────────────────────────────────────────────────────────────────┤
│  OS mechanisms                                                  │
│  Windows Job Object · Linux cgroup v2 · POSIX process group     │
└─────────────────────────────────────────────────────────────────┘
```

Every `Command` run gets containment for free: the one-shot helpers spawn into
a fresh private group that dies with the run, so a panicking caller never
leaks a process tree. The layers are also usable independently — a raw
`ProcessGroup` can contain children you spawn yourself, and the runner's
test doubles never touch the OS at all.

## 3.0 highlights

- **PTY support is here.** With the opt-in `pty` feature,
  `Command::use_pty()` gives terminal-only tools a real controlling terminal:
  `openpty` on Unix and `CreatePseudoConsole` (ConPTY) on Windows. The terminal
  has one merged output stream, supports interactive input plus initial/live
  window sizing, and stays inside the same kill-on-drop containment as a piped
  run. Start with the [PTY dialog](streaming.md#pty-dialog-wait-for-a-prompt-then-answer)
  and [window-size](streaming.md#pty-window-size-and-live-resize) sections, then
  check the [platform matrix](platform-support.md#pty-mode-use_pty-the-pty-feature).
- **Errors are split by responsibility.** `Error` is now a pointer-sized wrapper
  around `Box<ErrorReason>`; its existing accessors remain the convenient path,
  `reason()` exposes the structured variant, and `ErrorKind` is the coarse total
  classification for routing. See [Errors](errors.md) for the model and
  [Upgrading](upgrading.md#300-from-2x) for the 2.x migration.

The complete release record is in the
[`v3.0.0` release](https://github.com/ZelAnton/ProcessKit-rs/releases/tag/v3.0.0).

## Guides

**New to the crate?** Start with the [Cookbook](cookbook.md) — short
task-to-snippet recipes for everything the crate does — then read
[Running commands](commands.md) end to end (it's the vocabulary every other
guide builds on). Reach for the rest as the need arises, and keep
[Platform support](platform-support.md) handy before you ship: it collects
every per-OS caveat in one place.

| Guide | Covers |
|---|---|
| [Cookbook](cookbook.md) | "I want to …" → working snippet, for every capability; each recipe links to its deep guide |
| [Running commands](commands.md) | The `Command` builder end to end: args, env, stdin sources, encodings, buffer policies, line handlers, timeouts, retry, privileges — and every consuming verb (`run`, `output_string`, `probe`, …) with its error semantics |
| [Running many at once](batch.md) | Bounded `output_all` / `output_all_bytes` fan-out, shared versus independent containment, and `wait_any` / `wait_all` races and joins |
| [Comparative benchmarks](comparison.md) | End-to-end processkit vs. plain Tokio and standard-library baselines across capture, streaming, and concurrent fan-out |
| [Process groups](process-groups.md) | Kill-on-drop containment: creating groups, spawning/adopting, teardown verbs, whole-tree signals, suspend/resume, member listing, resource limits, stats sampling |
| [Streaming & interactive I/O](streaming.md) | `start()` and the live `RunningProcess`: line streaming, interactive stdin, PTY dialogs/window resize/output hygiene, readiness probes (`wait_for_line` / `wait_for_port` / `wait_for`), racing children with `wait_any`, per-run profiling |
| [Pipelines](pipelines.md) | `a \| b \| c` without a shell (the `\|` operator works too): wiring, pipefail attribution, `unchecked_in_pipe()` stages for the `\| head` pattern, timeouts, stdin/stdout at the ends, re-running chains |
| [Timeouts, retries & cancellation](timeouts-and-cancellation.md) | How a deadline is *captured* vs when it errors, retry policies and their classifier, and cancellation: per-command tokens and the client-level `default_cancel_on` |
| [Errors](errors.md) | The `Error` wrapper, every `ErrorReason` variant, `ErrorKind`, subtle look-alikes (`Timeout` vs `Cancelled`, `NotReady` vs `Timeout`, `NotFound` vs `Spawn`, `CassetteMiss`), classifiers, and how retries/supervision use them |
| [Supervision](supervision.md) | Keeping a child alive: restart policies, backoff & jitter math, the failure-storm guard, stop conditions, outcomes, supervising inside a shared group |
| [Observability](observability.md) | The `tracing` event seam and the `metrics` counters/histograms over data the crate already computes — what's measured, the secret-hygiene guarantee (never argv/env), label cardinality, and wiring a backend exporter |
| [Testing your code](testing.md) | The `ProcessRunner` seam — bulk **and** streaming: `ScriptedRunner` (incl. scripted `start()` with canned, paced lines), `RecordingRunner`, `MockRunner`, record/replay cassettes, and building hermetically-testable CLI wrappers with `CliClient` |
| [Platform support](platform-support.md) | The containment mechanisms, every per-feature support matrix in one place, and the platform caveats worth knowing before you ship |
| [Running in containers](containers.md) | Docker/Kubernetes specifics: which mechanism you actually get, PID 1 signal/reaping behavior, graceful shutdown on the orchestrator's `SIGTERM`, minimal musl/Alpine images, and container limits vs. the crate's own `limits` |
| [Running untrusted children](untrusted-children.md) | A hardening checklist for launching a program you don't trust: containment, resource limits, privilege drop order, environment hygiene, output/wall-time bounds — what the crate guarantees, what it doesn't, and where to go for real isolation |
| [Upgrading](upgrading.md) | Per-version consumer upgrade notes, including the 2.x → 3.0 `ErrorReason` and lifecycle-event migrations |
| [What's next](whats-next.md) | Where the containment/runner approach is headed beyond this Rust crate |

## Feature flags

Every flag is **additive** and only gates *visibility* — the kill-on-drop tree
guarantee is unconditional in every configuration.

| Feature | Default | Adds | Extra dependency |
|---|---|---|---|
| `stats` | off | `ProcessGroup::stats` / `sample_stats`, `RunningProcess::cpu_time` / `peak_memory_bytes` / `profile` | `windows-sys/ProcessStatus` (Windows) |
| `process-control` | **on** | `Signal`, `ProcessGroup::{signal, suspend, resume, members, adopt}` | — |
| `limits` | off | Whole-tree resource caps on `ProcessGroupOptions` (`max_memory` / `max_processes` / `cpu_quota`), `ErrorReason::ResourceLimit`; implies `stats` | — |
| `mock` | off | A `mockall`-generated `MockRunner` for expectation-style tests (test-only; its `expect_*` surface is **semver-exempt**, tracking `mockall` — prefer `ScriptedRunner`/`RecordingRunner` for a stable double) | `mockall` |
| `tracing` | off | Events on the `processkit` target: spawn/exit, timeout & cancel firing, per-transition graceful teardown (`soft_signal` → `grace_started` → `drained`/`escalated`/`spared`), retries, supervisor storms, teardown anomalies (never argv/env) | `tracing` |
| `metrics` | off | Counters/histograms over already-computed run data: run/spawn counters, run-duration histograms, an exit-code/timeout/cancel tally, retry/restart/storm events — into any `metrics` recorder you install (never argv/env). See [Observability](observability.md) | `metrics` |
| `record` | off | `RecordReplayRunner` JSON cassettes over the runner seam | `serde`, `serde_json` |
| `pty` | off | Real pseudo-terminal launch mode (`openpty` on Unix, ConPTY on Windows), interactive master input, merged output, initial/live window sizing | `windows-sys/Win32_System_Pipes` (Windows) |

```toml
[dependencies]
processkit = { version = "3", features = ["limits"] }
```

## The 60-second tour

```rust,no_run
use processkit::{Command, ProcessGroup, Stdin};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // One-shot: capture everything. A non-zero exit is data, not an Err.
    let head = Command::new("git").args(["rev-parse", "HEAD"]).output_string().await?;
    println!("HEAD = {}", head.stdout().trim());

    // Success-checking: non-zero exit / timeout / signal-kill become typed errors.
    let version = Command::new("cargo").arg("--version").run().await?;

    // Stdin, timeout, streaming, pipelines, supervision … see the guides.
    let sorted = Command::new("sort")
        .stdin(Stdin::from_string("b\na\n"))
        .timeout(std::time::Duration::from_secs(5))
        .run()
        .await?;

    // Containment: anything spawned through a group dies with it.
    let group = ProcessGroup::new()?;
    let _server = group.start(&Command::new("dev-server")).await?;
    drop(group); // the server — and everything *it* spawned — is reaped

    let _ = (version, sorted);
    Ok(())
}
```

## API reference

The rustdoc on [docs.rs](https://docs.rs/processkit) is the authoritative
per-item reference; these guides are the narrative layer on top — they explain
how the pieces compose, with the platform fine print collected in
[Platform support](platform-support.md).

**These examples are compiler-checked.** Every fenced Rust block across these
guides and the root `README.md` is compiled (and, unless annotated `no_run` or
`ignore`, actually run) as an ordinary doctest by `cargo test --all-features`
(as CI does) — a signature change that stops matching a guide's snippet fails
CI instead of silently lying to a reader. The hidden harness only builds under
`--all-features`, so a plain `cargo test` with the default features does
**not** exercise this check. See `src/doc_examples.rs` for the (test-only,
hidden) harness.

## Other languages

Not on Rust? [`processkit-py`](https://pypi.org/project/processkit-py/) wraps
this crate's core in a Python (PyO3/asyncio) API — this crate stays the single
source of truth for the containment/runner logic underneath.
