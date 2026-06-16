# processkit — documentation

`processkit` is a child-process toolkit for Rust in two layers:

```text
┌─────────────────────────────────────────────────────────────────┐
│  Runner layer (async, tokio)                                    │
│  Command · RunningProcess · Pipeline · Supervisor · CliClient   │
│  capture / streaming / interactive stdin / readiness probes     │
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
| [Process groups](process-groups.md) | Kill-on-drop containment: creating groups, spawning/adopting, teardown verbs, whole-tree signals, suspend/resume, member listing, resource limits, stats sampling |
| [Streaming & interactive I/O](streaming.md) | `start()` and the live `RunningProcess`: line streaming, interactive stdin, readiness probes (`wait_for_line` / `wait_for_port` / `wait_for`), racing children with `wait_any`, per-run profiling |
| [Pipelines](pipelines.md) | `a \| b \| c` without a shell (the `\|` operator works too): wiring, pipefail attribution, `unchecked_in_pipe()` stages for the `\| head` pattern, timeouts, stdin/stdout at the ends, re-running chains |
| [Timeouts, retries & cancellation](timeouts-and-cancellation.md) | How a deadline is *captured* vs when it errors, retry policies and their classifier, and cancellation: per-command tokens and the client-level `default_cancel_on` |
| [Supervision](supervision.md) | Keeping a child alive: restart policies, backoff & jitter math, the failure-storm guard, stop conditions, outcomes, supervising inside a shared group |
| [Testing your code](testing.md) | The `ProcessRunner` seam — bulk **and** streaming: `ScriptedRunner` (incl. scripted `start()` with canned, paced lines), `RecordingRunner`, `MockRunner`, record/replay cassettes, and building hermetically-testable CLI wrappers with `CliClient` |
| [Platform support](platform-support.md) | The containment mechanisms, every per-feature support matrix in one place, and the platform caveats worth knowing before you ship |
| [Upgrading](upgrading.md) | Per-version consumer upgrade notes — what changed on each release and the exact change to make across a major bump |

## Feature flags

Every flag is **additive** and only gates *visibility* — the kill-on-drop tree
guarantee is unconditional in every configuration.

| Feature | Default | Adds | Extra dependency |
|---|---|---|---|
| `stats` | off | `ProcessGroup::stats` / `sample_stats`, `RunningProcess::cpu_time` / `peak_memory_bytes` / `profile` | `windows-sys/ProcessStatus` (Windows) |
| `process-control` | **on** | `Signal`, `ProcessGroup::{signal, suspend, resume, members, adopt}` | — |
| `limits` | off | Whole-tree resource caps on `ProcessGroupOptions` (`memory_max` / `max_processes` / `cpu_quota`), `Error::ResourceLimit`; implies `stats` | — |
| `mock` | off | A `mockall`-generated `MockRunner` for expectation-style tests (test-only; its `expect_*` surface is **semver-exempt**, tracking `mockall` — prefer `ScriptedRunner`/`RecordingRunner` for a stable double) | `mockall` |
| `tracing` | off | Events on the `processkit` target: spawn/exit, timeout & cancel firing, group teardown, retries, supervisor storms, teardown anomalies (never argv/env) | `tracing` |
| `record` | off | `RecordReplayRunner` JSON cassettes over the runner seam | `serde`, `serde_json` |

```toml
[dependencies]
processkit = { version = "…", features = ["limits"] }
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
