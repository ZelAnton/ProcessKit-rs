# Observability

[‹ docs index](README.md)

The crate can narrate what it does without you instrumenting a single call site,
through two independent, additive, off-by-default seams:

- **[`tracing`](#tracing)** — one structured *event* per lifecycle transition
  (spawn, exit, timeout/cancel firing, per-phase graceful teardown, retries,
  supervisor restarts). Good for a human reading a live log or a distributed
  trace.
- **[`metrics`](#metrics)** — aggregate *counters and histograms* over the same
  moments (how many runs, how long they took, how they ended, how often you
  retried or restarted). Good for a dashboard, an SLO alert, or a fleet-wide
  latency percentile.

Both derive everything from data the crate **already computes** on the run's
existing path — they add no extra work, no second wall-clock read, and no new
public API. With a feature off, its call sites compile out entirely; the
kill-on-drop tree guarantee and every run's behavior are byte-identical either
way.

The motivating consumer is an orchestrator supervising many agentic-CLI
subprocesses: SLO/latency signals across a production fleet, without hand-wiring
a timer around every spawn.

## metrics

Enable the `metrics` feature and the crate emits into whatever global
[`metrics`](https://docs.rs/metrics) recorder your process installs. The crate
ships **no exporter of its own** — you pick the backend (Prometheus, OpenTelemetry,
StatsD, a test recorder …) exactly as you would for your own application metrics,
and the crate's series simply appear alongside yours.

```toml
# Cargo.toml
[dependencies]
processkit = { version = "…", features = ["metrics"] }
metrics = "0.24"
# …plus any metrics-compatible exporter, e.g. metrics-exporter-prometheus.
```

Install a recorder once at startup — before you run any commands — and that is
the entire wiring:

```text
// Any `metrics::Recorder` works; a Prometheus exporter is shown for concreteness.
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new()
    .install()                       // registers the global recorder + scrape endpoint
    .expect("install Prometheus recorder");

// From here on, every processkit run feeds the counters/histograms below.
```

With the feature on and a recorder installed, an ordinary run needs no metrics
code at all:

```rust,no_run
use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Emits `processkit.spawns.total`, then on completion `processkit.runs.total`
    // + `processkit.run.duration_seconds` (+ `processkit.exit_code.total` on a
    // clean exit) — no per-call-site instrumentation.
    let _ = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output_string()
        .await?;
    Ok(())
}
```

### What is measured

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `processkit.spawns.total` | counter | `program`, `mechanism` | A child process was launched, under the given containment [mechanism](process-groups.md) (`job_object` / `cgroup_v2` / `process_group`). |
| `processkit.runs.total` | counter | `program`, `outcome` | A run reached a terminal outcome. `outcome` is one of `exited` / `signalled` / `timed_out` / `cancelled` — the timeout/cancel/signal tally. |
| `processkit.run.duration_seconds` | histogram | `program` | Wall-clock duration of a completed run, taken from the run's own already-measured elapsed time (no extra clock read). |
| `processkit.exit_code.total` | counter | `program`, `code` | Per-exit-code tally, recorded only for a genuine self-exit (a `signalled`/`timed_out`/`cancelled` run has no exit code). |
| `processkit.retries.total` | counter | `program` | A retryable failure was about to be retried (see [retries](timeouts-and-cancellation.md#retries)). |
| `processkit.restarts.total` | counter | — | A [`Supervisor`](supervision.md) was about to restart its child. |
| `processkit.storm_pauses.total` | counter | — | A supervisor tripped its failure-storm guard and paused restarts. |
| `processkit.teardown.total` | counter | `phase` | A graceful teardown reached a terminal `phase`: `drained` (exited within the grace), `escalated` (grace elapsed → hard kill), or `spared` (grace elapsed, a non-escalating stop left survivors). |
| `processkit.teardown.duration_seconds` | histogram | `phase` | Grace-window duration of a completed whole-tree teardown, keyed by terminal phase. |

The `restarts`/`storm_pauses` counters carry no `program` label: a supervisor
drives a *runner*, not one fixed command, so it has no single program name.

### Secret hygiene

The label values are exactly the same secret-safe facts the `tracing` seam logs:
a program **name**, a containment mechanism, an outcome class, an exit code, a
teardown phase. Command **argv** and **environment values** — which routinely
carry tokens, passwords, and connection strings — are **never** emitted, in a
metric name or a label. This is a structural guarantee, not a review convention:
the emission layer is handed only the already-derived program name and outcome,
never the `Command`, so there is no code path through which argv or env could
reach a series. The crate carries a test that runs a real child whose argv and
environment hold sentinel secrets and asserts neither ever surfaces in an emitted
name or label.

### Cardinality

Labels are kept to **bounded** dimensions so a time-series backend is not flooded
with unique series. In particular the process **pid** — harmless on the one-shot
`tracing` seam but effectively unbounded as an aggregation key — is deliberately
*not* a metric label; nor is any file path or argv fragment. The one numeric
label, the exit `code`, is bounded in practice (a process exit code is a small
integer). If your programs are themselves unbounded in number (say, a distinct
binary per tenant), pre-aggregate or drop the `program` label in your exporter's
relabeling — the same discipline you would apply to any high-cardinality
dimension.

## tracing

The `tracing` feature emits one event per lifecycle transition on the
`processkit` target — spawn/exit (with program, pid, mechanism), timeout and
cancellation firing, the per-phase graceful teardown timeline
(`soft_signal` → `grace_started` → `drained`/`escalated`/`spared`), retry
attempts, and supervisor restarts and storm pauses. It follows the same
secret-hygiene rule: argv and environment values are never logged. Enable it with
`features = ["tracing"]` and subscribe with any `tracing` subscriber; the two
seams are independent and can be enabled together or separately.

## See also

- [Supervision](supervision.md) — the restart/backoff/storm machinery the
  `restarts`/`storm_pauses` counters instrument.
- [Timeouts, retries & cancellation](timeouts-and-cancellation.md) — where the
  `retries` counter and the `timed_out`/`cancelled` outcomes come from.
- [Process groups](process-groups.md) — the containment mechanisms behind the
  `mechanism` label and the graceful-teardown phases.
