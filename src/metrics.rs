//! Optional [`metrics`](https://docs.rs/metrics) emission — a thin, additive
//! façade that turns data the crate **already computes** into counters and
//! histograms, mirroring the [`tracing`](crate) integration's shape and its
//! secret-hygiene rule.
//!
//! Gated behind the off-by-default `metrics` feature. With the feature off every
//! call site below compiles out (a pure visibility gate); the kill-on-drop tree
//! guarantee and every run's behavior are byte-identical either way. The crate
//! emits into whatever global [`Recorder`](metrics::Recorder) the consumer
//! installs (a Prometheus / OpenTelemetry exporter, say) and ships no exporter of
//! its own — see `docs/observability.md`.
//!
//! # Secret hygiene (load-bearing)
//!
//! Every label value emitted here comes from a **bounded, secret-free** source:
//! the program *name* (never the full argv), the containment
//! [`Mechanism`]'s stable identifier, a fixed outcome-class
//! token, an exit code, or a teardown-phase token. Command **argv** and
//! **environment values** — which routinely carry secrets — are *never* passed
//! into this module: the emission functions below do not even take the
//! [`Command`](crate::Command), only the already-derived program name and
//! outcome. This is the same stance the `tracing` seam takes, verified by the
//! tests at the bottom of this file.
//!
//! # Cardinality
//!
//! Labels are kept to **bounded** dimensions so a downstream time-series backend
//! is not flooded with unique series. In particular the process **pid** — safe to
//! log on the one-shot `tracing` seam but effectively unbounded as an aggregation
//! key — is deliberately *not* a metric label; nor is any file path or argv. The
//! one numeric label, the exit `code`, is bounded in practice (a process exit code
//! is a small integer) and mirrors the `tracing` seam's `exit code` field.

use std::time::Duration;

use crate::mechanism::Mechanism;
use crate::result::Outcome;

// Metric names — stable, dot-namespaced identifiers documented in
// `docs/observability.md`. Kept as constants so the vocabulary lives in one place
// and the emission sites can't drift apart.
const SPAWNS_TOTAL: &str = "processkit.spawns.total";
const RUNS_TOTAL: &str = "processkit.runs.total";
const RUN_DURATION: &str = "processkit.run.duration_seconds";
const EXIT_CODE_TOTAL: &str = "processkit.exit_code.total";
const RETRIES_TOTAL: &str = "processkit.retries.total";
const RESTARTS_TOTAL: &str = "processkit.restarts.total";
const STORM_PAUSES_TOTAL: &str = "processkit.storm_pauses.total";
const TEARDOWN_TOTAL: &str = "processkit.teardown.total";
const TEARDOWN_DURATION: &str = "processkit.teardown.duration_seconds";

/// The bounded outcome-class token for the `outcome` label of a completed run.
///
/// `cancelled` is distinguished from a plain signal kill by the run's cancel
/// disposition, which the reap path resolves first-observation-wins and passes in
/// (a cancelled run is reported as `Signalled(None)` by the reaper, so the raw
/// outcome alone cannot tell the two apart).
pub(crate) fn outcome_label(outcome: &Outcome, cancelled: bool) -> &'static str {
    if cancelled {
        return "cancelled";
    }
    // Exhaustive (no `_` arm) though `Outcome` is `#[non_exhaustive]`: within the
    // defining crate a new variant is a compile error here, forcing a deliberate
    // decision on its label rather than silently bucketing it as "unknown".
    match outcome {
        Outcome::Exited(_) => "exited",
        Outcome::Signalled(_) => "signalled",
        Outcome::TimedOut => "timed_out",
        Outcome::InactivityTimedOut => "inactivity_timed_out",
    }
}

/// A child was spawned: bump the spawn counter, keyed by program name and the
/// containment mechanism actually in effect.
pub(crate) fn record_spawn(program: &str, mechanism: Mechanism) {
    metrics::counter!(
        SPAWNS_TOTAL,
        "program" => program.to_owned(),
        "mechanism" => mechanism.name(),
    )
    .increment(1);
}

/// A run reached a terminal [`Outcome`]: bump the run tally (by outcome class),
/// record the duration histogram, and — for a clean exit — bump the per-code
/// tally.
///
/// `elapsed` is the run duration the caller already measured off its own
/// `started` anchor; this reuses it rather than reading a third clock (see
/// K-007's `started`/`deadline_anchor` split — metrics add no new time source).
pub(crate) fn record_run(program: &str, outcome: &Outcome, cancelled: bool, elapsed: Duration) {
    metrics::counter!(
        RUNS_TOTAL,
        "program" => program.to_owned(),
        "outcome" => outcome_label(outcome, cancelled),
    )
    .increment(1);
    metrics::histogram!(
        RUN_DURATION,
        "program" => program.to_owned(),
    )
    .record(elapsed.as_secs_f64());
    // Per-code tally only for a genuine self-exit. A cancelled run surfaces as
    // `Signalled(None)` at this point, so gate on the cancel disposition too.
    if !cancelled && let Outcome::Exited(code) = outcome {
        metrics::counter!(
            EXIT_CODE_TOTAL,
            "program" => program.to_owned(),
            "code" => code.to_string(),
        )
        .increment(1);
    }
}

/// A retryable failure is about to be retried: bump the retry counter.
pub(crate) fn record_retry(program: &str) {
    metrics::counter!(RETRIES_TOTAL, "program" => program.to_owned()).increment(1);
}

/// A supervisor is about to restart its child: bump the restart counter. The
/// supervisor is program-agnostic (it drives a runner, not a fixed command), so
/// this carries no program label.
pub(crate) fn record_restart() {
    metrics::counter!(RESTARTS_TOTAL).increment(1);
}

/// A supervisor tripped its failure-storm gate and is pausing restarts: bump the
/// storm-pause counter.
pub(crate) fn record_storm_pause() {
    metrics::counter!(STORM_PAUSES_TOTAL).increment(1);
}

/// A graceful teardown reached its terminal `phase` (`drained` / `escalated` /
/// `spared`): bump the teardown tally, keyed by that stable phase token (the same
/// vocabulary the `tracing` seam narrates).
pub(crate) fn record_teardown(phase: &'static str) {
    metrics::counter!(TEARDOWN_TOTAL, "phase" => phase).increment(1);
}

/// Record a completed whole-tree teardown's grace duration, keyed by terminal
/// phase. `elapsed` is the driver's own already-computed grace time (same anchor
/// the `ShutdownReport`/`tracing` seam reports — no new clock).
pub(crate) fn record_teardown_duration(phase: &'static str, elapsed: Duration) {
    metrics::histogram!(TEARDOWN_DURATION, "phase" => phase).record(elapsed.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    // The label vocabulary is a fixed, bounded set of snake_case tokens. This
    // pins it so a new `Outcome` variant (or a renamed token) is a loud test
    // failure, and documents that no run-supplied data (argv/env/path) can flow
    // into the `outcome` label — the classifier's only input is the `Outcome`
    // enum and a bool.
    #[test]
    fn outcome_labels_are_a_bounded_secret_free_vocabulary() {
        assert_eq!(outcome_label(&Outcome::Exited(0), false), "exited");
        assert_eq!(outcome_label(&Outcome::Exited(3), false), "exited");
        assert_eq!(
            outcome_label(&Outcome::Signalled(Some(9)), false),
            "signalled"
        );
        assert_eq!(outcome_label(&Outcome::Signalled(None), false), "signalled");
        assert_eq!(outcome_label(&Outcome::TimedOut, false), "timed_out");
        // The cancel disposition wins over the raw outcome: the reaper reports a
        // cancelled run as `Signalled(None)`, so the classifier must lean on the
        // passed-in disposition, not the enum, to call it `cancelled`.
        assert_eq!(outcome_label(&Outcome::Signalled(None), true), "cancelled");
        assert_eq!(outcome_label(&Outcome::Exited(0), true), "cancelled");
    }
}

// End-to-end secret-hygiene guard: install a capturing recorder, run a real child
// whose argv AND environment carry unique secret sentinels, and assert those
// sentinels never surface in any emitted metric name or label. This exercises the
// live emission path (`record_spawn`/`record_run`) rather than the classifier in
// isolation, so it catches a hypothetical future call site that mistakenly reached
// for argv/env.
//
// A single, process-global recorder is installed here (metrics' recorder, like
// `tracing`'s subscriber, is set-once per process — see K-064 for the analogous
// global-state discipline); this is the crate's only metrics-recorder test. The
// real spawn makes it `#[ignore]`d, run under CI's `--include-ignored` pass.
#[cfg(test)]
mod secret_hygiene {
    use std::sync::{Mutex, OnceLock};

    use metrics::{
        Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    // `output_string` is a `ProcessRunner` trait method.
    use crate::ProcessRunner;

    /// Every metric name and every label key/value seen by the recorder, flattened
    /// into a single string list for substring scanning.
    fn captured() -> &'static Mutex<Vec<String>> {
        static CAPTURED: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
        CAPTURED.get_or_init(|| Mutex::new(Vec::new()))
    }

    #[derive(Debug)]
    struct CapturingRecorder;

    impl CapturingRecorder {
        fn note(key: &Key) {
            let mut buf = captured().lock().expect("capture buffer poisoned");
            buf.push(key.name().to_owned());
            for label in key.labels() {
                buf.push(label.key().to_owned());
                buf.push(label.value().to_owned());
            }
        }
    }

    impl Recorder for CapturingRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
            Self::note(key);
            Counter::noop()
        }
        fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
            Self::note(key);
            Gauge::noop()
        }
        fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
            Self::note(key);
            Histogram::noop()
        }
    }

    #[tokio::test]
    #[ignore = "exercises the real spawn path and installs a process-global metrics recorder"]
    async fn labels_never_carry_argv_or_env_values() {
        const SECRET_ARG: &str = "pk-metrics-secret-argv-9f3a2b7c";
        const SECRET_ENV: &str = "pk-metrics-secret-env-7c1d4e8a";

        metrics::set_global_recorder(CapturingRecorder)
            .expect("this is the only test that installs a metrics recorder");

        // A trivial, quickly-exiting child whose argv and env both carry a unique
        // sentinel. The exit code is irrelevant — we only need the run to complete
        // so the spawn/run metrics fire; we never assert success.
        let command = if cfg!(windows) {
            crate::Command::new("cmd")
                .args(["/c", "exit", "0"])
                .arg(SECRET_ARG)
                .env("PK_METRICS_SECRET", SECRET_ENV)
        } else {
            crate::Command::new("sh")
                .args(["-c", "exit 0", "pk_argv0", SECRET_ARG])
                .env("PK_METRICS_SECRET", SECRET_ENV)
        };
        let _ = crate::JobRunner::new().output_string(&command).await;

        // Snapshot under a short lock, then assert lock-free so a failing assertion
        // can't poison the buffer for any concurrently-running test.
        let snapshot = captured().lock().expect("capture buffer poisoned").clone();
        for entry in &snapshot {
            assert!(
                !entry.contains(SECRET_ARG),
                "argv value leaked into a metric name/label: {entry:?}"
            );
            assert!(
                !entry.contains(SECRET_ENV),
                "env value leaked into a metric name/label: {entry:?}"
            );
        }
        // Guard against a vacuous pass: the run must actually have emitted metrics,
        // so the loop above inspected live emissions rather than an empty buffer.
        assert!(
            snapshot
                .iter()
                .any(|entry| entry == "processkit.runs.total"),
            "expected the run to emit `processkit.runs.total`; captured: {snapshot:?}"
        );
    }
}
