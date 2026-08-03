//! Internals shared by the `report-serde` [`Serialize`](serde::Serialize)
//! impls, plus the schema tests that pin them.
//!
//! The impls themselves live next to the types they serialize (`result.rs`,
//! `stats.rs`, `shutdown_report.rs`, `member.rs`, `limits.rs`, `supervisor.rs`,
//! …) so that adding a field to a report type puts its wire form directly in
//! view; what lives here is the handful of rules every one of them shares, in
//! one place rather than re-decided per file:
//!
//! - **Every tagged enum carries its identifier under [`KIND`]** — the value is
//!   always the type's own `name()`, never a serde-derived variant tag, so the
//!   wire spelling is the same dictionary `spec/identifiers.json` publishes and
//!   cannot drift from it.
//! - **Every time value is a number of seconds** ([`secs`]): a [`Duration`] as
//!   fractional seconds, a `SystemTime` as fractional seconds since the Unix
//!   epoch. The same unit the `metrics` seam records, so a duration means the
//!   same thing whichever of the two a consumer reads.
//! - **Reports about processes, never what a process produced.** No impl in
//!   this feature serializes captured stdout/stderr content, argv, or
//!   environment values — see the crate-root `report-serde` section for the
//!   full rule and the types deliberately left without an impl.

use std::time::Duration;

/// The object key every *tagged* enum in this schema puts its stable `name()`
/// identifier under (`{"kind": "exited", …}`).
///
/// One spelling for the whole feature: a consumer that learns the rule on
/// [`Outcome`](crate::Outcome) already knows it for
/// [`SupervisionEvent`](crate::SupervisionEvent) and every other tagged enum.
/// An enum with no payload at all is not tagged — it serializes as the bare
/// identifier string (`"stopped"`), since there is nothing to carry alongside.
pub(crate) const KIND: &str = "kind";

/// A [`Duration`] as fractional seconds — the one time unit this schema uses.
///
/// `f64` rather than integer milliseconds (the unit the `record` cassette
/// picked, where a fixture only needs to replay a coarse duration): a report
/// line should not quietly round a sub-millisecond run down to `0`, and
/// fractional seconds are what the `metrics` histograms already record.
pub(crate) fn secs(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

/// [`secs`] over an optional duration — `None` stays `None` (a missing
/// measurement is `null`, never a fabricated `0.0`).
pub(crate) fn secs_opt(duration: Option<Duration>) -> Option<f64> {
    duration.map(secs)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::Value;

    use crate::{Outcome, ProcessResult};

    /// Serialize `value` and decode it back into a `serde_json::Value` — the
    /// schema assertions below read the *wire* form, not the Rust value.
    fn json<T: serde::Serialize>(value: &T) -> Value {
        serde_json::to_value(value).expect("a report type serializes")
    }

    /// The tag of a serialized enum object, or the bare identifier string for
    /// an untagged (payload-free) enum.
    fn kind(value: &Value) -> &str {
        match value {
            Value::String(identifier) => identifier.as_str(),
            Value::Object(fields) => fields
                .get(super::KIND)
                .and_then(Value::as_str)
                .expect("a tagged enum object carries its identifier under `kind`"),
            other => panic!("expected an enum identifier or tagged object, got {other}"),
        }
    }

    fn result_with_streams(stdout: &str, stderr: &str) -> ProcessResult<String> {
        ProcessResult::from_parts(
            "tool".to_owned(),
            stdout.to_owned(),
            stderr.to_owned(),
            Outcome::Exited(3),
            Some(Duration::from_secs(30)),
            Duration::from_millis(1500),
            false,
            0,
            0,
            vec![0, 3],
        )
    }

    #[test]
    fn outcome_is_tagged_by_its_stable_identifier_and_carries_its_payload() {
        for outcome in [
            Outcome::Exited(7),
            Outcome::Signalled(Some(9)),
            Outcome::Signalled(None),
            Outcome::TimedOut,
            Outcome::InactivityTimedOut,
        ] {
            let value = json(&outcome);
            // The tag is the enum's own `name()` — never serde's derived
            // variant tag (`{"Exited": 7}`), which is the whole point of the
            // feature.
            assert_eq!(kind(&value), outcome.name(), "for {outcome:?}");
            // …and the payload the identifier deliberately does not carry
            // travels beside it, straight from the accessors.
            assert_eq!(
                value["code"],
                outcome.code().map_or(Value::Null, Value::from),
                "for {outcome:?}"
            );
            assert_eq!(
                value["signal"],
                outcome.signal().map_or(Value::Null, Value::from),
                "for {outcome:?}"
            );
        }
    }

    #[test]
    fn outcome_identifiers_are_the_documented_spellings() {
        // Pins the wire spellings themselves, so a rename would fail here as
        // well as in the identifier-manifest test.
        assert_eq!(json(&Outcome::Exited(0))["kind"], "exited");
        assert_eq!(json(&Outcome::Signalled(None))["kind"], "signalled");
        assert_eq!(json(&Outcome::TimedOut)["kind"], "timed_out");
        assert_eq!(
            json(&Outcome::InactivityTimedOut)["kind"],
            "inactivity_timed_out"
        );
    }

    #[test]
    fn process_result_reports_the_run() {
        let value = json(&result_with_streams("out", "err"));
        assert_eq!(value["program"], "tool");
        assert_eq!(kind(&value["outcome"]), "exited");
        assert_eq!(value["outcome"]["code"], 3);
        // `3` is in `ok_codes`, so this run *succeeded* — a consumer must not
        // have to re-derive the crate's own policy from the two fields.
        assert_eq!(value["success"], true);
        assert_eq!(value["ok_codes"], serde_json::json!([0, 3]));
        assert_eq!(value["duration_secs"], 1.5);
        assert_eq!(value["configured_timeout_secs"], 30.0);
        assert_eq!(value["truncated"], false);
        assert_eq!(value["total_lines"], 0);
        assert_eq!(value["total_bytes"], 0);
    }

    #[test]
    fn process_result_never_serializes_captured_output() {
        // The load-bearing secret-hygiene property: a child's output routinely
        // carries tokens (that is why `Command::capture_policy` exists), and a
        // report line is exactly where it must not land.
        let text =
            serde_json::to_string(&result_with_streams("TOKEN-IN-STDOUT", "TOKEN-IN-STDERR"))
                .expect("a ProcessResult serializes");
        assert!(!text.contains("TOKEN-IN-STDOUT"), "got {text}");
        assert!(!text.contains("TOKEN-IN-STDERR"), "got {text}");
        assert!(!text.contains("stdout"), "got {text}");
        assert!(!text.contains("stderr"), "got {text}");
    }

    #[test]
    fn process_result_reports_a_raw_bytes_payload_the_same_way() {
        // The impl is payload-agnostic (no `T: Serialize` bound), so the
        // bytes-capturing twin reports identically instead of emitting a
        // multi-megabyte array of integers — or failing to encode non-UTF-8
        // bytes at all in a text format.
        let bytes: ProcessResult<Vec<u8>> = ProcessResult::from_parts(
            "tool".to_owned(),
            b"\xff\xfe binary TOKEN".to_vec(),
            String::new(),
            Outcome::Exited(0),
            None,
            Duration::from_millis(250),
            true,
            12,
            2048,
            vec![0],
        );
        let value = json(&bytes);
        assert_eq!(value["program"], "tool");
        assert_eq!(value["success"], true);
        assert_eq!(value["configured_timeout_secs"], Value::Null);
        assert_eq!(value["duration_secs"], 0.25);
        assert_eq!(value["truncated"], true);
        assert_eq!(value["total_lines"], 12);
        assert_eq!(value["total_bytes"], 2048);
        assert!(
            !serde_json::to_string(&bytes)
                .expect("a bytes result serializes")
                .contains("TOKEN")
        );
    }

    #[test]
    fn a_timed_out_run_reports_no_code_and_no_success() {
        let timed_out = ProcessResult::from_parts(
            "tool".to_owned(),
            String::new(),
            String::new(),
            Outcome::TimedOut,
            Some(Duration::from_millis(500)),
            Duration::from_millis(500),
            false,
            0,
            0,
            vec![0],
        );
        let value = json(&timed_out);
        assert_eq!(kind(&value["outcome"]), "timed_out");
        assert_eq!(value["outcome"]["code"], Value::Null);
        assert_eq!(value["outcome"]["signal"], Value::Null);
        assert_eq!(value["success"], false);
        assert_eq!(value["configured_timeout_secs"], 0.5);
    }

    #[test]
    fn error_kind_serializes_as_its_bare_identifier() {
        for error in [
            crate::ErrorKind::NotFound,
            crate::ErrorKind::Spawn,
            crate::ErrorKind::Timeout,
            crate::ErrorKind::Cancelled,
            crate::ErrorKind::Other,
        ] {
            let value = json(&error);
            assert_eq!(value, Value::String(error.name().to_owned()));
        }
    }

    #[test]
    fn stop_reason_serializes_as_its_bare_identifier() {
        for reason in [
            crate::StopReason::Predicate,
            crate::StopReason::PolicySatisfied,
            crate::StopReason::GaveUp,
            crate::StopReason::RestartsExhausted,
            crate::StopReason::Unhealthy,
            crate::StopReason::Stopped,
        ] {
            assert_eq!(json(&reason), Value::String(reason.name().to_owned()));
            // The inverse still parses the very same string, so a report and a
            // config file speak one vocabulary.
            assert_eq!(crate::StopReason::from_name(reason.name()), Some(reason));
        }
    }

    #[test]
    fn supervision_outcome_reports_the_final_run_and_the_counters() {
        let outcome = crate::SupervisionOutcome::from_parts(
            result_with_streams("out", "err"),
            2,
            crate::StopReason::RestartsExhausted,
            1,
            3,
        );
        let value = json(&outcome);
        assert_eq!(value["stopped"], "restarts_exhausted");
        assert_eq!(value["restarts"], 2);
        assert_eq!(value["storm_pauses"], 1);
        assert_eq!(value["liveness_kills"], 3);
        assert_eq!(value["final_result"]["program"], "tool");
        assert_eq!(kind(&value["final_result"]["outcome"]), "exited");
    }

    #[test]
    fn supervision_events_are_tagged_by_their_stable_identifiers() {
        let events = [
            crate::SupervisionEvent::IncarnationStarted {
                attempt: 1,
                pid: Some(4242),
            },
            crate::SupervisionEvent::IncarnationFinished {
                attempt: 1,
                outcome: Outcome::Exited(1),
                duration: Duration::from_millis(1250),
                success: false,
            },
            crate::SupervisionEvent::IncarnationFailed {
                attempt: 2,
                error: crate::ErrorKind::Spawn,
            },
            crate::SupervisionEvent::RestartScheduled {
                restart: 1,
                delay: Duration::from_millis(500),
            },
            crate::SupervisionEvent::StormPaused {
                pause: 1,
                delay: Duration::from_secs(30),
            },
            crate::SupervisionEvent::HealthCheckFailed {
                attempt: 3,
                terminal: false,
            },
            crate::SupervisionEvent::GaveUp { attempt: 3 },
            crate::SupervisionEvent::Stopped {
                reason: crate::StopReason::Stopped,
            },
            crate::SupervisionEvent::SupervisionFailed {
                error: crate::ErrorKind::Other,
            },
            crate::SupervisionEvent::Lagged { skipped: 7 },
        ];
        for event in &events {
            let value = json(event);
            assert_eq!(kind(&value), event.name(), "for {event:?}");
        }

        // …and each payload travels under its own key, in this schema's units.
        let started = json(&events[0]);
        assert_eq!(started["attempt"], 1);
        assert_eq!(started["pid"], 4242);

        let finished = json(&events[1]);
        assert_eq!(kind(&finished["outcome"]), "exited");
        assert_eq!(finished["outcome"]["code"], 1);
        assert_eq!(finished["duration_secs"], 1.25);
        assert_eq!(finished["success"], false);

        assert_eq!(json(&events[2])["error"], "spawn");
        assert_eq!(json(&events[3])["delay_secs"], 0.5);
        assert_eq!(json(&events[4])["delay_secs"], 30.0);
        assert_eq!(json(&events[5])["terminal"], false);
        assert_eq!(json(&events[6])["attempt"], 3);
        assert_eq!(json(&events[7])["reason"], "stopped");
        assert_eq!(json(&events[8])["error"], "other");
        assert_eq!(json(&events[9])["skipped"], 7);
    }

    #[test]
    fn a_pidless_incarnation_reports_a_null_pid_never_a_sentinel() {
        let value = json(&crate::SupervisionEvent::IncarnationStarted {
            attempt: 4,
            pid: None,
        });
        assert_eq!(value["pid"], Value::Null);
    }

    #[cfg(feature = "stats")]
    #[test]
    fn run_profile_reports_the_outcome_and_the_telemetry() {
        let profile = crate::RunProfile::from_parts(
            Outcome::Signalled(Some(9)),
            Duration::from_secs(2),
            Some(Duration::from_secs(1)),
            Some(4096),
            8,
        );
        let value = json(&profile);
        assert_eq!(kind(&value["outcome"]), "signalled");
        assert_eq!(value["outcome"]["signal"], 9);
        assert_eq!(value["duration_secs"], 2.0);
        assert_eq!(value["cpu_time_secs"], 1.0);
        assert_eq!(value["peak_memory_bytes"], 4096);
        assert_eq!(value["samples"], 8);
    }

    #[cfg(feature = "stats")]
    #[test]
    fn unavailable_metrics_are_null_never_zero() {
        // The honesty rule the types themselves keep: a platform that cannot
        // report CPU/memory says so, and the wire form must not turn that into
        // a plausible-looking `0`.
        let profile =
            crate::RunProfile::from_parts(Outcome::Exited(0), Duration::ZERO, None, None, 0);
        let value = json(&profile);
        assert_eq!(value["cpu_time_secs"], Value::Null);
        assert_eq!(value["peak_memory_bytes"], Value::Null);

        let stats = crate::ProcessGroupStats {
            active_process_count: 3,
            total_cpu_time: None,
            peak_memory_bytes: None,
        };
        let value = json(&stats);
        assert_eq!(value["active_process_count"], 3);
        assert_eq!(value["total_cpu_time_secs"], Value::Null);
        assert_eq!(value["peak_memory_bytes"], Value::Null);
    }

    #[cfg(feature = "stats")]
    #[test]
    fn process_group_stats_report_available_measurements() {
        let stats = crate::ProcessGroupStats {
            active_process_count: 2,
            total_cpu_time: Some(Duration::from_millis(1500)),
            peak_memory_bytes: Some(65_536),
        };
        let value = json(&stats);
        assert_eq!(value["active_process_count"], 2);
        assert_eq!(value["total_cpu_time_secs"], 1.5);
        assert_eq!(value["peak_memory_bytes"], 65_536);
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn member_info_reports_metadata_and_never_a_command_line() {
        let member = crate::MemberInfo::new(
            4242,
            Some(1),
            Some("worker.exe".to_owned()),
            Some(133_000_000_000_000_000),
        );
        let value = json(&member);
        assert_eq!(value["pid"], 4242);
        assert_eq!(value["ppid"], 1);
        assert_eq!(value["exe_name"], "worker.exe");
        // An opaque identity token, serialized verbatim as the `u64` it is.
        assert_eq!(value["start_time"], 133_000_000_000_000_000u64);
        // The type's "No command line" rule reaches the wire form too: there is
        // no argv/env key to accidentally fill in later.
        let text = serde_json::to_string(&member).expect("a MemberInfo serializes");
        assert!(!text.contains("args"), "got {text}");
        assert!(!text.contains("cmdline"), "got {text}");
        assert!(!text.contains("env"), "got {text}");
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn unreadable_member_metadata_is_null_never_fabricated() {
        let value = json(&crate::MemberInfo::new(7, None, None, None));
        assert_eq!(value["pid"], 7);
        assert_eq!(value["ppid"], Value::Null);
        assert_eq!(value["exe_name"], Value::Null);
        assert_eq!(value["start_time"], Value::Null);
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn signals_serialize_as_their_identifier_or_a_raw_number() {
        for signal in [
            crate::Signal::Term,
            crate::Signal::Kill,
            crate::Signal::Int,
            crate::Signal::Hup,
            crate::Signal::Quit,
            crate::Signal::Usr1,
            crate::Signal::Usr2,
        ] {
            let expected = signal.name().expect("a curated signal has an identifier");
            assert_eq!(json(&signal), Value::String(expected.to_owned()));
        }
        // The raw-number escape hatch has no curated identifier, so it renders
        // as its `i32` — exactly what `Signal::name()`'s `None` prescribes.
        assert_eq!(json(&crate::Signal::Other(37)), Value::from(37));
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn shutdown_report_reports_the_soft_tier_and_the_teardown_facts() {
        use crate::sys::graceful::{GracefulOutcome, SoftDelivery};

        let report = crate::ShutdownReport::from_outcome(
            GracefulOutcome {
                soft: SoftDelivery::Sent,
                members_before: Some(3),
                members_after: Some(0),
                drained: true,
                escalated: false,
                elapsed: Duration::from_millis(120),
            },
            crate::Signal::Term,
        );
        let value = json(&report);
        assert_eq!(kind(&value["soft_signal"]), "sent");
        assert_eq!(value["soft_signal"]["signal"], "term");
        assert_eq!(value["members_before"], 3);
        assert_eq!(value["members_after"], 0);
        assert_eq!(value["drained_within_grace"], true);
        assert_eq!(value["escalated"], false);
        assert_eq!(value["elapsed_secs"], 0.12);
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn every_soft_signal_fate_is_tagged_by_its_identifier() {
        for (soft_signal, identifier, signal) in [
            (
                crate::SoftSignal::Sent(crate::Signal::Term),
                "sent",
                Value::String("term".to_owned()),
            ),
            (crate::SoftSignal::Unsupported, "unsupported", Value::Null),
            (
                crate::SoftSignal::Failed(crate::Signal::Int),
                "failed",
                Value::String("int".to_owned()),
            ),
        ] {
            let value = json(&soft_signal);
            assert_eq!(kind(&value), soft_signal.name());
            assert_eq!(kind(&value), identifier);
            // A platform with no soft tier reports a `null` signal rather than
            // inventing one it never attempted.
            assert_eq!(value["signal"], signal, "for {soft_signal:?}");
        }
    }

    #[cfg(feature = "process-control")]
    #[test]
    fn an_unreadable_membership_is_null_never_a_fabricated_zero() {
        use crate::sys::graceful::{GracefulOutcome, SoftDelivery};

        let report = crate::ShutdownReport::from_outcome(
            GracefulOutcome {
                soft: SoftDelivery::Unsupported,
                members_before: None,
                members_after: None,
                drained: false,
                escalated: true,
                elapsed: Duration::from_secs(5),
            },
            crate::Signal::Term,
        );
        let value = json(&report);
        assert_eq!(kind(&value["soft_signal"]), "unsupported");
        assert_eq!(value["soft_signal"]["signal"], Value::Null);
        assert_eq!(value["members_before"], Value::Null);
        assert_eq!(value["members_after"], Value::Null);
        assert_eq!(value["escalated"], true);
    }

    #[cfg(feature = "limits")]
    #[test]
    fn limit_evidence_reports_a_verdict_identifier_per_axis() {
        let evidence = crate::LimitEvidence::new(
            crate::LimitVerdict::Tripped,
            crate::LimitVerdict::NotTripped,
            crate::LimitVerdict::Unknown,
        );
        let value = json(&evidence);
        assert_eq!(value["memory"], "tripped");
        assert_eq!(value["processes"], "not_tripped");
        assert_eq!(value["cpu"], "unknown");
        // Each axis keeps its own three-valued answer — `unknown` is never
        // folded into a "no" by the wire form either.
        for verdict in [
            crate::LimitVerdict::Tripped,
            crate::LimitVerdict::NotTripped,
            crate::LimitVerdict::Unknown,
        ] {
            assert_eq!(json(&verdict), Value::String(verdict.name().to_owned()));
            assert_eq!(
                crate::LimitVerdict::from_name(verdict.name()),
                Some(verdict)
            );
        }
    }
}
