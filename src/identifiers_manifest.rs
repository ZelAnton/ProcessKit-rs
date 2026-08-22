use std::collections::HashSet;
use std::fmt::{self, Write as _};
use std::path::PathBuf;

use crate::{
    ErrorKind, LimitKind, LimitReason, LimitVerdict, LineTerminator, Mechanism, Outcome,
    OutputLine, OutputStream, OverflowMode, ParentDeathCleanup, Priority, ProcessEvent,
    RestartPolicy, RlimitResource, Signal, SoftSignal, SoftStopScope, StdioMode, StopReason,
    SupervisionEvent, TeardownCause,
};

struct Variant {
    rust_name: &'static str,
    identifier: &'static str,
}

struct EnumSpec {
    path: &'static str,
    class: &'static str,
    variants: Vec<Variant>,
}

fn configurable<T, N, P>(
    path: &'static str,
    values: &[(&'static str, T)],
    name: N,
    from_name: P,
) -> EnumSpec
where
    T: Copy + fmt::Debug + Eq,
    N: Fn(T) -> &'static str,
    P: Fn(&str) -> Option<T>,
{
    let variants = values
        .iter()
        .map(|(rust_name, value)| {
            let identifier = name(*value);
            assert_eq!(
                from_name(identifier),
                Some(*value),
                "{path}::{rust_name} does not round-trip through from_name"
            );
            Variant {
                rust_name,
                identifier,
            }
        })
        .collect();
    EnumSpec {
        path,
        class: "configurable",
        variants,
    }
}

fn configurable_optional<T, N, P>(
    path: &'static str,
    values: &[(&'static str, T)],
    name: N,
    from_name: P,
) -> EnumSpec
where
    T: Copy + fmt::Debug + Eq,
    N: Fn(T) -> Option<&'static str>,
    P: Fn(&str) -> Option<T>,
{
    configurable(
        path,
        values,
        |value| name(value).expect("curated manifest variants must have names"),
        from_name,
    )
}

fn report_only<T, N>(path: &'static str, values: &[(&'static str, T)], name: N) -> EnumSpec
where
    N: Fn(&T) -> &'static str,
{
    EnumSpec {
        path,
        class: "report_only",
        variants: values
            .iter()
            .map(|(rust_name, value)| Variant {
                rust_name,
                identifier: name(value),
            })
            .collect(),
    }
}

/// Curate one dictionary enum's variant list **with a compile-time completeness
/// guard**.
///
/// Expands a single list into two things: the `(rust_name, value)` pairs the
/// generator serializes, and a private `match` over the same enum carrying one arm
/// per listed variant. Inside the defining crate that `match` is exhaustive
/// (`#[non_exhaustive]` binds only downstream crates), so adding a variant to a
/// dictionary enum without listing it here is a **compile error** — the list can no
/// longer drift silently behind the type it claims to describe.
///
/// That drift is exactly what shipped `Mechanism::ProcessReaper` against a
/// three-mechanism `spec/identifiers.json`: the generator carried a hand-written
/// array rather than a `match`, so the new variant compiled fine, the generator and
/// the committed baseline went stale together, and `identifiers_manifest_matches`
/// stayed green comparing two equally stale artifacts. A hand-written array cannot
/// fail closed; this one does.
///
/// A unit variant is written bare — the manifest value *is* the variant. One
/// carrying data is written `Variant = <expr>` with a representative value, since
/// the manifest names variants and cannot invent payloads. A variant that must stay
/// **out** of the dictionary is listed under `omitted:`: it still has to be
/// acknowledged here, but contributes no manifest entry.
macro_rules! curated {
    (@value $ty:ident, $variant:ident) => { $ty::$variant };
    (@value $ty:ident, $variant:ident, $value:expr) => { $value };
    (
        $ty:ident,
        [ $( $variant:ident $( = $value:expr )? ),+ $(,)? ]
        $(, omitted: [ $( $omitted:ident ),+ $(,)? ] )?
    ) => {{
        #[allow(dead_code)]
        fn completeness(value: &$ty) {
            // Exhaustive on purpose (no `_` arm): a new variant fails to compile
            // here until it is either curated into the manifest or explicitly
            // omitted above.
            match value {
                $( $ty::$variant { .. } => (), )+
                $( $( $ty::$omitted { .. } => (), )+ )?
            }
        }
        [ $( (stringify!($variant), curated!(@value $ty, $variant $(, $value)?)) ),+ ]
    }};
}

fn dictionary() -> Vec<EnumSpec> {
    vec![
        configurable(
            "processkit::Mechanism",
            &curated!(
                Mechanism,
                [JobObject, CgroupV2, ProcessGroup, ProcessReaper]
            ),
            |value| value.name(),
            Mechanism::from_name,
        ),
        configurable(
            "processkit::ParentDeathCleanup",
            &curated!(
                ParentDeathCleanup,
                [WholeTree, DirectChildOnly, Unsupported]
            ),
            |value| value.name(),
            ParentDeathCleanup::from_name,
        ),
        configurable(
            "processkit::SoftStopScope",
            &curated!(SoftStopScope, [WholeTree, OptInMembers, Unsupported]),
            |value| value.name(),
            SoftStopScope::from_name,
        ),
        configurable(
            "processkit::StopReason",
            &curated!(
                StopReason,
                [
                    Predicate,
                    PolicySatisfied,
                    GaveUp,
                    RestartsExhausted,
                    Unhealthy,
                    Stopped
                ]
            ),
            |value| value.name(),
            StopReason::from_name,
        ),
        configurable(
            "processkit::LimitKind",
            &curated!(LimitKind, [Memory, Processes, Cpu]),
            |value| value.name(),
            LimitKind::from_name,
        ),
        configurable(
            "processkit::LimitReason",
            &curated!(LimitReason, [Invalid, Unsupported, Unenforceable]),
            |value| value.name(),
            LimitReason::from_name,
        ),
        configurable(
            "processkit::LimitVerdict",
            &curated!(LimitVerdict, [Tripped, NotTripped, Unknown]),
            |value| value.name(),
            LimitVerdict::from_name,
        ),
        configurable(
            "processkit::StdioMode",
            &curated!(StdioMode, [Piped, Inherit, Null]),
            |value| value.name(),
            StdioMode::from_name,
        ),
        configurable(
            "processkit::LineTerminator",
            &curated!(LineTerminator, [Newline, CarriageReturn]),
            |value| value.name(),
            LineTerminator::from_name,
        ),
        configurable(
            "processkit::OverflowMode",
            &curated!(OverflowMode, [DropOldest, DropNewest, Error]),
            |value| value.name(),
            OverflowMode::from_name,
        ),
        configurable(
            "processkit::OutputStream",
            &curated!(OutputStream, [Stdout, Stderr]),
            |value| value.name(),
            OutputStream::from_name,
        ),
        configurable(
            "processkit::Priority",
            &curated!(Priority, [Idle, BelowNormal, Normal, AboveNormal, High]),
            |value| value.name(),
            Priority::from_name,
        ),
        configurable(
            "processkit::RestartPolicy",
            &curated!(RestartPolicy, [Always, OnCrash, Never]),
            |value| value.name(),
            RestartPolicy::from_name,
        ),
        configurable_optional(
            "processkit::Signal",
            // `Other(n)` is the raw-number escape hatch: `name()` answers `None`
            // for it by design (render the `i32`), so it carries no stable
            // identifier and stays out of the dictionary — deliberately, not by
            // omission.
            &curated!(
                Signal,
                [Term, Kill, Int, Hup, Quit, Usr1, Usr2],
                omitted: [Other]
            ),
            |value| value.name(),
            Signal::from_name,
        ),
        configurable(
            "processkit::RlimitResource",
            &curated!(RlimitResource, [Cpu, Core, Data, FileSize, NoFile, Stack]),
            RlimitResource::name,
            RlimitResource::from_name,
        ),
        report_only(
            "processkit::Outcome",
            &curated!(
                Outcome,
                [
                    Exited = Outcome::Exited(0),
                    Signalled = Outcome::Signalled(None),
                    TimedOut,
                    InactivityTimedOut
                ]
            ),
            |value| value.name(),
        ),
        report_only(
            "processkit::ErrorKind",
            &curated!(
                ErrorKind,
                [
                    NotFound,
                    Spawn,
                    PermissionDenied,
                    ResourceLimit,
                    Unsupported,
                    Timeout,
                    Cancelled,
                    Teardown,
                    Predicate,
                    Exit,
                    Signalled,
                    Other
                ]
            ),
            |value| value.name(),
        ),
        configurable(
            "processkit::TeardownCause",
            &curated!(
                TeardownCause,
                [
                    Timeout,
                    InactivityTimeout,
                    Cancellation,
                    ExplicitKill,
                    PipelineFailure
                ]
            ),
            |value| value.name(),
            TeardownCause::from_name,
        ),
        report_only(
            "processkit::ProcessEvent",
            &curated!(
                ProcessEvent,
                [
                    Started = ProcessEvent::Started { pid: None },
                    Stdout = ProcessEvent::Stdout(OutputLine::for_test("")),
                    Stderr = ProcessEvent::Stderr(OutputLine::for_test("")),
                    Exited = ProcessEvent::Exited(Outcome::Exited(0))
                ]
            ),
            ProcessEvent::name,
        ),
        report_only(
            "processkit::SupervisionEvent",
            &curated!(
                SupervisionEvent,
                [
                    IncarnationStarted = SupervisionEvent::IncarnationStarted {
                        attempt: 1,
                        pid: None,
                    },
                    IncarnationFinished = SupervisionEvent::IncarnationFinished {
                        attempt: 1,
                        outcome: Outcome::Exited(0),
                        duration: std::time::Duration::from_secs(0),
                        success: true,
                    },
                    IncarnationFailed = SupervisionEvent::IncarnationFailed {
                        attempt: 1,
                        error: ErrorKind::Spawn,
                    },
                    RestartScheduled = SupervisionEvent::RestartScheduled {
                        restart: 1,
                        delay: std::time::Duration::from_secs(0),
                    },
                    StormPaused = SupervisionEvent::StormPaused {
                        pause: 1,
                        delay: std::time::Duration::from_secs(0),
                    },
                    HealthCheckFailed = SupervisionEvent::HealthCheckFailed {
                        attempt: 1,
                        terminal: false,
                    },
                    GaveUp = SupervisionEvent::GaveUp { attempt: 1 },
                    Stopped = SupervisionEvent::Stopped {
                        reason: StopReason::Stopped,
                    },
                    SupervisionFailed = SupervisionEvent::SupervisionFailed {
                        error: ErrorKind::Other,
                    },
                    Lagged = SupervisionEvent::Lagged { skipped: 1 }
                ]
            ),
            |value| value.name(),
        ),
        report_only(
            "processkit::SoftSignal",
            &curated!(
                SoftSignal,
                [
                    Sent = SoftSignal::Sent(Signal::Term),
                    Unsupported,
                    Failed = SoftSignal::Failed(Signal::Term)
                ]
            ),
            |value| value.name(),
        ),
    ]
}

fn push_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                write!(output, "\\u{:04x}", u32::from(character)).expect("writing to String")
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

fn generated_manifest() -> String {
    let enums = dictionary();
    let mut paths = HashSet::new();
    let mut output = String::from(
        "{\n  \"schema_version\": 1,\n  \"maintenance\": \"Canonical stable-identifier dictionary; update docs/errors.md together with this manifest.\",\n  \"enums\": [\n",
    );

    for (enum_index, enum_spec) in enums.iter().enumerate() {
        assert!(paths.insert(enum_spec.path), "duplicate enum path");
        let mut identifiers = HashSet::new();
        output.push_str("    {\n      \"path\": ");
        push_json_string(&mut output, enum_spec.path);
        output.push_str(",\n      \"class\": ");
        push_json_string(&mut output, enum_spec.class);
        output.push_str(",\n      \"variants\": [\n");

        for (variant_index, variant) in enum_spec.variants.iter().enumerate() {
            assert!(
                identifiers.insert(variant.identifier),
                "{} has duplicate identifier {:?}",
                enum_spec.path,
                variant.identifier
            );
            output.push_str("        { \"variant\": ");
            push_json_string(&mut output, variant.rust_name);
            output.push_str(", \"identifier\": ");
            push_json_string(&mut output, variant.identifier);
            output.push_str(" }");
            if variant_index + 1 != enum_spec.variants.len() {
                output.push(',');
            }
            output.push('\n');
        }

        output.push_str("      ]\n    }");
        if enum_index + 1 != enums.len() {
            output.push(',');
        }
        output.push('\n');
    }
    output.push_str("  ]\n}\n");
    output
}

fn manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("spec/identifiers.json")
}

#[test]
fn identifiers_manifest_matches() {
    let expected = std::fs::read(manifest_path()).expect("read spec/identifiers.json");
    let actual = generated_manifest();
    assert!(
        expected == actual.as_bytes(),
        "spec/identifiers.json differs byte-for-byte from the live dictionary; run `just identifiers-diff`"
    );
}

#[test]
#[ignore = "used by the identifiers-diff recipe"]
fn write_identifiers_manifest() {
    let Some(output) = std::env::var_os("PROCESSKIT_IDENTIFIERS_OUTPUT").map(PathBuf::from) else {
        return;
    };
    std::fs::write(output, generated_manifest()).expect("write generated identifier manifest");
}
