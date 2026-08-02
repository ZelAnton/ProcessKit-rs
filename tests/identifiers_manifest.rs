use std::collections::HashSet;
use std::fmt::{self, Write as _};
use std::path::PathBuf;

use processkit::{
    ErrorKind, LimitKind, LimitReason, LimitVerdict, LineTerminator, Mechanism, Outcome,
    OutputStream, OverflowMode, ParentDeathCleanup, Priority, RestartPolicy, RlimitResource,
    Signal, SoftStopScope, StdioMode, StopReason, SupervisionEvent,
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

fn report_only_identifiers(
    path: &'static str,
    values: &[(&'static str, &'static str)],
) -> EnumSpec {
    EnumSpec {
        path,
        class: "report_only",
        variants: values
            .iter()
            .map(|(rust_name, identifier)| Variant {
                rust_name,
                identifier,
            })
            .collect(),
    }
}

fn dictionary() -> Vec<EnumSpec> {
    vec![
        configurable(
            "processkit::Mechanism",
            &[
                ("JobObject", Mechanism::JobObject),
                ("CgroupV2", Mechanism::CgroupV2),
                ("ProcessGroup", Mechanism::ProcessGroup),
            ],
            |value| value.name(),
            Mechanism::from_name,
        ),
        configurable(
            "processkit::ParentDeathCleanup",
            &[
                ("WholeTree", ParentDeathCleanup::WholeTree),
                ("DirectChildOnly", ParentDeathCleanup::DirectChildOnly),
                ("Unsupported", ParentDeathCleanup::Unsupported),
            ],
            |value| value.name(),
            ParentDeathCleanup::from_name,
        ),
        configurable(
            "processkit::SoftStopScope",
            &[
                ("WholeTree", SoftStopScope::WholeTree),
                ("OptInMembers", SoftStopScope::OptInMembers),
                ("Unsupported", SoftStopScope::Unsupported),
            ],
            |value| value.name(),
            SoftStopScope::from_name,
        ),
        configurable(
            "processkit::StopReason",
            &[
                ("Predicate", StopReason::Predicate),
                ("PolicySatisfied", StopReason::PolicySatisfied),
                ("GaveUp", StopReason::GaveUp),
                ("RestartsExhausted", StopReason::RestartsExhausted),
                ("Unhealthy", StopReason::Unhealthy),
                ("Stopped", StopReason::Stopped),
            ],
            |value| value.name(),
            StopReason::from_name,
        ),
        configurable(
            "processkit::LimitKind",
            &[
                ("Memory", LimitKind::Memory),
                ("Processes", LimitKind::Processes),
                ("Cpu", LimitKind::Cpu),
            ],
            |value| value.name(),
            LimitKind::from_name,
        ),
        configurable(
            "processkit::LimitReason",
            &[
                ("Invalid", LimitReason::Invalid),
                ("Unsupported", LimitReason::Unsupported),
                ("Unenforceable", LimitReason::Unenforceable),
            ],
            |value| value.name(),
            LimitReason::from_name,
        ),
        configurable(
            "processkit::LimitVerdict",
            &[
                ("Tripped", LimitVerdict::Tripped),
                ("NotTripped", LimitVerdict::NotTripped),
                ("Unknown", LimitVerdict::Unknown),
            ],
            |value| value.name(),
            LimitVerdict::from_name,
        ),
        configurable(
            "processkit::StdioMode",
            &[
                ("Piped", StdioMode::Piped),
                ("Inherit", StdioMode::Inherit),
                ("Null", StdioMode::Null),
            ],
            |value| value.name(),
            StdioMode::from_name,
        ),
        configurable(
            "processkit::LineTerminator",
            &[
                ("Newline", LineTerminator::Newline),
                ("CarriageReturn", LineTerminator::CarriageReturn),
            ],
            |value| value.name(),
            LineTerminator::from_name,
        ),
        configurable(
            "processkit::OverflowMode",
            &[
                ("DropOldest", OverflowMode::DropOldest),
                ("DropNewest", OverflowMode::DropNewest),
                ("Error", OverflowMode::Error),
            ],
            |value| value.name(),
            OverflowMode::from_name,
        ),
        configurable(
            "processkit::OutputStream",
            &[
                ("Stdout", OutputStream::Stdout),
                ("Stderr", OutputStream::Stderr),
            ],
            |value| value.name(),
            OutputStream::from_name,
        ),
        configurable(
            "processkit::Priority",
            &[
                ("Idle", Priority::Idle),
                ("BelowNormal", Priority::BelowNormal),
                ("Normal", Priority::Normal),
                ("AboveNormal", Priority::AboveNormal),
                ("High", Priority::High),
            ],
            |value| value.name(),
            Priority::from_name,
        ),
        configurable(
            "processkit::RestartPolicy",
            &[
                ("Always", RestartPolicy::Always),
                ("OnCrash", RestartPolicy::OnCrash),
                ("Never", RestartPolicy::Never),
            ],
            |value| value.name(),
            RestartPolicy::from_name,
        ),
        configurable_optional(
            "processkit::Signal",
            &[
                ("Term", Signal::Term),
                ("Kill", Signal::Kill),
                ("Int", Signal::Int),
                ("Hup", Signal::Hup),
                ("Quit", Signal::Quit),
                ("Usr1", Signal::Usr1),
                ("Usr2", Signal::Usr2),
            ],
            |value| value.name(),
            Signal::from_name,
        ),
        configurable(
            "processkit::RlimitResource",
            &[
                ("Cpu", RlimitResource::Cpu),
                ("Core", RlimitResource::Core),
                ("Data", RlimitResource::Data),
                ("FileSize", RlimitResource::FileSize),
                ("NoFile", RlimitResource::NoFile),
                ("Stack", RlimitResource::Stack),
            ],
            RlimitResource::name,
            RlimitResource::from_name,
        ),
        report_only(
            "processkit::Outcome",
            &[
                ("Exited", Outcome::Exited(0)),
                ("Signalled", Outcome::Signalled(None)),
                ("TimedOut", Outcome::TimedOut),
                ("InactivityTimedOut", Outcome::InactivityTimedOut),
            ],
            |value| value.name(),
        ),
        report_only(
            "processkit::ErrorKind",
            &[
                ("NotFound", ErrorKind::NotFound),
                ("Spawn", ErrorKind::Spawn),
                ("PermissionDenied", ErrorKind::PermissionDenied),
                ("ResourceLimit", ErrorKind::ResourceLimit),
                ("Unsupported", ErrorKind::Unsupported),
                ("Timeout", ErrorKind::Timeout),
                ("Cancelled", ErrorKind::Cancelled),
                ("Predicate", ErrorKind::Predicate),
                ("Exit", ErrorKind::Exit),
                ("Signalled", ErrorKind::Signalled),
                ("Other", ErrorKind::Other),
            ],
            |value| value.name(),
        ),
        // OutputLine deliberately has no public constructor, so an external
        // contract test cannot construct the two line-carrying variants.
        report_only_identifiers(
            "processkit::ProcessEvent",
            &[
                ("Started", "started"),
                ("Stdout", "stdout"),
                ("Stderr", "stderr"),
                ("Exited", "exited"),
            ],
        ),
        report_only(
            "processkit::SupervisionEvent",
            &[
                (
                    "IncarnationStarted",
                    SupervisionEvent::IncarnationStarted {
                        attempt: 1,
                        pid: None,
                    },
                ),
                (
                    "IncarnationFinished",
                    SupervisionEvent::IncarnationFinished {
                        attempt: 1,
                        outcome: Outcome::Exited(0),
                        duration: std::time::Duration::from_secs(0),
                        success: true,
                    },
                ),
                (
                    "IncarnationFailed",
                    SupervisionEvent::IncarnationFailed {
                        attempt: 1,
                        error: ErrorKind::Spawn,
                    },
                ),
                (
                    "RestartScheduled",
                    SupervisionEvent::RestartScheduled {
                        restart: 1,
                        delay: std::time::Duration::from_secs(0),
                    },
                ),
                (
                    "StormPaused",
                    SupervisionEvent::StormPaused {
                        pause: 1,
                        delay: std::time::Duration::from_secs(0),
                    },
                ),
                (
                    "HealthCheckFailed",
                    SupervisionEvent::HealthCheckFailed {
                        attempt: 1,
                        terminal: false,
                    },
                ),
                ("GaveUp", SupervisionEvent::GaveUp { attempt: 1 }),
                (
                    "Stopped",
                    SupervisionEvent::Stopped {
                        reason: StopReason::Stopped,
                    },
                ),
                (
                    "SupervisionFailed",
                    SupervisionEvent::SupervisionFailed {
                        error: ErrorKind::Other,
                    },
                ),
                ("Lagged", SupervisionEvent::Lagged { skipped: 1 }),
            ],
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
