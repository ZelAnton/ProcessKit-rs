use std::time::Duration;

use processkit::{Outcome, ProcessResult};

#[test]
fn external_callers_can_round_trip_overflow_totals() {
    let original = ProcessResult::from_parts(
        "tool",
        "stdout".to_owned(),
        "stderr".to_owned(),
        Outcome::Exited(3),
        Some(Duration::from_millis(25)),
        Duration::from_secs(2),
        true,
        17,
        4096,
        vec![0, 3],
    );

    assert_eq!(original.total_lines(), 17);
    assert_eq!(original.total_bytes(), 4096);

    let rebuilt = ProcessResult::from_parts(
        original.program(),
        original.stdout().clone(),
        original.stderr(),
        original.outcome(),
        original.configured_timeout(),
        original.duration(),
        original.truncated(),
        original.total_lines(),
        original.total_bytes(),
        original.ok_codes().to_vec(),
    );

    assert_eq!(rebuilt.total_lines(), original.total_lines());
    assert_eq!(rebuilt.total_bytes(), original.total_bytes());
    assert_eq!(rebuilt.duration(), original.duration());
    assert_eq!(rebuilt.truncated(), original.truncated());
}
