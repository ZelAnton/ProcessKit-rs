use processkit::{Outcome, ProcessEvent};

// The complete manifest generator is a library unit test because constructing
// every ProcessEvent variant requires crate-private access to OutputLine. Keep
// this target as a public-API smoke test for the documented broad command.
#[test]
fn process_event_lifecycle_names_are_public() {
    assert_eq!(ProcessEvent::Started { pid: None }.name(), "started");
    assert_eq!(ProcessEvent::Exited(Outcome::Exited(0)).name(), "exited");
}
