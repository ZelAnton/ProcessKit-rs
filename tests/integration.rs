//! Integration tests for the `processkit` library.
//!
//! Files in `tests/` are each compiled as a separate crate that links against
//! `processkit` and exercises its public surface. As the `group` and `runner`
//! APIs land, replace this placeholder with tests that spawn real children and
//! assert on lifetime / capture behavior.

#[test]
fn links_against_crate() {
    // Proves the crate links from an external test crate; a real assertion
    // replaces this once there is public API to drive.
    assert_eq!(2 + 2, 4);
}
