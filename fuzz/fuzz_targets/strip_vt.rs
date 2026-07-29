#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    // Invalid UTF-8 is accepted by the crate seam through a lossy conversion;
    // the target checks removal, idempotence, and non-expansion invariants.
    processkit::fuzz_strip_vt(bytes);
});
