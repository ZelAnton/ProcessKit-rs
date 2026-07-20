#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    // The target must accept every byte sequence. For a syntactically valid
    // future-version header, the shared loader rejects it at its version gate
    // before attempting to decode arbitrary entry shapes.
    processkit::fuzz_cassette_parse(bytes);
});
