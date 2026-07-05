#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

/// Structured fuzz input for the `pump_lines` decode path
/// (`processkit::fuzz_decode_pump_lines`, `cfg(fuzzing)`-only in
/// `src/pump.rs`): raw (possibly invalid) bytes to decode, the chunk sizes to
/// split them into before feeding them to the pump one read at a time (so
/// multibyte sequences routinely split across reads), and an encoding
/// selector. Mirrors the `pump_never_panics_on_arbitrary_bytes_under_any_chunking`
/// proptest in `src/pump.rs`, but driven by libFuzzer-guided input instead of
/// proptest-shrunk cases.
#[derive(Debug, Arbitrary)]
struct Input {
    raw: Vec<u8>,
    chunk_sizes: Vec<u8>,
    encoding_idx: u8,
}

fuzz_target!(|input: Input| {
    processkit::fuzz_decode_pump_lines(&input.raw, &input.chunk_sizes, input.encoding_idx);
});
