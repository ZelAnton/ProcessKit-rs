//! Shared FNV-1a (64-bit) hashing behind the record/replay cassette match keys.
//!
//! Two cassette-key digests use FNV-1a: [`Stdin::content_digest`](crate::Stdin)
//! (the stdin *source identity*) and the opt-in `MatchPolicy` digest (cwd +
//! selected env values) — both in the `record` feature. They deliberately avoid
//! `std`'s [`DefaultHasher`](std::collections::hash_map::DefaultHasher), whose
//! output may change between Rust releases: a digest recorded today must replay
//! **bit-for-bit identically** tomorrow and — more sharply — must still equal the
//! digests already written into committed cassette fixtures. FNV-1a is a fixed
//! algorithm over two hard-coded 64-bit constants, so it is stable across releases
//! by construction.
//!
//! This module is the **single** home of those constants and the mix loop. Each
//! call site used to define its own copy; a constant edited in one but not the
//! other would silently invalidate every already-recorded cassette (the recomputed
//! digest stops matching the stored one), surfacing not as a build error but as a
//! baffling [`CassetteMiss`](crate::Error::CassetteMiss) at replay. Centralising
//! them here makes that drift impossible: there is one definition to change, and
//! changing it is — by definition — a cassette-format break.

/// FNV-1a 64-bit offset basis (the canonical constant).
///
/// **Do not change.** This value is baked into every cassette digest ever
/// recorded; altering it silently invalidates existing fixtures (they replay as a
/// `CassetteMiss`, not a build error).
const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64-bit prime (the canonical constant). **Do not change** — see
/// [`OFFSET`].
const PRIME: u64 = 0x0000_0100_0000_01b3;

/// An incremental 64-bit FNV-1a hash, seeded from the fixed [`OFFSET`] basis.
///
/// Encapsulating the seed is the point: a caller obtains a hasher already at the
/// correct starting state and can only fold bytes in, so it cannot accidentally
/// start from the wrong basis — the precise footgun that would silently invalidate
/// recorded cassettes. Fold bytes with [`mix`](Self::mix) in a deterministic
/// order, then read the result with [`finish`](Self::finish).
pub(crate) struct Fnv1a(u64);

impl Fnv1a {
    /// A fresh hasher seeded from the FNV-1a offset basis.
    pub(crate) fn new() -> Self {
        Fnv1a(OFFSET)
    }

    /// Fold `bytes` into the running hash: for each byte, XOR it in, then multiply
    /// by the FNV-1a prime (`wrapping_mul` — the defining 64-bit-modular step).
    pub(crate) fn mix(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    /// The digest of every byte mixed in so far.
    pub(crate) fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fold a single slice and read the digest — the common `new/mix/finish`
    /// shape.
    fn digest(bytes: &[u8]) -> u64 {
        let mut h = Fnv1a::new();
        h.mix(bytes);
        h.finish()
    }

    #[test]
    fn matches_the_canonical_fnv1a_64_test_vectors() {
        // Published FNV-1a-64 reference vectors — anchor the algorithm itself
        // (constants + mix order), independent of any call site. If these ever
        // change, the hash is no longer FNV-1a and every recorded cassette is
        // invalidated.
        assert_eq!(digest(b""), 0xcbf2_9ce4_8422_2325, "empty == offset basis");
        assert_eq!(digest(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(digest(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn mixing_is_incremental_and_order_sensitive() {
        // Folding two chunks equals folding their concatenation (one running
        // state), so the call sites' repeated `mix` calls are equivalent to
        // hashing the whole byte sequence at once...
        let split = {
            let mut h = Fnv1a::new();
            h.mix(b"foo");
            h.mix(b"bar");
            h.finish()
        };
        assert_eq!(split, digest(b"foobar"));
        // ...and byte order matters (XOR-then-multiply doesn't commute across
        // bytes), which is what lets the digests distinguish reordered inputs.
        assert_ne!(digest(b"foobar"), digest(b"raboof"));
    }
}
