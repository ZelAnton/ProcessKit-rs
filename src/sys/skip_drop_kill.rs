//! [`SkipDropKill`] — the generation-guarded "don't kill on Drop" latch shared by
//! every backend, hardened against the spawn/shutdown re-arm race (T-079).
//!
//! Split into its own file (from `sys/mod.rs`) so the standalone loom harness
//! (`loom/`) can `#[path]`-include just this pure core and model-check the re-arm
//! race — it uses only the `cfg(loom)`-swappable [`crate::sync`] layer, nothing
//! from tokio or the platform backends.

use crate::sync::atomic::{AtomicUsize, Ordering};

/// A "don't kill on Drop" latch shared by every backend, hardened against the
/// spawn/shutdown re-arm race.
///
/// `graceful_shutdown(escalate = false)` [`request`](Self::request)s the latch to
/// spare the survivors the caller chose to leave running; each backend's `Drop`
/// [reads](Self::is_set) it and skips the hard kill when the latch is set.
/// Spawning or adopting a fresh child into a reused group [`clear`](Self::clear)s
/// the latch so the newcomer is not silently spared.
///
/// # The re-arm race this guards
///
/// A non-escalating `graceful_shutdown` runs *concurrently* with `spawn`/`adopt`:
/// it can be mid-poll (or merely between deciding to spare and finishing) while a
/// fresh child is spawned into the same group and re-arms the backstop. A tokio
/// task can migrate across threads at every `.await`, so the shutdown's final
/// `request()` can land **after** the spawn's `clear()`. A plain boolean would let
/// that stale `request()` re-set the skip flag and silently strip the fresh child
/// of its Drop-kill backstop — the exact orphan-leak this type exists to prevent.
///
/// The guard is a **generation counter packed with the skip flag into one atomic
/// word**: bit 0 is the skip flag, bits `1..` are the generation. Every `clear()`
/// bumps the generation and clears the skip bit; a shutdown snapshots the
/// generation up front with [`begin_shutdown`](Self::begin_shutdown) and its later
/// [`request`](Self::request) spares the survivors **only if the generation is
/// unchanged**, expressed as a single compare-exchange. A `clear()` that raced the
/// shutdown has bumped the generation, so the stale `request` compare-exchange
/// fails and the backstop stays armed for the new child. Because the flag and the
/// generation live in one word, the check and the set are indivisible — no store
/// can slip between them (the flaw a separate flag + counter would reintroduce on
/// weakly-ordered hardware).
///
/// Centralizing this keeps the load-bearing memory ordering correct in one place:
/// the `Release` stores pair with the `Acquire` load so the decision — and the
/// generation it is keyed to — is visible to whichever thread runs `Drop`.
// The packed word is built from the crate's `cfg(loom)`-swappable sync layer
// (`std::sync::atomic` in ordinary builds, loom's model under the standalone loom
// harness) so the `skip_drop_kill_loom_model` suite below can permute the re-arm
// race this latch guards. `Default` is hand-written as `new()` rather than derived:
// loom's atomics do not guarantee a `Default` impl, and the two are byte-for-byte
// identical (a zeroed word — generation 0, skip clear). See `crate::sync`.
#[derive(Debug)]
pub(crate) struct SkipDropKill(AtomicUsize);

impl Default for SkipDropKill {
    fn default() -> Self {
        Self::new()
    }
}

/// The re-arm generation snapshotted at the **start** of a non-escalating
/// shutdown, handed back to [`SkipDropKill::request`] so a spawn/adopt that
/// re-armed the backstop in the meantime wins over the shutdown's stale spare.
/// Opaque: only [`begin_shutdown`](SkipDropKill::begin_shutdown) mints one and only
/// [`request`](SkipDropKill::request) consumes it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ShutdownEpoch(usize);

impl SkipDropKill {
    /// Bit 0 of the packed word: set means `Drop` must skip its hard kill.
    const SKIP_BIT: usize = 1;
    /// Added to the packed word to bump the generation (bits `1..`) by one
    /// without disturbing the skip bit.
    const GEN_STEP: usize = Self::SKIP_BIT << 1;

    /// A fresh latch — generation 0, `Drop` will hard-kill until
    /// [`request`](Self::request).
    pub(crate) fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// Snapshot the re-arm generation at the **start** of a non-escalating
    /// shutdown, before it signals or polls the tree. Hand the returned
    /// [`ShutdownEpoch`] to [`request`](Self::request) once the shutdown finishes:
    /// a `spawn`/`adopt` that [`clear`](Self::clear)s the latch after this snapshot
    /// bumps the generation, so the later `request` no-ops and the fresh child
    /// keeps its Drop-kill backstop.
    ///
    /// The skip bit is masked out of the snapshot so the epoch names the *armed*
    /// state at the current generation — precisely the state `request`
    /// compare-exchanges away from. `Acquire` pairs with the `Release` stores.
    pub(crate) fn begin_shutdown(&self) -> ShutdownEpoch {
        ShutdownEpoch(self.0.load(Ordering::Acquire) & !Self::SKIP_BIT)
    }

    /// Mark that `Drop` must **not** hard-kill the survivors — but only if no
    /// `spawn`/`adopt` re-armed the backstop since `epoch` was taken. Implemented
    /// as one compare-exchange from "armed at the snapshot generation" to "spared
    /// at that generation": if a racing [`clear`](Self::clear) bumped the
    /// generation (or already spared it), the exchange fails and the latch is left
    /// in the already-correct state — armed at a newer generation, so the fresh
    /// child is still torn down on Drop. `Release` (on success) pairs with the
    /// `Acquire` in [`is_set`](Self::is_set).
    pub(crate) fn request(&self, epoch: ShutdownEpoch) {
        // A failed exchange is the point of the guard (a concurrent re-arm won),
        // so the result is deliberately ignored — on failure the latch is already
        // correct.
        let _ = self.0.compare_exchange(
            epoch.0,
            epoch.0 | Self::SKIP_BIT,
            Ordering::Release,
            Ordering::Relaxed,
        );
    }

    /// Re-arm `Drop`'s hard kill for a reused group and **bump the generation** so
    /// an in-flight non-escalating shutdown's later [`request`](Self::request)
    /// cannot re-spare the fresh member. Spawning/adopting a child into a group
    /// that was gracefully shut down with `escalate = false` calls this so the
    /// child (and the rest of the reused group) is not silently spared by a stale
    /// latch — a group left with the latch set but never reused keeps its spared
    /// survivors. `Release` pairs with the `Acquire` in [`is_set`](Self::is_set).
    pub(crate) fn clear(&self) {
        // Bump the generation and clear the skip bit as one atomic step. The CAS
        // loop composes with concurrent `clear`s (each retries against the other's
        // bump) and with a racing `request` (whose compare-exchange keys off the
        // exact generation this steps past). The generation wraps harmlessly after
        // `usize::MAX >> 1` re-arms; an ABA there would need that many spawns
        // inside a single shutdown's window, which is not reachable — hence
        // `wrapping_add` rather than a checked add that could panic in a debug
        // build.
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let next = cur.wrapping_add(Self::GEN_STEP) & !Self::SKIP_BIT;
            match self
                .0
                .compare_exchange_weak(cur, next, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Whether `Drop` should skip the kill. `Acquire` pairs with the `Release`
    /// in [`request`](Self::request) / [`clear`](Self::clear).
    pub(crate) fn is_set(&self) -> bool {
        (self.0.load(Ordering::Acquire) & Self::SKIP_BIT) != 0
    }
}

// These deterministic single-thread tests construct a `SkipDropKill` and drive it
// directly; under `--cfg loom` its packed word is a loom model usable only inside
// `loom::model`, so this suite is compiled out there and the
// exhaustive-interleaving equivalents live in `skip_drop_kill_loom_model` below.
#[cfg(all(test, not(loom)))]
mod skip_drop_kill_tests {
    use super::SkipDropKill;

    #[test]
    fn request_then_clear_re_arms_the_drop_kill() {
        let latch = SkipDropKill::new();
        assert!(!latch.is_set(), "a fresh latch does not skip Drop's kill");
        let epoch = latch.begin_shutdown();
        latch.request(epoch);
        assert!(latch.is_set(), "request() spares survivors on Drop");
        // Spawning into a reused group calls clear() so a fresh child is not
        // spared by the stale latch.
        latch.clear();
        assert!(
            !latch.is_set(),
            "clear() re-arms Drop's kill for a reused group"
        );
    }

    // The re-arm race (T-079): a spawn/adopt that clears the latch AFTER a
    // non-escalating shutdown snapshotted its generation but BEFORE that shutdown's
    // `request` lands must win — the stale request must not silently re-spare the
    // fresh child. This is the single-word generation guard doing its job; a plain
    // boolean latch would fail this.
    #[test]
    fn a_stale_request_does_not_override_a_concurrent_clear() {
        let latch = SkipDropKill::new();
        // A live reused group: an earlier spawn armed the backstop.
        latch.clear();
        // A non-escalating shutdown begins and snapshots the generation…
        let epoch = latch.begin_shutdown();
        // …then a concurrent spawn/adopt re-arms the backstop for a fresh child…
        latch.clear();
        // …and only now does the shutdown's (stale) request land.
        latch.request(epoch);
        assert!(
            !latch.is_set(),
            "a stale non-escalating request must not re-spare a child that a \
             concurrent spawn/adopt already re-armed the backstop for"
        );
    }

    // Without an intervening clear, the shutdown's request still spares survivors:
    // the generation is unchanged, so the compare-exchange succeeds. A later
    // spawn/adopt then re-arms the backstop for the newcomer, as before.
    #[test]
    fn request_spares_survivors_when_no_clear_intervenes() {
        let latch = SkipDropKill::new();
        latch.clear(); // a spawned survivor
        let epoch = latch.begin_shutdown();
        latch.request(epoch);
        assert!(
            latch.is_set(),
            "an unraced non-escalating request spares survivors"
        );
        latch.clear();
        assert!(
            !latch.is_set(),
            "a spawn after the spare re-arms Drop's kill"
        );
    }

    // Generations are monotonic: an epoch captured before a `clear` can never
    // match again, so its request stays a no-op even as a *fresh* epoch at the new
    // generation spares normally.
    #[test]
    fn a_stale_epoch_never_matches_a_later_generation() {
        let latch = SkipDropKill::new();
        let stale = latch.begin_shutdown();
        latch.clear(); // generation advances past `stale`
        let fresh = latch.begin_shutdown();
        latch.request(stale);
        assert!(
            !latch.is_set(),
            "a stale epoch cannot spare at a newer generation"
        );
        latch.request(fresh);
        assert!(
            latch.is_set(),
            "a current-generation request spares as usual"
        );
    }
}

/// Loom model-checking suite for the [`SkipDropKill`] re-arm race (run under
/// `--cfg loom` by the standalone `loom/` harness; see [`crate::sync`]).
///
/// The latch packs a generation counter and a skip flag into one word so a
/// non-escalating shutdown's `begin_shutdown` snapshot + `request` and a
/// concurrent `spawn`/`adopt`'s `clear` compose race-free (T-079). The
/// single-thread `skip_drop_kill_tests` above sequence those operations by hand;
/// loom instead **exhaustively** permutes the true concurrency — every
/// interleaving and every permitted memory ordering — and fails if a fresh
/// child's Drop-kill backstop is ever silently stripped, or if the CAS-loop
/// livelocks.
#[cfg(all(test, loom))]
mod skip_drop_kill_loom_model {
    use super::SkipDropKill;
    use loom::sync::Arc;

    /// The T-079 orphan-leak guard: a `spawn`/`adopt` that re-arms the backstop
    /// (`clear`) after a non-escalating shutdown snapshotted its generation must
    /// win over that shutdown's now-stale `request`. Modeled as a fresh child that
    /// clears the latch and immediately checks its own backstop while the stale
    /// `request` races: across every interleaving the child must never observe
    /// itself spared — its `clear` bumped the generation past the epoch the
    /// `request` keys on, so the stale spare can never take.
    #[test]
    fn a_stale_request_never_re_spares_a_freshly_cleared_child() {
        loom::model(|| {
            let latch = Arc::new(SkipDropKill::new());
            // A live reused group: an earlier spawn armed the backstop (generation 1).
            latch.clear();
            // A non-escalating shutdown begins and snapshots the generation.
            let epoch = latch.begin_shutdown();

            // The shutdown's (soon-to-be-stale) request races on another thread.
            let shutdown = {
                let latch = latch.clone();
                loom::thread::spawn(move || latch.request(epoch))
            };

            // A concurrent spawn/adopt re-arms the backstop for a fresh child, then
            // that child reads its own backstop. It must never see itself spared.
            latch.clear();
            assert!(
                !latch.is_set(),
                "a stale non-escalating request re-spared a child that a fresh \
                 spawn/adopt had already re-armed the backstop for"
            );

            shutdown.join().unwrap();
            // And after everything settles the fresh child is still torn down.
            assert!(
                !latch.is_set(),
                "the freshly re-armed backstop must survive the stale request"
            );
        });
    }

    /// Two concurrent `spawn`/`adopt`s re-arm the same reused group at once. The
    /// generation-bump CAS loop must compose (terminate, no livelock) and leave the
    /// backstop armed — neither `clear` may leave the skip bit set.
    #[test]
    fn concurrent_clears_leave_the_backstop_armed() {
        loom::model(|| {
            let latch = Arc::new(SkipDropKill::new());

            let other = {
                let latch = latch.clone();
                loom::thread::spawn(move || latch.clear())
            };
            latch.clear();
            other.join().unwrap();

            assert!(
                !latch.is_set(),
                "two racing re-arms must leave Drop's kill armed"
            );
        });
    }
}
