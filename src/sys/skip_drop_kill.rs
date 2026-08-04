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
/// the latch so the newcomer is not silently spared. A spawn that is afterwards
/// *undone* — a Unix PTY launch whose master wiring fails once the child exists —
/// hands the [`DisplacedSpare`] that `clear` returned to [`restore`](Self::restore),
/// which puts the spare back only while no other spawn/adopt has re-armed the
/// backstop since.
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

/// What one [`clear`](SkipDropKill::clear) took away: the spare it displaced — if
/// the latch carried one — pinned to the generation that same `clear` installed.
///
/// Handed to [`restore`](SkipDropKill::restore) by a spawn that is being undone (the
/// Unix PTY launch whose master wiring fails after the child already exists), so the
/// failed launch does not leave the backstop re-armed for a member that never
/// joined. Both halves are read out of the one successful compare-exchange inside
/// `clear`, which is what makes the pair transactional: a second `clear` racing this
/// one cannot hand *its* caller a spare the first had already displaced.
///
/// The default — also what a `clear` of an already-armed latch returns — is
/// "nothing to restore", so restoring it is a no-op rather than a fabricated spare.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DisplacedSpare(Option<ShutdownEpoch>);

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
    ///
    /// Returns the [`DisplacedSpare`] this re-arm took away. Callers whose
    /// spawn/adopt stands ignore it — a member that joined is a member the backstop
    /// is now correctly armed for. It is read only where the spawn can still be
    /// undone (the Unix PTY rollback), which hands it to [`restore`](Self::restore).
    pub(crate) fn clear(&self) -> DisplacedSpare {
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
                // The exchange that succeeded knows both halves of the token at
                // once: whether the word it replaced still carried a spare, and the
                // exact generation it installed. Sampling either separately (an
                // `is_set()` just before, a load just after) would leave a window
                // for another `clear`, and the restore built on it could then
                // re-spare *that* spawn's fresh member.
                Ok(_) => {
                    let displaced = (cur & Self::SKIP_BIT) != 0;
                    return DisplacedSpare(displaced.then_some(ShutdownEpoch(next)));
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Put back the spare a [`clear`](Self::clear) displaced — the undo half of a
    /// spawn that is being rolled back, so a launch failure does not silently
    /// convert a `graceful_shutdown(escalate = false)` into a `Drop` hard kill of
    /// the survivors the caller chose to leave running.
    ///
    /// Race-free relative to a concurrent `spawn`/`adopt` on the same latch in
    /// exactly the sense a shutdown's own spare is, and by the same code:
    /// [`request`](Self::request)'s compare-exchange keys off the generation
    /// `displaced` carries, so a `clear` that landed in between bumped it, the
    /// exchange fails, and the newcomer keeps its Drop-kill backstop. A
    /// `DisplacedSpare` that displaced nothing restores nothing.
    // Dead on Windows — no caller there undoes a spawn through this latch — and
    // with the `pty` feature off, the K-092 asymmetric-backend shape. Allowed on
    // exactly those builds rather than cfg'd out, so the latch's protocol stays
    // readable in one piece.
    #[cfg_attr(any(windows, not(feature = "pty")), allow(dead_code))]
    pub(crate) fn restore(&self, displaced: DisplacedSpare) {
        if let Some(epoch) = displaced.0 {
            self.request(epoch);
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

    // The rollback restore (T-270): a spawn re-arms the backstop as usual, then the
    // launch that spawn belongs to fails and is undone. Putting the displaced spare
    // back is what keeps a launch failure from silently overriding a
    // `graceful_shutdown(escalate = false)` and letting `Drop` kill the survivors
    // the caller chose to leave running.
    #[test]
    fn a_rolled_back_spawn_restores_the_spare_its_clear_displaced() {
        let latch = SkipDropKill::new();
        let epoch = latch.begin_shutdown();
        latch.request(epoch); // graceful_shutdown(escalate = false)
        let displaced = latch.clear(); // the spawn re-arms the backstop
        assert!(!latch.is_set(), "the spawn's re-arm displaces the spare");
        latch.restore(displaced); // …and its rollback puts the spare back
        assert!(
            latch.is_set(),
            "a rolled-back spawn must restore the spare its re-arm displaced"
        );
    }

    // The other side of the same rule: a rollback restores only what its own clear
    // took away, so undoing a spawn into an ordinary (never-spared) group leaves
    // Drop's kill armed rather than inventing a spare.
    #[test]
    fn a_rollback_creates_no_spare_its_clear_never_displaced() {
        let latch = SkipDropKill::new();
        let displaced = latch.clear(); // a spawn into a group nothing had spared
        latch.restore(displaced);
        assert!(
            !latch.is_set(),
            "a rollback must not spare a group no shutdown ever spared"
        );
    }

    // The transactional half (the race the generation guard is reused for): another
    // spawn/adopt joined the same group between the rolled-back spawn's `clear` and
    // its `restore`. That newcomer is a live member nothing chose to spare, so the
    // restore must lose — exactly as a stale shutdown `request` does.
    #[test]
    fn a_spawn_between_the_clear_and_the_restore_defeats_it() {
        let latch = SkipDropKill::new();
        let epoch = latch.begin_shutdown();
        latch.request(epoch);
        let displaced = latch.clear(); // the spawn that is about to be rolled back
        latch.clear(); // a second spawn/adopt joins the same group
        latch.restore(displaced);
        assert!(
            !latch.is_set(),
            "a spawn that re-armed the backstop after the rolled-back one must keep \
             it armed — restoring the older spare would silently strip the newcomer"
        );
    }

    // Why the token is minted *by* the clear rather than read around it: only the
    // clear that actually displaced the spare carries one. A second clear (the
    // shape two concurrent spawns take) gets an empty token, so it can never put a
    // spare back over the member the first one re-armed for.
    #[test]
    fn only_the_clear_that_displaced_the_spare_carries_it() {
        let latch = SkipDropKill::new();
        let epoch = latch.begin_shutdown();
        latch.request(epoch);
        let first = latch.clear();
        let second = latch.clear();
        latch.restore(second);
        assert!(
            !latch.is_set(),
            "a clear that displaced no spare must restore none"
        );
        latch.restore(first);
        assert!(
            !latch.is_set(),
            "and the clear that did displace it is by then out of date"
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

    /// The rolled-back spawn's restore under the same race (T-270): one spawn
    /// re-armed the backstop and is now being undone, while a second `spawn`/`adopt`
    /// joins the same group concurrently. Across every interleaving the group must
    /// end with the backstop **armed** — the newcomer is a live member nothing chose
    /// to spare, and the restore may only take while the generation it captured
    /// still stands.
    #[test]
    fn a_concurrent_spawn_defeats_a_rolled_back_spawns_restore() {
        loom::model(|| {
            let latch = Arc::new(SkipDropKill::new());
            // A group a non-escalating shutdown spared…
            let epoch = latch.begin_shutdown();
            latch.request(epoch);
            // …then the spawn that is about to be rolled back re-arms the backstop.
            let displaced = latch.clear();

            // Its rollback's restore races a fresh spawn/adopt into the same group.
            let rollback = {
                let latch = latch.clone();
                loom::thread::spawn(move || latch.restore(displaced))
            };
            latch.clear();
            rollback.join().unwrap();

            assert!(
                !latch.is_set(),
                "a rolled-back spawn's restore re-spared a group a concurrent \
                 spawn/adopt had already re-armed the backstop for"
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
