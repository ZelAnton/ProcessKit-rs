//! The shared deadline-arbitration core: "how long until the deadline, sleep
//! that long, then try to claim the arbiter" — reused by the three deadline
//! watchdogs ([`stream::arm_stream_deadline`](super::stream), scripted's
//! `arm_scripted_deadline`, and `drive_to_exit_inner`'s deadline arm) so the
//! CAS protocol lives in exactly one place. Only the teardown that follows a
//! win (graceful/group/pid/scripted-kill, or `select!` integration) stays at
//! each call site — that part is genuinely different per site.
//!
//! Every terminal claimant of the single-word arbiter is funnelled through the
//! helpers here — [`claim_timed_out`] (absolute deadline),
//! [`claim_inactivity_timed_out`] (quiet output), and [`claim_exited`] (natural
//! reap) — so the CAS from `TS_PENDING` and its memory ordering live in one
//! place, and the `loom_model` suite can exhaustively check that only one wins.

// The arbiter atomic comes from the crate's `cfg(loom)`-swappable sync layer
// (`std::sync::atomic` in ordinary builds, loom's model under the standalone loom
// harness) so `loom_model` below can permute the deadline-vs-reap CAS race. See
// `crate::sync`.
use crate::sync::atomic::{AtomicU8, Ordering};
// The wait side is the arbiter's only impure part (tokio's clock + sleep); the
// loom models exercise only the pure `claim_*` CAS below, so this and
// `wait_deadline_and_claim` are `cfg(not(loom))` — that lets the standalone loom
// harness (`loom/`) `#[path]`-include this file without tokio.
#[cfg(not(loom))]
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`): the remaining budget is
// computed as `limit - started.elapsed()` and slept out via `tokio::time::sleep`
// below, so the anchor must share tokio's clock. Otherwise, under a paused
// runtime, virtual time already burned (e.g. by a readiness probe that advanced
// the clock without arming this watchdog) would not count against the limit, and
// a late arm would silently re-grant the full budget — diverging from the live
// clock the hermetic tests are meant to mirror. `sys::graceful` anchors its own
// deadline on the same clock for the same reason.
#[cfg(not(loom))]
use tokio::time::Instant;

use super::{TS_EXITED, TS_INACTIVITY_TIMED_OUT, TS_PENDING, TS_TIMED_OUT};

/// Wait out the remaining time until `started + limit`, then try to claim the
/// timeout arbiter by CASing `flag` from `TS_PENDING` to `TS_TIMED_OUT`.
///
/// Anchored to `started` (not "now") so a late arm can't re-grant the full
/// limit. `started` is a [`tokio::time::Instant`] so that remaining-budget
/// arithmetic shares the clock the `sleep` below runs on (see the import note).
/// Returns `true` when this call won the race and now owns the
/// "timed out vs exited" decision (the caller may proceed to kill/teardown
/// and publish `TimedOut`); `false` when a natural reap already claimed the
/// arbiter first — the caller must not kill the child or publish its own
/// outcome.
#[cfg(not(loom))]
pub(crate) async fn wait_deadline_and_claim(
    started: Instant,
    limit: Duration,
    flag: &AtomicU8,
) -> bool {
    let remaining = limit
        .checked_sub(started.elapsed())
        .unwrap_or(Duration::ZERO);
    tokio::time::sleep(remaining).await;
    claim_timed_out(flag)
}

/// Claim the timeout arbiter for a **fired deadline**: `compare_exchange` `flag`
/// from `TS_PENDING` to `TS_TIMED_OUT`. Returns `true` when this call won the
/// arbiter (the caller owns the "timed out vs exited" decision and may kill and
/// publish `TimedOut`); `false` when a natural reap already claimed it — the
/// caller must not kill or publish. The winning side is unique: at most one of
/// `claim_timed_out` / [`claim_exited`] can succeed from `TS_PENDING`.
///
/// `AcqRel` on success / `Relaxed` on failure: the release publishes the decision
/// to the finisher that later reads the arbiter (`classify_timed_out`), and the
/// acquire pairs with the losing side's release.
pub(super) fn claim_timed_out(flag: &AtomicU8) -> bool {
    flag.compare_exchange(
        TS_PENDING,
        TS_TIMED_OUT,
        Ordering::AcqRel,
        Ordering::Relaxed,
    )
    .is_ok()
}

/// Claim the shared watchdog arbiter for a fired output-inactivity deadline.
/// It races the absolute deadline and natural reap on the same `PENDING` word;
/// only the first terminal cause may own teardown and classification.
pub(super) fn claim_inactivity_timed_out(flag: &AtomicU8) -> bool {
    flag.compare_exchange(
        TS_PENDING,
        TS_INACTIVITY_TIMED_OUT,
        Ordering::AcqRel,
        Ordering::Relaxed,
    )
    .is_ok()
}

/// Claim the timeout arbiter for a **natural reap**: `compare_exchange` `flag`
/// from `TS_PENDING` to `TS_EXITED`. Returns `true` when this call won (a clean
/// exit is recorded); `false` when a fired deadline already claimed
/// `TS_TIMED_OUT` first — the run stays `TimedOut`. The counterpart to
/// [`claim_timed_out`]; the two race on the same word and exactly one can win.
///
/// Same `AcqRel`/`Relaxed` ordering and rationale as [`claim_timed_out`].
pub(crate) fn claim_exited(flag: &AtomicU8) -> bool {
    flag.compare_exchange(TS_PENDING, TS_EXITED, Ordering::AcqRel, Ordering::Relaxed)
        .is_ok()
}

/// Loom model-checking suite for the deadline arbiter (run under `--cfg loom`;
/// see [`crate::sync`]).
///
/// The arbiter is a single `AtomicU8` both watchdogs and the natural reap race on:
/// each terminal cause claims its own state from `TS_PENDING`. Loom exhaustively
/// permutes that race and asserts the two
/// core invariants: **no double claim** (at most one side wins, so exactly one of
/// "kill + publish TimedOut" and "record a clean exit" ever fires — never both),
/// and **no missed timeout** (with no reap, the deadline claim always wins, so a
/// real timeout is never silently swallowed). A finisher's later
/// `classify`-style acquire load always observes the unique winner's value.
#[cfg(all(test, loom))]
mod loom_model {
    use super::{
        TS_EXITED, TS_INACTIVITY_TIMED_OUT, TS_PENDING, TS_TIMED_OUT, claim_exited,
        claim_inactivity_timed_out, claim_timed_out,
    };
    use crate::sync::atomic::{AtomicU8, Ordering};
    use loom::sync::Arc;

    /// The deadline claim and the natural-reap claim race on the fresh arbiter.
    /// Exactly one wins; the loser observes the winner's terminal state, and a
    /// finisher's acquire load agrees — never a torn or `PENDING` read.
    #[test]
    fn a_fired_deadline_and_a_natural_reap_never_both_claim() {
        loom::model(|| {
            let flag = Arc::new(AtomicU8::new(TS_PENDING));

            let reaper = {
                let flag = flag.clone();
                loom::thread::spawn(move || claim_exited(&flag))
            };
            let deadline_won = claim_timed_out(&flag);
            let exit_won = reaper.join().unwrap();

            assert!(
                deadline_won ^ exit_won,
                "exactly one of the deadline / reap claims must win the arbiter \
                 (won: deadline={deadline_won}, exit={exit_won})"
            );
            // Whoever lost, the finisher's classify-time acquire load sees the
            // unique winner's terminal state — never `TS_PENDING`, never both.
            let final_state = flag.load(Ordering::Acquire);
            let expected = if deadline_won {
                TS_TIMED_OUT
            } else {
                TS_EXITED
            };
            assert_eq!(
                final_state, expected,
                "the arbiter must settle on the winner's state"
            );
        });
    }

    /// No missed timeout: when nothing reaps (the child is still running at the
    /// deadline), the deadline claim always wins `TS_PENDING → TS_TIMED_OUT`, so
    /// the timeout is enforced rather than silently dropped.
    #[test]
    fn a_fired_deadline_claims_when_no_reap_competes() {
        loom::model(|| {
            let flag = AtomicU8::new(TS_PENDING);
            assert!(
                claim_timed_out(&flag),
                "an uncontested deadline claim must win and enforce the timeout"
            );
            assert_eq!(flag.load(Ordering::Acquire), TS_TIMED_OUT);
        });
    }

    /// Absolute timeout, inactivity timeout, and natural reap share one winner.
    /// This is the full production race: no two teardown paths may both publish
    /// an outcome or act on the same pid.
    #[test]
    fn both_watchdogs_and_reap_have_exactly_one_winner() {
        loom::model(|| {
            let flag = Arc::new(AtomicU8::new(TS_PENDING));
            let deadline = {
                let flag = flag.clone();
                loom::thread::spawn(move || claim_timed_out(&flag))
            };
            let inactivity = {
                let flag = flag.clone();
                loom::thread::spawn(move || claim_inactivity_timed_out(&flag))
            };
            let exit_won = claim_exited(&flag);
            let deadline_won = deadline.join().unwrap();
            let inactivity_won = inactivity.join().unwrap();

            assert_eq!(
                usize::from(deadline_won) + usize::from(inactivity_won) + usize::from(exit_won),
                1,
                "exactly one terminal cause must claim the arbiter"
            );
            let expected = if deadline_won {
                TS_TIMED_OUT
            } else if inactivity_won {
                TS_INACTIVITY_TIMED_OUT
            } else {
                TS_EXITED
            };
            assert_eq!(flag.load(Ordering::Acquire), expected);
        });
    }

    #[test]
    fn fired_inactivity_claims_when_uncontested() {
        loom::model(|| {
            let flag = AtomicU8::new(TS_PENDING);
            assert!(claim_inactivity_timed_out(&flag));
            assert_eq!(flag.load(Ordering::Acquire), TS_INACTIVITY_TIMED_OUT);
        });
    }
}
