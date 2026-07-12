//! The shared deadline-arbitration core: "how long until the deadline, sleep
//! that long, then try to claim the arbiter" — reused by the three deadline
//! watchdogs ([`stream::arm_stream_deadline`](super::stream), scripted's
//! `arm_scripted_deadline`, and `drive_to_exit_inner`'s deadline arm) so the
//! CAS protocol lives in exactly one place. Only the teardown that follows a
//! win (graceful/group/pid/scripted-kill, or `select!` integration) stays at
//! each call site — that part is genuinely different per site.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

// `tokio::time::Instant` (not `std::time::Instant`): the remaining budget is
// computed as `limit - started.elapsed()` and slept out via `tokio::time::sleep`
// below, so the anchor must share tokio's clock. Otherwise, under a paused
// runtime, virtual time already burned (e.g. by a readiness probe that advanced
// the clock without arming this watchdog) would not count against the limit, and
// a late arm would silently re-grant the full budget — diverging from the live
// clock the hermetic tests are meant to mirror. `sys::graceful` anchors its own
// deadline on the same clock for the same reason.
use tokio::time::Instant;

use super::{TS_PENDING, TS_TIMED_OUT};

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
pub(super) async fn wait_deadline_and_claim(
    started: Instant,
    limit: Duration,
    flag: &AtomicU8,
) -> bool {
    let remaining = limit
        .checked_sub(started.elapsed())
        .unwrap_or(Duration::ZERO);
    tokio::time::sleep(remaining).await;
    flag.compare_exchange(
        TS_PENDING,
        TS_TIMED_OUT,
        Ordering::AcqRel,
        Ordering::Relaxed,
    )
    .is_ok()
}
