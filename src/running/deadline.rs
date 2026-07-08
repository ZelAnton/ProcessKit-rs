//! The shared deadline-arbitration core: "how long until the deadline, sleep
//! that long, then try to claim the arbiter" — reused by the three deadline
//! watchdogs ([`stream::arm_stream_deadline`](super::stream), scripted's
//! `arm_scripted_deadline`, and `drive_to_exit_inner`'s deadline arm) so the
//! CAS protocol lives in exactly one place. Only the teardown that follows a
//! win (graceful/group/pid/scripted-kill, or `select!` integration) stays at
//! each call site — that part is genuinely different per site.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use super::{TS_PENDING, TS_TIMED_OUT};

/// Wait out the remaining time until `started + limit`, then try to claim the
/// timeout arbiter by CASing `flag` from `TS_PENDING` to `TS_TIMED_OUT`.
///
/// Anchored to `started` (not "now") so a late arm can't re-grant the full
/// limit. Returns `true` when this call won the race and now owns the
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
