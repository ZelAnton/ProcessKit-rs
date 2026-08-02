//! [`ShutdownReport`] — the observed facts of a graceful
//! [`ProcessGroup`](crate::ProcessGroup) teardown.

use std::time::Duration;

use crate::signal::Signal;
use crate::sys::graceful::{GracefulOutcome, SoftDelivery};

/// The fate of a graceful teardown's best-effort **soft-signal** tier — what the
/// kernel actually observed of the polite "please exit" request
/// [`ProcessGroup::stop`](crate::ProcessGroup::stop) issues before the grace
/// window, as opposed to what it *tried* to do.
///
/// The soft signal is a `SIGTERM` (the graceful signal; [`Signal::Term`]) on the
/// Unix mechanisms, and a `CTRL_BREAK`/`WM_CLOSE` trigger on the Windows soft tier.
/// It is deliberately **not** the hard kill: escalation to `SIGKILL` /
/// `cgroup.kill` / `TerminateJobObject` is reported separately by
/// [`ShutdownReport::escalated`].
///
/// # No string format
///
/// This is an accessor/variant type, not a rendered string — match on it (or read
/// [`ShutdownReport::attempted_signal`]) rather than parsing a `Debug`/`Display`
/// form, which is not a stability contract.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SoftSignal {
    /// The soft signal was delivered best-effort to the tree: a POSIX signal on the
    /// Unix mechanisms, or at least one `CTRL_BREAK`/`WM_CLOSE` trigger that reached
    /// a live member on the Windows soft tier. Carries the [`Signal`] attempted.
    ///
    /// "Best-effort" is exact: a member that ignores the signal and keeps running is
    /// still counted as *sent to* — whether the tree then drained within the grace
    /// is [`ShutdownReport::drained_within_grace`], not this.
    Sent(Signal),
    /// This platform has **no soft-signal tier** for the group, so the teardown
    /// could only hard-kill (or spare) — there was nothing polite to send. The one
    /// case: a **windowless Windows Job Object with no console-CTRL leader** (no
    /// member opted into
    /// [`Command::windows_graceful_ctrl_break`](crate::Command::windows_graceful_ctrl_break)
    /// and no member owns a top-level window), whose kill is atomic. Every Unix
    /// mechanism always has a real `SIGTERM` tier, so this never arises there.
    Unsupported,
    /// A soft-signal tier exists but the best-effort delivery **failed for every
    /// target**: a uid-changed (`sudo`/setuid) member that rejected the signal with
    /// `EPERM` on Unix, or — on the Windows soft tier — no live console leader /
    /// member window remained to receive the trigger by the time it was posted.
    /// Carries the [`Signal`] that could not be delivered. The teardown proceeded to
    /// its grace/escalation regardless.
    Failed(Signal),
}

/// The observed facts of one graceful group teardown, returned by
/// [`ProcessGroup::stop`](crate::ProcessGroup::stop).
///
/// Where the fire-and-forget [`shutdown`](crate::ProcessGroup::shutdown) /
/// [`shutdown_ref`](crate::ProcessGroup::shutdown_ref) report only success or an
/// error, this carries what the teardown **actually observed**: which soft signal
/// was attempted and whether it landed, how many members were alive before and
/// after, whether the tree drained within the grace or had to be hard-killed, and
/// how long it really took. A consumer that owns its own end-of-run race (its
/// deadline is not [`Command::timeout`](crate::Command::timeout) but a
/// timeout ⨯ Ctrl-C ⨯ control-socket race) can report the *observed* tier instead
/// of re-deriving it, and stop waiting the instant the tree is empty rather than
/// always spending the whole grace.
///
/// # Point-in-time member counts
///
/// [`members_before`](Self::members_before) / [`members_after`](Self::members_after)
/// count the same member set the group's
/// [`members`](crate::ProcessGroup::members) reports — the whole tree on the
/// Windows Job Object, Linux cgroup and FreeBSD process-reaper mechanisms, the
/// tracked group **leaders** on the POSIX process-group fallback (macOS/the other
/// BSDs and Linux without a usable cgroup).
/// Each is `None` only if that membership read failed (an unreadable `cgroup.procs`,
/// a failed Job Object query), never a fabricated `0`.
///
/// On the process-group fallback an **unreaped zombie still counts as a member**
/// (its process-group entry survives until the child is `wait`ed), so a tree
/// hard-killed with `SIGKILL` can still report a non-zero
/// [`members_after`](Self::members_after) until those exits are reaped — the same
/// reaping caveat [`shutdown`](crate::ProcessGroup::shutdown) documents. The
/// mechanisms that see an exit directly (`cgroup.procs`, the Job Object, and the
/// reaper listing, whose zombie flag this crate never counts as a live member)
/// drop a process on exit, before reaping.
///
/// # Non-exhaustive, accessor-only
///
/// A read-only snapshot the crate produces: non-exhaustive so new facts can be
/// added without a breaking change, and each fact is exposed through a method
/// (documenting its own platform caveats) rather than a public field. There is
/// **no** string format to parse as a contract.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    soft_signal: SoftSignal,
    members_before: Option<usize>,
    members_after: Option<usize>,
    drained_within_grace: bool,
    escalated: bool,
    elapsed: Duration,
}

impl ShutdownReport {
    /// Assemble a report from the internal driver outcome and the graceful signal
    /// that was attempted. Called only by
    /// [`ProcessGroup::stop`](crate::ProcessGroup::stop), which pins `signal` to the
    /// group's graceful signal (`SIGTERM`).
    pub(crate) fn from_outcome(outcome: GracefulOutcome, signal: Signal) -> Self {
        let soft_signal = match outcome.soft {
            SoftDelivery::Sent => SoftSignal::Sent(signal),
            SoftDelivery::Unsupported => SoftSignal::Unsupported,
            SoftDelivery::Failed => SoftSignal::Failed(signal),
        };
        Self {
            soft_signal,
            members_before: outcome.members_before,
            members_after: outcome.members_after,
            drained_within_grace: outcome.drained,
            escalated: outcome.escalated,
            elapsed: outcome.elapsed,
        }
    }

    /// The fate of the best-effort soft-signal tier: [`Sent`](SoftSignal::Sent),
    /// [`Unsupported`](SoftSignal::Unsupported), or [`Failed`](SoftSignal::Failed)
    /// (see [`SoftSignal`]). Distinct from the hard kill — see
    /// [`escalated`](Self::escalated).
    pub fn soft_signal(&self) -> SoftSignal {
        self.soft_signal
    }

    /// The soft [`Signal`] the teardown attempted, or `None` where the platform has
    /// no soft-signal tier ([`SoftSignal::Unsupported`] — a windowless Windows Job
    /// Object with no console-CTRL leader). A convenience over
    /// [`soft_signal`](Self::soft_signal): `Some` for both a delivered and a failed
    /// attempt (use [`soft_signal`](Self::soft_signal) to tell those apart), `None`
    /// only when nothing soft could be sent at all.
    pub fn attempted_signal(&self) -> Option<Signal> {
        match self.soft_signal {
            SoftSignal::Sent(signal) | SoftSignal::Failed(signal) => Some(signal),
            SoftSignal::Unsupported => None,
        }
    }

    /// How many members were alive **before** the soft signal, or `None` if the
    /// membership could not be read. See the type-level note on which member set
    /// this counts and the process-group zombie caveat.
    pub fn members_before(&self) -> Option<usize> {
        self.members_before
    }

    /// How many members were still alive **after** the grace window and any hard
    /// kill, or `None` if the membership could not be read. A non-`escalate` stop
    /// reports the survivors it spared here; on the process-group fallback a
    /// `SIGKILL`'d tree can still count unreaped zombies (see the type-level note).
    pub fn members_after(&self) -> Option<usize> {
        self.members_after
    }

    /// Whether the tree **drained within the grace window**, before any hard kill —
    /// every member exited in response to the soft signal in time. `false` means the
    /// grace elapsed with survivors still alive (they were then hard-killed when
    /// `escalate` was set, or spared when it was not), or that there was no soft
    /// tier to drain on (a windowless Windows Job Object, unless it was already
    /// empty).
    pub fn drained_within_grace(&self) -> bool {
        self.drained_within_grace
    }

    /// Whether the teardown **escalated to a hard kill** (`SIGKILL` / `cgroup.kill`
    /// / `TerminateJobObject`) because the tree had not drained within the grace and
    /// `escalate` was set. `false` for a tree that drained in time, for a
    /// non-escalating stop that spared its survivors, and for an already-empty group
    /// (nothing to kill).
    pub fn escalated(&self) -> bool {
        self.escalated
    }

    /// How long the teardown **actually took** — from issuing the soft signal to the
    /// final drain/kill decision. An early drain reports a short duration (it does
    /// not spend the whole grace); a tree that rides out the grace reports roughly
    /// the grace plus the escalation. Wall-clock in production; under a paused tokio
    /// test runtime it tracks virtual time.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}
