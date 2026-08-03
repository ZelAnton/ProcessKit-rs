//! [`ShutdownReport`] — the observed facts of a graceful
//! [`ProcessGroup`](crate::ProcessGroup) teardown.

use std::time::Duration;

#[cfg(feature = "report-serde")]
use serde::ser::{Serialize, SerializeStruct as _, Serializer};

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
/// # No rendered string format
///
/// This is an accessor/variant type — match on it (or read
/// [`ShutdownReport::attempted_signal`]) rather than parsing a `Debug`/`Display`
/// form, which is not a stability contract. Its **stable machine identifier**
/// for machine-readable output is [`name`](Self::name), which *is* one.
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

impl SoftSignal {
    /// This fate's **stable machine identifier** — a short, lowercase
    /// `snake_case` string (`"sent"`, `"unsupported"`, `"failed"`), part of the
    /// crate's compatibility surface.
    ///
    /// Use it for machine-readable output — a CLI's JSONL schema, a
    /// cross-language binding, a structured log field — where a consumer needs
    /// one canonical spelling per fate instead of hand-maintaining its own
    /// mapping table. It is a *diagnostic* name — a stable **vocabulary**
    /// rather than a frozen record schema — and the exact string the opt-in
    /// `report-serde` feature serializes this fate as. It is held stable
    /// either way: a **new** variant gets a **new**
    /// identifier, and an existing identifier is **never renamed** without a
    /// major release.
    ///
    /// This names the fate **only**; the [`Signal`] a
    /// [`Sent`](Self::Sent)/[`Failed`](Self::Failed) attempt carries travels
    /// separately (via [`ShutdownReport::attempted_signal`], or the variant's
    /// own field). There is deliberately no `from_name` inverse — like
    /// [`Outcome::name`](crate::Outcome::name), this is a fate the crate
    /// *reports* after a teardown, never one supplied to it from outside.
    #[must_use]
    pub fn name(&self) -> &'static str {
        // Exhaustive (no `_` arm) though the enum is `#[non_exhaustive]`: within
        // the defining crate a new variant is a compile error here, so it can
        // never silently ship without a stable identifier.
        match self {
            SoftSignal::Sent(_) => "sent",
            SoftSignal::Unsupported => "unsupported",
            SoftSignal::Failed(_) => "failed",
        }
    }

    /// The [`Signal`] this fate concerns — `Some` for both a delivered and a
    /// failed attempt, `None` for [`Unsupported`](Self::Unsupported), where
    /// nothing soft could be sent at all.
    ///
    /// The single source behind [`ShutdownReport::attempted_signal`] (and the
    /// `report-serde` wire form), so the two can never disagree about which
    /// attempt a fate describes.
    pub(crate) fn signal(&self) -> Option<Signal> {
        // Exhaustive (no `_` arm), same reasoning as `name`.
        match self {
            SoftSignal::Sent(signal) | SoftSignal::Failed(signal) => Some(*signal),
            SoftSignal::Unsupported => None,
        }
    }
}

/// *(feature `report-serde`)* Serialized as a tagged object — the stable
/// [`name()`](SoftSignal::name) identifier under `"kind"`, plus the [`Signal`]
/// the fate concerns:
///
/// ```json
/// {"kind": "sent",        "signal": "term"}
/// {"kind": "failed",      "signal": "term"}
/// {"kind": "unsupported", "signal": null}
/// ```
///
/// The tag is the published identifier, not a serde-derived variant tag; the
/// signal is `null` exactly where the platform had no soft tier to attempt one
/// on, never a fabricated default. See [`Outcome`](crate::Outcome)'s impl for
/// why this feature is `Serialize`-only.
#[cfg(feature = "report-serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "report-serde")))]
impl Serialize for SoftSignal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SoftSignal", 2)?;
        state.serialize_field(crate::report_serde::KIND, self.name())?;
        state.serialize_field("signal", &self.signal())?;
        state.end()
    }
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
        self.soft_signal.signal()
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

/// *(feature `report-serde`)* The observed teardown facts, field for field:
///
/// ```json
/// {
///   "soft_signal": {"kind": "sent", "signal": "term"},
///   "members_before": 3,
///   "members_after": 0,
///   "drained_within_grace": true,
///   "escalated": false,
///   "elapsed_secs": 0.118
/// }
/// ```
///
/// Every value comes from the accessor of the same name; the member counts stay
/// `null` where the membership could not be read, never a fabricated `0` (the
/// same honesty the accessors keep). `attempted_signal` is not a separate key —
/// it is the `soft_signal` object's own `signal`. There is deliberately no
/// `Deserialize` (see [`Outcome`](crate::Outcome)'s impl).
#[cfg(feature = "report-serde")]
#[cfg_attr(docsrs, doc(cfg(feature = "report-serde")))]
impl Serialize for ShutdownReport {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ShutdownReport", 6)?;
        state.serialize_field("soft_signal", &self.soft_signal())?;
        state.serialize_field("members_before", &self.members_before())?;
        state.serialize_field("members_after", &self.members_after())?;
        state.serialize_field("drained_within_grace", &self.drained_within_grace())?;
        state.serialize_field("escalated", &self.escalated())?;
        state.serialize_field("elapsed_secs", &crate::report_serde::secs(self.elapsed()))?;
        state.end()
    }
}
