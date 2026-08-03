//! Deterministic **fault injection at the OS-primitive boundary** of the
//! containment backends — compiled only under `cfg(test)`.
//!
//! The happy paths of `sys::linux` / `sys::windows` / `sys::pgroup` are covered by
//! real-subprocess integration tests, but their *error* paths are not: a failed
//! `cgroup.freeze` write, a `SetInformationJobObject` rejected on the second info
//! class after the first succeeded, an `EPERM` from `killpg` against a uid-changed
//! child. Each needs a privileged, degraded or otherwise hand-built host to
//! reproduce, so none of them is regression-tested today. This module lets a unit
//! test order one specific call of one specific primitive to fail with one specific
//! errno, and assert what the crate reports for it.
//!
//! # Why this shape (and not the alternatives)
//!
//! The crate already has four hermetic-seam precedents, and this seam was chosen by
//! elimination against them rather than invented fresh:
//!
//! - **`ScriptedRunner` / the `ProcessRunner` doubles** (`src/doubles.rs`) — a trait
//!   double at a boundary that *is already an abstraction* (the runner), replacing
//!   the OS wholesale. It does not fit here: the OS-primitive boundary inside a
//!   backend is a handful of FFI calls owned by `Job`/`Cgroup`/`Tracked`, and making
//!   it a trait means either a `dyn` field on every containment object (an indirect
//!   call on every happy-path `kill`/`write`, and a pointer per group — the runtime
//!   cost this seam must not have) or a generic parameter that goes viral through
//!   `sys::Job` into the **public** `ProcessGroup` type. Faking the OS is also more
//!   than is wanted: these tests should exercise the *real* probe, identity and
//!   liveness logic and only substitute the one call whose failure is under test.
//! - **The `cfg(loom)` `#[path]`-included cores** (`sys/skip_drop_kill.rs`,
//!   `sys/pid_gate.rs`, `running/deadline.rs`) — extract a *pure* algorithmic core
//!   so a separate harness can model-check it. Inapplicable by construction: a
//!   cgroup write or a `killpg` is the effect itself; there is no pure core to lift
//!   out, and the crate has already applied that technique everywhere one exists.
//! - **The `*_with(read: impl Fn(&Path) -> io::Result<String>)` closure seams**
//!   (`Cgroup::members_with` / `signal_with` / `kill_with` / `stats_with` /
//!   `limit_evidence_with`) — the closest existing relative, and deliberately kept:
//!   it is the right tool when the injected primitive is already threaded as a
//!   parameter and the assertion is at the backend's own return value. It does not
//!   reach these cases, because the failing calls sit on *write*/FFI paths behind
//!   public constructors (`ProcessGroup::with_options` → `Job::new` →
//!   `SetInformationJobObject`) that cannot take a closure without changing the
//!   public API, and because the assertion wanted here is the *crate's* error
//!   contract at the end of the whole call, not an `io::Error` at the backend edge.
//! - **The `cfg(test)` task-local probes in `src/pump.rs`** (T-128) — an *ambient*,
//!   test-only channel consulted at an interesting point deep inside a call whose
//!   public entry point takes no test parameter, compiled out of production
//!   entirely. That is exactly this problem, one step further along: T-128 needed to
//!   **observe** an internal event, this needs to **substitute its result**. So this
//!   module is the T-128 probe pattern generalized from observation to injection —
//!   ambient (nothing threaded through production signatures), scoped (parallel
//!   tests stay isolated), and absent outside `cfg(test)`.
//!
//! Storage is a `thread_local` rather than `pump.rs`'s `tokio::task_local`: every
//! path this covers is synchronous and stays on the caller's thread
//! (`ProcessGroup::with_options`/`update_limits`/`suspend`/`resume`/`signal` are
//! plain `fn`s calling straight into the backend), and a thread-local additionally
//! works in a `#[test]` with no tokio runtime. libtest gives each test its own
//! thread and `cargo nextest` its own process, so armed rules never leak between
//! tests. The one limitation this implies is deliberate and named: a primitive
//! invoked on a *different* thread than the one that armed the rules sees no rules
//! and calls the real OS — the safe direction.
//!
//! # How a backend is wired in
//!
//! Each backend routes the primitive through **one** owning wrapper (the convention
//! the crate already uses for launch/config boundaries) and consults [`check`] there
//! under `#[cfg(test)]`, so production keeps the bare call and there is exactly one
//! place per primitive to audit:
//!
//! - `sys::linux::cgroup_write` — every `write(2)` to a cgroup v2 interface file;
//!   the target label is the file name (`memory.max`, `cgroup.freeze`, …).
//! - `sys::windows::set_information_job_object` — every `SetInformationJobObject`
//!   call; the target label is the info-class axis (`extended-limit`, `cpu-rate`).
//! - `sys::pgroup::deliver_signal` — every real signal *delivery* in the tracked
//!   sweep; the target label is the syscall (`killpg`, `kill`).
//!
//! A faulted call never reaches the OS at all, which is also what makes a test able
//! to name a signal it must not actually send.

use std::cell::RefCell;
use std::io;

/// An OS primitive a backend routes every call through, and that an armed
/// [`Rule`] can therefore make fail.
///
/// One variant per wrapped primitive, gated to the platforms that have it so an
/// unreachable variant can never be armed by mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Site {
    /// Linux: one `write(2)` to a cgroup v2 interface file, through
    /// `sys::linux::cgroup_write`. Target label: the file name.
    #[cfg(target_os = "linux")]
    CgroupWrite,
    /// Windows: one `SetInformationJobObject` info-class write, through
    /// `sys::windows::set_information_job_object`. Target label: the axis name.
    #[cfg(windows)]
    JobObjectSetInformation,
    /// POSIX: one `killpg(2)`/`kill(2)` delivery from the process-group backend's
    /// tracked sweep, through `sys::pgroup::deliver_signal`. Target label: the
    /// syscall name.
    #[cfg(unix)]
    PgroupSignalDelivery,
}

/// One armed fault: which primitive to fail, which of its calls, and with what.
#[derive(Debug)]
struct Rule {
    site: Site,
    /// `None` matches every call at the site; `Some(label)` only those whose target
    /// label is exactly `label`.
    target: Option<&'static str>,
    /// Matching calls still to let through before this rule starts failing.
    skip: usize,
    /// The raw OS error a faulted call reports (`EIO`, `ERROR_ACCESS_DENIED`, …).
    errno: i32,
    /// Matching calls seen so far — let through and failed alike.
    matched: usize,
    /// Matching calls actually failed so far.
    fired: usize,
}

thread_local! {
    /// The rules armed on this thread, in the order the test declared them.
    static ARMED: RefCell<Vec<Rule>> = const { RefCell::new(Vec::new()) };
}

/// Consulted by a wrapped OS primitive **before** it calls the OS: `Some(err)` means
/// this call must fail with `err` and never reach the kernel, `None` means proceed.
///
/// The first rule that matches `site` (and, when it names one, `target`) decides —
/// later rules do not also see the call.
pub(crate) fn check(site: Site, target: &str) -> Option<io::Error> {
    ARMED.with(|armed| {
        // `try_borrow_mut` rather than `borrow_mut`: a re-entrant consultation would
        // otherwise panic *inside a backend teardown path*, which is a far worse
        // failure than declining to inject. Nothing re-enters today (no rule body
        // runs user code), so this is a guard, not a live case.
        let Ok(mut armed) = armed.try_borrow_mut() else {
            return None;
        };
        for rule in armed.iter_mut() {
            if rule.site != site {
                continue;
            }
            if let Some(want) = rule.target
                && want != target
            {
                continue;
            }
            rule.matched += 1;
            if rule.skip > 0 {
                rule.skip -= 1;
                return None;
            }
            rule.fired += 1;
            return Some(io::Error::from_raw_os_error(rule.errno));
        }
        None
    })
}

/// A set of faults being built up, armed with [`arm`](Self::arm).
#[derive(Debug, Default)]
pub(crate) struct Faults {
    rules: Vec<Rule>,
}

impl Faults {
    /// An empty set — arming it injects nothing (every primitive calls the OS).
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Fail **every** call at `site` (optionally only those whose target label is
    /// `target`) with `errno`.
    pub(crate) fn fail_every(self, site: Site, target: Option<&'static str>, errno: i32) -> Self {
        self.fail_from_nth(site, target, 1, errno)
    }

    /// Let the first `nth - 1` matching calls reach the OS for real, then fail the
    /// `nth` **and every later one** with `errno`.
    ///
    /// The "after a genuinely successful first" shape: `fail_from_nth(site, None, 2,
    /// …)` makes a two-call sequence fail on its second call only, with the first
    /// really applied by the kernel.
    ///
    /// # Panics
    ///
    /// If `nth` is `0` — call ordinals are 1-based.
    pub(crate) fn fail_from_nth(
        mut self,
        site: Site,
        target: Option<&'static str>,
        nth: usize,
        errno: i32,
    ) -> Self {
        assert!(nth >= 1, "call ordinals are 1-based; `nth` must be >= 1");
        self.rules.push(Rule {
            site,
            target,
            skip: nth - 1,
            errno,
            matched: 0,
            fired: 0,
        });
        self
    }

    /// Install these rules on the current thread until the returned [`Armed`] guard
    /// drops, which restores whatever was armed before (so a nested scope is
    /// harmless rather than a panic).
    pub(crate) fn arm(self) -> Armed {
        let previous = ARMED.with(|armed| std::mem::replace(&mut *armed.borrow_mut(), self.rules));
        Armed {
            previous: Some(previous),
        }
    }
}

/// The live scope of a set of armed faults: it disarms them on drop (including on
/// unwind, so a failing assertion cannot leave a fault armed for the next test on
/// this thread) and reports what the seam saw while it was armed.
#[derive(Debug)]
pub(crate) struct Armed {
    /// The rules that were armed before this scope, restored on drop. `None` only
    /// after the drop has run.
    previous: Option<Vec<Rule>>,
}

impl Armed {
    /// How many calls at `site` were matched by one of this scope's rules — let
    /// through and failed alike. With a rule that names no target this is the total
    /// number of calls the primitive made, which is how a test proves an *earlier*
    /// call in a sequence really did reach the OS.
    pub(crate) fn matched(&self, site: Site) -> usize {
        self.fold(site, |rule| rule.matched)
    }

    /// How many calls at `site` this scope actually failed.
    pub(crate) fn fired(&self, site: Site) -> usize {
        self.fold(site, |rule| rule.fired)
    }

    fn fold(&self, site: Site, count: impl Fn(&Rule) -> usize) -> usize {
        ARMED.with(|armed| {
            armed
                .borrow()
                .iter()
                .filter(|rule| rule.site == site)
                .map(count)
                .sum()
        })
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            ARMED.with(|armed| *armed.borrow_mut() = previous);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Faults, Site, check};

    /// Whichever site this platform compiles — the seam's own bookkeeping is
    /// platform-independent, so one is enough to exercise it.
    #[cfg(unix)]
    const SITE: Site = Site::PgroupSignalDelivery;
    #[cfg(windows)]
    const SITE: Site = Site::JobObjectSetInformation;

    #[test]
    fn no_armed_rules_never_injects() {
        assert!(check(SITE, "anything").is_none());
    }

    #[test]
    fn fail_from_nth_lets_the_earlier_calls_through() {
        let armed = Faults::new().fail_from_nth(SITE, None, 2, 13).arm();

        assert!(check(SITE, "first").is_none(), "call 1 reaches the OS");
        let injected = check(SITE, "second").expect("call 2 is faulted");

        assert_eq!(injected.raw_os_error(), Some(13));
        assert_eq!(armed.matched(SITE), 2);
        assert_eq!(armed.fired(SITE), 1);
    }

    #[test]
    fn a_named_target_does_not_match_another_call_at_the_same_site() {
        let armed = Faults::new().fail_every(SITE, Some("wanted"), 5).arm();

        assert!(check(SITE, "other").is_none());
        assert!(check(SITE, "wanted").is_some());

        assert_eq!(armed.matched(SITE), 1, "only the named target is counted");
        assert_eq!(armed.fired(SITE), 1);
    }

    #[test]
    fn dropping_the_scope_disarms() {
        drop(Faults::new().fail_every(SITE, None, 5).arm());

        assert!(check(SITE, "after the scope").is_none());
    }
}
