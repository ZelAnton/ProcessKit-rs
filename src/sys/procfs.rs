//! Shared `/proc/<pid>/stat` parsing for the Linux/Android backends.
//!
//! Three call sites read the same file with the same field convention: the
//! process-group backend's liveness identity token
//! ([`pgroup::read_identity`](super::pgroup)) and the Linux per-process metrics
//! sampler's identity gate and CPU read ([`imp::process_metrics`](super)). They
//! used to carry three hand-kept-in-sync copies of the parse, guarded only by
//! cross-referencing comments — a silent drift of the cut point or the field index
//! would not fail the build, it would quietly weaken the anti-pid-reuse identity
//! gate (matching a *recycled* pid). Centralizing the parse here makes that
//! convention exist exactly once.
//!
//! The one rule every caller shares: `/proc/<pid>/stat` field 2 (`comm`) is
//! wrapped in parentheses and may itself contain spaces or `)`, so the only robust
//! split is at the **last** `)`. Everything up to and including it (the pid and the
//! comm) is dropped; the first whitespace token after it is field 3 (state).
//! Field _N_ is therefore whitespace index _N_-3 — e.g. `starttime` (field 22) is
//! index 19, and `utime`/`stime` (fields 14/15) are indices 11/12.

/// The slice of a `/proc/<pid>/stat` line **after** the comm field's closing `)`.
///
/// The comm field (field 2) is parenthesized and may contain spaces or `)`, so the
/// split is at the *last* `)`: everything through it (pid + comm) is dropped,
/// leaving field 3 (state) as the first whitespace token. `None` when the content
/// has no `)` at all (an unreadable / truncated stat line). This is the single cut
/// rule the identity token and the CPU read both build on, so they cannot disagree
/// on where the numeric fields begin.
pub(crate) fn after_comm(stat: &str) -> Option<&str> {
    stat.rsplit_once(')').map(|(_, after)| after)
}

/// Parse the `starttime` (field 22, clock ticks since boot) out of a
/// `/proc/<pid>/stat` line — the process's start-time identity anchor.
///
/// `starttime` is fixed at process creation and distinct for a pid recycled by a
/// later process, so it tells a reused number apart from the original. It is the
/// 20th whitespace field after the comm ([`after_comm`]): index 0 is field 3
/// (state), so field 22 is index 19. `None` if the line has no `)`, has fewer than
/// 20 fields after the comm, or that field is not a number.
pub(crate) fn starttime_from_stat(stat: &str) -> Option<u64> {
    after_comm(stat)?
        .split_whitespace()
        .nth(19)?
        .parse::<u64>()
        .ok()
}

/// Best-effort read of `pid`'s start-time identity anchor: read `/proc/<pid>/stat`
/// and pull its `starttime` via [`starttime_from_stat`]. `None` if the process is
/// gone (the read fails) or the stat is unparsable.
pub(crate) fn read_starttime(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    starttime_from_stat(&stat)
}

/// Parse the `ppid` (field 4, parent process id) out of a `/proc/<pid>/stat`
/// line. It is the 2nd whitespace field after the comm ([`after_comm`]): index 0
/// is field 3 (state), so field 4 is index 1. `None` if the line has no `)`, has
/// fewer than 2 fields after the comm, or that field is not a number.
#[cfg(feature = "process-control")]
pub(crate) fn ppid_from_stat(stat: &str) -> Option<u32> {
    after_comm(stat)?
        .split_whitespace()
        .nth(1)?
        .parse::<u32>()
        .ok()
}

/// Extract the `comm` (field 2, the process's short image name) out of a
/// `/proc/<pid>/stat` line — the text between the *first* `(` and the *last* `)`.
///
/// The comm is parenthesized and may itself contain spaces or `)` (the very shape
/// [`after_comm`] exists to survive), so the whole span between the first `(` and
/// the last `)` is the name; taking the last `)` mirrors the cut every numeric
/// field is read past. The kernel truncates `comm` to 15 bytes and a process can
/// rewrite it via `prctl(PR_SET_NAME)`, so it is the *current* name, not a
/// canonical path. `None` if the line has no `(` / `)` pair or the span is empty.
#[cfg(feature = "process-control")]
pub(crate) fn comm_from_stat(stat: &str) -> Option<String> {
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    // Guard a malformed line whose `)` precedes (or abuts) its `(` — an empty or
    // reversed span is no name.
    let name = stat.get(open + 1..close)?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Best-effort enriching metadata for `pid` from a **single** `/proc/<pid>/stat`
/// read — parent pid (field 4), image name (`comm`, field 2), and the start-time
/// identity anchor (`starttime`, field 22) — so the three describe one consistent
/// instant rather than straddling a pid recycle across separate reads. `None` when
/// the process is gone (the read fails); each field is independently `Option` (a
/// parse miss on one leaves the others intact).
#[cfg(feature = "process-control")]
pub(crate) fn read_stat_meta(pid: u32) -> Option<StatMeta> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    Some(StatMeta {
        ppid: ppid_from_stat(&stat),
        comm: comm_from_stat(&stat),
        starttime: starttime_from_stat(&stat),
    })
}

/// The trio of enriching fields [`read_stat_meta`] pulls from one `/proc/<pid>/stat`
/// read. Each is independently `Option` — an unparsable field is `None`, never a
/// fabricated value.
#[cfg(feature = "process-control")]
pub(crate) struct StatMeta {
    pub ppid: Option<u32>,
    pub comm: Option<String>,
    pub starttime: Option<u64>,
}

/// Parse the **state** char (field 3) out of a `/proc/<pid>/stat` line.
///
/// State is the first whitespace token after the comm ([`after_comm`]): `R`/`S`/
/// `D`/`T`/`I`/… for a live process, `Z` for a zombie, `X`/`x` for a fully-dead
/// one. This is what tells a genuinely-alive member (whose `SIGKILL` `EPERM` is a
/// real containment gap) apart from a harmless unreaped zombie whose group
/// `killpg` also reports `EPERM` — see
/// [`pgroup::is_live_non_zombie`](super::pgroup). `None` if the line has no `)`,
/// has no token after the comm, or that token is empty.
pub(crate) fn state_from_stat(stat: &str) -> Option<char> {
    after_comm(stat)?.split_whitespace().next()?.chars().next()
}

/// Best-effort read of `pid`'s process-state char: read `/proc/<pid>/stat` and
/// pull field 3 via [`state_from_stat`]. `None` if the process is gone (the read
/// fails) or the stat is unparsable — a caller treats an unknown state as *not*
/// positively live, so a vanished pid is never reported as a live containment gap.
pub(crate) fn read_state(pid: u32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    state_from_stat(&stat)
}

#[cfg(all(test, feature = "process-control"))]
mod meta_tests {
    use super::{comm_from_stat, ppid_from_stat};

    // The same tricky `/proc/<pid>/stat` shape the identity tests use: a comm that
    // itself contains a `)` and spaces, so a naive "split on first `)`" or plain
    // field-split mis-parses. Field 3 (state) is `S`, field 4 (ppid) is `1` — the
    // first two whitespace tokens after the comm's *last* `)`.
    const TRICKY_COMM: &str = "1234 (weird ) proc :) S 1 1234 1234 0 -1 4194304 100 0 0 0 50 25 0 0 20 0 1 0 987654321 1000 2000";

    #[test]
    fn ppid_reads_field_4_past_a_tricky_comm() {
        // Field 4 (ppid) is whitespace index 1 after the comm's last ')': for the
        // tricky line above that is `1`. A shifted cut (e.g. a split on the first
        // ')') would read a comm fragment instead.
        assert_eq!(ppid_from_stat(TRICKY_COMM), Some(1));
    }

    #[test]
    fn ppid_yields_none_when_absent_or_unparsable() {
        // No `)` at all — unparsable.
        assert_eq!(ppid_from_stat("1234 no-paren S 1"), None);
        // Only the state field after the comm — index 1 (ppid) is absent.
        assert_eq!(ppid_from_stat("1234 (proc) S"), None);
        // A non-numeric ppid field.
        assert_eq!(ppid_from_stat("1234 (proc) S not_a_pid"), None);
    }

    #[test]
    fn comm_keeps_a_name_with_inner_parens_and_spaces() {
        // The whole span between the first '(' and the last ')' is the name, so an
        // inner ')' and spaces survive intact — the point of taking the last ')'.
        assert_eq!(
            comm_from_stat(TRICKY_COMM).as_deref(),
            Some("weird ) proc :")
        );
        // A plain comm.
        assert_eq!(
            comm_from_stat("1234 (worker) S 1").as_deref(),
            Some("worker")
        );
    }

    #[test]
    fn comm_yields_none_without_a_paren_pair_or_when_empty() {
        // No parentheses at all.
        assert_eq!(comm_from_stat("1234 no-paren S 1"), None);
        // An empty comm span `()`.
        assert_eq!(comm_from_stat("1234 () S 1"), None);
        // A `)` before the `(` — reversed/malformed span.
        assert_eq!(comm_from_stat("1234 )weird( S 1"), None);
    }
}

#[cfg(test)]
mod tests {
    use super::{after_comm, starttime_from_stat, state_from_stat};

    // A synthetic `/proc/<pid>/stat` line whose comm contains BOTH a `)` and spaces
    // — the exact shape a naive "split on the first `)`" or a plain field-split
    // would mis-parse. After the comm's *last* `)`, index 0 is field 3 (state), so
    // utime/stime (fields 14/15) are indices 11/12 and starttime (field 22) is
    // index 19. Locking these here is the drift guard the three real call sites
    // lacked: a shifted cut or a shifted index would fail this test instead of
    // silently weakening the identity gate.
    const TRICKY_COMM: &str = "1234 (weird ) proc :) S 1 1234 1234 0 -1 4194304 100 0 0 0 50 25 0 0 20 0 1 0 987654321 1000 2000";

    #[test]
    fn after_comm_skips_a_comm_with_inner_parens_and_spaces() {
        let fields: Vec<&str> = after_comm(TRICKY_COMM)
            .expect("the line has a closing ')'")
            .split_whitespace()
            .collect();
        assert_eq!(fields[0], "S", "index 0 must be field 3 (state)");
        assert_eq!(fields[11], "50", "index 11 must be field 14 (utime)");
        assert_eq!(fields[12], "25", "index 12 must be field 15 (stime)");
        assert_eq!(
            fields[19], "987654321",
            "index 19 must be field 22 (starttime)"
        );
    }

    #[test]
    fn starttime_reads_field_22_past_a_tricky_comm() {
        assert_eq!(starttime_from_stat(TRICKY_COMM), Some(987654321));
    }

    #[test]
    fn no_closing_paren_yields_none() {
        assert_eq!(after_comm("1234 no-paren S 1"), None);
        assert_eq!(starttime_from_stat("1234 no-paren S 1"), None);
    }

    #[test]
    fn too_few_fields_yields_none() {
        // Only three fields after the comm — index 19 (starttime) is absent.
        assert_eq!(starttime_from_stat("1234 (proc) S 1 1234"), None);
    }

    #[test]
    fn a_non_numeric_starttime_yields_none() {
        // 20 fields after the comm, but field 22 (index 19) is non-numeric.
        let stat = "1 (p) S 1 1 1 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 not_a_number";
        assert_eq!(starttime_from_stat(stat), None);
    }

    #[test]
    fn state_reads_field_3_past_a_tricky_comm() {
        // Field 3 (state) is the first token after the comm's last ')': for the
        // tricky comm above that is `S`. A shifted cut point (e.g. a split on the
        // first ')') would read a comm fragment instead of the state.
        assert_eq!(state_from_stat(TRICKY_COMM), Some('S'));
    }

    #[test]
    fn state_reads_a_zombie() {
        // The exact shape the live/zombie discrimination hinges on: a `Z` state is
        // an unreaped zombie, whose group `killpg` `EPERM` must stay swallowed.
        assert_eq!(state_from_stat("1234 (proc) Z 1 1234 1234 0 -1"), Some('Z'));
    }

    #[test]
    fn state_yields_none_without_a_closing_paren_or_a_field() {
        // No `)` at all — unparsable.
        assert_eq!(state_from_stat("1234 no-paren R 1"), None);
        // A trailing `)` with nothing after it — no state token.
        assert_eq!(state_from_stat("1234 (proc)"), None);
    }
}
