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

#[cfg(test)]
mod tests {
    use super::{after_comm, starttime_from_stat};

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
}
