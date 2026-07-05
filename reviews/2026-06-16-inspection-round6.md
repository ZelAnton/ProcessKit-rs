# 2026-06-16 inspection round 6 — bug-focused (clean)

Seventh fresh-eyes pass over the whole `src/` tree (~21k LOC) on `main` (v0.11.1
base; round-4/5 fixes + the interface freeze unreleased), after five prior
bug-review rounds
([1](2026-06-15-inspection.md) · [2](2026-06-15-inspection-round2.md) ·
[3](2026-06-15-inspection-round3.md) · [round-4](2026-06-15-fix-plan-round4.md) ·
[round-5](2026-06-16-fix-plan-round5.md)) and the
[interface-freeze audit](2026-06-16-interface-freeze-audit.md).

**Five readers**, each having read every prior report (so resolved and
deliberately-deferred/accepted items were excluded by construction):

1. Core run lifecycle (`running/mod.rs`, `stream.rs`, `probes.rs`, `pump.rs`).
2. `sys/` containment backends (linux/windows/pgroup/graceful/unix/mod).
3. Data/value types (command/result/error/stdin/signal/buffer/stats/mechanism/limits).
4. Orchestration & seams (runner/pipeline/batch/cassette/doubles/client/supervisor/group).
5. **Cross-cutting** concerns spanning modules — the gaps *between* the per-area
   reviews, where bugs hide this late: end-to-end concurrency under a
   multi-threaded runtime, cancellation propagation across layers, Drop ordering of
   composed types, panic/unwind safety of user closures, feature-combo paths, and
   `Send`/`Sync` of the public futures across `.await`.

## Result: no genuinely new bug found

All five readers independently concluded the area sound. Every concern traced
maps to either an already-landed fix (verified not regressed) or a
documented/accepted deferral — nothing cleared the (very high) bar for a new
finding.

Notable re-verifications this round:

- **R5-1** (`has_exited_now` claims the timeout arbiter) and **R5-2**
  (`Pipeline::run` fails loud on a truncated last stage) — both fixed and correct;
  no reap path mutates state without claiming the B1 arbiter.
- **Freeze fixes** — `OutputLine.text` accessor-fronting and the
  `Error::ResourceLimit { message }` struct variant are fully wired (Display/Debug,
  all match/construction sites, the `limits`-gated arms).
- **Saturating arithmetic** (N-2/A1) complete across linux + windows stats paths.
- **SkipDropKill** latch ordering, the Windows spawn-guard window (N-1), cgroup
  freeze-downgrade (R2-3), and the pgroup `group_seen` recycled-pid latch — all
  intact.
- **Cross-cutting**: the timeout/exit/cancel arbiter is race-free across module
  boundaries under a multi-threaded runtime; cancellation is classified terminally
  and consistently in every layer (runner/pipeline/batch/supervisor/client); the
  composed-type Drop orders (RunningProcess, Pipeline `capture`, batch) tear down
  correctly; user-closure panics leak no child and poison no kill-path lock;
  `stats`-without-`process-control` and the other feature combos are sound.

Deferred/accepted items encountered (NOT re-raised, per the prior records): P2-7
pump scratch shrink, P3-3 pump poison mix, P2-10 profile recycled-pid window, P2-1
cgroup / P2-3-5 Windows-TID / solo-pid recycle hazards, P1-2/P3-13 blocking Drop
sleep + orphan dirs, R2-2 Windows peak-memory semantics, the cgroup-join-after-uid
spawn-fail (documented), and the one-shot-stdin not-re-runnable-under-Supervisor
footgun (same class as the documented pipeline re-run note).

## Conclusion

After six rounds plus the interface freeze, the crate is exceptionally hardened.
This pass produced **no fix plan** — there was nothing new and defensible to fix.
The surface remains freeze-ready (see the interface-freeze audit). No code was
changed.
