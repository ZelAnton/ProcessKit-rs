# later: advanced testing — the full risk-zone inventory

> **Status:** open idea (later). From the 2026-06-09 test-coverage audit. The crate
> already has strong coverage: ~89 `#[ignore]`d real-subprocess integration tests, a
> nightly stress tier (`PROCESSKIT_STRESS=1`), and solid unit tests for the parsing
> hot spots (`pump.rs` line-splitting/encoding/handler-panic isolation, `result.rs`,
> `error.rs`). **The cheapest, highest-risk gaps graduated to the roadmap** (now
> ROADMAP item 1: cancellation races + pump edge cases + the 0.9.1 graceful-timeout
> coverage). The arbitrary-chunk-boundary multibyte case is best *proved* by the
> property test sketched here (the line is reassembled by `BufReader` in practice).
> This file is the backlog of the rest — heavier infrastructure or lower-probability zones.

## Risk zones & candidate tests

### A. Concurrency model checking (`loom`)
*Cost: major*

The genuinely racy spots have no model-checked coverage: the Windows
spawn → `AssignProcessToJobObject` → resume window and the `suspend_lock` count
nesting (`sys/windows.rs`), the pgroup recycled-pid probe-before-signal window
(`sys/pgroup.rs`). `loom` would need these extracted behind testable abstractions —
expensive, but it's the only way to *prove* the lock discipline rather than hope.

### B. Property testing (`proptest`) for the pump/decoder
*Cost: moderate*

The line pump + `encoding_rs` decode is a parsing surface ripe for proptest:
arbitrary byte streams chunked at arbitrary boundaries → assert (lines never lost,
counts accurate, no panic, multibyte sequences split across reads reconstruct).
Generalizes the existing hand-picked Shift-JIS / lone-lead-byte unit tests.

### C. Fuzzing (`cargo-fuzz`) the decoder
*Cost: moderate*

A fuzz target over `pump_lines`' decode path — cheap to stand up once proptest
generators exist, catches the long tail proptest's shrinking misses.

### D. Cross-platform leak checks
*Cost: moderate*

The FD-leak churn stress test is **Linux-only** (`/proc/self/fd`). Add macOS (`lsof`)
and Windows (handle count) equivalents, and verify **cgroup directory cleanup** on
drop (currently best-effort, unverified — `sys/linux.rs`).

### E. Platform-divergence & forking-tree zones
*Cost: moderate*

- A `setsid` child that *forks* (does only the direct child escape; is the grandchild
  still reaped?) — `sys/pgroup.rs`/`sys/unix.rs`.
- A tight fork loop *during* a signal broadcast (how many caught vs escaped) — the
  documented best-effort pgroup limit.
- The Linux cgroup×uid failure path and the cgroup→process-group **fallback** when
  delegation is unavailable — asserted indirectly today, never directly.

### F. Lower-probability functional gaps
*Cost: trivial each*

Real-subprocess handler-panic (unit-covered only); `RestartPolicy::Always` on a
*successful* child; per-stage pipeline timeout; `wait_for_port` against a listener
that closes mid-retry; `Stdin::from_file`; broken-pipe error-code on Windows.

## Assessment

None block 1.0 — the shipped suite already proves the headline guarantees end-to-end.
This is *depth*: (A) is the highest-confidence-per-effort-but-most-expensive; (B)+(C)
are the best value (the pump is the riskiest pure-Rust surface); (D)+(E) harden the
platform claims; (F) is a cheap grab-bag to fold into any test PR.

**Revisit:** (B)/(C) when hardening the pump; (D)/(E) before claiming the
cross-platform containment guarantees are *proven* rather than *tested*; (A) only if
a real lock-ordering bug ever surfaces.
