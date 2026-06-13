---
name: Bug report
about: Report incorrect behavior (a leaked process, a wrong result, a panic, …)
title: ""
labels: bug
assignees: ""
---

**What happened**
A clear description of the bug.

**Expected behavior**
What you expected instead.

**Reproduction**
A minimal snippet or steps. The smaller, the faster it gets fixed.

```rust
// minimal repro
```

**Environment**
- `processkit` version:
- OS + version: <!-- Windows / Linux (distro + cgroup v1/v2) / macOS / BSD -->
- Rust version (`rustc --version`):
- Containment mechanism, if known (from `Mechanism` / `ProcessGroup`): <!-- Job Object / cgroup v2 / process group / none -->
- Relevant feature flags: <!-- stats, process-control, limits, record, … -->

**Additional context**
Logs (the `tracing` feature, if enabled — but **never paste secrets/argv/env**),
stack traces, or anything else useful.
