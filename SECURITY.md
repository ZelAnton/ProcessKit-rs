# Security policy

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue for a
vulnerability.

Use GitHub's private vulnerability reporting:
**[Report a vulnerability](https://github.com/ZelAnton/ProcessKit-rs/security/advisories/new)**
(Security → Advisories → *Report a vulnerability* on the repository).

Include, as far as you can: the affected version, the platform (Windows / Linux /
macOS), a description of the issue, and a minimal reproduction. You can expect an
acknowledgement within a few days; a fix and coordinated disclosure follow once the
issue is confirmed.

## Why this crate is security-relevant

`processkit` manages process *trees* and touches privileged OS surfaces — Windows
**Job Objects**, Linux **cgroup v2**, POSIX **process groups**, and on Unix it can
**drop privileges** (`uid`/`gid`/supplementary `groups`/`setsid`). Bugs in these
paths can have safety or isolation consequences (a leaked subprocess, an incomplete
privilege drop, a containment escape), so they are treated as security issues, not
just functional ones.

## Supported versions

Security fixes land on the **latest published version** on
[crates.io](https://crates.io/crates/processkit); please reproduce on the latest
release before reporting. Within the current `1.x` line, fixes ship as patch
releases.
