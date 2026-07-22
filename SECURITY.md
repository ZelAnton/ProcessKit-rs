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

If you're launching a program you don't trust, read
[Running untrusted children](docs/untrusted-children.md) first: it lays out what
this crate does and does not guarantee (it hardens process management, it is
**not** a sandbox — seccomp/AppContainer/namespaces/a real OS container remain
the caller's job) and the concrete checklist for containment, resource limits,
privilege drop, and environment hygiene.

## Supported versions

Security fixes land on the **latest published version** on
[crates.io](https://crates.io/crates/processkit); please reproduce on the latest
release before reporting. Within the current `2.x` line, fixes ship as patch
releases.
