# Summary

<!-- The four entries below form the implementation switcher, pinned above
     Overview at the very top of the sidebar. mdBook cannot point a SUMMARY.md
     entry at an external URL (a raw URL fails the build), so they are carried
     as DRAFT prefix chapters — bare `[Title]()` links (no leading `-`, unlike a
     bullet list) with no chapter files, in the same un-numbered prefix block as
     Overview itself. theme/nav-links.js upgrades "CLI Runner", "Python
     wrapper", and ".NET version" into live external links; "Rust version"
     remains a labelled, non-clickable indicator for this current
     implementation. Production and
     the ProcessKit-fSharp reference both pin mdBook v0.4.40 in CI, whose
     draft entries are bare <div> elements targeted by the theme. -->
[Rust version]()
[CLI Runner]()
[Python wrapper]()
[.NET version]()

---

[Overview](README.md)

---

- [Cookbook](cookbook.md)
- [Running commands](commands.md)
- [Running many at once](batch.md)
- [Comparative benchmarks](comparison.md)
- [Process groups](process-groups.md)
- [Streaming & interactive I/O](streaming.md)
- [Pipelines](pipelines.md)
- [Timeouts, retries & cancellation](timeouts-and-cancellation.md)
- [Errors](errors.md)
- [Supervision](supervision.md)
- [Observability](observability.md)
- [Testing your code](testing.md)
- [Platform support](platform-support.md)
- [Running in containers](containers.md)
- [Running untrusted children](untrusted-children.md)
- [Upgrading](upgrading.md)
- [What's next](whats-next.md)
