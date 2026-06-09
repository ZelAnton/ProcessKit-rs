# next: launch & builder ergonomics

> **Status:** open idea (next). From the 2026-06-09 cross-language sweep. Small,
> mostly-independent builder conveniences that remove common caller hand-rolls.
> Grouped by size, not coupling — each could ship alone.

## Candidates

### A. `which` / PATH resolution + a precise "not found" error
*Borrow: zx `which`, execa `preferLocal`, plumbum `local.which` · Cost: moderate*

No way to resolve a program to an absolute path before spawn, or to report *which*
PATH was searched on failure. Cross-platform correctness is the cost (PATHEXT and
`.exe`/`.cmd`/`.bat` resolution on Windows). Pairs with the already-shipped
`is_not_found()` classifier + `command_line()` quoting + cwd pre-check (all in 0.9.1) to
turn an opaque ENOENT into "`foo` not found on PATH (searched: …)". **(A) is promoted to
ROADMAP item 3** (the 2026-06-10 sweep — its 0.9.1 dependencies shipped); B/E wait here.

### B. `prefer_local` — prepend a project-local bin dir
*Borrow: execa `preferLocal`/`localDir`, node `.bin` · Cost: moderate*

Run project-local toolchains (e.g. a vendored binary) without the caller mangling
`PATH` by hand. Depends on (A)'s resolution logic.

### C. Bulk environment loaders — from map / from iterator
*Borrow: mixlib `environment` hash, common deploy tooling · Cost: trivial*

`env`/`env_remove` are one-at-a-time. Accept `IntoIterator<Item=(K,V)>` for a
HashMap/Vec bulk apply. **Leave `.env`-file *parsing* to the caller** (dotenv
formats vary) — just accept the parsed pairs.

### D. `current_dir` conveniences
*Borrow: xshell `pushd`, zx `cd`/`within`, plumbum `with_cwd` · Cost: trivial–moderate*

Beyond setting the path: **validate it exists up front** (clear error vs opaque spawn
failure — this part shipped in 0.9.1). A scoped pushd-style RAII guard is
the moderate, optional extension.

### E. `send_control(char)` on the live handle
*Borrow: rexpect `send_control`, MedallionShell `ControlC` · Cost: trivial*

Even without a PTY, a typed "send Ctrl-C / Ctrl-D to the child's stdin" convenience
helps drive REPLs and interactive tools the caller already keeps stdin open for.
(Real terminal-signal semantics still need the PTY — see `later-pty-support.md`.)

## Assessment

All low-risk and additive. (A)+(B) are the only non-trivial pieces (Windows PATH
resolution); (C)(D-validate)(E) are nearly free. **The cwd-validate and quoting parts
shipped in 0.9.1; (A) which/PATH resolution is now ROADMAP item 3** (with bulk env (C)
riding along); B/E wait here. Ship the rest opportunistically when touching the `Command`
builder.
