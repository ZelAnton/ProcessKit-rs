# later: PTY support for prompt-driven tools

> **Status:** open idea (later — gated on a concrete consumer). This is the
> forward-looking pointer for the PTY work; the **full design sketch and the
> defer rationale already live in**
> [`../decisions/permissions-privileges-pty-network.md`](../decisions/permissions-privileges-pty-network.md)
> §4. This file exists so PTY stays visible in the open backlog without duplicating
> that sketch.

## The gap

Tools that *demand* a controlling terminal — `ssh`/`sudo` **password**/passphrase
prompts, some credential helpers, certain installers — detect "not a tty" and either
refuse or hang. ProcessKit's I/O is entirely pipe-based (three independent streams),
so it can't satisfy them. Key-auth SSH and `BatchMode=yes` work today; interactive
secret entry does not. Borrow: rexpect (`exp_regex`/`send_line`/`send_control`),
pyinvoke (`pty=True` + responders), Python `sh`.

## Why later, not next

- **Major and architecturally misaligned** (~2–3k LoC). A PTY **merges stdout/stderr**
  onto one master fd and adds terminal line-discipline (echo, `ICANON`, signals) — it
  breaks the `on_stdout_line`/`on_stderr_line` split the whole pump is built around.
- **No concrete consumer.** The repo's discipline is to wait for a real ask.
- Containment is unaffected (the PTY child still lives in the job/cgroup/pgroup), so
  there's no urgency from the safety side.

## Shape (summary — full version in the decisions record)

A feature-gated `use_pty` mode: `openpty` (Unix) / ConPTY `CreatePseudoConsole`
(Windows) instead of three piped stdios; a `Backend::Pty` variant with a single
merged pump; termios echo-disable for secret entry; a PTY-mode `ScriptedRunner` for
tests. Build the **minimal single-master-fd mode**, not a terminal emulator.

**Revisit when:** a concrete consumer needs interactive password/passphrase or a
tty-only tool that can't use key-auth/`BatchMode`. Pairs with `send_control` from
[`next-launch-ergonomics.md`](next-launch-ergonomics.md).
