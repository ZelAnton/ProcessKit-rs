# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Add entries to `[Unreleased]` as you work — manual bullets always win over the
git-cliff auto-fill (config: `cliff.toml`). On release, promote `[Unreleased]`
to a dated version section.

## [Unreleased]

### Added
- `ProcessGroup` — a kill-on-drop container for a child-process tree, backed by
  Windows Job Objects, Linux cgroup v2 (with a POSIX process-group fallback), or
  no containment elsewhere. Async `shutdown` performs a graceful
  SIGTERM → wait → SIGKILL teardown on Unix; the mechanism in effect is
  observable via `Mechanism`.
- `Command` builder and async run-and-capture helpers: `output_string`,
  `output_bytes`, `exit_code`, `run`, `first_line`, and `start` (live handle).
- `RunningProcess` handle with incremental `stdout_lines` streaming (stderr
  drained in the background), `output_string`/`output_bytes`/`wait`, and process
  metadata.
- `ProcessResult<T>` with `is_success` / `ensure_success`, and a structured
  `Error` (`Spawn` / `Exit` / `Timeout` / `Io`).
- `Stdin` sources: `empty`, `from_string`, `from_bytes`, `from_file`,
  `from_iter_lines`, `from_reader`, and `from_lines` (async stream).
- `ProcessRunner` mock seam with `JobRunner`, `ScriptedRunner`,
  `RecordingRunner`, and a `mock`-feature `MockRunner`.
- Interactive stdin: `Command::keep_stdin_open` plus `RunningProcess::standard_input`
  returning a `ProcessStdin` writer (`write`/`write_line`/`flush`/`finish`).
- Push line-handlers: `Command::on_stdout_line` / `on_stderr_line`, invoked per
  decoded line as it is read.
- Output-buffer policy: `OutputBufferPolicy` (`bounded`/`unbounded`) with
  `OverflowMode::{DropOldest, DropNewest}`, plus exact `RunningProcess::stdout_line_count`
  / `stderr_line_count` (count survives dropped lines).
- Encoding overrides: `Command::stdout_encoding` / `stderr_encoding` / `encoding`
  to decode non-UTF-8 legacy output (via `encoding_rs`); default stays UTF-8.
- Diagnostics: `ProcessGroup::stats` → `ProcessGroupStats` (active count, and
  CPU/peak-memory where the platform reports them), and per-process
  `RunningProcess::cpu_time` / `peak_memory_bytes` / `elapsed`.
- `CliClient<R>` + the `cli_client!` macro — a reusable core for building typed
  wrappers around an external CLI tool (`command`/`command_in` builders;
  `text`/`capture`/`unit`/`code`/`parse`/`try_parse` run helpers), with the
  runner injectable for hermetic tests.
- Top-level `processkit::run` / `processkit::output` free functions.
- Public `Command` accessors (`program`/`arguments`/`working_dir`/
  `env_overrides`/`stdin_source`/`configured_timeout`) so external
  `ScriptedRunner::when` predicates can inspect a command; plus public
  `Command::to_tokio_command`.
- `ProcessRunnerExt::checked`, `ProcessResult::combined`, `Invocation::args_str`,
  `RunningProcess::finish_streamed` (exit code + collected stderr after
  streaming) and `RunningProcess::start_kill`.
- `Error::Parse { program, message }` for fallible output parsing.
- The `tracing` feature now emits a per-run `debug` event (program, exit code,
  timed-out, elapsed) on the `processkit` target.

### Changed
- Output capture is now line-oriented (pumped): captured text is normalized to
  `\n` line endings. `output_bytes` still returns exact raw stdout.

### Fixed
-

### Fixed
-

## [0.1.0] - 2026-05-31

### Added
- Initial project skeleton.

[Unreleased]: https://github.com/ZelAnton/ProcessKit-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ZelAnton/ProcessKit-rs/releases/tag/v0.1.0
