# Comparative benchmarks

`processkit` adds process-tree containment, decoded line streaming, bounded
capture, and a consistent async API around child processes. Those guarantees
have a cost, so this repository keeps an end-to-end comparison against the
plain Tokio and standard-library APIs. The benchmark is intended to answer
"what does the convenience and safety layer cost for this workload?", not to
declare one API universally faster.

On Windows there is a second question the end-to-end comparison cannot answer:
creating a process costs tens of milliseconds all by itself on a machine with
real-time antivirus, so an absolute "start took N ms" number attributes nothing.
`benches/win_spawn_phases.rs` answers it by splitting the fixed start cost into
phases; see [Where a Windows start goes](#where-a-windows-start-goes).

## What is measured

`benches/compare.rs` runs the same real passthrough child (`cat` on Unix,
`cmd /c findstr` on Windows) for every contender. Payload construction is
outside the timed sections where possible. Each sample includes child spawn,
pipe setup, stdin/stdout transfer, output handling, and process wait.
The children are intentionally trivial: they only echo stdin to stdout, so the
measurement is about process handling rather than application work.

| Scenario | Children | Input / output | ProcessKit path | Plain baselines |
|---|---:|---:|---|---|
| Small capture | 1 | 32 bytes, 3 lines | `Command::output_string`, plus `processkit_resolved_program` (absolute program path) and `stdout(StdioMode::Null)` + `start().wait()` to isolate program lookup and capture cost | `tokio::process::Command` and `std::process::Command` |
| Large streaming | 1 | 1,032,000 bytes, 8,000 lines of 128 bytes | `start` + `stdout_lines` | async Tokio line reader and synchronous `BufRead::lines` |
| Concurrent fan-out | 16 | 32 bytes per child | 16 concurrent `output_string` runs | 16 Tokio tasks or 16 standard threads |

The recorded run used an Intel Core i9-12900H CPU with 20 logical processors,
Windows 11 Enterprise (build 10.0.26200) with Defender real-time protection
enabled, and Rust 1.93.0. The machine had 514 live processes and 7,761 live
threads while the numbers were taken — a figure that matters for the start-cost
attribution below, and that a quiet CI runner will not reproduce. Criterion was
configured in `benches/compare.rs::configure` with `sample_size=20`,
`warm_up_time=10s`, and `measurement_time=5s` for each series. The table reports
the Criterion mean and standard deviation; throughput is derived from the fixed
workload size (it is not a separately configured Criterion throughput
measurement). Deltas use the `std_process` result in the same scenario as the
baseline. Absolute values are machine-dependent: CPU, OS process-spawn cost,
antivirus, scheduler load, filesystem state, Rust toolchain, and available Tokio
runtime threads can all change them. Compare contenders only within the same
invocation and retain the command and environment with any published result.

One Windows-specific measurement hazard is handled inside the benchmark rather
than left to the reader. A host with behavioural monitoring throttles a process
that abruptly starts creating children and only settles tens of seconds later,
which is longer than Criterion's per-series warm-up; without intervention that
whole penalty lands on whichever series runs first, and it was observed making
the first contender read two to four times its value from a later run while
every other series stayed stable. `benches/compare.rs::prime_process_creation`
therefore spawns real children, recording nothing, until batch times stop
falling, and only then lets the first series start. On a host that does not
throttle it costs a few seconds.

## Measured results

These measurements were produced by `cargo bench --bench compare` on the host
described above. Times are end-to-end and shown as mean +/- standard deviation.
The throughput units are runs/s for small capture, MiB/s for the 1,032,000-byte
stream, and child processes/s for the 16-child fan-out.

| Scenario | Contender | Time | Derived throughput | Delta vs `std_process` |
|---|---|---:|---:|---:|
| Small capture | `processkit` | 57.161 +/- 5.283 ms | 17.49 runs/s | +3.7% |
| Small capture | `processkit_resolved_program` | 54.513 +/- 7.712 ms | 18.34 runs/s | -1.1% |
| Small capture | `processkit_discard_stdout` | 58.810 +/- 6.221 ms | 17.00 runs/s | +6.7% |
| Small capture | `tokio_process` | 52.413 +/- 6.716 ms | 19.08 runs/s | -4.9% |
| Small capture | `std_process` | 55.108 +/- 8.267 ms | 18.15 runs/s | baseline |
| Large streaming | `processkit` | 66.876 +/- 5.329 ms | 14.72 MiB/s | +16.9% |
| Large streaming | `tokio_process` | 57.782 +/- 4.756 ms | 17.03 MiB/s | +1.0% |
| Large streaming | `std_process` | 57.226 +/- 8.170 ms | 17.20 MiB/s | baseline |
| Concurrent fan-out | `processkit` | 209.388 +/- 32.835 ms | 76.41 children/s | +9.2% |
| Concurrent fan-out | `tokio_process` | 192.012 +/- 24.948 ms | 83.33 children/s | +0.2% |
| Concurrent fan-out | `std_process` | 191.690 +/- 28.314 ms | 83.47 children/s | baseline |

## Where a Windows start goes

The small-capture delta above is a few milliseconds on a host where creating the
child itself costs about 50. Interpreting that requires knowing which part of a
start belongs to ProcessKit and which to the operating system, so
`benches/win_spawn_phases.rs` measures the phases directly. It uses a fixed
absolute-path child (`cmd /c exit`) so no series pays for a program lookup, and
it is configured with `sample_size=50`, `warm_up_time=15s`, and
`measurement_time=20s`.

`os_spawn_plain` is the floor: `CreateProcess` and wait, nothing else. The
`os_spawn_suspended_resume_*` series add ProcessKit's `CREATE_SUSPENDED` and
resume cycle to it, and the `containment_sequence_*` series add Job Object
creation and assignment on top of that, mirroring `sys::windows::Job::{new,
spawn}` step for step. Within each pair, `..._snapshot` and `..._direct` differ
only in how the launcher finds the primary thread of the suspended child it must
release: the documented system-wide `TH32CS_SNAPTHREAD` ToolHelp snapshot, or the
per-process `ntdll!NtGetNextThread` walk ProcessKit uses (falling back to the
snapshot when that entry point is unavailable).

| Phase series | Time | Delta vs `os_spawn_plain` |
|---|---:|---:|
| `os_spawn_plain` | 22.527 +/- 2.002 ms | baseline |
| `os_spawn_suspended_resume_direct` | 22.643 +/- 2.653 ms | +0.5% |
| `containment_sequence_direct` | 22.661 +/- 1.661 ms | +0.6% |
| `os_spawn_suspended_resume_snapshot` | 96.378 +/- 4.053 ms | +327.8% |
| `containment_sequence_snapshot` | 97.695 +/- 4.043 ms | +333.7% |

The primitives behind those phases, measured with no child process involved so
that a spawn outlier cannot hide them:

| Primitive | Time |
|---|---:|
| `job_object_create` | 4.457 +/- 0.156 us |
| `direct_thread_walk` | 8.634 +/- 0.934 us |
| `program_lookup_absolute_path` | 37.255 +/- 2.355 us |
| `program_lookup_bare_name` | 5.402 +/- 0.428 ms |
| `thread_snapshot_walk` | 73.990 +/- 1.601 ms |

The two tables reconcile. `os_spawn_plain` (22.527 ms) plus `thread_snapshot_walk`
(73.990 ms) plus `job_object_create` (0.004 ms) is 96.521 ms against a measured
`containment_sequence_snapshot` of 97.695 ms, leaving 1.2 ms for the assignment,
the per-thread open and resume, and the suspended spawn itself. The same sum with
`direct_thread_walk` instead is 22.540 ms against a measured 22.661 ms.

Read together:

- **Creating the Job Object is free.** At 4.5 microseconds it is four orders of
  magnitude below the `CreateProcess` it contains. Whole-tree containment on
  Windows is not what makes a start expensive, and no amount of deferring or
  pooling the container could pay for itself.
- **Finding the suspended child's primary thread was the whole cost.** The only
  *documented* pid-to-thread mapping on Windows is a snapshot that is
  system-wide — the process-id argument is ignored for thread lists — so it
  materialises all 7,761 threads on the machine to locate the one thread that was
  just created. At 74 ms that was 3.3x the cost of the `CreateProcess` it
  followed, and it dominated everything else in the start path put together.
  Asking the same question per-process instead answers it in 8.6 microseconds,
  about 8,600 times faster, and collapses the whole containment sequence to
  within noise of a plain unguarded spawn (+0.6%).
- **Program lookup is the remaining ProcessKit-specific cost**, and it is under
  the caller's control. Resolving a bare name across `PATH` x PATHEXT — which
  ProcessKit does so that a launch spawns exactly what the spawn-free
  `Command::resolve_program` preflight reports, including a `.cmd`/`.bat` the
  OS's own `.exe`-only search would never find — cost 5.4 ms against 37
  microseconds for a program named by an absolute path, on a `PATH` with 77
  entries and a PATHEXT with 14 extensions. That is the `processkit` versus
  `processkit_resolved_program` gap in the table above. A caller starting the
  same program repeatedly can resolve it once with `Command::resolve_program`
  and pass the resulting path.
- **Serialising the spawn call itself is not the bottleneck.** ProcessKit holds
  one process-global lock across every child creation, so an ordinary spawn
  cannot observe the ConPTY path's temporary process-global standard handles.
  Running the same 16 spawns with and without that lock measured 131.893 +/-
  24.799 ms parallel against 150.864 +/- 62.608 ms serialised. That +14.4% sits
  inside the serialised series' own standard deviation, and a repeat run put the
  same pair at -1.2%; the two runs disagree in sign, so no reliable penalty is
  resolved at this noise level. Windows serialises process creation internally
  in any case. What it does rule out is the lock as an explanation for a slow
  fan-out — that was the same per-spawn thread snapshot, paid once per child.

## Running the comparison

Run the complete local suite with:

```text
just bench-compare
```

The equivalent Cargo command is:

```text
cargo bench --bench compare
```

The Windows start-cost attribution is a separate bench:

```text
just bench-win-phases
```

```text
cargo bench --bench win_spawn_phases
```

Criterion's normal filtering and measurement options remain available, for
example `cargo bench --bench compare -- stream_large_stdout`. Both benchmarks
spawn real children and are deliberately not part of the ordinary CI test
gate; use them when comparing a change, a platform, or a toolchain.
`win_spawn_phases` is Windows-specific and prints an explanatory message
elsewhere.

## Reading the result

The comparison is meaningful only when the workloads and semantics match:

- `processkit` measures its normal private process-group setup. This is the
  price of its unconditional kill-on-drop tree guarantee, and is not present
  in the plain baselines. On Windows the phase table above shows what that
  actually costs.
- The small-capture group includes a `processkit_resolved_program` series using
  an absolute program path and a `processkit_discard_stdout` series using
  `stdout(StdioMode::Null)` + `start().wait()`. Compare each with `processkit`
  in the same group to see the crate's program-lookup and capture/pump costs
  separately; `StdioMode::Null` is a discard path, so it is not an
  output-equivalent replacement for capture.
- The capture case compares a decoded `ProcessResult<String>` with byte output
  from the plain APIs. The benchmark uses ASCII payloads so decoding does not
  change the transferred content; the APIs still have deliberately different
  result and failure semantics.
- The streaming case consumes every line before waiting for the child. This
  prevents a full stdout pipe from turning the benchmark into a deadlock and
  measures the live line-delivery path rather than only a bulk read.
- The fan-out case gives each API its normal concurrency primitive: Tokio tasks
  for async contenders and scoped standard threads for `std`. It measures the
  whole batch, not a synthetic per-child loop.

## Conclusions

Using a 10% delta as a practical negligible-overhead threshold, the small
capture (+3.7%) and concurrent fan-out (+9.2%) were negligible relative to the
direct `std_process` API in this run, and large streaming (+16.9%) was not. The
plain Tokio contender stayed within 5% of the standard-library baseline in all
three scenarios. Small capture with an absolute program path (-1.1%) was
indistinguishable from the baseline, which places the crate's remaining
short-run overhead in program resolution rather than in containment.

For a short-lived Windows child, expect ProcessKit's own fixed start cost to be
a few milliseconds against a `CreateProcess` that costs tens of them on an
antivirus-equipped host — dominated by the `PATH`/PATHEXT lookup when the
program is named by a bare name, and essentially nothing when it is not. If you
are diagnosing a slow start, measure the difference against a plain spawn of the
same command on the same machine, as `spawn_capture_small` does, before
attributing it to this crate: the absolute number is mostly the operating
system's.

The remaining streaming cost is the trade-off for ProcessKit's decoded,
bounded-capture line delivery. Choose ProcessKit when containment, streaming,
bounded capture, and typed outcomes matter; choose a plain process API when
minimum overhead on a bulk transfer is the priority and those guarantees are not
required. These are single-machine measurements, so they describe this workload
and environment, not a universal performance ranking. For this Windows run,
`std_process` is the direct `CreateProcess`-based reference; Unix users should
rerun the benchmark before treating it as a comparison with their platform's
fork/exec path.

If a change improves one scenario and regresses another, report the scenario
and its Criterion output rather than collapsing the results into one score.
Containment, streaming, and typed outcome handling are features; a faster
plain spawn is not automatically a better substitute for them.
