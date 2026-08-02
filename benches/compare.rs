//! Comparative benchmarks for processkit's public run surface.
//!
//! Every contender runs the same real passthrough child and receives the same
//! pre-built payload. The measurements include process creation, pipe I/O, and
//! output handling; they are intentionally end-to-end rather than an isolated
//! comparison of a single parser. `processkit` also includes its unconditional
//! private process-group setup, because that containment is part of the API's
//! normal cost.
//!
//! The three scenarios are:
//!
//! 1. one child plus a small captured output;
//! 2. one child streaming a large stdout payload line by line;
//! 3. a concurrent fan-out of many small children.
//!
//! Run with `cargo bench --bench compare` or use `just bench-compare`.

use std::io::{BufRead as _, Read as _, Write as _};
use std::process::Stdio;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use processkit::prelude::StreamExt;
use processkit::{Command, Stdin, StdioMode};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::Command as TokioCommand;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

const SMALL_PAYLOAD: &[u8] = b"processkit\ncomparison\nbenchmark\n";
const STREAM_LINE_COUNT: usize = 8_000;
const STREAM_LINE_WIDTH: usize = 128;
const FAN_OUT: usize = 16;

fn processkit_passthrough(payload: &[u8]) -> Command {
    let command = if cfg!(windows) {
        Command::new("cmd").args(["/c", "findstr", "^^"])
    } else {
        Command::new("cat")
    };
    command.stdin(Stdin::from_bytes(payload.to_vec()))
}

/// The same passthrough child named by an **absolute** path instead of a bare
/// name. `processkit` resolves a bare name itself (`PATH` x PATHEXT, so the
/// launch spawns exactly what the spawn-free `resolve_program` preflight
/// reports); the plain baselines hand the bare name to the OS, whose own search
/// runs inside the kernel. Comparing this series with `processkit` in the same
/// group therefore attributes that lookup, instead of leaving it inside an
/// unexplained "the crate is slower" delta.
fn processkit_passthrough_resolved(program: &std::path::Path, payload: &[u8]) -> Command {
    let command = if cfg!(windows) {
        Command::new(program).args(["/c", "findstr", "^^"])
    } else {
        Command::new(program)
    };
    command.stdin(Stdin::from_bytes(payload.to_vec()))
}

/// Resolve the passthrough child's absolute path once, outside every timed
/// section, using the crate's own spawn-free lookup.
fn resolved_passthrough_program() -> std::path::PathBuf {
    processkit_passthrough(b"")
        .resolve_program()
        .expect("resolve the passthrough child's absolute path")
}

fn tokio_passthrough() -> TokioCommand {
    let mut command = if cfg!(windows) {
        let mut command = TokioCommand::new("cmd");
        command.args(["/c", "findstr", "^^"]);
        command
    } else {
        TokioCommand::new("cat")
    };
    command.stdin(Stdio::piped()).stdout(Stdio::piped());
    command
}

fn std_passthrough() -> std::process::Command {
    let mut command = if cfg!(windows) {
        let mut command = std::process::Command::new("cmd");
        command.args(["/c", "findstr", "^^"]);
        command
    } else {
        std::process::Command::new("cat")
    };
    command.stdin(Stdio::piped()).stdout(Stdio::piped());
    command
}

fn stream_payload() -> Vec<u8> {
    let line = format!("{}\n", "x".repeat(STREAM_LINE_WIDTH));
    line.repeat(STREAM_LINE_COUNT).into_bytes()
}

async fn tokio_capture(payload: &[u8]) -> Vec<u8> {
    let mut child = tokio_passthrough().spawn().expect("spawn tokio child");
    let mut stdin = child.stdin.take().expect("tokio stdin pipe");
    let mut stdout = child.stdout.take().expect("tokio stdout pipe");
    let payload = payload.to_vec();
    let write = async move {
        stdin.write_all(&payload).await.expect("write tokio stdin");
        drop(stdin);
    };
    let read = async move {
        let mut output = Vec::new();
        stdout
            .read_to_end(&mut output)
            .await
            .expect("read tokio stdout");
        output
    };
    let (_, output) = tokio::join!(write, read);
    child.wait().await.expect("wait tokio child");
    output
}

fn std_capture(payload: &[u8]) -> Vec<u8> {
    let mut child = std_passthrough().spawn().expect("spawn std child");
    let mut stdin = child.stdin.take().expect("std stdin pipe");
    let mut stdout = child.stdout.take().expect("std stdout pipe");
    let payload = payload.to_vec();
    let writer = std::thread::spawn(move || {
        stdin.write_all(&payload).expect("write std stdin");
        drop(stdin);
    });
    let mut output = Vec::new();
    stdout.read_to_end(&mut output).expect("read std stdout");
    writer.join().expect("join std stdin writer");
    child.wait().expect("wait std child");
    output
}

async fn processkit_stream(payload: &[u8]) -> usize {
    let mut running = processkit_passthrough(payload)
        .start()
        .await
        .expect("start processkit child");
    let mut lines = running
        .stdout_lines()
        .expect("open processkit stdout stream");
    let mut count = 0;
    while lines.next().await.is_some() {
        count += 1;
    }
    let finished = running.finish().await.expect("finish processkit stream");
    assert_eq!(finished.outcome.code(), Some(0), "processkit child failed");
    count
}

async fn tokio_stream(payload: &[u8]) -> usize {
    let mut child = tokio_passthrough().spawn().expect("spawn tokio child");
    let mut stdin = child.stdin.take().expect("tokio stdin pipe");
    let stdout = child.stdout.take().expect("tokio stdout pipe");
    let payload = payload.to_vec();
    let write = async move {
        stdin.write_all(&payload).await.expect("write tokio stdin");
        drop(stdin);
    };
    let read = async move {
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let mut count = 0;
        while lines.next_line().await.expect("read tokio line").is_some() {
            count += 1;
        }
        count
    };
    let (_, count) = tokio::join!(write, read);
    child.wait().await.expect("wait tokio child");
    count
}

fn std_stream(payload: &[u8]) -> usize {
    let mut child = std_passthrough().spawn().expect("spawn std child");
    let mut stdin = child.stdin.take().expect("std stdin pipe");
    let stdout = child.stdout.take().expect("std stdout pipe");
    let payload = payload.to_vec();
    let writer = std::thread::spawn(move || {
        stdin.write_all(&payload).expect("write std stdin");
        drop(stdin);
    });
    let mut lines = std::io::BufReader::new(stdout).lines();
    let mut count = 0;
    while lines.next().transpose().expect("read std line").is_some() {
        count += 1;
    }
    writer.join().expect("join std stdin writer");
    child.wait().expect("wait std child");
    count
}

async fn processkit_fan_out(payload: &[u8]) {
    let mut tasks = JoinSet::new();
    for _ in 0..FAN_OUT {
        let payload = payload.to_vec();
        tasks.spawn(async move {
            let result = processkit_passthrough(&payload)
                .output_string()
                .await
                .expect("processkit fan-out run");
            assert!(result.is_success());
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("join processkit fan-out task");
    }
}

async fn tokio_fan_out(payload: &[u8]) {
    let mut tasks = JoinSet::new();
    for _ in 0..FAN_OUT {
        let payload = payload.to_vec();
        tasks.spawn(async move {
            let output = tokio_capture(&payload).await;
            assert_eq!(output, payload);
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("join tokio fan-out task");
    }
}

fn std_fan_out(payload: &[u8]) {
    std::thread::scope(|scope| {
        for _ in 0..FAN_OUT {
            scope.spawn(|| {
                let output = std_capture(payload);
                assert_eq!(output, payload);
            });
        }
    });
}

/// Spawn children until process creation reaches a steady state, recording
/// nothing.
///
/// A Windows host with real-time behavioural monitoring throttles a process that
/// abruptly starts creating children, and only settles tens of seconds later —
/// well past criterion's per-series warm-up, which is scoped to one series and
/// cannot be made long enough without multiplying the whole run's duration by the
/// number of series. Left alone, that entire penalty lands on whichever series
/// criterion measures first; observed here as the first contender reading two to
/// four times its own value from a later run, while every other series stayed
/// stable. That is an artefact of measurement order, not a property of any
/// contender, so it is removed rather than reported.
///
/// Batches of real runs are timed until several consecutive batches land close to
/// the best batch seen, or the budget runs out. The run of stable batches is
/// deliberately long: while the host is still settling, batch times fall
/// unevenly, and a short run of "close enough" batches is reached by noise well
/// before the cost has actually bottomed out. On a host that does not throttle,
/// this costs the handful of seconds those batches take.
fn prime_process_creation(rt: &Runtime) {
    const BATCH: usize = 16;
    const BUDGET: Duration = Duration::from_secs(240);
    const STABLE_ROUNDS: u32 = 5;
    const STABLE_FACTOR: f64 = 1.10;
    let started = std::time::Instant::now();
    let mut best = Duration::MAX;
    let mut stable = 0;
    while started.elapsed() < BUDGET {
        let round = std::time::Instant::now();
        rt.block_on(async {
            for _ in 0..BATCH {
                let result = processkit_passthrough(SMALL_PAYLOAD)
                    .output_string()
                    .await
                    .expect("processkit priming run");
                assert!(result.is_success());
            }
        });
        let elapsed = round.elapsed();
        if elapsed < best {
            best = elapsed;
            stable = 0;
        } else if elapsed <= best.mul_f64(STABLE_FACTOR) {
            stable += 1;
            if stable >= STABLE_ROUNDS {
                return;
            }
        } else {
            stable = 0;
        }
    }
}

fn bench_spawn_capture(c: &mut Criterion) {
    let rt = Runtime::new().expect("build benchmark runtime");
    // Runs before the first series of the first group, so every contender in
    // every group is measured from the same steady state.
    prime_process_creation(&rt);
    let mut group = c.benchmark_group("spawn_capture_small");
    group.bench_function("processkit", |b| {
        b.to_async(&rt).iter(|| async {
            let result = processkit_passthrough(SMALL_PAYLOAD)
                .output_string()
                .await
                .expect("processkit capture");
            assert!(result.is_success());
            let _ = std::hint::black_box(result);
        });
    });
    group.bench_function("processkit_resolved_program", |b| {
        let program = resolved_passthrough_program();
        b.to_async(&rt).iter(|| {
            let program = program.clone();
            async move {
                let result = processkit_passthrough_resolved(&program, SMALL_PAYLOAD)
                    .output_string()
                    .await
                    .expect("processkit capture");
                assert!(result.is_success());
                let _ = std::hint::black_box(result);
            }
        });
    });
    group.bench_function("processkit_discard_stdout", |b| {
        b.to_async(&rt).iter(|| async {
            let outcome = processkit_passthrough(SMALL_PAYLOAD)
                .stdout(StdioMode::Null)
                .start()
                .await
                .expect("processkit discard start")
                .wait()
                .await
                .expect("processkit discard wait");
            assert_eq!(outcome.code(), Some(0));
            std::hint::black_box(outcome);
        });
    });
    group.bench_function("tokio_process", |b| {
        b.to_async(&rt).iter(|| async {
            let output = tokio_capture(SMALL_PAYLOAD).await;
            assert_eq!(output, SMALL_PAYLOAD);
            std::hint::black_box(output);
        });
    });
    group.bench_function("std_process", |b| {
        b.iter(|| {
            let output = std_capture(SMALL_PAYLOAD);
            assert_eq!(output, SMALL_PAYLOAD);
            std::hint::black_box(output);
        });
    });
    group.finish();
}

fn bench_stream_large_stdout(c: &mut Criterion) {
    let rt = Runtime::new().expect("build benchmark runtime");
    let payload = stream_payload();
    let mut group = c.benchmark_group("stream_large_stdout");
    group.bench_function("processkit", |b| {
        b.to_async(&rt).iter(|| {
            let payload = payload.clone();
            async move {
                assert_eq!(processkit_stream(&payload).await, STREAM_LINE_COUNT);
            }
        });
    });
    group.bench_function("tokio_process", |b| {
        b.to_async(&rt).iter(|| {
            let payload = payload.clone();
            async move {
                assert_eq!(tokio_stream(&payload).await, STREAM_LINE_COUNT);
            }
        });
    });
    group.bench_function("std_process", |b| {
        b.iter(|| {
            assert_eq!(std_stream(&payload), STREAM_LINE_COUNT);
        });
    });
    group.finish();
}

fn bench_concurrent_fan_out(c: &mut Criterion) {
    let rt = Runtime::new().expect("build benchmark runtime");
    let mut group = c.benchmark_group("concurrent_fan_out");
    group.bench_function("processkit", |b| {
        b.to_async(&rt).iter(|| async {
            processkit_fan_out(SMALL_PAYLOAD).await;
        });
    });
    group.bench_function("tokio_process", |b| {
        b.to_async(&rt).iter(|| async {
            tokio_fan_out(SMALL_PAYLOAD).await;
        });
    });
    group.bench_function("std_process", |b| {
        b.iter(|| std_fan_out(SMALL_PAYLOAD));
    });
    group.finish();
}

/// The warm-up is deliberately longer than criterion's default: on a Windows host
/// with a real-time scanner the first seconds of a run cost visibly more than the
/// rest, and without a warm-up long enough to absorb that, the whole penalty
/// lands on whichever contender criterion happens to measure first.
fn configure() -> Criterion {
    Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(5))
}

criterion_group! {
    name = comparison_benches;
    config = configure();
    targets = bench_spawn_capture, bench_stream_large_stdout, bench_concurrent_fan_out
}
criterion_main!(comparison_benches);
