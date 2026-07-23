//! Seeded **randomized-interleaving** harness for the process lifecycle.
//!
//! The other stress scenarios (`main.rs`) fix *one* concurrency shape each
//! (a spawn burst, a cancel storm, a mass teardown). This one instead lets a
//! seed pick a **random combination** of public operations over a shared
//! [`ProcessGroup`] and its children, then runs them concurrently so real
//! thread scheduling explores the interleavings. That is exactly the dimension
//! the historically most expensive defects lived in — use-after-teardown on the
//! kill paths, races on a shared-group handle, silently faulted background
//! pumps.
//!
//! ## What a seed fixes vs. what stays random
//!
//! The seed is the *plan* generator: given a seed, [`gen_combo_plan`] produces a
//! fully deterministic plan — the child classes, each child's operation
//! sequence, the group-level operations, and the pre-teardown race window. A
//! re-run with the same seed replays the identical plan (proven by the pure
//! [`combo_plans_are_seed_deterministic`] unit test, which needs no
//! subprocess). What the seed does **not** fix is the OS/runtime scheduling of
//! those concurrently-issued operations — that non-determinism is the whole
//! point, the fuzzing dimension the invariants must survive under *any*
//! ordering. On a failure the offending seed is printed; set
//! `PROCESSKIT_STRESS_SEED=<seed>` to re-run that single plan in a loop while
//! debugging.
//!
//! ## Invariants checked after every combination (must hold for any ordering)
//!
//! 1. **No panic, no impossible error.** No spawned task panics, and no
//!    operation returns an `Error` variant that cannot legitimately arise for a
//!    hermetic child that already started and does lifecycle work with no
//!    limits / no cassette / no parsing / a bounded (drop) output policy — see
//!    [`is_impossible_error`]. Operational variants (`Io`, `Signalled`,
//!    `Timeout`, `Cancelled`, `NotReady`, `Unsupported`, `Stdin`) are allowed.
//! 2. **No survivors / zombies.** Every child handle's terminal verb returns
//!    within the grace (a returned `wait`/`finish`/`output_string` is
//!    unambiguous proof the child was reaped), and — where the tier is compiled
//!    with `process-control` — the group reports **no live members** once torn
//!    down.
//! 3. **No silently faulted background task.** A faulted pump/drain/supervision
//!    task surfaces as either a task panic or an error on the terminal verb, so
//!    invariant 1 covers it.
//! 4. **Descriptors and groups released.** Across the whole sweep the process's
//!    open fd/handle count does not grow (Job handles / pipes closed), and on
//!    Linux every group's cgroup directory is removed on drop (reusing the
//!    `common.rs` reap helpers).
//!
//! Gated behind `PROCESSKIT_STRESS` like every scenario here, so the normal PR
//! matrix compiles it but pays nothing; the nightly `stress.yml` runs it.

use std::sync::Arc;
use std::time::Duration;

use processkit::{Command, Error, ErrorReason, OutputBufferPolicy, ProcessGroup, RunningProcess};

use crate::common::*;

/// How many distinct seed-combinations the sweep runs (several dozen, as the
/// task requires). Each combination spawns only a handful of short children, so
/// the whole sweep stays well inside the stress job's 30-minute budget it
/// shares with the other scenarios.
const SEED_COUNT: u64 = 48;
/// Fixed base so the default sweep is fully reproducible run-to-run (the only
/// non-determinism is real thread scheduling, never the plan). Combination `i`
/// uses `BASE_SEED + i`.
const BASE_SEED: u64 = 0x00C0_FFEE_1234_5678;
/// Lines a "stdout flooder" child emits — enough to drive the pump under
/// back-pressure, small enough to stay cheap per combination.
const FLOOD_LINES: u32 = 3_000;
/// Bound on how long we wait for any one handle/task to settle after teardown.
/// Every child is force-terminated by the safety-net `kill_all`, so a handle
/// that blocks past this is a real survivor/zombie, not a slow machine.
const REAP_GRACE: Duration = Duration::from_secs(20);
/// Cap on how many failing seeds a single run reports (keeps the panic message
/// bounded when a regression trips many seeds at once).
const MAX_REPORTED: usize = 12;

/// A tiny, dependency-free SplitMix64 PRNG. Deterministic and hermetic — the
/// harness must not pull in `rand`, and it must reproduce a plan bit-for-bit
/// from a seed, which a fixed-algorithm generator guarantees.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64 (Steele/Vigna): a full-period generator with good
        // avalanche, standard for seeding without a crate dependency.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`. `n` must be non-zero (all call sites pass a literal).
    fn below(&mut self, n: u32) -> u32 {
        (self.next_u64() % u64::from(n)) as u32
    }

    /// `true` with probability `1/n`.
    fn one_in(&mut self, n: u32) -> bool {
        self.below(n) == 0
    }

    /// Pick one element of a non-empty slice.
    fn choose<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u32) as usize]
    }
}

/// The three child shapes the task calls out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildClass {
    /// Exits 0 immediately.
    ShortLived,
    /// ~90s silent sleeper — a process to kill/tear down, never to wait out.
    LongLived,
    /// Floods stdout under a bounded buffer policy.
    StdoutFlooder,
}

/// The consuming verb that ends a child's plan and reaps it. (Per-handle
/// *graceful* stop — `RunningProcess::shutdown` — is deliberately absent: a
/// `group.start()` child is a *shared-group* handle, for which that verb is
/// documented to return `ErrorReason::Unsupported`; group-level graceful stop is
/// exercised via [`GroupOp::ShutdownRef`] instead.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Wait,
    OutputString,
    Finish,
}

/// A single child's deterministic operation sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildPlan {
    class: ChildClass,
    keep_stdin: bool,
    take_stdin: bool,
    start_kill: bool,
    inspect: bool,
    wait_for_line: bool,
    terminal: Terminal,
}

/// A group-level operation run concurrently with the children. Feature-gated
/// variants are compiled only where the backing API exists, so the harness
/// still builds (and runs a meaningful, if smaller, interleaving) under
/// `--no-default-features`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupOp {
    /// A harmless read that always exists — keeps the pool non-empty under
    /// `--no-default-features` so even that build races a group method against
    /// the children.
    Mechanism,
    /// Group-level graceful teardown, concurrent with in-flight child ops.
    ShutdownRef,
    #[cfg(feature = "process-control")]
    Members,
    #[cfg(feature = "process-control")]
    MembersInfo,
    #[cfg(feature = "process-control")]
    SuspendResume,
    #[cfg(feature = "process-control")]
    Signal(processkit::Signal),
    #[cfg(feature = "stats")]
    Stats,
}

/// The full plan a seed expands to. Kept as data (not spawned) so the
/// determinism unit test can compare two expansions of one seed without
/// touching the OS.
#[derive(Debug, Clone)]
struct ComboPlan {
    children: Vec<ChildPlan>,
    group_ops: Vec<GroupOp>,
    /// Deterministic race window before the safety-net teardown, letting the
    /// concurrently-issued child and group ops actually overlap first.
    interleave_ms: u64,
}

/// The group ops available in this build (feature-gated variants included only
/// when their API is compiled in).
fn group_op_pool() -> Vec<GroupOp> {
    #[allow(unused_mut)]
    let mut pool = vec![GroupOp::Mechanism, GroupOp::ShutdownRef];
    #[cfg(feature = "process-control")]
    {
        pool.push(GroupOp::Members);
        pool.push(GroupOp::MembersInfo);
        pool.push(GroupOp::SuspendResume);
        pool.push(GroupOp::Signal(processkit::Signal::Term));
        pool.push(GroupOp::Signal(processkit::Signal::Int));
        pool.push(GroupOp::Signal(processkit::Signal::Hup));
    }
    #[cfg(feature = "stats")]
    {
        pool.push(GroupOp::Stats);
    }
    pool
}

/// Expand a seed into its full, deterministic plan. Pure — no I/O — so the same
/// seed always yields the same plan (the reproducibility contract).
fn gen_combo_plan(seed: u64) -> ComboPlan {
    let mut rng = Rng::new(seed);

    let n_children = 2 + rng.below(3); // 2..=4
    let mut children = Vec::with_capacity(n_children as usize);
    for _ in 0..n_children {
        let class = rng.choose(&[
            ChildClass::ShortLived,
            ChildClass::LongLived,
            ChildClass::StdoutFlooder,
        ]);
        let keep_stdin = rng.one_in(2);
        let take_stdin = rng.one_in(2);
        let start_kill = rng.one_in(3);
        let inspect = rng.one_in(2);
        let terminal = rng.choose(&[Terminal::Wait, Terminal::OutputString, Terminal::Finish]);
        // Streaming a line only makes sense for a flooder, and only when the
        // terminal verb won't also try to consume stdout in bulk (which would
        // be a documented — but here uninteresting — "stdout already consumed"
        // Io error). So restrict it to the non-capturing terminals.
        let wait_for_line = matches!(class, ChildClass::StdoutFlooder)
            && matches!(terminal, Terminal::Wait | Terminal::Finish)
            && rng.one_in(2);
        children.push(ChildPlan {
            class,
            keep_stdin,
            take_stdin,
            start_kill,
            inspect,
            wait_for_line,
            terminal,
        });
    }

    let pool = group_op_pool();
    let n_ops = 2 + rng.below(4); // 2..=5
    let mut group_ops = Vec::with_capacity(n_ops as usize);
    for _ in 0..n_ops {
        group_ops.push(rng.choose(pool.as_slice()));
    }

    let interleave_ms = u64::from(5 + rng.below(35)); // 5..40 ms

    ComboPlan {
        children,
        group_ops,
        interleave_ms,
    }
}

/// The command backing a child class.
fn child_command(plan: &ChildPlan) -> Command {
    let mut cmd = match plan.class {
        ChildClass::ShortLived => quick_exit(),
        ChildClass::LongLived => long_sleeper(),
        // A bounded (drop-oldest) policy keeps the flood's memory bounded and
        // means `OutputTooLarge` can never fire — so seeing it *would* be a bug
        // (`is_impossible_error` treats it as one).
        ChildClass::StdoutFlooder => {
            line_emitter(FLOOD_LINES).output_buffer(OutputBufferPolicy::bounded(1000))
        }
    };
    if plan.keep_stdin {
        cmd = cmd.keep_stdin_open();
    }
    cmd
}

/// Errors that cannot legitimately arise for a hermetic child that already
/// started and only does lifecycle work here — no cassette, no parser, no
/// resource limits, an already-spawned child, and a bounded (drop) output
/// policy. Seeing one is a real defect; every *operational* variant (`Io`,
/// `Signalled`, `Timeout`, `Cancelled`, `NotReady`, `Unsupported`, `Stdin`) is
/// a legitimate outcome of a concurrent teardown and is allowed.
fn is_impossible_error(e: &Error) -> bool {
    if matches!(
        e.reason(),
        ErrorReason::CassetteMiss { .. }
            | ErrorReason::Parse { .. }
            | ErrorReason::NotFound { .. }
            | ErrorReason::Spawn { .. }
            | ErrorReason::OutputTooLarge { .. }
    ) {
        return true;
    }
    // `ResourceLimit` only exists (and can only fire) with the `limits` feature;
    // this harness sets no limits, so it too would be a defect.
    #[cfg(feature = "limits")]
    if matches!(e.reason(), ErrorReason::ResourceLimit { .. }) {
        return true;
    }
    false
}

/// Fold an operation result into the harness verdict: `Ok`/operational-`Err` is
/// fine, an impossible `Err` is a failure carrying a description.
fn check(result: Result<(), Error>, what: &str) -> Result<(), String> {
    match result {
        Ok(()) => Ok(()),
        Err(e) if is_impossible_error(&e) => {
            Err(format!("{what} returned impossible error: {e:?}"))
        }
        Err(_) => Ok(()),
    }
}

/// Drive one child through its planned sequence, ending in a reaping terminal
/// verb.
async fn run_child(mut child: RunningProcess, plan: ChildPlan) -> Result<(), String> {
    if plan.inspect {
        // Pure reads; exercise the concurrent inspection paths.
        let _ = child.pid();
        let _ = child.elapsed();
        let _ = child.stdout_line_count();
    }
    if plan.take_stdin {
        // `Some` iff the command was built with `keep_stdin_open`; dropping the
        // writer closes the pipe. Either arm is a documented outcome.
        let _ = child.take_stdin();
    }
    if plan.start_kill {
        check(child.start_kill(), "start_kill")?;
    }
    if plan.wait_for_line {
        // Best-effort probe: `NotReady`/`Io` (the child exited or was killed
        // before a line matched) are fine, only an impossible error fails.
        check(
            child
                .wait_for_line(|l| !l.is_empty(), Duration::from_secs(5))
                .await
                .map(|_| ()),
            "wait_for_line",
        )?;
    }
    let terminal = match plan.terminal {
        Terminal::Wait => child.wait().await.map(|_| ()),
        Terminal::OutputString => child.output_string().await.map(|_| ()),
        Terminal::Finish => child.finish().await.map(|_| ()),
    };
    check(terminal, "terminal verb")
}

/// Run one group-level operation concurrently with the children.
async fn run_group_op(group: &ProcessGroup, op: GroupOp) -> Result<(), String> {
    match op {
        GroupOp::Mechanism => {
            let _ = group.mechanism();
            Ok(())
        }
        GroupOp::ShutdownRef => check(group.shutdown_ref().await, "group.shutdown_ref"),
        #[cfg(feature = "process-control")]
        GroupOp::Members => check(group.members().map(|_| ()), "group.members"),
        #[cfg(feature = "process-control")]
        GroupOp::MembersInfo => check(group.members_info().map(|_| ()), "group.members_info"),
        #[cfg(feature = "process-control")]
        GroupOp::SuspendResume => {
            check(group.suspend(), "group.suspend")?;
            check(group.resume(), "group.resume")
        }
        #[cfg(feature = "process-control")]
        GroupOp::Signal(sig) => check(group.signal(sig), "group.signal"),
        #[cfg(feature = "stats")]
        GroupOp::Stats => check(group.stats().map(|_| ()), "group.stats"),
    }
}

/// Run a single seed-combination and return every invariant violation it found
/// (empty = clean).
async fn run_combo(seed: u64) -> Result<(), String> {
    let plan = gen_combo_plan(seed);

    let group = match ProcessGroup::new() {
        Ok(g) => Arc::new(g),
        Err(e) => return Err(format!("ProcessGroup::new failed: {e:?}")),
    };

    // Start every child (a shared-group handle) before the concurrent phase, so
    // no `start` races the group teardown ops.
    let mut child_handles = Vec::with_capacity(plan.children.len());
    for child_plan in &plan.children {
        let child = match group.start(&child_command(child_plan)).await {
            Ok(c) => c,
            Err(e) => return Err(format!("group.start failed: {e:?}")),
        };
        let child_plan = child_plan.clone();
        child_handles.push(tokio::spawn(run_child(child, child_plan)));
    }

    // Fire the group-level ops concurrently.
    let mut group_handles = Vec::with_capacity(plan.group_ops.len());
    for op in plan.group_ops {
        let g = Arc::clone(&group);
        group_handles.push(tokio::spawn(async move { run_group_op(&g, op).await }));
    }

    // Let the concurrently-issued child and group ops overlap for a
    // seed-chosen window, then guarantee every child becomes reapable so no
    // terminal verb can block forever on a survivor.
    tokio::time::sleep(Duration::from_millis(plan.interleave_ms)).await;
    let _ = group.kill_all();

    let mut failures = Vec::new();

    // Invariants 1-3: no panic, no impossible error, every child reaped in time.
    for handle in child_handles {
        match tokio::time::timeout(REAP_GRACE, handle).await {
            Err(_) => failures.push(
                "a child handle was not reaped within the grace (survivor/zombie)".to_string(),
            ),
            Ok(Err(join)) if join.is_panic() => {
                failures.push("a child task panicked (faulted background/op)".to_string());
            }
            Ok(Err(_)) => failures.push("a child task was cancelled".to_string()),
            Ok(Ok(Err(msg))) => failures.push(msg),
            Ok(Ok(Ok(()))) => {}
        }
    }
    for handle in group_handles {
        match tokio::time::timeout(REAP_GRACE, handle).await {
            Err(_) => {
                failures.push("a group-op task did not settle within the grace".to_string());
            }
            Ok(Err(join)) if join.is_panic() => {
                failures.push("a group-op task panicked".to_string());
            }
            Ok(Err(_)) => failures.push("a group-op task was cancelled".to_string()),
            Ok(Ok(Err(msg))) => failures.push(msg),
            Ok(Ok(Ok(()))) => {}
        }
    }

    // All spawned tasks (the only other Arc holders) have joined, so this is the
    // sole owner — recover it to inspect and drop the group deterministically.
    let group = match Arc::into_inner(group) {
        Some(g) => g,
        None => {
            failures.push("internal: group Arc still shared after all tasks joined".to_string());
            return Err(failures.join("; "));
        }
    };

    // Invariant 2 (where the API exists): no live members remain. Poll to empty
    // — a just-terminated pid can linger job-member-true for a brief OS window.
    #[cfg(feature = "process-control")]
    {
        let deadline = std::time::Instant::now() + REAP_GRACE;
        loop {
            match group.members() {
                Ok(m) if m.is_empty() => break,
                Ok(_) if std::time::Instant::now() >= deadline => {
                    failures.push("group still reports live members after teardown".to_string());
                    break;
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(e) if is_impossible_error(&e) => {
                    failures.push(format!("group.members returned impossible error: {e:?}"));
                    break;
                }
                // A transient Io/Unsupported read failure is not a survivor signal.
                Err(_) => break,
            }
        }
    }

    // Invariant 4: graceful final teardown, then drop releases the descriptors /
    // cgroup directory (the aggregate leak checks run once after the sweep).
    if let Err(e) = group.shutdown().await
        && is_impossible_error(&e)
    {
        failures.push(format!("group.shutdown returned impossible error: {e:?}"));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// The seeded randomized-interleaving sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomized_lifecycle_interleavings() {
    if skip_unless_enabled("randomized_lifecycle_interleavings") {
        return;
    }

    // Baseline the fd/handle count after a warmup spawn (the first spawn lazily
    // initializes OS/runtime handles that are then reused — a one-time jump, not
    // a leak), and snapshot our own cgroup directories, so the post-sweep checks
    // measure only steady-state growth / leftovers.
    let warmup = quick_exit().output_string().await.expect("warm up a child");
    assert!(warmup.is_success());
    let before_fds = open_handle_count();
    #[cfg(target_os = "linux")]
    let before_cgroups = own_cgroup_v2_parent().map(|parent| own_processkit_cgroup_dirs(&parent));

    let seeds: Vec<u64> = match std::env::var("PROCESSKIT_STRESS_SEED") {
        Ok(raw) => {
            let seed: u64 = raw
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("PROCESSKIT_STRESS_SEED must be a u64, got {raw:?}"));
            eprintln!("[stress] interleave: replaying single seed {seed}");
            vec![seed]
        }
        Err(_) => (0..SEED_COUNT).map(|i| BASE_SEED.wrapping_add(i)).collect(),
    };

    let mut failures = Vec::new();
    for &seed in &seeds {
        if let Err(msg) = run_combo(seed).await {
            failures.push(format!("seed {seed}: {msg}"));
            if failures.len() >= MAX_REPORTED {
                break;
            }
        }
    }
    assert!(
        failures.is_empty(),
        "randomized interleaving harness found invariant violations \
         (re-run one with PROCESSKIT_STRESS_SEED=<seed>):\n{}",
        failures.join("\n")
    );

    // Invariant 4 (aggregate): the sweep spawned/tore down hundreds of children
    // and dozens of groups; if descriptors were released, the count is flat. A
    // per-combination leak (an unclosed Job handle or pipe) would grow it far
    // past this slack, which absorbs runtime bookkeeping noise.
    if let (Some(before), Some(after)) = (before_fds, open_handle_count()) {
        assert!(
            after <= before + 32,
            "fd/handle count grew across the interleaving sweep: {before} -> {after}"
        );
    }
    // Invariant 4 (Linux): every group's cgroup directory was removed on drop —
    // no `processkit-<pid>-*` directory this sweep created is left behind.
    #[cfg(target_os = "linux")]
    if let (Some(parent), Some(before)) = (own_cgroup_v2_parent(), before_cgroups) {
        let leaked: Vec<_> = own_processkit_cgroup_dirs(&parent)
            .into_iter()
            .filter(|p| !before.contains(p))
            .collect();
        assert!(
            leaked.is_empty(),
            "cgroup directories leaked after the interleaving sweep: {leaked:?}"
        );
    }
}

/// The reproducibility contract, checked without a subprocess so it runs in the
/// normal PR matrix too: the same seed expands to the same plan, and different
/// seeds generally differ.
#[test]
fn combo_plans_are_seed_deterministic() {
    for seed in [1u64, 42, 12_345, BASE_SEED] {
        assert_eq!(
            format!("{:?}", gen_combo_plan(seed)),
            format!("{:?}", gen_combo_plan(seed)),
            "the same seed must reproduce an identical plan"
        );
    }
    assert_ne!(
        format!("{:?}", gen_combo_plan(1)),
        format!("{:?}", gen_combo_plan(2)),
        "different seeds should generally produce different plans"
    );
}
