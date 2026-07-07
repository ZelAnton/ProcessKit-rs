//! Keep a short-lived child alive with policy-driven restarts, exponential
//! backoff, a restart budget, and a `stop_when` condition — a minimal
//! `runit`/`systemd`-style keeper built on `Supervisor` (see
//! `src/supervisor.rs`).
//!
//! The supervised child (`cargo --version`) always exits cleanly and
//! immediately, so `RestartPolicy::Always` — not a crash — is what keeps the
//! supervisor restarting it; `stop_when` is what ends supervision once it has
//! seen enough runs, with `max_restarts` as a safety net in case that
//! condition never fires.
//!
//! Run with: `cargo run --example supervisor`

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use processkit::{Command, RestartPolicy, Supervisor};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let command = Command::new("cargo").arg("--version");

    // Stop once the fifth run completes (`restarts == 4`, since the first run
    // is not itself a restart).
    let runs_seen = AtomicU32::new(0);

    let outcome = Supervisor::new(command)
        .restart(RestartPolicy::Always)
        .backoff(Duration::from_millis(50), 2.0)
        .max_backoff(Duration::from_millis(200))
        .max_restarts(20) // safety net: bounds the loop even if stop_when never fires
        .stop_when(move |_| runs_seen.fetch_add(1, Ordering::SeqCst) == 4)
        .run()
        .await?;

    println!("stopped: {:?}", outcome.stopped);
    println!("restarts: {}", outcome.restarts);
    println!("storm pauses: {}", outcome.storm_pauses);
    println!("final exit code: {:?}", outcome.final_result.code());
    Ok(())
}
