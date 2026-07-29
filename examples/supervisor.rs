//! Keep a short-lived child alive with policy-driven restarts, exponential
//! backoff, a restart budget, a `stop_when` condition, and an opt-in liveness
//! `health_check`, and typed lifecycle events — a minimal
//! `runit`/`systemd`-style keeper built on `Supervisor` (see
//! `src/supervisor.rs`).
//!
//! The supervised child (`cargo --version`) always exits cleanly and
//! immediately, so `RestartPolicy::Always` — not a crash — is what keeps the
//! supervisor restarting it; `stop_when` is what ends supervision once it has
//! seen enough runs, with `max_restarts` as a safety net in case that
//! condition never fires. The `health_check` here is illustrative: it would
//! force-restart an incarnation that stopped answering the probe, but this
//! instant-exit child never lives long enough for the first probe to fire — so
//! `liveness_kills` stays `0`. For a real long-lived server it would detect a
//! wedged-but-alive process the exit-driven policy can't see.
//!
//! Run with: `cargo run --example supervisor`

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use processkit::prelude::StreamExt;
use processkit::{Command, RestartPolicy, Supervisor};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let command = Command::new("cargo").arg("--version");

    // Stop once the fifth run completes (`restarts == 4`, since the first run
    // is not itself a restart).
    let runs_seen = AtomicU32::new(0);

    let mut session = Supervisor::new(command)
        .restart(RestartPolicy::Always)
        .backoff(Duration::from_millis(50), 2.0)
        .max_backoff(Duration::from_millis(200))
        .max_restarts(20) // safety net: bounds the loop even if stop_when never fires
        // Liveness: probe every second; force-restart after 3 consecutive misses.
        // A real server would check a port / health endpoint here.
        .health_check(|| async { true }, Duration::from_secs(1))
        .stop_when(move |_| runs_seen.fetch_add(1, Ordering::SeqCst) == 4)
        .start();

    let mut events = session.events()?;
    let observe = async move {
        while let Some(event) = events.next().await {
            println!("event: {} ({event:?})", event.name());
        }
    };
    let ((), outcome) = tokio::join!(observe, session.wait());
    let outcome = outcome?;

    println!("stopped: {:?}", outcome.stopped);
    println!("restarts: {}", outcome.restarts);
    println!("storm pauses: {}", outcome.storm_pauses);
    println!("liveness kills: {}", outcome.liveness_kills);
    println!("final exit code: {:?}", outcome.final_result.code());
    Ok(())
}
