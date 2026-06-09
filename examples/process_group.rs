//! Spawn children into one kill-on-drop group. Dropping the group — or calling
//! `shutdown()` — tears down the whole tree (grandchildren included), so an
//! early return or panic never leaks subprocesses.
//!
//! Run with: `cargo run --example process_group`

use processkit::{Command, ProcessGroup};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let group = ProcessGroup::new()?;

    // In a real program these would be long-running (a dev DB, an API server …).
    // They all share the group's fate.
    let _a = group.start(&Command::new("cargo").arg("--version")).await?;
    let _b = group.start(&Command::new("cargo").arg("--version")).await?;

    // Graceful teardown: SIGTERM → bounded wait → SIGKILL on Unix; atomic on
    // Windows. Or just `drop(group)` for an immediate hard kill of everything.
    group.shutdown().await?;
    println!("group torn down");
    Ok(())
}
