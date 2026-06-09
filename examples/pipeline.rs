//! Chain commands stdout→stdin without a shell — no quoting, no injection
//! surface. The outcome is *pipefail*: stdout comes from the last stage, the
//! reported failure from the first stage that didn't exit cleanly. All stages
//! share one kill-on-drop group.
//!
//! Run with: `cargo run --example pipeline`
//! (Uses `sort`/`uniq`, available on Unix; on Windows substitute equivalents.
//! Run it inside a git repository.)

use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let authors = Command::new("git")
        .args(["log", "--format=%an", "-n", "50"])
        .pipe(Command::new("sort"))
        .pipe(Command::new("uniq").arg("-c"))
        .output_string()
        .await?;

    println!("recent commit counts by author:\n{}", authors.stdout());
    Ok(())
}
