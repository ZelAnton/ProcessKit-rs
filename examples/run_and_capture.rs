//! Run a command and capture its output two ways.
//!
//! - `run()` requires a zero exit and returns stdout as a trimmed `String`.
//! - `output_string()` treats the exit code as *data* — a non-zero exit is in
//!   the [`ProcessResult`], not an error.
//!
//! Run with: `cargo run --example run_and_capture`

use processkit::Command;

#[tokio::main]
async fn main() -> processkit::Result<()> {
    // Success required; trailing whitespace trimmed.
    let version = Command::new("cargo").arg("--version").run().await?;
    println!("cargo reports: {version}");

    // Exit code as data — `Err` only if the run couldn't happen at all.
    let result = Command::new("cargo")
        .arg("--version")
        .output_string()
        .await?;
    println!(
        "exit={:?} success={} stdout_len={}",
        result.code(),
        result.is_success(),
        result.stdout().len(),
    );

    Ok(())
}
