//! Stream a child's stdout line by line as it arrives, then collect the exit
//! code and buffered stderr. No waiting for exit, no full-output buffering;
//! stderr is drained in the background so the child can't block.
//!
//! Run with: `cargo run --example streaming`

use processkit::{Command, Finished, StreamExt};

#[tokio::main]
async fn main() -> processkit::Result<()> {
    let mut run = Command::new("cargo").args(["--version"]).start().await?;

    let mut lines = run.stdout_lines()?;
    while let Some(line) = lines.next().await {
        println!("stdout: {line}");
    }

    let Finished { outcome, stderr } = run.finish().await?;
    println!("exit={outcome:?}");
    if !stderr.is_empty() {
        eprintln!("stderr: {stderr}");
    }
    Ok(())
}
