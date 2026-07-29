//! Run a bounded fan-out and consume results in completion order while keeping
//! each result associated with its original input index.
//!
//! Run with: `cargo run --example batch_stream`

use processkit::prelude::StreamExt;
use processkit::{Command, JobRunner, output_stream};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _program = args.next();
    if args.next().as_deref() == Some("--child") {
        let delay_ms: u64 = args.next().expect("delay").parse().expect("integer delay");
        let label = args.next().expect("label");
        std::thread::sleep(Duration::from_millis(delay_ms));
        println!("{label}");
        return Ok(());
    }

    let executable = std::env::current_exe()?;
    let commands = [(180, "slow"), (20, "fast"), (80, "middle")]
        .into_iter()
        .map(|(delay, label)| {
            Command::new(&executable)
                .arg("--child")
                .arg(delay.to_string())
                .arg(label)
        });

    let runner = JobRunner;
    let mut results = output_stream(commands, 2, &runner);
    while let Some((input_index, result)) = results.next().await {
        let result = result?;
        println!("input #{input_index} completed: {}", result.stdout().trim());
    }
    Ok(())
}
