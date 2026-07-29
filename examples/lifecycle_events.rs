//! Consume `Started -> output -> Exited` as one typed lifecycle stream. The
//! event stream and `finish()` must be polled together because reaping produces
//! the terminal `Exited` event.
//!
//! Run with: `cargo run --example lifecycle_events`

use processkit::prelude::StreamExt;
use processkit::{Command, ProcessEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--child") {
        println!("ready");
        eprintln!("working");
        println!("done");
        return Ok(());
    }

    let mut run = Command::new(std::env::current_exe()?)
        .arg("--child")
        .start()
        .await?;
    let mut events = run.events()?;

    let render = async {
        while let Some(event) = events.next().await {
            match event {
                ProcessEvent::Started { pid } => eprintln!("started {pid:?}"),
                ProcessEvent::Stdout(line) => println!("stdout: {}", line.text()),
                ProcessEvent::Stderr(line) => eprintln!("stderr: {}", line.text()),
                ProcessEvent::Exited(outcome) => eprintln!("exited: {outcome:?}"),
                _ => {}
            }
        }
    };

    let (_, finished) = tokio::join!(render, run.finish());
    let finished = finished?;
    eprintln!("finish confirmed: {:?}", finished.outcome);
    Ok(())
}
