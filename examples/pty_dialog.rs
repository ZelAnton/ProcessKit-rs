//! Drive an unterminated terminal prompt through a real PTY, then resize the
//! live session. The example launches a child copy of itself, so it needs no
//! platform-specific shell or external interactive program.
//!
//! Run with: `cargo run --example pty_dialog --features pty`

use processkit::Command;
use std::io::{BufRead, Write};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--child") {
        print!("Name: ");
        std::io::stdout().flush()?;

        let mut answer = String::new();
        std::io::stdin().lock().read_line(&mut answer)?;
        println!("Hello, {}!", answer.trim());
        return Ok(());
    }

    let mut run = Command::new(std::env::current_exe()?)
        .arg("--child")
        .use_pty()
        .pty_size(100, 30)
        .sanitize_vt()
        .keep_stdin_open()
        .start()
        .await?;

    run.wait_for_output(|tail| tail.contains("Name:"), Duration::from_secs(5))
        .await?;
    run.resize_pty(120, 40)?;

    let mut input = run.take_stdin().expect("PTY stdin was kept open");
    input.write_line("ProcessKit").await?;
    input.finish().await?;

    let result = run.output_string().await?;
    print!("{}", result.stdout());
    Ok(())
}
