//! Launch a deliberately detached child and observe that dropping its minimal
//! handle does not kill it. The child copy of this example writes one marker
//! and exits, so running the example never leaves a daemon behind.
//!
//! Run with: `cargo run --example detached`

use processkit::Command;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().as_deref() == Some(std::ffi::OsStr::new("--child")) {
        let marker = args.next().expect("child marker path");
        std::fs::write(marker, b"detached child completed\n")?;
        return Ok(());
    }

    let marker = std::env::temp_dir().join(format!(
        "processkit-detached-example-{}.txt",
        std::process::id()
    ));
    if marker.exists() {
        std::fs::remove_file(&marker)?;
    }

    let pid = {
        let child = Command::new(std::env::current_exe()?)
            .arg("--child")
            .arg(&marker)
            .spawn_detached()?;
        child.pid()
    }; // the handle ends here; the detached process keeps running
    println!("detached pid = {pid}; handle released");

    let deadline = Instant::now() + Duration::from_secs(5);
    let message = loop {
        match std::fs::read_to_string(&marker) {
            Ok(message) if message == "detached child completed\n" => break message,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "detached child did not write its marker within five seconds",
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    print!("{message}");
    std::fs::remove_file(marker)?;
    Ok(())
}
