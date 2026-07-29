//! Record a real self-exec run, scrub its cassette, then replay it without
//! spawning the child again.
//!
//! Run with: `cargo run --example record_replay --features record`

use processkit::testing::{CassetteField, RecordReplayRunner};
use processkit::{Command, JobRunner, ProcessRunnerExt};

const SECRET: &str = "example-secret";
const REPLAY_GUARD: &str = "PROCESSKIT_RECORD_REPLAY_MUST_NOT_SPAWN";

fn scrub(field: CassetteField, text: &str) -> String {
    match field {
        CassetteField::Argument | CassetteField::Stdout | CassetteField::Stderr => {
            text.replace(SECRET, "<redacted>")
        }
        CassetteField::Cwd => text.to_owned(),
        _ => text.to_owned(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _program = args.next();
    if args.next().as_deref() == Some("--child") {
        if std::env::var_os(REPLAY_GUARD).is_some() {
            return Err("replay unexpectedly spawned a child".into());
        }
        println!("{}", args.next().expect("secret argument"));
        return Ok(());
    }

    let temp = tempfile::tempdir()?;
    let cassette = temp.path().join("self-exec.json");
    let executable = std::env::current_exe()?;
    let command = Command::new(&executable).args(["--child", SECRET]);

    let recorder = RecordReplayRunner::record(&cassette, JobRunner::new()).scrub_with(scrub);
    let live = recorder.run(&command).await?;
    assert_eq!(live, SECRET, "record mode returns the unsanitized result");
    recorder.save()?;

    let fixture = std::fs::read_to_string(&cassette)?;
    assert!(
        !fixture.contains(SECRET),
        "the scrub hook must remove the secret from the cassette"
    );

    let replayer = RecordReplayRunner::replay(&cassette)?.scrub_with(scrub);
    let replay_command = Command::new(executable)
        .args(["--child", SECRET])
        .env(REPLAY_GUARD, "1");
    let replayed = replayer.run(&replay_command).await?;
    assert_eq!(replayed, "<redacted>");

    println!("recorded `{live}`, replayed `{replayed}` without spawning");
    Ok(())
}
