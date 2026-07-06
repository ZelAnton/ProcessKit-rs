//! Wrap an external CLI behind a typed API with the `cli_client!` macro. The
//! generated type carries a [`ProcessRunner`], so the *same* wrapper is testable
//! with a `ScriptedRunner` (no subprocess) — see `docs/testing.md`.
//!
//! Run with: `cargo run --example cli_client` (inside a git repository)

use processkit::{ProcessRunner, Result, cli_client};

cli_client!(
    /// Typed wrapper around the `git` CLI.
    pub struct Git => "git"
);

impl<R: ProcessRunner> Git<R> {
    /// The current branch name (`git branch --show-current`).
    ///
    /// # Errors
    ///
    /// Propagates any failure from running `git` — the tool not being found, a
    /// non-zero exit, or a timeout — as the crate's [`Error`](processkit::Error).
    pub async fn current_branch(&self) -> Result<String> {
        self.core
            .run(self.core.command(["branch", "--show-current"]))
            .await
    }

    /// Whether the working tree is clean (`git diff --quiet` → exit 0).
    ///
    /// # Errors
    ///
    /// Propagates any failure from running `git diff --quiet` other than the
    /// `0`/`1` exit that encodes the answer (e.g. `git` not found, or a timeout)
    /// as the crate's [`Error`](processkit::Error).
    pub async fn is_clean(&self) -> Result<bool> {
        self.core
            .probe(self.core.command(["diff", "--quiet"]))
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let git = Git::new(); // real runner; tests would use `Git::with_runner(scripted)`
    println!("on branch: {}", git.current_branch().await?);
    println!("working tree clean: {}", git.is_clean().await?);
    Ok(())
}
