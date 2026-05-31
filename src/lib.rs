//! `processkit` — child-process management for Rust.
//!
//! Two layers, mirroring the .NET ProcessKit they are ported from:
//!
//! - **process groups** — spawn a child as the root of a process tree that is
//!   killed as a unit when the group is dropped (Windows Job Objects / POSIX
//!   process groups), so no descendant outlives its owner.
//! - **process runner** — async run-and-capture of a child's stdout/stderr and
//!   exit status, built on the group layer.
//!
//! The public surface is still being built out; track progress in `CHANGELOG.md`.

#[cfg(test)]
mod tests {
    #[test]
    fn crate_builds() {
        // Smoke test until the public API lands; replaced as `group`/`runner` grow.
        assert_eq!(2 + 2, 4);
    }
}
