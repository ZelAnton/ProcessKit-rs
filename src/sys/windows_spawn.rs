//! Shared helpers for Windows raw `CreateProcessW` spawn paths.

use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;

use tokio::process::Command;

/// Append `arg` to a `CreateProcessW` command line using the MSVCRT quoting
/// rules `std` applies internally (which it does not expose): quote when the arg
/// is empty or holds whitespace, double any run of backslashes that precedes a
/// quote (or the closing quote), and escape embedded quotes.
pub(crate) fn append_arg(out: &mut Vec<u16>, arg: &OsStr, force_quote: bool) {
    let wide: Vec<u16> = arg.encode_wide().collect();
    let quote = force_quote
        || wide.is_empty()
        || wide
            .iter()
            .any(|&c| c == u16::from(b' ') || c == u16::from(b'\t'));
    if quote {
        out.push(u16::from(b'"'));
    }
    let mut backslashes = 0usize;
    for &c in &wide {
        if c == u16::from(b'\\') {
            backslashes += 1;
        } else {
            if c == u16::from(b'"') {
                // 2n+1 backslashes before an embedded quote: the n already
                // pushed in the loop, plus n+1 here.
                for _ in 0..=backslashes {
                    out.push(u16::from(b'\\'));
                }
            }
            backslashes = 0;
        }
        out.push(c);
    }
    if quote {
        // Double a trailing backslash run so it is not read as escaping the
        // closing quote.
        for _ in 0..backslashes {
            out.push(u16::from(b'\\'));
        }
        out.push(u16::from(b'"'));
    }
}

/// Build the NUL-terminated UTF-16 command line for the child from the resolved
/// program and its arguments (read back from the tokio `Command`).
pub(crate) fn build_command_line(cmd: &Command) -> Vec<u16> {
    let std_cmd = cmd.as_std();
    let mut line: Vec<u16> = Vec::new();
    append_arg(&mut line, std_cmd.get_program(), true);
    for arg in std_cmd.get_args() {
        line.push(u16::from(b' '));
        append_arg(&mut line, arg, false);
    }
    line.push(0);
    line
}

/// Build the double-NUL-terminated UTF-16 environment block, or `None` to inherit
/// the parent environment unchanged.
pub(crate) fn build_env_block(env: Option<Vec<(OsString, OsString)>>) -> Option<Vec<u16>> {
    let pairs = env?;
    let mut block: Vec<u16> = Vec::new();
    for (k, v) in pairs {
        block.extend(k.encode_wide());
        block.push(u16::from(b'='));
        block.extend(v.encode_wide());
        block.push(0);
    }
    // The block ends with an extra NUL; an empty block is just "\0\0".
    block.push(0);
    if block.len() == 1 {
        block.push(0);
    }
    Some(block)
}

/// A NUL-terminated wide string for a Win32 wide-string argument.
pub(crate) fn to_wide_nul(s: &OsStr) -> Vec<u16> {
    let mut v: Vec<u16> = s.encode_wide().collect();
    v.push(0);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_line_quotes_program_and_escapes_windows_arguments() {
        let mut command = Command::new(r"C:\Program Files\tool.exe");
        command.args(["", r#"a\"b"#, "trailing slash\\", "two words"]);
        let line = build_command_line(&command);
        let line = String::from_utf16(&line[..line.len() - 1]).unwrap();
        assert_eq!(
            line,
            r#""C:\Program Files\tool.exe" "" a\\\"b "trailing slash\\" "two words""#
        );
    }

    #[test]
    fn environment_block_is_double_nul_terminated() {
        assert_eq!(build_env_block(None), None);
        assert_eq!(build_env_block(Some(vec![])), Some(vec![0, 0]));
        assert_eq!(
            build_env_block(Some(vec![("A".into(), "one".into())])),
            Some(vec![65, 61, 111, 110, 101, 0, 0])
        );
    }

    #[test]
    fn wide_arguments_end_with_one_nul() {
        assert_eq!(to_wide_nul(OsStr::new("path")), vec![112, 97, 116, 104, 0]);
    }
}
