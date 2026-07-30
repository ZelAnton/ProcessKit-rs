//! Shared JSON decoding and bounded parse diagnostics.

use serde::de::DeserializeOwned;

use crate::error::{ErrorReason, Result};

// Keep child-controlled fragments smaller than ErrorReason::Parse's display
// cap even after location/context text is added. The full input is never stored
// in the error, unlike a caller-built generic Parse message.
const FRAGMENT_BYTES: usize = 160;

struct Location {
    line: usize,
    column: usize,
    offset: usize,
    fragment_offset: usize,
}

pub(crate) fn decode<T>(program: &str, input: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(input).map_err(|error| {
        let offset = byte_offset(input, error.line(), error.column());
        parse_error(
            program,
            "JSON",
            input,
            Location {
                line: error.line(),
                column: error.column(),
                offset,
                fragment_offset: offset,
            },
            error,
        )
    })
}

pub(crate) fn decode_line<T>(
    program: &str,
    line_number: usize,
    line_offset: usize,
    input: &str,
) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(input).map_err(|error| {
        let column_offset = error.column().saturating_sub(1);
        let offset = line_offset
            .saturating_add(column_offset)
            .min(line_offset.saturating_add(input.len()));
        parse_error(
            program,
            "NDJSON",
            input,
            Location {
                line: line_number,
                column: error.column(),
                offset,
                fragment_offset: column_offset,
            },
            error,
        )
    })
}

fn parse_error(
    program: &str,
    format: &str,
    input: &str,
    location: Location,
    error: serde_json::Error,
) -> crate::Error {
    let detail = bounded_fragment(&error.to_string(), 0);
    ErrorReason::Parse {
        program: program.to_owned(),
        message: format!(
            "{format} decode failed at line {}, column {}, byte offset {}: {detail}; fragment `{}`",
            location.line,
            location.column,
            location.offset,
            bounded_fragment(input, location.fragment_offset.min(input.len()))
        ),
    }
    .into()
}

fn byte_offset(input: &str, line: usize, column: usize) -> usize {
    let line_start = if line <= 1 {
        0
    } else {
        input
            .match_indices('\n')
            .nth(line - 2)
            .map_or(input.len(), |(index, _)| index + 1)
    };
    line_start
        .saturating_add(column.saturating_sub(1))
        .min(input.len())
}

fn bounded_fragment(input: &str, offset: usize) -> String {
    if input.is_empty() {
        return "<empty>".to_owned();
    }

    let mut start = offset.saturating_sub(FRAGMENT_BYTES / 2).min(input.len());
    while start < input.len() && !input.is_char_boundary(start) {
        start += 1;
    }
    let mut end = start.saturating_add(FRAGMENT_BYTES).min(input.len());
    while end > start && !input.is_char_boundary(end) {
        end -= 1;
    }

    let mut fragment = String::new();
    if start > 0 {
        fragment.push('…');
    }
    for character in input[start..end].chars() {
        fragment.extend(character.escape_default());
    }
    if end < input.len() {
        fragment.push('…');
    }
    fragment
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_is_bounded_and_escapes_controls() {
        let input = format!("{}\n\x1b{}", "a".repeat(200), "z".repeat(200));
        let fragment = bounded_fragment(&input, 201);
        assert!(fragment.starts_with('…'));
        assert!(fragment.ends_with('…'));
        assert!(fragment.contains("\\n\\u{1b}"), "got {fragment:?}");
        assert!(!fragment.contains(&"a".repeat(100)));
        assert!(!fragment.contains(&"z".repeat(100)));
    }

    #[test]
    fn whole_json_error_reports_location_without_full_input() {
        let input = format!("{{\"padding\":\"{}\",\"value\": nope}}", "x".repeat(500));
        let error = decode::<serde_json::Value>("tool", &input).unwrap_err();
        let ErrorReason::Parse { program, message } = error.reason() else {
            panic!("expected Parse, got {error:?}");
        };
        assert_eq!(program, "tool");
        assert!(message.contains("line 1"));
        assert!(message.contains("byte offset"));
        assert!(message.contains("fragment `…"));
        assert!(!message.contains(&"x".repeat(200)));
    }

    #[test]
    fn whole_json_byte_offset_is_zero_based_across_lines() {
        let input = "{\n  \"value\": nope\n}";
        let error = decode::<serde_json::Value>("tool", input).unwrap_err();
        let ErrorReason::Parse { message, .. } = error.reason() else {
            panic!("expected Parse, got {error:?}");
        };
        assert!(
            message.contains("line 2, column 13, byte offset 14"),
            "got {message:?}"
        );
    }

    #[test]
    fn ndjson_byte_offset_is_zero_based_from_the_complete_stream() {
        let input = "{\"value\": nope}";
        let error = decode_line::<serde_json::Value>("tool", 2, 15, input).unwrap_err();
        let ErrorReason::Parse { message, .. } = error.reason() else {
            panic!("expected Parse, got {error:?}");
        };
        assert!(
            message.contains("line 2, column 12, byte offset 26"),
            "got {message:?}"
        );
    }

    #[test]
    fn serde_error_detail_is_bounded_before_entering_the_public_message() {
        use serde::de::Error as _;

        let detail = "sensitive".repeat(100);
        let serde_error = serde_json::Error::custom(detail.clone());
        let error = parse_error(
            "tool",
            "JSON",
            "null",
            Location {
                line: 1,
                column: 1,
                offset: 1,
                fragment_offset: 1,
            },
            serde_error,
        );
        let ErrorReason::Parse { message, .. } = error.reason() else {
            panic!("expected Parse, got {error:?}");
        };
        assert!(!message.contains(&detail));
        assert!(message.contains('…'));
    }
}
