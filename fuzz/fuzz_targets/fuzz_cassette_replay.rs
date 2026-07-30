#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use serde_json::json;

/// A capture candidate. The target builds these into valid cassette entries,
/// including recorded errors, then makes a structured sequence of matching and
/// deliberately missing invocations against the real replay implementation.
#[derive(Debug, Arbitrary)]
struct EntryInput {
    program: String,
    args: Vec<String>,
    stdout: String,
    stderr: String,
    error_message: String,
    kind: u8,
}

#[derive(Debug, Arbitrary)]
struct Input {
    entries: Vec<EntryInput>,
    calls: Vec<u8>,
}

fuzz_target!(|input: Input| {
    // Bound the work per test case while preserving duplicates (which exercise
    // capture order and repeat-last) and arbitrary text in every key/output.
    let entries: Vec<_> = input.entries.into_iter().take(32).collect();
    let cassette_entries: Vec<_> = entries
        .iter()
        .map(|entry| {
            let mut value = json!({ "program": entry.program, "args": entry.args,
            "stdout": entry.stdout, "stderr": entry.stderr });
            let object = value.as_object_mut().expect("JSON object");
            match entry.kind % 4 {
                0 => {
                    object.insert(
                        "error".to_owned(),
                        json!({ "kind": "Unsupported", "operation": entry.error_message }),
                    );
                }
                1 => {
                    object.insert("code".to_owned(), json!(0));
                }
                2 => {
                    object.insert("timed_out".to_owned(), json!(true));
                }
                _ => {
                    object.insert("signal".to_owned(), json!(9));
                }
            }
            value
        })
        .collect();
    let cassette = json!({ "version": 4, "entries": cassette_entries }).to_string();
    let calls: Vec<_> = input
        .calls
        .into_iter()
        .take(64)
        .enumerate()
        .map(|(index, selector)| {
            if !entries.is_empty() && selector % 2 == 0 {
                let entry = &entries[usize::from(selector) % entries.len()];
                (entry.program.clone(), entry.args.clone())
            } else {
                // This pair cannot collide with an entry: grow the argument until
                // it differs from every recorded key.
                let program = format!("__processkit_fuzz_miss_{index}__");
                let mut args = vec!["--miss".to_owned()];
                while entries
                    .iter()
                    .any(|entry| entry.program == program && entry.args == args)
                {
                    args.push("_".to_owned());
                }
                (program, args)
            }
        })
        .collect();
    processkit::fuzz_cassette_replay(&cassette, &calls);
});
