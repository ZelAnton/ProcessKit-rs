#!/usr/bin/env bash
# ============================================================================
# UserPromptSubmit hook — injects a jj workflow reminder into Claude's context
# on every new user prompt. Enforces the "describe early" / "continuation vs
# new work" decision tree documented in AGENTS.md "Version control workflow".
#
# The reminder text lives in jj-prompt-reminder.txt next to this script so it
# can be edited without touching JSON escapes.
#
# Output format: a single line of JSON wrapping the reminder in
# `hookSpecificOutput.additionalContext`, which Claude Code surfaces to the
# model as a system reminder for this turn.
# ============================================================================

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEXT_FILE="${SCRIPT_DIR}/jj-prompt-reminder.txt"

if [ ! -f "$TEXT_FILE" ]; then
    # Fail silently — a missing text file shouldn't break the user's turn.
    exit 0
fi

# JSON-escape the file contents:
#   - backslash → \\
#   - double-quote → \"
#   - newline → \n
#   - carriage return → \r  (Windows-LF files)
#   - tab → \t
escaped=$(
    sed \
        -e 's/\\/\\\\/g' \
        -e 's/"/\\"/g' \
        -e 's/\t/\\t/g' \
        -e 's/\r/\\r/g' \
        "$TEXT_FILE" \
    | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/\\n/g'
)

printf '{"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":"%s"}}\n' "$escaped"
