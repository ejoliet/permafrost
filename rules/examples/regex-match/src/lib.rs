//! Permafrost example alert rule: fire when a log line matches a regex.
//!
//! Implements the ABI in `rules/ABI.md`. Copy this file, edit the four consts below,
//! `cargo build --release --target wasm32-unknown-unknown`, drop the `.wasm` in `--rules-dir`.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Rule configuration — this is the only part you need to change.
// ---------------------------------------------------------------------------

/// Rule identity. Shown in Slack and used by the host as the alert's dedup key.
const RULE_NAME: &str = "regex-match";

/// One of `warn`, `error`, `crit`. Anything else is rejected by the host.
const SEVERITY: &str = "error";

/// The pattern every log line is tested against.
const PATTERN: &str = r"(?i)\b(panic|fatal|out of memory)\b";

/// Sample matches carried in the result. The ABI caps this at 10.
const MAX_MATCHES: usize = 10;

// ---------------------------------------------------------------------------
// ABI exports
// ---------------------------------------------------------------------------

/// Reserve `len` bytes for the host to write the input batch into.
///
/// AIDEV-NOTE: the allocation is deliberately leaked. The ABI gives the host ownership of the
/// pointer and defines no `dealloc`; reclaiming it here would need the exact capacity, which the
/// host does not report back. Growth is bounded by the host's 16 MB linear-memory cap.
#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: u32) -> u32 {
    let buf = vec![0u8; len as usize];
    // `into_boxed_slice` guarantees capacity == len, so the pointer the host gets covers
    // exactly the region it is allowed to write.
    let leaked = Box::leak(buf.into_boxed_slice());
    leaked.as_mut_ptr() as u32
}

/// Evaluate one batch. Returns `(out_ptr << 32) | out_len`, or `0` for "no match".
#[unsafe(no_mangle)]
pub extern "C" fn check(ptr: u32, len: u32) -> u64 {
    // SAFETY: the host wrote exactly `len` bytes at `ptr`, which came from `alloc(len)` above
    // and was never freed. Borrowing rather than reconstructing a `Vec` keeps the leak intact.
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };

    let Some(result) = evaluate(input) else {
        return 0;
    };

    // AIDEV-NOTE: the result is leaked too — the host reads these bytes straight out of linear
    // memory right after `check` returns and never calls back to free them.
    let out = Box::leak(result.into_bytes().into_boxed_slice());
    let out_ptr = out.as_mut_ptr() as u64;
    let out_len = out.len() as u64;

    // AIDEV-NOTE: pointer/length packed into one i64 because the ABI keeps `check` to a single
    // scalar return (multi-value returns are not portable across every guest toolchain).
    // wasm32 pointers are 32-bit, so neither half can overflow into the other.
    (out_ptr << 32) | out_len
}

// ---------------------------------------------------------------------------
// Rule logic — pure, so it can be unit-tested on the host target.
// ---------------------------------------------------------------------------

// AIDEV-NOTE: `Option` rather than `unwrap`: an invalid PATTERN is an edit-time mistake, but a
// panic in wasm is a trap and the host would permanently disable the rule. Failing closed
// (never matching) is the conservative behaviour.
static RE: LazyLock<Option<Regex>> = LazyLock::new(|| Regex::new(PATTERN).ok());

/// Returns the result JSON, or `None` when nothing matched or the input is unusable.
///
/// Every field access is fallible on purpose: malformed input must yield `None`, never a panic.
fn evaluate(input: &[u8]) -> Option<String> {
    let re = RE.as_ref()?;
    let batch: Value = serde_json::from_slice(input).ok()?;
    let streams = batch.get("streams")?.as_array()?;

    let mut samples: Vec<Value> = Vec::new();
    let mut total: u64 = 0;

    for stream in streams {
        let Some(entries) = stream.get("entries").and_then(Value::as_array) else {
            continue;
        };
        for entry in entries {
            let Some(line) = entry.get("line").and_then(Value::as_str) else {
                continue;
            };
            if !re.is_match(line) {
                continue;
            }
            total += 1;
            if samples.len() < MAX_MATCHES {
                // `ts` is unix nanoseconds (i64). A missing or non-numeric one is not worth
                // dropping the alert over.
                let ts = entry.get("ts").and_then(Value::as_i64).unwrap_or(0);
                samples.push(json!({ "ts": ts, "line": line }));
            }
        }
    }

    if total == 0 {
        return None;
    }

    let result = json!({
        "rule": RULE_NAME,
        "severity": SEVERITY,
        "message": format!("{total} line(s) matched rule '{RULE_NAME}'"),
        "matches": samples,
    });
    serde_json::to_string(&result).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_MATCH: &str = r#"{"streams":[{"labels":{"job":"api"},"entries":[{"ts":1234567890,"line":"FATAL: worker died"}]}]}"#;

    #[test]
    fn emits_expected_json_for_one_match() {
        assert_eq!(
            evaluate(ONE_MATCH.as_bytes()).unwrap(),
            r#"{"matches":[{"line":"FATAL: worker died","ts":1234567890}],"message":"1 line(s) matched rule 'regex-match'","rule":"regex-match","severity":"error"}"#
        );
    }

    // AIDEV-NOTE: the `\b` in PATTERN is deliberate — "panicked" does NOT match "panic".
    #[test]
    fn word_boundary_is_enforced() {
        let batch = r#"{"streams":[{"entries":[{"ts":1,"line":"thread 'main' panicked"}]}]}"#;
        assert!(evaluate(batch.as_bytes()).is_none());
    }

    #[test]
    fn no_match_returns_none() {
        let batch = r#"{"streams":[{"labels":{},"entries":[{"ts":1,"line":"all good"}]}]}"#;
        assert!(evaluate(batch.as_bytes()).is_none());
    }

    #[test]
    fn malformed_input_returns_none() {
        assert!(evaluate(b"not json").is_none());
        assert!(evaluate(br#"{"streams":"nope"}"#).is_none());
        assert!(evaluate(br#"{"streams":[{"entries":[{"line":42}]}]}"#).is_none());
    }

    #[test]
    fn samples_are_capped_at_ten() {
        let entries: Vec<String> = (0..25)
            .map(|i| format!(r#"{{"ts":{i},"line":"FATAL {i}"}}"#))
            .collect();
        let batch = format!(r#"{{"streams":[{{"entries":[{}]}}]}}"#, entries.join(","));
        let out = evaluate(batch.as_bytes()).unwrap();
        let parsed: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["matches"].as_array().unwrap().len(), MAX_MATCHES);
        assert_eq!(parsed["message"], "25 line(s) matched rule 'regex-match'");
    }
}
