# `regex-match` — example Permafrost rule

Fires an alert when any log line in a push batch matches a regex. This is the reference
implementation of [`rules/ABI.md`](../../ABI.md); copy the directory and edit the consts.

## Configure

Everything tunable is at the top of [`src/lib.rs`](src/lib.rs):

| Const | Default | Meaning |
|-------|---------|---------|
| `RULE_NAME` | `"regex-match"` | Alert name, also the host's dedup key |
| `SEVERITY` | `"error"` | `warn` \| `error` \| `crit` |
| `PATTERN` | `(?i)\b(panic\|fatal\|out of memory)\b` | Tested against every line |
| `MAX_MATCHES` | `10` | Sample matches in the result (ABI cap) |

## Build

```bash
cargo build --release --target wasm32-unknown-unknown
```

The module lands at:

```
target/wasm32-unknown-unknown/release/regex_match.wasm
```

(note the underscore — cargo mangles the crate name).

## Install

Copy it into your `--rules-dir`:

```bash
cp target/wasm32-unknown-unknown/release/regex_match.wasm /path/to/rules/
```

The collector hot-reloads that directory, so no restart is needed.

## Test

```bash
cargo test
```

Runs the rule logic on the host target. For the input batch

```json
{"streams":[{"labels":{"job":"api"},"entries":[{"ts":1234567890,"line":"FATAL: worker died"}]}]}
```

`check` returns a pointer/length pair addressing these bytes:

```json
{"matches":[{"line":"FATAL: worker died","ts":1234567890}],"message":"1 line(s) matched rule 'regex-match'","rule":"regex-match","severity":"error"}
```

Note the `\b` word boundaries in the default pattern: `panicked` does **not** match `panic`.
