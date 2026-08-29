# `hostile-loop` — runaway rule test fixture

**Do not install this in a real `--rules-dir`.** `check` never returns.

It exists so the rule engine can be tested against the acceptance criterion "a hostile
infinite-loop rule is fuel-killed without ingest disruption" (README §Acceptance Criteria): the
host must exhaust `PERMAFROST_RULE_FUEL`, trap the instance, disable the rule, post one Slack
notice, and keep ingesting.

Exports the same `alloc` / `check` pair as [`rules/ABI.md`](../../ABI.md).

## Build

```bash
cargo build --release --target wasm32-unknown-unknown
```

Output: `target/wasm32-unknown-unknown/release/hostile_loop.wasm`
