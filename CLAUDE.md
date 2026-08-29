# permafrost — agent notes

Read `README.md` first; it is the spec. This file holds the invariants an agent
has to keep and the burn log of decisions taken while building.

## Invariants

- Rust 2024, `thiserror` for typed errors, no `unwrap` outside tests.
- `AIDEV-NOTE:` comments for non-obvious decisions. If you delete the code, delete the note.
- Secrets only from env. Never log a webhook URL or a ticket at `info`; `redact()`
  in `config.rs` exists for this and returns `scheme://host` only.
- All iroh types stay in `crates/collector/src/p2p.rs` and `crates/web-node`.
  `crates/protocol` is iroh-free on purpose: it must compile for wasm32.
- The workspace excludes `crates/web-node` and `rules/examples/*`. They are
  wasm-only and would break a native `cargo test --workspace`.

## Layout

| Path | What |
|---|---|
| `crates/protocol` | Wire format: ticket, manifest, `FetchRequest`, `BlobHash`. No I/O. |
| `crates/collector` | The `permafrost` binary: ingest, segments, rules, alerts, iroh. |
| `crates/web-node` | Browser wasm node. Built by `web/build-web-node.sh`. |
| `web/` | Static page. No framework, no bundler beyond wasm-pack output. |
| `rules/` | WASM rule ABI (`ABI.md`) and example rules. |
| `deploy/` | Dockerfile, promtail example. |

## Build

```sh
cargo test --workspace                 # native: collector + protocol
./web/build-web-node.sh                # wasm: needs a wasm-capable clang, see below
./web/fetch-vendor.sh                  # duckdb-wasm into web/vendor/
(cd rules/examples/regex-match && cargo build --release --target wasm32-unknown-unknown)
```

## Burn log

### Phase 1 spike — GO, via the fallback protocol

**Question:** does `iroh-blobs` compile and run under wasm-bindgen today, or do we
ship `permafrost/fetch/1`?

**Answer: ship the fallback.** `crates/web-node` links `iroh` 1.1 alone and
compiles clean for `wasm32-unknown-unknown`; `iroh-blobs` 0.103 pulls
`FsStore` and a filesystem-backed tag store that has no browser equivalent.
The collector serves both ALPNs (`iroh_blobs::ALPN` and `permafrost/fetch/1`),
so native peers still get bao verified streaming and only the browser takes the
simpler path. What the browser gives up is incremental hash verification of the
blob; acceptable for v1, where possession of the ticket is already read access.

**Verified:** `cargo check --target wasm32-unknown-unknown` on `crates/web-node`
passes, and `wasm-pack build --target web` emits `web_node.js` +
`web_node_bg.wasm` (2.7 MB) with `PermafrostNode.connect/fetch_blob/
fetch_manifest/node_id/connection_type` on the JS side. The protocol itself is
covered natively by `crates/collector/tests/fetch_protocol.rs`, which stands up
two real iroh endpoints and fetches a blob whole, by range, and by a hash that
does not exist. Not yet verified from a real browser tab against a live
collector — that is the remaining Phase 5 smoke test.

**DEVIATION: `ring` needs a wasm-capable clang.** iroh pulls `ring`, whose build
script compiles C for wasm32, and Apple's bundled clang cannot target wasm
("No available targets are compatible with triple wasm32-unknown-unknown").
`web/build-web-node.sh` points `CC_wasm32_unknown_unknown` at Homebrew LLVM and
fails with a readable message if that clang is also incapable. Ubuntu's clang
works unmodified, so CI needs no special casing.

### Phase 2 — ingest and segments

**DEVIATION: no `duckdb` crate.** The README asks for the native `duckdb` crate
as a dev-dependency. Rejected: `duckdb` 1.10505.0 binds DuckDB 1.5.x while the
locally installed `libduckdb` is 1.2.2 (ABI mismatch), and the `bundled` feature
means a 10–15 minute C++ build on every clean CI run for one assertion.
`crates/collector/tests/push_to_duckdb.rs` shells out to the `duckdb` CLI
instead, and skips only the DuckDB half if the binary is absent — the Parquet
round-trip itself is asserted in-process with the `parquet` crate either way.
CI installs the CLI so nothing silently skips there.

**No `protoc`.** The `logproto.PushRequest` prost structs in `ingest.rs` are
transcribed by hand rather than generated, so building permafrost needs no
protobuf compiler and no vendored Loki `.proto`.

**Segment filenames carry the time range** (`seg-{start_ns}-{end_ns}.parquet`)
precisely so the retention loop never has to open and parse a Parquet footer.

### Phase 3 — iroh, manifest, ticket

iroh 1.1 renamed `NodeAddr`/`NodeId` to `EndpointAddr`/`EndpointId`; the README
was written against 1.0.3 naming. `NodeAddrWire` in `crates/protocol` keeps the
old field names on the wire, which is fine — it is our own format, and keeping
it stable insulates the page from further iroh churn.

`store.add_path` returns a persistent `TagInfo`, not a `TempTag`, so announced
segments survive restart and GC without the collector holding tags alive.

### Phase 4 — rules

Fuel + a 16 MB `StoreLimits` memory cap per evaluation; a rule that traps or
exhausts fuel is disabled and self-alerted, and ingest continues. `reload()`
clears the disabled map so an operator edit re-enables a rule.

Rule host tests assemble WAT fixtures inline rather than depending on the
example crates being compiled to wasm first, so `cargo test` needs no wasm
toolchain.

`RuleOutcome.labels` is `#[serde(skip)]`: the host fills it in, not the rule, so
a rule cannot forge labels into the deep link.

### Phase 5 — web page

Ticket lives in the URL fragment, so it never reaches the page's HTTP server.
Unix nanosecond timestamps exceed `Number.MAX_SAFE_INTEGER`, so `app.js` handles
all timestamps as `BigInt`; parsing them as Numbers would silently shift alert
windows.

**Open Question 3 resolved, favourably.** DuckDB 1.2.2 returns a scalar
`VARCHAR` for `labels['pod']` and `NULL` for a missing key — no list unwrapping,
no error. The promoted label set stays `job`, `host`, `level`.

**DEVIATION: duckdb-wasm has no stable 1.33.x on npm**; that line ships only as
`-devN.0` prereleases. `web/fetch-vendor.sh` pins `1.33.1-dev57.0`, overridable
via `DUCKDB_WASM_VERSION`.

### Phase 6 — ops

Metrics are hand-rolled Prometheus text exposition. Nine counters do not justify
a metrics framework.

`.dockerignore` uses `**/target/`, not `target/`. `rules/examples/*` and
`crates/web-node` are separate cargo roots with their own `target/`, and a bare
`target/` only matches the one at the context root — with it, the build context
was 1.1 GB instead of 19 kB.

Those same separate cargo roots mean `cargo fmt --all` and
`cargo clippy --workspace` at the repo root do not cover `crates/web-node` or
`rules/examples/*`. CI checks each root explicitly in the `wasm` and `rules`
jobs; the `rules` job runs host tests only for `regex-match`, because
`hostile-loop` does not return when run natively.

The demo is verified end to end: `docker run` reaches an n0 relay with no
published iroh port, a JSON push flushes a segment, and the segment is announced
as a blob.

### Found by actually running it

- **Retention could delete a segment before it was announced.** The maintenance
  tick flushed first and purged second, so a segment whose *log timestamps* were
  already older than the retention window (backfill, or a fixture with an old
  timestamp) was written, handed to the announce task, and deleted in the same
  tick — the announce then failed on a missing file. The tick now purges before
  flushing, which gives the announce task a full 30 s window. Found by running
  the Docker demo with a 2023 timestamp, not by any test. Re-verified against a
  rebuilt image: push → `segments_flushed_total 1` → `segment announced ...
  rows=1`, with the Parquet file still on disk and no `blob announce failed`.
- **`.dockerignore` needed `**/target/`** (see Phase 6 above), found when the
  build context came to 1.1 GB.

### Found while writing the acceptance tests

- `permafrost_rules_disabled_total` was rendered but never incremented. Now
  bumped in `Alerter::dispatch` on `RuleEvent::Disabled`, which is the single
  point every disabled event passes through. Covered by
  `tests/rule_to_slack.rs`.
- `tests/rule_to_slack.rs` takes ~110 s in a debug build. Nearly all of it is
  wasmtime compiling the 1 MB `regex_match.wasm` with an unoptimised Cranelift.
  A release collector compiles the same module in a couple of seconds at
  startup, so this is a test-speed problem, not a startup-time one — but if the
  rule set grows large, measure `permafrost` startup before assuming that holds.

## Known gaps

- Phase 5's Playwright smoke test is not written. The page's query generation is
  verified against a real spec-schema Parquet file via the DuckDB CLI, and its
  four degradation paths are exercised, but no test drives a real browser
  against a live collector.
- Acceptance items that need a running deployment are unverified: real promtail,
  the <10 s 50 MB relay fetch, and the firewall test.
- Open Question 2 (n0 relay fair-use for launch traffic) and Question 4 (segment
  encryption at rest) are deferred, not answered. Both are launch-prep, not
  build-blocking.
