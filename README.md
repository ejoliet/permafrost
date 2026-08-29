# Permafrost

> A Loki-compatible log endpoint that freezes your logs into Parquet and serves them peer-to-peer over iroh — Slack alert links open a browser SQL explorer with no Grafana, no VPN, no query server.

**Type**: RDD Type A — spec for agent implementation. **Implemented**; this document remains the spec, with resolutions recorded inline under §Open Questions and §Acceptance Criteria.
**Author**: Emmanuel Joliet (`ejoliet`) · **Status**: Draft · **License**: MIT

Jump to [Build](#build) · [Quick Start](#quick-start) — both verified against the current tree.

---

## Purpose

**Problem.** Loki stacks are heavy (Grafana + Loki + object store), LogQL is a niche query language, and giving anyone outside the VPN access to production logs means auth infrastructure nobody wants to run.

**Solution.** One Rust binary accepts the standard Loki push API, writes zstd Parquet segments to local disk, and serves them as content-addressed blobs over iroh. Alert rules run as sandboxed WASM modules at ingest. On match, a Slack message carries a deep link; a static browser page fetches only the relevant Parquet row groups over iroh and queries them locally with DuckDB-WASM.

**Who benefits.** Small teams running promtail/Alloy who want SQL over logs, incident links that work from anywhere, and zero standing query infrastructure.

---

## Architecture

```
promtail/Alloy ──POST /loki/api/v1/push──▶ ┌────────────────────────────┐
                                           │  permafrost-collector      │
                                           │  ├─ ingest (axum)          │
                                           │  ├─ rule engine (wasmtime) ├──▶ Slack webhook
                                           │  ├─ segment writer         │    (alert + deep link)
                                           │  │   (arrow-rs → Parquet)  │
                                           │  └─ iroh endpoint + blobs  │
                                           └──────────┬─────────────────┘
                                                      │ iroh (QUIC / relay)
                                     browser ◀────────┘
                                     static page: iroh-wasm wrapper + DuckDB-WASM
```

Data flow:

1. Agents push log batches (snappy protobuf or JSON) to the collector.
2. Rule engine evaluates each batch against loaded `.wasm` rules; matches fire Slack alerts.
3. Segment writer buffers per-stream, flushes Parquet segments on time/size triggers.
4. Each segment is announced as an iroh blob; a manifest blob indexes segments by time range and labels.
5. The Slack deep link encodes an iroh ticket + time window. The browser page fetches manifest → matching segments → queries locally.

> 💡 WASM appears twice by design: wasmtime sandboxes ingest rules on the server; DuckDB-WASM is the query engine in the browser.

---

## Recommended Stack

Researched 2026-08-23. One round of overrides welcome before implementation.

| Layer | Chosen | Version | Why chosen | Rejected |
|-------|--------|---------|------------|----------|
| P2P transport | `iroh` + `iroh-blobs` | 1.0.3 (stable since June 2026; wire protocol + bindings guaranteed) | Content-addressed blobs, NAT traversal, relay fallback, browser build exists | libp2p (heavier API, no first-class blob layer); raw WebRTC (no content addressing) |
| Browser P2P | iroh compiled via wasm-bindgen, custom wrapper crate | iroh 1.x, `default-features = false` | Official browser support; relay-only in browser but end-to-end encrypted; n0 recommends an app-specific wasm-bindgen wrapper crate (no NPM package published) | iroh NAPI JS bindings (Node-only, not browser) |
| Browser SQL | `@duckdb/duckdb-wasm` | 1.33.x (tracks DuckDB 1.5.4) | Reads Parquet natively, Arrow-fluent, tested Chrome/Firefox/Safari, dominant choice | sql.js (no Parquet, row-oriented); perspective (viz-first, weaker SQL) |
| HTTP ingest | `axum` | 0.8.x | De-facto standard tokio HTTP framework; tower middleware; iroh is tokio-native | actix-web (separate runtime idioms); rocket (smaller ecosystem) |
| Parquet writer | `parquet` + `arrow` (arrow-rs) | latest stable | Only production-grade Rust Parquet writer; zstd built in | parquet2 (unmaintained); DuckDB-as-writer (heavyweight dependency server-side) |
| WASM runtime (ingest rules) | `wasmtime` | latest stable | Bytecode Alliance reference runtime, best-documented Rust embedding, fuel metering for untrusted rules | wasmer (fine, but wasmtime embedding docs and epoch/fuel interruption are stronger); rlua/quickjs (loses "any language" story) |
| Loki wire format | `prost` + `snap` | latest | promtail default is snappy-compressed protobuf `PushRequest`; JSON fallback via serde | hand-rolled decoding |
| Slack | Incoming webhook via `reqwest` | — | No Slack app review needed for v1 | Slack Bolt app (needed only for interactive buttons, later) |

Sources: iroh 1.0 release and browser/WASM docs (iroh.computer), duckdb-wasm npm + GitHub. Key browser constraints confirmed: browser nodes connect **relay-only** (no UDP in the sandbox, still end-to-end encrypted), and the WASM build requires `default-features = false`.

> ⚠️ `iroh-blobs` in the browser is the single biggest technical risk — browser support has been a tracking issue, not a headline feature. Spike it first (Phase 1). Fallback contract is defined below so the rest of the system doesn't care which path wins.

---

## Repository Layout

```
permafrost/
├── Cargo.toml                  # workspace
├── crates/
│   ├── collector/              # the binary
│   │   ├── src/
│   │   │   ├── main.rs         # CLI + startup wiring
│   │   │   ├── ingest.rs       # axum routes, protobuf/JSON decode
│   │   │   ├── segment.rs      # buffering, flush triggers, Parquet write
│   │   │   ├── rules.rs        # wasmtime host, rule ABI, hot reload
│   │   │   ├── alert.rs        # Slack payload + deep-link builder
│   │   │   ├── p2p.rs          # iroh endpoint, blob announce, manifest
│   │   │   └── config.rs       # env + TOML config
│   │   └── tests/
│   ├── protocol/               # shared types: manifest schema, ticket codec, blob protocol
│   └── web-node/               # wasm-bindgen wrapper crate: iroh-in-browser API for the UI
├── rules/
│   ├── examples/regex-match/   # Rust → wasm example rule
│   └── ABI.md                  # rule contract (mirrors §Interface Contracts)
├── web/
│   ├── index.html              # single static page
│   ├── app.js                  # ticket parse, fetch, DuckDB-WASM glue
│   └── vendor/                 # duckdb-wasm bundle + web-node pkg output
├── deploy/
│   ├── docker/Dockerfile
│   └── promtail-example.yaml   # one-line agent config change
├── CLAUDE.md                   # agent invariants + burn log
└── README.md                   # this file
```

---

## Prerequisites

- Rust stable ≥ 1.85 (2024 edition). `wasm32-unknown-unknown` and `wasm-pack` only if you build the browser page.
- A Slack incoming webhook URL — optional; §Quick Start substitutes a local stand-in.
- A log source: existing promtail/Alloy, or just `curl`, as below.
- iroh public relays (n0-hosted) suffice; a self-hosted relay is a config change, not a code change.
- Optional: the `duckdb` CLI, so `tests/push_to_duckdb.rs` asserts its DuckDB half instead of skipping it.

---

## Build

All commands run from the repository root.

```bash
# Toolchain, once. Only needed for steps 2 and 3.
rustup target add wasm32-unknown-unknown
cargo install wasm-pack

# 1. The collector  ->  target/release/permafrost   (42 MB)
cargo build --release

# 2. The example rule  ->  rules/examples/regex-match/target/wasm32-unknown-unknown/release/regex_match.wasm
(cd rules/examples/regex-match && cargo build --release --target wasm32-unknown-unknown)

# 3. Browser assets, only for the query page
./web/fetch-vendor.sh      # duckdb-wasm  ->  web/vendor/duckdb/    (~74 MB download)
./web/build-web-node.sh    # iroh peer    ->  web/vendor/web_node/  (wasm-pack)
```

| Step | Required for | Skip it if |
|---|---|---|
| 1 | everything | never |
| 2 | firing alerts | you supply your own `.wasm` rules |
| 3 | the browser query page | you want ingest and alerting only |

Step 1 takes roughly 20 minutes from cold — nearly all of it wasmtime and its
Cranelift backend. Incremental rebuilds are seconds.

> **macOS.** `ring`, pulled in by iroh, compiles C for `wasm32`, and Apple's
> bundled clang cannot target it. `web/build-web-node.sh` points
> `CC_wasm32_unknown_unknown` at Homebrew LLVM (`brew install llvm`) and fails
> with a readable message if that clang is also incapable. Linux clang works
> unmodified, so CI needs no special casing.

Tests: `cargo test --workspace` (55 tests). `crates/web-node` and
`rules/examples/*` are separate cargo roots excluded from the workspace — they
are wasm-only and would break a native `--workspace` run — so `cargo fmt`,
`clippy` and `test` at the root do not cover them. CI checks each root
explicitly; see `.github/workflows/ci.yml`.

---

## Quick Start

The whole loop on one machine in about two minutes: no Slack workspace, no
promtail, no deployment. A local HTTP server stands in for the webhook so you
can read the alert payload yourself. Needs build steps 1–3 above.

**1. Stand up a fake Slack webhook.**

```bash
cat > /tmp/fake-webhook.py <<'PY'
import http.server, sys

class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        print(self.rfile.read(int(self.headers["content-length"])).decode())
        sys.stdout.flush()
        self.send_response(200)
        self.end_headers()
    def log_message(self, *a): pass

http.server.HTTPServer(("127.0.0.1", 9099), H).serve_forever()
PY
python3 /tmp/fake-webhook.py &
```

**2. Run the collector** with the example rule in place.

```bash
mkdir -p demo/data demo/rules
cp rules/examples/regex-match/target/wasm32-unknown-unknown/release/regex_match.wasm demo/rules/

PERMAFROST_SLACK_WEBHOOK="http://127.0.0.1:9099/services/T0/B0/FAKE" \
PERMAFROST_UI_BASE_URL="http://127.0.0.1:8080/" \
./target/release/permafrost \
  --listen 127.0.0.1:3100 \
  --data-dir demo/data \
  --rules-dir demo/rules \
  --segment-max-age 5s
```

`--segment-max-age 5s` overrides the 1 h default so the first segment appears
while you are watching. Startup looks like this — note that the webhook URL is
redacted, as it is everywhere else:

```
INFO permafrost_collector::alert: slack alerts enabled target_url=http://127.0.0.1:9099/<redacted>
INFO permafrost_collector::rules: rules loaded count=1 rules={"regex_match"}
INFO permafrost_collector::p2p: iroh endpoint listening node=0e07baa5b00948c2…
INFO permafrost: permafrost ready listen=127.0.0.1:3100 data_dir=demo/data node=0e07baa5b00948c2…
```

**3. Push a log line** the example rule matches. In a second terminal — the
collector is running in the foreground. The rule's pattern is
`(?i)\b(panic|fatal|out of memory)\b`, word-bounded, so `panicked` does *not*
match:

```bash
NOW=$(python3 -c "import time;print(int(time.time()*1e9))")
curl -XPOST http://127.0.0.1:3100/loki/api/v1/push \
  -H 'content-type: application/json' \
  --data "{\"streams\":[{\"stream\":{\"job\":\"api\",\"host\":\"web-1\",\"level\":\"error\"},
           \"values\":[[\"$NOW\",\"FATAL: payment worker died\"]]}]}"
```

`204`, and the fake webhook prints a Slack blocks message: severity emoji, rule
name, the matched line, and label chips. The first one carries **no link** —

```
_no query link: set PERMAFROST_UI_BASE_URL and wait for the first segment flush_
```

— because nothing has been announced yet. There is no ticket to link to until a
segment exists. That is the expected cold-start behaviour, not a
misconfiguration.

**4. Wait for the flush.** The maintenance tick runs every 30 s; watch for:

```
INFO permafrost_collector::p2p: segment announced segment=hizq2ebg262l7v2yxvcvyevoawmaq5fnjleupe6dujr6t7t67ikq rows=1
```

Push a second matching line now and the alert carries a real deep link:

```
http://127.0.0.1:8080/#t=byd3vjnqbfemftx7i4nbh44yhgf4at77iplid2s5nmxy3i262gjqci3ior2ha4z2f4x…
&from=1787973574475410944&to=1787974174475410944&labels=host%3Dweb-1%2Cjob%3Dapi%2Clevel%3Derror
```

The ticket sits in the **fragment**, so it never reaches the page's HTTP server;
`from`/`to` bracket the match by ±5 minutes.

**5. Open it.** Serve the page and paste the link into a browser:

```bash
python3 -m http.server 8080 --directory web
```

The page parses the ticket, dials the collector over iroh, pulls the manifest,
fetches the matching segment, and opens a DuckDB SQL editor pre-filtered to the
alert's labels and time window. Any static server works as long as it sends
`.wasm` as `application/wasm` and speaks HTTP rather than `file://` — Python
3.14's does both.

**6. Tear down:** `Ctrl-C` the collector, `pkill -f fake-webhook.py`, `rm -rf demo`.

### Going from the demo to real infrastructure

| Demo | Real |
|---|---|
| `/tmp/fake-webhook.py` | `PERMAFROST_SLACK_WEBHOOK=https://hooks.slack.com/services/…` |
| `python3 -m http.server` | the page from `web/` on any static host; set `PERMAFROST_UI_BASE_URL` to it |
| `curl` pushes | promtail with one line changed — see [`deploy/promtail-example.yaml`](deploy/promtail-example.yaml) |
| `--segment-max-age 5s` | drop it; the 1 h default is the intended setting |
| local binary | `docker run` — see [`deploy/docker/README.md`](deploy/docker/README.md) |

Only port 3100 needs to be reachable, and only by your log agents. The browser
query path requires **no inbound port**: iroh dials out and falls back to a
relay when UDP is blocked.

---

## Configuration Reference

| Env var | Type | Default | Required | Description |
|---------|------|---------|----------|-------------|
| `PERMAFROST_LISTEN` | addr | `0.0.0.0:3100` | no | Loki push API bind address (3100 = Loki's port, drop-in) |
| `PERMAFROST_DATA_DIR` | path | `./data` | no | Parquet segments + iroh blob store |
| `PERMAFROST_RULES_DIR` | path | `./rules` | no | Directory of `.wasm` rule modules; hot-reloaded on change |
| `PERMAFROST_SLACK_WEBHOOK` | url | — | yes (for alerts) | Slack incoming webhook |
| `PERMAFROST_UI_BASE_URL` | url | `https://<pages-host>/` | yes (for links) | Base URL of the static page; ticket goes in the fragment |
| `PERMAFROST_SEGMENT_MAX_AGE` | duration | `1h` | no | Time-based flush trigger |
| `PERMAFROST_SEGMENT_MAX_BYTES` | bytes | `64MiB` | no | Size-based flush trigger (uncompressed buffer estimate) |
| `PERMAFROST_RETENTION` | duration | `720h` | no | Delete segments older than this |
| `PERMAFROST_RULE_FUEL` | u64 | `10_000_000` | no | wasmtime fuel per rule per batch (kills runaway rules) |
| `PERMAFROST_SECRET_KEY_FILE` | path | `<data-dir>/iroh.key` | no | iroh node identity; generated if absent |

CLI flags mirror env vars (`--listen`, `--data-dir`, …). Env wins over TOML, flags win over env.

---

## Interface Contracts

### 1. Ingest — Loki push API

- `POST /loki/api/v1/push`
- Accept: `application/x-protobuf` (snappy-compressed `logproto.PushRequest`, promtail default) and `application/json` (Loki JSON push format).
- Respond `204` on success, `400` on decode failure, `429` with `Retry-After` under backpressure.
- Also implement `GET /ready` and `GET /metrics` (Prometheus text) — agents and operators expect them.

### 2. Parquet segment schema

| Column | Arrow type | Notes |
|--------|-----------|-------|
| `ts` | `Timestamp(Nanosecond, UTC)` | Loki timestamps are ns |
| `line` | `Utf8` | raw log line |
| `labels` | `Map<Utf8, Utf8>` | full label set |
| `job`, `host`, `level` | `Utf8` (nullable) | promoted copies of common labels for cheap filtering |

Sorted by `ts` within a segment. zstd compression. Row groups ≤ 8 MB so the browser can fetch narrow time slices. One segment file per flush across all streams (labels distinguish streams) — keeps file count and manifest small.

### 3. Manifest (JSON blob, re-announced on every flush)

```json
{
  "version": 1,
  "node": "<iroh node id>",
  "segments": [
    {
      "hash": "<blake3 blob hash>",
      "start_ns": 0,
      "end_ns": 0,
      "rows": 0,
      "bytes": 0,
      "label_index": { "job": ["api", "worker"], "level": ["error", "info"] }
    }
  ]
}
```

The manifest **is** the index — no database. `label_index` holds distinct values of promoted labels only.

### 4. Ticket / deep link

```
https://<ui-host>/#t=<ticket>&from=<unix_ns>&to=<unix_ns>&labels=<urlencoded k=v,...>
```

`<ticket>` = base32(postcard-encoded `{node_addr, manifest_hash_hint}`). Reuse iroh's `NodeAddr` serialization; define the codec in `crates/protocol` so collector and `web-node` share it. Possession of a ticket = read access (v1 model; see Non-Goals).

### 5. Blob fetch protocol (browser ↔ collector)

Primary: `iroh-blobs` over the browser endpoint.
Fallback (if the blobs crate fights wasm): a minimal custom protocol on a raw iroh bi-stream, ALPN `permafrost/fetch/1`:

- Request: `{hash, offset, len}` (postcard) — enables row-group range reads.
- Response: length-prefixed bytes.

`crates/protocol` defines both; the UI depends only on a `fetch_blob(hash, range)` trait so the spike outcome swaps in cleanly.

### 6. WASM rule ABI (`rules/ABI.md`)

- Target `wasm32-unknown-unknown`, no WASI required.
- Exports: `alloc(len: u32) -> ptr`, `check(ptr: u32, len: u32) -> u64` (packed ptr/len of result, 0 = no match).
- Input: JSON batch `{"streams":[{"labels":{...},"entries":[{"ts":ns,"line":"..."}]}]}`.
- Output: JSON `{"rule":"name","severity":"warn|error|crit","message":"...","matches":[{"ts":ns,"line":"..."}]}` (≤ 10 sample matches).
- Host imposes fuel (`PERMAFROST_RULE_FUEL`) and 16 MB memory cap; a trapping or over-budget rule is disabled and reported to Slack once.

> 💡 JSON-over-linear-memory is deliberately boring: it makes "write a rule in Go/AssemblyScript/Zig" a one-page example, which is the launch hook.

### 7. Slack alert payload

Blocks message: severity emoji + rule name, up to 3 sample lines (code block), label chips, and a button-style link `Open logs →` pointing at the deep link with `from = first_match - 5m`, `to = last_match + 5m`.

---

## Error Handling

| Error | Behavior |
|-------|----------|
| `DecodeError` (bad push payload) | 400, count metric, never crash ingest |
| `SegmentWriteError` (disk) | retry once, then 500 on ingest + Slack self-alert; buffered data held up to memory cap |
| `RuleTrap` / `RuleFuelExhausted` | disable rule, one-time Slack notice, keep ingesting |
| `SlackDeliveryError` | retry 3× exponential backoff, drop with warn log (alerts are best-effort) |
| Backpressure (buffer > 512 MB) | 429 + `Retry-After: 1` — promtail retries natively |
| iroh relay unreachable | segments still written locally; blob announce retries in background |

Named error enums per module via `thiserror`; no `anyhow` in library crates.

---

## Testing

- `cargo test --workspace` — unit tests; ingest decode tests use captured promtail protobuf fixtures; Parquet round-trip verified by reading back with `parquet` crate.
- Rule engine tests load the compiled example rule from `rules/examples`.
- Integration test: spin collector on a port, push JSON batch, assert flush produces a segment readable by DuckDB (native `duckdb` crate, dev-dependency only).
- Browser path: Playwright smoke test against the static page with a local collector + local iroh relay (`iroh-relay` binary) — CI-friendly, no public relay dependency.
- No test touches Slack: dispatcher takes a `WebhookSink` trait; tests use a mock.

**As built**, two of those five differ:

- The DuckDB assertion shells out to the `duckdb` **CLI** rather than using the
  native crate. The crate binds DuckDB 1.5.x against a locally installed
  `libduckdb` 1.2.2 (ABI mismatch), and its `bundled` feature means a 10–15
  minute C++ build on every clean CI run for one assertion. The Parquet
  round-trip is asserted in-process with the `parquet` crate either way, and CI
  installs the CLI so the DuckDB half never silently skips.
- The Playwright smoke test is **not written** — the one stated deliverable that
  is missing. `tests/fetch_protocol.rs` covers the same wire path natively
  between two live iroh endpoints, and the page's query generation is checked
  against a real spec-schema Parquet file, but nothing drives a browser.

---

## Non-Goals (v1)

- No auth/RBAC beyond ticket possession — signed, scoped, expiring tickets are the paid-tier roadmap, not v1.
- No LogQL endpoint — this replaces the query side with SQL; it only mimics Loki's *push* API.
- No S3/object-store offload, no compaction tiers, no multi-collector federation.
- No Slack interactive app (buttons/ack) — webhook only.
- No Windows collector support (macOS/Linux only).
- No log line size > 1 MB (truncate + flag).

---

## Open Questions

- [x] **`iroh-blobs` under wasm-bindgen? No — we ship `permafrost/fetch/1`.** `iroh` 1.1 alone compiles clean for `wasm32-unknown-unknown`; `iroh-blobs` 0.103 pulls `FsStore` and a filesystem tag store with no browser equivalent. The collector serves both ALPNs, so native peers keep bao verified streaming and only the browser takes the simpler path. See the Phase 1 entry in `CLAUDE.md`.
- [ ] **Deferred to launch prep:** public n0 relay bandwidth/fair-use limits for demo traffic. Nothing in the build depends on the answer; if it turns out to be tight, self-host `iroh-relay` on a small VM — the ticket already carries the relay URL, so it is a config change, not a code change.
- [x] **DuckDB-WASM over `Map` columns: fine as is.** DuckDB 1.2.2 returns a scalar `VARCHAR` for `labels['pod']` and `NULL` for a missing key — no list unwrapping, no error. Asserted in `crates/collector/tests/push_to_duckdb.rs`. The promoted set stays `job`, `host`, `level`.
- [ ] **Deferred:** segment encryption at rest. Not a cheap win — it would have to cover the iroh blob store as well as the Parquet files, or it protects nothing, and possession of a ticket is already read access in v1. Revisit together with ticket expiry.

---

## Agent Build Instructions

> Implement end-to-end from this README. Resolve Open Question 1 (spike) before Phase 2+. Ask nothing that this document already answers.

### Build Order

| Phase | Deliverable | Done when |
|-------|-------------|-----------|
| 0 | Workspace scaffold, CI (fmt, clippy, test) | `cargo clippy --workspace -- -D warnings` passes |
| 1 | **Spike**: `web-node` wasm crate connects browser → native collector via relay and fetches one blob (iroh-blobs, else fallback protocol) | Blob bytes verified in browser console; GO/NO-GO note written to `CLAUDE.md` burn log |
| 2 | Ingest + segment writer | promtail fixture push → Parquet segment readable by DuckDB in integration test |
| 3 | iroh announce + manifest + ticket codec | Ticket round-trips; manifest lists flushed segment |
| 4 | Rule engine + example regex rule + Slack dispatcher | Pushed matching line → Slack message with valid deep link |
| 5 | Static page: ticket parse → manifest → segment fetch → DuckDB pre-filled query + results table | Playwright smoke test green end-to-end |
| 6 | Dockerfile, promtail example, retention loop, `/metrics` | `docker run` demo works from clean checkout |

### Constraints

- Rust 2024 edition, typed errors (`thiserror`), no `unwrap` outside tests.
- `AIDEV-` prefixed comments for non-obvious decisions; maintain `CLAUDE.md` burn log per phase.
- Secrets only via env; never log webhook URLs or ticket contents at info level.
- Web page: no framework, no bundler beyond wasm-pack output — `index.html` + `app.js` + vendored duckdb-wasm.
- All iroh usage behind `crates/protocol` traits so relay/transport changes don't touch business logic.

### Acceptance Criteria

- [x] `cargo test --workspace` green (55 tests); `tests/push_to_duckdb.rs` covers push → Parquet → DuckDB read, for both the JSON and the snappy-protobuf push formats, and `tests/fetch_protocol.rs` fetches an announced segment between two live iroh endpoints.
- [ ] Real promtail (not just fixtures) pushes successfully with only a `url:` change. **Unverified** — needs a running collector and a promtail binary. `deploy/promtail-example.yaml` is the config to point at it.
- [ ] Slack link → browser → query results in < 10 s on a ~50 MB segment over relay. **Unverified** — needs a deployment.
- [x] A rule compiled from the `rules/examples` template loads, fires, and a hostile infinite-loop rule is fuel-killed without ingest disruption. Both halves asserted in `crates/collector/tests/rule_to_slack.rs` against the actual compiled `.wasm`.
- [ ] Kill the collector's inbound firewall except iroh: browser link still works (proves no HTTP query path). **Unverified** — needs a deployment. By construction there is no HTTP query route: the collector serves only `/loki/api/v1/push`, `/ready` and `/metrics`.
- [x] All Open Questions resolved or explicitly deferred above.

---

## Next Steps

1. Emmanuel: one override pass on the Stack table (notably: wasmtime vs wasmer, promoted label set).
2. Run Phase 1 spike (iroh-blobs-in-browser) — this gates the whole design.
3. Implement Phases 2–6 per Build Order.
4. Launch prep: demo GIF (Slack ping → SQL in tab), decide public vs self-hosted relay, HN/thenewstack draft.
