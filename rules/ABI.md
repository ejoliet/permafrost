# Permafrost WASM Rule ABI

The contract between the collector's rule engine (`crates/collector/src/rules.rs`, wasmtime host)
and a `.wasm` alert rule dropped into `--rules-dir`.

This mirrors README §Interface Contracts item 6. If the two disagree, the README wins.

---

## 1. Module shape

- **Target**: `wasm32-unknown-unknown`. **No WASI.** The host provides no imports at all —
  a module that imports anything (including `wasi_snapshot_preview1`) fails to instantiate.
- **Exports** (exactly two, both required):

| Export | Signature | Meaning |
|--------|-----------|---------|
| `alloc` | `(len: u32) -> u32` | Reserve `len` bytes in the module's linear memory, return the pointer. The host writes the input JSON there. |
| `check` | `(ptr: u32, len: u32) -> u64` | Evaluate the batch at `[ptr, ptr+len)`. Return the packed pointer/length of a UTF-8 JSON result, or `0` for "no match". |

- The module must also export its `memory` (the default `wasm32-unknown-unknown` cdylib layout
  already does this). The host reads and writes it directly.

### Return packing

```
result = (ptr << 32) | len
```

- high 32 bits = pointer to a UTF-8 JSON byte string in linear memory
- low 32 bits = its length in bytes
- `0` means **no match** — the host ignores the return value entirely and fires no alert.
  A zero-length result is therefore indistinguishable from "no match"; do not return an empty
  buffer expecting it to be parsed.

Ownership: the host reads the result bytes immediately after `check` returns and never calls back
into the module to free them. Rules are expected to leak both the input and the output buffer;
the 16 MB memory cap plus the fact that the host may recycle the instance keeps this bounded.

---

## 2. Input JSON

The host writes a single UTF-8 JSON object describing one push batch:

```json
{
  "streams": [
    {
      "labels": { "job": "api" },
      "entries": [
        { "ts": 1234567890, "line": "..." }
      ]
    }
  ]
}
```

- `streams` — array, may be empty.
- `labels` — flat string→string map, the full Loki label set of that stream.
- `entries` — array, ordered by `ts` ascending.
- `ts` — **unix nanoseconds**, `i64` (JSON number, not a string).
- `line` — the raw log line, UTF-8, ≤ 1 MB (the collector truncates longer lines before this point).

A rule must tolerate missing or mistyped fields by returning `0`. See §4.

---

## 3. Output JSON

```json
{
  "rule": "name",
  "severity": "warn",
  "message": "...",
  "matches": [
    { "ts": 1234567890, "line": "..." }
  ]
}
```

- `rule` — the rule's own name; used as the alert's dedup identity and shown in Slack.
- `severity` — one of `"warn"`, `"error"`, `"crit"`. Anything else is rejected by the host.
- `message` — one-line human summary for the Slack message.
- `matches` — **at most 10** sample matches. Rules that match more should report a total in
  `message` and truncate the samples. The Slack payload shows at most 3 of these; the extra
  samples exist so the host can compute the alert's time window
  (`from = first_match - 5m`, `to = last_match + 5m`).

Key order is irrelevant — the host parses with serde.

---

## 4. Host limits

| Limit | Value | Source |
|-------|-------|--------|
| Fuel | `PERMAFROST_RULE_FUEL`, default `10_000_000` | wasmtime fuel, charged **per rule per batch** |
| Memory | 16 MB | wasmtime `StoreLimits` on linear memory growth |
| Imports | none | no WASI, no host functions, no clock, no network, no filesystem |

Failure handling (README §Error Handling, `RuleTrap` / `RuleFuelExhausted`):

- A rule that **traps** (panic, unreachable, OOB access, memory-cap hit) or **exhausts its fuel
  budget** is **disabled for the lifetime of the process** and reported to Slack **once**.
- Ingest is never disrupted: the batch is still buffered, written, and announced.
- Because a Rust `panic!` compiles to a trap, a rule that panics on malformed input takes itself
  permanently offline. Treat every field access as fallible and return `0` instead of panicking.
- `rules/examples/hostile-loop/` is the test fixture for the fuel path.

Fuel budgeting, roughly: 10 M fuel is several million wasm instructions — ample for a regex scan
over a normal batch, and it dies in well under a second on a runaway loop.

---

## 5. Writing a rule in another language

The whole ABI is two exports and a JSON string. Any language that targets
`wasm32-unknown-unknown` (or emits a freestanding `.wasm` with no imports) can implement it.

**Rust** — the reference implementation is `rules/examples/regex-match/`. `crate-type =
["cdylib"]`, `#[unsafe(no_mangle)] pub extern "C" fn`, `Box::leak` for the buffers.

**Go / TinyGo** — build with `tinygo build -o rule.wasm -target=wasm-unknown ./`. The
`wasm-unknown` target emits no WASI imports. Export with the `//export` pragma:

```go
//export alloc
func alloc(size uint32) uint32 { buf := make([]byte, size); return ptrOf(buf) }

//export check
func check(ptr, length uint32) uint64 { /* ... */ return uint64(outPtr)<<32 | uint64(outLen) }
```

Keep a package-level slice referencing every buffer you hand out, otherwise TinyGo's GC reclaims
it before the host reads it.

**AssemblyScript** — `asc rule.ts -o rule.wasm --runtime stub --use abort=`. `--runtime stub`
gives a bump allocator and drops the GC; `--use abort=` removes the only default import.

```ts
export function alloc(len: u32): u32 { return heap.alloc(len) as u32; }
export function check(ptr: u32, len: u32): u64 { /* ... */ return (u64(outPtr) << 32) | u64(outLen); }
```

**Zig** — `zig build-lib rule.zig -target wasm32-freestanding -dynamic -O ReleaseSmall`.

```zig
export fn alloc(len: u32) u32 { ... }        // a FixedBufferAllocator over a static array is enough
export fn check(ptr: u32, len: u32) u64 { ... }
```

**C** — `clang --target=wasm32 -nostdlib -Wl,--no-entry -Wl,--export=alloc -Wl,--export=check`.

Whatever the language: no imports, no `abort`/`panic` that traps on bad input, JSON in, JSON out,
`0` for no match.
