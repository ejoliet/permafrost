# `web/vendor/` — third-party assets

This directory is **not checked in** and is **not built by a bundler**. The static
page is exactly `index.html` + `app.js` + the two vendored ES modules below
(README §Constraints: *"no framework, no bundler beyond wasm-pack output"*).

Add it to `.gitignore` — the DuckDB wasm binaries alone are ~74 MB:

```gitignore
web/vendor/
```

Expected layout once populated:

```
web/vendor/
├── duckdb/                          # from npm  (./web/fetch-vendor.sh)
│   ├── .version                     # pin stamp, written by the script
│   ├── duckdb-browser.mjs           #  31 KB   ES module entry point
│   ├── duckdb-browser.mjs.map       # 129 KB   optional
│   ├── duckdb-mvp.wasm              #  39 MB   baseline bundle
│   ├── duckdb-browser-mvp.worker.js # 820 KB
│   ├── duckdb-eh.wasm               #  34 MB   exception-handling bundle
│   ├── duckdb-browser-eh.worker.js  # 755 KB
│   └── duckdb-browser-eh.worker.js.map  # 2.3 MB  optional
└── web_node/                        # from wasm-pack
    ├── web_node.js                  # JS shim, default export = init()
    ├── web_node_bg.wasm
    └── … (.d.ts, package.json — unused by the page)
```

---

## 1. DuckDB-WASM — `./web/fetch-vendor.sh`

```bash
./web/fetch-vendor.sh            # idempotent; no-op if already at the pinned version
./web/fetch-vendor.sh --force    # re-download
DUCKDB_WASM_VERSION=1.33.1-dev58.0 ./web/fetch-vendor.sh
```

The script runs `npm pack @duckdb/duckdb-wasm@<version>`, extracts only the
`package/dist/*` files listed above, and drops them in `web/vendor/duckdb/`.
No `node_modules`, no `package.json` in the repo.

> **Version pin.** `@duckdb/duckdb-wasm` 1.33.x is published **only** as
> `-devN.0` prereleases — there is no plain `1.33.0` or `1.33.1` on npm. The
> `latest` dist-tag currently resolves to `1.33.1-dev57.0`, which is what the
> script pins. Verify with `npm view @duckdb/duckdb-wasm dist-tags`.

### Doing it by hand

```bash
npm pack @duckdb/duckdb-wasm@1.33.1-dev57.0
tar -xzf duckdb-duckdb-wasm-1.33.1-dev57.0.tgz
mkdir -p web/vendor/duckdb
cp package/dist/duckdb-browser.mjs \
   package/dist/duckdb-browser.mjs.map \
   package/dist/duckdb-mvp.wasm \
   package/dist/duckdb-browser-mvp.worker.js \
   package/dist/duckdb-eh.wasm \
   package/dist/duckdb-browser-eh.worker.js \
   web/vendor/duckdb/
echo 1.33.1-dev57.0 > web/vendor/duckdb/.version
```

### Which bundle is selected at runtime

`app.js` does **not** hard-code a bundle. It hands both to DuckDB's own feature
detector and lets it pick:

```js
const bundle = await duckdb.selectBundle({
  mvp: { mainModule: 'duckdb-mvp.wasm', mainWorker: 'duckdb-browser-mvp.worker.js' },
  eh:  { mainModule: 'duckdb-eh.wasm',  mainWorker: 'duckdb-browser-eh.worker.js' },
});
```

- **`eh`** — WebAssembly exception handling. Chosen on every current
  Chrome/Firefox/Safari. Faster and smaller; this is the normal path.
- **`mvp`** — baseline WebAssembly, no exception handling. The fallback for
  older engines. Ship it too; it is the only reason the page degrades instead of
  failing on an old browser.
- **`coi`** — cross-origin-isolated (multi-threaded) bundle. **Deliberately not
  vendored.** It needs `Cross-Origin-Opener-Policy: same-origin` +
  `Cross-Origin-Embedder-Policy: require-corp` response headers, which we cannot
  assume from a plain static host. Single-threaded is fast enough for the
  ~50 MB-segment acceptance criterion.

The workers are classic (non-module) scripts, loaded same-origin as
`new Worker(url)` — no Blob-URL shim needed.

---

## 2. `web-node` — the iroh browser node (wasm-pack)

Built from `crates/web-node` (the wasm-bindgen wrapper; there is no npm package
for iroh-in-the-browser, see README §Recommended Stack):

```bash
# from the repo root
wasm-pack build --target web --out-dir ../../web/vendor/web_node crates/web-node
```

`--target web` is required: it emits a `web_node.js` shim with a **default
export `init()`** that `app.js` awaits before touching any exported class.
`--target bundler` output will not load without a bundler.

Prerequisites: the `wasm32-unknown-unknown` target and `wasm-pack`.

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-pack
```

> The `--out-dir` is resolved **relative to the crate directory**, which is why
> the path is `../../web/vendor/web_node` and not `web/vendor/web_node`.

### JS API `app.js` codes against

```js
import init, { PermafrostNode } from './vendor/web_node/web_node.js';

await init();                                        // loads web_node_bg.wasm
const node = await PermafrostNode.connect(ticket);   // ticket = raw base32 `t=` string
const id   = await node.node_id();                   // -> String
const json = await node.fetch_manifest();            // -> String (manifest JSON)
const buf  = await node.fetch_blob(hash, 0, 0);      // -> Uint8Array; (0, 0) = whole blob
```

Everything is awaited and everything is assumed fallible. `hash` is the
lowercase base32 string straight out of the manifest's `segments[].hash`.

---

## Serving the page

Any static file server works, but it **must** serve `.wasm` as
`application/wasm` (DuckDB uses streaming instantiation) and it must be HTTP,
not `file://` (ES modules + workers are blocked on `file://`).

```bash
python3 -m http.server 8080 --directory web
# then open http://localhost:8080/#t=...&from=...&to=...
```

If `web/vendor/` is empty, the page does not crash: it detects the missing
assets up front and renders *"vendor assets missing — run web/fetch-vendor.sh"*.
