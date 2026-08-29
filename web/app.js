// permafrost — browser log explorer.
//
// Flow: parse the deep-link fragment -> connect to the collector's iroh node via
// the `web-node` wasm wrapper -> fetch the manifest -> select overlapping
// segments -> hand their Parquet bytes to DuckDB-WASM -> run a pre-filled query.
//
// No framework, no bundler. Everything below is plain ES modules loaded from
// ./vendor/, populated by ./fetch-vendor.sh + `wasm-pack build --target web`.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Labels promoted to real Parquet columns (README §2). Everything else lives
 *  in the `labels` MAP<VARCHAR,VARCHAR> column. */
const PROMOTED_COLUMNS = new Set(['job', 'host', 'level']);

/** Manifest schema version this page understands (protocol::MANIFEST_VERSION). */
const MANIFEST_VERSION = 1;

const DEFAULT_LIMIT = 500;

/** Hard cap on rendered rows, independent of the query's own LIMIT. */
const MAX_RENDERED_ROWS = 1000;

// Resolved against this module's URL so the page works from any sub-path.
const VENDOR = {
  duckdbModule: new URL('./vendor/duckdb/duckdb-browser.mjs', import.meta.url).href,
  webNodeModule: new URL('./vendor/web_node/web_node.js', import.meta.url).href,
  duckdbBundles: {
    mvp: {
      mainModule: new URL('./vendor/duckdb/duckdb-mvp.wasm', import.meta.url).href,
      mainWorker: new URL('./vendor/duckdb/duckdb-browser-mvp.worker.js', import.meta.url).href,
    },
    eh: {
      mainModule: new URL('./vendor/duckdb/duckdb-eh.wasm', import.meta.url).href,
      mainWorker: new URL('./vendor/duckdb/duckdb-browser-eh.worker.js', import.meta.url).href,
    },
  },
};

const NS_PER_MS = 1_000_000n;
const NS_PER_S = 1_000_000_000n;

// ---------------------------------------------------------------------------
// DOM handles
// ---------------------------------------------------------------------------

const el = {
  conn: document.getElementById('conn'),
  connText: document.querySelector('#conn .txt'),
  window: document.getElementById('window'),
  chips: document.getElementById('chips'),
  banner: document.getElementById('banner'),
  sql: document.getElementById('sql'),
  run: document.getElementById('run'),
  reset: document.getElementById('reset'),
  status: document.getElementById('status'),
  results: document.getElementById('results'),
};

/** Mutable page state. */
const state = {
  ctx: null,          // parsed fragment
  conn: null,         // DuckDB connection
  db: null,           // AsyncDuckDB
  defaultSql: '',     // the generated alert-scoped query
  segmentCount: 0,
  ready: false,
};

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

/** Message text of anything that got thrown — wasm-bindgen and DuckDB both
 *  reject with non-Error values often enough that this is worth centralising. */
function errText(err) {
  if (err == null) return 'unknown error';
  if (typeof err === 'string') return err;
  if (err instanceof Error) return err.message || String(err);
  if (typeof err.message === 'string' && err.message) return err.message;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}

/** Replace an element's children with plain text (never innerHTML — log lines
 *  and label values are untrusted). */
function setText(node, text) {
  node.textContent = text;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function elem(tag, className, text) {
  const n = document.createElement(tag);
  if (className) n.className = className;
  if (text !== undefined && text !== null) n.textContent = text;
  return n;
}

// ---------------------------------------------------------------------------
// Time helpers
//
// Unix nanoseconds exceed Number.MAX_SAFE_INTEGER (now ~1.8e18 vs 9.0e15), so
// every timestamp coming out of the fragment or the manifest is handled as a
// BigInt. Getting this wrong silently shifts alert windows by microseconds.
// ---------------------------------------------------------------------------

/** Coerce a manifest / fragment value to BigInt nanoseconds, or null. */
function toNs(value) {
  try {
    if (typeof value === 'bigint') return value;
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) return null;
      // JSON.parse yields a double; at ns magnitudes every double is an
      // integer, so this is lossless-enough for segment overlap tests
      // (worst case ~256ns of rounding on a segment boundary).
      return BigInt(Math.trunc(value));
    }
    if (typeof value === 'string' && /^-?\d+$/.test(value.trim())) {
      return BigInt(value.trim());
    }
  } catch {
    /* fall through */
  }
  return null;
}

/** Split ns into (floor'd epoch millis, remainder ns within that milli). */
function nsParts(ns) {
  let ms = ns / NS_PER_MS;
  let rem = ns % NS_PER_MS;
  if (rem < 0n) {
    ms -= 1n;
    rem += NS_PER_MS;
  }
  return { ms, rem };
}

/** `YYYY-MM-DD HH:MM:SS.ffffff` in UTC — the form DuckDB parses unambiguously. */
function nsToSqlTimestamp(ns) {
  const { ms, rem } = nsParts(ns);
  const msNum = Number(ms);
  if (!Number.isFinite(msNum)) return null;
  const d = new Date(msNum);
  if (Number.isNaN(d.getTime())) return null;
  const iso = d.toISOString();                        // 2026-08-28T11:55:00.123Z
  const base = iso.slice(0, 19).replace('T', ' ');    // 2026-08-28 11:55:00
  const millis = iso.slice(20, 23);                   // 123
  const micros = String(rem / 1000n).padStart(3, '0');
  return `${base}.${millis}${micros}`;
}

/** Human display form, always UTC so it matches what the collector wrote. */
function nsToHuman(ns) {
  const sql = nsToSqlTimestamp(ns);
  return sql === null ? '(invalid time)' : `${sql} UTC`;
}

/** Rough duration label for the alert window chip. */
function humanDuration(ns) {
  if (ns < 0n) ns = -ns;
  const secs = Number(ns / NS_PER_S);
  if (secs < 1) return '<1s';
  if (secs < 90) return `${secs}s`;
  const mins = Math.round(secs / 60);
  if (mins < 90) return `${mins}m`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.round(hours / 24)}d`;
}

// ---------------------------------------------------------------------------
// Fragment parsing  (README §4 / protocol::deep_link)
//
//   #t=<ticket>&from=<unix_ns>&to=<unix_ns>&labels=<urlencoded k=v,k=v>
// ---------------------------------------------------------------------------

/**
 * Mirror of `protocol::parse_labels`: split on ',', drop empties, split each
 * piece at its *first* '=' (so values may themselves contain '=').
 */
function parseLabels(raw) {
  const out = [];
  if (!raw) return out;
  for (const part of raw.split(',')) {
    if (part === '') continue;
    const i = part.indexOf('=');
    if (i < 0) continue;
    out.push([part.slice(0, i), part.slice(i + 1)]);
  }
  return out;
}

function parseFragment(hash) {
  const raw = String(hash || '').replace(/^#/, '');
  if (raw === '') {
    return { ok: false, error: 'This page needs an alert deep link. Open it from the "Open logs →" button in the Slack alert.' };
  }

  // URLSearchParams decodes percent-escapes for us; the collector encodes the
  // joined label string with %20/%3D, never bare '+', so this is safe.
  let params;
  try {
    params = new URLSearchParams(raw);
  } catch (err) {
    return { ok: false, error: `Malformed link fragment: ${errText(err)}` };
  }

  const ticket = (params.get('t') || '').trim();
  const fromRaw = (params.get('from') || '').trim();
  const toRaw = (params.get('to') || '').trim();
  const labelsRaw = params.get('labels') || '';

  const missing = [];
  if (!ticket) missing.push('t');
  if (!fromRaw) missing.push('from');
  if (!toRaw) missing.push('to');
  if (missing.length) {
    return { ok: false, error: `Link fragment is missing required field(s): ${missing.join(', ')}.` };
  }

  // The ticket is base32-nopad of a postcard struct; we never decode it here —
  // the wasm module owns that codec. Just sanity-check the alphabet so garbage
  // fails with a useful message instead of a wasm panic.
  if (!/^[a-z2-7]+$/.test(ticket)) {
    return { ok: false, error: 'Ticket is not valid lowercase base32 — the link looks truncated or mangled.' };
  }

  const fromNs = toNs(fromRaw);
  const toNsVal = toNs(toRaw);
  if (fromNs === null || toNsVal === null) {
    return { ok: false, error: '`from` and `to` must be integer unix nanosecond timestamps.' };
  }
  if (toNsVal < fromNs) {
    return { ok: false, error: '`to` is before `from` — the link\'s time window is inverted.' };
  }

  return { ok: true, ticket, fromNs, toNs: toNsVal, labels: parseLabels(labelsRaw) };
}

// ---------------------------------------------------------------------------
// Segment selection  (exact mirror of protocol::Manifest::select)
// ---------------------------------------------------------------------------

/**
 * `SegmentEntry::matches` — overlap with [from,to] inclusive, AND every label
 * filter satisfied. A filter on a key the segment does not index is a *match*:
 * `label_index` only covers promoted labels, so it can rule segments out,
 * never in.
 */
function segmentMatches(seg, fromNs, toNs_, labels) {
  const start = toNs(seg && seg.start_ns);
  const end = toNs(seg && seg.end_ns);
  if (start === null || end === null) return false;
  if (end < fromNs || start > toNs_) return false;

  const index = (seg && seg.label_index) || {};
  return labels.every(([k, v]) => {
    if (!Object.prototype.hasOwnProperty.call(index, k)) return true;
    const values = index[k];
    if (values === null || values === undefined) return true;
    return Array.isArray(values) && values.some((got) => got === v);
  });
}

/** `Manifest::select` — matching segments, oldest first. */
function selectSegments(manifest, fromNs, toNs_, labels) {
  const segments = Array.isArray(manifest && manifest.segments) ? manifest.segments : [];
  const hits = segments.filter((s) => segmentMatches(s, fromNs, toNs_, labels));
  hits.sort((a, b) => {
    const sa = toNs(a.start_ns) ?? 0n;
    const sb = toNs(b.start_ns) ?? 0n;
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  });
  return hits;
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/** Single-quoted SQL string literal; doubles embedded quotes. */
function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

/** One WHERE predicate per label filter, using promoted columns where possible. */
function labelPredicate(key, value) {
  if (PROMOTED_COLUMNS.has(key)) {
    // Promoted columns are plain identifiers from a fixed allow-list.
    return `${key} = ${sqlString(value)}`;
  }
  return `labels[${sqlString(key)}] = ${sqlString(value)}`;
}

function buildQuery(ctx) {
  const from = nsToSqlTimestamp(ctx.fromNs);
  const to = nsToSqlTimestamp(ctx.toNs);

  const where = [];
  if (from !== null && to !== null) {
    where.push(`ts BETWEEN TIMESTAMP ${sqlString(from)} AND TIMESTAMP ${sqlString(to)}`);
  }
  for (const [k, v] of ctx.labels) where.push(labelPredicate(k, v));

  const labelNote = ctx.labels.length
    ? ctx.labels.map(([k, v]) => `${k}=${v}`).join(', ')
    : 'no label filter';

  const lines = [
    `-- alert window (UTC): ${from ?? '?'} .. ${to ?? '?'}`,
    `-- labels: ${labelNote}`,
    'SELECT ts, level, job, line',
    'FROM logs',
  ];
  if (where.length) {
    lines.push(`WHERE ${where[0]}`);
    for (let i = 1; i < where.length; i++) lines.push(`  AND ${where[i]}`);
  }
  lines.push('ORDER BY ts');
  lines.push(`LIMIT ${DEFAULT_LIMIT}`);
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// UI rendering
// ---------------------------------------------------------------------------

function setConn(stateName, text) {
  el.conn.dataset.state = stateName;
  setText(el.connText, text);
  el.conn.title = text;
}

function setStatus(stateName, ...parts) {
  clear(el.status);
  el.status.dataset.state = stateName;
  parts.forEach((part, i) => {
    if (i > 0) el.status.appendChild(elem('span', 'sep', '·'));
    el.status.appendChild(document.createTextNode(part));
  });
}

function showBanner(title, paragraphs, kind) {
  clear(el.banner);
  el.banner.className = kind === 'info' ? 'banner banner--info' : 'banner';
  el.banner.appendChild(elem('h2', null, title));
  for (const para of paragraphs) {
    const p = elem('p');
    // Each paragraph is an array of strings and {code: '...'} fragments.
    for (const frag of Array.isArray(para) ? para : [para]) {
      if (typeof frag === 'string') p.appendChild(document.createTextNode(frag));
      else if (frag && frag.code) p.appendChild(elem('code', null, frag.code));
    }
    el.banner.appendChild(p);
  }
  el.banner.hidden = false;
}

function renderContext(ctx) {
  clear(el.chips);

  if (!ctx || !ctx.ok) {
    setText(el.window, '—');
    el.chips.appendChild(elem('span', 'chip chip--none', 'unavailable'));
    return;
  }

  clear(el.window);
  el.window.appendChild(document.createTextNode(nsToHuman(ctx.fromNs)));
  el.window.appendChild(elem('span', 'arrow', '→'));
  el.window.appendChild(document.createTextNode(nsToHuman(ctx.toNs)));
  el.window.appendChild(elem('span', 'dur', `  (${humanDuration(ctx.toNs - ctx.fromNs)})`));

  if (!ctx.labels.length) {
    el.chips.appendChild(elem('span', 'chip chip--none', 'none'));
    return;
  }
  for (const [k, v] of ctx.labels) {
    const chip = elem('span', 'chip');
    chip.appendChild(elem('span', 'k', k));
    chip.appendChild(elem('span', 'eq', '='));
    chip.appendChild(document.createTextNode(v));
    el.chips.appendChild(chip);
  }
}

function renderPlaceholder(text) {
  clear(el.results);
  el.results.appendChild(elem('div', 'placeholder', text));
}

// --- Arrow value formatting -------------------------------------------------

/** Is this Arrow schema field a Timestamp? (duck-typed: apache-arrow is bundled
 *  inside duckdb-browser.mjs and not re-exported, so we cannot import its enums.) */
function isTimestampField(field) {
  const t = field && field.type;
  if (!t) return false;
  if (typeof t.typeId === 'number' && t.typeId === 10) return true; // arrow Type.Timestamp
  try {
    return /^Timestamp/.test(String(t));
  } catch {
    return false;
  }
}

function isNumericField(field) {
  const t = field && field.type;
  if (!t) return false;
  try {
    return /^(Int|Uint|Float|Decimal)/.test(String(t));
  } catch {
    return false;
  }
}

/** Scale an epoch integer of unknown unit to nanoseconds by magnitude.
 *  arrow-js hands back ms, µs or ns depending on version and column unit. */
function scaleEpochToNs(x) {
  const a = x < 0n ? -x : x;
  if (a >= 100_000_000_000_000_000n) return x;              // >= 1e17 -> ns
  if (a >= 100_000_000_000_000n) return x * 1000n;          // >= 1e14 -> µs
  if (a >= 100_000_000_000n) return x * NS_PER_MS;          // >= 1e11 -> ms
  return x * NS_PER_S;                                      // seconds
}

function formatTimestamp(v) {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) {
    const t = v.getTime();
    return Number.isNaN(t) ? '(invalid time)' : nsToHuman(BigInt(t) * NS_PER_MS);
  }
  if (typeof v === 'bigint') return nsToHuman(scaleEpochToNs(v));
  if (typeof v === 'number' && Number.isFinite(v)) {
    return nsToHuman(scaleEpochToNs(BigInt(Math.trunc(v))));
  }
  return String(v);
}

function jsonReplacer(_key, value) {
  return typeof value === 'bigint' ? value.toString() : value;
}

function formatValue(v, field) {
  if (v === null || v === undefined) return null;
  if (isTimestampField(field)) return formatTimestamp(v);
  if (typeof v === 'bigint') return v.toString();
  if (v instanceof Date) return formatTimestamp(v);
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return String(v);
  if (v instanceof Uint8Array || ArrayBuffer.isView(v)) return `<${v.byteLength ?? v.length} bytes>`;
  // MAP / LIST / STRUCT vectors: arrow row proxies expose toJSON().
  try {
    const plain = typeof v.toJSON === 'function' ? v.toJSON() : v;
    return JSON.stringify(plain, jsonReplacer);
  } catch {
    return String(v);
  }
}

/** Render an Arrow Table into the results div. */
function renderTable(table) {
  clear(el.results);

  const fields = (table && table.schema && table.schema.fields) || [];
  const numRows = Number(table && table.numRows) || 0;

  if (!fields.length) {
    el.results.appendChild(elem('div', 'placeholder', 'Statement completed with no result set.'));
    return 0;
  }
  if (numRows === 0) {
    el.results.appendChild(elem('div', 'placeholder', 'Query returned 0 rows.'));
    return 0;
  }

  const shown = Math.min(numRows, MAX_RENDERED_ROWS);

  const tbl = elem('table');
  const thead = elem('thead');
  const headRow = elem('tr');
  for (const field of fields) {
    const th = elem('th');
    th.appendChild(document.createTextNode(field.name));
    let typeName = '';
    try {
      typeName = String(field.type);
    } catch {
      typeName = '';
    }
    if (typeName) th.appendChild(elem('span', 'type', typeName));
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);
  tbl.appendChild(thead);

  // Column-wise access via child vectors: stable across arrow-js versions and
  // avoids allocating a row proxy per row.
  const columns = fields.map((_, i) =>
    typeof table.getChildAt === 'function' ? table.getChildAt(i) : null,
  );
  const kinds = fields.map((f) =>
    isTimestampField(f) ? 'ts' : isNumericField(f) ? 'num' : '',
  );

  const tbody = elem('tbody');
  for (let r = 0; r < shown; r++) {
    const tr = elem('tr');
    for (let c = 0; c < fields.length; c++) {
      const col = columns[c];
      let raw = null;
      try {
        raw = col ? col.get(r) : null;
      } catch {
        raw = null;
      }
      const text = formatValue(raw, fields[c]);
      const td = elem('td', text === null ? 'null' : kinds[c]);
      td.textContent = text === null ? 'NULL' : text;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  tbl.appendChild(tbody);
  el.results.appendChild(tbl);

  if (shown < numRows) {
    el.results.appendChild(
      elem('div', 'truncated', `Showing the first ${shown} of ${numRows} rows — add a LIMIT to narrow the result.`),
    );
  }
  return numRows;
}

// ---------------------------------------------------------------------------
// Vendor loading
// ---------------------------------------------------------------------------

/** Probe a vendored asset without importing it, so a missing vendor dir
 *  produces a sentence instead of a module-resolution stack trace. */
async function assetPresent(url) {
  if (!/^https?:$/.test(location.protocol)) return true; // file:// — can't probe, let import decide
  try {
    const res = await fetch(url, { method: 'HEAD', cache: 'no-store' });
    return res.ok;
  } catch {
    return false;
  }
}

async function checkVendor() {
  const [duckdbOk, nodeOk] = await Promise.all([
    assetPresent(VENDOR.duckdbModule),
    assetPresent(VENDOR.webNodeModule),
  ]);
  const missing = [];
  if (!duckdbOk) missing.push('vendor/duckdb/');
  if (!nodeOk) missing.push('vendor/web_node/');
  return missing;
}

function reportMissingVendor(missing) {
  el.sql.value = '-- query engine unavailable: web/vendor/ is empty.';
  setConn('error', 'vendor assets missing');
  setStatus('error', `vendor assets missing — run web/fetch-vendor.sh (missing: ${missing.join(', ')})`);
  showBanner('vendor assets missing — run web/fetch-vendor.sh', [
    ['This page loads DuckDB-WASM and the iroh browser node from ', { code: 'web/vendor/' },
      `, which is not checked in. Missing: ${missing.join(', ')}.`],
    ['DuckDB: ', { code: './web/fetch-vendor.sh' }, ' (downloads and unpacks the npm tarball).'],
    ['iroh browser node: ',
      { code: 'wasm-pack build --target web --out-dir ../../web/vendor/web_node crates/web-node' }, '.'],
    ['See ', { code: 'web/vendor/README.md' }, ' for details.'],
  ]);
  renderPlaceholder('Cannot query without the vendored engine.');
}

async function loadDuckDb() {
  const duckdb = await import(/* @vite-ignore */ VENDOR.duckdbModule);
  const bundle = await duckdb.selectBundle(VENDOR.duckdbBundles);
  if (!bundle.mainWorker) throw new Error('no usable DuckDB-WASM bundle for this browser');

  const worker = new Worker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();
  // Pin the session zone so naive TIMESTAMP literals in the generated query
  // mean UTC regardless of the viewer's locale — segments are written in UTC.
  await conn.query("SET TimeZone='UTC'");
  return { db, conn, bundleName: bundle.mainModule.includes('eh') ? 'eh' : 'mvp' };
}

async function loadWebNode(ticket) {
  const mod = await import(/* @vite-ignore */ VENDOR.webNodeModule);
  const init = mod.default;
  if (typeof init !== 'function') {
    throw new Error('vendor/web_node/web_node.js has no default init() export — is this a `wasm-pack --target web` build?');
  }
  await init();
  if (typeof mod.PermafrostNode?.connect !== 'function') {
    throw new Error('vendor/web_node/web_node.js does not export PermafrostNode.connect()');
  }
  return await mod.PermafrostNode.connect(ticket);
}

/** wasm-bindgen maps Rust `u64`/`i64` params to JS BigInt. We call with plain
 *  Numbers (the crate should use f64/u32); retry once as BigInt so a signature
 *  mismatch across the JS↔wasm seam does not break the demo. */
function looksLikeBigIntArgError(err) {
  return /bigint|cannot convert .* to a ?bigint|expected a bigint/i.test(errText(err));
}

async function fetchBlob(node, hash, offset, len) {
  try {
    return await node.fetch_blob(hash, offset, len);
  } catch (err) {
    if (!looksLikeBigIntArgError(err)) throw err;
    return await node.fetch_blob(hash, BigInt(offset), BigInt(len));
  }
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

let queryToken = 0;

async function runQuery(sql, { prefix } = {}) {
  if (!state.conn) return;
  const token = ++queryToken;
  const trimmed = String(sql || '').trim();
  if (!trimmed) {
    setStatus('error', 'nothing to run — the editor is empty');
    return;
  }

  el.run.disabled = true;
  setStatus('busy', 'running query…');

  const t0 = performance.now();
  try {
    const table = await state.conn.query(trimmed);
    if (token !== queryToken) return; // superseded
    const rows = renderTable(table);
    const secs = ((performance.now() - t0) / 1000).toFixed(2);
    const parts = [];
    if (prefix) parts.push(prefix);
    parts.push(`${rows} row${rows === 1 ? '' : 's'} in ${secs} s`);
    setStatus('ok', ...parts);
  } catch (err) {
    if (token !== queryToken) return;
    setStatus('error', `DuckDB: ${errText(err)}`);
    renderPlaceholder('Query failed — see the message above.');
  } finally {
    if (token === queryToken) el.run.disabled = !state.ready;
  }
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

function fatal(title, paragraphs, statusLine) {
  setConn('error', 'error');
  setStatus('error', statusLine);
  showBanner(title, paragraphs);
  renderPlaceholder('Nothing to query.');
}

async function boot() {
  const t0 = performance.now();

  // (a) fragment ---------------------------------------------------------
  const ctx = parseFragment(location.hash);
  state.ctx = ctx;
  renderContext(ctx);
  if (!ctx.ok) {
    fatal('Cannot read the alert link', [
      ctx.error,
      ['Expected fragment: ', { code: '#t=<ticket>&from=<unix_ns>&to=<unix_ns>&labels=job%3Dapi' }],
    ], ctx.error);
    return;
  }

  el.sql.value = '-- connecting…';

  // (4) vendor preflight -------------------------------------------------
  const missing = await checkVendor();
  if (missing.length) {
    reportMissingVendor(missing);
    return;
  }

  // (b) engines ----------------------------------------------------------
  setStatus('busy', 'loading query engine and iroh browser node…');
  let duck;
  let node;
  try {
    // DuckDB instantiation and the relay handshake are independent — overlap them.
    const [duckResult, nodeResult] = await Promise.all([loadDuckDb(), loadWebNode(ctx.ticket)]);
    duck = duckResult;
    node = nodeResult;
  } catch (err) {
    fatal('Could not start the query engine', [
      errText(err),
      ['If ', { code: 'web/vendor/' }, ' was populated by hand, re-run ', { code: 'web/fetch-vendor.sh' }, '.'],
    ], errText(err));
    return;
  }
  state.db = duck.db;
  state.conn = duck.conn;

  let nodeId = '';
  try {
    nodeId = String(await node.node_id());
  } catch (err) {
    nodeId = `(node_id failed: ${errText(err)})`;
  }
  // Browser nodes are relay-only: no UDP in the sandbox, still end-to-end
  // encrypted (README §Recommended Stack). If the wasm wrapper ever exposes a
  // transport getter we surface it; otherwise relay-only is the known truth.
  let relayOnly = true;
  try {
    if (typeof node.connection_type === 'function') {
      relayOnly = String(await node.connection_type()).toLowerCase().includes('relay');
    }
  } catch {
    /* keep the default */
  }
  const short = nodeId.length > 24 ? `${nodeId.slice(0, 12)}…${nodeId.slice(-6)}` : nodeId;
  setConn(relayOnly ? 'relay' : 'ok', relayOnly ? `relay-only · ${short}` : `connected to node ${short}`);

  // (c) manifest ---------------------------------------------------------
  setStatus('busy', 'fetching manifest…');
  let manifest;
  try {
    const raw = await node.fetch_manifest();
    manifest = JSON.parse(typeof raw === 'string' ? raw : new TextDecoder().decode(raw));
  } catch (err) {
    fatal('Could not read the manifest', [
      errText(err),
      'The ticket carries the manifest hash hint; the collector may be offline or the relay unreachable.',
    ], `manifest fetch failed: ${errText(err)}`);
    return;
  }
  if (!manifest || typeof manifest !== 'object' || !Array.isArray(manifest.segments)) {
    fatal('Manifest is malformed', ['Expected a JSON object with a `segments` array.'], 'malformed manifest');
    return;
  }
  if (manifest.version !== MANIFEST_VERSION) {
    showBanner('Manifest version mismatch', [
      `This page understands manifest version ${MANIFEST_VERSION}; the collector sent version ${manifest.version}. Continuing anyway — results may be wrong.`,
    ], 'info');
  }

  // (d) selection --------------------------------------------------------
  const selected = selectSegments(manifest, ctx.fromNs, ctx.toNs, ctx.labels);
  state.segmentCount = selected.length;
  state.defaultSql = buildQuery(ctx);
  el.sql.value = state.defaultSql;
  el.sql.disabled = false;
  el.reset.disabled = false;

  if (selected.length === 0) {
    const total = manifest.segments.length;
    setConn(relayOnly ? 'relay' : 'ok', relayOnly ? `relay-only · ${short}` : `connected to node ${short}`);
    setStatus('error', `no segments match this alert window (manifest lists ${total} segment${total === 1 ? '' : 's'})`);
    showBanner('No segments match this alert', [
      `The manifest has ${total} segment${total === 1 ? '' : 's'}, but none overlaps ${nsToHuman(ctx.fromNs)} → ${nsToHuman(ctx.toNs)}` +
        (ctx.labels.length ? ` with ${ctx.labels.map(([k, v]) => `${k}=${v}`).join(', ')}.` : '.'),
      'Nothing was loaded, so there is nothing to query. The segment may have aged out of retention, or the alert window may predate the collector.',
    ], 'info');
    renderPlaceholder('No segments to query.');
    return;
  }

  // (e) fetch + register -------------------------------------------------
  const totalBytes = selected.reduce((n, s) => n + (Number(s.bytes) || 0), 0);
  setStatus('busy', `fetching ${selected.length} segment${selected.length === 1 ? '' : 's'} (${formatBytes(totalBytes)}) over iroh…`);

  const fileNames = [];
  try {
    const buffers = await Promise.all(
      selected.map(async (seg, i) => {
        const bytes = await fetchBlob(node, String(seg.hash), 0, 0); // 0,0 = whole blob
        if (!bytes || typeof bytes.byteLength !== 'number') {
          throw new Error(`segment ${String(seg.hash)} returned no bytes`);
        }
        return { name: `seg_${i}.parquet`, bytes };
      }),
    );
    for (const { name, bytes } of buffers) {
      await state.db.registerFileBuffer(name, bytes);
      fileNames.push(name);
    }
  } catch (err) {
    fatal('Segment fetch failed', [
      errText(err),
      'Segments are fetched peer-to-peer over iroh; the collector may have gone offline mid-transfer.',
    ], `segment fetch failed: ${errText(err)}`);
    return;
  }

  const fileList = fileNames.map((n) => sqlString(n)).join(', ');
  try {
    await state.conn.query(`CREATE OR REPLACE VIEW logs AS SELECT * FROM read_parquet([${fileList}])`);
  } catch (err) {
    fatal('Could not open the segments', [
      errText(err),
      'DuckDB rejected the Parquet files — the segment schema may not match this page.',
    ], `read_parquet failed: ${errText(err)}`);
    return;
  }

  // (f,g,h) run the alert query and report the whole-path timing -----------
  state.ready = true;
  el.run.disabled = false;
  const setupSecs = ((performance.now() - t0) / 1000).toFixed(2);
  await runQuery(state.defaultSql, {
    prefix: `fetched ${selected.length} segment${selected.length === 1 ? '' : 's'} (${formatBytes(totalBytes)}) in ${setupSecs} s`,
  });

  const totalSecs = ((performance.now() - t0) / 1000).toFixed(2);
  el.status.appendChild(elem('span', 'sep', '·'));
  el.status.appendChild(document.createTextNode(`total ${totalSecs} s`));
}

function formatBytes(n) {
  if (!Number.isFinite(n) || n <= 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

el.run.addEventListener('click', () => runQuery(el.sql.value));

el.reset.addEventListener('click', () => {
  el.sql.value = state.defaultSql;
  el.sql.focus();
});

el.sql.addEventListener('keydown', (ev) => {
  if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') {
    ev.preventDefault();
    if (!el.run.disabled) runQuery(el.sql.value);
  }
});

// A changed fragment means a different alert; simplest correct thing is a reload.
window.addEventListener('hashchange', () => location.reload());

boot().catch((err) => {
  fatal('Unexpected failure', [errText(err)], errText(err));
});

// Exported for the Playwright smoke test (README §Testing) and for poking at
// the pure helpers from the console; the page itself never reads these back.
export { parseFragment, parseLabels, selectSegments, segmentMatches, buildQuery, sqlString, nsToSqlTimestamp };
