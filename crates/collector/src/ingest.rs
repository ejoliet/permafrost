//! Loki push API: axum routes plus protobuf/JSON decoding (README §Interface Contracts 1).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use permafrost_protocol::MAX_LINE_BYTES;
use prost::Message as _;
use serde::Deserialize;

use crate::alert::Alerter;
use crate::config::BACKPRESSURE_MAX_BYTES;
use crate::rules::RuleEngine;
use crate::segment::SegmentWriter;

/// Appended to a line we had to cut, so a reader can tell truncation from a
/// genuinely short line (README §Non-Goals: no log line size > 1 MB).
pub const TRUNCATION_MARKER: &str = "…[permafrost: truncated]";

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("snappy decompression failed: {0}")]
    Snappy(#[from] snap::Error),
    #[error("protobuf decode failed: {0}")]
    Protobuf(#[from] prost::DecodeError),
    #[error("json decode failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported Content-Type {0:?}; expected application/x-protobuf or application/json")]
    ContentType(String),
    #[error("stream label set {0:?} is not a valid Loki label selector")]
    Labels(String),
    #[error("timestamp {0:?} is not an integer nanosecond value")]
    Timestamp(String),
}

// ---------------------------------------------------------------------------
// Internal batch representation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub ts_ns: i64,
    pub line: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    pub labels: BTreeMap<String, String>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    pub streams: Vec<Stream>,
}

impl Batch {
    pub fn rows(&self) -> usize {
        self.streams.iter().map(|s| s.entries.len()).sum()
    }

    /// Rough uncompressed footprint, used for flush and backpressure decisions.
    pub fn bytes(&self) -> u64 {
        self.streams
            .iter()
            .map(|s| {
                let labels: usize = s
                    .labels
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 2)
                    .sum::<usize>();
                let lines: usize = s.entries.iter().map(|e| e.line.len() + 8).sum();
                (labels + lines) as u64
            })
            .sum()
    }

    /// Truncate over-long lines in place, returning how many were cut.
    fn truncate_long_lines(&mut self) -> u64 {
        let mut cut = 0;
        for stream in &mut self.streams {
            for entry in &mut stream.entries {
                if entry.line.len() > MAX_LINE_BYTES {
                    // AIDEV-NOTE: slice on a char boundary or `String::truncate` panics
                    // on multi-byte UTF-8 straddling the limit.
                    let mut end = MAX_LINE_BYTES;
                    while end > 0 && !entry.line.is_char_boundary(end) {
                        end -= 1;
                    }
                    entry.line.truncate(end);
                    entry.line.push_str(TRUNCATION_MARKER);
                    cut += 1;
                }
            }
        }
        cut
    }

    /// The JSON shape handed to WASM rules (README §Interface Contracts 6).
    pub fn to_rule_json(&self) -> serde_json::Value {
        serde_json::json!({
            "streams": self.streams.iter().map(|s| serde_json::json!({
                "labels": s.labels,
                "entries": s.entries.iter().map(|e| serde_json::json!({
                    "ts": e.ts_ns,
                    "line": e.line,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>()
        })
    }
}

// ---------------------------------------------------------------------------
// Protobuf wire types (hand-written from Loki's logproto.proto)
// ---------------------------------------------------------------------------

// AIDEV-NOTE: transcribed rather than generated from a .proto so building permafrost
// needs no `protoc` and no vendored Loki sources. The message is four fields wide
// and has been stable for years; the decode tests below pin the wire format.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct StreamAdapter {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<EntryAdapter>,
    #[prost(uint64, tag = "3")]
    pub hash: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<Timestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

impl Timestamp {
    fn to_nanos(self) -> i64 {
        self.seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(self.nanos as i64)
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Parse Loki's label selector syntax: `{job="api", level="error"}`.
pub fn parse_label_selector(raw: &str) -> Result<BTreeMap<String, String>, DecodeError> {
    let bad = || DecodeError::Labels(raw.to_string());
    let inner = raw
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(bad)?;
    let mut out = BTreeMap::new();
    let mut chars = inner.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace() || *c == ',') {
            chars.next();
        }
        if chars.peek().is_none() {
            return Ok(out);
        }
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' || c.is_whitespace() {
                break;
            }
            key.push(c);
            chars.next();
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        if chars.next() != Some('=') || key.is_empty() {
            return Err(bad());
        }
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        if chars.next() != Some('"') {
            return Err(bad());
        }
        let mut value = String::new();
        loop {
            match chars.next() {
                Some('"') => break,
                Some('\\') => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some('t') => value.push('\t'),
                    Some('r') => value.push('\r'),
                    Some(c) => value.push(c),
                    None => return Err(bad()),
                },
                Some(c) => value.push(c),
                None => return Err(bad()),
            }
        }
        out.insert(key, value);
    }
}

#[derive(Debug, Deserialize)]
struct JsonPush {
    streams: Vec<JsonStream>,
}

#[derive(Debug, Deserialize)]
struct JsonStream {
    stream: BTreeMap<String, String>,
    /// `[["<ts ns as string>", "<line>"], ...]`; Loki may append a third
    /// structured-metadata element, which serde_json keeps in the tail.
    values: Vec<Vec<serde_json::Value>>,
}

pub fn decode_json(body: &[u8]) -> Result<Batch, DecodeError> {
    let push: JsonPush = serde_json::from_slice(body)?;
    let mut streams = Vec::with_capacity(push.streams.len());
    for s in push.streams {
        let mut entries = Vec::with_capacity(s.values.len());
        for pair in s.values {
            let ts_raw = pair.first().map(ToString::to_string).unwrap_or_default();
            let ts_ns = match pair.first() {
                Some(serde_json::Value::String(s)) => s
                    .parse::<i64>()
                    .map_err(|_| DecodeError::Timestamp(s.clone()))?,
                Some(serde_json::Value::Number(n)) => {
                    n.as_i64().ok_or(DecodeError::Timestamp(ts_raw.clone()))?
                }
                _ => return Err(DecodeError::Timestamp(ts_raw)),
            };
            let line = match pair.get(1) {
                Some(serde_json::Value::String(s)) => s.clone(),
                other => other.map(ToString::to_string).unwrap_or_default(),
            };
            entries.push(Entry { ts_ns, line });
        }
        streams.push(Stream {
            labels: s.stream,
            entries,
        });
    }
    Ok(Batch { streams })
}

pub fn decode_protobuf(body: &[u8]) -> Result<Batch, DecodeError> {
    let raw = snap::raw::Decoder::new().decompress_vec(body)?;
    let push = PushRequest::decode(raw.as_slice())?;
    let mut streams = Vec::with_capacity(push.streams.len());
    for s in push.streams {
        let labels = parse_label_selector(&s.labels)?;
        let entries = s
            .entries
            .into_iter()
            .map(|e| Entry {
                ts_ns: e.timestamp.unwrap_or_default().to_nanos(),
                line: e.line,
            })
            .collect();
        streams.push(Stream { labels, entries });
    }
    Ok(Batch { streams })
}

pub fn decode(content_type: Option<&str>, body: &[u8]) -> Result<Batch, DecodeError> {
    let ct = content_type.unwrap_or("application/x-protobuf");
    let base = ct.split(';').next().unwrap_or("").trim();
    match base {
        // promtail's default; some agents send the vendored spelling.
        "application/x-protobuf" | "application/protobuf" | "" => decode_protobuf(body),
        "application/json" => decode_json(body),
        other => Err(DecodeError::ContentType(other.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Hand-rolled Prometheus text exposition. Nine counters do not justify a
/// metrics framework, and operators only need `GET /metrics` to parse.
#[derive(Debug, Default)]
pub struct Metrics {
    pub pushes_total: AtomicU64,
    pub push_errors_total: AtomicU64,
    pub push_rejected_total: AtomicU64,
    pub entries_total: AtomicU64,
    pub lines_truncated_total: AtomicU64,
    pub segments_flushed_total: AtomicU64,
    pub rule_matches_total: AtomicU64,
    pub rules_disabled_total: AtomicU64,
    pub alerts_dropped_total: AtomicU64,
    pub buffered_bytes: AtomicU64,
}

impl Metrics {
    pub fn render(&self) -> String {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let mut out = String::new();
        for (name, help, kind, value) in [
            (
                "permafrost_pushes_total",
                "Accepted push requests",
                "counter",
                g(&self.pushes_total),
            ),
            (
                "permafrost_push_errors_total",
                "Rejected push requests (decode failures)",
                "counter",
                g(&self.push_errors_total),
            ),
            (
                "permafrost_push_rejected_total",
                "Push requests rejected by backpressure",
                "counter",
                g(&self.push_rejected_total),
            ),
            (
                "permafrost_entries_total",
                "Log entries ingested",
                "counter",
                g(&self.entries_total),
            ),
            (
                "permafrost_lines_truncated_total",
                "Log lines truncated at the 1MiB cap",
                "counter",
                g(&self.lines_truncated_total),
            ),
            (
                "permafrost_segments_flushed_total",
                "Parquet segments written",
                "counter",
                g(&self.segments_flushed_total),
            ),
            (
                "permafrost_rule_matches_total",
                "Rule evaluations that produced a match",
                "counter",
                g(&self.rule_matches_total),
            ),
            (
                "permafrost_rules_disabled_total",
                "Rules disabled after a trap or fuel exhaustion",
                "counter",
                g(&self.rules_disabled_total),
            ),
            (
                "permafrost_alerts_dropped_total",
                "Slack alerts dropped after retries",
                "counter",
                g(&self.alerts_dropped_total),
            ),
            (
                "permafrost_buffered_bytes",
                "Bytes buffered in unflushed segments",
                "gauge",
                g(&self.buffered_bytes),
            ),
        ] {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
            ));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub segments: Arc<SegmentWriter>,
    pub rules: Arc<RuleEngine>,
    pub alerter: Arc<Alerter>,
    pub metrics: Arc<Metrics>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/loki/api/v1/push", post(push))
        .route("/ready", get(|| async { "ready\n" }))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn metrics(State(state): State<AppState>) -> Response {
    state
        .metrics
        .buffered_bytes
        .store(state.segments.buffered_bytes(), Ordering::Relaxed);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

async fn push(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let m = &state.metrics;

    if state.segments.buffered_bytes() > BACKPRESSURE_MAX_BYTES {
        m.push_rejected_total.fetch_add(1, Ordering::Relaxed);
        // promtail retries a 429 natively, so shedding here is lossless for it.
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", "1")],
            "buffer full\n",
        )
            .into_response();
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let mut batch = match decode(content_type, &body) {
        Ok(batch) => batch,
        Err(err) => {
            m.push_errors_total.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%err, "rejecting push");
            // A bad payload must never take ingest down.
            return (StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
    };

    let truncated = batch.truncate_long_lines();
    m.lines_truncated_total
        .fetch_add(truncated, Ordering::Relaxed);
    m.pushes_total.fetch_add(1, Ordering::Relaxed);
    m.entries_total
        .fetch_add(batch.rows() as u64, Ordering::Relaxed);

    for outcome in state.rules.evaluate(&batch) {
        m.rule_matches_total.fetch_add(1, Ordering::Relaxed);
        state.alerter.dispatch(outcome).await;
    }

    if let Err(err) = state.segments.append(&batch).await {
        tracing::error!(%err, "segment write failed");
        state
            .alerter
            .self_alert(&format!("segment write failed: {err}"))
            .await;
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response();
    }

    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_selector_parses_loki_syntax() {
        let got = parse_label_selector("{job=\"api\", level=\"error\"}").expect("valid selector");
        assert_eq!(got.get("job").map(String::as_str), Some("api"));
        assert_eq!(got.get("level").map(String::as_str), Some("error"));

        assert!(
            parse_label_selector("{}")
                .expect("empty is valid")
                .is_empty()
        );
        assert_eq!(
            parse_label_selector(r#"{msg="say \"hi\"", path="a\\b"}"#)
                .expect("escapes")
                .get("msg")
                .map(String::as_str),
            Some("say \"hi\"")
        );
        assert!(
            parse_label_selector("job=\"api\"").is_err(),
            "braces are required"
        );
        assert!(
            parse_label_selector("{job=api}").is_err(),
            "values must be quoted"
        );
    }

    #[test]
    fn protobuf_roundtrip_matches_promtail_wire_format() {
        let push = PushRequest {
            streams: vec![StreamAdapter {
                labels: "{job=\"api\", host=\"h1\"}".into(),
                entries: vec![EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 123,
                    }),
                    line: "boom".into(),
                }],
                hash: 0,
            }],
        };
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&push.encode_to_vec())
            .expect("snappy compress");

        let batch = decode(Some("application/x-protobuf"), &compressed).expect("decodes");
        assert_eq!(batch.streams.len(), 1);
        assert_eq!(batch.streams[0].entries[0].ts_ns, 1_700_000_000_000_000_123);
        assert_eq!(batch.streams[0].entries[0].line, "boom");
        assert_eq!(
            batch.streams[0].labels.get("host").map(String::as_str),
            Some("h1")
        );
    }

    #[test]
    fn json_push_format_decodes_string_and_numeric_timestamps() {
        let body = br#"{"streams":[
            {"stream":{"job":"api"},"values":[["1700000000000000123","hello"],[1700000000000000124,"world"]]}
        ]}"#;
        let batch = decode(Some("application/json"), body).expect("decodes");
        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.streams[0].entries[0].ts_ns, 1_700_000_000_000_000_123);
        assert_eq!(batch.streams[0].entries[1].line, "world");
    }

    #[test]
    fn json_push_tolerates_structured_metadata_third_element() {
        let body =
            br#"{"streams":[{"stream":{"job":"api"},"values":[["1","x",{"trace":"abc"}]]}]}"#;
        let batch = decode(Some("application/json"), body).expect("decodes");
        assert_eq!(batch.streams[0].entries[0].line, "x");
    }

    #[test]
    fn decode_errors_are_typed_never_panics() {
        assert!(matches!(
            decode(Some("text/yaml"), b"{}"),
            Err(DecodeError::ContentType(_))
        ));
        assert!(decode(Some("application/json"), b"not json").is_err());
        assert!(decode(Some("application/x-protobuf"), b"\xff\xff not snappy").is_err());
    }

    #[test]
    fn long_lines_are_truncated_and_flagged_on_char_boundaries() {
        let mut batch = Batch {
            streams: vec![Stream {
                labels: BTreeMap::new(),
                // AIDEV-NOTE: 'é' is 2 bytes, so the cap lands mid-character — the
                // regression this test exists for.
                entries: vec![Entry {
                    ts_ns: 1,
                    line: "é".repeat(MAX_LINE_BYTES),
                }],
            }],
        };
        assert_eq!(batch.truncate_long_lines(), 1);
        let line = &batch.streams[0].entries[0].line;
        assert!(line.ends_with(TRUNCATION_MARKER));
        assert!(line.len() <= MAX_LINE_BYTES + TRUNCATION_MARKER.len());
    }

    #[test]
    fn rule_json_matches_the_documented_abi_shape() {
        let batch = Batch {
            streams: vec![Stream {
                labels: BTreeMap::from([("job".to_string(), "api".to_string())]),
                entries: vec![Entry {
                    ts_ns: 7,
                    line: "panic".into(),
                }],
            }],
        };
        let json = batch.to_rule_json().to_string();
        assert_eq!(
            json,
            r#"{"streams":[{"entries":[{"line":"panic","ts":7}],"labels":{"job":"api"}}]}"#
        );
    }

    #[test]
    fn metrics_render_as_prometheus_text() {
        let m = Metrics::default();
        m.pushes_total.store(3, Ordering::Relaxed);
        let out = m.render();
        assert!(out.contains("# TYPE permafrost_pushes_total counter"));
        assert!(out.contains("\npermafrost_pushes_total 3\n"));
        assert!(out.contains("# TYPE permafrost_buffered_bytes gauge"));
    }
}
