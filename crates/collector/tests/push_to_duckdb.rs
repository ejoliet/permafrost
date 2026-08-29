//! Phase 2 acceptance: a push over the real HTTP surface produces a Parquet
//! segment that DuckDB can read (README §Testing, §Acceptance Criteria).

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use permafrost_collector::alert::{Alerter, MockSink};
use permafrost_collector::ingest::{AppState, Metrics, router};
use permafrost_collector::rules::RuleEngine;
use permafrost_collector::segment::{SegmentMeta, SegmentWriter};

struct Harness {
    base: String,
    segments: Arc<SegmentWriter>,
    metrics: Arc<Metrics>,
    sink: Arc<MockSink>,
}

/// Start the collector's HTTP surface on an ephemeral port. No iroh endpoint:
/// this exercises ingest → segment only, so the test needs no network.
async fn start(rules_dir: &std::path::Path, data_dir: &std::path::Path) -> Harness {
    let metrics = Arc::new(Metrics::default());
    let sink = Arc::new(MockSink::default());
    let alerter = Arc::new(Alerter::new(sink.clone(), None, metrics.clone()));
    let rules = RuleEngine::new(rules_dir.to_path_buf(), 10_000_000).expect("rule engine");
    let (segments, _rx) =
        SegmentWriter::new(data_dir.to_path_buf(), Duration::from_secs(3600), u64::MAX);

    let state = AppState {
        segments: segments.clone(),
        rules,
        alerter,
        metrics: metrics.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });

    Harness {
        base: format!("http://{addr}"),
        segments,
        metrics,
        sink,
    }
}

async fn post(url: &str, content_type: &str, body: Vec<u8>) -> u16 {
    reqwest::Client::new()
        .post(url)
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .expect("request")
        .status()
        .as_u16()
}

/// Query a segment with the real DuckDB binary if one is installed.
fn duckdb_query(meta: &SegmentMeta, sql: &str) -> Option<String> {
    let path = meta.path.to_str()?;
    let out = Command::new("duckdb")
        .arg("-noheader")
        .arg("-list")
        .arg("-c")
        .arg(sql.replace("{}", path))
        .output()
        .ok()?;
    assert!(
        out.status.success(),
        "duckdb failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[tokio::test]
async fn json_push_becomes_a_segment_duckdb_can_query() {
    let data = tempfile::tempdir().expect("tempdir");
    let rules = tempfile::tempdir().expect("tempdir");
    let h = start(rules.path(), data.path()).await;

    let body = serde_json::json!({
        "streams": [
            {
                "stream": {"job": "api", "host": "h1", "level": "error", "pod": "api-7"},
                "values": [
                    ["1700000000000000000", "panic: nil pointer dereference"],
                    ["1700000000000000001", "recovered"]
                ]
            },
            {
                "stream": {"job": "worker", "host": "h2", "level": "info"},
                "values": [["1700000000000000002", "job done"]]
            }
        ]
    });
    let status = post(
        &format!("{}/loki/api/v1/push", h.base),
        "application/json",
        body.to_string().into_bytes(),
    )
    .await;
    assert_eq!(status, 204, "Loki push must answer 204");

    let meta = h.segments.flush().await.expect("flush").expect("a segment");
    assert_eq!(meta.rows, 3);

    let Some(duck) = duckdb_query(&meta, "SELECT count(*) FROM read_parquet('{}')") else {
        // AIDEV-NOTE: the assertion above already proves the segment exists and is
        // well formed; only the DuckDB-compatibility half is skipped.
        eprintln!("skipping DuckDB assertions: `duckdb` binary not on PATH");
        return;
    };
    assert_eq!(duck, "3", "DuckDB must read every row back");

    // Promoted columns are queryable directly.
    let errors = duckdb_query(
        &meta,
        "SELECT line FROM read_parquet('{}') WHERE level = 'error' ORDER BY ts",
    )
    .expect("duckdb");
    assert_eq!(errors.lines().count(), 2, "{errors}");
    assert!(errors.contains("panic: nil pointer dereference"));

    // Non-promoted labels survive in the map column.
    let pod = duckdb_query(
        &meta,
        "SELECT DISTINCT labels['pod'] FROM read_parquet('{}') WHERE labels['pod'] IS NOT NULL",
    )
    .expect("duckdb");
    assert_eq!(
        pod, "api-7",
        "the labels MAP must round-trip through DuckDB"
    );

    // The ts column keeps nanosecond UTC semantics through Parquet.
    let ts_type =
        duckdb_query(&meta, "SELECT typeof(ts) FROM read_parquet('{}') LIMIT 1").expect("duckdb");
    assert!(
        ts_type.starts_with("TIMESTAMP"),
        "unexpected ts type: {ts_type}"
    );
}

#[tokio::test]
async fn promtail_protobuf_push_becomes_a_segment() {
    use permafrost_collector::ingest::{EntryAdapter, PushRequest, StreamAdapter, Timestamp};
    use prost::Message as _;

    let data = tempfile::tempdir().expect("tempdir");
    let rules = tempfile::tempdir().expect("tempdir");
    let h = start(rules.path(), data.path()).await;

    // The exact shape promtail sends: snappy-compressed logproto.PushRequest.
    let push = PushRequest {
        streams: vec![StreamAdapter {
            labels: "{job=\"api\", host=\"h1\", level=\"warn\"}".into(),
            entries: vec![EntryAdapter {
                timestamp: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 5,
                }),
                line: "disk 91% full".into(),
            }],
            hash: 0,
        }],
    };
    let body = snap::raw::Encoder::new()
        .compress_vec(&push.encode_to_vec())
        .expect("snappy");

    let status = post(
        &format!("{}/loki/api/v1/push", h.base),
        "application/x-protobuf",
        body,
    )
    .await;
    assert_eq!(status, 204);

    let meta = h.segments.flush().await.expect("flush").expect("a segment");
    assert_eq!(meta.rows, 1);
    assert_eq!(meta.start_ns, 1_700_000_000_000_000_005);

    if let Some(line) = duckdb_query(&meta, "SELECT job || '/' || line FROM read_parquet('{}')") {
        assert_eq!(line, "api/disk 91% full");
    }
}

#[tokio::test]
async fn malformed_push_is_rejected_without_killing_ingest() {
    let data = tempfile::tempdir().expect("tempdir");
    let rules = tempfile::tempdir().expect("tempdir");
    let h = start(rules.path(), data.path()).await;
    let url = format!("{}/loki/api/v1/push", h.base);

    assert_eq!(
        post(&url, "application/json", b"{not json".to_vec()).await,
        400
    );
    assert_eq!(post(&url, "text/csv", b"a,b".to_vec()).await, 400);

    // Ingest still works afterwards.
    let good = serde_json::json!({"streams":[{"stream":{"job":"api"},"values":[["1","ok"]]}]});
    assert_eq!(
        post(&url, "application/json", good.to_string().into_bytes()).await,
        204
    );

    let metrics = h.metrics.render();
    assert!(
        metrics.contains("permafrost_push_errors_total 2"),
        "{metrics}"
    );
    assert!(metrics.contains("permafrost_pushes_total 1"), "{metrics}");
    assert!(
        h.sink.sent.lock().expect("lock").is_empty(),
        "no rule, no alert"
    );
}

#[tokio::test]
async fn agent_endpoints_answer_what_promtail_and_prometheus_expect() {
    let data = tempfile::tempdir().expect("tempdir");
    let rules = tempfile::tempdir().expect("tempdir");
    let h = start(rules.path(), data.path()).await;

    let ready = reqwest::get(format!("{}/ready", h.base))
        .await
        .expect("ready");
    assert!(ready.status().is_success());

    let metrics = reqwest::get(format!("{}/metrics", h.base))
        .await
        .expect("metrics");
    assert_eq!(
        metrics
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; version=0.0.4")
    );
    let body = metrics.text().await.expect("body");
    assert!(
        body.contains("# TYPE permafrost_entries_total counter"),
        "{body}"
    );
}
