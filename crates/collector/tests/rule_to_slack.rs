//! Phase 4 acceptance: a pushed line that matches a real compiled WASM rule
//! produces a Slack payload carrying a deep link the page can actually open,
//! and a hostile rule is fuel-killed without disturbing ingest.
//!
//! Skipped (not failed) when the example rules have not been built — the wasm
//! toolchain is not required for `cargo test`. Build them with:
//!   cd rules/examples/regex-match && cargo build --release --target wasm32-unknown-unknown

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use permafrost_collector::alert::{Alerter, MockSink};
use permafrost_collector::ingest::{AppState, Metrics, router};
use permafrost_collector::rules::RuleEngine;
use permafrost_collector::segment::SegmentWriter;
use permafrost_protocol::{BlobHash, NodeAddrWire, Ticket};

const UI_BASE: &str = "https://logs.example.com/";

fn example_wasm(name: &str, artifact: &str) -> Option<PathBuf> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../rules/examples")
        .join(name)
        .join("target/wasm32-unknown-unknown/release")
        .join(artifact);
    path.exists().then_some(path)
}

fn a_ticket() -> Ticket {
    Ticket {
        node_addr: NodeAddrWire {
            node_id: [4u8; 32],
            relay_url: Some("https://relay.example/".into()),
            direct_addrs: vec![],
        },
        manifest_hash_hint: BlobHash([9u8; 32]),
    }
}

struct Harness {
    base: String,
    sink: Arc<MockSink>,
    metrics: Arc<Metrics>,
    rules: Arc<RuleEngine>,
}

async fn start(rules_dir: PathBuf, data_dir: PathBuf) -> Harness {
    let metrics = Arc::new(Metrics::default());
    let sink = Arc::new(MockSink::default());
    let alerter = Arc::new(Alerter::new(
        sink.clone(),
        Some(UI_BASE.to_string()),
        metrics.clone(),
    ));
    alerter.set_ticket(a_ticket());

    let rules = RuleEngine::new(rules_dir, 10_000_000).expect("rule engine");
    let (segments, _rx) = SegmentWriter::new(data_dir, Duration::from_secs(3600), u64::MAX);
    let state = AppState {
        segments,
        rules: rules.clone(),
        alerter,
        metrics: metrics.clone(),
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });
    Harness {
        base,
        sink,
        metrics,
        rules,
    }
}

async fn push(h: &Harness, line: &str) -> u16 {
    let body = serde_json::json!({
        "streams": [{
            "stream": {"job": "api", "host": "h1", "level": "error"},
            "values": [["1700000000000000000", line]]
        }]
    });
    reqwest::Client::new()
        .post(format!("{}/loki/api/v1/push", h.base))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("push")
        .status()
        .as_u16()
}

#[tokio::test]
async fn a_matching_line_produces_a_slack_message_with_a_working_deep_link() {
    let Some(wasm) = example_wasm("regex-match", "regex_match.wasm") else {
        eprintln!("skipping: rules/examples/regex-match not built for wasm32");
        return;
    };
    let rules_dir = tempfile::tempdir().expect("tempdir");
    std::fs::copy(&wasm, rules_dir.path().join("regex_match.wasm")).expect("copy rule");
    let data = tempfile::tempdir().expect("tempdir");
    let h = start(rules_dir.path().to_path_buf(), data.path().to_path_buf()).await;

    assert_eq!(h.rules.rule_names(), vec!["regex_match".to_string()]);

    // A line the rule ignores must not alert.
    assert_eq!(push(&h, "request served in 4ms").await, 204);
    assert!(
        h.sink.sent.lock().expect("lock").is_empty(),
        "quiet line alerted"
    );

    assert_eq!(push(&h, "FATAL: worker died").await, 204);
    let sent = h.sink.sent.lock().expect("lock").clone();
    assert_eq!(sent.len(), 1, "expected exactly one Slack message");

    let text = serde_json::to_string(&sent[0]).expect("json");
    assert!(
        text.contains("FATAL: worker died"),
        "sample line missing: {text}"
    );
    assert!(
        text.contains("regex-match"),
        "rule name missing from payload: {text}"
    );

    // The deep link must be openable: correct base, ticket in the fragment
    // (never in the query, or it would reach the page's HTTP server), and the
    // alert's own labels pre-filled.
    let link = text
        .split('<')
        .find(|s| s.starts_with(UI_BASE))
        .and_then(|s| s.split('|').next())
        .unwrap_or_else(|| panic!("no deep link in payload: {text}"));
    let (before, fragment) = link.split_once('#').expect("link has a fragment");
    assert!(
        !before.contains("t="),
        "ticket leaked into the query string"
    );
    assert!(
        fragment.contains(&format!("t={}", a_ticket().encode())),
        "fragment lacks the ticket: {fragment}"
    );
    assert!(fragment.contains("from=") && fragment.contains("to="));
    // Labels ride as one urlencoded `k=v,k=v` list.
    assert!(
        fragment.contains("job%3Dapi") && fragment.contains("level%3Derror"),
        "alert labels are not pre-filled: {fragment}"
    );

    let metrics = h.metrics.render();
    assert!(
        metrics.contains("permafrost_rule_matches_total 1"),
        "{metrics}"
    );
    assert!(
        metrics.contains("permafrost_alerts_dropped_total 0"),
        "{metrics}"
    );
}

#[tokio::test]
async fn a_hostile_rule_is_fuel_killed_and_ingest_keeps_working() {
    let Some(wasm) = example_wasm("hostile-loop", "hostile_loop.wasm") else {
        eprintln!("skipping: rules/examples/hostile-loop not built for wasm32");
        return;
    };
    let rules_dir = tempfile::tempdir().expect("tempdir");
    std::fs::copy(&wasm, rules_dir.path().join("hostile_loop.wasm")).expect("copy rule");
    let data = tempfile::tempdir().expect("tempdir");
    let h = start(rules_dir.path().to_path_buf(), data.path().to_path_buf()).await;

    // The push itself must succeed: a rule burning its fuel budget is the
    // rule's problem, never the ingest path's.
    assert_eq!(push(&h, "anything at all").await, 204);

    let sent = h.sink.sent.lock().expect("lock").clone();
    assert_eq!(sent.len(), 1, "operator was not told the rule died");
    let text = serde_json::to_string(&sent[0]).expect("json");
    assert!(text.contains("hostile_loop"), "{text}");
    assert!(text.to_lowercase().contains("fuel"), "{text}");

    let metrics = h.metrics.render();
    assert!(
        metrics.contains("permafrost_rules_disabled_total 1"),
        "{metrics}"
    );

    // Second push: the rule is disabled, so it is not even attempted, and no
    // second alert is emitted.
    assert_eq!(push(&h, "still fine").await, 204);
    assert_eq!(h.sink.sent.lock().expect("lock").len(), 1, "alert storm");
    assert!(
        h.metrics.render().contains("permafrost_entries_total 2"),
        "ingest stopped after the rule died"
    );
}
