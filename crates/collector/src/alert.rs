//! Slack payload construction and delivery (README §Interface Contracts 7).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use permafrost_protocol::{Ticket, deep_link};

use crate::config::redact;
use crate::ingest::Metrics;
use crate::rules::{RuleEvent, RuleOutcome};

/// Padding around the matched window so the operator lands with context.
const WINDOW_PAD_NS: i64 = 5 * 60 * 1_000_000_000;

/// Sample lines shown inline before the reader is sent to the SQL page.
const INLINE_SAMPLES: usize = 3;

const MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, thiserror::Error)]
pub enum SlackDeliveryError {
    #[error("slack request failed: {0}")]
    Transport(String),
    #[error("slack returned {status}")]
    Status { status: u16 },
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Delivery target. Tests substitute a mock so no test ever touches Slack.
pub trait WebhookSink: Send + Sync {
    fn send(&self, payload: serde_json::Value) -> BoxFuture<'_, Result<(), SlackDeliveryError>>;
}

pub struct HttpSink {
    client: reqwest::Client,
    url: String,
}

impl HttpSink {
    pub fn new(url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
        }
    }
}

impl WebhookSink for HttpSink {
    fn send(&self, payload: serde_json::Value) -> BoxFuture<'_, Result<(), SlackDeliveryError>> {
        Box::pin(async move {
            let resp = self
                .client
                .post(&self.url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| SlackDeliveryError::Transport(e.to_string()))?;
            if resp.status().is_success() {
                Ok(())
            } else {
                Err(SlackDeliveryError::Status {
                    status: resp.status().as_u16(),
                })
            }
        })
    }
}

/// Drops everything. Used when no webhook is configured, so the rest of the
/// system needs no `Option<Alerter>` branching.
pub struct NullSink;

impl WebhookSink for NullSink {
    fn send(&self, _payload: serde_json::Value) -> BoxFuture<'_, Result<(), SlackDeliveryError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Collects payloads instead of sending them.
#[derive(Default)]
pub struct MockSink {
    pub sent: Mutex<Vec<serde_json::Value>>,
    /// Number of leading attempts to fail, for retry tests.
    pub fail_first: Mutex<u32>,
}

impl WebhookSink for MockSink {
    fn send(&self, payload: serde_json::Value) -> BoxFuture<'_, Result<(), SlackDeliveryError>> {
        Box::pin(async move {
            if let Ok(mut left) = self.fail_first.lock()
                && *left > 0
            {
                *left -= 1;
                return Err(SlackDeliveryError::Status { status: 500 });
            }
            if let Ok(mut sent) = self.sent.lock() {
                sent.push(payload);
            }
            Ok(())
        })
    }
}

pub struct Alerter {
    sink: Arc<dyn WebhookSink>,
    ui_base_url: Option<String>,
    /// Set once the iroh endpoint is up and refreshed on every manifest change.
    ticket: RwLock<Option<Ticket>>,
    metrics: Arc<Metrics>,
    retry_base: Duration,
}

impl Alerter {
    pub fn new(
        sink: Arc<dyn WebhookSink>,
        ui_base_url: Option<String>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            sink,
            ui_base_url,
            ticket: RwLock::new(None),
            metrics,
            retry_base: Duration::from_millis(200),
        }
    }

    pub fn set_ticket(&self, ticket: Ticket) {
        if let Ok(mut slot) = self.ticket.write() {
            *slot = Some(ticket);
        }
    }

    pub fn current_ticket(&self) -> Option<Ticket> {
        self.ticket.read().ok().and_then(|t| t.clone())
    }

    /// Build the deep link for a match, if we have both a ticket and a UI base.
    pub fn link_for(&self, outcome: &RuleOutcome) -> Option<String> {
        let base = self.ui_base_url.as_ref()?;
        let ticket = self.current_ticket()?;
        let (from, to) = window(outcome);
        Some(deep_link(base, &ticket, from, to, &outcome.labels))
    }

    pub async fn dispatch(&self, event: RuleEvent) {
        let payload = match event {
            RuleEvent::Match(outcome) => {
                let link = self.link_for(&outcome);
                match_payload(&outcome, link.as_deref())
            }
            RuleEvent::Disabled { rule, reason } => {
                // The engine emits this once per rule until the file changes,
                // so the counter tracks distinct rules lost, not batches.
                self.metrics
                    .rules_disabled_total
                    .fetch_add(1, Ordering::Relaxed);
                disabled_payload(&rule, &reason)
            }
        };
        self.deliver(payload).await;
    }

    /// Operational problem in the collector itself, e.g. a failed segment write.
    pub async fn self_alert(&self, message: &str) {
        self.deliver(serde_json::json!({
            "text": format!("🧊 permafrost: {message}"),
        }))
        .await;
    }

    /// Alerts are best-effort: retry with backoff, then drop with a warning
    /// rather than block ingest (README §Error Handling).
    async fn deliver(&self, payload: serde_json::Value) {
        let mut delay = self.retry_base;
        for attempt in 1..=MAX_ATTEMPTS {
            match self.sink.send(payload.clone()).await {
                Ok(()) => return,
                Err(err) if attempt == MAX_ATTEMPTS => {
                    self.metrics
                        .alerts_dropped_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(%err, attempts = attempt, "dropping slack alert");
                }
                Err(err) => {
                    tracing::debug!(%err, attempt, "retrying slack alert");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
    }
}

/// Time window for the deep link: the matched span padded by five minutes.
fn window(outcome: &RuleOutcome) -> (i64, i64) {
    let first = outcome.matches.iter().map(|m| m.ts).min().unwrap_or(0);
    let last = outcome.matches.iter().map(|m| m.ts).max().unwrap_or(first);
    (
        first.saturating_sub(WINDOW_PAD_NS),
        last.saturating_add(WINDOW_PAD_NS),
    )
}

fn match_payload(outcome: &RuleOutcome, link: Option<&str>) -> serde_json::Value {
    let heading = format!("{} *{}*", outcome.severity.emoji(), outcome.rule);
    let mut blocks = vec![serde_json::json!({
        "type": "section",
        "text": { "type": "mrkdwn", "text":
            if outcome.message.is_empty() { heading.clone() } else { format!("{heading}\n{}", outcome.message) }
        },
    })];

    if !outcome.matches.is_empty() {
        let samples = outcome
            .matches
            .iter()
            .take(INLINE_SAMPLES)
            .map(|m| m.line.replace("```", "'''"))
            .collect::<Vec<_>>()
            .join("\n");
        blocks.push(serde_json::json!({
            "type": "section",
            "text": { "type": "mrkdwn", "text": format!("```\n{samples}\n```") },
        }));
    }

    if !outcome.labels.is_empty() {
        let chips = outcome
            .labels
            .iter()
            .map(|(k, v)| format!("`{k}={v}`"))
            .collect::<Vec<_>>()
            .join("  ");
        blocks.push(serde_json::json!({
            "type": "context",
            "elements": [{ "type": "mrkdwn", "text": chips }],
        }));
    }

    match link {
        Some(url) => blocks.push(serde_json::json!({
            "type": "section",
            "text": { "type": "mrkdwn", "text": format!("<{url}|Open logs →>") },
        })),
        None => blocks.push(serde_json::json!({
            "type": "context",
            "elements": [{ "type": "mrkdwn", "text":
                "_no query link: set PERMAFROST_UI_BASE_URL and wait for the first segment flush_" }],
        })),
    }

    serde_json::json!({
        "text": format!("{} {} matched {} line(s)", outcome.severity.emoji(), outcome.rule, outcome.matches.len()),
        "blocks": blocks,
    })
}

fn disabled_payload(rule: &str, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "text": format!("🧯 rule `{rule}` disabled: {reason}"),
        "blocks": [{
            "type": "section",
            "text": { "type": "mrkdwn", "text":
                format!("🧯 rule `{rule}` disabled — ingest is unaffected.\n```\n{reason}\n```") },
        }],
    })
}

/// Sink chosen from config. Logs the target with the secret path stripped.
pub fn sink_from_config(webhook: Option<&str>) -> Arc<dyn WebhookSink> {
    match webhook {
        Some(url) => {
            tracing::info!(target_url = %redact(url), "slack alerts enabled");
            Arc::new(HttpSink::new(url.to_string()))
        }
        None => {
            tracing::warn!("no PERMAFROST_SLACK_WEBHOOK set; alerts will be discarded");
            Arc::new(NullSink)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::{MatchSample, Severity};
    use permafrost_protocol::{BlobHash, NodeAddrWire};

    fn outcome() -> RuleOutcome {
        RuleOutcome {
            rule: "panic-detector".into(),
            severity: Severity::Crit,
            message: "3 panics in api".into(),
            matches: vec![
                MatchSample {
                    ts: 1_000_000_000_000,
                    line: "panic: nil deref".into(),
                },
                MatchSample {
                    ts: 2_000_000_000_000,
                    line: "panic: again".into(),
                },
                MatchSample {
                    ts: 3_000_000_000_000,
                    line: "panic: and again".into(),
                },
                MatchSample {
                    ts: 4_000_000_000_000,
                    line: "panic: fourth".into(),
                },
            ],
            labels: vec![("job".into(), "api".into())],
        }
    }

    fn alerter(sink: Arc<MockSink>, ui: Option<&str>) -> Alerter {
        let mut a = Alerter::new(sink, ui.map(str::to_string), Arc::new(Metrics::default()));
        a.retry_base = Duration::from_millis(1);
        a
    }

    #[test]
    fn window_pads_the_matched_span_by_five_minutes() {
        let (from, to) = window(&outcome());
        assert_eq!(from, 1_000_000_000_000 - WINDOW_PAD_NS);
        assert_eq!(to, 4_000_000_000_000 + WINDOW_PAD_NS);
    }

    #[test]
    fn window_does_not_overflow_on_extreme_timestamps() {
        let mut o = outcome();
        o.matches = vec![MatchSample {
            ts: i64::MAX,
            line: "x".into(),
        }];
        let (from, to) = window(&o);
        assert_eq!(to, i64::MAX);
        assert!(from < to);
    }

    #[tokio::test]
    async fn match_alert_carries_emoji_samples_chips_and_link() {
        let sink = Arc::new(MockSink::default());
        let alerter = alerter(sink.clone(), Some("https://ui.example"));
        alerter.set_ticket(Ticket {
            node_addr: NodeAddrWire::default(),
            manifest_hash_hint: BlobHash([1; 32]),
        });

        alerter
            .dispatch(RuleEvent::Match(Box::new(outcome())))
            .await;

        let sent = sink.sent.lock().expect("lock");
        let payload = sent.first().expect("one message");
        let text = payload.to_string();
        assert!(text.contains("🚨"), "severity emoji missing: {text}");
        assert!(text.contains("panic-detector"));
        assert!(text.contains("`job=api`"), "label chips missing");
        assert!(text.contains("Open logs →"));
        assert!(text.contains("https://ui.example/#t="));

        let blocks = payload["blocks"].as_array().expect("blocks");
        let code = blocks[1]["text"]["text"].as_str().expect("code block");
        assert_eq!(
            code.lines().count(),
            5,
            "3 samples plus the fence lines: {code}"
        );
        assert!(!code.contains("fourth"), "only 3 samples go inline");
    }

    #[tokio::test]
    async fn missing_ui_config_still_alerts_without_a_link() {
        let sink = Arc::new(MockSink::default());
        let alerter = alerter(sink.clone(), None);
        alerter
            .dispatch(RuleEvent::Match(Box::new(outcome())))
            .await;

        let sent = sink.sent.lock().expect("lock");
        let text = sent.first().expect("one message").to_string();
        assert!(text.contains("panic-detector"));
        assert!(!text.contains("Open logs"), "no ticket, no link");
    }

    #[tokio::test]
    async fn code_fences_in_log_lines_cannot_break_out_of_the_block() {
        let sink = Arc::new(MockSink::default());
        let alerter = alerter(sink.clone(), None);
        let mut o = outcome();
        o.matches = vec![MatchSample {
            ts: 1,
            line: "``` injected".into(),
        }];
        alerter.dispatch(RuleEvent::Match(Box::new(o))).await;

        let sent = sink.sent.lock().expect("lock");
        let code = sent[0]["blocks"][1]["text"]["text"]
            .as_str()
            .expect("code block");
        assert_eq!(
            code.matches("```").count(),
            2,
            "exactly the opening and closing fence"
        );
    }

    #[tokio::test]
    async fn delivery_retries_then_succeeds() {
        let sink = Arc::new(MockSink::default());
        *sink.fail_first.lock().expect("lock") = 2;
        let alerter = alerter(sink.clone(), None);
        alerter.self_alert("disk on fire").await;
        assert_eq!(sink.sent.lock().expect("lock").len(), 1);
    }

    #[tokio::test]
    async fn delivery_gives_up_after_three_attempts_and_counts_the_drop() {
        let sink = Arc::new(MockSink::default());
        *sink.fail_first.lock().expect("lock") = 99;
        let metrics = Arc::new(Metrics::default());
        let mut alerter = Alerter::new(sink.clone(), None, metrics.clone());
        alerter.retry_base = Duration::from_millis(1);

        alerter.self_alert("nope").await;
        assert!(sink.sent.lock().expect("lock").is_empty());
        assert_eq!(metrics.alerts_dropped_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            *sink.fail_first.lock().expect("lock"),
            99 - MAX_ATTEMPTS,
            "exactly three attempts"
        );
    }

    #[tokio::test]
    async fn disabled_rule_notice_names_the_rule_and_the_reason() {
        let sink = Arc::new(MockSink::default());
        let alerter = alerter(sink.clone(), None);
        alerter
            .dispatch(RuleEvent::Disabled {
                rule: "hostile".into(),
                reason: "exhausted its fuel budget".into(),
            })
            .await;
        let sent = sink.sent.lock().expect("lock");
        let text = sent[0].to_string();
        assert!(text.contains("hostile"));
        assert!(text.contains("fuel"));
    }

    #[test]
    fn webhook_urls_never_reach_a_log_line_intact() {
        // Guards README §Constraints: secrets only via env, never logged.
        let sink = sink_from_config(Some("https://hooks.slack.com/services/T/B/SECRET"));
        assert!(Arc::strong_count(&sink) >= 1);
        assert!(!redact("https://hooks.slack.com/services/T/B/SECRET").contains("SECRET"));
    }
}
