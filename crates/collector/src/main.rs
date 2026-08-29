//! CLI entry point and startup wiring.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::Parser;
use permafrost_collector::alert::{Alerter, sink_from_config};
use permafrost_collector::config::{Cli, Config, ensure_dirs};
use permafrost_collector::ingest::{AppState, Metrics, router};
use permafrost_collector::p2p::P2p;
use permafrost_collector::rules::{RuleEngine, watch};
use permafrost_collector::segment::{SegmentWriter, purge_expired};

/// How often the age-based flush and retention checks run.
const TICK: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error(transparent)]
    Config(#[from] permafrost_collector::config::ConfigError),
    #[error("preparing data dir: {0}")]
    DataDir(#[from] std::io::Error),
    #[error(transparent)]
    Rules(#[from] permafrost_collector::rules::RuleError),
    #[error(transparent)]
    P2p(#[from] permafrost_collector::p2p::P2pError),
    #[error("watching rules dir: {0}")]
    Watch(#[from] notify::Error),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,iroh=warn".into()),
        )
        .init();

    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "startup failed");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), StartupError> {
    let cfg = Config::load(Cli::parse())?;
    ensure_dirs(&cfg)?;

    let metrics = Arc::new(Metrics::default());
    let alerter = Arc::new(Alerter::new(
        sink_from_config(cfg.slack_webhook.as_deref()),
        cfg.ui_base_url.clone(),
        metrics.clone(),
    ));

    let rules = RuleEngine::new(cfg.rules_dir.clone(), cfg.rule_fuel)?;
    // The watcher must outlive this scope or hot reload stops firing.
    let _watcher = watch(rules.clone())?;

    let p2p = P2p::spawn(&cfg).await?;
    let (segments, mut flushed) = SegmentWriter::new(
        cfg.segments_dir(),
        cfg.segment_max_age,
        cfg.segment_max_bytes,
    );

    // Announce flushed segments and refresh the ticket the alerter links to.
    tokio::spawn({
        let (p2p, alerter) = (p2p.clone(), alerter.clone());
        async move {
            while let Some(meta) = flushed.recv().await {
                match p2p.announce(&meta).await {
                    Ok(_) => {
                        if let Some(ticket) = p2p.ticket() {
                            alerter.set_ticket(ticket);
                        }
                    }
                    // Segments are already on disk; a failed announce is retried
                    // on the next flush rather than dropped data.
                    Err(err) => {
                        tracing::error!(%err, "blob announce failed, will retry next flush")
                    }
                }
            }
        }
    });

    // Age-based flush plus retention.
    tokio::spawn({
        let (segments, p2p, alerter, metrics, cfg) = (
            segments.clone(),
            p2p.clone(),
            alerter.clone(),
            metrics.clone(),
            cfg.clone(),
        );
        async move {
            let mut ticker = tokio::time::interval(TICK);
            loop {
                ticker.tick().await;

                // AIDEV-NOTE: purge before flush, not after. Retention keys off the log
                // timestamps in the filename, so a backfilled segment can be
                // born already expired; purging first gives the announce task
                // a full tick to pick the segment up instead of racing it and
                // failing to hash a file retention just deleted.
                let expired = purge_expired(&cfg.segments_dir(), cfg.retention, SystemTime::now());
                if !expired.is_empty() {
                    tracing::info!(count = expired.len(), "retention removed segments");
                    if let Err(err) = p2p.forget_segments(&expired).await {
                        tracing::error!(%err, "manifest update after retention failed");
                    }
                }

                match segments.flush_if_stale().await {
                    Ok(Some(_)) => {
                        metrics
                            .segments_flushed_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::error!(%err, "age-triggered flush failed");
                        alerter
                            .self_alert(&format!("segment flush failed: {err}"))
                            .await;
                    }
                }
            }
        }
    });

    tokio::spawn({
        let p2p = p2p.clone();
        async move {
            p2p.wait_online().await;
            tracing::info!(node = %p2p.endpoint_id(), "iroh relay connection established");
        }
    });

    let state = AppState {
        segments: segments.clone(),
        rules,
        alerter,
        metrics,
    };
    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    tracing::info!(
        listen = %cfg.listen,
        data_dir = %cfg.data_dir.display(),
        node = %p2p.endpoint_id(),
        "permafrost ready"
    );

    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    // Do not lose whatever is still buffered on a clean exit.
    match segments.flush().await {
        Ok(Some(meta)) => {
            if let Err(err) = p2p.announce(&meta).await {
                tracing::error!(%err, "final announce failed");
            }
        }
        Ok(None) => {}
        Err(err) => tracing::error!(%err, "final flush failed"),
    }
    p2p.shutdown().await;
    Ok(())
}
