use anyhow::Result;
use axum::{routing::get, Router};
use common::SpreadOpportunity;
use prometheus::{Encoder, TextEncoder};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::info;

mod consumer;
mod decision;
mod metrics;

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = metrics::REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn start_metrics_server(port: u16) {
    let app = Router::new().route("/metrics", get(metrics_handler));
    let addr = format!("0.0.0.0:{}", port);
    info!(port, "Starting metrics server");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("execution_engine=info".parse().unwrap()),
        )
        .init();

    metrics::register();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://redis:6379/".into());

    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9093".into())
        .parse()
        .unwrap_or(9093);

    // Minimum spread (%) required to trigger a trade signal
    let min_spread_pct: Decimal = std::env::var("MIN_SPREAD_PCT")
        .unwrap_or_else(|_| "0.15".into())
        .parse()
        .unwrap_or(Decimal::new(1, 1));

    // Notional trade size in USD per leg
    let trade_size_usd: Decimal = std::env::var("TRADE_SIZE_USD")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap_or(Decimal::from(100));

    // Per-symbol cooldown seconds after a trade to prevent over-trading
    let cooldown_secs: u64 = std::env::var("COOLDOWN_SECS")
        .unwrap_or_else(|_| "30".into())
        .parse()
        .unwrap_or(30);

    // DRY_RUN=false to enable live trading
    let dry_run = std::env::var("DRY_RUN")
        .map(|v| v.to_lowercase() != "false")
        .unwrap_or(true);

    info!(
        redis_url = %redis_url,
        metrics_port,
        min_spread_pct = %min_spread_pct,
        trade_size_usd = %trade_size_usd,
        cooldown_secs,
        dry_run,
        "Starting execution engine"
    );

    let (tx, mut rx) = mpsc::channel::<SpreadOpportunity>(256);

    tokio::spawn(start_metrics_server(metrics_port));
    tokio::spawn(consumer::consume_spreads(redis_url, tx));

    let mut engine = decision::DecisionEngine::new(
        min_spread_pct,
        trade_size_usd,
        cooldown_secs,
        dry_run,
    );

    while let Some(opp) = rx.recv().await {
        engine.evaluate(opp);
    }

    Ok(())
}
