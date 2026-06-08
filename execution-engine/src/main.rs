use anyhow::Result;
use axum::{routing::get, Router};
use common::{RedisClient, SpreadOpportunity, TradeSignal};
use prometheus::{Encoder, TextEncoder};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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

async fn trade_publisher_worker(redis_url: String, mut rx: mpsc::Receiver<TradeSignal>) {
    let redis_client = RedisClient::from_url(&redis_url);

    let mut conn = match redis_client.get_connection().await {
        Ok(c) => c,
        Err(e) => {
            error!("Trade publisher: failed to connect to Redis: {}", e);
            return;
        }
    };

    info!("Trade publisher started");

    while let Some(signal) = rx.recv().await {
        match redis_client.publish_trade_signal(&mut conn, &signal).await {
            Ok(_) => {
                info!(
                    symbol = %signal.symbol,
                    spread_pct = %signal.spread_percent,
                    dry_run = signal.dry_run,
                    "Published trade signal"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to publish trade signal, reconnecting...");
                match redis_client.get_connection().await {
                    Ok(new_conn) => conn = new_conn,
                    Err(e) => error!("Trade publisher: reconnect failed: {}", e),
                }
            }
        }
    }
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

    let min_spread_pct: Decimal = std::env::var("MIN_SPREAD_PCT")
        .unwrap_or_else(|_| "0.15".into())
        .parse()
        .unwrap_or(Decimal::new(15, 2));

    let trade_size_usd: Decimal = std::env::var("TRADE_SIZE_USD")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap_or(Decimal::from(100));

    let cooldown_secs: u64 = std::env::var("COOLDOWN_SECS")
        .unwrap_or_else(|_| "30".into())
        .parse()
        .unwrap_or(30);

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

    let (spread_tx, mut spread_rx) = mpsc::channel::<SpreadOpportunity>(256);
    let (trade_tx, trade_rx) = mpsc::channel::<TradeSignal>(64);

    tokio::spawn(start_metrics_server(metrics_port));
    tokio::spawn(consumer::consume_spreads(redis_url.clone(), spread_tx));
    tokio::spawn(trade_publisher_worker(redis_url, trade_rx));

    let mut engine = decision::DecisionEngine::new(
        min_spread_pct,
        trade_size_usd,
        cooldown_secs,
        dry_run,
        trade_tx,
    );

    while let Some(opp) = spread_rx.recv().await {
        engine.evaluate(opp);
    }

    Ok(())
}
