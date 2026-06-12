use anyhow::Result;
use common::{FillResult, RedisClient};
use rust_decimal::Decimal;
use sqlx::postgres::PgPoolOptions;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::info;

mod close_monitor;
mod close_result_consumer;
mod closer;
mod db;
mod fill_consumer;
mod poller;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("position_manager=info".parse().unwrap()),
        )
        .init();

    dotenvy::dotenv().ok();
    common::models::position::warmup_connections().await;

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://redis:6379/".into());

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL env var required");

    let close_min_spread_pct = std::env::var("CLOSE_MIN_SPREAD_PCT")
        .ok()
        .and_then(|s| Decimal::from_str(&s).ok())
        .unwrap_or(Decimal::ZERO);

    let poll_interval_secs: u64 = std::env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);

    info!(
        redis_url = %redis_url,
        close_min_spread_pct = %close_min_spread_pct,
        poll_interval_secs = poll_interval_secs,
        "Starting position-manager"
    );

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await?;

    db::migrate(&pool).await?;
    info!("Database schema ready");

    let redis_client = RedisClient::from_url(&redis_url);

    let (new_position_tx, new_position_rx) = mpsc::channel::<FillResult>(64);

    // Fill consumer: reads fills → inserts open positions into DB
    tokio::spawn(fill_consumer::consume_fills(
        redis_url.clone(),
        pool.clone(),
        new_position_tx,
    ));

    // Poller: every N seconds fetch real position state from exchanges
    tokio::spawn(poller::run_poller(pool.clone(), poll_interval_secs));

    // Close result consumer: reads close_results → marks positions as closed in DB
    tokio::spawn(close_result_consumer::consume_close_results(
        redis_url.clone(),
        pool.clone(),
    ));

    // Close monitor: watches quotes, triggers close when spread ≤ threshold
    close_monitor::run_close_monitor(
        pool,
        redis_url,
        redis_client,
        new_position_rx,
        close_min_spread_pct,
    )
    .await?;

    Ok(())
}
