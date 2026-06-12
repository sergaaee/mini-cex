use anyhow::Result;
use common::{FillResult, PendingFill, RedisClient};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

mod binance_ws;
mod hibachi_ws;
mod tracker;

use tracker::FillTracker;

fn parse_exchange(s: &str) -> Option<common::Exchange> {
    match s {
        "Binance" => Some(common::Exchange::Binance),
        "Hibachi" => Some(common::Exchange::Hibachi),
        "Aster" => Some(common::Exchange::Aster),
        "Bybit" => Some(common::Exchange::Bybit),
        "Backpack" => Some(common::Exchange::Backpack),
        "BloFin" => Some(common::Exchange::BloFin),
        "OKX" => Some(common::Exchange::OKX),
        _ => None,
    }
}

fn parse_pending_fill(fields: &[(String, redis::Value)]) -> Option<PendingFill> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, value) in fields {
        if let redis::Value::BulkString(bytes) = value {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                map.insert(key.clone(), s);
            }
        }
    }
    Some(PendingFill {
        trade_id: map.get("trade_id")?.clone(),
        symbol: map.get("symbol")?.clone(),
        long_exchange: parse_exchange(map.get("long_exchange")?)?,
        long_order_id: map.get("long_order_id")?.clone(),
        short_exchange: parse_exchange(map.get("short_exchange")?)?,
        short_order_id: map.get("short_order_id")?.clone(),
        planned_spread_pct: Decimal::from_str(map.get("planned_spread_pct")?).ok()?,
        planned_long_price: Decimal::from_str(map.get("planned_long_price")?).ok()?,
        planned_short_price: Decimal::from_str(map.get("planned_short_price")?).ok()?,
        qty: Decimal::from_str(map.get("qty")?).unwrap_or(Decimal::ZERO),
        dry_run: map.get("dry_run").map(|v| v == "true").unwrap_or(false),
        timestamp: map.get("timestamp")?.parse().unwrap_or(0),
    })
}

fn no_timeout_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_connection_timeout(None)
        .set_response_timeout(None)
}

async fn consume_pending_fills(redis_url: String, tracker: Arc<Mutex<FillTracker>>) -> Result<()> {
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&no_timeout_config())
        .await?;

    let mut last_id: String = redis::cmd("XREVRANGE")
        .arg("pending_fills")
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1usize)
        .query_async::<redis::streams::StreamRangeReply>(&mut conn)
        .await
        .ok()
        .and_then(|r| r.ids.into_iter().next())
        .map(|e| e.id)
        .unwrap_or_else(|| "0-0".to_string());

    info!(cursor = %last_id, "Consuming pending_fills stream");

    loop {
        let opts = StreamReadOptions::default().block(1000).count(100);
        let result: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&["pending_fills"], &[last_id.as_str()], &opts).await;

        match result {
            Ok(reply) => {
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();
                        let fields: Vec<(String, redis::Value)> =
                            entry.map.into_iter().collect();
                        if let Some(pf) = parse_pending_fill(&fields) {
                            info!(trade_id = %pf.trade_id, "Received PendingFill");
                            tracker.lock().unwrap().add_pending(pf);
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading pending_fills, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
        }
    }
}

async fn fill_publisher_worker(redis_url: String, mut rx: mpsc::Receiver<FillResult>) {
    let redis_client = RedisClient::from_url(&redis_url);
    let mut conn = loop {
        match redis_client.get_connection().await {
            Ok(c) => break c,
            Err(e) => {
                error!("Fill publisher: failed to connect to Redis: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    };

    info!("Fill publisher worker started");

    while let Some(result) = rx.recv().await {
        match redis_client.publish_fill_result(&mut conn, &result).await {
            Ok(_) => info!(trade_id = %result.trade_id, "Published FillResult"),
            Err(e) => {
                warn!(error = %e, "Failed to publish FillResult, reconnecting...");
                match redis_client.get_connection().await {
                    Ok(new_conn) => conn = new_conn,
                    Err(e) => error!("Fill publisher: reconnect failed: {}", e),
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
                .add_directive("fill_tracker=info".parse().unwrap()),
        )
        .init();

    dotenvy::dotenv().ok();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://redis:6379/".into());

    let binance_api_key = std::env::var("BINANCE_API_KEY")
        .expect("BINANCE_API_KEY env var required");

    let hibachi_account_id: u64 = std::env::var("HIBACHI_CLIENT_ID")
        .expect("HIBACHI_CLIENT_ID env var required")
        .parse()
        .expect("HIBACHI_CLIENT_ID must be a number");

    let hibachi_api_key = std::env::var("HIBACHI_API_KEY")
        .expect("HIBACHI_API_KEY env var required");

    info!(redis_url = %redis_url, "Starting fill-tracker");

    let (fill_tx, fill_rx) = mpsc::channel::<FillResult>(64);
    let tracker = Arc::new(Mutex::new(FillTracker::new(fill_tx)));

    tokio::spawn(consume_pending_fills(redis_url.clone(), Arc::clone(&tracker)));
    tokio::spawn(binance_ws::run(Arc::clone(&tracker), binance_api_key));
    tokio::spawn(hibachi_ws::run(
        Arc::clone(&tracker),
        hibachi_account_id,
        hibachi_api_key,
    ));

    fill_publisher_worker(redis_url, fill_rx).await;

    Ok(())
}
