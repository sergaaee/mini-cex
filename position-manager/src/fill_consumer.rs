use anyhow::Result;
use common::{Exchange, FillResult};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{info, warn};

fn parse_exchange(s: &str) -> Option<Exchange> {
    match s {
        "Binance" => Some(Exchange::Binance),
        "Hibachi" => Some(Exchange::Hibachi),
        "Aster" => Some(Exchange::Aster),
        "Bybit" => Some(Exchange::Bybit),
        "Backpack" => Some(Exchange::Backpack),
        "BloFin" => Some(Exchange::BloFin),
        "OKX" => Some(Exchange::OKX),
        _ => None,
    }
}

fn parse_fill_result(fields: &[(String, redis::Value)]) -> Option<FillResult> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, value) in fields {
        if let redis::Value::BulkString(bytes) = value {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                map.insert(key.clone(), s);
            }
        }
    }
    Some(FillResult {
        trade_id: map.get("trade_id")?.clone(),
        symbol: map.get("symbol")?.clone(),
        long_exchange: parse_exchange(map.get("long_exchange")?)?,
        long_order_id: map.get("long_order_id")?.clone(),
        long_avg_price: Decimal::from_str(map.get("long_avg_price")?).ok()?,
        long_filled_qty: Decimal::from_str(map.get("long_filled_qty")?).ok()?,
        short_exchange: parse_exchange(map.get("short_exchange")?)?,
        short_order_id: map.get("short_order_id")?.clone(),
        short_avg_price: Decimal::from_str(map.get("short_avg_price")?).ok()?,
        short_filled_qty: Decimal::from_str(map.get("short_filled_qty")?).ok()?,
        planned_spread_pct: Decimal::from_str(map.get("planned_spread_pct")?).ok()?,
        realized_spread_pct: Decimal::from_str(map.get("realized_spread_pct")?).ok()?,
        dry_run: map.get("dry_run").map(|v| v == "true").unwrap_or(false),
        timestamp: map.get("timestamp")?.parse().unwrap_or(0),
    })
}

fn no_timeout_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_connection_timeout(None)
        .set_response_timeout(None)
}

pub async fn consume_fills(
    redis_url: String,
    pool: PgPool,
    new_position_tx: mpsc::Sender<FillResult>,
) -> Result<()> {
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&no_timeout_config())
        .await?;

    // Start from current tail — only track fills from now on
    let mut last_id: String = redis::cmd("XREVRANGE")
        .arg("fills")
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

    info!(cursor = %last_id, "Consuming fills stream for position tracking");

    loop {
        let opts = StreamReadOptions::default().block(5000).count(50);
        let result: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&["fills"], &[last_id.as_str()], &opts).await;

        match result {
            Ok(reply) => {
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();
                        let fields: Vec<(String, redis::Value)> =
                            entry.map.into_iter().collect();
                        if let Some(fill) = parse_fill_result(&fields) {
                            info!(trade_id = %fill.trade_id, symbol = %fill.symbol, "New fill — inserting position");
                            if let Err(e) = crate::db::insert_position(&pool, &fill).await {
                                warn!(error = %e, "Failed to insert position");
                            }
                            let _ = new_position_tx.send(fill).await;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading fills stream, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
        }
    }
}
