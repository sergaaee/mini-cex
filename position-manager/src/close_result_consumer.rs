use anyhow::Result;
use common::{CloseResult, Exchange};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
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

fn parse_close_result(fields: &[(String, redis::Value)]) -> Option<CloseResult> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, value) in fields {
        if let redis::Value::BulkString(bytes) = value {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                map.insert(key.clone(), s);
            }
        }
    }
    Some(CloseResult {
        trade_id: map.get("trade_id")?.clone(),
        symbol: map.get("symbol")?.clone(),
        long_exchange: parse_exchange(map.get("long_exchange")?)?,
        long_close_order_id: map.get("long_close_order_id")?.clone(),
        long_close_avg_price: Decimal::from_str(map.get("long_close_avg_price")?).ok()?,
        long_entry_price: Decimal::from_str(map.get("long_entry_price")?).ok()?,
        long_qty: Decimal::from_str(map.get("long_qty")?).ok()?,
        short_exchange: parse_exchange(map.get("short_exchange")?)?,
        short_close_order_id: map.get("short_close_order_id")?.clone(),
        short_close_avg_price: Decimal::from_str(map.get("short_close_avg_price")?).ok()?,
        short_entry_price: Decimal::from_str(map.get("short_entry_price")?).ok()?,
        short_qty: Decimal::from_str(map.get("short_qty")?).ok()?,
        entry_spread_pct: Decimal::from_str(map.get("entry_spread_pct")?).ok()?,
        close_spread_pct: Decimal::from_str(map.get("close_spread_pct")?).ok()?,
        realized_pnl: Decimal::from_str(map.get("realized_pnl")?).ok()?,
        long_fee: Decimal::from_str(map.get("long_fee")?).ok()?,
        short_fee: Decimal::from_str(map.get("short_fee")?).ok()?,
        dry_run: map.get("dry_run").map(|v| v == "true").unwrap_or(false),
        timestamp: map.get("timestamp")?.parse().unwrap_or(0),
    })
}

fn no_timeout_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_connection_timeout(None)
        .set_response_timeout(None)
}

pub async fn consume_close_results(redis_url: String, pool: PgPool) -> Result<()> {
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&no_timeout_config())
        .await?;

    let mut last_id: String = redis::cmd("XREVRANGE")
        .arg("close_results")
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

    info!(cursor = %last_id, "Consuming close_results stream");

    loop {
        let opts = StreamReadOptions::default().block(5000).count(50);
        let result: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&["close_results"], &[last_id.as_str()], &opts).await;

        match result {
            Ok(reply) => {
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();
                        let fields: Vec<(String, redis::Value)> =
                            entry.map.into_iter().collect();
                        if let Some(cr) = parse_close_result(&fields) {
                            info!(
                                trade_id = %cr.trade_id,
                                realized_pnl = %cr.realized_pnl,
                                "CloseResult received — updating DB"
                            );
                            if let Err(e) = crate::db::mark_closed(&pool, &cr).await {
                                warn!(error = %e, "Failed to mark position as closed");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading close_results stream, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
        }
    }
}
