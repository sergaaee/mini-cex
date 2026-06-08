use anyhow::Result;
use common::{Exchange, SpreadOpportunity};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tracing::{debug, warn};

fn parse_exchange(s: &str) -> Option<Exchange> {
    match s {
        "Binance" => Some(Exchange::Binance),
        "Bybit" => Some(Exchange::Bybit),
        "OKX" => Some(Exchange::OKX),
        "Hibachi" => Some(Exchange::Hibachi),
        "Backpack" => Some(Exchange::Backpack),
        "Aster" => Some(Exchange::Aster),
        "BloFin" => Some(Exchange::BloFin),
        _ => None,
    }
}

fn parse_spread_entry(fields: &[(String, redis::Value)]) -> Option<SpreadOpportunity> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, value) in fields {
        if let redis::Value::BulkString(bytes) = value {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                map.insert(key.clone(), s);
            }
        }
    }

    Some(SpreadOpportunity {
        symbol: map.get("symbol")?.clone(),
        long_exchange: parse_exchange(map.get("long_exchange")?)?,
        long_exchange_price: Decimal::from_str(map.get("long_exchange_price")?).ok()?,
        short_exchange: parse_exchange(map.get("short_exchange")?)?,
        short_exchange_price: Decimal::from_str(map.get("short_exchange_price")?).ok()?,
        spread_percent: Decimal::from_str(map.get("spread_percent")?).ok()?,
        size: Decimal::from_str(map.get("size")?).unwrap_or(Decimal::ZERO),
        timestamp: map.get("timestamp")?.parse().unwrap_or(0),
    })
}

/// Reads `SpreadOpportunity` entries from the `spreads` Redis Stream and
/// forwards them to the decision engine via a channel.
/// Starts at `$` so only new entries (posted after startup) are processed.
pub async fn consume_spreads(redis_url: String, tx: mpsc::Sender<SpreadOpportunity>) -> Result<()> {
    let redis_url = redis_url.as_str();
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut last_id = "$".to_string();

    tracing::info!("Consuming spreads stream");

    loop {
        let opts = StreamReadOptions::default().block(1000).count(50);

        let result: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&["spreads"], &[last_id.as_str()], &opts).await;

        match result {
            Ok(reply) => {
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();

                        let fields: Vec<(String, redis::Value)> =
                            entry.map.into_iter().collect();

                        if let Some(opp) = parse_spread_entry(&fields) {
                            debug!(
                                symbol = %opp.symbol,
                                spread_pct = %opp.spread_percent,
                                long = %opp.long_exchange,
                                short = %opp.short_exchange,
                                "Received spread opportunity"
                            );
                            if tx.send(opp).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading spreads stream, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}
