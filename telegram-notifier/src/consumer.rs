use anyhow::Result;
use common::{Exchange, TradeSignal};
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

fn parse_trade_entry(fields: &[(String, redis::Value)]) -> Option<TradeSignal> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (key, value) in fields {
        if let redis::Value::BulkString(bytes) = value {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                map.insert(key.clone(), s);
            }
        }
    }

    Some(TradeSignal {
        symbol: map.get("symbol")?.clone(),
        long_exchange: parse_exchange(map.get("long_exchange")?)?,
        long_price: Decimal::from_str(map.get("long_price")?).ok()?,
        short_exchange: parse_exchange(map.get("short_exchange")?)?,
        short_price: Decimal::from_str(map.get("short_price")?).ok()?,
        spread_percent: Decimal::from_str(map.get("spread_percent")?).ok()?,
        qty: Decimal::from_str(map.get("qty")?).unwrap_or(Decimal::ZERO),
        dry_run: map.get("dry_run").map(|v| v == "true").unwrap_or(true),
        timestamp: map.get("timestamp")?.parse().unwrap_or(0),
    })
}

pub async fn consume_trades(redis_url: String, tx: mpsc::Sender<TradeSignal>) -> Result<()> {
    let client = redis::Client::open(redis_url.as_str())?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    let mut last_id = "$".to_string();

    tracing::info!("Consuming trades stream");

    loop {
        let opts = StreamReadOptions::default().block(5000).count(20);

        let result: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&["trades"], &[last_id.as_str()], &opts).await;

        match result {
            Ok(reply) => {
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();

                        let fields: Vec<(String, redis::Value)> =
                            entry.map.into_iter().collect();

                        if let Some(signal) = parse_trade_entry(&fields) {
                            debug!(
                                symbol = %signal.symbol,
                                spread = %signal.spread_percent,
                                dry_run = signal.dry_run,
                                "Received trade signal"
                            );
                            if tx.send(signal).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading trades stream, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    }
}
