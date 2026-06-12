use anyhow::Result;
use common::{Exchange, FillResult, RedisClient};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::db::{load_open_positions, PositionRow};

#[derive(Debug, Clone)]
struct Quote {
    bid: Decimal,
    ask: Decimal,
    ts_ms: u64,
}

type QuoteCache = Arc<Mutex<HashMap<(String, String), Quote>>>; // (symbol, exchange_str) → Quote

pub async fn run_close_monitor(
    pool: PgPool,
    redis_url: String,
    redis_client: RedisClient,
    mut new_position_rx: mpsc::Receiver<FillResult>,
    close_min_spread_pct: Decimal,
) -> Result<()> {
    let quote_cache: QuoteCache = Arc::new(Mutex::new(HashMap::new()));

    // Load existing open positions and collect symbols to monitor
    let initial_positions = load_open_positions(&pool).await?;
    let mut monitored_symbols: std::collections::HashSet<String> = initial_positions
        .iter()
        .map(|p| p.symbol.clone())
        .collect();

    info!(
        symbols = ?monitored_symbols,
        positions = initial_positions.len(),
        "Close monitor started"
    );

    let redis_client = Arc::new(redis_client);
    let pool = Arc::new(pool);

    // Spawn quote consumers for each known symbol
    for symbol in &monitored_symbols {
        spawn_quote_consumer(
            symbol.clone(),
            redis_url.clone(),
            Arc::clone(&quote_cache),
        );
    }

    // Main loop: check close conditions on every new position or periodically
    let mut check_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

    loop {
        tokio::select! {
            Some(fill) = new_position_rx.recv() => {
                if !monitored_symbols.contains(&fill.symbol) {
                    monitored_symbols.insert(fill.symbol.clone());
                    spawn_quote_consumer(
                        fill.symbol.clone(),
                        redis_url.clone(),
                        Arc::clone(&quote_cache),
                    );
                }
            }

            _ = check_interval.tick() => {
                let positions = match load_open_positions(&pool).await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(error = %e, "Failed to load positions for close check");
                        continue;
                    }
                };

                for pos in positions {
                    if pos.dry_run {
                        continue;
                    }

                    let cache = quote_cache.lock().unwrap();
                    let long_key = (pos.symbol.clone(), pos.long_exchange.to_string());
                    let short_key = (pos.symbol.clone(), pos.short_exchange.to_string());

                    let (long_quote, short_quote) = match (cache.get(&long_key), cache.get(&short_key)) {
                        (Some(l), Some(s)) => (l.clone(), s.clone()),
                        _ => continue,
                    };
                    drop(cache);

                    let now = now_ms();
                    if now - long_quote.ts_ms > 100 || now - short_quote.ts_ms > 100 {
                        continue; // stale quotes
                    }

                    // Closing spread: we sell long leg at bid, buy back short leg at ask
                    let closing_spread = if short_quote.ask.is_zero() {
                        continue;
                    } else {
                        (long_quote.bid - short_quote.ask) / short_quote.ask * Decimal::from(100u32)
                    };

                    if closing_spread >= close_min_spread_pct {
                        info!(
                            trade_id = %pos.trade_id,
                            symbol = %pos.symbol,
                            closing_spread = %closing_spread.round_dp(4),
                            "Close condition met — spread captured, initiating close"
                        );

                        let pool_clone = Arc::clone(&pool);
                        let redis_client_clone = Arc::clone(&redis_client);

                        tokio::spawn(async move {
                            crate::closer::close_position(
                                &pool_clone,
                                &redis_client_clone,
                                pos,
                            )
                            .await;
                        });
                    }
                }
            }
        }
    }
}

fn spawn_quote_consumer(
    symbol: String,
    redis_url: String,
    cache: QuoteCache,
) {
    tokio::spawn(async move {
        if let Err(e) = consume_quotes_for_symbol(symbol.clone(), redis_url, cache).await {
            warn!(symbol = %symbol, error = %e, "Quote consumer error");
        }
    });
}

async fn consume_quotes_for_symbol(
    symbol: String,
    redis_url: String,
    cache: QuoteCache,
) -> Result<()> {
    let stream_key = format!("prices:{}", symbol);
    let client = redis::Client::open(redis_url.as_str())?;
    let config = redis::AsyncConnectionConfig::new()
        .set_connection_timeout(None)
        .set_response_timeout(None);
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&config)
        .await?;

    let mut last_id = "$".to_string();

    loop {
        let opts = StreamReadOptions::default().block(1000).count(50);
        let result: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&[stream_key.as_str()], &[last_id.as_str()], &opts).await;

        match result {
            Ok(reply) => {
                for stream in reply.keys {
                    for entry in stream.ids {
                        last_id = entry.id.clone();

                        let mut map: HashMap<String, String> = HashMap::new();
                        for (k, v) in &entry.map {
                            if let redis::Value::BulkString(b) = v {
                                if let Ok(s) = String::from_utf8(b.clone()) {
                                    map.insert(k.clone(), s);
                                }
                            }
                        }

                        let exchange_str = match map.get("exchange") {
                            Some(e) => e.clone(),
                            None => continue,
                        };
                        let bid = match map.get("bid").and_then(|s| Decimal::from_str(s).ok()) {
                            Some(v) => v,
                            None => continue,
                        };
                        let ask = match map.get("ask").and_then(|s| Decimal::from_str(s).ok()) {
                            Some(v) => v,
                            None => continue,
                        };
                        let ts_ms = map
                            .get("timestamp")
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or_else(now_ms);

                        let key = (symbol.clone(), exchange_str);
                        cache.lock().unwrap().insert(key, Quote { bid, ask, ts_ms });
                    }
                }
            }
            Err(e) => {
                warn!(symbol = %symbol, error = %e, "Quote stream error, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
