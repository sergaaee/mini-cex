use anyhow::Result;
use axum::{routing::get, Router};
use common::{Exchange, SpreadOpportunity};
use lazy_static::lazy_static;
use prometheus::{Encoder, GaugeVec, IntCounter, Opts, Registry, TextEncoder};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    static ref SPREAD_PERCENT: GaugeVec = GaugeVec::new(
        Opts::new("spread_percent", "Spread between exchanges in percent"),
        &["symbol", "exchange_high", "exchange_low"]
    ).unwrap();

    static ref SPREAD_ABSOLUTE: GaugeVec = GaugeVec::new(
        Opts::new("spread_absolute", "Absolute spread between exchanges"),
        &["symbol", "exchange_high", "exchange_low"]
    ).unwrap();

    static ref BEST_BID: GaugeVec = GaugeVec::new(
        Opts::new("best_bid", "Best bid price across exchanges"),
        &["symbol", "exchange"]
    ).unwrap();

    static ref BEST_ASK: GaugeVec = GaugeVec::new(
        Opts::new("best_ask", "Best ask price across exchanges"),
        &["symbol", "exchange"]
    ).unwrap();

    static ref EVENTS_PROCESSED: IntCounter = IntCounter::new(
        "spread_events_processed_total",
        "Total number of price events processed"
    ).unwrap();

    static ref SPREADS_CALCULATED: IntCounter = IntCounter::new(
        "spreads_calculated_total",
        "Total number of spreads calculated"
    ).unwrap();

    static ref SPREADS_PUBLISHED: IntCounter = IntCounter::new(
        "spreads_published_total",
        "Total number of spread opportunities published to Redis"
    ).unwrap();

    static ref QUOTE_EVENT_TIMESTAMP_MS: GaugeVec = GaugeVec::new(
        Opts::new("quote_event_timestamp_ms", "Last exchange-emitted quote timestamp per exchange (ms)"),
        &["symbol", "exchange"]
    ).unwrap();

    static ref QUOTE_RECEIVED_TIMESTAMP_MS: GaugeVec = GaugeVec::new(
        Opts::new("quote_received_timestamp_ms", "When our aggregator received the quote (ms)"),
        &["symbol", "exchange"]
    ).unwrap();
}

fn register_metrics() {
    REGISTRY.register(Box::new(SPREAD_PERCENT.clone())).ok();
    REGISTRY.register(Box::new(SPREAD_ABSOLUTE.clone())).ok();
    REGISTRY.register(Box::new(BEST_BID.clone())).ok();
    REGISTRY.register(Box::new(BEST_ASK.clone())).ok();
    REGISTRY.register(Box::new(EVENTS_PROCESSED.clone())).ok();
    REGISTRY.register(Box::new(SPREADS_CALCULATED.clone())).ok();
    REGISTRY.register(Box::new(SPREADS_PUBLISHED.clone())).ok();
    REGISTRY.register(Box::new(QUOTE_EVENT_TIMESTAMP_MS.clone())).ok();
    REGISTRY.register(Box::new(QUOTE_RECEIVED_TIMESTAMP_MS.clone())).ok();
}

#[derive(Debug, Clone)]
struct QuoteData {
    bid: Decimal,
    ask: Decimal,
    timestamp: u64,   // ms — exchange event time
    received_at: u64, // ms — local aggregator receive time
}

type QuoteStore = Arc<RwLock<HashMap<String, HashMap<Exchange, QuoteData>>>>;

#[derive(Debug, Deserialize)]
struct StreamEntry {
    exchange: String,
    bid: String,
    ask: String,
    #[allow(dead_code)]
    bid_size: String,
    #[allow(dead_code)]
    ask_size: String,
    #[allow(dead_code)]
    mid: String,
    timestamp: String,
    received_at: String,
}

fn parse_stream_entry(fields: &[(String, redis::Value)]) -> Option<StreamEntry> {
    let mut map: HashMap<String, String> = HashMap::new();

    for (key, value) in fields {
        if let redis::Value::BulkString(bytes) = value {
            if let Ok(s) = String::from_utf8(bytes.clone()) {
                map.insert(key.clone(), s);
            }
        }
    }

    Some(StreamEntry {
        exchange: map.get("exchange")?.clone(),
        bid: map.get("bid")?.clone(),
        ask: map.get("ask")?.clone(),
        bid_size: map.get("bid_size").cloned().unwrap_or_default(),
        ask_size: map.get("ask_size").cloned().unwrap_or_default(),
        mid: map.get("mid").cloned().unwrap_or_default(),
        timestamp: map.get("timestamp")?.clone(),
        received_at: map.get("received_at").cloned().unwrap_or_default(),
    })
}

fn parse_exchange(s: &str) -> Option<Exchange> {
    match s.to_lowercase().as_str() {
        "binance" => Some(Exchange::Binance),
        "bybit" => Some(Exchange::Bybit),
        "okx" => Some(Exchange::OKX),
        "hibachi" => Some(Exchange::Hibachi),
        "backpack" => Some(Exchange::Backpack),
        "aster" => Some(Exchange::Aster),
        "blofin" => Some(Exchange::BloFin),
        _ => None,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Calculates spreads between all exchange pairs for a symbol.
/// Returns opportunities where spread_pct >= publish_min_pct.
/// Skips any pair where either quote is older than max_age_ms.
fn calculate_spreads(
    symbol: &str,
    quotes: &HashMap<Exchange, QuoteData>,
    publish_min_pct: Decimal,
    max_age_ms: u64,
) -> Vec<SpreadOpportunity> {
    let mut opportunities = Vec::new();

    if quotes.len() < 2 {
        return opportunities;
    }

    let exchanges: Vec<_> = quotes.iter().collect();
    let ts = now_ms();

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let (ex1, q1) = exchanges[i];
            let (ex2, q2) = exchanges[j];

            let age1 = ts.saturating_sub(q1.timestamp);
            let age2 = ts.saturating_sub(q2.timestamp);

            if age1 > max_age_ms {
                warn!(
                    symbol, exchange = %ex1, age_ms = age1, max_ms = max_age_ms,
                    "Stale quote, skipping pair"
                );
                continue;
            }
            if age2 > max_age_ms {
                warn!(
                    symbol, exchange = %ex2, age_ms = age2, max_ms = max_age_ms,
                    "Stale quote, skipping pair"
                );
                continue;
            }

            // Buy on ex1 (ask), sell on ex2 (bid)
            if q2.bid > q1.ask {
                let spread_abs = q2.bid - q1.ask;
                let spread_pct = (spread_abs / q1.ask) * Decimal::from(100);

                SPREAD_ABSOLUTE
                    .with_label_values(&[symbol, &ex2.to_string(), &ex1.to_string()])
                    .set(spread_abs.to_string().parse().unwrap_or(0.0));
                SPREAD_PERCENT
                    .with_label_values(&[symbol, &ex2.to_string(), &ex1.to_string()])
                    .set(spread_pct.to_string().parse().unwrap_or(0.0));
                SPREADS_CALCULATED.inc();

                debug!(
                    symbol, buy_at = %ex1, sell_at = %ex2, spread_pct = %spread_pct,
                    "Arbitrage opportunity detected"
                );

                if spread_pct >= publish_min_pct {
                    opportunities.push(SpreadOpportunity {
                        symbol: symbol.to_string(),
                        long_exchange: *ex1,
                        long_exchange_price: q1.ask,
                        short_exchange: *ex2,
                        short_exchange_price: q2.bid,
                        spread_percent: spread_pct,
                        size: Decimal::ZERO,
                        timestamp: ts,
                    });
                }
            }

            // Buy on ex2 (ask), sell on ex1 (bid)
            if q1.bid > q2.ask {
                let spread_abs = q1.bid - q2.ask;
                let spread_pct = (spread_abs / q2.ask) * Decimal::from(100);

                SPREAD_ABSOLUTE
                    .with_label_values(&[symbol, &ex1.to_string(), &ex2.to_string()])
                    .set(spread_abs.to_string().parse().unwrap_or(0.0));
                SPREAD_PERCENT
                    .with_label_values(&[symbol, &ex1.to_string(), &ex2.to_string()])
                    .set(spread_pct.to_string().parse().unwrap_or(0.0));
                SPREADS_CALCULATED.inc();

                debug!(
                    symbol, buy_at = %ex2, sell_at = %ex1, spread_pct = %spread_pct,
                    "Arbitrage opportunity detected"
                );

                if spread_pct >= publish_min_pct {
                    opportunities.push(SpreadOpportunity {
                        symbol: symbol.to_string(),
                        long_exchange: *ex2,
                        long_exchange_price: q2.ask,
                        short_exchange: *ex1,
                        short_exchange_price: q1.bid,
                        spread_percent: spread_pct,
                        size: Decimal::ZERO,
                        timestamp: ts,
                    });
                }
            }
        }
    }

    opportunities
}

fn update_best_prices(symbol: &str, quotes: &HashMap<Exchange, QuoteData>) {
    for (exchange, quote) in quotes {
        let ex = exchange.to_string();
        BEST_BID.with_label_values(&[symbol, &ex]).set(quote.bid.to_string().parse().unwrap_or(0.0));
        BEST_ASK.with_label_values(&[symbol, &ex]).set(quote.ask.to_string().parse().unwrap_or(0.0));
        QUOTE_EVENT_TIMESTAMP_MS.with_label_values(&[symbol, &ex]).set(quote.timestamp as f64);
        QUOTE_RECEIVED_TIMESTAMP_MS.with_label_values(&[symbol, &ex]).set(quote.received_at as f64);
    }
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
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

/// Reads spread opportunities from a channel and publishes them to Redis Streams.
async fn spread_publisher_worker(
    redis_url: String,
    mut rx: mpsc::Receiver<SpreadOpportunity>,
) {
    let redis_client = common::RedisClient::from_url(&redis_url);

    let mut conn = match redis_client.get_connection().await {
        Ok(c) => c,
        Err(e) => {
            error!("Spread publisher: failed to connect to Redis: {}", e);
            return;
        }
    };

    info!("Spread publisher worker started");

    while let Some(opp) = rx.recv().await {
        match redis_client.publish_spread(&mut conn, &opp).await {
            Ok(_) => {
                SPREADS_PUBLISHED.inc();
                debug!(
                    symbol = %opp.symbol,
                    spread_pct = %opp.spread_percent,
                    "Published spread opportunity"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to publish spread, reconnecting...");
                match redis_client.get_connection().await {
                    Ok(new_conn) => conn = new_conn,
                    Err(e) => error!("Spread publisher: reconnect failed: {}", e),
                }
            }
        }
    }
}

const GROUP: &str = "spread-calculator";
const CONSUMER: &str = "consumer-1";

async fn consume_streams(
    redis_url: &str,
    symbols: Vec<String>,
    store: QuoteStore,
    spread_tx: mpsc::Sender<SpreadOpportunity>,
    publish_min_pct: Decimal,
    max_quote_age_ms: u64,
) -> Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    let stream_keys: Vec<String> = symbols.iter().map(|s| format!("prices:{}", s)).collect();

    // Create consumer groups starting at $ (skip existing backlog entirely)
    for key in &stream_keys {
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE").arg(key).arg(GROUP).arg("$").arg("MKSTREAM")
            .query_async(&mut conn).await;
        match result {
            Ok(_) => info!(stream = %key, "Consumer group created"),
            Err(e) if e.to_string().contains("BUSYGROUP") => {
                debug!(stream = %key, "Consumer group already exists");
            }
            Err(e) => return Err(anyhow::anyhow!("XGROUP CREATE {}: {}", key, e)),
        }
    }

    // Flush any pending messages left from a previous crashed session — ACK without processing
    let drain_opts = StreamReadOptions::default().group(GROUP, CONSUMER).count(10_000);
    let zero_ids: Vec<&str> = vec!["0"; stream_keys.len()];
    loop {
        let pending: StreamReadReply = conn.xread_options(&stream_keys, &zero_ids, &drain_opts).await?;
        let mut flushed = 0usize;
        for stream in &pending.keys {
            let ids: Vec<&str> = stream.ids.iter().map(|e| e.id.as_str()).collect();
            if !ids.is_empty() {
                let n: i64 = conn.xack(&stream.key, GROUP, &ids).await?;
                flushed += n as usize;
            }
        }
        if flushed == 0 { break; }
        info!(flushed, "Flushed stale pending messages from previous session");
    }

    info!(streams = ?stream_keys, "Starting consumer group read loop");

    // Large COUNT so a single read drains any burst backlog in one shot
    let read_opts = StreamReadOptions::default().group(GROUP, CONSUMER).block(50).count(10_000);
    let new_ids: Vec<&str> = vec![">"; stream_keys.len()];

    loop {
        let result: redis::RedisResult<StreamReadReply> = conn
            .xread_options(&stream_keys, &new_ids, &read_opts)
            .await;

        match result {
            Ok(reply) => {
                let mut symbols_updated: HashSet<String> = HashSet::new();

                for stream_key in reply.keys {
                    let symbol = stream_key
                        .key
                        .strip_prefix("prices:")
                        .unwrap_or(&stream_key.key)
                        .to_string();

                    let mut ids_to_ack: Vec<String> = Vec::with_capacity(stream_key.ids.len());
                    // Last entry per exchange wins — stream entries are ordered oldest→newest
                    let mut latest: HashMap<Exchange, QuoteData> = HashMap::new();

                    for entry in stream_key.ids {
                        ids_to_ack.push(entry.id.clone());
                        let fields: Vec<(String, redis::Value)> = entry.map.into_iter().collect();
                        if let Some(se) = parse_stream_entry(&fields) {
                            if let Some(exchange) = parse_exchange(&se.exchange) {
                                let bid: Decimal = se.bid.parse().unwrap_or_default();
                                let ask: Decimal = se.ask.parse().unwrap_or_default();
                                let timestamp: u64 = se.timestamp.parse().unwrap_or(0);
                                let received_at: u64 = se.received_at.parse().unwrap_or(0);
                                latest.insert(exchange, QuoteData { bid, ask, timestamp, received_at });
                            }
                        }
                    }

                    // ACK everything — processed or skipped, we never want to re-read old data
                    if !ids_to_ack.is_empty() {
                        let _: redis::RedisResult<i64> = conn.xack(&stream_key.key, GROUP, &ids_to_ack).await;
                        EVENTS_PROCESSED.inc_by(ids_to_ack.len() as u64);
                    }

                    if !latest.is_empty() {
                        let mut store_guard = store.write().await;
                        let sym_quotes = store_guard.entry(symbol.clone()).or_default();
                        for (exchange, quote) in latest {
                            sym_quotes.insert(exchange, quote);
                        }
                        symbols_updated.insert(symbol);
                    }
                }

                // Calculate spreads once per symbol after the whole batch is applied
                for symbol in symbols_updated {
                    let opportunities = {
                        let store_guard = store.read().await;
                        if let Some(quotes) = store_guard.get(&symbol) {
                            update_best_prices(&symbol, quotes);
                            calculate_spreads(&symbol, quotes, publish_min_pct, max_quote_age_ms)
                        } else {
                            vec![]
                        }
                    };
                    for opp in opportunities {
                        if spread_tx.try_send(opp).is_err() {
                            warn!("Spread publish channel full, dropping opportunity");
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Error reading from streams, retrying...");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("spread_calculator=info".parse().unwrap()),
        )
        .init();

    register_metrics();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://redis:6379/".into());

    let metrics_port: u16 = std::env::var("METRICS_PORT")
        .unwrap_or_else(|_| "9092".into())
        .parse()
        .unwrap_or(9092);

    let symbols: Vec<String> = std::env::var("SYMBOLS")
        .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Minimum spread (%) to publish to Redis for the execution engine
    let publish_min_pct: Decimal = std::env::var("PUBLISH_MIN_SPREAD_PCT")
        .unwrap_or_else(|_| "0.05".into())
        .parse()
        .unwrap_or(Decimal::new(5, 2));

    // Max age of an exchange quote before it is considered stale (ms)
    let max_quote_age_ms: u64 = std::env::var("MAX_QUOTE_AGE_MS")
        .unwrap_or_else(|_| "300".into())
        .parse()
        .unwrap_or(300);

    info!(
        redis_url = %redis_url,
        metrics_port,
        symbols = ?symbols,
        publish_min_pct = %publish_min_pct,
        max_quote_age_ms,
        "Starting spread calculator"
    );

    let store: QuoteStore = Arc::new(RwLock::new(HashMap::new()));

    let (spread_tx, spread_rx) = mpsc::channel::<SpreadOpportunity>(256);

    tokio::spawn(start_metrics_server(metrics_port));
    tokio::spawn(spread_publisher_worker(redis_url.clone(), spread_rx));

    consume_streams(&redis_url, symbols, store, spread_tx, publish_min_pct, max_quote_age_ms).await
}
