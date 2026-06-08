use anyhow::Result;
use axum::{routing::get, Router};
use common::Exchange;
use lazy_static::lazy_static;
use prometheus::{Encoder, GaugeVec, IntCounter, Opts, Registry, TextEncoder};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    /// Spread между биржами в процентах
    /// Labels: symbol, exchange_high (где дороже), exchange_low (где дешевле)
    static ref SPREAD_PERCENT: GaugeVec = GaugeVec::new(
        Opts::new("spread_percent", "Spread between exchanges in percent"),
        &["symbol", "exchange_high", "exchange_low"]
    ).unwrap();

    /// Абсолютный spread в единицах цены
    static ref SPREAD_ABSOLUTE: GaugeVec = GaugeVec::new(
        Opts::new("spread_absolute", "Absolute spread between exchanges"),
        &["symbol", "exchange_high", "exchange_low"]
    ).unwrap();

    /// Лучший bid по символу
    static ref BEST_BID: GaugeVec = GaugeVec::new(
        Opts::new("best_bid", "Best bid price across exchanges"),
        &["symbol", "exchange"]
    ).unwrap();

    /// Лучший ask по символу
    static ref BEST_ASK: GaugeVec = GaugeVec::new(
        Opts::new("best_ask", "Best ask price across exchanges"),
        &["symbol", "exchange"]
    ).unwrap();

    /// Количество обработанных событий
    static ref EVENTS_PROCESSED: IntCounter = IntCounter::new(
        "spread_events_processed_total",
        "Total number of price events processed"
    ).unwrap();

    /// Количество рассчитанных спредов
    static ref SPREADS_CALCULATED: IntCounter = IntCounter::new(
        "spreads_calculated_total",
        "Total number of spreads calculated"
    ).unwrap();
}

fn register_metrics() {
    REGISTRY.register(Box::new(SPREAD_PERCENT.clone())).ok();
    REGISTRY.register(Box::new(SPREAD_ABSOLUTE.clone())).ok();
    REGISTRY.register(Box::new(BEST_BID.clone())).ok();
    REGISTRY.register(Box::new(BEST_ASK.clone())).ok();
    REGISTRY.register(Box::new(EVENTS_PROCESSED.clone())).ok();
    REGISTRY.register(Box::new(SPREADS_CALCULATED.clone())).ok();
}

#[derive(Debug, Clone)]
struct QuoteData {
    bid: Decimal,
    ask: Decimal,
    timestamp: i64,
}

/// Хранилище последних котировок: symbol -> exchange -> QuoteData
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

/// Рассчитывает спреды между биржами для одного символа
fn calculate_spreads(symbol: &str, quotes: &HashMap<Exchange, QuoteData>) {
    if quotes.len() < 2 {
        return;
    }

    let exchanges: Vec<_> = quotes.iter().collect();

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let (ex1, q1) = exchanges[i];
            let (ex2, q2) = exchanges[j];

            // Ищем арбитражную возможность:
            // Можно купить на одной бирже (ask) и продать на другой (bid)

            // Вариант 1: покупаем на ex1 (ask), продаём на ex2 (bid)
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
                    symbol = symbol,
                    buy_at = %ex1,
                    sell_at = %ex2,
                    spread_pct = %spread_pct,
                    "Arbitrage opportunity detected"
                );
            }

            // Вариант 2: покупаем на ex2 (ask), продаём на ex1 (bid)
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
                    symbol = symbol,
                    buy_at = %ex2,
                    sell_at = %ex1,
                    spread_pct = %spread_pct,
                    "Arbitrage opportunity detected"
                );
            }
        }
    }
}

/// Обновляет метрики лучших bid/ask
fn update_best_prices(symbol: &str, quotes: &HashMap<Exchange, QuoteData>) {
    for (exchange, quote) in quotes {
        BEST_BID
            .with_label_values(&[symbol, &exchange.to_string()])
            .set(quote.bid.to_string().parse().unwrap_or(0.0));
        BEST_ASK
            .with_label_values(&[symbol, &exchange.to_string()])
            .set(quote.ask.to_string().parse().unwrap_or(0.0));
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

    info!(port = port, "Starting metrics server");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn consume_streams(
    redis_url: &str,
    symbols: Vec<String>,
    store: QuoteStore,
) -> Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut conn = client.get_multiplexed_tokio_connection().await?;

    // Собираем ключи стримов
    let stream_keys: Vec<String> = symbols
        .iter()
        .map(|s| format!("prices:{}", s))
        .collect();

    // Начинаем читать с последних записей ($ = только новые)
    let mut last_ids: Vec<String> = vec!["$".to_string(); stream_keys.len()];

    info!(streams = ?stream_keys, "Starting to consume streams");

    loop {
        let opts = StreamReadOptions::default()
            .block(1000) // Блокируемся на 1 секунду
            .count(100); // Читаем до 100 записей за раз

        let keys_with_ids: Vec<(&str, &str)> = stream_keys
            .iter()
            .zip(last_ids.iter())
            .map(|(k, id)| (k.as_str(), id.as_str()))
            .collect();

        let result: redis::RedisResult<StreamReadReply> = conn
            .xread_options(&stream_keys, &last_ids, &opts)
            .await;

        match result {
            Ok(reply) => {
                for stream_key in reply.keys {
                    // Извлекаем symbol из ключа "prices:BTCUSDT" -> "BTCUSDT"
                    let symbol = stream_key
                        .key
                        .strip_prefix("prices:")
                        .unwrap_or(&stream_key.key)
                        .to_string();

                    // Находим индекс для обновления last_id
                    let key_idx = stream_keys
                        .iter()
                        .position(|k| k == &stream_key.key);

                    for entry in stream_key.ids {
                        // Обновляем last_id
                        if let Some(idx) = key_idx {
                            last_ids[idx] = entry.id.clone();
                        }

                        // Парсим поля
                        let fields: Vec<(String, redis::Value)> = entry
                            .map
                            .into_iter()
                            .collect();

                        if let Some(stream_entry) = parse_stream_entry(&fields) {
                            if let Some(exchange) = parse_exchange(&stream_entry.exchange) {
                                let bid: Decimal = stream_entry.bid.parse().unwrap_or_default();
                                let ask: Decimal = stream_entry.ask.parse().unwrap_or_default();
                                let timestamp: i64 = stream_entry.timestamp.parse().unwrap_or(0);

                                let quote_data = QuoteData { bid, ask, timestamp };

                                // Обновляем хранилище
                                {
                                    let mut store_guard = store.write().await;
                                    store_guard
                                        .entry(symbol.clone())
                                        .or_default()
                                        .insert(exchange, quote_data);
                                }

                                EVENTS_PROCESSED.inc();

                                // Рассчитываем спреды
                                {
                                    let store_guard = store.read().await;
                                    if let Some(quotes) = store_guard.get(&symbol) {
                                        update_best_prices(&symbol, quotes);
                                        calculate_spreads(&symbol, quotes);
                                    }
                                }
                            }
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

    // Символы для мониторинга (можно расширить через env)
    let symbols: Vec<String> = std::env::var("SYMBOLS")
        .unwrap_or_else(|_| "BTCUSDT,ETHUSDT".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    info!(
        redis_url = %redis_url,
        metrics_port = metrics_port,
        symbols = ?symbols,
        "Starting spread calculator"
    );

    let store: QuoteStore = Arc::new(RwLock::new(HashMap::new()));

    // Запускаем metrics server в отдельной задаче
    tokio::spawn(start_metrics_server(metrics_port));

    // Основной цикл потребления
    consume_streams(&redis_url, symbols, store).await
}
