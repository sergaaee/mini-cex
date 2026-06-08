use crate::metrics::{update_price_metrics, WS_MESSAGES_RECEIVED};
use crate::models::{self, Quotes};
use crate::publisher::{PriceEvent, PublisherHandle};
use crate::utils::create_subscribe_message;
use common::Exchange;
use common::models::symbol;
use common::models::ticker;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tracing::{debug, error, info};

/// Универсальная функция запуска WS для любой биржи
pub async fn start_ws(
    quotes: Quotes,
    publisher: PublisherHandle,
    exchange: Exchange,
    symbols: Vec<String>,
    ws_url_fn: fn(&String) -> String, // функция, которая возвращает URL для конкретной монеты
    parse_fn: fn(&str) -> Option<ticker::Quote>, // функция парсинга сообщения в Quote
) {
    let mut handles = vec![];

    for sym in symbols {
        sleep(Duration::from_millis(500));
        info!("Starting WS for {} on {}", sym, exchange.to_string());
        let quotes_clone = Arc::clone(&quotes);
        let publisher_clone = publisher.clone();
        let sym_clone = sym.clone();

        let handle = tokio::spawn(async move {
            let url = ws_url_fn(&sym);
            let mut request = url
                .clone()
                .into_client_request()
                .expect("Couldn't parse url");
            request.headers_mut().insert(
                "User-Agent",
                "Mozilla/5.0 (compatible; aggregator-bot/1.0)"
                    .parse()
                    .unwrap(),
            );
            request.headers_mut().insert("Origin", url.parse().unwrap());

            let (mut ws_stream, _) = connect_async(request).await.expect("Failed to connect WS");

            if let Some(subscribe) = create_subscribe_message(exchange, &sym) {
                ws_stream
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        subscribe.to_string().into(),
                    ))
                    .await
                    .unwrap();
            }

            while let Some(msg) = ws_stream.next().await {
                if let Ok(msg) = msg {
                    if msg.is_text() {
                        if let Some(quote) = parse_fn(msg.to_text().unwrap()) {
                            // Обновляем Prometheus метрики
                            let exchange_str = exchange.to_string();
                            WS_MESSAGES_RECEIVED.with_label_values(&[&exchange_str]).inc();
                            update_price_metrics(
                                &sym_clone,
                                &exchange_str,
                                quote.bid.try_into().unwrap_or(0.0),
                                quote.ask.try_into().unwrap_or(0.0),
                                quote.mid.try_into().unwrap_or(0.0),
                            );

                            // Публикуем в Redis Stream (non-blocking)
                            publisher_clone.publish(PriceEvent {
                                symbol: sym_clone.clone(),
                                exchange,
                                quote: quote.clone(),
                            });

                            // Обновляем in-memory quotes
                            quotes_clone
                                .write()
                                .await
                                .entry(sym.to_string())
                                .or_default()
                                .insert(exchange, quote);
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    for h in handles {
        let _ = h;
    }
}

/// Пример функции для генерации URL Binance
pub fn binance_url(sym: &String) -> String {
    format!(
        "wss://stream.binance.com:9443/ws/{}usdt@ticker",
        sym.to_lowercase()
    )
}

pub fn aster_url(sym: &String) -> String {
    format!(
        "wss://fstream.asterdex.com/ws/{}usdt@bookTicker",
        sym.to_lowercase()
    )
}

pub fn backpack_url(_: &String) -> String {
    "wss://ws.backpack.exchange".to_string()
}

pub fn hibachi_url(_: &String) -> String {
    "wss://data-api.hibachi.xyz/ws/market".to_string()
}

pub fn bybit_url(_: &String) -> String {
    "wss://stream.bybit.com/v5/public/linear".to_string()
}

pub fn blofin_url(_: &String) -> String {
    "wss://openapi.blofin.com/ws/public".to_string()
}

pub fn okx_url(_: &String) -> String {
    "wss://ws.okx.com:8443/ws/v5/public".to_string()
}

/// Пример функции парсинга Binance
pub fn parse_binance(msg: &str) -> Option<ticker::Quote> {
    if let Ok(data) = serde_json::from_str::<models::BinanceTicker>(msg) {
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (bid + ask) / Decimal::TWO;
        let timestamp = data.timestamp / 1_000;

        Some(ticker::Quote {
            bid,
            bid_size,
            ask,
            ask_size,
            mid,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_aster(msg: &str) -> Option<ticker::Quote> {
    if let Ok(data) = serde_json::from_str::<models::AsterTicker>(msg) {
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (bid + ask) / Decimal::TWO;
        let timestamp = data.timestamp / 1_000;

        Some(ticker::Quote {
            bid,
            bid_size,
            ask,
            ask_size,
            mid,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_backpack(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::BackpackResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (bid + ask) / Decimal::TWO;
        let timestamp = data.timestamp / 1_000;

        Some(ticker::Quote {
            bid,
            bid_size,
            ask,
            ask_size,
            mid,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_hibachi(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::HibachiResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (bid + ask) / Decimal::TWO;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Some(ticker::Quote {
            bid,
            bid_size,
            ask,
            ask_size,
            mid,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_bybit(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::BybitResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = wrapper.ts;

        Some(ticker::Quote {
            mid,
            bid,
            bid_size,
            ask,
            ask_size,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_blofin(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::BloFinResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = data.ts;

        Some(ticker::Quote {
            mid,
            bid,
            bid_size,
            ask,
            ask_size,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_okx(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::OKXResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.best_bid).unwrap_or(Decimal::ZERO);
        let bid_size = Decimal::from_str_exact(&data.best_bid_size).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.best_ask).unwrap_or(Decimal::ZERO);
        let ask_size = Decimal::from_str_exact(&data.best_ask_size).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = data.ts;

        Some(ticker::Quote {
            mid,
            bid,
            bid_size,
            ask,
            ask_size,
            timestamp,
        })
    } else {
        None
    }
}

/// Запуск Binance WS для нескольких монет
pub async fn run_binance(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::Binance;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        publisher,
        exchange,
        supported_symbols,
        binance_url,
        parse_binance,
    )
    .await;
}

pub async fn run_aster(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::Aster;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(quotes, publisher, exchange, supported_symbols, aster_url, parse_aster).await;
}

pub async fn run_backpack(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::Backpack;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        publisher,
        exchange,
        supported_symbols,
        backpack_url,
        parse_backpack,
    )
    .await;
}

pub async fn run_hibachi(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::Hibachi;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        publisher,
        exchange,
        supported_symbols,
        hibachi_url,
        parse_hibachi,
    )
    .await;
}

pub async fn run_bybit(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::Bybit;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(quotes, publisher, exchange, supported_symbols, bybit_url, parse_bybit).await;
}

pub async fn run_blofin(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::BloFin;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        publisher,
        exchange,
        supported_symbols,
        blofin_url,
        parse_blofin,
    )
    .await;
}

pub async fn run_okx(quotes: Quotes, publisher: PublisherHandle) {
    let exchange = Exchange::OKX;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(quotes, publisher, exchange, supported_symbols, okx_url, parse_okx).await;
}
