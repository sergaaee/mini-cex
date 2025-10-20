use crate::models::{self, Quotes};
use crate::utils::create_subscribe_message;
use common::Exchange;
use common::models::symbol;
use common::models::ticker;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

/// Универсальная функция запуска WS для любой биржи
pub async fn start_ws(
    quotes: Quotes,
    exchange: Exchange,
    symbols: Vec<String>,
    ws_url_fn: fn(String) -> String, // функция, которая возвращает URL для конкретной монеты
    parse_fn: fn(&str) -> Option<ticker::Quote>, // функция парсинга сообщения в Quote
) {
    let mut handles = vec![];

    for sym in symbols {
        let quotes_clone = Arc::clone(&quotes);
        let sym_string = sym.to_string();
        let sym_clone = sym.clone();

        let handle = tokio::spawn(async move {
            let url = ws_url_fn(sym);
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

            if let Some(subscribe) = create_subscribe_message(exchange, &sym_clone) {
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
                            quotes_clone
                                .write()
                                .await
                                .entry(sym_string.clone())
                                .or_default()
                                .insert(exchange.clone(), quote);
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }
}

/// Пример функции для генерации URL Binance
pub fn binance_url(sym: String) -> String {
    format!(
        "wss://stream.binance.com:9443/ws/{}usdt@ticker",
        sym.to_lowercase()
    )
}

pub fn aster_url(sym: String) -> String {
    format!(
        "wss://fstream.asterdex.com/ws/{}usdt@bookTicker",
        sym.to_lowercase()
    )
}

pub fn backpack_url(_: String) -> String {
    "wss://ws.backpack.exchange".to_string()
}

pub fn hibachi_url(_: String) -> String {
    "wss://data-api.hibachi.xyz/ws/market".to_string()
}

pub fn bybit_url(_: String) -> String {
    "wss://stream.bybit.com/v5/public/linear".to_string()
}

pub fn blofin_url(_: String) -> String {
    "wss://openapi.blofin.com/ws/public".to_string()
}

pub fn okx_url(_: String) -> String {
    "wss://ws.okx.com:8443/ws/v5/public".to_string()
}

/// Пример функции парсинга Binance
pub fn parse_binance(msg: &str) -> Option<ticker::Quote> {
    if let Ok(data) = serde_json::from_str::<models::BinanceTicker>(msg) {
        let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
        let mid = (bid + ask) / Decimal::TWO;
        let timestamp = data.E / 1_000;

        Some(ticker::Quote {
            bid,
            ask,
            mid,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_aster(msg: &str) -> Option<ticker::Quote> {
    if let Ok(data) = serde_json::from_str::<models::AsterTicker>(msg) {
        let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
        let mid = (bid + ask) / Decimal::TWO;
        let timestamp = data.E;

        Some(ticker::Quote {
            bid,
            ask,
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
        let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
        let event_time_us: u64 = data.E;
        let timestamp = event_time_us / 1_000;
        let mid = (ask + bid) / Decimal::TWO;

        Some(ticker::Quote {
            mid,
            bid,
            ask,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_hibachi(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::HibachiResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.askPrice).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.bidPrice).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Some(ticker::Quote {
            mid,
            bid,
            ask,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_bybit(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::BybitResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.ask1Price).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.bid1Price).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = wrapper.ts;

        Some(ticker::Quote {
            mid,
            bid,
            ask,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_blofin(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::BloFinResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.askPrice).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.bidPrice).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = data.ts;

        Some(ticker::Quote {
            mid,
            bid,
            ask,
            timestamp,
        })
    } else {
        None
    }
}

pub fn parse_okx(msg: &str) -> Option<ticker::Quote> {
    if let Ok(wrapper) = serde_json::from_str::<models::OKXResponse>(msg) {
        let data = wrapper.data;
        let bid = Decimal::from_str_exact(&data.askPx).unwrap_or(Decimal::ZERO);
        let ask = Decimal::from_str_exact(&data.bidPx).unwrap_or(Decimal::ZERO);
        let mid = (ask + bid) / Decimal::TWO;
        let timestamp = data.ts;

        Some(ticker::Quote {
            mid,
            bid,
            ask,
            timestamp,
        })
    } else {
        None
    }
}

/// Запуск Binance WS для нескольких монет
pub async fn run_binance(quotes: Quotes) {
    let exchange = Exchange::Binance;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        exchange,
        supported_symbols,
        binance_url,
        parse_binance,
    )
    .await;
}

pub async fn run_aster(quotes: Quotes) {
    let exchange = Exchange::Aster;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(quotes, exchange, supported_symbols, aster_url, parse_aster).await;
}

pub async fn run_backpack(quotes: Quotes) {
    let exchange = Exchange::Backpack;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        exchange,
        supported_symbols,
        backpack_url,
        parse_backpack,
    )
    .await;
}

pub async fn run_hibachi(quotes: Quotes) {
    let exchange = Exchange::Hibachi;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        exchange,
        supported_symbols,
        hibachi_url,
        parse_hibachi,
    )
    .await;
}

pub async fn run_bybit(quotes: Quotes) {
    let exchange = Exchange::Bybit;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(quotes, exchange, supported_symbols, bybit_url, parse_bybit).await;
}

pub async fn run_blofin(quotes: Quotes) {
    let exchange = Exchange::BloFin;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(
        quotes,
        exchange,
        supported_symbols,
        blofin_url,
        parse_blofin,
    )
    .await;
}

pub async fn run_okx(quotes: Quotes) {
    let exchange = Exchange::OKX;
    let supported_symbols = symbol::Symbol::supported_by(exchange)
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    start_ws(quotes, exchange, supported_symbols, okx_url, parse_okx).await;
}
