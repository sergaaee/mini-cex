use crate::models;
use crate::models::Quotes;
use common::Exchange;
use common::models::ticker;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::connect_async;

pub async fn binance_ws(quotes: Quotes) {
    let exchange = Exchange::Binance;
    let url = "wss://stream.binance.com:9443/ws/btcusdt@ticker";
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to Binance WS");
    let mut read = ws_stream;

    while let Some(msg) = read.next().await {
        if let Ok(msg) = msg {
            if msg.is_text() {
                if let Ok(data) =
                    serde_json::from_str::<models::BinanceTicker>(msg.to_text().unwrap())
                {
                    let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
                    let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
                    let mid = (ask + bid) / Decimal::TWO;
                    let event_time_ms: u64 = data.E;
                    let timestamp = event_time_ms / 1_000;

                    let quote = ticker::Quote {
                        mid,
                        bid,
                        ask,
                        timestamp,
                    };

                    quotes
                        .write()
                        .await
                        .entry("BTC".into())
                        .or_default()
                        .insert(exchange.clone(), quote);
                }
            }
        }
    }
}

pub async fn backpack_ws(quotes: Quotes) {
    let exchange = Exchange::Backpack;
    let url = "wss://ws.backpack.exchange";
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let subscribe = json!({ "method": "SUBSCRIBE", "params": ["bookTicker.BTC_USDC_PERP"] });
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            subscribe.to_string().into(),
        ))
        .await
        .unwrap();

    while let Some(msg) = ws_stream.next().await {
        if let Ok(msg) = msg {
            if msg.is_text() {
                if let Ok(wrapper) =
                    serde_json::from_str::<models::BackpackResponse>(msg.to_text().unwrap())
                {
                    let data = wrapper.data;
                    let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
                    let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
                    let event_time_us: u64 = data.E;
                    let timestamp = event_time_us / 1_000_000;
                    let mid = (ask + bid) / Decimal::TWO;

                    let quote = ticker::Quote {
                        mid,
                        bid,
                        ask,
                        timestamp,
                    };

                    quotes
                        .write()
                        .await
                        .entry("BTC".into())
                        .or_default()
                        .insert(exchange.clone(), quote);
                }
            }
        }
    }
}

pub async fn hibachi_ws(quotes: Quotes) {
    let exchange = Exchange::Hibachi;
    let url = "wss://data-api.hibachi.xyz/ws/market";
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let subscribe = json!({
        "method": "subscribe",
        "parameters": { "subscriptions": [{"symbol": "BTC/USDT-P", "topic": "ask_bid_price"}] }
    });
    ws_stream
        .send(tokio_tungstenite::tungstenite::Message::Text(
            subscribe.to_string().into(),
        ))
        .await
        .unwrap();

    while let Some(msg) = ws_stream.next().await {
        if let Ok(msg) = msg {
            if msg.is_text() {
                if let Ok(wrapper) =
                    serde_json::from_str::<models::HibachiResponse>(msg.to_text().unwrap())
                {
                    let data = wrapper.data;
                    let bid = Decimal::from_str_exact(&data.askPrice).unwrap_or(Decimal::ZERO);
                    let ask = Decimal::from_str_exact(&data.bidPrice).unwrap_or(Decimal::ZERO);
                    let mid = (ask + bid) / Decimal::TWO;
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    let quote = ticker::Quote {
                        mid,
                        bid,
                        ask,
                        timestamp,
                    };

                    quotes
                        .write()
                        .await
                        .entry("BTC".into())
                        .or_default()
                        .insert(exchange.clone(), quote);
                }
            }
        }
    }
}
