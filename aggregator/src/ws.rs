use common::models::price;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;

#[derive(Deserialize, Debug)]
struct BinanceTicker {
    b: String,
    a: String,
}

#[derive(Deserialize, Debug)]
struct HibachiResponse {
    data: HibachiTicker,
}

#[derive(Deserialize, Debug)]
struct HibachiTicker {
    bidPrice: String,
    askPrice: String,
}

#[derive(Deserialize, Debug)]
struct BackpackResponse {
    data: BackpackTicker,
}

#[derive(Deserialize, Debug)]
struct BackpackTicker {
    b: String,
    a: String,
}

pub async fn binance_ws(quotes: Arc<RwLock<HashMap<String, price::Quote>>>) {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@ticker";
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to Binance WS");
    let mut read = ws_stream;

    while let Some(msg) = read.next().await {
        if let Ok(msg) = msg {
            if msg.is_text() {
                if let Ok(data) = serde_json::from_str::<BinanceTicker>(&msg.to_text().unwrap()) {
                    let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
                    let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
                    let quote = price::Quote {
                        exchange: "Binance".into(),
                        symbol: "BTC".into(),
                        bid,
                        ask,
                    };

                    quotes.write().await.insert("Binance".into(), quote);
                }
            }
        }
    }
}

pub async fn backpack_ws(quotes: Arc<RwLock<HashMap<String, price::Quote>>>) {
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
                    serde_json::from_str::<BackpackResponse>(&msg.to_text().unwrap())
                {
                    let data = wrapper.data;
                    let bid = Decimal::from_str_exact(&data.b).unwrap_or(Decimal::ZERO);
                    let ask = Decimal::from_str_exact(&data.a).unwrap_or(Decimal::ZERO);
                    let quote = price::Quote {
                        exchange: "Backpack".into(),
                        symbol: "BTC".into(),
                        bid,
                        ask,
                    };

                    quotes.write().await.insert("Backpack".into(), quote);
                }
            }
        }
    }
}

pub async fn hibachi_ws(quotes: Arc<RwLock<HashMap<String, price::Quote>>>) {
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
                    serde_json::from_str::<HibachiResponse>(&msg.to_text().unwrap())
                {
                    let data = wrapper.data;
                    let bid = Decimal::from_str_exact(&data.askPrice).unwrap_or(Decimal::ZERO);
                    let ask = Decimal::from_str_exact(&data.bidPrice).unwrap_or(Decimal::ZERO);
                    let quote = price::Quote {
                        exchange: "Hibachi".into(),
                        symbol: "BTC".into(),
                        bid,
                        ask,
                    };

                    quotes.write().await.insert("Hibachi".into(), quote);
                }
            }
        }
    }
}
