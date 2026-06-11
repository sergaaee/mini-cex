use crate::tracker::{FillLeg, FillTracker};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Debug, Deserialize)]
struct OrderUpdate {
    #[serde(rename = "i")]
    order_id: i64,
    #[serde(rename = "ap")]
    avg_price: String,
    #[serde(rename = "z")]
    executed_qty: String,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "s")]
    symbol: String,
}

#[derive(Debug, Deserialize)]
struct OrderTradeUpdate {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "o")]
    order: OrderUpdate,
}

async fn get_listen_key(api_key: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .post("https://fapi.binance.com/fapi/v1/listenKey")
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?
        .json::<ListenKeyResponse>()
        .await?;
    Ok(resp.listen_key)
}

async fn keepalive_listen_key(api_key: &str) {
    let client = reqwest::Client::new();
    let _ = client
        .put("https://fapi.binance.com/fapi/v1/listenKey")
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await;
}

pub async fn run(tracker: Arc<Mutex<FillTracker>>, api_key: String) {
    loop {
        match run_once(&tracker, &api_key).await {
            Ok(_) => warn!("Binance WS stream ended, reconnecting..."),
            Err(e) => error!("Binance WS error: {}, reconnecting...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_once(tracker: &Arc<Mutex<FillTracker>>, api_key: &str) -> anyhow::Result<()> {
    let listen_key = get_listen_key(api_key).await?;
    info!(listen_key = %&listen_key[..8], "Got Binance listenKey, connecting WS");

    let url = format!("wss://fstream.binance.com/private/ws/{}", listen_key);
    let request = url.clone().into_client_request()?;
    let (mut ws_stream, _) = connect_async(request).await?;
    info!("Connected to Binance user stream");

    // Keepalive: PUT /fapi/v1/listenKey every 30 minutes
    let api_key_clone = api_key.to_string();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(30 * 60));
        ticker.tick().await; // skip first immediate tick
        loop {
            ticker.tick().await;
            keepalive_listen_key(&api_key_clone).await;
        }
    });

    while let Some(msg) = ws_stream.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            if let Ok(update) = serde_json::from_str::<OrderTradeUpdate>(&text) {
                if update.event_type == "ORDER_TRADE_UPDATE" && update.order.status == "FILLED" {
                    let order_id = update.order.order_id.to_string();
                    let avg_price =
                        Decimal::from_str(&update.order.avg_price).unwrap_or(Decimal::ZERO);
                    let filled_qty =
                        Decimal::from_str(&update.order.executed_qty).unwrap_or(Decimal::ZERO);

                    info!(
                        order_id = %order_id,
                        symbol = %update.order.symbol,
                        avg_price = %avg_price,
                        filled_qty = %filled_qty,
                        "Binance fill received"
                    );

                    tracker.lock().unwrap().record_fill(
                        &order_id,
                        FillLeg { avg_price, filled_qty },
                    );
                }
            }
        }
    }

    Ok(())
}
