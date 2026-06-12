use crate::tracker::{FillLeg, FillTracker};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
struct StreamStartResult {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Debug, Deserialize)]
struct StreamStartResponse {
    id: u64,
    result: StreamStartResult,
    status: u32,
}

#[derive(Debug, Deserialize)]
struct OrderClosedData {
    #[serde(rename = "orderId")]
    order_id: serde_json::Value, // can be int or string
    #[serde(rename = "avgFillPrice")]
    avg_fill_price: Option<String>,
    #[serde(rename = "filledQuantity")]
    filled_quantity: Option<String>,
    status: Option<String>,
    symbol: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HibachiEvent {
    event: String,
    data: serde_json::Value,
}

pub async fn run(tracker: Arc<Mutex<FillTracker>>, account_id: u64, api_key: String) {
    loop {
        match run_once(&tracker, account_id, &api_key).await {
            Ok(_) => warn!("Hibachi WS stream ended, reconnecting..."),
            Err(e) => error!("Hibachi WS error: {}, reconnecting...", e),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_once(
    tracker: &Arc<Mutex<FillTracker>>,
    account_id: u64,
    api_key: &str,
) -> anyhow::Result<()> {
    let url = format!(
        "wss://api.hibachi.xyz/ws/account?accountId={}",
        account_id
    );

    let mut request = url.clone().into_client_request()?;
    request
        .headers_mut()
        .insert("Authorization", api_key.parse()?);

    let (mut ws_stream, _) = connect_async(request).await?;
    info!("Connected to Hibachi account stream");

    // Send stream.start
    let start_msg = json!({
        "id": 1,
        "method": "stream.start",
        "params": { "accountId": account_id }
    });
    ws_stream
        .send(Message::Text(start_msg.to_string().into()))
        .await?;

    // Wait for stream.start response to get listenKey
    let listen_key = loop {
        match ws_stream.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(resp) = serde_json::from_str::<StreamStartResponse>(&text) {
                    if resp.status == 200 {
                        break resp.result.listen_key;
                    }
                }
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("WS closed before stream.start response"),
        }
    };

    info!(listen_key = %&listen_key, "Hibachi stream started");

    // Keepalive: send stream.ping every 10 seconds (listenKey expires in ~30s)
    let mut ws_write = {
        let (write, read) = ws_stream.split();
        (write, read)
    };

    let lk = listen_key.clone();
    let aid = account_id;
    let mut ping_id: u64 = 2;

    let mut ping_interval = interval(Duration::from_secs(10));
    ping_interval.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                let ping = json!({
                    "id": ping_id,
                    "method": "stream.ping",
                    "params": {
                        "accountId": aid,
                        "listenKey": lk
                    },
                    "timestamp": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0)
                });
                ping_id += 1;
                ws_write.0.send(Message::Text(ping.to_string().into())).await?;
            }

            msg = ws_write.1.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_hibachi_message(&text, tracker);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn handle_hibachi_message(text: &str, tracker: &Arc<Mutex<FillTracker>>) {
    let event: HibachiEvent = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(_) => return,
    };

    if event.event == "order_closed" {
        let data: OrderClosedData = match serde_json::from_value(event.data) {
            Ok(d) => d,
            Err(_) => return,
        };

        let status = data.status.as_deref().unwrap_or("");
        if status != "Filled" {
            return;
        }

        let order_id = match &data.order_id {
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let avg_price = data
            .avg_fill_price
            .as_deref()
            .and_then(|s| Decimal::from_str(s).ok())
            .unwrap_or(Decimal::ZERO);

        let filled_qty = data
            .filled_quantity
            .as_deref()
            .and_then(|s| Decimal::from_str(s).ok())
            .unwrap_or(Decimal::ZERO);

        info!(
            order_id = %order_id,
            symbol = %data.symbol.as_deref().unwrap_or("?"),
            avg_price = %avg_price,
            filled_qty = %filled_qty,
            "Hibachi fill received"
        );

        tracker.lock().unwrap().record_fill(
            &order_id,
            FillLeg { avg_price, filled_qty },
        );
    }
}
