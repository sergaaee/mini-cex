use anyhow::Result;
use common::Exchange;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::PgPool;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::db::{load_open_positions, update_poll_data};

#[derive(Deserialize)]
struct BinancePosRisk {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "unrealizedProfit")]
    unrealized_profit: String,
}

#[derive(Deserialize)]
struct HibachiAccountInfo {
    positions: Vec<HibachiPosition>,
}

#[derive(Deserialize)]
struct HibachiPosition {
    symbol: String,
    quantity: String,
    #[serde(rename = "unrealizedTradingPnl")]
    unrealized_trading_pnl: String,
}

pub async fn run_poller(pool: PgPool, interval_secs: u64) {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build HTTP client");

    let binance_api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
    let binance_secret = std::env::var("BINANCE_SECRET").unwrap_or_default();
    let hibachi_client_id = std::env::var("HIBACHI_CLIENT_ID").unwrap_or_default();
    let hibachi_api_key = std::env::var("HIBACHI_API_KEY").unwrap_or_default();

    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // skip first immediate tick

    loop {
        ticker.tick().await;

        let positions = match load_open_positions(&pool).await {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "Failed to load open positions for polling");
                continue;
            }
        };

        if positions.is_empty() {
            continue;
        }

        info!(count = positions.len(), "Polling exchange positions");

        let binance_data = fetch_binance_positions(&client, &binance_api_key, &binance_secret).await;
        let hibachi_data =
            fetch_hibachi_positions(&client, &hibachi_client_id, &hibachi_api_key).await;

        for pos in &positions {
            let long_qty;
            let long_upnl;
            let short_qty;
            let short_upnl;

            match pos.long_exchange {
                Exchange::Binance => {
                    let sym = format!("{}USDT", pos.symbol);
                    (long_qty, long_upnl) = binance_data
                        .as_ref()
                        .ok()
                        .and_then(|v| v.iter().find(|p| p.symbol == sym))
                        .map(|p| {
                            (
                                Decimal::from_str(&p.position_amt).ok(),
                                Decimal::from_str(&p.unrealized_profit).ok(),
                            )
                        })
                        .unwrap_or((None, None));
                }
                _ => (long_qty, long_upnl) = (None, None),
            }

            match pos.short_exchange {
                Exchange::Hibachi => {
                    let sym = format!("{}/USDT-P", pos.symbol);
                    (short_qty, short_upnl) = hibachi_data
                        .as_ref()
                        .ok()
                        .and_then(|v| v.iter().find(|p| p.symbol == sym))
                        .map(|p| {
                            (
                                Decimal::from_str(&p.quantity).ok(),
                                Decimal::from_str(&p.unrealized_trading_pnl).ok(),
                            )
                        })
                        .unwrap_or((None, None));
                }
                _ => (short_qty, short_upnl) = (None, None),
            }

            if let Err(e) = update_poll_data(
                &pool,
                &pos.trade_id,
                long_qty,
                long_upnl,
                short_qty,
                short_upnl,
            )
            .await
            {
                warn!(trade_id = %pos.trade_id, error = %e, "Failed to update poll data");
            }
        }
    }
}

async fn fetch_binance_positions(
    client: &Client,
    api_key: &str,
    secret: &str,
) -> Result<Vec<BinancePosRisk>> {
    use hmac::{Hmac, Mac, KeyInit};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis();

    let query = format!("timestamp={}", timestamp);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
    mac.update(query.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let url = format!(
        "https://fapi.binance.com/fapi/v2/positionRisk?{}&signature={}",
        query, signature
    );

    let resp = client
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await?
        .json::<Vec<BinancePosRisk>>()
        .await?;

    Ok(resp)
}

async fn fetch_hibachi_positions(
    client: &Client,
    account_id: &str,
    api_key: &str,
) -> Result<Vec<HibachiPosition>> {
    let url = format!(
        "https://api.hibachi.xyz/trade/account/info?accountId={}",
        account_id
    );

    let resp = client
        .get(&url)
        .header("Authorization", api_key)
        .send()
        .await?
        .json::<HibachiAccountInfo>()
        .await?;

    Ok(resp.positions)
}
