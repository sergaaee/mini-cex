use crate::models::order::Side;
use crate::models::symbol::Symbol;
use async_trait::async_trait;
use bpx_api_client::types::markets::Asset;
use bpx_api_client::{BpxClient, BACKPACK_API_BASE_URL};
use dotenvy::dotenv;
use hex;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};
use secp256k1::ecdsa::RecoverableSignature;
use secp256k1::{Message, Secp256k1, SecretKey};
use serde::Deserialize;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
struct BinancePosition {
    symbol: String,
    positionAmt: String, // Decimal
}

#[derive(Deserialize, Debug)]
struct BinanceOrder {
    status: String,
    side: String,
}

#[derive(Deserialize)]
struct HibachiPosition {
    symbol: String,
    positionAmt: String, // Decimal
}

#[async_trait]
pub trait PositionManagement {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String>;
    async fn close_position(&self, symbol: &str) -> Result<(), String>;
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String>;
}

#[derive(Debug, Clone)]
pub struct BinanceClient;

#[async_trait]
impl PositionManagement for BinanceClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Binance: opening {}, quantity: {} {}", side, qty, symbol);
        dotenv().ok();

        let api_key = std::env::var("BINANCE_API_KEY").map_err(|_| "Missing Binance API key")?;
        let secret = std::env::var("BINANCE_SECRET").map_err(|_| "Missing Binance secret")?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let (order_side, position_side) = match side {
            Side::Buy => ("BUY", "LONG"),
            Side::Sell => ("SELL", "SHORT"),
            _ => {
                panic!("Unknown side!")
            }
        };

        let query = format!(
            "type=MARKET&symbol={}USDT&side={}&quantity={}&positionSide={}&timestamp={}",
            symbol, order_side, qty, position_side, timestamp
        );

        // HMAC подпись
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.binance.com/fapi/v1/order?{}&signature={}",
            query, signature
        );

        let client = Client::new();
        let resp = client
            .post(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        // .json::<BinanceOrder>()
        // .await
        // .unwrap();

        let text = resp.text().await.unwrap();

        println!("binance response = {:?}", text);

        Ok(())
    }

    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Binance: closing position {}", symbol);
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        dotenv().ok();

        let api_key = std::env::var("BINANCE_API_KEY").map_err(|_| "Missing Binance API key")?;
        let secret = std::env::var("BINANCE_SECRET").map_err(|_| "Missing Binance secret")?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let query = format!("timestamp={}", timestamp);

        // HMAC подпись
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.binance.com/fapi/v2/positionRisk?{}&signature={}",
            query, signature
        );

        let client = Client::new();
        let resp = client
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Vec<BinancePosition>>()
            .await
            .map_err(|e| e.to_string())?;

        // ищем позицию по символу
        for pos in resp {
            if pos.symbol.eq_ignore_ascii_case(symbol) {
                let amt = Decimal::from_str(&pos.positionAmt).map_err(|e| e.to_string())?;
                if amt.is_zero() {
                    return Ok(None);
                } else if amt.is_sign_positive() {
                    return Ok(Some("LONG".to_string()));
                } else {
                    return Ok(Some("SHORT".to_string()));
                }
            }
        }

        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct BybitClient;

#[async_trait]
impl PositionManagement for BybitClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Bybit: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Bybit: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        dotenv().ok();
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct HibachiClient;

fn u64_be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn u32_be(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

fn encode_price(price: Decimal, settlement_decimals: i64, underlying_decimals: i64) -> u64 {
    let decimal_diff = settlement_decimals - underlying_decimals; // -4 для BTC/USDT-P

    let scale = Decimal::TEN.powi(decimal_diff);
    let multiplier = Decimal::from(1u64 << 32); // 2^32 = 4294967296

    (price * scale * multiplier).trunc().to_u64().unwrap_or(0)
}

fn build_order_payload(
    symbol: &str,
    nonce: u64,
    contract_id: u32,
    quantity: Decimal,
    side: u32, // 0 = ASK, 1 = BID
    fee_rate: Decimal,
    price: Option<Decimal>,
) -> Vec<u8> {
    // Сначала определяем decimals
    let (settlement_decimals, underlying_decimals) = match symbol {
        "BTC" => (6i64, 10i64),
        "ETH" => (6i64, 9i64),
        "SOL" => (6i64, 8i64),
        _ => panic!("Unknown symbol!"),
    };

    // Теперь правильно считаем quantity
    let quantity_raw = (quantity * Decimal::from(10i64).powi(underlying_decimals as i64))
        .trunc()
        .to_u64()
        .unwrap_or(0);

    let max_fees = (fee_rate * Decimal::from(100_000_000u64))
        .trunc()
        .to_u64()
        .unwrap_or(0);

    let mut payload = Vec::with_capacity(40);

    payload.extend_from_slice(&u64_be(nonce));
    payload.extend_from_slice(&u32_be(contract_id));
    payload.extend_from_slice(&u64_be(quantity_raw));
    payload.extend_from_slice(&u32_be(side));

    if let Some(p) = price {
        payload.extend_from_slice(&u64_be(encode_price(
            p,
            settlement_decimals,
            underlying_decimals,
        )));
    }

    payload.extend_from_slice(&u64_be(max_fees));

    payload
}

fn sign_payload(payload: &[u8], private_key_hex: &str) -> Result<String, secp256k1::Error> {
    let digest = Sha256::digest(payload);

    let secret_key = SecretKey::from_slice(
        &hex::decode(private_key_hex).map_err(|_| secp256k1::Error::InvalidMessage)?,
    )?;

    let msg = Message::from_digest_slice(&digest).map_err(|_| secp256k1::Error::InvalidMessage)?;

    let secp = Secp256k1::new();
    let sig: RecoverableSignature = secp.sign_ecdsa_recoverable(&msg, &secret_key);

    let (recid, compact) = sig.serialize_compact();

    let mut out = Vec::with_capacity(65);
    out.extend_from_slice(&compact);
    out.push(recid.to_i32() as u8); // recovery id в конце

    Ok(hex::encode(out))
}

#[async_trait]
impl PositionManagement for HibachiClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Hibachi: opening {}, quantity: {} {}", side, qty, symbol);
        dotenv().ok();

        const URL: &str = "https://api.hibachi.xyz/trade/order";

        let account_id =
            std::env::var("HIBACHI_CLIENT_ID").map_err(|_| "Missing Hibachi client id")?;
        let private_key =
            std::env::var("HIBACHI_PRIVATE_KEY").map_err(|_| "Missing Hibachi private key")?;
        let api_key = std::env::var("HIBACHI_API_KEY").map_err(|_| "Missing Hibachi api key")?;

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as u64;

        let (body_side, payload_side, body_side_close, payload_side_close) = match side {
            Side::Buy => ("BID", 1, "ASK", 0),
            Side::Sell => ("ASK", 0, "BID", 1),
            _ => {
                panic!("Unknown side!")
            }
        };

        let contract_id = match symbol {
            "BTC" => 2,
            "ETH" => 1,
            "SOL" => 3,
            _ => {
                panic!("Unknown symbol!")
            }
        };

        let payload = build_order_payload(
            symbol,
            nonce,
            contract_id,
            qty,
            payload_side,
            Decimal::from_str_exact("0.0005").unwrap(),
            None,
        );

        let signature = sign_payload(&payload, private_key.as_str()).unwrap();

        let body = json!({
            "symbol": format!("{}/USDT-P", symbol),
            "accountId": 14257,
            "side": body_side,
            "orderType": "MARKET",
            "nonce": nonce,
            "quantity": qty.to_string(),
            "maxFeesPercent": "0.00050000",
            "signature": signature
        });

        let client = Client::new();

        let response = client
            .post(URL)
            .header("Authorization", api_key.clone())
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let text = response.text().await.map_err(|e| e.to_string())?;

        println!("hibachi open response = {}", text);

        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Hibachi: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        dotenv().ok();

        let account_id =
            std::env::var("HIBACHI_CLIENT_ID").map_err(|_| "Missing Hibachi client id")?;

        let url = format!(
            "https://api.hibachi.xyz/trade/orders?accountId={}",
            account_id
        );

        let client = Client::new();
        let resp = client
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Vec<BinancePosition>>()
            .await
            .map_err(|e| e.to_string())?;

        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct BackpackClient;

#[async_trait]
impl PositionManagement for BackpackClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Backpack: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Backpack: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        dotenv().ok();

        let base_url = BACKPACK_API_BASE_URL.to_string();
        let api_key = std::env::var("API_KEY_BP").map_err(|_| "Missing Backpack API key")?;
        let secret = std::env::var("SECRET_KEY_BP").map_err(|_| "Missing Backpack secret")?;
        let headers = None;

        let client = BpxClient::init(base_url, secret.as_ref(), headers)
            .expect("Failed to initialize Backpack API client");

        match client
            .get_open_orders(Some(format!("{symbol}_USDC_PERP").as_ref()))
            .await
        {
            Ok(orders) => println!("Open Orders: {:?}", orders),
            Err(err) => tracing::error!("Error: {:?}", err),
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct AsterClient;

#[async_trait]
impl PositionManagement for AsterClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Aster: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Aster: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        dotenv().ok();

        let api_key = std::env::var("ASTER_API_KEY").map_err(|_| "Missing Aster API key")?;
        let secret = std::env::var("ASTER_SECRET").map_err(|_| "Missing Aster secret")?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let query = format!("timestamp={}", timestamp);

        // HMAC подпись
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.asterdex.com/fapi/v2/positionRisk?{}&signature={}",
            query, signature
        );

        let client = Client::new();
        let resp = client
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Vec<BinancePosition>>()
            .await
            .map_err(|e| e.to_string())?;

        // ищем позицию по символу
        for pos in resp {
            if pos.symbol.eq_ignore_ascii_case(symbol) {
                let amt = Decimal::from_str(&pos.positionAmt).map_err(|e| e.to_string())?;
                if amt.is_zero() {
                    return Ok(None);
                } else if amt.is_sign_positive() {
                    return Ok(Some("LONG".to_string()));
                } else {
                    return Ok(Some("SHORT".to_string()));
                }
            }
        }

        Ok(None)
    }
}
