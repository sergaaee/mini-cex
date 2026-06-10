use crate::models::order::Side;
use async_trait::async_trait;
use bpx_api_client::{BpxClient, BACKPACK_API_BASE_URL};
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
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ── Static HTTP client ────────────────────────────────────────────────────────

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

fn http_client() -> &'static Client {
    HTTP_CLIENT.get_or_init(|| {
        Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client")
    })
}

// ── Static secp256k1 context ─────────────────────────────────────────────────

static SECP256K1: OnceLock<Secp256k1<secp256k1::All>> = OnceLock::new();

fn secp() -> &'static Secp256k1<secp256k1::All> {
    SECP256K1.get_or_init(Secp256k1::new)
}

// ── Cached credentials ───────────────────────────────────────────────────────

static BINANCE_API_KEY: OnceLock<String> = OnceLock::new();
static BINANCE_SECRET: OnceLock<String> = OnceLock::new();

fn binance_api_key() -> &'static str {
    BINANCE_API_KEY.get_or_init(|| {
        std::env::var("BINANCE_API_KEY").expect("Missing BINANCE_API_KEY")
    })
}
fn binance_secret() -> &'static str {
    BINANCE_SECRET.get_or_init(|| {
        std::env::var("BINANCE_SECRET").expect("Missing BINANCE_SECRET")
    })
}

static ASTER_API_KEY: OnceLock<String> = OnceLock::new();
static ASTER_SECRET: OnceLock<String> = OnceLock::new();

fn aster_api_key() -> &'static str {
    ASTER_API_KEY.get_or_init(|| {
        std::env::var("ASTER_API_KEY").expect("Missing ASTER_API_KEY")
    })
}
fn aster_secret() -> &'static str {
    ASTER_SECRET.get_or_init(|| {
        std::env::var("ASTER_SECRET").expect("Missing ASTER_SECRET")
    })
}

static HIBACHI_CLIENT_ID: OnceLock<String> = OnceLock::new();
static HIBACHI_PRIVATE_KEY: OnceLock<String> = OnceLock::new();
static HIBACHI_API_KEY: OnceLock<String> = OnceLock::new();

fn hibachi_client_id() -> &'static str {
    HIBACHI_CLIENT_ID.get_or_init(|| {
        std::env::var("HIBACHI_CLIENT_ID").expect("Missing HIBACHI_CLIENT_ID")
    })
}
fn hibachi_private_key() -> &'static str {
    HIBACHI_PRIVATE_KEY.get_or_init(|| {
        std::env::var("HIBACHI_PRIVATE_KEY").expect("Missing HIBACHI_PRIVATE_KEY")
    })
}
fn hibachi_api_key() -> &'static str {
    HIBACHI_API_KEY.get_or_init(|| {
        std::env::var("HIBACHI_API_KEY").expect("Missing HIBACHI_API_KEY")
    })
}

// ── Warmup ───────────────────────────────────────────────────────────────────

/// Fires a cheap unauthenticated GET to each exchange's public ping endpoint so
/// that TLS connections are established and pooled before the first real order.
/// Also loads and caches all credentials eagerly so the first order has zero
/// env-var / dotenv overhead.
pub async fn warmup_connections() {
    dotenvy::dotenv().ok();

    // Prime all credential caches now — panics early if a key is missing.
    let _ = binance_api_key();
    let _ = binance_secret();
    let _ = aster_api_key();
    let _ = aster_secret();
    let _ = hibachi_client_id();
    let _ = hibachi_private_key();
    let _ = hibachi_api_key();

    // Prime secp256k1 context.
    let _ = secp();

    let client = http_client();
    let _ = tokio::join!(
        client.get("https://fapi.binance.com/fapi/v1/ping").send(),
        client.get("https://fapi.asterdex.com/fapi/v1/ping").send(),
        client.get("https://api.hibachi.xyz/").send(),
    );
}

// ── Shared types ─────────────────────────────────────────────────────────────

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
struct BinancePosition {
    symbol: String,
    positionAmt: String,
}

#[derive(Deserialize, Debug)]
struct BinanceOrder {
    status: String,
    side: String,
}

#[derive(Deserialize)]
struct HibachiPosition {
    symbol: String,
    positionAmt: String,
}

// ── Trait ────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait PositionManagement {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String>;
    async fn close_position(&self, symbol: &str) -> Result<(), String>;
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String>;
}

// ── Binance ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BinanceClient;

#[async_trait]
impl PositionManagement for BinanceClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Binance: opening {}, quantity: {} {}", side, qty, symbol);

        let api_key = binance_api_key();
        let secret = binance_secret();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let (order_side, position_side) = match side {
            Side::Buy => ("BUY", "LONG"),
            Side::Sell => ("SELL", "SHORT"),
            _ => panic!("Unknown side!"),
        };

        let query = format!(
            "type=MARKET&symbol={}USDT&side={}&quantity={}&positionSide={}&timestamp={}",
            symbol, order_side, qty, position_side, timestamp
        );

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.binance.com/fapi/v1/order?{}&signature={}",
            query, signature
        );

        let resp = http_client()
            .post(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let text = resp.text().await.unwrap();
        println!("binance response = {:?}", text);

        Ok(())
    }

    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Binance: closing position {}", symbol);
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        let api_key = binance_api_key();
        let secret = binance_secret();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let query = format!("timestamp={}", timestamp);

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.binance.com/fapi/v2/positionRisk?{}&signature={}",
            query, signature
        );

        let resp = http_client()
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Vec<BinancePosition>>()
            .await
            .map_err(|e| e.to_string())?;

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

// ── Bybit ────────────────────────────────────────────────────────────────────

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
    async fn get_position(&self, _symbol: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
}

// ── Hibachi ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HibachiClient;

fn u64_be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

fn u32_be(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

fn encode_price(price: Decimal, settlement_decimals: i64, underlying_decimals: i64) -> u64 {
    let decimal_diff = settlement_decimals - underlying_decimals;
    let scale = Decimal::TEN.powi(decimal_diff);
    let multiplier = Decimal::from(1u64 << 32);
    (price * scale * multiplier).trunc().to_u64().unwrap_or(0)
}

fn build_order_payload(
    symbol: &str,
    nonce: u64,
    contract_id: u32,
    quantity: Decimal,
    side: u32,
    fee_rate: Decimal,
    price: Option<Decimal>,
) -> Vec<u8> {
    let (settlement_decimals, underlying_decimals) = match symbol {
        "BTC" => (6i64, 10i64),
        "ETH" => (6i64, 9i64),
        "SOL" => (6i64, 8i64),
        "XRP" => (6i64, 6i64),
        "BNB" => (6i64, 8i64),
        "HYPE" => (6i64, 7i64),
        "SUI" => (6i64, 6i64),
        _ => panic!("Unknown symbol!"),
    };

    let quantity_raw = (quantity * Decimal::from(10i64).powi(underlying_decimals))
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
        &hex::decode(private_key_hex).map_err(|_| secp256k1::Error::InvalidPublicKey)?,
    )?;

    let msg = Message::from_digest_slice(&digest).map_err(|_| secp256k1::Error::InvalidMessage)?;

    let sig: RecoverableSignature = secp().sign_ecdsa_recoverable(&msg, &secret_key);

    let (recid, compact) = sig.serialize_compact();

    let mut out = Vec::with_capacity(65);
    out.extend_from_slice(&compact);
    out.push(recid.to_i32() as u8);

    Ok(hex::encode(out))
}

#[async_trait]
impl PositionManagement for HibachiClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Hibachi: opening {}, quantity: {} {}", side, qty, symbol);

        const URL: &str = "https://api.hibachi.xyz/trade/order";

        let account_id = hibachi_client_id();
        let private_key = hibachi_private_key();
        let api_key = hibachi_api_key();

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as u64;

        let (body_side, payload_side) = match side {
            Side::Buy => ("BID", 1u32),
            Side::Sell => ("ASK", 0u32),
            _ => panic!("Unknown side!"),
        };

        let contract_id = match symbol {
            "BTC" => 2,
            "ETH" => 1,
            "SOL" => 3,
            "XRP" => 24,
            "BNB" => 30,
            "HYPE" => 49,
            "SUI" => 23,
            _ => panic!("Unknown symbol!"),
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

        let signature = sign_payload(&payload, private_key).unwrap();

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

        let response = http_client()
            .post(URL)
            .header("Authorization", api_key)
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

    async fn get_position(&self, _symbol: &str) -> Result<Option<String>, String> {
        let account_id = hibachi_client_id();
        let url = format!("https://api.hibachi.xyz/trade/orders?accountId={}", account_id);

        let resp = http_client()
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

// ── Backpack ─────────────────────────────────────────────────────────────────

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
        let base_url = BACKPACK_API_BASE_URL.to_string();
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

// ── Aster ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AsterClient;

#[async_trait]
impl PositionManagement for AsterClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Aster: opening {}, quantity: {} {}", side, qty, symbol);

        let api_key = aster_api_key();
        let secret = aster_secret();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let (order_side, position_side) = match side {
            Side::Buy => ("BUY", "LONG"),
            Side::Sell => ("SELL", "SHORT"),
            _ => panic!("Unknown side!"),
        };

        let query = format!(
            "type=MARKET&symbol={}USDT&side={}&quantity={}&positionSide={}&timestamp={}",
            symbol, order_side, qty, position_side, timestamp
        );

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.asterdex.com/fapi/v1/order?{}&signature={}",
            query, signature
        );

        let resp = http_client()
            .post(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let text = resp.text().await.unwrap();
        println!("aster response = {:?}", text);

        Ok(())
    }

    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Aster: closing position {}", symbol);
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        let api_key = aster_api_key();
        let secret = aster_secret();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        let query = format!("timestamp={}", timestamp);

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "https://fapi.asterdex.com/fapi/v2/positionRisk?{}&signature={}",
            query, signature
        );

        let resp = http_client()
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Vec<BinancePosition>>()
            .await
            .map_err(|e| e.to_string())?;

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
