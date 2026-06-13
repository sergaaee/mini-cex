use crate::models::order::Side;
use async_trait::async_trait;
use base64ct::{Base64, Encoding};
use ed25519_dalek::{Signer, SigningKey};
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
use std::collections::BTreeMap;
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

static BACKPACK_SIGNING_KEY: OnceLock<SigningKey> = OnceLock::new();
static BACKPACK_API_KEY: OnceLock<String> = OnceLock::new();

fn backpack_signing_key() -> &'static SigningKey {
    BACKPACK_SIGNING_KEY.get_or_init(|| {
        let secret = std::env::var("SECRET_KEY_BP").expect("Missing SECRET_KEY_BP");
        let bytes = Base64::decode_vec(&secret).expect("Invalid base64 SECRET_KEY_BP");
        let arr: [u8; 32] = bytes.try_into().expect("SECRET_KEY_BP must be 32 bytes");
        SigningKey::from_bytes(&arr)
    })
}

fn backpack_api_key() -> &'static str {
    BACKPACK_API_KEY.get_or_init(|| {
        let vk = backpack_signing_key().verifying_key();
        Base64::encode_string(&vk.to_bytes())
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
    let _ = backpack_signing_key();
    let _ = backpack_api_key();

    // Prime secp256k1 context.
    let _ = secp();

    let client = http_client();
    let _ = tokio::join!(
        client.get("https://fapi.binance.com/fapi/v1/ping").send(),
        client.get("https://fapi.asterdex.com/fapi/v1/ping").send(),
        client.get("https://api.hibachi.xyz/").send(),
        client.get("https://api.backpack.exchange/api/v1/status").send(),
    );
}

// ── Shared types ─────────────────────────────────────────────────────────────

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
struct BinancePosition {
    symbol: String,
    positionAmt: String,
}

#[derive(Deserialize)]
struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    order_id: i64,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct HibachiOrderResponse {
    #[serde(rename = "orderId")]
    order_id: serde_json::Value, // can be int or string
}

#[derive(Deserialize)]
struct HibachiPosition {
    symbol: String,
    positionAmt: String,
}

// ── Trait ────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait PositionManagement {
    /// Places a market order. Returns the exchange-assigned order ID.
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<String, String>;
    /// Places a closing market order. close_side is the opposite of how the position was opened.
    /// Returns the exchange-assigned order ID of the close order.
    async fn close_position(&self, symbol: &str, qty: Decimal, close_side: Side) -> Result<String, String>;
    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String>;
}

// ── Binance ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BinanceClient;

#[async_trait]
impl PositionManagement for BinanceClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<String, String> {
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

        // newOrderRespType=RESULT ensures the full fill info is returned immediately
        let query = format!(
            "type=MARKET&symbol={}USDT&side={}&quantity={}&positionSide={}&newOrderRespType=RESULT&timestamp={}",
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

        let text = resp.text().await.map_err(|e| e.to_string())?;
        println!("binance response = {:?}", text);

        let order_id = serde_json::from_str::<BinanceOrderResponse>(&text)
            .map(|r| r.order_id.to_string())
            .map_err(|e| format!("Binance order parse error: {} — body: {}", e, text))?;

        Ok(order_id)
    }

    async fn close_position(&self, symbol: &str, qty: Decimal, close_side: Side) -> Result<String, String> {
        println!("Binance: closing {} {} {}", close_side, qty, symbol);

        let api_key = binance_api_key();
        let secret = binance_secret();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis();

        // To close a LONG position we SELL with positionSide=LONG.
        // To close a SHORT position we BUY with positionSide=SHORT.
        let (order_side, position_side) = match close_side {
            Side::Sell => ("SELL", "LONG"),
            Side::Buy => ("BUY", "SHORT"),
            _ => panic!("Unknown close side"),
        };

        let query = format!(
            "type=MARKET&symbol={}USDT&side={}&quantity={}&positionSide={}&newOrderRespType=RESULT&timestamp={}",
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

        let text = resp.text().await.map_err(|e| e.to_string())?;
        println!("binance close response = {:?}", text);

        let order_id = serde_json::from_str::<BinanceOrderResponse>(&text)
            .map(|r| r.order_id.to_string())
            .map_err(|e| format!("Binance close order parse error: {} — body: {}", e, text))?;

        Ok(order_id)
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
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<String, String> {
        println!("Bybit: opening {}, quantity: {} {}", side, qty, symbol);
        Ok("stub".to_string())
    }
    async fn close_position(&self, symbol: &str, _qty: Decimal, _close_side: Side) -> Result<String, String> {
        println!("Bybit: closing position {}", symbol);
        Ok("stub".to_string())
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
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<String, String> {
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

        // orderId can be returned as integer or string
        let order_id = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| {
                v.get("orderId").map(|id| match id {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            })
            .ok_or_else(|| format!("Hibachi: missing orderId in response: {}", text))?;

        Ok(order_id)
    }

    async fn close_position(&self, symbol: &str, qty: Decimal, close_side: Side) -> Result<String, String> {
        println!("Hibachi: closing {} {} {}", close_side, qty, symbol);

        const URL: &str = "https://api.hibachi.xyz/trade/order";

        let account_id = hibachi_client_id();
        let private_key = hibachi_private_key();
        let api_key = hibachi_api_key();

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as u64;

        // Closing a SHORT means we BUY back (BID). Closing a LONG means we SELL (ASK).
        let (body_side, payload_side) = match close_side {
            Side::Buy => ("BID", 1u32),
            Side::Sell => ("ASK", 0u32),
            _ => panic!("Unknown close side"),
        };

        let contract_id: u32 = match symbol {
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

        let signature = sign_payload(&payload, private_key).map_err(|e| e.to_string())?;

        let body = json!({
            "symbol": format!("{}/USDT-P", symbol),
            "accountId": account_id.parse::<u64>().unwrap_or(0),
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
        println!("hibachi close response = {}", text);

        let order_id = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| {
                v.get("orderId").map(|id| match id {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            })
            .ok_or_else(|| format!("Hibachi close: missing orderId in response: {}", text))?;

        Ok(order_id)
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        let account_id = hibachi_client_id();
        let api_key = hibachi_api_key();
        let url = format!(
            "https://api.hibachi.xyz/trade/account/info?accountId={}",
            account_id
        );

        #[derive(Deserialize)]
        struct AccountInfo {
            positions: Vec<HibachiAccountPosition>,
        }
        #[derive(Deserialize)]
        struct HibachiAccountPosition {
            direction: String,
            symbol: String,
            quantity: String,
        }

        let resp = http_client()
            .get(&url)
            .header("Authorization", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<AccountInfo>()
            .await
            .map_err(|e| e.to_string())?;

        let hibachi_symbol = format!("{}/USDT-P", symbol);
        for pos in resp.positions {
            if pos.symbol == hibachi_symbol {
                let qty = Decimal::from_str(&pos.quantity).unwrap_or(Decimal::ZERO);
                if qty.is_zero() {
                    return Ok(None);
                }
                return Ok(Some(pos.direction)); // "Long" or "Short"
            }
        }
        Ok(None)
    }
}

// ── Backpack ─────────────────────────────────────────────────────────────────

const BP_BASE: &str = "https://api.backpack.exchange";
const BP_WINDOW: u32 = 5000;

#[derive(Deserialize)]
struct BpOrderResponse {
    id: String,
}

#[derive(Deserialize)]
struct BpFuturePosition {
    symbol: String,
    #[serde(rename = "netQuantity")]
    net_quantity: String,
}

fn bp_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Builds the signee string and returns (signee, timestamp).
/// body_json must be a JSON object; keys are sorted alphabetically.
fn bp_signee(instruction: &str, body_json: Option<&serde_json::Value>, timestamp: u64) -> String {
    let mut s = format!("instruction={instruction}");
    if let Some(serde_json::Value::Object(map)) = body_json {
        let sorted: BTreeMap<&str, &serde_json::Value> =
            map.iter().map(|(k, v)| (k.as_str(), v)).collect();
        for (k, v) in sorted {
            let vs = v.to_string();
            let vs = vs.trim_matches('"');
            s.push_str(&format!("&{k}={vs}"));
        }
    }
    s.push_str(&format!("&timestamp={timestamp}&window={BP_WINDOW}"));
    s
}

fn bp_sign(signee: &str) -> String {
    let sig = backpack_signing_key().sign(signee.as_bytes());
    Base64::encode_string(&sig.to_bytes())
}

#[derive(Debug, Clone)]
pub struct BackpackClient;

#[async_trait]
impl PositionManagement for BackpackClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<String, String> {
        println!("Backpack: opening {side}, quantity: {qty} {symbol}");

        let symbol_bp = format!("{symbol}_USDC_PERP");
        let side_str = match side {
            Side::Buy => "Bid",
            Side::Sell => "Ask",
            _ => panic!("Unknown side"),
        };

        let body = json!({
            "orderType": "Market",
            "quantity": qty.to_string(),
            "side": side_str,
            "symbol": symbol_bp,
        });

        let ts = bp_now_ms();
        let signee = bp_signee("orderExecute", Some(&body), ts);
        let signature = bp_sign(&signee);

        let url = format!("{BP_BASE}/api/v1/order");
        let resp = http_client()
            .post(&url)
            .header("X-API-Key", backpack_api_key())
            .header("X-Signature", &signature)
            .header("X-Timestamp", ts.to_string())
            .header("X-Window", BP_WINDOW.to_string())
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let text = resp.text().await.map_err(|e| e.to_string())?;
        println!("backpack open response = {text}");

        serde_json::from_str::<BpOrderResponse>(&text)
            .map(|r| r.id)
            .map_err(|e| format!("Backpack order parse error: {e} — body: {text}"))
    }

    async fn close_position(&self, symbol: &str, qty: Decimal, close_side: Side) -> Result<String, String> {
        println!("Backpack: closing {close_side} {qty} {symbol}");

        let symbol_bp = format!("{symbol}_USDC_PERP");
        let side_str = match close_side {
            Side::Sell => "Ask",
            Side::Buy => "Bid",
            _ => panic!("Unknown close side"),
        };

        let body = json!({
            "orderType": "Market",
            "quantity": qty.to_string(),
            "reduceOnly": true,
            "side": side_str,
            "symbol": symbol_bp,
        });

        let ts = bp_now_ms();
        let signee = bp_signee("orderExecute", Some(&body), ts);
        let signature = bp_sign(&signee);

        let url = format!("{BP_BASE}/api/v1/order");
        let resp = http_client()
            .post(&url)
            .header("X-API-Key", backpack_api_key())
            .header("X-Signature", &signature)
            .header("X-Timestamp", ts.to_string())
            .header("X-Window", BP_WINDOW.to_string())
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let text = resp.text().await.map_err(|e| e.to_string())?;
        println!("backpack close response = {text}");

        serde_json::from_str::<BpOrderResponse>(&text)
            .map(|r| r.id)
            .map_err(|e| format!("Backpack close order parse error: {e} — body: {text}"))
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<String>, String> {
        let symbol_bp = format!("{symbol}_USDC_PERP");

        let ts = bp_now_ms();
        let signee = bp_signee("positionQuery", None, ts);
        let signature = bp_sign(&signee);

        let url = format!("{BP_BASE}/api/v1/position");
        let resp = http_client()
            .get(&url)
            .header("X-API-Key", backpack_api_key())
            .header("X-Signature", &signature)
            .header("X-Timestamp", ts.to_string())
            .header("X-Window", BP_WINDOW.to_string())
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Vec<BpFuturePosition>>()
            .await
            .map_err(|e| e.to_string())?;

        for pos in resp {
            if pos.symbol == symbol_bp {
                let qty = Decimal::from_str(&pos.net_quantity).unwrap_or(Decimal::ZERO);
                if qty.is_zero() {
                    return Ok(None);
                } else if qty.is_sign_positive() {
                    return Ok(Some("LONG".to_string()));
                } else {
                    return Ok(Some("SHORT".to_string()));
                }
            }
        }
        Ok(None)
    }
}

// ── Aster ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AsterClient;

#[async_trait]
impl PositionManagement for AsterClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<String, String> {
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
            "type=MARKET&symbol={}USDT&side={}&quantity={}&positionSide={}&newOrderRespType=RESULT&timestamp={}",
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

        let text = resp.text().await.map_err(|e| e.to_string())?;
        println!("aster response = {:?}", text);

        let order_id = serde_json::from_str::<BinanceOrderResponse>(&text)
            .map(|r| r.order_id.to_string())
            .map_err(|e| format!("Aster order parse error: {} — body: {}", e, text))?;

        Ok(order_id)
    }

    async fn close_position(&self, symbol: &str, _qty: Decimal, _close_side: Side) -> Result<String, String> {
        println!("Aster: closing position {}", symbol);
        Ok("stub".to_string())
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
