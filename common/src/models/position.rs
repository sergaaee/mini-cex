use crate::models::order::Side;
use async_trait::async_trait;
use bpx_api_client::{BpxClient, BACKPACK_API_BASE_URL};
use dotenvy::dotenv;
use hex;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::Sha256;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
struct BinancePosition {
    symbol: String,
    positionAmt: String, // Decimal
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

#[async_trait]
impl PositionManagement for HibachiClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Hibachi: opening {}, quantity: {} {}", side, qty, symbol);
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
