use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Price {
    pub mid: Decimal,
    pub timestamp: i64,
}

#[derive(Serialize)]
pub struct PriceInfo {
    pub symbol: String,
    pub price: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub exchange: String,
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
}
