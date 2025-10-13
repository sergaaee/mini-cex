use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;

mod redis_client;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub exchange: String,
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Price {
    pub mid: Decimal,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Side { Buy, Sell }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType { Market, Limit }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub client_id: Option<String>,
    pub order_type: OrderType
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub client_id: Option<String>,
}
