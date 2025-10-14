use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use serde_json::json;

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
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub client_id: Option<String>,
}

impl IntoResponse for OrderError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            OrderError::WrongAmount => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Amount must be greater than zero" }),
            ),
            OrderError::WrongPrice => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Price must be greater than zero" }),
            ),
            OrderError::BadPrecision => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Price precision is too high" }),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "success": false, "message": "Internal server error" }),
            ),
        };

        (status, Json(body)).into_response()
    }
}

impl Order {
    pub fn new(
        id: u64,
        amount: Decimal,
        price: Decimal,
        side: Side,
        order_type: OrderType,
        client_id: Option<String>,
        created_at: u64,
    ) -> Result<Self, OrderError> {
        if amount <= dec!(0) {
            return Err(OrderError::WrongAmount);
        }
        if price <= dec!(0) {
            return Err(OrderError::WrongPrice);
        }
        if price.scale() > 2 {
            return Err(OrderError::BadPrecision);
        }

        dbg!(&price.scale());

        Ok(Self {
            id,
            amount,
            price,
            side,
            order_type,
            status: OrderStatus::Open,
            client_id,
            created_at,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub created_at: u64,
}

#[derive(Serialize)]
pub struct PriceInfo {
    pub symbol: String,
    pub price: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    Filled,
    Cancelled,
}

#[derive(Debug)]
pub enum PriceError {
    NotFound,
    RedisError(redis::RedisError),
}

#[derive(Debug, Serialize)]
pub enum OrderError {
    WrongSymbol,
    WrongAmount,
    WrongPrice,
    BadPrecision,
}
