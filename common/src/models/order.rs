use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use crate::errors::order::OrderError;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    Filled,
    Cancelled,
}
