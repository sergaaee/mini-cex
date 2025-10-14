use crate::errors::order::{EngineError, OrderError, OrderTypeError, SideError};
use crate::models::symbol::Symbol;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
    #[serde(other)]
    Unknown,
}

impl Side {
    pub fn validate(&self) -> Result<(), SideError> {
        match self {
            Side::Unknown => Err(SideError::UnsupportedSide),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    #[serde(other)]
    Unknown,
}

impl OrderType {
    pub fn validate(&self) -> Result<(), OrderTypeError> {
        match self {
            OrderType::Unknown => Err(OrderTypeError::UnsupportedOrderType),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub side: Side,
    pub symbol: Symbol,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub client_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub symbol: Symbol,
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub created_at: u64,
}

impl Order {
    pub fn new(
        id: u64,
        symbol: Symbol,
        amount: Decimal,
        price: Decimal,
        side: Side,
        order_type: OrderType,
        client_id: Option<String>,
        created_at: u64,
    ) -> Result<Self, EngineError> {
        if amount <= dec!(0) {
            return Err(OrderError::WrongAmount.into());
        }
        if price <= dec!(0) {
            return Err(OrderError::WrongPrice.into());
        }
        if price.scale() > 2 {
            return Err(OrderError::PriceBadPrecision.into());
        }
        if amount.scale() > 2 {
            return Err(OrderError::AmountBadPrecision.into());
        }
        symbol.validate()?;
        side.validate()?;
        order_type.validate()?;

        Ok(Self {
            id,
            symbol,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    Filled,
    Cancelled,
}
