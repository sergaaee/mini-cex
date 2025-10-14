use crate::models::order;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker {
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

#[derive(Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub book_buy: BTreeMap<Decimal, Vec<order::Order>>,
    pub book_sell: BTreeMap<Decimal, Vec<order::Order>>,
    pub timestamp: u64,
}
