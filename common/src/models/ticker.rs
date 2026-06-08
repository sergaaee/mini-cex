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
    pub mid: Decimal,
    pub bid: Decimal,
    pub bid_size: Decimal,
    pub ask: Decimal,
    pub ask_size: Decimal,
    pub timestamp: u64, // ms
}

#[derive(Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub book_buy: BTreeMap<Decimal, Vec<order::Order>>,
    pub book_sell: BTreeMap<Decimal, Vec<order::Order>>,
    pub timestamp: u64,
}
