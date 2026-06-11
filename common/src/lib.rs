use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use strum_macros::Display;
pub use models::order::Side;

pub mod errors;
pub mod models;
mod redis_client;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Copy, Display)]
pub enum Exchange {
    Hibachi,
    Backpack,
    Binance,
    Aster,
    Bybit,
    BloFin,
    OKX,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpreadOpportunity {
    pub symbol: String,
    pub long_exchange: Exchange,
    pub long_exchange_price: Decimal,
    pub short_exchange: Exchange,
    pub short_exchange_price: Decimal,
    pub spread_percent: Decimal,
    pub size: Decimal,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeSignal {
    pub symbol: String,
    pub long_exchange: Exchange,
    pub long_price: Decimal,
    pub short_exchange: Exchange,
    pub short_price: Decimal,
    pub spread_percent: Decimal,
    pub qty: Decimal,
    pub available_size: Decimal,
    pub dry_run: bool,
    pub timestamp: u64,
}

/// Published by execution-engine after placing both legs — links the two order IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingFill {
    pub trade_id: String,
    pub symbol: String,
    pub long_exchange: Exchange,
    pub long_order_id: String,
    pub short_exchange: Exchange,
    pub short_order_id: String,
    pub planned_spread_pct: Decimal,
    pub planned_long_price: Decimal,
    pub planned_short_price: Decimal,
    pub qty: Decimal,
    pub dry_run: bool,
    pub timestamp: u64,
}

/// Published by fill-tracker after both legs are confirmed filled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillResult {
    pub trade_id: String,
    pub symbol: String,
    pub long_exchange: Exchange,
    pub long_order_id: String,
    pub long_avg_price: Decimal,
    pub long_filled_qty: Decimal,
    pub short_exchange: Exchange,
    pub short_order_id: String,
    pub short_avg_price: Decimal,
    pub short_filled_qty: Decimal,
    pub planned_spread_pct: Decimal,
    pub realized_spread_pct: Decimal,
    pub dry_run: bool,
    pub timestamp: u64,
}

impl Display for SpreadOpportunity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Symbol: {}\nLong on: {} = {} \nShort on: {} = {} \nSpread = {}%\nSize = {}\nTimestamp: {}",
            self.symbol,
            self.long_exchange,
            self.long_exchange_price,
            self.short_exchange,
            self.short_exchange_price,
            self.spread_percent,
            self.size,
            self.timestamp,
        )
    }
}
