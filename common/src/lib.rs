use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use strum_macros::Display;

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
    pub short_exchange: Exchange,
    pub spread_percent: Decimal,
}

impl Display for SpreadOpportunity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Symbol: {}\nLong on: {}\nShort on: {}\nSpread: {}%",
            self.symbol, self.long_exchange, self.short_exchange, self.spread_percent
        )
    }
}
