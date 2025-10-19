use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

pub mod errors;
pub mod models;
mod redis_client;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Copy)]
pub enum Exchange {
    Hibachi,
    Backpack,
    Binance,
    Aster,
    Bybit,
    BloFin,
}

impl Display for Exchange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Hibachi => write!(f, "Hibachi"),
            Exchange::Backpack => write!(f, "Backpack"),
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Aster => write!(f, "Aster"),
            Exchange::Bybit => write!(f, "Bybit"),
            Exchange::BloFin => write!(f, "BloFin"),
        }
    }
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
