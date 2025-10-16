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
}

impl Display for Exchange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Hibachi => write!(f, "Hibachi"),
            Exchange::Backpack => write!(f, "Backpack"),
            Exchange::Binance => write!(f, "Binance"),
            Exchange::Aster => write!(f, "Aster"),
        }
    }
}
