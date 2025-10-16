use serde::{Deserialize, Serialize};

pub mod errors;
pub mod models;
mod redis_client;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Exchange {
    Hibachi,
    Backpack,
    Binance,
}
