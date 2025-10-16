use common::Exchange;
use common::models::ticker;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Deserialize, Debug)]
pub struct BinanceTicker {
    pub E: u64,
    pub b: String,
    pub a: String,
}

#[derive(Deserialize, Debug)]
pub struct AsterTicker {
    pub E: u64,
    pub b: String,
    pub a: String,
}

#[derive(Deserialize, Debug)]
pub struct HibachiResponse {
    pub data: HibachiTicker,
}

#[derive(Deserialize, Debug)]
pub struct HibachiTicker {
    pub bidPrice: String,
    pub askPrice: String,
}

#[derive(Deserialize, Debug)]
pub struct BybitResponse {
    pub ts: u64,
    pub data: BybitTicker,
}

#[derive(Deserialize, Debug)]
pub struct BybitTicker {
    pub bid1Price: String,
    pub ask1Price: String,
}

#[derive(Deserialize, Debug)]
pub struct BackpackResponse {
    pub data: BackpackTicker,
}

#[derive(Deserialize, Debug)]
pub struct BackpackTicker {
    pub E: u64,
    pub b: String,
    pub a: String,
}

pub type Quotes = Arc<RwLock<HashMap<String, HashMap<Exchange, ticker::Quote>>>>;

#[derive(Clone)]
pub struct Aggregator {
    pub quotes: Quotes,
}
