use common::models::ticker;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Deserialize, Debug)]
pub struct BinanceTicker {
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
pub struct BackpackResponse {
    pub data: BackpackTicker,
}

#[derive(Deserialize, Debug)]
pub struct BackpackTicker {
    pub b: String,
    pub a: String,
}

#[derive(Clone)]
pub struct Aggregator {
    pub quotes: Arc<RwLock<HashMap<String, ticker::Quote>>>,
}
