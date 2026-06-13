use common::Exchange;
use common::models::ticker;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Deserialize, Debug)]
pub struct BinanceTicker {
    #[serde(rename = "E")]
    pub timestamp: u64,
    #[serde(rename = "b")]
    pub best_bid: String,
    #[serde(rename = "B")]
    pub best_bid_size: String,
    #[serde(rename = "a")]
    pub best_ask: String,
    #[serde(rename = "A")]
    pub best_ask_size: String,
}

#[derive(Deserialize, Debug)]
pub struct AsterTicker {
    #[serde(rename = "E")]
    pub timestamp: u64,
    #[serde(rename = "b")]
    pub best_bid: String,
    #[serde(rename = "B")]
    pub best_bid_size: String,
    #[serde(rename = "a")]
    pub best_ask: String,
    #[serde(rename = "A")]
    pub best_ask_size: String,
}

#[derive(Deserialize, Debug)]
pub struct HibachiResponse {
    pub data: HibachiTicker,
}

#[derive(Deserialize, Debug)]
pub struct HibachiTicker {
    #[serde(rename = "bidPrice")]
    pub best_bid: String,
    #[serde(rename = "bidSize")]
    pub best_bid_size: String,
    #[serde(rename = "askPrice")]
    pub best_ask: String,
    #[serde(rename = "askSize")]
    pub best_ask_size: String,
}

#[derive(Deserialize, Debug)]
pub struct BybitResponse {
    pub ts: u64,
    pub data: BybitTicker,
}

#[derive(Deserialize, Debug)]
pub struct BybitTicker {
    #[serde(rename = "bid1Price")]
    pub best_bid: String,
    #[serde(rename = "bid1Size")]
    pub best_bid_size: String,
    #[serde(rename = "ask1Price")]
    pub best_ask: String,
    #[serde(rename = "ask1Size")]
    pub best_ask_size: String,
}

#[derive(Deserialize, Debug)]
pub struct BloFinResponse {
    pub data: BloFinTicker,
}

#[derive(Deserialize, Debug)]
pub struct BloFinTicker {
    #[serde(rename = "bidPrice")]
    pub best_bid: String,
    #[serde(rename = "bidSize")]
    pub best_bid_size: String,
    #[serde(rename = "askPrice")]
    pub best_ask: String,
    #[serde(rename = "askSize")]
    pub best_ask_size: String,
    pub ts: u64,
}

#[derive(Deserialize, Debug)]
pub struct OKXResponse {
    pub data: Vec<OKXTicker>,
}

#[derive(Deserialize, Debug)]
pub struct OKXTicker {
    #[serde(rename = "bidPx")]
    pub best_bid: String,
    #[serde(rename = "bidSz")]
    pub best_bid_size: String,
    #[serde(rename = "askPx")]
    pub best_ask: String,
    #[serde(rename = "askSz")]
    pub best_ask_size: String,

    #[serde(rename = "ts")]
    pub ts: String,
}

#[derive(Deserialize, Debug)]
pub struct BackpackResponse {
    pub data: BackpackTicker,
}

#[derive(Deserialize, Debug)]
pub struct BackpackTicker {
    #[serde(rename = "E")]
    pub timestamp: u64,
    #[serde(rename = "b")]
    pub best_bid: String,
    #[serde(rename = "B")]
    pub best_bid_size: String,
    #[serde(rename = "a")]
    pub best_ask: String,
    #[serde(rename = "A")]
    pub best_ask_size: String,
}

pub type Quotes = Arc<RwLock<HashMap<String, HashMap<Exchange, ticker::Quote>>>>;

#[derive(Clone)]
pub struct Aggregator {
    pub quotes: Quotes,
}
