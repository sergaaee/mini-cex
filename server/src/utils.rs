use common::models::{order, ticker};
use common::RedisClient;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub type SharedState = Arc<AppState>;

#[derive(Clone)]
pub struct AppState {
    pub price_tx: broadcast::Sender<ticker::Ticker>,
    pub book_buy: Arc<RwLock<BTreeMap<Decimal, Vec<order::Order>>>>,
    pub book_sell: Arc<RwLock<BTreeMap<Decimal, Vec<order::Order>>>>,
    pub id_counter: Arc<RwLock<u64>>,
    pub redis: RedisClient,
}
