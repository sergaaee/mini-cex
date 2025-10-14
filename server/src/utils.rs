use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use tokio::sync::broadcast;
use common::models::{order, price};
use common::RedisClient;

pub type SharedState = Arc<AppState>;

#[derive(Clone)]
pub struct AppState {
    pub price_tx: broadcast::Sender<price::Price>,
    pub book_buy: Arc<RwLock<BTreeMap<Decimal, Vec<order::Order>>>>,
    pub book_sell: Arc<RwLock<BTreeMap<Decimal, Vec<order::Order>>>>,
    pub id_counter: Arc<RwLock<u64>>,
    pub redis: RedisClient,
}