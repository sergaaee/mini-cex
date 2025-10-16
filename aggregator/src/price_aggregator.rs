use crate::models::Aggregator;
use crate::ws;
use common::Exchange;
use common::models::ticker;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

impl Aggregator {
    /// Создаёт Aggregator и запускает WS-потоки
    pub fn new() -> Self {
        let quotes = Arc::new(RwLock::new(HashMap::new()));

        // Запуск потоков
        let quotes_clone = quotes.clone();
        tokio::spawn(ws::run_binance(quotes_clone));

        let quotes_clone = quotes.clone();
        tokio::spawn(ws::run_backpack(quotes_clone));

        let quotes_clone = quotes.clone();
        tokio::spawn(ws::run_hibachi(quotes_clone));

        let quotes_clone = quotes.clone();
        tokio::spawn(ws::run_aster(quotes_clone));

        Self { quotes }
    }

    /// Возвращает snapshot текущих котировок
    pub async fn snapshot(&self) -> HashMap<String, HashMap<Exchange, ticker::Quote>> {
        self.quotes.read().await.clone()
    }

    pub async fn calc_spread_percent(&self, symbol: String) -> Option<Decimal> {
        let mut snapshot = self.quotes.read().await.clone();
        if snapshot.is_empty() {
            return None;
        }

        let max_price = snapshot
            .entry(symbol.to_string())
            .or_default()
            .values()
            .map(|q| q.mid)
            .max()?;
        let min_price = snapshot
            .entry(symbol.to_string())
            .or_default()
            .values()
            .map(|q| q.mid)
            .min()?;

        if min_price.is_zero() || max_price.is_zero() {
            return None;
        }

        let spread_percent = (max_price - min_price) / min_price * Decimal::from(100u32);

        (spread_percent > Decimal::new(5, 1)).then(|| spread_percent.round_dp(3))
    }

    /// Вычисляет mid по текущим котировкам
    pub async fn calculate_mid(&self, symbol: String) -> Option<Decimal> {
        let mut snapshot = self.quotes.read().await.clone();
        if snapshot.is_empty() {
            return None;
        }

        let entry = snapshot.entry(symbol.to_string()).or_default();
        if entry.is_empty() {
            return None;
        }
        let sum: Decimal = entry.values().map(|q| q.mid).sum();

        let mid = sum / Decimal::from(snapshot.entry(symbol.to_string()).or_default().len() as u64);
        Some(mid.round_dp(2))
    }
}
