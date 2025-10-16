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
        tokio::spawn(ws::binance_ws(quotes_clone));

        let quotes_clone = quotes.clone();
        tokio::spawn(ws::backpack_ws(quotes_clone));

        let quotes_clone = quotes.clone();
        tokio::spawn(ws::hibachi_ws(quotes_clone));

        Self { quotes }
    }

    /// Возвращает snapshot текущих котировок
    pub async fn snapshot(&self) -> HashMap<String, HashMap<Exchange, ticker::Quote>> {
        self.quotes.read().await.clone()
    }

    /// Вычисляет mid по текущим котировкам
    pub async fn calculate_mid(&self, symbol: String) -> Option<Decimal> {
        let mut snapshot = self.quotes.read().await.clone();
        if snapshot.is_empty() {
            return None;
        }
        dbg!(&snapshot);
        let entry = snapshot.entry(symbol.to_string()).or_default();
        let sum: Decimal = entry.values().map(|q| q.mid).sum();

        let mid = sum / Decimal::from(snapshot.entry(symbol.to_string()).or_default().len() as u64);
        Some(mid.round_dp(2))
    }
}
