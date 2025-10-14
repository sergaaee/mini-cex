use crate::ws;
use common::models::price;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Aggregator {
    quotes: Arc<RwLock<HashMap<String, price::Quote>>>,
}

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
    pub async fn snapshot(&self) -> HashMap<String, price::Quote> {
        self.quotes.read().await.clone()
    }

    /// Вычисляет mid по текущим котировкам
    pub async fn calculate_mid(&self) -> Option<Decimal> {
        let snapshot = self.quotes.read().await;
        if snapshot.is_empty() {
            return None;
        }

        let sum = snapshot
            .values()
            .map(|q| (q.bid + q.ask) / Decimal::from(2u32))
            .sum::<Decimal>();

        // Сначала делим, потом округляем до 2 знаков
        let mid = sum / Decimal::from(snapshot.len() as u64);
        Some(mid.round_dp(2))
    }
}
