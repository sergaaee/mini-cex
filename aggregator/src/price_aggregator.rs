use crate::models::Aggregator;
use crate::ws;
use common::models::ticker;
use common::{Exchange, SpreadOpportunity};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

impl Aggregator {
    /// Создаёт Aggregator и запускает WS-потоки
    pub fn new() -> Self {
        let quotes = Arc::new(RwLock::new(HashMap::new()));

        // Запуск потоков
        let quotes_clone = Arc::clone(&quotes);
        tokio::spawn(ws::run_binance(quotes_clone));
        println!("Binance started!");

        let quotes_clone = Arc::clone(&quotes);
        tokio::spawn(ws::run_backpack(quotes_clone));
        println!("Backpack started!");

        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_hibachi(quotes_clone));
        // println!("Hibachi started!");

        let quotes_clone = Arc::clone(&quotes);
        tokio::spawn(ws::run_aster(quotes_clone));
        println!("Aster started!");

        let quotes_clone = Arc::clone(&quotes);
        tokio::spawn(ws::run_bybit(quotes_clone));
        println!("Bybit started!");

        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_blofin(quotes_clone));
        //
        let quotes_clone = Arc::clone(&quotes);
        tokio::spawn(ws::run_okx(quotes_clone));

        Self { quotes }
    }

    /// Возвращает snapshot текущих котировок
    pub async fn snapshot(&self) -> HashMap<String, HashMap<Exchange, ticker::Quote>> {
        self.quotes.read().await.clone()
    }

    pub async fn calc_spread_opportunity(&self, symbol: String) -> Option<SpreadOpportunity> {
        let snapshot = self.quotes.read().await;
        let quotes = snapshot.get(&symbol)?;

        if quotes.is_empty() {
            return None;
        }

        // Найти минимальную и максимальную цену
        let (min_exchange, min_quote) = quotes.iter().min_by_key(|(_, q)| q.mid)?;
        let (max_exchange, max_quote) = quotes.iter().max_by_key(|(_, q)| q.mid)?;

        if min_quote.timestamp != max_quote.timestamp {
            return None;
        }

        if min_quote.mid.is_zero() || max_quote.mid.is_zero() {
            return None;
        }

        let spread_percent =
            (max_quote.mid - min_quote.mid) / min_quote.mid * Decimal::from(100u32);

        let threshold = Decimal::new(1, 1); // 0.5

        if spread_percent > threshold {
            Some(SpreadOpportunity {
                symbol,
                long_exchange: *min_exchange,
                short_exchange: *max_exchange,
                spread_percent: spread_percent.round_dp(3),
            })
        } else {
            None
        }
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
