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

        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_backpack(quotes_clone));
        // println!("Backpack started!");

        let quotes_clone = Arc::clone(&quotes);
        tokio::spawn(ws::run_hibachi(quotes_clone));
        println!("Hibachi started!");
        //
        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_aster(quotes_clone));
        // println!("Aster started!");
        //
        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_bybit(quotes_clone));
        // println!("Bybit started!");

        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_blofin(quotes_clone));
        //
        // let quotes_clone = Arc::clone(&quotes);
        // tokio::spawn(ws::run_okx(quotes_clone));

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
        let (min_exchange, min_quote) =
            quotes.iter().min_by_key(|(_, q)| q.ask)?;

        let (max_exchange, max_quote) =
            quotes.iter().max_by_key(|(_, q)| q.bid)?;

        let age_diff =
            max_quote.timestamp.abs_diff(min_quote.timestamp);

        // ms
        if age_diff > 100 {
            return None;
        }

        if min_exchange == max_exchange {
            return None;
        }

        if min_quote.mid.is_zero() || max_quote.mid.is_zero() {
            return None;
        }

        let spread_percent =
            (max_quote.bid - min_quote.ask) / min_quote.ask * Decimal::from(100u32);

        let threshold = Decimal::new(1, 1); // 0.1%

        if spread_percent > threshold {
            Some(SpreadOpportunity {
                symbol,
                long_exchange: *min_exchange,
                long_exchange_price: min_quote.ask.round_dp(1),
                short_exchange: *max_exchange,
                short_exchange_price: max_quote.bid.round_dp(1),
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
