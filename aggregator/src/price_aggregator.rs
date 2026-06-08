use crate::models::Aggregator;
use crate::publisher::{spawn_publisher, PublisherHandle};
use crate::ws;
use common::models::ticker;
use common::{Exchange, RedisClient, SpreadOpportunity};
use rust_decimal::{Decimal, dec};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

impl Aggregator {
    /// Создаёт Aggregator и запускает WS-потоки
    pub fn new(publisher: PublisherHandle) -> Self {
        let quotes = Arc::new(RwLock::new(HashMap::new()));

        // Запуск потоков с publisher
        let quotes_clone = Arc::clone(&quotes);
        let publisher_clone = publisher.clone();
        tokio::spawn(ws::run_binance(quotes_clone, publisher_clone));
        info!("Binance started!");

        // let quotes_clone = Arc::clone(&quotes);
        // let publisher_clone = publisher.clone();
        // tokio::spawn(ws::run_backpack(quotes_clone, publisher_clone));
        // info!("Backpack started!");

        let quotes_clone = Arc::clone(&quotes);
        let publisher_clone = publisher.clone();
        tokio::spawn(ws::run_hibachi(quotes_clone, publisher_clone));
        info!("Hibachi started!");

        // let quotes_clone = Arc::clone(&quotes);
        // let publisher_clone = publisher.clone();
        // tokio::spawn(ws::run_aster(quotes_clone, publisher_clone));
        // info!("Aster started!");

        // let quotes_clone = Arc::clone(&quotes);
        // let publisher_clone = publisher.clone();
        // tokio::spawn(ws::run_bybit(quotes_clone, publisher_clone));
        // info!("Bybit started!");

        // let quotes_clone = Arc::clone(&quotes);
        // let publisher_clone = publisher.clone();
        // tokio::spawn(ws::run_blofin(quotes_clone, publisher_clone));
        // info!("Blofin started!");

        // let quotes_clone = Arc::clone(&quotes);
        // let publisher_clone = publisher.clone();
        // tokio::spawn(ws::run_okx(quotes_clone, publisher_clone));
        // info!("OKX started!");

        Self { quotes }
    }

    /// Возвращает snapshot текущих котировок
    pub async fn snapshot(&self) -> HashMap<String, HashMap<Exchange, ticker::Quote>> {
        self.quotes.read().await.clone()
    }

    pub async fn current_spread_percent(&self, symbol: String) -> Option<Decimal> {
        let snapshot = self.quotes.read().await;
        let quotes = snapshot.get(&symbol)?;

        let (_, min_quote) = quotes.iter().min_by_key(|(_, q)| q.ask)?;

        let (_, max_quote) = quotes.iter().max_by_key(|(_, q)| q.bid)?;

        Some((max_quote.bid - min_quote.ask) / min_quote.ask * dec!(100))
    }

    pub async fn calc_spread_opportunity(&self, symbol: String) -> Option<SpreadOpportunity> {
        let snapshot = self.quotes.read().await;
        let quotes = snapshot.get(&symbol)?;

        if quotes.is_empty() {
            return None;
        }

        // Найти минимальную и максимальную цену
        let (min_exchange, min_quote) = quotes.iter().min_by_key(|(_, q)| q.ask)?;

        let (max_exchange, max_quote) = quotes.iter().max_by_key(|(_, q)| q.bid)?;
        //
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let max_age_ms: u64 = 100;
        if now_ms.saturating_sub(min_quote.timestamp) > max_age_ms
            || now_ms.saturating_sub(max_quote.timestamp) > max_age_ms
        {
            return None; // stale quote from one of the exchanges
        }

        // Both quotes must be within 100ms of each other
        if min_quote.timestamp.abs_diff(max_quote.timestamp) > 100 {
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

        let buy_spread = (min_quote.ask - min_quote.bid) / min_quote.mid * dec!(100);

        let sell_spread = (max_quote.ask - max_quote.bid) / max_quote.mid * dec!(100);

        let max_book_spread = buy_spread.max(sell_spread);

        // if buy_spread > dec!(0.08) || sell_spread > dec!(0.08) {
        //     return None; // слишком широкий стакан
        // }
        //
        // // Внутренние спреды не должны съедать больше 10% от арбитражного спреда
        // if (buy_spread + sell_spread) > spread_percent * dec!(0.1) {
        //     return None;
        // }

        let threshold = dec!(0.1); // 0.15%
        let current_ts = std::time::SystemTime::now();
        let size = min_quote.ask_size.min(max_quote.bid_size);

        if spread_percent > threshold {
            Some(SpreadOpportunity {
                symbol,
                long_exchange: *min_exchange,
                long_exchange_price: min_quote.ask,
                short_exchange: *max_exchange,
                short_exchange_price: max_quote.bid,
                spread_percent: spread_percent.round_dp(3),
                size,
                timestamp: current_ts
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
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
