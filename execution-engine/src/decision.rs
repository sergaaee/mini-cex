use common::models::order::Side;
use common::models::position::{
    AsterClient, BackpackClient, BinanceClient, BybitClient, HibachiClient, PositionManagement,
};
use common::{Exchange, SpreadOpportunity, TradeSignal};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::metrics::{SPREADS_RECEIVED, TRADES_EXECUTED, TRADES_SKIPPED};

pub struct DecisionEngine {
    min_spread_pct: Decimal,
    trade_size_usd: Decimal,
    cooldown: Duration,
    pub dry_run: bool,
    last_trade: HashMap<String, Instant>,
    trade_tx: mpsc::Sender<TradeSignal>,
}

impl DecisionEngine {
    pub fn new(
        min_spread_pct: Decimal,
        trade_size_usd: Decimal,
        cooldown_secs: u64,
        dry_run: bool,
        trade_tx: mpsc::Sender<TradeSignal>,
    ) -> Self {
        Self {
            min_spread_pct,
            trade_size_usd,
            cooldown: Duration::from_secs(cooldown_secs),
            dry_run,
            last_trade: HashMap::new(),
            trade_tx,
        }
    }

    /// Fast decision path — all in-memory, never blocks on I/O.
    /// If a trade is warranted, spawns a separate task for the exchange API calls
    /// so this loop can immediately move on to the next opportunity.
    pub fn evaluate(&mut self, opp: SpreadOpportunity) {
        SPREADS_RECEIVED.with_label_values(&[&opp.symbol]).inc();

        if opp.spread_percent < self.min_spread_pct {
            debug!(
                symbol = %opp.symbol,
                spread = %opp.spread_percent,
                min = %self.min_spread_pct,
                "Below threshold, skipping"
            );
            TRADES_SKIPPED
                .with_label_values(&[&opp.symbol, "below_threshold"])
                .inc();
            return;
        }

        if let Some(last) = self.last_trade.get(&opp.symbol) {
            if last.elapsed() < self.cooldown {
                let remaining = self.cooldown.saturating_sub(last.elapsed());
                debug!(
                    symbol = %opp.symbol,
                    remaining_secs = remaining.as_secs(),
                    "In cooldown, skipping"
                );
                TRADES_SKIPPED
                    .with_label_values(&[&opp.symbol, "cooldown"])
                    .inc();
                return;
            }
        }

        self.last_trade.insert(opp.symbol.clone(), Instant::now());

        let precision = match opp.symbol.as_str() {
            "BTC" => 3,
            "ETH" => 3,
            "SOL" => 2,
            _ => {
                panic!("Unsupported symbol: {}", opp.symbol);
            }
        };

        let qty = (self.trade_size_usd / opp.long_exchange_price).round_dp(precision);
        let dry_run = self.dry_run;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        info!(
            symbol = %opp.symbol,
            long_exchange = %opp.long_exchange,
            long_price = %opp.long_exchange_price,
            short_exchange = %opp.short_exchange,
            short_price = %opp.short_exchange_price,
            spread_pct = %opp.spread_percent,
            qty = %qty,
            dry_run,
            "TRADE SIGNAL"
        );

        TRADES_EXECUTED
            .with_label_values(&[
                &opp.symbol,
                &opp.long_exchange.to_string(),
                &opp.short_exchange.to_string(),
            ])
            .inc();

        let signal = TradeSignal {
            symbol: opp.symbol.clone(),
            long_exchange: opp.long_exchange,
            long_price: opp.long_exchange_price,
            short_exchange: opp.short_exchange,
            short_price: opp.short_exchange_price,
            spread_percent: opp.spread_percent,
            qty,
            dry_run,
            timestamp: ts,
        };

        // Non-blocking publish — drop if channel is full rather than block the decision loop
        if self.trade_tx.try_send(signal).is_err() {
            warn!(symbol = %opp.symbol, "Trade signal channel full, notification may be dropped");
        }

        if dry_run {
            return;
        }

        // Execution is off the hot path — runs concurrently with the next evaluation.
        tokio::spawn(execute_trade(opp, qty));
    }
}

/// Sends both legs to their respective exchanges in parallel.
/// Runs in its own task so HTTP latency never blocks the decision loop.
async fn execute_trade(opp: SpreadOpportunity, qty: Decimal) {
    let (long_result, short_result) = tokio::join!(
        open_on_exchange(opp.long_exchange, opp.symbol.clone(), qty, Side::Buy),
        open_on_exchange(opp.short_exchange, opp.symbol.clone(), qty, Side::Sell),
    );

    if let Err(e) = long_result {
        warn!(exchange = %opp.long_exchange, symbol = %opp.symbol, error = %e, "Failed to open long");
    }
    if let Err(e) = short_result {
        warn!(exchange = %opp.short_exchange, symbol = %opp.symbol, error = %e, "Failed to open short");
    }
}

async fn open_on_exchange(
    exchange: Exchange,
    symbol: String,
    qty: Decimal,
    side: Side,
) -> Result<(), String> {
    match exchange {
        Exchange::Binance => BinanceClient.open_position(&symbol, qty, side).await,
        Exchange::Bybit => BybitClient.open_position(&symbol, qty, side).await,
        Exchange::Hibachi => HibachiClient.open_position(&symbol, qty, side).await,
        Exchange::Backpack => BackpackClient.open_position(&symbol, qty, side).await,
        Exchange::Aster => AsterClient.open_position(&symbol, qty, side).await,
        other => Err(format!("No execution client for exchange: {}", other)),
    }
}
