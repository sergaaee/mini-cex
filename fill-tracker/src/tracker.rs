use common::{Exchange, FillResult, PendingFill, Side};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct FillLeg {
    pub avg_price: Decimal,
    pub filled_qty: Decimal,
}

struct PendingFillContext {
    trade_id: String,
    symbol: String,
    long_exchange: Exchange,
    long_order_id: String,
    short_exchange: Exchange,
    short_order_id: String,
    planned_spread_pct: Decimal,
    planned_long_price: Decimal,
    planned_short_price: Decimal,
    qty: Decimal,
    dry_run: bool,
    long_fill: Option<FillLeg>,
    short_fill: Option<FillLeg>,
}

pub struct FillTracker {
    /// orderId → (trade_id, is_long_leg)
    order_to_trade: HashMap<String, (String, bool)>,
    /// trade_id → context
    pending: HashMap<String, PendingFillContext>,
    /// Fills that arrived before their PendingFill record (race condition buffer)
    early_fills: HashMap<String, FillLeg>,
    fill_tx: mpsc::Sender<FillResult>,
}

impl FillTracker {
    pub fn new(fill_tx: mpsc::Sender<FillResult>) -> Self {
        Self {
            order_to_trade: HashMap::new(),
            pending: HashMap::new(),
            early_fills: HashMap::new(),
            fill_tx,
        }
    }

    /// Called when execution-engine publishes a new PendingFill.
    pub fn add_pending(&mut self, pf: PendingFill) {
        let ctx = PendingFillContext {
            trade_id: pf.trade_id.clone(),
            symbol: pf.symbol,
            long_exchange: pf.long_exchange,
            long_order_id: pf.long_order_id.clone(),
            short_exchange: pf.short_exchange,
            short_order_id: pf.short_order_id.clone(),
            planned_spread_pct: pf.planned_spread_pct,
            planned_long_price: pf.planned_long_price,
            planned_short_price: pf.planned_short_price,
            qty: pf.qty,
            dry_run: pf.dry_run,
            long_fill: None,
            short_fill: None,
        };

        self.order_to_trade
            .insert(pf.long_order_id.clone(), (pf.trade_id.clone(), true));
        self.order_to_trade
            .insert(pf.short_order_id.clone(), (pf.trade_id.clone(), false));

        // Apply any fills that arrived early
        let long_early = self.early_fills.remove(&pf.long_order_id);
        let short_early = self.early_fills.remove(&pf.short_order_id);

        self.pending.insert(pf.trade_id.clone(), ctx);

        if let Some(fill) = long_early {
            self.apply_fill_inner(&pf.trade_id, true, fill);
        }
        if let Some(fill) = short_early {
            self.apply_fill_inner(&pf.trade_id, false, fill);
        }
    }

    /// Called when a fill event arrives from an exchange WS stream.
    pub fn record_fill(&mut self, order_id: &str, fill: FillLeg) {
        match self.order_to_trade.get(order_id).cloned() {
            Some((trade_id, is_long)) => {
                self.apply_fill_inner(&trade_id, is_long, fill);
            }
            None => {
                // PendingFill not yet arrived — buffer it
                self.early_fills.insert(order_id.to_string(), fill);
            }
        }
    }

    fn apply_fill_inner(&mut self, trade_id: &str, is_long: bool, fill: FillLeg) {
        let ctx = match self.pending.get_mut(trade_id) {
            Some(c) => c,
            None => return,
        };

        if is_long {
            ctx.long_fill = Some(fill);
        } else {
            ctx.short_fill = Some(fill);
        }

        if let (Some(lf), Some(sf)) = (&ctx.long_fill, &ctx.short_fill) {
            let realized_spread_pct = if lf.avg_price.is_zero() {
                Decimal::ZERO
            } else {
                (sf.avg_price - lf.avg_price) / lf.avg_price * Decimal::from(100u32)
            };

            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let result = FillResult {
                trade_id: ctx.trade_id.clone(),
                symbol: ctx.symbol.clone(),
                long_exchange: ctx.long_exchange,
                long_order_id: ctx.long_order_id.clone(),
                long_avg_price: lf.avg_price,
                long_filled_qty: lf.filled_qty,
                short_exchange: ctx.short_exchange,
                short_order_id: ctx.short_order_id.clone(),
                short_avg_price: sf.avg_price,
                short_filled_qty: sf.filled_qty,
                planned_spread_pct: ctx.planned_spread_pct,
                realized_spread_pct: realized_spread_pct.round_dp(4),
                dry_run: ctx.dry_run,
                timestamp: ts,
            };

            info!(
                trade_id = %result.trade_id,
                symbol = %result.symbol,
                long_avg = %result.long_avg_price,
                short_avg = %result.short_avg_price,
                realized_spread = %result.realized_spread_pct,
                "Both legs filled — publishing FillResult"
            );

            // Clean up
            let long_oid = ctx.long_order_id.clone();
            let short_oid = ctx.short_order_id.clone();
            self.pending.remove(trade_id);
            self.order_to_trade.remove(&long_oid);
            self.order_to_trade.remove(&short_oid);

            if self.fill_tx.try_send(result).is_err() {
                warn!("FillResult channel full, dropping");
            }
        }
    }
}
