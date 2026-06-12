use common::{CloseResult, Exchange, FillResult, PendingClose, PendingFill};
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

// ── Open trade tracking ──────────────────────────────────────────────────────

struct PendingFillContext {
    trade_id: String,
    symbol: String,
    long_exchange: Exchange,
    long_order_id: String,
    short_exchange: Exchange,
    short_order_id: String,
    planned_spread_pct: Decimal,
    #[allow(dead_code)]
    planned_long_price: Decimal,
    #[allow(dead_code)]
    planned_short_price: Decimal,
    #[allow(dead_code)]
    qty: Decimal,
    dry_run: bool,
    long_fill: Option<FillLeg>,
    short_fill: Option<FillLeg>,
}

// ── Close trade tracking ─────────────────────────────────────────────────────

struct PendingCloseContext {
    trade_id: String,
    symbol: String,
    long_exchange: Exchange,
    long_close_order_id: String,
    short_exchange: Exchange,
    short_close_order_id: String,
    long_entry_price: Decimal,
    short_entry_price: Decimal,
    long_qty: Decimal,
    short_qty: Decimal,
    entry_spread_pct: Decimal,
    dry_run: bool,
    long_fill: Option<FillLeg>,
    short_fill: Option<FillLeg>,
}

pub enum TrackerOutput {
    Fill(FillResult),
    Close(CloseResult),
}

pub struct FillTracker {
    /// orderId → (trade_id, is_long_leg, is_close_order)
    order_to_trade: HashMap<String, (String, bool, bool)>,
    /// trade_id → open context
    pending: HashMap<String, PendingFillContext>,
    /// trade_id → close context
    pending_closes: HashMap<String, PendingCloseContext>,
    /// Fills that arrived before their PendingFill record (race condition buffer)
    early_fills: HashMap<String, FillLeg>,
    output_tx: mpsc::Sender<TrackerOutput>,
}

impl FillTracker {
    pub fn new(output_tx: mpsc::Sender<TrackerOutput>) -> Self {
        Self {
            order_to_trade: HashMap::new(),
            pending: HashMap::new(),
            pending_closes: HashMap::new(),
            early_fills: HashMap::new(),
            output_tx,
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
            .insert(pf.long_order_id.clone(), (pf.trade_id.clone(), true, false));
        self.order_to_trade
            .insert(pf.short_order_id.clone(), (pf.trade_id.clone(), false, false));

        let long_early = self.early_fills.remove(&pf.long_order_id);
        let short_early = self.early_fills.remove(&pf.short_order_id);

        self.pending.insert(pf.trade_id.clone(), ctx);

        if let Some(fill) = long_early {
            self.apply_open_fill(&pf.trade_id, true, fill);
        }
        if let Some(fill) = short_early {
            self.apply_open_fill(&pf.trade_id, false, fill);
        }
    }

    /// Called when position-manager publishes a new PendingClose.
    pub fn add_pending_close(&mut self, pc: PendingClose) {
        let ctx = PendingCloseContext {
            trade_id: pc.trade_id.clone(),
            symbol: pc.symbol,
            long_exchange: pc.long_exchange,
            long_close_order_id: pc.long_close_order_id.clone(),
            short_exchange: pc.short_exchange,
            short_close_order_id: pc.short_close_order_id.clone(),
            long_entry_price: pc.long_entry_price,
            short_entry_price: pc.short_entry_price,
            long_qty: pc.long_qty,
            short_qty: pc.short_qty,
            entry_spread_pct: pc.entry_spread_pct,
            dry_run: pc.dry_run,
            long_fill: None,
            short_fill: None,
        };

        self.order_to_trade.insert(
            pc.long_close_order_id.clone(),
            (pc.trade_id.clone(), true, true),
        );
        self.order_to_trade.insert(
            pc.short_close_order_id.clone(),
            (pc.trade_id.clone(), false, true),
        );

        let long_early = self.early_fills.remove(&pc.long_close_order_id);
        let short_early = self.early_fills.remove(&pc.short_close_order_id);

        self.pending_closes.insert(pc.trade_id.clone(), ctx);

        if let Some(fill) = long_early {
            self.apply_close_fill(&pc.trade_id, true, fill);
        }
        if let Some(fill) = short_early {
            self.apply_close_fill(&pc.trade_id, false, fill);
        }
    }

    /// Called when a fill event arrives from an exchange WS stream.
    pub fn record_fill(&mut self, order_id: &str, fill: FillLeg) {
        match self.order_to_trade.get(order_id).cloned() {
            Some((trade_id, is_long, is_close)) => {
                if is_close {
                    self.apply_close_fill(&trade_id, is_long, fill);
                } else {
                    self.apply_open_fill(&trade_id, is_long, fill);
                }
            }
            None => {
                self.early_fills.insert(order_id.to_string(), fill);
            }
        }
    }

    fn apply_open_fill(&mut self, trade_id: &str, is_long: bool, fill: FillLeg) {
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

            let ts = now_ms();

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

            let long_oid = ctx.long_order_id.clone();
            let short_oid = ctx.short_order_id.clone();
            self.pending.remove(trade_id);
            self.order_to_trade.remove(&long_oid);
            self.order_to_trade.remove(&short_oid);

            if self.output_tx.try_send(TrackerOutput::Fill(result)).is_err() {
                warn!("Output channel full, dropping FillResult");
            }
        }
    }

    fn apply_close_fill(&mut self, trade_id: &str, is_long: bool, fill: FillLeg) {
        let ctx = match self.pending_closes.get_mut(trade_id) {
            Some(c) => c,
            None => return,
        };

        if is_long {
            ctx.long_fill = Some(fill);
        } else {
            ctx.short_fill = Some(fill);
        }

        if let (Some(lf), Some(sf)) = (&ctx.long_fill, &ctx.short_fill) {
            let close_spread_pct = if sf.avg_price.is_zero() {
                Decimal::ZERO
            } else {
                (lf.avg_price - sf.avg_price) / sf.avg_price * Decimal::from(100u32)
            };

            let long_fee_rate = std::env::var("BINANCE_TAKER_FEE")
                .ok()
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::new(5, 4));
            let short_fee_rate = std::env::var("HIBACHI_TAKER_FEE")
                .ok()
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::new(5, 4));

            let long_fee = lf.avg_price * ctx.long_qty * long_fee_rate * Decimal::TWO;
            let short_fee = sf.avg_price * ctx.short_qty * short_fee_rate * Decimal::TWO;

            let realized_pnl = (lf.avg_price - ctx.long_entry_price) * ctx.long_qty - long_fee
                + (ctx.short_entry_price - sf.avg_price) * ctx.short_qty
                - short_fee;

            let ts = now_ms();

            let result = CloseResult {
                trade_id: ctx.trade_id.clone(),
                symbol: ctx.symbol.clone(),
                long_exchange: ctx.long_exchange,
                long_close_order_id: ctx.long_close_order_id.clone(),
                long_close_avg_price: lf.avg_price,
                long_entry_price: ctx.long_entry_price,
                long_qty: ctx.long_qty,
                short_exchange: ctx.short_exchange,
                short_close_order_id: ctx.short_close_order_id.clone(),
                short_close_avg_price: sf.avg_price,
                short_entry_price: ctx.short_entry_price,
                short_qty: ctx.short_qty,
                entry_spread_pct: ctx.entry_spread_pct,
                close_spread_pct: close_spread_pct.round_dp(4),
                realized_pnl: realized_pnl.round_dp(6),
                long_fee: long_fee.round_dp(6),
                short_fee: short_fee.round_dp(6),
                dry_run: ctx.dry_run,
                timestamp: ts,
            };

            info!(
                trade_id = %result.trade_id,
                symbol = %result.symbol,
                long_close = %result.long_close_avg_price,
                short_close = %result.short_close_avg_price,
                realized_pnl = %result.realized_pnl,
                "Both close legs filled — publishing CloseResult"
            );

            let long_oid = ctx.long_close_order_id.clone();
            let short_oid = ctx.short_close_order_id.clone();
            self.pending_closes.remove(trade_id);
            self.order_to_trade.remove(&long_oid);
            self.order_to_trade.remove(&short_oid);

            if self.output_tx.try_send(TrackerOutput::Close(result)).is_err() {
                warn!("Output channel full, dropping CloseResult");
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
