use anyhow::Result;
use common::models::order::Side;
use common::models::position::{BinanceClient, HibachiClient, PositionManagement};
use common::{Exchange, PendingClose, RedisClient};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

use crate::db::{mark_closing, PositionRow};

pub async fn close_position(
    pool: &PgPool,
    redis_client: &RedisClient,
    position: PositionRow,
) {
    // Atomic CAS: only proceed if we won the race to set status='closing'
    let won = match mark_closing(pool, &position.trade_id).await {
        Ok(won) => won,
        Err(e) => {
            error!(trade_id = %position.trade_id, error = %e, "Failed to mark_closing");
            return;
        }
    };

    if !won {
        return; // Another task already handling this position
    }

    info!(
        trade_id = %position.trade_id,
        symbol = %position.symbol,
        "Closing position — placing close orders"
    );

    if position.dry_run {
        info!(trade_id = %position.trade_id, "DRY RUN — skipping real close orders");
        return;
    }

    let (long_close_side, short_close_side) = match position.long_exchange {
        Exchange::Binance => (Side::Sell, Side::Buy), // LONG on Binance → close with SELL; SHORT on Hibachi → close with BUY
        _ => (Side::Sell, Side::Buy),
    };

    let long_result = close_leg(
        &position.long_exchange,
        &position.symbol,
        position.long_qty,
        long_close_side,
    );
    let short_result = close_leg(
        &position.short_exchange,
        &position.symbol,
        position.short_qty,
        short_close_side,
    );

    let (long_res, short_res) = tokio::join!(long_result, short_result);

    match (long_res, short_res) {
        (Ok(long_order_id), Ok(short_order_id)) => {
            info!(
                trade_id = %position.trade_id,
                long_close_order_id = %long_order_id,
                short_close_order_id = %short_order_id,
                "Close orders placed — waiting for WS fill confirmation"
            );

            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let pending_close = PendingClose {
                trade_id: position.trade_id.clone(),
                symbol: position.symbol.clone(),
                long_exchange: position.long_exchange,
                long_close_order_id: long_order_id,
                short_exchange: position.short_exchange,
                short_close_order_id: short_order_id,
                long_entry_price: position.long_entry_price,
                short_entry_price: position.short_entry_price,
                long_qty: position.long_qty,
                short_qty: position.short_qty,
                entry_spread_pct: position.entry_spread_pct,
                dry_run: false,
                timestamp: ts,
            };

            let mut conn = match redis_client.get_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "Failed to get Redis connection for PendingClose");
                    return;
                }
            };

            if let Err(e) = redis_client
                .publish_pending_close(&mut conn, &pending_close)
                .await
            {
                warn!(error = %e, "Failed to publish PendingClose");
            }
        }
        (long_res, short_res) => {
            error!(
                trade_id = %position.trade_id,
                long_err = ?long_res.err(),
                short_err = ?short_res.err(),
                "Failed to place one or both close orders — position remains in 'closing' status"
            );
        }
    }
}

async fn close_leg(
    exchange: &Exchange,
    symbol: &str,
    qty: Decimal,
    side: Side,
) -> Result<String, String> {
    match exchange {
        Exchange::Binance => BinanceClient.close_position(symbol, qty, side).await,
        Exchange::Hibachi => HibachiClient.close_position(symbol, qty, side).await,
        other => Err(format!("close_position not implemented for {:?}", other)),
    }
}
