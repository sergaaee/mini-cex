use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{CloseResult, Exchange, FillResult};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use std::str::FromStr;

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS positions (
            id                   BIGSERIAL PRIMARY KEY,
            trade_id             TEXT UNIQUE NOT NULL,
            symbol               TEXT NOT NULL,
            long_exchange        TEXT NOT NULL,
            long_order_id        TEXT NOT NULL,
            long_entry_price     NUMERIC(30, 10) NOT NULL,
            long_qty             NUMERIC(30, 10) NOT NULL,
            long_actual_qty      NUMERIC(30, 10),
            long_unrealized_pnl  NUMERIC(30, 10),
            long_close_price     NUMERIC(30, 10),
            short_exchange       TEXT NOT NULL,
            short_order_id       TEXT NOT NULL,
            short_entry_price    NUMERIC(30, 10) NOT NULL,
            short_qty            NUMERIC(30, 10) NOT NULL,
            short_actual_qty     NUMERIC(30, 10),
            short_unrealized_pnl NUMERIC(30, 10),
            short_close_price    NUMERIC(30, 10),
            planned_spread_pct   NUMERIC(10, 6) NOT NULL,
            entry_spread_pct     NUMERIC(10, 6) NOT NULL,
            realized_pnl         NUMERIC(30, 10),
            long_fee             NUMERIC(30, 10),
            short_fee            NUMERIC(30, 10),
            status               TEXT NOT NULL DEFAULT 'open',
            dry_run              BOOLEAN NOT NULL DEFAULT false,
            opened_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_at            TIMESTAMPTZ,
            last_polled_at       TIMESTAMPTZ
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)")
        .execute(pool)
        .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)")
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn insert_position(pool: &PgPool, fill: &FillResult) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO positions (
            trade_id, symbol,
            long_exchange, long_order_id, long_entry_price, long_qty,
            short_exchange, short_order_id, short_entry_price, short_qty,
            planned_spread_pct, entry_spread_pct, status, dry_run
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'open',$13)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
    )
    .bind(&fill.trade_id)
    .bind(&fill.symbol)
    .bind(fill.long_exchange.to_string())
    .bind(&fill.long_order_id)
    .bind(fill.long_avg_price)
    .bind(fill.long_filled_qty)
    .bind(fill.short_exchange.to_string())
    .bind(&fill.short_order_id)
    .bind(fill.short_avg_price)
    .bind(fill.short_filled_qty)
    .bind(fill.planned_spread_pct)
    .bind(fill.realized_spread_pct)
    .bind(fill.dry_run)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn update_poll_data(
    pool: &PgPool,
    trade_id: &str,
    long_actual_qty: Option<Decimal>,
    long_unrealized_pnl: Option<Decimal>,
    short_actual_qty: Option<Decimal>,
    short_unrealized_pnl: Option<Decimal>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE positions SET
            long_actual_qty = COALESCE($2, long_actual_qty),
            long_unrealized_pnl = COALESCE($3, long_unrealized_pnl),
            short_actual_qty = COALESCE($4, short_actual_qty),
            short_unrealized_pnl = COALESCE($5, short_unrealized_pnl),
            last_polled_at = NOW()
        WHERE trade_id = $1
        "#,
    )
    .bind(trade_id)
    .bind(long_actual_qty)
    .bind(long_unrealized_pnl)
    .bind(short_actual_qty)
    .bind(short_unrealized_pnl)
    .execute(pool)
    .await?;

    Ok(())
}

/// Atomically transitions status from 'open' to 'closing'.
/// Returns true if the update succeeded (i.e., we won the race).
pub async fn mark_closing(pool: &PgPool, trade_id: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE positions SET status = 'closing' WHERE trade_id = $1 AND status = 'open' RETURNING id",
    )
    .bind(trade_id)
    .fetch_all(pool)
    .await?;

    Ok(!rows.is_empty())
}

pub async fn mark_closed(pool: &PgPool, result: &CloseResult) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE positions SET
            long_qty        = long_qty - $2,
            short_qty       = short_qty - $2,
            realized_pnl    = COALESCE(realized_pnl, 0) + $3,
            long_fee        = COALESCE(long_fee, 0) + $4,
            short_fee       = COALESCE(short_fee, 0) + $5,
            long_close_price  = $6,
            short_close_price = $7,
            status    = CASE WHEN long_qty - $2 < 0.000001 THEN 'closed' ELSE 'open' END,
            closed_at = CASE WHEN long_qty - $2 < 0.000001 THEN NOW() ELSE NULL END
        WHERE trade_id = $1
        "#,
    )
    .bind(&result.trade_id)
    .bind(result.long_qty)
    .bind(result.realized_pnl)
    .bind(result.long_fee)
    .bind(result.short_fee)
    .bind(result.long_close_avg_price)
    .bind(result.short_close_avg_price)
    .execute(pool)
    .await?;

    Ok(())
}

#[derive(Debug, Clone)]
pub struct PositionRow {
    pub trade_id: String,
    pub symbol: String,
    pub long_exchange: Exchange,
    pub long_order_id: String,
    pub long_entry_price: Decimal,
    pub long_qty: Decimal,
    pub short_exchange: Exchange,
    pub short_order_id: String,
    pub short_entry_price: Decimal,
    pub short_qty: Decimal,
    pub entry_spread_pct: Decimal,
    pub dry_run: bool,
}

pub async fn load_open_positions(pool: &PgPool) -> Result<Vec<PositionRow>> {
    let rows = sqlx::query(
        r#"
        SELECT trade_id, symbol,
               long_exchange, long_order_id, long_entry_price, long_qty,
               short_exchange, short_order_id, short_entry_price, short_qty,
               entry_spread_pct, dry_run
        FROM positions WHERE status IN ('open', 'closing')
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    for row in rows {
        let long_ex: String = row.try_get("long_exchange")?;
        let short_ex: String = row.try_get("short_exchange")?;

        result.push(PositionRow {
            trade_id: row.try_get("trade_id")?,
            symbol: row.try_get("symbol")?,
            long_exchange: parse_exchange(&long_ex),
            long_order_id: row.try_get("long_order_id")?,
            long_entry_price: row.try_get("long_entry_price")?,
            long_qty: row.try_get("long_qty")?,
            short_exchange: parse_exchange(&short_ex),
            short_order_id: row.try_get("short_order_id")?,
            short_entry_price: row.try_get("short_entry_price")?,
            short_qty: row.try_get("short_qty")?,
            entry_spread_pct: row.try_get("entry_spread_pct")?,
            dry_run: row.try_get("dry_run")?,
        });
    }

    Ok(result)
}

fn parse_exchange(s: &str) -> Exchange {
    match s {
        "Binance" => Exchange::Binance,
        "Hibachi" => Exchange::Hibachi,
        "Aster" => Exchange::Aster,
        "Bybit" => Exchange::Bybit,
        "Backpack" => Exchange::Backpack,
        "BloFin" => Exchange::BloFin,
        "OKX" => Exchange::OKX,
        _ => Exchange::Binance,
    }
}
