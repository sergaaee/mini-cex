use anyhow::{anyhow, Result};
use common::{FillResult, TradeSignal};
use reqwest::Client;
use rust_decimal::prelude::ToPrimitive;
use serde_json::json;
use std::time::{Duration, UNIX_EPOCH};
use tracing::{info, warn};

pub struct TelegramNotifier {
    client: Client,
    bot_token: String,
    chat_ids: Vec<i64>,
}

impl TelegramNotifier {
    pub fn new(bot_token: String, chat_ids: Vec<i64>) -> Self {
        Self {
            client: Client::new(),
            bot_token,
            chat_ids,
        }
    }

    pub async fn send_trade_signal(&self, signal: &TradeSignal) -> Result<()> {
        let text = format_signal(signal);
        self.send(&text).await
    }

    pub async fn send_fill_result(&self, fill: &FillResult) -> Result<()> {
        let text = format_fill_result(fill);
        self.send(&text).await
    }

    async fn send(&self, text: &str) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );

        for &chat_id in &self.chat_ids {
            let resp = self
                .client
                .post(&url)
                .json(&json!({
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML"
                }))
                .send()
                .await?;

            if !resp.status().is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!("Telegram API error for chat {}: {}", chat_id, body));
            }

            // Telegram rate limit: 1 message/sec per chat
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

fn format_signal(signal: &TradeSignal) -> String {
    let header = if signal.dry_run {
        "🔍 <b>DRY RUN — Trade Signal</b>"
    } else {
        "🚨 <b>TRADE EXECUTED</b>"
    };

    let ts = UNIX_EPOCH + Duration::from_millis(signal.timestamp);
    let datetime = humanize_ts(signal.timestamp);

    format!(
        "{header}\n\
        \n\
        Symbol:    <b>{symbol}</b>\n\
        Long:      <b>{long_ex}</b> @ <b>${long_price:.2}</b>\n\
        Short:     <b>{short_ex}</b> @ <b>${short_price:.2}</b>\n\
        Spread:    <b>{spread:.3}%</b>\n\
        Exec size: <b>{qty:.6}</b>\n\
        Avail:     <b>{avail:.6}</b>\n\
        Time:      {datetime}",
        header = header,
        symbol = signal.symbol,
        long_ex = signal.long_exchange,
        long_price = signal.long_price,
        short_ex = signal.short_exchange,
        short_price = signal.short_price,
        spread = signal.spread_percent,
        qty = signal.qty,
        avail = signal.available_size,
        datetime = datetime,
    )
}

fn format_fill_result(fill: &FillResult) -> String {
    let header = if fill.dry_run {
        "🔍 <b>DRY RUN — Fill Confirmed</b>"
    } else {
        "✅ <b>FILL CONFIRMED</b>"
    };

    let datetime = humanize_ts(fill.timestamp);

    format!(
        "{header}\n\
        \n\
        Symbol:    <b>{symbol}</b>\n\
        Long:      <b>{long_ex}</b> @ <b>${long_price:.2}</b>  qty: {long_qty:.6}\n\
        Short:     <b>{short_ex}</b> @ <b>${short_price:.2}</b>  qty: {short_qty:.6}\n\
        Spread:    planned <b>{planned:.3}%</b>  →  realized <b>{realized:.3}%</b>\n\
        Time:      {datetime}",
        header = header,
        symbol = fill.symbol,
        long_ex = fill.long_exchange,
        long_price = fill.long_avg_price,
        long_qty = fill.long_filled_qty,
        short_ex = fill.short_exchange,
        short_price = fill.short_avg_price,
        short_qty = fill.short_filled_qty,
        planned = fill.planned_spread_pct,
        realized = fill.realized_spread_pct,
        datetime = datetime,
    )
}

fn humanize_ts(ts_ms: u64) -> String {
    // Simple UTC formatting without pulling in chrono
    let secs = ts_ms / 1000;
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
    // days since epoch → rough date (good enough for logging)
    let year = 1970 + days / 365;
    let day_of_year = days % 365;
    let month = day_of_year / 30 + 1;
    let day = day_of_year % 30 + 1;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC", year, month, day, h, m, s)
}
