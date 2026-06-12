use anyhow::Result;
use common::{CloseResult, FillResult, TradeSignal};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

mod consumer;
mod notifier;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("telegram_notifier=info".parse().unwrap()),
        )
        .init();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://redis:6379/".into());

    let bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
        .expect("TELEGRAM_BOT_TOKEN env var is required");

    let chat_ids: Vec<i64> = std::env::var("TELEGRAM_CHAT_IDS")
        .unwrap_or_else(|_| "5003767225,479449574,363276843".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    info!(redis_url = %redis_url, chat_count = chat_ids.len(), "Starting telegram notifier");

    let (signal_tx, mut signal_rx) = mpsc::channel::<TradeSignal>(64);
    let (fill_tx, mut fill_rx) = mpsc::channel::<FillResult>(64);
    let (close_tx, mut close_rx) = mpsc::channel::<CloseResult>(64);

    let notifier = Arc::new(notifier::TelegramNotifier::new(bot_token, chat_ids));

    tokio::spawn(consumer::consume_trades(redis_url.clone(), signal_tx));
    tokio::spawn(consumer::consume_fills(redis_url.clone(), fill_tx));
    tokio::spawn(consumer::consume_close_results(redis_url, close_tx));

    let notifier_fills = Arc::clone(&notifier);
    tokio::spawn(async move {
        while let Some(fill) = fill_rx.recv().await {
            info!(trade_id = %fill.trade_id, "Sending fill confirmation to Telegram");
            if let Err(e) = notifier_fills.send_fill_result(&fill).await {
                warn!(error = %e, "Failed to send fill result to Telegram");
            }
        }
    });

    let notifier_closes = Arc::clone(&notifier);
    tokio::spawn(async move {
        while let Some(close) = close_rx.recv().await {
            info!(trade_id = %close.trade_id, "Sending close result to Telegram");
            if let Err(e) = notifier_closes.send_close_result(&close).await {
                warn!(error = %e, "Failed to send close result to Telegram");
            }
        }
    });

    while let Some(signal) = signal_rx.recv().await {
        info!(
            symbol = %signal.symbol,
            spread = %signal.spread_percent,
            dry_run = signal.dry_run,
            "Sending Telegram notification"
        );
        if let Err(e) = notifier.send_trade_signal(&signal).await {
            warn!(error = %e, "Failed to send Telegram message");
        }
    }

    Ok(())
}
