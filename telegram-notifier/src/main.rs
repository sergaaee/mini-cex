use anyhow::Result;
use common::TradeSignal;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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

    info!(
        redis_url = %redis_url,
        chat_count = chat_ids.len(),
        "Starting telegram notifier"
    );

    let (tx, mut rx) = mpsc::channel::<TradeSignal>(64);
    let notifier = notifier::TelegramNotifier::new(bot_token, chat_ids);

    tokio::spawn(consumer::consume_trades(redis_url, tx));

    while let Some(signal) = rx.recv().await {
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
