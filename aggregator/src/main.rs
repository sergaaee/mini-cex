mod metrics;
mod models;
mod price_aggregator;
mod publisher;
mod utils;
mod ws;

use crate::metrics::start_metrics_server;
use crate::models::Aggregator;
use crate::publisher::spawn_publisher;
use common::Exchange;
use common::models::order::Side;
use common::models::symbol::Symbol;
use common::RedisClient;
use rust_decimal::{Decimal, dec};
use std::collections::{HashMap, HashSet};
use std::fmt::format;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Инициализация tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("aggregator=info".parse().unwrap())
        )
        .init();

    info!("Starting aggregator...");

    // Запускаем metrics server в отдельном таске
    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9090);
    tokio::spawn(start_metrics_server(metrics_port));
    info!("Metrics server started on port {}", metrics_port);

    // Создаем Redis клиент и publisher
    let redis_client = Arc::new(RedisClient::new());
    let publisher = spawn_publisher(redis_client, 10_000); // буфер на 10k событий

    info!("Publisher started");

    let aggregator = Arc::new(Aggregator::new(publisher));
    let open_positions: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));

    loop {
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for sym in Symbol::get_all_symbols() {
            let aggregator_clone: Arc<Aggregator> = Arc::clone(&aggregator);
            let open_positions_clone = Arc::clone(&open_positions);

            // Каждый символ в отдельном таске
            let handle: JoinHandle<()> = tokio::spawn(async move {
                loop {
                    let spread = aggregator_clone
                        .current_spread_percent(sym.as_ref().to_string())
                        .await;

                    {
                        let mut positions = open_positions_clone.write().await;

                        if positions.contains(sym.as_ref()) {
                            if let Some(spread) = spread {
                                // 0.01%
                                if spread < dec!(0.01) {
                                    positions.remove(sym.as_ref());
                                    let current_ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs();

                                    info!(
                                        "{} spread collapsed to {}% at {}",
                                        sym.as_ref(),
                                        spread.round_dp(3),
                                        current_ts
                                    );

                                    // закрыть позиции здесь
                                }
                            }

                            continue;
                        }
                    }

                    // позиции нет -> ищем вход
                    if let Some(diff) = aggregator_clone
                        .calc_spread_opportunity(sym.as_ref().to_string())
                        .await
                    {
                        let mut positions = open_positions_clone.write().await;

                        if positions.contains(sym.as_ref()) {
                            drop(positions);
                            continue;
                        }

                        // сразу резервируем позицию
                        positions.insert(sym.as_ref().to_string());
                        drop(positions);

                        info!("{} Spread detected: {}", sym.as_ref(), diff);

                        // теперь открываем сделку
                    }
                }
            });

            handles.push(handle);
        }

        // Ждём, чтобы все таски работали бесконечно
        futures::future::join_all(handles).await;
    }
}
