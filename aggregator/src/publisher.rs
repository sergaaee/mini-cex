//! Event publisher module for Redis Streams
//!
//! Масштабируемая архитектура:
//! - WS tasks отправляют события через channel (не блокируются)
//! - Publisher task читает из channel и пишет в Redis Streams батчами
//! - Легко добавить новые типы событий
//! - Можно запустить несколько publisher workers

use common::models::ticker::Quote;
use common::{Exchange, RedisClient};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Событие для публикации в Redis Streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceEvent {
    pub symbol: String,
    pub exchange: Exchange,
    pub quote: Quote,
}

/// Publisher handle - используется для отправки событий
#[derive(Clone)]
pub struct PublisherHandle {
    tx: mpsc::Sender<PriceEvent>,
}

impl PublisherHandle {
    /// Отправить событие в очередь на публикацию
    /// Non-blocking - если буфер полон, событие отбрасывается с warning
    pub fn publish(&self, event: PriceEvent) {
        if let Err(e) = self.tx.try_send(event) {
            match e {
                mpsc::error::TrySendError::Full(evt) => {
                    warn!(
                        symbol = %evt.symbol,
                        exchange = %evt.exchange,
                        "Publisher buffer full, dropping event"
                    );
                }
                mpsc::error::TrySendError::Closed(_) => {
                    error!("Publisher channel closed");
                }
            }
        }
    }
}

/// Запускает publisher worker
/// Возвращает handle для отправки событий
pub fn spawn_publisher(redis_client: Arc<RedisClient>, buffer_size: usize) -> PublisherHandle {
    let (tx, rx) = mpsc::channel::<PriceEvent>(buffer_size);

    tokio::spawn(publisher_worker(redis_client, rx));

    PublisherHandle { tx }
}

/// Publisher worker - читает события из channel и пишет в Redis
async fn publisher_worker(redis_client: Arc<RedisClient>, mut rx: mpsc::Receiver<PriceEvent>) {
    info!("Publisher worker started");

    // Получаем connection один раз и переиспользуем
    let mut conn = match redis_client.get_connection().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to connect to Redis: {}", e);
            return;
        }
    };

    let mut events_published: u64 = 0;

    while let Some(event) = rx.recv().await {
        match redis_client
            .publish_quote(&mut conn, &event.symbol, event.exchange, &event.quote)
            .await
        {
            Ok(_id) => {
                events_published += 1;
                if events_published % 10000 == 0 {
                    info!("Published {} events to Redis Streams", events_published);
                }
                debug!(
                    symbol = %event.symbol,
                    exchange = %event.exchange,
                    bid = %event.quote.bid,
                    ask = %event.quote.ask,
                    "Published quote to stream"
                );
            }
            Err(e) => {
                error!(
                    symbol = %event.symbol,
                    exchange = %event.exchange,
                    error = %e,
                    "Failed to publish quote"
                );
                // Пробуем переподключиться
                match redis_client.get_connection().await {
                    Ok(new_conn) => {
                        conn = new_conn;
                        warn!("Reconnected to Redis");
                    }
                    Err(e) => {
                        error!("Failed to reconnect to Redis: {}", e);
                    }
                }
            }
        }
    }

    info!(
        "Publisher worker stopped, total events: {}",
        events_published
    );
}

/// Метрики для Prometheus (подготовка)
#[derive(Debug, Default)]
pub struct PublisherMetrics {
    pub events_published: u64,
    pub events_dropped: u64,
    pub errors: u64,
}
