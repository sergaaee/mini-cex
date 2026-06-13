use common::{Exchange, RedisClient};
use common::models::ticker::Quote;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceEvent {
    pub symbol: String,
    pub exchange: Exchange,
    pub quote: Quote,
}

#[derive(Clone)]
pub struct PublisherHandle {
    tx: mpsc::Sender<PriceEvent>,
}

impl PublisherHandle {
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

pub fn spawn_publisher(redis_client: Arc<RedisClient>, buffer_size: usize) -> PublisherHandle {
    let (tx, rx) = mpsc::channel::<PriceEvent>(buffer_size);
    tokio::spawn(publisher_worker(redis_client, rx));
    PublisherHandle { tx }
}

async fn publisher_worker(redis_client: Arc<RedisClient>, mut rx: mpsc::Receiver<PriceEvent>) {
    // How many extra events to drain after the first one before sending the pipeline.
    // At ~2600 quotes/sec this typically batches 20-100 events per pipeline call.
    const MAX_DRAIN: usize = 500;

    info!("Publisher worker started");

    let mut conn = loop {
        match redis_client.get_connection().await {
            Ok(c) => break c,
            Err(e) => {
                error!("Failed to connect to Redis: {}, retrying in 1s", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    };

    let mut events_published: u64 = 0;

    loop {
        // Block until at least one event is ready — no busy-waiting at idle.
        let first = match rx.recv().await {
            Some(e) => e,
            None => break,
        };

        // Dedup by (symbol, exchange): last write wins. Intermediate quotes for the
        // same exchange are discarded here because the spread-calculator already keeps
        // only the latest per exchange in its own batch processing.
        let mut batch: HashMap<(String, Exchange), PriceEvent> = HashMap::new();
        batch.insert((first.symbol.clone(), first.exchange), first);

        for _ in 0..MAX_DRAIN {
            match rx.try_recv() {
                Ok(event) => {
                    batch.insert((event.symbol.clone(), event.exchange), event);
                }
                Err(_) => break,
            }
        }

        // Build a single pipeline — one TCP roundtrip for all deduplicated PUBLISHes.
        let mut pipe = redis::pipe();
        for event in batch.values() {
            let channel = format!("prices:{}", event.symbol);
            let payload = serde_json::json!({
                "exchange":    event.exchange.to_string(),
                "bid":         event.quote.bid.to_string(),
                "ask":         event.quote.ask.to_string(),
                "bid_size":    event.quote.bid_size.to_string(),
                "ask_size":    event.quote.ask_size.to_string(),
                "mid":         event.quote.mid.to_string(),
                "timestamp":   event.quote.timestamp,
                "received_at": event.quote.received_at,
            });
            pipe.cmd("PUBLISH").arg(channel).arg(payload.to_string()).ignore();
        }

        let batch_size = batch.len();
        match pipe.query_async::<()>(&mut conn).await {
            Ok(_) => {
                let prev = events_published;
                events_published += batch_size as u64;
                if events_published / 10_000 > prev / 10_000 {
                    info!("Published {} events via pub/sub", events_published);
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to publish batch to Redis");
                match redis_client.get_connection().await {
                    Ok(new_conn) => {
                        conn = new_conn;
                        warn!("Reconnected to Redis after pipeline error");
                    }
                    Err(re) => {
                        error!("Failed to reconnect to Redis: {}", re);
                    }
                }
            }
        }
    }

    info!("Publisher worker stopped, total events: {}", events_published);
}

#[derive(Debug, Default)]
pub struct PublisherMetrics {
    pub events_published: u64,
    pub events_dropped: u64,
    pub errors: u64,
}
