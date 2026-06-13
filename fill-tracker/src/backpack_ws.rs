use anyhow::Result;
use bpx_api_client::{BpxClient, BACKPACK_API_BASE_URL};
use bpx_api_client::types::order::{OrderStatus, OrderUpdate, OrderUpdateType};
use crate::tracker::{FillLeg, FillTracker};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub async fn run(tracker: Arc<Mutex<FillTracker>>, secret: String) {
    loop {
        match run_once(&tracker, &secret).await {
            Ok(_) => warn!("Backpack WS stream ended, reconnecting..."),
            Err(e) => error!("Backpack WS error: {e}, reconnecting..."),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn run_once(tracker: &Arc<Mutex<FillTracker>>, secret: &str) -> Result<()> {
    let client = BpxClient::init(BACKPACK_API_BASE_URL.to_string(), secret, None)
        .map_err(|e| anyhow::anyhow!("Backpack client init: {e}"))?;

    let (tx, mut rx) = mpsc::channel::<OrderUpdate>(128);
    let tracker_clone = Arc::clone(tracker);

    let processor = tokio::spawn(async move {
        while let Some(update) = rx.recv().await {
            if matches!(update.event_type, OrderUpdateType::OrderFill)
                && update.order_status == OrderStatus::Filled
                && !update.executed_quantity.is_zero()
            {
                let avg_price = update.executed_quantity_in_quote / update.executed_quantity;
                info!(
                    order_id = %update.order_id,
                    symbol = %update.symbol,
                    avg_price = %avg_price,
                    filled_qty = %update.executed_quantity,
                    "Backpack fill received"
                );
                tracker_clone.lock().unwrap().record_fill(
                    &update.order_id,
                    FillLeg { avg_price, filled_qty: update.executed_quantity },
                );
            }
        }
    });

    info!("Connecting to Backpack account.orderUpdate stream");
    let result = client.subscribe("account.orderUpdate", tx).await;
    processor.abort();
    result.map_err(|e| anyhow::anyhow!("{e}"))
}
