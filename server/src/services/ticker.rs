use crate::SharedState;
use common::errors::price::PriceError;
use common::models::ticker::Ticker;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn get_price(state: SharedState, symbol: &str) -> Result<Ticker, PriceError> {
    let price = state.redis.get_price(symbol).await?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Ok(Ticker {
        symbol: symbol.to_string(),
        price,
        timestamp,
    })
}
