use std::time::{SystemTime, UNIX_EPOCH};
use common::errors::price::PriceError;
use common::models::price;
use crate::SharedState;

pub async fn get_price(state: SharedState, symbol: &str) -> Result<price::PriceInfo, PriceError> {
    let price = state.redis.get_price(symbol).await?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Ok(price::PriceInfo {
        symbol: symbol.to_string(),
        price,
        timestamp,
    })
}