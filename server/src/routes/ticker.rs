use crate::services::ticker::get_price;
use crate::utils::SharedState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use common::errors::price::PriceError;
use rust_decimal::Decimal;
use serde_json::json;

pub async fn get_price_handler(
    State(state): State<SharedState>,
    Path(symbol): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match get_price(state, &symbol).await {
        Ok(info) => (StatusCode::OK, Json(json!(info))),
        Err(PriceError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "symbol": symbol, "error": "price not found" })),
        ),
        Err(PriceError::RedisError(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("redis error: {}", e) })),
        ),
    }
}

pub async fn get_book_handler(State(state): State<SharedState>) -> Json<serde_json::Value> {
    // return top 10 levels
    let buy = state.book_buy.read();
    let sell = state.book_sell.read();

    // For buy, iterate descending
    let top_buy: Vec<_> = buy.iter().rev().take(10).map(|(p, orders)| {
        json!({ "price": p, "size": orders.iter().map(|o| o.amount).sum::<Decimal>() })
    }).collect();

    let top_sell: Vec<_> = sell.iter().take(10).map(|(p, orders)| {
        json!({ "price": p, "size": orders.iter().map(|o| o.amount).sum::<Decimal>() })
    }).collect();

    Json(json!({ "buy": top_buy, "sell": top_sell }))
}
