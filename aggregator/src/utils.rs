use common::Exchange;
use serde_json::json;

pub fn create_subscribe_message(exchange: Exchange, symbol: &str) -> Option<serde_json::Value> {
    match exchange {
        Exchange::Hibachi => Some(json!({
            "method": "subscribe",
            "parameters": { "subscriptions": [{"symbol": format!("{symbol}/USDT-P"), "topic": "ask_bid_price"}] }
        })),
        Exchange::Backpack => Some(
            json!({ "method": "SUBSCRIBE", "params": [format!("bookTicker.{symbol}_USDC_PERP")] }),
        ),
        Exchange::Bybit => Some(json!({
            "op": "subscribe",
            "args": [format!("tickers.{symbol}USDT")]
        })),
        Exchange::BloFin => Some(json!({
            "op": "subscribe",
            "args": [{ "channel": "tickers", "instId": format!("{symbol}-USDT") }]
        })),
        Exchange::OKX => Some(json!({
            "op": "subscribe",
            "args": [{ "channel": "tickers", "instId": format!("{symbol}-USDT-SWAP") }]
        })),
        _ => None,
    }
}
