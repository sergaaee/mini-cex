use common::Exchange;
use common::models::position::{
    AsterClient, BackpackClient, BinanceClient, BybitClient, HibachiClient, PositionManagement,
};
use serde_json::json;

pub fn get_client(exchange: Exchange) -> Box<dyn PositionManagement + Send + Sync> {
    match exchange {
        Exchange::Binance => Box::new(BinanceClient),
        Exchange::Bybit => Box::new(BybitClient),
        Exchange::Backpack => Box::new(BackpackClient),
        Exchange::Aster => Box::new(AsterClient),
        Exchange::Hibachi => Box::new(HibachiClient),
        _ => unimplemented!("Client not implemented for {:?}", exchange),
    }
}

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
            "args": [
                {
                    "channel": "tickers",
                    "instId": format!("{symbol}-USDT")
                }
            ]
        })),
        Exchange::OKX => Some(json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": "tickers",
                    "instId": format!("{symbol}-USDT")
                }
            ]
        })),
        _ => None,
    }
}
