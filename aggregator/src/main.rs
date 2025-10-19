mod models;
mod price_aggregator;
mod utils;
mod ws;

use crate::utils::get_client;
use common::models::order::Side;
use common::models::symbol::Symbol;
use rust_decimal::Decimal;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    // создаем Redis клиент
    let client = common::RedisClient::new();

    let aggregator = models::Aggregator::new();

    loop {
        // if let Some(mid) = aggregator.calculate_mid("BTC".to_string()).await {
        //     // println!("Published mid: {}", mid);
        //
        //     // публикуем в канал "prices"
        //     let mut attempts = 0;
        //     while attempts < 5 {
        //         if let Err(e) = client.set_price("BTC", &mid.to_string()).await {
        //             eprintln!("Redis error: {}, retrying...", e);
        //             tokio::time::sleep(Duration::from_millis(200)).await;
        //             attempts += 1;
        //         } else {
        //             break;
        //         }
        //     }
        // }

        for sym in Symbol::get_all_symbols() {
            if let Some(diff) = aggregator
                .calc_spread_opportunity(sym.as_ref().to_string())
                .await
            {
                println!("{} Published diff: {}", sym.as_ref(), diff);
                let long_client = get_client(diff.long_exchange);
                let short_client = get_client(diff.short_exchange);
                let (buy_res, sell_res) = tokio::join!(
                    long_client.open_position(sym.as_ref(), Decimal::ONE, Side::Buy),
                    short_client.open_position(sym.as_ref(), Decimal::ONE, Side::Sell),
                );

                buy_res.unwrap();
                sell_res.unwrap();
            }
        }
    }
}
