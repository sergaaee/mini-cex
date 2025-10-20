mod models;
mod price_aggregator;
mod utils;
mod ws;

use crate::models::Aggregator;
use crate::utils::get_client;
use common::models::order::Side;
use common::models::symbol::Symbol;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    console_subscriber::init();
    // создаем Redis клиент
    //let client = common::RedisClient::new();

    let aggregator = Arc::new(Aggregator::new());

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

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for sym in Symbol::get_all_symbols() {
            let aggregator_clone: Arc<Aggregator> = Arc::clone(&aggregator);
            let sym_clone = sym.clone();

            // Каждый символ в отдельном таске
            let handle: JoinHandle<()> = tokio::spawn(async move {
                loop {
                    if let Some(diff) = aggregator_clone
                        .calc_spread_opportunity(sym_clone.as_ref().to_string())
                        .await
                    {
                        println!("{} Spread detected: {}", sym_clone.as_ref(), diff);

                        let long_client = get_client(diff.long_exchange);
                        let short_client = get_client(diff.short_exchange);

                        // Buy и Sell параллельно
                        let (buy_res, sell_res) = tokio::join!(
                            long_client.open_position(sym_clone.as_ref(), Decimal::ONE, Side::Buy),
                            short_client.open_position(
                                sym_clone.as_ref(),
                                Decimal::ONE,
                                Side::Sell
                            ),
                        );

                        buy_res.unwrap();
                        sell_res.unwrap();
                    }
                }
            });

            handles.push(handle);
        }

        // Ждём, чтобы все таски работали бесконечно
        futures::future::join_all(handles).await;
    }
}
