mod models;
mod price_aggregator;
mod utils;
mod ws;

use crate::models::Aggregator;
use crate::utils::{get_client, send_raw_message};
use common::Exchange;
use common::models::order::Side;
use common::models::symbol::Symbol;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fmt::format;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    console_subscriber::init();
    // создаем Redis клиент
    //let client = common::RedisClient::new();

    let aggregator = Arc::new(Aggregator::new());
    let active_spreads: Arc<RwLock<HashMap<String, Instant>>> =
        Arc::new(RwLock::new(HashMap::new()));

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
            let active_spreads_clone = Arc::clone(&active_spreads);

            // Каждый символ в отдельном таске
            let handle: JoinHandle<()> = tokio::spawn(async move {
                loop {
                    let now = Instant::now();

                    if let Some(diff) = aggregator_clone
                        .calc_spread_opportunity(sym.as_ref().to_string())
                        .await
                    {
                        let mut active = active_spreads_clone.write().await;

                        if !active.contains_key(sym.as_ref()) {
                            // Новый спред появился
                            active.insert(sym.as_ref().to_string(), now);

                            println!("{} Spread detected: {}", sym.as_ref(), diff);
                            drop(active);

                            let long_client = get_client(diff.long_exchange);
                            let short_client = get_client(diff.short_exchange);

                            // Buy и Sell параллельно
                            let open = Instant::now();
                            let qty = match sym {
                                Symbol::BTC => Decimal::from_str_exact("0.001").unwrap(),
                                // Symbol::ETH => Decimal::from_str_exact("0.05").unwrap(),
                                // Symbol::SOL => Decimal::from_str_exact("1").unwrap(),
                                _ => {
                                    panic!("Unknown symbol!")
                                }
                            };

                            if diff.long_exchange == Exchange::Hibachi {
                                long_client
                                    .open_position(
                                        sym.as_ref(),
                                        qty,
                                        Side::Buy,
                                        None,
                                        Some(diff.short_exchange_price),
                                    )
                                    .await
                                    .unwrap();
                            } else {
                                short_client
                                    .open_position(
                                        sym.as_ref(),
                                        qty,
                                        Side::Sell,
                                        None,
                                        Some(diff.long_exchange_price),
                                    )
                                    .await
                                    .unwrap();
                            }
                            let text = format!("Spread detected:\n{}", diff);
                            send_raw_message(text.as_str()).await.unwrap();
                            // let (buy_res, sell_res) = tokio::join!(
                            //     long_client.open_position(sym.as_ref(), qty, Side::Buy),
                            //     short_client.open_position(sym.as_ref(), qty, Side::Sell),
                            //     // long_client.get_position(sym.as_ref()),
                            //     // short_client.get_position(sym.as_ref())
                            // );
                            // let duration_open = open.elapsed();
                            // match (buy_res, sell_res) {
                            //     (Ok(_), Ok(_)) => {}
                            //     (Err(e), Ok(_)) => {
                            //         eprintln!("Buy failed: {e}");
                            //         // закрыть sell позицию
                            //     }
                            //     (Ok(_), Err(e)) => {
                            //         eprintln!("Sell failed: {e}");
                            //         // закрыть buy позицию
                            //     }
                            //     (Err(e1), Err(e2)) => {
                            //         eprintln!("Both failed: {e1}, {e2}");
                            //     }
                            // }
                            // println!("Open duration {} ms", duration_open.as_millis());
                        }
                    } else {
                        // Спред больше не существует — считаем время жизни
                        let mut active = active_spreads_clone.write().await;
                        if let Some(start) = active.remove(sym.as_ref()) {
                            let duration = now.duration_since(start);
                            println!(
                                "Spread for {} lasted {:?} ms",
                                sym.as_ref(),
                                duration.as_millis()
                            );
                        }
                    }
                    //tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });

            handles.push(handle);
        }

        // Ждём, чтобы все таски работали бесконечно
        futures::future::join_all(handles).await;
    }
}
