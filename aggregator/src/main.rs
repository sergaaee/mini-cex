mod price_aggregator;
mod ws;
mod models;

use tokio::time::Duration;
use common;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    // создаем Redis клиент
    let mut client = common::RedisClient::new();

    let aggregator = models::Aggregator::new();

    loop {
        if let Some(mid) = aggregator.calculate_mid().await {
            println!("Published mid: {}", mid);

            // публикуем в канал "prices"
            let mut attempts = 0;
            while attempts < 5 {
                if let Err(e) = client.set_price("BTC", &mid.to_string()).await {
                    eprintln!("Redis error: {}, retrying...", e);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    attempts += 1;
                } else {
                    break;
                }
            }

        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
