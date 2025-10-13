use redis::AsyncCommands;
use crate::RedisClient;


impl RedisClient {
    pub fn new() -> Self {
        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://redis-aggregator:6379/".into());
        let client = redis::Client::open(redis_url).expect("Invalid Redis URL");
        Self { client }
    }

    pub async fn set_price(&self, symbol: &str, price: &str) -> redis::RedisResult<()> {
        let mut conn = self.client.get_multiplexed_tokio_connection().await?;
        let key = format!("price:{}", symbol);
        conn.set(key, price).await
    }

    pub async fn get_price(&self, symbol: &str) -> Option<String> {
        let mut conn = self.client.get_multiplexed_tokio_connection().await.ok()?;
        let key = format!("price:{}", symbol);
        conn.get(key).await.ok()
    }
}

