use crate::errors::price::PriceError;
use crate::models::ticker::Quote;
use crate::{Exchange, RedisClient};
use redis::AsyncCommands;
use redis::streams::StreamMaxlen;

impl RedisClient {
    pub fn new() -> Self {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379/".into());
        let client = redis::Client::open(redis_url).expect("Invalid Redis URL");
        Self { client }
    }

    /// Получить multiplexed connection (можно переиспользовать)
    pub async fn get_connection(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_tokio_connection().await
    }

    pub async fn set_price(&self, symbol: &str, price: &str) -> redis::RedisResult<()> {
        let mut conn = self.client.get_multiplexed_tokio_connection().await?;
        let key = format!("price:{}", symbol);
        conn.set(key, price).await
    }

    pub async fn get_price(&self, symbol: &str) -> Result<String, PriceError> {
        let mut conn = self
            .client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(PriceError::RedisError)?;

        let key = format!("price:{}", symbol);
        match conn.get::<_, Option<String>>(key).await {
            Ok(Some(price)) => Ok(price),
            Ok(None) => Err(PriceError::NotFound),
            Err(e) => Err(PriceError::RedisError(e)),
        }
    }

    /// Публикует quote в Redis Stream
    /// Stream key: `prices:{symbol}`
    /// Fields: exchange, bid, ask, bid_size, ask_size, mid, timestamp
    pub async fn publish_quote(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        symbol: &str,
        exchange: Exchange,
        quote: &Quote,
    ) -> redis::RedisResult<String> {
        let stream_key = format!("prices:{}", symbol);

        // XADD с автоматическим ID (*) и MAXLEN ~10000 для ограничения размера
        let items: &[(&str, String)] = &[
            ("exchange", exchange.to_string()),
            ("bid", quote.bid.to_string()),
            ("ask", quote.ask.to_string()),
            ("bid_size", quote.bid_size.to_string()),
            ("ask_size", quote.ask_size.to_string()),
            ("mid", quote.mid.to_string()),
            ("timestamp", quote.timestamp.to_string()),
        ];

        redis::cmd("XADD")
            .arg(&stream_key)
            .arg(StreamMaxlen::Approx(10000))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }
}
