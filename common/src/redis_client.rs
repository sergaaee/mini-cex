use crate::errors::price::PriceError;
use crate::models::ticker::Quote;
use crate::{Exchange, RedisClient, SpreadOpportunity, TradeSignal};
use redis::AsyncCommands;
use redis::streams::StreamMaxlen;

impl RedisClient {
    pub fn new() -> Self {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379/".into());
        let client = redis::Client::open(redis_url).expect("Invalid Redis URL");
        Self { client }
    }

    pub fn from_url(url: &str) -> Self {
        let client = redis::Client::open(url).expect("Invalid Redis URL");
        Self { client }
    }

    /// Получить multiplexed connection (можно переиспользовать)
    pub async fn get_connection(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    pub async fn set_price(&self, symbol: &str, price: &str) -> redis::RedisResult<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("price:{}", symbol);
        conn.set(key, price).await
    }

    pub async fn get_price(&self, symbol: &str) -> Result<String, PriceError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
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
            ("received_at", quote.received_at.to_string()),
        ];

        redis::cmd("XADD")
            .arg(&stream_key)
            .arg(StreamMaxlen::Approx(10000))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }

    /// Publishes a trade signal to the `trades` Redis Stream.
    pub async fn publish_trade_signal(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        signal: &TradeSignal,
    ) -> redis::RedisResult<String> {
        let items: &[(&str, String)] = &[
            ("symbol", signal.symbol.clone()),
            ("long_exchange", signal.long_exchange.to_string()),
            ("long_price", signal.long_price.to_string()),
            ("short_exchange", signal.short_exchange.to_string()),
            ("short_price", signal.short_price.to_string()),
            ("spread_percent", signal.spread_percent.to_string()),
            ("qty", signal.qty.to_string()),
            ("dry_run", signal.dry_run.to_string()),
            ("timestamp", signal.timestamp.to_string()),
        ];

        redis::cmd("XADD")
            .arg("trades")
            .arg(StreamMaxlen::Approx(500))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }

    /// Publishes a spread opportunity to the `spreads` Redis Stream.
    pub async fn publish_spread(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        opportunity: &SpreadOpportunity,
    ) -> redis::RedisResult<String> {
        let items: &[(&str, String)] = &[
            ("symbol", opportunity.symbol.clone()),
            ("long_exchange", opportunity.long_exchange.to_string()),
            ("long_exchange_price", opportunity.long_exchange_price.to_string()),
            ("short_exchange", opportunity.short_exchange.to_string()),
            ("short_exchange_price", opportunity.short_exchange_price.to_string()),
            ("spread_percent", opportunity.spread_percent.to_string()),
            ("size", opportunity.size.to_string()),
            ("timestamp", opportunity.timestamp.to_string()),
        ];

        redis::cmd("XADD")
            .arg("spreads")
            .arg(StreamMaxlen::Approx(1000))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }
}
