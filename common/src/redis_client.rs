use crate::errors::price::PriceError;
use crate::models::ticker::Quote;
use crate::{CloseResult, Exchange, FillResult, PendingClose, PendingFill, RedisClient, SpreadOpportunity, TradeSignal};
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

    fn no_timeout_config() -> redis::AsyncConnectionConfig {
        redis::AsyncConnectionConfig::new()
            .set_connection_timeout(None)
            .set_response_timeout(None)
    }

    /// Получить multiplexed connection (можно переиспользовать)
    pub async fn get_connection(
        &self,
    ) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection_with_config(&Self::no_timeout_config())
            .await
    }

    pub async fn set_price(&self, symbol: &str, price: &str) -> redis::RedisResult<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection_with_config(&Self::no_timeout_config())
            .await?;
        let key = format!("price:{}", symbol);
        conn.set(key, price).await
    }

    pub async fn get_price(&self, symbol: &str) -> Result<String, PriceError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection_with_config(&Self::no_timeout_config())
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
            ("available_size", signal.available_size.to_string()),
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

    pub async fn publish_pending_fill(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        fill: &PendingFill,
    ) -> redis::RedisResult<String> {
        let items: &[(&str, String)] = &[
            ("trade_id", fill.trade_id.clone()),
            ("symbol", fill.symbol.clone()),
            ("long_exchange", fill.long_exchange.to_string()),
            ("long_order_id", fill.long_order_id.clone()),
            ("short_exchange", fill.short_exchange.to_string()),
            ("short_order_id", fill.short_order_id.clone()),
            ("planned_spread_pct", fill.planned_spread_pct.to_string()),
            ("planned_long_price", fill.planned_long_price.to_string()),
            ("planned_short_price", fill.planned_short_price.to_string()),
            ("qty", fill.qty.to_string()),
            ("dry_run", fill.dry_run.to_string()),
            ("timestamp", fill.timestamp.to_string()),
        ];
        redis::cmd("XADD")
            .arg("pending_fills")
            .arg(StreamMaxlen::Approx(1000))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }

    pub async fn publish_pending_close(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        close: &PendingClose,
    ) -> redis::RedisResult<String> {
        let items: &[(&str, String)] = &[
            ("trade_id", close.trade_id.clone()),
            ("symbol", close.symbol.clone()),
            ("long_exchange", close.long_exchange.to_string()),
            ("long_close_order_id", close.long_close_order_id.clone()),
            ("short_exchange", close.short_exchange.to_string()),
            ("short_close_order_id", close.short_close_order_id.clone()),
            ("long_entry_price", close.long_entry_price.to_string()),
            ("short_entry_price", close.short_entry_price.to_string()),
            ("long_qty", close.long_qty.to_string()),
            ("short_qty", close.short_qty.to_string()),
            ("entry_spread_pct", close.entry_spread_pct.to_string()),
            ("dry_run", close.dry_run.to_string()),
            ("timestamp", close.timestamp.to_string()),
        ];
        redis::cmd("XADD")
            .arg("pending_closes")
            .arg(StreamMaxlen::Approx(1000))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }

    pub async fn publish_close_result(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        result: &CloseResult,
    ) -> redis::RedisResult<String> {
        let items: &[(&str, String)] = &[
            ("trade_id", result.trade_id.clone()),
            ("symbol", result.symbol.clone()),
            ("long_exchange", result.long_exchange.to_string()),
            ("long_close_order_id", result.long_close_order_id.clone()),
            ("long_close_avg_price", result.long_close_avg_price.to_string()),
            ("long_entry_price", result.long_entry_price.to_string()),
            ("long_qty", result.long_qty.to_string()),
            ("short_exchange", result.short_exchange.to_string()),
            ("short_close_order_id", result.short_close_order_id.clone()),
            ("short_close_avg_price", result.short_close_avg_price.to_string()),
            ("short_entry_price", result.short_entry_price.to_string()),
            ("short_qty", result.short_qty.to_string()),
            ("entry_spread_pct", result.entry_spread_pct.to_string()),
            ("close_spread_pct", result.close_spread_pct.to_string()),
            ("realized_pnl", result.realized_pnl.to_string()),
            ("long_fee", result.long_fee.to_string()),
            ("short_fee", result.short_fee.to_string()),
            ("dry_run", result.dry_run.to_string()),
            ("timestamp", result.timestamp.to_string()),
        ];
        redis::cmd("XADD")
            .arg("close_results")
            .arg(StreamMaxlen::Approx(500))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }

    pub async fn publish_fill_result(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        result: &FillResult,
    ) -> redis::RedisResult<String> {
        let items: &[(&str, String)] = &[
            ("trade_id", result.trade_id.clone()),
            ("symbol", result.symbol.clone()),
            ("long_exchange", result.long_exchange.to_string()),
            ("long_order_id", result.long_order_id.clone()),
            ("long_avg_price", result.long_avg_price.to_string()),
            ("long_filled_qty", result.long_filled_qty.to_string()),
            ("short_exchange", result.short_exchange.to_string()),
            ("short_order_id", result.short_order_id.clone()),
            ("short_avg_price", result.short_avg_price.to_string()),
            ("short_filled_qty", result.short_filled_qty.to_string()),
            ("planned_spread_pct", result.planned_spread_pct.to_string()),
            ("realized_spread_pct", result.realized_spread_pct.to_string()),
            ("dry_run", result.dry_run.to_string()),
            ("timestamp", result.timestamp.to_string()),
        ];
        redis::cmd("XADD")
            .arg("fills")
            .arg(StreamMaxlen::Approx(500))
            .arg("*")
            .arg(items)
            .query_async(conn)
            .await
    }
}
