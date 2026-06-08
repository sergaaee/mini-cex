use lazy_static::lazy_static;
use prometheus::{CounterVec, Opts, Registry};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    /// Incremented every time a spread opportunity is received from Redis.
    pub static ref SPREADS_RECEIVED: CounterVec = CounterVec::new(
        Opts::new("execution_spreads_received_total", "Total spread opportunities received"),
        &["symbol"]
    ).unwrap();

    /// Incremented each time the engine decides to trade.
    pub static ref TRADES_EXECUTED: CounterVec = CounterVec::new(
        Opts::new("execution_trades_total", "Total trades executed"),
        &["symbol", "long_exchange", "short_exchange"]
    ).unwrap();

    /// Incremented when a spread is skipped (below threshold, cooldown, etc.).
    pub static ref TRADES_SKIPPED: CounterVec = CounterVec::new(
        Opts::new("execution_trades_skipped_total", "Total trades skipped"),
        &["symbol", "reason"]
    ).unwrap();
}

pub fn register() {
    REGISTRY.register(Box::new(SPREADS_RECEIVED.clone())).ok();
    REGISTRY.register(Box::new(TRADES_EXECUTED.clone())).ok();
    REGISTRY.register(Box::new(TRADES_SKIPPED.clone())).ok();
}
