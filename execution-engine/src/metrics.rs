use lazy_static::lazy_static;
use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts, Registry};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    pub static ref SPREADS_RECEIVED: CounterVec = CounterVec::new(
        Opts::new("execution_spreads_received_total", "Total spread opportunities received"),
        &["symbol"]
    ).unwrap();

    pub static ref TRADES_EXECUTED: CounterVec = CounterVec::new(
        Opts::new("execution_trades_total", "Total trades executed"),
        &["symbol", "long_exchange", "short_exchange"]
    ).unwrap();

    pub static ref TRADES_SKIPPED: CounterVec = CounterVec::new(
        Opts::new("execution_trades_skipped_total", "Total trades skipped"),
        &["symbol", "reason"]
    ).unwrap();

    /// Round-trip time for each exchange order: from HTTP send to response received (ms).
    pub static ref ORDER_RTT_MS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("execution_order_rtt_ms", "Order HTTP round-trip time per exchange (ms)")
            .buckets(vec![5.0, 10.0, 25.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0]),
        &["exchange", "result"]
    ).unwrap();
}

pub fn register() {
    REGISTRY.register(Box::new(SPREADS_RECEIVED.clone())).ok();
    REGISTRY.register(Box::new(TRADES_EXECUTED.clone())).ok();
    REGISTRY.register(Box::new(TRADES_SKIPPED.clone())).ok();
    REGISTRY.register(Box::new(ORDER_RTT_MS.clone())).ok();
}
