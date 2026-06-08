//! Prometheus metrics module
//!
//! Экспортирует метрики на HTTP endpoint /metrics
//! Метрики:
//! - price_bid_gauge{symbol, exchange} - текущий bid
//! - price_ask_gauge{symbol, exchange} - текущий ask
//! - price_mid_gauge{symbol, exchange} - текущий mid
//! - price_spread_percent_gauge{symbol} - текущий spread в процентах
//! - events_published_total - количество опубликованных событий
//! - events_dropped_total - количество отброшенных событий

use axum::{Router, routing::get, response::IntoResponse, http::StatusCode};
use lazy_static::lazy_static;
use prometheus::{
    Encoder, GaugeVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::net::SocketAddr;
use tracing::{info, error};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Gauge для цен (можно задавать значение напрямую)
    pub static ref PRICE_BID: GaugeVec = GaugeVec::new(
        Opts::new("price_bid", "Current bid price")
            .namespace("aggregator"),
        &["symbol", "exchange"]
    ).expect("metric can be created");

    pub static ref PRICE_ASK: GaugeVec = GaugeVec::new(
        Opts::new("price_ask", "Current ask price")
            .namespace("aggregator"),
        &["symbol", "exchange"]
    ).expect("metric can be created");

    pub static ref PRICE_MID: GaugeVec = GaugeVec::new(
        Opts::new("price_mid", "Current mid price")
            .namespace("aggregator"),
        &["symbol", "exchange"]
    ).expect("metric can be created");

    pub static ref PRICE_SPREAD_PERCENT: GaugeVec = GaugeVec::new(
        Opts::new("price_spread_percent", "Current spread between exchanges in percent")
            .namespace("aggregator"),
        &["symbol"]
    ).expect("metric can be created");

    // Counters для событий
    pub static ref EVENTS_PUBLISHED: IntCounter = IntCounter::new(
        "aggregator_events_published_total",
        "Total number of events published to Redis Streams"
    ).expect("metric can be created");

    pub static ref EVENTS_DROPPED: IntCounter = IntCounter::new(
        "aggregator_events_dropped_total",
        "Total number of events dropped due to buffer full"
    ).expect("metric can be created");

    pub static ref WS_MESSAGES_RECEIVED: IntCounterVec = IntCounterVec::new(
        Opts::new("ws_messages_received_total", "Total WebSocket messages received")
            .namespace("aggregator"),
        &["exchange"]
    ).expect("metric can be created");
}

/// Регистрирует все метрики в registry
pub fn register_metrics() {
    REGISTRY.register(Box::new(PRICE_BID.clone())).expect("collector registered");
    REGISTRY.register(Box::new(PRICE_ASK.clone())).expect("collector registered");
    REGISTRY.register(Box::new(PRICE_MID.clone())).expect("collector registered");
    REGISTRY.register(Box::new(PRICE_SPREAD_PERCENT.clone())).expect("collector registered");
    REGISTRY.register(Box::new(EVENTS_PUBLISHED.clone())).expect("collector registered");
    REGISTRY.register(Box::new(EVENTS_DROPPED.clone())).expect("collector registered");
    REGISTRY.register(Box::new(WS_MESSAGES_RECEIVED.clone())).expect("collector registered");
}

/// Обновляет метрики цен
pub fn update_price_metrics(symbol: &str, exchange: &str, bid: f64, ask: f64, mid: f64) {
    PRICE_BID.with_label_values(&[symbol, exchange]).set(bid);
    PRICE_ASK.with_label_values(&[symbol, exchange]).set(ask);
    PRICE_MID.with_label_values(&[symbol, exchange]).set(mid);
}

/// Обновляет метрику спреда
pub fn update_spread_metric(symbol: &str, spread_percent: f64) {
    PRICE_SPREAD_PERCENT.with_label_values(&[symbol]).set(spread_percent);
}

/// HTTP handler для /metrics
async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            buffer,
        ),
        Err(e) => {
            error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                format!("Error encoding metrics: {}", e).into_bytes(),
            )
        }
    }
}

/// Health check endpoint
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Запускает HTTP сервер для метрик
pub async fn start_metrics_server(port: u16) {
    register_metrics();

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Starting metrics server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind metrics port");
    axum::serve(listener, app).await.expect("metrics server failed");
}
