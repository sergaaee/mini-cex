use axum::http::StatusCode;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use common::{Order, OrderRequest, Price, Side};
use common::{PriceError, PriceInfo, RedisClient};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
use axum::extract::Path;
use tokio::sync::broadcast;
use tracing_subscriber::FmtSubscriber;

type SharedState = Arc<AppState>;

#[derive(Clone)]
struct AppState {
    price_tx: broadcast::Sender<Price>,
    book_buy: Arc<RwLock<BTreeMap<Decimal, Vec<Order>>>>,
    book_sell: Arc<RwLock<BTreeMap<Decimal, Vec<Order>>>>,
    id_counter: Arc<RwLock<u64>>,
    redis: RedisClient,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let (tx, _rx) = broadcast::channel::<Price>(16);

    let redis = RedisClient::new();

    let state = Arc::new(AppState {
        price_tx: tx.clone(),
        book_buy: Arc::new(RwLock::new(BTreeMap::new())),
        book_sell: Arc::new(RwLock::new(BTreeMap::new())),
        id_counter: Arc::new(RwLock::new(1)),
        redis,
    });

    // слушатель котировок
    {
        let mut rx = state.price_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(price) = rx.recv().await {
                tracing::info!("Internal price update: mid={}", price.mid);
            }
        });
    }

    // маршруты
    let app = Router::new()
        .route("/book", get(get_book))
        .route("/create_order", post(create_order))
        .route("/price/:symbol", get(get_price_handler))
        .with_state(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("Listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// --- Handlers ------------------------------------------------

async fn get_price_handler(
    State(state): State<SharedState>,
    Path(symbol): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    match state.redis.get_price(&symbol).await {
        Ok(price) => (
            StatusCode::OK,
            Json(json!(PriceInfo {
                symbol,
                price,
                timestamp: time,
            })),
        ),
        Err(PriceError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "symbol": symbol, "error": "price not found" })),
        ),
        Err(PriceError::RedisError(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("redis error: {}", e) })),
        ),
    }
}

async fn get_book(State(state): State<SharedState>) -> Json<serde_json::Value> {
    // return top 10 levels
    let buy = state.book_buy.read();
    let sell = state.book_sell.read();

    // For buy, iterate descending
    let top_buy: Vec<_> = buy.iter().rev().take(10).map(|(p, orders)| {
        json!({ "price": p, "size": orders.iter().map(|o| o.amount).sum::<Decimal>() })
    }).collect();

    let top_sell: Vec<_> = sell.iter().take(10).map(|(p, orders)| {
        json!({ "price": p, "size": orders.iter().map(|o| o.amount).sum::<Decimal>() })
    }).collect();

    Json(json!({ "buy": top_buy, "sell": top_sell }))
}

async fn create_order(
    State(state): State<SharedState>,
    Json(req): Json<OrderRequest>,
) -> Json<serde_json::Value> {
    // 1. Генерация уникального ID
    let id = {
        let mut counter = state.id_counter.write();
        let id = *counter;
        *counter += 1;
        id
    };

    // 2. Создаём ордер
    let order = Order {
        id,
        price: req.price,
        amount: req.amount,
        order_type: req.order_type,
        side: req.side,
        client_id: req.client_id,
    };

    // 3. Добавляем в книгу
    let book = match order.side {
        Side::Buy => &state.book_buy,
        Side::Sell => &state.book_sell,
    };

    let mut book_lock = book.write();
    book_lock
        .entry(order.price)
        .or_insert_with(Vec::new)
        .push(order.clone());

    Json(json!({ "status": "ok", "order_id": id }))
}
