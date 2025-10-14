mod routes;
mod services;
mod utils;

use axum::{
    routing::{get, post},
    Router,
};
use common::models::{price};
use common::RedisClient;
use parking_lot::RwLock;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
use tokio::sync::broadcast;
use tracing_subscriber::FmtSubscriber;
use utils::SharedState;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let (tx, _rx) = broadcast::channel::<price::Price>(16);

    let redis = RedisClient::new();

    let state = Arc::new(utils::AppState {
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
        .route("/book", get(routes::ticker::get_book_handler))
        .route("/create_order", post(routes::order::create_order_handler))
        .route("/price/:symbol", get(routes::ticker::get_price_handler))
        .with_state(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("Listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
