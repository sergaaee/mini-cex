use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

pub mod errors;
pub mod models;
mod redis_client;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
}
