use serde::Serialize;

pub mod errors;
pub mod models;
mod redis_client;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
}
