#[derive(Debug)]
pub enum PriceError {
    NotFound,
    RedisError(redis::RedisError),
}