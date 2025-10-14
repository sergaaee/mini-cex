use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;
use serde_json::json;

#[derive(Debug)]
pub enum PriceError {
    NotFound,
    RedisError(redis::RedisError),
}

#[derive(Debug, Serialize)]
pub enum OrderError {
    WrongSymbol,
    WrongAmount,
    WrongPrice,
    BadPrecision,
}

impl IntoResponse for OrderError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            OrderError::WrongAmount => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Amount must be greater than zero" }),
            ),
            OrderError::WrongPrice => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Price must be greater than zero" }),
            ),
            OrderError::BadPrecision => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Price precision is too high" }),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "success": false, "message": "Internal server error" }),
            ),
        };

        (status, Json(body)).into_response()
    }
}
