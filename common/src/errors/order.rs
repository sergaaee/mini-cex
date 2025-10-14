use crate::errors::symbol::SymbolError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt;
use std::fmt::Display;
use thiserror::Error;

#[derive(Debug, Serialize, Error, Deserialize)]
pub enum OrderError {
    #[error("Amount must be greater than zero")]
    WrongAmount,
    #[error("Price must be greater than zero")]
    WrongPrice,
    #[error("Price precision is too high")]
    PriceBadPrecision,
    #[error("Amount precision is too high")]
    AmountBadPrecision,
}

impl IntoResponse for OrderError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            OrderError::WrongAmount => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Amount must be greater than zero" }),
            ),
            OrderError::WrongPrice => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Price must be greater than zero" }),
            ),
            OrderError::PriceBadPrecision => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Price precision is too high" }),
            ),
            OrderError::AmountBadPrecision => (
                StatusCode::BAD_REQUEST,
                json!({ "success": false, "message": "Amount precision is too high" }),
            ),
        };

        (status, Json(body)).into_response()
    }
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum EngineError {
    Order(#[from] OrderError),

    Symbol(#[from] SymbolError),

    Side(#[from] SideError),

    OrderType(#[from] OrderTypeError),
}

impl Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Order(e) => write!(f, "Order error: {}", e),
            EngineError::Symbol(e) => write!(f, "Symbol error: {}", e),
            EngineError::Side(e) => write!(f, "Side error: {}", e),
            EngineError::OrderType(e) => write!(f, "Order type error: {}", e),
        }
    }
}

impl IntoResponse for EngineError {
    fn into_response(self) -> Response {
        match self {
            EngineError::Order(e) => e.into_response(),
            EngineError::Side(e) => (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "success": false,
                    "message": e.to_string()
                })),
            )
                .into_response(),
            EngineError::Symbol(e) => (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "success": false,
                    "message": e.to_string()
                })),
            )
                .into_response(),
            EngineError::OrderType(e) => (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "success": false,
                    "message": e.to_string()
                })),
            )
                .into_response(),
        }
    }
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SideError {
    #[error("unsupported side")]
    UnsupportedSide,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum OrderTypeError {
    #[error("unsupported order type")]
    UnsupportedOrderType,
}
