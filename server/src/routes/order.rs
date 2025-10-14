use crate::services::order::create_order;
use crate::utils::SharedState;
use axum::extract::State;
use axum::Json;
use common::errors::order::OrderError;
use common::models::order;
use serde_json::json;

pub async fn create_order_handler(
    State(state): State<SharedState>,
    Json(req): Json<order::OrderRequest>,
) -> Result<Json<serde_json::Value>, OrderError> {
    let order = create_order(state, req).await?;
    Ok(Json(json!({ "success": true, "order": order })))
}
