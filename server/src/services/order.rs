use crate::SharedState;
use common::errors::order::{EngineError, SideError};
use common::models::order;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn create_order(
    state: SharedState,
    order_req: order::OrderRequest,
) -> Result<order::Order, EngineError> {
    // 1. Генерация уникального ID
    let id = {
        let mut counter = state.id_counter.write();
        let id = *counter;
        *counter += 1;
        id
    };

    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // 2. Создаём ордер
    let order = order::Order::new(
        id,
        order_req.symbol,
        order_req.amount,
        order_req.price,
        order_req.side.clone(),
        order_req.order_type.clone(),
        order_req.client_id.clone(),
        time,
    )?;

    // 3. Добавляем в книгу
    let book = match order.side {
        order::Side::Buy => &state.book_buy,
        order::Side::Sell => &state.book_sell,
        order::Side::Unknown => return Err(SideError::UnsupportedSide.into()),
    };

    let mut book_lock = book.write();
    book_lock
        .entry(order.price)
        .or_insert_with(Vec::new)
        .push(order.clone());

    Ok(order)
}
