use crate::SharedState;
use common::{Order, OrderError, OrderRequest, PriceError, PriceInfo, Side};
use std::time::{SystemTime, UNIX_EPOCH};


pub async fn get_price(state: SharedState, symbol: &str) -> Result<PriceInfo, PriceError> {
    let price = state.redis.get_price(symbol).await?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    Ok(PriceInfo {
        symbol: symbol.to_string(),
        price,
        timestamp,
    })
}

pub async fn create_order(
    state: SharedState,
    order_req: OrderRequest,
) -> Result<Order, OrderError> {
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
    let order = Order::new(
        id,
        order_req.amount,
        order_req.price,
        order_req.side.clone(),
        order_req.order_type.clone(),
        order_req.client_id.clone(),
        time,
    )?;

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

    Ok(order)
}
