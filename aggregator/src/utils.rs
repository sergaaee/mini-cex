use common::Exchange;
use common::models::position::{
    AsterClient, BackpackClient, BinanceClient, BybitClient, HibachiClient, PositionManagement,
};

pub fn get_client(exchange: Exchange) -> Box<dyn PositionManagement + Send + Sync> {
    match exchange {
        Exchange::Binance => Box::new(BinanceClient),
        Exchange::Bybit => Box::new(BybitClient),
        Exchange::Backpack => Box::new(BackpackClient),
        Exchange::Aster => Box::new(AsterClient),
        Exchange::Hibachi => Box::new(HibachiClient),
        _ => unimplemented!("Client not implemented for {:?}", exchange),
    }
}
