use crate::models::ticker::Quote;
use async_trait::async_trait;
use rust_decimal::Decimal;
use crate::models::order::Side;

#[async_trait]
pub trait PositionManagement {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String>;
    async fn close_position(&self, symbol: &str) -> Result<(), String>;
    async fn get_position(&self, symbol: &str) -> Result<Option<Quote>, String>;
}

#[derive(Debug, Clone)]
pub struct BinanceClient;

#[async_trait]
impl PositionManagement for BinanceClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Binance: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }

    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Binance: closing position {}", symbol);
        Ok(())
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<Quote>, String> {
        // возвращаем фиктивные данные для примера
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct BybitClient;

#[async_trait]
impl PositionManagement for BybitClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Bybit: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Bybit: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<Quote>, String> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct HibachiClient;

#[async_trait]
impl PositionManagement for HibachiClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Hibachi: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Hibachi: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<Quote>, String> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct BackpackClient;

#[async_trait]
impl PositionManagement for BackpackClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Backpack: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Backpack: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<Quote>, String> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct AsterClient;

#[async_trait]
impl PositionManagement for AsterClient {
    async fn open_position(&self, symbol: &str, qty: Decimal, side: Side) -> Result<(), String> {
        println!("Aster: opening {}, quantity: {} {}", side, qty, symbol);
        Ok(())
    }
    async fn close_position(&self, symbol: &str) -> Result<(), String> {
        println!("Aster: closing position {}", symbol);
        Ok(())
    }
    async fn get_position(&self, symbol: &str) -> Result<Option<Quote>, String> {
        Ok(None)
    }
}
