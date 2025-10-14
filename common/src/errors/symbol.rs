use crate::models::symbol::Symbol;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SymbolError {
    #[error("unsupported symbol")]
    UnsupportedSymbol,
}

impl FromStr for Symbol {
    type Err = SymbolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BTC" => Ok(Symbol::BTC),
            "ETH" => Ok(Symbol::ETH),
            "SOL" => Ok(Symbol::SOL),
            _ => Err(SymbolError::UnsupportedSymbol),
        }
    }
}
