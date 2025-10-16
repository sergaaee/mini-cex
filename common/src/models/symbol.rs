use crate::errors::symbol::SymbolError;
use crate::Exchange;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Symbol {
    BTC,
    ETH,
    SOL,
    #[serde(other)]
    Unknown,
}

impl Symbol {
    pub fn validate(&self) -> Result<(), SymbolError> {
        match self {
            Symbol::Unknown => Err(SymbolError::UnsupportedSymbol),
            _ => Ok(()),
        }
    }

    pub fn supported_by(exchange: Exchange) -> Vec<Self> {
        match exchange {
            Exchange::Aster => vec![Self::BTC, Self::ETH, Self::SOL],
            Exchange::Binance => vec![Self::BTC, Self::ETH, Self::SOL],
            Exchange::Backpack => vec![Self::BTC, Self::ETH, Self::SOL],
            _ => vec![],
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Symbol::BTC => "BTC",
            Symbol::ETH => "ETH",
            Symbol::SOL => "SOL",
            Symbol::Unknown => "UNKNOWN",
        }
    }
}
