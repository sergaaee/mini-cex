use crate::errors::symbol::SymbolError;
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
}
