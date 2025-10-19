use crate::errors::symbol::SymbolError;
use crate::Exchange;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use strum_macros::{AsRefStr, EnumIter};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, EnumIter, AsRefStr)]
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
pub enum Symbol {
    BTC,
    ETH,
    SOL,
    XRP,
    BNB,
    HYPE,
    DOGE,
    ADA,
    LINK,
    ENA,
    LTC,
    XPL,
    SUI,
    ASTER,
    JUP,
    PUMP,
    APT,
    WLFI,
    PENGU,
    NEAR,
    SEI,
    AAVE,
    TAO,
    BERA,
    UNI,
    TIA,
    WIF,
    TON,
    LDO,
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

    pub fn get_all_symbols() -> Vec<Self> {
        Symbol::iter().collect()
    }

    pub fn supported_by(exchange: Exchange) -> Vec<Self> {
        match exchange {
            Exchange::Aster => Self::get_all_symbols(),
            Exchange::Binance => Self::get_all_symbols(),
            Exchange::Backpack => Self::get_all_symbols(),
            Exchange::Bybit => Self::get_all_symbols(),
            Exchange::OKX => Self::get_all_symbols(),
            Exchange::BloFin => vec![Symbol::BTC],
            _ => vec![],
        }
    }
}
