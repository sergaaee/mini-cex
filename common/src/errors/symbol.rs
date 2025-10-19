use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum SymbolError {
    #[error("unsupported symbol")]
    UnsupportedSymbol,
}
