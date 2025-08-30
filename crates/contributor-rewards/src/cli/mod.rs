pub mod demand_strategies;
pub mod export;
pub mod rewards;
pub mod shapley;

use anyhow::Result;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Unified output format for all CLI commands
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
pub enum OutputFormat {
    #[value(name = "csv")]
    Csv,
    #[value(name = "json")]
    Json,
    #[value(name = "json-pretty")]
    JsonPretty,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Csv => write!(f, "csv"),
            Self::Json => write!(f, "json"),
            Self::JsonPretty => write!(f, "json-pretty"),
        }
    }
}

/// Trait for types that can be exported to various formats
pub trait Exportable {
    fn export(&self, format: OutputFormat) -> Result<String>;
}
