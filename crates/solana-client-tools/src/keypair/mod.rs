//! Keypair loading module with support for multiple input sources.
//!
//! This module provides flexible keypair loading with the following precedence:
//! 1. CLI argument (`--keypair /path/to/key.json`)
//! 2. Environment variable (`DOUBLEZERO_SOLANA_KEYPAIR` - can be JSON or file path)
//! 3. Stdin (if piped, not a TTY)
//! 4. Default path (`~/.config/solana/id.json`)
//!
//! # Example
//!
//! ```ignore
//! use solana_client_tools::keypair::{load_keypair, KeypairSource, ENV_KEYPAIR};
//! use std::path::PathBuf;
//!
//! let home = home::home_dir().unwrap();
//! let default_path = home.join(".config/solana/id.json");
//!
//! let result = load_keypair(
//!     Some(PathBuf::from("/path/from/cli")),
//!     default_path,
//! );
//!
//! match result {
//!     Ok(result) => {
//!         println!("Loaded keypair from: {}", result.source);
//!     }
//!     Err(e) => eprintln!("Failed to load keypair: {}", e),
//! }
//! ```
//!
//! # Environment Variable
//!
//! The `DOUBLEZERO_SOLANA_KEYPAIR` environment variable can contain either:
//! - A file path: `export DOUBLEZERO_SOLANA_KEYPAIR=/path/to/key.json`
//! - Raw JSON: `export DOUBLEZERO_SOLANA_KEYPAIR='[1,2,3,...,64 bytes]'`
//!
//! The loader auto-detects which format is used.

mod error;
mod loader;
mod source;

pub use error::KeypairLoadError;
pub use loader::{
    ENV_KEYPAIR, KeypairLoadResult, is_keypair_json_content, load_keypair, parse_keypair_json,
};
pub use source::KeypairSource;
