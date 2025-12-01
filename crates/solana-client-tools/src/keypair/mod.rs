//! Keypair loading module with support for multiple input sources.
//!
//! This module provides flexible keypair loading with the following precedence:
//! 1. CLI argument (`--keypair /path/to/key.json`)
//! 2. Stdin (if piped, not a TTY)
//! 3. Default path (`~/.config/solana/id.json`)
//!
//! # Example
//!
//! ```ignore
//! use solana_client_tools::keypair::{load_keypair, KeypairSource};
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

mod error;
mod loader;
mod source;

pub use error::KeypairLoadError;
pub use loader::{KeypairLoadResult, load_keypair, parse_keypair_json};
pub use source::KeypairSource;
