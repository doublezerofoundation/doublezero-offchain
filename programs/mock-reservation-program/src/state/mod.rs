mod client_seat;
mod execution_controller;
mod history;
mod payment_escrow;
mod program_config;

pub use client_seat::*;
pub use execution_controller::*;
pub use history::*;
pub use payment_escrow::*;
pub use program_config::*;

use solana_pubkey::Pubkey;

/// USDC decimal places (1 USDC = 1_000_000 atomic units).
pub const USDC_DECIMALS: u32 = 6;

pub const TOKEN_PDA_SEED_PREFIX: &[u8] = b"token";

pub fn find_token_pda_address(token_owner_key: &Pubkey, mint_key: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            TOKEN_PDA_SEED_PREFIX,
            token_owner_key.as_ref(),
            mint_key.as_ref(),
        ],
        &crate::ID,
    )
}
