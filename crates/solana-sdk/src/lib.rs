pub mod passport;
pub mod revenue_distribution;
pub mod shred_subscription;

//

pub use doublezero_program_tools::{
    DISCRIMINATOR_LEN, Discriminator, PrecomputedDiscriminator, get_program_data_address,
    instruction::try_build_instruction, zero_copy,
};
pub use doublezero_revenue_distribution::DOUBLEZERO_MINT_DECIMALS;
pub use doublezero_sol_conversion_interface as sol_conversion;
pub use doublezero_solana_client_tools::rpc::NetworkEnvironment;
use solana_sdk::instruction::Instruction;
pub use solana_sdk::pubkey::Pubkey;
pub use svm_hash::{merkle, sha2};

// TODO: Determine where to remove this duplicate. Re-export?
pub const fn compute_units_for_bump_seed(bump: u8) -> u32 {
    1_500 * (255 - bump) as u32
}

pub fn environment_2z_token_mint_key(network_env: NetworkEnvironment) -> Pubkey {
    match network_env {
        NetworkEnvironment::Testnet | NetworkEnvironment::Devnet => {
            revenue_distribution::env::development::DOUBLEZERO_MINT_KEY
        }
        _ => revenue_distribution::env::mainnet::DOUBLEZERO_MINT_KEY,
    }
}

pub fn environment_usdc_token_mint_key(network_env: NetworkEnvironment) -> Pubkey {
    match network_env {
        NetworkEnvironment::Testnet => shred_subscription::env::development::USDC_MINT_KEY,
        NetworkEnvironment::Devnet => shred_subscription::env::solana_devnet::USDC_MINT_KEY,
        _ => shred_subscription::env::mainnet::USDC_MINT_KEY,
    }
}

pub fn build_memo_instruction(memo: &[u8]) -> Instruction {
    spl_memo_interface::instruction::build_memo(
        &spl_memo_interface::v3::ID,
        memo,
        Default::default(),
    )
}

// Compute-unit cost of an spl-memo instruction with zero signer accounts. The v3
// program logs the memo with debug formatting, so the cost is a fixed base plus a
// per-byte term. Calibrated against the program in solana-program-test (see
// tests/memo_compute_units.rs): plain-text consumption tracks 1_382 + 352 per
// byte, and these rounded values keep a small margin above that line. Memos with
// bytes that debug-escape to several characters cost more per byte, so this fits
// the printable text memos callers pass, not arbitrary binary input.
const MEMO_CU_BASE: u32 = 2_000;
const MEMO_CU_PER_BYTE: u32 = 400;

pub fn memo_compute_units(memo_len: usize) -> u32 {
    MEMO_CU_BASE + MEMO_CU_PER_BYTE * memo_len as u32
}

pub fn build_memo_instruction_with_compute_units(memo: &[u8]) -> (Instruction, u32) {
    (build_memo_instruction(memo), memo_compute_units(memo.len()))
}
