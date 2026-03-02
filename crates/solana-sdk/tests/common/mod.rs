#![allow(dead_code)]

mod admin;
pub mod integration;

use admin::*;
use borsh::BorshSerialize;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

pub const PROGRAM_ID: Pubkey = doublezero_solana_sdk::reservation::ID;

pub const FIRST_EPOCH: u64 = 100;

/// Build an instruction from on-chain account and data types using only
/// standard traits (avoids doublezero_program_tools version conflicts).
fn build_instruction<A: Into<Vec<AccountMeta>>, D: BorshSerialize>(
    accounts: A,
    data: &D,
) -> Instruction {
    Instruction {
        program_id: PROGRAM_ID,
        accounts: accounts.into(),
        data: borsh::to_vec(data).unwrap(),
    }
}
