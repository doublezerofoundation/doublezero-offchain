use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{
    types::{Flags, StorageGap},
    Discriminator, PrecomputedDiscriminator,
};
use solana_pubkey::Pubkey;

use crate::types::RingBuffer;

use super::MAX_HISTORY_COUNT;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct MetroHistory {
    /// Metro areas are synonymous with an "exchange," which is how these metro
    /// areas are represented in the Serviceability program on the DoubleZero
    /// Ledger network. The exchange account pubkey on DoubleZero Ledger is this
    /// PDA seed.
    pub exchange_key: Pubkey,

    /// Reserved for future use.
    _flags: Flags,

    pub total_initialized_devices: u16,
    _padding: [u8; 6],

    _gap: StorageGap<4>,

    pub prices: RingBuffer<MetroPrice, { MAX_HISTORY_COUNT as usize }>,
}

impl PrecomputedDiscriminator for MetroHistory {
    const DISCRIMINATOR: Discriminator<8> = Discriminator::new_sha2(b"dz::account::metro_history");
}

impl MetroHistory {
    pub const SEED_PREFIX: &'static [u8] = b"metro_history";

    pub fn find_address(exchange_key: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[Self::SEED_PREFIX, exchange_key.as_ref()], &crate::ID)
    }

    pub fn current_price(&self) -> Option<MetroPrice> {
        self.prices.current_entry().map(|e| e.data)
    }

    pub fn is_history_at_capacity(&self) -> bool {
        self.prices.is_at_capacity()
    }
}

/// Representation of the price of a metro for a given epoch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct MetroPrice {
    /// Whole dollar price (max $65,535).
    pub usdc_price: u16,
    _padding: [u8; 6],

    _gap: StorageGap<2>,
}
