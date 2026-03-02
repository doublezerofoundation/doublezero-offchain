use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{
    types::{Flags, StorageGap},
    Discriminator, PrecomputedDiscriminator,
};
use solana_pubkey::Pubkey;

use crate::types::RingBuffer;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct DeviceHistory {
    pub device_key: Pubkey,

    pub flags: Flags,

    pub metro_exchange_key: Pubkey,

    _gap: StorageGap<4>,

    // Note: If the size of the history ever increases, we should re-initialize
    // the array with the history starting at index 0 and setting the current
    // index at the subscription count. This should be performed during the
    // account migration.
    pub history: RingBuffer<DeviceSubscription, { super::MAX_HISTORY_COUNT as usize }>,
}

impl PrecomputedDiscriminator for DeviceHistory {
    const DISCRIMINATOR: Discriminator<8> = Discriminator::new_sha2(b"dz::account::device_history");
}

impl DeviceHistory {
    pub const SEED_PREFIX: &'static [u8] = b"device_history";

    pub const FLAG_RESERVED_BIT: usize = 0;
    pub const FLAG_IS_ENABLED_BIT: usize = 1;

    pub fn find_address(device_key: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[Self::SEED_PREFIX, device_key.as_ref()], &crate::ID)
    }

    pub fn is_enabled(&self) -> bool {
        self.flags.bit(Self::FLAG_IS_ENABLED_BIT)
    }

    pub fn set_is_enabled(&mut self, is_enabled: bool) {
        self.flags.set_bit(Self::FLAG_IS_ENABLED_BIT, is_enabled);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct DeviceSubscription {
    /// If negative, this value represents a discount. To compute the device
    /// seat price, add this value to the metro price.
    pub usdc_price_premium: i16,

    pub requested_seat_count: u16,

    pub total_available_seats: u16,
    pub granted_seat_count: u16,

    pub usdc_payment_amount: u64,

    _gap: StorageGap<2>,
}
