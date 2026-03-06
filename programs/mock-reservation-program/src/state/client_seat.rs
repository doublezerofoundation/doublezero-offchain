use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{Discriminator, PrecomputedDiscriminator};
use solana_pubkey::Pubkey;
use svm_hash::sha2::Hash;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct Flags(u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct ClientSeat {
    pub device_key: Pubkey,
    pub client_ip_bits: u32,

    pub _padding: [u8; 2],
    pub tenure_epochs: u16,

    pub _flags: Flags,

    pub funded_epoch: u64,
    pub active_epoch: u64,
    pub funding_index: u64,

    pub settlement_sort_key: Hash,
}

impl PrecomputedDiscriminator for ClientSeat {
    const DISCRIMINATOR: Discriminator<8> = Discriminator::new_sha2(b"dz::account::client_seat");
}

impl ClientSeat {
    pub const SEED_PREFIX: &'static [u8] = b"client_seat";

    pub fn find_address(device_key: &Pubkey, client_ip_bits: u32) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[
                Self::SEED_PREFIX,
                device_key.as_ref(),
                &client_ip_bits.to_le_bytes(),
            ],
            &crate::ID,
        )
    }
}
