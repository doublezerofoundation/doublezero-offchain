use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{types::StorageGap, Discriminator, PrecomputedDiscriminator};
use solana_pubkey::Pubkey;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct ClientSeat {
    pub device_key: Pubkey,
    pub client_ip_bits: u32,

    pub bump_seed: u8,
    pub usdc_token_pda_bump_seed: u8,
    pub tenure_epochs: u16,

    pub requested_epoch: u64,

    pub usdc_funding_account_key: Pubkey,

    _gap: StorageGap<4>,
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
