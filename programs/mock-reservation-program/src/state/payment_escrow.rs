use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{types::StorageGap, Discriminator, PrecomputedDiscriminator};
use solana_pubkey::Pubkey;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct PaymentEscrow {
    pub client_seat_key: Pubkey,
    pub withdraw_authority_key: Pubkey,
    pub usdc_balance: u64,
    _gap: StorageGap<2>,
}

impl PrecomputedDiscriminator for PaymentEscrow {
    const DISCRIMINATOR: Discriminator<8> =
        Discriminator::new_sha2(b"dz::account::payment_escrow");
}

impl PaymentEscrow {
    pub const SEED_PREFIX: &'static [u8] = b"payment_escrow";

    pub fn find_address(
        client_seat_key: &Pubkey,
        withdraw_authority_key: &Pubkey,
    ) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[
                Self::SEED_PREFIX,
                client_seat_key.as_ref(),
                withdraw_authority_key.as_ref(),
            ],
            &crate::ID,
        )
    }
}
