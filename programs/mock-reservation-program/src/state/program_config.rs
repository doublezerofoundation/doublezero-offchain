use bytemuck::{Pod, Zeroable};
use doublezero_program_tools::{types::Flags, Discriminator, PrecomputedDiscriminator};
use doublezero_revenue_distribution::types::UnitShare16;
use solana_pubkey::Pubkey;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Pod, Zeroable)]
#[repr(C, align(8))]
pub struct ProgramConfig {
    pub flags: Flags,

    pub admin_key: Pubkey,

    pub closed_for_requests_grace_period_slots: u32,

    pub usdc_2z_max_slippage_bps: UnitShare16,

    pub usdc_2z_conversion_grace_period_epochs: u8,
    _padding: [u8; 1],

    pub oracle_key: Pubkey,
    pub usdc_2z_oracle_key: Pubkey,

    /// Configurable USDC mint (differs between mainnet and testnet).
    pub usdc_mint_key: Pubkey,
}

impl PrecomputedDiscriminator for ProgramConfig {
    const DISCRIMINATOR: Discriminator<8> = Discriminator::new_sha2(b"dz::account::program_config");
}

impl ProgramConfig {
    pub const SEED_PREFIX: &'static [u8] = b"program_config";

    pub const FLAG_IS_PAUSED_BIT: usize = 0;
    pub const FLAG_IS_MIGRATED_BIT: usize = 1;

    pub fn find_address() -> (Pubkey, u8) {
        Pubkey::find_program_address(&[Self::SEED_PREFIX], &crate::ID)
    }

    pub fn is_paused(&self) -> bool {
        self.flags.bit(Self::FLAG_IS_PAUSED_BIT)
    }

    pub fn set_is_paused(&mut self, should_pause: bool) {
        self.flags.set_bit(Self::FLAG_IS_PAUSED_BIT, should_pause);
    }

    pub fn is_migrated(&self) -> bool {
        self.flags.bit(Self::FLAG_IS_MIGRATED_BIT)
    }

    pub fn set_is_migrated(&mut self, should_migrate: bool) {
        self.flags
            .set_bit(Self::FLAG_IS_MIGRATED_BIT, should_migrate);
    }

    pub fn closed_for_requests_grace_period_slots(&self) -> Option<u64> {
        if self.closed_for_requests_grace_period_slots == 0 {
            return None;
        }

        Some(self.closed_for_requests_grace_period_slots.into())
    }
}
