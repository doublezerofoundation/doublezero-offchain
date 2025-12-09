pub mod passport;
pub mod revenue_distribution;

pub use doublezero_program_tools::{
    DISCRIMINATOR_LEN, Discriminator, PrecomputedDiscriminator, get_program_data_address,
    instruction::try_build_instruction, zero_copy,
};
pub use svm_hash::{merkle, sha2};

// TODO: Determine where to remove this duplicate. Re-export?
pub const fn compute_units_for_bump_seed(bump: u8) -> u32 {
    1_500 * (255 - bump) as u32
}
