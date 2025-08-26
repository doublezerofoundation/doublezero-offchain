use anyhow::{Result, bail};
use doublezero_revenue_distribution::{
    ID as REVENUE_DISTRIBUTION_PROGRAM_ID, instruction::RevenueDistributionInstructionData,
    state::Distribution, types::DoubleZeroEpoch,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use svm_hash::sha2::Hash;
use tracing::{info, warn};

/// Post the contributor rewards merkle root to the revenue distribution program
pub async fn post_rewards_merkle_root(
    rpc_client: &RpcClient,
    payer_signer: &Keypair,
    epoch: u64,
    total_contributors: u32,
    merkle_root: Hash,
) -> Result<()> {
    let program_id = REVENUE_DISTRIBUTION_PROGRAM_ID;

    info!(
        "Posting merkle root for epoch {} with {} contributors to program {}",
        epoch, total_contributors, program_id
    );

    // Derive the Distribution account PDA
    let dz_epoch = DoubleZeroEpoch::new(epoch);
    let (distribution_pubkey, _) = Distribution::find_address(dz_epoch);

    // Check if Distribution account exists
    let distribution_account = rpc_client.get_account(&distribution_pubkey).await;

    if distribution_account.is_err() {
        bail!(
            "Distribution account for epoch {} does not exist at {}. \
            It should be initialized by validator-revenue crate first.",
            epoch,
            distribution_pubkey
        );
    }

    // Build the ConfigureDistributionRewards instruction
    let instruction_data = RevenueDistributionInstructionData::ConfigureDistributionRewards {
        total_contributors,
        merkle_root,
    };

    // Serialize instruction data (includes discriminator)
    let instruction_bytes = borsh::to_vec(&instruction_data)?;

    // Build instruction
    // Account order for ConfigureDistributionRewards:
    // 0: Program config
    // 1: Rewards accountant (signer)
    // 2: Distribution account
    let (program_config_pubkey, _) =
        doublezero_revenue_distribution::state::ProgramConfig::find_address();

    let instruction = Instruction {
        program_id,
        accounts: vec![
            solana_sdk::instruction::AccountMeta::new_readonly(program_config_pubkey, false),
            solana_sdk::instruction::AccountMeta::new_readonly(payer_signer.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(distribution_pubkey, false),
        ],
        data: instruction_bytes,
    };

    // Build and send transaction
    let recent_blockhash = rpc_client.get_latest_blockhash().await?;

    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&payer_signer.pubkey()),
        &[payer_signer],
        recent_blockhash,
    );

    // Send transaction with retries
    match rpc_client.send_and_confirm_transaction(&transaction).await {
        Ok(signature) => {
            info!(
                "Successfully posted merkle root for epoch {} with signature: {}",
                epoch, signature
            );
            Ok(())
        }
        Err(e) => {
            warn!("Failed to post merkle root for epoch {}: {}", epoch, e);
            bail!("Failed to post merkle root: {}", e)
        }
    }
}
