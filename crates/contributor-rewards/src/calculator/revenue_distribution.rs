use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use doublezero_program_tools::{instruction::try_build_instruction, zero_copy};
use doublezero_revenue_distribution::{
    ID as REVENUE_DISTRIBUTION_PROGRAM_ID,
    instruction::{
        RevenueDistributionInstructionData, account::ConfigureDistributionRewardsAccounts,
    },
    state::Distribution,
    types::DoubleZeroEpoch,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::{VersionedMessage, v0::Message},
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use svm_hash::sha2::Hash;
use tokio::time::sleep;
use tracing::{info, warn};

/// Check if calculation is allowed for a given distribution based on current timestamp
fn check_calculation_allowed(distribution: &Distribution) -> Result<bool> {
    let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

    let is_allowed = distribution
        .checked_calculation_allowed_timestamp()
        .is_some_and(|allowed_timestamp| current_timestamp >= allowed_timestamp);

    Ok(is_allowed)
}

/// Wait for the grace period to expire before posting merkle root
/// Returns the Distribution account data for reuse
async fn wait_for_grace_period(
    rpc_client: &RpcClient,
    epoch: u64,
    max_wait_seconds: u64,
) -> Result<Distribution> {
    let dz_epoch = DoubleZeroEpoch::new(epoch);
    let (distribution_pubkey, _) = Distribution::find_address(dz_epoch);

    info!(
        "Checking grace period for epoch {} at address {}",
        epoch, distribution_pubkey
    );

    // Fetch Distribution account
    let distribution_account = rpc_client
        .get_account_with_commitment(&distribution_pubkey, CommitmentConfig::confirmed())
        .await?
        .value
        .ok_or_else(|| {
            anyhow!(
                "Distribution account for epoch {} does not exist at {}. \
                It should be initialized by validator-debt crate first.",
                epoch,
                distribution_pubkey
            )
        })?;

    // Deserialize Distribution
    let distribution = zero_copy::checked_from_bytes_with_discriminator::<Distribution>(
        &distribution_account.data,
    )
    .ok_or_else(|| anyhow!("Failed to deserialize Distribution for epoch {}", epoch))?
    .0;

    // Poll until grace period is satisfied
    let max_wait = Duration::from_secs(max_wait_seconds);
    let poll_interval = Duration::from_secs(60);
    let start = Instant::now();

    loop {
        if check_calculation_allowed(distribution)? {
            info!(
                "Grace period satisfied for epoch {} after waiting {:?}",
                epoch,
                start.elapsed()
            );
            return Ok(*distribution);
        }

        if start.elapsed() >= max_wait {
            bail!(
                "Exceeded max wait time ({:?}) for grace period on epoch {}",
                max_wait,
                epoch
            );
        }

        if let Some(allowed_timestamp) = distribution.checked_calculation_allowed_timestamp() {
            let current_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            let wait_seconds = allowed_timestamp - current_timestamp;

            warn!(
                "Calculation grace period not satisfied for epoch {}. Waiting approximately {} more seconds (elapsed: {:?})",
                epoch,
                wait_seconds.max(0),
                start.elapsed()
            );
        }

        sleep(poll_interval).await;
    }
}

/// Post the contributor rewards merkle root to the revenue distribution program
pub async fn post_rewards_merkle_root(
    rpc_client: &RpcClient,
    payer_signer: &Keypair,
    epoch: u64,
    total_contributors: u32,
    merkle_root: Hash,
    max_wait_seconds: u64,
) -> Result<()> {
    info!(
        "Posting merkle root for epoch {} with {} contributors to program {}",
        epoch, total_contributors, REVENUE_DISTRIBUTION_PROGRAM_ID
    );

    // Wait for grace period and get Distribution account (validates existence and grace period)
    let _distribution = wait_for_grace_period(rpc_client, epoch, max_wait_seconds).await?;

    let dz_epoch = DoubleZeroEpoch::new(epoch);

    // Build the ConfigureDistributionRewards instruction with the helper
    let ix_data = RevenueDistributionInstructionData::ConfigureDistributionRewards {
        total_contributors,
        merkle_root,
    };

    let accounts = ConfigureDistributionRewardsAccounts::new(&payer_signer.pubkey(), dz_epoch);

    let ix = try_build_instruction(&REVENUE_DISTRIBUTION_PROGRAM_ID, accounts, &ix_data)?;

    // Build versioned transaction
    let recent_blockhash = rpc_client.get_latest_blockhash().await?;

    let message = Message::try_compile(&payer_signer.pubkey(), &[ix], &[], recent_blockhash)?;

    let transaction =
        VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer_signer])?;

    // Send transaction
    rpc_client
        .send_and_confirm_transaction(&transaction)
        .await
        .map(|signature| {
            info!(
                "Successfully posted merkle root for epoch {} with signature: {}",
                epoch, signature
            );
        })
        .map_err(|e| anyhow!("Failed to post merkle root for epoch {epoch}: {e}"))
}
