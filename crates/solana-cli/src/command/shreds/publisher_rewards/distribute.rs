// Per-epoch distribute pass run after a successful `configure`. Walks back N
// recent subscription epochs, identifies which ones have this validator's
// rewards still pending, and submits a `DistributeValidatorRewards`
// instruction for each (prepended with `InitializeClaimHolding` when the
// per-(epoch, mint=2Z) claim holding account does not yet exist).
//
// Subscription epoch == Solana epoch in this codebase (the S3 export and
// `ShredDistribution.subscription_epoch` use the same value), so we resolve
// the current epoch via `getEpochInfo` on the connection that hosts the
// program and scan `[current - lookback, current]`.

use anyhow::{Context, Result};
use doublezero_solana_client_tools::{
    account::zero_copy::ZeroCopyAccountOwnedData,
    payer::{TransactionOutcome, Wallet},
    rpc::NetworkEnvironment,
};
use doublezero_solana_sdk::{
    Pubkey, environment_2z_token_mint_key,
    revenue_distribution::{state::Distribution as ParentDistribution, types::DoubleZeroEpoch},
    shred_subscription::{
        ID,
        instruction::{
            ShredSubscriptionInstructionData,
            account::{
                DistributeValidatorRewardsAccountsInitializer, InitializeClaimHoldingAccounts,
            },
        },
        state::{
            ShredDistribution, ShredDistributionJournal, find_claim_holding_address,
            find_shred_distribution_address, find_shred_distribution_journal_address,
            find_validator_client_rewards_address,
        },
    },
    try_build_instruction,
};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, instruction::Instruction};

use super::s3;

/// Subscription-epoch lookback window for the post-configure distribute pass.
/// Wider than typical "validator just rejoined" gaps; bounded so the S3 fan-out
/// stays sane.
const DISTRIBUTE_LOOKBACK_EPOCHS: u64 = 20;

#[derive(Debug, Default)]
pub struct DistributeOutcome {
    pub successful: u32,
    pub skipped: u32,
    pub failed: u32,
}

pub async fn try_distribute_pending(
    wallet: &Wallet,
    node_id: &Pubkey,
    rewards_token_owner_key: &Pubkey,
    rewards_token_mint_key: &Pubkey,
    network_env: NetworkEnvironment,
) -> Result<DistributeOutcome> {
    let lookback = DISTRIBUTE_LOOKBACK_EPOCHS;
    // The latest in-flight epoch may not have accumulated yet; the
    // `is_validator_rewards_accumulated()` check below filters those out.
    let current_epoch = wallet
        .connection
        .0
        .get_epoch_info()
        .await
        .context("fetching current epoch")?
        .epoch;
    let from_epoch = current_epoch.saturating_sub(lookback);

    println!("\nScanning epochs {from_epoch}..={current_epoch} for unsettled validator rewards.");

    let candidate_pdas = (from_epoch..=current_epoch)
        .map(|epoch| find_shred_distribution_address(epoch).0)
        .collect::<Vec<_>>();
    let shred_distributions = wallet
        .connection
        .try_fetch_multiple_accounts(&candidate_pdas)
        .await
        .context("fetching candidate ShredDistribution accounts")?;

    let dz_mint_key = environment_2z_token_mint_key(network_env);
    let s3_client = s3::build_s3_client()?;

    let mut outcome = DistributeOutcome::default();
    for (offset, account) in shred_distributions.into_iter().enumerate() {
        let epoch = from_epoch + offset as u64;
        if account.data.is_empty() {
            continue;
        }
        let shred_distribution: ZeroCopyAccountOwnedData<ShredDistribution> =
            match account.try_into() {
                Ok(data) => data,
                Err(_) => continue,
            };
        if !shred_distribution.is_validator_rewards_accumulated() {
            continue;
        }

        match try_distribute_one_epoch(
            wallet,
            &s3_client,
            epoch,
            &shred_distribution,
            node_id,
            rewards_token_owner_key,
            rewards_token_mint_key,
            &dz_mint_key,
        )
        .await
        {
            Ok(EpochOutcome::Distributed) => outcome.successful += 1,
            Ok(EpochOutcome::Skipped(reason)) => {
                println!("  epoch {epoch}: skipped — {reason}");
                outcome.skipped += 1;
            }
            Err(error) => {
                eprintln!("  epoch {epoch}: failed: {error:#}");
                outcome.failed += 1;
            }
        }
    }

    println!(
        "\nDistribute pass complete: {} succeeded, {} skipped, {} failed.",
        outcome.successful, outcome.skipped, outcome.failed,
    );
    Ok(outcome)
}

enum EpochOutcome {
    Distributed,
    Skipped(&'static str),
}

#[allow(clippy::too_many_arguments)]
async fn try_distribute_one_epoch(
    wallet: &Wallet,
    s3_client: &reqwest::Client,
    subscription_epoch: u64,
    shred_distribution: &ShredDistribution,
    node_id: &Pubkey,
    rewards_token_owner_key: &Pubkey,
    rewards_token_mint_key: &Pubkey,
    dz_mint_key: &Pubkey,
) -> Result<EpochOutcome> {
    // Pull S3 leaves for this epoch. The merkle tree is built from the full
    // leaf set; per-leaf proofs are positional, so the index in the sorted
    // vec is the same as the on-chain `leaf_index`.
    let entries = s3::fetch_leader_slot_data(s3_client, subscription_epoch)
        .await
        .with_context(|| format!("fetching S3 leaves for epoch {subscription_epoch}"))?;
    let leaves_with_proofs = s3::compute_leaves_with_proofs(&entries)
        .with_context(|| format!("computing merkle proofs for epoch {subscription_epoch}"))?;

    let Some(leaf_index) = leaves_with_proofs
        .leaves
        .iter()
        .position(|leaf| &leaf.node_id == node_id)
    else {
        return Ok(EpochOutcome::Skipped("no leaf for this validator"));
    };
    let leaf = leaves_with_proofs.leaves[leaf_index];
    let proof = leaves_with_proofs.proofs[leaf_index].clone();

    // Publisher journal is at (subscription_epoch, vpr.rewards_token_mint_key).
    // If it doesn't exist, accumulate never routed this validator's leaf here
    // — nothing to distribute under the current configuration.
    let publisher_journal_key =
        find_shred_distribution_journal_address(subscription_epoch, rewards_token_mint_key).0;
    let parent_distribution_key = ParentDistribution::find_address(DoubleZeroEpoch::new(
        shred_distribution.associated_dz_epoch.value(),
    ))
    .0;

    let prefetch = wallet
        .connection
        .try_fetch_multiple_accounts(&[publisher_journal_key, parent_distribution_key])
        .await
        .context("fetching journal/parent")?;
    let [publisher_journal_account, parent_distribution_account] =
        <[_; 2]>::try_from(prefetch).expect("requested 2 accounts");
    if publisher_journal_account.data.is_empty() {
        return Ok(EpochOutcome::Skipped(
            "no publisher journal for this mint at this epoch",
        ));
    }
    let publisher_journal: ZeroCopyAccountOwnedData<ShredDistributionJournal> =
        match publisher_journal_account.try_into() {
            Ok(data) => data,
            Err(_) => return Ok(EpochOutcome::Skipped("publisher journal malformed")),
        };
    if !publisher_journal.is_swap_complete() {
        return Ok(EpochOutcome::Skipped("publisher journal swap incomplete"));
    }
    if parent_distribution_account.data.is_empty() {
        return Ok(EpochOutcome::Skipped("parent distribution missing"));
    }
    let parent_distribution: ZeroCopyAccountOwnedData<ParentDistribution> =
        match parent_distribution_account.try_into() {
            Ok(data) => data,
            Err(_) => return Ok(EpochOutcome::Skipped("parent distribution malformed")),
        };
    if !parent_distribution.is_rewards_calculation_finalized() {
        return Ok(EpochOutcome::Skipped("parent rewards not finalized"));
    }

    // Check the publisher accumulation bitmap. A SET bit means accumulate
    // routed here and distribute has not yet run; a CLEAR bit means either
    // accumulate didn't route here (different mint) or distribute already
    // ran (replay).
    let Some(bitmap_range) = publisher_journal.checked_publisher_accumulation_bitmap_range() else {
        return Ok(EpochOutcome::Skipped("journal bitmap not allocated"));
    };
    let journal_account_data = wallet
        .connection
        .0
        .get_account_data(&publisher_journal_key)
        .await
        .context("re-reading journal data for bitmap")?;
    let inline_data_offset =
        doublezero_solana_sdk::DISCRIMINATOR_LEN + std::mem::size_of::<ShredDistributionJournal>();
    let bitmap = journal_account_data
        .get(inline_data_offset + bitmap_range.start..inline_data_offset + bitmap_range.end)
        .context("journal data shorter than declared bitmap range")?;
    if !bitmap_bit_set(bitmap, leaf_index) {
        return Ok(EpochOutcome::Skipped(
            "leaf bit clear (already distributed or routed elsewhere)",
        ));
    }

    // Determine whether the client claim holding exists. If not, prepend
    // the `InitializeClaimHolding` ix in the same tx.
    let validator_client_rewards_key = find_validator_client_rewards_address(leaf.client_id).0;
    let client_claim_holding_key = find_claim_holding_address(
        &validator_client_rewards_key,
        subscription_epoch,
        dz_mint_key,
    )
    .0;
    let needs_claim_holding_init = wallet
        .connection
        .0
        .get_account(&client_claim_holding_key)
        .await
        .is_err();

    let mut instructions: Vec<Instruction> =
        vec![super::super::build_check_cli_version_instruction()?];

    if needs_claim_holding_init {
        let init_claim_holding_ix = try_build_instruction(
            &ID,
            InitializeClaimHoldingAccounts::new(
                leaf.client_id,
                subscription_epoch,
                dz_mint_key,
                &wallet.pubkey(),
            ),
            &ShredSubscriptionInstructionData::InitializeClaimHolding(subscription_epoch),
        )?;
        instructions.push(init_claim_holding_ix);
    }

    // The publisher-and-client-same-mint case fires the omit-rule and
    // collapses to a single journal account. We model that by passing
    // `None` for `client_mint_key` when our publisher mint is already 2Z.
    let client_mint_arg = if rewards_token_mint_key == dz_mint_key {
        None
    } else {
        Some(dz_mint_key)
    };
    let distribute_ix = try_build_instruction(
        &ID,
        DistributeValidatorRewardsAccountsInitializer {
            subscription_epoch,
            associated_dz_epoch: shred_distribution.associated_dz_epoch.value(),
            node_id,
            client_id: leaf.client_id,
            rewards_token_owner_key,
            publisher_mint_key: rewards_token_mint_key,
            publisher_reward_mint_key: &publisher_journal.reward_mint_key,
            client_mint_key: client_mint_arg,
        },
        &ShredSubscriptionInstructionData::DistributeValidatorRewards {
            leader_slots: leaf.leader_slots,
            proof,
        },
    )?;
    instructions.push(distribute_ix);

    // ~30k CU per ix (init_claim_holding + distribute) — both upper bounds
    // observed in the admin command's submission loop.
    let cu_limit = if needs_claim_holding_init {
        180_000
    } else {
        150_000
    };
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
    if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
        instructions.push(compute_unit_price_ix.clone());
    }

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_outcome = wallet.send_or_simulate_transaction(&transaction).await?;
    if let TransactionOutcome::Executed(tx_sig) = tx_outcome {
        println!("  epoch {subscription_epoch}: distributed ({tx_sig})");
        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(EpochOutcome::Distributed)
}

fn bitmap_bit_set(bitmap: &[u8], leaf_index: usize) -> bool {
    let byte_idx = leaf_index / 8;
    let bit_idx = leaf_index % 8;
    bitmap
        .get(byte_idx)
        .map(|b| (b >> bit_idx) & 1 == 1)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_bit_set_in_range() {
        let bitmap = vec![0b0000_1001, 0b0000_0010];
        assert!(bitmap_bit_set(&bitmap, 0));
        assert!(!bitmap_bit_set(&bitmap, 1));
        assert!(bitmap_bit_set(&bitmap, 3));
        assert!(!bitmap_bit_set(&bitmap, 8));
        assert!(bitmap_bit_set(&bitmap, 9));
    }

    #[test]
    fn test_bitmap_bit_set_out_of_range() {
        let bitmap = vec![0xff];
        assert!(!bitmap_bit_set(&bitmap, 8));
        assert!(!bitmap_bit_set(&bitmap, 1_000));
    }

    #[test]
    fn test_bitmap_bit_set_empty() {
        assert!(!bitmap_bit_set(&[], 0));
    }
}
