// Per-epoch distribute pass run after a successful `configure`. Walks back
// `DISTRIBUTE_LOOKBACK_EPOCHS` recent subscription epochs, fans out S3 leaf
// fetches to find this validator's leaf position per epoch, then batches
// state reads (publisher journals across 2Z/USDC/WSOL, parent
// distributions, claim holdings) into a handful of upfront RPCs so the
// per-epoch loop runs in memory. For each (epoch, journal-mint) where the
// publisher-accumulation bitmap's leaf bit is still set, submits a
// `DistributeValidatorRewards` ix (prepended with `InitializeClaimHolding`
// on the first distribute per epoch when the 2Z claim holding doesn't yet
// exist).
//
// Subscription epoch == Solana epoch in this codebase (the S3 export and
// `ShredDistribution.subscription_epoch` use the same value), so we
// resolve the current epoch via `getEpochInfo` on the connection that
// hosts the program and scan `[current - lookback, current]`.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use doublezero_solana_client_tools::{
    account::zero_copy::ZeroCopyAccountOwnedData,
    payer::{TransactionOutcome, Wallet},
    rpc::NetworkEnvironment,
};
use doublezero_solana_sdk::{
    Pubkey, environment_2z_token_mint_key, environment_usdc_token_mint_key,
    merkle::MerkleProof,
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
        types::ValidatorRewardsLeaf,
    },
    try_build_instruction,
};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, instruction::Instruction};
use spl_associated_token_account_interface::instruction::create_associated_token_account_idempotent;

use super::s3;

/// Subscription-epoch lookback window for the post-configure distribute
/// pass. Sized to fit one `getMultipleAccounts` chunk (100 keys) for the
/// `ShredDistribution` batch; journal batches at 3 mints fit in 3 chunks.
const DISTRIBUTE_LOOKBACK_EPOCHS: u64 = 100;

#[derive(Debug, Default)]
pub struct DistributeOutcome {
    pub successful: u32,
    pub skipped: u32,
    pub failed: u32,
}

/// Per-(epoch, leaf) bundle of everything needed to build a distribute tx
/// after the upfront batched fetches complete.
struct Candidate {
    subscription_epoch: u64,
    associated_dz_epoch: u64,
    leaf_index: usize,
    leaf: ValidatorRewardsLeaf,
    proof: MerkleProof,
}

pub async fn try_distribute_pending(
    wallet: &Wallet,
    node_id: &Pubkey,
    rewards_token_owner_key: &Pubkey,
    rewards_token_mint_key: &Pubkey,
    network_env: NetworkEnvironment,
) -> Result<DistributeOutcome> {
    let current_epoch = wallet
        .connection
        .0
        .get_epoch_info()
        .await
        .context("fetching current epoch")?
        .epoch;
    let from_epoch = current_epoch.saturating_sub(DISTRIBUTE_LOOKBACK_EPOCHS);

    println!("\nScanning epochs {from_epoch}..={current_epoch} for unsettled validator rewards.");

    // Mints we probe each epoch. We don't assume the validator's current
    // VPR mint matches what they were configured against historically;
    // accumulate may have routed earlier-epoch rewards into a different
    // journal. Probing all three covers that case.
    let dz_mint_key = environment_2z_token_mint_key(network_env);
    let usdc_mint_key = environment_usdc_token_mint_key(network_env);
    let wsol_mint_key = spl_token_interface::native_mint::ID;
    let journal_mint_candidates = [dz_mint_key, usdc_mint_key, wsol_mint_key];
    let _ = rewards_token_mint_key; // kept in signature for callers that want
    // to log it; not load-bearing now that we
    // probe all three mints.

    let mut outcome = DistributeOutcome::default();

    // ----- Step 1: batch fetch ShredDistribution accounts in the window -----

    let shred_distribution_pdas: Vec<Pubkey> = (from_epoch..=current_epoch)
        .map(|epoch| find_shred_distribution_address(epoch).0)
        .collect();
    let shred_distribution_accounts = wallet
        .connection
        .try_fetch_multiple_accounts(&shred_distribution_pdas)
        .await
        .context("fetching candidate ShredDistribution accounts")?;

    let accumulated: Vec<(u64, ZeroCopyAccountOwnedData<ShredDistribution>)> =
        shred_distribution_accounts
            .into_iter()
            .enumerate()
            .filter_map(|(offset, account)| {
                if account.data.is_empty() {
                    return None;
                }
                let epoch = from_epoch + offset as u64;
                let shred_distribution: ZeroCopyAccountOwnedData<ShredDistribution> =
                    account.try_into().ok()?;
                shred_distribution
                    .is_validator_rewards_accumulated()
                    .then_some((epoch, shred_distribution))
            })
            .collect();

    if accumulated.is_empty() {
        println!("No accumulated epochs in the window; nothing to distribute.");
        return Ok(outcome);
    }

    // ----- Step 2: fan out S3 to find this validator's leaf per epoch -----

    let s3_client = s3::build_s3_client()?;
    let mut candidates: Vec<Candidate> = Vec::new();
    for (epoch, _shred_distribution) in &accumulated {
        let entries = match s3::fetch_leader_slot_data(&s3_client, *epoch).await {
            Ok(entries) => entries,
            Err(err) => {
                eprintln!("  epoch {epoch}: failed to fetch S3 leaves: {err:#}");
                outcome.failed += 1;
                continue;
            }
        };
        let leaves = match s3::compute_leaves_with_proofs(&entries) {
            Ok(leaves) => leaves,
            Err(err) => {
                eprintln!("  epoch {epoch}: failed to compute merkle proofs: {err:#}");
                outcome.failed += 1;
                continue;
            }
        };
        let Some(leaf_index) = leaves
            .leaves
            .iter()
            .position(|leaf| &leaf.node_id == node_id)
        else {
            // Validator wasn't a leader this epoch; silent skip (no work
            // to do, not an error).
            continue;
        };
        let shred_distribution = accumulated
            .iter()
            .find(|(e, _)| e == epoch)
            .map(|(_, sd)| sd)
            .expect("epoch was just iterated from `accumulated`");
        candidates.push(Candidate {
            subscription_epoch: *epoch,
            associated_dz_epoch: shred_distribution.associated_dz_epoch.value(),
            leaf_index,
            leaf: leaves.leaves[leaf_index],
            proof: leaves.proofs[leaf_index].clone(),
        });
    }

    if candidates.is_empty() {
        println!("No candidate epochs with this validator as a leaf; nothing to distribute.");
        return Ok(outcome);
    }

    // ----- Step 3: batch fetch journals at all three mints per candidate -----
    //
    // Layout: journal_accounts[candidate_idx * 3 + mint_idx]. The internal
    // chunking in `try_fetch_multiple_accounts` keeps a single call under
    // the 100-key getMultipleAccounts limit, so a 100-candidate × 3-mint
    // (= 300-key) batch lands in three RPCs.

    let mut journal_pdas: Vec<Pubkey> = Vec::with_capacity(candidates.len() * 3);
    for candidate in &candidates {
        for mint in &journal_mint_candidates {
            journal_pdas.push(
                find_shred_distribution_journal_address(candidate.subscription_epoch, mint).0,
            );
        }
    }
    let journal_accounts = wallet
        .connection
        .try_fetch_multiple_accounts(&journal_pdas)
        .await
        .context("fetching journal accounts")?;

    // ----- Step 4: batch fetch parent distributions, deduped by DZ epoch -----

    let mut parent_index_for_dz_epoch: HashMap<u64, usize> = HashMap::new();
    let mut parent_pdas: Vec<Pubkey> = Vec::new();
    for candidate in &candidates {
        parent_index_for_dz_epoch
            .entry(candidate.associated_dz_epoch)
            .or_insert_with(|| {
                let pda = ParentDistribution::find_address(DoubleZeroEpoch::new(
                    candidate.associated_dz_epoch,
                ))
                .0;
                parent_pdas.push(pda);
                parent_pdas.len() - 1
            });
    }
    let parent_accounts = wallet
        .connection
        .try_fetch_multiple_accounts(&parent_pdas)
        .await
        .context("fetching parent Distribution accounts")?;

    // ----- Step 5: batch fetch claim holdings (one per candidate) -----

    let claim_holding_pdas: Vec<Pubkey> = candidates
        .iter()
        .map(|candidate| {
            let validator_client_rewards_key =
                find_validator_client_rewards_address(candidate.leaf.client_id).0;
            find_claim_holding_address(
                &validator_client_rewards_key,
                candidate.subscription_epoch,
                &dz_mint_key,
            )
            .0
        })
        .collect();
    let claim_holding_accounts = wallet
        .connection
        .try_fetch_multiple_accounts(&claim_holding_pdas)
        .await
        .context("fetching claim holding accounts")?;

    // ----- Step 6: in-memory decision loop, submit per (epoch, mint) -----
    //
    // `InitializeClaimHolding` is idempotent on-chain but a re-issued init
    // still costs a CPI and tx bytes. Track which epochs we've already
    // emitted an init for in this pass so subsequent (epoch, mint) txs
    // skip it.

    let mut emitted_init_for_epoch: HashSet<u64> = HashSet::new();

    for (candidate_index, candidate) in candidates.iter().enumerate() {
        // Parent distribution gate.
        let parent_index = parent_index_for_dz_epoch[&candidate.associated_dz_epoch];
        let parent_account = &parent_accounts[parent_index];
        if parent_account.data.is_empty() {
            println!(
                "  epoch {}: skipped — parent distribution missing",
                candidate.subscription_epoch
            );
            outcome.skipped += 1;
            continue;
        }
        let parent_distribution: ZeroCopyAccountOwnedData<ParentDistribution> =
            match parent_account.clone().try_into() {
                Ok(data) => data,
                Err(_) => {
                    println!(
                        "  epoch {}: skipped — parent distribution malformed",
                        candidate.subscription_epoch
                    );
                    outcome.skipped += 1;
                    continue;
                }
            };
        if !parent_distribution.is_rewards_calculation_finalized() {
            println!(
                "  epoch {}: skipped — parent rewards not finalized",
                candidate.subscription_epoch
            );
            outcome.skipped += 1;
            continue;
        }

        let claim_holding_exists = !claim_holding_accounts[candidate_index].data.is_empty();
        let mut any_distributed_this_epoch = false;

        for (mint_index, publisher_mint) in journal_mint_candidates.iter().enumerate() {
            let journal_account = &journal_accounts[candidate_index * 3 + mint_index];
            if journal_account.data.is_empty() {
                continue;
            }
            let publisher_journal: ZeroCopyAccountOwnedData<ShredDistributionJournal> =
                match journal_account.clone().try_into() {
                    Ok(data) => data,
                    Err(_) => continue,
                };
            if !publisher_journal.is_swap_complete() {
                continue;
            }
            let Some(bitmap_range) =
                publisher_journal.checked_publisher_accumulation_bitmap_range()
            else {
                continue;
            };
            let bitmap = match publisher_journal
                .remaining_data
                .get(bitmap_range.start..bitmap_range.end)
            {
                Some(slice) => slice,
                None => continue,
            };
            if !bitmap_bit_set(bitmap, candidate.leaf_index) {
                continue;
            }

            let needs_init = !claim_holding_exists
                && !emitted_init_for_epoch.contains(&candidate.subscription_epoch);
            match submit_distribute_tx(
                wallet,
                candidate,
                &publisher_journal,
                publisher_mint,
                rewards_token_owner_key,
                node_id,
                &dz_mint_key,
                needs_init,
            )
            .await
            {
                Ok(()) => {
                    outcome.successful += 1;
                    any_distributed_this_epoch = true;
                    if needs_init {
                        emitted_init_for_epoch.insert(candidate.subscription_epoch);
                    }
                }
                Err(error) => {
                    eprintln!(
                        "  epoch {} mint {publisher_mint}: failed: {error:#}",
                        candidate.subscription_epoch
                    );
                    outcome.failed += 1;
                }
            }
        }

        if !any_distributed_this_epoch {
            outcome.skipped += 1;
        }
    }

    println!(
        "\nDistribute pass complete: {} succeeded, {} skipped, {} failed.",
        outcome.successful, outcome.skipped, outcome.failed,
    );
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
async fn submit_distribute_tx(
    wallet: &Wallet,
    candidate: &Candidate,
    publisher_journal: &ZeroCopyAccountOwnedData<ShredDistributionJournal>,
    publisher_mint_key: &Pubkey,
    rewards_token_owner_key: &Pubkey,
    node_id: &Pubkey,
    dz_mint_key: &Pubkey,
    needs_init: bool,
) -> Result<()> {
    let mut instructions: Vec<Instruction> =
        vec![super::super::build_check_cli_version_instruction()?];

    if needs_init {
        let init_ix = try_build_instruction(
            &ID,
            InitializeClaimHoldingAccounts::new(
                candidate.leaf.client_id,
                candidate.subscription_epoch,
                dz_mint_key,
                &wallet.pubkey(),
            ),
            &ShredSubscriptionInstructionData::InitializeClaimHolding(candidate.subscription_epoch),
        )?;
        instructions.push(init_ix);
    }

    // The destination ATA is at `(rewards_token_owner_key, journal's
    // reward_mint_key)` — which can differ from the validator's currently
    // configured mint when we're distributing from a journal seeded
    // historically against a different mint. `configure` only creates the
    // ATA for the current mint, so we (idempotently) create whatever
    // destination this specific tx needs. No-op when it already exists.
    instructions.push(create_associated_token_account_idempotent(
        &wallet.pubkey(),
        rewards_token_owner_key,
        &publisher_journal.reward_mint_key,
        &spl_token_interface::ID,
    ));

    // Omit-rule: when the validator's publisher mint is already 2Z, the
    // publisher journal also plays the client role. Signal that to the
    // account-builder by passing `None`.
    let client_mint_arg = if publisher_mint_key == dz_mint_key {
        None
    } else {
        Some(dz_mint_key)
    };

    let distribute_ix = try_build_instruction(
        &ID,
        DistributeValidatorRewardsAccountsInitializer {
            subscription_epoch: candidate.subscription_epoch,
            associated_dz_epoch: candidate.associated_dz_epoch,
            node_id,
            client_id: candidate.leaf.client_id,
            rewards_token_owner_key,
            publisher_mint_key,
            publisher_reward_mint_key: &publisher_journal.reward_mint_key,
            client_mint_key: client_mint_arg,
        },
        &ShredSubscriptionInstructionData::DistributeValidatorRewards {
            leader_slots: candidate.leaf.leader_slots,
            proof: candidate.proof.clone(),
        },
    )?;
    instructions.push(distribute_ix);

    // Per-ix headroom: ~30k init_claim_holding (only when `needs_init`),
    // ~25k create_ata_idempotent (no-op if the ATA already exists, but the
    // SPL Token program's existence check still costs some CU), ~150k
    // distribute. Same upper bounds as the admin command's submission loop.
    let cu_limit = if needs_init { 205_000 } else { 175_000 };
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
    if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
        instructions.push(compute_unit_price_ix.clone());
    }

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_outcome = wallet.send_or_simulate_transaction(&transaction).await?;
    if let TransactionOutcome::Executed(tx_sig) = tx_outcome {
        println!(
            "  epoch {} mint {publisher_mint_key}: distributed ({tx_sig})",
            candidate.subscription_epoch
        );
        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(())
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
