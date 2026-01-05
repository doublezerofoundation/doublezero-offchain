use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use doublezero_solana_client_tools::{
    payer::{TransactionOutcome, Wallet},
    rpc::{DoubleZeroLedgerConnection, NetworkEnvironment},
};
use doublezero_solana_sdk::{
    environment_2z_token_mint_key,
    revenue_distribution::{
        self, GENESIS_DZ_EPOCH_MAINNET_BETA, ID,
        instruction::{
            RevenueDistributionInstructionData,
            account::{
                EnableSolanaValidatorDebtWriteOffAccounts, InitializeDistributionAccounts,
                InitializeSolanaValidatorDepositAccounts, PaySolanaValidatorDebtAccounts,
                WriteOffSolanaValidatorDebtAccounts,
            },
        },
        state::{self, Distribution, ProgramConfig, SolanaValidatorDeposit},
        types::DoubleZeroEpoch,
    },
    try_build_instruction,
};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, pubkey::Pubkey, signer::Signer};

pub async fn try_initialize_distribution(
    wallet: Wallet,
    dz_env_override: Option<NetworkEnvironment>,
    bypass_dz_epoch_check: bool,
    record_accountant_key: Option<Pubkey>,
) -> Result<()> {
    let network_env = wallet.connection.try_network_environment().await?;

    // Allow an override to the DoubleZero Ledger environment.
    let dz_env = dz_env_override.unwrap_or(network_env);
    let dz_connection = DoubleZeroLedgerConnection::from(dz_env);

    let config = wallet
        .connection
        .try_fetch_zero_copy_data::<ProgramConfig>(&ProgramConfig::find_address().0)
        .await?;

    let record_accountant_key = match record_accountant_key {
        Some(accountant_key) => {
            // Disallow if the accountant key is not used with localnet.
            ensure!(
                network_env.is_localnet(),
                "Cannot specify accountant key with non-localnet network"
            );

            accountant_key
        }
        None => {
            let expected_accountant_key = config.debt_accountant_key;
            ensure!(
                wallet.signer.pubkey() == expected_accountant_key,
                "Signer does not match expected debt accountant"
            );

            expected_accountant_key
        }
    };

    let next_dz_epoch = config.next_completed_dz_epoch;

    // We want to make sure the next DZ epoch is in sync with the last
    // completed DZ epoch.
    if bypass_dz_epoch_check {
        // Disallow if the bypass is not used with localnet.
        ensure!(
            network_env.is_localnet(),
            "Cannot bypass DZ epoch check with non-localnet network"
        );
    } else {
        let expected_completed_dz_epoch = dz_connection
            .get_epoch_info()
            .await?
            .epoch
            .saturating_sub(1);

        // Ensure that the epoch from the DoubleZero Ledger network equals
        // the next one known by the Revenue Distribution program.
        if next_dz_epoch.value() != expected_completed_dz_epoch {
            tracing::warn!(
                "Last completed DZ epoch {expected_completed_dz_epoch} != program's epoch {next_dz_epoch}"
            );
            return Ok(());
        }
    }

    let minimum_epoch_duration_to_finalize_rewards = config
        .checked_minimum_epoch_duration_to_finalize_rewards()
        .context("Minimum epoch duration to finalize rewards not set")?;

    if config.is_debt_write_off_feature_activated() {
        // Try to write off distribution debt for the distribution that will have
        // rewards distributed to network contributors. If rewards were already
        // distributed or all debt is already accounted for, this is a no-op.
        try_write_off_distribution_debt(
            &wallet,
            &dz_connection,
            &record_accountant_key,
            next_dz_epoch,
            minimum_epoch_duration_to_finalize_rewards,
        )
        .await?;
    } else {
        tracing::warn!("Debt write off feature is not activated yet");
    }

    let wallet_key = wallet.pubkey();
    let dz_mint_key = environment_2z_token_mint_key(network_env);

    let initialize_distribution_ix = try_build_instruction(
        &ID,
        InitializeDistributionAccounts::new(&wallet_key, &wallet_key, next_dz_epoch, &dz_mint_key),
        &RevenueDistributionInstructionData::InitializeDistribution,
    )
    .unwrap();

    let mut compute_unit_limit = 24_000;

    let (distribution_key, bump) = Distribution::find_address(next_dz_epoch);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let (_, bump) = state::find_2z_token_pda_address(&distribution_key);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let instructions = vec![
        initialize_distribution_ix,
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
        ComputeBudgetInstruction::set_compute_unit_price(1_000_000), // Land it.
    ];

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

    if let TransactionOutcome::Executed(tx_sig) = tx_sig {
        tracing::info!("Initialize distribution: {tx_sig}");

        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(())
}

//

// TODO: This method may need a rate limiter for account fetches.
async fn try_write_off_distribution_debt(
    wallet: &Wallet,
    dz_ledger_connection: &DoubleZeroLedgerConnection,
    record_accountant_key: &Pubkey,
    next_dz_epoch: DoubleZeroEpoch,
    minimum_epoch_duration_to_finalize_rewards: u32,
) -> Result<()> {
    let wallet_key = wallet.pubkey();

    // Track running deposit balances when we iterate through epochs.
    let mut deposit_balances = HashMap::new();

    let rewards_dz_epoch = next_dz_epoch
        .value()
        .saturating_sub(minimum_epoch_duration_to_finalize_rewards.into())
        .saturating_add(1);
    tracing::info!("Processing debt write-offs affecting epoch {rewards_dz_epoch}");

    let (distribution_key, _) = Distribution::find_address(DoubleZeroEpoch::new(rewards_dz_epoch));
    let mut rewards_distribution = wallet
        .connection
        .try_fetch_zero_copy_data::<Distribution>(&distribution_key)
        .await?;

    if rewards_distribution.is_rewards_calculation_finalized() {
        tracing::info!("Rewards already finalized for epoch {rewards_dz_epoch}");
        return Ok(());
    }

    if rewards_distribution.solana_validator_debt_merkle_root == Default::default() {
        tracing::info!("No debt found for epoch {rewards_dz_epoch}");
        return Ok(());
    }

    // Write-offs will have to terminate if the uncollectible debt exceeds the
    // total debt. This boolean will never be false if the only debt written off
    // is from the same epoch. But for any lingering bad debt, we may have to
    // bail out.
    let mut must_terminate_debt_write_offs = false;

    // Traverse backwards through epochs to write off debt.
    //
    // TODO: We should be able to terminate this loop early if we find that
    // all processed debt is already accounted for. But for now, we will just
    // iterate through all epochs.
    for dz_epoch in (GENESIS_DZ_EPOCH_MAINNET_BETA..=rewards_dz_epoch).rev() {
        if must_terminate_debt_write_offs {
            tracing::warn!(
                "Terminating debt write-offs because uncollectible debt exceeds total debt"
            );
            break;
        }

        let (distribution_key, _) = Distribution::find_address(DoubleZeroEpoch::new(dz_epoch));

        let distribution = if dz_epoch == rewards_dz_epoch {
            rewards_distribution.clone()
        } else {
            wallet
                .connection
                .try_fetch_zero_copy_data::<Distribution>(&distribution_key)
                .await?
        };

        if distribution.is_all_solana_validator_debt_processed() {
            continue;
        }

        let processed_range = distribution.processed_solana_validator_debt_bitmap_range();
        let processed_leaf_data = &distribution.remaining_data[processed_range];

        let (_, computed_debt) = crate::ledger::try_fetch_debt_record(
            dz_ledger_connection,
            record_accountant_key,
            dz_epoch,
            dz_ledger_connection.commitment(),
        )
        .await?;

        let rent_sysvar = wallet
            .connection
            .try_fetch_sysvar::<solana_sdk::rent::Rent>()
            .await?;

        let dz_epoch = DoubleZeroEpoch::new(dz_epoch);

        let mut instructions_and_compute_units = Vec::new();
        let mut pay_count = 0;
        let mut write_off_count = 0;

        for (leaf_index, debt) in computed_debt.debts.iter().enumerate() {
            if revenue_distribution::try_is_processed_leaf(processed_leaf_data, leaf_index).unwrap()
            {
                continue;
            }

            let remaining_sol_debt = rewards_distribution
                .checked_total_sol_debt()
                .unwrap_or_default();

            let node_id = debt.node_id;
            let (deposit_key, deposit_bump) = SolanaValidatorDeposit::find_address(&node_id);

            if let std::collections::hash_map::Entry::Vacant(entry) =
                deposit_balances.entry(node_id)
            {
                let deposit_account_info = wallet
                    .connection
                    .get_account(&deposit_key)
                    .await
                    .unwrap_or_default();

                if deposit_account_info.data.is_empty() {
                    let instruction = try_build_instruction(
                        &ID,
                        InitializeSolanaValidatorDepositAccounts::new(&wallet_key, &node_id),
                        &RevenueDistributionInstructionData::InitializeSolanaValidatorDeposit(
                            node_id,
                        ),
                    )
                    .unwrap();

                    let compute_units = Wallet::compute_units_for_bump_seed(deposit_bump);
                    instructions_and_compute_units.push((instruction, compute_units));
                }

                let deposit_balance = doublezero_solana_client_tools::account::balance(
                    &deposit_account_info,
                    &rent_sysvar,
                );
                entry.insert(deposit_balance);
                tracing::debug!("Fetched deposit balance for node {node_id}: {deposit_balance}");
            }

            let deposit_balance = deposit_balances.get_mut(&node_id).unwrap();

            let (_, proof) = computed_debt.find_debt_proof(&node_id).unwrap();

            if debt.amount == 0 || *deposit_balance >= debt.amount {
                let compute_units =
                    revenue_distribution::compute_unit::pay_solana_validator_debt(&proof);

                let instruction = try_build_instruction(
                    &ID,
                    PaySolanaValidatorDebtAccounts::new(dz_epoch, &node_id),
                    &RevenueDistributionInstructionData::PaySolanaValidatorDebt {
                        amount: debt.amount,
                        proof,
                    },
                )
                .unwrap();

                instructions_and_compute_units.push((instruction, compute_units));

                *deposit_balance -= debt.amount;
                tracing::debug!("Updated deposit balance for node {node_id} to {deposit_balance}");

                pay_count += 1;
            }
            // Only write off debt if there is enough remaining SOL debt to
            // cover the write-off.
            else if debt.amount <= remaining_sol_debt {
                tracing::info!(
                    "Remaining {remaining_sol_debt} debt on rewards epoch {rewards_dz_epoch}. Writing off {} from epoch {dz_epoch}",
                    debt.amount
                );
                if !distribution.is_solana_validator_debt_write_off_enabled()
                    && write_off_count == 0
                {
                    let instruction = try_build_instruction(
                        &ID,
                        EnableSolanaValidatorDebtWriteOffAccounts::new(dz_epoch, &wallet_key),
                        &RevenueDistributionInstructionData::EnableSolanaValidatorDebtWriteOff,
                    )
                    .unwrap();

                    instructions_and_compute_units.push((instruction, 5_000));
                }

                let compute_units =
                    revenue_distribution::compute_unit::write_off_solana_validator_debt(&proof);

                let instruction = try_build_instruction(
                    &ID,
                    WriteOffSolanaValidatorDebtAccounts::new(
                        &wallet_key,
                        dz_epoch,
                        &node_id,
                        DoubleZeroEpoch::new(rewards_dz_epoch),
                    ),
                    &RevenueDistributionInstructionData::WriteOffSolanaValidatorDebt {
                        amount: debt.amount,
                        proof,
                    },
                )
                .unwrap();

                instructions_and_compute_units.push((instruction, compute_units));
                write_off_count += 1;

                // Update the uncollectible debt locally.
                rewards_distribution.mucked_data.uncollectible_sol_debt += debt.amount;
            } else {
                must_terminate_debt_write_offs = true;
            }
        }

        if pay_count == 0 && write_off_count == 0 {
            continue;
        }

        tracing::info!(
            "Epoch {dz_epoch} summary: {pay_count} payments, {write_off_count} write-offs"
        );

        let instruction_batches =
        doublezero_solana_client_tools::transaction::try_batch_instructions_with_common_signers(
            instructions_and_compute_units,
            &[wallet],
            &[],
            false, // allow_compute_price_instruction
        )?;

        for instructions in instruction_batches {
            let transaction = wallet.new_transaction(&instructions).await?;
            let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

            if let TransactionOutcome::Executed(tx_sig) = tx_sig {
                tracing::info!("Process Solana validator debt for epoch {dz_epoch}: {tx_sig}");

                wallet.print_verbose_output(&[tx_sig]).await?;
            }
        }
    }

    Ok(())
}
