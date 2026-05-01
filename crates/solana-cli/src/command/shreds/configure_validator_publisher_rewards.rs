use anyhow::{Result, bail};
use clap::Args;
use doublezero_solana_client_tools::payer::{SolanaPayerOptions, TransactionOutcome, Wallet};
use doublezero_solana_sdk::{
    shred_subscription::{
        ID,
        instruction::{
            ShredSubscriptionInstructionData,
            account::{
                ConfigureValidatorPublisherRewardsAccounts,
                InitializeValidatorPublisherRewardsAccounts,
            },
        },
        state,
    },
    try_build_instruction,
};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, pubkey::Pubkey};

/*
   doublezero-solana shreds configure-validator-publisher-rewards \
       --rewards-token-mint <MINT> \
       [--rewards-token-owner <OWNER>] \
       [--node-id <NODE_ID>]
*/

#[derive(Debug, Args)]
pub struct ConfigureValidatorPublisherRewardsCommand {
    /// Mint of the protocol-registered shred reward token to receive rewards
    /// in. The mint must already be initialized via `InitializeShredRewardToken`
    /// (admin-only); validators cannot point at arbitrary mints.
    #[arg(long)]
    rewards_token_mint: Pubkey,
    /// Owner of the destination token account that will receive rewards.
    /// Defaults to the payer.
    #[arg(long)]
    rewards_token_owner: Option<Pubkey>,
    /// Validator node identity. Defaults to the payer. The current implementation
    /// requires the node identity to sign the transaction (direct-signer auth),
    /// so this flag must either be omitted or equal the payer.
    #[arg(long)]
    node_id: Option<Pubkey>,

    #[command(flatten)]
    solana_payer_options: SolanaPayerOptions,
}

impl ConfigureValidatorPublisherRewardsCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let dz_connection = self
            .solana_payer_options
            .connection_options
            .clone()
            .into_shred_subscription_connection();
        let mut wallet = Wallet::try_from(self.solana_payer_options)?;
        wallet.connection = dz_connection;
        let wallet_key = wallet.pubkey();

        let node_id = self.node_id.unwrap_or(wallet_key);
        if node_id != wallet_key {
            bail!(
                "--node-id must equal the payer ({wallet_key}); offchain authorization \
                 (e.g. for Ledger or cold-keyed validator identities) is not yet \
                 supported in this CLI",
            );
        }

        let rewards_token_owner_key = self.rewards_token_owner.unwrap_or(wallet_key);

        println!("Shred subscription - Configure Validator Publisher Rewards");
        println!("  Node ID:           {node_id}");
        println!("  Reward token mint: {}", self.rewards_token_mint);
        println!("  Reward token owner: {rewards_token_owner_key}");

        let validator_publisher_rewards_key =
            state::find_validator_publisher_rewards_address(&node_id).0;

        let publisher_rewards_account = wallet
            .connection
            .get_account_with_commitment(
                &validator_publisher_rewards_key,
                solana_commitment_config::CommitmentConfig::confirmed(),
            )
            .await?
            .value;

        let mut instructions = vec![super::build_check_cli_version_instruction()?];
        let mut compute_unit_limit = 5_000u32;

        if publisher_rewards_account.is_none() {
            println!("Initializing validator publisher rewards account...");
            let init_ix = try_build_instruction(
                &ID,
                InitializeValidatorPublisherRewardsAccounts::new(&wallet_key, &node_id),
                &ShredSubscriptionInstructionData::InitializeValidatorPublisherRewards { node_id },
            )?;
            instructions.push(init_ix);
            compute_unit_limit += 50_000;
        }

        let configure_ix = try_build_instruction(
            &ID,
            ConfigureValidatorPublisherRewardsAccounts::new(
                &node_id,
                &self.rewards_token_mint,
                true,
            ),
            &ShredSubscriptionInstructionData::ConfigureValidatorPublisherRewards {
                rewards_token_owner_key,
                offchain_authorization: None,
            },
        )?;
        instructions.push(configure_ix);
        compute_unit_limit += 35_000;

        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            compute_unit_limit,
        ));
        if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
            instructions.push(compute_unit_price_ix.clone());
        }

        let transaction = wallet.new_transaction(&instructions).await?;
        let tx_outcome = wallet.send_or_simulate_transaction(&transaction).await?;

        if let TransactionOutcome::Executed(tx_sig) = tx_outcome {
            println!("Configured validator publisher rewards: {tx_sig}");
            wallet.print_verbose_output(&[tx_sig]).await?;
        }

        Ok(())
    }
}
