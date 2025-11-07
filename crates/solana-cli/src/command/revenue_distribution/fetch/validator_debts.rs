use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use doublezero_program_tools::{PrecomputedDiscriminator, zero_copy};
use doublezero_revenue_distribution::state::SolanaValidatorDeposit;
use doublezero_solana_client_tools::rpc::{SolanaConnection, SolanaConnectionOptions};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::pubkey::Pubkey;

use crate::command::revenue_distribution::try_fetch_program_config;

const DEFAULT_SINCE_DZ_EPOCH_MAINNET: u64 = 31;

#[derive(Debug, Args)]
pub struct ValidatorDebtsCommand {
    #[arg(long, short = 'n', value_name = "PUBKEY")]
    node_id: Option<Pubkey>,

    /// Show validator debts since the specified DoubleZero epoch.
    #[arg(long)]
    since_dz_epoch: Option<u64>,

    #[command(flatten)]
    connection_options: SolanaConnectionOptions,
}

impl ValidatorDebtsCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let Self {
            node_id: _,
            since_dz_epoch,
            connection_options,
        } = self;

        let connection = SolanaConnection::try_from(connection_options)?;

        let is_mainnet = connection.try_is_mainnet().await?;
        ensure!(
            is_mainnet,
            "`validator-debts` command is only supported on mainnet-beta"
        );

        let (_, config) = try_fetch_program_config(&connection).await?;
        let last_dz_epoch = config.next_completed_dz_epoch.value().saturating_sub(1);

        let _since_dz_epoch = since_dz_epoch
            .unwrap_or(DEFAULT_SINCE_DZ_EPOCH_MAINNET)
            .max(DEFAULT_SINCE_DZ_EPOCH_MAINNET)
            .min(last_dz_epoch);

        Ok(())
    }
}
