use anyhow::{Result, ensure};
use clap::Args;
use doublezero_solana_client_tools::{
    payer::{SolanaPayerOptions, Wallet},
    rpc::{DoubleZeroLedgerConnection, DoubleZeroLedgerConnectionOptions},
};
use doublezero_solana_sdk::revenue_distribution::state::ProgramConfig;
use solana_sdk::{commitment_config::CommitmentConfig, signer::Signer};

use crate::worker;

#[derive(Debug, Args, Clone)]
pub struct InitializeDistributionCommand {
    #[command(flatten)]
    solana_payer_options: SolanaPayerOptions,

    #[command(flatten)]
    dz_ledger_connection_options: DoubleZeroLedgerConnectionOptions,
}

impl InitializeDistributionCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let Self {
            solana_payer_options,
            dz_ledger_connection_options,
        } = self;

        let wallet = Wallet::try_from(solana_payer_options)?;

        let ProgramConfig {
            debt_accountant_key: expected_accountant_key,
            ..
        } = *wallet
            .connection
            .try_fetch_zero_copy_data::<ProgramConfig>(&ProgramConfig::find_address().0)
            .await?;

        ensure!(
            wallet.signer.pubkey() == expected_accountant_key,
            "Signer does not match expected debt accountant"
        );

        let dz_ledger_rpc_client = DoubleZeroLedgerConnection::new_with_commitment(
            dz_ledger_connection_options.dz_ledger_url,
            CommitmentConfig::confirmed(),
        );

        worker::initialize_distribution(wallet, dz_ledger_rpc_client).await?;

        Ok(())
    }
}
