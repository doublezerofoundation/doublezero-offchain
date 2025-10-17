mod contributor_rewards;
mod convert_sol;
mod fetch;
mod relay;
mod validator_deposit;

//

use anyhow::{Context, Result, ensure};
use borsh::BorshDeserialize;
use clap::{Args, Subcommand};
use doublezero_revenue_distribution::state::{Journal, ProgramConfig, SolanaValidatorDeposit};
use doublezero_sol_conversion_interface::{
    oracle::OraclePriceData,
    state::{
        ConfigurationRegistry as SolConversionConfigurationRegistry,
        ProgramState as SolConversionProgramState,
    },
};
use doublezero_solana_client_tools::{rpc::SolanaConnection, zero_copy::ZeroCopyAccountOwned};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

// TODO: Add testnet?
const SOL_2Z_ORACLE_ENDPOINT: &str =
    "https://sol-2z-oracle-api-v1.mainnet-beta.doublezero.xyz/swap-rate";

#[derive(Debug, Args)]
pub struct RevenueDistributionCommand {
    #[command(subcommand)]
    pub command: RevenueDistributionSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RevenueDistributionSubcommand {
    /// Fetch accounts associated with the Revenue Distribution program.
    Fetch(fetch::FetchCommand),

    /// Contributor rewards account management.
    ContributorRewards(contributor_rewards::ContributorRewardsCommand),

    /// Convert SOL to 2Z tokens.
    ConvertSol(convert_sol::ConvertSolCommand),

    /// Solana validator deposit account management.
    ValidatorDeposit(validator_deposit::ValidatorDepositCommand),

    /// Relayer instructions for the Revenue Distribution program.
    Relay(relay::RevenueDistributionRelayCommand),
}

impl RevenueDistributionSubcommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            Self::Fetch(command) => command.try_into_execute().await,
            Self::ContributorRewards(command) => command.try_into_execute().await,
            Self::ConvertSol(command) => command.try_into_execute().await,
            Self::ValidatorDeposit(command) => command.try_into_execute().await,
            Self::Relay(command) => command.inner.try_into_execute().await,
        }
    }
}

//

async fn try_fetch_program_config(
    connection: &SolanaConnection,
) -> Result<(Pubkey, Box<ProgramConfig>)> {
    let (program_config_key, _) = ProgramConfig::find_address();

    let program_config =
        ZeroCopyAccountOwned::from_rpc_client(&connection.rpc_client, &program_config_key)
            .await
            .context("Revenue Distribution program not initialized")?;

    Ok((program_config_key, program_config.data.unwrap().0))
}

async fn try_fetch_journal(connection: &SolanaConnection) -> Result<(Pubkey, Box<Journal>)> {
    let (journal_key, _) = Journal::find_address();

    let journal = ZeroCopyAccountOwned::from_rpc_client(&connection.rpc_client, &journal_key)
        .await
        .context("Revenue Distribution program not initialized")?;

    Ok((journal_key, journal.data.unwrap().0))
}

async fn fetch_solana_validator_deposit(
    connection: &SolanaConnection,
    node_id: &Pubkey,
) -> (
    Pubkey,
    Option<SolanaValidatorDeposit>,
    u64, // balance
) {
    let (solana_validator_deposit_key, _) = SolanaValidatorDeposit::find_address(node_id);

    match ZeroCopyAccountOwned::from_rpc_client(
        &connection.rpc_client,
        &solana_validator_deposit_key,
    )
    .await
    {
        Ok(solana_validator_deposit) => match solana_validator_deposit.data {
            Some(data) => (
                solana_validator_deposit_key,
                Some(*data.0),
                solana_validator_deposit.balance,
            ),
            None => (
                solana_validator_deposit_key,
                None,
                solana_validator_deposit.lamports,
            ),
        },
        Err(_) => (solana_validator_deposit_key, None, 0),
    }
}

pub struct SolConversionState {
    pub program_state: (Pubkey, Box<SolConversionProgramState>),
    pub configuration_registry: (Pubkey, Box<SolConversionConfigurationRegistry>),
}

impl SolConversionState {
    pub async fn try_fetch(rpc_client: &RpcClient) -> Result<Self> {
        let (program_state_key, _) = SolConversionProgramState::find_address();
        let (configuration_registry_key, _) = SolConversionConfigurationRegistry::find_address();

        let accounts = rpc_client
            .get_multiple_accounts(&[program_state_key, configuration_registry_key])
            .await?;
        let account_datas = accounts
            .into_iter()
            .filter_map(|account| account.map(|account| account.data))
            .collect::<Vec<_>>();
        ensure!(
            account_datas.len() == 2,
            "SOL Conversion program not initialized"
        );

        let program_state_data =
            SolConversionProgramState::deserialize(&mut &account_datas[0][8..]).map(Into::into)?;
        let configuration_registry_data =
            SolConversionConfigurationRegistry::deserialize(&mut &account_datas[1][8..])
                .map(Into::into)?;

        Ok(Self {
            program_state: (program_state_key, program_state_data),
            configuration_registry: (configuration_registry_key, configuration_registry_data),
        })
    }
}

pub async fn try_request_oracle_conversion_price() -> Result<OraclePriceData> {
    reqwest::Client::new()
        .get(SOL_2Z_ORACLE_ENDPOINT)
        .header("User-Agent", "DoubleZero Solana CLI")
        .send()
        .await?
        .json()
        .await
        .context(format!(
            "Failed to request SOL/2Z price from {SOL_2Z_ORACLE_ENDPOINT}"
        ))
}
