use crate::{
    payer::{SolanaPayerOptions, Wallet},
    rpc::{Connection, SolanaConnectionOptions},
};
use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};
use doublezero_program_tools::{instruction::try_build_instruction, zero_copy};
use doublezero_revenue_distribution::{
    DOUBLEZERO_MINT_DECIMALS, ID,
    instruction::{
        RevenueDistributionInstructionData,
        account::{InitializeContributorRewardsAccounts, InitializePrepaidConnectionAccounts},
    },
    state::{ContributorRewards, Journal, PrepaidConnection, ProgramConfig},
};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, pubkey::Pubkey};
use spl_associated_token_account_interface::{
    address::get_associated_token_address_and_bump_seed, program::ID as SPL_ATA_ID_BYTES,
};
use spl_token::ID as SPL_TOKEN_ID_BYTES;

#[derive(Debug, Args)]
pub struct RevenueDistributionCliCommand {
    #[command(subcommand)]
    pub command: RevenueDistributionSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum RevenueDistributionSubCommand {
    Fetch {
        #[arg(long)]
        program_config: bool,

        #[arg(long)]
        journal: bool,

        // TODO: --distribution with Option<u64>.
        // TODO: --contributor-rewards with Option<Pubkey>.
        // TODO: --prepaid-connection with Option<Pubkey>.
        //
        #[command(flatten)]
        solana_connection_options: SolanaConnectionOptions,
    },

    /// Initialize contributor rewards account for a contributor's service key.
    InitializeContributorRewards {
        service_key: Pubkey,

        #[command(flatten)]
        solana_payer_options: SolanaPayerOptions,
    },

    /// Initialize a prepaid connection for a user.
    PrepaidInitialize {
        /// User public key for the prepaid connection. Required.
        user_key: Pubkey,

        /// Source account owner. Optional (defaults to payer).
        #[arg(long)]
        src_key: Option<Pubkey>,

        #[command(flatten)]
        solana_payer_options: SolanaPayerOptions,
    },
}

impl RevenueDistributionSubCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            RevenueDistributionSubCommand::Fetch {
                program_config,
                journal,
                solana_connection_options,
            } => execute_fetch(program_config, journal, solana_connection_options).await,
            RevenueDistributionSubCommand::InitializeContributorRewards {
                service_key,
                solana_payer_options,
            } => execute_initialize_contributor_rewards(service_key, solana_payer_options).await,
            RevenueDistributionSubCommand::PrepaidInitialize {
                user_key,
                src_key,
                solana_payer_options,
            } => execute_prepaid_initialize(user_key, src_key, solana_payer_options).await,
        }
    }
}

//
// RevenueDistributionSubCommand::Fetch.
//

async fn execute_fetch(
    program_config: bool,
    journal: bool,
    solana_connection_options: SolanaConnectionOptions,
) -> Result<()> {
    let connection = Connection::try_from(solana_connection_options)?;

    if program_config {
        let program_config_key = ProgramConfig::find_address().0;
        let program_config_info = connection.get_account(&program_config_key).await?;

        let (program_config, _) =
            zero_copy::checked_from_bytes_with_discriminator::<ProgramConfig>(
                &program_config_info.data,
            )
            .ok_or(anyhow!("Failed to deserialize program config"))?;

        // TODO: Pretty print.
        println!("Program config: {program_config:?}");
    }

    if journal {
        let journal_key = Journal::find_address().0;
        let journal_info = connection.get_account(&journal_key).await?;

        let (journal, _) =
            zero_copy::checked_from_bytes_with_discriminator::<Journal>(&journal_info.data)
                .ok_or(anyhow!("Failed to deserialize journal"))?;

        // TODO: Pretty print.
        println!("Journal: {journal:?}");
    }

    Ok(())
}

//
// RevenueDistributionSubCommand::InitializeContributorRewards.
//

pub async fn execute_initialize_contributor_rewards(
    service_key: Pubkey,
    solana_payer_options: SolanaPayerOptions,
) -> Result<()> {
    let wallet = Wallet::try_from(solana_payer_options)?;
    let wallet_key = wallet.pubkey();

    let initialize_contributor_rewards_ix = try_build_instruction(
        &ID,
        InitializeContributorRewardsAccounts::new(&wallet_key, &service_key),
        &RevenueDistributionInstructionData::InitializeContributorRewards(service_key),
    )?;

    let mut compute_unit_limit = 10_000;

    let (_, bump) = ContributorRewards::find_address(&service_key);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let mut instructions = vec![
        initialize_contributor_rewards_ix,
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
    ];

    if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
        instructions.push(compute_unit_price_ix.clone());
    }

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

    if let Some(tx_sig) = tx_sig {
        println!("Initialized contributor rewards: {tx_sig}");

        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(())
}

//
// RevenueDistributionSubCommand::PrepaidInitialize.
//

pub async fn execute_prepaid_initialize(
    user_key: Pubkey,
    src_key: Option<Pubkey>,
    solana_payer_options: SolanaPayerOptions,
) -> Result<()> {
    let mut wallet = Wallet::try_from(solana_payer_options)?;
    let wallet_key = wallet.pubkey();

    // Determine the source account owner (use provided src_key or default to payer)
    let source_owner_key = src_key.unwrap_or(wallet_key);

    // Detect network and get appropriate 2Z mint address
    wallet.connection.cache_if_mainnet().await?;
    let dz_mint_key = if wallet.connection.is_mainnet {
        doublezero_revenue_distribution::env::mainnet::DOUBLEZERO_MINT_KEY
    } else {
        doublezero_revenue_distribution::env::development::DOUBLEZERO_MINT_KEY
    };

    // Convert Pubkey types for the SPL function (NOTE: it uses solana_pubkey::Pubkey)
    let source_owner_bytes: [u8; 32] = source_owner_key.to_bytes();
    let source_owner_spl = solana_pubkey::Pubkey::from(source_owner_bytes);

    let dz_mint_bytes: [u8; 32] = dz_mint_key.to_bytes();
    let dz_mint_spl = solana_pubkey::Pubkey::from(dz_mint_bytes);

    let spl_ata_id = solana_pubkey::Pubkey::from(SPL_ATA_ID_BYTES.to_bytes());
    let spl_token_id = solana_pubkey::Pubkey::from(SPL_TOKEN_ID_BYTES.to_bytes());

    // Derive the source ATA address
    let (source_2z_token_account_spl, _bump) = get_associated_token_address_and_bump_seed(
        &source_owner_spl,
        &dz_mint_spl,
        &spl_ata_id,
        &spl_token_id,
    );

    // Convert back to solana_sdk::pubkey::Pubkey
    let source_2z_token_account_key = Pubkey::from(source_2z_token_account_spl.to_bytes());

    // Build the instruction
    let initialize_prepaid_connection_ix = try_build_instruction(
        &ID,
        InitializePrepaidConnectionAccounts::new(
            &source_2z_token_account_key,
            &dz_mint_key,
            &source_owner_key,
            &wallet_key,
            &user_key,
        ),
        &RevenueDistributionInstructionData::InitializePrepaidConnection {
            user_key,
            decimals: DOUBLEZERO_MINT_DECIMALS,
        },
    )?;

    // Debugging (temporary for now before real tests)
    println!("Initialize Prepaid Connection Instruction:");
    println!("{initialize_prepaid_connection_ix:#?}");

    // Calculate compute units
    let mut compute_unit_limit = 10_000;

    // Add compute units for bump seeds
    let (_, bump) = PrepaidConnection::find_address(&user_key);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let (program_config_key, bump) = ProgramConfig::find_address();
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    // The reserve 2Z key also has a bump seed
    let (_, bump) =
        doublezero_revenue_distribution::state::find_2z_token_pda_address(&program_config_key);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let mut instructions = vec![
        initialize_prepaid_connection_ix,
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
    ];

    if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
        instructions.push(compute_unit_price_ix.clone());
    }

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

    if let Some(tx_sig) = tx_sig {
        println!("Initialized prepaid connection: {tx_sig}");
        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(())
}
