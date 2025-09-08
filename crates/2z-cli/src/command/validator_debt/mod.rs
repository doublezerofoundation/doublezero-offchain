use crate::rpc::SolanaDebtPaymentConnectionOptions;
use anyhow::Result;
use clap::{Args, Subcommand};
use doublezero_solana_validator_debt::{
    ledger,
    solana_debt_calculator::{SolanaDebtCalculator, ValidatorRewards},
    transaction::Transaction,
    validator_debt::ComputedSolanaValidatorDebts,
};
use solana_sdk::signer::keypair::Keypair;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ValidatorDebtCliCommand {
    #[command(subcommand)]
    pub command: ValidatorDebtSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum ValidatorDebtSubCommand {
    PayDebt {
        /// Filepath or URL to a keypair.
        #[arg(long = "keypair", short = 'k', value_name = "KEYPAIR")]
        keypair_path: Option<String>,

        /// DoubleZero ledger epoch
        #[arg(long)]
        doublezero_epoch: u64,

        /// Connection options to Solana and DoubleZero ledger
        #[command(flatten)]
        solana_debt_payer_options: SolanaDebtPaymentConnectionOptions,

        /// Simulate the command
        #[arg(long)]
        dry_run: bool,
    },
}

impl ValidatorDebtSubCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            ValidatorDebtSubCommand::PayDebt {
                doublezero_epoch,
                solana_debt_payer_options,
                keypair_path,
                dry_run,
            } => {
                execute_pay_debt(
                    doublezero_epoch,
                    solana_debt_payer_options,
                    keypair_path,
                    dry_run,
                )
                .await
            }
        }
    }
}

async fn execute_pay_debt(
    doublezero_epoch: u64,
    solana_debt_payer_options: SolanaDebtPaymentConnectionOptions,
    keypair_path: Option<String>,
    dry_run: bool,
) -> Result<()> {
    let debt_calculator = SolanaDebtCalculator::try_from(solana_debt_payer_options)?;
    let signer = try_load_keypair(keypair_path.map(Into::into))?;
    let prefix = b"solana_validator_debt_test";
    let dz_epoch_bytes = doublezero_epoch.to_le_bytes();
    let seeds: &[&[u8]] = &[prefix, &dz_epoch_bytes];

    let read = ledger::read_from_ledger(
        debt_calculator.ledger_rpc_client(),
        &signer,
        seeds,
        debt_calculator.ledger_commitment_config(),
    )
    .await?;

    let deserialized: ComputedSolanaValidatorDebts = borsh::from_slice(read.1.as_slice()).unwrap();

    let transaction = Transaction::new(signer, dry_run);
    let transactions = transaction
        .pay_solana_validator_debt(
            &debt_calculator.solana_rpc_client,
            deserialized,
            doublezero_epoch,
        )
        .await?;
    for t in transactions {
        transaction
            .send_or_simulate_transaction(&debt_calculator.solana_rpc_client, &t)
            .await?;
    }
    Ok(())
}

fn try_load_keypair(path: Option<PathBuf>) -> Result<Keypair> {
    let home_path = std::env::var_os("HOME").unwrap();
    let default_keypair_path = ".config/solana/id.json";

    let keypair_path = path.unwrap_or_else(|| PathBuf::from(home_path).join(default_keypair_path));
    try_load_specified_keypair(&keypair_path)
}

fn try_load_specified_keypair(path: &PathBuf) -> Result<Keypair> {
    let keypair_file = std::fs::read_to_string(path)?;
    let keypair_bytes = serde_json::from_str::<Vec<u8>>(&keypair_file)?;
    let default_keypair = Keypair::try_from(keypair_bytes.as_slice())?;

    Ok(default_keypair)
}
