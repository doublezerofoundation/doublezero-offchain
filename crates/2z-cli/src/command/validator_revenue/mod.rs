use crate::rpc::SolanaDebtPaymentConnectionOptions;
use anyhow::Result;
use clap::{Args, Subcommand};
use doublezero_solana_validator_debt::{solana_debt_calculator::SolanaDebtCalculator, worker};
use solana_sdk::signer::keypair::Keypair;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ValidatorRevenueCliCommand {
    #[command(subcommand)]
    pub command: ValidatorRevenueSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum ValidatorRevenueSubCommand {
    PayDebt {
        #[arg(long)]
        doublezero_epoch: u64,
        #[command(flatten)]
        solana_debt_payer_options: SolanaDebtPaymentConnectionOptions,
        #[arg(long)]
        dry_run: bool,
    },
}

impl ValidatorRevenueSubCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            ValidatorRevenueSubCommand::PayDebt {
                doublezero_epoch,
                solana_debt_payer_options,
                dry_run,
            } => execute_pay_debt(doublezero_epoch, solana_debt_payer_options, dry_run).await,
        }
    }
}

async fn execute_pay_debt(
    doublezero_epoch: u64,
    solana_debt_payer_options: SolanaDebtPaymentConnectionOptions,
    dry_run: bool,
) -> Result<()> {
    let debt_calculator = SolanaDebtCalculator::try_from(solana_debt_payer_options)?;
    let signer = try_load_keypair(None)?;
    let debt = worker::write_debts(
        &debt_calculator,
        signer,
        // TODO: pull in validator IDs from access pass
        vec!["va1i6T6vTcijrCz6G8r89H6igKjwkLfF6g5fnpvZu1b".to_string()],
        doublezero_epoch,
        dry_run,
    )
    .await?;
    dbg!(debt);
    Ok(())
}

// tmp hack
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
