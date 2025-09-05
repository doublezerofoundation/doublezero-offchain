use crate::rpc::SolanaDebtPaymentConnectionOptions;
use anyhow::Result;
use clap::{Args, Subcommand};
use doublezero_solana_validator_debt::{solana_debt_calculator::SolanaDebtCalculator, worker};
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

        /// Connectin options to Solana and DoubleZero ledger
        #[command(flatten)]
        solana_debt_payer_options: SolanaDebtPaymentConnectionOptions,

        /// Temporary: pass in comma-separated list of validator pubkeys
        #[arg(long)]
        validator_pubkeys: String,
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
                validator_pubkeys,
                dry_run,
            } => {
                execute_pay_debt(
                    doublezero_epoch,
                    solana_debt_payer_options,
                    keypair_path,
                    validator_pubkeys,
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
    validator_pubkeys: String,
    dry_run: bool,
) -> Result<()> {
    let validator_pubkeys: Vec<String> = validator_pubkeys
        .split(",")
        .map(|validator_id| validator_id.to_string()) //
        .collect();
    let debt_calculator = SolanaDebtCalculator::try_from(solana_debt_payer_options)?;
    let signer = try_load_keypair(keypair_path.map(Into::into))?;
    let debt = worker::write_debts(
        &debt_calculator,
        signer,
        // TODO: pull in validator IDs from access pass
        validator_pubkeys,
        doublezero_epoch,
        dry_run,
    )
    .await?;
    dbg!(debt);
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
