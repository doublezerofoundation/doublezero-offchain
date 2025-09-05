use anyhow::Result;
use clap::{Args, Subcommand};


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
    },
}

impl ValidatorRevenueSubCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            ValidatorRevenueSubCommand::PayDebt { doublezero_epoch, solana_debt_payer_options } => {
                execute_pay_debt(doublezero_epoch).await
            }
        }
    }
}

async fn execute_pay_debt(doublezero_epoch: u64) -> Result<()> {
    dbg!(doublezero_epoch);
    Ok(())
}
