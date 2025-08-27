// TODO: keeping this for now, remove when 2z-cli is ported

use anyhow::Result;
use clap::{Parser, Subcommand};
use contributor_rewards::{calculator::orchestrator::Orchestrator, settings::Settings};
use solana_sdk::pubkey::Pubkey;
use std::{path::PathBuf, str::FromStr};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(
    name = "contributor-rewards",
    about = "Off-chain contributor-rewards calculation for DoubleZero network",
    version,
    author
)]
pub struct Cli {
    // Path to the config file
    #[clap(short = 'c', long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Calculate epoch rewards
    CalculateRewards {
        /// If specified, rewards are calculated for that epoch, otherwise `current_epoch - 1`
        #[arg(short, long)]
        epoch: Option<u64>,

        /// If specified, output intermediate CSV files for cross-checking
        #[arg(short, long)]
        output_dir: Option<PathBuf>,

        /// Path to the keypair file to use for signing transactions
        #[arg(short = 'k', long)]
        keypair: Option<PathBuf>,

        /// Run in dry-run mode (skip writing to ledger, show what would be written)
        #[arg(long)]
        dry_run: bool,
    },
    /// Read telemetry aggregates from the ledger
    ReadTelemAgg {
        /// Require DZ Epoch
        #[arg(short, long)]
        epoch: u64,

        /// Payer's public key (e.g., DZF's public key) used for address derivation
        #[arg(short = 'p', long)]
        payer_pubkey: String,
    },
    /// Check and verify contributor reward
    CheckReward {
        /// Contributor address
        #[arg(short, long)]
        contributor: String,

        /// DZ Epoch
        #[arg(short, long)]
        epoch: u64,

        /// Payer's public key (e.g., DZF's public key) used for address derivation
        #[arg(short = 'p', long)]
        payer_pubkey: String,
    },
    /// Read reward input configuration from the ledger
    ReadRewardInput {
        /// DZ Epoch
        #[arg(short, long)]
        epoch: u64,

        /// Payer's public key (e.g., DZF's public key) used for address derivation
        #[arg(short = 'p', long)]
        payer_pubkey: String,
    },
    /// Realloc a record account
    ReallocRecord {
        /// Type of record to realloc (device-telemetry, internet-telemetry, reward-input, shapley-storage)
        #[arg(short = 't', long)]
        r#type: String,

        /// DZ Epoch
        #[arg(short, long)]
        epoch: u64,

        /// New size
        #[arg(short, long)]
        size: u64,

        /// Run in dry-run mode
        #[arg(long)]
        dry_run: bool,

        /// Path to the keypair file to use for signing transactions
        #[arg(short, long)]
        keypair: Option<PathBuf>,
    },
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = if let Some(config_path) = &self.config {
            Settings::from_path(config_path)?
        } else {
            Settings::from_env()?
        };
        init_logging(&settings.log_level)?;

        let orchestrator = Orchestrator::new(&settings);

        // Handle subcommands
        match self.command {
            Commands::ReadTelemAgg {
                epoch,
                payer_pubkey,
            } => {
                let pubkey = Pubkey::from_str(&payer_pubkey)?;
                orchestrator.read_telemetry_aggregates(epoch, &pubkey).await
            }
            Commands::CheckReward {
                contributor,
                epoch,
                payer_pubkey,
            } => {
                let pubkey = Pubkey::from_str(&payer_pubkey)?;
                orchestrator
                    .check_contributor_reward(&contributor, epoch, &pubkey)
                    .await
            }
            Commands::ReadRewardInput {
                epoch,
                payer_pubkey,
            } => {
                let pubkey = Pubkey::from_str(&payer_pubkey)?;
                orchestrator.read_reward_input(epoch, &pubkey).await
            }
            Commands::CalculateRewards {
                epoch,
                output_dir,
                keypair,
                dry_run,
            } => {
                orchestrator
                    .calculate_rewards(epoch, output_dir, keypair, dry_run)
                    .await
            }
            Commands::ReallocRecord {
                r#type,
                epoch,
                size,
                keypair,
                dry_run,
            } => {
                orchestrator
                    .realloc_record(r#type, epoch, size, keypair, dry_run)
                    .await
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}

fn init_logging(log_level: &str) -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level)))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false),
        )
        .init();

    Ok(())
}
