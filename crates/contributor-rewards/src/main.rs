// TODO: keeping this for now, remove when 2z-cli is ported

mod cli;

use anyhow::Result;
use clap::{Parser, Subcommand};
use cli::{rewards::RewardsCommands, shapley::ShapleyCommands};
use contributor_rewards::{calculator::orchestrator::Orchestrator, settings::Settings};
use std::path::PathBuf;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(
    name = "contributor-rewards",
    about = "Off-chain contributor-rewards calculation for DoubleZero network",
    version,
    author,
    after_help = r#"Configuration:
    Configuration can be provided via:
    1. Environment variables with DZ__ prefix (e.g., DZ__RPC__DZ_URL)
    2. .env file in the current directory (see .env.example)
    3. Config file with -c option (see example.config.toml)

Examples:
    # Dry run for a specific epoch
    contributor-rewards calculate-rewards --epoch 123 --dry-run

    # Calculate rewards for the previous epoch
    contributor-rewards calculate-rewards -k keypair.json

    # Read telemetry aggregates
    contributor-rewards read-telem-agg --epoch 123

    # Check a contributor's reward
    contributor-rewards check-reward --contributor <PUBKEY> --epoch 123"#
)]
pub struct Cli {
    /// Path to the configuration file (TOML format)
    ///
    /// If not provided, will attempt to load from environment variables
    #[clap(short = 'c', long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(flatten)]
    Rewards(RewardsCommands),
    #[command(flatten)]
    Shapley(ShapleyCommands),
    /// Export raw chain data snapshots for debugging and analysis
    Snapshot {
        #[command(subcommand)]
        cmd: cli::snapshot::SnapshotCommands,
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

        // Route to module handlers
        match self.command {
            Commands::Rewards(cmd) => cli::rewards::handle(&orchestrator, cmd).await,
            Commands::Shapley(cmd) => cli::shapley::handle(&orchestrator, cmd).await,
            Commands::Snapshot { cmd } => cli::snapshot::handle(&orchestrator, cmd).await,
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
