use anyhow::Result;
use clap::Parser;
use doublezero_sentinel_admin_cli::command::SentinelAdminSubcommand;

#[derive(Debug, Parser)]
#[command(term_width = 0)]
#[command(version = option_env!("BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))]
#[command(about = "DoubleZero Sentinel Admin Commands", long_about = None)]
struct DoubleZeroSentinelAdminApp {
    #[command(subcommand)]
    command: SentinelAdminSubcommand,
}

#[tokio::main]
async fn main() -> Result<()> {
    DoubleZeroSentinelAdminApp::parse()
        .command
        .try_into_execute()
        .await
}
