mod create_multicast_publishers;
mod find_validator_publishers;

use anyhow::Result;
use clap::Subcommand;
pub use create_multicast_publishers::CreateValidatorMulticastPublishersCommand;
pub use find_validator_publishers::FindValidatorMulticastPublishersCommand;

#[derive(Debug, Subcommand)]
pub enum SentinelAdminSubcommand {
    FindValidatorMulticastPublishers(FindValidatorMulticastPublishersCommand),
    CreateValidatorMulticastPublishers(CreateValidatorMulticastPublishersCommand),
}

impl SentinelAdminSubcommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            Self::FindValidatorMulticastPublishers(cmd) => cmd.try_execute().await,
            Self::CreateValidatorMulticastPublishers(cmd) => cmd.try_execute().await,
        }
    }
}
