use anyhow::Result;
use clap::{Args, Subcommand};
use solana_sdk::pubkey::Pubkey;

use crate::{
    command::passport::find::execute_find, payer::SolanaPayerOptions, rpc::SolanaConnectionOptions,
};

mod fetch;
mod find;
mod request_access;

#[derive(Debug, Args)]
pub struct PassportCliCommand {
    #[command(subcommand)]
    pub command: PassportSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum PassportSubCommand {
    Find {
        #[arg(long, value_name = "PUBKEY")]
        node_id: Option<Pubkey>,

        #[arg(long, value_name = "IP_ADDRESS")]
        server_ip: Option<String>,

        #[command(flatten)]
        solana_connection_options: SolanaConnectionOptions,
    },

    Fetch {
        #[arg(long)]
        program_config: bool,

        #[command(flatten)]
        solana_connection_options: SolanaConnectionOptions,
    },

    RequestSolanaValidatorAccess {
        service_key: Pubkey,

        #[arg(long, value_name = "PUBKEY")]
        node_id: Pubkey,

        #[arg(long, short = 's', value_name = "BASE58_STRING")]
        signature: String,

        /// Offchain message version. ONLY 0 IS SUPPORTED.
        #[arg(long, value_name = "U8", default_value = "0")]
        message_version: u8,

        #[command(flatten)]
        solana_payer_options: SolanaPayerOptions,
    },
}

impl PassportSubCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            PassportSubCommand::Find {
                node_id,
                server_ip,
                solana_connection_options,
            } => execute_find(node_id, server_ip, solana_connection_options).await,
            PassportSubCommand::Fetch {
                program_config,
                solana_connection_options,
            } => fetch::execute_fetch(program_config, solana_connection_options).await,
            PassportSubCommand::RequestSolanaValidatorAccess {
                service_key,
                node_id,
                signature,
                message_version,
                solana_payer_options,
            } => {
                request_access::execute_request_solana_validator_access(
                    service_key,
                    node_id,
                    signature,
                    message_version,
                    solana_payer_options,
                )
                .await
            }
        }
    }
}
