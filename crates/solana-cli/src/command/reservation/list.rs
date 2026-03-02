use std::net::Ipv4Addr;

use anyhow::Result;
use clap::Args;
use doublezero_solana_client_tools::rpc::{SolanaConnection, SolanaConnectionOptions};
use doublezero_solana_sdk::reservation::{self, state};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{account::Account, pubkey::Pubkey};
use tabled::{Table, Tabled, settings::Style};

/*
   doublezero-solana reservation list [--device <PUBKEY> | --device-code <CODE>]
*/

#[derive(Debug, Args)]
pub struct ListCommand {
    /// Filter seats by device.
    #[command(flatten)]
    device_args: super::DeviceArgs,

    #[command(flatten)]
    connection_options: SolanaConnectionOptions,
}

#[derive(Debug, Tabled)]
struct SeatRow {
    #[tabled(rename = "Seat PDA")]
    seat_pda: String,
    #[tabled(rename = "Device")]
    device: String,
    #[tabled(rename = "Client IP")]
    client_ip: Ipv4Addr,
    #[tabled(rename = "Tenure")]
    tenure: u16,
    #[tabled(rename = "Epoch")]
    epoch: u64,
    #[tabled(rename = "Funding Account")]
    funding_account: String,
}

impl ListCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let connection = SolanaConnection::from(self.connection_options);

        let discriminator_bytes =
            borsh::to_vec(&state::CLIENT_SEAT_DISCRIMINATOR).expect("discriminator serialization");

        let mut filters = vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            0,
            discriminator_bytes,
        ))];

        // Resolve device filter (supports --device <PUBKEY> or --device-code <CODE>).
        if self.device_args.device.is_some() || self.device_args.device_code.is_some() {
            let network_env = connection.try_network_environment().await?;
            let device = self.device_args.resolve(network_env).await?;
            filters.push(RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                state::CLIENT_SEAT_DEVICE_KEY_OFFSET,
                device.to_bytes().to_vec(),
            )));
        }

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts: Vec<(Pubkey, Account)> = connection
            .get_program_accounts_with_config(&reservation::ID, config)
            .await?;

        if accounts.is_empty() {
            println!("No client seats found.");
            return Ok(());
        }

        let mut rows: Vec<SeatRow> = accounts
            .iter()
            .filter_map(|(seat_key, account)| {
                let (device_key, client_ip, tenure, epoch, funding_account_key) =
                    state::parse_client_seat(&account.data)?;
                Some(SeatRow {
                    seat_pda: seat_key.to_string(),
                    device: device_key.to_string(),
                    client_ip,
                    tenure,
                    epoch,
                    funding_account: funding_account_key.to_string(),
                })
            })
            .collect();

        rows.sort_by(|a, b| a.device.cmp(&b.device).then(a.client_ip.cmp(&b.client_ip)));

        println!("{} seat(s) found:\n", rows.len());

        let mut table = Table::new(rows);
        table.with(Style::markdown());
        println!("{table}");

        Ok(())
    }
}
