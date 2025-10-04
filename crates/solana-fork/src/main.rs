use std::fs;
use std::io::{self, Write};
use std::process::Command;

use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Parser;
use doublezero_passport::ID as PASSPORT_PROGRAM_ID;
use doublezero_revenue_distribution::ID as REVENUE_DISTRIBUTION_PROGRAM_ID;
use doublezero_solana_client_tools::{
    payer::try_load_keypair,
    rpc::{SolanaConnection, SolanaConnectionOptions},
};
use serde::Serialize;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_sdk::{pubkey::Pubkey, signer::Signer};

const ACCOUNTS_PATH: &str = "forked-accounts";

#[derive(Serialize)]
struct AccountData {
    lamports: u64,
    data: (String, String),
    owner: String,
    executable: bool,
    #[serde(rename = "rentEpoch")]
    rent_epoch: u64,
    space: usize,
}

#[derive(Serialize)]
struct AccountWrapper {
    pubkey: String,
    account: AccountData,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Upgrade authority for the program (defaults to pubkey from default
    /// keypair).
    #[arg(long)]
    upgrade_authority: Option<Pubkey>,

    #[command(flatten)]
    solana_connection_options: SolanaConnectionOptions,
}

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        upgrade_authority,
        solana_connection_options,
    } = Args::parse();

    let connection = SolanaConnection::try_from(solana_connection_options)?;

    // Get upgrade authority from argument or default keypair.
    let upgrade_authority = match upgrade_authority {
        Some(key) => key,
        None => {
            let keypair = try_load_keypair(None)?;
            keypair.pubkey()
        }
    };

    let should_fetch = if fs::metadata(ACCOUNTS_PATH).is_ok() {
        // If the directory exists, prompt user.
        print!(
            "Directory {} already exists. Clear contents and fetch fresh data? [y/N]: ",
            ACCOUNTS_PATH
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.trim().to_lowercase().starts_with('y') {
            // Clear directory contents.
            for entry in fs::read_dir(ACCOUNTS_PATH)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    fs::remove_file(path)?;
                }
            }

            true
        } else {
            false
        }
    } else {
        // If the directory does not exist, create it.
        fs::create_dir_all(ACCOUNTS_PATH)?;
        true
    };

    let revenue_distribution_program_path = format!("{}/revenue_distribution.so", ACCOUNTS_PATH);
    let passport_program_path = format!("{}/passport.so", ACCOUNTS_PATH);

    if should_fetch {
        let config = RpcProgramAccountsConfig {
            filters: None,
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        // Fetch all program accounts.

        try_fetch_and_write_program_accounts(
            &connection,
            &REVENUE_DISTRIBUTION_PROGRAM_ID,
            "revenue distribution",
            ACCOUNTS_PATH,
            &config,
        )
        .await?;

        try_fetch_and_write_program_accounts(
            &connection,
            &PASSPORT_PROGRAM_ID,
            "passport",
            ACCOUNTS_PATH,
            &config,
        )
        .await?;

        // Dump programs.

        try_dump_program(
            &connection,
            &REVENUE_DISTRIBUTION_PROGRAM_ID,
            "Revenue distribution",
            &revenue_distribution_program_path,
        )?;

        try_dump_program(
            &connection,
            &PASSPORT_PROGRAM_ID,
            "Passport",
            &passport_program_path,
        )?;
    } else {
        println!("Using existing accounts from {}/", ACCOUNTS_PATH);
    }

    // Check if solana-test-validator is available.
    let check = Command::new("which")
        .arg("solana-test-validator")
        .output()?;

    ensure!(
        check.status.success(),
        "solana-test-validator not found. Please install Solana CLI tools."
    );

    let status = Command::new("solana-test-validator")
        .arg("--url")
        .arg(connection.rpc_client.url())
        .arg("--account-dir")
        .arg(ACCOUNTS_PATH)
        .arg("--reset")
        .arg("--upgradeable-program")
        .arg(REVENUE_DISTRIBUTION_PROGRAM_ID.to_string())
        .arg(&revenue_distribution_program_path)
        .arg(upgrade_authority.to_string())
        .arg("--upgradeable-program")
        .arg(PASSPORT_PROGRAM_ID.to_string())
        .arg(&passport_program_path)
        .arg(upgrade_authority.to_string())
        .status()?;

    ensure!(
        status.success(),
        "solana-test-validator exited with status: {}",
        status
    );

    Ok(())
}

//

async fn try_fetch_and_write_program_accounts(
    connection: &SolanaConnection,
    program_id: &Pubkey,
    program_name: &str,
    accounts_dir: &str,
    config: &RpcProgramAccountsConfig,
) -> Result<usize> {
    let accounts = connection
        .get_program_accounts_with_config(program_id, config.clone())
        .await?;

    for (key, account) in &accounts {
        let account_data = AccountData {
            lamports: account.lamports,
            data: (BASE64.encode(&account.data), "base64".to_string()),
            owner: account.owner.to_string(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            space: account.data.len(),
        };

        let wrapper = AccountWrapper {
            pubkey: key.to_string(),
            account: account_data,
        };

        let json = serde_json::to_string_pretty(&wrapper)?;
        let file_path = format!("{}/{}.json", accounts_dir, key);
        fs::write(&file_path, json)?;
    }

    println!(
        "Wrote {} {} accounts to {}/",
        accounts.len(),
        program_name,
        accounts_dir
    );

    Ok(accounts.len())
}

fn try_dump_program(
    connection: &SolanaConnection,
    program_id: &Pubkey,
    program_name: &str,
    output_path: &str,
) -> Result<()> {
    println!("Dumping {} program to {}...", program_name, output_path);

    let dump_status = Command::new("solana")
        .arg("program")
        .arg("dump")
        .arg("--url")
        .arg(connection.rpc_client.url())
        .arg(program_id.to_string())
        .arg(output_path)
        .status()?;

    ensure!(
        dump_status.success(),
        "solana program dump exited with status: {}",
        dump_status
    );

    println!("{} program dumped successfully", program_name);
    Ok(())
}
