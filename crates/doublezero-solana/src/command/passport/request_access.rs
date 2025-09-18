use anyhow::{Result, bail};
use doublezero_passport::{
    ID,
    instruction::{AccessMode, PassportInstructionData, account::RequestAccessAccounts},
    state::AccessRequest,
};
use doublezero_program_tools::instruction::try_build_instruction;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction, offchain_message::OffchainMessage, pubkey::Pubkey,
    signature::Signature,
};
use std::{net::Ipv4Addr, str::FromStr};

use crate::{
    command::helpers::get_public_ipv4,
    payer::{SolanaPayerOptions, Wallet},
};

// 0.2 SOL in lamports (1 SOL = 1_000_000_000 lamports)
const MIN_BALANCE_LAMPORTS: u64 = 200_000_000;

pub async fn execute_request_solana_validator_access(
    service_key: Pubkey,
    node_id: Pubkey,
    signature: String,
    message_version: u8,
    solana_payer_options: SolanaPayerOptions,
) -> Result<()> {
    let verbose = solana_payer_options.signer_options.verbose;
    let wallet = Wallet::try_from(solana_payer_options)?;
    let wallet_key = wallet.pubkey();

    // Check balance
    let balance = wallet.connection.get_balance(&wallet_key).await?;
    if balance <= MIN_BALANCE_LAMPORTS {
        bail!(
            "Your wallet balance is below 0.2 SOL. Please fund your wallet to proceed."
        );
    } else if verbose {
        println!("Wallet balance: {} lamports", balance);
    }

    // Check if the node ID is in gossip.
    match get_public_ipv4() {
        Ok(ip) => {
            if verbose {
                println!("Detected public IP: {ip}");
            }

            let server_ip: Ipv4Addr = match ip.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    println!("Failed to parse detected public IP: {e}");
                    return Ok(());
                }
            };

            // Fetch the cluster nodes
            let nodes = wallet.connection.get_cluster_nodes().await?;
            let node = nodes
                .iter()
                .find(|n| n.gossip.is_some() && n.gossip.unwrap().ip() == server_ip);
            match node {
                Some(node) => {
                    if verbose {
                        println!("Found node in gossip: {}", node.pubkey);
                    }
                    if node.pubkey != node_id.to_string() {
                        bail!(
                            "⚠️  Warning: The provided node ID does not match the node ID associated with the detected public IP in gossip"
                        );
                    }

                    if let Some(gossip) = &node.gossip {
                        println!("Server IP: {}", gossip.ip());
                    } else {
                        println!("Server IP: <unknown>");
                    }
                }
                None => println!(
                    "⚠️  Warning: Your public IP {ip} is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                ),
            }
        }
        Err(e) => println!("Failed to get public IP: {e}"),
    }

    let ed25519_signature = Signature::from_str(&signature)?;

    // Verify the signature.
    let raw_message = AccessRequest::access_request_message(&service_key);

    if verbose {
        println!("Raw message: {raw_message}");
    }

    let message = OffchainMessage::new(message_version, raw_message.as_bytes())?;
    let serialized_message = message.serialize()?;

    if !ed25519_signature.verify(node_id.as_array(), &serialized_message) {
        bail!("Signature verification failed");
    } else if verbose {
        println!("Signature recovers node ID: {node_id}");
    }

    let request_access_ix = try_build_instruction(
        &ID,
        RequestAccessAccounts::new(&wallet_key, &service_key),
        &PassportInstructionData::RequestAccess(AccessMode::SolanaValidator {
            validator_id: node_id,
            service_key,
            ed25519_signature: ed25519_signature.into(),
        }),
    )?;

    let mut compute_unit_limit = 10_000;

    let (_, bump) = AccessRequest::find_address(&service_key);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let mut instructions = vec![
        request_access_ix,
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
    ];

    if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
        instructions.push(compute_unit_price_ix.clone());
    }

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

    if let Some(tx_sig) = tx_sig {
        println!("Request Solana validator access: {tx_sig}");

        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(())
}
