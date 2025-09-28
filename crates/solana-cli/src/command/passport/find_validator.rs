use std::net::Ipv4Addr;

use anyhow::Result;
use clap::Args;
use doublezero_sdk::get_doublezero_pubkey;
use doublezero_solana_client_tools::rpc::{SolanaConnection, SolanaConnectionOptions};
use solana_client::rpc_response::{RpcContactInfo, RpcVoteAccountStatus};
use solana_sdk::{pubkey::Pubkey, signer::Signer};

use crate::helpers::{find_node_by_ip, find_node_by_node_id, get_public_ipv4, identify_cluster};

#[derive(Debug, Args)]
pub struct FindValidatorCommand {
    #[arg(long, value_name = "PUBKEY")]
    validator_id: Option<Pubkey>,

    #[arg(long, value_name = "IP_ADDRESS")]
    gossip_ip: Option<String>,

    #[command(flatten)]
    solana_connection_options: SolanaConnectionOptions,
}

impl FindValidatorCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let FindValidatorCommand {
            validator_id,
            gossip_ip,
            solana_connection_options,
        } = self;

        println!("DoubleZero Passport - Find Validator");

        // Establish a connection to the Solana cluster
        let connection = SolanaConnection::try_from(solana_connection_options)?;
        // Identify the cluster
        let cluster = identify_cluster(&connection).await;
        println!("Connected to Solana: {:}\n", cluster);

        if let Ok(kp) = get_doublezero_pubkey() {
            println!("DoubleZero ID: {}", kp.pubkey())
        }

        // Fetch the cluster nodes
        let nodes = connection.get_cluster_nodes().await?;
        if nodes.is_empty() {
            anyhow::bail!("Unable to fetch cluster nodes. Is your RPC endpoint correct?");
        }
        // Fetch the cluster voters
        let voters = connection.get_vote_accounts().await?;
        if voters.current.is_empty() {
            anyhow::bail!("Unable to fetch cluster voters. Is your RPC endpoint correct?");
        }

        // Check if either node_id or server_ip is provided
        if let Some(node_id) = validator_id {
            // Search by node_id
            if let Some(node) = find_node_by_node_id(&nodes, &node_id) {
                print_node_info(node, voters);
            } else {
                println!(
                    "⚠️  Warning: Your node ID is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                );
            }
        } else if let Some(ip_str) = gossip_ip {
            // Search by server_ip
            let server_ip: Ipv4Addr = match ip_str.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    println!("Failed to parse server IP: {e}");
                    return Ok(());
                }
            };
            if let Some(node) = find_node_by_ip(&nodes, server_ip) {
                print_node_info(node, voters);
            } else {
                println!(
                    "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                );
            }
        } else {
            // Neither node_id nor server_ip provided, attempt to detect public IP
            match get_public_ipv4() {
                Ok(ip) => {
                    println!("Detected public IP: {ip}");
                    let server_ip: Ipv4Addr = match ip.parse() {
                        Ok(addr) => addr,
                        Err(e) => {
                            println!("Failed to parse detected public IP: {e}");
                            return Ok(());
                        }
                    };
                    if let Some(node) = find_node_by_ip(&nodes, server_ip) {
                        print_node_info(node, voters);
                    } else {
                        println!(
                            "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                        );
                    }
                }
                Err(e) => println!("Failed to get public IP: {e}"),
            }
        }

        Ok(())
    }
}

fn print_node_info(node: &RpcContactInfo, voters: RpcVoteAccountStatus) {
    println!("Validator ID: {}", node.pubkey);
    match &node.gossip {
        Some(gossip) => println!("Gossip IP: {}", gossip.ip()),
        None => println!("Gossip IP: <unknown>"),
    }

    let info = voters.current.iter().find(|v| v.node_pubkey == node.pubkey);

    if let Some(info) = info {
        // 1 SOL = 1_000_000_000 lamports
        let sol = info.activated_stake as f64 / 1_000_000_000.0;
        println!("Active stake: {:.6} SOL", sol);
        println!(
            "✅ This validator can connect as a primary in DoubleZero 🖥️  💎. It is a staked validator."
        );
    } else {
        println!(
            "✅ This validator can only connect as a backup in DoubleZero 🖥️  🛟. It is not staked and cannot act as a primary validator."
        );
    }
}
