use anyhow::Result;
use doublezero_solana_client_tools::rpc::SolanaConnection;
use doublezero_solana_client_tools::rpc::SolanaConnectionOptions;
use solana_sdk::pubkey::Pubkey;
use std::net::Ipv4Addr;

use crate::helpers::get_public_ipv4;

pub async fn execute_find(
    node_id: Option<Pubkey>,
    server_ip: Option<String>,
    solana_connection_options: SolanaConnectionOptions,
) -> Result<()> {
    println!("DoubleZero Passport - Find");

    // Establish a connection to the Solana cluster
    let connection = SolanaConnection::try_from(solana_connection_options)?;

    // Fetch the cluster nodes
    let nodes = connection.get_cluster_nodes().await?;

    // Check if either node_id or server_ip is provided
    if let Some(node_id) = node_id {
        // Search by node_id
        let node_id = node_id.to_string();
        let node = nodes.iter().find(|n| n.pubkey == node_id);
        match node {
            Some(node) => {
                println!("Node-Id: {}", node.pubkey);
                match &node.gossip {
                    Some(gossip) => println!("Server IP: {}", gossip.ip()),
                    None => println!("Server IP: <unknown>"),
                }
            }
            None => println!(
                "⚠️  Warning: Your node ID is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
            ),
        }
    } else if server_ip.is_some() {
        // Search by server_ip
        let server_ip: Ipv4Addr = match server_ip {
            Some(ref ip_str) => match ip_str.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    println!("Failed to parse server IP: {e}");
                    return Ok(());
                }
            },
            None => {
                println!("No server IP provided.");
                return Ok(());
            }
        };
        let node = nodes.iter().find(|n| match &n.gossip {
            Some(gossip) => gossip.ip() == server_ip,
            None => false,
        });
        match node {
            Some(node) => {
                println!("Node-Id: {}", node.pubkey);
                match &node.gossip {
                    Some(gossip) => println!("Server IP: {}", gossip.ip()),
                    None => println!("Server IP: <unknown>"),
                }
            }
            None => println!(
                "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
            ),
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
                let node = nodes.iter().find(|n| match &n.gossip {
                    Some(gossip) => gossip.ip() == server_ip,
                    None => false,
                });
                match node {
                    Some(node) => {
                        println!("Node-Id: {}", node.pubkey);
                        match &node.gossip {
                            Some(gossip) => println!("Server IP: {}", gossip.ip()),
                            None => println!("Server IP: <unknown>"),
                        }
                    }
                    None => println!(
                        "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                    ),
                }
            }
            Err(e) => println!("Failed to get public IP: {e}"),
        }
    }

    Ok(())
}
