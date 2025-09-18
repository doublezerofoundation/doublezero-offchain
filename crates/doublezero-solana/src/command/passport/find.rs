use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpStream, ToSocketAddrs},
};

use crate::rpc::{Connection, SolanaConnectionOptions};

pub async fn execute_find(
    node_id: Option<Pubkey>,
    server_ip: Option<String>,
    solana_connection_options: SolanaConnectionOptions,
) -> Result<()> {
    println!("DoubleZero Passport - Find");

    let connection = Connection::try_from(solana_connection_options)?;

    let nodes = connection.get_cluster_nodes().await?;

    if let Some(node_id) = node_id {
        let node_id = node_id.to_string();
        let node = nodes.iter().find(|n| n.pubkey == node_id);
        match node {
            Some(node) => {
                println!("Node-Id: {}", node.pubkey);
                println!("Server IP: {}", node.gossip.unwrap().ip());
            }
            None => println!(
                "⚠️  Warning: Your node ID is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
            ),
        }
    } else if server_ip.is_some() {
        let server_ip: Ipv4Addr = server_ip.unwrap().parse().unwrap();
        let node = nodes
            .iter()
            .find(|n| n.gossip.is_some() && n.gossip.unwrap().ip() == server_ip);
        match node {
            Some(node) => {
                println!("Node-Id: {}", node.pubkey);
                println!("Server IP: {}", node.gossip.unwrap().ip());
            }
            None => println!(
                "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
            ),
        }
    } else {
        match get_public_ipv4() {
            Ok(ip) => {
                println!("Detected public IP: {ip}");
                let server_ip: Ipv4Addr = ip.parse().unwrap();
                let node = nodes
                    .iter()
                    .find(|n| n.gossip.is_some() && n.gossip.unwrap().ip() == server_ip);
                match node {
                    Some(node) => {
                        println!("Node-Id: {}", node.pubkey);
                        println!("Server IP: {}", node.gossip.unwrap().ip());
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

pub fn get_public_ipv4() -> Result<String, Box<dyn std::error::Error>> {
    // Resolve the host `ifconfig.me` to IPv4 addresses
    let addrs = "ifconfig.me:80"
        .to_socket_addrs()?
        .filter_map(|addr| match addr {
            std::net::SocketAddr::V4(ipv4) => Some(ipv4),
            _ => None,
        })
        .next()
        .ok_or("Failed to resolve an IPv4 address")?;

    // Establish a connection to the IPv4 address
    let mut stream = TcpStream::connect(addrs)?;

    // Send an HTTP GET request to retrieve only IPv4
    let request = "GET /ip HTTP/1.1\r\nHost: ifconfig.me\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes())?;

    // Read the response from the server
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;

    // Convert the response to text and find the body of the response
    let response_text = str::from_utf8(&response)?;

    // The IP will be in the body after the HTTP headers
    if let Some(body_start) = response_text.find("\r\n\r\n") {
        let ip = &response_text[body_start + 4..].trim();

        return Ok(ip.to_string());
    }

    Err("Failed to extract the IP from the response".into())
}
