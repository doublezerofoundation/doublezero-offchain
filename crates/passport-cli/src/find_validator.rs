use std::{io::Write, net::Ipv4Addr, sync::Arc};

use anyhow::Result;
use clap::Args;
use doublezero_cli_core::{CliContext, OutputFormat};
use doublezero_ledger_sentinel::{
    client::solana::SolRpcClient, constants::ENV_PREVIOUS_LEADER_EPOCHS,
};
use doublezero_sdk::get_doublezero_pubkey;
use doublezero_solana_client_tools::rpc::SolanaConnection;
use serde::Serialize;
use solana_client::rpc_response::RpcContactInfo;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use url::Url;

use crate::{
    output::{emit_json, is_json, resolve_format},
    util::{find_node_by_ip, find_node_by_node_id, identify_cluster, try_get_public_ipv4},
};

#[derive(Debug, Args)]
pub struct FindValidatorArgs {
    #[arg(long, value_name = "PUBKEY")]
    pub validator_id: Option<Pubkey>,

    #[arg(long, value_name = "IP_ADDRESS")]
    pub gossip_ip: Option<String>,

    /// Output as pretty JSON
    #[arg(long, default_value_t = false, conflicts_with = "json_compact")]
    pub json: bool,
    /// Output as single-line JSON suitable for piping
    #[arg(long = "json-compact", default_value_t = false, conflicts_with = "json")]
    pub json_compact: bool,
}

impl FindValidatorArgs {
    pub async fn execute(self, ctx: &CliContext, out: &mut impl Write) -> eyre::Result<()> {
        let format = resolve_format(self.json, self.json_compact, ctx.output_format);
        if is_json(format) {
            self.run_json(ctx, out, format)
                .await
                .map_err(|e| eyre::eyre!("{e:#}"))
        } else {
            self.run_human(ctx, out).await.map_err(|e| eyre::eyre!("{e:#}"))
        }
    }

    /// Human-readable output. Reproduces the exact pre-RFC-20 behavior, including
    /// branch-specific warnings and the print-and-return handling of parse / IP
    /// detection failures.
    async fn run_human(self, ctx: &CliContext, out: &mut impl Write) -> Result<()> {
        tracing::debug!(env = %ctx.env, "passport find-validator");

        writeln!(out, "DoubleZero Passport - Find Validator")?;

        let connection = SolanaConnection::new(ctx.solana_l1_rpc_url.clone());
        let sol_client = SolRpcClient::new(
            Url::parse(&connection.url()).unwrap(),
            Arc::new(Keypair::new()),
        );

        let cluster = identify_cluster(&connection).await;
        writeln!(out, "Connected to Solana: {cluster}\n")?;

        if let Ok(kp) = get_doublezero_pubkey() {
            writeln!(out, "DoubleZero ID: {}", kp.pubkey())?;
        }

        let nodes = connection.get_cluster_nodes().await?;
        if nodes.is_empty() {
            anyhow::bail!("Unable to fetch cluster nodes. Is your RPC endpoint correct?");
        }

        if let Some(node_id) = self.validator_id {
            if let Some(node) = find_node_by_node_id(&nodes, &node_id) {
                print_node_info(node, &sol_client, out).await?;
            } else {
                writeln!(
                    out,
                    "⚠️  Warning: Your node ID is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                )?;
            }
        } else if let Some(ip_str) = self.gossip_ip {
            let server_ip: Ipv4Addr = match ip_str.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    writeln!(out, "Failed to parse server IP: {e}")?;
                    return Ok(());
                }
            };
            if let Some(node) = find_node_by_ip(&nodes, server_ip) {
                print_node_info(node, &sol_client, out).await?;
            } else {
                writeln!(
                    out,
                    "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                )?;
            }
        } else {
            match try_get_public_ipv4() {
                Ok(ip) => {
                    writeln!(out, "Detected public IP: {ip}")?;
                    let server_ip: Ipv4Addr = match ip.parse() {
                        Ok(addr) => addr,
                        Err(e) => {
                            writeln!(out, "Failed to parse detected public IP: {e}")?;
                            return Ok(());
                        }
                    };
                    if let Some(node) = find_node_by_ip(&nodes, server_ip) {
                        print_node_info(node, &sol_client, out).await?;
                    } else {
                        writeln!(
                            out,
                            "⚠️  Warning: Your IP is not appearing in gossip. Your validator must be visible in gossip in order to connect to DoubleZero."
                        )?;
                    }
                }
                Err(e) => writeln!(out, "Failed to get public IP: {e}")?,
            }
        }

        Ok(())
    }

    /// Additive JSON output for the read verb. Collects the same lookup into a
    /// serializable view.
    async fn run_json(
        self,
        ctx: &CliContext,
        out: &mut impl Write,
        format: OutputFormat,
    ) -> Result<()> {
        tracing::debug!(env = %ctx.env, "passport find-validator (json)");

        let connection = SolanaConnection::new(ctx.solana_l1_rpc_url.clone());
        let sol_client = SolRpcClient::new(
            Url::parse(&connection.url()).unwrap(),
            Arc::new(Keypair::new()),
        );

        let mut view = ValidatorLookupView {
            cluster: identify_cluster(&connection).await.to_string(),
            doublezero_id: get_doublezero_pubkey().ok().map(|kp| kp.pubkey().to_string()),
            ..Default::default()
        };

        let nodes = connection.get_cluster_nodes().await?;
        if nodes.is_empty() {
            anyhow::bail!("Unable to fetch cluster nodes. Is your RPC endpoint correct?");
        }

        let node: Option<&RpcContactInfo> = if let Some(node_id) = self.validator_id {
            find_node_by_node_id(&nodes, &node_id)
        } else if let Some(ip_str) = self.gossip_ip {
            let server_ip: Ipv4Addr = ip_str
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse server IP: {e}"))?;
            find_node_by_ip(&nodes, server_ip)
        } else {
            let ip = try_get_public_ipv4()?;
            view.detected_public_ip = Some(ip.clone());
            let server_ip: Ipv4Addr = ip
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse detected public IP: {e}"))?;
            find_node_by_ip(&nodes, server_ip)
        };

        match node {
            Some(node) => {
                let pubkey = node.pubkey.parse::<Pubkey>().expect("Invalid pubkey");
                let in_leader_schedule = sol_client
                    .is_scheduled_leader(&pubkey, ENV_PREVIOUS_LEADER_EPOCHS)
                    .await?;
                view.validator_id = Some(node.pubkey.clone());
                view.gossip_ip = Some(
                    node.gossip
                        .as_ref()
                        .map(|g| g.ip().to_string())
                        .unwrap_or_else(|| "<unknown>".to_string()),
                );
                view.in_leader_schedule = Some(in_leader_schedule);
                view.role = Some(if in_leader_schedule { "primary" } else { "backup" }.to_string());
                view.visible_in_gossip = true;
            }
            None => {
                view.visible_in_gossip = false;
                view.warning = Some(NOT_IN_GOSSIP_WARNING.to_string());
            }
        }

        emit_json(out, &view, format).map_err(|e| anyhow::anyhow!("{e:#}"))
    }
}

#[derive(Debug, Default, Serialize)]
struct ValidatorLookupView {
    cluster: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    doublezero_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detected_public_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    validator_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gossip_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_leader_schedule: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    visible_in_gossip: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<String>,
}

const NOT_IN_GOSSIP_WARNING: &str =
    "Your validator must be visible in gossip in order to connect to DoubleZero.";

async fn print_node_info<W: Write>(
    node: &RpcContactInfo,
    sol_client: &SolRpcClient,
    out: &mut W,
) -> Result<()> {
    writeln!(out, "Validator ID: {}", node.pubkey)?;
    match &node.gossip {
        Some(gossip) => writeln!(out, "Gossip IP: {}", gossip.ip())?,
        None => writeln!(out, "Gossip IP: <unknown>")?,
    }

    let pubkey = node.pubkey.parse::<Pubkey>().expect("Invalid pubkey");

    if sol_client
        .is_scheduled_leader(&pubkey, ENV_PREVIOUS_LEADER_EPOCHS)
        .await?
    {
        writeln!(out, "In Leader scheduler")?;
        writeln!(
            out,
            "✅ This validator can connect as a primary in DoubleZero 🖥️  💎. It is a leader scheduled validator."
        )?;
    } else {
        writeln!(
            out,
            "✅ This validator can only connect as a backup in DoubleZero 🖥️  🛟. It is not leader scheduled and cannot act as a primary validator."
        )?;
    }

    Ok(())
}
