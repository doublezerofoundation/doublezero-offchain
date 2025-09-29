use anyhow::Result;
use clap::Args;
use doublezero_passport::{
    instruction::{AccessMode, SolanaValidatorAttestation},
    state::AccessRequest,
};
use doublezero_solana_client_tools::rpc::{SolanaConnection, SolanaConnectionOptions};
use solana_sdk::pubkey::Pubkey;

use crate::helpers::{find_node_by_node_id, find_voter_by_node_id, identify_cluster, parse_pubkey};

/*
   doublezero-solana passport request-access --doublezero-address SSSS --primary-validator-id AAA --backup-validator-ids BBB,CCC --signature XXXXX
*/

#[derive(Debug, Args)]
pub struct PrepareValidatorAccessCommand {
    /// The DoubleZero service key to request access from
    #[arg(long, value_parser = parse_pubkey)]
    doublezero_address: Pubkey,
    /// The validator's node ID (identity pubkey)
    #[arg(long, value_name = "PUBKEY", value_parser = parse_pubkey)]
    primary_validator_id: Pubkey,
    /// Optional backup validator IDs (identity pubkeys)
    #[arg(long, value_name = "PUBKEY,PUBKEY,PUBKEY", value_delimiter = ',', value_parser = parse_pubkey)]
    backup_validator_ids: Vec<Pubkey>,

    #[arg(long, default_value_t = false)]
    force: bool,

    #[command(flatten)]
    solana_connection_options: SolanaConnectionOptions,
}

impl PrepareValidatorAccessCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let PrepareValidatorAccessCommand {
            doublezero_address,
            primary_validator_id,
            backup_validator_ids,
            solana_connection_options,
            force,
        } = self;

        // Establish a connection to the Solana cluster
        let connection = SolanaConnection::try_from(solana_connection_options)?;
        // Identify the cluster
        let cluster = identify_cluster(&connection).await;
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

        // Collect errors
        let mut errors = Vec::<String>::new();

        println!("DoubleZero Passport - Prepare Validator Access Request");
        println!("Connected to Solana: {:}", cluster);

        println!("\nDoubleZero Address: {doublezero_address}\n");

        println!("Primary validator 🖥️  💎:\n  ID: {primary_validator_id} ");
        if let Some(node) = find_node_by_node_id(&nodes, &primary_validator_id) {
            println!(
                "  Gossip: ✅ OK ({})",
                node.gossip.as_ref().map(|g| g.ip()).unwrap()
            );
            print!("  Leader scheduler: ");
            if let Some(voter) = find_voter_by_node_id(&voters, &primary_validator_id) {
                let sol = voter.activated_stake as f64 / 1_000_000_000.0;
                print!(" ✅ OK (Stake: {:.6} SOL)", sol);
            } else {
                print!(" ❌ Invalid ",);
                errors.push(format!(
                    "Primary validator ID ({}) is not an active staked validator. The primary must have stake delegated and be participating in the leader scheduler.",
                    primary_validator_id
                ));
            }
        } else {
            println!(" ❌ Gossip Fail",);
            errors.push(format!(
                "Primary validator ID ({}) is not visible in gossip. The primary validator must appear in gossip to be considered active.",
                primary_validator_id
            ));
        }
        println!();

        if !backup_validator_ids.is_empty() {
            println!("\nBackup validator 🖥️  🛟: ");

            for backup_id in &backup_validator_ids {
                print!("  ID: {backup_id}\n  Gossip: ");
                if let Some(node) = find_node_by_node_id(&nodes, backup_id) {
                    println!(" ✅ OK ({})", node.gossip.as_ref().map(|g| g.ip()).unwrap());
                    print!("  Leader scheduler: ");

                    if let Some(voter) = find_voter_by_node_id(&voters, backup_id) {
                        let sol = voter.activated_stake as f64 / 1_000_000_000.0;
                        println!(" ❌ Fail (stake: {:.2} SOL)", sol);
                        errors.push(format!(
                            "Backup validator ID ({}) should not be on leader scheduler. It must be a non-staked validator.",
                            backup_id
                        ));
                    } else {
                        println!(" ✅ OK (not a staked validator)");
                    }
                } else {
                    println!("❌ Gossip Fail",);
                    errors.push(format!(
                        "Backup validator ID ({}) is not visible in gossip. Backup validators must appear in gossip to be considered valid.",
                        backup_id
                    ));
                }
            }
        }

        if !errors.is_empty() {
            println!("\nErrors found:");
            for error in errors {
                println!(" - {}", error);
            }
            if !force {
                return Ok(());
            }
        }

        println!(
            "\n\nTo request access, sign the following message with your validator's identity key:\n"
        );

        // Create attestation
        let attestation = SolanaValidatorAttestation {
            validator_id: primary_validator_id,
            service_key: doublezero_address,
            ed25519_signature: [0u8; 64],
        };

        // Verify the signature.
        let raw_message = if backup_validator_ids.is_empty() {
            AccessRequest::access_request_message(&AccessMode::SolanaValidator(attestation))
        } else {
            AccessRequest::access_request_message(&AccessMode::SolanaValidatorWithBackupIds {
                attestation,
                backup_ids: backup_validator_ids.clone(),
            })
        };

        println!(
            "solana sign-offchain-message \\\n   {raw_message} \\\n   -k <identity-keypair-file.json>\n"
        );

        Ok(())
    }
}
