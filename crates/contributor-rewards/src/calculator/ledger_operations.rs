use crate::{
    calculator::{
        input::RewardInput,
        keypair_loader::load_keypair,
        proof::{ShapleyOutputStorage, generate_proof_from_shapley},
        recorder::{compute_record_address, write_to_ledger},
    },
    ingestor::fetcher::Fetcher,
    processor::{
        internet::{InternetTelemetryStatMap, print_internet_stats},
        telemetry::{DZDTelemetryStatMap, print_telemetry_stats},
    },
    settings::Settings,
};
use anyhow::{Result, anyhow, bail};
use backon::{ExponentialBuilder, Retryable};
use borsh::BorshSerialize;
use doublezero_record::{instruction as record_ix, state::RecordData};
use solana_client::{
    client_error::ClientError as SolanaClientError, nonblocking::rpc_client::RpcClient,
};
use solana_sdk::{
    commitment_config::CommitmentConfig, message::Message, pubkey::Pubkey, signature::Keypair,
    signer::Signer, transaction::Transaction,
};
use std::{fmt, mem::size_of, path::PathBuf, str::FromStr, time::Duration};
use tabled::{Table, Tabled, settings::Style};
use tracing::{debug, info, warn};

// Helper functions to get prefixes from config
fn get_device_telemetry_prefix(settings: &Settings) -> Result<Vec<u8>> {
    Ok(settings.prefixes.device_telemetry.as_bytes().to_vec())
}

fn get_internet_telemetry_prefix(settings: &Settings) -> Result<Vec<u8>> {
    Ok(settings.prefixes.internet_telemetry.as_bytes().to_vec())
}

fn get_contributor_rewards_prefix(settings: &Settings) -> Result<Vec<u8>> {
    Ok(settings.prefixes.contributor_rewards.as_bytes().to_vec())
}

fn get_reward_input_prefix(settings: &Settings) -> Result<Vec<u8>> {
    Ok(settings.prefixes.reward_input.as_bytes().to_vec())
}

/// Result of a write operation
#[derive(Debug)]
pub enum WriteResult {
    Success(String),
    Failed(String, String), // (description, error)
}

/// Summary of all ledger writes
#[derive(Debug, Default)]
pub struct WriteSummary {
    pub results: Vec<WriteResult>,
}

impl WriteSummary {
    pub fn add_success(&mut self, description: String) {
        self.results.push(WriteResult::Success(description));
    }

    pub fn add_failure(&mut self, description: String, error: String) {
        self.results.push(WriteResult::Failed(description, error));
    }

    pub fn successful_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| matches!(r, WriteResult::Success(_)))
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| matches!(r, WriteResult::Failed(_, _)))
            .count()
    }

    pub fn total_count(&self) -> usize {
        self.results.len()
    }

    pub fn all_successful(&self) -> bool {
        self.failed_count() == 0
    }
}

impl fmt::Display for WriteSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=========================================")?;
        writeln!(f, "Ledger Write Summary")?;
        writeln!(f, "=========================================")?;
        writeln!(
            f,
            "Total: {}/{} successful",
            self.successful_count(),
            self.total_count()
        )?;

        if !self.all_successful() {
            writeln!(f, " Failed writes:")?;
            for result in &self.results {
                if let WriteResult::Failed(desc, error) = result {
                    writeln!(f, "  [FAILED] {desc}: {error}")?;
                }
            }
        }
        writeln!(f, " All writes:")?;
        for result in &self.results {
            match result {
                WriteResult::Success(desc) => writeln!(f, "  [OK] {desc}")?,
                WriteResult::Failed(desc, _) => writeln!(f, "  [FAILED] {desc}")?,
            }
        }

        writeln!(f, "=========================================")?;
        Ok(())
    }
}

/// Simple helper to write and track results
pub async fn write_and_track<T: BorshSerialize>(
    rpc_client: &RpcClient,
    payer_signer: &Keypair,
    seeds: &[&[u8]],
    data: &T,
    description: &str,
    summary: &mut WriteSummary,
    rps_limit: u32,
) {
    match write_to_ledger(
        rpc_client,
        payer_signer,
        seeds,
        data,
        description,
        rps_limit,
    )
    .await
    {
        Ok(_) => {
            info!("[OK] Successfully wrote {}", description);
            summary.add_success(description.to_string());
        }
        Err(e) => {
            warn!("[FAILED] Failed to write {}: {}", description, e);
            summary.add_failure(description.to_string(), e.to_string());
        }
    }
}

// ========== READ OPERATIONS ==========

/// Read telemetry aggregates from the ledger
pub async fn read_telemetry_aggregates(
    settings: &Settings,
    epoch: u64,
    payer_pubkey: &Pubkey,
    telemetry_type: &str,
    output_csv: Option<PathBuf>,
) -> Result<()> {
    // Create fetcher
    let fetcher = Fetcher::from_settings(settings)?;

    let mut device_stats: Option<DZDTelemetryStatMap> = None;
    let mut internet_stats: Option<InternetTelemetryStatMap> = None;

    // Read device telemetry if requested
    if telemetry_type == "device" || telemetry_type == "all" {
        let prefix = get_device_telemetry_prefix(settings)?;
        let epoch_bytes = epoch.to_le_bytes();
        let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
        let record_key = compute_record_address(payer_pubkey, seeds)?;

        info!("Re-created record_key: {record_key}");

        let maybe_account = (|| async {
            fetcher
                .rpc_client
                .get_account_with_commitment(&record_key, CommitmentConfig::confirmed())
                .await
        })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;

        match maybe_account.value {
            None => bail!("account {record_key} has no data!"),
            Some(acc) => {
                let stats: DZDTelemetryStatMap =
                    borsh::from_slice(&acc.data[size_of::<RecordData>()..])?;
                device_stats = Some(stats.clone());
                println!(
                    "Device Telemetry Aggregates:\n{}",
                    print_telemetry_stats(&stats)
                );
            }
        }
    }

    // Read internet telemetry if requested
    if telemetry_type == "internet" || telemetry_type == "all" {
        let prefix = get_internet_telemetry_prefix(settings)?;
        let epoch_bytes = epoch.to_le_bytes();
        let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
        let record_key = compute_record_address(payer_pubkey, seeds)?;

        info!("Re-created record_key: {record_key}");

        let maybe_account = (|| async {
            fetcher
                .rpc_client
                .get_account_with_commitment(&record_key, CommitmentConfig::confirmed())
                .await
        })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;

        match maybe_account.value {
            None => bail!("account {record_key} has no data!"),
            Some(acc) => {
                let stats: InternetTelemetryStatMap =
                    borsh::from_slice(&acc.data[size_of::<RecordData>()..])?;
                internet_stats = Some(stats.clone());
                println!(
                    "Internet Telemetry Aggregates:\n{}",
                    print_internet_stats(&stats)
                );
            }
        }
    }

    // Export to CSV if requested
    if let Some(output_path) = output_csv {
        use csv::Writer;

        // Create parent directories if they don't exist
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Export device telemetry if available
        if let Some(device_data) = device_stats {
            let device_file = if telemetry_type == "all" {
                output_path.with_file_name(format!(
                    "{}_device.csv",
                    output_path
                        .file_stem()
                        .unwrap_or_default()
                        .to_string_lossy()
                ))
            } else {
                output_path.clone()
            };

            let mut writer = Writer::from_path(&device_file)?;
            // Write headers
            writer.write_record([
                "circuit",
                "link_pubkey",
                "origin_device",
                "target_device",
                "rtt_mean_us",
                "rtt_median_us",
                "rtt_min_us",
                "rtt_max_us",
                "rtt_p90_us",
                "rtt_p95_us",
                "rtt_p99_us",
                "rtt_stddev_us",
                "avg_jitter_us",
                "jitter_ewma_us",
                "max_jitter_us",
                "packet_loss",
                "loss_count",
                "success_count",
                "total_samples",
            ])?;

            for (_, stats) in device_data.iter() {
                writer.write_record([
                    &stats.circuit,
                    &stats.link_pubkey.to_string(),
                    &stats.origin_device.to_string(),
                    &stats.target_device.to_string(),
                    &stats.rtt_mean_us.to_string(),
                    &stats.rtt_median_us.to_string(),
                    &stats.rtt_min_us.to_string(),
                    &stats.rtt_max_us.to_string(),
                    &stats.rtt_p90_us.to_string(),
                    &stats.rtt_p95_us.to_string(),
                    &stats.rtt_p99_us.to_string(),
                    &stats.rtt_stddev_us.to_string(),
                    &stats.avg_jitter_us.to_string(),
                    &stats.jitter_ewma_us.to_string(),
                    &stats.max_jitter_us.to_string(),
                    &stats.packet_loss.to_string(),
                    &stats.loss_count.to_string(),
                    &stats.success_count.to_string(),
                    &stats.total_samples.to_string(),
                ])?;
            }
            writer.flush()?;
            info!("Device telemetry exported to: {}", device_file.display());
        }

        // Export internet telemetry if available
        if let Some(internet_data) = internet_stats {
            let internet_file = if telemetry_type == "all" {
                output_path.with_file_name(format!(
                    "{}_internet.csv",
                    output_path
                        .file_stem()
                        .unwrap_or_default()
                        .to_string_lossy()
                ))
            } else {
                output_path.clone()
            };

            let mut writer = Writer::from_path(&internet_file)?;
            // Write headers
            writer.write_record([
                "circuit",
                "origin_exchange_code",
                "target_exchange_code",
                "data_provider_name",
                "oracle_agent_pk",
                "origin_exchange_pk",
                "target_exchange_pk",
                "rtt_mean_us",
                "rtt_median_us",
                "rtt_min_us",
                "rtt_max_us",
                "rtt_p90_us",
                "rtt_p95_us",
                "rtt_p99_us",
                "rtt_stddev_us",
                "avg_jitter_us",
                "jitter_ewma_us",
                "max_jitter_us",
                "packet_loss",
                "loss_count",
                "success_count",
                "total_samples",
            ])?;

            for (_, stats) in internet_data.iter() {
                writer.write_record([
                    &stats.circuit,
                    &stats.origin_exchange_code,
                    &stats.target_exchange_code,
                    &stats.data_provider_name,
                    &stats.oracle_agent_pk.to_string(),
                    &stats.origin_exchange_pk.to_string(),
                    &stats.target_exchange_pk.to_string(),
                    &stats.rtt_mean_us.to_string(),
                    &stats.rtt_median_us.to_string(),
                    &stats.rtt_min_us.to_string(),
                    &stats.rtt_max_us.to_string(),
                    &stats.rtt_p90_us.to_string(),
                    &stats.rtt_p95_us.to_string(),
                    &stats.rtt_p99_us.to_string(),
                    &stats.rtt_stddev_us.to_string(),
                    &stats.avg_jitter_us.to_string(),
                    &stats.jitter_ewma_us.to_string(),
                    &stats.max_jitter_us.to_string(),
                    &stats.packet_loss.to_string(),
                    &stats.loss_count.to_string(),
                    &stats.success_count.to_string(),
                    &stats.total_samples.to_string(),
                ])?;
            }
            writer.flush()?;
            info!(
                "Internet telemetry exported to: {}",
                internet_file.display()
            );
        }
    }

    // Validate type parameter
    if telemetry_type != "device" && telemetry_type != "internet" && telemetry_type != "all" {
        bail!(
            "Invalid telemetry type '{}'. Must be 'device', 'internet', or 'all'",
            telemetry_type
        );
    }

    Ok(())
}

/// Read reward input from the ledger
pub async fn read_reward_input(
    settings: &Settings,
    epoch: u64,
    payer_pubkey: &Pubkey,
) -> Result<()> {
    // Create fetcher
    let fetcher = Fetcher::from_settings(settings)?;

    let prefix = get_reward_input_prefix(settings)?;
    let epoch_bytes = epoch.to_le_bytes();
    let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
    let record_key = compute_record_address(payer_pubkey, seeds)?;

    info!("Fetching calculation input from: {}", record_key);

    let maybe_account = (|| async {
        fetcher
            .rpc_client
            .get_account_with_commitment(&record_key, CommitmentConfig::confirmed())
            .await
    })
    .retry(&ExponentialBuilder::default().with_jitter())
    .notify(|err: &SolanaClientError, dur: Duration| {
        info!("retrying error: {:?} with sleeping {:?}", err, dur)
    })
    .await?;

    let input_config = match maybe_account.value {
        None => bail!(
            "Calculation input account {} not found for epoch {}",
            record_key,
            epoch
        ),
        Some(acc) => {
            let data: RewardInput = borsh::from_slice(&acc.data[size_of::<RecordData>()..])?;
            data
        }
    };

    // Display the configuration using tabled

    #[derive(Tabled)]
    struct RewardInputDisplay {
        #[tabled(rename = "Field")]
        field: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let input_data = vec![
        RewardInputDisplay {
            field: "Epoch".to_string(),
            value: input_config.epoch.to_string(),
        },
        RewardInputDisplay {
            field: "Timestamp".to_string(),
            value: input_config.timestamp.to_string(),
        },
        RewardInputDisplay {
            field: "Devices".to_string(),
            value: input_config.devices.len().to_string(),
        },
        RewardInputDisplay {
            field: "Private Links".to_string(),
            value: input_config.private_links.len().to_string(),
        },
        RewardInputDisplay {
            field: "Public Links".to_string(),
            value: input_config.public_links.len().to_string(),
        },
        RewardInputDisplay {
            field: "Demands".to_string(),
            value: input_config.demands.len().to_string(),
        },
        RewardInputDisplay {
            field: "Cities".to_string(),
            value: input_config.city_summaries.len().to_string(),
        },
        RewardInputDisplay {
            field: "Operator Uptime".to_string(),
            value: input_config.shapley_settings.operator_uptime.to_string(),
        },
        RewardInputDisplay {
            field: "Contiguity Bonus".to_string(),
            value: input_config.shapley_settings.contiguity_bonus.to_string(),
        },
        RewardInputDisplay {
            field: "Demand Multiplier".to_string(),
            value: input_config.shapley_settings.demand_multiplier.to_string(),
        },
    ];

    println!("Reward Calculation Input Configuration");
    println!("=========================================");
    println!(
        "{}",
        Table::new(input_data).with(Style::psql().remove_horizontals())
    );

    // Optionally validate checksums if telemetry data is available
    info!(
        "Successfully retrieved calculation input for epoch {}",
        epoch
    );

    Ok(())
}

/// Check contributor reward and verify merkle proof dynamically
pub async fn check_contributor_reward(
    settings: &Settings,
    contributor: &str,
    epoch: u64,
    payer_pubkey: &Pubkey,
) -> Result<()> {
    // Parse contributor as a Pubkey
    let contributor_pubkey = Pubkey::from_str(contributor)
        .map_err(|e| anyhow!("Invalid contributor pubkey '{}': {}", contributor, e))?;

    // Fetch the shapley output storage
    let shapley_storage = read_shapley_output(settings, epoch, payer_pubkey).await?;

    // Generate proof dynamically
    info!(
        "Generating proof dynamically for contributor: {}",
        contributor
    );
    let (proof, reward, computed_root) =
        generate_proof_from_shapley(&shapley_storage, &contributor_pubkey)?;
    debug!("proof: {:?}", proof);

    // POD-based proof verification is handled by comparing roots
    // POD verification - check that the proof is valid by comparing roots
    use doublezero_revenue_distribution::types::RewardShare;
    use svm_hash::merkle::merkle_root_from_indexed_pod_leaves;
    let verification_root = merkle_root_from_indexed_pod_leaves(
        &shapley_storage.rewards,
        Some(RewardShare::LEAF_PREFIX),
    )
    .unwrap();
    let verification_result = verification_root == computed_root;

    #[derive(Tabled)]
    struct RewardVerification {
        #[tabled(rename = "Field")]
        field: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let verification_data = vec![
        RewardVerification {
            field: "Epoch".to_string(),
            value: epoch.to_string(),
        },
        RewardVerification {
            field: "Contributor".to_string(),
            value: contributor.to_string(),
        },
        RewardVerification {
            field: "Pubkey".to_string(),
            value: reward.contributor_key.to_string(),
        },
        RewardVerification {
            field: "Unit Share".to_string(),
            value: format!("{}", reward.unit_share),
        },
        RewardVerification {
            field: "Merkle Root".to_string(),
            value: format!("{computed_root:?}"),
        },
        RewardVerification {
            field: "Total Contributors".to_string(),
            value: shapley_storage.rewards.len().to_string(),
        },
        RewardVerification {
            field: "Total Units".to_string(),
            value: format!(
                "{} (should be 1,000,000,000)",
                shapley_storage.total_unit_shares
            ),
        },
        RewardVerification {
            field: "Verification Status".to_string(),
            value: if verification_result {
                "[VALID] Proof verified successfully!".to_string()
            } else {
                "[INVALID] Proof verification failed!".to_string()
            },
        },
    ];

    println!("Contributor Reward Verification");
    println!("=========================================");
    println!(
        "{}",
        Table::new(verification_data).with(Style::psql().remove_horizontals())
    );

    if !verification_result {
        anyhow::bail!("Merkle proof verification failed");
    }

    Ok(())
}

/// Write shapley output storage to the ledger
pub async fn write_shapley_output(
    rpc_client: &RpcClient,
    payer_signer: &Keypair,
    epoch: u64,
    shapley_storage: &ShapleyOutputStorage,
    settings: &Settings,
) -> Result<()> {
    let prefix = get_contributor_rewards_prefix(settings)?;
    let epoch_bytes = epoch.to_le_bytes();
    let seeds: &[&[u8]] = &[&prefix, &epoch_bytes, b"shapley_output"];

    let mut summary = WriteSummary::default();
    write_and_track(
        rpc_client,
        payer_signer,
        seeds,
        shapley_storage,
        "shapley output storage",
        &mut summary,
        settings.rpc.rps_limit,
    )
    .await;

    if !summary.all_successful() {
        bail!("Failed to write shapley output storage");
    }

    Ok(())
}

/// Read shapley output storage from the ledger
pub async fn read_shapley_output(
    settings: &Settings,
    epoch: u64,
    payer_pubkey: &Pubkey,
) -> Result<ShapleyOutputStorage> {
    let fetcher = Fetcher::from_settings(settings)?;
    let prefix = get_contributor_rewards_prefix(settings)?;
    let epoch_bytes = epoch.to_le_bytes();
    let seeds: &[&[u8]] = &[&prefix, &epoch_bytes, b"shapley_output"];
    let storage_key = compute_record_address(payer_pubkey, seeds)?;

    info!("Fetching shapley output from: {}", storage_key);

    let maybe_account = (|| async {
        fetcher
            .rpc_client
            .get_account_with_commitment(&storage_key, CommitmentConfig::confirmed())
            .await
    })
    .retry(&ExponentialBuilder::default().with_jitter())
    .notify(|err: &SolanaClientError, dur: Duration| {
        info!("retrying error: {:?} with sleeping {:?}", err, dur)
    })
    .await?;

    let shapley_storage = match maybe_account.value {
        None => bail!(
            "Shapley output storage account {} not found for epoch {}",
            storage_key,
            epoch
        ),
        Some(acc) => {
            let data: ShapleyOutputStorage =
                borsh::from_slice(&acc.data[size_of::<RecordData>()..])?;
            data
        }
    };

    Ok(shapley_storage)
}

/// NOTE: This is mostly just for debugging
/// Realloc a record account
pub async fn realloc_record(
    settings: &Settings,
    r#type: &str,
    epoch: u64,
    size: u64,
    keypair_path: Option<PathBuf>,
    dry_run: bool,
) -> Result<()> {
    // Load keypair
    let payer_signer = load_keypair(&keypair_path)?;

    // Create fetcher for RPC client
    let fetcher = Fetcher::from_settings(settings)?;

    // Determine the prefix and compute the record address based on record type
    let epoch_bytes = epoch.to_le_bytes();
    let record_key = match r#type {
        "device-telemetry" => {
            let prefix = get_device_telemetry_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        "internet-telemetry" => {
            let prefix = get_internet_telemetry_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        "reward-input" => {
            let prefix = get_reward_input_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        "contributor-rewards" => {
            let prefix = get_contributor_rewards_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes, b"shapley_output"];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        _ => bail!(
            "Invalid record type. Must be one of: device-telemetry, internet-telemetry, reward-input, contributor-rewards"
        ),
    };

    info!("Reallocating record account: {}", record_key);
    info!("Record type: {}, Epoch: {}", r#type, epoch);

    // Check if the account exists
    let maybe_account = (|| async {
        fetcher
            .rpc_client
            .get_account_with_commitment(&record_key, CommitmentConfig::confirmed())
            .await
    })
    .retry(&ExponentialBuilder::default().with_jitter())
    .notify(|err: &SolanaClientError, dur: Duration| {
        info!("retrying error: {:?} with sleeping {:?}", err, dur)
    })
    .await?;

    if maybe_account.value.is_none() {
        bail!("Record account {} does not exist", record_key);
    }

    // Create realloc instruction
    let realloc_ix = record_ix::reallocate(&record_key, &payer_signer.pubkey(), size);

    // Create and send transaction
    let recent_blockhash = (|| async { fetcher.rpc_client.get_latest_blockhash().await })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;

    let message = Message::new(&[realloc_ix], Some(&payer_signer.pubkey()));
    let transaction = Transaction::new(&[&payer_signer], message, recent_blockhash);

    if !dry_run {
        let signature = (|| async {
            fetcher
                .rpc_client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &transaction,
                    CommitmentConfig::confirmed(),
                )
                .await
        })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;
        info!("Transaction signature: {}", signature);
        info!("Account realloc successful!");
    } else {
        info!("DRY-RUN mode, would have sent {:#?}", transaction)
    }

    Ok(())
}

/// NOTE: This is mostly just for debugging
/// Close a record account and reclaim lamports
pub async fn close_record(
    settings: &Settings,
    r#type: &str,
    epoch: u64,
    keypair_path: Option<PathBuf>,
    dry_run: bool,
) -> Result<()> {
    // Load keypair
    let payer_signer = load_keypair(&keypair_path)?;

    // Create fetcher for RPC client
    let fetcher = Fetcher::from_settings(settings)?;

    // Determine the prefix and compute the record address based on record type
    let epoch_bytes = epoch.to_le_bytes();
    let record_key = match r#type {
        "device-telemetry" => {
            let prefix = get_device_telemetry_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        "internet-telemetry" => {
            let prefix = get_internet_telemetry_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        "reward-input" => {
            let prefix = get_reward_input_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        "contributor-rewards" => {
            let prefix = get_contributor_rewards_prefix(settings)?;
            let seeds: &[&[u8]] = &[&prefix, &epoch_bytes, b"shapley_output"];
            compute_record_address(&payer_signer.pubkey(), seeds)?
        }
        _ => bail!(
            "Invalid record type. Must be one of: device-telemetry, internet-telemetry, reward-input, contributor-rewards"
        ),
    };

    info!("Closing record account: {}", record_key);
    info!("Record type: {}, Epoch: {}", r#type, epoch);

    // Check if the account exists
    let maybe_account = (|| async {
        fetcher
            .rpc_client
            .get_account_with_commitment(&record_key, CommitmentConfig::confirmed())
            .await
    })
    .retry(&ExponentialBuilder::default().with_jitter())
    .notify(|err: &SolanaClientError, dur: Duration| {
        info!("retrying error: {:?} with sleeping {:?}", err, dur)
    })
    .await?;

    if maybe_account.value.is_none() {
        bail!("Record account {} does not exist", record_key);
    }

    // Create close instruction
    let close_ix = record_ix::close_account(
        &record_key,
        &payer_signer.pubkey(),
        &payer_signer.pubkey(), // Return lamports to payer
    );

    // Create and send transaction
    let recent_blockhash = (|| async { fetcher.rpc_client.get_latest_blockhash().await })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;

    let message = Message::new(&[close_ix], Some(&payer_signer.pubkey()));
    let transaction = Transaction::new(&[&payer_signer], message, recent_blockhash);

    if !dry_run {
        let signature = (|| async {
            fetcher
                .rpc_client
                .send_and_confirm_transaction_with_spinner_and_commitment(
                    &transaction,
                    CommitmentConfig::confirmed(),
                )
                .await
        })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;
        info!("Transaction signature: {}", signature);
        info!("Account closed successfully!");
    } else {
        info!("DRY-RUN mode, would have sent {:#?}", transaction)
    }

    Ok(())
}

/// Inspect record accounts for a given epoch
pub async fn inspect_records(
    settings: &Settings,
    epoch: u64,
    payer_pubkey: &Pubkey,
    record_type: Option<String>,
) -> Result<()> {
    let fetcher = Fetcher::from_settings(settings)?;
    let epoch_bytes = epoch.to_le_bytes();

    // Define all record types to inspect
    let record_types = if let Some(specific_type) = record_type {
        vec![specific_type]
    } else {
        vec![
            "device-telemetry".to_string(),
            "internet-telemetry".to_string(),
            "reward-input".to_string(),
            "contributor-rewards".to_string(),
        ]
    };

    #[derive(Tabled)]
    struct RecordInfo {
        #[tabled(rename = "Type")]
        record_type: String,
        #[tabled(rename = "Address")]
        address: String,
        #[tabled(rename = "Size (bytes)")]
        size: String,
        #[tabled(rename = "Status")]
        status: String,
    }

    let mut records = Vec::new();

    for r_type in record_types {
        let record_key = match r_type.as_str() {
            "device-telemetry" => {
                let prefix = get_device_telemetry_prefix(settings)?;
                let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
                compute_record_address(payer_pubkey, seeds)?
            }
            "internet-telemetry" => {
                let prefix = get_internet_telemetry_prefix(settings)?;
                let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
                compute_record_address(payer_pubkey, seeds)?
            }
            "reward-input" => {
                let prefix = get_reward_input_prefix(settings)?;
                let seeds: &[&[u8]] = &[&prefix, &epoch_bytes];
                compute_record_address(payer_pubkey, seeds)?
            }
            "contributor-rewards" => {
                let prefix = get_contributor_rewards_prefix(settings)?;
                let seeds: &[&[u8]] = &[&prefix, &epoch_bytes, b"shapley_output"];
                compute_record_address(payer_pubkey, seeds)?
            }
            _ => bail!("Unknown record type: {}", r_type),
        };

        // Try to fetch the account
        let maybe_account = (|| async {
            fetcher
                .rpc_client
                .get_account_with_commitment(&record_key, CommitmentConfig::confirmed())
                .await
        })
        .retry(&ExponentialBuilder::default().with_jitter())
        .notify(|err: &SolanaClientError, dur: Duration| {
            info!("retrying error: {:?} with sleeping {:?}", err, dur)
        })
        .await?;

        let (size, status) = match maybe_account.value {
            None => ("0".to_string(), "Not found".to_string()),
            Some(acc) => {
                let data_size = acc.data.len();
                if data_size <= size_of::<RecordData>() {
                    (data_size.to_string(), "Empty (header only)".to_string())
                } else {
                    (data_size.to_string(), "Contains data".to_string())
                }
            }
        };

        records.push(RecordInfo {
            record_type: r_type,
            address: record_key.to_string(),
            size,
            status,
        });
    }

    println!("Record Accounts for Epoch {epoch}");
    println!("=========================================");
    println!(
        "{}",
        Table::new(records).with(Style::psql().remove_horizontals())
    );

    Ok(())
}
