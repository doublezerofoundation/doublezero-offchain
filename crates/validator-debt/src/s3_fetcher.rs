//! S3 Validator Pubkeys Fetcher
//!
//! This module fetches validator public keys from the S3 metrics bucket by:
//! 1. Downloading hourly Parquet snapshots for a given Solana epoch
//! 2. Merging gossip, validators, users, and devices datasets
//! 3. Applying the 12-hour connection rule (validators must appear in >12 hourly snapshots)
//! 4. Returning the list of qualifying validator public keys
//!
//! This replicates the canonical Python script approach for identifying validators
//! eligible for fees, replacing the point-in-time access pass approach.
//!
//! ## Environment Variables
//!
//! Required:
//! - `VALIDATOR_DEBT_AWS_ACCESS_KEY_ID`: AWS access key ID for S3 access
//! - `VALIDATOR_DEBT_AWS_SECRET_ACCESS_KEY`: AWS secret access key for S3 access
//!
//! Optional:
//! - `VALIDATOR_DEBT_S3_BUCKET`: S3 bucket name (default: "malbeclabs-data-metrics-dev")
//! - `VALIDATOR_DEBT_AWS_REGION`: AWS region (default: "us-east-1")
//! - `VALIDATOR_DEBT_S3_MAX_CONSECUTIVE_FAILURES`: Max consecutive failures before stopping (default: 12)
//! - `VALIDATOR_DEBT_S3_ENDPOINT`: Custom S3 endpoint for S3-compatible services (optional)

use std::{collections::HashMap, env, fs::File as StdFile};

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    config::{Credentials, Region},
};
use chrono::{DateTime, Duration, Timelike, Utc};
use polars::prelude::*;
use serde::Serialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use tempfile::NamedTempFile;
use tokio::{fs::File, io::AsyncWriteExt};
use tracing::{debug, info, warn};

/// Mainnet threshold date (same as python)
const MAINNET_THRESHOLD: &str = "2025-09-12T21:00:00Z";

/// Validator identity pubkey and vote account pubkey pair
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ValidatorKey {
    pub pubkey: String,
}

/// Network type for dataset selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    MainnetBeta,
    Testnet,
}

impl Network {
    fn prefix(&self) -> &'static str {
        match self {
            Network::MainnetBeta => "mainnet-beta",
            Network::Testnet => "testnet",
        }
    }
}

/// S3 configuration
struct S3Config {
    client: S3Client,
    bucket: String,
    max_consecutive_failures: usize,
}

impl S3Config {
    async fn new() -> Result<Self> {
        let bucket = env::var("VALIDATOR_DEBT_S3_BUCKET")
            .unwrap_or_else(|_| "malbeclabs-data-metrics-dev".to_string());

        let max_consecutive_failures = env::var("VALIDATOR_DEBT_S3_MAX_CONSECUTIVE_FAILURES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(12);

        // Load AWS credentials from environment variables
        let access_key_id = env::var("VALIDATOR_DEBT_AWS_ACCESS_KEY_ID")
            .context("VALIDATOR_DEBT_AWS_ACCESS_KEY_ID environment variable not set")?;

        let secret_access_key = env::var("VALIDATOR_DEBT_AWS_SECRET_ACCESS_KEY")
            .context("VALIDATOR_DEBT_AWS_SECRET_ACCESS_KEY environment variable not set")?;

        let region =
            env::var("VALIDATOR_DEBT_AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        // Create credentials
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "validator-debt-s3-fetcher",
        );

        // Build S3 config with explicit credentials
        let mut config_builder = aws_sdk_s3::Config::builder()
            .region(Region::new(region.clone()))
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(credentials);

        // Support custom endpoint (for MinIO or other S3-compatible services)
        if let Ok(endpoint) = env::var("VALIDATOR_DEBT_S3_ENDPOINT") {
            info!("Using custom S3 endpoint: {}", endpoint);
            config_builder = config_builder.endpoint_url(endpoint).force_path_style(true);
        }

        let config = config_builder.build();
        let client = S3Client::from_conf(config);

        info!(
            "S3 client initialized: bucket={}, region={}, max_consecutive_failures={}",
            bucket, region, max_consecutive_failures
        );

        Ok(Self {
            client,
            bucket,
            max_consecutive_failures,
        })
    }
}

/// Fetches validator public keys for a given Solana epoch from S3 metrics bucket
///
/// This function replicates the canonical Python script approach:
/// 1. Converts epoch to timestamp range
/// 2. Downloads hourly Parquet files from S3
/// 3. Merges datasets and applies filters
/// 4. Applies 12-hour connection rule
/// 5. Returns validator keys
pub async fn fetch_validator_pubkeys(
    solana_epoch: u64,
    rpc_client: &RpcClient,
    network: Network,
) -> Result<Vec<ValidatorKey>> {
    info!(
        "Fetching validator pubkeys for Solana epoch {} ({:?})",
        solana_epoch, network
    );

    let s3_config = S3Config::new().await?;

    // Convert epoch to timestamp range
    let (start_time, end_time) = epoch_to_timestamps(rpc_client, solana_epoch).await?;
    info!(
        "Epoch {} time range: {} to {}",
        solana_epoch, start_time, end_time
    );

    // Generate hourly timestamps
    let hourly_timestamps = generate_hourly_timestamps(start_time, end_time);
    info!(
        "Processing {} hourly snapshots for epoch {}",
        hourly_timestamps.len(),
        solana_epoch
    );

    // Check if we need mainnet datasets based on threshold
    let mainnet_threshold: DateTime<Utc> = MAINNET_THRESHOLD.parse()?;
    let include_mainnet = network == Network::MainnetBeta && end_time >= mainnet_threshold;

    // Fetch and process hourly data
    let mut all_validators: HashMap<String, usize> = HashMap::new();
    let mut consecutive_failures = 0;

    for (idx, timestamp) in hourly_timestamps.iter().enumerate() {
        debug!(
            "Processing hour {}/{}: {}",
            idx + 1,
            hourly_timestamps.len(),
            timestamp
        );

        match process_hourly_data(&s3_config, *timestamp, network, include_mainnet).await {
            Ok(validators) => {
                consecutive_failures = 0;
                let count = validators.len();

                // Count appearances for each validator
                for validator in validators {
                    *all_validators
                        .entry(validator.pubkey.clone())
                        .or_insert(0) += 1;
                }

                info!(
                    "Hour {}: Found {} validators (total unique: {})",
                    timestamp.format("%Y-%m-%d %H:00"),
                    count,
                    all_validators.len()
                );
            }
            Err(e) => {
                consecutive_failures += 1;
                warn!(
                    "Failed to process hour {} (consecutive failures: {}): {}",
                    timestamp.format("%Y-%m-%d %H:00"),
                    consecutive_failures,
                    e
                );

                if consecutive_failures >= s3_config.max_consecutive_failures {
                    warn!(
                        "Reached {} consecutive failures, stopping processing",
                        s3_config.max_consecutive_failures
                    );
                    break;
                }
            }
        }
    }

    // Apply 12-hour connection rule: keep only validators with >12 appearances
    let qualified_validators: Vec<ValidatorKey> = all_validators
        .into_iter()
        .filter_map(|(identity, count)| {
            if count > 12 {
                Some(ValidatorKey {
                    pubkey: identity,
                })
            } else {
                None
            }
        })
        .collect();

    info!(
        "Applied 12-hour rule: {} validators qualified (appeared in >12 hourly snapshots)",
        qualified_validators.len()
    );

    Ok(qualified_validators)
}

/// Converts Solana epoch number to start and end timestamps
async fn epoch_to_timestamps(
    rpc_client: &RpcClient,
    epoch: u64,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    // Calculate the first slot of the target epoch
    // Solana epochs have 432,000 slots each
    const SLOTS_PER_EPOCH: u64 = 432_000;
    let epoch_start_slot = epoch * SLOTS_PER_EPOCH;
    let epoch_end_slot = epoch_start_slot + SLOTS_PER_EPOCH - 1;

    // Get block time for first slot of epoch
    let start_timestamp = rpc_client
        .get_block_time(epoch_start_slot)
        .await
        .context("Failed to get block time for epoch start")?;

    // Get block time for last slot of epoch
    let end_timestamp = rpc_client
        .get_block_time(epoch_end_slot)
        .await
        .context("Failed to get block time for epoch end")?;

    let start_time =
        DateTime::from_timestamp(start_timestamp, 0).context("Invalid start timestamp")?;
    let end_time = DateTime::from_timestamp(end_timestamp, 0).context("Invalid end timestamp")?;

    Ok((start_time, end_time))
}

/// Generates list of hourly timestamps between start and end (inclusive)
fn generate_hourly_timestamps(start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    let mut timestamps = Vec::new();
    let mut current = start
        .date_naive()
        .and_hms_opt(start.hour(), 0, 0)
        .unwrap()
        .and_utc();

    while current <= end {
        timestamps.push(current);
        current += Duration::hours(1);
    }

    timestamps
}

/// Processes data for a single hour: downloads Parquet files, merges, filters
async fn process_hourly_data(
    s3_config: &S3Config,
    timestamp: DateTime<Utc>,
    network: Network,
    _include_mainnet: bool,
) -> Result<Vec<ValidatorKey>> {
    // Download Parquet files for this hour
    let gossip_df = download_and_parse_parquet(
        s3_config,
        &format!("snapshot-solana-{}-gossip", network.prefix()),
        timestamp,
    )
    .await?;

    let validators_df = download_and_parse_parquet(
        s3_config,
        &format!("snapshot-solana-{}-validators", network.prefix()),
        timestamp,
    )
    .await?;

    let users_df = download_and_parse_parquet(
        s3_config,
        &format!("snapshot-doublezero-{}-device-users", network.prefix()),
        timestamp,
    )
    .await?;

    let devices_df = download_and_parse_parquet(
        s3_config,
        &format!("snapshot-doublezero-{}-devices", network.prefix()),
        timestamp,
    )
    .await?;

    // Merge datasets
    let merged = merge_hourly_datasets(gossip_df, validators_df, users_df, devices_df)?;

    // Extract validator keys
    extract_validator_keys(merged)
}

/// Downloads a Parquet file from S3 and parses it with Polars
async fn download_and_parse_parquet(
    s3_config: &S3Config,
    prefix: &str,
    timestamp: DateTime<Utc>,
) -> Result<DataFrame> {
    let key = build_s3_key(prefix, timestamp);
    debug!("Downloading s3://{}/{}", s3_config.bucket, key);

    // Download to temporary file
    let temp_file = NamedTempFile::new().context("Failed to create temporary file")?;
    let temp_path = temp_file.path().to_path_buf();

    let response = s3_config
        .client
        .get_object()
        .bucket(&s3_config.bucket)
        .key(&key)
        .send()
        .await
        .context(format!("Failed to download S3 object: {}", key))?;

    // Write to temp file
    let mut file = File::create(&temp_path).await?;
    let body = response.body.collect().await?;
    file.write_all(&body.into_bytes()).await?;
    file.flush().await?;
    drop(file); // Close file before reading

    // Parse Parquet with Polars
    let file = StdFile::open(&temp_path)?;
    let df = ParquetReader::new(file)
        .finish()
        .context(format!("Failed to parse Parquet file: {}", key))?;

    debug!(
        "Parsed {}: {} rows, {} columns",
        key,
        df.height(),
        df.width()
    );

    Ok(df)
}

/// Builds S3 key for a Parquet file
/// Format: datasets/{prefix}/date={YYYY-MM-DD}/hour={HH}/part-00000.parquet
fn build_s3_key(prefix: &str, timestamp: DateTime<Utc>) -> String {
    format!(
        "datasets/{}/date={}/hour={:02}/part-00000.parquet",
        prefix,
        timestamp.format("%Y-%m-%d"),
        timestamp.hour()
    )
}

/// Merges hourly datasets (gossip + validators + users + devices)
fn merge_hourly_datasets(
    gossip: DataFrame,
    validators: DataFrame,
    users: DataFrame,
    devices: DataFrame,
) -> Result<DataFrame> {
    debug!("Gossip columns: {:?}", gossip.get_column_names());
    debug!("Validators columns: {:?}", validators.get_column_names());
    debug!("Users columns: {:?}", users.get_column_names());
    debug!("Devices columns: {:?}", devices.get_column_names());

    // Merge gossip + validators on identity_pubkey
    let merged = gossip
        .join(
            &validators,
            ["identity_pubkey"],
            ["identity_pubkey"],
            JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns),
            None,
        )
        .context("Failed to join gossip and validators")?;

    debug!("After gossip+validators join: {} rows", merged.height());

    // Merge with users on IP address
    let merged = merged
        .join(
            &users,
            ["ip_address"],
            ["client_ip"],
            JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns),
            None,
        )
        .context("Failed to join with users")?;

    debug!("After users join: {} rows", merged.height());

    // Merge with devices on device_pubkey
    let merged = merged
        .join(
            &devices,
            ["device_pubkey"],
            ["pubkey"],
            JoinArgs::new(JoinType::Inner).with_coalesce(JoinCoalesce::CoalesceColumns),
            None,
        )
        .context("Failed to join with devices")?;

    debug!("After devices join: {} rows", merged.height());

    // Filter: only non-delinquent validators
    let filtered = merged
        .lazy()
        .filter(col("delinquent").eq(lit(false)))
        .collect()
        .context("Failed to filter delinquent validators")?;

    debug!("After filtering delinquent: {} rows", filtered.height());

    Ok(filtered)
}

/// Extracts validator keys from merged DataFrame
fn extract_validator_keys(df: DataFrame) -> Result<Vec<ValidatorKey>> {
    let identity_series = df
        .column("identity_pubkey")
        .context("Missing identity_pubkey column")?;
    let vote_series = df
        .column("vote_account_pubkey")
        .context("Missing vote_account_pubkey column")?;

    let identity_vec = identity_series.str()?.into_iter();
    let vote_vec = vote_series.str()?.into_iter();

    let validators: Vec<ValidatorKey> = identity_vec
        .zip(vote_vec)
        .filter_map(|(identity, _vote)| {
            Some(ValidatorKey {
                pubkey: identity?.to_string(),
            })
        })
        .collect();

    Ok(validators)
}
