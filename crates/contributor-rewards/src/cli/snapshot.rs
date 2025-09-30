use crate::{
    calculator::{data_prep::PreparedData, orchestrator::Orchestrator},
    cli::{
        common::{OutputFormat, OutputOptions, to_json_string},
        traits::Exportable,
    },
    ingestor::{
        epoch::{EpochFinder, LeaderSchedule},
        fetcher::Fetcher,
        types::FetchData,
    },
};
use anyhow::{Result, bail};
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, warn};

/// Snapshot export commands for raw chain data
#[derive(Subcommand, Debug)]
pub enum SnapshotCommands {
    #[command(
        about = "Export all chain data for an epoch (fetch_data, leader schedule, etc.)",
        after_help = r#"Examples:
    # Export everything for epoch 9
    snapshot all --epoch 9 --output-format json --output-file epoch-9-complete.json

    # Export to directory with automatic naming
    snapshot all --epoch 9 --output-format json-pretty --output-dir ./snapshots/"#
    )]
    All {
        /// DZ epoch to snapshot
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// Output format for export
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Directory to export files
        #[arg(short = 'o', long, value_name = "DIR")]
        output_dir: Option<PathBuf>,

        /// Specific output file path
        #[arg(long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },

    #[command(
        about = "Export raw fetch_data from chain",
        after_help = r#"Examples:
    # Export fetch_data for debugging
    snapshot fetch-data --epoch 9 --output-format json-pretty --output-file fetch-data.json"#
    )]
    FetchData {
        /// DZ epoch to fetch
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// Output format for export
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Directory to export files
        #[arg(short = 'o', long, value_name = "DIR")]
        output_dir: Option<PathBuf>,

        /// Specific output file path
        #[arg(long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },

    #[command(
        about = "Export Solana leader schedule for an epoch",
        after_help = r#"Examples:
    # Export leader schedule as CSV (using DZ epoch)
    snapshot leader-schedule -e 83 --output-format csv --output-file leaders.csv

    # Export as JSON for analysis
    snapshot leader-schedule -e 83 --output-format json-pretty
    "#
    )]
    LeaderSchedule {
        /// DZ epoch to use for timestamp mapping
        #[arg(short = 'e', long = "epoch", value_name = "EPOCH")]
        epoch: u64,

        /// Output format for export
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Directory to export files
        #[arg(short = 'o', long, value_name = "DIR")]
        output_dir: Option<PathBuf>,

        /// Specific output file path
        #[arg(long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },

    #[command(
        about = "Export processed snapshot with same logic as calculate-rewards (v2)",
        after_help = r#"Examples:
    # Export v2 snapshot for epoch 27
    snapshot snapshot-v2 --epoch 27 --output-format json-pretty --output-file snapshot-v2-27.json

    # Export to directory with automatic naming
    snapshot snapshot-v2 --epoch 27 --output-dir ./snapshots/"#
    )]
    SnapshotV2 {
        /// DZ epoch to snapshot
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// Output format for export
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Directory to export files
        #[arg(short = 'o', long, value_name = "DIR")]
        output_dir: Option<PathBuf>,

        /// Specific output file path
        #[arg(long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },
}

/// Complete snapshot containing all data
#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteSnapshot {
    pub dz_epoch: u64,
    pub solana_epoch: Option<u64>,
    pub fetch_data: FetchData,
    pub leader_schedule: Option<LeaderSchedule>,
    pub metadata: SnapshotMetadata,
}

/// Metadata about the snapshot
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub created_at: String,
    pub network: String,
    pub exchanges_count: usize,
    pub locations_count: usize,
    pub devices_count: usize,
    pub internet_samples_count: usize,
    pub device_samples_count: usize,
}

impl CompleteSnapshot {
    /// Load and validate snapshot from file
    pub fn load_from_file(path: &std::path::Path) -> Result<Self> {
        info!("Loading snapshot from: {:?}", path);
        let contents = std::fs::read_to_string(path)?;
        let snapshot: Self = serde_json::from_str(&contents)?;
        snapshot.validate()?;
        info!("✅ Snapshot loaded and validated successfully");
        Ok(snapshot)
    }

    /// Validate snapshot completeness and quality
    pub fn validate(&self) -> Result<()> {
        let mut issues = Vec::new();

        // Check serviceability completeness
        if self.fetch_data.dz_serviceability.devices.is_empty() {
            issues.push("No devices in snapshot");
        }
        if self.fetch_data.dz_serviceability.contributors.is_empty() {
            issues.push("No contributors in snapshot");
        }
        if self.fetch_data.dz_serviceability.exchanges.is_empty() {
            issues.push("No exchanges in snapshot");
        }
        if self.fetch_data.dz_serviceability.users.is_empty() {
            issues.push("No users in snapshot");
        }

        // Check telemetry completeness
        if self
            .fetch_data
            .dz_telemetry
            .device_latency_samples
            .is_empty()
        {
            issues.push("No device telemetry samples");
        }
        if self
            .fetch_data
            .dz_internet
            .internet_latency_samples
            .is_empty()
        {
            issues.push("No internet telemetry samples");
        }

        // Check leader schedule
        if self.leader_schedule.is_none() {
            issues.push("Missing leader schedule");
        } else if let Some(schedule) = &self.leader_schedule
            && schedule.schedule_map.is_empty()
        {
            issues.push("Leader schedule is empty");
        }

        if !issues.is_empty() {
            bail!("Snapshot validation failed:\n  - {}", issues.join("\n  - "));
        }

        info!("✅ Snapshot validation passed");
        info!("  - Epoch: {}", self.dz_epoch);
        info!("  - Devices: {}", self.metadata.devices_count);
        info!("  - Device samples: {}", self.metadata.device_samples_count);
        info!(
            "  - Internet samples: {}",
            self.metadata.internet_samples_count
        );
        info!("  - Exchanges: {}", self.metadata.exchanges_count);
        if let Some(schedule) = &self.leader_schedule {
            info!("  - Leaders: {}", schedule.schedule_map.len());
        }

        Ok(())
    }
}

// Implement Exportable traits
impl Exportable for CompleteSnapshot {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                bail!(
                    "CSV export not supported for complete snapshot. Export individual components instead."
                )
            }
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

impl Exportable for FetchData {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                bail!("CSV export not supported for FetchData. Use JSON format instead.")
            }
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

/// Handle snapshot commands
pub async fn handle(orchestrator: &Orchestrator, cmd: SnapshotCommands) -> Result<()> {
    match cmd {
        SnapshotCommands::All {
            epoch,
            output_format,
            output_dir,
            output_file,
        } => {
            info!("Starting complete snapshot export");

            // Create fetcher
            let fetcher = Fetcher::from_settings(orchestrator.settings())?;

            // Fetch data for epoch
            let (fetch_epoch, fetch_data) = fetcher.fetch(epoch).await?;

            info!("Fetched data for DZ epoch {}", fetch_epoch);

            // Try to get Solana epoch and leader schedule
            let mut epoch_finder = EpochFinder::new(
                fetcher.dz_rpc_client.clone(),
                fetcher.solana_read_client.clone(),
            );
            let solana_epoch = match epoch_finder
                .find_epoch_at_timestamp(fetch_data.start_us)
                .await
            {
                Ok(epoch) => Some(epoch),
                Err(e) => {
                    warn!("Failed to determine Solana epoch: {}", e);
                    None
                }
            };

            let leader_schedule = if solana_epoch.is_some() {
                match epoch_finder
                    .fetch_leader_schedule(fetch_epoch, fetch_data.start_us)
                    .await
                {
                    Ok(schedule) => Some(schedule),
                    Err(e) => {
                        warn!("Failed to get leader schedule: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            // Create metadata
            let metadata = SnapshotMetadata {
                created_at: chrono::Utc::now().to_rfc3339(),
                network: orchestrator.settings().network.to_string(),
                exchanges_count: fetch_data.dz_serviceability.exchanges.len(),
                locations_count: fetch_data.dz_serviceability.locations.len(),
                devices_count: fetch_data.dz_serviceability.devices.len(),
                internet_samples_count: fetch_data.dz_internet.internet_latency_samples.len(),
                device_samples_count: fetch_data.dz_telemetry.device_latency_samples.len(),
            };

            // Create complete snapshot
            let snapshot = CompleteSnapshot {
                dz_epoch: fetch_epoch,
                solana_epoch,
                fetch_data,
                leader_schedule,
                metadata,
            };

            // Export based on options
            let export_options = OutputOptions {
                output_format,
                output_dir: output_dir.map(|p| p.to_string_lossy().to_string()),
                output_file: output_file.map(|p| p.to_string_lossy().to_string()),
            };

            let default_filename = format!("snapshot-epoch-{fetch_epoch}");
            export_options.write(&snapshot, &default_filename)?;

            info!("Complete snapshot exported successfully");
            Ok(())
        }

        SnapshotCommands::FetchData {
            epoch,
            output_format,
            output_dir,
            output_file,
        } => {
            info!("Starting fetch_data export");

            // Create fetcher
            let fetcher = Fetcher::from_settings(orchestrator.settings())?;

            // Fetch data for epoch
            let (fetch_epoch, fetch_data) = fetcher.fetch(epoch).await?;

            info!("Fetched data for DZ epoch {}", fetch_epoch);
            info!(
                "Data contains: {} exchanges, {} locations, {} devices",
                fetch_data.dz_serviceability.exchanges.len(),
                fetch_data.dz_serviceability.locations.len(),
                fetch_data.dz_serviceability.devices.len(),
            );

            // Export based on options
            let export_options = OutputOptions {
                output_format,
                output_dir: output_dir.map(|p| p.to_string_lossy().to_string()),
                output_file: output_file.map(|p| p.to_string_lossy().to_string()),
            };

            let default_filename = format!("fetch-data-epoch-{fetch_epoch}");
            export_options.write(&fetch_data, &default_filename)?;

            info!("Fetch data exported successfully");
            Ok(())
        }

        SnapshotCommands::LeaderSchedule {
            epoch,
            output_format,
            output_dir,
            output_file,
        } => {
            info!("Starting leader schedule export");

            // Create fetcher
            let fetcher = Fetcher::from_settings(orchestrator.settings())?;

            // Create epoch finder with explicit RPC clients
            let mut epoch_finder = EpochFinder::new(
                fetcher.dz_rpc_client.clone(),
                fetcher.solana_read_client.clone(),
            );

            info!("Using DZ epoch {}", epoch);

            // Fetch DZ data to get timestamp
            let (_, fetch_data) = fetcher.fetch(Some(epoch)).await?;

            // Get leader schedule
            let leader_schedule = epoch_finder
                .fetch_leader_schedule(epoch, fetch_data.start_us)
                .await?;

            // Export based on options
            let export_options = OutputOptions {
                output_format,
                output_dir: output_dir.map(|p| p.to_string_lossy().to_string()),
                output_file: output_file.map(|p| p.to_string_lossy().to_string()),
            };

            let default_filename = format!("leader-schedule-epoch-{epoch}");
            export_options.write(&leader_schedule, &default_filename)?;

            info!("Leader schedule exported successfully");
            Ok(())
        }

        SnapshotCommands::SnapshotV2 {
            epoch,
            output_format,
            output_dir,
            output_file,
        } => {
            info!("Starting snapshot-v2 export (with calculate-rewards processing)");

            // Create fetcher
            let fetcher = Fetcher::from_settings(orchestrator.settings())?;

            // Use PreparedData to do the same processing as calculate-rewards
            // This includes:
            // - Previous epoch cache lookups
            // - Internet telemetry accumulator
            // - Telemetry processing
            let prep_data = PreparedData::new(&fetcher, epoch, false).await?;
            let fetch_epoch = prep_data.epoch;

            info!("Processed data for DZ epoch {}", fetch_epoch);

            // Get the fetch_data by re-fetching (PreparedData doesn't expose it directly)
            // But we need to replicate the same logic for internet accumulator
            let (_, mut fetch_data) = fetcher.fetch(Some(fetch_epoch)).await?;

            // Apply internet accumulator if enabled (same as PreparedData does)
            if fetcher.settings.inet_lookback.enable_accumulator {
                use crate::ingestor::internet;
                use std::collections::BTreeSet;

                // Calculate expected internet links (same logic as in data_prep.rs)
                let mut unique_routes = BTreeSet::new();
                for sample in &fetch_data.dz_internet.internet_latency_samples {
                    unique_routes.insert((
                        sample.origin_exchange_pk,
                        sample.target_exchange_pk,
                        sample.data_provider_name.clone(),
                    ));
                }
                let expected_inet_samples = unique_routes.len();
                let (inet_epoch, internet_data) = internet::fetch_with_accumulator(
                    &fetcher.dz_rpc_client,
                    &fetcher.settings,
                    fetch_epoch,
                    expected_inet_samples,
                )
                .await?;

                if inet_epoch != fetch_epoch {
                    warn!(
                        "Using historical internet telemetry from epoch {} (target was {})",
                        inet_epoch, fetch_epoch
                    );
                }

                fetch_data.dz_internet = internet_data;
            }

            // Try to get Solana epoch and leader schedule
            let mut epoch_finder = EpochFinder::new(
                fetcher.dz_rpc_client.clone(),
                fetcher.solana_read_client.clone(),
            );
            let solana_epoch = match epoch_finder
                .find_epoch_at_timestamp(fetch_data.start_us)
                .await
            {
                Ok(epoch) => Some(epoch),
                Err(e) => {
                    warn!("Failed to determine Solana epoch: {}", e);
                    None
                }
            };

            let leader_schedule = if solana_epoch.is_some() {
                match epoch_finder
                    .fetch_leader_schedule(fetch_epoch, fetch_data.start_us)
                    .await
                {
                    Ok(schedule) => Some(schedule),
                    Err(e) => {
                        warn!("Failed to get leader schedule: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            // Create metadata
            let metadata = SnapshotMetadata {
                created_at: chrono::Utc::now().to_rfc3339(),
                network: orchestrator.settings().network.to_string(),
                exchanges_count: fetch_data.dz_serviceability.exchanges.len(),
                locations_count: fetch_data.dz_serviceability.locations.len(),
                devices_count: fetch_data.dz_serviceability.devices.len(),
                internet_samples_count: fetch_data.dz_internet.internet_latency_samples.len(),
                device_samples_count: fetch_data.dz_telemetry.device_latency_samples.len(),
            };

            // Create complete snapshot
            let snapshot = CompleteSnapshot {
                dz_epoch: fetch_epoch,
                solana_epoch,
                fetch_data,
                leader_schedule,
                metadata,
            };

            info!("Snapshot includes:");
            info!(
                "  - Internet accumulator: {}",
                fetcher.settings.inet_lookback.enable_accumulator
            );
            info!(
                "  - Previous epoch defaults: {}",
                fetcher
                    .settings
                    .telemetry_defaults
                    .enable_previous_epoch_lookup
            );
            info!("  - Devices: {}", snapshot.metadata.devices_count);
            info!(
                "  - Internet samples: {}",
                snapshot.metadata.internet_samples_count
            );
            info!(
                "  - Device samples: {}",
                snapshot.metadata.device_samples_count
            );

            // Export based on options
            let export_options = OutputOptions {
                output_format,
                output_dir: output_dir.map(|p| p.to_string_lossy().to_string()),
                output_file: output_file.map(|p| p.to_string_lossy().to_string()),
            };

            let default_filename = format!("snapshot-v2-epoch-{fetch_epoch}");
            export_options.write(&snapshot, &default_filename)?;

            info!("Snapshot-v2 exported successfully");
            Ok(())
        }
    }
}
