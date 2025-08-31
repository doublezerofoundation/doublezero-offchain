use crate::cli::{
    common::{OutputFormat, OutputOptions, collection_to_csv, to_json_string},
    traits::Exportable,
};
use anyhow::{Result, bail};
use clap::Subcommand;
use contributor_rewards::{
    calculator::orchestrator::Orchestrator,
    ingestor::{epoch::EpochFinder, fetcher::Fetcher, types::FetchData},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
    # Export leader schedule as CSV
    snapshot leader-schedule --epoch 9 --output-format csv --output-file leaders.csv

    # Export as JSON for analysis
    snapshot leader-schedule --epoch 9 --output-format json-pretty"#
    )]
    LeaderSchedule {
        /// Solana epoch to fetch (will auto-determine from DZ epoch if not provided)
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// DZ epoch to use for timestamp mapping
        #[arg(long, value_name = "EPOCH")]
        dz_epoch: Option<u64>,

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
    pub leader_schedule: Option<BTreeMap<String, Vec<usize>>>,
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

/// Leader schedule wrapper for export
#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderScheduleExport {
    pub epoch: u64,
    pub leaders: BTreeMap<String, LeaderInfo>,
    pub total_slots: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderInfo {
    pub validator: String,
    pub slots: Vec<usize>,
    pub slot_count: usize,
    pub percentage: f64,
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

impl Exportable for LeaderScheduleExport {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                // Create CSV with validator,slots,percentage
                let mut records = Vec::new();
                for (validator, info) in &self.leaders {
                    records.push(LeaderCsvRecord {
                        validator: validator.clone(),
                        slot_count: info.slot_count,
                        percentage: info.percentage,
                    });
                }
                collection_to_csv(&records)
            }
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

#[derive(Debug, Serialize)]
struct LeaderCsvRecord {
    validator: String,
    slot_count: usize,
    percentage: f64,
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
            let (fetch_epoch, fetch_data) = match epoch {
                Some(e) => fetcher.with_epoch(e).await?,
                None => fetcher.fetch().await?,
            };

            info!("Fetched data for DZ epoch {}", fetch_epoch);

            // Try to get Solana epoch and leader schedule
            let mut epoch_finder = EpochFinder::new(&fetcher.solana_read_client);
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

            let leader_schedule = if let Some(sol_epoch) = solana_epoch {
                match fetcher
                    .solana_read_client
                    .get_leader_schedule(Some(sol_epoch))
                    .await
                {
                    Ok(Some(schedule)) => {
                        info!(
                            "Retrieved leader schedule with {} validators",
                            schedule.len()
                        );
                        let btree_schedule: BTreeMap<String, Vec<usize>> =
                            schedule.into_iter().collect();
                        Some(btree_schedule)
                    }
                    Ok(None) => {
                        warn!(
                            "No leader schedule available for Solana epoch {}",
                            sol_epoch
                        );
                        None
                    }
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
            let (fetch_epoch, fetch_data) = match epoch {
                Some(e) => fetcher.with_epoch(e).await?,
                None => fetcher.fetch().await?,
            };

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
            dz_epoch,
            output_format,
            output_dir,
            output_file,
        } => {
            info!("Starting leader schedule export");

            // Create fetcher
            let fetcher = Fetcher::from_settings(orchestrator.settings())?;

            // Determine which epoch to use
            let solana_epoch = if let Some(e) = epoch {
                e
            } else if let Some(dz_e) = dz_epoch {
                // Fetch DZ data to get timestamp
                let (_, fetch_data) = fetcher.with_epoch(dz_e).await?;
                let mut epoch_finder = EpochFinder::new(&fetcher.solana_read_client);
                epoch_finder
                    .find_epoch_at_timestamp(fetch_data.start_us)
                    .await? as u64
            } else {
                // Get current epoch
                let (_, fetch_data) = fetcher.fetch().await?;
                let mut epoch_finder = EpochFinder::new(&fetcher.solana_read_client);
                epoch_finder
                    .find_epoch_at_timestamp(fetch_data.start_us)
                    .await? as u64
            };

            info!("Fetching leader schedule for Solana epoch {}", solana_epoch);

            // Get leader schedule
            let schedule = fetcher
                .solana_read_client
                .get_leader_schedule(Some(solana_epoch))
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("No leader schedule available for epoch {}", solana_epoch)
                })?;

            info!("Retrieved schedule with {} validators", schedule.len());

            // Convert to BTreeMap for determinism
            let btree_schedule: BTreeMap<String, Vec<usize>> = schedule.into_iter().collect();

            // Calculate total slots and percentages
            let total_slots: usize = btree_schedule.values().map(|v| v.len()).sum();

            let mut leaders = BTreeMap::new();
            for (validator, slots) in btree_schedule {
                let slot_count = slots.len();
                let percentage = (slot_count as f64 / total_slots as f64) * 100.0;
                leaders.insert(
                    validator.clone(),
                    LeaderInfo {
                        validator,
                        slots,
                        slot_count,
                        percentage,
                    },
                );
            }

            let leader_export = LeaderScheduleExport {
                epoch: solana_epoch,
                leaders,
                total_slots,
            };

            // Export based on options
            let export_options = OutputOptions {
                output_format,
                output_dir: output_dir.map(|p| p.to_string_lossy().to_string()),
                output_file: output_file.map(|p| p.to_string_lossy().to_string()),
            };

            let default_filename = format!("leader-schedule-epoch-{solana_epoch}");
            export_options.write(&leader_export, &default_filename)?;

            info!("Leader schedule exported successfully");
            Ok(())
        }
    }
}
