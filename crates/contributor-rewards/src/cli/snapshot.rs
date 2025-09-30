use crate::{
    calculator::{data_prep::PreparedData, input::RewardInput, orchestrator::Orchestrator},
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
use anyhow::{Result, anyhow, bail};
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{info, warn};

/// Snapshot commands
#[derive(Subcommand, Debug)]
pub enum SnapshotCommands {
    #[command(
        about = "Create a complete snapshot for deterministic reward calculations",
        long_about = "Creates a complete snapshot with all processing applied (internet accumulator, \
                      previous epoch lookups, etc.). This snapshot can be used with calculate-rewards \
                      --snapshot for deterministic, reproducible reward calculations.",
        after_help = r#"Examples:
    # Export snapshot for epoch 27
    snapshot all --epoch 27 --output-file epoch-27.json

    # Export to directory with automatic naming
    snapshot all --epoch 27 --output-dir ./snapshots/

    # Use with calculate-rewards for deterministic results
    snapshot all --epoch 27 -o snapshot.json
    calculate-rewards --snapshot snapshot.json --dry-run"#
    )]
    All {
        /// DZ epoch to snapshot (defaults to previous epoch)
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
        about = "Extract Shapley inputs from a snapshot",
        long_about = "Extracts RewardInput (Shapley calculation inputs) from a complete snapshot. \
                      Optionally filter by city to get inputs for a specific demand topology.",
        after_help = r#"Examples:
    # Extract all Shapley inputs
    snapshot shapley-inputs --from snapshot.json -o all-inputs.json

    # Extract only Amsterdam-based demands
    snapshot shapley-inputs --from snapshot.json --city AMS -o ams-inputs.json

    # Extract for specific city with CSV format
    snapshot shapley-inputs --from snapshot.json --city NYC -f json -o nyc.json"#
    )]
    ShapleyInputs {
        /// Path to complete snapshot file
        #[arg(long, value_name = "FILE")]
        from: PathBuf,

        /// Optional city code to filter demands (e.g., AMS, NYC, LAX)
        #[arg(long, value_name = "CITY")]
        city: Option<String>,

        /// Output format for export
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Specific output file path
        #[arg(short = 'o', long, value_name = "FILE")]
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
        info!("Snapshot loaded and validated successfully");
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

        info!("Snapshot validation passed");
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
        } => handle_all(orchestrator, epoch, output_format, output_dir, output_file).await,
        SnapshotCommands::ShapleyInputs {
            from,
            city,
            output_format,
            output_file,
        } => handle_shapley_inputs(orchestrator, from, city, output_format, output_file).await,
    }
}

/// Create a complete snapshot with all processing applied
async fn handle_all(
    orchestrator: &Orchestrator,
    epoch: Option<u64>,
    output_format: OutputFormat,
    output_dir: Option<PathBuf>,
    output_file: Option<PathBuf>,
) -> Result<()> {
    info!("Creating complete snapshot");

    // Create fetcher
    let fetcher = Fetcher::from_settings(orchestrator.settings())?;

    // Use PreparedData to apply same processing as calculate-rewards
    // This includes previous epoch cache lookups and internet telemetry accumulator
    let prep_data = PreparedData::new(&fetcher, epoch, false).await?;
    let fetch_epoch = prep_data.epoch;

    info!("Processed data for DZ epoch {}", fetch_epoch);

    // Get the fetch_data by re-fetching and applying same internet accumulator logic
    let (_, mut fetch_data) = fetcher.fetch(Some(fetch_epoch)).await?;

    // Apply internet accumulator if enabled (same as PreparedData does)
    if fetcher.settings.inet_lookback.enable_accumulator {
        use crate::ingestor::internet;
        use std::collections::BTreeSet;

        // Calculate expected internet links
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

    info!("Snapshot processing summary:");
    info!(
        "  - Internet accumulator: {}",
        fetcher.settings.inet_lookback.enable_accumulator
    );
    info!(
        "  - Previous epoch lookups: {}",
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

    let default_filename = format!("snapshot-epoch-{fetch_epoch}");
    export_options.write(&snapshot, &default_filename)?;

    info!("Snapshot exported successfully");
    Ok(())
}

/// Extract Shapley inputs from a snapshot
async fn handle_shapley_inputs(
    orchestrator: &Orchestrator,
    from: PathBuf,
    city: Option<String>,
    output_format: OutputFormat,
    output_file: Option<PathBuf>,
) -> Result<()> {
    info!("Extracting Shapley inputs from snapshot: {:?}", from);

    // Load snapshot
    let snapshot = CompleteSnapshot::load_from_file(&from)?;

    // Generate PreparedData from snapshot with shapley inputs
    let prep_data = PreparedData::from_snapshot(&snapshot, orchestrator.settings(), true)?;

    // Extract shapley_inputs
    let shapley_inputs = prep_data
        .shapley_inputs
        .ok_or_else(|| anyhow!("Shapley inputs not generated"))?;

    // Serialize telemetry for checksums
    let device_telemetry_bytes = borsh::to_vec(&prep_data.device_telemetry)?;
    let internet_telemetry_bytes = borsh::to_vec(&prep_data.internet_telemetry)?;

    // Create RewardInput
    let mut reward_input = RewardInput::new(
        snapshot.dz_epoch,
        orchestrator.settings().shapley.clone(),
        &shapley_inputs,
        &device_telemetry_bytes,
        &internet_telemetry_bytes,
    );

    // Filter by city if specified
    if let Some(ref city_code) = city {
        let city_upper = city_code.to_uppercase();
        info!("Filtering demands for city: {}", city_upper);

        let original_count = reward_input.demands.len();
        reward_input
            .demands
            .retain(|demand| demand.start == city_upper);

        let filtered_count = reward_input.demands.len();
        info!(
            "Filtered demands: {} -> {} (city: {})",
            original_count, filtered_count, city_upper
        );

        if filtered_count == 0 {
            warn!("No demands found for city: {}", city_upper);
        }
    }

    // Export
    let export_options = OutputOptions {
        output_format,
        output_dir: None,
        output_file: output_file.map(|p| p.to_string_lossy().to_string()),
    };

    let default_filename = if let Some(ref city_code) = city {
        format!(
            "shapley-inputs-{}-epoch-{}",
            city_code.to_lowercase(),
            snapshot.dz_epoch
        )
    } else {
        format!("shapley-inputs-epoch-{}", snapshot.dz_epoch)
    };

    export_options.write(&reward_input, &default_filename)?;

    info!("Shapley inputs exported successfully");
    Ok(())
}
