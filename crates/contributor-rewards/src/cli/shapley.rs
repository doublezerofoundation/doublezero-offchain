use crate::cli::{
    Exportable, OutputFormat,
    demand_strategies::generate_demands,
    export::ExportOptions,
    export::{to_csv_string, to_json_string},
};
use anyhow::{Result, bail};
use clap::Subcommand;
use contributor_rewards::{
    calculator::{
        orchestrator::Orchestrator,
        shapley_handler::{build_devices, build_private_links, build_public_links},
    },
    ingestor::{epoch::EpochFinder, fetcher::Fetcher},
    processor::{internet::InternetTelemetryProcessor, telemetry::DZDTelemetryProcessor},
};
use network_shapley::types::{Demands, Devices, PrivateLinks, PublicLinks};
use std::{collections::BTreeSet, path::PathBuf};
use tracing::{info, warn};

/// Shapley debug and testing commands
#[derive(Subcommand, Debug)]
pub enum ShapleyCommands {
    #[command(
        about = "Debug Shapley calculations with alternative demand strategies",
        after_help = r#"Examples:
    # Test with uniform demands (no users required)
    shapley-debug --epoch 9 --strategy uniform --skip-users

    # Export ShapleyInputs to JSON
    shapley-debug --epoch 9 --strategy synthetic --output-format json-pretty --output-dir ./debug/

    # Use manual validator mapping
    shapley-debug --epoch 9 --strategy manual --mapping-file validators.json --output-format csv"#
    )]
    ShapleyDebug {
        /// DZ epoch to debug
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// Demand generation strategy
        #[arg(short = 's', long, default_value = "uniform")]
        strategy: DemandStrategy,

        /// Skip serviceability user requirement check
        #[arg(long)]
        skip_users: bool,

        /// Output format for exports
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Directory to export files
        #[arg(short = 'o', long, value_name = "DIR")]
        output_dir: Option<PathBuf>,

        /// Specific output file path
        #[arg(long, value_name = "FILE")]
        output_file: Option<PathBuf>,

        /// Manual mapping file for validator->city mappings
        #[arg(short = 'm', long, value_name = "FILE")]
        mapping_file: Option<PathBuf>,
    },
}

/// Demand generation strategies
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum DemandStrategy {
    /// Original validator-based demands (requires users)
    Validator,
    /// Equal traffic between all city pairs
    Uniform,
    /// Synthetic leaders with configurable distribution
    Synthetic,
    /// Manual validator->city mapping from file
    Manual,
    /// Distance-based traffic weighting
    Distance,
    /// Population-based traffic weighting
    Population,
}

impl std::fmt::Display for DemandStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validator => write!(f, "validator"),
            Self::Uniform => write!(f, "uniform"),
            Self::Synthetic => write!(f, "synthetic"),
            Self::Manual => write!(f, "manual"),
            Self::Distance => write!(f, "distance"),
            Self::Population => write!(f, "population"),
        }
    }
}

/// Container for Shapley inputs that can be exported
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ShapleyInputsExport {
    pub epoch: u64,
    pub devices: Devices,
    pub private_links: PrivateLinks,
    pub public_links: PublicLinks,
    pub demands: Demands,
}

impl Exportable for ShapleyInputsExport {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                bail!(
                    "CSV export not supported for complete ShapleyInputs. Export individual components instead."
                )
            }
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

// Individual component exports
impl Exportable for Devices {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => to_csv_string(self),
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

impl Exportable for PrivateLinks {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => to_csv_string(self),
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

impl Exportable for PublicLinks {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => to_csv_string(self),
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

impl Exportable for Demands {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => to_csv_string(self),
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

/// Handle shapley commands
pub async fn handle(orchestrator: &Orchestrator, cmd: ShapleyCommands) -> Result<()> {
    match cmd {
        ShapleyCommands::ShapleyDebug {
            epoch,
            strategy,
            skip_users,
            output_format,
            output_dir,
            output_file,
            mapping_file,
        } => {
            info!("Starting Shapley debug with strategy: {}", strategy);

            // Create fetcher
            let fetcher = Fetcher::from_settings(orchestrator.settings())?;

            // Fetch data for epoch
            let (fetch_epoch, fetch_data) = match epoch {
                Some(e) => fetcher.with_epoch(e).await?,
                None => fetcher.fetch().await?,
            };

            info!("Fetched data for epoch {}", fetch_epoch);

            // Process telemetry data
            let device_telemetry = DZDTelemetryProcessor::process(&fetch_data)?;
            let internet_telemetry = InternetTelemetryProcessor::process(&fetch_data)?;

            // Build network components
            let devices = build_devices(&fetch_data)?;
            let public_links =
                build_public_links(orchestrator.settings(), &internet_telemetry, &fetch_data)?;
            let private_links = build_private_links(&fetch_data, &device_telemetry);

            // Get cities from devices
            let cities: BTreeSet<String> = devices
                .iter()
                .map(|device| device.device.split('-').next().unwrap_or("").to_string())
                .filter(|city| !city.is_empty())
                .collect();
            let cities: Vec<String> = cities.into_iter().collect();

            info!("Found {} cities in network", cities.len());

            // Get leader schedule if available
            let leader_schedule = if !skip_users || matches!(strategy, DemandStrategy::Validator) {
                // Try to get leader schedule from Solana
                let mut epoch_finder = EpochFinder::new(&fetcher.solana_read_client);
                let solana_epoch = match epoch_finder
                    .find_epoch_at_timestamp(fetch_data.start_us)
                    .await
                {
                    Ok(epoch) => epoch,
                    Err(e) => {
                        warn!("Failed to determine epoch from timestamp: {}", e);
                        fetch_epoch
                    }
                };

                info!("Using Solana epoch {} for leader schedule", solana_epoch);

                match fetcher
                    .solana_read_client
                    .get_leader_schedule(Some(solana_epoch as u64))
                    .await
                {
                    Ok(Some(schedule)) => {
                        info!(
                            "Retrieved leader schedule with {} validators",
                            schedule.len()
                        );
                        // Convert HashMap to BTreeMap for determinism
                        let btree_schedule: std::collections::BTreeMap<String, Vec<usize>> =
                            schedule.into_iter().collect();
                        Some(btree_schedule)
                    }
                    Ok(None) => {
                        warn!("No leader schedule available for epoch {}", solana_epoch);
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

            // Generate demands based on strategy
            let demands = generate_demands(
                &strategy,
                cities,
                leader_schedule,
                mapping_file.as_deref(),
                skip_users,
            )?;

            info!(
                "Generated {} demands using {} strategy",
                demands.len(),
                strategy
            );

            // Create export structure
            let shapley_inputs = ShapleyInputsExport {
                epoch: fetch_epoch,
                devices,
                private_links,
                public_links,
                demands,
            };

            // Export based on options
            let export_options = ExportOptions {
                format: output_format,
                output_dir: output_dir.map(|p| p.to_string_lossy().to_string()),
                output_file: output_file.map(|p| p.to_string_lossy().to_string()),
            };

            let default_filename = format!("shapley-inputs-epoch-{fetch_epoch}-{strategy}");
            export_options.write(&shapley_inputs, &default_filename)?;

            info!("Shapley debug completed successfully");
            Ok(())
        }
    }
}
