use crate::{
    calculator::{
        data_prep::PreparedData,
        input::RewardInput,
        orchestrator::Orchestrator,
        proof::{ContributorRewardsMerkleTree, ShapleyOutputStorage},
        shapley_aggregator::aggregate_shapley_outputs,
    },
    cli::{
        common::{OutputFormat, OutputOptions, to_json_string},
        traits::Exportable,
    },
    ingestor::fetcher::Fetcher,
};
use anyhow::{Context, Result, bail};
use clap::Subcommand;
use network_shapley::{shapley::ShapleyInput, types::Demand};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use tabled::{builder::Builder as TableBuilder, settings::Style};
use tracing::info;

/// Debug commands for step-by-step reward calculation debugging
#[derive(Subcommand, Debug)]
pub enum DebugCommands {
    #[command(
        about = "Step 1: Ingest raw on-chain data for an epoch",
        after_help = r#"Examples:
    # Ingest data for epoch 27
    debug ingest-data --epoch 27 --output-file fetch-data-27.json"#
    )]
    IngestData {
        /// DZ epoch to ingest
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// Output format
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Output file path
        #[arg(short = 'o', long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },

    #[command(
        about = "Step 2: Prepare Shapley inputs from raw data",
        after_help = r#"Examples:
    # Prepare inputs from previously ingested data
    debug shapley-input --input-file fetch-data-27.json --output-file shapley-input-27.json

    # Or fetch and prepare in one step
    debug shapley-input --epoch 27 --output-file shapley-input-27.json"#
    )]
    ShapleyInput {
        /// Input file with FetchData (from ingest-data step)
        #[arg(short = 'i', long, value_name = "FILE")]
        input_file: Option<PathBuf>,

        /// DZ epoch (if not loading from input file)
        #[arg(short, long, value_name = "EPOCH")]
        epoch: Option<u64>,

        /// Output format
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Output file path
        #[arg(short = 'o', long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },

    #[command(
        about = "Step 3: Calculate Shapley proportions from inputs",
        after_help = r#"Examples:
    # Calculate from prepared inputs
    debug calculate-proportions --input-file shapley-input-27.json --output-file proportions-27.json

    # Output as CSV for R comparison
    debug calculate-proportions --input-file shapley-input-27.json -f csv -o proportions-27.csv"#
    )]
    CalculateProportions {
        /// Input file with RewardInput (from shapley-input step)
        #[arg(short = 'i', long, value_name = "FILE")]
        input_file: PathBuf,

        /// Output format
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Output file path
        #[arg(short = 'o', long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },

    #[command(
        about = "Step 4: Build merkle tree from proportions",
        after_help = r#"Examples:
    # Build merkle tree from proportions
    debug post-merkle --input-file proportions-27.json --output-file merkle-27.json"#
    )]
    PostMerkle {
        /// Input file with ShapleyProportions (from calculate-proportions step)
        #[arg(short = 'i', long, value_name = "FILE")]
        input_file: PathBuf,

        /// Output format
        #[arg(short = 'f', long, default_value = "json-pretty")]
        output_format: OutputFormat,

        /// Output file path
        #[arg(short = 'o', long, value_name = "FILE")]
        output_file: Option<PathBuf>,
    },
}

/// Output from calculate-proportions step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapleyProportions {
    pub epoch: u64,
    pub proportions: BTreeMap<String, ProportionEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProportionEntry {
    pub value: f64,
    pub proportion: f64,
}

/// Output from post-merkle step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleDebugOutput {
    pub epoch: u64,
    pub merkle_root: String,
    pub reward_count: usize,
    pub total_unit_shares: u32,
}

// Implement Exportable traits
impl Exportable for ShapleyProportions {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                let mut csv = String::from("operator,value,proportion\n");
                for (operator, entry) in &self.proportions {
                    csv.push_str(&format!(
                        "{},{},{}\n",
                        operator, entry.value, entry.proportion
                    ));
                }
                Ok(csv)
            }
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

impl Exportable for MerkleDebugOutput {
    fn export(&self, format: OutputFormat) -> Result<String> {
        match format {
            OutputFormat::Csv => {
                bail!("CSV export not supported for merkle output. Use JSON format.")
            }
            OutputFormat::Json => to_json_string(self, false),
            OutputFormat::JsonPretty => to_json_string(self, true),
        }
    }
}

/// Handle debug commands
pub async fn handle(orchestrator: &Orchestrator, cmd: DebugCommands) -> Result<()> {
    match cmd {
        DebugCommands::IngestData {
            epoch,
            output_format,
            output_file,
        } => handle_ingest_data(orchestrator, epoch, output_format, output_file).await,

        DebugCommands::ShapleyInput {
            input_file,
            epoch,
            output_format,
            output_file,
        } => {
            handle_shapley_input(orchestrator, input_file, epoch, output_format, output_file).await
        }

        DebugCommands::CalculateProportions {
            input_file,
            output_format,
            output_file,
        } => {
            handle_calculate_proportions(orchestrator, input_file, output_format, output_file).await
        }

        DebugCommands::PostMerkle {
            input_file,
            output_format,
            output_file,
        } => handle_post_merkle(input_file, output_format, output_file).await,
    }
}

async fn handle_ingest_data(
    orchestrator: &Orchestrator,
    epoch: Option<u64>,
    output_format: OutputFormat,
    output_file: Option<PathBuf>,
) -> Result<()> {
    info!("Step 1: Ingesting on-chain data");

    let fetcher = Fetcher::from_settings(orchestrator.settings())?;
    let (fetch_epoch, fetch_data) = fetcher.fetch(epoch).await?;

    info!("Fetched data for epoch {}", fetch_epoch);
    info!(
        "Data contains: {} devices, {} private links",
        fetch_data.dz_serviceability.devices.len(),
        fetch_data.dz_telemetry.device_latency_samples.len()
    );

    let export_options = OutputOptions {
        output_format,
        output_dir: None,
        output_file: output_file.map(|p| p.to_string_lossy().to_string()),
    };

    let default_filename = format!("ingest-data-epoch-{fetch_epoch}");
    export_options.write(&fetch_data, &default_filename)?;

    info!("Ingest complete");
    Ok(())
}

async fn handle_shapley_input(
    _orchestrator: &Orchestrator,
    input_file: Option<PathBuf>,
    epoch: Option<u64>,
    output_format: OutputFormat,
    output_file: Option<PathBuf>,
) -> Result<()> {
    info!("Step 2: Preparing Shapley inputs");

    let fetcher = Fetcher::from_settings(_orchestrator.settings())?;

    // Either load from file or fetch fresh
    let (fetch_epoch, prep_data) = if let Some(input_path) = input_file {
        info!("Loading FetchData from {:?}", input_path);
        bail!("Loading from file not yet implemented - use --epoch instead");
    } else {
        let prep = PreparedData::new(&fetcher, epoch, true).await?;
        (prep.epoch, prep)
    };

    let shapley_inputs = prep_data
        .shapley_inputs
        .context("Shapley inputs required but not prepared")?;

    let device_telemetry_bytes = borsh::to_vec(&prep_data.device_telemetry)?;
    let internet_telemetry_bytes = borsh::to_vec(&prep_data.internet_telemetry)?;

    let reward_input = RewardInput::new(
        fetch_epoch,
        _orchestrator.settings().shapley.clone(),
        &shapley_inputs,
        &device_telemetry_bytes,
        &internet_telemetry_bytes,
    );

    info!("Shapley inputs prepared:");
    info!("  Devices: {}", reward_input.devices.len());
    info!("  Private links: {}", reward_input.private_links.len());
    info!("  Public links: {}", reward_input.public_links.len());
    info!("  Demands: {}", reward_input.demands.len());
    info!("  Cities: {}", reward_input.city_summaries.len());

    let export_options = OutputOptions {
        output_format,
        output_dir: None,
        output_file: output_file.map(|p| p.to_string_lossy().to_string()),
    };

    let default_filename = format!("shapley-input-epoch-{fetch_epoch}");
    export_options.write(&reward_input, &default_filename)?;

    info!("Shapley input preparation complete");
    Ok(())
}

async fn handle_calculate_proportions(
    _orchestrator: &Orchestrator,
    input_file: PathBuf,
    output_format: OutputFormat,
    output_file: Option<PathBuf>,
) -> Result<()> {
    info!("Step 3: Calculating Shapley proportions");
    info!("Loading RewardInput from {:?}", input_file);

    // Read and deserialize RewardInput
    let contents = std::fs::read(&input_file)
        .with_context(|| format!("Failed to read input file: {:?}", input_file))?;

    let reward_input: RewardInput = borsh::from_slice(&contents).or_else(|_| {
        // Try JSON fallback
        serde_json::from_slice(&contents).context("Failed to parse as Borsh or JSON")
    })?;

    let epoch = reward_input.epoch;
    info!("Loaded input for epoch {}", epoch);

    // Group demands by start city
    let mut demands_by_city: BTreeMap<String, Vec<Demand>> = BTreeMap::new();
    for demand in reward_input.demands.clone() {
        demands_by_city
            .entry(demand.start.clone())
            .or_default()
            .push(demand);
    }
    let demand_groups: Vec<(String, Vec<Demand>)> = demands_by_city.into_iter().collect();

    // Calculate per-city Shapley outputs in parallel
    info!(
        "Computing Shapley values for {} cities",
        demand_groups.len()
    );
    let per_city_shapley_outputs: BTreeMap<String, Vec<(String, f64)>> = demand_groups
        .par_iter()
        .map(|(city, demands)| {
            let city_name = city.clone();
            info!("Computing Shapley for city: {}", city_name);

            // Build shapley input
            let input = ShapleyInput {
                private_links: reward_input.private_links.clone(),
                devices: reward_input.devices.clone(),
                demands: demands.clone(),
                public_links: reward_input.public_links.clone(),
                operator_uptime: reward_input.shapley_settings.operator_uptime,
                contiguity_bonus: reward_input.shapley_settings.contiguity_bonus,
                demand_multiplier: reward_input.shapley_settings.demand_multiplier,
            };

            // Compute Shapley output
            let output = input
                .compute()
                .with_context(|| format!("Failed to compute Shapley for {city_name}"))?;

            // Print per-city table
            let table = TableBuilder::from(output.clone())
                .build()
                .with(Style::psql().remove_horizontals())
                .to_string();
            info!("Shapley Output for {city_name}:\n{}", table);

            // Store raw values for aggregation
            let city_values: Vec<(String, f64)> = output
                .into_iter()
                .map(|(operator, shapley_value)| (operator, shapley_value.value))
                .collect();

            Ok((city_name, city_values))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect();

    // Build city weights from reward input
    let city_weights: BTreeMap<String, f64> = reward_input
        .city_summaries
        .iter()
        .map(|(city, summary)| (city.clone(), summary.weight))
        .collect();

    // Aggregate Shapley outputs
    info!(
        "Aggregating {} city outputs",
        per_city_shapley_outputs.len()
    );
    let shapley_output = aggregate_shapley_outputs(&per_city_shapley_outputs, &city_weights)?;

    // Print final table
    let mut table_builder = TableBuilder::default();
    table_builder.push_record(["Operator", "Value", "Proportion"]);

    for (operator, val) in shapley_output.iter() {
        table_builder.push_record([
            operator,
            &val.value.to_string(),
            &val.proportion.to_string(),
        ]);
    }

    let table = table_builder
        .build()
        .with(Style::psql().remove_horizontals())
        .to_string();
    info!("Final Shapley Output:\n{}", table);

    // Convert to output format
    let proportions: BTreeMap<String, ProportionEntry> = shapley_output
        .into_iter()
        .map(|(operator, val)| {
            (
                operator,
                ProportionEntry {
                    value: val.value,
                    proportion: val.proportion,
                },
            )
        })
        .collect();

    let output = ShapleyProportions { epoch, proportions };

    let export_options = OutputOptions {
        output_format,
        output_dir: None,
        output_file: output_file.map(|p| p.to_string_lossy().to_string()),
    };

    let default_filename = format!("proportions-epoch-{epoch}");
    export_options.write(&output, &default_filename)?;

    info!("Proportion calculation complete");
    Ok(())
}

async fn handle_post_merkle(
    input_file: PathBuf,
    output_format: OutputFormat,
    output_file: Option<PathBuf>,
) -> Result<()> {
    info!("Step 4: Building merkle tree");
    info!("Loading ShapleyProportions from {:?}", input_file);

    // Read and deserialize ShapleyProportions
    let contents = std::fs::read(&input_file)
        .with_context(|| format!("Failed to read input file: {:?}", input_file))?;

    let proportions: ShapleyProportions =
        serde_json::from_slice(&contents).context("Failed to parse ShapleyProportions JSON")?;

    let epoch = proportions.epoch;
    info!("Loaded proportions for epoch {}", epoch);

    // Convert back to ShapleyOutput format
    let mut shapley_output = BTreeMap::new();
    for (operator, entry) in proportions.proportions {
        shapley_output.insert(
            operator,
            network_shapley::shapley::ShapleyValue {
                value: entry.value,
                proportion: entry.proportion,
            },
        );
    }

    // Build merkle tree
    info!("Building merkle tree");
    let merkle_tree = ContributorRewardsMerkleTree::new(epoch, &shapley_output)?;
    let merkle_root = merkle_tree.compute_root()?;

    info!("Merkle root: {}", merkle_root);
    info!("Total rewards: {}", merkle_tree.rewards().len());

    let shapley_storage = ShapleyOutputStorage {
        epoch,
        rewards: merkle_tree.rewards().to_vec(),
        total_unit_shares: merkle_tree.rewards().iter().map(|r| r.unit_share).sum(),
    };

    let output = MerkleDebugOutput {
        epoch,
        merkle_root: merkle_root.to_string(),
        reward_count: shapley_storage.rewards.len(),
        total_unit_shares: shapley_storage.total_unit_shares,
    };

    let export_options = OutputOptions {
        output_format,
        output_dir: None,
        output_file: output_file.map(|p| p.to_string_lossy().to_string()),
    };

    let default_filename = format!("merkle-epoch-{epoch}");
    export_options.write(&output, &default_filename)?;

    info!("Merkle tree complete");
    Ok(())
}
