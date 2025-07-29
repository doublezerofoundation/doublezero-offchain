use crate::city_aggregator::CityAggregate;
use anyhow::Result;
use csv::Writer;
use network_shapley::types::Demand;
use std::path::PathBuf;

/// Configuration for demand generation
#[derive(Debug, Clone)]
pub struct DemandConfig {
    pub traffic_multiplier: f64,
    pub high_priority_stake_threshold: f64,
    // Percentage of total stake to qualify for Type 2
    pub type_2_threshold: f64,
}

// TODO: Discuss if we default to this or not
impl Default for DemandConfig {
    fn default() -> Self {
        Self {
            traffic_multiplier: 10.0,
            // 1M SOL
            high_priority_stake_threshold: 1_000_000.0,
            // 10% of total stake
            type_2_threshold: 0.1,
        }
    }
}

/// Generates demand matrix from city aggregates
pub fn generate_demand_matrix(
    city_aggregates: &[CityAggregate],
    config: &DemandConfig,
) -> Result<Vec<Demand>> {
    let mut demands = Vec::new();

    // Calculate total network stake
    let total_stake = city_aggregates.iter().map(|c| c.total_stake_sol).sum();

    // Generate bidirectional flows between all city pairs
    for source in city_aggregates {
        for destination in city_aggregates {
            // Skip self-loops
            if source.data_center_key == destination.data_center_key {
                continue;
            }

            // Calculate traffic based on stake weights
            let traffic = calculate_traffic(
                source.total_stake_sol,
                destination.total_stake_sol,
                total_stake,
                config.traffic_multiplier,
            );

            // Calculate priority based on combined stake
            let priority = calculate_priority(
                source.total_stake_sol,
                destination.total_stake_sol,
                total_stake,
            );

            // Determine type based on source stake concentration
            let kind = if (source.total_stake_sol / total_stake) as f64 >= config.type_2_threshold {
                2 // High-stake cities use Type 2
            } else {
                1 // Standard type
            };

            let demand = Demand::new(
                source.data_center_key.clone(),
                destination.data_center_key.clone(),
                destination.validator_count,
                traffic,
                priority,
                kind,
                false, // multicast: default to false
            );

            demands.push(demand);
        }
    }

    Ok(demands)
}

/// Calculates traffic volume between two cities
fn calculate_traffic(source_stake: u64, dest_stake: u64, total_stake: u64, multiplier: f64) -> f64 {
    // Use geometric mean of stakes, normalized by total stake
    let geometric_mean = (source_stake * dest_stake).isqrt();
    let normalized = geometric_mean / total_stake;

    // Apply multiplier and round to reasonable precision
    (normalized as f64 * multiplier * 100.0).round() / 100.0
}

/// Calculates priority based on stake concentration
fn calculate_priority(source_stake: u64, dest_stake: u64, total_stake: u64) -> f64 {
    // Average of source and destination stake concentrations
    let avg_concentration = (source_stake + dest_stake) / (2 * total_stake);

    // Ensure priority is between 0 and 1, round to 2 decimal places
    (avg_concentration.min(1) * 100) as f64 / 100.0
}

/// Writes demand matrix to CSV file
pub fn write_demand_csv(path: &PathBuf, demands: &[Demand]) -> Result<()> {
    let mut writer = Writer::from_path(path)?;

    for demand in demands {
        writer.serialize(demand)?;
    }

    writer.flush()?;
    Ok(())
}
