use crate::aggregator::DataCenterAggregate;
use anyhow::Result;
use network_shapley::types::Demand;

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
    dc_aggregates: &[DataCenterAggregate],
    config: &DemandConfig,
) -> Result<Vec<Demand>> {
    let mut demands = Vec::new();

    // Calculate total network stake
    let total_stake = dc_aggregates.iter().map(|c| c.total_stake).sum();

    // Generate bidirectional flows between all city pairs
    for source in dc_aggregates {
        for destination in dc_aggregates {
            // Skip self-loops
            if source.data_center_key == destination.data_center_key {
                continue;
            }

            // Calculate traffic based on stake weights
            let traffic = calculate_traffic(
                source.total_stake,
                destination.total_stake,
                total_stake,
                config.traffic_multiplier,
            );

            // Calculate priority based on combined stake
            let priority =
                calculate_priority(source.total_stake, destination.total_stake, total_stake);

            // Determine type based on source stake concentration
            let kind = if (source.total_stake / total_stake) as f64 >= config.type_2_threshold {
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

// TODO: Do this better? This is very naive.
/// Calculates traffic volume between two data centers
fn calculate_traffic(source_stake: u64, dest_stake: u64, total_stake: u64, multiplier: f64) -> f64 {
    // Use geometric mean of stakes, normalized by total stake
    let geometric_mean = ((source_stake as f64) * (dest_stake as f64)).sqrt();
    let normalized = geometric_mean / (total_stake as f64);

    // Apply multiplier and round to reasonable precision
    (normalized * multiplier * 100.0).round() / 100.0
}

// TODO: Do this better? This is also very naive.
/// Calculates priority based on stake concentration
fn calculate_priority(source_stake: u64, dest_stake: u64, total_stake: u64) -> f64 {
    // Average of source and destination stake concentrations
    let avg_concentration = (source_stake + dest_stake) as f64 / (2.0 * total_stake as f64);

    // Ensure priority is between 0 and 1, round to 2 decimal places
    (avg_concentration.min(1.0) * 100.0).round() / 100.0
}
