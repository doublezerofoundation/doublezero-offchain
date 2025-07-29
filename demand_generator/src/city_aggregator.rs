use crate::validators_app::ValidatorsAppResponse;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};

pub type CityAggregates = Vec<CityAggregate>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CityAggregate {
    pub data_center_key: String,
    pub total_stake_sol: u64,
    pub validator_count: u32,
    pub latitude: String,
    pub longitude: String,
}

/// Aggregates validators by city
pub fn aggregate_by_city(validators: &[ValidatorsAppResponse]) -> Result<CityAggregates> {
    let mut city_map: HashMap<String, CityAggregate> = HashMap::new();

    for validator in validators {
        let entry = city_map
            .entry(
                validator
                    .data_center_key
                    .clone()
                    .unwrap_or("default_dck".to_string())
                    .to_string(),
            )
            .or_insert_with(|| CityAggregate {
                data_center_key: validator
                    .data_center_key
                    .clone()
                    .unwrap_or("default_dck".to_string())
                    .to_string(),
                total_stake_sol: 0,
                validator_count: 0,
                latitude: validator
                    .latitude
                    .clone()
                    .unwrap_or("nope".to_string())
                    .to_string(),
                longitude: validator
                    .longitude
                    .clone()
                    .unwrap_or("nope".to_string())
                    .to_string(),
            });

        entry.total_stake_sol += validator.active_stake.unwrap_or(0);
        entry.validator_count += 1;
    }

    let mut aggregates: Vec<CityAggregate> = city_map.into_values().collect();
    // Sort by stake descending
    aggregates.sort_by(|a, b| {
        b.total_stake_sol
            .partial_cmp(&a.total_stake_sol)
            .unwrap_or(Ordering::Equal)
    });

    Ok(aggregates)
}
