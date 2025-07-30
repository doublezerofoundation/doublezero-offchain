use crate::validators_app::ValidatorsAppResponse;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type CityAggregates = Vec<DataCenterAggregate>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterAggregate {
    pub data_center_key: String,
    pub total_stake: u64,
    pub validator_count: u32,
    pub latitude: String,
    pub longitude: String,
}

/// Aggregates validators by data center
pub fn aggregate_by_dc(validators: &[ValidatorsAppResponse]) -> Result<CityAggregates> {
    let mut dc_map: HashMap<String, DataCenterAggregate> = HashMap::new();

    for validator in validators {
        // Skip validators without required data
        let Some(dc_key) = &validator.data_center_key else {
            continue;
        };
        let Some(stake) = validator.active_stake else {
            continue;
        };
        let Some(lat) = &validator.latitude else {
            continue;
        };
        let Some(lon) = &validator.longitude else {
            continue;
        };

        let entry = dc_map
            .entry(dc_key.clone())
            .or_insert_with(|| DataCenterAggregate {
                data_center_key: dc_key.clone(),
                total_stake: 0,
                validator_count: 0,
                latitude: lat.clone(),
                longitude: lon.clone(),
            });

        entry.total_stake += stake;
        entry.validator_count += 1;
    }

    let mut aggregates: Vec<DataCenterAggregate> = dc_map.into_values().collect();

    // Sort by stake descending
    aggregates.sort_by(|a, b| b.total_stake.cmp(&a.total_stake));

    Ok(aggregates)
}
