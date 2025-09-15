use crate::ingestor::{epoch::EpochFinder, fetcher::Fetcher, types::FetchData};
use anyhow::{Result, anyhow, bail};
use doublezero_serviceability::state::{accesspass::AccessPassType, user::User as DZUser};
use network_shapley::types::{Demand, Demands};
use rayon::prelude::*;
use solana_sdk::{pubkey::Pubkey, system_program::ID as SystemProgramID};
use std::collections::BTreeMap;
use tracing::info;

// key: location code, val: city stat
pub type CityStats = BTreeMap<String, CityStat>;

/// Statistics for validators in a city
#[derive(Debug, Clone)]
pub struct CityStat {
    /// Number of validators in this city
    pub validator_count: usize,
    /// Sum of all validator stake proxies (leader schedule lengths) in this city
    pub total_stake_proxy: usize,
}

/// Result of demand building containing both demands and city statistics
pub struct DemandBuildOutput {
    pub demands: Demands,
    pub city_stats: CityStats,
}

/// Builds demand tables for network traffic simulation based on validator distribution
///
/// This function:
/// 1. Filters validators from users who have non-system validator pubkeys
/// 2. Maps validators to their geographic locations
/// 3. Aggregates validators by city with their stake weights
/// 4. Generates demand entries for all city-to-city traffic pairs
pub async fn build(fetcher: &Fetcher, fetch_data: &FetchData) -> Result<DemandBuildOutput> {
    // Get first telemetry sample to extract epoch and timestamp
    let first_sample = fetch_data
        .dz_telemetry
        .device_latency_samples
        .first()
        .ok_or_else(|| anyhow!("No telemetry data found to determine DZ epoch"))?;

    let dz_epoch = first_sample.epoch;
    info!("Building demands for DZ epoch {}", dz_epoch);

    // Get the timestamp from first_sample
    let timestamp_us = first_sample.start_timestamp_us;
    assert_ne!(0, timestamp_us, "First sample timestamp is 0!");

    // Create an EpochFinder with explicit RPC clients
    let mut epoch_finder = EpochFinder::new(
        fetcher.dz_rpc_client.clone(),
        fetcher.solana_read_client.clone(),
    );

    // Fetch leader schedule for this DZ epoch
    let leader_schedule_map = epoch_finder
        .fetch_leader_schedule(dz_epoch, timestamp_us)
        .await?;

    build_with_schedule(fetch_data, leader_schedule_map)
}

/// Builds demands using pre-fetched leader schedule data
/// NOTE: This allows testing without RPC calls
pub fn build_with_schedule(
    fetch_data: &FetchData,
    leader_schedule: BTreeMap<String, usize>,
) -> Result<DemandBuildOutput> {
    // Build AccessPass user to Validator mapping
    let accessor_to_validator: BTreeMap<Pubkey, Pubkey> = fetch_data
        .dz_serviceability
        .access_passes
        .par_iter()
        .filter_map(|(_access_pass_pk, access_pass)| {
            match access_pass.accesspass_type {
                AccessPassType::Prepaid => {
                    // TODO: Ignore or include?
                    None
                }
                AccessPassType::SolanaValidator(validator_pk) => {
                    Some((access_pass.user_payer, validator_pk))
                }
            }
        })
        .collect();

    // Build validator to user mapping
    let validator_to_user: BTreeMap<String, &DZUser> = fetch_data
        .dz_serviceability
        .users
        .par_iter()
        .filter_map(
            |(_user_pk, user)| match accessor_to_validator.get(&user.owner) {
                None => None,
                Some(validator_pk) => {
                    (*validator_pk != SystemProgramID).then_some((validator_pk.to_string(), user))
                }
            },
        )
        .collect();

    if validator_to_user.is_empty() {
        bail!("Did not find any validators to build demands!")
    }

    // Process leaders and build city statistics
    let city_stats = build_city_stats(fetch_data, &validator_to_user, leader_schedule)?;
    if city_stats.is_empty() {
        bail!("Could not build any city_stats!")
    }

    // Generate demands
    let demands: Demands = generate(&city_stats);
    if demands.is_empty() {
        bail!("Could not build any demands!")
    }

    Ok(DemandBuildOutput {
        demands,
        city_stats,
    })
}

/// Build city statistics from fetch data and leader schedule
pub fn build_city_stats(
    fetch_data: &FetchData,
    validator_to_user: &BTreeMap<String, &DZUser>,
    leader_schedule: BTreeMap<String, usize>,
) -> Result<CityStats> {
    let mut city_stats = CityStats::new();

    // Process each leader
    for (validator_pubkey, stake_proxy) in leader_schedule {
        if let Some(user) = validator_to_user.get(&validator_pubkey)
            && let Some(device) = fetch_data.dz_serviceability.devices.get(&user.device_pk)
            && let Some(location) = fetch_data
                .dz_serviceability
                .locations
                .get(&device.location_pk)
        {
            let stats = city_stats
                .entry(location.code.to_string())
                .or_insert(CityStat {
                    validator_count: 0,
                    total_stake_proxy: 0,
                });
            stats.validator_count += 1;
            stats.total_stake_proxy += stake_proxy;
        }
    }

    Ok(city_stats)
}

/// Generates demand entries for cities
pub fn generate(city_stats: &CityStats) -> Demands {
    // TODO: move this to some constants.rs and/or make configurable
    const TRAFFIC: f64 = 0.05;
    const DEMAND_TYPE: u32 = 1;
    const MULTICAST: bool = false;
    const SLOTS_IN_EPOCH: f64 = 432000.0;

    // Filter cities with validators once
    let cities_with_validators: Vec<(&String, &CityStat)> = city_stats
        .iter()
        .filter(|(_, stats)| stats.validator_count > 0)
        .collect();

    // Generate demands for each source city
    cities_with_validators
        .par_iter()
        .flat_map(|(start_city, _start_stats)| {
            // Create demands from this city to all others
            cities_with_validators
                .iter()
                .filter_map(|(end_city, end_stats)| {
                    // Avoid self loops
                    if start_city == end_city {
                        return None;
                    }

                    // Calculate priority using formula: (1/slots_in_epoch) * (total_stake_proxy/validator_count)
                    let slots_per_validator =
                        end_stats.total_stake_proxy as f64 / end_stats.validator_count as f64;
                    let priority = (1.0 / SLOTS_IN_EPOCH) * slots_per_validator;

                    Some(Demand {
                        start: start_city.to_string(),
                        end: end_city.to_string(),
                        receivers: end_stats.validator_count as u32,
                        traffic: TRAFFIC,
                        priority,
                        kind: DEMAND_TYPE,
                        multicast: MULTICAST,
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
