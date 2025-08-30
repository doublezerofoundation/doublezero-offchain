use super::shapley::DemandStrategy;
use anyhow::{Result, bail};
use network_shapley::types::{Demand, Demands};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use tracing::{info, warn};

/// Generate demands based on the selected strategy
pub fn generate_demands(
    strategy: &DemandStrategy,
    cities: Vec<String>,
    leader_schedule: Option<BTreeMap<String, Vec<usize>>>,
    mapping_file: Option<&Path>,
    skip_users: bool,
) -> Result<Demands> {
    match strategy {
        DemandStrategy::Validator => {
            if skip_users {
                warn!("Validator strategy requires users, falling back to synthetic");
                generate_synthetic_demands(&cities, leader_schedule)
            } else {
                bail!("Validator strategy requires serviceability users")
            }
        }
        DemandStrategy::Uniform => generate_uniform_demands(&cities),
        DemandStrategy::Synthetic => generate_synthetic_demands(&cities, leader_schedule),
        DemandStrategy::Manual => {
            if let Some(path) = mapping_file {
                generate_manual_demands(path, &cities, leader_schedule)
            } else {
                bail!("Manual strategy requires --mapping-file")
            }
        }
        DemandStrategy::Distance => generate_distance_based_demands(&cities),
        DemandStrategy::Population => generate_population_based_demands(&cities),
    }
}

/// Generate uniform demands - equal traffic between all city pairs
pub fn generate_uniform_demands(cities: &[String]) -> Result<Demands> {
    let mut demands = Vec::new();
    let mut demand_type = 1u32;

    for source in cities {
        for destination in cities {
            if source != destination {
                demands.push(Demand::new(
                    source.clone(),
                    destination.clone(),
                    1,   // receivers
                    1.0, // traffic
                    1.0, // priority
                    demand_type,
                    false, // multicast
                ));
            }
        }
        demand_type += 1;
    }

    info!(
        "Generated {} uniform demands across {} cities",
        demands.len(),
        cities.len()
    );
    Ok(demands)
}

/// Generate synthetic demands with fake leaders per city
pub fn generate_synthetic_demands(
    cities: &[String],
    leader_schedule: Option<BTreeMap<String, Vec<usize>>>,
) -> Result<Demands> {
    let mut demands = Vec::new();
    let mut demand_type = 1u32;

    // If we have a leader schedule, use those weights
    // Otherwise, create synthetic weights
    let city_weights = if let Some(schedule) = leader_schedule {
        // Aggregate stake by city (simplified - assumes validator names contain city codes)
        // Vec<usize> contains slot indices, length represents stake weight
        let mut weights = BTreeMap::new();
        for (validator, slots) in schedule {
            // Try to extract city from validator name (heuristic)
            let city = extract_city_from_validator(&validator, cities);
            if let Some(city) = city {
                *weights.entry(city).or_insert(0) += slots.len();
            }
        }
        weights
    } else {
        // Create synthetic weights - larger cities get more stake
        let mut weights = BTreeMap::new();
        let city_importance = [
            ("NYC", 30),
            ("LON", 25),
            ("FRA", 20),
            ("TOK", 15),
            ("SIN", 10),
            ("SYD", 8),
            ("AMS", 7),
            ("LAX", 5),
        ];

        for city in cities {
            let weight = city_importance
                .iter()
                .find(|(code, _)| city.contains(code))
                .map(|(_, w)| *w)
                .unwrap_or(1);
            weights.insert(city.clone(), weight);
        }
        weights
    };

    // Generate demands based on weights
    for (source, source_weight) in &city_weights {
        for (destination, dest_weight) in &city_weights {
            if source != destination {
                let traffic = (*source_weight as f64) / 100.0;
                let priority = (*dest_weight as f64) / 100.0;

                demands.push(Demand::new(
                    source.clone(),
                    destination.clone(),
                    1, // receivers
                    traffic,
                    priority,
                    demand_type,
                    true, // multicast for leader traffic
                ));
            }
        }
        demand_type += 1;
    }

    info!(
        "Generated {} synthetic demands with {} weighted cities",
        demands.len(),
        city_weights.len()
    );
    Ok(demands)
}

/// Generate demands from manual validator->city mapping file
pub fn generate_manual_demands(
    mapping_file: &Path,
    cities: &[String],
    leader_schedule: Option<BTreeMap<String, Vec<usize>>>,
) -> Result<Demands> {
    // Read mapping file
    let content = std::fs::read_to_string(mapping_file)?;
    let mapping: ValidatorCityMapping = serde_json::from_str(&content)?;

    let mut city_stakes = BTreeMap::new();

    if let Some(schedule) = leader_schedule {
        // Map validators to cities using manual mapping
        for (validator, slots) in schedule {
            let stake = slots.len(); // Number of slots represents stake weight
            if let Some(city) = mapping.validators.get(&validator) {
                *city_stakes.entry(city.clone()).or_insert(0) += stake;
            } else if let Some(default_city) = &mapping.default_city {
                *city_stakes.entry(default_city.clone()).or_insert(0) += stake;
            } else {
                warn!("No mapping for validator {}", validator);
            }
        }
    } else {
        // No leader schedule, use uniform distribution
        for city in cities {
            city_stakes.insert(city.clone(), 100);
        }
    }

    // Generate demands based on mapped stakes
    let mut demands = Vec::new();
    let mut demand_type = 1u32;
    let total_stake: usize = city_stakes.values().sum();

    for (source, source_stake) in &city_stakes {
        for destination in city_stakes.keys() {
            if source != destination {
                let traffic = (*source_stake as f64) / (total_stake as f64) * 100.0;

                demands.push(Demand::new(
                    source.clone(),
                    destination.clone(),
                    1, // receivers
                    traffic,
                    1.0, // priority
                    demand_type,
                    true, // multicast
                ));
            }
        }
        demand_type += 1;
    }

    info!(
        "Generated {} demands from manual mapping with {} cities",
        demands.len(),
        city_stakes.len()
    );
    Ok(demands)
}

/// Generate distance-based demands (closer cities have more traffic)
pub fn generate_distance_based_demands(cities: &[String]) -> Result<Demands> {
    let mut demands = Vec::new();
    let mut demand_type = 1u32;

    // Simplified distance model based on geography
    // In production, this would use actual geographic distances
    for source in cities {
        for destination in cities {
            if source != destination {
                // Simple heuristic: same continent = high traffic, different = low
                let traffic = estimate_traffic_by_distance(source, destination);

                demands.push(Demand::new(
                    source.clone(),
                    destination.clone(),
                    1, // receivers
                    traffic,
                    1.0, // priority
                    demand_type,
                    false, // multicast
                ));
            }
        }
        demand_type += 1;
    }

    info!("Generated {} distance-based demands", demands.len());
    Ok(demands)
}

/// Generate population-based demands (larger cities have more traffic)
pub fn generate_population_based_demands(cities: &[String]) -> Result<Demands> {
    let mut demands = Vec::new();
    let mut demand_type = 1u32;

    // Simplified population model
    let city_populations = get_city_populations();

    for source in cities {
        let source_pop = city_populations
            .iter()
            .find(|(city, _)| source.contains(city))
            .map(|(_, pop)| *pop)
            .unwrap_or(1.0);

        for destination in cities {
            if source != destination {
                let dest_pop = city_populations
                    .iter()
                    .find(|(city, _)| destination.contains(city))
                    .map(|(_, pop)| *pop)
                    .unwrap_or(1.0);

                // Traffic proportional to population product
                let traffic = (source_pop * dest_pop).sqrt() / 100.0;

                demands.push(Demand::new(
                    source.clone(),
                    destination.clone(),
                    1, // receivers
                    traffic,
                    1.0, // priority
                    demand_type,
                    false, // multicast
                ));
            }
        }
        demand_type += 1;
    }

    info!("Generated {} population-based demands", demands.len());
    Ok(demands)
}

// Helper functions

fn extract_city_from_validator(validator: &str, cities: &[String]) -> Option<String> {
    // Try to find a city code in the validator name
    for city in cities {
        if validator.to_uppercase().contains(&city.to_uppercase()) {
            return Some(city.clone());
        }
    }
    None
}

fn estimate_traffic_by_distance(source: &str, destination: &str) -> f64 {
    // Simplified continental grouping
    let north_america = ["NYC", "LAX", "CHI", "TOR"];
    let europe = ["LON", "FRA", "AMS", "BER"];
    let asia = ["TOK", "SIN", "HKG", "BOM"];
    let oceania = ["SYD", "MEL", "AKL"];

    let source_continent = get_continent(source, &north_america, &europe, &asia, &oceania);
    let dest_continent = get_continent(destination, &north_america, &europe, &asia, &oceania);

    if source_continent == dest_continent {
        10.0 // High traffic within continent
    } else {
        2.0 // Lower traffic between continents
    }
}

fn get_continent(city: &str, na: &[&str], eu: &[&str], asia: &[&str], oc: &[&str]) -> &'static str {
    if na.iter().any(|c| city.contains(c)) {
        "NA"
    } else if eu.iter().any(|c| city.contains(c)) {
        "EU"
    } else if asia.iter().any(|c| city.contains(c)) {
        "ASIA"
    } else if oc.iter().any(|c| city.contains(c)) {
        "OC"
    } else {
        "OTHER"
    }
}

fn get_city_populations() -> Vec<(&'static str, f64)> {
    vec![
        ("NYC", 8.3),
        ("LON", 9.0),
        ("TOK", 13.9),
        ("FRA", 2.2),
        ("SIN", 5.7),
        ("LAX", 4.0),
        ("SYD", 5.3),
        ("AMS", 0.9),
        ("HKG", 7.5),
        ("BER", 3.7),
    ]
}

/// Validator to city mapping structure
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatorCityMapping {
    pub validators: BTreeMap<String, String>,
    pub default_city: Option<String>,
}
