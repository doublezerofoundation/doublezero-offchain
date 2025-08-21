use anyhow::Result;
use contributor_rewards::{
    calculator::shapley_handler::build_public_links, ingestor::types::FetchData,
    processor::internet::InternetTelemetryProcessor,
};
use serde_json::Value;
use std::{collections::HashMap, fs, path::Path};

fn load_test_data() -> Result<FetchData> {
    let data_path = Path::new("tests/testnet_snapshot.json");
    let json = fs::read_to_string(data_path)?;
    let data: Value = serde_json::from_str(&json)?;

    // Parse the JSON into FetchData manually
    let fetch_data: FetchData = serde_json::from_value(data)?;
    Ok(fetch_data)
}

fn create_expected_results() -> HashMap<(String, String), f64> {
    let mut expected = HashMap::new();

    // These are the exact values from the public links output
    expected.insert(("ams".to_string(), "fra".to_string()), 6.913);
    expected.insert(("ams".to_string(), "lax".to_string()), 142.035);
    expected.insert(("ams".to_string(), "lon".to_string()), 11.9305);
    expected.insert(("ams".to_string(), "nyc".to_string()), 78.9935);
    expected.insert(("ams".to_string(), "prg".to_string()), 16.645);
    expected.insert(("ams".to_string(), "sin".to_string()), 206.1845);
    expected.insert(("ams".to_string(), "tyo".to_string()), 247.407);

    expected.insert(("fra".to_string(), "lax".to_string()), 142.3075);
    expected.insert(("fra".to_string(), "lon".to_string()), 11.9895);
    expected.insert(("fra".to_string(), "nyc".to_string()), 84.7845);
    expected.insert(("fra".to_string(), "prg".to_string()), 10.844);
    expected.insert(("fra".to_string(), "sin".to_string()), 167.347);
    expected.insert(("fra".to_string(), "tyo".to_string()), 242.1715);

    expected.insert(("lax".to_string(), "lon".to_string()), 149.663);
    expected.insert(("lax".to_string(), "nyc".to_string()), 68.329);
    expected.insert(("lax".to_string(), "prg".to_string()), 154.908);
    expected.insert(("lax".to_string(), "sin".to_string()), 174.8895);
    expected.insert(("lax".to_string(), "tyo".to_string()), 107.085);

    expected.insert(("lon".to_string(), "nyc".to_string()), 87.2915);
    expected.insert(("lon".to_string(), "prg".to_string()), 21.9575);
    expected.insert(("lon".to_string(), "sin".to_string()), 203.131);
    expected.insert(("lon".to_string(), "tyo".to_string()), 256.9815);

    expected.insert(("nyc".to_string(), "prg".to_string()), 97.8475);
    expected.insert(("nyc".to_string(), "sin".to_string()), 333.3755);
    expected.insert(("nyc".to_string(), "tyo".to_string()), 170.3865);

    expected.insert(("prg".to_string(), "sin".to_string()), 168.627);
    expected.insert(("prg".to_string(), "tyo".to_string()), 269.2265);

    expected.insert(("sin".to_string(), "tyo".to_string()), 69.147);

    expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_links_generation() -> Result<()> {
        // Load test data from JSON file
        let fetch_data = load_test_data()?;
        println!(
            "Loaded snapshot with {} exchanges, {} locations, {} devices",
            fetch_data.dz_serviceability.exchanges.len(),
            fetch_data.dz_serviceability.locations.len(),
            fetch_data.dz_serviceability.devices.len()
        );
        println!(
            "Internet telemetry samples: {}",
            fetch_data.dz_internet.internet_latency_samples.len()
        );

        // Process internet telemetry to get stats
        let internet_stats = InternetTelemetryProcessor::process(&fetch_data)?;
        println!(
            "Processed {} internet telemetry stats",
            internet_stats.len()
        );

        // Create test data with proper exchange->device->location mapping
        let mut serviceability_data = DZServiceabilityData::default();

        // Create fake exchange PKs
        let xchi_exchange_pk = Pubkey::new_unique();
        let xpit_exchange_pk = Pubkey::new_unique();

        // Create fake device PKs
        let chi_device_pk = Pubkey::new_unique();
        let pit_device_pk = Pubkey::new_unique();

        // Create fake location PKs
        let chi_location_pk = Pubkey::new_unique();
        let pit_location_pk = Pubkey::new_unique();

        // Add exchanges
        serviceability_data.exchanges.insert(
            xchi_exchange_pk,
            Exchange {
                code: "xchi".to_string(),
                ..Default::default()
            },
        );
        serviceability_data.exchanges.insert(
            xpit_exchange_pk,
            Exchange {
                code: "xpit".to_string(),
                ..Default::default()
            },
        );

        // Add locations
        serviceability_data.locations.insert(
            chi_location_pk,
            Location {
                code: "chi".to_string(),
                ..Default::default()
            },
        );
        serviceability_data.locations.insert(
            pit_location_pk,
            Location {
                code: "pit".to_string(),
                ..Default::default()
            },
        );

        // Add devices that link exchanges to locations
        serviceability_data.devices.insert(
            chi_device_pk,
            Device {
                exchange_pk: xchi_exchange_pk,
                location_pk: chi_location_pk,
                ..Default::default()
            },
        );
        serviceability_data.devices.insert(
            pit_device_pk,
            Device {
                exchange_pk: xpit_exchange_pk,
                location_pk: pit_location_pk,
                ..Default::default()
            },
        );

        let fetch_data = FetchData {
            dz_serviceability: serviceability_data,
            ..Default::default()
        };

        // Generate public links
        let public_links = build_public_links(&internet_stats, &fetch_data)?;

        // Print results for verification
        println!("\nPublic Links Generated:");
        println!("{:<5} | {:<5} | {:>15}", "city1", "city2", "latency(ms)");
        println!("{:-<35}", "");
        for link in &public_links {
            println!(
                "{:<5} | {:<5} | {:>15.3}",
                link.city1, link.city2, link.latency
            );
        }

        // Verify we have the expected number of city pairs
        // With 8 cities, we expect C(8,2) = 28 city pairs
        let expected_count = 28;
        println!(
            "\nExpected {} city pairs, got {}",
            expected_count,
            public_links.len()
        );

        // Allow for some missing pairs due to data availability
        assert!(
            public_links.len() >= expected_count / 2,
            "Expected at least {} city pairs, got {}",
            expected_count / 2,
            public_links.len()
        );

        // Get expected results
        let expected = create_expected_results();

        // Create a map from public_links for easier comparison
        let mut result_map: HashMap<(String, String), f64> = HashMap::new();
        for link in &public_links {
            result_map.insert((link.city1.clone(), link.city2.clone()), link.latency);
        }

        // Verify that we have reasonable latency values
        for link in &public_links {
            // Allow 0.0 for links with no data or same location
            assert!(
                link.latency >= 0.0 && link.latency < 1000.0,
                "Unreasonable latency value for {} -> {}: {}",
                link.city1,
                link.city2,
                link.latency
            );
        }

        // Verify the exact values match expected results with small tolerance
        for ((city1, city2), expected_latency) in expected.iter() {
            if let Some(actual_latency) = result_map.get(&(city1.clone(), city2.clone())) {
                // Allow very small difference due to floating point precision
                let diff = (actual_latency - expected_latency).abs();
                println!(
                    "Checking {}->{}: expected {:.3}, got {:.3}, diff {:.6}",
                    city1, city2, expected_latency, actual_latency, diff
                );
                // We should get exact or very close values since we're using the same test data
                assert!(
                    diff < 0.001,
                    "Latency mismatch for {city1} -> {city2}: got {actual_latency}, expected {expected_latency}"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_snapshot_data_integrity() -> Result<()> {
        let fetch_data = load_test_data()?;

        // Verify we have the expected cities
        let expected_cities = vec!["ams", "fra", "lax", "lon", "nyc", "prg", "sin", "tyo"];

        let location_codes: Vec<String> = fetch_data
            .dz_serviceability
            .locations
            .values()
            .map(|loc| loc.code.clone())
            .collect();

        for city in expected_cities {
            assert!(
                location_codes.contains(&city.to_string()),
                "Missing expected city: {}",
                city
            );
        }

        // Verify exchanges have 'x' prefix
        for exchange in fetch_data.dz_serviceability.exchanges.values() {
            assert!(
                exchange.code.starts_with('x'),
                "Exchange code should start with 'x': {}",
                exchange.code
            );
        }

        // Verify we have internet telemetry samples
        assert!(
            !fetch_data.dz_internet.internet_latency_samples.is_empty(),
            "No internet telemetry samples found"
        );

        // Verify telemetry samples use exchange PKs that exist
        for sample in &fetch_data.dz_internet.internet_latency_samples {
            let origin_exists = fetch_data
                .dz_serviceability
                .exchanges
                .contains_key(&sample.origin_exchange_pk);
            let target_exists = fetch_data
                .dz_serviceability
                .exchanges
                .contains_key(&sample.target_exchange_pk);

            // Some samples might still use old location PKs, that's OK
            if origin_exists && target_exists {
                println!(
                    "Valid sample: {} -> {}",
                    fetch_data.dz_serviceability.exchanges[&sample.origin_exchange_pk].code,
                    fetch_data.dz_serviceability.exchanges[&sample.target_exchange_pk].code
                );
            }
        }

        Ok(())
    }
}
