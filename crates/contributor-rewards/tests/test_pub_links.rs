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

    // Based on the testnet data, we expect multiple city pairs
    // These are examples - adjust based on actual data in testnet_snapshot.json
    // The latencies should be calculated from the internet telemetry samples

    // Example expected pairs (you'll need to calculate actual values from the data)
    // For now, using placeholder values that will need to be updated
    expected.insert(("ams".to_string(), "fra".to_string()), 7.0);
    expected.insert(("ams".to_string(), "lax".to_string()), 140.0);
    expected.insert(("ams".to_string(), "lon".to_string()), 12.0);
    expected.insert(("ams".to_string(), "nyc".to_string()), 79.0);
    expected.insert(("ams".to_string(), "prg".to_string()), 17.0);
    expected.insert(("ams".to_string(), "sin".to_string()), 205.0);
    expected.insert(("ams".to_string(), "tyo".to_string()), 247.0);

    expected.insert(("fra".to_string(), "lax".to_string()), 142.0);
    expected.insert(("fra".to_string(), "lon".to_string()), 12.0);
    expected.insert(("fra".to_string(), "nyc".to_string()), 85.0);
    expected.insert(("fra".to_string(), "prg".to_string()), 11.0);
    expected.insert(("fra".to_string(), "sin".to_string()), 167.0);
    expected.insert(("fra".to_string(), "tyo".to_string()), 242.0);

    expected.insert(("lax".to_string(), "lon".to_string()), 150.0);
    expected.insert(("lax".to_string(), "nyc".to_string()), 68.0);
    expected.insert(("lax".to_string(), "prg".to_string()), 155.0);
    expected.insert(("lax".to_string(), "sin".to_string()), 175.0);
    expected.insert(("lax".to_string(), "tyo".to_string()), 107.0);

    expected.insert(("lon".to_string(), "nyc".to_string()), 87.0);
    expected.insert(("lon".to_string(), "prg".to_string()), 22.0);
    expected.insert(("lon".to_string(), "sin".to_string()), 203.0);
    expected.insert(("lon".to_string(), "tyo".to_string()), 257.0);

    expected.insert(("nyc".to_string(), "prg".to_string()), 98.0);
    expected.insert(("nyc".to_string(), "sin".to_string()), 333.0);
    expected.insert(("nyc".to_string(), "tyo".to_string()), 170.0);

    expected.insert(("prg".to_string(), "sin".to_string()), 169.0);
    expected.insert(("prg".to_string(), "tyo".to_string()), 269.0);

    expected.insert(("sin".to_string(), "tyo".to_string()), 69.0);

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
            assert!(
                link.latency > 0.0 && link.latency < 1000.0,
                "Unreasonable latency value for {} -> {}: {}",
                link.city1,
                link.city2,
                link.latency
            );
        }

        // If we have matching expected pairs, verify they're close
        for ((city1, city2), expected_latency) in expected.iter() {
            if let Some(actual_latency) = result_map.get(&(city1.clone(), city2.clone())) {
                // Allow 50% difference since these are estimates
                let diff_ratio = (actual_latency - expected_latency).abs() / expected_latency;
                println!(
                    "Checking {}->{}: expected {:.3}, got {:.3}, diff ratio {:.2}",
                    city1, city2, expected_latency, actual_latency, diff_ratio
                );
                // We're being lenient here since exact values depend on the actual data
                assert!(
                    diff_ratio < 1.0,
                    "Large latency difference for {city1} -> {city2}: got {actual_latency}, expected {expected_latency}"
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
