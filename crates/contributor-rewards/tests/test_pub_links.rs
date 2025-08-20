use anyhow::Result;
use contributor_rewards::{
    calculator::shapley_handler::build_public_links,
    ingestor::types::{DZServiceabilityData, FetchData},
    processor::internet::{InternetTelemetryStatMap, InternetTelemetryStats},
};
use doublezero_serviceability::state::{device::Device, exchange::Exchange, location::Location};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, fs, path::Path, str::FromStr};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestInternetStats {
    circuit: String,
    origin_code: String,
    target_code: String,
    data_provider_name: String,
    oracle_agent_pk: String,
    origin_exchange_pk: String,
    target_exchange_pk: String,
    rtt_mean_us: f64,
    rtt_median_us: f64,
    rtt_min_us: f64,
    rtt_max_us: f64,
    rtt_p95_us: f64,
    rtt_p99_us: f64,
    avg_jitter_us: f64,
    max_jitter_us: f64,
    packet_loss: f64,
    total_samples: usize,
}

fn load_test_data() -> Result<HashMap<String, TestInternetStats>> {
    let data_path = Path::new("tests/devnet_inet_data.json");
    let json = fs::read_to_string(data_path)?;
    let data: HashMap<String, TestInternetStats> = serde_json::from_str(&json)?;
    Ok(data)
}

fn convert_to_internet_stat_map(
    test_data: HashMap<String, TestInternetStats>,
) -> InternetTelemetryStatMap {
    let mut result = HashMap::new();

    for (key, test_stats) in test_data {
        let internet_stats = InternetTelemetryStats {
            circuit: test_stats.circuit,
            origin_exchange_code: test_stats.origin_code,
            target_exchange_code: test_stats.target_code,
            data_provider_name: test_stats.data_provider_name,
            oracle_agent_pk: Pubkey::from_str(&test_stats.oracle_agent_pk).unwrap_or_default(),
            origin_exchange_pk: Pubkey::from_str(&test_stats.origin_exchange_pk)
                .unwrap_or_default(),
            target_exchange_pk: Pubkey::from_str(&test_stats.target_exchange_pk)
                .unwrap_or_default(),
            rtt_mean_us: test_stats.rtt_mean_us,
            rtt_median_us: test_stats.rtt_median_us,
            rtt_min_us: test_stats.rtt_min_us,
            rtt_max_us: test_stats.rtt_max_us,
            rtt_p95_us: test_stats.rtt_p95_us,
            rtt_p99_us: test_stats.rtt_p99_us,
            avg_jitter_us: test_stats.avg_jitter_us,
            max_jitter_us: test_stats.max_jitter_us,
            packet_loss: test_stats.packet_loss,
            total_samples: test_stats.total_samples,
        };

        result.insert(key, internet_stats);
    }

    result
}

fn create_expected_results() -> HashMap<(String, String), f64> {
    let mut expected = HashMap::new();

    // Expected output for devnet data: chi → pit
    // Now using location codes directly from internet telemetry
    // Average of wheresitup (17.988237ms) and ripeatlas (9.992551ms) p95 values
    // Rounding p95 values: wheresitup (18.010ms) and ripeatlas (10.183ms) = 14.0965ms average
    expected.insert(("chi".to_string(), "pit".to_string()), 14.0965);

    expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_links_generation() -> Result<()> {
        // Load test data from JSON file
        let test_data = load_test_data()?;
        println!("Loaded {} internet telemetry records", test_data.len());

        // Convert to InternetTelemetryStatMap
        let internet_stats = convert_to_internet_stat_map(test_data);

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

        // Verify we have the expected number of city pairs
        assert_eq!(
            public_links.len(),
            1,
            "Expected 1 city pair, got {}",
            public_links.len()
        );

        // Get expected results
        let expected = create_expected_results();

        // Create a map from public_links for easier comparison
        let mut result_map: HashMap<(String, String), f64> = HashMap::new();
        for link in &public_links {
            result_map.insert((link.city1.clone(), link.city2.clone()), link.latency);
        }

        // Verify each expected city pair exists and has the correct latency
        for ((city1, city2), expected_latency) in expected.iter() {
            let actual_latency = result_map.get(&(city1.clone(), city2.clone())).unwrap();

            // Use approximate equality for floating point comparison
            // Allow small difference due to floating point precision
            let diff = (actual_latency - expected_latency).abs();
            assert!(
                diff < 0.001,
                "Latency mismatch for {city1} -> {city2}: got {actual_latency}, expected {expected_latency}, diff {diff}",
            );
        }

        // Verify no unexpected city pairs
        assert_eq!(
            result_map.len(),
            expected.len(),
            "Result contains unexpected city pairs"
        );

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

        Ok(())
    }

    #[test]
    fn test_expected_results_completeness() {
        // Verify that we have the expected city pairs for devnet data
        let expected = create_expected_results();

        // For devnet data, we only have one city pair: xchi -> xpit
        assert_eq!(
            expected.len(),
            1,
            "Expected results should contain exactly 1 entry for devnet data"
        );

        assert!(
            expected.contains_key(&("xchi".to_string(), "xpit".to_string())),
            "Missing city pair: xchi -> xpit"
        );
    }
}
