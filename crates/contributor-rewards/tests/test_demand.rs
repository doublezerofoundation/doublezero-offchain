use anyhow::Result;
use contributor_rewards::ingestor::{demand, types::FetchData};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fs, path::Path};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestLeaderSchedules {
    // validator_pubkey -> schedule_length (stake proxy)
    pub leader_schedule: BTreeMap<String, usize>,
}

fn load_test_data() -> Result<FetchData> {
    let data_path = Path::new("tests/testnet_snapshot.json");
    let json = fs::read_to_string(data_path)?;
    let data: Value = serde_json::from_str(&json)?;

    // Parse the JSON into FetchData
    let fetch_data: FetchData = serde_json::from_value(data)?;
    Ok(fetch_data)
}

fn load_test_leader_schedules() -> Result<TestLeaderSchedules> {
    let data_path = Path::new("tests/test_leader_schedules.json");
    let json = fs::read_to_string(data_path)?;
    let data: Value = serde_json::from_str(&json)?;

    // Parse the JSON into FetchData
    let schedules: TestLeaderSchedules = serde_json::from_value(data)?;
    Ok(schedules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_demand_generation_from_json() -> Result<()> {
        // Load test data
        let fetch_data = load_test_data()?;
        let test_leader_schedules = load_test_leader_schedules()?;

        // Build demands using the refactored function
        let result =
            demand::build_with_schedule(&fetch_data, test_leader_schedules.leader_schedule)?;

        // Verify results
        println!("\nGenerated {} demands", result.demands.len());

        // Basic assertions
        assert!(
            !result.demands.is_empty(),
            "Should generate at least one demand"
        );

        // Verify no self-loops
        for demand in &result.demands {
            assert_ne!(demand.start, demand.end, "Should not have self-loops");
        }

        let expected = [
            ("ams", "fra", 88, 0.0009177188552188551),
            ("ams", "lax", 13, 0.00031410256410256405),
            ("ams", "lon", 26, 0.00034152421652421653),
            ("ams", "nyc", 23, 0.0008019323671497584),
            ("ams", "sin", 12, 0.0007106481481481482),
            ("ams", "tyo", 3, 0.00023765432098765433),
            ("fra", "ams", 25, 0.0004188888888888889),
            ("fra", "lax", 13, 0.00031410256410256405),
            ("fra", "lon", 26, 0.00034152421652421653),
            ("fra", "nyc", 23, 0.0008019323671497584),
            ("fra", "sin", 12, 0.0007106481481481482),
            ("fra", "tyo", 3, 0.00023765432098765433),
            ("lax", "ams", 25, 0.0004188888888888889),
            ("lax", "fra", 88, 0.0009177188552188551),
            ("lax", "lon", 26, 0.00034152421652421653),
            ("lax", "nyc", 23, 0.0008019323671497584),
            ("lax", "sin", 12, 0.0007106481481481482),
            ("lax", "tyo", 3, 0.00023765432098765433),
            ("lon", "ams", 25, 0.0004188888888888889),
            ("lon", "fra", 88, 0.0009177188552188551),
            ("lon", "lax", 13, 0.00031410256410256405),
            ("lon", "nyc", 23, 0.0008019323671497584),
            ("lon", "sin", 12, 0.0007106481481481482),
            ("lon", "tyo", 3, 0.00023765432098765433),
            ("nyc", "ams", 25, 0.0004188888888888889),
            ("nyc", "fra", 88, 0.0009177188552188551),
            ("nyc", "lax", 13, 0.00031410256410256405),
            ("nyc", "lon", 26, 0.00034152421652421653),
            ("nyc", "sin", 12, 0.0007106481481481482),
            ("nyc", "tyo", 3, 0.00023765432098765433),
            ("sin", "ams", 25, 0.0004188888888888889),
            ("sin", "fra", 88, 0.0009177188552188551),
            ("sin", "lax", 13, 0.00031410256410256405),
            ("sin", "lon", 26, 0.00034152421652421653),
            ("sin", "nyc", 23, 0.0008019323671497584),
            ("sin", "tyo", 3, 0.00023765432098765433),
            ("tyo", "ams", 25, 0.0004188888888888889),
            ("tyo", "fra", 88, 0.0009177188552188551),
            ("tyo", "lax", 13, 0.00031410256410256405),
            ("tyo", "lon", 26, 0.00034152421652421653),
            ("tyo", "nyc", 23, 0.0008019323671497584),
            ("tyo", "sin", 12, 0.0007106481481481482),
        ];

        println!("{:#?}", result.demands);

        // Should have exactly 56 demands (8 cities * 7 destinations each)
        assert_eq!(result.demands.len(), 42, "Should have exactly 56 demands");
        assert_eq!(
            expected.len(),
            42,
            "Test data should have 42 expected values"
        );

        // Verify each expected demand exists with correct priority
        for (exp_start, exp_end, exp_receivers, exp_priority) in expected {
            let found = result
                .demands
                .iter()
                .find(|d| d.start == exp_start && d.end == exp_end)
                .unwrap_or_else(|| {
                    panic!("Expected demand from {exp_start} to {exp_end} not found")
                });

            // Check receivers match
            assert_eq!(
                found.receivers, exp_receivers,
                "Receivevers mismatch for {}->{}: expected: {}, got: {}",
                exp_start, exp_end, exp_receivers, found.receivers
            );

            // Check priority match
            let diff = (found.priority - exp_priority).abs();
            assert!(
                diff < 1e-9,
                "Priority mismatch for {}->{}: expected {:.9e}, got {:.9e}, diff {:.9e}",
                exp_start,
                exp_end,
                exp_priority,
                found.priority,
                diff
            );
        }

        // Print demands (for debugging)
        for (i, demand) in result.demands.iter().enumerate() {
            println!(
                "  {}: {} -> {} (receivers: {}, priority: {:.4})",
                i + 1,
                demand.start,
                demand.end,
                demand.receivers,
                demand.priority
            );
        }

        Ok(())
    }
}
