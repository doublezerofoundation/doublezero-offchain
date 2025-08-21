use anyhow::{Result, ensure};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub struct RttStats {
    pub mean_us: f64,
    pub median_us: f64,
    pub min_us: f64,
    pub max_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JitterStats {
    pub avg_jitter_us: f64,
    pub max_jitter_us: f64,
    pub ewma_jitter_us: f64,
}

pub fn display_us_as_ms(us: &f64) -> String {
    format!("{}", us / 1000.0)
}

pub fn calculate_rtt_statistics(values: &[f64]) -> Result<RttStats> {
    if values.is_empty() {
        return Ok(RttStats {
            mean_us: 0.0,
            median_us: 0.0,
            min_us: 0.0,
            max_us: 0.0,
            p95_us: 0.0,
            p99_us: 0.0,
        });
    }

    // Validate all values are finite
    ensure!(
        values.iter().all(|v| v.is_finite()),
        "RTT values must be finite numbers"
    );

    let mut sorted_values = values.to_vec();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let len = sorted_values.len();
    let sum: f64 = sorted_values.iter().sum();
    let mean = sum / len as f64;

    let median = if len % 2 == 0 {
        (sorted_values[len / 2 - 1] + sorted_values[len / 2]) / 2.0
    } else {
        sorted_values[len / 2]
    };

    let p95_index = ((len as f64 * 0.95) - 1.0).max(0.0) as usize;
    let p99_index = ((len as f64 * 0.99) - 1.0).max(0.0) as usize;

    let p95 = sorted_values.get(p95_index).copied().unwrap_or(mean);
    let p99 = sorted_values.get(p99_index).copied().unwrap_or(mean);

    Ok(RttStats {
        mean_us: mean,
        median_us: median,
        min_us: sorted_values[0],
        max_us: sorted_values[len - 1],
        p95_us: p95,
        p99_us: p99,
    })
}

pub fn calculate_jitter_statistics(
    samples: &[u32],
    start_idx: usize,
    end_idx: usize,
) -> Result<JitterStats> {
    ensure!(
        start_idx <= end_idx,
        "Start index must be less than or equal to end index"
    );

    if start_idx >= end_idx || start_idx >= samples.len() {
        return Ok(JitterStats {
            avg_jitter_us: 0.0,
            max_jitter_us: 0.0,
            ewma_jitter_us: 0.0,
        });
    }

    let actual_end_idx = end_idx.min(samples.len());

    // Extract non-zero samples (successful RTT measurements)
    let mut ordered: Vec<f64> = Vec::new();
    for &sample in samples.iter().take(actual_end_idx).skip(start_idx) {
        if sample > 0 {
            ordered.push(sample as f64);
        }
    }

    if ordered.len() < 2 {
        return Ok(JitterStats {
            avg_jitter_us: 0.0,
            max_jitter_us: 0.0,
            ewma_jitter_us: 0.0,
        });
    }

    // Calculate deltas and absolute deltas (IPDV methodology)
    let mut abs_deltas = Vec::new();

    // Initialize EWMA with first absolute delta
    let first_abs = (ordered[1] - ordered[0]).abs();
    let mut ewma = first_abs;
    let mut max_abs = first_abs;
    abs_deltas.push(first_abs);

    // Process remaining samples with EWMA calculation
    for i in 2..ordered.len() {
        let delta = ordered[i] - ordered[i - 1];
        let abs_delta = delta.abs();
        abs_deltas.push(abs_delta);

        // EWMA update with α = 1/16 (matching Go implementation)
        ewma += (abs_delta - ewma) / 16.0;

        if abs_delta > max_abs {
            max_abs = abs_delta;
        }
    }

    // Calculate average of absolute deltas
    let sum: f64 = abs_deltas.iter().sum();
    let avg = sum / abs_deltas.len() as f64;

    Ok(JitterStats {
        avg_jitter_us: avg,
        max_jitter_us: max_abs,
        ewma_jitter_us: ewma,
    })
}

pub fn calculate_packet_loss(total_expected: usize, total_actual: usize) -> Result<f64> {
    ensure!(
        total_actual <= total_expected,
        "Actual packets cannot exceed expected packets"
    );

    if total_expected == 0 {
        return Ok(0.0);
    }

    let loss = total_expected.saturating_sub(total_actual) as f64;
    Ok((loss / total_expected as f64).clamp(0.0, 1.0))
}

pub fn haversine_distance(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lng = (lng2 - lng1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_KM * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtt_statistics() {
        let values = vec![100.0, 200.0, 300.0, 400.0, 500.0];
        let stats = calculate_rtt_statistics(&values).unwrap();

        assert_eq!(stats.mean_us, 300.0);
        assert_eq!(stats.median_us, 300.0);
        assert_eq!(stats.min_us, 100.0);
        assert_eq!(stats.max_us, 500.0);
    }

    #[test]
    fn test_empty_rtt_statistics() {
        let values = vec![];
        let stats = calculate_rtt_statistics(&values).unwrap();

        assert_eq!(stats.mean_us, 0.0);
        assert_eq!(stats.median_us, 0.0);
    }

    #[test]
    fn test_jitter_statistics() {
        let samples = vec![100, 150, 140, 180, 170];
        let stats = calculate_jitter_statistics(&samples, 0, 5).unwrap();

        // Verify average jitter
        let expected_deltas = [50.0, 10.0, 40.0, 10.0];
        let expected_avg = expected_deltas.iter().sum::<f64>() / expected_deltas.len() as f64;
        assert!((stats.avg_jitter_us - expected_avg).abs() < 0.001);

        // Verify max jitter
        assert_eq!(stats.max_jitter_us, 50.0); // 150 - 100

        // Verify EWMA calculation
        // EWMA starts at 50, then updates with each delta
        let mut ewma = 50.0; // First delta
        ewma += (10.0 - ewma) / 16.0; // Second delta
        ewma += (40.0 - ewma) / 16.0; // Third delta
        ewma += (10.0 - ewma) / 16.0; // Fourth delta
        assert!((stats.ewma_jitter_us - ewma).abs() < 0.001);
    }

    #[test]
    fn test_ipdv_with_packet_loss() {
        // Test with some zero values (packet loss)
        let samples = vec![100, 0, 150, 140, 0, 180];
        let stats = calculate_jitter_statistics(&samples, 0, 6).unwrap();

        // Should only process non-zero samples: [100, 150, 140, 180]
        // Deltas: |150-100|=50, |140-150|=10, |180-140|=40
        assert_eq!(stats.max_jitter_us, 50.0);
        let expected_avg = (50.0 + 10.0 + 40.0) / 3.0;
        assert!((stats.avg_jitter_us - expected_avg).abs() < 0.001);
    }

    #[test]
    fn test_ipdv_single_sample() {
        let samples = vec![100];
        let stats = calculate_jitter_statistics(&samples, 0, 1).unwrap();

        assert_eq!(stats.avg_jitter_us, 0.0);
        assert_eq!(stats.max_jitter_us, 0.0);
        assert_eq!(stats.ewma_jitter_us, 0.0);
    }

    #[test]
    fn test_ipdv_two_samples() {
        let samples = vec![100, 120];
        let stats = calculate_jitter_statistics(&samples, 0, 2).unwrap();

        assert_eq!(stats.avg_jitter_us, 20.0);
        assert_eq!(stats.max_jitter_us, 20.0);
        assert_eq!(stats.ewma_jitter_us, 20.0); // Only one delta, so EWMA = delta
    }

    #[test]
    fn test_packet_loss() {
        assert_eq!(calculate_packet_loss(100, 95).unwrap(), 0.05);
        assert_eq!(calculate_packet_loss(100, 100).unwrap(), 0.0);
        assert_eq!(calculate_packet_loss(0, 0).unwrap(), 0.0);
    }

    #[test]
    fn test_invalid_packet_loss() {
        // Test that actual > expected returns an error
        assert!(calculate_packet_loss(100, 101).is_err());
    }

    #[test]
    fn test_invalid_rtt_values() {
        let values = vec![100.0, f64::NAN, 300.0];
        assert!(calculate_rtt_statistics(&values).is_err());

        let values = vec![100.0, f64::INFINITY, 300.0];
        assert!(calculate_rtt_statistics(&values).is_err());
    }

    #[test]
    fn test_display_us_as_ms() {
        assert_eq!(display_us_as_ms(&1000.0), "1");
        assert_eq!(display_us_as_ms(&1500.0), "1.5");
    }
}
