use crate::constants::{MICRO_S_TO_MILLI_S, SEC_TO_MICRO_S};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, TimeZone, Utc, offset::LocalResult};
use humantime::{parse_duration, parse_rfc3339_weak};
use metrics_processor::dzd_telemetry_processor::DZDTelemetryStatMap;
use network_shapley::types::{Demand, PrivateLink};
use std::time::{SystemTime, UNIX_EPOCH};
use tabled::{builder::Builder as TableBuilder, settings::Style};

/// Parse a time range from before/after strings
pub fn parse_time_range(before: &str, after: &str) -> Result<(u64, u64)> {
    let before_us = parse_timestamp(before)
        .with_context(|| format!("Failed to parse 'before' timestamp: {before}"))?;
    let after_us = parse_timestamp(after)
        .with_context(|| format!("Failed to parse 'after' timestamp: {after}"))?;

    if before_us <= after_us {
        bail!(
            "'before' timestamp must be later than 'after' timestamp. Got before={}, after={}",
            before,
            after
        );
    }

    Ok((before_us, after_us))
}

/// Parse a single timestamp string to microseconds
pub fn parse_timestamp(input: &str) -> Result<u64> {
    // First try to parse as RFC3339 or weak RFC3339
    if let Ok(time) = parse_rfc3339_weak(input) {
        return Ok(system_time_to_micros(time));
    }

    // Check if it ends with "ago" for relative time
    if input.ends_with("ago") {
        // Parse the duration part (everything except "ago")
        let duration_str = input.trim_end_matches("ago").trim();
        let duration = parse_duration(duration_str)
            .with_context(|| format!("Failed to parse duration: {duration_str}"))?;

        // Calculate timestamp by subtracting from now
        let now = SystemTime::now();
        let timestamp = now
            .checked_sub(duration)
            .ok_or_else(|| anyhow!("Duration {} is too large", input))?;

        return Ok(system_time_to_micros(timestamp));
    }

    bail!(
        "Unable to parse timestamp: {}. Expected RFC3339 format (2024-01-15T10:00:00Z) or relative time (2 hours ago)",
        input
    )
}

/// Convert SystemTime to microseconds since Unix epoch
pub fn system_time_to_micros(time: SystemTime) -> u64 {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    duration.as_secs() * SEC_TO_MICRO_S + duration.subsec_micros() as u64
}

/// Convert microseconds to chrono DateTime for formatting
pub fn micros_to_datetime(micros: u64) -> Result<DateTime<Utc>> {
    let secs = (micros / SEC_TO_MICRO_S) as i64;
    let nanos = ((micros % SEC_TO_MICRO_S) * 1_000) as u32;
    match TimeZone::timestamp_opt(&Utc, secs, nanos) {
        LocalResult::Single(t) => Ok(t),
        other => bail!(format!("{other:?}")),
    }
}

pub fn print_telemetry_stats(map: &DZDTelemetryStatMap) -> String {
    let header = vec![
        "circuit".to_string(),
        "rtt_mean_ms".to_string(),
        "rtt_median_ms".to_string(),
        "rtt_min_ms".to_string(),
        "rtt_max_ms".to_string(),
        "rtt_p95_ms".to_string(),
        "rtt_p99_ms".to_string(),
        "avg_jitter_ms".to_string(),
        "max_jitter_ms".to_string(),
        "packet_loss".to_string(),
        "total_samples".to_string(),
    ];

    let mut printable = vec![];
    for stat in map.values() {
        let row = vec![
            stat.circuit.to_string(),
            format!("{:.4}", (stat.rtt_mean_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.rtt_median_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.rtt_min_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.rtt_max_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.rtt_p95_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.rtt_p99_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.avg_jitter_us / MICRO_S_TO_MILLI_S as f64)),
            format!("{:.4}", (stat.max_jitter_us / MICRO_S_TO_MILLI_S as f64)),
            stat.packet_loss.to_string(),
            stat.total_samples.to_string(),
        ];
        printable.push(row);
    }
    printable.sort();

    // Insert header at the beginning
    printable.insert(0, header);

    TableBuilder::from(printable)
        .build()
        .with(Style::psql().remove_horizontals())
        .to_string()
}

pub fn print_private_links(private_links: &[PrivateLink]) -> String {
    let header = vec![
        "device1".to_string(),
        "device2".to_string(),
        "latency_ms".to_string(),
        "bandwidth_Gbps".to_string(),
        "uptime".to_string(),
        "shared".to_string(),
    ];

    let mut printable = vec![];
    for pl in private_links {
        let row = vec![
            pl.device1.to_string(),
            pl.device2.to_string(),
            format!("{:.4}", pl.latency),
            format!("{:.4}", pl.bandwidth),
            format!("{:.4}", pl.uptime),
            format!("{:?}", pl.shared),
        ];
        printable.push(row);
    }
    printable.sort();

    // Insert header at the beginning
    printable.insert(0, header);

    TableBuilder::from(printable)
        .build()
        .with(Style::psql().remove_horizontals())
        .to_string()
}

pub fn print_demands(demands: &[Demand], k: usize) -> String {
    let header = vec![
        "start".to_string(),
        "end".to_string(),
        "receivers".to_string(),
        "traffic".to_string(),
        "priority".to_string(),
        "type".to_string(),
        "multicast".to_string(),
    ];

    let mut printable = vec![];
    for demand in demands.iter().take(k) {
        let row = vec![
            demand.start.to_string(),
            demand.end.to_string(),
            demand.receivers.to_string(),
            demand.traffic.to_string(),
            demand.priority.to_string(),
            demand.kind.to_string(),
            demand.multicast.to_string(),
        ];
        printable.push(row);
    }

    printable.sort();
    printable.insert(0, header);

    TableBuilder::from(printable)
        .build()
        .with(Style::psql().remove_horizontals())
        .to_string()
}
