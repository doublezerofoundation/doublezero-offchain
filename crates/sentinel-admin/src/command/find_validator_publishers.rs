use std::{collections::HashMap, net::Ipv4Addr};

use anyhow::Result;
use clap::Args;
use doublezero_ledger_sentinel::client::{
    dz_ledger_reader::{self, DzLedgerReader, RpcDzLedgerReader},
    validator_metadata_reader::{
        DataApiValidatorMetadataReader, ValidatorMetadataReader, ValidatorRecord,
    },
};
use doublezero_sdk::UserType;
use doublezero_serviceability::pda::get_tenant_pda;
use doublezero_solana_client_tools::rpc::SolanaConnectionOptions;
use serde::Serialize;
use tabled::Tabled;

use crate::output::{OutputOptions, print_table};

const DEFAULT_DATA_API_URL: &str = "https://data.malbeclabs.com/api/sql/query";

/// Find IBRL validators and their multicast publisher status.
#[derive(Debug, Args)]
pub struct FindValidatorMulticastPublishersCommand {
    /// Filter by multicast group (pubkey or code, e.g. "edge-solana-shreds").
    #[arg(long, value_name = "KEY_OR_CODE")]
    multicast_group: Option<String>,

    /// Only show validators that are already a publisher.
    #[arg(long)]
    is_publisher: bool,

    /// Only show validators that are NOT a publisher.
    #[arg(long)]
    not_publisher: bool,

    /// Minimum activated stake in SOL to include.
    #[arg(long, value_name = "SOL")]
    min_stake: Option<f64>,

    /// Maximum activated stake in SOL to include.
    #[arg(long, value_name = "SOL")]
    max_stake: Option<f64>,

    /// Filter by validator client name (e.g. "JitoLabs", "AgaveBam", "Frankendancer").
    #[arg(long, value_name = "NAME")]
    client: Option<String>,

    /// Include validators not yet connected to DZ.
    #[arg(long)]
    include_not_on_dz: bool,

    /// Show a summary breakdown by client type instead of per-validator rows.
    #[arg(long)]
    summary: bool,

    /// Data API URL for validator metadata.
    #[arg(long, value_name = "URL", default_value = DEFAULT_DATA_API_URL)]
    data_api_url: String,

    #[command(flatten)]
    connection_options: SolanaConnectionOptions,

    #[command(flatten)]
    output: OutputOptions,
}

#[derive(Serialize, Tabled)]
struct ValidatorPublisherRow {
    #[tabled(rename = "OWNER")]
    owner: String,
    #[tabled(rename = "CLIENT IP")]
    client_ip: String,
    #[tabled(rename = "DEVICE")]
    device: String,
    #[tabled(rename = "VOTE ACCOUNT")]
    vote_account: String,
    #[tabled(rename = "STAKE (SOL)")]
    stake_sol: String,
    #[tabled(rename = "CLIENT")]
    client: String,
    #[tabled(rename = "VERSION")]
    version: String,
    #[tabled(rename = "PUB")]
    is_publisher: String,
}

#[derive(Serialize, Tabled)]
struct SummaryRow {
    #[tabled(rename = "CLIENT")]
    client: String,
    #[tabled(rename = "VALIDATORS")]
    validators: usize,
    #[tabled(rename = "ON DZ")]
    on_dz: usize,
    #[tabled(rename = "NOT ON DZ")]
    not_on_dz: usize,
    #[tabled(rename = "PUB")]
    publishers: usize,
    #[tabled(rename = "NOT PUB")]
    not_publishers: usize,
}

/// Filter parameters for the find command.
pub(crate) struct FindFilters {
    pub min_stake: Option<f64>,
    pub max_stake: Option<f64>,
    pub client: Option<String>,
    pub is_publisher: bool,
    pub not_publisher: bool,
}

/// Apply filters to a validator record.
pub(crate) fn apply_filters(filters: &FindFilters, val: &ValidatorRecord, is_pub: bool) -> bool {
    if let Some(min) = filters.min_stake
        && val.activated_stake_sol < min
    {
        return false;
    }
    if let Some(max) = filters.max_stake
        && val.activated_stake_sol > max
    {
        return false;
    }
    if let Some(ref client_filter) = filters.client
        && !val
            .software_client
            .to_lowercase()
            .contains(&client_filter.to_lowercase())
    {
        return false;
    }
    if filters.is_publisher && !is_pub {
        return false;
    }
    if filters.not_publisher && is_pub {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Command implementation
// ---------------------------------------------------------------------------

impl FindValidatorMulticastPublishersCommand {
    pub async fn try_execute(mut self) -> Result<()> {
        let connection_options = std::mem::take(&mut self.connection_options);
        let connection =
            doublezero_solana_client_tools::rpc::SolanaConnection::from(connection_options);

        let codes = dz_ledger_reader::fetch_codes_for_network(&connection)
            .await
            .ok();

        let validator_metadata = DataApiValidatorMetadataReader {
            api_url: self.data_api_url.clone(),
        };
        let dz_ledger = RpcDzLedgerReader::new(
            doublezero_solana_client_tools::rpc::SolanaConnection::new(connection.url()),
        );

        // Resolve multicast group filter (pubkey or code).
        let multicast_group_pk = match &self.multicast_group {
            Some(key_or_code) => {
                if let Ok(pk) = key_or_code.parse::<solana_pubkey::Pubkey>() {
                    Some(pk)
                } else {
                    let resolved = dz_ledger.resolve_multicast_group_code(key_or_code).await?;
                    match resolved {
                        Some(pk) => {
                            eprintln!("Resolved multicast group '{key_or_code}' -> {pk}");
                            Some(pk)
                        }
                        None => {
                            anyhow::bail!(
                                "Multicast group not found: {key_or_code} \
                                 (not a valid pubkey or known group code)"
                            );
                        }
                    }
                }
            }
            None => None,
        };

        // Derive the solana tenant PDA to scope user queries.
        let (serviceability_program_id, _) =
            dz_ledger_reader::resolve_dz_ledger_connection(&connection).await?;
        let (solana_tenant_pk, _) = get_tenant_pda(&serviceability_program_id, "solana");
        let default_tenant_pk = solana_pubkey::Pubkey::default();

        eprintln!("Fetching DZ Ledger users and validator metadata...");
        let (all_users_unfiltered, validators) = tokio::try_join!(
            dz_ledger.fetch_all_dz_users(),
            validator_metadata.fetch_validators(),
        )?;

        // Scope to solana tenant (or default/unset tenant).
        let all_users: Vec<_> = all_users_unfiltered
            .into_iter()
            .filter(|u| u.tenant_pk == solana_tenant_pk || u.tenant_pk == default_tenant_pk)
            .collect();

        let ibrl_users: Vec<_> = all_users
            .iter()
            .filter(|u| {
                u.user_type == UserType::IBRL || u.user_type == UserType::IBRLWithAllocatedIP
            })
            .collect();
        let ibrl_ips: std::collections::HashSet<Ipv4Addr> =
            ibrl_users.iter().map(|u| u.client_ip).collect();

        // Build per-IP set of multicast groups the IP publishes to.
        let mut publisher_groups_by_ip: HashMap<
            Ipv4Addr,
            std::collections::HashSet<solana_pubkey::Pubkey>,
        > = HashMap::new();
        for u in all_users
            .iter()
            .filter(|u| u.user_type == UserType::Multicast)
        {
            for pk in &u.publishers {
                publisher_groups_by_ip
                    .entry(u.client_ip)
                    .or_default()
                    .insert(*pk);
            }
        }

        // User type breakdown.
        let ibrl_count = all_users
            .iter()
            .filter(|u| u.user_type == UserType::IBRL)
            .count();
        let ibrl_ip_count = all_users
            .iter()
            .filter(|u| u.user_type == UserType::IBRLWithAllocatedIP)
            .count();
        let multicast_count = all_users
            .iter()
            .filter(|u| u.user_type == UserType::Multicast)
            .count();
        let edge_count = all_users
            .iter()
            .filter(|u| u.user_type == UserType::EdgeFiltering)
            .count();
        let other_count =
            all_users.len() - ibrl_count - ibrl_ip_count - multicast_count - edge_count;

        // "On DZ" = validator's gossip IP matches an IBRL user's client IP.
        let dz_validator_count = validators.keys().filter(|ip| ibrl_ips.contains(ip)).count();
        let not_dz_count = validators.len() - dz_validator_count;
        eprintln!(
            "User accounts: {} total ({} IBRL, {} IBRL+IP, {} Multicast, {} EdgeFiltering{})",
            all_users.len(),
            ibrl_count,
            ibrl_ip_count,
            multicast_count,
            edge_count,
            if other_count > 0 {
                format!(", {} other", other_count)
            } else {
                String::new()
            },
        );
        eprintln!(
            "IBRL users: {} | Validators: {} ({} on DZ, {} not on DZ)",
            ibrl_users.len(),
            validators.len(),
            dz_validator_count,
            not_dz_count,
        );

        let filters = FindFilters {
            min_stake: self.min_stake,
            max_stake: self.max_stake,
            client: self.client.clone(),
            is_publisher: self.is_publisher,
            not_publisher: self.not_publisher,
        };

        // Cross-reference IBRL users with validators by IP.
        let mut rows: Vec<ValidatorPublisherRow> = Vec::new();

        for user in &ibrl_users {
            if let Some(val) = validators.get(&user.client_ip) {
                let is_pub = publisher_groups_by_ip
                    .get(&user.client_ip)
                    .is_some_and(|groups| match &multicast_group_pk {
                        Some(group) => groups.contains(group),
                        None => !groups.is_empty(),
                    });

                if !apply_filters(&filters, val, is_pub) {
                    continue;
                }

                let device_label = codes
                    .as_ref()
                    .and_then(|c| c.device_codes.get(&user.device_pk).cloned())
                    .unwrap_or_else(|| user.device_pk.to_string());

                rows.push(ValidatorPublisherRow {
                    owner: user.owner.to_string(),
                    client_ip: user.client_ip.to_string(),
                    device: device_label,
                    vote_account: val.vote_account.clone(),
                    stake_sol: format!("{:.2}", val.activated_stake_sol),
                    client: val.software_client.clone(),
                    version: val.software_version.clone(),
                    is_publisher: if is_pub { "yes" } else { "no" }.to_string(),
                });
            }
        }

        // Include validators not on DZ for summary or when explicitly requested.
        if self.include_not_on_dz || self.summary {
            for val in validators.values() {
                if ibrl_ips.contains(&val.gossip_ip) {
                    continue; // already included above
                }

                if !apply_filters(&filters, val, false) {
                    continue;
                }

                rows.push(ValidatorPublisherRow {
                    owner: String::new(),
                    client_ip: val.gossip_ip.to_string(),
                    device: String::new(),
                    vote_account: val.vote_account.clone(),
                    stake_sol: format!("{:.2}", val.activated_stake_sol),
                    client: val.software_client.clone(),
                    version: val.software_version.clone(),
                    is_publisher: "no".to_string(),
                });
            }
        }

        // Sort by stake descending.
        rows.sort_by(|a, b| {
            let sa: f64 = a.stake_sol.parse().unwrap_or(0.0);
            let sb: f64 = b.stake_sol.parse().unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });

        if self.summary {
            // (count, on_dz, publishers)
            let mut by_client: HashMap<String, (usize, usize, usize)> = HashMap::new();
            for row in &rows {
                let entry = by_client.entry(row.client.clone()).or_insert((0, 0, 0));
                entry.0 += 1;
                if !row.owner.is_empty() {
                    entry.1 += 1; // on DZ
                }
                if row.is_publisher == "yes" {
                    entry.2 += 1;
                }
            }

            let total = rows.len();
            let total_on_dz = rows.iter().filter(|r| !r.owner.is_empty()).count();
            let total_pubs = rows.iter().filter(|r| r.is_publisher == "yes").count();

            let mut summary_rows: Vec<SummaryRow> = by_client
                .into_iter()
                .map(|(client, (count, on_dz, pubs))| SummaryRow {
                    client,
                    validators: count,
                    on_dz,
                    not_on_dz: count - on_dz,
                    publishers: pubs,
                    not_publishers: on_dz - pubs,
                })
                .collect();
            summary_rows.sort_by(|a, b| b.validators.cmp(&a.validators));
            summary_rows.push(SummaryRow {
                client: "TOTAL".to_string(),
                validators: total,
                on_dz: total_on_dz,
                not_on_dz: total - total_on_dz,
                publishers: total_pubs,
                not_publishers: total_on_dz - total_pubs,
            });
            print_table(summary_rows, &self.output, &[1, 2, 3, 4, 5]);
        } else {
            if rows.is_empty() {
                if self.output.json {
                    println!("[]");
                } else {
                    eprintln!("No IBRL validators found matching filters.");
                }
                return Ok(());
            }

            if !self.output.json {
                eprintln!("\nFound {} IBRL validator(s)\n", rows.len());
            }

            // right-align: STAKE (SOL) is column index 4
            print_table(rows, &self.output, &[4]);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    fn make_validator(ip: Ipv4Addr, stake: f64, client: &str) -> ValidatorRecord {
        ValidatorRecord {
            vote_account: String::new(),
            software_client: client.to_string(),
            software_version: String::new(),
            activated_stake_sol: stake,
            gossip_ip: ip,
        }
    }

    fn base_filters() -> FindFilters {
        FindFilters {
            min_stake: None,
            max_stake: None,
            client: None,
            is_publisher: false,
            not_publisher: false,
        }
    }

    #[test]
    fn filter_min_stake() {
        let val = make_validator(Ipv4Addr::new(1, 2, 3, 4), 500.0, "agave");
        let filters = FindFilters {
            min_stake: Some(1000.0),
            ..base_filters()
        };
        assert!(!apply_filters(&filters, &val, false));

        let filters = FindFilters {
            min_stake: Some(100.0),
            ..base_filters()
        };
        assert!(apply_filters(&filters, &val, false));
    }

    #[test]
    fn filter_max_stake() {
        let val = make_validator(Ipv4Addr::new(1, 2, 3, 4), 1500.0, "agave");
        let filters = FindFilters {
            max_stake: Some(1000.0),
            ..base_filters()
        };
        assert!(!apply_filters(&filters, &val, false));

        let filters = FindFilters {
            max_stake: Some(2000.0),
            ..base_filters()
        };
        assert!(apply_filters(&filters, &val, false));
    }

    #[test]
    fn filter_client_case_insensitive() {
        let val = make_validator(Ipv4Addr::new(1, 2, 3, 4), 1000.0, "Jito-Solana");

        let filters = FindFilters {
            client: Some("jito".to_string()),
            ..base_filters()
        };
        assert!(apply_filters(&filters, &val, false));

        let filters = FindFilters {
            client: Some("agave".to_string()),
            ..base_filters()
        };
        assert!(!apply_filters(&filters, &val, false));
    }

    #[test]
    fn filter_is_publisher() {
        let val = make_validator(Ipv4Addr::new(1, 2, 3, 4), 1000.0, "agave");

        let filters = FindFilters {
            is_publisher: true,
            ..base_filters()
        };
        assert!(!apply_filters(&filters, &val, false));
        assert!(apply_filters(&filters, &val, true));
    }

    #[test]
    fn filter_not_publisher() {
        let val = make_validator(Ipv4Addr::new(1, 2, 3, 4), 1000.0, "agave");

        let filters = FindFilters {
            not_publisher: true,
            ..base_filters()
        };
        assert!(apply_filters(&filters, &val, false));
        assert!(!apply_filters(&filters, &val, true));
    }

    #[test]
    fn combined_filters() {
        let val = make_validator(Ipv4Addr::new(1, 2, 3, 4), 1500.0, "Jito-Solana");

        // Passes all: stake in range, client matches, is publisher
        let filters = FindFilters {
            min_stake: Some(1000.0),
            max_stake: Some(2000.0),
            client: Some("jito".to_string()),
            is_publisher: true,
            not_publisher: false,
        };
        assert!(apply_filters(&filters, &val, true));

        // Fails: not a publisher but is_publisher required
        assert!(!apply_filters(&filters, &val, false));
    }
}
