use std::{
    collections::{HashMap, HashSet},
    io::Write,
    net::Ipv4Addr,
};

use anyhow::{Context, Result, bail};
use clap::Args;
use doublezero_ledger_sentinel::client::{
    dz_ledger_reader::{self as dz_ledger_reader, DzLedgerReader, DzUser, RpcDzLedgerReader},
    dz_ledger_writer::build_create_multicast_publisher_instructions,
    validator_metadata_reader::{
        DataApiValidatorMetadataReader, ValidatorMetadataReader, ValidatorRecord,
    },
};
use doublezero_sdk::UserType;
use doublezero_solana_client_tools::payer::{SolanaPayerOptions, TransactionOutcome, Wallet};
use solana_pubkey::Pubkey;
use solana_sdk::instruction::Instruction;

const DEFAULT_DATA_API_URL: &str = "https://data.malbeclabs.com/api/sql/query";

/// Create multicast publisher users for IBRL validators that don't have one yet.
#[derive(Debug, Args)]
pub struct CreateValidatorMulticastPublishersCommand {
    /// Multicast group (pubkey or code, e.g. "edge-solana-shreds"). Required.
    #[arg(long, value_name = "KEY_OR_CODE")]
    multicast_group: String,

    /// Maximum number of users to create in this run.
    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    /// Minimum activated stake in SOL to include.
    #[arg(long, value_name = "SOL")]
    min_stake: Option<f64>,

    /// Maximum activated stake in SOL to include.
    #[arg(long, value_name = "SOL")]
    max_stake: Option<f64>,

    /// Filter by validator client name (e.g. "JitoLabs", "AgaveBam", "Frankendancer").
    #[arg(long, value_name = "NAME")]
    client: Option<String>,

    /// Data API URL for validator metadata.
    #[arg(long, value_name = "URL", default_value = DEFAULT_DATA_API_URL)]
    data_api_url: String,

    #[command(flatten)]
    solana_payer_options: SolanaPayerOptions,
}

/// A validator that needs a multicast publisher user created.
#[allow(dead_code)]
pub(crate) struct Candidate {
    pub owner: Pubkey,
    pub client_ip: Ipv4Addr,
    pub device_pk: Pubkey,
    pub vote_account: String,
    pub stake_sol: f64,
    pub software_client: String,
    pub device_label: String,
}

/// Filters for candidate selection.
pub(crate) struct CandidateFilters {
    pub min_stake: Option<f64>,
    pub max_stake: Option<f64>,
    pub client: Option<String>,
    pub limit: Option<usize>,
}

/// Find IBRL validators that need a multicast publisher created.
///
/// Pure function: no I/O, no network calls. Returns candidates sorted by stake
/// descending, with limit applied.
pub(crate) fn find_candidates(
    all_users: &[DzUser],
    validators: &HashMap<Ipv4Addr, ValidatorRecord>,
    multicast_group_pk: &Pubkey,
    filters: &CandidateFilters,
    device_labels: &HashMap<Pubkey, String>,
) -> Vec<Candidate> {
    let ibrl_users: Vec<_> = all_users
        .iter()
        .filter(|u| u.user_type == UserType::IBRL || u.user_type == UserType::IBRLWithAllocatedIP)
        .collect();

    // Build set of IPs that already publish to this group.
    let publisher_ips: HashSet<Ipv4Addr> = all_users
        .iter()
        .filter(|u| u.user_type == UserType::Multicast && u.publishers.contains(multicast_group_pk))
        .map(|u| u.client_ip)
        .collect();

    let mut candidates: Vec<Candidate> = Vec::new();
    for user in &ibrl_users {
        if publisher_ips.contains(&user.client_ip) {
            continue;
        }

        let Some(val) = validators.get(&user.client_ip) else {
            continue;
        };

        if let Some(min) = filters.min_stake
            && val.activated_stake_sol < min
        {
            continue;
        }
        if let Some(max) = filters.max_stake
            && val.activated_stake_sol > max
        {
            continue;
        }
        if let Some(ref client_filter) = filters.client
            && !val
                .software_client
                .to_lowercase()
                .contains(&client_filter.to_lowercase())
        {
            continue;
        }

        let device_label = device_labels
            .get(&user.device_pk)
            .cloned()
            .unwrap_or_else(|| user.device_pk.to_string());

        candidates.push(Candidate {
            owner: user.owner,
            client_ip: user.client_ip,
            device_pk: user.device_pk,
            vote_account: val.vote_account.clone(),
            stake_sol: val.activated_stake_sol,
            software_client: val.software_client.clone(),
            device_label,
        });
    }

    // Sort by stake descending.
    candidates.sort_by(|a, b| {
        b.stake_sol
            .partial_cmp(&a.stake_sol)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if let Some(limit) = filters.limit {
        candidates.truncate(limit);
    }

    candidates
}

impl CreateValidatorMulticastPublishersCommand {
    pub async fn try_execute(self) -> Result<()> {
        let wallet = Wallet::try_from(self.solana_payer_options)?;
        let payer = wallet.pubkey();

        let connection = &wallet.connection;
        let codes = dz_ledger_reader::fetch_codes_for_network(connection)
            .await
            .ok();

        // Detect network and resolve serviceability program ID.
        let (serviceability_program_id, _dz_connection) =
            dz_ledger_reader::resolve_dz_ledger_connection(connection).await?;

        let validator_metadata = DataApiValidatorMetadataReader {
            api_url: self.data_api_url.clone(),
        };
        let dz_ledger = RpcDzLedgerReader::new(
            doublezero_solana_client_tools::rpc::SolanaConnection::new(connection.url()),
        );

        // Resolve multicast group.
        let multicast_group_pk =
            dz_ledger_reader::resolve_multicast_group(&self.multicast_group, &dz_ledger).await?;
        eprintln!(
            "Multicast group: {} ({})",
            multicast_group_pk, self.multicast_group
        );

        // Fetch users and validator data.
        eprintln!("Fetching DZ Ledger users and validator metadata...");
        let (all_users, validators) = tokio::try_join!(
            dz_ledger.fetch_all_dz_users(),
            validator_metadata.fetch_validators(),
        )?;

        let device_labels: HashMap<Pubkey, String> = codes
            .as_ref()
            .map(|c| c.device_codes.clone())
            .unwrap_or_default();

        let filters = CandidateFilters {
            min_stake: self.min_stake,
            max_stake: self.max_stake,
            client: self.client,
            limit: self.limit,
        };

        let candidates = find_candidates(
            &all_users,
            &validators,
            &multicast_group_pk,
            &filters,
            &device_labels,
        );

        if candidates.is_empty() {
            eprintln!("No candidates found — all matching validators already have a publisher.");
            return Ok(());
        }

        // Display plan.
        eprintln!(
            "\nWill create {} multicast publisher user(s) on group {}:\n",
            candidates.len(),
            self.multicast_group,
        );
        eprintln!(
            "  {:<44}  {:<15}  {:<24}  {:<20}  {:>14}",
            "OWNER", "CLIENT IP", "DEVICE", "CLIENT", "STAKE (SOL)",
        );
        eprintln!("  {}", "-".repeat(125));
        for c in &candidates {
            eprintln!(
                "  {:<44}  {:<15}  {:<24}  {:<20}  {:>14.2}",
                c.owner, c.client_ip, c.device_label, c.software_client, c.stake_sol,
            );
        }
        eprintln!();

        if wallet.dry_run {
            eprintln!("Dry run — no transactions sent.");
            return Ok(());
        }

        // Confirmation prompt.
        eprint!("Proceed? [y/N] ");
        std::io::stderr().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();
        if input != "y" && input != "yes" {
            bail!("Aborted");
        }

        // Execute: for each candidate, run the 3-step creation flow.
        let program_id = serviceability_program_id;

        for (i, candidate) in candidates.iter().enumerate() {
            eprintln!(
                "\n[{}/{}] Creating multicast publisher for {} (ip: {}, device: {})...",
                i + 1,
                candidates.len(),
                candidate.owner,
                candidate.client_ip,
                candidate.device_label,
            );

            let dz_user = DzUser {
                owner: candidate.owner,
                client_ip: candidate.client_ip,
                device_pk: candidate.device_pk,
                tenant_pk: Pubkey::default(),
                user_type: doublezero_sdk::UserType::IBRL,
                publishers: vec![],
            };

            let ixs = build_create_multicast_publisher_instructions(
                &program_id,
                &payer,
                &multicast_group_pk,
                &dz_user,
            )?;

            send_instruction(&wallet, &[ixs.set_access_pass], "set_access_pass").await?;
            send_instruction(&wallet, &[ixs.add_allowlist], "add_pub_allowlist").await?;
            send_instruction(&wallet, &[ixs.create_user], "create_subscribe_user").await?;

            eprintln!(
                "  Created multicast publisher for {} on {}",
                candidate.client_ip, candidate.device_label,
            );
        }

        eprintln!(
            "\nDone — created {} multicast publisher(s).",
            candidates.len()
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn send_instruction(wallet: &Wallet, ixs: &[Instruction], label: &str) -> Result<()> {
    let tx = wallet.new_transaction(ixs).await?;
    let outcome = wallet
        .send_or_simulate_transaction(&tx)
        .await
        .with_context(|| format!("failed to send {label}"))?;

    if let TransactionOutcome::Executed(sig) = outcome {
        eprintln!("  {label}: {sig}");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use solana_pubkey::Pubkey;

    use super::*;

    fn make_ibrl_user(ip: [u8; 4], owner: Pubkey, device_pk: Pubkey) -> DzUser {
        DzUser {
            owner,
            client_ip: Ipv4Addr::from(ip),
            device_pk,
            tenant_pk: Pubkey::default(),
            user_type: UserType::IBRL,
            publishers: vec![],
        }
    }

    fn make_multicast_user(ip: [u8; 4], groups: Vec<Pubkey>) -> DzUser {
        DzUser {
            owner: Pubkey::new_unique(),
            client_ip: Ipv4Addr::from(ip),
            device_pk: Pubkey::new_unique(),
            tenant_pk: Pubkey::default(),
            user_type: UserType::Multicast,
            publishers: groups,
        }
    }

    fn make_validator(ip: Ipv4Addr, stake: f64, client: &str) -> ValidatorRecord {
        ValidatorRecord {
            vote_account: Pubkey::new_unique().to_string(),
            software_client: client.to_string(),
            software_version: "1.0.0".to_string(),
            activated_stake_sol: stake,
            gossip_ip: ip,
        }
    }

    fn no_filters() -> CandidateFilters {
        CandidateFilters {
            min_stake: None,
            max_stake: None,
            client: None,
            limit: None,
        }
    }

    #[test]
    fn no_validators_returns_empty() {
        let group = Pubkey::new_unique();
        let users = vec![make_ibrl_user(
            [10, 0, 0, 1],
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        )];
        let validators = HashMap::new();

        let result = find_candidates(&users, &validators, &group, &no_filters(), &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn no_ibrl_users_returns_empty() {
        let group = Pubkey::new_unique();
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        let users = vec![make_multicast_user([10, 0, 0, 1], vec![])];
        let mut validators = HashMap::new();
        validators.insert(ip, make_validator(ip, 1000.0, "agave"));

        let result = find_candidates(&users, &validators, &group, &no_filters(), &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn existing_publishers_are_skipped() {
        let group = Pubkey::new_unique();
        let ip = [10, 0, 0, 1];
        let ip_addr = Ipv4Addr::from(ip);

        let users = vec![
            make_ibrl_user(ip, Pubkey::new_unique(), Pubkey::new_unique()),
            make_multicast_user(ip, vec![group]),
        ];
        let mut validators = HashMap::new();
        validators.insert(ip_addr, make_validator(ip_addr, 1000.0, "agave"));

        let result = find_candidates(&users, &validators, &group, &no_filters(), &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn min_stake_filter() {
        let group = Pubkey::new_unique();
        let ip1 = Ipv4Addr::new(10, 0, 0, 1);
        let ip2 = Ipv4Addr::new(10, 0, 0, 2);

        let users = vec![
            make_ibrl_user([10, 0, 0, 1], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 2], Pubkey::new_unique(), Pubkey::new_unique()),
        ];
        let mut validators = HashMap::new();
        validators.insert(ip1, make_validator(ip1, 500.0, "agave"));
        validators.insert(ip2, make_validator(ip2, 1500.0, "agave"));

        let filters = CandidateFilters {
            min_stake: Some(1000.0),
            ..no_filters()
        };

        let result = find_candidates(&users, &validators, &group, &filters, &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].client_ip, ip2);
    }

    #[test]
    fn max_stake_filter() {
        let group = Pubkey::new_unique();
        let ip1 = Ipv4Addr::new(10, 0, 0, 1);
        let ip2 = Ipv4Addr::new(10, 0, 0, 2);

        let users = vec![
            make_ibrl_user([10, 0, 0, 1], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 2], Pubkey::new_unique(), Pubkey::new_unique()),
        ];
        let mut validators = HashMap::new();
        validators.insert(ip1, make_validator(ip1, 500.0, "agave"));
        validators.insert(ip2, make_validator(ip2, 1500.0, "agave"));

        let filters = CandidateFilters {
            max_stake: Some(1000.0),
            ..no_filters()
        };

        let result = find_candidates(&users, &validators, &group, &filters, &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].client_ip, ip1);
    }

    #[test]
    fn client_filter_case_insensitive() {
        let group = Pubkey::new_unique();
        let ip1 = Ipv4Addr::new(10, 0, 0, 1);
        let ip2 = Ipv4Addr::new(10, 0, 0, 2);

        let users = vec![
            make_ibrl_user([10, 0, 0, 1], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 2], Pubkey::new_unique(), Pubkey::new_unique()),
        ];
        let mut validators = HashMap::new();
        validators.insert(ip1, make_validator(ip1, 1000.0, "Jito-Solana"));
        validators.insert(ip2, make_validator(ip2, 1000.0, "Agave"));

        let filters = CandidateFilters {
            client: Some("jito".to_string()),
            ..no_filters()
        };

        let result = find_candidates(&users, &validators, &group, &filters, &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].client_ip, ip1);
    }

    #[test]
    fn sorted_by_stake_descending() {
        let group = Pubkey::new_unique();
        let ip1 = Ipv4Addr::new(10, 0, 0, 1);
        let ip2 = Ipv4Addr::new(10, 0, 0, 2);
        let ip3 = Ipv4Addr::new(10, 0, 0, 3);

        let users = vec![
            make_ibrl_user([10, 0, 0, 1], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 2], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 3], Pubkey::new_unique(), Pubkey::new_unique()),
        ];
        let mut validators = HashMap::new();
        validators.insert(ip1, make_validator(ip1, 500.0, "agave"));
        validators.insert(ip2, make_validator(ip2, 2000.0, "agave"));
        validators.insert(ip3, make_validator(ip3, 1000.0, "agave"));

        let result = find_candidates(&users, &validators, &group, &no_filters(), &HashMap::new());
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].client_ip, ip2); // 2000
        assert_eq!(result[1].client_ip, ip3); // 1000
        assert_eq!(result[2].client_ip, ip1); // 500
    }

    #[test]
    fn limit_applied_after_sort() {
        let group = Pubkey::new_unique();
        let ip1 = Ipv4Addr::new(10, 0, 0, 1);
        let ip2 = Ipv4Addr::new(10, 0, 0, 2);
        let ip3 = Ipv4Addr::new(10, 0, 0, 3);

        let users = vec![
            make_ibrl_user([10, 0, 0, 1], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 2], Pubkey::new_unique(), Pubkey::new_unique()),
            make_ibrl_user([10, 0, 0, 3], Pubkey::new_unique(), Pubkey::new_unique()),
        ];
        let mut validators = HashMap::new();
        validators.insert(ip1, make_validator(ip1, 500.0, "agave"));
        validators.insert(ip2, make_validator(ip2, 2000.0, "agave"));
        validators.insert(ip3, make_validator(ip3, 1000.0, "agave"));

        let filters = CandidateFilters {
            limit: Some(2),
            ..no_filters()
        };

        let result = find_candidates(&users, &validators, &group, &filters, &HashMap::new());
        assert_eq!(result.len(), 2);
        // Top 2 by stake: ip2 (2000) and ip3 (1000)
        assert_eq!(result[0].client_ip, ip2);
        assert_eq!(result[1].client_ip, ip3);
    }
}
