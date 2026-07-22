use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use backon::{ExponentialBuilder, Retryable};
use doublezero_serviceability::state::{
    accesspass::AccessPass, accounttype::AccountType, contributor::Contributor, device::Device,
    exchange::Exchange, link::Link, location::Location, multicastgroup::MulticastGroup, user::User,
};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    client_error::ClientError as SolanaClientError,
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, info, warn};

use crate::{ingestor::types::DZServiceabilityData, settings::Settings};

/// Account types that we actually process in the rewards calculator
/// We ignore GlobalState, Config, ProgramConfig, and Contributor
const PROCESSED_ACCOUNT_TYPES: &[AccountType] = &[
    AccountType::Location,
    AccountType::Exchange,
    AccountType::Device,
    AccountType::Link,
    AccountType::User,
    AccountType::MulticastGroup,
    AccountType::Contributor,
    AccountType::AccessPass,
];

pub async fn fetch(rpc_client: &RpcClient, settings: &Settings) -> Result<DZServiceabilityData> {
    // NOTE: This fetches current serviceability state only
    // Historical state is not available as serviceability accounts
    // don't have timestamp/epoch fields and updates overwrite data.
    // This creates a temporal mismatch with historical telemetry data.
    let mut serviceability_data = DZServiceabilityData::default();
    let mut total_processed = 0;
    let mut total_errors = 0;
    let mut decode_errors = 0;

    // Fetch each account type separately with RPC filtering
    for account_type in PROCESSED_ACCOUNT_TYPES {
        match fetch_by_type(rpc_client, settings, *account_type).await {
            Err(e) => {
                warn!("Failed to fetch {} accounts: {}", account_type, e);
                total_errors += 1;
            }
            Ok(accounts) => {
                debug!("Processing {} {} accounts", accounts.len(), account_type);

                for (pubkey, account_data) in accounts {
                    if account_data.is_empty() {
                        continue;
                    }

                    // A decode failure on one account must degrade gracefully:
                    // warn, count it, and skip it — never fail the whole epoch
                    // snapshot (which the scheduler would then retry forever).
                    match decode_account(
                        &mut serviceability_data,
                        *account_type,
                        pubkey,
                        &account_data,
                    ) {
                        Ok(true) => total_processed += 1,
                        // Unexpected account type: already warned inside the
                        // dispatch, and neither processed nor a decode failure.
                        Ok(false) => {}
                        Err(e) => {
                            warn!(
                                "Failed to decode {} account {} ({} bytes), skipping: {}",
                                account_type,
                                pubkey,
                                account_data.len(),
                                e
                            );
                            decode_errors += 1;
                        }
                    }
                }
            }
        }
    }

    info!(
        "Processed {} serviceability accounts, contributors={}, locations={}, exchanges={}, devices={}, links={}, users={}, mcast_groups={}, access_passes={}. Errors={}, DecodeErrors={}",
        total_processed,
        serviceability_data.contributors.len(),
        serviceability_data.locations.len(),
        serviceability_data.exchanges.len(),
        serviceability_data.devices.len(),
        serviceability_data.links.len(),
        serviceability_data.users.len(),
        serviceability_data.multicast_groups.len(),
        serviceability_data.access_passes.len(),
        total_errors,
        decode_errors,
    );

    Ok(serviceability_data)
}

// Decode a single fetched account and insert it into `serviceability_data`.
// Returns whether an account was actually stored (`false` only for an
// unexpected account type, which is warned and neither processed nor treated
// as a decode failure). Kept separate from the RPC fetch so the per-account
// decode dispatch can be tested against real serialized bytes without an RPC
// round-trip.
fn decode_account(
    serviceability_data: &mut DZServiceabilityData,
    account_type: AccountType,
    pubkey: Pubkey,
    account_data: &[u8],
) -> Result<bool> {
    match account_type {
        AccountType::Location => {
            let location = Location::try_from(account_data)?;
            serviceability_data.locations.insert(pubkey, location);
        }
        AccountType::Exchange => {
            let exchange = Exchange::try_from(account_data)?;
            serviceability_data.exchanges.insert(pubkey, exchange);
        }
        AccountType::Device => {
            let device = Device::try_from(account_data)?;
            serviceability_data.devices.insert(pubkey, device);
        }
        AccountType::Link => {
            let link = Link::try_from(account_data)?;
            serviceability_data.links.insert(pubkey, link);
        }
        AccountType::User => {
            let user = User::try_from(account_data)?;
            serviceability_data.users.insert(pubkey, user);
        }
        AccountType::MulticastGroup => {
            let multicast_group = MulticastGroup::try_from(account_data)?;
            serviceability_data
                .multicast_groups
                .insert(pubkey, multicast_group);
        }
        AccountType::Contributor => {
            let contributor = Contributor::try_from(account_data)?;
            serviceability_data.contributors.insert(pubkey, contributor);
        }
        AccountType::AccessPass => {
            let access_pass = AccessPass::try_from(account_data)?;
            serviceability_data
                .access_passes
                .insert(pubkey, access_pass);
        }
        _ => {
            warn!(
                "Unexpected account type {:?} in processed list",
                account_type
            );
            return Ok(false);
        }
    }

    Ok(true)
}

/// Fetch serviceability data by account type using RPC filters
async fn fetch_by_type(
    rpc_client: &RpcClient,
    settings: &Settings,
    account_type: AccountType,
) -> Result<Vec<(Pubkey, Vec<u8>)>> {
    let program_id = &settings.programs.serviceability_program_id;
    let program_pubkey = Pubkey::from_str(program_id)
        .with_context(|| format!("Invalid serviceability program ID: {program_id}"))?;

    let filters = vec![RpcFilterType::Memcmp(Memcmp::new_base58_encoded(
        0,
        &[account_type as u8],
    ))];

    let config = RpcProgramAccountsConfig {
        filters: Some(filters),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            commitment: Some(CommitmentConfig::finalized()),
            ..RpcAccountInfoConfig::default()
        },
        ..RpcProgramAccountsConfig::default()
    };

    let start = Instant::now();
    let accounts = (|| async {
        rpc_client
            .get_program_accounts_with_config(&program_pubkey, config.clone())
            .await
    })
    .retry(&ExponentialBuilder::default().with_jitter())
    .notify(|err: &SolanaClientError, dur: Duration| {
        info!("retrying error: {:?} with sleeping {:?}", err, dur)
    })
    .await?;
    debug!(
        "Fetching serviceability account took: {:?}",
        start.elapsed()
    );

    debug!("Found {} {} accounts", accounts.len(), account_type);
    // Convert from Vec<(Pubkey, Account)> to Vec<(Pubkey, Vec<u8>)>
    let accounts_with_data: Vec<(Pubkey, Vec<u8>)> = accounts
        .into_iter()
        .map(|(pubkey, account)| (pubkey, account.data))
        .collect();

    Ok(accounts_with_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use doublezero_serviceability::state::location::LocationStatus;
    use solana_sdk::program_error::ProgramError;

    fn valid_location() -> Location {
        Location {
            account_type: AccountType::Location,
            owner: Pubkey::new_unique(),
            index: 1,
            bump_seed: 255,
            lat: 52.37,
            lng: 4.9,
            loc_id: 42,
            status: LocationStatus::Activated,
            code: "ams".to_string(),
            name: "Amsterdam".to_string(),
            country: "NL".to_string(),
            reference_count: 0,
        }
    }

    #[test]
    fn test_decode_account_valid_location() {
        let mut serviceability_data = DZServiceabilityData::default();
        let location = valid_location();
        let pubkey = Pubkey::new_unique();
        let account_data = borsh::to_vec(&location).unwrap();

        let stored = decode_account(
            &mut serviceability_data,
            AccountType::Location,
            pubkey,
            &account_data,
        )
        .unwrap();

        assert!(stored);
        assert_eq!(serviceability_data.locations.len(), 1);
        assert_eq!(serviceability_data.locations[&pubkey].code, "ams");
    }

    #[test]
    fn test_decode_account_corrupt_location_errors() {
        let mut serviceability_data = DZServiceabilityData::default();
        // The SDK parser defaults absent trailing fields, so tail-truncation of a
        // valid account still decodes; a leading discriminant that is not
        // `AccountType::Location` is what makes `Location::try_from` return Err.
        let corrupt_data = [0xFF, 0x00, 0x00];

        let error = decode_account(
            &mut serviceability_data,
            AccountType::Location,
            Pubkey::new_unique(),
            &corrupt_data,
        )
        .unwrap_err();

        // The only Err source in the SDK's `try_from` is the discriminant check.
        assert_eq!(
            error.downcast_ref::<ProgramError>(),
            Some(&ProgramError::InvalidAccountData)
        );
        assert!(serviceability_data.locations.is_empty());
    }

    // The test that would have caught the incident: a batch with one valid and one
    // undecodable account of the same type must keep the valid account, count one
    // decode error, and not fail the whole snapshot.
    #[test]
    fn test_decode_batch_skips_corrupt_account() {
        let mut serviceability_data = DZServiceabilityData::default();
        let valid_pubkey = Pubkey::new_unique();
        let batch = [
            (valid_pubkey, borsh::to_vec(&valid_location()).unwrap()),
            (Pubkey::new_unique(), vec![0xFF, 0x00, 0x00]),
        ];

        let mut decode_errors = 0;
        for (pubkey, account_data) in &batch {
            if decode_account(
                &mut serviceability_data,
                AccountType::Location,
                *pubkey,
                account_data,
            )
            .is_err()
            {
                decode_errors += 1;
            }
        }

        assert_eq!(decode_errors, 1);
        assert_eq!(serviceability_data.locations.len(), 1);
        assert!(serviceability_data.locations.contains_key(&valid_pubkey));
    }
}
