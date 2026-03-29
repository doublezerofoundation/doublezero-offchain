use std::{collections::HashMap, net::Ipv4Addr};

use anyhow::{Context, Result, bail};
use doublezero_sdk::{
    AccountData, AccountType, DeviceStatus, MulticastGroupStatus, UserStatus, UserType,
};
use doublezero_solana_client_tools::rpc::{NetworkEnvironment, SolanaConnection};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::pubkey::Pubkey;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAINNET_BETA_SERVICEABILITY_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("ser2VaTMAcYTaauMrTSfSrxBaUDq7BLNs2xfUugTAGv");
pub const TESTNET_SERVICEABILITY_PROGRAM_ID: Pubkey =
    solana_sdk::pubkey!("DZtnuQ839pSaDMFG5q1ad2V95G82S5EC4RrB3Ndw2Heb");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// DZ Ledger user info.
#[derive(Debug, Clone)]
pub struct DzUser {
    pub owner: Pubkey,
    pub client_ip: Ipv4Addr,
    pub device_pk: Pubkey,
    pub tenant_pk: Pubkey,
    pub user_type: UserType,
    pub publishers: Vec<Pubkey>,
}

/// Maps device pubkey → device code.
pub struct DzLedgerCodes {
    pub device_codes: HashMap<Pubkey, String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Client for querying DZ Ledger onchain state.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait DzLedgerReader: Send + Sync {
    /// Fetch all activated users from the DZ Ledger.
    async fn fetch_all_dz_users(&self) -> Result<Vec<DzUser>>;

    /// Resolve a multicast group code to its onchain pubkey.
    async fn resolve_multicast_group_code(&self, code: &str) -> Result<Option<Pubkey>>;
}

// ---------------------------------------------------------------------------
// RPC implementation
// ---------------------------------------------------------------------------

pub struct RpcDzLedgerReader {
    connection: SolanaConnection,
}

impl RpcDzLedgerReader {
    pub fn new(connection: SolanaConnection) -> Self {
        Self { connection }
    }
}

#[async_trait::async_trait]
impl DzLedgerReader for RpcDzLedgerReader {
    async fn fetch_all_dz_users(&self) -> Result<Vec<DzUser>> {
        let (program_id, dz_connection) = resolve_dz_ledger_connection(&self.connection).await?;

        let user_type_byte = AccountType::User as u8;
        let accounts = dz_connection
            .get_program_accounts_with_config(
                &program_id,
                RpcProgramAccountsConfig {
                    filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                        0,
                        vec![user_type_byte],
                    ))]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .context("failed to fetch User accounts from DZ Ledger")?;

        let mut users = Vec::new();
        for (_pk, account) in accounts {
            let Ok(ad) = AccountData::try_from(account.data.as_slice()) else {
                continue;
            };
            let Ok(user) = ad.get_user() else {
                continue;
            };
            if user.status != UserStatus::Activated {
                continue;
            }
            users.push(DzUser {
                owner: user.owner,
                client_ip: user.client_ip,
                device_pk: user.device_pk,
                tenant_pk: user.tenant_pk,
                user_type: user.user_type,
                publishers: user.publishers.clone(),
            });
        }

        Ok(users)
    }

    async fn resolve_multicast_group_code(&self, code: &str) -> Result<Option<Pubkey>> {
        let (program_id, dz_connection) = resolve_dz_ledger_connection(&self.connection).await?;

        let mgroup_type_byte = AccountType::MulticastGroup as u8;
        let accounts = dz_connection
            .get_program_accounts_with_config(
                &program_id,
                RpcProgramAccountsConfig {
                    filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                        0,
                        vec![mgroup_type_byte],
                    ))]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .context("failed to fetch MulticastGroup accounts from DZ Ledger")?;

        for (pk, account) in accounts {
            let Ok(ad) = AccountData::try_from(account.data.as_slice()) else {
                continue;
            };
            let Ok(group) = ad.get_multicastgroup() else {
                continue;
            };
            if group.code == code && group.status == MulticastGroupStatus::Activated {
                return Ok(Some(pk));
            }
        }

        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Detect the network and return the serviceability program ID and a connection
/// to the DZ Ledger.
pub async fn resolve_dz_ledger_connection(
    connection: &SolanaConnection,
) -> Result<(Pubkey, SolanaConnection)> {
    let is_testnet = connection
        .get_account(&TESTNET_SERVICEABILITY_PROGRAM_ID)
        .await
        .is_ok();

    if is_testnet {
        Ok((
            TESTNET_SERVICEABILITY_PROGRAM_ID,
            SolanaConnection::new(connection.url()),
        ))
    } else {
        let dz_url = NetworkEnvironment::PUBLIC_DOUBLEZERO_LEDGER_MAINNET_BETA_URL;
        Ok((
            MAINNET_BETA_SERVICEABILITY_PROGRAM_ID,
            SolanaConnection::new(dz_url.to_string()),
        ))
    }
}

/// Fetch device codes from the serviceability program.
pub async fn fetch_codes_for_network(connection: &SolanaConnection) -> Result<DzLedgerCodes> {
    let (program_id, dz_connection) = resolve_dz_ledger_connection(connection).await?;

    let device_accounts = dz_connection
        .get_program_accounts_with_config(
            &program_id,
            RpcProgramAccountsConfig {
                filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                    0,
                    vec![AccountType::Device as u8],
                ))]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .context("failed to fetch Device accounts from DZ Ledger")?;

    let mut device_codes = HashMap::new();
    for (pk, account) in device_accounts {
        let Ok(ad) = AccountData::try_from(account.data.as_slice()) else {
            continue;
        };
        let Ok(device) = ad.get_device() else {
            continue;
        };
        if device.status == DeviceStatus::Activated {
            device_codes.insert(pk, device.code.clone());
        }
    }

    Ok(DzLedgerCodes { device_codes })
}

/// Resolve a multicast group key-or-code to a pubkey.
pub async fn resolve_multicast_group(
    key_or_code: &str,
    dz_client: &dyn DzLedgerReader,
) -> Result<Pubkey> {
    if let Ok(pk) = key_or_code.parse::<Pubkey>() {
        return Ok(pk);
    }

    match dz_client.resolve_multicast_group_code(key_or_code).await? {
        Some(pk) => {
            eprintln!("Resolved multicast group '{key_or_code}' -> {pk}");
            Ok(pk)
        }
        None => {
            bail!(
                "Multicast group not found: {key_or_code} \
                 (not a valid pubkey or known group code)"
            );
        }
    }
}
