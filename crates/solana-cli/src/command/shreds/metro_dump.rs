use std::{
    collections::{HashMap, HashSet},
    io::Write,
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use clap::Args;
use doublezero_cli_core::CliContext;
use doublezero_serviceability::state::{accounttype::AccountType, exchange::Exchange};
use doublezero_solana_client_tools::rpc::SolanaConnectionOptions;
use doublezero_solana_sdk::shred_subscription::{self as shred_subscription, state};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::pubkey::Pubkey;
use tabled::{
    Table, Tabled,
    settings::{Remove, Style, location::ByColumnName},
};

use super::{make_dz_connection, serviceability_program_id};

/*
   doublezero-solana shreds metro-dump [--metro <PUBKEY_OR_CODE>]
                                       [--epoch <N> | --num-epochs <N>]
                                       [--wide] [--json]
*/

// Columns hidden unless `--wide` is given.
const WIDE_ONLY_COLUMNS: &[&str] = &[
    "Exchange Pubkey",
    "History PDA",
    "Bump",
    "Ring Index",
    "Ring Count",
];

#[derive(Debug, Args)]
pub struct MetroDumpCommand {
    /// Filter by metro, given as either the exchange public key or its
    /// human-readable code (e.g. "ams"). Omitted dumps every metro.
    #[arg(long, value_name = "PUBKEY_OR_CODE")]
    metro: Option<String>,

    /// Show only the price entry for this subscription epoch.
    #[arg(long, group = "epoch_filter")]
    epoch: Option<u64>,

    /// Show only the N most recent price entries per metro.
    #[arg(long, group = "epoch_filter", value_name = "N")]
    num_epochs: Option<usize>,

    /// Show public keys and ring buffer internals.
    #[arg(long)]
    wide: bool,

    #[arg(long)]
    json: bool,

    #[command(flatten)]
    connection_options: SolanaConnectionOptions,
}

#[derive(Debug, Tabled, serde::Serialize)]
struct MetroPriceRow {
    #[tabled(rename = "Metro Code")]
    metro_code: String,
    #[tabled(rename = "Metro Name")]
    metro_name: String,
    #[tabled(rename = "Epoch")]
    epoch: u64,
    #[tabled(rename = "Price (USDC)")]
    usdc_price_dollars: u16,
    // `is_current_price_finalized` describes the newest price only, so older
    // rows show "-" rather than implying the flag was set back then.
    #[tabled(rename = "Finalized")]
    finalized: String,
    #[tabled(rename = "Retransmit Only")]
    retransmit_only: String,
    #[tabled(rename = "Devices")]
    total_initialized_devices: u16,
    #[tabled(rename = "Exchange Pubkey")]
    exchange_key: String,
    #[tabled(rename = "History PDA")]
    metro_history_key: String,
    #[tabled(rename = "Bump")]
    bump_seed: u8,
    #[tabled(rename = "Ring Index")]
    ring_current_index: u8,
    #[tabled(rename = "Ring Count")]
    ring_total_count: u8,
}

impl MetroDumpCommand {
    pub async fn execute(
        self,
        dz_ledger_url: Option<String>,
        ctx: &CliContext,
        out: &mut impl Write,
    ) -> Result<()> {
        let connection = crate::command::solana_connection(ctx, &self.connection_options);
        let network_env =
            crate::command::resolve_network_env(&connection, self.connection_options.moniker_env())
                .await?;

        // Metro codes and names come from the Serviceability program on DZ
        // Ledger. Best effort: the substance of the dump lives on Solana, so an
        // unreachable ledger — or localnet, where `serviceability_program_id`
        // has nothing to point at — degrades those two columns to "?" instead of
        // failing the whole command. Resolving `--metro` by code is the one
        // exception; see below.
        let mut exchanges = HashMap::new();
        let mut exchange_lookup_error = None;
        match serviceability_program_id(network_env) {
            Err(error) => exchange_lookup_error = Some(error),
            Ok(program_id) => {
                let config = RpcProgramAccountsConfig {
                    filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                        0,
                        vec![AccountType::Exchange as u8],
                    ))]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                    ..Default::default()
                };
                let fetched = make_dz_connection(&dz_ledger_url, network_env)
                    .get_program_accounts_with_config(&program_id, config)
                    .await;
                match fetched {
                    Err(error) => exchange_lookup_error = Some(error.into()),
                    Ok(accounts) => {
                        for (key, account) in &accounts {
                            if let Ok(exchange) = Exchange::try_from(account.data.as_slice()) {
                                exchanges.insert(*key, exchange);
                            }
                        }
                    }
                }
            }
        }

        // A code can only be resolved through the exchange listing, so here a
        // lookup failure is fatal even though it is tolerated for display.
        let exchange_key = match self.metro.as_deref() {
            None => None,
            Some(metro) => match Pubkey::from_str(metro) {
                Ok(key) => Some(key),
                Err(_) => {
                    if let Some(error) = exchange_lookup_error.as_ref() {
                        bail!(
                            "Cannot resolve metro code \"{metro}\" without the DoubleZero Ledger \
                             exchange listing: {error:#}"
                        );
                    }
                    let matched = exchanges
                        .iter()
                        .filter(|(_, exchange)| exchange.code.eq_ignore_ascii_case(metro))
                        .map(|(key, _)| *key)
                        .collect::<Vec<_>>();
                    match matched.as_slice() {
                        [] => bail!("No metro found with code \"{metro}\""),
                        [key] => Some(*key),
                        keys => bail!(
                            "Ambiguous: {} metros found with code \"{metro}\"",
                            keys.len()
                        ),
                    }
                }
            },
        };

        let metro_history_accounts = match exchange_key {
            Some(exchange_key) => {
                // Deliberately not `try_fetch_multiple_accounts`: it maps a
                // missing account to `Account::default()`, so an absent PDA
                // would fall through to the parse and report as unparseable
                // rather than as not existing.
                let (metro_history_key, _) = state::find_metro_history_address(&exchange_key);
                let account = connection
                    .get_account(&metro_history_key)
                    .await
                    .with_context(|| {
                        format!(
                            "no metro history account at {metro_history_key} for exchange \
                             {exchange_key}"
                        )
                    })?;
                vec![(metro_history_key, account)]
            }
            None => {
                let discriminator_bytes = borsh::to_vec(&state::METRO_HISTORY_DISCRIMINATOR)
                    .expect("discriminator serialization");
                let config = RpcProgramAccountsConfig {
                    filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                        0,
                        discriminator_bytes,
                    ))]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                    ..Default::default()
                };
                connection
                    .get_program_accounts_with_config(&shred_subscription::ID, config)
                    .await
                    .context("failed to fetch MetroHistory accounts")?
            }
        };

        let mut rows = Vec::new();
        for (metro_history_key, account) in &metro_history_accounts {
            let Some(metro_history) = state::parse_metro_history_account(&account.data) else {
                writeln!(
                    out,
                    "warning: skipping unparseable account {metro_history_key}"
                )?;
                continue;
            };

            // `getProgramAccounts` returns the PDA but not its bump, so the bump
            // comes from re-deriving. Comparing the derivation against the key we
            // were handed is free and catches an `exchange_key` that does not
            // belong to this PDA.
            let (derived_key, bump_seed) =
                state::find_metro_history_address(&metro_history.exchange_key);
            if derived_key != *metro_history_key {
                writeln!(
                    out,
                    "warning: skipping {metro_history_key}: exchange key {} derives {derived_key}",
                    metro_history.exchange_key,
                )?;
                continue;
            }

            let exchange = exchanges.get(&metro_history.exchange_key);
            let metro_code = exchange
                .map(|exchange| exchange.code.clone())
                .unwrap_or_else(|| "?".to_string());
            let metro_name = exchange
                .map(|exchange| exchange.name.clone())
                .unwrap_or_else(|| "?".to_string());
            let retransmit_only = if metro_history.is_retransmit_only_enabled {
                "yes"
            } else {
                "no"
            };
            let newest_epoch = metro_history.price_entries.first().map(|entry| entry.epoch);

            let selected = metro_history
                .price_entries
                .iter()
                .filter(|entry| self.epoch.is_none_or(|epoch| entry.epoch == epoch))
                .take(self.num_epochs.unwrap_or(usize::MAX));

            for entry in selected {
                let finalized = if Some(entry.epoch) == newest_epoch {
                    if metro_history.is_current_price_finalized {
                        "yes"
                    } else {
                        "no"
                    }
                } else {
                    "-"
                };

                rows.push(MetroPriceRow {
                    metro_code: metro_code.clone(),
                    metro_name: metro_name.clone(),
                    epoch: entry.epoch,
                    usdc_price_dollars: entry.usdc_price_dollars,
                    finalized: finalized.to_string(),
                    retransmit_only: retransmit_only.to_string(),
                    total_initialized_devices: metro_history.total_initialized_devices,
                    exchange_key: metro_history.exchange_key.to_string(),
                    metro_history_key: metro_history_key.to_string(),
                    bump_seed,
                    ring_current_index: metro_history.ring_current_index,
                    ring_total_count: metro_history.ring_total_count,
                });
            }
        }

        if rows.is_empty() {
            if self.json {
                writeln!(out, "[]")?;
            } else {
                writeln!(out, "No metro price entries found.")?;
            }
            return Ok(());
        }

        // Codes are the primary sort key, but every metro shares the code "?"
        // when the exchange listing is unavailable, so the exchange key breaks
        // the tie to keep the ordering stable.
        rows.sort_by(|left, right| {
            left.metro_code
                .cmp(&right.metro_code)
                .then(left.exchange_key.cmp(&right.exchange_key))
                .then(right.epoch.cmp(&left.epoch))
        });

        if self.json {
            writeln!(out, "{}", serde_json::to_string_pretty(&rows)?)?;
            return Ok(());
        }

        if let Some(error) = exchange_lookup_error.as_ref() {
            writeln!(
                out,
                "warning: metro codes and names are unavailable: {error:#}\n"
            )?;
        }

        let metro_count = rows
            .iter()
            .map(|row| row.exchange_key.as_str())
            .collect::<HashSet<_>>()
            .len();
        writeln!(
            out,
            "{} price entries across {metro_count} metro(s):\n",
            rows.len(),
        )?;

        let mut table = Table::new(rows);
        if !self.wide {
            for column in WIDE_ONLY_COLUMNS {
                table.with(Remove::column(ByColumnName::new(*column)));
            }
        }
        table.with(Style::markdown());
        writeln!(out, "{table}")?;

        Ok(())
    }
}
