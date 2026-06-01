use std::io::Write;

use clap::Args;
use doublezero_cli_core::CliContext;
use doublezero_solana_client_tools::rpc::SolanaConnection;
use doublezero_solana_sdk::passport::{
    instruction::AccessMode,
    state::{AccessRequest, ProgramConfig},
};
use serde::Serialize;
use solana_sdk::pubkey::Pubkey;

use crate::output::{emit_json, is_json, resolve_format};

#[derive(Debug, Args)]
pub struct FetchArgs {
    #[arg(long)]
    pub config: bool,

    #[arg(long, value_name = "DOUBLEZERO_PUBKEY")]
    pub access_request: Option<Pubkey>,

    /// Output as pretty JSON
    #[arg(long, default_value_t = false, conflicts_with = "json_compact")]
    pub json: bool,
    /// Output as single-line JSON suitable for piping
    #[arg(long = "json-compact", default_value_t = false, conflicts_with = "json")]
    pub json_compact: bool,
}

#[derive(Serialize)]
struct ProgramConfigView {
    program_config: String,
    is_paused: bool,
    is_request_access_paused: bool,
    admin_key: String,
    sentinel_key: String,
    request_deposit_sol: f64,
    request_fee_sol: f64,
    solana_validator_backup_ids_limit: u64,
}

#[derive(Serialize)]
struct AccessRequestView {
    access_request: String,
    service_key: String,
    rent_beneficiary_key: String,
    request_fee_sol: f64,
    access_mode: String,
}

impl FetchArgs {
    pub async fn execute(self, ctx: &CliContext, out: &mut impl Write) -> eyre::Result<()> {
        tracing::debug!(env = %ctx.env, "passport fetch");

        let connection = SolanaConnection::new(ctx.solana_l1_rpc_url.clone());
        let format = resolve_format(self.json, self.json_compact, ctx.output_format);
        let emit_as_json = is_json(format);

        if self.config {
            let (program_config_key, program_config) = fetch_program_config(&connection)
                .await
                .map_err(|e| eyre::eyre!("{e:#}"))?;

            if emit_as_json {
                let view = ProgramConfigView {
                    program_config: program_config_key.to_string(),
                    is_paused: program_config.is_paused(),
                    is_request_access_paused: program_config.is_request_access_paused(),
                    admin_key: program_config.admin_key.to_string(),
                    sentinel_key: program_config.sentinel_key.to_string(),
                    request_deposit_sol: program_config.request_deposit_lamports as f64 * 1e-9,
                    request_fee_sol: program_config.request_fee_lamports as f64 * 1e-9,
                    solana_validator_backup_ids_limit: program_config
                        .solana_validator_backup_ids_limit
                        as u64,
                };
                emit_json(out, &view, format)?;
            } else {
                writeln!(out, "Program config: {program_config_key}")?;
                writeln!(out)?;
                writeln!(out, "Parameter                         | Value")?;
                writeln!(
                    out,
                    "----------------------------------+-------------------------------------------------"
                )?;
                writeln!(out, "Is program paused?                | {}", program_config.is_paused())?;
                writeln!(
                    out,
                    "Is request access paused?         | {}",
                    program_config.is_request_access_paused()
                )?;
                writeln!(out, "Admin key                         | {}", program_config.admin_key)?;
                writeln!(out, "Sentinel key                      | {}", program_config.sentinel_key)?;
                writeln!(
                    out,
                    "Request deposit                   | {:.9} SOL",
                    program_config.request_deposit_lamports as f64 * 1e-9
                )?;
                writeln!(
                    out,
                    "Request fee                       | {:.9} SOL",
                    program_config.request_fee_lamports as f64 * 1e-9
                )?;
                writeln!(
                    out,
                    "Solana validator backup IDs limit | {}",
                    program_config.solana_validator_backup_ids_limit
                )?;
                writeln!(out)?;
            }
        }

        // NOTE: If an access request is found, the sentinel is not doing its job.
        if let Some(access_request) = self.access_request {
            let (access_request_key, access_request) =
                fetch_access_request(&connection, &access_request)
                    .await
                    .map_err(|e| eyre::eyre!("{e:#}"))?;

            let access_mode_str = match access_request.checked_access_mode() {
                Some(AccessMode::SolanaValidator(_)) => "Solana validator",
                Some(AccessMode::SolanaValidatorWithBackupIds { .. }) => {
                    "Solana validator with backup IDs"
                }
                None => "Unknown",
            };

            if emit_as_json {
                let view = AccessRequestView {
                    access_request: access_request_key.to_string(),
                    service_key: access_request.service_key.to_string(),
                    rent_beneficiary_key: access_request.rent_beneficiary_key.to_string(),
                    request_fee_sol: access_request.request_fee_lamports as f64 * 1e-9,
                    access_mode: access_mode_str.to_string(),
                };
                emit_json(out, &view, format)?;
            } else {
                writeln!(out, "Access request: {access_request_key}")?;
                writeln!(out)?;
                writeln!(out, "Field                | Value")?;
                writeln!(out, "---------------------+-------------------------------------------------")?;
                writeln!(out, "Service key          | {}", access_request.service_key)?;
                writeln!(
                    out,
                    "Rent beneficiary key | {}",
                    access_request.rent_beneficiary_key
                )?;
                writeln!(
                    out,
                    "Request fee          | {:.9} SOL",
                    access_request.request_fee_lamports as f64 * 1e-9
                )?;
                writeln!(out, "Access mode          | {access_mode_str}")?;
                writeln!(out)?;
            }
        }

        Ok(())
    }
}

async fn fetch_program_config(
    connection: &SolanaConnection,
) -> anyhow::Result<(Pubkey, ProgramConfig)> {
    let (program_config_key, _) = ProgramConfig::find_address();

    let program_config = connection
        .try_fetch_zero_copy_data(&program_config_key)
        .await?;
    Ok((program_config_key, *program_config))
}

async fn fetch_access_request(
    connection: &SolanaConnection,
    service_key: &Pubkey,
) -> anyhow::Result<(Pubkey, AccessRequest)> {
    use anyhow::Context;

    let (access_request_key, _) = AccessRequest::find_address(service_key);

    let access_request = connection
        .try_fetch_zero_copy_data(&access_request_key)
        .await
        .with_context(|| format!("Access request not found for service key {service_key}"))?;

    Ok((access_request_key, *access_request.mucked_data))
}
