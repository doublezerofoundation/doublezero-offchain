use anyhow::{Result, anyhow, bail};
use clap::{Args, Subcommand};
use doublezero_passport::{
    ID,
    instruction::{AccessMode, PassportInstructionData, account::RequestAccessAccounts},
    state::{AccessRequest, ProgramConfig},
};
use doublezero_program_tools::{instruction::try_build_instruction, zero_copy};
use solana_sdk::{compute_budget::ComputeBudgetInstruction, pubkey::Pubkey, signature::Signature};

use crate::{
    payer::{SolanaPayerOptions, Wallet},
    rpc::{Connection, SolanaConnectionOptions},
};

#[derive(Debug, Args)]
pub struct PassportCliCommand {
    #[command(subcommand)]
    pub command: PassportSubCommand,
}

#[derive(Debug, Subcommand)]
pub enum PassportSubCommand {
    Fetch {
        #[arg(long)]
        program_config: bool,

        #[command(flatten)]
        solana_connection_options: SolanaConnectionOptions,
    },

    RequestSolanaValidatorAccess {
        service_key: Pubkey,

        #[arg(long, value_name = "PUBKEY")]
        node_id: Pubkey,

        #[arg(long, short = 's', value_name = "BASE58_STRING")]
        signature: String,

        #[command(flatten)]
        solana_payer_options: SolanaPayerOptions,
    },
}

impl PassportSubCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        match self {
            PassportSubCommand::Fetch {
                program_config,
                solana_connection_options,
            } => execute_fetch(program_config, solana_connection_options).await,
            PassportSubCommand::RequestSolanaValidatorAccess {
                service_key,
                node_id,
                signature,
                solana_payer_options,
            } => {
                execute_request_solana_validator_access(
                    service_key,
                    node_id,
                    signature,
                    solana_payer_options,
                )
                .await
            }
        }
    }
}

//
// PassportSubCommand::Fetch.
//

async fn execute_fetch(
    program_config: bool,
    solana_connection_options: SolanaConnectionOptions,
) -> Result<()> {
    let connection = Connection::try_from(solana_connection_options)?;

    if program_config {
        let program_config_key = ProgramConfig::find_address().0;
        let program_config_info = connection.get_account(&program_config_key).await?;

        let (program_config, _) =
            zero_copy::checked_from_bytes_with_discriminator::<ProgramConfig>(
                &program_config_info.data,
            )
            .ok_or(anyhow!("Failed to deserialize program config"))?;

        // TODO: Pretty print.
        println!("Program config: {program_config:?}");
    }

    Ok(())
}

//
// PassportSubCommand::RequestSolanaValidatorAccess.
//

async fn execute_request_solana_validator_access(
    service_key: Pubkey,
    node_id: Pubkey,
    signature: String,
    solana_payer_options: SolanaPayerOptions,
) -> Result<()> {
    let wallet = Wallet::try_from(solana_payer_options)?;
    let wallet_key = wallet.pubkey();

    let ed25519_signature = Signature::try_from(signature.as_bytes())?;

    // Verify the signature.
    let message = AccessRequest::access_request_message(&service_key);

    if !ed25519_signature.verify(node_id.as_array(), message.as_bytes()) {
        bail!("Signature verification failed");
    }

    let request_access_ix = try_build_instruction(
        &ID,
        RequestAccessAccounts::new(&wallet_key, &service_key),
        &PassportInstructionData::RequestAccess(AccessMode::SolanaValidator {
            validator_id: node_id,
            service_key,
            ed25519_signature: ed25519_signature.into(),
        }),
    )?;

    let mut compute_unit_limit = 10_000;

    let (_, bump) = AccessRequest::find_address(&service_key);
    compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

    let mut instructions = vec![
        request_access_ix,
        ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
    ];

    if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
        instructions.push(compute_unit_price_ix.clone());
    }

    let transaction = wallet.new_transaction(&instructions).await?;
    let tx_sig = wallet.send_or_simulate_transaction(&transaction).await?;

    if let Some(tx_sig) = tx_sig {
        println!("Request Solana validator access: {tx_sig}");

        wallet.print_verbose_output(&[tx_sig]).await?;
    }

    Ok(())
}
