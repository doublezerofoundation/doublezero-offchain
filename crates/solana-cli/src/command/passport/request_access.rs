use std::{str::FromStr, sync::Arc};

use anyhow::{Result, bail};
use clap::Args;
use doublezero_ledger_sentinel::client::solana::SolRpcClient;
use doublezero_passport::{
    ID,
    instruction::{
        AccessMode, PassportInstructionData, SolanaValidatorAttestation,
        account::RequestAccessAccounts,
    },
    state::AccessRequest,
};
use doublezero_program_tools::instruction::try_build_instruction;
use doublezero_solana_client_tools::payer::{SolanaPayerOptions, TransactionOutcome, Wallet};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    offchain_message::OffchainMessage,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
};
use url::Url;

use super::access_validation::{should_continue_after_validation, validate_validator_access};
use crate::utils::identify_cluster;
/*
   doublezero-solana passport request-access --doublezero-address SSSS --primary-validator-id AAA --backup-validator-ids BBB,CCC --signature XXXXX
*/

#[derive(Debug, Args)]
pub struct RequestValidatorAccessCommand {
    /// The DoubleZero service key to request access from
    #[arg(long)]
    doublezero_address: Pubkey,
    /// The validator's node ID (identity pubkey)
    #[arg(long, value_name = "PUBKEY")]
    primary_validator_id: Pubkey,
    /// Optional backup validator IDs (identity pubkeys)
    #[arg(long, value_name = "PUBKEY,PUBKEY,PUBKEY", value_delimiter = ',')]
    backup_validator_ids: Vec<Pubkey>,
    /// Base58-encoded ed25519 signature of the access request message (service_key=AAA,backup_ids=BBBB,CCCC,DDDD)
    #[arg(long, short = 's', value_name = "BASE58_STRING")]
    signature: String,

    /// Continue and submit transaction even if validation fails
    #[arg(long, short = 'f', default_value_t = false)]
    force: bool,

    /// Offchain message version. ONLY 0 IS SUPPORTED.
    #[arg(long, value_name = "U8", default_value = "0")]
    message_version: u8,

    #[command(flatten)]
    solana_payer_options: SolanaPayerOptions,
}

impl RequestValidatorAccessCommand {
    pub async fn try_into_execute(self) -> Result<()> {
        let RequestValidatorAccessCommand {
            doublezero_address,
            primary_validator_id,
            backup_validator_ids,
            signature,
            force,
            message_version,
            solana_payer_options,
        } = self;

        let wallet = Wallet::try_from(solana_payer_options.clone())?;

        println!("DoubleZero Passport - Request Validator Access");

        let cluster = identify_cluster(&wallet.connection).await;
        println!("Connected to Solana: {:}", cluster);
        println!("\nDoubleZero Address: {doublezero_address}\n");

        let sol_client = SolRpcClient::new(
            Url::parse(&wallet.connection.url()).unwrap(),
            Arc::new(Keypair::new()),
        );

        let validation_errors = validate_validator_access(
            &wallet.connection,
            &sol_client,
            &primary_validator_id,
            &backup_validator_ids,
        )
        .await?;
        if !should_continue_after_validation(&validation_errors, force) {
            return Ok(());
        }

        let (address, _) = AccessRequest::find_address(&doublezero_address);

        let request_account = wallet.connection.get_account(&address).await;
        if request_account.is_ok() {
            bail!("Access request already exists: {address}");
        }

        let tx_sig = Self::request_access(
            &wallet,
            signature,
            doublezero_address,
            primary_validator_id,
            backup_validator_ids,
            message_version,
            solana_payer_options.signer_options.verbose,
        )
        .await?;

        if let TransactionOutcome::Executed(tx_sig) = tx_sig {
            println!("Request Solana validator access: {tx_sig}");

            wallet.print_verbose_output(&[tx_sig]).await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_access(
        wallet: &Wallet,
        signature: String,
        doublezero_address: Pubkey,
        primary_validator_id: Pubkey,
        backup_validator_ids: Vec<Pubkey>,
        message_version: u8,
        verbose: bool,
    ) -> Result<TransactionOutcome> {
        let ed25519_signature = Signature::from_str(&signature)?;
        let wallet_key = wallet.pubkey();

        // Create attestation
        let attestation = SolanaValidatorAttestation {
            validator_id: primary_validator_id,
            service_key: doublezero_address,
            ed25519_signature: ed25519_signature.into(),
        };

        // Verify the signature.
        let access_mode = if backup_validator_ids.is_empty() {
            AccessMode::SolanaValidator(attestation)
        } else {
            AccessMode::SolanaValidatorWithBackupIds {
                attestation,
                backup_ids: backup_validator_ids.clone(),
            }
        };

        let raw_message = AccessRequest::access_request_message(&access_mode);

        if verbose {
            println!("Raw message: {raw_message}");
        }

        let message = OffchainMessage::new(message_version, raw_message.as_bytes())?;
        let serialized_message = message.serialize()?;

        if !ed25519_signature.verify(primary_validator_id.as_array(), &serialized_message) {
            bail!("Signature verification failed");
        } else if verbose {
            println!("Signature recovers node ID: {}", primary_validator_id);
        }

        let request_access_ix = try_build_instruction(
            &ID,
            RequestAccessAccounts::new(&wallet_key, &doublezero_address),
            &PassportInstructionData::RequestAccess(access_mode),
        )?;

        let (_, bump) = AccessRequest::find_address(&doublezero_address);

        let mut compute_unit_limit = 10_000;
        compute_unit_limit += Wallet::compute_units_for_bump_seed(bump);

        let mut instructions = vec![
            request_access_ix,
            ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit),
        ];

        if let Some(ref compute_unit_price_ix) = wallet.compute_unit_price_ix {
            instructions.push(compute_unit_price_ix.clone());
        }

        let transaction = wallet.new_transaction(&instructions).await?;

        wallet.send_or_simulate_transaction(&transaction).await
    }
}

#[cfg(test)]
mod tests {
    use crate::command::passport::access_validation::should_continue_after_validation;

    #[test]
    fn request_access_aborts_when_validation_fails_without_force() {
        let errors = vec!["validation failed".to_string()];
        let mut request_invoked = false;

        if should_continue_after_validation(&errors, false) {
            request_invoked = true;
        }

        assert!(!request_invoked, "request should not run without --force");
    }

    #[test]
    fn request_access_proceeds_when_forced() {
        let errors = vec!["validation failed".to_string()];
        let mut request_invoked = false;

        if should_continue_after_validation(&errors, true) {
            request_invoked = true;
        }

        assert!(request_invoked, "request should run when --force is set");
    }
}
