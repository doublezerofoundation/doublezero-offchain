use crate::{AccessIds, Error, Result, new_transaction};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STD};
use bincode;
use borsh::de::BorshDeserialize;
use doublezero_passport::{
    id as passport_id,
    instruction::{
        AccessMode, PassportInstructionData,
        account::{DenyAccessAccounts, GrantAccessAccounts},
    },
    state::AccessRequest,
};
use doublezero_program_tools::{
    PrecomputedDiscriminator, instruction::try_build_instruction, zero_copy,
};
use futures::{future::BoxFuture, stream::BoxStream};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{
        RpcAccountInfoConfig, RpcLeaderScheduleConfig, RpcProgramAccountsConfig,
        RpcTransactionConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter,
    },
    rpc_filter::{Memcmp, RpcFilterType},
    rpc_response::{Response, RpcLogsResponse},
};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::VersionedTransaction,
};
use solana_transaction_status_client_types::{
    EncodedTransaction, TransactionBinaryEncoding, UiTransactionEncoding,
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tracing::{debug, info, warn};
use url::Url;

pub struct SolRpcClient {
    client: RpcClient,
    payer: Arc<Keypair>,
}

impl SolRpcClient {
    pub fn new(rpc_url: Url, payer: Arc<Keypair>) -> Self {
        Self {
            client: RpcClient::new_with_commitment(rpc_url.into(), CommitmentConfig::confirmed()),
            payer,
        }
    }

    pub async fn grant_access(
        &self,
        access_request_key: &Pubkey,
        rent_beneficiary_key: &Pubkey,
    ) -> Result<Signature> {
        let signer = &self.payer;
        let grant_ix = try_build_instruction(
            &passport_id(),
            GrantAccessAccounts::new(&signer.pubkey(), access_request_key, rent_beneficiary_key),
            &PassportInstructionData::GrantAccess,
        )?;

        let recent_blockhash = self.client.get_latest_blockhash().await?;

        let transaction = new_transaction(&[grant_ix], &[signer], recent_blockhash);

        Ok(self
            .client
            .send_and_confirm_transaction(&transaction)
            .await?)
    }

    pub async fn deny_access(&self, access_request_key: &Pubkey) -> Result<Signature> {
        let signer = &self.payer;
        let deny_ix = try_build_instruction(
            &passport_id(),
            DenyAccessAccounts::new(&signer.pubkey(), access_request_key),
            &PassportInstructionData::DenyAccess,
        )?;

        let recent_blockhash = self.client.get_latest_blockhash().await?;

        let transaction = new_transaction(&[deny_ix], &[signer], recent_blockhash);

        Ok(self
            .client
            .send_and_confirm_transaction(&transaction)
            .await?)
    }

    pub async fn get_access_request_from_signature(
        &self,
        signature: Signature,
    ) -> Result<AccessIds> {
        // Get the transaction to find the AccessRequest account pubkey
        let txn = self
            .client
            .get_transaction_with_config(
                &signature,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: Some(CommitmentConfig {
                        commitment: CommitmentLevel::Confirmed,
                    }),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await?;

        // Extract the AccessRequest account pubkey from the transaction
        let request_pda =
            if let EncodedTransaction::Binary(data, TransactionBinaryEncoding::Base64) =
                txn.transaction.transaction
            {
                let data: &[u8] = &BASE64_STD.decode(data)?;
                let tx: VersionedTransaction = bincode::deserialize(data)?;

                // Find the passport instruction
                let compiled_ix = tx
                    .message
                    .instructions()
                    .iter()
                    .find(|ix| ix.program_id(tx.message.static_account_keys()) == &passport_id())
                    .ok_or(Error::InstructionNotFound(signature))?;

                // Get the AccessRequest account (index 2)
                let accounts = compiled_ix
                    .accounts
                    .iter()
                    .map(|&idx| tx.message.static_account_keys().get(idx as usize).copied())
                    .collect::<Option<Vec<_>>>()
                    .ok_or(Error::MissingAccountKeys(signature))?;

                accounts
                    .get(2)
                    .copied()
                    .ok_or(Error::InstructionInvalid(signature))?
            } else {
                return Err(Error::TransactionEncoding(signature));
            };

        // Fetch the AccessRequest account data
        let account = self.client.get_account(&request_pda).await?;

        // Deserialize the AccessRequest and extract the AccessMode
        deserialize_access_request_from_account(&request_pda, &account.data)
    }

    pub async fn get_access_requests(&self) -> Result<Vec<AccessIds>> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                0,
                AccessRequest::discriminator_slice().to_vec(),
            ))]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self
            .client
            .get_program_accounts_with_config(&passport_id(), config)
            .await?;

        let mut access_ids = Vec::new();
        let mut legacy_count = 0;
        let mut new_format_count = 0;
        let mut failed_count = 0;

        for (pubkey, account) in accounts {
            match deserialize_access_request_from_account(&pubkey, &account.data) {
                Ok(ids) => {
                    new_format_count += 1;
                    access_ids.push(ids);
                }
                Err(Error::LegacyFormat(account_pubkey)) => {
                    // Legacy account - need to fetch transaction to get AccessMode
                    legacy_count += 1;
                    match self.get_legacy_access_request(account_pubkey).await {
                        Ok(ids) => access_ids.push(ids),
                        Err(e) => {
                            warn!(
                                account = %account_pubkey,
                                error = ?e,
                                "Failed to process legacy AccessRequest account"
                            );
                            failed_count += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        account = %pubkey,
                        error = ?e,
                        "Failed to deserialize AccessRequest account"
                    );
                    failed_count += 1;
                }
            }
        }

        if legacy_count > 0 || failed_count > 0 {
            info!(
                new_format = new_format_count,
                legacy = legacy_count,
                failed = failed_count,
                "Processed AccessRequest accounts with mixed formats"
            );
        }

        Ok(access_ids)
    }

    /// Fallback function to handle legacy AccessRequest accounts that don't have
    /// encoded_access_mode populated. This fetches the creation transaction and
    /// extracts the AccessMode from instruction data.
    async fn get_legacy_access_request(&self, request_pda: Pubkey) -> Result<AccessIds> {
        debug!(
            account = %request_pda,
            "Fetching transaction history for legacy AccessRequest"
        );

        // Get the creation transaction signature
        let signatures = self.client.get_signatures_for_address(&request_pda).await?;

        let creation_signature: Signature = signatures
            .first()
            .ok_or(Error::MissingTxnSignature)
            .and_then(|sig| sig.signature.parse().map_err(Error::from))?;

        debug!(
            account = %request_pda,
            signature = %creation_signature,
            "Found creation transaction for legacy AccessRequest"
        );

        // Parse the transaction to get the AccessMode
        let txn = self
            .client
            .get_transaction_with_config(
                &creation_signature,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: Some(CommitmentConfig {
                        commitment: CommitmentLevel::Confirmed,
                    }),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await?;

        if let EncodedTransaction::Binary(data, TransactionBinaryEncoding::Base64) =
            txn.transaction.transaction
        {
            let data: &[u8] = &BASE64_STD.decode(data)?;
            let tx: VersionedTransaction = bincode::deserialize(data)?;

            deserialize_legacy_access_request_ids(tx, request_pda)
        } else {
            Err(Error::TransactionEncoding(creation_signature))
        }
    }

    pub async fn check_leader_schedule(
        &self,
        validator_id: &Pubkey,
        previous_leader_epochs: u8,
    ) -> Result<bool> {
        let latest_slot = self.client.get_slot().await?;

        for slot in PreviousEpochSlots::new(latest_slot).take(previous_leader_epochs as usize) {
            let config = RpcLeaderScheduleConfig {
                identity: Some(validator_id.to_string()),
                ..Default::default()
            };

            if !self
                .client
                .get_leader_schedule_with_config(Some(slot), config)
                .await?
                .is_some_and(|schedule| schedule.is_empty())
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn get_validator_ip(&self, validator_id: &Pubkey) -> Result<Option<Ipv4Addr>> {
        let address = self
            .client
            .get_cluster_nodes()
            .await?
            .iter()
            .find(|contact| contact.pubkey == validator_id.to_string())
            .and_then(|contact| contact.gossip)
            .and_then(|addr| match addr {
                SocketAddr::V4(addr_v4) => Some(*addr_v4.ip()),
                SocketAddr::V6(addr_v6) => addr_v6.ip().to_ipv4_mapped(),
            });
        Ok(address)
    }
}

pub struct SolPubsubClient {
    client: PubsubClient,
}

impl SolPubsubClient {
    pub async fn new(ws_url: Url) -> Result<Self> {
        let client = PubsubClient::new(ws_url.as_ref()).await?;

        Ok(Self { client })
    }

    pub async fn subscribe_to_access_requests(
        &self,
    ) -> Result<(
        BoxStream<'_, Response<RpcLogsResponse>>,
        Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>,
    )> {
        let config = RpcTransactionLogsConfig {
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let filter = RpcTransactionLogsFilter::Mentions(vec![passport_id().to_string()]);

        Ok(self.client.logs_subscribe(filter, config).await?)
    }
}

/// Helper function to deserialize legacy AccessRequest from transaction instruction data.
/// This is only used as a fallback for accounts created before the encoded_access_mode field was added.
fn deserialize_legacy_access_request_ids(
    txn: VersionedTransaction,
    request_pda: Pubkey,
) -> Result<AccessIds> {
    let signature = txn.signatures.first().ok_or(Error::MissingTxnSignature)?;
    let compiled_ix = txn
        .message
        .instructions()
        .iter()
        .find(|ix| ix.program_id(txn.message.static_account_keys()) == &passport_id())
        .ok_or(Error::InstructionNotFound(*signature))?;

    let accounts = compiled_ix
        .accounts
        .iter()
        .map(|&idx| txn.message.static_account_keys().get(idx as usize).copied())
        .collect::<Option<Vec<_>>>()
        .ok_or(Error::MissingAccountKeys(*signature))?;

    // Deserialize the AccessMode from instruction data
    let Ok(PassportInstructionData::RequestAccess(mode)) =
        PassportInstructionData::try_from_slice(&compiled_ix.data)
    else {
        return Err(Error::InstructionInvalid(*signature));
    };

    // Get the rent beneficiary (payer) from the accounts
    let rent_beneficiary_key = accounts
        .get(1)
        .copied()
        .ok_or(Error::InstructionInvalid(*signature))?;

    info!(
        account = %request_pda,
        "Successfully processed legacy AccessRequest using transaction data"
    );
    metrics::counter!("doublezero_sentinel_legacy_processed_via_txn").increment(1);

    Ok(AccessIds {
        request_pda,
        rent_beneficiary_key,
        mode,
    })
}

/// Helper function to deserialize AccessMode from AccessRequest account data.
/// Handles both new format (with encoded_access_mode) and legacy format.
fn deserialize_access_request_from_account(
    request_pda: &Pubkey,
    account_data: &[u8],
) -> Result<AccessIds> {
    // Parse the AccessRequest structure using zero_copy
    let (access_request, _) =
        zero_copy::checked_from_bytes_with_discriminator::<AccessRequest>(account_data)
            .ok_or_else(|| Error::Deserialize("Failed to deserialize AccessRequest".to_string()))?;

    // Check if encoded_access_mode is populated (new format)
    // Legacy accounts will have all zeros in this field
    let is_legacy = access_request
        .encoded_access_mode
        .iter()
        .all(|&byte| byte == 0);

    if is_legacy {
        // Legacy format detected - cannot process without transaction data
        debug!(
            account = %request_pda,
            "Legacy AccessRequest format detected - missing encoded_access_mode"
        );
        metrics::counter!("doublezero_sentinel_legacy_access_request").increment(1);
        return Err(Error::LegacyFormat(*request_pda));
    }

    // New format - deserialize the encoded_access_mode field
    let access_mode =
        AccessMode::try_from_slice(&access_request.encoded_access_mode).map_err(|e| {
            warn!(
                account = %request_pda,
                error = %e,
                "Failed to deserialize AccessMode from encoded_access_mode"
            );
            metrics::counter!("doublezero_sentinel_access_mode_deserialize_failed").increment(1);
            Error::Deserialize(format!("Failed to deserialize AccessMode: {e}"))
        })?;

    debug!(
        account = %request_pda,
        "Successfully deserialized AccessRequest with new format"
    );
    metrics::counter!("doublezero_sentinel_new_format_access_request").increment(1);

    Ok(AccessIds {
        request_pda: *request_pda,
        rent_beneficiary_key: access_request.rent_beneficiary_key,
        mode: access_mode,
    })
}

pub struct PreviousEpochSlots {
    current: u64,
    step: u64,
}

impl PreviousEpochSlots {
    // Number of slots per epoch
    const SLOTS_PER_EPOCH: u64 = 432_000;

    pub fn new(start: u64) -> Self {
        Self {
            current: start,
            step: Self::SLOTS_PER_EPOCH,
        }
    }
}

impl Iterator for PreviousEpochSlots {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.current;
        if self.current < self.step {
            return None;
        }
        self.current -= self.step;
        Some(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_reverse_iter() {
        let start_slot = 2_000_000;
        let num_epochs = 4;
        let epoch_slots = PreviousEpochSlots::new(start_slot)
            .take(num_epochs)
            .collect::<Vec<_>>();
        assert_eq!(epoch_slots.len(), 4);
        assert_eq!(epoch_slots.first().unwrap(), &start_slot);
        assert_eq!(
            epoch_slots.last().unwrap(),
            &(start_slot - 3 * PreviousEpochSlots::SLOTS_PER_EPOCH),
        );
    }
}
