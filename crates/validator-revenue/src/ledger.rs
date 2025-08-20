use anyhow::{Context, Result};
use doublezero_record::state::RecordData;
use doublezero_sdk::record::{self, client, state::read_record_data};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{commitment_config::CommitmentConfig, signer::Signer, signer::keypair::Keypair};

pub async fn write_record_to_ledger<T: borsh::BorshSerialize>(
    rpc_client: &RpcClient,
    payer_signer: &Keypair,
    record_data: &T,
    commitment_config: CommitmentConfig,
    seeds: &[&[u8]],
) -> Result<()> {
    let recent_blockhash = rpc_client.get_latest_blockhash().await?;
    let payer_key = payer_signer.pubkey();

    let serialized = borsh::to_vec(record_data)?;
    client::try_create_record(
        rpc_client,
        recent_blockhash,
        payer_signer,
        seeds,
        serialized.len(),
    )
    .await?;

    for chunk in record::instruction::write_record_chunks(&payer_key, seeds, &serialized) {
        chunk
            .into_send_transaction_with_config(
                rpc_client,
                recent_blockhash,
                payer_signer,
                true,
                RpcSendTransactionConfig {
                    preflight_commitment: Some(commitment_config.commitment),
                    ..Default::default()
                },
            )
            .await?;
    }

    Ok(())
}

pub async fn read_from_ledger(
    rpc_client: &RpcClient,
    payer_signer: &Keypair,
    seeds: &[&[u8]],
    commitment_config: CommitmentConfig,
) -> Result<(RecordData, Vec<u8>)> {
    let payer_key = payer_signer.pubkey();

    let record_key = record::pubkey::create_record_key(&payer_key, seeds);
    let get_account_response = rpc_client
        .get_account_with_commitment(&record_key, commitment_config)
        .await
        .with_context(|| format!("Failed to fetch account {record_key}"))?;

    let record_account_info = get_account_response
        .value
        .ok_or_else(|| anyhow::anyhow!("Record account not found at address {record_key}"))?;

    let (record_header, record_body) = read_record_data(&record_account_info.data)
        .with_context(|| format!("Failed to parse record data from account {record_key}"))?;

    Ok((*record_header, record_body.to_vec()))
}
