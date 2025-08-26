#![allow(unexpected_cfgs)]
#![cfg(local_validator_test)]
// #[cfg(test)]
// mod tests {
use anyhow::Result;
use validator_revenue::{
    fee_payment_calculator::FeePaymentCalculator,
    ledger::{read_from_ledger, write_record_to_ledger},
    rewards::{EpochRewards, Reward},
    worker::write_payments,
};

use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcGetVoteAccountsConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, signer::Signer, signer::keypair::Keypair,
};

use solana_transaction_status_client_types::{TransactionDetails, UiTransactionEncoding};
use std::{dbg, str::FromStr, time::Duration};

#[tokio::test]
async fn test_write_to_read_from_ledger() -> anyhow::Result<()> {
    let validator_id = "devgM7SXHvoHH6jPXRsjn97gygPUo58XEnc9bqY1jpj";
    let commitment_config = CommitmentConfig::processed();
    let rpc_client =
        RpcClient::new_with_commitment("http://localhost:8899".to_string(), commitment_config);
    let vote_account_config = RpcGetVoteAccountsConfig {
        vote_pubkey: Some(validator_id.to_string()),
        commitment: CommitmentConfig::finalized().into(),
        keep_unstaked_delinquents: None,
        delinquent_slot_distance: None,
    };

    let rpc_block_config = RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base58),
        transaction_details: Some(TransactionDetails::None),
        rewards: Some(true),
        commitment: None,
        max_supported_transaction_version: Some(0),
    };
    let fpc = FeePaymentCalculator::new(rpc_client, rpc_block_config, vote_account_config);
    let rpc_client = fpc.rpc_client;
    let epoch_info = rpc_client.get_epoch_info().await?;
    let payer_signer = Keypair::new();

    let seeds: &[&[u8]] = &[b"test_validator_revenue", &epoch_info.epoch.to_le_bytes()];

    let tx_sig = rpc_client
        .request_airdrop(&payer_signer.pubkey(), 1_000_000_000)
        .await
        .unwrap();

    while !rpc_client
        .confirm_transaction_with_commitment(&tx_sig, commitment_config)
        .await
        .unwrap()
        .value
    {
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    // Make sure airdrop went through.
    while rpc_client
        .get_balance_with_commitment(&payer_signer.pubkey(), commitment_config)
        .await
        .unwrap()
        .value
        == 0
    {
        // Airdrop doesn't get processed after a slot unfortunately.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let data = EpochRewards {
        epoch: epoch_info.epoch,
        rewards: vec![Reward {
            epoch: epoch_info.epoch,
            validator_id: validator_id.to_string(),
            total: 17500,
            block_priority: 0,
            jito: 10000,
            inflation: 2500,
            block_base: 5000,
        }],
    };

    write_record_to_ledger(&rpc_client, &payer_signer, &data, commitment_config, seeds).await?;

    let (record_header, record_body) =
        read_from_ledger(&rpc_client, &payer_signer, seeds, commitment_config).await?;

    assert_eq!(record_header.version, 1);

    let deserialized = borsh::from_slice::<EpochRewards>(record_body.as_slice()).unwrap();

    assert_eq!(deserialized.epoch, epoch_info.epoch);
    assert_eq!(
        deserialized.rewards.first().unwrap().validator_id,
        String::from_str(validator_id).unwrap()
    );

    Ok(())
}

#[tokio::test]
async fn test_execute_worker() -> Result<()> {
    let validator_id = "devgM7SXHvoHH6jPXRsjn97gygPUo58XEnc9bqY1jpj";
    let commitment_config = CommitmentConfig::confirmed();
    let rpc_client =
        RpcClient::new_with_commitment("http://localhost:8899".to_string(), commitment_config);
    let vote_account_config = RpcGetVoteAccountsConfig {
        vote_pubkey: Some(validator_id.to_string()),
        commitment: CommitmentConfig::finalized().into(),
        keep_unstaked_delinquents: None,
        delinquent_slot_distance: None,
    };

    let rpc_block_config = RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base58),
        transaction_details: Some(TransactionDetails::None),
        rewards: Some(true),
        commitment: None,
        max_supported_transaction_version: Some(0),
    };
    let fpc = FeePaymentCalculator::new(rpc_client, rpc_block_config, vote_account_config);
    // let rpc_client = fpc.rpc_client;
    let epoch_info = fpc.rpc_client.get_epoch_info().await?;
    dbg!(&epoch_info);
    let payer_signer = Keypair::new();

    let tx_sig = fpc
        .rpc_client
        .request_airdrop(&payer_signer.pubkey(), 1_000_000_000)
        .await
        .unwrap();

    while !fpc
        .rpc_client
        .confirm_transaction_with_commitment(&tx_sig, commitment_config)
        .await
        .unwrap()
        .value
    {
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    // Make sure airdrop went through.
    while fpc
        .rpc_client
        .get_balance_with_commitment(&payer_signer.pubkey(), commitment_config)
        .await
        .unwrap()
        .value
        == 0
    {
        // Airdrop doesn't get processed after a slot unfortunately.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let validator_ids: Vec<String> = vec![String::from(validator_id)];
    let fake_fetched_epoch = 820;
    let block_reward: u64 = 5000;
    let inflation_reward = 2500;
    let jito_reward = 10000;

    let record_result = write_payments(&fpc, validator_ids).await?;

    assert_eq!(
        record_result.last_written_epoch.unwrap(),
        fake_fetched_epoch
    );

    let computed_payments = record_result.computed_payments.unwrap();

    let first_validator_payment_proof = computed_payments
        .find_payment_proof(&computed_payments.payments[0].node_id)
        .unwrap();

    assert_eq!(
        first_validator_payment_proof.0.amount,
        block_reward + inflation_reward + jito_reward
    );

    assert_eq!(
        first_validator_payment_proof.0.node_id,
        Pubkey::from_str(validator_id).clone().unwrap()
    );

    Ok(())
}
// }
