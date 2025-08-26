// get epoch on start
// fetch last record
// if epoch from last record is the same as initial record, shut down
// maybe write the last time the remote record was checked
// if epoch is greater than fetched record, calculate validator payments for the local epoch
// generate merkle tree from payments
// write record

use crate::{
    fee_payment_calculator::ValidatorRewards,
    rewards,
    validator_payment::{ComputedSolanaValidatorPayments, SolanaValidatorPayment},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use svm_hash::sha2::Hash;

#[derive(Debug)]
pub struct RecordResult {
    pub last_written_epoch: Option<u64>,
    pub last_check: Option<DateTime<Utc>>,
    pub data_written: Option<Hash>,
    pub computed_payments: Option<ComputedSolanaValidatorPayments>,
}

pub async fn write_payments<T: ValidatorRewards>(
    fee_payment_calculator: &T,
    validator_ids: Vec<String>,
) -> Result<RecordResult> {
    let fetched_epoch_info = fee_payment_calculator.get_epoch_info().await?;
    let record_result: RecordResult;

    // TODO: fetch record from ledger
    let now = Utc::now();
    let fake_fetched_epoch: u64 = 820; // 819 is the mock
    if fetched_epoch_info.epoch == fake_fetched_epoch {
        record_result = RecordResult {
            last_written_epoch: Some(fake_fetched_epoch),
            last_check: Some(now),
            data_written: None, // probably will be something if we want to record "heartbeats"
            computed_payments: None,
        };
        // maybe write last check time or maybe epoch + counter ?
        // return early as there's nothing to write
        return Ok(record_result);
    };

    // fetch rewards for validators
    let validator_rewards = rewards::get_total_rewards(
        fee_payment_calculator,
        validator_ids.as_slice(),
        fetched_epoch_info.epoch,
    )
    .await?;

    // TODO: post rewards to ledger

    // gather rewards into payments
    let computed_solana_validator_payment_vec: Vec<SolanaValidatorPayment> = validator_rewards
        .rewards
        .iter()
        .map(|reward| SolanaValidatorPayment {
            node_id: Pubkey::from_str(&reward.validator_id).unwrap(),
            amount: reward.total,
        })
        .collect();

    let computed_solana_validator_payments = ComputedSolanaValidatorPayments {
        epoch: fetched_epoch_info.epoch,
        payments: computed_solana_validator_payment_vec,
    };

    let data = computed_solana_validator_payments.merkle_root();

    record_result = RecordResult {
        last_written_epoch: Some(fake_fetched_epoch),
        last_check: Some(now),
        data_written: data,
        computed_payments: Some(computed_solana_validator_payments),
    };
    Ok(record_result)
}
