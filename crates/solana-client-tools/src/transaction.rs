use anyhow::{Context, Result, ensure};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::{AddressLookupTableAccount, VersionedMessage, v0::Message},
    packet::PACKET_DATA_SIZE,
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};

pub(crate) const TRANSACTION_CU_BUFFER: u32 = 5_000;

/// Maximum serialized transaction size after accounting for ComputeBudget overhead.
/// When `allow_compute_price_instruction` is true, an extra 9 bytes are reserved.
pub(crate) fn transaction_size_limit(allow_compute_price_instruction: bool) -> usize {
    if allow_compute_price_instruction {
        PACKET_DATA_SIZE - 32 - 5 - 9
    } else {
        PACKET_DATA_SIZE - 32 - 5
    }
}

pub fn try_new_transaction(
    instructions: &[Instruction],
    signers: &[&Keypair],
    address_lookup_table_accounts: &[AddressLookupTableAccount],
    recent_blockhash: Hash,
) -> Result<VersionedTransaction> {
    let message = Message::try_compile(
        &signers[0].pubkey(),
        instructions,
        address_lookup_table_accounts,
        recent_blockhash,
    )?;

    VersionedTransaction::try_new(VersionedMessage::V0(message), signers)
        .context("Failed to create versioned transaction")
}

pub fn try_batch_instructions_with_common_signers(
    mut instructions_and_compute_units: Vec<(Instruction, u32)>,
    signers: &[&Keypair],
    address_lookup_table_accounts: &[AddressLookupTableAccount],
    allow_compute_price_instruction: bool,
) -> Result<Vec<Vec<Instruction>>> {
    let size_limit = transaction_size_limit(allow_compute_price_instruction);

    instructions_and_compute_units.reverse();

    let mut batches = Vec::new();

    let mut last_batch = Vec::new();
    let mut last_compute_units = TRANSACTION_CU_BUFFER;

    while let Some((instruction, compute_units)) = instructions_and_compute_units.pop() {
        last_batch.push(instruction);
        last_compute_units += compute_units;

        let transaction = try_new_transaction(
            &last_batch,
            signers,
            address_lookup_table_accounts,
            Default::default(),
        )?;

        if bincode::serialize(&transaction).unwrap().len() > size_limit {
            let instruction = last_batch.pop().unwrap();
            let batch_compute_units = last_compute_units - compute_units;

            let mut batch = std::mem::replace(&mut last_batch, vec![instruction]);
            try_complete_instructions_batch(
                &mut batch,
                signers,
                address_lookup_table_accounts,
                size_limit,
                batch_compute_units,
            )?;

            batches.push(batch);
            last_compute_units = TRANSACTION_CU_BUFFER + compute_units;
        }
    }

    if !last_batch.is_empty() {
        try_complete_instructions_batch(
            &mut last_batch,
            signers,
            address_lookup_table_accounts,
            size_limit,
            last_compute_units,
        )?;

        batches.push(last_batch);
    }

    Ok(batches)
}

fn try_complete_instructions_batch(
    batch: &mut Vec<Instruction>,
    signers: &[&Keypair],
    address_lookup_table_accounts: &[AddressLookupTableAccount],
    transaction_size_limit: usize,
    current_compute_units: u32,
) -> Result<()> {
    batch.push(ComputeBudgetInstruction::set_compute_unit_limit(
        current_compute_units,
    ));

    // Out of paranoia, try to serialize the transaction again.
    let transaction = try_new_transaction(
        batch,
        signers,
        address_lookup_table_accounts,
        Default::default(),
    )?;
    ensure!(
        bincode::serialize(&transaction).unwrap().len() <= transaction_size_limit,
        "Transaction is too large"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use solana_sdk::{instruction::AccountMeta, pubkey::Pubkey};

    use super::*;

    /// Synthetic instruction with ID in first data byte; accounts sized for batch testing.
    fn synthetic_instruction(id: u8, data_size: usize, num_accounts: usize) -> Instruction {
        Instruction {
            program_id: Pubkey::new_from_array([id; 32]),
            accounts: (0..num_accounts)
                .map(|i| {
                    AccountMeta::new(
                        Pubkey::new_from_array([id.wrapping_add(i as u8 + 100); 32]),
                        false,
                    )
                })
                .collect(),
            data: vec![id; data_size],
        }
    }

    fn instruction_id(ix: &Instruction) -> Option<u8> {
        if ix.program_id == solana_sdk::compute_budget::ID {
            None
        } else {
            ix.data.first().copied()
        }
    }

    fn is_compute_budget_instruction(ix: &Instruction) -> bool {
        ix.program_id == solana_sdk::compute_budget::ID
    }

    /// Decode CU-limit from a ComputeBudget instruction.
    fn decode_cu_limit(ix: &Instruction) -> Option<u32> {
        if ix.data.len() == 5 && ix.data[0] == 2 {
            Some(u32::from_le_bytes([
                ix.data[1], ix.data[2], ix.data[3], ix.data[4],
            ]))
        } else {
            None
        }
    }

    #[test]
    fn batching_preserves_order_and_respects_size_limit() {
        // Random signer OK: pubkey only for Message::try_compile; assertions don't depend on it.
        let signer = Keypair::new();
        let signers = vec![&signer];

        // 30 instructions (~100 bytes data + 3 accounts each) guarantee multiple batches
        let instructions: Vec<(Instruction, u32)> = (0u8..30)
            .map(|id| (synthetic_instruction(id, 100, 3), 10_000))
            .collect();

        let batches =
            try_batch_instructions_with_common_signers(instructions, &signers, &[], false)
                .expect("batching should succeed");

        assert!(
            batches.len() > 1,
            "Expected >1 batch, got {}",
            batches.len()
        );

        let limit = transaction_size_limit(false);
        let mut observed_ids: Vec<u8> = Vec::new();

        for (i, batch) in batches.iter().enumerate() {
            // (b) size check
            let tx = try_new_transaction(batch, &signers, &[], Default::default()).unwrap();
            let size = bincode::serialize(&tx).unwrap().len();
            assert!(size <= limit, "Batch {} size {} > limit {}", i, size, limit);

            // (c) exactly one CU-limit instruction
            let cu_limit_count = batch
                .iter()
                .filter(|ix| decode_cu_limit(ix).is_some())
                .count();
            assert_eq!(
                cu_limit_count, 1,
                "Batch {} should have exactly 1 CU-limit ix",
                i
            );

            // (a) collect user instruction ids
            for ix in batch {
                if let Some(id) = instruction_id(ix) {
                    observed_ids.push(id);
                }
            }
        }

        // (a) completeness check: all expected ids present
        let expected_ids: Vec<u8> = (0u8..30).collect();
        assert_eq!(
            observed_ids, expected_ids,
            "Not all user instructions present or order violated"
        );
    }

    /// Asserts ComputeBudget CU value == TRANSACTION_CU_BUFFER + sum(batch CUs).
    #[test]
    fn compute_budget_cu_equals_buffer_plus_sum() {
        let signer = Keypair::new();
        let signers = vec![&signer];

        // 3 small instructions (guaranteed single batch) each with 100_000 CU
        let instructions: Vec<(Instruction, u32)> = (0u8..3)
            .map(|id| (synthetic_instruction(id, 20, 2), 100_000))
            .collect();

        let batches =
            try_batch_instructions_with_common_signers(instructions, &signers, &[], false)
                .expect("batching should succeed");

        assert_eq!(
            batches.len(),
            1,
            "Expected single batch for small instructions"
        );

        let cu = batches[0]
            .iter()
            .find_map(decode_cu_limit)
            .expect("batch must have CU-limit ix");
        let expected = TRANSACTION_CU_BUFFER + 3 * 100_000;
        assert_eq!(cu, expected, "CU limit should be buffer + sum");
    }

    /// With allow_compute_price_instruction=true, size limit is smaller and all batches respect it.
    #[test]
    fn compute_price_flag_uses_smaller_limit() {
        assert!(
            transaction_size_limit(true) < transaction_size_limit(false),
            "price flag should reduce size limit"
        );

        let signer = Keypair::new();
        let signers = vec![&signer];

        let instructions: Vec<(Instruction, u32)> = (0u8..20)
            .map(|id| (synthetic_instruction(id, 80, 4), 5_000))
            .collect();

        let batches = try_batch_instructions_with_common_signers(instructions, &signers, &[], true)
            .expect("batching with price should succeed");

        let limit = transaction_size_limit(true);
        for (i, batch) in batches.iter().enumerate() {
            let tx = try_new_transaction(batch, &signers, &[], Default::default()).unwrap();
            let size = bincode::serialize(&tx).unwrap().len();
            assert!(
                size <= limit,
                "Batch {} size {} > limit {} (with price)",
                i,
                size,
                limit
            );
        }
    }

    /// Oversized single instruction either errors or produces valid batch (no silent truncation).
    #[test]
    fn oversized_instruction_not_silently_truncated() {
        let signer = Keypair::new();
        let signers = vec![&signer];

        let large_ix = synthetic_instruction(1, 800, 10);
        let instructions = vec![(large_ix, 100_000)];

        match try_batch_instructions_with_common_signers(instructions, &signers, &[], false) {
            Ok(batches) => {
                assert!(!batches.is_empty());
                let intact = batches[0]
                    .iter()
                    .any(|ix| !is_compute_budget_instruction(ix) && ix.data.len() == 800);
                assert!(intact, "Large instruction must be untruncated");
            }
            Err(_) => { /* acceptable for truly oversized */ }
        }
    }
}
