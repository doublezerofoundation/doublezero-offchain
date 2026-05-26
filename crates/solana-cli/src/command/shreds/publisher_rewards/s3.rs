// VENDORED from `malbeclabs/doublezero-shreds`:
// `crates/shred-oracle/src/validator_rewards/s3.rs`. Kept in sync by hand
// because offchain only needs the S3 fetch + merkle-tree primitives, not the
// rest of the oracle. Remove this file once the shreds repo is merged into
// the monorepo and we can depend on `doublezero_shred_oracle::validator_rewards::s3`
// directly.

use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use doublezero_solana_sdk::{
    Pubkey,
    merkle::{MerkleProof, merkle_root_from_indexed_pod_leaves},
    sha2::Hash,
    shred_subscription::types::ValidatorRewardsLeaf,
};
use reqwest::Client;
use serde::Deserialize;
use tracing::debug;

pub const S3_BASE_URL: &str = "https://doublezero-foundation-public.s3.us-east-2.amazonaws.com/exports/multicast_validator_leader_slots";

pub fn build_s3_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .context("build S3 reqwest client")
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // `epoch` is in the wire format but unused offchain (URL carries it).
pub struct ValidatorLeaderSlotEntry {
    pub epoch: u64,
    pub node_identity: String,
    pub client_id: u16,
    pub number_of_leader_slots: u32,
}

#[derive(Debug, Clone)]
pub struct ComputedLeaves {
    pub leaves: Vec<ValidatorRewardsLeaf>,
    pub root: Hash,
    pub total_publishing_validators: u32,
    pub total_published_leader_slots: u32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // `root` + totals are kept for parity with the canonical impl; not used offchain today.
pub struct DistributionLeaves {
    pub leaves: Vec<ValidatorRewardsLeaf>,
    pub proofs: Vec<MerkleProof>,
    pub root: Hash,
    pub total_publishing_validators: u32,
    pub total_published_leader_slots: u32,
}

pub async fn fetch_leader_slot_data(
    client: &Client,
    solana_epoch: u64,
) -> Result<Vec<ValidatorLeaderSlotEntry>> {
    let url = format!("{S3_BASE_URL}/{solana_epoch}.json");
    debug!(url, "Fetching validator leader-slot data");

    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("HTTP request to {url}"))?;

    ensure!(
        response.status().is_success(),
        "S3 returned status {} for epoch {solana_epoch}",
        response.status(),
    );

    let entries = response
        .json::<Vec<ValidatorLeaderSlotEntry>>()
        .await
        .with_context(|| format!("deserialize leader-slot JSON for epoch {solana_epoch}"))?;

    debug!(count = entries.len(), "Fetched validator entries");
    Ok(entries)
}

pub fn compute_leaves(entries: &[ValidatorLeaderSlotEntry]) -> Result<ComputedLeaves> {
    ensure!(!entries.is_empty(), "no validator entries to compute root");

    let mut leaves = entries
        .iter()
        .filter_map(|entry| {
            let pubkey = Pubkey::from_str(&entry.node_identity).ok()?;
            Some(ValidatorRewardsLeaf::new(
                pubkey,
                entry.number_of_leader_slots,
                entry.client_id,
            ))
        })
        .collect::<Vec<_>>();

    leaves.sort_unstable_by_key(|l| (l.node_id, l.client_id));

    if let Some(pair) = leaves
        .windows(2)
        .find(|w| w[0].node_id == w[1].node_id && w[0].client_id == w[1].client_id)
    {
        anyhow::bail!(
            "duplicate (node_id, client_id) pair: node_id {}, client_id {} in validator leader-slot data",
            pair[0].node_id,
            pair[0].client_id,
        );
    }

    let total = <u32>::try_from(leaves.len()).context("too many validators")?;
    ensure!(total > 0, "no valid validator entries after filtering");

    let total_published_leader_slots = leaves.iter().map(|leaf| leaf.leader_slots).try_fold(
        u32::default(),
        |running_total, slots| {
            running_total
                .checked_add(slots)
                .context("total published leader slots overflow")
        },
    )?;

    let root =
        merkle_root_from_indexed_pod_leaves(&leaves, Some(ValidatorRewardsLeaf::LEAF_PREFIX))
            .context("failed to compute merkle root")?;

    Ok(ComputedLeaves {
        leaves,
        root,
        total_publishing_validators: total,
        total_published_leader_slots,
    })
}

pub fn compute_leaves_with_proofs(
    entries: &[ValidatorLeaderSlotEntry],
) -> Result<DistributionLeaves> {
    let ComputedLeaves {
        leaves,
        root,
        total_publishing_validators,
        total_published_leader_slots,
    } = compute_leaves(entries)?;

    let proofs = (0..total_publishing_validators)
        .map(|i| {
            MerkleProof::from_indexed_pod_leaves(
                &leaves,
                i,
                Some(ValidatorRewardsLeaf::LEAF_PREFIX),
            )
            .with_context(|| format!("compute merkle proof for leaf {i}"))
        })
        .collect::<Result<_>>()?;

    Ok(DistributionLeaves {
        leaves,
        proofs,
        root,
        total_publishing_validators,
        total_published_leader_slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(identity: &str, client_id: u16, slots: u32) -> ValidatorLeaderSlotEntry {
        ValidatorLeaderSlotEntry {
            epoch: 951,
            node_identity: identity.to_string(),
            client_id,
            number_of_leader_slots: slots,
        }
    }

    #[test]
    fn test_compute_leaves_with_proofs_returns_sorted_leaves_and_valid_proofs() {
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let pk3 = Pubkey::new_unique();
        let entries = vec![
            make_entry(&pk2.to_string(), 2, 200),
            make_entry(&pk1.to_string(), 1, 100),
            make_entry(&pk3.to_string(), 3, 300),
        ];

        let distribution_leaves = compute_leaves_with_proofs(&entries).unwrap();

        assert_eq!(distribution_leaves.leaves.len(), 3);
        assert_eq!(distribution_leaves.proofs.len(), 3);
        assert_eq!(distribution_leaves.total_publishing_validators, 3);
        assert_eq!(distribution_leaves.total_published_leader_slots, 600);

        assert!(distribution_leaves.leaves[0].node_id < distribution_leaves.leaves[1].node_id);
        assert!(distribution_leaves.leaves[1].node_id < distribution_leaves.leaves[2].node_id);

        for (i, proof) in distribution_leaves.proofs.iter().enumerate() {
            let reconstructed = proof.root_from_pod_leaf(
                &distribution_leaves.leaves[i],
                Some(ValidatorRewardsLeaf::LEAF_PREFIX),
            );
            assert_eq!(reconstructed, distribution_leaves.root);
        }
    }
}
