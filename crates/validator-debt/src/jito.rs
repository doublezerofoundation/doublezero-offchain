use crate::solana_debt_calculator::ValidatorRewards;
use anyhow::{Result, anyhow};
use backon::{ExponentialBuilder, Retryable};
use futures::{StreamExt, stream};
use serde::Deserialize;
use std::{collections::HashMap, time::Duration};
use tracing::info;

const JITO_BASE_URL: &str = "https://kobe.mainnet.jito.network/api/v1/";

pub const DEFAULT_JITO_REWARDS_LIMIT: u16 = 1_500;

#[derive(Deserialize, Debug)]
pub struct JitoRewards {
    // TODO: check total_count to see if it exceeds entries in a single response
    // limit - default: 100, max: 10000
    pub total_count: u16,
    pub rewards: Vec<JitoReward>,
}

#[derive(Deserialize, Debug)]
pub struct JitoReward {
    pub vote_account: String,
    pub mev_revenue: u64,
}

/// Fetches Jito MEV rewards for specified validators and epoch
///
/// This function retrieves MEV reward data from the Jito API for a given epoch
/// and returns a mapping of validator vote accounts to their MEV revenue.
///
/// # Arguments
/// * `solana_debt_calculator` - HTTP client for making API requests
/// * `validator_ids` - List of validator vote account addresses
/// * `epoch` - Epoch number to fetch rewards for
///
/// # Returns
/// HashMap mapping validator vote account strings to MEV revenue amounts (in lamports)
///
/// # Errors
/// Returns error if API request fails or data cannot be parsed
pub async fn get_jito_rewards<T: ValidatorRewards>(
    solana_debt_calculator: &T,
    validator_ids: &[String],
    epoch: u64,
) -> Result<HashMap<String, u64>> {
    get_jito_rewards_with_limit(solana_debt_calculator, validator_ids, epoch, DEFAULT_JITO_REWARDS_LIMIT).await
}

/// Get Jito rewards with configurable limit
pub async fn get_jito_rewards_with_limit<T: ValidatorRewards>(
    solana_debt_calculator: &T,
    validator_ids: &[String],
    epoch: u64,
    limit: u16,
) -> Result<HashMap<String, u64>> {
    let url = format!(
        "{JITO_BASE_URL}validator_rewards?epoch={epoch}&limit={limit}"
    );

    println!("Fetching Jito rewards for epoch {epoch} with limit {limit}");
    let rewards = (|| async { solana_debt_calculator.get::<JitoRewards>(&url).await })
        .retry(
            &ExponentialBuilder::default()
                .with_max_times(5)
                .with_min_delay(Duration::from_millis(100))
                .with_max_delay(Duration::from_secs(10))
                .with_jitter(),
        )
        .notify(|err, dur: Duration| {
            info!("Jito API call failed, retrying in {:?}: {}", dur, err);
        })
        .await
        .map_err(|e| {
            anyhow!("Failed to fetch Jito rewards for epoch {epoch} after retries: {e:#?}")
        })?;

    if rewards.total_count > limit {
        println!(
            "Warning: received total count ({}) higher than request limit ({}); some rewards may be missing",
            rewards.total_count, limit
        );
    }
    let jito_rewards: HashMap<String, u64> = stream::iter(validator_ids)
        .map(|validator_id| {
            let validator_id = validator_id.to_string();
            println!("Fetching Jito rewards for validator_id {validator_id}");
            let rewards = &rewards.rewards;
            async move {
                let mev_revenue = rewards
                    .iter()
                    .find(|reward| *validator_id == reward.vote_account)
                    .map(|reward| reward.mev_revenue)
                    .unwrap_or(0);
                (validator_id, mev_revenue)
            }
        })
        .buffer_unordered(5)
        .collect()
        .await;

    Ok(jito_rewards)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solana_debt_calculator::MockValidatorRewards;

    #[tokio::test]
    async fn test_get_jito_rewards() {
        let mut jito_mock_fetcher = MockValidatorRewards::new();
        let pubkey = "CvSb7wdQAFpHuSpTYTJnX5SYH4hCfQ9VuGnqrKaKwycB";
        let validator_ids: &[String] = &[String::from(pubkey)];
        let epoch = 812;
        let expected_mev_revenue = 503423196855;
        jito_mock_fetcher
            .expect_get::<JitoRewards>()
            .withf(move |url| url.contains(&format!("epoch={epoch}")))
            .times(1)
            .returning(move |_| {
                Ok(JitoRewards {
                    total_count: 1000,
                    rewards: vec![JitoReward {
                        vote_account: pubkey.to_string(),
                        mev_revenue: expected_mev_revenue,
                    }],
                })
            });

        let mock_response = get_jito_rewards(&jito_mock_fetcher, validator_ids, epoch)
            .await
            .unwrap();

        assert_eq!(mock_response.get(pubkey), Some(&expected_mev_revenue));
    }
}
