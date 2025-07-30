// TODO:
// - Investigate if we need to make more requests to get stake data, gossip data etc.
// - If so, then we should utilize that extra information to build more accurate "real-time" demand

use crate::settings::Settings;
use anyhow::{Context, Result, anyhow};
use backon::Retryable;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;

pub type ValidatorsAppResponses = Vec<ValidatorsAppResponse>;

/// NOTE:
/// - Sometimes the response does _not_ contain the key itself, hence Option with default
/// - Only need the partial response from the validators.app API
#[derive(Debug, Deserialize, Serialize)]
pub struct ValidatorsAppResponse {
    pub network: String,
    pub account: String,
    pub is_active: bool,
    pub is_dz: bool,
    #[serde(default)]
    pub active_stake: Option<u64>,
    #[serde(default)]
    pub latitude: Option<String>,
    #[serde(default)]
    pub longitude: Option<String>,
    #[serde(default)]
    pub data_center_key: Option<String>,
    #[serde(default)]
    pub ip: Option<String>,
}

pub async fn fetch(settings: &Settings) -> Result<ValidatorsAppResponses> {
    let http_client = reqwest::Client::new();

    // Determine network from RPC URL
    let network = if settings.demand_generator.solana_rpc_url.contains("testnet") {
        "testnet"
    } else if settings.demand_generator.solana_rpc_url.contains("devnet") {
        "devnet"
    } else {
        "mainnet"
    };

    info!(
        "Fetching validator data from validators.app for {} network",
        network
    );

    let validators = (|| async { send_request(settings, &http_client, network).await })
        .retry(&settings.backoff())
        .notify(|err: &anyhow::Error, dur: Duration| {
            info!(
                "retrying validators.app API error: {:?} with sleeping {:?}",
                err, dur
            )
        })
        .await?;

    info!(
        "Received data for {} validators from validators.app",
        validators.len()
    );

    Ok(validators)
}

async fn send_request(
    settings: &Settings,
    client: &reqwest::Client,
    network: &str,
) -> Result<ValidatorsAppResponses> {
    let url = format!(
        "{}/validators/{}.json?order=score",
        settings.demand_generator.validators_app.base_url, network
    );

    let api_token = settings
        .demand_generator
        .validators_app
        .api_token
        .as_ref()
        .ok_or_else(|| anyhow!("API token not configured for validators.app"))?;

    let response = client
        .get(&url)
        .header("Token", api_token)
        .send()
        .await?
        .error_for_status()?;

    let validators = response
        .json()
        .await
        .context("Failed to parse JSON from validators.app response")?;

    Ok(validators)
}
