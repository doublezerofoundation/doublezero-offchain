use crate::{
    city_aggregator::aggregate_by_city,
    demand_matrix::{DemandConfig, generate_demand_matrix},
    settings::Settings,
    validators_app::ValidatorsAppResponses,
};
use anyhow::{Context, Result, anyhow};
use backon::Retryable;
use network_shapley::types::{Demand, Demands};
use std::time::Duration;
use tracing::info;

#[derive(Debug)]
pub struct DemandGenerator {
    settings: Settings,
}

impl DemandGenerator {
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    pub async fn generate(&self) -> Result<Vec<Demand>> {
        let (_, demands) = self.generate_with_validators().await?;
        Ok(demands)
    }

    pub async fn generate_with_validators(&self) -> Result<(ValidatorsAppResponses, Demands)> {
        // Get validators from validators.app
        let validators = fetch_validators(&self.settings).await?;
        info!("Fetched validators: {}", validators.len());

        // Aggregate validators by city
        let city_aggregates = aggregate_by_city(&validators)?;
        info!("Stake aggregated into cities: {}", city_aggregates.len());

        // Generate demand matrix
        let config = DemandConfig::default();
        let demands = generate_demand_matrix(&city_aggregates, &config)?;
        info!("Generated demand entries: {}", demands.len());

        Ok((validators, demands))
    }
}

async fn fetch_validators(settings: &Settings) -> Result<ValidatorsAppResponses> {
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

    // Single API call to get all validator data
    let validators = (|| async { get_validators_app_data(settings, &http_client, network).await })
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

/// Validates an IP address string
async fn get_validators_app_data(
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
        .ok_or_else(|| anyhow!("Validators.app API token not configured"))?;

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
