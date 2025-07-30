use crate::{
    aggregator,
    demand_matrix::{DemandConfig, generate_demand_matrix},
    settings::Settings,
    validators_app,
};
use anyhow::Result;
use network_shapley::types::{Demand, Demands};
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

    pub async fn generate_with_validators(
        &self,
    ) -> Result<(validators_app::ValidatorsAppResponses, Demands)> {
        // Get validators from validators.app
        let validators = validators_app::fetch(&self.settings).await?;
        info!("Fetched validators: {}", validators.len());

        // Aggregate validators by data_center_key
        let dc_aggregates = aggregator::aggregate_by_dc(&validators)?;
        info!("Stake aggregated into cities: {}", dc_aggregates.len());

        // Generate demand matrix
        let config = DemandConfig::default();
        let demands = generate_demand_matrix(&dc_aggregates, &config)?;
        info!("Generated demand entries: {}", demands.len());

        Ok((validators, demands))
    }
}
