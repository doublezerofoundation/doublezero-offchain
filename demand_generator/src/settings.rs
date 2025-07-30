use anyhow::{Context, Result, bail};
use backon::ExponentialBuilder;
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub demand_generator: DemandGeneratorSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemandGeneratorSettings {
    pub validators_app: ValidatorsAppSettings,
    #[serde(default = "default_solana_rpc_url")]
    pub solana_rpc_url: String,
    #[serde(default = "default_max_requests")]
    pub max_requests: u32,
    pub backoff: Option<BackoffSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorsAppSettings {
    #[serde(default = "default_validators_app_base_url")]
    pub base_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffSettings {
    #[serde(default = "default_backoff_factor")]
    pub factor: f32,
    #[serde(default = "default_backoff_min_delay_ms")]
    pub min_delay_ms: u64,
    #[serde(default = "default_backoff_max_delay_ms")]
    pub max_delay_ms: u64,
    #[serde(default = "default_backoff_max_times")]
    pub max_times: usize,
}

impl BackoffSettings {
    pub fn min_delay_duration(&self) -> Duration {
        Duration::from_millis(self.min_delay_ms)
    }

    pub fn max_delay_duration(&self) -> Duration {
        Duration::from_millis(self.max_delay_ms)
    }
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        // Get environment from DZ_ENV or default to "devnet"
        let env = std::env::var("DZ_ENV").unwrap_or_else(|_| "devnet".to_string());
        Self::load(env)
    }

    pub fn backoff(&self) -> ExponentialBuilder {
        match &self.demand_generator.backoff {
            None => ExponentialBuilder::default().with_jitter(),
            Some(bs) => ExponentialBuilder::default()
                .with_jitter()
                .with_factor(bs.factor)
                .with_max_times(bs.max_times)
                .with_min_delay(bs.min_delay_duration())
                .with_max_delay(bs.max_delay_duration()),
        }
    }

    pub fn load(env: String) -> Result<Self> {
        let mut figment = Figment::new()
            // Load default configuration
            .merge(Toml::file("config/default.toml"));

        // Load environment-specific configuration if it exists
        let env_config_path = format!("config/{env}.toml");
        if std::path::Path::new(&env_config_path).exists() {
            figment = figment.merge(Toml::file(&env_config_path));
        }

        // Load local overrides if present (git-ignored)
        let local_config_path = "config/local.toml";
        if std::path::Path::new(local_config_path).exists() {
            figment = figment.merge(Toml::file(local_config_path));
        }

        // Environment variables can still override
        figment = figment.merge(Env::prefixed("DZ_").split("__"));

        let mut config: Settings = figment.extract()?;

        // Validate and set API token from environment
        config.validate_and_set_api_token()?;

        Ok(config)
    }

    fn validate_and_set_api_token(&mut self) -> Result<()> {
        // Check if token is already set in config
        if let Some(ref token) = self.demand_generator.validators_app.api_token {
            if token.contains("_token") || token.is_empty() {
                bail!(
                    "Validators.app API token appears to be a placeholder or empty. Please set DZ__DEMAND_GENERATOR__VALIDATORS_APP__API_TOKEN environment variable."
                );
            }
            // Token exists and looks valid
            return Ok(());
        }

        // No token in config, check environment
        let env_key = "DZ__DEMAND_GENERATOR__VALIDATORS_APP__API_TOKEN";
        let env_token = std::env::var("DZ__DEMAND_GENERATOR__VALIDATORS_APP__API_TOKEN").with_context(|| {
            format!(
                "Please set the {env_key} environment variable with your validators.app API token.",
            )
        })?;

        if env_token.is_empty() {
            bail!("Validators.app API token is empty. Please provide a valid token.");
        }

        // Set the token from environment
        self.demand_generator.validators_app.api_token = Some(env_token);
        Ok(())
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            demand_generator: DemandGeneratorSettings::default(),
        }
    }
}

impl Default for DemandGeneratorSettings {
    fn default() -> Self {
        Self {
            validators_app: ValidatorsAppSettings {
                base_url: default_validators_app_base_url(),
                api_token: None,
            },
            solana_rpc_url: default_solana_rpc_url(),
            max_requests: default_max_requests(),
            backoff: None,
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_validators_app_base_url() -> String {
    "https://www.validators.app/api/v1".to_string()
}

fn default_solana_rpc_url() -> String {
    "https://api.mainnet-beta.solana.com".to_string()
}

fn default_max_requests() -> u32 {
    8
}

fn default_backoff_factor() -> f32 {
    2.0
}

fn default_backoff_min_delay_ms() -> u64 {
    1000 // 1s
}

fn default_backoff_max_delay_ms() -> u64 {
    60 * 1000 // 60s
}

fn default_backoff_max_times() -> usize {
    3
}
