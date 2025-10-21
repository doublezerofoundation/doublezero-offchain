use crate::settings::{aws::AwsNetworkConfig, network::Network};
use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_s3::config::{Credentials, Region};
use tracing::info;

pub struct CredentialLoader {
    network: Network,
    config: AwsNetworkConfig,
    region: String,
}

impl CredentialLoader {
    pub fn new(network: Network, config: AwsNetworkConfig, region: String) -> Self {
        Self {
            network,
            config,
            region,
        }
    }

    pub async fn load_config(&self) -> Result<aws_sdk_s3::Config> {
        info!("Loading AWS configuration for network: {:?}", self.network);

        let mut config_builder = aws_sdk_s3::Config::builder()
            .region(Region::new(self.region.clone()))
            .behavior_version(BehaviorVersion::latest());

        // Set custom endpoint if provided (for minio or other S3-compatible services)
        if let Some(endpoint) = &self.config.endpoint {
            info!("Using custom S3 endpoint: {}", endpoint);
            config_builder = config_builder.endpoint_url(endpoint);
            // Force path-style for minio compatibility
            config_builder = config_builder.force_path_style(true);
        }

        // Priority 1: Explicit credentials from config (TOML or env vars)
        if let (Some(access_key), Some(secret_key)) =
            (&self.config.access_key_id, &self.config.secret_access_key)
        {
            info!("Using AWS credentials from configuration");
            let credentials = Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "contributor-rewards-config",
            );

            return Ok(config_builder.credentials_provider(credentials).build());
        }

        // Priority 2: Default AWS credential chain (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, IAM role)
        info!("Using default AWS credential chain (environment variables)");

        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(RegionProviderChain::default_provider())
            .load()
            .await;

        // Override region if SDK config doesn't have one
        let mut s3_config = aws_sdk_s3::Config::from(&sdk_config);
        if s3_config.region().is_none() {
            s3_config = s3_config
                .to_builder()
                .region(Region::new(self.region.clone()))
                .build();
        }

        Ok(s3_config)
    }

    pub async fn validate(&self) -> Result<()> {
        let config = self.load_config().await?;
        let client = aws_sdk_s3::Client::from_conf(config);

        // Verify credentials by checking bucket exists
        client
            .head_bucket()
            .bucket(&self.config.bucket)
            .send()
            .await
            .context("Failed to validate AWS credentials - cannot access bucket")?;

        info!(
            "AWS credentials validated successfully for bucket: {}",
            self.config.bucket
        );
        Ok(())
    }
}
