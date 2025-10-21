pub mod credentials;
pub mod local;
pub mod s3;

use crate::{
    cli::snapshot::CompleteSnapshot,
    settings::{Settings, aws::StorageBackend, network::Network},
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::path::PathBuf;

/// Trait for snapshot storage backends
#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    /// Upload/save a snapshot and return its location (path or URL)
    async fn save(&self, snapshot: &CompleteSnapshot, filename: &str) -> Result<String>;

    /// Verify a snapshot exists at the given location
    async fn exists(&self, filename: &str) -> Result<bool>;

    /// Load a snapshot from the given location
    async fn load(&self, filename: &str) -> Result<CompleteSnapshot>;

    /// Get storage type name for logging
    fn storage_type(&self) -> &'static str;
}

/// Factory for creating storage backends
pub async fn create_storage(settings: &Settings) -> Result<Box<dyn SnapshotStorage>> {
    match settings.scheduler.storage_backend {
        StorageBackend::S3 => {
            // Create S3 storage
            let network = settings.network;
            let network_config = match network {
                Network::MainnetBeta | Network::Mainnet => settings
                    .aws
                    .mainnet_beta
                    .as_ref()
                    .ok_or_else(|| anyhow!("AWS mainnet-beta configuration not found"))?,
                Network::Testnet | Network::Devnet => settings
                    .aws
                    .testnet
                    .as_ref()
                    .ok_or_else(|| anyhow!("AWS testnet configuration not found"))?,
            };

            let storage =
                s3::S3Storage::new(network, network_config.clone(), settings.aws.region.clone())
                    .await?;

            Ok(Box::new(storage))
        }
        StorageBackend::LocalFile => {
            // Create local file storage
            let path = PathBuf::from(&settings.scheduler.snapshot_dir);
            Ok(Box::new(local::LocalFileStorage::new(path)))
        }
    }
}
