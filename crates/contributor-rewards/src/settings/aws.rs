use serde::{Deserialize, Serialize};

/// AWS configuration for S3 snapshot storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSettings {
    /// AWS region (default: us-east-1)
    #[serde(default = "default_region")]
    pub region: String,

    /// Testnet S3 configuration
    pub testnet: Option<AwsNetworkConfig>,

    /// Mainnet-beta S3 configuration
    #[serde(rename = "mainnet-beta")]
    pub mainnet_beta: Option<AwsNetworkConfig>,
}

/// Per-network AWS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsNetworkConfig {
    /// S3 bucket name
    pub bucket: String,

    /// AWS access key ID (loaded from environment variables)
    /// Environment variable: DZ__AWS__<NETWORK>__ACCESS_KEY_ID
    pub access_key_id: Option<String>,

    /// AWS secret access key (loaded from environment variables)
    /// Environment variable: DZ__AWS__<NETWORK>__SECRET_ACCESS_KEY
    pub secret_access_key: Option<String>,

    /// Custom S3 endpoint (for minio or other S3-compatible services)
    /// Example: "http://localhost:9000" for local minio
    /// Leave None for AWS S3
    pub endpoint: Option<String>,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

impl Default for AwsSettings {
    fn default() -> Self {
        Self {
            region: default_region(),
            testnet: Some(AwsNetworkConfig {
                bucket: "doublezero-contributor-rewards-testnet-snapshots".to_string(),
                access_key_id: None,
                secret_access_key: None,
                endpoint: None,
            }),
            mainnet_beta: Some(AwsNetworkConfig {
                bucket: "doublezero-contributor-rewards-mn-beta-snapshots".to_string(),
                access_key_id: None,
                secret_access_key: None,
                endpoint: None,
            }),
        }
    }
}

/// Storage backend for snapshots
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageBackend {
    /// S3-compatible storage (AWS S3, minio, etc.)
    S3,
    /// Local filesystem storage
    LocalFile,
}

impl Default for StorageBackend {
    fn default() -> Self {
        Self::S3 // Default to S3 for new deployments
    }
}
