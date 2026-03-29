use std::{collections::HashMap, net::Ipv4Addr};

use anyhow::{Context, Result, bail};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Validator metadata from the validator metadata service (client name, version, etc.).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ValidatorRecord {
    pub vote_account: String,
    pub software_client: String,
    pub software_version: String,
    pub activated_stake_sol: f64,
    pub gossip_ip: Ipv4Addr,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Source for validator metadata not available onchain (client name, version, etc.).
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ValidatorMetadataReader: Send + Sync {
    /// Fetch active validators with their metadata, keyed by IP.
    async fn fetch_validators(&self) -> Result<HashMap<Ipv4Addr, ValidatorRecord>>;
}

// ---------------------------------------------------------------------------
// HTTP implementation
// ---------------------------------------------------------------------------

pub struct DataApiValidatorMetadataReader {
    pub api_url: String,
}

#[derive(serde::Deserialize)]
struct SqlResponse {
    rows: Vec<Vec<serde_json::Value>>,
}

#[async_trait::async_trait]
impl ValidatorMetadataReader for DataApiValidatorMetadataReader {
    async fn fetch_validators(&self) -> Result<HashMap<Ipv4Addr, ValidatorRecord>> {
        let query = r#"
            SELECT
                v.vote_account,
                v.software_client,
                v.software_version,
                v.active_stake,
                v.ip
            FROM validatorsapp_validators_current v
            WHERE v.is_active = 1
        "#;

        let client = reqwest::Client::new();
        let resp = client
            .post(&self.api_url)
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await
            .context("failed to query data API for validators")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("data API returned {status}: {body}");
        }

        let body: SqlResponse = resp
            .json()
            .await
            .context("failed to parse data API response")?;

        let mut map = HashMap::new();
        for row in &body.rows {
            if row.len() < 5 {
                continue;
            }

            let gossip_ip: Ipv4Addr = match row[4].as_str().unwrap_or_default().parse() {
                Ok(ip) => ip,
                Err(_) => continue,
            };

            map.insert(
                gossip_ip,
                ValidatorRecord {
                    vote_account: row[0].as_str().unwrap_or_default().to_string(),
                    software_client: row[1].as_str().unwrap_or_default().to_string(),
                    software_version: row[2].as_str().unwrap_or_default().to_string(),
                    activated_stake_sol: row[3].as_i64().unwrap_or(0) as f64 / 1_000_000_000.0,
                    gossip_ip,
                },
            );
        }

        Ok(map)
    }
}
