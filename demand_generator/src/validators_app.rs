use serde::{Deserialize, Serialize};
pub type ValidatorsAppResponses = Vec<ValidatorsAppResponse>;

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
