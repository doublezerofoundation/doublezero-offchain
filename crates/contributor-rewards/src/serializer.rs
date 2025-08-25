//! Custom serializers for BTreeMap to ensure deterministic ordering

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use solana_sdk::pubkey::Pubkey;
use std::collections::BTreeMap;
use std::str::FromStr;

/// Serialize a BTreeMap with Pubkey keys as a map with string keys
pub fn serialize_pubkey_btreemap<S, T>(
    map: &BTreeMap<Pubkey, T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    let string_map: BTreeMap<String, &T> = map.iter().map(|(k, v)| (k.to_string(), v)).collect();
    string_map.serialize(serializer)
}

/// Deserialize a BTreeMap with Pubkey keys from a map with string keys
pub fn deserialize_pubkey_btreemap<'de, D, T>(
    deserializer: D,
) -> Result<BTreeMap<Pubkey, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let string_map: BTreeMap<String, T> = BTreeMap::deserialize(deserializer)?;

    string_map
        .into_iter()
        .map(|(k, v)| {
            Pubkey::from_str(&k)
                .map(|pubkey| (pubkey, v))
                .map_err(|e| serde::de::Error::custom(format!("Invalid pubkey: {e}")))
        })
        .collect()
}
