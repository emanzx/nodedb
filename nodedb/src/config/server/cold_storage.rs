use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Cold storage configuration for L2 tiering.
///
/// Example TOML:
/// ```toml
/// [cold_storage]
/// bucket = "my-nodedb-cold"
/// region = "us-east-1"
/// tier_after_secs = 3600
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdStorageSettings {
    /// S3-compatible endpoint URL. Empty = local filesystem (dev/testing).
    #[serde(default)]
    pub endpoint: String,
    /// Bucket name.
    #[serde(default = "default_cold_bucket")]
    pub bucket: String,
    /// Prefix path within the bucket.
    #[serde(default = "default_cold_prefix")]
    pub prefix: String,
    /// Access key (empty = IAM role / instance credentials).
    #[serde(default)]
    pub access_key: String,
    /// Secret key.
    #[serde(default)]
    pub secret_key: String,
    /// Region (required for AWS S3; ignored by most S3-compatible stores).
    #[serde(default = "default_cold_region")]
    pub region: String,
    /// Local directory for cold storage (used when endpoint is empty).
    #[serde(default)]
    pub local_dir: Option<PathBuf>,
    /// Parquet compression: "zstd" (default), "snappy", "lz4", "none".
    #[serde(default = "default_cold_compression")]
    pub compression: String,
    /// Target Parquet row group size.
    #[serde(default = "default_cold_row_group_size")]
    pub row_group_size: usize,
    /// Tier segments older than this many seconds to cold storage.
    #[serde(default = "default_tier_after_secs")]
    pub tier_after_secs: u64,
    /// How often to check for tierable segments (seconds).
    #[serde(default = "default_tier_check_interval_secs")]
    pub tier_check_interval_secs: u64,
}

fn default_cold_bucket() -> String {
    "nodedb-cold".into()
}

fn default_cold_prefix() -> String {
    "data/".into()
}

fn default_cold_region() -> String {
    "us-east-1".into()
}

fn default_cold_compression() -> String {
    "zstd".into()
}

fn default_cold_row_group_size() -> usize {
    65_536
}

fn default_tier_after_secs() -> u64 {
    3600 // 1 hour
}

fn default_tier_check_interval_secs() -> u64 {
    300 // 5 minutes
}

impl ColdStorageSettings {
    /// Convert to the `ColdStorageConfig` used by the storage engine.
    pub fn to_cold_storage_config(&self) -> crate::storage::cold::ColdStorageConfig {
        let compression = match self.compression.as_str() {
            "snappy" => crate::storage::cold::ParquetCompression::Snappy,
            "lz4" => crate::storage::cold::ParquetCompression::Lz4,
            "none" => crate::storage::cold::ParquetCompression::None,
            _ => crate::storage::cold::ParquetCompression::Zstd,
        };
        crate::storage::cold::ColdStorageConfig {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            region: self.region.clone(),
            local_dir: self.local_dir.clone(),
            compression,
            row_group_size: self.row_group_size,
        }
    }
}
