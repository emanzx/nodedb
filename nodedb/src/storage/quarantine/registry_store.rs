// SPDX-License-Identifier: BUSL-1.1

//! Quarantine object-store backend construction.
//!
//! `QuarantineStorageConfig` + `build_quarantine_store` are extracted here
//! so `registry.rs` stays under the 500-line production-code cap.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;

/// Configuration for the quarantine archive storage layer.
#[derive(Debug, Clone)]
pub struct QuarantineStorageConfig {
    /// S3-compatible endpoint URL. Empty = local filesystem.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Prefix path within the bucket.
    pub prefix: String,
    /// Access key (empty = IAM role / instance credentials).
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
    /// Region.
    pub region: String,
    /// Local directory for quarantine storage (used when endpoint is empty).
    pub local_dir: Option<PathBuf>,
}

/// Build an `ObjectStore` from a `QuarantineStorageConfig`.
///
/// When `endpoint` is empty, uses `LocalFileSystem` backed by `local_dir`
/// (or `data_dir/quarantine` if `local_dir` is unset).
pub fn build_quarantine_store(
    config: &QuarantineStorageConfig,
    data_dir: &Path,
) -> crate::Result<Arc<dyn ObjectStore>> {
    let dir = config
        .local_dir
        .clone()
        .unwrap_or_else(|| data_dir.join("quarantine"));

    if config.endpoint.is_empty() {
        // no-objectstore: bootstrap for the LocalFileSystem ObjectStore backend;
        // the store cannot create its own root.
        std::fs::create_dir_all(&dir).map_err(crate::Error::Io)?;
        let store = LocalFileSystem::new_with_prefix(&dir).map_err(|e| crate::Error::Storage {
            engine: "quarantine".into(),
            detail: format!("local quarantine storage init: {e}"),
        })?;
        Ok(Arc::new(store))
    } else {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_allow_http(config.endpoint.starts_with("http://"));
        if !config.access_key.is_empty() {
            builder = builder
                .with_access_key_id(&config.access_key)
                .with_secret_access_key(&config.secret_key);
        }
        let s3 = builder.build().map_err(|e| crate::Error::Storage {
            engine: "quarantine".into(),
            detail: format!("S3 quarantine client init: {e}"),
        })?;
        Ok(Arc::new(s3))
    }
}
