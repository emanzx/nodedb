use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Encryption at rest settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionSettings {
    /// Path to the 32-byte AES-256-GCM key file.
    /// Generate with: `head -c 32 /dev/urandom > /etc/nodedb/keys/wal.key`
    pub key_path: PathBuf,
}

/// Client-facing TLS settings (distinct from inter-node mTLS in mtls.rs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSettings {
    /// Path to server certificate (PEM).
    pub cert_path: PathBuf,
    /// Path to server private key (PEM).
    pub key_path: PathBuf,
    /// Certificate hot-reload check interval (seconds). The server watches
    /// cert/key files for mtime changes and atomically swaps the TLS config.
    /// Default: 3600 (1 hour). Set to 0 to disable hot rotation.
    #[serde(default)]
    pub cert_reload_interval_secs: Option<u64>,
}
