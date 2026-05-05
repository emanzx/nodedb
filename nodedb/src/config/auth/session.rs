//! `SessionHandleConfig` and `SessionFingerprintMode` configuration types.

use serde::{Deserialize, Serialize};

/// Configuration for `SessionHandleStore`: fingerprint binding, per-connection
/// resolve rate limit, miss-spike detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHandleConfig {
    /// Session handle TTL in seconds. Default: 3600 (1 hour).
    #[serde(default = "default_session_ttl_secs")]
    pub ttl_secs: u64,

    /// Fingerprint-binding strictness. Default: `subnet`.
    #[serde(default)]
    pub fingerprint_mode: SessionFingerprintMode,

    /// Per-connection resolve attempts allowed within
    /// `rate_limit_window_secs`. Exceed → fatal pgwire error + connection
    /// close. Default: 20.
    #[serde(default = "default_session_rate_limit_max")]
    pub resolve_attempts_per_window: u32,

    /// Sliding-window length for the rate limiter in seconds.
    /// Default: 60.
    #[serde(default = "default_session_rate_limit_window_secs")]
    pub rate_limit_window_secs: u64,

    /// Number of resolve misses on a single connection within
    /// `miss_spike_window_secs` that triggers a
    /// `SessionHandleResolveMissSpike` audit event. Default: 10.
    #[serde(default = "default_session_miss_spike_threshold")]
    pub miss_spike_threshold: u32,

    /// Sliding-window length for the spike detector in seconds.
    /// Default: 60.
    #[serde(default = "default_session_miss_spike_window_secs")]
    pub miss_spike_window_secs: u64,
}

/// Serde-facing mirror of
/// [`crate::control::security::session_handle::FingerprintMode`]. Two
/// enums exist because the core type lives in the `security` module and
/// the config module would pull it into the serde surface.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionFingerprintMode {
    Strict,
    #[default]
    Subnet,
    Disabled,
}

impl From<SessionFingerprintMode> for crate::control::security::session_handle::FingerprintMode {
    fn from(m: SessionFingerprintMode) -> Self {
        use crate::control::security::session_handle::FingerprintMode as Core;
        match m {
            SessionFingerprintMode::Strict => Core::Strict,
            SessionFingerprintMode::Subnet => Core::Subnet,
            SessionFingerprintMode::Disabled => Core::Disabled,
        }
    }
}

impl Default for SessionHandleConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_session_ttl_secs(),
            fingerprint_mode: SessionFingerprintMode::default(),
            resolve_attempts_per_window: default_session_rate_limit_max(),
            rate_limit_window_secs: default_session_rate_limit_window_secs(),
            miss_spike_threshold: default_session_miss_spike_threshold(),
            miss_spike_window_secs: default_session_miss_spike_window_secs(),
        }
    }
}

fn default_session_ttl_secs() -> u64 {
    3600
}
fn default_session_rate_limit_max() -> u32 {
    20
}
fn default_session_rate_limit_window_secs() -> u64 {
    60
}
fn default_session_miss_spike_threshold() -> u32 {
    10
}
fn default_session_miss_spike_window_secs() -> u64 {
    60
}
