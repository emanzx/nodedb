//! JWKS HTTP fetcher: downloads and parses JWKS from provider endpoints.
//!
//! Handles HTTP fetch, JSON deserialization, key extraction, and error
//! reporting. Used by the registry for initial load and periodic refresh.

use tracing::{debug, info, warn};

use super::cache::JwksCache;
use super::key::{JwksResponse, VerificationKey, parse_jwk};

/// Fetch JWKS from a provider's endpoint and update the cache.
///
/// Returns the number of keys successfully parsed.
/// On HTTP or parse failure, logs a warning and returns 0 (cache unchanged).
pub async fn fetch_and_cache(provider_name: &str, jwks_url: &str, cache: &JwksCache) -> usize {
    match fetch_jwks(jwks_url).await {
        Ok(keys) => {
            let count = keys.len();
            if count > 0 {
                info!(
                    provider = %provider_name,
                    url = %jwks_url,
                    keys = count,
                    "JWKS fetched successfully"
                );
                cache.update_provider(provider_name, keys);
            } else {
                warn!(
                    provider = %provider_name,
                    url = %jwks_url,
                    "JWKS response contained no usable signature keys"
                );
            }
            count
        }
        Err(e) => {
            warn!(
                provider = %provider_name,
                url = %jwks_url,
                error = %e,
                "JWKS fetch failed — using cached keys if available"
            );
            0
        }
    }
}

/// Fetch and parse JWKS from a URL.
async fn fetch_jwks(url: &str) -> Result<Vec<VerificationKey>, JwksFetchError> {
    debug!(url = %url, "fetching JWKS");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| JwksFetchError::HttpClient(e.to_string()))?;

    let response = client
        .get(url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| JwksFetchError::HttpRequest(e.to_string()))?;

    let status = response.status();
    if !status.is_success() {
        return Err(JwksFetchError::HttpStatus(status.as_u16()));
    }

    let body = response
        .text()
        .await
        .map_err(|e| JwksFetchError::HttpBody(e.to_string()))?;

    let jwks: JwksResponse =
        sonic_rs::from_str(&body).map_err(|e| JwksFetchError::JsonParse(e.to_string()))?;

    let keys: Vec<VerificationKey> = jwks.keys.iter().filter_map(parse_jwk).collect();

    Ok(keys)
}

/// JWKS fetch errors.
#[derive(Debug, thiserror::Error)]
pub enum JwksFetchError {
    #[error("HTTP client construction failed: {0}")]
    HttpClient(String),
    #[error("HTTP request failed: {0}")]
    HttpRequest(String),
    #[error("JWKS endpoint returned HTTP {0}")]
    HttpStatus(u16),
    #[error("failed to read response body: {0}")]
    HttpBody(String),
    #[error("JWKS JSON parse failed: {0}")]
    JsonParse(String),
}

/// Start the periodic JWKS refresh task.
///
/// Spawns a Tokio task that refreshes JWKS keys for all providers
/// at the configured interval. Runs on the Control Plane.
pub fn spawn_refresh_task(
    providers: Vec<(String, String)>, // (name, jwks_url) pairs
    cache: std::sync::Arc<JwksCache>,
    refresh_interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(refresh_interval_secs));
        // First tick fires immediately — skip it since we fetch on startup.
        interval.tick().await;

        loop {
            interval.tick().await;
            for (name, url) in &providers {
                fetch_and_cache(name, url, &cache).await;
            }
        }
    })
}
