//! TLS acceptor construction for server listeners.

use std::sync::Arc;

pub fn build_tls_acceptor(
    tls: &crate::config::server::TlsSettings,
) -> anyhow::Result<pgwire::tokio::TlsAcceptor> {
    use std::fs::File;
    use std::io::BufReader;

    let cert_file = File::open(&tls.cert_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS cert {}: {e}", tls.cert_path.display()))?;
    let key_file = File::open(&tls.key_path)
        .map_err(|e| anyhow::anyhow!("failed to open TLS key {}: {e}", tls.key_path.display()))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse TLS certs: {e}"))?;

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| anyhow::anyhow!("failed to parse TLS key: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", tls.key_path.display()))?;

    let server_config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("TLS config error: {e}"))?;

    Ok(pgwire::tokio::TlsAcceptor::from(Arc::new(server_config)))
}
