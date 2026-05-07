// SPDX-License-Identifier: Apache-2.0

use nodedb_types::error::{NodeDbError, NodeDbResult};

/// TLS configuration for client connections.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS.
    pub enabled: bool,
    /// Path to CA certificate file (PEM). If None, uses system roots.
    pub ca_cert_path: Option<std::path::PathBuf>,
    /// Server name for SNI. If None, derived from connect address.
    pub server_name: Option<String>,
    /// Accept invalid certificates (DANGEROUS — for testing only).
    pub danger_accept_invalid_certs: bool,
}

/// Build a rustls ClientConfig for TLS connections.
pub(super) fn build_tls_client_config(
    tls: &TlsConfig,
) -> NodeDbResult<tokio_rustls::rustls::ClientConfig> {
    use tokio_rustls::rustls;

    let builder = rustls::ClientConfig::builder();

    if tls.danger_accept_invalid_certs {
        let config = builder
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoCertVerifier))
            .with_no_client_auth();
        return Ok(config);
    }

    if let Some(ref ca_path) = tls.ca_cert_path {
        let mut root_store = rustls::RootCertStore::empty();
        let cert_file = std::fs::File::open(ca_path).map_err(|e| {
            NodeDbError::sync_connection_failed(format!("open CA cert {}: {e}", ca_path.display()))
        })?;
        let mut reader = std::io::BufReader::new(cert_file);
        for cert in rustls_pemfile::certs(&mut reader) {
            match cert {
                Ok(c) => {
                    root_store.add(c).map_err(|e| {
                        NodeDbError::sync_connection_failed(format!("add CA cert: {e}"))
                    })?;
                }
                Err(e) => {
                    return Err(NodeDbError::sync_connection_failed(format!(
                        "parse CA cert: {e}"
                    )));
                }
            }
        }
        let config = builder
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(config)
    } else {
        let root_store = rustls::RootCertStore::empty();
        let config = builder
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(config)
    }
}

/// Certificate verifier that accepts everything (DANGEROUS).
#[derive(Debug)]
struct NoCertVerifier;

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
