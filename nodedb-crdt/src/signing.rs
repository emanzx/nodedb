//! Delta signing and verification using HMAC-SHA256.
//!
//! Optional feature: agents can sign deltas with a user-specific key
//! before submitting. The validator verifies signatures before accepting
//! deltas, ensuring authenticity and integrity.
//!
//! Key derivation: The signing key is derived from the user's credentials
//! (e.g., SCRAM salted password). The exact key material is provided by
//! the caller — this module only handles HMAC computation and verification.

use std::collections::HashMap;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::{CrdtError, Result};

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA256 signature size (32 bytes).
pub const SIGNATURE_SIZE: usize = 32;

/// Signs deltas with a per-user HMAC-SHA256 key.
pub struct DeltaSigner {
    /// user_id -> HMAC key (32 bytes).
    keys: HashMap<u64, [u8; 32]>,
}

impl DeltaSigner {
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Register a signing key for a user.
    pub fn register_key(&mut self, user_id: u64, key: [u8; 32]) {
        self.keys.insert(user_id, key);
    }

    /// Remove a user's signing key.
    pub fn remove_key(&mut self, user_id: u64) {
        self.keys.remove(&user_id);
    }

    /// Sign delta bytes with the user's key. Returns a 32-byte HMAC-SHA256.
    pub fn sign(&self, user_id: u64, delta_bytes: &[u8]) -> Result<[u8; SIGNATURE_SIZE]> {
        let key = self
            .keys
            .get(&user_id)
            .ok_or_else(|| CrdtError::InvalidSignature {
                user_id,
                detail: "no signing key registered for user".into(),
            })?;

        Ok(hmac_sha256(key, delta_bytes))
    }

    /// Verify a delta signature. Returns Ok(()) if valid.
    pub fn verify(
        &self,
        user_id: u64,
        delta_bytes: &[u8],
        signature: &[u8; SIGNATURE_SIZE],
    ) -> Result<()> {
        let key = self
            .keys
            .get(&user_id)
            .ok_or_else(|| CrdtError::InvalidSignature {
                user_id,
                detail: "no signing key registered for user".into(),
            })?;

        let expected = hmac_sha256(key, delta_bytes);
        if constant_time_eq(&expected, signature) {
            Ok(())
        } else {
            Err(CrdtError::InvalidSignature {
                user_id,
                detail: "HMAC-SHA256 mismatch".into(),
            })
        }
    }
}

impl Default for DeltaSigner {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute HMAC-SHA256(key, message).
fn hmac_sha256(key: &[u8; 32], message: &[u8]) -> [u8; SIGNATURE_SIZE] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(message);
    let result = mac.finalize();
    let mut out = [0u8; SIGNATURE_SIZE];
    out.copy_from_slice(&result.into_bytes());
    out
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let mut signer = DeltaSigner::new();
        signer.register_key(1, [0x42u8; 32]);

        let delta = b"some delta bytes";
        let sig = signer.sign(1, delta).unwrap();

        signer.verify(1, delta, &sig).unwrap();
    }

    #[test]
    fn wrong_key_fails_verification() {
        let mut signer = DeltaSigner::new();
        signer.register_key(1, [0x42u8; 32]);
        signer.register_key(2, [0x99u8; 32]);

        let delta = b"some delta bytes";
        let sig = signer.sign(1, delta).unwrap();

        let err = signer.verify(2, delta, &sig).unwrap_err();
        assert!(matches!(err, CrdtError::InvalidSignature { .. }));
    }

    #[test]
    fn tampered_delta_fails_verification() {
        let mut signer = DeltaSigner::new();
        signer.register_key(1, [0x42u8; 32]);

        let delta = b"original delta";
        let sig = signer.sign(1, delta).unwrap();

        let tampered = b"tampered delta";
        let err = signer.verify(1, tampered, &sig).unwrap_err();
        assert!(matches!(err, CrdtError::InvalidSignature { .. }));
    }

    #[test]
    fn unregistered_user_fails() {
        let signer = DeltaSigner::new();
        let err = signer.sign(99, b"data").unwrap_err();
        assert!(matches!(
            err,
            CrdtError::InvalidSignature { user_id: 99, .. }
        ));
    }
}
