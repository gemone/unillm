//! Virtual-key lifecycle helpers (`DESIGN.md` §13.1, §16, D11).
//!
//! The raw secret is never stored or logged — only its pepper-mixed SHA-256 hash is persisted (and
//! looked up by `KeyStore`). These helpers derive that hash, mint a new secret, and compute its
//! display prefix.

use sha2::{Digest, Sha256};
use uuid::Uuid;

/// The pepper-mixed SHA-256 hash of a key secret, hex-encoded. This is the value `KeyStore`
/// persists and the auth path looks up.
pub fn hash_secret(secret: &str, pepper: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pepper.as_bytes());
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

/// Mint a new random secret (`sk-unillm-<random>`). Shown once to the caller at creation.
pub fn generate_secret() -> String {
    format!("sk-unillm-{}", Uuid::new_v4().simple())
}

/// The first ~8 characters of a secret, for display and prefix lookup
/// (`DESIGN.md` §11.3 `key_prefix`).
pub fn key_prefix(secret: &str) -> String {
    secret.chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_and_pepper_dependent() {
        let a = hash_secret("sk-unillm-xyz", "pepper-A");
        let a2 = hash_secret("sk-unillm-xyz", "pepper-A");
        let b = hash_secret("sk-unillm-xyz", "pepper-B");
        let c = hash_secret("sk-unillm-other", "pepper-A");
        assert_eq!(a, a2, "same secret+pepper → same hash");
        assert_ne!(a, b, "different pepper → different hash");
        assert_ne!(a, c, "different secret → different hash");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generated_secrets_are_unique_and_prefixed() {
        let s1 = generate_secret();
        let s2 = generate_secret();
        assert!(s1.starts_with("sk-unillm-"));
        assert_ne!(s1, s2);
        assert_eq!(key_prefix(&s1), &s1[..8]);
    }
}
