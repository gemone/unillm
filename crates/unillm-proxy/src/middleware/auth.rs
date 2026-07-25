//! Virtual-key authentication + admin-token authorization (`DESIGN.md` §10.3 step 1, §13.1, §16).

use axum::http::{HeaderMap, Uri};
use chrono::Utc;
use subtle::ConstantTimeEq;
use unillm_core::CoreError;
use unillm_storage::{KeyStore, VirtualKey, hash_secret};

/// `DESIGN.md` §16: API keys must not be passed as query parameters.
pub fn reject_query_key(uri: &Uri) -> Result<(), CoreError> {
    if let Some(q) = uri.query() {
        let leaked = q.split('&').any(|kv| {
            let key = kv.split('=').next().unwrap_or("");
            key == "key" || key == "api_key"
        });
        if leaked {
            return Err(CoreError::InvalidRequest {
                message: "API keys must not be passed as query parameters".into(),
            });
        }
    }
    Ok(())
}

/// Extract the bearer token from `Authorization: Bearer <token>` or `X-Unillm-Key: <token>`.
/// The same header carries either a virtual key (data plane) or the admin token (`/admin/*`); the
/// request path decides which validation applies.
pub fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(rest) = value.strip_prefix("Bearer ") {
            return Some(rest.trim().to_string());
        }
    }
    headers
        .get("x-unillm-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
}

/// Resolve and validate a virtual key from its secret. `Ok` only if the key exists and is active
/// (not revoked, not expired).
pub async fn authenticate(
    store: &dyn KeyStore,
    secret: Option<&str>,
    pepper: &str,
) -> Result<VirtualKey, CoreError> {
    let secret = secret.ok_or_else(|| CoreError::Unauthorized {
        message: "missing API key".into(),
    })?;
    let hash = hash_secret(secret, pepper);
    match store.find_by_hash(&hash).await {
        Ok(Some(key)) if key.is_active(Utc::now()) => Ok(key),
        Ok(Some(_)) => Err(CoreError::Unauthorized {
            message: "key is revoked or expired".into(),
        }),
        Ok(None) => Err(CoreError::Unauthorized {
            message: "unknown or invalid API key".into(),
        }),
        Err(e) => Err(CoreError::Other {
            message: format!("key lookup failed: {e}"),
        }),
    }
}

/// Require the key to hold `scope` (`DESIGN.md` §13.1: `data` / `admin` / `read-usage`).
pub fn require_scope(key: &VirtualKey, scope: &str) -> Result<(), CoreError> {
    if key.scopes.iter().any(|s| s == scope) {
        Ok(())
    } else {
        Err(CoreError::Unauthorized {
            message: format!("key lacks required scope '{scope}'"),
        })
    }
}

/// Authorize an admin request via the distinct admin token (`DESIGN.md` §16, §10.6). The data-plane
/// virtual keys never satisfy this — `/admin/*` is gated by a separate secret. The comparison is
/// constant-time to avoid leaking the token through a timing side channel.
pub fn require_admin(token: Option<&str>, admin_token: &Option<String>) -> Result<(), CoreError> {
    let expected = admin_token
        .as_ref()
        .ok_or_else(|| CoreError::Unauthorized {
            message: "admin endpoints are disabled (no admin token configured)".into(),
        })?;
    let provided = token.unwrap_or("");
    let matches = provided.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1;
    if matches {
        Ok(())
    } else {
        Err(CoreError::Unauthorized {
            message: "invalid admin token".into(),
        })
    }
}
