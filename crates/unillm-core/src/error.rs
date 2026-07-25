//! Error model (`DESIGN.md` §15).
//!
//! `CoreError` is the single error type used across the core, SDK, and proxy. It is an internally
//! tagged enum (tag `kind`) so it round-trips through JSON on the wire — the proxy serializes it
//! into response bodies, and the Python SDK maps it onto a typed exception hierarchy (`DESIGN.md`
//! §15.3).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The canonical error type (`DESIGN.md` §15.1).
///
/// Variants carrying only human context use a `message` field; `ProviderError` additionally
/// captures the upstream HTTP status and an optional raw body for diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreError {
    #[error("{message}")]
    InvalidRequest { message: String },

    #[error("{message}")]
    Unauthorized { message: String },

    #[error("{message}")]
    NotFound { message: String },

    #[error("{message}")]
    RateLimited { message: String },

    #[error("provider error {status}: {message}")]
    ProviderError {
        status: u16,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        raw: Option<Value>,
    },

    #[error("{message}")]
    Io { message: String },

    #[error("{message}")]
    Stream { message: String },

    #[error("{message}")]
    Serde { message: String },

    #[error("{message}")]
    Other { message: String },
}

impl CoreError {
    /// Suggested HTTP status for this error (`DESIGN.md` §15.1).
    ///
    /// `ProviderError` echoes the upstream status; `Io`/`Stream` map to 502 (bad gateway);
    /// decode/fallback faults map to 500.
    pub fn status_code(&self) -> u16 {
        match self {
            Self::InvalidRequest { .. } => 400,
            Self::Unauthorized { .. } => 401,
            Self::NotFound { .. } => 404,
            Self::RateLimited { .. } => 429,
            Self::ProviderError { status, .. } => *status,
            Self::Io { .. } | Self::Stream { .. } => 502,
            Self::Serde { .. } | Self::Other { .. } => 500,
        }
    }

    /// Stable machine kind string for this error (e.g. `"rate_limited"`); the single source of
    /// truth for kind names used by SDKs/proxies to map onto their own error types (`DESIGN.md` §15.1).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "invalid_request",
            Self::Unauthorized { .. } => "unauthorized",
            Self::NotFound { .. } => "not_found",
            Self::RateLimited { .. } => "rate_limited",
            Self::ProviderError { .. } => "provider_error",
            Self::Io { .. } => "io",
            Self::Stream { .. } => "stream",
            Self::Serde { .. } => "serde",
            Self::Other { .. } => "other",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes() {
        assert_eq!(
            CoreError::InvalidRequest {
                message: "x".into()
            }
            .status_code(),
            400
        );
        assert_eq!(
            CoreError::Unauthorized {
                message: "x".into()
            }
            .status_code(),
            401
        );
        assert_eq!(
            CoreError::NotFound {
                message: "x".into()
            }
            .status_code(),
            404
        );
        assert_eq!(
            CoreError::RateLimited {
                message: "x".into()
            }
            .status_code(),
            429
        );
        assert_eq!(
            CoreError::ProviderError {
                status: 503,
                message: "x".into(),
                raw: None
            }
            .status_code(),
            503
        );
        assert_eq!(
            CoreError::Io {
                message: "x".into()
            }
            .status_code(),
            502
        );
        assert_eq!(
            CoreError::Stream {
                message: "x".into()
            }
            .status_code(),
            502
        );
        assert_eq!(
            CoreError::Serde {
                message: "x".into()
            }
            .status_code(),
            500
        );
        assert_eq!(
            CoreError::Other {
                message: "x".into()
            }
            .status_code(),
            500
        );
    }
}
