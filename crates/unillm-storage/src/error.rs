//! Storage error type.

use thiserror::Error;

/// A storage-layer error. Mapped to HTTP by the proxy where relevant.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// A referenced row was not found (maps to 404 on the data plane).
    #[error("record not found: {0}")]
    NotFound(String),
    /// A row's stored value was malformed (e.g. corrupt JSON/UUID).
    #[error("invalid stored value: {0}")]
    Invalid(String),
}
