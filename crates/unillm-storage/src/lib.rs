//! unillm-storage: pluggable storage backends for the proxy (`DESIGN.md` §11).
//!
//! The proxy depends only on the storage sub-traits ([`KeyStore`], [`ModelStore`], [`RouteStore`]);
//! each has a swappable backend. This crate ships the SQLite backend (`SqliteStore`) and the
//! migration runner. The Postgres backend and the request-log/usage tables arrive in M4.5.

pub mod error;
pub mod keys;
pub mod migrate;
pub mod model;
pub mod rate_limit;
pub mod sqlite;
pub mod store;

pub use error::StoreError;
pub use keys::{generate_secret, hash_secret, key_prefix};
pub use model::{
    FallbackTarget, ModelRow, NewModel, NewRoute, NewVirtualKey, RouteRow, VirtualKey,
};
pub use rate_limit::{
    DenyReason, InMemoryRateLimiter, KeyLimits, RateDecision, RateHeaders, RateLimiter,
    TokenActual, TokenEstimate,
};
pub use sqlite::SqliteStore;
pub use store::{KeyStore, ModelStore, RouteStore};
