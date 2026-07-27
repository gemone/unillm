//! unillm-storage: pluggable storage backends for the proxy (`DESIGN.md` §11).
//!
//! The proxy depends only on the storage sub-traits ([`KeyStore`], [`ModelStore`], [`RouteStore`],
//! [`LogStore`], [`CacheStore`]); each has a swappable backend. This crate ships the SQLite backend
//! ([`SqliteStore`]) and the migration runner; the PostgreSQL backend ([`PostgresStore`]) is
//! available with the `postgres` feature. The exact-hash response cache ([`InMemoryCache`]) is
//! in-process now; Redis/DB are the production primaries behind the same [`CacheStore`] trait.

pub mod cache;
pub mod error;
pub mod keys;
pub mod migrate;
pub mod model;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod rate_limit;
pub mod sqlite;
pub mod store;

pub use cache::{CacheStore, InMemoryCache};
pub use error::StoreError;
pub use keys::{generate_secret, hash_secret, key_prefix};
pub use model::{
    FallbackTarget, GroupBy, ModelRow, NewModel, NewRequestLog, NewRoute, NewUsage, NewVirtualKey,
    RequestLog, RouteRow, UpdateKey, UsageBucket, VirtualKey,
};
#[cfg(feature = "postgres")]
pub use postgres::PostgresStore;
pub use rate_limit::{
    DenyReason, InMemoryRateLimiter, KeyLimits, RateDecision, RateHeaders, RateLimiter,
    TokenActual, TokenEstimate,
};
pub use sqlite::SqliteStore;
pub use store::{KeyStore, LogStore, ModelStore, RouteStore};
