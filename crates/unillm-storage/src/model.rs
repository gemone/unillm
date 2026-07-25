//! Domain types for the config tables (`DESIGN.md` §11.3).
//!
//! These are storage-level types (provider is a `String`, the snake_case serialization of
//! `unillm_core::ProviderId`); the proxy converts to/from the core `ProviderId` at its boundary.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A fallback target in a route's chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallbackTarget {
    pub provider: String,
    pub native_model: String,
}

/// A virtual API key row (`DESIGN.md` §11.3 `virtual_keys`). The secret is never stored — only the
/// `key_hash` (computed by the caller, M4.2: SHA-256 + pepper) and `key_prefix` for display/lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualKey {
    pub id: Uuid,
    pub key_hash: String,
    pub key_prefix: String,
    pub tenant_id: Uuid,
    pub scopes: Vec<String>,
    pub model_allowlist: Option<Vec<String>>,
    pub budget_daily_tokens: Option<i64>,
    pub rpm: Option<i32>,
    pub tpm: Option<i64>,
    pub max_concurrency: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl VirtualKey {
    /// Whether this key is currently usable: not revoked and not past expiry (`DESIGN.md` §13.1).
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|e| e > now)
    }
}

/// Input for creating a virtual key. The caller supplies `key_hash`/`key_prefix` (derived from the
/// raw secret); the store assigns `id` and `created_at`.
#[derive(Debug, Clone)]
pub struct NewVirtualKey {
    pub key_hash: String,
    pub key_prefix: String,
    pub tenant_id: Uuid,
    pub scopes: Vec<String>,
    pub model_allowlist: Option<Vec<String>>,
    pub budget_daily_tokens: Option<i64>,
    pub rpm: Option<i32>,
    pub tpm: Option<i64>,
    pub max_concurrency: Option<i32>,
    pub expires_at: Option<DateTime<Utc>>,
}

/// A model-catalog row (`DESIGN.md` §11.3 `models`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRow {
    pub id: Uuid,
    pub provider: String,
    pub native_model: String,
    pub display_name: String,
    pub context_window: Option<i32>,
    pub max_output: Option<i32>,
    pub price_in: Option<f64>,
    pub price_out: Option<f64>,
    pub price_cache_read: Option<f64>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

/// Input for upserting a model. The store assigns `id`/`created_at`.
#[derive(Debug, Clone)]
pub struct NewModel {
    pub provider: String,
    pub native_model: String,
    pub display_name: String,
    pub context_window: Option<i32>,
    pub max_output: Option<i32>,
    pub price_in: Option<f64>,
    pub price_out: Option<f64>,
    pub price_cache_read: Option<f64>,
    pub enabled: bool,
}

/// A routing rule (`DESIGN.md` §11.3 `routes`). `tenant_id` `None` = global default.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRow {
    pub alias: String,
    pub tenant_id: Option<Uuid>,
    pub provider: String,
    pub native_model: String,
    pub fallback: Vec<FallbackTarget>,
    pub priority: i32,
    pub enabled: bool,
}

/// Input for upserting a route.
#[derive(Debug, Clone)]
pub struct NewRoute {
    pub alias: String,
    pub tenant_id: Option<Uuid>,
    pub provider: String,
    pub native_model: String,
    pub fallback: Vec<FallbackTarget>,
    pub priority: i32,
    pub enabled: bool,
}
