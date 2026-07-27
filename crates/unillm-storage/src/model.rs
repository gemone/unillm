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

/// A partial update to a virtual key (`DESIGN.md` §10.6 `PATCH /admin/keys/:id`). Each field is
/// applied only when `Some`; `None` leaves the column unchanged. `revoked = Some(true)` marks the
/// key revoked (`Some(false)` clears it).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateKey {
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub model_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub budget_daily_tokens: Option<i64>,
    #[serde(default)]
    pub rpm: Option<i32>,
    #[serde(default)]
    pub tpm: Option<i64>,
    #[serde(default)]
    pub max_concurrency: Option<i32>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub revoked: Option<bool>,
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

/// A request-log row (`DESIGN.md` §11.3 `request_logs`). Per §16 PII hygiene, **no request or
/// response bodies are stored** — only metadata + sizes; token usage lives in the associated
/// `usage` row (written via [`NewUsage`]). Logging happens only for authenticated data-plane
/// requests (after §10.3 step 1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLog {
    pub id: Uuid,
    pub request_id: String,
    pub virtual_key_id: Uuid,
    pub tenant_id: Uuid,
    pub provider: String,
    pub model: String,
    pub inbound_format: String,
    pub outbound_format: String,
    /// HTTP status returned to the client.
    pub status: i16,
    /// `false` until the exact-hash cache lands (M5).
    pub cached: bool,
    pub latency_ms: Option<i32>,
    pub created_at: DateTime<Utc>,
}

/// Input for inserting a request log. The store assigns `id`/`created_at`.
#[derive(Debug, Clone)]
pub struct NewRequestLog {
    pub request_id: String,
    pub virtual_key_id: Uuid,
    pub tenant_id: Uuid,
    pub provider: String,
    pub model: String,
    pub inbound_format: String,
    pub outbound_format: String,
    pub status: i16,
    /// `true` when the response was served from the exact-hash cache (M5) rather than the upstream.
    pub cached: bool,
    pub latency_ms: Option<i32>,
}

/// Actual token usage to record alongside a request log (`DESIGN.md` §13.5).
#[derive(Debug, Clone, Copy, Default)]
pub struct NewUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read: i64,
    pub cache_creation: i64,
    pub cost_usd: Option<f64>,
}

/// Aggregation dimension for usage analytics (`DESIGN.md` §13.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupBy {
    /// Group by virtual key.
    Key,
    /// Group by requested model.
    Model,
    /// Group by upstream provider.
    Provider,
    /// Group by calendar day (UTC).
    Day,
}

impl GroupBy {
    /// Parse the `group_by` query-param value (`key`/`model`/`provider`/`day`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "key" => Some(Self::Key),
            "model" => Some(Self::Model),
            "provider" => Some(Self::Provider),
            "day" => Some(Self::Day),
            _ => None,
        }
    }
}

/// One row of an aggregated usage query (`DESIGN.md` §13.5). `key` is the grouping value
/// (`virtual_key_id` / `model` / `provider` / `day`, ISO date); `None` for an ungrouped total.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageBucket {
    pub key: Option<String>,
    pub requests: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read: i64,
    pub cache_creation: i64,
    pub cost_usd: Option<f64>,
}
