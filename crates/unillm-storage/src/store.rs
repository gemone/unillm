//! Storage sub-traits (`DESIGN.md` §11.1). One trait per concern so each can have its own backend;
//! the proxy holds `Arc<dyn _>` for each.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::StoreError;
use crate::model::{
    GroupBy, ModelRow, NewModel, NewRequestLog, NewRoute, NewUsage, NewVirtualKey, RequestLog,
    RouteRow, UpdateKey, UsageBucket, VirtualKey,
};

/// Persistence for virtual keys (`virtual_keys`).
#[async_trait]
pub trait KeyStore: Send + Sync {
    async fn create_key(&self, new: NewVirtualKey) -> Result<VirtualKey, StoreError>;
    /// Look up a key by its hash (the auth path).
    async fn find_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError>;
    async fn list_keys(&self, tenant: Option<Uuid>) -> Result<Vec<VirtualKey>, StoreError>;
    /// Fetch a key by id (admin read-merge-write; never returns the secret — only the hash).
    async fn get_key(&self, id: Uuid) -> Result<Option<VirtualKey>, StoreError>;
    /// Apply a partial update (`DESIGN.md` §10.6). Returns the updated row; `NotFound` if missing.
    async fn update_key(&self, id: Uuid, update: UpdateKey) -> Result<VirtualKey, StoreError>;
    /// Mark a key revoked (`revoked_at = now`). `NotFound` if the id does not exist.
    async fn revoke_key(&self, id: Uuid) -> Result<(), StoreError>;
}

/// Persistence for the model catalog (`models`).
#[async_trait]
pub trait ModelStore: Send + Sync {
    /// Insert or update by `(provider, native_model)`.
    async fn upsert_model(&self, new: NewModel) -> Result<ModelRow, StoreError>;
    async fn get_model(
        &self,
        provider: &str,
        native_model: &str,
    ) -> Result<Option<ModelRow>, StoreError>;
    async fn list_models(&self) -> Result<Vec<ModelRow>, StoreError>;
    /// Remove a model by `(provider, native_model)`. `NotFound` if absent.
    async fn delete_model(&self, provider: &str, native_model: &str) -> Result<(), StoreError>;
}

/// Persistence for routing rules (`routes`).
#[async_trait]
pub trait RouteStore: Send + Sync {
    /// Insert or replace by `(alias, tenant_id)`.
    async fn upsert_route(&self, new: NewRoute) -> Result<RouteRow, StoreError>;
    /// Resolve the best route for an alias: a tenant-scoped row if present, else the global default.
    async fn resolve(
        &self,
        alias: &str,
        tenant: Option<Uuid>,
    ) -> Result<Option<RouteRow>, StoreError>;
    async fn list_routes(&self, tenant: Option<Uuid>) -> Result<Vec<RouteRow>, StoreError>;
    /// Remove a route by `(alias, tenant_id)`. `NotFound` if absent.
    async fn delete_route(&self, alias: &str, tenant: Option<Uuid>) -> Result<(), StoreError>;
}

/// Persistence for request logs + usage (`DESIGN.md` §11.1 logs/usage, §10.3 step 9). Writes are
/// fire-and-forget on the data plane; §16 PII hygiene is enforced by the types (no bodies stored).
#[async_trait]
pub trait LogStore: Send + Sync {
    /// Insert a request log and, when `usage` is `Some`, its associated usage row. Returns the new
    /// request-log id (also the usage PK).
    async fn insert_request_log(
        &self,
        log: NewRequestLog,
        usage: Option<NewUsage>,
    ) -> Result<Uuid, StoreError>;

    /// Paginated request logs, newest first. `key_id` filters by virtual key; `before` is a cursor
    /// (the `created_at` of the last row seen) returning rows strictly older than it; `limit` caps
    /// the page.
    async fn list_logs(
        &self,
        key_id: Option<Uuid>,
        before: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<RequestLog>, StoreError>;

    /// Aggregated usage (request count + summed tokens + cost) optionally filtered by key/model/
    /// time range, grouped by the given dimension. An ungrouped query yields a single total bucket.
    async fn usage_summary(
        &self,
        key_id: Option<Uuid>,
        model: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        group_by: Option<GroupBy>,
    ) -> Result<Vec<UsageBucket>, StoreError>;
}
