//! Storage sub-traits (`DESIGN.md` §11.1). One trait per concern so each can have its own backend;
//! the proxy holds `Arc<dyn _>` for each.

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::StoreError;
use crate::model::{ModelRow, NewModel, NewRoute, NewVirtualKey, RouteRow, VirtualKey};

/// Persistence for virtual keys (`virtual_keys`).
#[async_trait]
pub trait KeyStore: Send + Sync {
    async fn create_key(&self, new: NewVirtualKey) -> Result<VirtualKey, StoreError>;
    /// Look up a key by its hash (the auth path).
    async fn find_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError>;
    async fn list_keys(&self, tenant: Option<Uuid>) -> Result<Vec<VirtualKey>, StoreError>;
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
}
