//! SQLite backend (`DESIGN.md` §11.2 fallback/dev) for the config tables.
//!
//! UUIDs and JSON columns are stored as `TEXT` (§11.3 SQLite shapes); this module parses them back
//! to typed Rust at the row boundary. Timestamps bind via sqlx's `chrono` support (RFC3339 `TEXT`).

use async_trait::async_trait;
use chrono::Utc;
use sqlx::Row;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use uuid::Uuid;

use crate::error::StoreError;
use crate::migrate::run_sqlite;
use crate::model::{ModelRow, NewModel, NewRoute, NewVirtualKey, RouteRow, VirtualKey};
use crate::store::{KeyStore, ModelStore, RouteStore};

/// A SQLite-backed store holding a connection pool.
#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open a pool at `url` (e.g. `sqlite::memory:` or `sqlite://./unillm.db`) and apply migrations.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;
        Self::from_pool(pool).await
    }

    /// Adopt an existing pool and apply migrations (useful for tests / shared pools).
    pub async fn from_pool(pool: SqlitePool) -> Result<Self, StoreError> {
        run_sqlite(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

// --- row mapping (TEXT/JSON → typed Rust) -------------------------------------

/// Serialize `v` as JSON, falling back to an empty JSON array on failure (every call site binds an
/// array column).
fn json_str<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "[]".into())
}

fn uuid_col(row: &SqliteRow, col: &str) -> Result<Uuid, StoreError> {
    Uuid::parse_str(&row.try_get::<String, _>(col)?)
        .map_err(|e| StoreError::Invalid(format!("{col}: {e}")))
}

fn opt_uuid_col(row: &SqliteRow, col: &str) -> Result<Option<Uuid>, StoreError> {
    row.try_get::<Option<String>, _>(col)?
        .map(|s| Uuid::parse_str(&s).map_err(|e| StoreError::Invalid(format!("{col}: {e}"))))
        .transpose()
}

fn json_col<T: serde::de::DeserializeOwned>(row: &SqliteRow, col: &str) -> Result<T, StoreError> {
    serde_json::from_str(&row.try_get::<String, _>(col)?)
        .map_err(|e| StoreError::Invalid(format!("{col}: {e}")))
}

fn opt_json_col<T: serde::de::DeserializeOwned>(
    row: &SqliteRow,
    col: &str,
) -> Result<Option<T>, StoreError> {
    row.try_get::<Option<String>, _>(col)?
        .map(|s| serde_json::from_str(&s).map_err(|e| StoreError::Invalid(format!("{col}: {e}"))))
        .transpose()
}

fn key_from_row(row: &SqliteRow) -> Result<VirtualKey, StoreError> {
    Ok(VirtualKey {
        id: uuid_col(row, "id")?,
        key_hash: row.try_get("key_hash")?,
        key_prefix: row.try_get("key_prefix")?,
        tenant_id: uuid_col(row, "tenant_id")?,
        scopes: json_col(row, "scopes")?,
        model_allowlist: opt_json_col(row, "model_allowlist")?,
        budget_daily_tokens: row.try_get("budget_daily_tokens")?,
        rpm: row.try_get("rpm")?,
        tpm: row.try_get("tpm")?,
        max_concurrency: row.try_get("max_concurrency")?,
        created_at: row.try_get("created_at")?,
        expires_at: row.try_get("expires_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn model_from_row(row: &SqliteRow) -> Result<ModelRow, StoreError> {
    Ok(ModelRow {
        id: uuid_col(row, "id")?,
        provider: row.try_get("provider")?,
        native_model: row.try_get("native_model")?,
        display_name: row.try_get("display_name")?,
        context_window: row.try_get("context_window")?,
        max_output: row.try_get("max_output")?,
        price_in: row.try_get("price_in")?,
        price_out: row.try_get("price_out")?,
        price_cache_read: row.try_get("price_cache_read")?,
        enabled: row.try_get("enabled")?,
        created_at: row.try_get("created_at")?,
    })
}

fn route_from_row(row: &SqliteRow) -> Result<RouteRow, StoreError> {
    Ok(RouteRow {
        alias: row.try_get("alias")?,
        tenant_id: opt_uuid_col(row, "tenant_id")?,
        provider: row.try_get("provider")?,
        native_model: row.try_get("native_model")?,
        fallback: json_col(row, "fallback")?,
        priority: row.try_get("priority")?,
        enabled: row.try_get("enabled")?,
    })
}

// --- KeyStore ----------------------------------------------------------------

#[async_trait]
impl KeyStore for SqliteStore {
    async fn create_key(&self, new: NewVirtualKey) -> Result<VirtualKey, StoreError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let scopes = json_str(&new.scopes);
        let allow = new.model_allowlist.as_ref().map(json_str);
        sqlx::query(
            "INSERT INTO virtual_keys
               (id, key_hash, key_prefix, tenant_id, scopes, model_allowlist,
                budget_daily_tokens, rpm, tpm, max_concurrency, created_at, expires_at, revoked_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        )
        .bind(id.to_string())
        .bind(&new.key_hash)
        .bind(&new.key_prefix)
        .bind(new.tenant_id.to_string())
        .bind(&scopes)
        .bind(&allow)
        .bind(new.budget_daily_tokens)
        .bind(new.rpm)
        .bind(new.tpm)
        .bind(new.max_concurrency)
        .bind(now)
        .bind(new.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(VirtualKey {
            id,
            key_hash: new.key_hash,
            key_prefix: new.key_prefix,
            tenant_id: new.tenant_id,
            scopes: new.scopes,
            model_allowlist: new.model_allowlist,
            budget_daily_tokens: new.budget_daily_tokens,
            rpm: new.rpm,
            tpm: new.tpm,
            max_concurrency: new.max_concurrency,
            created_at: now,
            expires_at: new.expires_at,
            revoked_at: None,
        })
    }

    async fn find_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError> {
        let row = sqlx::query("SELECT * FROM virtual_keys WHERE key_hash = ?")
            .bind(key_hash)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(key_from_row).transpose()
    }

    async fn list_keys(&self, tenant: Option<Uuid>) -> Result<Vec<VirtualKey>, StoreError> {
        let rows = match tenant {
            Some(t) => {
                sqlx::query("SELECT * FROM virtual_keys WHERE tenant_id = ? ORDER BY created_at")
                    .bind(t.to_string())
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("SELECT * FROM virtual_keys ORDER BY created_at")
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.iter().map(key_from_row).collect()
    }

    async fn revoke_key(&self, id: Uuid) -> Result<(), StoreError> {
        let res = sqlx::query("UPDATE virtual_keys SET revoked_at = ? WHERE id = ?")
            .bind(Utc::now())
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        if res.rows_affected() == 0 {
            Err(StoreError::NotFound(format!("key {id}")))
        } else {
            Ok(())
        }
    }
}

// --- ModelStore ---------------------------------------------------------------

#[async_trait]
impl ModelStore for SqliteStore {
    async fn upsert_model(&self, new: NewModel) -> Result<ModelRow, StoreError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO models
               (id, provider, native_model, display_name, context_window, max_output,
                price_in, price_out, price_cache_read, enabled, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(provider, native_model) DO UPDATE SET
               display_name = excluded.display_name,
               context_window = excluded.context_window,
               max_output    = excluded.max_output,
               price_in      = excluded.price_in,
               price_out     = excluded.price_out,
               price_cache_read = excluded.price_cache_read,
               enabled       = excluded.enabled",
        )
        .bind(id.to_string())
        .bind(&new.provider)
        .bind(&new.native_model)
        .bind(&new.display_name)
        .bind(new.context_window)
        .bind(new.max_output)
        .bind(new.price_in)
        .bind(new.price_out)
        .bind(new.price_cache_read)
        .bind(new.enabled)
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.get_model(&new.provider, &new.native_model)
            .await?
            .ok_or_else(|| StoreError::Invalid("upsert did not produce a row".into()))
    }

    async fn get_model(
        &self,
        provider: &str,
        native_model: &str,
    ) -> Result<Option<ModelRow>, StoreError> {
        let row = sqlx::query("SELECT * FROM models WHERE provider = ? AND native_model = ?")
            .bind(provider)
            .bind(native_model)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(model_from_row).transpose()
    }

    async fn list_models(&self) -> Result<Vec<ModelRow>, StoreError> {
        let rows = sqlx::query("SELECT * FROM models ORDER BY provider, native_model")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(model_from_row).collect()
    }
}

// --- RouteStore ---------------------------------------------------------------

#[async_trait]
impl RouteStore for SqliteStore {
    async fn upsert_route(&self, new: NewRoute) -> Result<RouteRow, StoreError> {
        let fallback = json_str(&new.fallback);
        // Replace any existing row for this (alias, tenant_id). NULL tenant_id compares via IS NULL.
        match new.tenant_id {
            Some(t) => {
                sqlx::query("DELETE FROM routes WHERE alias = ? AND tenant_id = ?")
                    .bind(&new.alias)
                    .bind(t.to_string())
                    .execute(&self.pool)
                    .await?;
            }
            None => {
                sqlx::query("DELETE FROM routes WHERE alias = ? AND tenant_id IS NULL")
                    .bind(&new.alias)
                    .execute(&self.pool)
                    .await?;
            }
        }
        sqlx::query(
            "INSERT INTO routes
               (alias, tenant_id, provider, native_model, fallback, priority, enabled)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&new.alias)
        .bind(new.tenant_id.map(|t| t.to_string()))
        .bind(&new.provider)
        .bind(&new.native_model)
        .bind(&fallback)
        .bind(new.priority)
        .bind(new.enabled)
        .execute(&self.pool)
        .await?;
        self.resolve(&new.alias, new.tenant_id)
            .await?
            .ok_or_else(|| StoreError::Invalid("upsert did not produce a row".into()))
    }

    async fn resolve(
        &self,
        alias: &str,
        tenant: Option<Uuid>,
    ) -> Result<Option<RouteRow>, StoreError> {
        let tenant_row = match tenant {
            Some(t) => {
                sqlx::query(
                    "SELECT * FROM routes
                     WHERE alias = ? AND tenant_id = ? AND enabled = 1
                     ORDER BY priority LIMIT 1",
                )
                .bind(alias)
                .bind(t.to_string())
                .fetch_optional(&self.pool)
                .await?
            }
            None => None,
        };
        let row = match tenant_row {
            Some(r) => Some(r),
            None => {
                sqlx::query(
                    "SELECT * FROM routes
                     WHERE alias = ? AND tenant_id IS NULL AND enabled = 1
                     ORDER BY priority LIMIT 1",
                )
                .bind(alias)
                .fetch_optional(&self.pool)
                .await?
            }
        };
        row.as_ref().map(route_from_row).transpose()
    }

    async fn list_routes(&self, tenant: Option<Uuid>) -> Result<Vec<RouteRow>, StoreError> {
        let rows = match tenant {
            Some(t) => {
                sqlx::query("SELECT * FROM routes WHERE tenant_id = ? ORDER BY alias, priority")
                    .bind(t.to_string())
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("SELECT * FROM routes ORDER BY alias, priority")
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.iter().map(route_from_row).collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::model::FallbackTarget;

    #[test]
    fn fallback_target_round_trips_json() {
        let f = FallbackTarget {
            provider: "openai".into(),
            native_model: "gpt-4o".into(),
        };
        let s = serde_json::to_string(&vec![f.clone()]).unwrap();
        let back: Vec<FallbackTarget> = serde_json::from_str(&s).unwrap();
        assert_eq!(back, vec![f]);
    }
}
