//! SQLite backend (`DESIGN.md` §11.2 fallback/dev) for the config tables.
//!
//! UUIDs and JSON columns are stored as `TEXT` (§11.3 SQLite shapes); this module parses them back
//! to typed Rust at the row boundary. Timestamps bind via sqlx's `chrono` support (RFC3339 `TEXT`).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use uuid::Uuid;

use crate::error::StoreError;
use crate::migrate::run_sqlite;
use crate::model::{
    GroupBy, ModelRow, NewModel, NewRequestLog, NewRoute, NewUsage, NewVirtualKey, RequestLog,
    RouteRow, UpdateKey, UsageBucket, VirtualKey,
};
use crate::store::{KeyStore, LogStore, ModelStore, RouteStore};

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

    async fn get_key(&self, id: Uuid) -> Result<Option<VirtualKey>, StoreError> {
        let row = sqlx::query("SELECT * FROM virtual_keys WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(key_from_row).transpose()
    }

    async fn update_key(&self, id: Uuid, update: UpdateKey) -> Result<VirtualKey, StoreError> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("UPDATE virtual_keys SET ");
        let mut sep = "";
        if let Some(scopes) = &update.scopes {
            qb.push(sep).push(" scopes = ").push_bind(json_str(scopes));
            sep = ", ";
        }
        if let Some(allow) = &update.model_allowlist {
            qb.push(sep)
                .push(" model_allowlist = ")
                .push_bind(json_str(allow));
            sep = ", ";
        }
        if let Some(b) = update.budget_daily_tokens {
            qb.push(sep).push(" budget_daily_tokens = ").push_bind(b);
            sep = ", ";
        }
        if let Some(rpm) = update.rpm {
            qb.push(sep).push(" rpm = ").push_bind(rpm);
            sep = ", ";
        }
        if let Some(tpm) = update.tpm {
            qb.push(sep).push(" tpm = ").push_bind(tpm);
            sep = ", ";
        }
        if let Some(c) = update.max_concurrency {
            qb.push(sep).push(" max_concurrency = ").push_bind(c);
            sep = ", ";
        }
        if let Some(exp) = update.expires_at {
            qb.push(sep).push(" expires_at = ").push_bind(exp);
            sep = ", ";
        }
        if let Some(revoked) = update.revoked {
            let when: Option<DateTime<Utc>> = if revoked { Some(Utc::now()) } else { None };
            qb.push(sep).push(" revoked_at = ").push_bind(when);
            sep = ", ";
        }
        if sep.is_empty() {
            // No fields to update.
            return self
                .get_key(id)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("key {id}")));
        }
        qb.push(" WHERE id = ").push_bind(id.to_string());
        let res = qb.build().execute(&self.pool).await?;
        if res.rows_affected() == 0 {
            return Err(StoreError::NotFound(format!("key {id}")));
        }
        self.get_key(id)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("key {id}")))
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

    async fn delete_model(&self, provider: &str, native_model: &str) -> Result<(), StoreError> {
        let res = sqlx::query("DELETE FROM models WHERE provider = ? AND native_model = ?")
            .bind(provider)
            .bind(native_model)
            .execute(&self.pool)
            .await?;
        if res.rows_affected() == 0 {
            Err(StoreError::NotFound(format!(
                "model {provider}/{native_model}"
            )))
        } else {
            Ok(())
        }
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

    async fn delete_route(&self, alias: &str, tenant: Option<Uuid>) -> Result<(), StoreError> {
        let res = match tenant {
            Some(t) => {
                sqlx::query("DELETE FROM routes WHERE alias = ? AND tenant_id = ?")
                    .bind(alias)
                    .bind(t.to_string())
                    .execute(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("DELETE FROM routes WHERE alias = ? AND tenant_id IS NULL")
                    .bind(alias)
                    .execute(&self.pool)
                    .await?
            }
        };
        if res.rows_affected() == 0 {
            Err(StoreError::NotFound(format!("route {alias}")))
        } else {
            Ok(())
        }
    }
}

// --- LogStore ----------------------------------------------------------------

fn log_from_row(row: &SqliteRow) -> Result<RequestLog, StoreError> {
    Ok(RequestLog {
        id: uuid_col(row, "id")?,
        request_id: row.try_get("request_id")?,
        virtual_key_id: uuid_col(row, "virtual_key_id")?,
        tenant_id: uuid_col(row, "tenant_id")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        inbound_format: row.try_get("inbound_format")?,
        outbound_format: row.try_get("outbound_format")?,
        status: row.try_get("status")?,
        cached: row.try_get::<i64, _>("cached")? != 0,
        latency_ms: row.try_get("latency_ms")?,
        created_at: row.try_get("created_at")?,
    })
}

#[async_trait]
impl LogStore for SqliteStore {
    async fn insert_request_log(
        &self,
        log: NewRequestLog,
        usage: Option<NewUsage>,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO request_logs
               (id, request_id, virtual_key_id, tenant_id, provider, model,
                inbound_format, outbound_format, status, cached, latency_ms, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(&log.request_id)
        .bind(log.virtual_key_id.to_string())
        .bind(log.tenant_id.to_string())
        .bind(&log.provider)
        .bind(&log.model)
        .bind(&log.inbound_format)
        .bind(&log.outbound_format)
        .bind(log.status)
        .bind(log.cached)
        .bind(log.latency_ms)
        .bind(now)
        .execute(&self.pool)
        .await?;

        if let Some(u) = usage {
            sqlx::query(
                "INSERT INTO usage
                   (request_log_id, input_tokens, output_tokens, cache_read, cache_creation, cost_usd)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(u.input_tokens)
            .bind(u.output_tokens)
            .bind(u.cache_read)
            .bind(u.cache_creation)
            .bind(u.cost_usd)
            .execute(&self.pool)
            .await?;
        }
        Ok(id)
    }

    async fn list_logs(
        &self,
        key_id: Option<Uuid>,
        before: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<RequestLog>, StoreError> {
        // QueryBuilder keeps every value bound (never interpolated) while letting filters compose.
        let mut qb =
            sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT * FROM request_logs WHERE 1=1");
        if let Some(k) = key_id {
            qb.push(" AND virtual_key_id = ").push_bind(k.to_string());
        }
        if let Some(b) = before {
            qb.push(" AND created_at < ").push_bind(b);
        }
        qb.push(" ORDER BY created_at DESC LIMIT ").push_bind(limit);
        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter().map(log_from_row).collect()
    }

    async fn usage_summary(
        &self,
        key_id: Option<Uuid>,
        model: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        group_by: Option<GroupBy>,
    ) -> Result<Vec<UsageBucket>, StoreError> {
        // The group expression is chosen from a fixed enum (never user text), so pushing it is safe;
        // all filter values bind as parameters.
        let group_expr: Option<&'static str> = match group_by {
            Some(GroupBy::Key) => Some("l.virtual_key_id"),
            Some(GroupBy::Model) => Some("l.model"),
            Some(GroupBy::Provider) => Some("l.provider"),
            Some(GroupBy::Day) => Some("substr(l.created_at, 1, 10)"),
            None => None,
        };
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT ");
        if let Some(g) = group_expr {
            qb.push(g);
        } else {
            qb.push("NULL");
        }
        qb.push(
            " AS k, COUNT(*) AS requests,
              COALESCE(SUM(u.input_tokens), 0)  AS input_tokens,
              COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
              COALESCE(SUM(u.cache_read), 0)    AS cache_read,
              COALESCE(SUM(u.cache_creation), 0) AS cache_creation,
              SUM(u.cost_usd)                   AS cost_usd
             FROM request_logs l LEFT JOIN usage u ON u.request_log_id = l.id WHERE 1=1",
        );
        if let Some(k) = key_id {
            qb.push(" AND l.virtual_key_id = ").push_bind(k.to_string());
        }
        if let Some(m) = model {
            qb.push(" AND l.model = ").push_bind(m);
        }
        if let Some(f) = from {
            qb.push(" AND l.created_at >= ").push_bind(f);
        }
        if let Some(t) = to {
            qb.push(" AND l.created_at < ").push_bind(t);
        }
        if let Some(g) = group_expr {
            qb.push(" GROUP BY ").push(g).push(" ORDER BY ").push(g);
        }
        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                Ok(UsageBucket {
                    key: row.try_get::<Option<String>, _>("k")?,
                    requests: row.try_get("requests")?,
                    input_tokens: row.try_get("input_tokens")?,
                    output_tokens: row.try_get("output_tokens")?,
                    cache_read: row.try_get("cache_read")?,
                    cache_creation: row.try_get("cache_creation")?,
                    cost_usd: row.try_get("cost_usd")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::SqliteStore;
    use crate::model::{FallbackTarget, GroupBy, NewRequestLog, NewUsage};
    use crate::store::LogStore;

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

    fn log(req: &str, key: Uuid, tenant: Uuid, model: &str, status: i16) -> NewRequestLog {
        NewRequestLog {
            request_id: req.into(),
            virtual_key_id: key,
            tenant_id: tenant,
            provider: "openai".into(),
            model: model.into(),
            inbound_format: "openai_chat".into(),
            outbound_format: "openai_chat".into(),
            status,
            cached: false,
            latency_ms: Some(42),
        }
    }

    #[tokio::test]
    async fn log_store_inserts_lists_and_aggregates() {
        let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let tenant = Uuid::new_v4();

        // key A: one success with usage, one error without usage (same model).
        store
            .insert_request_log(
                log("req-1", a, tenant, "gpt-4o", 200),
                Some(NewUsage {
                    input_tokens: 100,
                    output_tokens: 50,
                    cache_read: 0,
                    cache_creation: 0,
                    cost_usd: Some(0.001),
                }),
            )
            .await
            .unwrap();
        store
            .insert_request_log(log("req-2", a, tenant, "gpt-4o", 500), None)
            .await
            .unwrap();
        // key B: one success with a different model.
        store
            .insert_request_log(
                log("req-3", b, tenant, "claude-3", 200),
                Some(NewUsage {
                    input_tokens: 200,
                    output_tokens: 100,
                    cache_read: 10,
                    cache_creation: 0,
                    cost_usd: Some(0.002),
                }),
            )
            .await
            .unwrap();

        // list_logs filters by key.
        assert_eq!(store.list_logs(Some(a), None, 10).await.unwrap().len(), 2);
        assert_eq!(store.list_logs(Some(b), None, 10).await.unwrap().len(), 1);
        assert_eq!(store.list_logs(None, None, 10).await.unwrap().len(), 3);

        // Ungrouped usage for key A: 2 requests, only req-1 had tokens.
        let total = store
            .usage_summary(Some(a), None, None, None, None)
            .await
            .unwrap();
        assert_eq!(total.len(), 1);
        assert_eq!(total[0].requests, 2);
        assert_eq!(total[0].input_tokens, 100);
        assert_eq!(total[0].output_tokens, 50);
        assert_eq!(total[0].cost_usd, Some(0.001));
        assert_eq!(total[0].key, None);

        // Grouped by model across all keys: two distinct models.
        let by_model = store
            .usage_summary(None, None, None, None, Some(GroupBy::Model))
            .await
            .unwrap();
        assert_eq!(by_model.len(), 2);
        let claude = by_model
            .iter()
            .find(|x| x.key.as_deref() == Some("claude-3"))
            .unwrap();
        assert_eq!(claude.input_tokens, 200);
        assert_eq!(claude.cache_read, 10);

        // Filter by model.
        let gpt = store
            .usage_summary(None, Some("gpt-4o"), None, None, None)
            .await
            .unwrap();
        assert_eq!(gpt.len(), 1);
        assert_eq!(gpt[0].requests, 2);
    }
}
