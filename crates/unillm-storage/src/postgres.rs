//! PostgreSQL backend (`DESIGN.md` §11.2 primary) — mirrors [`crate::SqliteStore`] against native
//! PG types. Available with the `postgres` feature. JSON columns bind/decode via `sqlx::types::Json`
//! (JSONB); UUID/BOOLEAN/NUMERIC/TIMESTAMPTZ are native. Static SQL uses `$N` placeholders;
//! dynamic SQL (filters, partial key update) uses `QueryBuilder`, which numbers placeholders per
//! backend.

use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::types::Json as SqlxJson;
use uuid::Uuid;

use crate::error::StoreError;
use crate::migrate::run_postgres;
use crate::model::{
    GroupBy, ModelRow, NewModel, NewRequestLog, NewRoute, NewUsage, NewVirtualKey, RequestLog,
    RouteRow, UpdateKey, UsageBucket, VirtualKey,
};
use crate::store::{KeyStore, LogStore, ModelStore, RouteStore};

/// A PostgreSQL-backed store holding a connection pool.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Open a pool at `url` and apply migrations.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        Self::from_pool(pool).await
    }

    /// Adopt an existing pool and apply migrations.
    pub async fn from_pool(pool: PgPool) -> Result<Self, StoreError> {
        run_postgres(&pool).await?;
        Ok(Self { pool })
    }
}

// --- row mapping --------------------------------------------------------------

fn json_col<T: serde::de::DeserializeOwned>(row: &PgRow, col: &str) -> Result<T, StoreError> {
    Ok(row.try_get::<SqlxJson<T>, _>(col)?.0)
}

fn opt_json_col<T: serde::de::DeserializeOwned>(
    row: &PgRow,
    col: &str,
) -> Result<Option<T>, StoreError> {
    Ok(row.try_get::<Option<SqlxJson<T>>, _>(col)?.map(|j| j.0))
}

fn key_from_row(row: &PgRow) -> Result<VirtualKey, StoreError> {
    Ok(VirtualKey {
        id: row.try_get("id")?,
        key_hash: row.try_get("key_hash")?,
        key_prefix: row.try_get("key_prefix")?,
        tenant_id: row.try_get("tenant_id")?,
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

fn model_from_row(row: &PgRow) -> Result<ModelRow, StoreError> {
    Ok(ModelRow {
        id: row.try_get("id")?,
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

fn route_from_row(row: &PgRow) -> Result<RouteRow, StoreError> {
    Ok(RouteRow {
        alias: row.try_get("alias")?,
        tenant_id: row.try_get("tenant_id")?,
        provider: row.try_get("provider")?,
        native_model: row.try_get("native_model")?,
        fallback: json_col(row, "fallback")?,
        priority: row.try_get("priority")?,
        enabled: row.try_get("enabled")?,
    })
}

fn log_from_row(row: &PgRow) -> Result<RequestLog, StoreError> {
    Ok(RequestLog {
        id: row.try_get("id")?,
        request_id: row.try_get("request_id")?,
        virtual_key_id: row.try_get("virtual_key_id")?,
        tenant_id: row.try_get("tenant_id")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        inbound_format: row.try_get("inbound_format")?,
        outbound_format: row.try_get("outbound_format")?,
        status: row.try_get("status")?,
        cached: row.try_get("cached")?,
        latency_ms: row.try_get("latency_ms")?,
        created_at: row.try_get("created_at")?,
    })
}

// --- KeyStore -----------------------------------------------------------------

#[async_trait::async_trait]
impl KeyStore for PostgresStore {
    async fn create_key(&self, new: NewVirtualKey) -> Result<VirtualKey, StoreError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let row = sqlx::query(
            "INSERT INTO virtual_keys
               (id, key_hash, key_prefix, tenant_id, scopes, model_allowlist,
                budget_daily_tokens, rpm, tpm, max_concurrency, created_at, expires_at, revoked_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL)
             RETURNING *",
        )
        .bind(id)
        .bind(&new.key_hash)
        .bind(&new.key_prefix)
        .bind(new.tenant_id)
        .bind(SqlxJson(&new.scopes))
        .bind(new.model_allowlist.as_ref().map(SqlxJson))
        .bind(new.budget_daily_tokens)
        .bind(new.rpm)
        .bind(new.tpm)
        .bind(new.max_concurrency)
        .bind(now)
        .bind(new.expires_at)
        .fetch_one(&self.pool)
        .await?;
        key_from_row(&row)
    }

    async fn find_by_hash(&self, key_hash: &str) -> Result<Option<VirtualKey>, StoreError> {
        let row = sqlx::query("SELECT * FROM virtual_keys WHERE key_hash = $1")
            .bind(key_hash)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(key_from_row).transpose()
    }

    async fn list_keys(&self, tenant: Option<Uuid>) -> Result<Vec<VirtualKey>, StoreError> {
        let rows = match tenant {
            Some(t) => {
                sqlx::query("SELECT * FROM virtual_keys WHERE tenant_id = $1 ORDER BY created_at")
                    .bind(t)
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
        let row = sqlx::query("SELECT * FROM virtual_keys WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(key_from_row).transpose()
    }

    async fn update_key(&self, id: Uuid, update: UpdateKey) -> Result<VirtualKey, StoreError> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("UPDATE virtual_keys SET ");
        let mut sep = "";
        if let Some(scopes) = &update.scopes {
            qb.push(sep).push(" scopes = ").push_bind(scopes.clone());
            sep = ", ";
        }
        if let Some(allow) = &update.model_allowlist {
            qb.push(sep)
                .push(" model_allowlist = ")
                .push_bind(allow.clone());
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
            return self
                .get_key(id)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("key {id}")));
        }
        qb.push(" WHERE id = ").push_bind(id);
        let res = qb.build().execute(&self.pool).await?;
        if res.rows_affected() == 0 {
            return Err(StoreError::NotFound(format!("key {id}")));
        }
        self.get_key(id)
            .await?
            .ok_or_else(|| StoreError::NotFound(format!("key {id}")))
    }

    async fn revoke_key(&self, id: Uuid) -> Result<(), StoreError> {
        let res = sqlx::query("UPDATE virtual_keys SET revoked_at = $1 WHERE id = $2")
            .bind(Utc::now())
            .bind(id)
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

#[async_trait::async_trait]
impl ModelStore for PostgresStore {
    async fn upsert_model(&self, new: NewModel) -> Result<ModelRow, StoreError> {
        let row = sqlx::query(
            "INSERT INTO models
               (id, provider, native_model, display_name, context_window, max_output,
                price_in, price_out, price_cache_read, enabled, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (provider, native_model) DO UPDATE SET
               display_name    = EXCLUDED.display_name,
               context_window  = EXCLUDED.context_window,
               max_output      = EXCLUDED.max_output,
               price_in        = EXCLUDED.price_in,
               price_out       = EXCLUDED.price_out,
               price_cache_read = EXCLUDED.price_cache_read,
               enabled         = EXCLUDED.enabled
             RETURNING *",
        )
        .bind(Uuid::new_v4())
        .bind(&new.provider)
        .bind(&new.native_model)
        .bind(&new.display_name)
        .bind(new.context_window)
        .bind(new.max_output)
        .bind(new.price_in)
        .bind(new.price_out)
        .bind(new.price_cache_read)
        .bind(new.enabled)
        .bind(Utc::now())
        .fetch_one(&self.pool)
        .await?;
        model_from_row(&row)
    }

    async fn get_model(
        &self,
        provider: &str,
        native_model: &str,
    ) -> Result<Option<ModelRow>, StoreError> {
        let row = sqlx::query("SELECT * FROM models WHERE provider = $1 AND native_model = $2")
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
        let res = sqlx::query("DELETE FROM models WHERE provider = $1 AND native_model = $2")
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

#[async_trait::async_trait]
impl RouteStore for PostgresStore {
    async fn upsert_route(&self, new: NewRoute) -> Result<RouteRow, StoreError> {
        // Replace any existing row for this (alias, tenant_id), mirroring the SQLite impl.
        match new.tenant_id {
            Some(t) => {
                sqlx::query("DELETE FROM routes WHERE alias = $1 AND tenant_id = $2")
                    .bind(&new.alias)
                    .bind(t)
                    .execute(&self.pool)
                    .await?;
            }
            None => {
                sqlx::query("DELETE FROM routes WHERE alias = $1 AND tenant_id IS NULL")
                    .bind(&new.alias)
                    .execute(&self.pool)
                    .await?;
            }
        }
        let row = sqlx::query(
            "INSERT INTO routes
               (alias, tenant_id, provider, native_model, fallback, priority, enabled)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING *",
        )
        .bind(&new.alias)
        .bind(new.tenant_id)
        .bind(&new.provider)
        .bind(&new.native_model)
        .bind(SqlxJson(&new.fallback))
        .bind(new.priority)
        .bind(new.enabled)
        .fetch_one(&self.pool)
        .await?;
        route_from_row(&row)
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
                     WHERE alias = $1 AND tenant_id = $2 AND enabled = TRUE
                     ORDER BY priority LIMIT 1",
                )
                .bind(alias)
                .bind(t)
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
                     WHERE alias = $1 AND tenant_id IS NULL AND enabled = TRUE
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
                sqlx::query("SELECT * FROM routes WHERE tenant_id = $1 ORDER BY alias, priority")
                    .bind(t)
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
                sqlx::query("DELETE FROM routes WHERE alias = $1 AND tenant_id = $2")
                    .bind(alias)
                    .bind(t)
                    .execute(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("DELETE FROM routes WHERE alias = $1 AND tenant_id IS NULL")
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

// --- LogStore -----------------------------------------------------------------

#[async_trait::async_trait]
impl LogStore for PostgresStore {
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
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE, $10, $11)",
        )
        .bind(id)
        .bind(&log.request_id)
        .bind(log.virtual_key_id)
        .bind(log.tenant_id)
        .bind(&log.provider)
        .bind(&log.model)
        .bind(&log.inbound_format)
        .bind(&log.outbound_format)
        .bind(log.status)
        .bind(log.latency_ms)
        .bind(now)
        .execute(&self.pool)
        .await?;

        if let Some(u) = usage {
            sqlx::query(
                "INSERT INTO usage
                   (request_log_id, input_tokens, output_tokens, cache_read, cache_creation, cost_usd)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(id)
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
        let mut qb =
            sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT * FROM request_logs WHERE 1=1");
        if let Some(k) = key_id {
            qb.push(" AND virtual_key_id = ").push_bind(k);
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
        let group_expr: Option<&'static str> = match group_by {
            Some(GroupBy::Key) => Some("l.virtual_key_id::text"),
            Some(GroupBy::Model) => Some("l.model"),
            Some(GroupBy::Provider) => Some("l.provider"),
            Some(GroupBy::Day) => Some("l.created_at::date::text"),
            None => None,
        };
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
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
            qb.push(" AND l.virtual_key_id = ").push_bind(k);
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
