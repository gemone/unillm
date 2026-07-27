//! Admin/management REST API (`DESIGN.md` §10.6, §13). All routes live under `/admin/*` and are
//! gated by the distinct admin token (never a data-plane virtual key, `DESIGN.md` §16). When no
//! admin token is configured every admin route returns 401 (secure default).
//!
//! The data-plane virtual keys minted here are immediately usable on the data plane; the secret is
//! returned exactly once (POST /admin/keys) and never stored in plaintext.

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use unillm_storage::{
    GroupBy, NewModel, NewRoute, NewVirtualKey, StoreError, UpdateKey, hash_secret, key_prefix,
};

use crate::middleware::auth::{extract_token, require_admin};
use crate::server::{AppState, error_response};

/// The `/admin/*` router, state-generic so [`build_app`] supplies the shared [`AppState`].
pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/admin/keys", post(create_key).get(list_keys))
        .route("/admin/keys/{id}", patch(update_key).delete(revoke_key))
        .route(
            "/admin/models",
            post(upsert_model).patch(upsert_model).get(list_models),
        )
        .route(
            "/admin/models/{provider}/{native_model}",
            delete(delete_model),
        )
        .route(
            "/admin/routes",
            post(upsert_route).patch(upsert_route).get(list_routes),
        )
        .route("/admin/routes/{alias}", delete(delete_route))
        .route("/admin/usage", get(usage))
        .route("/admin/logs", get(logs))
        .route("/admin/cache/invalidate", post(invalidate_cache))
        .route_layer(middleware::from_fn_with_state(state, admin_auth))
}

/// Admin auth gate (`DESIGN.md` §16, §10.6): require the distinct admin token on every `/admin/*`
/// route. Applied as a layer so each handler can assume an authorized caller.
async fn admin_auth(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    let token = extract_token(req.headers());
    if let Err(e) = require_admin(token.as_deref(), &state.admin_token) {
        return error_response(&e);
    }
    next.run(req).await
}

// --- helpers ------------------------------------------------------------------

fn ok_json<T: Serialize>(v: T) -> Response {
    Json(v).into_response()
}

/// Map a storage error to an HTTP response: `NotFound` → 404, anything else → 500.
fn err_store(e: StoreError) -> Response {
    let (code, message) = match e {
        StoreError::NotFound(m) => (StatusCode::NOT_FOUND, m),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    };
    (code, Json(json!({ "error": { "message": message } }))).into_response()
}

// --- keys (§13.1) -------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateKeyRequest {
    tenant_id: Uuid,
    #[serde(default)]
    scopes: Vec<String>,
    model_allowlist: Option<Vec<String>>,
    budget_daily_tokens: Option<i64>,
    rpm: Option<i32>,
    tpm: Option<i64>,
    max_concurrency: Option<i32>,
    expires_at: Option<DateTime<Utc>>,
}

/// The secret is returned exactly once at creation (`DESIGN.md` §13.1, §16).
#[derive(Debug, Serialize)]
struct CreateKeyResponse {
    id: Uuid,
    key_id: String,
    key: String,
}

async fn create_key(State(state): State<AppState>, Json(req): Json<CreateKeyRequest>) -> Response {
    let secret = unillm_storage::generate_secret();
    let key_hash = hash_secret(&secret, &state.key_pepper);
    let key_prefix = key_prefix(&secret);
    match state
        .stores
        .keys
        .create_key(NewVirtualKey {
            key_hash,
            key_prefix: key_prefix.clone(),
            tenant_id: req.tenant_id,
            scopes: req.scopes,
            model_allowlist: req.model_allowlist,
            budget_daily_tokens: req.budget_daily_tokens,
            rpm: req.rpm,
            tpm: req.tpm,
            max_concurrency: req.max_concurrency,
            expires_at: req.expires_at,
        })
        .await
    {
        Ok(key) => ok_json(CreateKeyResponse {
            id: key.id,
            key_id: key_prefix,
            key: secret,
        }),
        Err(e) => err_store(e),
    }
}

#[derive(Debug, Deserialize)]
struct TenantQuery {
    tenant_id: Option<Uuid>,
}

async fn list_keys(State(state): State<AppState>, Query(q): Query<TenantQuery>) -> Response {
    match state.stores.keys.list_keys(q.tenant_id).await {
        Ok(keys) => ok_json(keys),
        Err(e) => err_store(e),
    }
}

async fn update_key(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(update): Json<UpdateKey>,
) -> Response {
    match state.stores.keys.update_key(id, update).await {
        Ok(key) => ok_json(key),
        Err(e) => err_store(e),
    }
}

async fn revoke_key(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response {
    match state.stores.keys.revoke_key(id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_store(e),
    }
}

// --- models (§13.2) -----------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ModelInput {
    provider: String,
    native_model: String,
    display_name: String,
    context_window: Option<i32>,
    max_output: Option<i32>,
    price_in: Option<f64>,
    price_out: Option<f64>,
    price_cache_read: Option<f64>,
    #[serde(default = "default_true")]
    enabled: bool,
}

fn default_true() -> bool {
    true
}

async fn upsert_model(State(state): State<AppState>, Json(m): Json<ModelInput>) -> Response {
    match state
        .stores
        .models
        .upsert_model(NewModel {
            provider: m.provider,
            native_model: m.native_model,
            display_name: m.display_name,
            context_window: m.context_window,
            max_output: m.max_output,
            price_in: m.price_in,
            price_out: m.price_out,
            price_cache_read: m.price_cache_read,
            enabled: m.enabled,
        })
        .await
    {
        Ok(model) => ok_json(model),
        Err(e) => err_store(e),
    }
}

async fn list_models(State(state): State<AppState>) -> Response {
    match state.stores.models.list_models().await {
        Ok(models) => ok_json(models),
        Err(e) => err_store(e),
    }
}

async fn delete_model(
    State(state): State<AppState>,
    Path((provider, native_model)): Path<(String, String)>,
) -> Response {
    match state
        .stores
        .models
        .delete_model(&provider, &native_model)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_store(e),
    }
}

// --- routes (§13.3) -----------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RouteInput {
    alias: String,
    tenant_id: Option<Uuid>,
    provider: String,
    native_model: String,
    #[serde(default)]
    fallback: Vec<unillm_storage::model::FallbackTarget>,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_true")]
    enabled: bool,
}

async fn upsert_route(State(state): State<AppState>, Json(r): Json<RouteInput>) -> Response {
    match state
        .stores
        .routes
        .upsert_route(NewRoute {
            alias: r.alias,
            tenant_id: r.tenant_id,
            provider: r.provider,
            native_model: r.native_model,
            fallback: r.fallback,
            priority: r.priority,
            enabled: r.enabled,
        })
        .await
    {
        Ok(route) => ok_json(route),
        Err(e) => err_store(e),
    }
}

async fn list_routes(State(state): State<AppState>, Query(q): Query<TenantQuery>) -> Response {
    match state.stores.routes.list_routes(q.tenant_id).await {
        Ok(routes) => ok_json(routes),
        Err(e) => err_store(e),
    }
}

#[derive(Debug, Deserialize)]
struct AliasQuery {
    tenant_id: Option<Uuid>,
}

async fn delete_route(
    State(state): State<AppState>,
    Path(alias): Path<String>,
    Query(q): Query<AliasQuery>,
) -> Response {
    match state.stores.routes.delete_route(&alias, q.tenant_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_store(e),
    }
}

// --- usage & logs (§13.5) -----------------------------------------------------

#[derive(Debug, Deserialize)]
struct UsageQuery {
    key_id: Option<Uuid>,
    model: Option<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    group_by: Option<String>,
}

async fn usage(State(state): State<AppState>, Query(q): Query<UsageQuery>) -> Response {
    let group_by = q.group_by.as_deref().and_then(GroupBy::parse);
    match state
        .stores
        .logs
        .usage_summary(q.key_id, q.model.as_deref(), q.from, q.to, group_by)
        .await
    {
        Ok(buckets) => ok_json(buckets),
        Err(e) => err_store(e),
    }
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    key_id: Option<Uuid>,
    cursor: Option<DateTime<Utc>>,
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_limit() -> i64 {
    100
}

async fn logs(State(state): State<AppState>, Query(q): Query<LogsQuery>) -> Response {
    match state
        .stores
        .logs
        .list_logs(q.key_id, q.cursor, q.limit)
        .await
    {
        Ok(logs) => ok_json(logs),
        Err(e) => err_store(e),
    }
}

// --- cache (§7.4 invalidation, §10.6) -------------------------------------------

#[derive(Debug, Deserialize)]
struct InvalidateRequest {
    /// Flush only this scope (virtual key id); `None` = all scopes.
    scope: Option<String>,
    /// Flush only this cache key hash; `None` = all hashes.
    key_hash: Option<String>,
}

/// Flush cached responses (`DESIGN.md` §7.4 invalidation, §10.6). Both fields `None` flushes
/// everything; together they flush one entry. Returns the count removed.
async fn invalidate_cache(
    State(state): State<AppState>,
    Json(req): Json<InvalidateRequest>,
) -> Response {
    let invalidated = state
        .stores
        .cache
        .invalidate(req.scope.as_deref(), req.key_hash.as_deref())
        .await;
    ok_json(json!({ "invalidated": invalidated }))
}
