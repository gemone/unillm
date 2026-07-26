//! axum application: the request handler pipeline (`DESIGN.md` §10.3).
//!
//! The pipeline: authenticate virtual key → detect inbound format → parse to canonical → enforce
//! §16 caps → rate-limit/acquire (§12, M4.4) → resolve route (DB-backed, M4.3) → validate
//! (allowlist + catalog) → walk the (primary, fallbacks) chain calling the backend `Client` →
//! translate the response into the client's outbound format. Streaming requests are re-translated
//! event-by-event (`DESIGN.md` §10.5); the rate-limit concurrency slot releases when the stream
//! ends or the client drops. Request/usage logging (M4.5) is not yet wired.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;

use unillm_core::ir::{ModelRef, Request as CanonicalRequest};
use unillm_core::retry::RetryPolicy;
use unillm_core::stream::StreamEvent;
use unillm_core::{Client as CoreClient, CoreError, ProviderId};
use unillm_storage::{
    KeyLimits, KeyStore, ModelStore, RateDecision, RateHeaders, RateLimiter, RouteStore,
    TokenActual, VirtualKey,
};
use uuid::Uuid;

use crate::config::RequestLimits;
use crate::inbound::{Format, detect_format, parse_request};
use crate::middleware::auth::{
    authenticate, extract_token, reject_query_key, require_admin, require_scope,
};
use crate::middleware::rate_limit::{
    ReleaseGuard, apply_rate_headers, estimate_tokens, rate_limited_response,
};
use crate::outbound::build_response;
use crate::outbound::stream::encoder_for;
use crate::route::{RouteTarget, row_to_chain};

const MAX_BODY: usize = 16 * 1024 * 1024;

/// The storage + limits layer: the four pluggable trait-object backends held by [`AppState`].
#[derive(Clone)]
pub struct Stores {
    pub keys: Arc<dyn KeyStore>,
    pub routes: Arc<dyn RouteStore>,
    pub models: Arc<dyn ModelStore>,
    pub rate_limiter: Arc<dyn RateLimiter>,
}

/// Shared proxy state: per-provider backend clients, the storage+limits layer, auth secrets, and
/// inbound request caps.
#[derive(Clone)]
pub struct AppState {
    pub clients: Arc<HashMap<ProviderId, Arc<CoreClient>>>,
    pub stores: Stores,
    pub key_pepper: String,
    pub admin_token: Option<String>,
    pub limits: RequestLimits,
}

impl AppState {
    pub fn new(
        clients: HashMap<ProviderId, Arc<CoreClient>>,
        stores: Stores,
        key_pepper: String,
        admin_token: Option<String>,
        limits: RequestLimits,
    ) -> Self {
        Self {
            clients: Arc::new(clients),
            stores,
            key_pepper,
            admin_token,
            limits,
        }
    }
}

/// Build the proxy `Router`.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(proxy))
        .route("/v1/messages", post(proxy))
        .route("/unillm/v1/responses", post(proxy))
        .route("/admin/{*rest}", any(admin_gate))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(state)
}

/// Admin gate (`/admin/*`, `DESIGN.md` §10.6, §16): requires the distinct admin token — a data-plane
/// virtual key never satisfies it. The REST routes themselves arrive in M4.5; until then an
/// authenticated admin request 404s.
async fn admin_gate(State(state): State<AppState>, req: Request) -> Response {
    let token = extract_token(req.headers());
    match require_admin(token.as_deref(), &state.admin_token) {
        Ok(()) => error_response(&CoreError::NotFound {
            message: "admin routes arrive in M4.5".into(),
        }),
        Err(e) => error_response(&e),
    }
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let ready = !state.clients.is_empty();
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(serde_json::json!({ "ready": ready })))
}

/// The universal translator handler (`DESIGN.md` §10.3).
async fn proxy(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    // §16: never accept API keys as query parameters.
    if let Err(e) = reject_query_key(req.uri()) {
        return error_response(&e);
    }
    let (format_header, response_format_header, token) = {
        let headers = req.headers();
        (
            headers
                .get("x-unillm-format")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            headers
                .get("x-unillm-response-format")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            extract_token(headers),
        )
    };

    // Data plane (§10.3 step 1): authenticate the virtual key, then require the `data` scope.
    let key = match authenticate(&*state.stores.keys, token.as_deref(), &state.key_pepper).await {
        Ok(k) => k,
        Err(e) => return error_response(&e),
    };
    if let Err(e) = require_scope(&key, "data") {
        return error_response(&e);
    }

    let body_bytes = match axum::body::to_bytes(req.into_body(), MAX_BODY).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(&CoreError::Other {
                message: "failed to read request body".into(),
            });
        }
    };
    let body_len = body_bytes.len();
    let body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return error_response(&CoreError::Serde {
                message: format!("invalid JSON body: {e}"),
            });
        }
    };

    let inbound = detect_format(&path, format_header.as_deref(), &body);
    let canonical = match parse_request(inbound, &body) {
        Ok(r) => r,
        Err(e) => return error_response(&e),
    };
    // §16: enforce inbound request caps.
    if let Err(e) = check_caps(&canonical, &state.limits) {
        return error_response(&e);
    }
    let outbound = pick_outbound(inbound, response_format_header.as_deref());

    // Rate limit (§12): acquire under the key's limits before doing any upstream work.
    let limits = KeyLimits::from_key(&key);
    let estimate = estimate_tokens(body_len, canonical.max_tokens);
    let rate_headers = match state
        .stores
        .rate_limiter
        .acquire(key.id, &limits, estimate)
        .await
    {
        RateDecision::Allow(h) => h,
        RateDecision::Deny {
            retry_after,
            headers,
            ..
        } => return rate_limited_response(retry_after, headers),
    };

    // Resolve the route (DB for aliases, M4.3) scoped to the key's tenant.
    let chain =
        match resolve_model(&*state.stores.routes, &canonical.model, Some(key.tenant_id)).await {
            Ok(c) => c,
            Err(e) => return error_response(&e),
        };
    // §10.3 step 3 (validate): key model allowlist + catalog enabled-check.
    if let Err(e) = check_allowlist(&key, &crate::route::model_id(&canonical.model)) {
        return error_response(&e);
    }
    if let Err(e) = check_catalog_enabled(&*state.stores.models, &chain[0]).await {
        return error_response(&e);
    }
    if canonical.stream {
        return stream_request(
            &state,
            outbound,
            chain,
            &canonical,
            key.id,
            limits,
            rate_headers,
        )
        .await;
    }

    let policy = RetryPolicy::default();
    let mut last_err = CoreError::NotFound {
        message: "no upstream available for this model".into(),
    };
    for target in &chain {
        let Some(client) = state.clients.get(&target.provider) else {
            last_err = CoreError::NotFound {
                message: format!(
                    "no upstream client configured for provider {:?}",
                    target.provider
                ),
            };
            continue;
        };
        let try_req = request_for_target(&canonical, target);
        match client.create(&try_req).await {
            Ok(resp) => {
                let actual = TokenActual {
                    input: resp.usage.total_input(),
                    output: resp.usage.output_tokens,
                };
                state
                    .stores
                    .rate_limiter
                    .release(key.id, &limits, Some(actual))
                    .await;
                let mut r = Json(build_response(outbound, &resp)).into_response();
                apply_rate_headers(r.headers_mut(), &rate_headers);
                return r;
            }
            Err(e) if policy.should_retry(&e) => {
                last_err = e;
                continue;
            }
            Err(e) => {
                state
                    .stores
                    .rate_limiter
                    .release(key.id, &limits, None)
                    .await;
                return error_response(&e);
            }
        }
    }
    state
        .stores
        .rate_limiter
        .release(key.id, &limits, None)
        .await;
    error_response(&last_err)
}

/// §16 inbound caps (`DESIGN.md` §16): input items, tools, output tokens.
fn check_caps(req: &CanonicalRequest, limits: &RequestLimits) -> Result<(), CoreError> {
    if req.input.len() > limits.max_input_items {
        return Err(CoreError::InvalidRequest {
            message: format!(
                "too many input items ({}, limit {})",
                req.input.len(),
                limits.max_input_items
            ),
        });
    }
    if let Some(tools) = &req.tools
        && tools.len() > limits.max_tools
    {
        return Err(CoreError::InvalidRequest {
            message: format!(
                "too many tools ({}, limit {})",
                tools.len(),
                limits.max_tools
            ),
        });
    }
    if let Some(mt) = req.max_tokens
        && mt > limits.max_output_tokens
    {
        return Err(CoreError::InvalidRequest {
            message: format!("max_tokens {mt} exceeds cap {}", limits.max_output_tokens),
        });
    }
    Ok(())
}

/// Resolve a [`ModelRef`] into an ordered target chain (`DESIGN.md` §10.2). An explicit
/// `(provider, model)` pair is a single-target chain; an alias is looked up in the route store
/// (tenant-scoped, falling back to the global default).
async fn resolve_model(
    store: &dyn RouteStore,
    model: &ModelRef,
    tenant: Option<Uuid>,
) -> Result<Vec<RouteTarget>, CoreError> {
    match model {
        ModelRef::Explicit { provider, model } => Ok(vec![RouteTarget {
            provider: *provider,
            native_model: model.clone(),
        }]),
        ModelRef::Alias(alias) => {
            let row = store
                .resolve(alias, tenant)
                .await
                .map_err(|e| CoreError::Other {
                    message: format!("route lookup failed: {e}"),
                })?
                .ok_or_else(|| CoreError::NotFound {
                    message: format!("no route for model alias '{alias}'"),
                })?;
            row_to_chain(&row)
        }
    }
}

/// The key's `model_allowlist` (if set) must contain the requested model id (`DESIGN.md` §10.3 step 3).
fn check_allowlist(key: &VirtualKey, requested: &str) -> Result<(), CoreError> {
    if let Some(allowed) = &key.model_allowlist
        && !allowed.iter().any(|m| m == requested)
    {
        return Err(CoreError::InvalidRequest {
            message: format!("model '{requested}' is not in this key's allowlist"),
        });
    }
    Ok(())
}

/// Best-effort catalog check (`DESIGN.md` §13.2): if the primary model is registered and disabled,
/// reject. Models absent from the catalog are allowed (the catalog is optional metadata).
async fn check_catalog_enabled(
    store: &dyn ModelStore,
    target: &RouteTarget,
) -> Result<(), CoreError> {
    if let Ok(Some(m)) = store
        .get_model(provider_snake(target.provider), &target.native_model)
        .await
    {
        if !m.enabled {
            return Err(CoreError::InvalidRequest {
                message: format!("model '{}' is disabled", target.native_model),
            });
        }
    }
    Ok(())
}

/// The snake_case string form of a [`ProviderId`] (matches its serde serialization).
fn provider_snake(p: ProviderId) -> &'static str {
    match p {
        ProviderId::Openai => "openai",
        ProviderId::Anthropic => "anthropic",
        ProviderId::Openrouter => "openrouter",
        ProviderId::Deepseek => "deepseek",
    }
}

/// Clone `canonical` pinned to an explicit `(provider, model)` target for one upstream attempt
/// (shared by the non-stream walk and the streaming handler).
fn request_for_target(canonical: &CanonicalRequest, target: &RouteTarget) -> CanonicalRequest {
    let mut req = canonical.clone();
    req.model = ModelRef::Explicit {
        provider: target.provider,
        model: target.native_model.clone(),
    };
    req
}

/// Streaming request handler (`DESIGN.md` §10.5). Walks the route chain like the non-stream path,
/// but **commits** to a target only once its first canonical event arrives: establishment faults
/// (connection error, non-2xx) and immediate stream errors fall back to the next target, while any
/// real content commits us for the remainder of the stream (a mid-stream upstream fault is surfaced
/// as a terminal error event, not retried — re-sending already-streamed output would duplicate it).
async fn stream_request(
    state: &AppState,
    outbound: Format,
    chain: Vec<RouteTarget>,
    canonical: &CanonicalRequest,
    key_id: Uuid,
    limits: KeyLimits,
    rate_headers: RateHeaders,
) -> Response {
    let policy = RetryPolicy::default();
    let mut last_err = CoreError::NotFound {
        message: "no upstream available for this model".into(),
    };
    for target in &chain {
        let Some(client) = state.clients.get(&target.provider) else {
            last_err = CoreError::NotFound {
                message: format!(
                    "no upstream client configured for provider {:?}",
                    target.provider
                ),
            };
            continue;
        };
        let try_req = request_for_target(canonical, target);
        match client.stream(&try_req).await {
            Ok(mut events) => match events.next().await {
                Some(Ok(ev)) => {
                    // Commit: the guard releases the concurrency slot when the stream completes or
                    // the client disconnects (the body generator drops → guard drops).
                    let guard =
                        ReleaseGuard::new(state.stores.rate_limiter.clone(), key_id, limits);
                    return sse_response(outbound, ev, events, guard, rate_headers);
                }
                Some(Err(e)) if policy.should_retry(&e) => {
                    last_err = e;
                    continue;
                }
                Some(Err(e)) => {
                    state
                        .stores
                        .rate_limiter
                        .release(key_id, &limits, None)
                        .await;
                    return error_response(&e);
                }
                None => {
                    last_err = CoreError::Stream {
                        message: "upstream stream closed without emitting any events".into(),
                    };
                    continue;
                }
            },
            Err(e) if policy.should_retry(&e) => {
                last_err = e;
                continue;
            }
            Err(e) => {
                state
                    .stores
                    .rate_limiter
                    .release(key_id, &limits, None)
                    .await;
                return error_response(&e);
            }
        }
    }
    state
        .stores
        .rate_limiter
        .release(key_id, &limits, None)
        .await;
    error_response(&last_err)
}

/// Translate a canonical stream into an outbound SSE response (`DESIGN.md` §10.5): the first event
/// plus every subsequent item is run through the format's [`StreamEncoder`] and flushed as soon as it
/// arrives — no whole-response buffering.
fn sse_response(
    outbound: Format,
    first: StreamEvent,
    mut rest: BoxStream<'static, Result<StreamEvent, CoreError>>,
    guard: ReleaseGuard,
    rate_headers: RateHeaders,
) -> Response {
    let mut encoder = encoder_for(outbound);
    let body = Body::from_stream(async_stream::stream! {
        // Hold the concurrency slot until the stream completes or the client disconnects (the
        // generator drops → guard drops → RateLimiter::release).
        let _guard = guard;
        for line in encoder.encode_event(&first) {
            yield Ok::<String, std::io::Error>(line);
        }
        while let Some(item) = rest.next().await {
            let ev = match item {
                Ok(ev) => ev,
                Err(e) => StreamEvent::Error { error: e },
            };
            for line in encoder.encode_event(&ev) {
                yield Ok::<String, std::io::Error>(line);
            }
        }
    });
    let mut resp = Response::new(body);
    let headers = resp.headers_mut();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        axum::http::header::CONNECTION,
        HeaderValue::from_static("keep-alive"),
    );
    apply_rate_headers(headers, &rate_headers);
    resp
}

/// The outbound format: the client's inbound format unless `X-Unillm-Response-Format` overrides.
fn pick_outbound(inbound: Format, override_header: Option<&str>) -> Format {
    match override_header {
        Some("openai_chat") => Format::OpenaiChat,
        Some("anthropic") => Format::Anthropic,
        _ => inbound,
    }
}

/// Map a [`CoreError`] onto an HTTP error response (`DESIGN.md` §15.1).
fn error_response(e: &CoreError) -> Response {
    let status = StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = serde_json::json!({ "error": { "kind": e.kind(), "message": e.to_string() } });
    (status, Json(body)).into_response()
}
