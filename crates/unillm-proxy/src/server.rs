//! axum application: the request handler pipeline (`DESIGN.md` §10.3).
//!
//! In-memory only (no DB/Redis/keys/RL — those land in M4). The pipeline: detect inbound format →
//! parse to canonical → resolve route → walk the (primary, fallbacks) chain, calling the backend
//! `Client` → translate the response into the client's outbound format. Streaming requests are
//! re-translated event-by-event (`DESIGN.md` §10.5).

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
use unillm_storage::KeyStore;

use crate::inbound::{Format, detect_format, parse_request};
use crate::middleware::auth::{
    authenticate, extract_token, reject_query_key, require_admin, require_scope,
};
use crate::outbound::build_response;
use crate::outbound::stream::encoder_for;
use crate::route::{RouteTarget, Routes, resolve_chain};

const MAX_BODY: usize = 16 * 1024 * 1024;

/// Shared proxy state: the routing table, per-provider backend clients, the key store, and auth
/// secrets.
#[derive(Clone)]
pub struct AppState {
    pub routes: Arc<Routes>,
    pub clients: Arc<HashMap<ProviderId, Arc<CoreClient>>>,
    pub key_store: Arc<dyn KeyStore>,
    pub key_pepper: String,
    pub admin_token: Option<String>,
}

impl AppState {
    pub fn new(
        routes: Routes,
        clients: HashMap<ProviderId, Arc<CoreClient>>,
        key_store: Arc<dyn KeyStore>,
        key_pepper: String,
        admin_token: Option<String>,
    ) -> Self {
        Self {
            routes: Arc::new(routes),
            clients: Arc::new(clients),
            key_store,
            key_pepper,
            admin_token,
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
    let key = match authenticate(&*state.key_store, token.as_deref(), &state.key_pepper).await {
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
    let outbound = pick_outbound(inbound, response_format_header.as_deref());

    let chain = match resolve_chain(&canonical.model, &state.routes) {
        Ok(c) => c,
        Err(e) => return error_response(&e),
    };
    if canonical.stream {
        return stream_request(&state, outbound, chain, &canonical).await;
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
                let body = build_response(outbound, &resp);
                return Json(body).into_response();
            }
            Err(e) if policy.should_retry(&e) => {
                last_err = e;
                continue;
            }
            Err(e) => return error_response(&e),
        }
    }
    error_response(&last_err)
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
                Some(Ok(ev)) => return sse_response(outbound, ev, events),
                Some(Err(e)) if policy.should_retry(&e) => {
                    last_err = e;
                    continue;
                }
                Some(Err(e)) => return error_response(&e),
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
            Err(e) => return error_response(&e),
        }
    }
    error_response(&last_err)
}

/// Translate a canonical stream into an outbound SSE response (`DESIGN.md` §10.5): the first event
/// plus every subsequent item is run through the format's [`StreamEncoder`] and flushed as soon as it
/// arrives — no whole-response buffering.
fn sse_response(
    outbound: Format,
    first: StreamEvent,
    mut rest: BoxStream<'static, Result<StreamEvent, CoreError>>,
) -> Response {
    let mut encoder = encoder_for(outbound);
    let body = Body::from_stream(async_stream::stream! {
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
