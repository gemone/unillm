//! axum application: the request handler pipeline (`DESIGN.md` §10.3).
//!
//! In-memory only (no DB/Redis/keys/RL — those land in M4). The pipeline: detect inbound format →
//! parse to canonical → resolve route → walk the (primary, fallbacks) chain, calling the backend
//! `Client` → translate the response into the client's outbound format. Streaming is M3.4.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;

use unillm_core::ir::ModelRef;
use unillm_core::retry::RetryPolicy;
use unillm_core::{Client as CoreClient, CoreError, ProviderId};

use crate::inbound::{Format, detect_format, parse_request};
use crate::outbound::build_response;
use crate::route::{Routes, resolve_chain};

const MAX_BODY: usize = 16 * 1024 * 1024;

/// Shared proxy state: the routing table and a per-provider backend client.
#[derive(Clone)]
pub struct AppState {
    pub routes: Arc<Routes>,
    pub clients: Arc<HashMap<ProviderId, Arc<CoreClient>>>,
}

impl AppState {
    pub fn new(routes: Routes, clients: HashMap<ProviderId, Arc<CoreClient>>) -> Self {
        Self {
            routes: Arc::new(routes),
            clients: Arc::new(clients),
        }
    }
}

/// Build the proxy `Router`.
pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(proxy))
        .route("/v1/messages", post(proxy))
        .route("/unillm/v1/responses", post(proxy))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(state)
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
    let (format_header, response_format_header) = {
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
        )
    };

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
    if canonical.stream {
        // Streaming arrives in M3.4.
        return error_response(&CoreError::Other {
            message: "streaming is not implemented yet".into(),
        });
    }
    let outbound = pick_outbound(inbound, response_format_header.as_deref());

    let chain = match resolve_chain(&canonical.model, &state.routes) {
        Ok(c) => c,
        Err(e) => return error_response(&e),
    };

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
        let mut try_req = canonical.clone();
        try_req.model = ModelRef::Explicit {
            provider: target.provider,
            model: target.native_model.clone(),
        };
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
