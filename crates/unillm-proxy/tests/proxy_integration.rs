//! Proxy integration tests: the full HTTP path through the axum server against wiremock upstreams.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, Utc};
use serde_json::{Value, json};
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use unillm_core::retry::RetryPolicy;
use unillm_core::{Client as CoreClient, ProviderConfig, ProviderId};
use unillm_proxy::route::{Route, RouteTarget, Routes};
use unillm_proxy::server::{AppState, build_app};
use unillm_storage::{KeyStore, NewVirtualKey, SqliteStore};

const PEPPER: &str = "test-pepper";

/// Start the proxy on an ephemeral port with `store`/`admin_token`; return its base URL. Upstreams
/// use `RetryPolicy::none()` so fallback is exercised without per-target backoff.
async fn start_proxy(
    routes: Routes,
    clients: HashMap<ProviderId, Arc<CoreClient>>,
    store: Arc<SqliteStore>,
    admin_token: Option<String>,
) -> String {
    let app = build_app(AppState::new(
        routes,
        clients,
        store,
        PEPPER.into(),
        admin_token,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// Fresh in-proc SQLite store (migrations applied).
async fn mem_store() -> Arc<SqliteStore> {
    Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap())
}

/// Seed a key with `scopes`, returning its raw secret.
async fn seed_key(store: &Arc<SqliteStore>, scopes: &[&str]) -> String {
    let secret = unillm_storage::generate_secret();
    store
        .create_key(NewVirtualKey {
            key_hash: unillm_storage::hash_secret(&secret, PEPPER),
            key_prefix: unillm_storage::key_prefix(&secret),
            tenant_id: Uuid::new_v4(),
            scopes: scopes.iter().map(|s| (*s).to_string()).collect(),
            model_allowlist: None,
            budget_daily_tokens: None,
            rpm: None,
            tpm: None,
            max_concurrency: None,
            expires_at: None,
        })
        .await
        .unwrap();
    secret
}

/// Start the proxy with a freshly-seeded `data`-scoped key; return `(base_url, secret)`.
async fn authed_proxy(
    routes: Routes,
    clients: HashMap<ProviderId, Arc<CoreClient>>,
) -> (String, String) {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let url = start_proxy(routes, clients, store, None).await;
    (url, secret)
}

fn client_for(provider: ProviderId, base_url: String) -> Arc<CoreClient> {
    let mut pc = ProviderConfig::new(provider, "sk-test");
    pc.base_url = base_url;
    Arc::new(CoreClient::new(pc).unwrap().with_retry(RetryPolicy::none()))
}

fn http() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

/// A Chat Completions upstream SSE stream: role chunk → "Hel" → "lo" → finish_reason stop → [DONE].
const CC_SSE: &str = concat!(
    "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

/// An Anthropic upstream SSE stream: message_start → text block → "Hel"+"lo" → stop → message_stop.
const ANTHROPIC_SSE: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hel\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"lo\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);

async fn mount_sse(server: &MockServer, p: &str, body: &'static str) {
    Mock::given(method("POST"))
        .and(path(p))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body.as_bytes(), "text/event-stream"))
        .mount(server)
        .await;
}

// --- health / ready (no auth) ----------------------------------------------------

#[tokio::test]
async fn health_ok() {
    let url = start_proxy(Routes::new(), HashMap::new(), mem_store().await, None).await;
    let resp = http().get(format!("{url}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn ready_reflects_configured_clients() {
    let url = start_proxy(Routes::new(), HashMap::new(), mem_store().await, None).await;
    let resp = http().get(format!("{url}/ready")).send().await.unwrap();
    assert_eq!(resp.status(), 503);
}

// --- auth (M4.2) -----------------------------------------------------------------

#[tokio::test]
async fn data_plane_rejects_missing_key() {
    let url = start_proxy(Routes::new(), HashMap::new(), mem_store().await, None).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .json(&json!({"model": "x", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn data_plane_rejects_invalid_key() {
    let url = start_proxy(Routes::new(), HashMap::new(), mem_store().await, None).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", "Bearer sk-unillm-bogus")
        .json(&json!({"model": "x", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn data_plane_rejects_revoked_key() {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let hash = unillm_storage::hash_secret(&secret, PEPPER);
    let key = store.find_by_hash(&hash).await.unwrap().unwrap();
    store.revoke_key(key.id).await.unwrap();
    let url = start_proxy(Routes::new(), HashMap::new(), store, None).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "x", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn data_plane_rejects_expired_key() {
    let store = mem_store().await;
    let secret = unillm_storage::generate_secret();
    store
        .create_key(NewVirtualKey {
            key_hash: unillm_storage::hash_secret(&secret, PEPPER),
            key_prefix: unillm_storage::key_prefix(&secret),
            tenant_id: Uuid::new_v4(),
            scopes: vec!["data".into()],
            model_allowlist: None,
            budget_daily_tokens: None,
            rpm: None,
            tpm: None,
            max_concurrency: None,
            expires_at: Some(Utc::now() - Duration::seconds(5)),
        })
        .await
        .unwrap();
    let url = start_proxy(Routes::new(), HashMap::new(), store, None).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "x", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn data_plane_rejects_wrong_scope() {
    let store = mem_store().await;
    // `read-usage` only — no `data` scope.
    let secret = seed_key(&store, &["read-usage"]).await;
    let url = start_proxy(Routes::new(), HashMap::new(), store, None).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "x", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn data_plane_rejects_query_param_key() {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let url = start_proxy(Routes::new(), HashMap::new(), store, None).await;
    // A key leaked via query string is rejected (§16) before auth is even attempted.
    let resp = http()
        .post(format!("{url}/v1/chat/completions?key={secret}"))
        .json(&json!({"model": "x", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn admin_rejects_when_unconfigured() {
    // No admin token configured → admin endpoints are disabled (401).
    let url = start_proxy(Routes::new(), HashMap::new(), mem_store().await, None).await;
    let resp = http()
        .get(format!("{url}/admin/keys"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn admin_rejects_missing_token() {
    let url = start_proxy(
        Routes::new(),
        HashMap::new(),
        mem_store().await,
        Some("admin-secret".into()),
    )
    .await;
    assert_eq!(
        http()
            .get(format!("{url}/admin/keys"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}

#[tokio::test]
async fn admin_rejects_data_key_as_admin() {
    // A data-plane virtual key is not the admin token.
    let store = mem_store().await;
    let data_secret = seed_key(&store, &["data"]).await;
    let url = start_proxy(
        Routes::new(),
        HashMap::new(),
        store,
        Some("admin-secret".into()),
    )
    .await;
    assert_eq!(
        http()
            .get(format!("{url}/admin/keys"))
            .header("authorization", format!("Bearer {data_secret}"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
}

#[tokio::test]
async fn admin_token_passes_then_404s() {
    // Authenticated admin request reaches the (not-yet-existing) admin router → 404, not 401.
    let url = start_proxy(
        Routes::new(),
        HashMap::new(),
        mem_store().await,
        Some("admin-secret".into()),
    )
    .await;
    let resp = http()
        .get(format!("{url}/admin/keys"))
        .header("authorization", "Bearer admin-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// --- data plane (authenticated) --------------------------------------------------

#[tokio::test]
async fn cc_inbound_to_openai() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2}
        })))
        .mount(&upstream)
        .await;

    let mut routes = Routes::new();
    routes.insert("gpt-4o", Route::single(ProviderId::Openai, "gpt-4o"));
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["object"], "chat.completion");
    assert_eq!(body["choices"][0]["message"]["content"], "hello");
}

#[tokio::test]
async fn anthropic_inbound_openai_backend_cross_dialect() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi from gpt"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1}
        })))
        .mount(&upstream)
        .await;

    let mut routes = Routes::new();
    routes.insert(
        "claude-sonnet-4-6",
        Route::single(ProviderId::Openai, "gpt-4o"),
    );
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/messages"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "claude-sonnet-4-6", "max_tokens": 100, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "message");
    assert_eq!(body["content"][0]["text"], "hi from gpt");
}

#[tokio::test]
async fn fallback_on_500() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(json!({"error": {"message": "boom"}})),
        )
        .mount(&primary)
        .await;
    let fallback = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c2", "model": "deepseek-chat", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "fallback ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        })))
        .mount(&fallback)
        .await;

    let mut routes = Routes::new();
    routes.insert(
        "m",
        Route {
            primary: RouteTarget {
                provider: ProviderId::Openai,
                native_model: "gpt-4o".into(),
            },
            fallback: vec![RouteTarget {
                provider: ProviderId::Deepseek,
                native_model: "deepseek-chat".into(),
            }],
        },
    );
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, primary.uri()),
    );
    clients.insert(
        ProviderId::Deepseek,
        client_for(ProviderId::Deepseek, fallback.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "fallback ok");
}

#[tokio::test]
async fn unknown_alias_is_not_found() {
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, "http://127.0.0.1:1".into()),
    );
    let (url, secret) = authed_proxy(Routes::new(), clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "nope", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// --- streaming (M3.4): canonical event → outbound SSE per format, flush per event. ---
//
// The meaningful translation matrix is 3 inbound formats × 2 dialects (OpenAI/DeepSeek/OpenRouter
// share the CC dialect, so one CC-backend case covers all three) × {stream, non-stream}. The
// non-stream halves are covered above + the outbound unit tests; these cover every stream path.

#[tokio::test]
async fn stream_cc_inbound_to_cc_backend() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;

    let mut routes = Routes::new();
    routes.insert("gpt-4o", Route::single(ProviderId::Openai, "gpt-4o"));
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let body = resp.text().await.unwrap();
    assert!(body.contains("\"content\":\"Hel\""));
    assert!(body.contains("\"content\":\"lo\""));
    assert!(body.contains("\"finish_reason\":\"stop\""));
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn stream_anthropic_inbound_cc_backend_cross_dialect() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;

    let mut routes = Routes::new();
    routes.insert("claude", Route::single(ProviderId::Openai, "gpt-4o"));
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/messages"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "claude", "max_tokens": 100, "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("event: message_start"));
    assert!(body.contains("event: content_block_delta"));
    assert!(body.contains("\"text\":\"Hel\""));
    assert!(body.contains("\"text\":\"lo\""));
    assert!(body.contains("event: message_stop"));
}

#[tokio::test]
async fn stream_cc_inbound_anthropic_backend_cross_dialect() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/messages", ANTHROPIC_SSE).await;

    let mut routes = Routes::new();
    routes.insert(
        "gpt-4o",
        Route::single(ProviderId::Anthropic, "claude-sonnet-4-6"),
    );
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Anthropic,
        client_for(ProviderId::Anthropic, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("chat.completion.chunk"));
    assert!(body.contains("\"content\":\"Hel\""));
    assert!(body.contains("\"content\":\"lo\""));
    assert!(body.contains("data: [DONE]"));
}

#[tokio::test]
async fn stream_anthropic_inbound_passthrough() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/messages", ANTHROPIC_SSE).await;

    let mut routes = Routes::new();
    routes.insert(
        "claude",
        Route::single(ProviderId::Anthropic, "claude-sonnet-4-6"),
    );
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Anthropic,
        client_for(ProviderId::Anthropic, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/messages"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "claude", "max_tokens": 100, "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let message_start = body.split("\n\n").next().unwrap();
    assert!(message_start.contains("event: message_start"));
    assert!(message_start.contains("\"input_tokens\":10"));
    assert!(body.contains("event: message_stop"));
}

#[tokio::test]
async fn stream_falls_back_on_primary_500() {
    let primary = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(json!({"error": {"message": "boom"}})),
        )
        .mount(&primary)
        .await;
    let fallback = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"id\":\"c2\",\"model\":\"deepseek-chat\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"fallback ok\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"c2\",\"model\":\"deepseek-chat\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n",
            ).as_bytes(),
            "text/event-stream",
        ))
        .mount(&fallback)
        .await;

    let mut routes = Routes::new();
    routes.insert(
        "m",
        Route {
            primary: RouteTarget {
                provider: ProviderId::Openai,
                native_model: "gpt-4o".into(),
            },
            fallback: vec![RouteTarget {
                provider: ProviderId::Deepseek,
                native_model: "deepseek-chat".into(),
            }],
        },
    );
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, primary.uri()),
    );
    clients.insert(
        ProviderId::Deepseek,
        client_for(ProviderId::Deepseek, fallback.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(
            &json!({"model": "m", "stream": true, "messages": [{"role": "user", "content": "hi"}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("\"content\":\"fallback ok\""));
    assert!(body.contains("data: [DONE]"));
}

#[tokio::test]
async fn stream_canonical_inbound_emits_canonical_events() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;

    let mut routes = Routes::new();
    routes.insert("gpt-4o", Route::single(ProviderId::Openai, "gpt-4o"));
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/unillm/v1/responses"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "stream": true, "input": [{"type": "message", "role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("\"type\":\"created\""));
    assert!(body.contains("\"type\":\"text_delta\""));
    assert!(body.contains("\"type\":\"completed\""));
}

#[tokio::test]
async fn stream_routes_every_cc_dialect_provider() {
    // OpenAI (CC) covered by stream_cc_inbound_to_cc_backend; Anthropic by the passthrough test.
    // DeepSeek and OpenRouter share the CC dialect (§5.6) — this proves both are routable as primary
    // stream sources (the "4 backends" dimension of the M3 gate).
    for provider in [ProviderId::Deepseek, ProviderId::Openrouter] {
        let upstream = MockServer::start().await;
        mount_sse(&upstream, "/chat/completions", CC_SSE).await;

        let mut routes = Routes::new();
        routes.insert("m", Route::single(provider, "cc-model"));
        let mut clients = HashMap::new();
        clients.insert(provider, client_for(provider, upstream.uri()));
        let (url, secret) = authed_proxy(routes, clients).await;

        let resp = http()
            .post(format!("{url}/v1/chat/completions"))
            .header("authorization", format!("Bearer {secret}"))
            .json(&json!({"model": "m", "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{provider:?}: expected 200");
        let body = resp.text().await.unwrap();
        assert!(
            body.contains("\"content\":\"Hel\""),
            "{provider:?}: missing streamed content"
        );
        assert!(
            body.contains("data: [DONE]"),
            "{provider:?}: missing [DONE]"
        );
    }
}
