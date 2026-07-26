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
use unillm_proxy::config::RequestLimits;
use unillm_proxy::server::{AppState, build_app};
use unillm_storage::model::FallbackTarget;
use unillm_storage::{
    KeyStore, ModelStore, NewModel, NewRoute, NewVirtualKey, RouteStore, SqliteStore,
};

const PEPPER: &str = "test-pepper";

fn default_limits() -> RequestLimits {
    RequestLimits {
        max_input_items: 1000,
        max_tools: 128,
        max_output_tokens: 16_384,
    }
}

/// Fresh in-proc SQLite store (migrations applied).
async fn mem_store() -> Arc<SqliteStore> {
    Arc::new(SqliteStore::connect("sqlite::memory:").await.unwrap())
}

/// Seed a key with `scopes` (no model allowlist), returning its raw secret.
async fn seed_key(store: &Arc<SqliteStore>, scopes: &[&str]) -> String {
    seed_key_with(store, scopes, None, None).await
}

/// Seed a key with full control over scopes/allowlist/expiry, returning its raw secret.
async fn seed_key_with(
    store: &Arc<SqliteStore>,
    scopes: &[&str],
    allowlist: Option<Vec<&str>>,
    expires_at: Option<chrono::DateTime<Utc>>,
) -> String {
    let secret = unillm_storage::generate_secret();
    store
        .create_key(NewVirtualKey {
            key_hash: unillm_storage::hash_secret(&secret, PEPPER),
            key_prefix: unillm_storage::key_prefix(&secret),
            tenant_id: Uuid::new_v4(),
            scopes: scopes.iter().map(|s| (*s).to_string()).collect(),
            model_allowlist: allowlist.map(|a| a.into_iter().map(String::from).collect()),
            budget_daily_tokens: None,
            rpm: None,
            tpm: None,
            max_concurrency: None,
            expires_at,
        })
        .await
        .unwrap();
    secret
}

/// Seed a global (tenant-less) route with optional fallback chain.
async fn seed_route(
    store: &Arc<SqliteStore>,
    alias: &str,
    provider: &str,
    native_model: &str,
    fallback: Vec<FallbackTarget>,
) {
    store
        .upsert_route(NewRoute {
            alias: alias.into(),
            tenant_id: None,
            provider: provider.into(),
            native_model: native_model.into(),
            fallback,
            priority: 0,
            enabled: true,
        })
        .await
        .unwrap();
}

/// Start the proxy on an ephemeral port; return its base URL.
async fn start_proxy(
    clients: HashMap<ProviderId, Arc<CoreClient>>,
    store: Arc<SqliteStore>,
    admin_token: Option<String>,
    limits: RequestLimits,
) -> String {
    let key_store: Arc<dyn KeyStore> = store.clone();
    let route_store: Arc<dyn RouteStore> = store.clone();
    let model_store: Arc<dyn ModelStore> = store;
    let app = build_app(AppState::new(
        clients,
        key_store,
        route_store,
        model_store,
        PEPPER.into(),
        admin_token,
        limits,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// Start the proxy with a freshly-seeded `data`-scoped key and the given global routes; returns
/// `(base_url, secret)`.
async fn authed_proxy(
    clients: HashMap<ProviderId, Arc<CoreClient>>,
    routes: &[(&str, &str, &str)],
) -> (String, String) {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    for (alias, provider, native_model) in routes {
        seed_route(&store, alias, provider, native_model, vec![]).await;
    }
    let url = start_proxy(clients, store, None, default_limits()).await;
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

const CC_SSE: &str = concat!(
    "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

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
    let url = start_proxy(HashMap::new(), mem_store().await, None, default_limits()).await;
    let resp = http().get(format!("{url}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn ready_reflects_configured_clients() {
    let url = start_proxy(HashMap::new(), mem_store().await, None, default_limits()).await;
    let resp = http().get(format!("{url}/ready")).send().await.unwrap();
    assert_eq!(resp.status(), 503);
}

// --- auth (M4.2) -----------------------------------------------------------------

#[tokio::test]
async fn data_plane_rejects_missing_key() {
    let store = mem_store().await;
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
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
    let store = mem_store().await;
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
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
    let key = store
        .find_by_hash(&unillm_storage::hash_secret(&secret, PEPPER))
        .await
        .unwrap()
        .unwrap();
    store.revoke_key(key.id).await.unwrap();
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
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
    let secret = seed_key_with(
        &store,
        &["data"],
        None,
        Some(Utc::now() - Duration::seconds(5)),
    )
    .await;
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
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
    let secret = seed_key(&store, &["read-usage"]).await;
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
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
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
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
    let url = start_proxy(HashMap::new(), mem_store().await, None, default_limits()).await;
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
async fn admin_rejects_missing_token() {
    let url = start_proxy(
        HashMap::new(),
        mem_store().await,
        Some("admin-secret".into()),
        default_limits(),
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
    let store = mem_store().await;
    let data_secret = seed_key(&store, &["data"]).await;
    let url = start_proxy(
        HashMap::new(),
        store,
        Some("admin-secret".into()),
        default_limits(),
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
    let url = start_proxy(
        HashMap::new(),
        mem_store().await,
        Some("admin-secret".into()),
        default_limits(),
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

// --- validation: caps + allowlist + catalog (M4.3) -------------------------------

#[tokio::test]
async fn caps_reject_max_tokens() {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let limits = RequestLimits {
        max_output_tokens: 100,
        ..default_limits()
    };
    let url = start_proxy(HashMap::new(), store, None, limits).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "max_tokens": 99999, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn caps_reject_too_many_input_items() {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let limits = RequestLimits {
        max_input_items: 2,
        ..default_limits()
    };
    let url = start_proxy(HashMap::new(), store, None, limits).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [
            {"role": "user", "content": "a"},
            {"role": "user", "content": "b"},
            {"role": "user", "content": "c"}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn caps_reject_too_many_tools() {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let limits = RequestLimits {
        max_tools: 1,
        ..default_limits()
    };
    let url = start_proxy(HashMap::new(), store, None, limits).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(
            &json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}], "tools": [
                {"type": "function", "function": {"name": "a", "parameters": {}}},
                {"type": "function", "function": {"name": "b", "parameters": {}}}
            ]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn allowlist_rejects_unlisted_model() {
    let store = mem_store().await;
    let secret = seed_key_with(&store, &["data"], Some(vec!["gpt-4o"]), None).await;
    seed_route(&store, "claude", "anthropic", "claude-sonnet-4-6", vec![]).await;
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "claude", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn allowlist_allows_listed_model() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );

    let store = mem_store().await;
    let secret = seed_key_with(&store, &["data"], Some(vec!["gpt-4o"]), None).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store, None, default_limits()).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn disabled_model_in_catalog_is_rejected() {
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    store
        .upsert_model(NewModel {
            provider: "openai".into(),
            native_model: "gpt-4o".into(),
            display_name: "GPT-4o".into(),
            context_window: None,
            max_output: None,
            price_in: None,
            price_out: None,
            price_cache_read: None,
            enabled: false,
        })
        .await
        .unwrap();
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn db_route_tenant_scoped_overrides_global() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg_1", "model": "claude", "content": [{"type": "text", "text": "tenant hit"}],
            "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 1}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Anthropic,
        client_for(ProviderId::Anthropic, upstream.uri()),
    );

    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    let tenant = store
        .find_by_hash(&unillm_storage::hash_secret(&secret, PEPPER))
        .await
        .unwrap()
        .unwrap()
        .tenant_id;
    // Global default → OpenAI; tenant override → Anthropic.
    seed_route(&store, "fast", "openai", "gpt-4o-mini", vec![]).await;
    store
        .upsert_route(NewRoute {
            alias: "fast".into(),
            tenant_id: Some(tenant),
            provider: "anthropic".into(),
            native_model: "claude".into(),
            fallback: vec![],
            priority: 0,
            enabled: true,
        })
        .await
        .unwrap();
    let url = start_proxy(clients, store, None, default_limits()).await;

    let resp = http()
        .post(format!("{url}/v1/messages"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "fast", "max_tokens": 10, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["content"][0]["text"], "tenant hit");
}

// --- data plane (authenticated, DB routes) ---------------------------------------

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
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(clients, &[("gpt-4o", "openai", "gpt-4o")]).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
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
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(clients, &[("claude-sonnet-4-6", "openai", "gpt-4o")]).await;

    let resp = http()
        .post(format!("{url}/v1/messages"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "claude-sonnet-4-6", "max_tokens": 100, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
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

    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(
        &store,
        "m",
        "openai",
        "gpt-4o",
        vec![FallbackTarget {
            provider: "deepseek".into(),
            native_model: "deepseek-chat".into(),
        }],
    )
    .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, primary.uri()),
    );
    clients.insert(
        ProviderId::Deepseek,
        client_for(ProviderId::Deepseek, fallback.uri()),
    );
    let url = start_proxy(clients, store, None, default_limits()).await;

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
    let (url, secret) = authed_proxy(clients, &[]).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "nope", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// --- streaming (DB routes, authenticated) ----------------------------------------

#[tokio::test]
async fn stream_cc_inbound_to_cc_backend() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(clients, &[("gpt-4o", "openai", "gpt-4o")]).await;

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
    assert!(body.ends_with("data: [DONE]\n\n"));
}

#[tokio::test]
async fn stream_anthropic_inbound_cc_backend_cross_dialect() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(clients, &[("claude", "openai", "gpt-4o")]).await;

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
    assert!(body.contains("event: message_stop"));
}

#[tokio::test]
async fn stream_cc_inbound_anthropic_backend_cross_dialect() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/messages", ANTHROPIC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Anthropic,
        client_for(ProviderId::Anthropic, upstream.uri()),
    );
    let (url, secret) =
        authed_proxy(clients, &[("gpt-4o", "anthropic", "claude-sonnet-4-6")]).await;

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
    assert!(body.contains("data: [DONE]"));
}

#[tokio::test]
async fn stream_anthropic_inbound_passthrough() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/messages", ANTHROPIC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Anthropic,
        client_for(ProviderId::Anthropic, upstream.uri()),
    );
    let (url, secret) =
        authed_proxy(clients, &[("claude", "anthropic", "claude-sonnet-4-6")]).await;

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

    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(
        &store,
        "m",
        "openai",
        "gpt-4o",
        vec![FallbackTarget {
            provider: "deepseek".into(),
            native_model: "deepseek-chat".into(),
        }],
    )
    .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, primary.uri()),
    );
    clients.insert(
        ProviderId::Deepseek,
        client_for(ProviderId::Deepseek, fallback.uri()),
    );
    let url = start_proxy(clients, store, None, default_limits()).await;

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
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let (url, secret) = authed_proxy(clients, &[("gpt-4o", "openai", "gpt-4o")]).await;

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
    assert!(body.contains("\"type\":\"completed\""));
}

#[tokio::test]
async fn stream_routes_every_cc_dialect_provider() {
    for provider in [ProviderId::Deepseek, ProviderId::Openrouter] {
        let upstream = MockServer::start().await;
        mount_sse(&upstream, "/chat/completions", CC_SSE).await;
        let mut clients = HashMap::new();
        clients.insert(provider, client_for(provider, upstream.uri()));
        let prov = match provider {
            ProviderId::Deepseek => "deepseek",
            ProviderId::Openrouter => "openrouter",
            _ => unreachable!(),
        };
        let (url, secret) = authed_proxy(clients, &[("m", prov, "cc-model")]).await;

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
            body.contains("data: [DONE]"),
            "{provider:?}: missing [DONE]"
        );
    }
}
