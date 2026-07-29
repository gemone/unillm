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
use unillm_proxy::metrics::Metrics;
use unillm_proxy::middleware::cache::CacheConfig;
use unillm_proxy::server::{AppState, Stores, build_app};
use unillm_storage::model::FallbackTarget;
use unillm_storage::{
    InMemoryCache, InMemoryRateLimiter, KeyStore, LogStore, ModelStore, NewModel, NewRoute,
    NewVirtualKey, RequestLog, RouteStore, SqliteStore,
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

/// Seed a `data`-scoped key with rate/concurrency/budget limits set.
async fn seed_key_rl(
    store: &Arc<SqliteStore>,
    rpm: Option<i32>,
    conc: Option<i32>,
    budget: Option<i64>,
) -> String {
    let secret = unillm_storage::generate_secret();
    store
        .create_key(NewVirtualKey {
            key_hash: unillm_storage::hash_secret(&secret, PEPPER),
            key_prefix: unillm_storage::key_prefix(&secret),
            tenant_id: Uuid::new_v4(),
            scopes: vec!["data".into()],
            model_allowlist: None,
            budget_daily_tokens: budget,
            rpm,
            tpm: None,
            max_concurrency: conc,
            expires_at: None,
        })
        .await
        .unwrap();
    secret
}

/// Start the proxy on an ephemeral port; return its base URL. The response cache is disabled.
async fn start_proxy(
    clients: HashMap<ProviderId, Arc<CoreClient>>,
    store: Arc<SqliteStore>,
    admin_token: Option<String>,
    limits: RequestLimits,
) -> String {
    start_proxy_with_cache(clients, store, admin_token, limits, CacheConfig::disabled()).await
}

/// Start the proxy with an explicit response-cache config (for the cache-hit tests).
async fn start_proxy_with_cache(
    clients: HashMap<ProviderId, Arc<CoreClient>>,
    store: Arc<SqliteStore>,
    admin_token: Option<String>,
    limits: RequestLimits,
    cache_cfg: CacheConfig,
) -> String {
    let keys: Arc<dyn KeyStore> = store.clone();
    let routes: Arc<dyn RouteStore> = store.clone();
    let models: Arc<dyn ModelStore> = store.clone();
    let logs: Arc<dyn LogStore> = store;
    let stores = Stores {
        keys,
        routes,
        models,
        logs,
        rate_limiter: Arc::new(InMemoryRateLimiter::new()),
        cache: Arc::new(InMemoryCache::new()),
    };
    let app = build_app(AppState::new(
        clients,
        stores,
        PEPPER.into(),
        admin_token,
        limits,
        cache_cfg,
        Arc::new(Metrics::new()),
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

// --- rate limiting & concurrency (M4.4) ------------------------------------------

#[tokio::test]
async fn rpm_third_request_is_429_with_retry_after() {
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
    let secret = seed_key_rl(&store, Some(2), None, None).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store, None, default_limits()).await;

    let req = || {
        http()
            .post(format!("{url}/v1/chat/completions"))
            .header("authorization", format!("Bearer {secret}"))
            .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
    };
    assert_eq!(req().send().await.unwrap().status(), 200);
    assert_eq!(req().send().await.unwrap().status(), 200);
    let third = req().send().await.unwrap();
    assert_eq!(third.status(), 429);
    assert!(third.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn concurrency_one_blocks_second_parallel() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_millis(300))
                .set_body_json(json!({
                    "id": "c1", "model": "gpt-4o", "object": "chat.completion",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1}
                })),
        )
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );

    let store = mem_store().await;
    let secret = seed_key_rl(&store, None, Some(1), None).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store, None, default_limits()).await;

    let body = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
    let hdr = format!("Bearer {secret}");
    // Two simultaneous in-flight requests against a concurrency-1 key: one wins (200), one is
    // denied (429). The 300ms upstream delay keeps the winner's slot held while the loser acquires.
    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send();
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send();
    let (r1, r2) = tokio::join!(r1, r2);
    let codes = [r1.unwrap().status().as_u16(), r2.unwrap().status().as_u16()];
    assert!(codes.contains(&429), "expected one 429, got {codes:?}");
    assert!(codes.contains(&200), "expected one 200, got {codes:?}");
}

#[tokio::test]
async fn budget_exhausts_then_429() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 80, "completion_tokens": 10}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );

    let store = mem_store().await;
    // budget=100; one request reconciles 90 actual tokens, exhausting it for the next.
    let secret = seed_key_rl(&store, None, None, Some(100)).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store, None, default_limits()).await;

    // max_tokens=4 keeps the pre-call estimate (~prompt + 4) under the budget so the first is admitted.
    let body = json!({"model": "gpt-4o", "max_tokens": 4, "messages": [{"role": "user", "content": "hi"}]});
    let hdr = format!("Bearer {secret}");
    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 429);
}

#[tokio::test]
async fn stream_slot_released_after_completion() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );

    let store = mem_store().await;
    let secret = seed_key_rl(&store, None, Some(1), None).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store, None, default_limits()).await;

    let body =
        json!({"model": "gpt-4o", "stream": true, "messages": [{"role": "user", "content": "hi"}]});
    let hdr = format!("Bearer {secret}");
    // First stream: drain the body so the ReleaseGuard drops and the concurrency slot releases.
    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let _ = r1.text().await.unwrap();
    // Second stream: the slot was freed, so this is admitted (200), not 429.
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
}

// --- M4.5 logging -------------------------------------------------------------

/// Regression (M5 review): an early-return error path after rate-limit acquire must release the
/// concurrency slot. With concurrency=1, a leaked slot would lock the key out after one error —
/// the second request here would be 429 instead of 404.
#[tokio::test]
async fn concurrency_slot_released_on_error_path() {
    let store = mem_store().await;
    let secret = seed_key_rl(&store, None, Some(1), None).await; // concurrency = 1
    // No route seeded → "nope" is an unknown alias → 404 after acquiring the slot.
    let url = start_proxy(HashMap::new(), store, None, default_limits()).await;
    let hdr = format!("Bearer {secret}");
    let body = json!({"model": "nope", "messages": []});

    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 404);

    // If the 404 leaked the slot, this acquire sees in_flight=1 → 429. After the fix it's 404 again.
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r2.status(),
        404,
        "concurrency slot leaked on the error path"
    );
}

/// Poll until at least one request log exists (the write is fire-and-forget).
async fn await_log(store: &SqliteStore) -> Vec<RequestLog> {
    for _ in 0..100 {
        let logs = store.list_logs(None, None, 10).await.unwrap();
        if !logs.is_empty() {
            return logs;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("no request log written within ~1s");
}

#[tokio::test]
async fn data_plane_logs_request_and_usage() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );

    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    // Priced model so the logger computes a cost from the catalog.
    store
        .upsert_model(NewModel {
            provider: "openai".into(),
            native_model: "gpt-4o".into(),
            display_name: "GPT-4o".into(),
            context_window: None,
            max_output: None,
            price_in: Some(2.0),
            price_out: Some(6.0),
            price_cache_read: None,
            enabled: true,
        })
        .await
        .unwrap();
    // Keep a store handle for assertions (the proxy gets a clone of the same pool).
    let url = start_proxy(clients, store.clone(), None, default_limits()).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let logs = await_log(&store).await;
    assert_eq!(logs.len(), 1);
    let log = &logs[0];
    assert_eq!(log.status, 200);
    assert_eq!(log.provider, "openai");
    assert_eq!(log.model, "gpt-4o");
    assert_eq!(log.inbound_format, "openai_chat");
    assert_eq!(log.outbound_format, "openai_chat");
    assert!(!log.cached);
    assert!(log.latency_ms.is_some());

    // Usage + cost: input 10 × $2/1M + output 5 × $6/1M = $0.00005.
    let usage = store
        .usage_summary(None, None, None, None, None)
        .await
        .unwrap();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].requests, 1);
    assert_eq!(usage[0].input_tokens, 10);
    assert_eq!(usage[0].output_tokens, 5);
    let cost = usage[0].cost_usd.expect("cost computed from catalog");
    assert!((cost - 0.00005).abs() < 1e-9, "cost was {cost}");
}

#[tokio::test]
async fn stream_logs_request_at_completion() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store.clone(), None, default_limits()).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Drain the body: the generator runs to completion, which fires the log write.
    let _ = resp.text().await.unwrap();

    let logs = await_log(&store).await;
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].status, 200);
    assert_eq!(logs[0].provider, "openai");
}

// --- M4.5 admin REST ----------------------------------------------------------

const ADMIN: &str = "admin-secret";

fn admin_hdr() -> reqwest::header::HeaderValue {
    reqwest::header::HeaderValue::from_str(&format!("Bearer {ADMIN}")).unwrap()
}

#[tokio::test]
async fn admin_requires_token() {
    let url = start_proxy(
        HashMap::new(),
        mem_store().await,
        Some(ADMIN.into()),
        default_limits(),
    )
    .await;
    // No token → 401.
    let r = http()
        .get(format!("{url}/admin/keys"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // Wrong token → 401.
    let r = http()
        .get(format!("{url}/admin/keys"))
        .header("authorization", "Bearer wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);
    // Correct token → 200 (empty list).
    let r = http()
        .get(format!("{url}/admin/keys"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

/// Gate (§M4.5): create a key via admin → call the data plane → request logged + usage recorded →
/// `/admin/usage` and `/admin/logs` return the row.
#[tokio::test]
async fn admin_end_to_end_create_call_usage() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );

    let store = mem_store().await;
    let url = start_proxy(clients, store.clone(), Some(ADMIN.into()), default_limits()).await;
    let tenant = Uuid::new_v4();

    // Seed a route via admin.
    let r = http()
        .post(format!("{url}/admin/routes"))
        .header("authorization", admin_hdr())
        .json(&json!({"alias": "gpt-4o", "provider": "openai", "native_model": "gpt-4o"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // Create a key via admin; capture the one-time secret.
    let r = http()
        .post(format!("{url}/admin/keys"))
        .header("authorization", admin_hdr())
        .json(&json!({"tenant_id": tenant, "scopes": ["data"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let key: Value = r.json().await.unwrap();
    let secret = key["key"].as_str().unwrap().to_string();
    assert!(!secret.is_empty());

    // Use it on the data plane.
    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    await_log(&store).await; // wait for the fire-and-forget write

    // /admin/usage returns the aggregated row.
    let r = http()
        .get(format!("{url}/admin/usage"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let buckets: Vec<Value> = r.json().await.unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["requests"], 1);
    assert_eq!(buckets[0]["input_tokens"], 7);
    assert_eq!(buckets[0]["output_tokens"], 3);

    // /admin/logs returns the request row.
    let r = http()
        .get(format!("{url}/admin/logs"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let logs: Vec<Value> = r.json().await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["status"], 200);
    assert_eq!(logs[0]["model"], "gpt-4o");
}

#[tokio::test]
async fn admin_keys_crud() {
    let store = mem_store().await;
    let url = start_proxy(
        HashMap::new(),
        store.clone(),
        Some(ADMIN.into()),
        default_limits(),
    )
    .await;
    let tenant = Uuid::new_v4();

    let r = http()
        .post(format!("{url}/admin/keys"))
        .header("authorization", admin_hdr())
        .json(&json!({"tenant_id": tenant, "scopes": ["data"], "rpm": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let key: Value = r.json().await.unwrap();
    let id = Uuid::parse_str(key["id"].as_str().unwrap()).unwrap();

    let r = http()
        .get(format!("{url}/admin/keys"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let list: Vec<Value> = r.json().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["rpm"], 10);
    assert!(list[0]["key"].is_null()); // secret never returned on list

    // PATCH updates scopes.
    let r = http()
        .patch(format!("{url}/admin/keys/{id}"))
        .header("authorization", admin_hdr())
        .json(&json!({"scopes": ["data", "read-usage"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let updated: Value = r.json().await.unwrap();
    assert!(
        updated["scopes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s == "read-usage")
    );

    // DELETE revokes (sets revoked_at).
    let r = http()
        .delete(format!("{url}/admin/keys/{id}"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let r = http()
        .get(format!("{url}/admin/keys"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let list: Vec<Value> = r.json().await.unwrap();
    assert!(list[0]["revoked_at"].is_string());
}

#[tokio::test]
async fn admin_models_crud() {
    let store = mem_store().await;
    let url = start_proxy(
        HashMap::new(),
        store.clone(),
        Some(ADMIN.into()),
        default_limits(),
    )
    .await;

    let r = http()
        .post(format!("{url}/admin/models"))
        .header("authorization", admin_hdr())
        .json(&json!({"provider": "openai", "native_model": "gpt-test", "display_name": "Test", "price_in": 1.0, "price_out": 2.0}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = http()
        .get(format!("{url}/admin/models"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let list: Vec<Value> = r.json().await.unwrap();
    assert_eq!(list.len(), 1);

    let r = http()
        .delete(format!("{url}/admin/models/openai/gpt-test"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let r = http()
        .get(format!("{url}/admin/models"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let list: Vec<Value> = r.json().await.unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
async fn admin_routes_crud() {
    let store = mem_store().await;
    let url = start_proxy(
        HashMap::new(),
        store.clone(),
        Some(ADMIN.into()),
        default_limits(),
    )
    .await;

    let r = http()
        .post(format!("{url}/admin/routes"))
        .header("authorization", admin_hdr())
        .json(&json!({"alias": "gpt-4o", "provider": "openai", "native_model": "gpt-4o"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let r = http()
        .get(format!("{url}/admin/routes"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let list: Vec<Value> = r.json().await.unwrap();
    assert_eq!(list.len(), 1);

    let r = http()
        .delete(format!("{url}/admin/routes/gpt-4o"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let r = http()
        .get(format!("{url}/admin/routes"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let list: Vec<Value> = r.json().await.unwrap();
    assert!(list.is_empty());
}

// --- M5.1 response cache -------------------------------------------------------

/// Mount a deterministic CC completion mock that allows at most `n` upstream calls.
async fn mount_cc_completion(server: &MockServer, n: u64) {
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "cached!"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 1}
        })))
        .expect(n)
        .mount(server)
        .await;
}

/// Poll until at least one request log with `cached = true` exists.
async fn await_cached_log(store: &SqliteStore) {
    for _ in 0..100 {
        let logs = store.list_logs(None, None, 20).await.unwrap();
        if logs.iter().any(|l| l.cached) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("no cached request log written within ~1s");
}

#[tokio::test]
async fn cache_hit_short_circuits_with_header() {
    let upstream = MockServer::start().await;
    mount_cc_completion(&upstream, 1).await; // only the miss may reach upstream
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy_with_cache(
        clients,
        store.clone(),
        None,
        default_limits(),
        CacheConfig {
            enabled: true,
            ttl: std::time::Duration::from_secs(60),
        },
    )
    .await;

    let body = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
    let hdr = format!("Bearer {secret}");

    // First call: miss, populates the cache.
    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    assert_eq!(r1.headers().get("x-unillm-cache").unwrap(), "MISS");

    // Second identical call: hit, no upstream contact, same body.
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    assert_eq!(r2.headers().get("x-unillm-cache").unwrap(), "HIT");
    let b2: Value = r2.json().await.unwrap();
    assert_eq!(b2["choices"][0]["message"]["content"], "cached!");

    await_cached_log(&store).await; // the hit is logged with cached = true
}

#[tokio::test]
async fn cache_key_is_scoped_to_virtual_key() {
    let upstream = MockServer::start().await;
    mount_cc_completion(&upstream, 2).await; // key A miss + key B miss; key A's 2nd is a hit
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret_a = seed_key(&store, &["data"]).await;
    let secret_b = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy_with_cache(
        clients,
        store,
        None,
        default_limits(),
        CacheConfig {
            enabled: true,
            ttl: std::time::Duration::from_secs(60),
        },
    )
    .await;

    let body = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});

    // Key A: miss.
    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret_a}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers().get("x-unillm-cache").unwrap(), "MISS");

    // Key B: same body, different key → NOT a hit (scope isolation, no cross-key leakage).
    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret_b}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers().get("x-unillm-cache").unwrap(), "MISS");

    // Key A again: now a hit.
    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret_a}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers().get("x-unillm-cache").unwrap(), "HIT");
}

#[tokio::test]
async fn cache_invalidate_flushes_entries() {
    let upstream = MockServer::start().await;
    // miss, then a second miss after invalidation (the hit between them does not call upstream).
    mount_cc_completion(&upstream, 2).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy_with_cache(
        clients,
        store,
        Some(ADMIN.into()),
        default_limits(),
        CacheConfig {
            enabled: true,
            ttl: std::time::Duration::from_secs(60),
        },
    )
    .await;

    let body = json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]});
    let hdr = format!("Bearer {secret}");

    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.headers().get("x-unillm-cache").unwrap(), "MISS");
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.headers().get("x-unillm-cache").unwrap(), "HIT");

    // Flush the cache via the admin endpoint.
    let r = http()
        .post(format!("{url}/admin/cache/invalidate"))
        .header("authorization", admin_hdr())
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let inv: Value = r.json().await.unwrap();
    assert!(inv["invalidated"].as_u64().unwrap() >= 1);

    // Next request is a miss again.
    let r3 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r3.headers().get("x-unillm-cache").unwrap(), "MISS");
}

#[tokio::test]
async fn stream_bypasses_cache() {
    let upstream = MockServer::start().await;
    mount_sse(&upstream, "/chat/completions", CC_SSE).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy_with_cache(
        clients,
        store,
        None,
        default_limits(),
        CacheConfig {
            enabled: true,
            ttl: std::time::Duration::from_secs(60),
        },
    )
    .await;

    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // Streams never touch the cache → no X-Unillm-Cache header.
    assert!(r.headers().get("x-unillm-cache").is_none());
    let _ = r.text().await.unwrap();
}

// --- M5.2 metrics --------------------------------------------------------------

#[tokio::test]
async fn metrics_endpoint_counts_requests() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy(clients, store, None, default_limits()).await;

    // Before any request, /metrics is already valid Prometheus exposition.
    let m0 = http().get(format!("{url}/metrics")).send().await.unwrap();
    assert_eq!(m0.status(), 200);
    assert!(
        m0.text()
            .await
            .unwrap()
            .contains("# TYPE unillm_requests_total counter")
    );

    // Make one data-plane request (cache disabled → cache outcome "none").
    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // /metrics now reflects the request, its tokens, and a latency observation.
    let body = http()
        .get(format!("{url}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains(
        "unillm_requests_total{provider=\"openai\",model=\"gpt-4o\",status=\"200\",cache=\"none\"} 1"
    ));
    assert!(
        body.contains(
            "unillm_tokens_total{provider=\"openai\",model=\"gpt-4o\",kind=\"input\"} 10"
        )
    );
    assert!(
        body.contains(
            "unillm_request_duration_seconds_count{provider=\"openai\",model=\"gpt-4o\"} 1"
        )
    );
}

// --- M5.3 OpenAPI --------------------------------------------------------------

#[tokio::test]
async fn openapi_and_docs_are_served() {
    let url = start_proxy(HashMap::new(), mem_store().await, None, default_limits()).await;

    let r = http()
        .get(format!("{url}/openapi.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let spec: Value = r.json().await.unwrap();
    assert_eq!(spec["openapi"], "3.0.3");
    assert!(
        spec["paths"]
            .as_object()
            .unwrap()
            .contains_key("/admin/keys")
    );

    let r = http().get(format!("{url}/docs")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(
        r.text()
            .await
            .unwrap()
            .contains("data-url=\"/openapi.json\"")
    );
}

// --- combination / 1.0-readiness coverage --------------------------------------

#[tokio::test]
async fn cache_replays_tool_call_on_hit() {
    // A tool-call response must round-trip through the cache intact.
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"q\":\"sf\"}"}}
            ]}, "finish_reason": "tool_calls"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1}
        })))
        .expect(1)
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy_with_cache(
        clients,
        store,
        None,
        default_limits(),
        CacheConfig {
            enabled: true,
            ttl: std::time::Duration::from_secs(60),
        },
    )
    .await;
    let body = json!({"model":"gpt-4o","messages":[{"role":"user","content":"weather?"}],
        "tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object"}}}]});
    let hdr = format!("Bearer {secret}");
    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.headers().get("x-unillm-cache").unwrap(), "MISS");
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.headers().get("x-unillm-cache").unwrap(), "HIT");
    let b2: Value = r2.json().await.unwrap();
    assert_eq!(
        b2["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "get_weather"
    );
}

#[tokio::test]
async fn fallback_logs_answering_target() {
    // Primary 500 → fallback answers; the request log must record the FALLBACK provider/model.
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
            "id": "c", "model": "deepseek-chat", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        })))
        .mount(&fallback)
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
    let url = start_proxy(clients, store.clone(), None, default_limits()).await;
    let r = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", format!("Bearer {secret}"))
        .json(&json!({"model":"m","messages":[{"role":"user","content":"hi"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let logs = await_log(&store).await;
    assert_eq!(logs[0].provider, "deepseek"); // the fallback answered, not the primary openai
    assert_eq!(logs[0].model, "deepseek-chat");
}

#[tokio::test]
async fn cache_hit_serves_requested_outbound_format() {
    // The cached value is canonical; a hit re-translates to whatever outbound format the client asks
    // for (proves cross-format caching — the key excludes outbound format by design).
    let upstream = MockServer::start().await;
    mount_cc_completion(&upstream, 1).await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    seed_route(&store, "gpt-4o", "openai", "gpt-4o", vec![]).await;
    let url = start_proxy_with_cache(
        clients,
        store,
        None,
        default_limits(),
        CacheConfig {
            enabled: true,
            ttl: std::time::Duration::from_secs(60),
        },
    )
    .await;
    let body = json!({"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]});
    let hdr = format!("Bearer {secret}");
    let r1 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.headers().get("x-unillm-cache").unwrap(), "MISS");
    // Hit, but request Anthropic outbound → the cached canonical response is re-translated.
    let r2 = http()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", &hdr)
        .header("x-unillm-response-format", "anthropic")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.headers().get("x-unillm-cache").unwrap(), "HIT");
    let b2: Value = r2.json().await.unwrap();
    assert_eq!(b2["type"], "message"); // Anthropic shape, not CC choices[]
    assert_eq!(b2["content"][0]["text"], "cached!");
}

#[tokio::test]
async fn usage_grouped_by_model() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o", "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 1}
        })))
        .mount(&upstream)
        .await;
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, upstream.uri()),
    );
    let store = mem_store().await;
    let secret = seed_key(&store, &["data"]).await;
    // Two distinct models → two group_by=model buckets.
    seed_route(&store, "a", "openai", "gpt-4o", vec![]).await;
    seed_route(&store, "b", "openai", "gpt-4o-mini", vec![]).await;
    let url = start_proxy(clients, store.clone(), Some(ADMIN.into()), default_limits()).await;
    let hdr = format!("Bearer {secret}");
    for m in ["a", "b"] {
        http()
            .post(format!("{url}/v1/chat/completions"))
            .header("authorization", &hdr)
            .json(&json!({"model": m, "messages": [{"role": "user", "content": "hi"}]}))
            .send()
            .await
            .unwrap();
    }
    await_log(&store).await;
    let r = http()
        .get(format!("{url}/admin/usage?group_by=model"))
        .header("authorization", admin_hdr())
        .send()
        .await
        .unwrap();
    let buckets: Vec<Value> = r.json().await.unwrap();
    let models: Vec<String> = buckets
        .iter()
        .map(|b| b["key"].as_str().unwrap().to_string())
        .collect();
    assert!(models.contains(&"gpt-4o".to_string()));
    assert!(models.contains(&"gpt-4o-mini".to_string()));
}
