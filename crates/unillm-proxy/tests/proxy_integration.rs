//! Proxy integration tests: the full HTTP path through the axum server against wiremock upstreams.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use unillm_core::retry::RetryPolicy;
use unillm_core::{Client as CoreClient, ProviderConfig, ProviderId};
use unillm_proxy::route::{Route, RouteTarget, Routes};
use unillm_proxy::server::{AppState, build_app};

/// Start the proxy on an ephemeral port; return its base URL. Test clients use `RetryPolicy::none()`
/// so fallback is exercised without waiting for per-target backoff.
async fn start_proxy(routes: Routes, clients: HashMap<ProviderId, Arc<CoreClient>>) -> String {
    let app = build_app(AppState::new(routes, clients));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

fn client_for(provider: ProviderId, base_url: String) -> Arc<CoreClient> {
    let mut pc = ProviderConfig::new(provider, "sk-test");
    pc.base_url = base_url;
    Arc::new(CoreClient::new(pc).unwrap().with_retry(RetryPolicy::none()))
}

fn http() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

#[tokio::test]
async fn health_ok() {
    let url = start_proxy(Routes::new(), HashMap::new()).await;
    let resp = http().get(format!("{url}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn ready_reflects_configured_clients() {
    let url = start_proxy(Routes::new(), HashMap::new()).await;
    let resp = http().get(format!("{url}/ready")).send().await.unwrap();
    assert_eq!(resp.status(), 503);
}

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
    let url = start_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
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
    // Anthropic-shaped request → OpenAI backend → Anthropic-shaped response (default outbound = inbound).
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
    let url = start_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/messages"))
        .json(&json!({
            "model": "claude-sonnet-4-6", "max_tokens": 100,
            "messages": [{"role": "user", "content": "hi"}]
        }))
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
    let url = start_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
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
    let url = start_proxy(Routes::new(), clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .json(&json!({"model": "nope", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn stream_request_returns_501_until_m4() {
    let mut routes = Routes::new();
    routes.insert("gpt-4o", Route::single(ProviderId::Openai, "gpt-4o"));
    let mut clients = HashMap::new();
    clients.insert(
        ProviderId::Openai,
        client_for(ProviderId::Openai, "http://127.0.0.1:1".into()),
    );
    let url = start_proxy(routes, clients).await;

    let resp = http()
        .post(format!("{url}/v1/chat/completions"))
        .json(&json!({"model": "gpt-4o", "stream": true, "messages": [{"role": "user", "content": "hi"}]}))
        .send()
        .await
        .unwrap();
    // Streaming is M3.4; until then it's Not Implemented (not a 500).
    assert_eq!(resp.status(), 501);
}
