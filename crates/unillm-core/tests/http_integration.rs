//! HTTP transport integration tests against a wiremock upstream (`DESIGN.md` §20).
//!
//! Exercises the full build → POST → parse / decode path for all four providers (OpenAI, DeepSeek,
//! OpenRouter via CC; Anthropic via Messages), non-streaming and streaming, plus auth headers,
//! error mapping, and retry. No real network.

use std::time::Duration;

use futures::StreamExt;
use serde_json::json;
use unillm_core::{
    Client, Content, Item, ProviderConfig, ProviderId, Request, RetryPolicy, StopReason,
    StreamEvent,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn req(s: &str) -> Request {
    serde_json::from_str(s).unwrap()
}

/// A client pointed at the mock server with retry disabled (deterministic).
async fn client(provider: ProviderId, server: &MockServer) -> Client {
    let mut cfg = ProviderConfig::new(provider, "sk-test-key");
    cfg.base_url = server.uri();
    Client::new(cfg).unwrap().with_retry(RetryPolicy::none())
}

fn assistant_text(resp: &unillm_core::Response) -> String {
    match resp.output.first() {
        Some(Item::Message {
            content: Content::Text(t),
            ..
        }) => t.clone(),
        other => panic!("expected assistant text, got {other:?}"),
    }
}

#[tokio::test]
async fn openai_create() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "chatcmpl-1",
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "hello"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 2,
                "prompt_tokens_details": {"cached_tokens": 4}
            }
        })))
        .mount(&server)
        .await;

    let c = client(ProviderId::Openai, &server).await;
    let resp = c
        .create(&req(
            r#"{"model":"gpt-4o","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(resp.stop_reason, StopReason::EndTurn);
    assert_eq!(assistant_text(&resp), "hello");
    assert_eq!(resp.usage.cache_read, 4);
    assert_eq!(resp.usage.input_tokens, 6);
}

#[tokio::test]
async fn openai_stream() {
    let server = MockServer::start().await;
    let sse = concat!(
        "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n",
        "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"id\":\"c1\",\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n"
    );
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(sse)
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&server)
        .await;

    let c = client(ProviderId::Openai, &server).await;
    let mut s = c
        .stream(&req(
            r#"{"model":"gpt-4o","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();

    let mut text = String::new();
    let mut completed = false;
    while let Some(ev) = s.next().await {
        match ev.unwrap() {
            StreamEvent::TextDelta { text: t } => text.push_str(&t),
            StreamEvent::Completed { .. } => completed = true,
            _ => {}
        }
    }
    assert_eq!(text, "Hi");
    assert!(completed);
}

#[tokio::test]
async fn anthropic_create() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "msg_1",
            "model": "claude-sonnet-4-6",
            "content": [{"type": "text", "text": "hello"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 3}
        })))
        .mount(&server)
        .await;

    let c = client(ProviderId::Anthropic, &server).await;
    let resp = c
        .create(&req(
            r#"{"model":"claude-sonnet-4-6","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(resp.provider, ProviderId::Anthropic);
    assert_eq!(assistant_text(&resp), "hello");
    assert_eq!(resp.usage.input_tokens, 10);
}

#[tokio::test]
async fn anthropic_stream() {
    let server = MockServer::start().await;
    let sse = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-6\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
    );
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(sse)
                .insert_header("content-type", "text/event-stream"),
        )
        .mount(&server)
        .await;

    let c = client(ProviderId::Anthropic, &server).await;
    let mut s = c
        .stream(&req(
            r#"{"model":"claude-sonnet-4-6","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ))
        .await
        .unwrap();

    let mut text = String::new();
    let mut completed = false;
    while let Some(ev) = s.next().await {
        match ev.unwrap() {
            StreamEvent::TextDelta { text: t } => text.push_str(&t),
            StreamEvent::Completed { .. } => completed = true,
            _ => {}
        }
    }
    assert_eq!(text, "Hi");
    assert!(completed);
}

#[tokio::test]
async fn deepseek_create_usage_quirks() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "ds-1",
            "model": "deepseek-chat",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 5,
                "prompt_cache_hit_tokens": 30,
                "prompt_cache_miss_tokens": 70
            }
        })))
        .mount(&server)
        .await;

    let c = client(ProviderId::Deepseek, &server).await;
    let resp = c
        .create(&req(r#"{"model":"deepseek-chat","input":[]}"#))
        .await
        .unwrap();
    assert_eq!(resp.usage.cache_read, 30);
    assert_eq!(resp.usage.input_tokens, 70);
    assert_eq!(resp.usage.total_input(), 100);
}

#[tokio::test]
async fn openrouter_create_cost() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "or-1",
            "model": "anthropic/claude-sonnet-4",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 50, "completion_tokens": 5, "cost": 0.0003}
        })))
        .mount(&server)
        .await;

    let c = client(ProviderId::Openrouter, &server).await;
    let resp = c
        .create(&req(r#"{"model":"anthropic/claude-sonnet-4","input":[]}"#))
        .await
        .unwrap();
    assert_eq!(resp.provider, ProviderId::Openrouter);
    assert_eq!(resp.usage.cost_usd, Some(0.0003));
}

#[tokio::test]
async fn error_429_maps_to_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({
            "error": {"message": "slow down"}
        })))
        .mount(&server)
        .await;

    let c = client(ProviderId::Openai, &server).await;
    let err = c
        .create(&req(r#"{"model":"gpt-4o","input":[]}"#))
        .await
        .unwrap_err();
    assert!(matches!(err, unillm_core::CoreError::RateLimited { .. }));
    assert_eq!(err.status_code(), 429);
}

#[tokio::test]
async fn auth_headers_cc_vs_anthropic() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "m1", "model": "claude-sonnet-4-6",
            "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        })))
        .mount(&server)
        .await;

    let cc = client(ProviderId::Openai, &server).await;
    cc.create(&req(r#"{"model":"gpt-4o","input":[]}"#))
        .await
        .unwrap();
    let an = client(ProviderId::Anthropic, &server).await;
    an.create(&req(r#"{"model":"claude-sonnet-4-6","input":[]}"#))
        .await
        .unwrap();

    let received = server.received_requests().await.expect("requests captured");
    let cc_req = received
        .iter()
        .find(|r| r.url.path() == "/chat/completions")
        .unwrap();
    assert_eq!(
        cc_req.headers.get("authorization").unwrap(),
        "Bearer sk-test-key"
    );
    let an_req = received
        .iter()
        .find(|r| r.url.path() == "/messages")
        .unwrap();
    assert_eq!(an_req.headers.get("x-api-key").unwrap(), "sk-test-key");
    assert_eq!(
        an_req.headers.get("anthropic-version").unwrap(),
        "2023-06-01"
    );
}

#[tokio::test]
async fn retries_on_500_then_succeeds() {
    let server = MockServer::start().await;
    // The 500 mock is matched first (wiremock prefers earlier mounts) and fires only once.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(json!({"error": {"message": "boom"}})),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "c1", "model": "gpt-4o",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        })))
        .mount(&server)
        .await;

    let mut cfg = ProviderConfig::new(ProviderId::Openai, "sk-test");
    cfg.base_url = server.uri();
    let c = Client::new(cfg).unwrap().with_retry(RetryPolicy {
        max_retries: 2,
        base_delay: Duration::from_millis(1),
    });

    let resp = c
        .create(&req(r#"{"model":"gpt-4o","input":[]}"#))
        .await
        .unwrap();
    assert_eq!(resp.stop_reason, StopReason::EndTurn);
    // The 500 mock was hit exactly once, then the success mock.
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}
