//! Chat Completions adapter — covers OpenAI, DeepSeek, and OpenRouter (`DESIGN.md` §5.2, §5.4).
//!
//! All three share the `/chat/completions` shape; only base URL, auth header, and a few usage
//! quirks differ (handled by [`ProviderConfig`](crate::provider::ProviderConfig) and
//! [`normalize_usage`](crate::cache::normalize_usage)). OpenRouter's `provider.order` fan-out is
//! deferred (decision D6).

use serde_json::{Value, json};

use crate::cache::normalize_usage;
use crate::error::CoreError;
use crate::ir::{
    Content, ContentBlock, ImageSource, Item, Request, Response, Role, StopReason, ToolChoice,
};
use crate::provider::{Provider, cc_finish_to_stop_reason, f32_to_value, model_string};

/// The Chat Completions dialect adapter, parameterized by the concrete provider (for usage quirks).
pub struct ChatCompletions {
    provider: crate::ir::ProviderId,
}

impl ChatCompletions {
    pub fn new(provider: crate::ir::ProviderId) -> Self {
        Self { provider }
    }
}

impl Provider for ChatCompletions {
    fn provider_id(&self) -> crate::ir::ProviderId {
        self.provider
    }

    fn dialect(&self) -> crate::provider::Dialect {
        crate::provider::Dialect::ChatCompletions
    }

    fn build_payload(&self, req: &Request) -> Value {
        let mut body = serde_json::Map::new();
        body.insert("model".into(), json!(model_string(&req.model)));
        body.insert("messages".into(), Value::Array(build_messages(req)));

        if let Some(mt) = req.max_tokens {
            body.insert("max_tokens".into(), json!(mt));
        }
        if let Some(t) = req.temperature {
            body.insert("temperature".into(), f32_to_value(t));
        }
        if let Some(p) = req.top_p {
            body.insert("top_p".into(), f32_to_value(p));
        }
        if let Some(stop) = &req.stop {
            if !stop.is_empty() {
                body.insert("stop".into(), json!(stop));
            }
        }
        if let Some(tools) = &req.tools {
            let tools_arr: Vec<Value> = tools
                .iter()
                .map(|t| {
                    let mut function = serde_json::Map::new();
                    function.insert("name".into(), json!(t.name));
                    if let Some(desc) = &t.description {
                        function.insert("description".into(), json!(desc));
                    }
                    function.insert("parameters".into(), t.input_schema.clone());
                    json!({ "type": "function", "function": Value::Object(function) })
                })
                .collect();
            body.insert("tools".into(), Value::Array(tools_arr));
        }
        if let Some(tc) = &req.tool_choice {
            body.insert("tool_choice".into(), build_tool_choice(tc));
        }
        if req.stream {
            body.insert("stream".into(), json!(true));
            body.insert("stream_options".into(), json!({ "include_usage": true }));
        }
        Value::Object(body)
    }

    fn parse_response(&self, body: &Value) -> Result<Response, CoreError> {
        let id = body
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let model = body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let choice =
            body.get("choices")
                .and_then(|c| c.get(0))
                .ok_or_else(|| CoreError::Serde {
                    message: "chat completions response missing choices[0]".into(),
                })?;

        let mut output = Vec::new();
        if let Some(msg) = choice.get("message") {
            // Reasoning models (DeepSeek reasoner / v4-flash, etc.) carry chain-of-thought in
            // `reasoning_content`; surface it as a canonical `Reasoning` item *before* the answer
            // (`DESIGN.md` §4.2). It is read-only — `build_payload` still drops inbound reasoning.
            if let Some(rc) = msg.get("reasoning_content").and_then(|v| v.as_str()) {
                if !rc.is_empty() {
                    output.push(Item::Reasoning {
                        summary: rc.to_string(),
                        encrypted: None,
                    });
                }
            }
            if let Some(content) = msg.get("content") {
                if !content.is_null() {
                    output.push(Item::Message {
                        role: Role::Assistant,
                        content: parse_content(content),
                    });
                }
            }
            if let Some(tool_calls) = msg.get("tool_calls").and_then(|v| v.as_array()) {
                for tc in tool_calls {
                    let id = tc
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let function = tc.get("function");
                    let name = function
                        .and_then(|f| f.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    // CC `function.arguments` is already a JSON string — canonical stores it verbatim.
                    let arguments = function
                        .and_then(|f| f.get("arguments"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    output.push(Item::FunctionCall {
                        id,
                        name,
                        arguments,
                    });
                }
            }
        }

        let stop_reason = choice
            .get("finish_reason")
            .and_then(|v| v.as_str())
            .map(cc_finish_to_stop_reason)
            .unwrap_or(StopReason::Other);

        let usage = body
            .get("usage")
            .map(|u| normalize_usage(self.provider, u))
            .unwrap_or_default();

        Ok(Response {
            id,
            model,
            provider: self.provider,
            output,
            stop_reason,
            usage,
        })
    }
}

/// Canonical items → CC `messages` array (`DESIGN.md` §5.2).
///
/// Consecutive `function_call` items merge into a single assistant message's `tool_calls` (their
/// natural grouping in one model turn); `reasoning` items are dropped (no CC equivalent).
fn build_messages(req: &Request) -> Vec<Value> {
    let mut messages: Vec<Value> = Vec::new();
    if let Some(sys) = &req.instructions {
        messages.push(json!({ "role": "system", "content": sys }));
    }
    for item in &req.input {
        match item {
            Item::Message { role, content } => {
                messages
                    .push(json!({ "role": role_str(*role), "content": build_content(content) }));
            }
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => {
                let call = json!({ "id": id, "type": "function", "function": { "name": name, "arguments": arguments } });
                // Merge into the previous message if it is an assistant tool_calls message.
                let merge = matches!(messages.last(), Some(last)
                    if last.get("role").and_then(|v| v.as_str()) == Some("assistant")
                    && last.get("tool_calls").is_some()
                    && last.get("content").is_none_or(|v| v.is_null()));
                if merge {
                    messages
                        .last_mut()
                        .and_then(|last| last["tool_calls"].as_array_mut())
                        .expect("tool_calls is an array")
                        .push(call);
                } else {
                    messages.push(json!({ "role": "assistant", "content": Value::Null, "tool_calls": [call] }));
                }
            }
            Item::FunctionCallOutput { call_id, output } => {
                messages
                    .push(json!({ "role": "tool", "tool_call_id": call_id, "content": output }));
            }
            Item::Reasoning { .. } => {
                // No CC equivalent; dropped (DESIGN.md §5.5).
            }
        }
    }
    messages
}

fn role_str(role: Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

/// Canonical content → CC message `content` (string or parts[]).
fn build_content(content: &Content) -> Value {
    match content {
        Content::Text(s) => Value::String(s.clone()),
        Content::Blocks(blocks) => Value::Array(blocks.iter().map(build_part).collect()),
    }
}

fn build_part(b: &ContentBlock) -> Value {
    match b {
        ContentBlock::Text { text, .. } => json!({ "type": "text", "text": text }),
        ContentBlock::Image { source, .. } => match source {
            ImageSource::Url { url } => json!({ "type": "image_url", "image_url": { "url": url } }),
            ImageSource::Base64 { media_type, data } => json!({
                "type": "image_url",
                "image_url": { "url": format!("data:{media_type};base64,{data}") }
            }),
        },
        // tool_use / tool_result blocks inside a message are atypical for CC (tools flow through
        // FunctionCall/FunctionCallOutput items) but are emitted faithfully rather than dropped.
        ContentBlock::ToolUse {
            id, name, input, ..
        } => {
            json!({ "type": "tool_use", "id": id, "name": name, "input": input })
        }
        ContentBlock::ToolResult {
            tool_use_id,
            content,
            ..
        } => {
            json!({ "type": "tool_result", "tool_use_id": tool_use_id, "content": build_content(content) })
        }
    }
}

fn build_tool_choice(tc: &ToolChoice) -> Value {
    match tc {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::Named { name } => json!({ "type": "function", "function": { "name": name } }),
    }
}

fn parse_content(content: &Value) -> Content {
    match content {
        Value::String(s) => Content::Text(s.clone()),
        Value::Array(parts) => Content::Blocks(parts.iter().map(parse_part).collect()),
        other => Content::Text(other.to_string()),
    }
}

fn parse_part(p: &Value) -> ContentBlock {
    match p.get("type").and_then(|v| v.as_str()).unwrap_or("text") {
        "image_url" => {
            let url = p
                .get("image_url")
                .and_then(|v| v.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            ContentBlock::Image {
                source: ImageSource::Url { url },
                cache_control: None,
            }
        }
        _ => ContentBlock::Text {
            text: p
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cache_control: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{CacheStrategy, ProviderId};
    use serde_json::json;

    fn cc() -> ChatCompletions {
        ChatCompletions::new(ProviderId::Openai)
    }

    fn req(json_str: &str) -> Request {
        serde_json::from_str(json_str).unwrap()
    }

    #[test]
    fn build_minimal() {
        let body = cc().build_payload(&req(
            r#"{"model":"gpt-4o","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ));
        assert_eq!(body["model"], "gpt-4o");
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"], "hi");
        assert!(body.get("stream").is_none());
    }

    #[test]
    fn build_instructions_become_system_message() {
        let body = cc().build_payload(&req(
            r#"{"model":"gpt-4o","instructions":"be brief","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ));
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][0]["content"], "be brief");
        assert_eq!(body["messages"][1]["role"], "user");
    }

    #[test]
    fn build_temperature_is_clean() {
        let body = cc().build_payload(&req(r#"{"model":"gpt-4o","temperature":0.7,"input":[]}"#));
        // Must serialize as a clean 0.7, not 0.699999988079071.
        assert_eq!(body["temperature"].to_string(), "0.7");
    }

    #[test]
    fn build_stream_adds_include_usage() {
        let body = cc().build_payload(&req(r#"{"model":"gpt-4o","stream":true,"input":[]}"#));
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn build_tools_and_tool_choice() {
        let body = cc().build_payload(&req(
            r#"{"model":"gpt-4o","input":[],"tools":[{"name":"get_weather","description":"d","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"}}"#,
        ));
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "get_weather");
        assert_eq!(body["tool_choice"], "auto");
    }

    #[test]
    fn build_image_url_and_base64() {
        let body = cc().build_payload(&req(
            r#"{"model":"gpt-4o","input":[{"type":"message","role":"user","content":[
                {"type":"image","source":{"type":"url","url":"https://x/a.png"}},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD"}}
            ]}]}"#,
        ));
        let parts = &body["messages"][0]["content"];
        assert_eq!(parts[0]["image_url"]["url"], "https://x/a.png");
        assert_eq!(parts[1]["image_url"]["url"], "data:image/png;base64,QUJD");
    }

    #[test]
    fn build_function_items_merge_into_one_assistant_message() {
        let body = cc().build_payload(&req(r#"{"model":"gpt-4o","input":[
                {"type":"function_call","id":"a","name":"f","arguments":"{}"},
                {"type":"function_call","id":"b","name":"g","arguments":"{}"},
                {"type":"function_call_output","call_id":"a","output":"{\"ok\":true}"}
            ]}"#));
        let msgs = body["messages"].as_array().unwrap();
        // Two function_calls merge into one assistant message; the output is a separate tool message.
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "assistant");
        assert_eq!(msgs[0]["tool_calls"].as_array().unwrap().len(), 2);
        assert_eq!(msgs[0]["tool_calls"][0]["id"], "a");
        assert_eq!(msgs[1]["role"], "tool");
        assert_eq!(msgs[1]["tool_call_id"], "a");
        assert_eq!(msgs[1]["content"], "{\"ok\":true}");
    }

    #[test]
    fn build_drops_reasoning_items() {
        let body = cc().build_payload(&req(r#"{"model":"gpt-4o","input":[
                {"type":"reasoning","summary":"thinking"},
                {"type":"message","role":"user","content":"hi"}
            ]}"#));
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
    }

    #[test]
    fn parse_text_response() {
        let resp = cc()
            .parse_response(&json!({
                "id":"chatcmpl-1","model":"gpt-4o",
                "choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":10,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":4}}
            }))
            .unwrap();
        assert_eq!(resp.id, "chatcmpl-1");
        assert_eq!(resp.stop_reason, StopReason::EndTurn);
        assert_eq!(resp.output.len(), 1);
        match &resp.output[0] {
            Item::Message { role, content } => {
                assert_eq!(*role, Role::Assistant);
                assert_eq!(content, &Content::Text("hello".to_string()));
            }
            other => panic!("expected Message, got {other:?}"),
        }
        assert_eq!(resp.usage.input_tokens, 6); // 10 - 4 cached
        assert_eq!(resp.usage.cache_read, 4);
        assert_eq!(resp.usage.total_input(), 10);
    }

    #[test]
    fn parse_tool_use_response() {
        let resp = cc()
            .parse_response(&json!({
                "id":"chatcmpl-2","model":"gpt-4o",
                "choices":[{"index":0,"message":{"role":"assistant","tool_calls":[
                    {"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"q\":\"sf\"}"}}
                ]},"finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":10,"completion_tokens":5}
            }))
            .unwrap();
        assert_eq!(resp.stop_reason, StopReason::ToolUse);
        assert_eq!(resp.output.len(), 1);
        match &resp.output[0] {
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => {
                assert_eq!(id, "call_1");
                assert_eq!(name, "get_weather");
                assert_eq!(arguments, "{\"q\":\"sf\"}");
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn parse_deepseek_reasoner_maps_reasoning_content() {
        let resp = ChatCompletions::new(ProviderId::Deepseek)
            .parse_response(&json!({
                "id":"ds-1","model":"deepseek-reasoner",
                "choices":[{"index":0,"message":{"role":"assistant","content":"answer","reasoning_content":"hidden"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":100,"completion_tokens":5,"prompt_cache_hit_tokens":30,"prompt_cache_miss_tokens":70}
            }))
            .unwrap();
        // reasoning_content is surfaced as a Reasoning item before the answer Message.
        assert_eq!(resp.output.len(), 2);
        match &resp.output[0] {
            Item::Reasoning { summary, encrypted } => {
                assert_eq!(summary, "hidden");
                assert_eq!(encrypted, &None);
            }
            other => panic!("expected Reasoning, got {other:?}"),
        }
        match &resp.output[1] {
            Item::Message { content, .. } => {
                assert_eq!(content, &Content::Text("answer".to_string()));
            }
            other => panic!("expected Message, got {other:?}"),
        }
        assert_eq!(resp.usage.cache_read, 30);
        assert_eq!(resp.usage.input_tokens, 70);
    }

    #[test]
    fn parse_openrouter_cost() {
        let resp = ChatCompletions::new(ProviderId::Openrouter)
            .parse_response(&json!({
                "id":"or-1","model":"anthropic/claude-sonnet-4",
                "choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":50,"completion_tokens":5,"cost":0.0003}
            }))
            .unwrap();
        assert_eq!(resp.usage.cost_usd, Some(0.0003));
        assert_eq!(resp.provider, ProviderId::Openrouter);
    }

    #[test]
    fn cache_strategy_is_not_forwarded() {
        // cache=explicit is a no-op for CC (auto-caching providers); it must not appear in the body.
        let mut r = req(r#"{"model":"gpt-4o","input":[]}"#);
        r.cache = CacheStrategy::Explicit {
            breakpoints: vec![crate::ir::Breakpoint::Last],
            ttl: crate::ir::Ttl::FiveMinutes,
        };
        let body = cc().build_payload(&r);
        assert!(body.get("cache").is_none());
    }
}
