//! Outbound: canonical `Response` → OpenAI Chat Completions body (inverse of §5.4).

use serde_json::{Map, Value, json};

use unillm_core::ir::{Content, ContentBlock, Item, Response, Role, StopReason};

/// Build a Chat Completions response body from a canonical [`Response`] (`DESIGN.md` §2.2, §5.4).
pub fn build_cc_response(resp: &Response) -> Value {
    let mut text = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    for item in &resp.output {
        match item {
            Item::Message {
                role: Role::Assistant,
                content,
            } => append_assistant_text(content, &mut text),
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => tool_calls.push(json!({
                "id": id,
                "type": "function",
                "function": { "name": name, "arguments": arguments }
            })),
            _ => {}
        }
    }

    let mut message = Map::new();
    message.insert("role".into(), json!("assistant"));
    if tool_calls.is_empty() {
        message.insert("content".into(), json!(text));
    } else {
        // CC puts tool calls on the message with null content.
        message.insert("content".into(), Value::Null);
        message.insert("tool_calls".into(), Value::Array(tool_calls));
    }

    let prompt_tokens = resp.usage.input_tokens + resp.usage.cache_read + resp.usage.cache_creation;
    let completion_tokens = resp.usage.output_tokens;

    json!({
        "id": resp.id,
        "object": "chat.completion",
        // Canonical `Response` carries no timestamp; `created` is not meaningful here.
        "created": 0,
        "model": resp.model,
        "choices": [{
            "index": 0,
            "message": Value::Object(message),
            "finish_reason": cc_finish_reason(resp.stop_reason),
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
            "prompt_tokens_details": { "cached_tokens": resp.usage.cache_read },
        }
    })
}

fn append_assistant_text(content: &Content, text: &mut String) {
    match content {
        Content::Text(s) => text.push_str(s),
        Content::Blocks(blocks) => {
            for block in blocks {
                if let ContentBlock::Text { text: t, .. } = block {
                    text.push_str(t);
                }
            }
        }
    }
}

fn cc_finish_reason(sr: StopReason) -> &'static str {
    match sr {
        StopReason::EndTurn | StopReason::Paused | StopReason::Other => "stop",
        StopReason::MaxTokens => "length",
        StopReason::StopSequence => "stop_sequence",
        StopReason::ToolUse => "tool_calls",
        StopReason::Refusal => "content_filter",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use unillm_core::{ChatCompletions, Provider, ProviderId};

    /// Round-trip: a native CC response → core parse → outbound build → recovers an equivalent CC
    /// response (proves `build_cc_response` is the inverse of `ChatCompletions::parse_response`).
    #[test]
    fn roundtrip_text_response() {
        let native = json!({
            "id": "chatcmpl-1",
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "hello" },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 2,
                "prompt_tokens_details": { "cached_tokens": 4 }
            }
        });
        let resp = ChatCompletions::new(ProviderId::Openai)
            .parse_response(&native)
            .unwrap();
        let rebuilt = build_cc_response(&resp);

        assert_eq!(rebuilt["id"], "chatcmpl-1");
        assert_eq!(rebuilt["model"], "gpt-4o");
        assert_eq!(rebuilt["object"], "chat.completion");
        assert_eq!(rebuilt["choices"][0]["message"]["content"], "hello");
        assert_eq!(rebuilt["choices"][0]["message"]["role"], "assistant");
        assert_eq!(rebuilt["choices"][0]["finish_reason"], "stop");
        // prompt_tokens == input(6) + cache_read(4) + creation(0) == original 10.
        assert_eq!(rebuilt["usage"]["prompt_tokens"], 10);
        assert_eq!(rebuilt["usage"]["completion_tokens"], 2);
        assert_eq!(rebuilt["usage"]["total_tokens"], 12);
        assert_eq!(
            rebuilt["usage"]["prompt_tokens_details"]["cached_tokens"],
            4
        );
    }

    #[test]
    fn roundtrip_tool_use_response() {
        let native = json!({
            "id": "c2",
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "get_weather", "arguments": "{\"q\":\"sf\"}" }
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": { "prompt_tokens": 5, "completion_tokens": 1 }
        });
        let resp = ChatCompletions::new(ProviderId::Openai)
            .parse_response(&native)
            .unwrap();
        let rebuilt = build_cc_response(&resp);

        assert_eq!(rebuilt["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(rebuilt["choices"][0]["message"]["content"], Value::Null);
        assert_eq!(
            rebuilt["choices"][0]["message"]["tool_calls"][0]["id"],
            "call_1"
        );
        assert_eq!(
            rebuilt["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            "{\"q\":\"sf\"}"
        );
    }

    #[test]
    fn anthropic_source_translated_to_cc() {
        // An Anthropic backend response, normalized to canonical, then emitted as CC.
        let anthropic_native = json!({
            "id": "msg_1",
            "model": "claude-sonnet-4-6",
            "content": [{ "type": "text", "text": "hi from claude" }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 8, "output_tokens": 3 }
        });
        let resp = unillm_core::Anthropic
            .parse_response(&anthropic_native)
            .unwrap();
        let cc_body = build_cc_response(&resp);

        assert_eq!(cc_body["object"], "chat.completion");
        assert_eq!(
            cc_body["choices"][0]["message"]["content"],
            "hi from claude"
        );
        assert_eq!(cc_body["choices"][0]["finish_reason"], "stop");
        assert_eq!(cc_body["usage"]["prompt_tokens"], 8); // canonical input_tokens
    }
}
