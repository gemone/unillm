//! Outbound: canonical `Response` → OpenAI Chat Completions body (inverse of §5.4).

use serde_json::{Map, Value, json};

use unillm_core::ir::{Content, ContentBlock, Item, Response, Role, StopReason, Usage};

/// Build a Chat Completions response body from a canonical [`Response`] (`DESIGN.md` §2.2, §5.4).
pub fn build_cc_response(resp: &Response) -> Value {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    for item in &resp.output {
        match item {
            Item::Message {
                role: Role::Assistant,
                content,
            } => append_assistant_text(content, &mut text),
            // Surface chain-of-thought from reasoning models (DeepSeek reasoner / v4-flash, …) as
            // CC `reasoning_content` on the assistant message (`DESIGN.md` §2.5, §5.4).
            Item::Reasoning { summary, .. } => reasoning.push_str(summary),
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
    if !reasoning.is_empty() {
        message.insert("reasoning_content".into(), json!(reasoning));
    }
    if tool_calls.is_empty() {
        message.insert("content".into(), json!(text));
    } else {
        // CC puts tool calls on the message with null content.
        message.insert("content".into(), Value::Null);
        message.insert("tool_calls".into(), Value::Array(tool_calls));
    }

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
        "usage": cc_usage(&resp.usage),
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

pub(crate) fn cc_finish_reason(sr: StopReason) -> &'static str {
    match sr {
        StopReason::EndTurn | StopReason::Paused | StopReason::Other => "stop",
        StopReason::MaxTokens => "length",
        StopReason::StopSequence => "stop_sequence",
        StopReason::ToolUse => "tool_calls",
        StopReason::Refusal => "content_filter",
    }
}

/// The Chat Completions `usage` object (shared by the non-stream builder and the stream encoder).
pub(crate) fn cc_usage(u: &Usage) -> Value {
    let prompt = u.total_input();
    let completion = u.output_tokens;
    json!({
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
        "prompt_tokens_details": { "cached_tokens": u.cache_read },
    })
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
    fn reasoning_surfaces_as_reasoning_content() {
        // A DeepSeek reasoning response → canonical → CC egress keeps `reasoning_content`.
        let native = json!({
            "id": "ds-1",
            "model": "deepseek-v4-flash",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "answer", "reasoning_content": "thinking..." },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 5 }
        });
        let resp = ChatCompletions::new(ProviderId::Deepseek)
            .parse_response(&native)
            .unwrap();
        let rebuilt = build_cc_response(&resp);
        assert_eq!(
            rebuilt["choices"][0]["message"]["reasoning_content"],
            "thinking..."
        );
        assert_eq!(rebuilt["choices"][0]["message"]["content"], "answer");
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
