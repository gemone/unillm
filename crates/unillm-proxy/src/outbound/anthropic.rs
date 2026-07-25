//! Outbound: canonical `Response` → Anthropic Messages body (inverse of §5.4).

use serde_json::{Value, json};

use unillm_core::ir::{Content, ContentBlock, Item, Response, Role, StopReason};

/// Build an Anthropic Messages response body from a canonical [`Response`] (`DESIGN.md` §2.4, §5.4).
pub fn build_anthropic_response(resp: &Response) -> Value {
    let mut content: Vec<Value> = Vec::new();
    for item in &resp.output {
        match item {
            Item::Message {
                role: Role::Assistant,
                content: c,
            } => push_assistant_blocks(c, &mut content),
            Item::FunctionCall {
                id,
                name,
                arguments,
            } => {
                // Canonical `arguments` (JSON string) → Anthropic `input` (object).
                let input: Value = serde_json::from_str(arguments)
                    .unwrap_or_else(|_| Value::Object(Default::default()));
                content.push(json!({ "type": "tool_use", "id": id, "name": name, "input": input }));
            }
            _ => {}
        }
    }

    json!({
        "id": resp.id,
        "type": "message",
        "role": "assistant",
        "model": resp.model,
        "content": content,
        "stop_reason": anthropic_stop_reason(resp.stop_reason),
        "stop_sequence": Value::Null,
        "usage": {
            // Anthropic `input_tokens` excludes cached tokens — matches canonical `input_tokens`.
            "input_tokens": resp.usage.input_tokens,
            "output_tokens": resp.usage.output_tokens,
            "cache_read_input_tokens": resp.usage.cache_read,
            "cache_creation_input_tokens": resp.usage.cache_creation,
        }
    })
}

fn push_assistant_blocks(content: &Content, out: &mut Vec<Value>) {
    match content {
        Content::Text(s) => out.push(json!({ "type": "text", "text": s })),
        Content::Blocks(blocks) => {
            for block in blocks {
                if let ContentBlock::Text { text, .. } = block {
                    out.push(json!({ "type": "text", "text": text }));
                }
            }
        }
    }
}

fn anthropic_stop_reason(sr: StopReason) -> &'static str {
    match sr {
        StopReason::EndTurn | StopReason::Other => "end_turn",
        StopReason::MaxTokens => "max_tokens",
        StopReason::StopSequence => "stop_sequence",
        StopReason::ToolUse => "tool_use",
        StopReason::Refusal => "refusal",
        StopReason::Paused => "pause_turn",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use unillm_core::{Anthropic, Provider};

    /// Round-trip: native Anthropic response → core parse → outbound build → equivalent Anthropic
    /// response (proves `build_anthropic_response` is the inverse of `Anthropic::parse_response`).
    #[test]
    fn roundtrip_text_response() {
        let native = json!({
            "id": "msg_1",
            "model": "claude-sonnet-4-6",
            "content": [{ "type": "text", "text": "hello" }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 10, "output_tokens": 3, "cache_read_input_tokens": 2, "cache_creation_input_tokens": 1 }
        });
        let resp = Anthropic.parse_response(&native).unwrap();
        let rebuilt = build_anthropic_response(&resp);

        assert_eq!(rebuilt["id"], "msg_1");
        assert_eq!(rebuilt["type"], "message");
        assert_eq!(rebuilt["role"], "assistant");
        assert_eq!(rebuilt["content"][0]["type"], "text");
        assert_eq!(rebuilt["content"][0]["text"], "hello");
        assert_eq!(rebuilt["stop_reason"], "end_turn");
        assert_eq!(rebuilt["usage"]["input_tokens"], 10);
        assert_eq!(rebuilt["usage"]["cache_read_input_tokens"], 2);
        assert_eq!(rebuilt["usage"]["cache_creation_input_tokens"], 1);
    }

    #[test]
    fn roundtrip_tool_use_response() {
        let native = json!({
            "id": "msg_2",
            "model": "claude-sonnet-4-6",
            "content": [
                { "type": "text", "text": "calling" },
                { "type": "tool_use", "id": "tool_1", "name": "get_weather", "input": { "q": "sf" } }
            ],
            "stop_reason": "tool_use",
            "usage": { "input_tokens": 5, "output_tokens": 2 }
        });
        let resp = Anthropic.parse_response(&native).unwrap();
        let rebuilt = build_anthropic_response(&resp);

        assert_eq!(rebuilt["stop_reason"], "tool_use");
        assert_eq!(rebuilt["content"].as_array().unwrap().len(), 2);
        assert_eq!(rebuilt["content"][1]["type"], "tool_use");
        // input round-trips through the arguments-string ↔ object boundary.
        assert_eq!(rebuilt["content"][1]["input"], json!({ "q": "sf" }));
    }

    #[test]
    fn openai_source_translated_to_anthropic() {
        let cc_native = json!({
            "id": "chatcmpl-9",
            "model": "gpt-4o",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "hi from gpt" },
                "finish_reason": "stop"
            }],
            "usage": { "prompt_tokens": 7, "completion_tokens": 2, "prompt_tokens_details": { "cached_tokens": 0 } }
        });
        let resp = unillm_core::ChatCompletions::new(unillm_core::ProviderId::Openai)
            .parse_response(&cc_native)
            .unwrap();
        let an_body = build_anthropic_response(&resp);

        assert_eq!(an_body["type"], "message");
        assert_eq!(an_body["content"][0]["text"], "hi from gpt");
        assert_eq!(an_body["stop_reason"], "end_turn");
        assert_eq!(an_body["usage"]["input_tokens"], 7);
    }
}
